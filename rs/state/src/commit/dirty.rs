//! The in-memory overlay of trie nodes changed since the last roll. `apply`
//! queues contract writes; `root` recomputes the state root touching only
//! the dirty paths and retains the produced nodes for the next round.
//!
//! The retained nodes live in one byte slab behind a hash index of slab
//! offsets: an entry is [owner u32][plen u8][path][cap u16][len u16][blob]
//! at a slab offset, rewritten in place when the new blob fits, else
//! appended (the old slot is dead; root compacts when dead space passes
//! live).

use super::file::File;
use super::roll::{NoSink, StackTrie};
use super::trie::{NodeReader, NodeSet, Trie};
use super::{account_leaf, err, key_to_nibbles, leaf_value, pack_nibbles, parse_leaf, put_compact, Result};
use crate::keccak::EMPTY_ROOT;
use crate::rlp;
use crate::Hash;
use hashbrown::HashTable;
use std::borrow::Cow;
use std::collections::HashMap;
use std::hash::BuildHasher;
use std::sync::{Arc, Mutex};

/// Returns the first row of the ROLLED flat state (the state the File was
/// rolled from, not the live one) whose contract key is >= prefix; None
/// when there is none. Called from several threads at once.
///
/// It is how a leaf is read: the file holds no leaves, and the trie needs
/// an untouched leaf's content when a new key splits it or a delete merges
/// it upward, and the key remainder of the leaf being updated.
pub type SeekFn = dyn Fn(&[u8]) -> Option<(Vec<u8>, Vec<u8>)> + Send + Sync;

#[derive(Default)]
struct Pending {
    row: Option<Vec<u8>>, // contract account value; None when only slots were applied
    del: bool,            // the last account write was a delete
    wiped: bool,          // a delete happened this round: storage starts empty
    slots: HashMap<Hash, Vec<u8>>,
}

/// Contract writes queued for one root computation off a Dirty (`layer_root`).
#[derive(Default)]
pub struct Writes(HashMap<Hash, Pending>);

impl Writes {
    pub fn apply(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        queue(&mut self.0, key, value)
    }
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// The trie nodes one pending block's root produced over its parent's state
/// (a copy-on-write layer: reads fall through to the parents, then the Dirty).
pub struct Layer {
    pub parent_root: Hash,
    pub root: Hash,
    nodes: HashMap<(Hash, Vec<u8>), Vec<u8>>,
}

impl Layer {
    pub fn nodes(&self) -> usize {
        self.nodes.len()
    }
    pub fn bytes(&self) -> usize {
        self.nodes.iter().map(|((_, p), b)| p.len() + b.len() + 40).sum()
    }
}

/// Layers newest-last over the base Dirty.
struct Stack<'a> {
    layers: &'a [&'a Layer],
    base: &'a Dirty,
}

impl NodeReader for Stack<'_> {
    fn node(&self, owner: &Hash, path: &[u8]) -> Result<Cow<'_, [u8]>> {
        for l in self.layers.iter().rev() {
            if let Some(b) = l.nodes.get(&(*owner, path.to_vec())) {
                return Ok(Cow::Borrowed(b));
            }
        }
        self.base.node(owner, path)
    }
}

fn queue(acct: &mut HashMap<Hash, Pending>, key: &[u8], value: &[u8]) -> Result<()> {
    if key.len() == 33 && key[32] == 0 {
        let p = acct.entry(key[..32].try_into().unwrap()).or_default();
        if value.is_empty() {
            *p = Pending { del: true, wiped: true, ..Default::default() };
        } else {
            p.row = Some(value.to_vec());
            p.del = false;
        }
    } else if key.len() == 65 && key[32] == 1 {
        let p = acct.entry(key[..32].try_into().unwrap()).or_default();
        p.slots.insert(key[33..].try_into().unwrap(), value.to_vec());
    } else {
        return err(format!("commit: malformed key {}", hex(key)));
    }
    Ok(())
}

fn storage(reader: &dyn NodeReader, owner: Hash, root: Hash, slots: &HashMap<Hash, Vec<u8>>) -> Result<(Hash, NodeSet)> {
    let mut t = Trie::new(reader, owner, root);
    for (k, v) in slots {
        if v.is_empty() {
            t.delete(k)?;
        } else {
            t.update(k, rlp::bytes(v))?;
        }
    }
    Ok(t.commit())
}

/// The new state root of `pending` applied over `root` as `reader` sees it,
/// and every node set produced (storage tries first, the account trie last).
/// Storage tries are hashed in parallel, the account trie after them.
fn compute(reader: &dyn NodeReader, root: Hash, pending: HashMap<Hash, Pending>, workers: usize) -> Result<(Hash, Vec<NodeSet>)> {
    struct Job {
        hash: Hash,
        p: Pending,
        cur: Option<super::LeafFields>,
        root: Hash,
    }
    let mut acc = Trie::new(reader, ZERO, root);
    let mut jobs = Vec::with_capacity(pending.len());
    for (hash, p) in pending {
        let mut j = Job { hash, p, cur: None, root: EMPTY_ROOT };
        if !j.p.del {
            if let Some(val) = acc.get(&hash)? {
                j.cur = Some(parse_leaf(&val)?);
            }
            if j.cur.is_none() && j.p.row.is_none() {
                return err(format!("commit: slots written for missing account {}", hex(&hash)));
            }
            if let Some(cur) = &j.cur {
                if !j.p.wiped {
                    j.root = cur.root;
                }
            }
        }
        jobs.push(j);
    }
    let work: Vec<usize> = jobs.iter().enumerate().filter(|(_, j)| !j.p.del && !j.p.slots.is_empty()).map(|(i, _)| i).collect();
    let results: Mutex<Vec<(usize, Result<(Hash, NodeSet)>)>> = Mutex::new(Vec::with_capacity(work.len()));
    let next = std::sync::atomic::AtomicUsize::new(0);
    let nworkers = workers.max(1).min(work.len().max(1));
    let slots: usize = work.iter().map(|&i| jobs[i].p.slots.len()).sum();
    if nworkers <= 1 || slots < PAR_MIN_SLOTS {
        // ponytail: a scoped thread costs tens of microseconds to spawn and
        // a per-block root has a handful of slots; hash them inline, and
        // fan out only when there is enough work to pay for the threads.
        for &i in &work {
            let j = &jobs[i];
            let r = storage(reader, j.hash, j.root, &j.p.slots);
            results.lock().unwrap().push((i, r));
        }
    } else {
        std::thread::scope(|s| {
            for _ in 0..nworkers {
                s.spawn(|| loop {
                    let i = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    if i >= work.len() {
                        break;
                    }
                    let j = &jobs[work[i]];
                    let r = storage(reader, j.hash, j.root, &j.p.slots);
                    results.lock().unwrap().push((work[i], r));
                });
            }
        });
    }
    let mut results = results.into_inner().unwrap();
    results.sort_by_key(|(i, _)| *i);
    let mut sets = Vec::with_capacity(results.len() + 1);
    for (i, r) in results {
        let (root, set) = r?;
        jobs[i].root = root;
        sets.push(set);
    }
    for j in &jobs {
        if j.p.del {
            // The account's retained storage nodes go stale here. Nothing
            // can reach them (a recreated account starts from the empty
            // root and rewrites every node it touches), so they are left
            // for the roll to drop.
            acc.delete(&j.hash)?;
            continue;
        }
        let val = match &j.p.row {
            Some(row) => account_leaf(row, &j.root)?,
            None => {
                let cur = j.cur.as_ref().unwrap();
                leaf_value(&cur.nonce, &cur.balance, &j.root, &cur.code)
            }
        };
        acc.update(&j.hash, val)?;
    }
    let (root, set) = acc.commit();
    sets.push(set);
    Ok((root, sets))
}

pub struct Dirty {
    f: Arc<File>,
    seek: Arc<SeekFn>,
    root: Hash,
    table: HashTable<u64>,
    slab: Vec<u8>,
    dead: usize,
    owners: Vec<Hash>,
    owner_ids: HashMap<Hash, u32>,
    acct: HashMap<Hash, Pending>,
    hasher: foldhash::fast::RandomState,
    /// Bounds the storage-trie hashing pool (default: available parallelism).
    pub workers: usize,
}

const ZERO: Hash = [0u8; 32];
/// Slot updates below which `root` hashes the storage tries on the calling
/// thread instead of a scoped pool.
const PAR_MIN_SLOTS: usize = 256;

impl Dirty {
    /// Starts an empty overlay over f.
    pub fn new(f: Arc<File>, seek: Arc<SeekFn>) -> Dirty {
        let root = f.root();
        Dirty {
            f,
            seek,
            root,
            table: HashTable::new(),
            slab: Vec::new(),
            dead: 0,
            owners: Vec::new(),
            owner_ids: HashMap::new(),
            acct: HashMap::new(),
            hasher: foldhash::fast::RandomState::default(),
            workers: std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1),
        }
    }

    /// Drops every retained node and rebinds the overlay to a freshly
    /// rolled file.
    pub fn reset(&mut self, f: Arc<File>) {
        self.root = f.root();
        self.f = f;
        self.table = HashTable::new();
        self.slab = Vec::new();
        self.dead = 0;
        self.owners.clear();
        self.owner_ids.clear();
        self.acct.clear();
    }

    /// The size of the retained node slab (dead space included).
    pub fn bytes(&self) -> usize {
        self.slab.len()
    }

    /// The retained node count.
    pub fn nodes(&self) -> usize {
        self.table.len()
    }

    pub fn current_root(&self) -> Hash {
        self.root
    }

    fn hash_key(&self, owner: u32, path: &[u8]) -> u64 {
        let mut h = self.hasher.build_hasher();
        std::hash::Hasher::write_u32(&mut h, owner);
        std::hash::Hasher::write(&mut h, path);
        std::hash::Hasher::finish(&h)
    }

    /// Entry layout helpers over the slab.
    #[inline]
    fn entry_matches(slab: &[u8], off: u64, owner: u32, path: &[u8]) -> bool {
        let o = off as usize;
        u32::from_le_bytes(slab[o..o + 4].try_into().unwrap()) == owner && slab[o + 4] as usize == path.len() && &slab[o + 5..o + 5 + path.len()] == path
    }

    #[inline]
    fn entry_blob(slab: &[u8], off: u64) -> &[u8] {
        let o = off as usize + 5 + slab[off as usize + 4] as usize;
        let len = u16::from_le_bytes([slab[o + 2], slab[o + 3]]) as usize;
        &slab[o + 4..o + 4 + len]
    }

    fn lookup(&self, owner: &Hash, path: &[u8]) -> Option<&[u8]> {
        let &id = self.owner_ids.get(owner)?;
        let h = self.hash_key(id, path);
        let off = *self.table.find(h, |&off| Self::entry_matches(&self.slab, off, id, path))?;
        Some(Self::entry_blob(&self.slab, off))
    }

    /// Stores blob at owner/path: in place when it fits the slot, else in a
    /// new slot with the length rounded up to 64 so a growing node relocates
    /// rarely.
    fn put(&mut self, owner: &Hash, path: &[u8], blob: &[u8]) {
        let id = match self.owner_ids.get(owner) {
            Some(&id) => id,
            None => {
                let id = self.owners.len() as u32;
                self.owners.push(*owner);
                self.owner_ids.insert(*owner, id);
                id
            }
        };
        let h = self.hash_key(id, path);
        let new_off = self.slab.len() as u64;
        match self.table.find_mut(h, |&off| Self::entry_matches(&self.slab, off, id, path)) {
            Some(cur) => {
                let o = *cur as usize + 5 + path.len();
                let cap = u16::from_le_bytes([self.slab[o], self.slab[o + 1]]) as usize;
                if blob.len() <= cap {
                    self.slab[o + 2..o + 4].copy_from_slice(&(blob.len() as u16).to_le_bytes());
                    self.slab[o + 4..o + 4 + blob.len()].copy_from_slice(blob);
                    return;
                }
                self.dead += cap + 9 + path.len();
                *cur = new_off;
            }
            None => {
                let (slab, owners, hasher) = (&self.slab, &self.owners, &self.hasher);
                let _ = owners;
                self.table.insert_unique(h, new_off, |&off| {
                    let o = off as usize;
                    let id = u32::from_le_bytes(slab[o..o + 4].try_into().unwrap());
                    let plen = slab[o + 4] as usize;
                    let mut hh = hasher.build_hasher();
                    std::hash::Hasher::write_u32(&mut hh, id);
                    std::hash::Hasher::write(&mut hh, &slab[o + 5..o + 5 + plen]);
                    std::hash::Hasher::finish(&hh)
                });
            }
        }
        let cap = (blob.len() + 63) & !63;
        self.slab.extend_from_slice(&id.to_le_bytes());
        self.slab.push(path.len() as u8);
        self.slab.extend_from_slice(path);
        self.slab.extend_from_slice(&(cap as u16).to_le_bytes());
        self.slab.extend_from_slice(&(blob.len() as u16).to_le_bytes());
        self.slab.extend_from_slice(blob);
        self.slab.resize(self.slab.len() + cap - blob.len(), 0);
    }

    /// Rewrites the slab without its dead slots once they outweigh the live
    /// ones (and are worth the copy).
    fn compact(&mut self) {
        if self.dead < 32 << 20 || self.dead < self.slab.len() / 2 {
            return;
        }
        let mut slab = Vec::with_capacity(self.slab.len() - self.dead);
        let old = std::mem::take(&mut self.slab);
        for off in self.table.iter_mut() {
            let o = *off as usize;
            let plen = old[o + 4] as usize;
            let cap = u16::from_le_bytes([old[o + 5 + plen], old[o + 6 + plen]]) as usize;
            let at = slab.len() as u64;
            slab.extend_from_slice(&old[o..o + 9 + plen + cap]);
            *off = at;
        }
        self.slab = slab;
        self.dead = 0;
    }

    /// Queues one contract write. Keys may come in any order; an empty
    /// value deletes, and deleting an account drops its slots.
    pub fn apply(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        queue(&mut self.acct, key, value)
    }

    /// Applies the queued writes and returns the new state root. Storage
    /// tries are hashed in parallel, the account trie after them.
    pub fn root(&mut self) -> Result<Hash> {
        let pending = std::mem::take(&mut self.acct);
        let (root, sets) = compute(self, self.root, pending, self.workers)?;
        for set in &sets {
            self.merge(set);
        }
        self.compact();
        self.root = root;
        Ok(root)
    }

    /// The root of `writes` applied on top of `parents` (newest last) over
    /// this Dirty, as a `Layer` that holds only the nodes it produced: a
    /// pending block's state root, siblings sharing everything below. Nothing
    /// here changes; `absorb` folds the layer in once the block is accepted.
    pub fn layer_root(&self, parents: &[&Layer], writes: Writes) -> Result<Layer> {
        let base = parents.last().map_or(self.root, |l| l.root);
        let stack = Stack { layers: parents, base: self };
        let (root, sets) = compute(&stack, base, writes.0, self.workers)?;
        let mut nodes = HashMap::new();
        for set in sets {
            for (path, blob) in set.nodes {
                nodes.insert((set.owner, path), blob);
            }
        }
        Ok(Layer { parent_root: base, root, nodes })
    }

    /// Folds an accepted layer in: its nodes and its root. The layer must
    /// have been computed over this Dirty's current root.
    pub fn absorb(&mut self, l: &Layer) -> Result<()> {
        if l.parent_root != self.root {
            return err(format!("commit: layer built over root {} absorbed into root {}", hex(&l.parent_root), hex(&self.root)));
        }
        for ((owner, path), blob) in &l.nodes {
            self.put(owner, path, blob);
        }
        self.compact();
        self.root = l.root;
        Ok(())
    }

    fn merge(&mut self, set: &NodeSet) {
        for (path, blob) in &set.nodes {
            self.put(&set.owner, path, blob);
        }
    }

    /// Rebuilds the leaf node at path from the rolled flat state.
    pub fn leaf(&self, owner: &Hash, path: &[u8]) -> Result<Vec<u8>> {
        let packed = pack_nibbles(path);
        let prefix = if owner == &ZERO {
            packed
        } else {
            let mut p = Vec::with_capacity(33 + packed.len());
            p.extend_from_slice(owner);
            p.push(1);
            p.extend_from_slice(&packed);
            p
        };
        let Some((key, val)) = (self.seek)(&prefix) else { return err("commit: no flat row under the requested trie path") };
        let (hashed, value): (&[u8], Vec<u8>) = if owner == &ZERO && key.len() == 33 && key[32] == 0 {
            let hashed: Hash = key[..32].try_into().unwrap();
            let (mut root, ok) = self.f.storage_root(&hashed);
            if !ok {
                // One slot, or none: the root is the leaf's own hash, or empty.
                let mut p = hashed.to_vec();
                p.push(1);
                if let Some((k, v)) = (self.seek)(&p) {
                    if k.len() == 65 && k[32] == 1 && k[..32] == hashed[..] {
                        let mut st = StackTrie::new();
                        st.update(&k[33..], &rlp::bytes(&v), &mut NoSink)?;
                        root = st.hash_root(&mut NoSink);
                    }
                }
            }
            (&key[..32], account_leaf(&val, &root)?)
        } else if owner != &ZERO && key.len() == 65 && key[32] == 1 && key[..32] == owner[..] {
            (&key[33..], rlp::bytes(&val))
        } else {
            return err("commit: no flat row under the requested trie path");
        };
        let nib = key_to_nibbles(hashed);
        if nib.len() < path.len() || nib[..path.len()] != path[..] {
            return err("commit: no flat row under the requested trie path");
        }
        let mut ck = Vec::with_capacity(33);
        put_compact(&mut ck, &nib[path.len()..], true);
        let mut body = Vec::with_capacity(ck.len() + value.len() + 4);
        rlp::put_bytes(&mut body, &ck);
        rlp::put_bytes(&mut body, &value);
        let mut out = Vec::with_capacity(body.len() + 3);
        rlp::put_header(&mut out, true, body.len());
        out.extend_from_slice(&body);
        Ok(out)
    }
}

/// Dirty nodes first, then the file, then a leaf fabricated from the rolled
/// flat row.
impl NodeReader for Dirty {
    fn node(&self, owner: &Hash, path: &[u8]) -> Result<Cow<'_, [u8]>> {
        if let Some(blob) = self.lookup(owner, path) {
            return Ok(Cow::Borrowed(blob));
        }
        if let Some(blob) = self.f.node(owner, path) {
            return Ok(Cow::Borrowed(blob));
        }
        self.leaf(owner, path).map(Cow::Owned)
    }
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

//! Revision lifecycle over the arena trie: proposals off a parent root,
//! block identities, acceptance with sibling invalidation, retention of the
//! last N accepted revisions, an accepted-record journal and full-arena
//! checkpoints with journal replay on restart. Mirrors the Go `pruned` rules.

use std::collections::{HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::trie::*;

#[derive(Debug)]
pub enum Error {
    /// The requested revision is not retained (Go: ErrPruned).
    Pruned,
    Msg(String),
}
impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Error::Msg(e.to_string())
    }
}
impl From<String> for Error {
    fn from(s: String) -> Self {
        Error::Msg(s)
    }
}
impl From<&str> for Error {
    fn from(s: &str) -> Self {
        Error::Msg(s.to_string())
    }
}
pub type Result<T> = std::result::Result<T, Error>;

pub struct Config {
    pub dir: PathBuf,
    pub retain: u64,
    pub commit_interval: u64,
    pub journal_limit: i64,
}

type RevId = u32;
const ZERO: Hash = [0; 32];

struct Rev {
    root: u32,
    hash: Hash,
    block: Hash,
    height: u64,
    seq: u64,
    parent: Option<RevId>,
    ops: Vec<u8>,
    fresh: Vec<u32>,
    garbage: Vec<u32>,
    accepted: bool,
    invalid: bool,
}

pub struct Store {
    cfg: Config,
    arena: Arena,
    revs: HashMap<RevId, Rev>,
    next: RevId,
    current: RevId,
    history: Vec<RevId>,
    blocks: HashMap<Hash, RevId>,
    roots: HashMap<Hash, Vec<RevId>>,
    possible: HashMap<(Hash, Hash), RevId>,
    wal: Option<File>,
    wal_bytes: i64,
    _lock: File,
    gen: u64,
    closed: bool,
    fault: Option<String>,
}

// Op stream shared with the Go side.
pub const OP_ACCOUNT: u8 = 1; // hashed(32) row(72)
pub const OP_DELETE: u8 = 2; // hashed(32)
pub const OP_SLOT: u8 = 3; // hashed(32) slot(32) len(1) value(len); len 0 deletes

#[derive(Default)]
struct Pending {
    row: Option<[u8; ROW]>,
    del: bool,
    wiped: bool,
    slots: HashMap<Hash, ([u8; 32], u8)>,
}

fn parse_ops(ops: &[u8]) -> Result<HashMap<Hash, Pending>> {
    let mut out: HashMap<Hash, Pending> = HashMap::new();
    let mut i = 0;
    let take = |i: &mut usize, n: usize| -> Result<&[u8]> {
        if *i + n > ops.len() {
            return Err("truncated op stream".into());
        }
        let s = &ops[*i..*i + n];
        *i += n;
        Ok(s)
    };
    while i < ops.len() {
        let tag = take(&mut i, 1)?[0];
        let key: Hash = take(&mut i, 32)?.try_into().unwrap();
        let p = out.entry(key).or_default();
        match tag {
            OP_ACCOUNT => {
                p.row = Some(take(&mut i, ROW)?.try_into().unwrap());
                p.del = false;
            }
            OP_DELETE => *p = Pending { del: true, wiped: true, ..Default::default() },
            OP_SLOT => {
                let slot: Hash = take(&mut i, 32)?.try_into().unwrap();
                let len = take(&mut i, 1)?[0] as usize;
                if len > 32 {
                    return Err("slot value over 32 bytes".into());
                }
                let mut v = [0u8; 32];
                v[..len].copy_from_slice(take(&mut i, len)?);
                p.slots.insert(slot, (v, len as u8));
            }
            _ => return Err(format!("unknown op tag {tag}").into()),
        }
    }
    Ok(out)
}

fn hex(h: &Hash) -> String {
    h.iter().map(|b| format!("{b:02x}")).collect()
}

impl Store {
    pub fn open(mut cfg: Config) -> Result<Store> {
        if cfg.retain == 0 {
            cfg.retain = 32;
        }
        if cfg.retain < 2 {
            return Err("retain must be at least two revisions".into());
        }
        if cfg.commit_interval == 0 {
            cfg.commit_interval = cfg.retain - 1;
        }
        cfg.commit_interval = cfg.commit_interval.min(cfg.retain - 1);
        if cfg.journal_limit == 0 {
            cfg.journal_limit = 64 << 20;
        }
        if cfg.journal_limit < 1 {
            return Err("journal limit must be positive".into());
        }
        fs::create_dir_all(&cfg.dir)?;
        let lock = OpenOptions::new().create(true).read(true).write(true).open(cfg.dir.join("LOCK"))?;
        if lock.try_lock().is_err() {
            return Err("state directory is in use".into());
        }
        let mut s = Store {
            cfg,
            arena: Arena::default(),
            revs: HashMap::new(),
            next: 0,
            current: 0,
            history: Vec::new(),
            blocks: HashMap::new(),
            roots: HashMap::new(),
            possible: HashMap::new(),
            wal: None,
            wal_bytes: 0,
            _lock: lock,
            gen: 0,
            closed: false,
            fault: None,
        };
        s.restore()?;
        Ok(s)
    }

    fn check(&self) -> Result<()> {
        if self.closed {
            return Err("database is closed".into());
        }
        match &self.fault {
            Some(f) => Err(f.clone().into()),
            None => Ok(()),
        }
    }

    fn rev(&self, id: RevId) -> &Rev {
        &self.revs[&id]
    }

    /// A revision is readable while it is not invalidated and the accepted
    /// revision it hangs off is still within the retention window.
    fn valid(&self, mut id: RevId) -> bool {
        loop {
            let Some(r) = self.revs.get(&id) else { return false };
            if r.invalid {
                return false;
            }
            if r.accepted {
                return self.rev(self.current).seq - r.seq < self.cfg.retain;
            }
            match r.parent {
                Some(p) => id = p,
                None => return false,
            }
        }
    }

    fn find(&self, root: &Hash) -> Result<RevId> {
        self.check()?;
        if self.rev(self.current).hash == *root {
            return Ok(self.current);
        }
        if let Some(list) = self.roots.get(root) {
            for &id in list.iter().rev() {
                if self.valid(id) {
                    return Ok(id);
                }
            }
        }
        Err(Error::Pruned)
    }

    pub fn has_root(&self, root: &Hash) -> Result<()> {
        self.find(root).map(|_| ())
    }

    pub fn get_account(&self, root: &Hash, key: &Hash) -> Result<Option<[u8; ROW]>> {
        let r = self.rev(self.find(root)?);
        Ok(self.arena.get(r.root, key).map(|n| n.data))
    }

    pub fn get_storage(&self, root: &Hash, key: &Hash, slot: &Hash) -> Result<Option<([u8; 32], u8)>> {
        let r = self.rev(self.find(root)?);
        let Some(acc) = self.arena.get(r.root, key) else { return Ok(None) };
        let mut v = [0u8; 32];
        Ok(self.arena.get(acc.link, slot).map(|n| {
            v[..n.len as usize].copy_from_slice(n.value());
            (v, n.len)
        }))
    }

    pub fn head(&self) -> (Hash, Hash, u64) {
        let c = self.rev(self.current);
        (c.hash, c.block, c.height)
    }

    pub fn bytes(&self) -> usize {
        self.arena.bytes()
    }

    fn compute(&mut self, parent: RevId, ops: &[u8]) -> Result<Rev> {
        let pending = parse_ops(ops)?;
        let mut root = self.rev(parent).root;
        let mut b = Build::new(&mut self.arena);
        let mut apply = || -> Result<u32> {
            for (h, p) in &pending {
                if p.del {
                    if let Some(r) = b.del(root, h, 0) {
                        root = r;
                    }
                    continue;
                }
                let cur = b.arena.get(root, h).copied();
                let row = match (p.row, &cur) {
                    (Some(row), _) => row,
                    (None, Some(c)) => c.data,
                    (None, None) => return Err(format!("slots written for missing account {}", hex(h)).into()),
                };
                let mut storage = match &cur {
                    Some(c) if !p.wiped => c.link,
                    Some(c) => {
                        b.discard_tree(c.link);
                        NONE
                    }
                    None => NONE,
                };
                for (k, (v, len)) in &p.slots {
                    storage = if *len == 0 {
                        b.del(storage, k, 0).unwrap_or(storage)
                    } else {
                        b.put(storage, 0, Node::slot(k, &v[..*len as usize]))
                    };
                }
                root = b.put(root, 0, Node::account(h, &row, storage));
            }
            b.hash(root);
            Ok(root)
        };
        let result = apply();
        let Build { fresh, garbage, .. } = b;
        match result {
            Err(e) => {
                for n in fresh {
                    self.arena.release(n);
                }
                Err(e)
            }
            Ok(root) => Ok(Rev {
                root,
                hash: self.arena.root_hash(root),
                block: ZERO,
                height: 0,
                seq: 0,
                parent: Some(parent),
                ops: ops.to_vec(),
                fresh,
                garbage,
                accepted: false,
                invalid: false,
            }),
        }
    }

    fn insert(&mut self, r: Rev) -> RevId {
        let id = self.next;
        self.next += 1;
        self.revs.insert(id, r);
        id
    }

    /// Drops a revision nobody references; a never-accepted one gives its
    /// nodes back.
    fn drop_rev(&mut self, id: RevId) {
        if let Some(r) = self.revs.remove(&id) {
            if !r.accepted {
                for n in r.fresh {
                    self.arena.release(n);
                }
            }
        }
    }

    fn gc_revs(&mut self) {
        let mut live: HashSet<RevId> = HashSet::new();
        live.insert(self.current);
        live.extend(self.history.iter());
        live.extend(self.blocks.values());
        live.extend(self.roots.values().flatten());
        live.extend(self.possible.values());
        let dead: Vec<RevId> = self.revs.keys().filter(|id| !live.contains(id)).copied().collect();
        for id in dead {
            self.drop_rev(id);
        }
    }

    pub fn propose(&mut self, parent: &Hash, ops: &[u8]) -> Result<Hash> {
        self.check()?;
        let p = self.find(parent)?;
        let r = self.compute(p, ops)?;
        let hash = r.hash;
        let id = self.insert(r);
        if let Some(old) = self.possible.insert((*parent, hash), id) {
            self.drop_rev(old);
        }
        Ok(hash)
    }

    fn clear_possible(&mut self) {
        self.possible.clear();
        self.gc_revs();
    }

    pub fn update(&mut self, root: &Hash, parent: &Hash, height: u64, parent_hash: &Hash, block_hash: &Hash) -> Result<()> {
        self.check()?;
        if let Some(&old) = self.blocks.get(block_hash) {
            if self.valid(old) {
                let o = self.rev(old);
                if o.hash != *root || o.height != height {
                    return Err("conflicting block identity".into());
                }
                self.clear_possible();
                return Ok(());
            }
        }
        let p = match self.blocks.get(parent_hash) {
            Some(&p) if self.valid(p) && self.rev(p).hash == *parent => p,
            _ => return Err(format!("unknown parent {} at height {height}", hex(parent_hash)).into()),
        };
        let pr = self.rev(p);
        if height != pr.height + 1 && !(height == 0 && *parent_hash == ZERO && pr.seq == 0) {
            return Err("nonconsecutive proposal height".into());
        }
        // Distinct block hashes may describe the same state transition.
        let alias = self.roots.get(root).and_then(|list| {
            list.iter().copied().find(|&e| {
                let er = self.rev(e);
                !er.accepted && !er.invalid && er.parent == Some(p) && er.height == height
            })
        });
        if let Some(e) = alias {
            self.blocks.insert(*block_hash, e);
            self.clear_possible();
            return Ok(());
        }
        let id = match self.possible.remove(&(*parent, *root)) {
            Some(id) if self.rev(id).parent == Some(p) => id,
            Some(id) => {
                // Computed off another revision with the parent root; rebuild
                // on p so the new nodes hang off p's tree.
                let ops = self.rev(id).ops.clone();
                self.drop_rev(id);
                let r = self.compute(p, &ops)?;
                self.insert(r)
            }
            None if root == parent => {
                let r = Rev {
                    root: self.rev(p).root,
                    hash: *root,
                    block: ZERO,
                    height: 0,
                    seq: 0,
                    parent: Some(p),
                    ops: Vec::new(),
                    fresh: Vec::new(),
                    garbage: Vec::new(),
                    accepted: false,
                    invalid: false,
                };
                self.insert(r)
            }
            None => return Err(format!("no computed proposal for root {}", hex(root)).into()),
        };
        let r = self.revs.get_mut(&id).unwrap();
        r.block = *block_hash;
        r.height = height;
        self.blocks.insert(*block_hash, id);
        self.roots.entry(*root).or_default().push(id);
        self.clear_possible();
        Ok(())
    }

    pub fn commit(&mut self, root: &Hash) -> Result<()> {
        self.check()?;
        let mut selected = None;
        for &id in self.roots.get(root).map(|v| v.as_slice()).unwrap_or(&[]) {
            let r = self.rev(id);
            if r.accepted || r.invalid || r.parent != Some(self.current) {
                continue;
            }
            if selected.is_some() {
                return Err("ambiguous accepted proposal".into());
            }
            selected = Some(id);
        }
        let Some(id) = selected else {
            return Err(format!("no accepted child for root {}", hex(root)).into());
        };
        let seq = self.rev(self.current).seq + 1;
        self.revs.get_mut(&id).unwrap().seq = seq;
        let r = || -> Result<()> {
            self.append_journal(id)?;
            self.accept(id);
            if self.wal_bytes >= self.cfg.journal_limit {
                self.checkpoint()?;
            }
            Ok(())
        }();
        if let Err(Error::Msg(m)) = &r {
            self.fault = Some(m.clone());
        }
        r
    }

    fn accept(&mut self, id: RevId) {
        let old = self.current;
        self.revs.get_mut(&id).unwrap().accepted = true;
        self.current = id;
        self.history.push(id);
        // A decision invalidates sibling branches, including their descendants.
        let candidates: Vec<RevId> = self.blocks.values().copied().collect();
        for c in candidates {
            if self.rev(c).accepted {
                continue;
            }
            let mut p = c;
            while let Some(pp) = self.rev(p).parent {
                if self.rev(pp).accepted {
                    break;
                }
                p = pp;
            }
            if self.rev(p).parent == Some(old) {
                self.revs.get_mut(&c).unwrap().invalid = true;
            }
        }
        let r = self.revs.get_mut(&id).unwrap();
        r.parent = None;
        r.ops = Vec::new();
        while self.history.len() > self.cfg.retain as usize {
            let retired = self.history.remove(0);
            let r = self.revs.get_mut(&retired).unwrap();
            r.invalid = true;
            let garbage = std::mem::take(&mut r.garbage);
            for n in garbage {
                self.arena.release(n);
            }
        }
        let stale: Vec<Hash> = self.blocks.iter().filter(|(_, &r)| !self.valid(r)).map(|(h, _)| *h).collect();
        for h in stale {
            self.blocks.remove(&h);
        }
        let mut roots = std::mem::take(&mut self.roots);
        roots.retain(|_, list| {
            list.retain(|&r| self.valid(r));
            !list.is_empty()
        });
        self.roots = roots;
        self.gc_revs();
    }

    pub fn set_hash_and_height(&mut self, hash: &Hash, height: u64) {
        if self.closed {
            return;
        }
        let cur = self.rev(self.current);
        if height != cur.height {
            self.fault = Some(format!("recovery height {height} differs from current state height {}", cur.height));
            return;
        }
        let old = cur.block;
        self.blocks.remove(&old);
        self.revs.get_mut(&self.current).unwrap().block = *hash;
        self.blocks.insert(*hash, self.current);
    }

    pub fn clear_all(&mut self) -> Result<()> {
        self.check()?;
        if self.rev(self.current).hash != EMPTY_ROOT {
            return Err("refusing to discard persisted state during genesis recovery".into());
        }
        let c = self.revs.get_mut(&self.current).unwrap();
        c.block = ZERO;
        c.height = 0;
        self.blocks = HashMap::from([(ZERO, self.current)]);
        Ok(())
    }

    pub fn close(&mut self) -> Result<()> {
        if self.closed {
            return Ok(());
        }
        self.closed = true;
        if let Some(w) = &self.wal {
            w.sync_all()?;
        }
        Ok(())
    }

    // Persistence. Files: LOCK, MANIFEST (gen, seq, height, root, block),
    // state.<gen> (header + arena dump), JOURNAL (accepted records).

    fn state_path(&self, gen: u64) -> PathBuf {
        self.cfg.dir.join(format!("state.{gen}"))
    }

    fn restore(&mut self) -> Result<()> {
        match fs::read(self.cfg.dir.join("MANIFEST")) {
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                let id = self.insert(Rev {
                    root: NONE,
                    hash: EMPTY_ROOT,
                    block: ZERO,
                    height: 0,
                    seq: 0,
                    parent: None,
                    ops: Vec::new(),
                    fresh: Vec::new(),
                    garbage: Vec::new(),
                    accepted: true,
                    invalid: false,
                });
                self.current = id;
                self.history = vec![id];
                self.blocks.insert(ZERO, id);
                self.roots.insert(EMPTY_ROOT, vec![id]);
                self.checkpoint()?;
            }
            Err(e) => return Err(e.into()),
            Ok(m) => {
                if m.len() != 88 {
                    return Err("manifest: bad length".into());
                }
                let u64at = |o: usize| u64::from_le_bytes(m[o..o + 8].try_into().unwrap());
                let (gen, seq, height) = (u64at(0), u64at(8), u64at(16));
                let root: Hash = m[24..56].try_into().unwrap();
                let block: Hash = m[56..88].try_into().unwrap();
                self.gen = gen;
                let root_idx = self.load_state(gen, seq, &root)?;
                let id = self.insert(Rev {
                    root: root_idx,
                    hash: root,
                    block,
                    height,
                    seq,
                    parent: None,
                    ops: Vec::new(),
                    fresh: Vec::new(),
                    garbage: Vec::new(),
                    accepted: true,
                    invalid: false,
                });
                self.current = id;
                self.history = vec![id];
                self.blocks.insert(block, id);
                self.roots.insert(root, vec![id]);
                self.wal = Some(OpenOptions::new().create(true).read(true).write(true).open(self.cfg.dir.join("JOURNAL"))?);
                self.replay_journal(seq)?;
            }
        }
        // Only the manifest's state file is reachable after a restart.
        let keep = format!("state.{}", self.gen);
        for entry in fs::read_dir(&self.cfg.dir)? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().into_owned();
            if name != keep && (name.starts_with("state.") || name.ends_with(".tmp")) {
                fs::remove_file(entry.path())?;
            }
        }
        Ok(())
    }

    const HEADER: usize = 64;
    const MAGIC: &'static [u8; 8] = b"PRUNEDST";

    fn write_state(&self, gen: u64) -> Result<()> {
        let cur = self.rev(self.current);
        let mut f = File::create(self.state_path(gen))?;
        let mut h = [0u8; Self::HEADER];
        h[..8].copy_from_slice(Self::MAGIC);
        h[8..12].copy_from_slice(&(NODE_SIZE as u32).to_le_bytes());
        h[12..16].copy_from_slice(&cur.root.to_le_bytes());
        h[16..24].copy_from_slice(&(self.arena.nodes.len() as u64).to_le_bytes());
        h[24..32].copy_from_slice(&cur.seq.to_le_bytes());
        h[32..64].copy_from_slice(&cur.hash);
        f.write_all(&h)?;
        // SAFETY: Node is repr(C) plain bytes with no padding gaps left uninitialised.
        let bytes = unsafe {
            std::slice::from_raw_parts(self.arena.nodes.as_ptr() as *const u8, self.arena.nodes.len() * NODE_SIZE)
        };
        f.write_all(bytes)?;
        f.sync_all()?;
        Ok(())
    }

    fn load_state(&mut self, gen: u64, seq: u64, root: &Hash) -> Result<u32> {
        let mut f = File::open(self.state_path(gen))?;
        let mut h = [0u8; Self::HEADER];
        f.read_exact(&mut h)?;
        let node_size = u32::from_le_bytes(h[8..12].try_into().unwrap()) as usize;
        let root_idx = u32::from_le_bytes(h[12..16].try_into().unwrap());
        let count = u64::from_le_bytes(h[16..24].try_into().unwrap()) as usize;
        let file_seq = u64::from_le_bytes(h[24..32].try_into().unwrap());
        if &h[..8] != Self::MAGIC || node_size != NODE_SIZE {
            return Err("state file: bad header".into());
        }
        if file_seq != seq || h[32..64] != root[..] {
            return Err("checkpoint files do not match manifest".into());
        }
        let mut nodes: Vec<Node> = Vec::with_capacity(count);
        // SAFETY: every bit pattern is a valid Node (integers and byte arrays only).
        unsafe {
            let buf = std::slice::from_raw_parts_mut(nodes.as_mut_ptr() as *mut u8, count * NODE_SIZE);
            f.read_exact(buf)?;
            nodes.set_len(count);
        }
        if root_idx != NONE && root_idx as usize >= count {
            return Err("state file: root out of range".into());
        }
        self.arena.nodes = nodes;
        self.arena.rebuild_free(root_idx);
        Ok(root_idx)
    }

    fn append_journal(&mut self, id: RevId) -> Result<()> {
        let r = self.rev(id);
        let parent = self.rev(r.parent.unwrap()).hash;
        let mut data = Vec::with_capacity(112 + r.ops.len());
        data.extend_from_slice(&r.seq.to_le_bytes());
        data.extend_from_slice(&r.height.to_le_bytes());
        data.extend_from_slice(&parent);
        data.extend_from_slice(&r.block);
        data.extend_from_slice(&r.hash);
        data.extend_from_slice(&r.ops);
        if data.len() > 256 << 20 {
            return Err("recovery record exceeds 256 MiB".into());
        }
        let mut frame = Vec::with_capacity(8 + data.len());
        frame.extend_from_slice(&(data.len() as u32).to_le_bytes());
        frame.extend_from_slice(&crc32fast::hash(&data).to_le_bytes());
        frame.extend_from_slice(&data);
        let seq = r.seq;
        let wal = self.wal.as_mut().ok_or("journal is not open")?;
        wal.write_all(&frame)?;
        self.wal_bytes += frame.len() as i64;
        if seq % self.cfg.commit_interval == 0 {
            wal.sync_data()?;
        }
        Ok(())
    }

    fn replay_journal(&mut self, checkpoint_seq: u64) -> Result<()> {
        let mut offset: u64 = 0;
        loop {
            let wal = self.wal.as_mut().unwrap();
            let mut header = [0u8; 8];
            match wal.read_exact(&mut header) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            }
            let size = u32::from_le_bytes(header[..4].try_into().unwrap()) as usize;
            if size == 0 || size > 256 << 20 {
                return Err(format!("invalid journal frame length at {offset}").into());
            }
            let mut data = vec![0u8; size];
            match wal.read_exact(&mut data) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            }
            if crc32fast::hash(&data) != u32::from_le_bytes(header[4..].try_into().unwrap()) {
                return Err(format!("journal checksum mismatch at {offset}").into());
            }
            if size < 112 {
                return Err(format!("journal record at {offset} is too short").into());
            }
            let seq = u64::from_le_bytes(data[..8].try_into().unwrap());
            let height = u64::from_le_bytes(data[8..16].try_into().unwrap());
            let parent: Hash = data[16..48].try_into().unwrap();
            let block: Hash = data[48..80].try_into().unwrap();
            let root: Hash = data[80..112].try_into().unwrap();
            if seq > checkpoint_seq {
                let cur = self.rev(self.current);
                if seq != cur.seq + 1 || parent != cur.hash {
                    return Err(format!("nonconsecutive journal record at {offset}").into());
                }
                let mut r = self.compute(self.current, &data[112..]).map_err(|e| match e {
                    Error::Msg(m) => Error::Msg(format!("recovery at {height}: {m}")),
                    e => e,
                })?;
                if r.hash != root {
                    return Err(format!("recovery root mismatch at {height}: got {} want {}", hex(&r.hash), hex(&root)).into());
                }
                r.seq = seq;
                r.height = height;
                r.block = block;
                let id = self.insert(r);
                self.blocks.insert(block, id);
                self.roots.entry(root).or_default().push(id);
                self.accept(id);
            }
            offset += 8 + size as u64;
        }
        // Only an incomplete final frame is discarded; a complete frame with
        // a bad checksum is corruption.
        let wal = self.wal.as_mut().unwrap();
        wal.set_len(offset)?;
        wal.seek(SeekFrom::Start(offset))?;
        self.wal_bytes = offset as i64;
        Ok(())
    }

    fn replace_synced(dir: &Path, name: &str, data: &[u8]) -> Result<()> {
        let tmp = dir.join(format!("{name}.tmp"));
        let mut f = File::create(&tmp)?;
        f.write_all(data)?;
        f.sync_all()?;
        drop(f);
        fs::rename(&tmp, dir.join(name))?;
        File::open(dir)?.sync_all()?;
        Ok(())
    }

    /// Writes the arena as a new state generation, publishes the manifest,
    /// then starts an empty journal.
    pub fn checkpoint(&mut self) -> Result<()> {
        self.check()?;
        let gen = self.gen + 1;
        self.write_state(gen)?;
        if let Some(w) = &self.wal {
            w.sync_all()?;
        }
        let cur = self.rev(self.current);
        let mut m = Vec::with_capacity(88);
        m.extend_from_slice(&gen.to_le_bytes());
        m.extend_from_slice(&cur.seq.to_le_bytes());
        m.extend_from_slice(&cur.height.to_le_bytes());
        m.extend_from_slice(&cur.hash);
        m.extend_from_slice(&cur.block);
        Self::replace_synced(&self.cfg.dir, "MANIFEST", &m)?;
        let old = self.gen;
        self.gen = gen;
        if old != 0 {
            fs::remove_file(self.state_path(old))?;
        }
        // Publication precedes rotation: recovery skips records the manifest covers.
        Self::replace_synced(&self.cfg.dir, "JOURNAL", &[])?;
        self.wal = Some(OpenOptions::new().read(true).write(true).open(self.cfg.dir.join("JOURNAL"))?);
        self.wal_bytes = 0;
        Ok(())
    }

    #[cfg(test)]
    pub fn generation(&self) -> u64 {
        self.gen
    }
    #[cfg(test)]
    pub fn journal_bytes(&self) -> i64 {
        self.wal_bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn h(n: u64) -> Hash {
        let mut k = [0u8; 32];
        k[24..].copy_from_slice(&n.to_be_bytes());
        k
    }
    fn account(n: u64, balance: u64) -> Vec<u8> {
        let mut op = vec![OP_ACCOUNT];
        op.extend_from_slice(&h(n));
        op.extend_from_slice(&[0u8; 8]);
        let mut bal = [0u8; 32];
        bal[24..].copy_from_slice(&balance.to_be_bytes());
        op.extend_from_slice(&bal);
        op.extend_from_slice(&keccak(&[]));
        op
    }
    fn slot(owner: u64, slot: u64, v: u8) -> Vec<u8> {
        let mut op = vec![OP_SLOT];
        op.extend_from_slice(&h(owner));
        op.extend_from_slice(&h(slot));
        op.push(1);
        op.push(v);
        op
    }
    fn cfg(dir: &Path) -> Config {
        Config { dir: dir.to_path_buf(), retain: 3, commit_interval: 0, journal_limit: 0 }
    }
    fn block(s: &mut Store, parent_block: u64, block: u64, height: u64, ops: &[Vec<u8>]) -> Hash {
        let parent = s.head_of(parent_block);
        let root = s.propose(&parent, &ops.concat()).unwrap();
        s.update(&root, &parent, height, &h(parent_block), &h(block)).unwrap();
        root
    }
    impl Store {
        fn head_of(&self, block: u64) -> Hash {
            self.rev(self.blocks[&h(block)]).hash
        }
    }

    #[test]
    fn branches_retention_and_restart() {
        let dir = std::env::temp_dir().join(format!("prunedffi-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        let mut s = Store::open(cfg(&dir)).unwrap();
        // h(0) is the zero block hash of the empty genesis revision.
        let g = block(&mut s, 0, 100, 0, &[account(1, 10)]);
        s.commit(&g).unwrap();
        let left = block(&mut s, 100, 101, 1, &[account(1, 20)]);
        let right = block(&mut s, 100, 102, 1, &[account(1, 30)]);
        let child = block(&mut s, 101, 103, 2, &[slot(1, 1, 9)]);
        assert_eq!(s.get_account(&right, &h(1)).unwrap().unwrap()[39], 30);
        s.commit(&left).unwrap();
        assert!(matches!(s.get_account(&right, &h(1)), Err(Error::Pruned)));
        assert_eq!(s.get_account(&child, &h(1)).unwrap().unwrap()[39], 20);
        assert_eq!(s.get_storage(&child, &h(1), &h(1)).unwrap().unwrap().0[0], 9);
        s.commit(&child).unwrap();
        let empty = block(&mut s, 103, 104, 3, &[]);
        assert_eq!(empty, child);
        s.commit(&empty).unwrap();
        assert!(matches!(s.has_root(&g), Err(Error::Pruned)));
        assert_eq!(s.get_account(&g, &h(1)).err().map(|e| matches!(e, Error::Pruned)), Some(true));
        // Everything freed by retirement is reusable; nothing double-released.
        let free = s.arena.free.len();
        assert!(free > 0);
        let (root, blk, height) = s.head();
        s.close().unwrap();
        drop(s);
        let s = Store::open(cfg(&dir)).unwrap();
        assert_eq!(s.head(), (root, blk, height));
        assert_eq!(s.get_storage(&root, &h(1), &h(1)).unwrap().unwrap().0[0], 9);
        assert_eq!(s.get_account(&root, &h(1)).unwrap().unwrap()[39], 20);
        drop(s);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn checkpoint_rotates_and_recovers() {
        let dir = std::env::temp_dir().join(format!("prunedffi-ckpt-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        let mut c = cfg(&dir);
        c.retain = 4;
        c.journal_limit = 700;
        let mut s = Store::open(c).unwrap();
        let mut prev = 0;
        for n in 0..35u64 {
            let root = block(&mut s, prev, 100 + n, n, &[account(1, n + 1), slot(1, n + 1, n as u8 + 1)]);
            s.commit(&root).unwrap();
            prev = 100 + n;
        }
        assert!(s.generation() > 2);
        assert!(s.journal_bytes() < 700);
        let head = s.head();
        s.close().unwrap();
        drop(s);
        let mut c = cfg(&dir);
        c.retain = 4;
        c.journal_limit = 700;
        let mut s = Store::open(c).unwrap();
        assert_eq!(s.head(), head);
        for n in 0..35u64 {
            assert_eq!(s.get_storage(&head.0, &h(1), &h(n + 1)).unwrap().unwrap().0[0], n as u8 + 1);
        }
        let files = fs::read_dir(&dir).unwrap().filter(|e| e.as_ref().unwrap().file_name().to_string_lossy().starts_with("state.")).count();
        assert_eq!(files, 1);
        let root = block(&mut s, prev, 200, 35, &[account(1, 100)]);
        s.commit(&root).unwrap();
        drop(s);
        let _ = fs::remove_dir_all(&dir);
    }
}

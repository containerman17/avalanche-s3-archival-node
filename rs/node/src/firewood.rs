//! The Firewood state engine behind the executor: ava-labs Firewood in
//! ethhash mode holds the whole state as ONE trie (account = keccak(addr),
//! 32 B, value RLP[nonce, balance, storageRoot, codeHash]; slot =
//! keccak(addr) ++ keccak(slot), 64 B, value RLP(trimmed word); an account
//! delete is a prefix delete of the 32 B key), and its `propose` IS the
//! state root: hashing happens when the block's batch is proposed, the
//! revision lands on `commit`.
//!
//! Shape: the executor writes a block into `cur` (a map in OUR contract key
//! form, so `take_ws` hands out the same ordered write set as `Backend`
//! does), reads go cur -> the pending chain (verified, not accepted: the
//! plugin's siblings) -> the accepted layers (accepted, not yet committed by
//! the committer thread) -> Firewood's latest committed revision. The
//! committer turns a layer into Firewood ops, proposes, compares the
//! proposal's root with the header, commits, and publishes the new revision
//! view; the executor drops the layer once the revision covers it.
//!
//! Recovery: Firewood keeps its own revisions; the height of the persisted
//! root is found by walking the store's headers back from the head (the
//! persist lag is at most `deferred_persistence_commit_count` commits).

use alloy_primitives::map::{AddressMap, B256Map, HashMap, HashSet};
use alloy_primitives::{keccak256, Address, Bytes, B256, U256};
use anyhow::{anyhow, Context, Result};
use exec::exec::{account_rlp, trimmed};
use exec::StateDb;
use firewood::db::{Db, DbConfig, Proposal};
use firewood::api::{ArcDynDbView, BatchOp, Db as _, DbView as _, HashKey, Proposal as _};
use firewood::manager::RevisionManagerConfig;
use firewood_storage::NodeHashAlgorithm;
use revm::state::{AccountInfo, Bytecode, EvmState};
use revm::{Database, DatabaseCommit};
use std::collections::VecDeque;
use std::convert::Infallible;
use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::{Arc, Mutex};

const HASH_CACHE_MAX: usize = 1 << 16;
const BLOCK_HASH_WINDOW: u64 = 256;
/// keccak(rlp("")) : the empty trie root, Firewood's root of an empty db.
pub const EMPTY_ROOT: B256 = alloy_primitives::b256!("56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421");

/// One block's writes in the contract key form (33 B account, 65 B slot;
/// an empty value is a delete) plus the accounts whose storage was wiped
/// (self-destruct / EIP-158 empty): a slot read under one of those stops
/// here unless the layer itself rewrote the slot.
#[derive(Default)]
pub struct Layer {
    pub map: HashMap<Vec<u8>, Vec<u8>>,
    pub wiped: HashSet<B256>,
    pub code: Vec<(B256, Bytecode)>,
}

impl Layer {
    /// Some(None) = deleted here, Some(Some(v)) = written here, None = ask below.
    fn get(&self, key: &[u8]) -> Option<Option<&[u8]>> {
        if let Some(v) = self.map.get(key) {
            return Some(if v.is_empty() { None } else { Some(v) });
        }
        if key.len() == 65 && self.wiped.contains(&B256::from_slice(&key[..32])) {
            return Some(None);
        }
        None
    }

    /// The Firewood batch: every wipe first (a recreate's puts follow), then
    /// the block's final value per key.
    pub fn ops(&self) -> Vec<BatchOp<Vec<u8>, Vec<u8>>> {
        let mut ops = Vec::with_capacity(self.map.len() + self.wiped.len());
        for ah in &self.wiped {
            ops.push(BatchOp::DeleteRange { prefix: ah.to_vec() });
        }
        for (k, v) in &self.map {
            if k.len() == 33 {
                if !v.is_empty() {
                    ops.push(BatchOp::Put { key: k[..32].to_vec(), value: account4(v) });
                }
                // An account delete is its DeleteRange above.
            } else {
                let mut key = Vec::with_capacity(64);
                key.extend_from_slice(&k[..32]);
                key.extend_from_slice(&k[33..]);
                if v.is_empty() {
                    ops.push(BatchOp::Delete { key });
                } else {
                    ops.push(BatchOp::Put { key, value: alloy_rlp::encode(v.as_slice()) });
                }
            }
        }
        ops
    }
}

/// Our RLP[nonce, balance, codeHash] -> Firewood's RLP[nonce, balance,
/// storageRoot, codeHash]; the storage root field is spliced by Firewood at
/// hash time, so the empty root is written.
fn account4(v3: &[u8]) -> Vec<u8> {
    use alloy_rlp::{Decodable, Encodable};
    let mut b = v3;
    let _ = alloy_rlp::Header::decode(&mut b).expect("account rlp");
    let nonce = u64::decode(&mut b).expect("nonce");
    let balance = U256::decode(&mut b).expect("balance");
    let code_hash = B256::decode(&mut b).expect("code hash");
    let mut out = Vec::with_capacity(110);
    let payload_length = nonce.length() + balance.length() + 33 + 33;
    alloy_rlp::Header { list: true, payload_length }.encode(&mut out);
    nonce.encode(&mut out);
    balance.encode(&mut out);
    EMPTY_ROOT.encode(&mut out);
    code_hash.encode(&mut out);
    out
}

/// Either RLP form (ours 3 fields, Firewood's 4) -> AccountInfo.
fn decode_account(mut v: &[u8]) -> AccountInfo {
    use alloy_rlp::Decodable;
    let h = alloy_rlp::Header::decode(&mut v).expect("account rlp");
    assert!(h.list, "account row is not a list");
    let nonce = u64::decode(&mut v).expect("account nonce");
    let balance = U256::decode(&mut v).expect("account balance");
    let mut code_hash = B256::decode(&mut v).expect("account code hash");
    if !v.is_empty() {
        code_hash = B256::decode(&mut v).expect("account code hash");
    }
    AccountInfo { balance, nonce, code_hash, code: None, ..Default::default() }
}

fn acct_key(ah: &B256) -> [u8; 33] {
    let mut k = [0u8; 33];
    k[..32].copy_from_slice(ah.as_slice());
    k
}

fn slot_key(ah: &B256, sh: &B256) -> [u8; 65] {
    let mut k = [0u8; 65];
    k[..32].copy_from_slice(ah.as_slice());
    k[32] = 1;
    k[33..].copy_from_slice(sh.as_slice());
    k
}

/// What the committer publishes: the latest committed revision and its height.
pub struct Committed {
    pub view: Option<ArcDynDbView>,
    pub height: u64,
}

/// A verified block's layer on top of its parent's (the plugin's pending chain).
pub struct Pending {
    pub number: u64,
    pub hash: B256,
    pub time: u64,
    pub parent: Option<Arc<Pending>>,
    pub layer: Arc<Layer>,
}

impl Pending {
    fn get(&self, key: &[u8]) -> Option<Option<&[u8]>> {
        self.layer.get(key).or_else(|| self.parent.as_ref().and_then(|p| p.get(key)))
    }
    fn code(&self, h: &B256) -> Option<Bytecode> {
        if let Some((_, c)) = self.layer.code.iter().find(|(x, _)| x == h) {
            return Some(c.clone());
        }
        self.parent.as_ref().and_then(|p| p.code(h))
    }
    fn block_hash(&self, n: u64) -> Option<B256> {
        if self.number == n {
            return Some(self.hash);
        }
        self.parent.as_ref().and_then(|p| p.block_hash(n))
    }
}

/// The executor's Firewood-backed `StateDb`.
pub struct Firewood {
    cur: Layer,
    parent: Option<Arc<Pending>>,
    /// Accepted blocks the committer has not committed yet, oldest first.
    accepted: VecDeque<(u64, Arc<Layer>)>,
    committed: Arc<Mutex<Committed>>,
    view: Option<ArcDynDbView>,
    pub code: B256Map<Bytecode>,
    addr_hash: AddressMap<B256>,
    slot_hash: B256Map<B256>,
    pub block_hashes: HashMap<u64, B256>,
    ws: Vec<(Vec<u8>, Vec<u8>)>,
    new_code: Vec<(B256, Bytes)>,
    /// Trie reads answered by Firewood (misses in every layer), for the split.
    pub trie_reads: u64,
}

impl Firewood {
    pub fn new(committed: Arc<Mutex<Committed>>) -> Firewood {
        let view = committed.lock().unwrap().view.clone();
        Firewood {
            cur: Layer::default(),
            parent: None,
            accepted: VecDeque::new(),
            committed,
            view,
            code: B256Map::default(),
            addr_hash: AddressMap::default(),
            slot_hash: B256Map::default(),
            block_hashes: HashMap::default(),
            ws: Vec::new(),
            new_code: Vec::new(),
            trie_reads: 0,
        }
    }

    /// Start a block on top of `parent` (None: the accepted head); picks up
    /// the committer's progress and drops the layers it covers.
    pub fn begin(&mut self, parent: Option<Arc<Pending>>) {
        self.parent = parent;
        self.cur = Layer::default();
        let c = self.committed.lock().unwrap();
        while self.accepted.front().is_some_and(|(h, _)| *h <= c.height) {
            self.accepted.pop_front();
        }
        self.view = c.view.clone();
    }

    /// Close the block: its Pending (the plugin keeps it until accept).
    pub fn finish(&mut self, number: u64, hash: B256, time: u64) -> Pending {
        let layer = Arc::new(std::mem::take(&mut self.cur));
        Pending { number, hash, time, parent: self.parent.take(), layer }
    }

    /// The block's layer becomes accepted state (readable until the
    /// committer's revision covers it); the caller hands the same layer to
    /// the committer.
    pub fn accept(&mut self, height: u64, layer: Arc<Layer>) {
        for (h, c) in &layer.code {
            self.code.insert(*h, c.clone());
        }
        self.accepted.push_back((height, layer));
    }

    pub fn take_ws(&mut self) -> (Vec<(Vec<u8>, Vec<u8>)>, Vec<(B256, Bytes)>) {
        (std::mem::take(&mut self.ws), std::mem::take(&mut self.new_code))
    }

    pub fn accepted_bytes(&self) -> usize {
        self.accepted.iter().map(|(_, l)| l.map.iter().map(|(k, v)| k.len() + v.len() + 48).sum::<usize>()).sum()
    }

    pub fn addr_hash(&mut self, a: Address) -> B256 {
        if let Some(h) = self.addr_hash.get(&a) {
            return *h;
        }
        if self.addr_hash.len() >= HASH_CACHE_MAX {
            self.addr_hash.clear();
        }
        let h = keccak256(a.as_slice());
        self.addr_hash.insert(a, h);
        h
    }

    pub fn slot_hash(&mut self, s: B256) -> B256 {
        if let Some(h) = self.slot_hash.get(&s) {
            return *h;
        }
        if self.slot_hash.len() >= HASH_CACHE_MAX {
            self.slot_hash.clear();
        }
        let h = keccak256(s.as_slice());
        self.slot_hash.insert(s, h);
        h
    }

    /// The live value in the contract key form: cur, the pending chain, the
    /// accepted layers (newest first), then Firewood's committed revision.
    pub fn get(&mut self, key: &[u8]) -> Option<Vec<u8>> {
        if let Some(v) = self.cur.get(key) {
            return v.map(<[u8]>::to_vec);
        }
        if let Some(v) = self.parent.as_ref().and_then(|p| p.get(key)) {
            return v.map(<[u8]>::to_vec);
        }
        for (_, l) in self.accepted.iter().rev() {
            if let Some(v) = l.get(key) {
                return v.map(<[u8]>::to_vec);
            }
        }
        self.trie_reads += 1;
        let view = self.view.as_ref()?;
        let mut k = [0u8; 64];
        k[..32].copy_from_slice(&key[..32]);
        let fk: &[u8] = if key.len() == 33 {
            &k[..32]
        } else {
            k[32..].copy_from_slice(&key[33..]);
            &k[..]
        };
        let v = view.val(fk).expect("firewood read")?;
        if key.len() == 33 {
            Some(v.to_vec())
        } else {
            // RLP(trimmed word) -> trimmed word.
            let mut b: &[u8] = &v;
            let h = alloy_rlp::Header::decode(&mut b).expect("slot rlp");
            Some(b[..h.payload_length].to_vec())
        }
    }

    fn put(&mut self, key: &[u8], val: &[u8]) {
        self.cur.map.insert(key.to_vec(), val.to_vec());
        self.ws.push((key.to_vec(), val.to_vec()));
    }

    fn has_code(&self, h: &B256) -> bool {
        self.cur.code.iter().any(|(x, _)| x == h) || self.parent.as_ref().is_some_and(|p| p.code(h).is_some()) || self.accepted.iter().any(|(_, l)| l.code.iter().any(|(x, _)| x == h)) || self.code.contains_key(h)
    }

    fn commit_rows(&mut self, changes: EvmState) {
        for (addr, a) in changes {
            if !a.is_touched() {
                continue;
            }
            let ah = self.addr_hash(addr);
            if a.is_selfdestructed() || a.is_empty() {
                self.put(&acct_key(&ah), &[]);
                self.cur.map.retain(|k, _| !(k.len() == 65 && k[..32] == ah[..]));
                self.cur.wiped.insert(ah);
                continue;
            }
            for (k, slot) in &a.storage {
                if slot.is_changed() {
                    let sh = self.slot_hash(B256::from(*k));
                    let v = trimmed(slot.present_value());
                    self.put(&slot_key(&ah, &sh), &v);
                }
            }
            self.put(&acct_key(&ah), &account_rlp(&a.info));
            if let Some(code) = a.info.code {
                if !a.info.code_hash.is_zero() && a.info.code_hash != alloy_primitives::KECCAK256_EMPTY && !self.has_code(&a.info.code_hash) {
                    self.new_code.push((a.info.code_hash, code.original_bytes()));
                    self.cur.code.push((a.info.code_hash, code));
                }
            }
        }
    }
}

impl Database for Firewood {
    type Error = Infallible;

    fn basic(&mut self, address: Address) -> Result<Option<AccountInfo>, Infallible> {
        let ah = self.addr_hash(address);
        Ok(self.get(&acct_key(&ah)).map(|v| decode_account(&v)))
    }

    fn code_by_hash(&mut self, code_hash: B256) -> Result<Bytecode, Infallible> {
        if let Some((_, c)) = self.cur.code.iter().find(|(x, _)| *x == code_hash) {
            return Ok(c.clone());
        }
        if let Some(c) = self.parent.as_ref().and_then(|p| p.code(&code_hash)) {
            return Ok(c);
        }
        for (_, l) in self.accepted.iter().rev() {
            if let Some((_, c)) = l.code.iter().find(|(x, _)| *x == code_hash) {
                return Ok(c.clone());
            }
        }
        Ok(self.code.get(&code_hash).cloned().unwrap_or_default())
    }

    fn storage(&mut self, address: Address, index: U256) -> Result<U256, Infallible> {
        let ah = self.addr_hash(address);
        let sh = self.slot_hash(B256::from(index));
        Ok(self.get(&slot_key(&ah, &sh)).map(|v| U256::from_be_slice(&v)).unwrap_or(U256::ZERO))
    }

    fn block_hash(&mut self, number: u64) -> Result<B256, Infallible> {
        if let Some(h) = self.parent.as_ref().and_then(|p| p.block_hash(number)) {
            return Ok(h);
        }
        Ok(self.block_hashes.get(&number).copied().unwrap_or_default())
    }
}

/// Backend::commit's rows into cur; sorted per commit like the plugin's
/// Layered (the write set is stored, so its bytes must not depend on
/// EvmState's iteration order).
impl DatabaseCommit for Firewood {
    fn commit(&mut self, changes: EvmState) {
        let start = self.ws.len();
        self.commit_rows(changes);
        self.ws[start..].sort_by(|a, b| a.0.cmp(&b.0));
    }
}

impl StateDb for Firewood {
    fn set_block_hash(&mut self, number: u64, hash: B256) {
        self.block_hashes.insert(number, hash);
        if number > BLOCK_HASH_WINDOW {
            self.block_hashes.remove(&(number - BLOCK_HASH_WINDOW - 1));
        }
    }
}

// ---------------------------------------------------------------------------
// The committer: Firewood's Db, proposals and commits, off the executor.

pub struct Committer {
    /// Leaked: `Proposal<'db>` borrows the Db, and one Db lives as long as
    /// the process; `close` reclaims it.
    db: &'static Db,
    committed: Arc<Mutex<Committed>>,
    /// Proposed, not committed, oldest first (a proposal chain: block N+1 is
    /// proposed on top of N's proposal).
    pending: Vec<(u64, Proposal<'static>)>,
}

// SAFETY: Db is Send + Sync (Arc'd node stores, parking_lot locks); Proposal
// holds an Arc<NodeStore> and &Db. Committer moves to the checker thread once.
unsafe impl Send for Committer {}

/// Firewood's revision manager settings we expose (the rest are its defaults).
#[derive(Clone, Copy, Debug)]
pub struct Opts {
    /// Node cache bytes (Firewood's default 192 MB).
    pub cache_bytes: usize,
    /// Committed revisions kept in memory (default 128).
    pub revisions: usize,
}

impl Default for Opts {
    fn default() -> Opts {
        Opts { cache_bytes: 192_000_000, revisions: 128 }
    }
}

impl Committer {
    /// Opens (or creates) the Firewood db under `dir/firewood`.
    pub fn open(dir: &Path, truncate: bool, opts: Opts) -> Result<Committer> {
        std::fs::create_dir_all(dir)?;
        let manager = RevisionManagerConfig::builder()
            .max_revisions(opts.revisions)
            .node_cache_memory_limit(NonZeroUsize::new(opts.cache_bytes).ok_or_else(|| anyhow!("cache 0"))?)
            .build();
        let cfg = DbConfig::builder().node_hash_algorithm(NodeHashAlgorithm::Ethereum).truncate(truncate).manager(manager).build();
        let db: &'static Db = Box::leak(Box::new(Db::new(dir.join("firewood"), cfg).map_err(|e| anyhow!("firewood open: {e}"))?));
        let root = db.root_hash();
        let view = match root {
            Some(r) if r.as_ref() != EMPTY_ROOT.as_slice() => Some(db.view(r).map_err(|e| anyhow!("firewood view: {e}"))?),
            _ => None,
        };
        let committed = Arc::new(Mutex::new(Committed { view, height: 0 }));
        Ok(Committer { db, committed, pending: Vec::new() })
    }

    pub fn committed(&self) -> Arc<Mutex<Committed>> {
        self.committed.clone()
    }

    /// The latest committed revision's root (the empty root for a new db).
    pub fn root(&self) -> B256 {
        self.db.root_hash().map(|h| B256::from_slice(h.as_ref())).unwrap_or(EMPTY_ROOT)
    }

    /// Proposes the block's ops on top of the newest pending proposal (or
    /// the committed revision): Firewood hashes here. Returns the root.
    pub fn propose(&mut self, height: u64, ops: Vec<BatchOp<Vec<u8>, Vec<u8>>>) -> Result<B256> {
        let p = match self.pending.last() {
            Some((_, parent)) => parent.propose(ops),
            None => self.db.propose(ops),
        }
        .map_err(|e| anyhow!("firewood propose {height}: {e}"))?;
        let root = p.root_hash().map(|h| B256::from_slice(h.as_ref())).unwrap_or(EMPTY_ROOT);
        self.pending.push((height, p));
        Ok(root)
    }

    /// Commits the oldest pending proposal and publishes its revision.
    pub fn commit(&mut self) -> Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let (h, p) = self.pending.remove(0);
        let root: Option<HashKey> = p.root_hash();
        p.commit().map_err(|e| anyhow!("firewood commit {h}: {e}"))?;
        let view = match root {
            Some(r) if r.as_ref() != EMPTY_ROOT.as_slice() => Some(self.db.view(r).map_err(|e| anyhow!("firewood view {h}: {e}"))?),
            _ => None,
        };
        let mut c = self.committed.lock().unwrap();
        c.view = view;
        c.height = h;
        Ok(())
    }

    /// Drops every pending proposal (a rejected chain).
    pub fn drop_pending(&mut self) {
        self.pending.clear();
    }

    pub fn pending_len(&self) -> usize {
        self.pending.len()
    }

    /// Closes the db: pending proposals go, the latest revision is persisted.
    pub fn close(mut self) -> Result<()> {
        self.pending.clear();
        let db = unsafe { Box::from_raw(self.db as *const Db as *mut Db) };
        db.close().map_err(|e| anyhow!("firewood close: {e}"))
    }

    /// The on-disk bytes under the db dir.
    pub fn disk_bytes(dir: &Path) -> u64 {
        std::fs::read_dir(dir.join("firewood")).map(|d| d.flatten().filter_map(|e| e.metadata().ok()).map(|m| m.len()).sum()).unwrap_or(0)
    }
}

/// Finds the height whose header root is `root`, walking back from `head`
/// through `header_root(h)`; the persist lag bounds the walk.
pub fn height_of_root(root: B256, head: u64, mut header_root: impl FnMut(u64) -> Result<B256>) -> Result<u64> {
    for h in (0..=head).rev() {
        if header_root(h)? == root {
            return Ok(h);
        }
        if head - h > 100_000 {
            break;
        }
    }
    Err(anyhow!("no header within 100,000 blocks of {head} has state root {root}")).context("firewood recovery")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn account4_roundtrip() {
        let info = AccountInfo { nonce: 7, balance: U256::from(1_000_000u64), code_hash: alloy_primitives::KECCAK256_EMPTY, ..Default::default() };
        let v4 = account4(&account_rlp(&info));
        let back = decode_account(&v4);
        assert_eq!((back.nonce, back.balance, back.code_hash), (7, U256::from(1_000_000u64), alloy_primitives::KECCAK256_EMPTY));
        assert_eq!(v4.len(), 2 + 1 + 4 + 33 + 33);
    }

    /// Two accounts, one with two slots, proposed into a fresh db: the root is
    /// alloy-trie's secure-trie root of the same state (the ethhash contract:
    /// keccak keys, RLP(trimmed) slots, Firewood splices the storage root).
    #[test]
    fn root_matches_alloy_trie() {
        use alloy_trie::{root::{state_root_unhashed, storage_root_unhashed}, TrieAccount};
        let dir = std::env::temp_dir().join(format!("epochdb-fw-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let mut c = Committer::open(&dir, true, Opts::default()).unwrap();
        let a = Address::repeat_byte(0x11);
        let b = Address::repeat_byte(0x22);
        let ia = AccountInfo { nonce: 1, balance: U256::from(5u64), code_hash: alloy_primitives::KECCAK256_EMPTY, ..Default::default() };
        let ib = AccountInfo { nonce: 2, balance: U256::from(0x0100u64), code_hash: B256::repeat_byte(0xcc), ..Default::default() };
        let slots = [(B256::from(U256::from(1u64)), U256::from(0x0100u64)), (B256::from(U256::from(9u64)), U256::from(1u64))];
        let mut l = Layer::default();
        l.map.insert(acct_key(&keccak256(a)).to_vec(), account_rlp(&ia));
        l.map.insert(acct_key(&keccak256(b)).to_vec(), account_rlp(&ib));
        for (k, v) in slots {
            l.map.insert(slot_key(&keccak256(b), &keccak256(k)).to_vec(), trimmed(v));
        }
        let root = c.propose(1, l.ops()).unwrap();
        let sr = storage_root_unhashed(slots.iter().copied());
        let want = state_root_unhashed([
            (a, TrieAccount { nonce: 1, balance: U256::from(5u64), storage_root: alloy_trie::EMPTY_ROOT_HASH, code_hash: ia.code_hash }),
            (b, TrieAccount { nonce: 2, balance: U256::from(0x0100u64), storage_root: sr, code_hash: ib.code_hash }),
        ]);
        assert_eq!(root, want);
        c.commit().unwrap();
        let view = c.committed().lock().unwrap().view.clone().unwrap();
        let v = view.val(keccak256(b).as_slice()).unwrap().unwrap();
        assert_eq!(decode_account(&v).nonce, 2);
        // Delete b: prefix delete takes the slots with it.
        let mut l2 = Layer::default();
        l2.wiped.insert(keccak256(b));
        l2.map.insert(acct_key(&keccak256(b)).to_vec(), Vec::new());
        let root2 = c.propose(2, l2.ops()).unwrap();
        assert_eq!(root2, state_root_unhashed([(a, TrieAccount { nonce: 1, balance: U256::from(5u64), storage_root: alloy_trie::EMPTY_ROOT_HASH, code_hash: ia.code_hash })]));
        c.commit().unwrap();
        c.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }
}

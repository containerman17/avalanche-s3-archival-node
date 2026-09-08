//! The flat state behind the executor (vmexec/engine.go + flatdb.go): reads
//! come from the fresh overlay, then the frozen overlay being merged in the
//! background, then the rolled run; a tx's commit writes post-image rows
//! into the fresh overlay and into the open block's ordered write set, which
//! the checker thread feeds to `Dirty` for the block's root.
//!
//! Contract keys: account = keccak(addr)+0x00 -> RLP[nonce, balance,
//! codeHash]; slot = keccak(addr)+0x01+keccak(slot) -> left-trimmed word;
//! empty value = delete. Code lives in an in-memory table by code hash
//! (seeded from genesis, fed by every deployment; the history file is its
//! only persistence, recovery is out of scope).

use alloy_primitives::map::{AddressMap, B256Map, HashMap, HashSet};
use alloy_primitives::{keccak256, Address, Bytes, B256, U256};
use exec::exec::{account_rlp, trimmed};
use exec::StateDb;
use revm::state::{AccountInfo, Bytecode, EvmState};
use revm::{Database, DatabaseCommit};
use state::commit::dirty::SeekFn;
use state::commit::file::File;
use state::commit::roll::{roll, Stats as RollStats};
use state::overlay::Overlay;
use state::run::Run;
use state::view::{merge, View};
use state::{Hash, KvIter};
use std::convert::Infallible;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{sync_channel, Receiver};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// hashCache in flatdb.go: memoised keccak(addr) and keccak(slot), cleared
/// once they reach this many entries.
const HASH_CACHE_MAX: usize = 1 << 16;
/// BLOCKHASH reaches back 256 blocks.
const BLOCK_HASH_WINDOW: u64 = 256;

pub struct Backend {
    pub overlay: Overlay,
    pub frozen: Option<Arc<Overlay>>,
    pub run: Option<Arc<Run>>,
    /// Accounts with a slot written into the fresh / frozen overlay: an
    /// account delete scans the overlays for slots to tombstone only when one
    /// of these or the run says it has any (Overlay::iter is a full sort).
    owners: HashSet<B256>,
    frozen_owners: HashSet<B256>,
    code: B256Map<Bytecode>,
    addr_hash: AddressMap<B256>,
    slot_hash: B256Map<B256>,
    block_hashes: HashMap<u64, B256>,
    /// The open block's writes in order (a delete before a recreate matters).
    pub ws: Vec<(Vec<u8>, Vec<u8>)>,
    /// Code deployed in the open block, for the history file.
    pub new_code: Vec<(B256, Bytes)>,
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

impl Backend {
    pub fn new() -> Backend {
        Backend {
            overlay: Overlay::new(),
            frozen: None,
            run: None,
            owners: HashSet::default(),
            frozen_owners: HashSet::default(),
            code: B256Map::default(),
            addr_hash: AddressMap::default(),
            slot_hash: B256Map::default(),
            block_hashes: HashMap::default(),
            ws: Vec::new(),
            new_code: Vec::new(),
        }
    }

    fn addr_hash(&mut self, a: Address) -> B256 {
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

    fn slot_hash(&mut self, s: B256) -> B256 {
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

    /// The live value: fresh overlay, frozen overlay, run. An empty value
    /// at any level is a delete.
    pub fn get(&self, key: &[u8]) -> Option<&[u8]> {
        if let Some(v) = self.overlay.get(key) {
            return if v.is_empty() { None } else { Some(v) };
        }
        if let Some(f) = &self.frozen {
            if let Some(v) = f.get(key) {
                return if v.is_empty() { None } else { Some(v) };
            }
        }
        let v = self.run.as_ref()?.get(key)?;
        if v.is_empty() {
            None
        } else {
            Some(v)
        }
    }

    fn put(&mut self, key: &[u8], val: &[u8]) {
        self.overlay.put(key, val);
        self.ws.push((key.to_vec(), val.to_vec()));
    }

    /// engine.tombstoneSlots: an account delete tombstones every live slot
    /// under it in the fresh overlay (Dirty wipes the storage itself).
    fn tombstone_slots(&mut self, ah: &B256) {
        let mut lo = [0u8; 33];
        lo[..32].copy_from_slice(ah.as_slice());
        let mut hi = lo;
        lo[32] = 1;
        hi[32] = 2;
        let in_run = |run: &Option<Arc<Run>>| run.as_ref().is_some_and(|r| r.iter(Some(&lo[..]), Some(&hi[..])).next());
        if !self.owners.contains(ah) && !self.frozen_owners.contains(ah) && !in_run(&self.run) {
            return;
        }
        let keys: Vec<Vec<u8>> = {
            let mut ovs: Vec<&Overlay> = vec![&self.overlay];
            if let Some(f) = &self.frozen {
                ovs.push(f);
            }
            let runs: Vec<&Run> = self.run.iter().map(|r| &**r).collect();
            let v = View::multi(&ovs, &runs);
            let mut it = v.iter(Some(&lo[..]), Some(&hi[..]));
            let mut keys = Vec::new();
            while it.next() {
                keys.push(it.key().to_vec());
            }
            keys
        };
        for k in keys {
            self.overlay.put(&k, &[]);
        }
    }

    pub fn take_ws(&mut self) -> (Vec<(Vec<u8>, Vec<u8>)>, Vec<(B256, Bytes)>) {
        (std::mem::take(&mut self.ws), std::mem::take(&mut self.new_code))
    }

    /// maybeRoll's freeze: the fresh overlay becomes the frozen one (shared
    /// with the roll thread) and a new fresh overlay starts.
    pub fn freeze(&mut self) -> Arc<Overlay> {
        let f = Arc::new(std::mem::take(&mut self.overlay));
        self.frozen = Some(f.clone());
        self.frozen_owners = std::mem::take(&mut self.owners);
        f
    }

    /// finishRoll's swap: the merged run replaces the frozen overlay and the
    /// old run.
    pub fn swap(&mut self, run: Arc<Run>) {
        self.run = Some(run);
        self.frozen = None;
        self.frozen_owners.clear();
    }

}

impl Default for Backend {
    fn default() -> Self {
        Self::new()
    }
}

fn decode_account(mut v: &[u8]) -> AccountInfo {
    use alloy_rlp::Decodable;
    let h = alloy_rlp::Header::decode(&mut v).expect("account rlp");
    assert!(h.list, "account row is not a list");
    let nonce = u64::decode(&mut v).expect("account nonce");
    let balance = U256::decode(&mut v).expect("account balance");
    let code_hash = B256::decode(&mut v).expect("account code hash");
    AccountInfo { balance, nonce, code_hash, code: None, ..Default::default() }
}

impl Database for Backend {
    type Error = Infallible;

    fn basic(&mut self, address: Address) -> Result<Option<AccountInfo>, Infallible> {
        let ah = self.addr_hash(address);
        Ok(self.get(&acct_key(&ah)).map(decode_account))
    }

    fn code_by_hash(&mut self, code_hash: B256) -> Result<Bytecode, Infallible> {
        Ok(self.code.get(&code_hash).cloned().unwrap_or_default())
    }

    fn storage(&mut self, address: Address, index: U256) -> Result<U256, Infallible> {
        let ah = self.addr_hash(address);
        let sh = self.slot_hash(B256::from(index));
        Ok(self.get(&slot_key(&ah, &sh)).map(U256::from_be_slice).unwrap_or(U256::ZERO))
    }

    fn block_hash(&mut self, number: u64) -> Result<B256, Infallible> {
        Ok(self.block_hashes.get(&number).copied().unwrap_or_default())
    }
}

/// The post-image rows a tx leaves (vmexec/capture.go at the tx's
/// IntermediateRoot): every touched account (deleted when self-destructed or
/// EIP-158 empty), every changed slot, the code an account was given.
impl DatabaseCommit for Backend {
    fn commit(&mut self, changes: EvmState) {
        for (addr, a) in changes {
            if !a.is_touched() {
                continue;
            }
            let ah = self.addr_hash(addr);
            if a.is_selfdestructed() || a.is_empty() {
                self.put(&acct_key(&ah), &[]);
                self.tombstone_slots(&ah);
                continue;
            }
            for (k, slot) in &a.storage {
                if slot.is_changed() {
                    let sh = self.slot_hash(B256::from(*k));
                    let v = trimmed(slot.present_value());
                    if !v.is_empty() {
                        self.owners.insert(ah);
                    }
                    self.put(&slot_key(&ah, &sh), &v);
                }
            }
            self.put(&acct_key(&ah), &account_rlp(&a.info));
            if let Some(code) = a.info.code {
                if !a.info.code_hash.is_zero() && a.info.code_hash != alloy_primitives::KECCAK256_EMPTY && !self.code.contains_key(&a.info.code_hash) {
                    self.new_code.push((a.info.code_hash, code.original_bytes()));
                    self.code.insert(a.info.code_hash, code);
                }
            }
        }
    }
}

impl StateDb for Backend {
    fn set_block_hash(&mut self, number: u64, hash: B256) {
        self.block_hashes.insert(number, hash);
        if number > BLOCK_HASH_WINDOW {
            self.block_hashes.remove(&(number - BLOCK_HASH_WINDOW - 1));
        }
    }
}

// ---------------------------------------------------------------------------
// Rolls and the manifest (engine.go).

/// The 32-byte user field of the run and the trie file: the roll height and
/// the first 24 bytes of the root at it.
pub fn user_data(h: u64, root: &B256) -> [u8; 32] {
    let mut u = [0u8; 32];
    u[..8].copy_from_slice(&h.to_le_bytes());
    u[8..].copy_from_slice(&root.as_slice()[..24]);
    u
}

pub fn run_path(dir: &Path, gen: u64) -> PathBuf {
    dir.join(format!("run.{gen}"))
}

pub fn trie_path(dir: &Path, gen: u64) -> PathBuf {
    dir.join(format!("trie.{gen}"))
}

/// MANIFEST {gen, height, root}: temp, fsync, rename, dir fsync.
pub fn write_manifest(dir: &Path, gen: u64, height: u64, root: &B256) -> std::io::Result<()> {
    let tmp = dir.join("MANIFEST.tmp");
    let mut f = std::fs::File::create(&tmp)?;
    f.write_all(format!("{{\"gen\":{gen},\"height\":{height},\"root\":\"{root}\"}}").as_bytes())?;
    f.sync_all()?;
    drop(f);
    std::fs::rename(&tmp, dir.join("MANIFEST"))?;
    std::fs::File::open(dir)?.sync_all()
}

/// Dirty's leaf source: the first row of the ROLLED state at or after prefix.
pub fn seek_fn(run: Arc<Run>) -> Arc<SeekFn> {
    Arc::new(move |prefix: &[u8]| {
        let mut it = run.iter(Some(prefix), None);
        if it.next() {
            Some((it.key().to_vec(), it.value().to_vec()))
        } else {
            None
        }
    })
}

pub struct RollResult {
    pub run: Run,
    pub file: File,
    pub root: Hash,
    pub stats: RollStats,
    pub merge: Duration,
    pub roll: Duration,
}

/// Merges the frozen overlay over the base run into run.<gen> and rolls
/// trie.<gen> on a background thread; the result arrives on the channel.
pub fn spawn_roll(dir: PathBuf, gen: u64, frozen: Arc<Overlay>, base: Arc<Run>, user: [u8; 32]) -> Receiver<Result<RollResult, String>> {
    let (tx, rx) = sync_channel(1);
    std::thread::spawn(move || {
        let r = (|| {
            let t0 = Instant::now();
            let v = View::new(Some(&frozen), &[&base]);
            let run = merge(&run_path(&dir, gen), &v, user).map_err(|e| format!("merge: {e}"))?;
            let merge_d = t0.elapsed();
            let t1 = Instant::now();
            let (root, stats) = roll(&mut run.iter(None, None), &trie_path(&dir, gen), user).map_err(|e| format!("roll: {e}"))?;
            let roll_d = t1.elapsed();
            let file = File::open(&trie_path(&dir, gen)).map_err(|e| format!("open trie: {e}"))?;
            Ok(RollResult { run, file, root, stats, merge: merge_d, roll: roll_d })
        })();
        let _ = tx.send(r);
    });
    rx
}

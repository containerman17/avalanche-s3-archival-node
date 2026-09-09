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
use anyhow::anyhow;
use exec::exec::{account_rlp, trimmed};
use exec::StateDb;
use revm::state::{AccountInfo, Bytecode, EvmState};
use revm::{Database, DatabaseCommit};
use state::commit::dirty::{Dirty, SeekFn};
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
use std::sync::{Arc, Mutex};
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
    pub code: B256Map<Bytecode>,
    addr_hash: AddressMap<B256>,
    slot_hash: B256Map<B256>,
    pub block_hashes: HashMap<u64, B256>,
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

    /// The live slot keys under an account (empty when the owners sets and
    /// the run say it has none: Overlay::iter is a full sort).
    pub fn slot_keys(&self, ah: &B256) -> Vec<Vec<u8>> {
        let mut lo = [0u8; 33];
        lo[..32].copy_from_slice(ah.as_slice());
        let mut hi = lo;
        lo[32] = 1;
        hi[32] = 2;
        let in_run = |run: &Option<Arc<Run>>| run.as_ref().is_some_and(|r| r.iter(Some(&lo[..]), Some(&hi[..])).next());
        if !self.owners.contains(ah) && !self.frozen_owners.contains(ah) && !in_run(&self.run) {
            return Vec::new();
        }
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
    }

    /// engine.tombstoneSlots: an account delete tombstones every live slot
    /// under it in the fresh overlay (Dirty wipes the storage itself).
    fn tombstone_slots(&mut self, ah: &B256) {
        for k in self.slot_keys(ah) {
            self.overlay.put(&k, &[]);
        }
    }

    /// engine.applyOverlay: one block's ordered write set (contract keys)
    /// into the fresh overlay, tombstoning the slots of deleted accounts.
    /// The write set is not recorded (it came from a verified block).
    pub fn apply_ws(&mut self, ws: &[(Vec<u8>, Vec<u8>)]) {
        for (k, v) in ws {
            if k.len() == 33 && v.is_empty() {
                self.tombstone_slots(&B256::from_slice(&k[..32]));
            } else if k.len() == 65 && !v.is_empty() {
                self.owners.insert(B256::from_slice(&k[..32]));
            }
            self.overlay.put(k, v);
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

/// The manifest {gen, height, root}: the rolled pair and what it holds.
#[derive(Clone, Copy, Debug)]
pub struct Manifest {
    pub gen: u64,
    pub height: u64,
    pub root: B256,
}

pub fn read_manifest(dir: &Path) -> std::io::Result<Option<Manifest>> {
    let b = match std::fs::read(dir.join("MANIFEST")) {
        Ok(b) => b,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    let v: serde_json::Value = serde_json::from_slice(&b).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, format!("MANIFEST: {e}")))?;
    let field = |k: &str| v.get(k).and_then(|x| x.as_u64()).ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, format!("MANIFEST: {k}")));
    let root: B256 = v
        .get("root")
        .and_then(|x| x.as_str())
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "MANIFEST: root"))?;
    Ok(Some(Manifest { gen: field("gen")?, height: field("height")?, root }))
}

/// openEngine: the pair the manifest names, checked against it, every other
/// file in dir swept (a torn roll or a stale temp is anything unnamed).
pub fn open_rolled(dir: &Path, m: &Manifest) -> anyhow::Result<(Arc<Run>, Arc<File>)> {
    use anyhow::Context;
    let run = Run::open(&run_path(dir, m.gen)).with_context(|| format!("vmstate run {}", m.gen))?;
    let file = File::open(&trie_path(dir, m.gen)).map_err(|e| anyhow::anyhow!("vmstate trie {}: {e}", m.gen))?;
    let want = user_data(m.height, &m.root);
    if run.user_data() != want || file.user_data() != want || file.root() != m.root.0 {
        anyhow::bail!("vmstate: manifest names gen {} at height {} root {}, but the files carry run={:?} trie={:?} root={}", m.gen, m.height, m.root, run.user_data(), file.user_data(), B256::from(file.root()));
    }
    let keep = ["MANIFEST".to_string(), format!("run.{}", m.gen), format!("trie.{}", m.gen)];
    for e in std::fs::read_dir(dir)? {
        let e = e?;
        let name = e.file_name().to_string_lossy().into_owned();
        if !keep.contains(&name) {
            let _ = std::fs::remove_file(e.path());
            eprintln!("epochdb-rs: swept {name}: not named by the manifest");
        }
    }
    Ok((Arc::new(run), Arc::new(file)))
}

/// maybeRoll / finishRoll over a Backend: the roll thread, the manifest,
/// Dirty's rebase. The caller parks its checker around `finish_roll`.
pub struct Roller {
    pub dir: PathBuf,
    pub gen: u64,
    pub rolls: u64,
    rolling: Option<Receiver<Result<RollResult, String>>>,
    roll_h: u64,
    roll_root: B256,
    roll_t0: Instant,
    pub dirty: Arc<Mutex<Dirty>>,
    pub workers: usize,
    /// The second trigger beside the byte budget: a roll is due once this
    /// many blocks or this much time passed since the last one (a manifest
    /// every so often bounds a crash replay; on beam 9.5M blocks of small rows
    /// never reached the 2 GB budget and every restart replayed everything).
    /// `u64::MAX` / `Duration::MAX` = off (the bench).
    pub every_blocks: u64,
    pub every: Duration,
    last_h: u64,
    last_t: Instant,
}

impl Roller {
    /// `rolled_h` is the manifest's height: the count and the clock start there.
    pub fn new(dir: PathBuf, gen: u64, rolled_h: u64, dirty: Arc<Mutex<Dirty>>, workers: usize) -> Roller {
        Roller { dir, gen, rolls: 0, rolling: None, roll_h: 0, roll_root: B256::ZERO, roll_t0: Instant::now(), dirty, workers, every_blocks: u64::MAX, every: Duration::MAX, last_h: rolled_h, last_t: Instant::now() }
    }

    pub fn rolling(&self) -> bool {
        self.rolling.is_some()
    }

    /// maybeRoll: freezes the overlay once it is over budget and merges +
    /// rolls it in the background; h and root are what the overlay's state
    /// is at, the oracle for the rolled file.
    pub fn maybe_roll(&mut self, be: &mut Backend, budget: usize, h: u64, root: B256) {
        if self.rolling.is_some() {
            return;
        }
        // The budget is over the overlay AND Dirty: Dirty keeps every trie
        // node touched since the last roll and only a roll drops them, and on
        // a chain of small rows (beam, 4M blocks) neither alone reaches 2 GB
        // while the two together hold 3 GB and keep growing. try_lock: the
        // checker holds Dirty for a whole block's root; a miss is checked
        // again next block.
        let dirty = self.dirty.try_lock().map(|d| d.bytes()).unwrap_or(0);
        let due = h.saturating_sub(self.last_h) >= self.every_blocks || self.last_t.elapsed() >= self.every;
        if (be.overlay.bytes() + dirty < budget && !due) || be.overlay.is_empty() {
            return;
        }
        self.last_h = h;
        self.last_t = Instant::now();
        let frozen = be.freeze();
        let base = be.run.clone().expect("run");
        let gen = self.gen + 1;
        eprintln!(
            "epochdb-rs: roll {gen} start: height={h} overlay={} keys/{:.0}MB dirty={:.0}MB",
            frozen.len(),
            frozen.bytes() as f64 / 1e6,
            self.dirty.lock().unwrap().bytes() as f64 / 1e6
        );
        self.roll_h = h;
        self.roll_root = root;
        self.roll_t0 = Instant::now();
        self.rolling = Some(spawn_roll(self.dir.clone(), gen, frozen, base, user_data(h, &root)));
    }

    /// The finished roll, if one is ready; None while none is or it still
    /// runs. `wait` blocks for it (shutdown).
    pub fn poll_roll(&mut self, wait: bool) -> anyhow::Result<Option<RollResult>> {
        let Some(rx) = &self.rolling else { return Ok(None) };
        let r = if wait {
            rx.recv().map_err(|_| anyhow!("roll thread gone"))?
        } else {
            match rx.try_recv() {
                Ok(r) => r,
                Err(_) => return Ok(None),
            }
        };
        self.rolling = None;
        Ok(Some(r.map_err(|e| anyhow!("roll {}: {e}", self.gen + 1))?))
    }

    /// swapRoll + finishRoll: with the checker parked (it has then verified
    /// every block handed to it), the rolled root must equal the verified
    /// one, `before_manifest` runs (the store's sync), the manifest names
    /// the new pair, Dirty is rebased on the new file with the fresh
    /// overlay's entries replayed, the backend swaps, the old pair goes.
    pub fn finish_roll(&mut self, be: &mut Backend, r: RollResult, before_manifest: impl FnOnce() -> anyhow::Result<()>) -> anyhow::Result<()> {
        if r.root != self.roll_root.0 {
            eprintln!("epochdb-rs: roll root mismatch at height {}: rolled {}, verified {}", self.roll_h, B256::from(r.root), self.roll_root);
            std::process::exit(1);
        }
        before_manifest()?;
        let gen = self.gen + 1;
        write_manifest(&self.dir, gen, self.roll_h, &self.roll_root)?;
        let run = Arc::new(r.run);
        let file = Arc::new(r.file);
        let (before, after, replayed) = {
            let mut d = self.dirty.lock().unwrap();
            let before = d.bytes();
            *d = Dirty::new(file, seek_fn(run.clone()));
            d.workers = self.workers;
            let mut it = be.overlay.iter(None, None);
            let mut n = 0;
            while it.next() {
                d.apply(it.key(), it.value())?;
                n += 1;
            }
            (before, d.bytes(), n)
        };
        be.swap(run.clone());
        let _ = std::fs::remove_file(run_path(&self.dir, self.gen));
        let _ = std::fs::remove_file(trie_path(&self.dir, self.gen));
        self.gen = gen;
        self.rolls += 1;
        eprintln!(
            "epochdb-rs: roll {gen} done: height={} keys={} nodes={} run={:.0}MB trie={:.0}MB merge={:.0}ms roll={:.0}ms total={:.0}ms replayed={replayed} overlay={:.0}MB dirty={:.0}MB->{:.0}MB",
            self.roll_h,
            r.stats.keys,
            r.stats.nodes,
            run.bytes() as f64 / 1e6,
            r.stats.bytes as f64 / 1e6,
            r.merge.as_secs_f64() * 1e3,
            r.roll.as_secs_f64() * 1e3,
            self.roll_t0.elapsed().as_secs_f64() * 1e3,
            be.overlay.bytes() as f64 / 1e6,
            before as f64 / 1e6,
            after as f64 / 1e6
        );
        Ok(())
    }
}

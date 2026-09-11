//! The node: import or restart, catch up, follow. One applier thread (execute,
//! hot apply, history), one checker thread (root per block, halt on
//! mismatch), one feed thread. Every public call is safe from any thread.

use crate::checker::{Checker, Mismatch};
use crate::exec::{Applier, HotBase};
use crate::feed::{self, now_ms, RawBlock};
use crate::history::{History, StoredBlock};
use crate::mempool::{Mempool, PendingEvent};
use crate::hot::{addr_hash, rows, slot_hash, Account, HotState};
use crate::{import, Config, Generation, Mode, Stale, Status};
use alloy_primitives::{Address, B256, U256};
use anyhow::{anyhow, bail, Context, Result};
use serde_json::json;
use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug)]
pub struct BlockEvent {
    pub height: u64,
    pub hash: B256,
    pub received_ms: u64,
    pub applied_ms: u64,
    pub exec_ms: u64,
    pub txs: usize,
}

struct Shared {
    hot: Arc<HotState>,
    halted: AtomicBool,
    halt: Mutex<Option<Mismatch>>,
    checked: AtomicU64,
    last_received_ms: AtomicU64,
    frozen: bool,
    subscribers: Mutex<Vec<Sender<BlockEvent>>>,
    mempool: Arc<Mempool>,
    /// The last applied block's header: the block context of `simulate`.
    head: Mutex<Option<block::eth::Header>>,
}

pub struct Node {
    s: Arc<Shared>,
    pub history: Arc<History>,
}

fn halt_file(dir: &Path, m: &Mismatch) {
    let _ = std::fs::write(dir.join("HALTED"), format!("{{\"height\":{},\"expected_root\":\"{}\",\"got_root\":\"{}\",\"at_ms\":{}}}\n", m.height, m.expected, m.got, now_ms()));
}

impl Node {
    pub fn open(cfg: Config, mode: Mode) -> Result<Node> {
        std::fs::create_dir_all(&cfg.data_dir)?;
        if cfg.data_dir.join("HALTED").exists() {
            bail!("{}: HALTED file present; the last run stopped on a root mismatch, decide by hand", cfg.data_dir.display());
        }
        let vmstate = cfg.data_dir.join("vmstate");
        let history = Arc::new(History::open(&cfg.data_dir.join("history.redb"))?);
        let hot = Arc::new(HotState::new(0, B256::ZERO));

        // 1. The state: a restart from the newest roll, or the bootstrap export.
        let t = std::time::Instant::now();
        let (mut checker, snap_height, _snap_root) = match crate::checker::read_manifest(&vmstate)? {
            Some(m) => {
                let (c, _) = Checker::open(&vmstate)?;
                let (a, s) = import::load_run(&c.run(), &hot)?;
                let codes = import::load_code(&cfg.bootstrap_dir, &hot).context("code.bin from the bootstrap export")?;
                eprintln!("node: restart from roll gen {} at height {} root {}: {a} accounts, {s} slots, {codes} codes in {:.1?}", m.gen, m.height, m.root, t.elapsed());
                (c, m.height, m.root)
            }
            None => {
                let meta = import::read_meta(&cfg.bootstrap_dir).with_context(|| format!("no vmstate in {} and no bootstrap export in {}: run cmd/cnode-export first", cfg.data_dir.display(), cfg.bootstrap_dir.display()))?;
                let (a, s, c) = import::load(&cfg.bootstrap_dir, &hot)?;
                eprintln!("node: import at height {} root {}: {a} accounts, {s} slots, {c} codes into the hot state in {:.1?}", meta.height, meta.state_root, t.elapsed());
                let mut rows = import::ExportIter::open(&cfg.bootstrap_dir)?;
                let c = Checker::seed(&vmstate, &mut rows, meta.height, meta.state_root)?;
                eprintln!("node: checker seeded, {} rows, root verified at {} in {:.1?} total", rows.rows, meta.height, t.elapsed());
                (c, meta.height, meta.state_root)
            }
        };

        // 2. Replay the history past the snapshot (already checked blocks).
        let mut height = snap_height;
        let mut hash = match history.block(height)? {
            Some(b) => b.block["hash"].as_str().unwrap_or_default().parse().unwrap_or(B256::ZERO),
            None => B256::ZERO,
        };
        let stop = match mode {
            Mode::AtHeight(h) => h,
            Mode::Tip => u64::MAX,
        };
        let head = history.head()?.unwrap_or(snap_height);
        let mut codes_replayed = 0usize;
        while height < head.min(stop) {
            let h = height + 1;
            let (Some(sb), Some(diff_rows)) = (history.block(h)?, history.diff(h)?) else { bail!("history: block {h} missing after snapshot {snap_height}") };
            let root: B256 = sb.block["stateRoot"].as_str().unwrap_or_default().parse()?;
            let d = crate::history::diff_from_rows(&diff_rows)?;
            for (ch, code) in &d.code {
                hot.put_code(*ch, code.clone());
                codes_replayed += 1;
            }
            hash = sb.block["hash"].as_str().unwrap_or_default().parse()?;
            hot.apply(h, hash, &d);
            checker.replay(h, &diff_rows, root);
            height = h;
        }
        if height > snap_height {
            eprintln!("node: replayed history {} to {height} ({codes_replayed} codes)", snap_height + 1);
        }
        hot.apply(height, hash, &Default::default());

        let s = Arc::new(Shared {
            hot: hot.clone(),
            halted: AtomicBool::new(false),
            halt: Mutex::new(None),
            checked: AtomicU64::new(height),
            last_received_ms: AtomicU64::new(0),
            frozen: matches!(mode, Mode::AtHeight(_)),
            subscribers: Mutex::new(Vec::new()),
            mempool: Mempool::new(5 * 60 * 1000),
            head: Mutex::new(match history.block(height)? {
                Some(b) => Some(crate::exec::block_from_json(&b.block)?.header),
                None => None,
            }),
        });
        let node = Node { s: s.clone(), history: history.clone() };
        if let Mode::AtHeight(h) = mode {
            if height != h {
                bail!("AtHeight({h}): the history reaches {height} only");
            }
            return Ok(node);
        }

        // 3. Follow: feed -> applier -> checker; the validators' mempools on the side.
        for (i, ws) in cfg.validator_ws.iter().enumerate() {
            let (m, ws) = (s.mempool.clone(), ws.clone());
            std::thread::Builder::new().name(format!("cnode-mempool-{i}")).spawn(move || m.run(i, ws))?;
        }
        let (raw_tx, raw_rx) = channel::<RawBlock>();
        let (chk_tx, chk_rx) = channel::<(u64, Vec<(Vec<u8>, Vec<u8>)>, B256)>();
        {
            let (ws, http) = (cfg.rpc_ws.clone(), cfg.rpc_http.clone());
            std::thread::Builder::new().name("cnode-feed".into()).spawn(move || feed::stream(&ws, &http, height + 1, &raw_tx))?;
        }
        {
            let s = s.clone();
            let every = cfg.snapshot_every_blocks.max(1);
            let dir = cfg.data_dir.clone();
            std::thread::Builder::new().name("cnode-checker".into()).spawn(move || checker_loop(s, checker, chk_rx, every, &dir))?;
        }
        {
            let s = s.clone();
            let history = history.clone();
            let http = cfg.rpc_http.clone();
            let ring = block_hash_ring(&http, height, &history)?;
            std::thread::Builder::new().name("cnode-applier".into()).spawn(move || {
                if let Err(e) = applier_loop(s.clone(), history, hot, ring, raw_rx, chk_tx, height, hash) {
                    eprintln!("node: applier stopped: {e:#}");
                    s.halted.store(true, Ordering::Release);
                }
            })?;
        }
        Ok(node)
    }

    pub fn generation(&self) -> Generation {
        self.s.hot.generation()
    }

    pub fn status(&self) -> Status {
        if let Some(m) = *self.s.halt.lock().unwrap() {
            return Status::Halted { height: m.height, expected_root: m.expected, got_root: m.got };
        }
        let g = self.s.hot.generation();
        if self.s.halted.load(Ordering::Acquire) {
            return Status::Halted { height: g.height, expected_root: B256::ZERO, got_root: B256::ZERO };
        }
        if self.s.frozen {
            return Status::Frozen { height: g.height };
        }
        let r = self.s.last_received_ms.load(Ordering::Relaxed);
        Status::Following { height: g.height, lag_ms: if r == 0 { 0 } else { now_ms().saturating_sub(r) } }
    }

    /// Blocks applied minus blocks checked: the checker's backlog.
    pub fn checker_lag(&self) -> u64 {
        self.s.hot.generation().height.saturating_sub(self.s.checked.load(Ordering::Relaxed))
    }

    fn live(&self) -> Result<(), Stale> {
        if self.s.halted.load(Ordering::Acquire) {
            Err(Stale)
        } else {
            Ok(())
        }
    }

    pub fn account(&self, g: Generation, a: Address) -> Result<Option<Account>, Stale> {
        self.live()?;
        self.s.hot.account(g, &addr_hash(&a))
    }
    pub fn account_by_hash(&self, g: Generation, h: &[u8; 32]) -> Result<Option<Account>, Stale> {
        self.live()?;
        self.s.hot.account(g, h)
    }
    pub fn storage(&self, g: Generation, a: Address, k: U256) -> Result<U256, Stale> {
        self.live()?;
        self.s.hot.storage(g, &addr_hash(&a), &slot_hash(&k))
    }
    pub fn storage_by_hash(&self, g: Generation, h: &[u8; 32], k: &[u8; 32]) -> Result<U256, Stale> {
        self.live()?;
        self.s.hot.storage(g, h, k)
    }
    pub fn code(&self, hash: B256) -> Option<Arc<[u8]>> {
        self.s.hot.code(&hash)
    }
    pub fn hot(&self) -> &Arc<HotState> {
        &self.s.hot
    }

    /// The last few minutes of pending transactions, oldest first.
    pub fn pending(&self) -> Vec<PendingEvent> {
        self.s.mempool.pending()
    }
    pub fn subscribe_pending(&self) -> Receiver<PendingEvent> {
        self.s.mempool.subscribe()
    }

    /// Runs `txs` (RPC-shaped tx JSON with `from`, as the mempool delivers
    /// them) as the next block on an overlay over the hot state at `g`,
    /// in order, each seeing the previous ones. Nothing is written. If the
    /// generation changes while it runs, `Stale`; a tx that cannot apply
    /// (nonce, funds, intrinsic gas) fails the batch with its index.
    // ponytail: whole-batch failure on one bad tx; per-tx skipping when the host needs it.
    pub fn simulate(&self, g: Generation, txs: &[serde_json::Value]) -> std::result::Result<Result<Vec<exec::TxResult>>, Stale> {
        self.live()?;
        let head = self.s.head.lock().unwrap().clone().ok_or(Stale)?;
        if head.number != g.height {
            return Err(Stale);
        }
        let base = crate::exec::GenBase { hot: self.s.hot.clone(), g, stale: AtomicBool::new(false) };
        let mut ap = Applier::new(base);
        let mut b = json!({"transactions": txs});
        for (k, v) in [("number", format!("0x{:x}", head.number + 1)), ("timestamp", format!("0x{:x}", now_ms() / 1000)), ("gasLimit", format!("0x{:x}", head.gas_limit)), ("baseFeePerGas", format!("0x{:x}", head.base_fee.unwrap_or_default())), ("miner", format!("{}", head.coinbase)), ("difficulty", "0x1".into()), ("extraData", "0x".into()), ("parentHash", format!("{}", g.hash)), ("excessBlobGas", "0x0".into())] {
            b[k] = serde_json::Value::String(v);
        }
        for k in ["sha3Uncles", "stateRoot", "transactionsRoot", "receiptsRoot", "mixHash", "hash"] {
            b[k] = serde_json::Value::String(format!("{}", B256::ZERO));
        }
        b["logsBloom"] = serde_json::Value::String(format!("0x{}", "00".repeat(256)));
        b["nonce"] = serde_json::Value::String("0x0000000000000000".into());
        b["gasUsed"] = serde_json::Value::String("0x0".into());
        let r = (|| {
            let blk = crate::exec::block_from_json(&b)?;
            let r = ap.ex.execute_block(&blk, blk.header.time)?;
            Ok(r.txs)
        })();
        if ap.ex.db_mut().base.stale.load(Ordering::Acquire) || self.s.hot.generation() != g {
            return Err(Stale);
        }
        Ok(r)
    }

    pub fn subscribe_blocks(&self) -> Receiver<BlockEvent> {
        let (tx, rx) = channel();
        self.s.subscribers.lock().unwrap().push(tx);
        rx
    }
}

/// The 256 hashes before `height` (BLOCKHASH), from the history where it has
/// them and the node otherwise.
fn block_hash_ring(http: &str, height: u64, history: &History) -> Result<HashMap<u64, B256>> {
    let mut ring = HashMap::new();
    for n in height.saturating_sub(256)..=height {
        let hash: B256 = match history.block(n)? {
            Some(b) => b.block["hash"].as_str().unwrap_or_default().parse()?,
            None => {
                let b = feed::rpc(http, "eth_getBlockByNumber", json!([format!("0x{n:x}"), false]))?;
                b["hash"].as_str().ok_or_else(|| anyhow!("block {n}: no hash"))?.parse()?
            }
        };
        ring.insert(n, hash);
    }
    Ok(ring)
}

fn applier_loop(
    s: Arc<Shared>,
    history: Arc<History>,
    hot: Arc<HotState>,
    ring: HashMap<u64, B256>,
    rx: Receiver<RawBlock>,
    chk: Sender<(u64, Vec<(Vec<u8>, Vec<u8>)>, B256)>,
    mut height: u64,
    mut hash: B256,
) -> Result<()> {
    let base = HotBase { hot: hot.clone(), hashes: Mutex::new(ring) };
    let mut ap = Applier::new(base);
    while let Ok(raw) = rx.recv() {
        if s.halted.load(Ordering::Acquire) {
            return Ok(());
        }
        if raw.height != height + 1 {
            bail!("feed gave block {} after {height}", raw.height);
        }
        let parent: B256 = raw.block["parentHash"].as_str().unwrap_or_default().parse()?;
        if hash != B256::ZERO && parent != hash {
            bail!("block {} parent {parent} is not our head {hash}", raw.height);
        }
        let t = std::time::Instant::now();
        let (b, _r) = ap.execute(&raw.block)?;
        let mut diff = ap.ex.db_mut().take_diff();
        // Acceptance test 3: CNODE_CORRUPT_AT=<height> flips one slot of that
        // block's diff in the hot state and the rows; the checker must halt there.
        if std::env::var("CNODE_CORRUPT_AT").ok().and_then(|v| v.parse::<u64>().ok()) == Some(b.height) {
            match diff.storage.first_mut() {
                Some((_, _, v)) => {
                    *v += U256::from(1u8);
                    eprintln!("CORRUPTION INJECTED at {}: one slot value +1", b.height);
                }
                None => eprintln!("CORRUPTION: block {} has no slot writes, try the next height", b.height),
            }
        }
        let exec_ms = t.elapsed().as_millis() as u64;
        hot.apply(b.height, b.hash, &diff);
        let applied_ms = now_ms();
        ap.ex.db_mut().base.hashes.lock().unwrap().insert(b.height, b.hash);
        ap.ex.db_mut().base.hashes.lock().unwrap().remove(&(b.height.saturating_sub(300)));
        height = b.height;
        hash = b.hash;
        s.last_received_ms.store(raw.received_ms, Ordering::Relaxed);
        let rows = rows(&diff);
        *s.head.lock().unwrap() = Some(b.header.clone());
        history.put(b.height, &StoredBlock { received_ms: raw.received_ms, applied_ms, block: raw.block }, &rows)?;
        let captured = s.mempool.flush();
        if !captured.is_empty() {
            history.put_mempool(b.height, &captured)?;
        }
        let _ = chk.send((b.height, rows, b.header.root));
        let ev = BlockEvent { height: b.height, hash: b.hash, received_ms: raw.received_ms, applied_ms, exec_ms, txs: b.txs.len() };
        s.subscribers.lock().unwrap().retain(|t| t.send(ev.clone()).is_ok());
    }
    Ok(())
}

fn checker_loop(s: Arc<Shared>, mut c: Checker, rx: Receiver<(u64, Vec<(Vec<u8>, Vec<u8>)>, B256)>, every: u64, dir: &Path) {
    while let Ok((h, rows, root)) = rx.recv() {
        if let Err(m) = c.apply(h, &rows, root) {
            eprintln!("CHECKER MISMATCH at {}: header root {} but our state rolls to {}. HALTED.", m.height, m.expected, m.got);
            halt_file(dir, &m);
            *s.halt.lock().unwrap() = Some(m);
            s.halted.store(true, Ordering::Release);
            return;
        }
        s.checked.store(h, Ordering::Relaxed);
        if let Err(e) = c.maybe_roll(every) {
            eprintln!("checker: roll failed: {e:#}");
        }
    }
}

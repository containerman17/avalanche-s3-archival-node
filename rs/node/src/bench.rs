//! epochdb-rs: the benchmark-grade Rust node. Blocks from a container dump
//! (rs/block, senders recovered on a pool ahead), revm execution (rs/exec)
//! over the flat state (rs/state: overlay + rolled run), the state root
//! checked against every header on a checker thread one block behind, the
//! overlay merged and the trie rolled in the background when it is over
//! budget, receipts + callTracer JSON + state rows appended to a history
//! file. One executor thread, like vmexec.
//!
//!   epochdb-rs --dump FILE --genesis chain.json --upgrade upgrade.json --data DIR
//!       [--from 1] [--to N] [--stop-at N] [--duration S] [--workers 14]
//!       [--roll-budget MB] [--history FILE] [--network 1]
//!       [--state native|firewood] [--fw-cache-mb 192] [--fw-revisions 128] [--root-inline]

use crate::engine;
use crate::firewood::{Committer, Firewood, Layer, Opts};
use alloy_eips::eip2718::Encodable2718;
use alloy_primitives::{Bytes, B256};
use anyhow::{anyhow, bail, Context, Result};
use engine::{run_path, seek_fn, trie_path, user_data, write_manifest, Backend, Roller};
use exec::{oracle, Config, Executor};
use state::commit::dirty::Dirty;
use state::commit::file::File;
use state::commit::roll::roll;
use state::view::{merge, View};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// How many executed blocks may wait for their root check (vmexec checkDepth).
const CHECK_DEPTH: usize = 4;
/// The history file's group-fsync cadence in blocks (vmexec flushEvery).
const FLUSH_EVERY: u64 = 256;

/// The bench snapshot, published per block by the checker (vmexec.Stats)
/// plus the time split of both threads.
#[derive(Clone, Copy, Default)]
struct Stats {
    height: u64,
    blocks: u64,
    txs: u64,
    gas: u64,
    overlay: usize,
    dirty: usize,
    /// Blocks whose root was hashed and compared (non-empty write set).
    checked: u64,
    rolls: u64,
    rolling: bool,
    t_read: Duration,
    t_evm: Duration,
    t_trace: Duration,
    t_commit: Duration,
    t_apply: Duration,
    t_root: Duration,
    t_write: Duration,
}

struct CheckItem {
    height: u64,
    want: B256,
    ws: Vec<(Vec<u8>, Vec<u8>)>,
    header_rlp: Bytes,
    receipts: Vec<u8>,
    traces: Vec<String>,
    code: Vec<(B256, Bytes)>,
    stats: Stats,
    /// --root-inline: the executor waits here until the root is checked.
    ack: Option<SyncSender<()>>,
    /// --state firewood: the block's layer (its Firewood ops come from it).
    layer: Option<Arc<Layer>>,
}

enum Msg {
    Block(Box<CheckItem>),
    /// A sync item: the checker reports parked, then waits to be resumed.
    Park(SyncSender<()>, Receiver<()>),
}

/// The flat history file: per block the header RLP, the receipts RLP, every
/// tx's callTracer JSON, the state rows and the deployed code, each
/// length-prefixed; fsync every FLUSH_EVERY blocks on a flusher thread.
struct History {
    w: std::io::BufWriter<std::fs::File>,
    flush: SyncSender<()>,
}

impl History {
    fn open(path: &str) -> Result<History> {
        let f = std::fs::File::create(path).with_context(|| format!("history {path}"))?;
        let f2 = f.try_clone()?;
        let (tx, rx) = sync_channel::<()>(1);
        std::thread::spawn(move || {
            for _ in rx {
                if let Err(e) = f2.sync_data() {
                    eprintln!("epochdb-rs: history fsync: {e}");
                    std::process::exit(1);
                }
            }
        });
        Ok(History { w: std::io::BufWriter::with_capacity(1 << 20, f), flush: tx })
    }

    fn put(&mut self, b: &[u8]) -> std::io::Result<()> {
        self.w.write_all(&(b.len() as u32).to_le_bytes())?;
        self.w.write_all(b)
    }

    fn write(&mut self, it: &CheckItem) -> std::io::Result<()> {
        self.put(&it.header_rlp)?;
        self.put(&it.receipts)?;
        for t in &it.traces {
            self.put(t.as_bytes())?;
        }
        for (k, v) in &it.ws {
            self.put(k)?;
            self.put(v)?;
        }
        for (h, c) in &it.code {
            self.put(h.as_slice())?;
            self.put(c)?;
        }
        if it.height % FLUSH_EVERY == 0 {
            self.w.flush()?;
            let _ = self.flush.try_send(()); // flusher busy: coalesce into the next multiple
        }
        Ok(())
    }
}

/// The checker thread: applies each block's write set to Dirty, checks the
/// root against the header (a mismatch kills the process), writes the block
/// to the history file and publishes the stats.
fn checker(rx: Receiver<Msg>, dirty: Arc<Mutex<Dirty>>, mut hist: Option<History>, stats: Arc<Mutex<Stats>>) -> Result<()> {
    let (mut t_apply, mut t_root, mut t_write) = (Duration::ZERO, Duration::ZERO, Duration::ZERO);
    let mut checked = 0u64;
    for msg in rx {
        let it = match msg {
            Msg::Park(parked, resume) => {
                let _ = parked.send(());
                let _ = resume.recv();
                continue;
            }
            Msg::Block(it) => it,
        };
        let dirty_bytes;
        {
            let mut d = dirty.lock().unwrap();
            let root = if it.ws.is_empty() {
                d.current_root()
            } else {
                let t0 = Instant::now();
                for (k, v) in &it.ws {
                    d.apply(k, v).with_context(|| format!("block {}: apply write set", it.height))?;
                }
                let t1 = Instant::now();
                t_apply += t1 - t0;
                let r = d.root().with_context(|| format!("block {}: state root", it.height))?;
                t_root += t1.elapsed();
                checked += 1;
                r
            };
            if root != it.want.0 {
                eprintln!("epochdb-rs: block {}: state root mismatch: computed {}, header {}", it.height, B256::from(root), it.want);
                std::process::exit(1);
            }
            dirty_bytes = d.bytes();
        }
        if let Some(ack) = &it.ack {
            let _ = ack.send(());
        }
        if let Some(h) = hist.as_mut() {
            let t0 = Instant::now();
            h.write(&it).with_context(|| format!("block {}: history write", it.height))?;
            t_write += t0.elapsed();
        }
        let mut s = it.stats;
        s.dirty = dirty_bytes;
        s.checked = checked;
        s.t_apply = t_apply;
        s.t_root = t_root;
        s.t_write = t_write;
        *stats.lock().unwrap() = s;
    }
    Ok(())
}

/// The Firewood checker: the block's ops proposed (Firewood hashes here, the
/// proposal's root is the block's), compared with the header, committed.
/// t_apply = propose, t_root = commit.
fn checker_fw(rx: Receiver<Msg>, mut c: Committer, mut hist: Option<History>, stats: Arc<Mutex<Stats>>) -> Result<Committer> {
    let (mut t_apply, mut t_root, mut t_write) = (Duration::ZERO, Duration::ZERO, Duration::ZERO);
    let mut checked = 0u64;
    for msg in rx {
        let it = match msg {
            Msg::Park(parked, resume) => {
                let _ = parked.send(());
                let _ = resume.recv();
                continue;
            }
            Msg::Block(it) => it,
        };
        let layer = it.layer.as_ref().expect("firewood item");
        let t0 = Instant::now();
        let root = c.propose(it.height, layer.ops())?;
        let t1 = Instant::now();
        t_apply += t1 - t0;
        if root != it.want {
            eprintln!("epochdb-rs: block {}: state root mismatch: firewood {root}, header {}", it.height, it.want);
            std::process::exit(1);
        }
        c.commit()?;
        t_root += t1.elapsed();
        checked += 1;
        if let Some(ack) = &it.ack {
            let _ = ack.send(());
        }
        if let Some(h) = hist.as_mut() {
            let t0 = Instant::now();
            h.write(&it).with_context(|| format!("block {}: history write", it.height))?;
            t_write += t0.elapsed();
        }
        let mut s = it.stats;
        s.checked = checked;
        s.t_apply = t_apply;
        s.t_root = t_root;
        s.t_write = t_write;
        *stats.lock().unwrap() = s;
    }
    Ok(c)
}

fn rss_mb() -> u64 {
    let s = std::fs::read_to_string("/proc/self/statm").unwrap_or_default();
    let pages: u64 = s.split_whitespace().nth(1).and_then(|p| p.parse().ok()).unwrap_or(0);
    pages * 4096 >> 20
}

/// RssAnon from /proc/self/status, MB (the leak watch: file-backed pages excluded).
fn rss_anon_mb() -> u64 {
    let s = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    s.lines().find(|l| l.starts_with("RssAnon:")).and_then(|l| l.split_whitespace().nth(1)).and_then(|k| k.parse::<u64>().ok()).unwrap_or(0) >> 10
}

/// The bench thread: one line every 10 s and one at exit (epochdb-vm's line
/// verbatim, full= always 0), the time split beside it; sets `stop` once
/// --duration seconds passed since the first executed block.
struct Bench {
    /// --state firewood: the checker's split is propose / commit.
    fw: bool,
    stats: Arc<Mutex<Stats>>,
    wait_ns: Arc<AtomicU64>,
    t0: Instant,
    first: Option<Instant>,
    last: Stats,
    last_t: Instant,
}

impl Bench {
    fn line(&mut self, tag: &str) {
        let s = *self.stats.lock().unwrap();
        let now = Instant::now();
        let dt = (now - self.last_t).as_secs_f64();
        let window = if dt > 0.0 { (s.gas - self.last.gas) as f64 / dt / 1e6 } else { 0.0 };
        let cum = match self.first {
            Some(f) if (now - f).as_secs_f64() > 0.0 => s.gas as f64 / (now - f).as_secs_f64() / 1e6,
            _ => 0.0,
        };
        eprintln!(
            "{tag} t={:.0} h={} blk={} tx={} mgas/s={window:.2} cum={cum:.2} wait={:.1} full=0 rss={} anon={} overlay={} dirty={} rolls={} rolling={}",
            (now - self.t0).as_secs_f64(),
            s.height,
            s.blocks,
            s.txs,
            self.wait_ns.load(Ordering::Relaxed) as f64 / 1e9,
            rss_mb(),
            rss_anon_mb(),
            s.overlay >> 20,
            s.dirty >> 20,
            s.rolls,
            s.rolling
        );
        let d = |a: Duration, b: Duration| a.saturating_sub(b).as_secs_f64();
        let l = self.last;
        let (la, lr) = if self.fw { ("propose", "commit") } else { ("apply", "root") };
        eprintln!(
            "split read={:.2}s evm={:.2}s trace={:.2}s commit={:.2}s | checker {la}={:.2}s {lr}={:.2}s write={:.2}s of {dt:.1}s",
            d(s.t_read, l.t_read),
            d(s.t_evm, l.t_evm),
            d(s.t_trace, l.t_trace),
            d(s.t_commit, l.t_commit),
            d(s.t_apply, l.t_apply),
            d(s.t_root, l.t_root),
            d(s.t_write, l.t_write)
        );
        self.last = s;
        self.last_t = now;
    }

    fn run(mut self, exit: Receiver<()>, duration: Option<f64>, stop: Arc<AtomicBool>) {
        let mut n = 0;
        loop {
            match exit.recv_timeout(Duration::from_secs(1)) {
                Ok(()) | Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
            }
            if self.first.is_none() && self.stats.lock().unwrap().blocks > 0 {
                self.first = Some(Instant::now());
            }
            if let (Some(f), Some(d)) = (self.first, duration) {
                if f.elapsed().as_secs_f64() >= d && !stop.swap(true, Ordering::Relaxed) {
                    eprintln!("epochdb-rs: --duration {d} s reached");
                }
            }
            n += 1;
            if n % 10 == 0 {
                self.line("bench");
            }
        }
        self.line("bench exit");
        let s = *self.stats.lock().unwrap();
        let ex = s.t_evm + s.t_trace + s.t_commit;
        let (la, lr) = if self.fw { ("propose", "commit") } else { ("apply", "root") };
        eprintln!(
            "split total read={:.2}s evm={:.2}s trace={:.2}s commit={:.2}s | checker {la}={:.2}s {lr}={:.2}s write={:.2}s | exec-thread {:.1} mgas/s | blocks={} root-checked={} rolls={}",
            s.t_read.as_secs_f64(),
            s.t_evm.as_secs_f64(),
            s.t_trace.as_secs_f64(),
            s.t_commit.as_secs_f64(),
            s.t_apply.as_secs_f64(),
            s.t_root.as_secs_f64(),
            s.t_write.as_secs_f64(),
            if ex.is_zero() { 0.0 } else { s.gas as f64 / ex.as_secs_f64() / 1e6 },
            s.blocks,
            s.checked,
            s.rolls
        );
    }
}

fn arg(args: &[String], name: &str) -> Option<String> {
    args.iter().position(|a| a == name).and_then(|i| args.get(i + 1).cloned())
}

fn num<T: std::str::FromStr>(args: &[String], name: &str, default: T) -> Result<T>
where
    T::Err: std::fmt::Display,
{
    match arg(args, name) {
        Some(s) => s.parse().map_err(|e| anyhow!("{name} {s}: {e}")),
        None => Ok(default),
    }
}

/// The bench mode: `args` is the whole argv.
pub fn main(args: Vec<String>) -> Result<()> {
    let dump = arg(&args, "--dump").ok_or_else(|| anyhow!("--dump FILE"))?;
    let mut genesis = std::fs::read(arg(&args, "--genesis").ok_or_else(|| anyhow!("--genesis chain.json"))?)?;
    let upgrade = match arg(&args, "--upgrade") {
        Some(p) => std::fs::read(p)?,
        None => Vec::new(),
    };
    let data = PathBuf::from(arg(&args, "--data").ok_or_else(|| anyhow!("--data DIR"))?);
    let mut network: u32 = num(&args, "--network", 1)?;
    let (mut blockchain_id, mut subnet_id) = (B256::ZERO, B256::ZERO);
    if let Ok(desc) = serde_json::from_slice::<serde_json::Value>(&genesis) {
        if let Some(gd) = desc.get("genesisData").and_then(|v| v.as_str()) {
            use base64::Engine;
            genesis = base64::engine::general_purpose::STANDARD.decode(gd).context("genesisData base64")?;
            if let Some(n) = desc.get("networkID").and_then(|v| v.as_u64()) {
                network = n as u32;
            }
            if let Some(id) = desc.get("blockchainID").and_then(|v| v.as_str()) {
                blockchain_id = exec::config::cb58(id)?;
            }
            if let Some(id) = desc.get("subnetID").and_then(|v| v.as_str()) {
                subnet_id = exec::config::cb58(id)?;
            }
        }
    }
    let from: u64 = num(&args, "--from", 1)?;
    let to: u64 = num(&args, "--to", u64::MAX)?;
    let stop_at: u64 = num(&args, "--stop-at", u64::MAX)?.min(to);
    let workers: usize = num(&args, "--workers", 4)?;
    let roll_budget: usize = num::<usize>(&args, "--roll-budget", 2048)? << 20;
    // --root-inline: the state root is checked before the next block executes
    // (the validator shape, Verify carries the root) instead of one block behind.
    let root_inline = args.iter().any(|a| a == "--root-inline");
    let duration: Option<f64> = arg(&args, "--duration").map(|s| s.parse()).transpose()?;
    let history = arg(&args, "--history").map(|p| History::open(&p)).transpose()?;
    if from != 1 {
        bail!("--from must be 1: the state starts at genesis (recovery is out of scope)");
    }
    let t0 = Instant::now();

    let cfg = Config::from_genesis(&genesis, &upgrade, network).context("config")?.with_chain(blockchain_id, subnet_id);
    match arg(&args, "--state").as_deref().unwrap_or("native") {
        "native" => {}
        "firewood" => {
            let opts = Opts { cache_bytes: num::<usize>(&args, "--fw-cache-mb", 192)? * 1_000_000, revisions: num(&args, "--fw-revisions", 128)? };
            return firewood_main(&args, cfg, dump, data, to, stop_at, workers, root_inline, duration, history, opts, t0);
        }
        other => bail!("--state {other}: native or firewood"),
    }
    let dirty_workers = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1);

    // Genesis: the alloc into the first run, the first trie rolled from it,
    // its root checked against a full alloy-trie recompute (newEngine).
    let dir = data.join("vmstate");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir)?;
    let mut ex = Executor::with_db(cfg.clone(), Backend::new())?;
    let want = oracle::state_root(Executor::new(cfg.clone())?.db());
    let (run0, file0) = {
        let be = ex.db_mut();
        be.take_ws();
        let frozen = be.freeze();
        let user = user_data(0, &want);
        let run0 = merge(&run_path(&dir, 0), &View::new(Some(&frozen), &[]), user).context("genesis merge")?;
        let (root, st) = roll(&mut run0.iter(None, None), &trie_path(&dir, 0), user).context("genesis roll")?;
        if root != want.0 {
            bail!("genesis root mismatch: rolled {}, alloc {want}", B256::from(root));
        }
        let file0 = File::open(&trie_path(&dir, 0))?;
        write_manifest(&dir, 0, 0, &want)?;
        eprintln!("epochdb-rs: genesis state ok: root={want} accounts={} keys={} nodes={} run={}B trie={}B", cfg.alloc.len(), st.keys, st.nodes, run0.bytes(), st.bytes);
        let run0 = Arc::new(run0);
        be.swap(run0.clone());
        (run0, Arc::new(file0))
    };
    let mut dirty = Dirty::new(file0, seek_fn(run0));
    dirty.workers = dirty_workers;
    let dirty = Arc::new(Mutex::new(dirty));

    let stats = Arc::new(Mutex::new(Stats::default()));
    let wait_ns = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let (check_tx, check_rx) = sync_channel::<Msg>(CHECK_DEPTH);
    let checker = {
        let (dirty, stats) = (dirty.clone(), stats.clone());
        std::thread::spawn(move || checker(check_rx, dirty, history, stats))
    };
    let (bench_exit_tx, bench_exit_rx) = sync_channel::<()>(1);
    let bench = {
        let b = Bench { fw: false, stats: stats.clone(), wait_ns: wait_ns.clone(), t0, first: None, last: Stats::default(), last_t: t0 };
        let stop = stop.clone();
        std::thread::spawn(move || b.run(bench_exit_rx, duration, stop))
    };
    eprintln!(
        "epochdb-rs: chainId={} dump={dump} heights={from}..{} roll-budget={}MB workers={workers} dirty-workers={dirty_workers} root-inline={root_inline} history={}",
        cfg.chain_id,
        if stop_at == u64::MAX { "end".to_string() } else { stop_at.to_string() },
        roll_budget >> 20,
        arg(&args, "--history").unwrap_or_default()
    );

    let mut ex = ex;
    let mut roller = Roller::new(dir, 0, 0, dirty, dirty_workers);
    let blocks = block::Blocks::open(&dump, from, to)?;
    let mut it = block::recovered(blocks, workers);
    let mut parent_time = cfg.genesis_timestamp;
    let (mut nblk, mut ntx, mut gas) = (0u64, 0u64, 0u64);
    let mut t_read = Duration::ZERO;
    let mut t_root_wait = Duration::ZERO;
    let mut first = true;
    let res = (|| -> Result<()> {
        loop {
            if stop.load(Ordering::Relaxed) {
                return Ok(());
            }
            if let Some(r) = roller.poll_roll(false)? {
                let (ptx, prx) = sync_channel(0);
                let (rtx, rrx) = sync_channel(0);
                check_tx.send(Msg::Park(ptx, rrx)).map_err(|_| anyhow!("checker gone"))?;
                prx.recv().map_err(|_| anyhow!("checker gone"))?;
                roller.finish_roll(ex.db_mut(), r, || Ok(()))?;
                rtx.send(()).map_err(|_| anyhow!("checker gone"))?;
            }
            let tw = Instant::now();
            let Some(b) = it.next() else { return Ok(()) };
            let w = tw.elapsed();
            t_read += w;
            wait_ns.fetch_add(w.as_nanos() as u64, Ordering::Relaxed);
            let b = b.map_err(|e| anyhow!("block decode: {e}"))?;
            let h = &b.header;
            if h.number > stop_at {
                eprintln!("epochdb-rs: reached --stop-at {stop_at}");
                return Ok(());
            }
            if first {
                ex.set_block_hash(h.number - 1, h.parent_hash);
                first = false;
            }
            let r = ex.execute_block(&b, parent_time).with_context(|| format!("block {}", h.number))?;
            if r.gas_used != h.gas_used {
                bail!("block {}: gasUsed {} != header {}", h.number, r.gas_used, h.gas_used);
            }
            if r.receipts_root != h.receipt_hash {
                bail!("block {}: receiptsRoot {} != header {}", h.number, r.receipts_root, h.receipt_hash);
            }
            if r.bloom != h.bloom {
                bail!("block {}: logsBloom differs from the header", h.number);
            }
            ex.set_block_hash(h.number, b.hash);
            nblk += 1;
            ntx += r.txs.len() as u64;
            gas += r.gas_used;
            parent_time = h.time;
            // From here the block's root is the header's or the checker dies.
            roller.maybe_roll(ex.db_mut(), roll_budget, h.number, h.root);
            let (ws, code) = ex.db_mut().take_ws();
            let mut receipts = Vec::new();
            let mut traces = Vec::with_capacity(r.txs.len());
            for t in r.txs {
                t.receipt.encode_2718(&mut receipts);
                traces.push(t.trace_json);
            }
            let stats = Stats {
                height: h.number,
                blocks: nblk,
                txs: ntx,
                gas,
                overlay: ex.db().overlay.bytes(),
                rolls: roller.rolls,
                rolling: roller.rolling(),
                t_read,
                t_evm: ex.t_evm,
                t_trace: ex.t_trace,
                t_commit: ex.t_commit,
                ..Default::default()
            };
            let (ack, ack_rx) = if root_inline { let (t, r) = sync_channel::<()>(1); (Some(t), Some(r)) } else { (None, None) };
            let item = CheckItem { height: h.number, want: h.root, ws, header_rlp: Bytes::from(b.header_rlp.clone()), receipts, traces, code, stats, ack, layer: None };
            if check_tx.send(Msg::Block(Box::new(item))).is_err() {
                bail!("checker stopped");
            }
            if let Some(rx) = ack_rx {
                let t0 = Instant::now();
                rx.recv().map_err(|_| anyhow!("checker stopped"))?;
                t_root_wait += t0.elapsed();
            }
        }
    })();
    drop(check_tx);
    let cres = checker.join().map_err(|_| anyhow!("checker panicked"))?;
    let _ = bench_exit_tx.send(());
    let _ = bench.join();
    if root_inline {
        eprintln!("epochdb-rs: root-inline: executor waited {:.2}s for the checker (root work on the execution path)", t_root_wait.as_secs_f64());
    }
    res?;
    cres?;
    let _ = Path::new(&dump);
    Ok(())
}

/// `--state firewood`: the same loop over the Firewood engine. No roll (Firewood
/// keeps its own node store and revisions); the checker proposes and commits.
#[allow(clippy::too_many_arguments)]
fn firewood_main(args: &[String], cfg: Config, dump: String, data: PathBuf, to: u64, stop_at: u64, workers: usize, root_inline: bool, duration: Option<f64>, history: Option<History>, opts: Opts, t0: Instant) -> Result<()> {
    let dir = data.join("vmstate");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir)?;
    let mut committer = Committer::open(&dir, true, opts)?;
    let mut ex = Executor::with_db(cfg.clone(), Firewood::new(committer.committed()))?;
    let want = oracle::state_root(Executor::new(cfg.clone())?.db());
    {
        let db = ex.db_mut();
        db.take_ws();
        let layer = db.finish(0, B256::ZERO, 0).layer;
        let nops = layer.map.len();
        db.accept(0, layer.clone());
        let tg = Instant::now();
        let root = committer.propose(0, layer.ops())?;
        if root != want {
            bail!("genesis root mismatch: firewood {root}, alloc {want}");
        }
        committer.commit()?;
        eprintln!("epochdb-rs: genesis state ok: root={want} accounts={} keys={nops} firewood={}B in {:.0}ms", cfg.alloc.len(), Committer::disk_bytes(&dir), tg.elapsed().as_secs_f64() * 1e3);
    }

    let stats = Arc::new(Mutex::new(Stats::default()));
    let wait_ns = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let (check_tx, check_rx) = sync_channel::<Msg>(CHECK_DEPTH);
    let checker = {
        let stats = stats.clone();
        std::thread::spawn(move || checker_fw(check_rx, committer, history, stats))
    };
    let (bench_exit_tx, bench_exit_rx) = sync_channel::<()>(1);
    let bench = {
        let b = Bench { fw: true, stats: stats.clone(), wait_ns: wait_ns.clone(), t0, first: None, last: Stats::default(), last_t: t0 };
        let stop = stop.clone();
        std::thread::spawn(move || b.run(bench_exit_rx, duration, stop))
    };
    eprintln!(
        "epochdb-rs: chainId={} dump={dump} heights=1..{} state=firewood cache={}MB revisions={} workers={workers} root-inline={root_inline} history={}",
        cfg.chain_id,
        if stop_at == u64::MAX { "end".to_string() } else { stop_at.to_string() },
        opts.cache_bytes / 1_000_000,
        opts.revisions,
        arg(args, "--history").unwrap_or_default()
    );

    let blocks = block::Blocks::open(&dump, 1, to)?;
    let mut it = block::recovered(blocks, workers);
    let mut parent_time = cfg.genesis_timestamp;
    let (mut nblk, mut ntx, mut gas) = (0u64, 0u64, 0u64);
    let mut t_read = Duration::ZERO;
    let mut t_root_wait = Duration::ZERO;
    let mut first = true;
    let res = (|| -> Result<()> {
        loop {
            if stop.load(Ordering::Relaxed) {
                return Ok(());
            }
            let tw = Instant::now();
            let Some(b) = it.next() else { return Ok(()) };
            let w = tw.elapsed();
            t_read += w;
            wait_ns.fetch_add(w.as_nanos() as u64, Ordering::Relaxed);
            let b = b.map_err(|e| anyhow!("block decode: {e}"))?;
            let h = &b.header;
            if h.number > stop_at {
                eprintln!("epochdb-rs: reached --stop-at {stop_at}");
                return Ok(());
            }
            if first {
                ex.set_block_hash(h.number - 1, h.parent_hash);
                first = false;
            }
            ex.db_mut().begin(None);
            let r = ex.execute_block(&b, parent_time).with_context(|| format!("block {}", h.number))?;
            if r.gas_used != h.gas_used {
                bail!("block {}: gasUsed {} != header {}", h.number, r.gas_used, h.gas_used);
            }
            if r.receipts_root != h.receipt_hash {
                bail!("block {}: receiptsRoot {} != header {}", h.number, r.receipts_root, h.receipt_hash);
            }
            if r.bloom != h.bloom {
                bail!("block {}: logsBloom differs from the header", h.number);
            }
            ex.set_block_hash(h.number, b.hash);
            nblk += 1;
            ntx += r.txs.len() as u64;
            gas += r.gas_used;
            parent_time = h.time;
            let db = ex.db_mut();
            let (ws, code) = db.take_ws();
            let layer = db.finish(h.number, b.hash, h.time).layer;
            db.accept(h.number, layer.clone());
            let mut receipts = Vec::new();
            let mut traces = Vec::with_capacity(r.txs.len());
            for t in r.txs {
                t.receipt.encode_2718(&mut receipts);
                traces.push(t.trace_json);
            }
            let stats = Stats {
                height: h.number,
                blocks: nblk,
                txs: ntx,
                gas,
                overlay: ex.db().accepted_bytes(),
                t_read,
                t_evm: ex.t_evm,
                t_trace: ex.t_trace,
                t_commit: ex.t_commit,
                ..Default::default()
            };
            let (ack, ack_rx) = if root_inline { let (t, r) = sync_channel::<()>(1); (Some(t), Some(r)) } else { (None, None) };
            let item = CheckItem { height: h.number, want: h.root, ws, header_rlp: Bytes::from(b.header_rlp.clone()), receipts, traces, code, stats, ack, layer: Some(layer) };
            if check_tx.send(Msg::Block(Box::new(item))).is_err() {
                bail!("checker stopped");
            }
            if let Some(rx) = ack_rx {
                let t0 = Instant::now();
                rx.recv().map_err(|_| anyhow!("checker stopped"))?;
                t_root_wait += t0.elapsed();
            }
        }
    })();
    drop(check_tx);
    let cres = checker.join().map_err(|_| anyhow!("checker panicked"))?;
    let _ = bench_exit_tx.send(());
    let _ = bench.join();
    if root_inline {
        eprintln!("epochdb-rs: root-inline: executor waited {:.2}s for the checker (root work on the execution path)", t_root_wait.as_secs_f64());
    }
    eprintln!("epochdb-rs: firewood: trie reads {} (misses in every layer)", ex.db().trie_reads);
    res?;
    let committer = cres?;
    let tc = Instant::now();
    let root = committer.root();
    committer.close()?;
    eprintln!("epochdb-rs: firewood closed: root={root} disk={}B in {:.0}ms", Committer::disk_bytes(&dir), tc.elapsed().as_secs_f64() * 1e3);
    Ok(())
}

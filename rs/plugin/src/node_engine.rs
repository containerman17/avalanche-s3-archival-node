//! NodeEngine: rs/node's executor and flat state behind the plugin's
//! `Engine` trait (the follower semantics of vmchain/vm.go + vmexec).
//!
//! parse decodes and recovers senders (BatchedParseBlock on a rayon pool),
//! keeping the parsed blocks by id so the host's re-parse in Verify finds
//! them. verify executes the block on `Layered` (the pending chain over the
//! accepted state) and checks gasUsed / receiptsRoot / logsBloom against the
//! header. accept applies the write set to the fresh overlay, hands the
//! block to the checker thread (Dirty root one block behind: a mismatch
//! prints both roots and exits, the Go follower's log.Fatal; then the store
//! write), and rolls by budget. Initialize opens the rolled pair the
//! MANIFEST names and replays the store's rows since the roll (recover.go).
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use alloy_eips::eip2718::Encodable2718;
use alloy_primitives::B256;
use anyhow::{anyhow, bail, Context as _};
use block::Block;
use bytes::Bytes;
use exec::{Config, Executor, StateDb};
use node::engine::{open_rolled, read_manifest, run_path, seek_fn, trie_path, user_data, write_manifest, Backend, Roller};
use rayon::prelude::*;
use revm::state::Bytecode;
use state::commit::dirty::Dirty;
use state::commit::file::File;
use state::commit::roll::roll;
use state::view::{merge, View};

use rpc::genesis;
use crate::layered::{Layered, Pending};
use crate::log::{BlockLog, BlockStore, CodeLog, Record};
use crate::tree::{hex, Engine, Error, Id, Meta};
use crate::vm::Init;

/// vmexec/budget.go: the overlay bytes that trigger a roll while catching
/// up and at the tip.
const SYNC_ROLL: usize = 2 << 30;
const TIP_ROLL: usize = 128 << 20;
/// How many accepted blocks may wait for their root check (checkDepth).
const CHECK_DEPTH: usize = 4;
/// The store's group-fsync cadence in blocks (flushEvery).
const FLUSH_EVERY: u64 = 256;
/// Parsed blocks kept by id (the host re-parses in Verify).
const PARSED_MAX: usize = 8192;

struct CheckItem {
    want: B256,
    record: Record,
}

enum Msg {
    Block(Box<CheckItem>),
    /// A sync item: the checker reports parked, then waits to be resumed.
    Park(SyncSender<()>, Receiver<()>),
}

pub struct Inner {
    pub ex: Executor<Layered>,
    roller: Roller,
    roll_budget: usize,
}

// SAFETY: revm's Evm holds an Rc (LocalContext's shared memory buffer) and
// raw bytecode pointers, all private to the Evm and touched only while it
// runs; Inner is only ever used under the engine's Mutex, one thread at a
// time, and nothing outside it clones those Rcs.
unsafe impl Send for Inner {}

#[derive(Default)]
pub struct Stats {
    pub executed: AtomicU64,
    pub txs: AtomicU64,
    pub gas: AtomicU64,
    pub checked: AtomicU64,
    /// Nanoseconds inside parse_batch / parse / verify / accept / the checker's block work.
    pub t_parse_batch: AtomicU64,
    pub t_parse: AtomicU64,
    pub t_verify: AtomicU64,
    pub t_accept: AtomicU64,
    pub t_check: AtomicU64,
}

fn tick(a: &AtomicU64, t0: Instant) {
    a.fetch_add(t0.elapsed().as_nanos() as u64, Ordering::Relaxed);
}

pub struct NodeEngine {
    pub chain_id: u64,
    pub genesis: Arc<Block>,
    pub inner: Arc<Mutex<Inner>>,
    pub store: Arc<Mutex<Box<dyn BlockStore>>>,
    code_log: Arc<Mutex<CodeLog>>,
    pub head: Arc<Mutex<Arc<Block>>>,
    pub rpc_store: Arc<crate::rpc_store::PluginStore>,
    pub rpc: rpc::Server,
    /// Accepted, not yet in the store (the checker is behind by at most CHECK_DEPTH).
    pub recent: Arc<Mutex<HashMap<Id, Arc<Block>>>>,
    parsed: Mutex<HashMap<Id, Arc<Block>>>,
    check_tx: Mutex<Option<SyncSender<Msg>>>,
    checker: Mutex<Option<std::thread::JoinHandle<anyhow::Result<()>>>>,
    pub stats: Arc<Stats>,
    pool: rayon::ThreadPool,
    sync_roll: usize,
    tip_roll: usize,
    t0: Instant,
}

/// The checker thread: Dirty apply + root per block (a mismatch kills the
/// process), then the store append, fsync every FLUSH_EVERY blocks on a
/// flusher thread.
fn checker(rx: Receiver<Msg>, dirty: Arc<Mutex<Dirty>>, store: Arc<Mutex<Box<dyn BlockStore>>>, code_log: Arc<Mutex<CodeLog>>, recent: Arc<Mutex<HashMap<Id, Arc<Block>>>>, stats: Arc<Stats>, flush: SyncSender<()>) -> anyhow::Result<()> {
    for msg in rx {
        let it = match msg {
            Msg::Park(parked, resume) => {
                let _ = parked.send(());
                let _ = resume.recv();
                continue;
            }
            Msg::Block(it) => it,
        };
        let h = it.record.height;
        let t0 = Instant::now();
        {
            let mut d = dirty.lock().unwrap();
            let root = if it.record.ws.is_empty() {
                d.current_root()
            } else {
                for (k, v) in &it.record.ws {
                    d.apply(k, v).with_context(|| format!("block {h}: apply write set"))?;
                }
                d.root().with_context(|| format!("block {h}: state root"))?
            };
            if root != it.want.0 {
                eprintln!("epochdb-rs: block {h}: state root mismatch: computed {}, header {}", B256::from(root), it.want);
                std::process::exit(1);
            }
        }
        store.lock().unwrap().append(&it.record).with_context(|| format!("block {h}: store append"))?;
        code_log.lock().unwrap().append(&it.record.code).with_context(|| format!("block {h}: code append"))?;
        recent.lock().unwrap().remove(&it.record.id);
        stats.checked.fetch_add(1, Ordering::Relaxed);
        tick(&stats.t_check, t0);
        if h % FLUSH_EVERY == 0 {
            let _ = flush.try_send(()); // flusher busy: coalesce into the next multiple
        }
    }
    store.lock().unwrap().sync()?;
    code_log.lock().unwrap().sync()?;
    Ok(())
}

impl NodeEngine {
    pub fn open(init: &Init) -> Result<NodeEngine, Error> {
        Self::open_inner(init).map_err(|e| format!("{e:#}").into())
    }

    fn open_inner(init: &Init) -> anyhow::Result<NodeEngine> {
        let t0 = Instant::now();
        let cfg = Config::from_genesis(&init.genesis_bytes, &init.upgrade_bytes, init.network_id).context("config")?;
        let genesis = Arc::new(genesis::block(&cfg, &init.genesis_bytes).map_err(|e| anyhow!("genesis: {e}"))?);
        let conf: serde_json::Value = serde_json::from_slice(&init.config_bytes).unwrap_or(serde_json::Value::Null);
        let sync_roll = conf.get("roll-budget-mb").and_then(|v| v.as_u64()).map(|m| (m as usize) << 20).unwrap_or(SYNC_ROLL);
        let tip_roll = TIP_ROLL.min(sync_roll);
        let cpus = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1);
        let workers = cpus.saturating_sub(2).max(1);
        let data = PathBuf::from(&init.chain_data_dir);
        std::fs::create_dir_all(&data)?;
        let dir = data.join("vmstate");

        let mut be = Backend::new();
        let (gen, rolled_h, run, file) = match read_manifest(&dir)? {
            Some(m) => {
                let (run, file) = open_rolled(&dir, &m)?;
                be.swap(run.clone());
                (m.gen, m.height, run, file)
            }
            None => {
                let _ = std::fs::remove_dir_all(&dir);
                std::fs::create_dir_all(&dir)?;
                // The genesis state through the executor (alloc + precompiles
                // active at 0), into the first run and trie, root-checked.
                let mut ex = Executor::with_db(cfg.clone(), Layered::new(Backend::new())).context("genesis state")?;
                let p = ex.db_mut().finish(0, genesis.hash, 0, Vec::new(), Vec::new(), 0, 0);
                let payload = p.payload.lock().unwrap().take().unwrap();
                be.apply_ws(&payload.ws);
                for (h, c) in p.code {
                    be.code.insert(h, c);
                }
                let want = genesis.header.root;
                let frozen = be.freeze();
                let user = user_data(0, &want);
                let run0 = merge(&run_path(&dir, 0), &View::new(Some(&frozen), &[]), user).context("genesis merge")?;
                let (root, st) = roll(&mut run0.iter(None, None), &trie_path(&dir, 0), user).context("genesis roll")?;
                if root != want.0 {
                    bail!("genesis root mismatch: rolled {}, alloc {want}", B256::from(root));
                }
                let file0 = File::open(&trie_path(&dir, 0))?;
                write_manifest(&dir, 0, 0, &want)?;
                eprintln!("epochdb-rs: genesis state ok: root={want} hash={} accounts={} keys={} nodes={} run={}B trie={}B", genesis.hash, cfg.alloc.len(), st.keys, st.nodes, run0.bytes(), st.bytes);
                let run0 = Arc::new(run0);
                be.swap(run0.clone());
                (0, 0, run0, Arc::new(file0))
            }
        };

        let (store, torn) = BlockLog::open(&data.join("blocks.log"))?;
        if torn > 0 {
            eprintln!("epochdb-rs: blocks.log: dropped a torn tail of {torn} bytes");
        }
        let (code_log, codes, torn) = CodeLog::open(&data.join("code.log"))?;
        if torn > 0 {
            eprintln!("epochdb-rs: code.log: dropped a torn tail of {torn} bytes");
        }
        for (h, c) in codes {
            be.code.insert(h, Bytecode::new_raw(c));
        }
        let head_h = store.head();
        if head_h < rolled_h {
            bail!("vmstate rolled at height {rolled_h}, but the store holds blocks only through {head_h}");
        }
        let header_at = |h: u64| -> anyhow::Result<block::Header> {
            if h == 0 {
                return Ok(genesis.header.clone());
            }
            let c = store.container(h)?.ok_or_else(|| anyhow!("the store holds no block at {h}"))?;
            Ok(block::decode_container(c).map_err(|e| anyhow!("block {h}: {e}"))?.header)
        };
        if head_h >= 1 && header_at(1)?.parent_hash != genesis.hash {
            bail!("the store's block 1 has parent {}, the genesis is {}", header_at(1)?.parent_hash, genesis.hash);
        }
        let want = header_at(rolled_h)?.root;
        if file.root() != want.0 {
            bail!("vmstate rolled at height {rolled_h} with root {}, but the chain's root there is {want}", B256::from(file.root()));
        }
        let mut dirty = Dirty::new(file, seek_fn(run));
        dirty.workers = cpus;
        let mut rows = 0usize;
        for h in rolled_h + 1..=head_h {
            let r = store.read(h)?.ok_or_else(|| anyhow!("the store holds no block at {h}"))?;
            be.apply_ws(&r.ws);
            for (k, v) in &r.ws {
                dirty.apply(k, v).with_context(|| format!("replay block {h}"))?;
            }
            rows += r.ws.len();
        }
        if head_h > rolled_h {
            let root = dirty.root().context("recovery root")?;
            let want = header_at(head_h)?.root;
            if root != want.0 {
                eprintln!("epochdb-rs: recovery: state rebuilt through height {head_h} has root {}, header {want}", B256::from(root));
                std::process::exit(1);
            }
        }
        for h in head_h.saturating_sub(256)..=head_h {
            let id = if h == 0 { genesis.hash } else { B256::from(store.id_at(h).unwrap()) };
            be.set_block_hash(h, id);
        }
        let head = if head_h == 0 {
            genesis.clone()
        } else {
            Arc::new(block::decode_container(store.container(head_h)?.unwrap()).map_err(|e| anyhow!("head: {e}"))?)
        };
        eprintln!(
            "epochdb-rs: recovered: rolled at {rolled_h} (gen {gen}), head {head_h} {}, rows replayed {rows}, root ok, in {:.0} ms",
            head.hash,
            t0.elapsed().as_secs_f64() * 1e3
        );

        let dirty = Arc::new(Mutex::new(dirty));
        let roller = Roller::new(dir, gen, dirty.clone(), cpus);
        let ex = Executor::open(cfg.clone(), Layered::new(be));
        let (blocks_fd, code_fd) = (store.dup()?, code_log.dup()?);
        let store: Arc<Mutex<Box<dyn BlockStore>>> = Arc::new(Mutex::new(Box::new(store)));
        let code_log = Arc::new(Mutex::new(code_log));
        let recent = Arc::new(Mutex::new(HashMap::new()));
        let stats = Arc::new(Stats::default());
        let (check_tx, check_rx) = sync_channel::<Msg>(CHECK_DEPTH);
        let (flush_tx, flush_rx) = sync_channel::<()>(1);
        // The flusher fsyncs through its own handles so the store lock stays free.
        std::thread::spawn(move || {
            for _ in flush_rx {
                if let Err(e) = blocks_fd.sync_data().and_then(|_| code_fd.sync_data()) {
                    eprintln!("epochdb-rs: store fsync: {e}");
                    std::process::exit(1);
                }
            }
        });
        let checker = {
            let (dirty, store, code_log, recent, stats) = (dirty.clone(), store.clone(), code_log.clone(), recent.clone(), stats.clone());
            std::thread::spawn(move || checker(check_rx, dirty, store, code_log, recent, stats, flush_tx))
        };
        eprintln!(
            "epochdb-rs: chainId={} data={} roll-budget={}MB (tip {}MB) workers={workers} dirty-workers={cpus}",
            cfg.chain_id,
            init.chain_data_dir,
            sync_roll >> 20,
            tip_roll >> 20
        );
        let inner = Arc::new(Mutex::new(Inner { ex, roller, roll_budget: sync_roll }));
        let head = Arc::new(Mutex::new(head));
        let rpc_store = Arc::new(crate::rpc_store::PluginStore::new(genesis.clone(), head.clone(), inner.clone(), store.clone(), recent.clone()));
        let t1 = Instant::now();
        let ntx = rpc_store.build_index().context("tx index")?;
        eprintln!("epochdb-rs: rpc tx index: {ntx} txs in {:.0} ms", t1.elapsed().as_secs_f64() * 1e3);
        let chain_config = serde_json::from_slice::<serde_json::Value>(&init.genesis_bytes).ok().and_then(|g| g.get("config").cloned()).unwrap_or_default();
        let rpc = rpc::Server::new(rpc_store.clone(), Arc::new(cfg.clone()), genesis.clone(), chain_config);
        Ok(NodeEngine {
            chain_id: cfg.chain_id,
            genesis,
            inner,
            store,
            code_log,
            head,
            rpc_store,
            rpc,
            recent,
            parsed: Mutex::new(HashMap::new()),
            check_tx: Mutex::new(Some(check_tx)),
            checker: Mutex::new(Some(checker)),
            stats,
            pool: rayon::ThreadPoolBuilder::new().num_threads(workers).build()?,
            sync_roll,
            tip_roll,
            t0,
        })
    }

    /// A finished roll swaps in with the checker parked; `wait` blocks for a
    /// roll still running (shutdown).
    fn swap_roll(&self, inner: &mut Inner, wait: bool) -> anyhow::Result<()> {
        let Some(r) = inner.roller.poll_roll(wait)? else { return Ok(()) };
        let tx = self.check_tx.lock().unwrap().clone().ok_or_else(|| anyhow!("checker stopped"))?;
        let (ptx, prx) = sync_channel(0);
        let (rtx, rrx) = sync_channel(0);
        tx.send(Msg::Park(ptx, rrx)).map_err(|_| anyhow!("checker gone"))?;
        prx.recv().map_err(|_| anyhow!("checker gone"))?;
        let (store, code_log) = (self.store.clone(), self.code_log.clone());
        let res = inner.roller.finish_roll(&mut inner.ex.db_mut().backend, r, || {
            store.lock().unwrap().sync()?;
            code_log.lock().unwrap().sync()?;
            Ok(())
        });
        rtx.send(()).map_err(|_| anyhow!("checker gone"))?;
        res
    }

    pub fn meta_of(b: &Block) -> Meta {
        Meta { id: b.hash.0, parent: b.header.parent_hash.0, height: b.height, timestamp: b.header.time }
    }
}

impl Engine for NodeEngine {
    type Block = Arc<Block>;
    type Pending = Pending;

    fn parse(&self, bytes: Bytes) -> Result<Self::Block, Error> {
        let t0 = Instant::now();
        let r = self.parse_inner(bytes);
        tick(&self.stats.t_parse, t0);
        r
    }

    /// BatchedParseBlock: the batch decoded and recovered on the pool.
    fn parse_batch(&self, raws: Vec<Bytes>) -> Result<Vec<Self::Block>, Error> {
        let t0 = Instant::now();
        let r = self.pool.install(|| raws.into_par_iter().map(|r| self.parse_inner(r)).collect());
        tick(&self.stats.t_parse_batch, t0);
        r
    }

    fn meta(&self, b: &Self::Block) -> Meta {
        Self::meta_of(b)
    }

    fn bytes(&self, b: &Self::Block) -> Bytes {
        b.container.clone()
    }

    fn verify(&self, b: &Self::Block, parent: Option<&Arc<Pending>>, _pchain_height: Option<u64>) -> Result<Pending, Error> {
        let t0 = Instant::now();
        let r = self.verify_inner(b, parent);
        tick(&self.stats.t_verify, t0);
        r
    }

    fn accept(&self, b: &Self::Block, p: &Pending) -> Result<(), Error> {
        let t0 = Instant::now();
        let r = self.accept_inner(b, p);
        tick(&self.stats.t_accept, t0);
        r
    }

    fn last_accepted(&self) -> Self::Block {
        self.head.lock().unwrap().clone()
    }

    fn get_block(&self, id: &Id) -> Option<Self::Block> {
        if *id == self.genesis.hash.0 {
            return Some(self.genesis.clone());
        }
        if let Some(b) = self.recent.lock().unwrap().get(id) {
            return Some(b.clone());
        }
        let store = self.store.lock().unwrap();
        let h = store.height_of(id)?;
        let c = store.container(h).ok()??;
        drop(store);
        block::decode_container(c).ok().map(Arc::new)
    }

    fn block_id_at_height(&self, height: u64) -> Option<Id> {
        if height == 0 {
            return Some(self.genesis.hash.0);
        }
        let head = self.head.lock().unwrap().clone();
        if height > head.height {
            return None;
        }
        if height == head.height {
            return Some(head.hash.0);
        }
        if let Some(id) = self.store.lock().unwrap().id_at(height) {
            return Some(id);
        }
        self.recent.lock().unwrap().values().find(|b| b.height == height).map(|b| b.hash.0)
    }

    fn rpc(&self, body: &[u8]) -> Vec<u8> {
        crate::rpc::handle(self, body)
    }

    fn health(&self) -> Result<serde_json::Value, Error> {
        let h = self.head.lock().unwrap().height;
        Ok(serde_json::json!({"height": h, "root-checked": self.stats.checked.load(Ordering::Relaxed)}))
    }

    /// Bootstrapping = the catch-up budget; NormalOp = the tip budget and
    /// one roll of whatever the overlay holds (tickBudget on the switch).
    fn set_state(&self, normal: bool) {
        let mut g = self.inner.lock().unwrap();
        let inner = &mut *g;
        inner.roll_budget = if normal { self.tip_roll } else { self.sync_roll };
        eprintln!("epochdb-rs: budget {}: roll-budget={}MB", if normal { "tip" } else { "catch-up" }, inner.roll_budget >> 20);
        if normal {
            let head = self.head.lock().unwrap().clone();
            let be = &mut inner.ex.db_mut().backend;
            if !be.overlay.is_empty() {
                inner.roller.maybe_roll(be, 0, head.height, head.header.root);
            }
        }
    }

    /// A roll in flight is finished and swapped in, the checker drains and
    /// the store is synced; the counters go to stderr.
    fn shutdown(&self) {
        let mut g = self.inner.lock().unwrap();
        let inner = &mut *g;
        if inner.roller.rolling() {
            eprintln!("epochdb-rs: shutdown: waiting for the roll");
            if let Err(e) = self.swap_roll(inner, true) {
                eprintln!("epochdb-rs: shutdown: roll: {e:#}");
            }
        }
        drop(self.check_tx.lock().unwrap().take());
        if let Some(j) = self.checker.lock().unwrap().take() {
            match j.join() {
                Ok(Err(e)) => eprintln!("epochdb-rs: checker: {e:#}"),
                Err(_) => eprintln!("epochdb-rs: checker panicked"),
                Ok(Ok(())) => {}
            }
        }
        let ex = &inner.ex;
        let s = &self.stats;
        let (blocks, txs, gas, checked) = (s.executed.load(Ordering::Relaxed), s.txs.load(Ordering::Relaxed), s.gas.load(Ordering::Relaxed), s.checked.load(Ordering::Relaxed));
        let busy = ex.t_evm + ex.t_trace + ex.t_commit;
        let secs = |a: &AtomicU64| a.load(Ordering::Relaxed) as f64 / 1e9;
        eprintln!(
            "epochdb-rs: exit: blocks={blocks} txs={txs} gas={gas} root-checked={checked} rolls={} head={} | evm={:.2}s trace={:.2}s commit={:.2}s exec-thread {:.1} mgas/s | parse-batch={:.2}s parse={:.2}s verify={:.2}s accept={:.2}s checker={:.2}s | uptime {:.0}s",
            inner.roller.rolls,
            self.head.lock().unwrap().height,
            ex.t_evm.as_secs_f64(),
            ex.t_trace.as_secs_f64(),
            ex.t_commit.as_secs_f64(),
            if busy.is_zero() { 0.0 } else { gas as f64 / busy.as_secs_f64() / 1e6 },
            secs(&s.t_parse_batch),
            secs(&s.t_parse),
            secs(&s.t_verify),
            secs(&s.t_accept),
            secs(&s.t_check),
            self.t0.elapsed().as_secs_f64()
        );
    }
}

impl NodeEngine {
    fn parse_inner(&self, bytes: Bytes) -> Result<Arc<Block>, Error> {
        let mut b = block::decode_container(bytes)?;
        if let Some(c) = self.parsed.lock().unwrap().get(&b.hash.0) {
            return Ok(c.clone());
        }
        for t in &mut b.txs {
            t.sender = block::recover(t);
        }
        let b = Arc::new(b);
        let mut p = self.parsed.lock().unwrap();
        if p.len() >= PARSED_MAX {
            let head = self.head.lock().unwrap().height;
            p.retain(|_, x| x.height > head);
            if p.len() >= PARSED_MAX {
                p.clear();
            }
        }
        p.insert(b.hash.0, b.clone());
        Ok(b)
    }

    fn verify_inner(&self, b: &Arc<Block>, parent: Option<&Arc<Pending>>) -> Result<Pending, Error> {
        let mut g = self.inner.lock().unwrap();
        let (ph, pn, pt) = match parent {
            Some(p) => (p.hash, p.number, p.time),
            None => {
                let h = self.head.lock().unwrap();
                (h.hash, h.height, h.header.time)
            }
        };
        if b.header.parent_hash != ph || b.height != pn + 1 {
            return Err(format!("block {} {} parent {} does not follow {} {}", b.height, b.hash, b.header.parent_hash, pn, ph).into());
        }
        let inner = &mut *g;
        inner.ex.db_mut().begin(parent.cloned());
        let r = inner.ex.execute_block(b, pt).map_err(|e| format!("block {}: {e:#}", b.height))?;
        let h = &b.header;
        if r.gas_used != h.gas_used {
            return Err(format!("block {}: gasUsed {} != header {}", h.number, r.gas_used, h.gas_used).into());
        }
        if r.receipts_root != h.receipt_hash {
            return Err(format!("block {}: receiptsRoot {} != header {}", h.number, r.receipts_root, h.receipt_hash).into());
        }
        if r.bloom != h.bloom {
            return Err(format!("block {}: logsBloom differs from the header", h.number).into());
        }
        let mut receipts = Vec::new();
        let mut traces = Vec::with_capacity(r.txs.len());
        for t in r.txs {
            t.receipt.encode_2718(&mut receipts);
            traces.push(t.trace_json);
        }
        let txs = traces.len() as u64;
        Ok(inner.ex.db_mut().finish(b.height, b.hash, h.time, receipts, traces, r.gas_used, txs))
    }

    fn accept_inner(&self, b: &Arc<Block>, p: &Pending) -> Result<(), Error> {
        let mut g = self.inner.lock().unwrap();
        let inner = &mut *g;
        self.swap_roll(inner, false)?;
        let payload = p.payload.lock().unwrap().take().ok_or("block accepted twice")?;
        let be = &mut inner.ex.db_mut().backend;
        be.apply_ws(&payload.ws);
        for (h, c) in &p.code {
            be.code.insert(*h, c.clone());
        }
        be.set_block_hash(b.height, b.hash);
        *self.head.lock().unwrap() = b.clone();
        self.recent.lock().unwrap().insert(b.hash.0, b.clone());
        self.rpc_store.index_block(b);
        // From here the block's root is the header's or the checker dies.
        inner.roller.maybe_roll(be, inner.roll_budget, b.height, b.header.root);
        self.stats.executed.fetch_add(1, Ordering::Relaxed);
        self.stats.txs.fetch_add(payload.txs, Ordering::Relaxed);
        self.stats.gas.fetch_add(payload.gas_used, Ordering::Relaxed);
        let record = Record { height: b.height, id: b.hash.0, container: b.container.clone(), receipts: payload.receipts, traces: payload.traces, ws: payload.ws, code: payload.code };
        let tx = self.check_tx.lock().unwrap().clone().ok_or("checker stopped")?;
        tx.send(Msg::Block(Box::new(CheckItem { want: b.header.root, record }))).map_err(|_| "checker stopped")?;
        if b.height % 256 == 0 {
            self.parsed.lock().unwrap().retain(|_, x| x.height > b.height);
        }
        Ok(())
    }

}

pub fn id_hex(id: &Id) -> String {
    hex(id)
}

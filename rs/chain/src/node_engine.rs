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
//! write: the archival rows built from the block and its exec result), and
//! rolls by budget. Initialize opens the rolled pair the MANIFEST names and
//! replays the store's write sets since the roll (recover.go).
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use alloy_eips::eip2718::Encodable2718;
use store::window::BlockWrite;
use alloy_primitives::B256;
use anyhow::{anyhow, bail, Context as _};
use block::Block;
use bytes::Bytes;
use exec::{Config, Executor, StateDb};
use node::engine::{open_rolled, read_manifest, run_path, seek_fn, trie_path, user_data, write_manifest, Backend, Roller};
use rayon::prelude::*;
use revm::state::Bytecode;
use revm::Database;
use state::commit::dirty::{Dirty, Layer, Writes};
use state::commit::file::File;
use state::commit::roll::roll;
use state::view::{merge, View};

use rpc::genesis;
use crate::build;
use crate::layered::{Layered, Pending};
use alloy_primitives::{Address, U256};
use exec::exec::SkipReason;
use crate::dbstore::{BlockStore, DbStore, Record};
use crate::tree::{hex, Engine, Error, Id, Meta};


/// vmexec/budget.go: the overlay bytes that trigger a roll while catching
/// up and at the tip.
const SYNC_ROLL: usize = 2 << 30;
const TIP_ROLL: usize = 128 << 20;
/// The second roll trigger (`roll-every-blocks`, `roll-every-secs`): blocks or
/// seconds since the last roll, whichever comes first, in both states.
const ROLL_EVERY_BLOCKS: u64 = 500_000;
const ROLL_EVERY_SECS: u64 = 3600;
/// `shutdown-grace-secs`: how long Shutdown waits for a seal or merge in
/// flight before it abandons it (avalanchego kills the plugin at its own
/// deadline; the frozen log is re-sealed at the next open either way).
const SHUTDOWN_GRACE_SECS: u64 = 10;
/// How many accepted blocks may wait for their root check (checkDepth).
const CHECK_DEPTH: usize = 4;
/// The store's group-fsync cadence in blocks (flushEvery).
const FLUSH_EVERY: u64 = 256;
/// Parsed blocks kept by id (the host re-parses in Verify).
const PARSED_MAX: usize = 8192;

struct CheckItem {
    block: Arc<Block>,
    payload: crate::layered::Payload,
    /// The root was computed and checked before accept (NormalOp): the
    /// checker only renders the traces and writes the store.
    root_done: bool,
}

enum Msg {
    Block(Box<CheckItem>),
    /// A sync item: the checker reports parked, then waits to be resumed.
    Park(SyncSender<()>, Receiver<()>),
}

/// Everything Initialize hands the engine (the snow context and the bytes).
pub struct Init {
    pub network_id: u32,
    pub subnet_id: Id,
    pub chain_id: Id,
    pub chain_data_dir: String,
    pub genesis_bytes: Vec<u8>,
    pub upgrade_bytes: Vec<u8>,
    pub config_bytes: Vec<u8>,
}

/// What `NodeEngine::build` returns: the block, its pending state (verified),
/// which candidates went in and why the rest did not.
pub struct BuildOut {
    pub block: Arc<Block>,
    pub pending: Pending,
    pub included: Vec<usize>,
    pub reasons: Vec<SkipReason>,
    /// The gas limit is not filled and every candidate was considered.
    pub needs_more: bool,
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
    /// Nanoseconds of the inline root (Dirty::layer_root) inside verify / build.
    pub t_root: AtomicU64,
}

fn tick(a: &AtomicU64, t0: Instant) {
    a.fetch_add(t0.elapsed().as_nanos() as u64, Ordering::Relaxed);
}

pub struct NodeEngine {
    pub chain_id: u64,
    pub genesis: Arc<Block>,
    pub inner: Arc<Mutex<Inner>>,
    pub store: Arc<Mutex<Box<dyn BlockStore>>>,
    /// The store's read API, shared with the RPC threads and the block
    /// lookups below: reads never take the writer's mutex.
    pub db: Arc<store::db::DB>,
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
    /// NormalOp: the state root is computed inside verify (and build) on a
    /// layer over the accepted Dirty, which then holds exactly the head's
    /// state (the checker no longer touches it).
    normal: AtomicBool,
    /// `min-delay-target` (ms, the subnet-evm config key): the ACP-226 delay
    /// excess a built block moves toward; None keeps the parent's.
    pub desired_delay_excess: Option<u64>,
}

/// The checker thread: Dirty apply + root per block (a mismatch kills the
/// process), then the store's rows (BlockWrite::from_exec + the record) and
/// the append, fsync every FLUSH_EVERY blocks on a flusher thread.
fn checker(rx: Receiver<Msg>, dirty: Arc<Mutex<Dirty>>, store: Arc<Mutex<Box<dyn BlockStore>>>, recent: Arc<Mutex<HashMap<Id, Arc<Block>>>>, stats: Arc<Stats>, flush: SyncSender<()>) -> anyhow::Result<()> {
    for msg in rx {
        let it = match msg {
            Msg::Park(parked, resume) => {
                let _ = parked.send(());
                let _ = resume.recv();
                continue;
            }
            Msg::Block(it) => it,
        };
        let CheckItem { block: b, mut payload, root_done } = *it;
        let h = b.height;
        let t0 = Instant::now();
        if !root_done {
            let mut d = dirty.lock().unwrap();
            let root = if payload.ws.is_empty() {
                d.current_root()
            } else {
                for (k, v) in &payload.ws {
                    d.apply(k, v).with_context(|| format!("block {h}: apply write set"))?;
                }
                d.root().with_context(|| format!("block {h}: state root"))?
            };
            if root != b.header.root.0 {
                eprintln!("epochdb-rs: block {h}: state root mismatch: computed {}, header {}", B256::from(root), b.header.root);
                std::process::exit(1);
            }
        }
        exec::exec::render_deferred(&mut payload.result).with_context(|| format!("block {h}: callTracer render"))?;
        let rows = BlockWrite::from_exec(&b, &payload.result).with_context(|| format!("block {h}: store rows"))?;
        let mut receipts = Vec::new();
        for t in &payload.result.txs {
            t.receipt.encode_2718(&mut receipts);
        }
        let record = Record { height: h, id: b.hash.0, container: b.container.clone(), receipts, traces: Vec::new(), ws: payload.ws, code: payload.code, rows: Some(rows) };
        store.lock().unwrap().append(record).with_context(|| format!("block {h}: store append"))?;
        recent.lock().unwrap().remove(&b.hash.0);
        stats.checked.fetch_add(1, Ordering::Relaxed);
        tick(&stats.t_check, t0);
        if h % FLUSH_EVERY == 0 {
            let _ = flush.try_send(()); // flusher busy: coalesce into the next multiple
        }
    }
    let mut s = store.lock().unwrap();
    s.sync()?;
    s.close()?;
    Ok(())
}

impl NodeEngine {
    pub fn open(init: &Init) -> Result<NodeEngine, Error> {
        Self::open_inner(init).map_err(|e| format!("{e:#}").into())
    }

    /// The executor's config from Initialize: genesis + upgrade bytes under the
    /// network's schedule, and the snow context's ids: warp's getBlockchainID
    /// answers chain_id and predicate verification signs over subnet_id;
    /// without them the executor runs with zeros (state root mismatch at beam
    /// 3,423,561, a constructor storing getBlockchainID).
    pub fn exec_config(init: &Init) -> anyhow::Result<Config> {
        Ok(Config::from_genesis(&init.genesis_bytes, &init.upgrade_bytes, init.network_id)
            .context("config")?
            .with_chain(B256::from(init.chain_id), B256::from(init.subnet_id)))
    }

    fn open_inner(init: &Init) -> anyhow::Result<NodeEngine> {
        let t0 = Instant::now();
        let cfg = Self::exec_config(init)?;
        let genesis = Arc::new(genesis::block(&cfg, &init.genesis_bytes).map_err(|e| anyhow!("genesis: {e}"))?);
        let conf: serde_json::Value = serde_json::from_slice(&init.config_bytes).unwrap_or(serde_json::Value::Null);
        // Store settings ride in the config bytes (the env filter, see config.rs); into the env before DbStore::open.
        let applied = crate::config::apply(&init.config_bytes);
        if !applied.is_empty() {
            eprintln!("epochdb-rs: config keys applied: {}", applied.join(" "));
        }
        let sync_roll = conf_u64(&conf, "roll-budget-mb").map(|m| (m as usize) << 20).unwrap_or(SYNC_ROLL);
        let tip_roll = TIP_ROLL.min(sync_roll);
        let every_blocks = conf_u64(&conf, "roll-every-blocks").unwrap_or(ROLL_EVERY_BLOCKS);
        let every_secs = conf_u64(&conf, "roll-every-secs").unwrap_or(ROLL_EVERY_SECS);
        let grace = std::time::Duration::from_secs(conf_u64(&conf, "shutdown-grace-secs").unwrap_or(SHUTDOWN_GRACE_SECS));
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
                let p = ex.db_mut().finish(0, genesis.hash, 0, exec::BlockResult { gas_used: 0, receipts_root: Default::default(), bloom: Default::default(), txs: Vec::new(), tail: Vec::new(), code: Vec::new() });
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

        let chain_root: [u8; 32] = <sha2::Sha256 as sha2::Digest>::digest(&init.genesis_bytes).into();
        let store = DbStore::open(&data.join("store"), chain_root, grace).context("open the store")?;
        let db = store.db.clone();
        let window_max_bytes = db.flush_bytes;
        let mut ncode = 0usize;
        db.each_code(|h, c| {
            be.code.insert(B256::from(*h), Bytecode::new_raw(alloy_primitives::Bytes::copy_from_slice(c)));
            ncode += 1;
            Ok(())
        })
        .context("the store's code table")?;
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
        // Code deployed since the roll came with the write sets above through
        // the code/ rows, so nothing else to load.
        for h in head_h.saturating_sub(256)..=head_h {
            let id = if h == 0 { genesis.hash } else { B256::from(store.id_at(h).unwrap()) };
            be.set_block_hash(h, id);
        }
        let head = if head_h == 0 {
            genesis.clone()
        } else {
            let mut b = block::decode_container(store.container(head_h)?.unwrap()).map_err(|e| anyhow!("head: {e}"))?;
            for t in &mut b.txs {
                t.sender = block::recover(t);
            }
            Arc::new(b)
        };
        eprintln!(
            "epochdb-rs: recovered: rolled at {rolled_h} (gen {gen}), head {head_h} {}, rows replayed {rows}, {ncode} code blobs, {} runs, root ok, in {:.0} ms",
            head.hash,
            db.manifest().runs.len(),
            t0.elapsed().as_secs_f64() * 1e3
        );

        let dirty = Arc::new(Mutex::new(dirty));
        let mut roller = Roller::new(dir, gen, rolled_h, dirty.clone(), cpus);
        roller.every_blocks = if every_blocks == 0 { u64::MAX } else { every_blocks };
        roller.every = if every_secs == 0 { std::time::Duration::MAX } else { std::time::Duration::from_secs(every_secs) };
        let mut ex = Executor::open(cfg.clone(), Layered::new(be));
        // The callTracer JSON is rendered on the checker thread, off the verify path.
        ex.defer_call_trace = true;
        let db_reads = db.clone();
        let store: Arc<Mutex<Box<dyn BlockStore>>> = Arc::new(Mutex::new(Box::new(store)));
        let recent = Arc::new(Mutex::new(HashMap::new()));
        let stats = Arc::new(Stats::default());
        let (check_tx, check_rx) = sync_channel::<Msg>(CHECK_DEPTH);
        let (flush_tx, flush_rx) = sync_channel::<()>(1);
        // The flusher fsyncs the window log off the checker (the DB flushes
        // its buffer under the window lock and fsyncs through a second handle).
        std::thread::spawn(move || {
            for _ in flush_rx {
                if let Err(e) = db.sync() {
                    eprintln!("epochdb-rs: store fsync: {e:#}");
                    std::process::exit(1);
                }
            }
        });
        let checker = {
            let (dirty, store, recent, stats) = (dirty.clone(), store.clone(), recent.clone(), stats.clone());
            std::thread::spawn(move || checker(check_rx, dirty, store, recent, stats, flush_tx))
        };
        eprintln!(
            "epochdb-rs: chainId={} data={} roll-budget={}MB (tip {}MB) roll-every={every_blocks} blocks / {every_secs}s window-max-bytes={} shutdown-grace={}s workers={workers} dirty-workers={cpus}",
            cfg.chain_id,
            init.chain_data_dir,
            sync_roll >> 20,
            tip_roll >> 20,
            window_max_bytes,
            grace.as_secs()
        );
        let inner = Arc::new(Mutex::new(Inner { ex, roller, roll_budget: sync_roll }));
        let head = Arc::new(Mutex::new(head));
        let rpc_store = Arc::new(crate::rpc_store::PluginStore::new(genesis.clone(), head.clone(), inner.clone(), db_reads.clone(), recent.clone(), Arc::new(cfg.clone())));
        let chain_config = serde_json::from_slice::<serde_json::Value>(&init.genesis_bytes).ok().and_then(|g| g.get("config").cloned()).unwrap_or_default();
        let upgrades = serde_json::from_slice::<serde_json::Value>(&init.upgrade_bytes).ok();
        let rpc = rpc::Server::new(rpc_store.clone(), Arc::new(cfg.clone()), genesis.clone(), chain_config, upgrades);
        Ok(NodeEngine {
            chain_id: cfg.chain_id,
            genesis,
            inner,
            store,
            db: db_reads,
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
            normal: AtomicBool::new(false),
            desired_delay_excess: conf_u64(&conf, "min-delay-target").map(build::desired_delay_excess),
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
        let store = self.store.clone();
        let res = inner.roller.finish_roll(&mut inner.ex.db_mut().backend, r, || {
            store.lock().unwrap().sync()?;
            Ok(())
        });
        rtx.send(()).map_err(|_| anyhow!("checker gone"))?;
        res?;
        if self.normal.load(Ordering::Relaxed) {
            self.flush_dirty(inner)?;
        }
        Ok(())
    }

    /// NormalOp keeps the accepted Dirty at exactly the head's state: a roll
    /// rebuilt it and queued the overlay's rows (finish_roll), so the queue
    /// is applied here and the root must be the head's.
    fn flush_dirty(&self, inner: &mut Inner) -> anyhow::Result<()> {
        let head = self.head.lock().unwrap().clone();
        let mut d = inner.roller.dirty.lock().unwrap();
        let root = d.root().context("dirty flush")?;
        if root != head.header.root.0 {
            eprintln!("epochdb-rs: dirty state at head {} has root {}, header {}", head.height, B256::from(root), head.header.root);
            std::process::exit(1);
        }
        Ok(())
    }

    /// Drains the checker thread (every accepted block's root checked and
    /// stored) and returns with it parked until `resume` is dropped.
    fn park_checker(&self) -> anyhow::Result<SyncSender<()>> {
        let tx = self.check_tx.lock().unwrap().clone().ok_or_else(|| anyhow!("checker stopped"))?;
        let (ptx, prx) = sync_channel(0);
        let (rtx, rrx) = sync_channel(0);
        tx.send(Msg::Park(ptx, rrx)).map_err(|_| anyhow!("checker gone"))?;
        prx.recv().map_err(|_| anyhow!("checker gone"))?;
        Ok(rtx)
    }

    /// The state root of `ws` on top of `parent` (its pending layers, then
    /// the accepted Dirty), as a layer.
    fn layer_for(&self, inner: &Inner, parent: Option<&Arc<Pending>>, ws: &[(Vec<u8>, Vec<u8>)]) -> anyhow::Result<Layer> {
        let t0 = Instant::now();
        let mut w = Writes::default();
        for (k, v) in ws {
            w.apply(k, v)?;
        }
        let parents: Vec<&Layer> = match parent {
            Some(p) => p.layers().ok_or_else(|| anyhow!("parent {} was verified without a state root", p.number))?,
            None => Vec::new(),
        };
        let d = inner.roller.dirty.lock().unwrap();
        let l = d.layer_root(&parents, w)?;
        tick(&self.stats.t_root, t0);
        Ok(l)
    }

    pub fn is_normal(&self) -> bool {
        self.normal.load(Ordering::Relaxed)
    }

    /// A block parsed earlier, by id (the ABI's verify-by-id).
    pub fn parsed(&self, id: &Id) -> Option<Arc<Block>> {
        self.parsed.lock().unwrap().get(id).cloned()
    }

    /// nonce and balance of `addrs` at `parent`'s state (None: the accepted head).
    pub fn accounts(&self, parent: Option<&Arc<Pending>>, addrs: &[Address]) -> Vec<(u64, U256)> {
        let mut g = self.inner.lock().unwrap();
        let be = &mut g.ex.db_mut().backend;
        addrs.iter().map(|a| Pending::account(parent.map(|p| &**p), be, *a).map_or((0, U256::ZERO), |i| (i.nonce, i.balance))).collect()
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
        let h = self.db.height_by_hash(id).ok()??;
        let c = self.db.container_at(h).ok()??;
        block::decode_container(Bytes::from(c)).ok().map(Arc::new)
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
        if let Some(hdr) = self.db.header_rlp(height).ok().flatten() {
            return Some(state::keccak::keccak256(&hdr));
        }
        self.recent.lock().unwrap().values().find(|b| b.height == height).map(|b| b.hash.0)
    }

    fn rpc(&self, body: &[u8]) -> Vec<u8> {
        self.rpc.handle(body)
    }
    fn ws_server(&self) -> Option<&rpc::Server> {
        Some(&self.rpc)
    }

    fn health(&self) -> Result<serde_json::Value, Error> {
        let h = self.head.lock().unwrap().height;
        Ok(serde_json::json!({"height": h, "root-checked": self.stats.checked.load(Ordering::Relaxed), "normal-op": self.is_normal()}))
    }

    /// Bootstrapping = the catch-up budget; NormalOp = the tip budget and
    /// one roll of whatever the overlay holds (tickBudget on the switch).
    fn set_state(&self, normal: bool) {
        let mut g = self.inner.lock().unwrap();
        let inner = &mut *g;
        inner.roll_budget = if normal { self.tip_roll } else { self.sync_roll };
        eprintln!("epochdb-rs: budget {}: roll-budget={}MB", if normal { "tip" } else { "catch-up" }, inner.roll_budget >> 20);
        if normal && !self.normal.load(Ordering::Relaxed) {
            // Every accepted block through the checker first: from here the
            // Dirty is the head's state and verify computes the root itself.
            match self.park_checker() {
                Ok(resume) => {
                    if let Err(e) = self.flush_dirty(inner) {
                        eprintln!("epochdb-rs: set_state: {e:#}");
                    }
                    drop(resume);
                }
                Err(e) => eprintln!("epochdb-rs: set_state: {e:#}"),
            }
            self.normal.store(true, Ordering::Relaxed);
            eprintln!("epochdb-rs: NormalOp: state root inside verify");
        } else if !normal {
            self.normal.store(false, Ordering::Relaxed);
        }
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
            "epochdb-rs: exit: blocks={blocks} txs={txs} gas={gas} root-checked={checked} rolls={} head={} | evm={:.2}s trace={:.2}s commit={:.2}s exec-thread {:.1} mgas/s | parse-batch={:.2}s parse={:.2}s verify={:.2}s (root {:.2}s) accept={:.2}s checker={:.2}s | uptime {:.0}s",
            inner.roller.rolls,
            self.head.lock().unwrap().height,
            ex.t_evm.as_secs_f64(),
            ex.t_trace.as_secs_f64(),
            ex.t_commit.as_secs_f64(),
            if busy.is_zero() { 0.0 } else { gas as f64 / busy.as_secs_f64() / 1e6 },
            secs(&s.t_parse_batch),
            secs(&s.t_parse),
            secs(&s.t_verify),
            secs(&s.t_root),
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
        let mut p = inner.ex.db_mut().finish(b.height, b.hash, h.time, r);
        p.root = h.root;
        // NormalOp: the root now, on a layer over the parent's (bootstrapping
        // leaves it to the checker, one block behind; a parent verified that
        // way has no layer, so its children follow the checker path too).
        if self.normal.load(Ordering::Relaxed) && parent.is_none_or(|pp| pp.layer.is_some()) {
            let layer = {
                let g = p.payload.lock().unwrap();
                self.layer_for(inner, parent, &g.as_ref().unwrap().ws).map_err(|e| format!("block {}: state root: {e:#}", h.number))?
            };
            if layer.root != h.root.0 {
                return Err(format!("block {}: state root mismatch: computed {}, header {}", h.number, B256::from(layer.root), h.root).into());
            }
            p.layer = Some(Arc::new(layer));
        }
        Ok(p)
    }

    /// epochdb_build: the miner's block on top of `parent` (None: the accepted
    /// head, whose header is `parent_hdr`) from `candidates` in the caller's
    /// order; the result is a verified block (its Pending carries the root).
    /// `pchain_height` is the proposervm context height for the predicates
    /// (own height from Etna, the epoch's under Granite).
    pub fn build(&self, parent: Option<&Arc<Pending>>, parent_hdr: &block::Header, params: &build::Params, pchain_height: Option<u64>, mut candidates: Vec<block::Tx>) -> Result<BuildOut, Error> {
        if !self.normal.load(Ordering::Relaxed) {
            return Err("build needs NormalOp (SetState 2)".into());
        }
        let t0 = Instant::now();
        let mut g = self.inner.lock().unwrap();
        let inner = &mut *g;
        // The fee config and the coinbase rule as the parent's state holds them.
        inner.ex.db_mut().begin(parent.cloned());
        let (cfg, db) = inner.ex.cfg_and_db();
        let fc = build::fee_config_at(cfg, parent_hdr.time, |slot| db.storage(exec::precompile::FEE_MANAGER, slot).unwrap());
        let rule = build::coinbase_rule(cfg, parent_hdr.time, || db.storage(exec::precompile::REWARD_MANAGER, exec::rewardmanager::reward_address_slot()).unwrap());
        let h = build::template(cfg, &fc, parent_hdr, params, rule)?;
        for t in &mut candidates {
            if t.sender.is_none() {
                t.sender = block::recover(t);
            }
        }
        let r = match inner.ex.build_block(&h, parent_hdr.time, pchain_height, pchain_height, &candidates) {
            Ok(r) => r,
            Err(e) => {
                inner.ex.db_mut().begin(None);
                return Err(format!("build on {}: {e:#}", parent_hdr.number).into());
            }
        };
        let txs: Vec<&block::Tx> = r.included.iter().map(|&i| &candidates[i]).collect();
        let gas: Vec<u64> = r.result.txs.iter().map(|t| t.gas_used).collect();
        if let Err(e) = build::verify_block_fee(h.base_fee.unwrap(), h.block_gas_cost.unwrap_or_default(), &txs, &gas) {
            inner.ex.db_mut().begin(None);
            return Err(format!("build on {}: {e}", parent_hdr.number).into());
        }
        let needs_more = r.reasons.iter().all(|x| *x != SkipReason::NotReached) && h.gas_limit - r.result.gas_used >= exec::exec::TX_GAS;
        // finish takes cur out of the executor's Layered; the hash comes after the root.
        let mut p = inner.ex.db_mut().finish(h.number, B256::ZERO, h.time, r.result);
        let layer = {
            let g = p.payload.lock().unwrap();
            self.layer_for(inner, parent, &g.as_ref().unwrap().ws).map_err(|e| format!("build on {}: state root: {e:#}", parent_hdr.number))?
        };
        let result = p.payload.lock().unwrap().take().unwrap();
        let (hdr, header_rlp, bytes) = build::assemble(h, &txs, B256::from(layer.root), &result.result, &r.predicate_bytes)?;
        let hash = alloy_primitives::keccak256(&header_rlp);
        p.hash = hash;
        p.root = hdr.root;
        p.layer = Some(Arc::new(layer));
        *p.payload.lock().unwrap() = Some(result);
        let b = Arc::new(Block {
            height: hdr.number,
            hash,
            container_id: hash,
            header: hdr,
            header_rlp: Bytes::from(header_rlp),
            txs: txs.into_iter().cloned().collect(),
            container: Bytes::from(bytes),
            pvm: None,
        });
        self.parsed.lock().unwrap().insert(hash.0, b.clone());
        tick(&self.stats.t_verify, t0);
        Ok(BuildOut { block: b, pending: p, included: r.included, reasons: r.reasons, needs_more })
    }

    fn accept_inner(&self, b: &Arc<Block>, p: &Pending) -> Result<(), Error> {
        let mut g = self.inner.lock().unwrap();
        let inner = &mut *g;
        self.swap_roll(inner, false)?;
        let payload = p.payload.lock().unwrap().take().ok_or("block accepted twice")?;
        // NormalOp: the accepted Dirty takes the block's layer (or, for a block
        // verified while bootstrapping, its write set and root now), so the
        // next verify's layer sits on the head's state.
        let root_done = self.normal.load(Ordering::Relaxed);
        if root_done {
            let mut d = inner.roller.dirty.lock().unwrap();
            let root = match &p.layer {
                Some(l) => {
                    d.absorb(l).map_err(|e| format!("block {}: absorb: {e}", b.height))?;
                    l.root
                }
                None if payload.ws.is_empty() => d.current_root(),
                None => {
                    for (k, v) in &payload.ws {
                        d.apply(k, v).map_err(|e| format!("block {}: apply: {e}", b.height))?;
                    }
                    d.root().map_err(|e| format!("block {}: root: {e}", b.height))?
                }
            };
            if root != b.header.root.0 {
                eprintln!("epochdb-rs: block {}: state root mismatch at accept: computed {}, header {}", b.height, B256::from(root), b.header.root);
                std::process::exit(1);
            }
        }
        let be = &mut inner.ex.db_mut().backend;
        be.apply_ws(&payload.ws);
        for (h, c) in &p.code {
            be.code.insert(*h, c.clone());
        }
        be.set_block_hash(b.height, b.hash);
        *self.head.lock().unwrap() = b.clone();
        self.recent.lock().unwrap().insert(b.hash.0, b.clone());
        // From here the block's root is the header's or the checker dies.
        inner.roller.maybe_roll(be, inner.roll_budget, b.height, b.header.root);
        self.stats.executed.fetch_add(1, Ordering::Relaxed);
        self.stats.txs.fetch_add(payload.result.txs.len() as u64, Ordering::Relaxed);
        self.stats.gas.fetch_add(payload.result.gas_used, Ordering::Relaxed);
        if self.rpc.heads.receiver_count() > 0 {
            // /ws subscribers: the head goes out before the checker stores it.
            let mut receipts = Vec::new();
            for t in &payload.result.txs {
                t.receipt.encode_2718(&mut receipts);
            }
            self.rpc.publish(b.clone(), &receipts);
        }
        let tx = self.check_tx.lock().unwrap().clone().ok_or("checker stopped")?;
        tx.send(Msg::Block(Box::new(CheckItem { block: b.clone(), payload, root_done }))).map_err(|_| "checker stopped")?;
        if b.height % 256 == 0 {
            self.parsed.lock().unwrap().retain(|_, x| x.height > b.height);
        }
        Ok(())
    }

}

pub fn id_hex(id: &Id) -> String {
    hex(id)
}

/// A numeric config value: a JSON number, or a string holding one (the hosts
/// merge `EPOCHDB_*` variables in as strings).
fn conf_u64(conf: &serde_json::Value, key: &str) -> Option<u64> {
    match conf.get(key)? {
        serde_json::Value::Number(n) => n.as_u64(),
        serde_json::Value::String(s) => s.trim().parse().ok(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    /// The Config the plugin runs the executor with carries the snow context's
    /// ids (beam 3,423,561: a constructor stored getBlockchainID()).
    #[test]
    fn exec_config_carries_the_snow_context_ids() {
        let init = Init {
            network_id: 1,
            subnet_id: [0x5bu8; 32],
            chain_id: [0xc4u8; 32],
            chain_data_dir: String::new(),
            genesis_bytes: br#"{"config":{"chainId":4337,"feeConfig":{"gasLimit":8000000,"minBaseFee":25000000000,"targetGas":15000000,"baseFeeChangeDenominator":36,"minBlockGasCost":0,"maxBlockGasCost":1000000,"targetBlockRate":2,"blockGasCostStep":200000},"warpConfig":{"blockTimestamp":0}},"alloc":{},"timestamp":"0x0"}"#.to_vec(),
            upgrade_bytes: b"{}".to_vec(),
            config_bytes: b"{}".to_vec(),
        };
        let cfg = NodeEngine::exec_config(&init).unwrap();
        assert_eq!(cfg.chain_id, 4337);
        assert_eq!(cfg.blockchain_id, B256::from(init.chain_id));
        assert_eq!(cfg.subnet_id, B256::from(init.subnet_id));
        assert_eq!(cfg.network_id, 1);
    }

    #[test]
    fn conf_u64_reads_numbers_and_strings() {
        let c: serde_json::Value = serde_json::from_str(r#"{"roll-every-blocks":200000,"roll-budget-mb":"8","roll-every-secs":true}"#).unwrap();
        assert_eq!(conf_u64(&c, "roll-every-blocks"), Some(200000));
        assert_eq!(conf_u64(&c, "roll-budget-mb"), Some(8));
        assert_eq!(conf_u64(&c, "roll-every-secs"), None);
        assert_eq!(conf_u64(&c, "missing"), None);
    }
}

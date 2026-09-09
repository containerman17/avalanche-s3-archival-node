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
//!
//! `state-engine: "firewood"` swaps the flat state for rs/node's Firewood
//! engine: verify executes on `Firewood` (the same pending chain, layers
//! instead of overlays), accept hands the block's layer to the checker,
//! which proposes it (Firewood hashes, the proposal's root is the block's),
//! and commits the proposal chain every FW_COMMIT_EVERY blocks right after
//! the store's fsync, so Firewood's persisted revision never runs ahead of
//! the store. No roll: Firewood's node store and revisions replace run /
//! trie / MANIFEST; recovery finds the persisted root's height among the
//! store's headers and replays the write sets since.
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
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
use node::firewood::{self, height_of_root, Committer, Firewood, Layer};
use rayon::prelude::*;
use revm::state::Bytecode;
use state::commit::dirty::Dirty;
use state::commit::file::File;
use state::commit::roll::roll;
use state::view::{merge, View};

use rpc::genesis;
use crate::layered::{Layered, Payload};
use crate::dbstore::{BlockStore, DbStore, Record};
use crate::tree::{hex, Engine, Error, Id, Meta};
use crate::vm::Init;

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
/// Firewood: proposals accumulate as a chain and commit after the store's
/// fsync every this many blocks (the chain depth bounds a proposal read).
const FW_COMMIT_EVERY: u64 = 32;

struct CheckItem {
    block: Arc<Block>,
    payload: Payload,
    /// Firewood: the block's layer (its ops).
    layer: Option<Arc<Layer>>,
}

/// A verified block's state on top of its parent's, for either engine.
pub enum Pending {
    Native(Arc<crate::layered::Pending>),
    Firewood { p: Arc<firewood::Pending>, payload: Mutex<Option<Payload>> },
}

impl Pending {
    fn take_payload(&self) -> Option<Payload> {
        match self {
            Pending::Native(p) => p.payload.lock().unwrap().take(),
            Pending::Firewood { payload, .. } => payload.lock().unwrap().take(),
        }
    }
    fn meta(&self) -> (B256, u64, u64) {
        match self {
            Pending::Native(p) => (p.hash, p.number, p.time),
            Pending::Firewood { p, .. } => (p.hash, p.number, p.time),
        }
    }
}

/// The executor over one of the two state engines.
pub enum Ex {
    Native(Executor<Layered>),
    Firewood(Executor<Firewood>),
}

impl Ex {
    fn split(&self) -> (std::time::Duration, std::time::Duration, std::time::Duration) {
        match self {
            Ex::Native(ex) => (ex.t_evm, ex.t_trace, ex.t_commit),
            Ex::Firewood(ex) => (ex.t_evm, ex.t_trace, ex.t_commit),
        }
    }
}

enum Msg {
    Block(Box<CheckItem>),
    /// A sync item: the checker reports parked, then waits to be resumed.
    Park(SyncSender<()>, Receiver<()>),
}

pub struct Inner {
    pub ex: Ex,
    /// None under Firewood (no roll).
    roller: Option<Roller>,
    roll_budget: usize,
}

impl Inner {
    /// The accepted head's state (the RPC's `latest`): outside a verify, the
    /// executor's db reads the accepted state.
    pub fn head_account(&mut self, a: alloy_primitives::Address) -> Option<revm::state::AccountInfo> {
        use revm::Database;
        match &mut self.ex {
            Ex::Native(ex) => ex.db_mut().backend.basic(a).unwrap(),
            Ex::Firewood(ex) => ex.db_mut().basic(a).unwrap(),
        }
    }
    pub fn head_storage(&mut self, a: alloy_primitives::Address, slot: alloy_primitives::U256) -> alloy_primitives::U256 {
        use revm::Database;
        match &mut self.ex {
            Ex::Native(ex) => ex.db_mut().backend.storage(a, slot).unwrap(),
            Ex::Firewood(ex) => ex.db_mut().storage(a, slot).unwrap(),
        }
    }
    pub fn head_code(&mut self, h: B256) -> Option<Bytecode> {
        match &mut self.ex {
            Ex::Native(ex) => ex.db_mut().backend.code.get(&h).cloned(),
            Ex::Firewood(ex) => ex.db_mut().code.get(&h).cloned(),
        }
    }
}

/// The checker's root oracle: Dirty (native) or Firewood's proposals.
enum RootCheck {
    Dirty(Arc<Mutex<Dirty>>),
    Firewood(Committer),
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
    checker: Mutex<Option<std::thread::JoinHandle<anyhow::Result<Option<Committer>>>>>,
    pub stats: Arc<Stats>,
    pool: rayon::ThreadPool,
    sync_roll: usize,
    tip_roll: usize,
    t0: Instant,
}

/// The checker thread: Dirty apply + root per block (a mismatch kills the
/// process), then the store's rows (BlockWrite::from_exec + the record) and
/// the append, fsync every FLUSH_EVERY blocks on a flusher thread.
fn checker(rx: Receiver<Msg>, mut root_check: RootCheck, store: Arc<Mutex<Box<dyn BlockStore>>>, recent: Arc<Mutex<HashMap<Id, Arc<Block>>>>, stats: Arc<Stats>, flush: SyncSender<()>) -> anyhow::Result<Option<Committer>> {
    for msg in rx {
        let it = match msg {
            Msg::Park(parked, resume) => {
                let _ = parked.send(());
                let _ = resume.recv();
                continue;
            }
            Msg::Block(it) => it,
        };
        let CheckItem { block: b, payload, layer } = *it;
        let h = b.height;
        let t0 = Instant::now();
        let root = match &mut root_check {
            RootCheck::Dirty(dirty) => {
                let mut d = dirty.lock().unwrap();
                if payload.ws.is_empty() {
                    B256::from(d.current_root())
                } else {
                    for (k, v) in &payload.ws {
                        d.apply(k, v).with_context(|| format!("block {h}: apply write set"))?;
                    }
                    B256::from(d.root().with_context(|| format!("block {h}: state root"))?)
                }
            }
            RootCheck::Firewood(c) => c.propose(h, layer.as_ref().expect("firewood layer").ops())?,
        };
        if root != b.header.root {
            eprintln!("epochdb-rs: block {h}: state root mismatch: computed {root}, header {}", b.header.root);
            std::process::exit(1);
        }
        let rows = BlockWrite::from_exec(&b, &payload.result).with_context(|| format!("block {h}: store rows"))?;
        let mut receipts = Vec::new();
        for t in &payload.result.txs {
            t.receipt.encode_2718(&mut receipts);
        }
        let record = Record { height: h, id: b.hash.0, container: b.container.clone(), receipts, traces: Vec::new(), ws: payload.ws, code: payload.code, rows: Some(rows) };
        store.lock().unwrap().append(record).with_context(|| format!("block {h}: store append"))?;
        recent.lock().unwrap().remove(&b.hash.0);
        stats.checked.fetch_add(1, Ordering::Relaxed);
        match &mut root_check {
            RootCheck::Dirty(_) => {
                if h % FLUSH_EVERY == 0 {
                    let _ = flush.try_send(()); // flusher busy: coalesce into the next multiple
                }
            }
            RootCheck::Firewood(c) => {
                // The store's rows through h are durable before Firewood's
                // revision at h can be: sync here, then commit the chain.
                if h % FW_COMMIT_EVERY == 0 {
                    store.lock().unwrap().sync()?;
                    c.commit_all().with_context(|| format!("block {h}: firewood commit"))?;
                }
            }
        }
        tick(&stats.t_check, t0);
    }
    let mut s = store.lock().unwrap();
    s.sync()?;
    let c = match root_check {
        RootCheck::Dirty(_) => None,
        RootCheck::Firewood(mut c) => {
            c.commit_all().context("firewood commit at close")?;
            Some(c)
        }
    };
    s.close()?;
    Ok(c)
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
        let engine = conf.get("state-engine").and_then(|v| v.as_str()).unwrap_or("native").to_string();
        if engine == "firewood" {
            let opts = firewood::Opts { cache_bytes: conf_u64(&conf, "firewood-cache-mb").unwrap_or(192) as usize * 1_000_000, ..Default::default() };
            return Self::open_firewood(init, cfg, genesis, conf, opts, sync_roll, tip_roll, grace, workers, cpus, data, t0);
        } else if engine != "native" {
            bail!("state-engine {engine:?}: native or firewood");
        }

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
        let ex = Ex::Native(Executor::open(cfg.clone(), Layered::new(be)));
        eprintln!(
            "epochdb-rs: chainId={} data={} roll-budget={}MB (tip {}MB) roll-every={every_blocks} blocks / {every_secs}s window-max-bytes={} shutdown-grace={}s workers={workers} dirty-workers={cpus}",
            cfg.chain_id,
            init.chain_data_dir,
            sync_roll >> 20,
            tip_roll >> 20,
            window_max_bytes,
            grace.as_secs()
        );
        Self::finish_open(init, cfg, genesis, store, db, head, ex, Some(roller), RootCheck::Dirty(dirty), sync_roll, tip_roll, workers, t0)
    }

    /// The Firewood engine's open: genesis into the first proposal on an
    /// empty db, else the persisted root's height found among the store's
    /// headers and the write sets since replayed through proposals.
    #[allow(clippy::too_many_arguments)]
    fn open_firewood(init: &Init, cfg: Config, genesis: Arc<Block>, conf: serde_json::Value, opts: firewood::Opts, sync_roll: usize, tip_roll: usize, grace: std::time::Duration, workers: usize, cpus: usize, data: PathBuf, t0: Instant) -> anyhow::Result<NodeEngine> {
        let _ = cpus;
        let dir = data.join("vmstate");
        std::fs::create_dir_all(&dir)?;
        let mut committer = Committer::open(&dir, false, opts).context("firewood")?;
        let fw_root = committer.root();
        let kv_bytes = (conf_u64(&conf, "firewood-kv-cache-mb").unwrap_or(0) as usize) << 20;
        let mut fw = Firewood::new(committer.committed()).with_kv_cache(kv_bytes);
        let mut rolled_h = 0u64;
        if fw_root == firewood::EMPTY_ROOT {
            let mut ex = Executor::with_db(cfg.clone(), fw).context("genesis state")?;
            let db = ex.db_mut();
            db.take_ws();
            let layer = db.finish(0, genesis.hash, 0).layer;
            db.accept(0, layer.clone());
            let root = committer.propose(0, layer.ops())?;
            if root != genesis.header.root {
                bail!("genesis root mismatch: firewood {root}, header {}", genesis.header.root);
            }
            committer.commit_all()?;
            eprintln!("epochdb-rs: genesis state ok: root={root} hash={} accounts={} keys={} firewood={}B", genesis.hash, cfg.alloc.len(), layer.map.len(), Committer::disk_bytes(&dir));
            fw = Firewood::new(committer.committed()).with_kv_cache(kv_bytes);
            for (h, c) in &layer.code {
                fw.code.insert(*h, c.clone());
            }
            let _ = ex;
        }

        let chain_root: [u8; 32] = <sha2::Sha256 as sha2::Digest>::digest(&init.genesis_bytes).into();
        let store = DbStore::open(&data.join("store"), chain_root, grace).context("open the store")?;
        let db = store.db.clone();
        let mut ncode = 0usize;
        db.each_code(|h, c| {
            fw.code.insert(B256::from(*h), Bytecode::new_raw(alloy_primitives::Bytes::copy_from_slice(c)));
            ncode += 1;
            Ok(())
        })
        .context("the store's code table")?;
        let head_h = store.head();
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
        if fw_root != firewood::EMPTY_ROOT {
            rolled_h = height_of_root(fw_root, head_h, |h| Ok(header_at(h)?.root))?;
        }
        let mut rows = 0usize;
        for h in rolled_h + 1..=head_h {
            let r = store.read(h)?.ok_or_else(|| anyhow!("the store holds no block at {h}"))?;
            let layer = Layer::from_ws(&r.ws);
            let root = committer.propose(h, layer.ops()).with_context(|| format!("replay block {h}"))?;
            let want = header_at(h)?.root;
            if root != want {
                eprintln!("epochdb-rs: recovery: state rebuilt through height {h} has root {root}, header {want}");
                std::process::exit(1);
            }
            rows += r.ws.len();
            if h % FW_COMMIT_EVERY == 0 {
                committer.commit_all()?;
            }
        }
        committer.commit_all()?;
        fw = Firewood::new(committer.committed()).with_kv_cache(kv_bytes).with_code(fw.code);
        for h in head_h.saturating_sub(256)..=head_h {
            let id = if h == 0 { genesis.hash } else { B256::from(store.id_at(h).unwrap()) };
            fw.set_block_hash(h, id);
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
            "epochdb-rs: recovered: firewood at {rolled_h} (root {}), head {head_h} {}, rows replayed {rows}, {ncode} code blobs, {} runs, root ok, in {:.0} ms",
            committer.root(),
            head.hash,
            db.manifest().runs.len(),
            t0.elapsed().as_secs_f64() * 1e3
        );
        let ex = Ex::Firewood(Executor::open(cfg.clone(), fw));
        eprintln!(
            "epochdb-rs: chainId={} data={} state-engine=firewood cache={}MB revisions={} kv-cache={}MB commit-every={FW_COMMIT_EVERY} window-max-bytes={} shutdown-grace={}s workers={workers}",
            cfg.chain_id,
            init.chain_data_dir,
            opts.cache_bytes / 1_000_000,
            opts.revisions,
            kv_bytes >> 20,
            db.flush_bytes,
            grace.as_secs()
        );
        Self::finish_open(init, cfg, genesis, store, db, head, ex, None, RootCheck::Firewood(committer), sync_roll, tip_roll, workers, t0)
    }

    /// The threads and the shared handles, the same for both engines.
    #[allow(clippy::too_many_arguments)]
    fn finish_open(init: &Init, cfg: Config, genesis: Arc<Block>, store: DbStore, db: Arc<store::db::DB>, head: Arc<Block>, ex: Ex, roller: Option<Roller>, root_check: RootCheck, sync_roll: usize, tip_roll: usize, workers: usize, t0: Instant) -> anyhow::Result<NodeEngine> {
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
            let (store, recent, stats) = (store.clone(), recent.clone(), stats.clone());
            std::thread::spawn(move || checker(check_rx, root_check, store, recent, stats, flush_tx))
        };
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
        })
    }

    /// A finished roll swaps in with the checker parked; `wait` blocks for a
    /// roll still running (shutdown).
    fn swap_roll(&self, inner: &mut Inner, wait: bool) -> anyhow::Result<()> {
        let (Some(roller), Ex::Native(ex)) = (&mut inner.roller, &mut inner.ex) else { return Ok(()) };
        let Some(r) = roller.poll_roll(wait)? else { return Ok(()) };
        let tx = self.check_tx.lock().unwrap().clone().ok_or_else(|| anyhow!("checker stopped"))?;
        let (ptx, prx) = sync_channel(0);
        let (rtx, rrx) = sync_channel(0);
        tx.send(Msg::Park(ptx, rrx)).map_err(|_| anyhow!("checker gone"))?;
        prx.recv().map_err(|_| anyhow!("checker gone"))?;
        let store = self.store.clone();
        let res = roller.finish_roll(&mut ex.db_mut().backend, r, || {
            store.lock().unwrap().sync()?;
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
        crate::rpc::handle(self, body)
    }
    fn ws_server(&self) -> Option<&rpc::Server> {
        Some(&self.rpc)
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
        let (Some(roller), Ex::Native(ex)) = (&mut inner.roller, &mut inner.ex) else { return };
        eprintln!("epochdb-rs: budget {}: roll-budget={}MB", if normal { "tip" } else { "catch-up" }, inner.roll_budget >> 20);
        if normal {
            let head = self.head.lock().unwrap().clone();
            let be = &mut ex.db_mut().backend;
            if !be.overlay.is_empty() {
                roller.maybe_roll(be, 0, head.height, head.header.root);
            }
        }
    }

    /// A roll in flight is finished and swapped in, the checker drains and
    /// the store is synced; the counters go to stderr.
    fn shutdown(&self) {
        let mut g = self.inner.lock().unwrap();
        let inner = &mut *g;
        if inner.roller.as_ref().is_some_and(|r| r.rolling()) {
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
                Ok(Ok(Some(c))) => {
                    let t = Instant::now();
                    let root = c.root();
                    match c.close() {
                        Ok(()) => eprintln!("epochdb-rs: firewood closed: root={root} in {:.0} ms", t.elapsed().as_secs_f64() * 1e3),
                        Err(e) => eprintln!("epochdb-rs: firewood close: {e:#}"),
                    }
                }
                Ok(Ok(None)) => {}
            }
        }
        let (t_evm, t_trace, t_commit) = inner.ex.split();
        let s = &self.stats;
        let (blocks, txs, gas, checked) = (s.executed.load(Ordering::Relaxed), s.txs.load(Ordering::Relaxed), s.gas.load(Ordering::Relaxed), s.checked.load(Ordering::Relaxed));
        let busy = t_evm + t_trace + t_commit;
        let secs = |a: &AtomicU64| a.load(Ordering::Relaxed) as f64 / 1e9;
        eprintln!(
            "epochdb-rs: exit: blocks={blocks} txs={txs} gas={gas} root-checked={checked} rolls={} head={} | evm={:.2}s trace={:.2}s commit={:.2}s exec-thread {:.1} mgas/s | parse-batch={:.2}s parse={:.2}s verify={:.2}s accept={:.2}s checker={:.2}s | uptime {:.0}s",
            inner.roller.as_ref().map_or(0, |r| r.rolls),
            self.head.lock().unwrap().height,
            t_evm.as_secs_f64(),
            t_trace.as_secs_f64(),
            t_commit.as_secs_f64(),
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
            Some(p) => p.meta(),
            None => {
                let h = self.head.lock().unwrap();
                (h.hash, h.height, h.header.time)
            }
        };
        if b.header.parent_hash != ph || b.height != pn + 1 {
            return Err(format!("block {} {} parent {} does not follow {} {}", b.height, b.hash, b.header.parent_hash, pn, ph).into());
        }
        let inner = &mut *g;
        let h = &b.header;
        let check = |r: &exec::BlockResult| -> Result<(), Error> {
            if r.gas_used != h.gas_used {
                return Err(format!("block {}: gasUsed {} != header {}", h.number, r.gas_used, h.gas_used).into());
            }
            if r.receipts_root != h.receipt_hash {
                return Err(format!("block {}: receiptsRoot {} != header {}", h.number, r.receipts_root, h.receipt_hash).into());
            }
            if r.bloom != h.bloom {
                return Err(format!("block {}: logsBloom differs from the header", h.number).into());
            }
            Ok(())
        };
        match &mut inner.ex {
            Ex::Native(ex) => {
                let parent = parent.map(|p| match &**p {
                    Pending::Native(p) => p.clone(),
                    Pending::Firewood { .. } => unreachable!("firewood pending under the native engine"),
                });
                ex.db_mut().begin(parent);
                let r = ex.execute_block(b, pt).map_err(|e| format!("block {}: {e:#}", b.height))?;
                check(&r)?;
                Ok(Pending::Native(Arc::new(ex.db_mut().finish(b.height, b.hash, h.time, r))))
            }
            Ex::Firewood(ex) => {
                let parent = parent.map(|p| match &**p {
                    Pending::Firewood { p, .. } => p.clone(),
                    Pending::Native(_) => unreachable!("native pending under the firewood engine"),
                });
                ex.db_mut().begin(parent);
                let r = ex.execute_block(b, pt).map_err(|e| format!("block {}: {e:#}", b.height))?;
                check(&r)?;
                let db = ex.db_mut();
                let (ws, code) = db.take_ws();
                let p = Arc::new(db.finish(b.height, b.hash, h.time));
                Ok(Pending::Firewood { p, payload: Mutex::new(Some(Payload { ws, code, result: r })) })
            }
        }
    }

    fn accept_inner(&self, b: &Arc<Block>, p: &Pending) -> Result<(), Error> {
        let mut g = self.inner.lock().unwrap();
        let inner = &mut *g;
        self.swap_roll(inner, false)?;
        let payload = p.take_payload().ok_or("block accepted twice")?;
        let mut layer = None;
        match (&mut inner.ex, p) {
            (Ex::Native(ex), Pending::Native(p)) => {
                let be = &mut ex.db_mut().backend;
                be.apply_ws(&payload.ws);
                for (h, c) in &p.code {
                    be.code.insert(*h, c.clone());
                }
                be.set_block_hash(b.height, b.hash);
                // From here the block's root is the header's or the checker dies.
                if let Some(roller) = &mut inner.roller {
                    roller.maybe_roll(be, inner.roll_budget, b.height, b.header.root);
                }
            }
            (Ex::Firewood(ex), Pending::Firewood { p, .. }) => {
                let db = ex.db_mut();
                db.accept(b.height, p.layer.clone());
                db.set_block_hash(b.height, b.hash);
                layer = Some(p.layer.clone());
            }
            _ => unreachable!("pending block of the other engine"),
        }
        *self.head.lock().unwrap() = b.clone();
        self.recent.lock().unwrap().insert(b.hash.0, b.clone());
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
        tx.send(Msg::Block(Box::new(CheckItem { block: b.clone(), payload, layer }))).map_err(|_| "checker stopped")?;
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
    use crate::vm::Host;
    use tonic::transport::Channel;

    /// The Config the plugin runs the executor with carries the snow context's
    /// ids (beam 3,423,561: a constructor stored getBlockchainID()).
    #[tokio::test]
    async fn exec_config_carries_the_snow_context_ids() {
        let ch = Channel::from_static("http://127.0.0.1:1").connect_lazy();
        let init = Init {
            network_id: 1,
            subnet_id: [0x5bu8; 32],
            chain_id: [0xc4u8; 32],
            node_id: vec![],
            public_key: vec![],
            x_chain_id: [0; 32],
            c_chain_id: [0; 32],
            avax_asset_id: [0; 32],
            chain_data_dir: String::new(),
            genesis_bytes: br#"{"config":{"chainId":4337,"feeConfig":{"gasLimit":8000000,"minBaseFee":25000000000,"targetGas":15000000,"baseFeeChangeDenominator":36,"minBlockGasCost":0,"maxBlockGasCost":1000000,"targetBlockRate":2,"blockGasCostStep":200000},"warpConfig":{"blockTimestamp":0}},"alloc":{},"timestamp":"0x0"}"#.to_vec(),
            upgrade_bytes: b"{}".to_vec(),
            config_bytes: b"{}".to_vec(),
            host: Host { db: crate::pb::rpcdb::database_client::DatabaseClient::new(ch.clone()), validator_state: crate::pb::validatorstate::validator_state_client::ValidatorStateClient::new(ch) },
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

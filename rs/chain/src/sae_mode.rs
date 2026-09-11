//! SAE (ACP-194) mode for the plugin engine. Streaming Asynchronous Execution:
//! Verify is `verify_light` (no execution, independent of the parent EXECUTED
//! state, so the block tree pipelines to OptimalProcessing depth); Accept
//! advances the projection and enqueues the block to a continuous executor that
//! runs `k` blocks behind the accepted head; the executor settles off the vote
//! path (the state root, receipts, the store row) and BuildBlock commits the
//! settled root of `h-k`. See rs/SAE.md.
//!
//! Head divergence: `self.head` (and last_accepted) is the ACCEPTED head,
//! advanced by Accept without execution; the store's head and the executor's
//! state are the SETTLED head, `k`-ish blocks behind. RPC `latest` reads the
//! executor's settled state.
//!
//! The settled-root gate is a rendezvous: Accept records the root a block's
//! header commits for `h-k` (`expected[h-k]`), the executor records the root it
//! computes at settlement (`settled[h]`); whichever arrives second compares,
//! and a mismatch halts the process (the sync checker's `log.Fatal`). This is
//! the SAE analog of the synchronous root check, deferred by the settlement
//! window.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, SyncSender};
use std::sync::{Arc, Mutex};

use alloy_eips::eip2718::Encodable2718;
use alloy_primitives::{Address, Bloom, B256, U256};
use anyhow::{bail, Context as _};
use block::Block;
use exec::StateDb;
use node::engine::Backend;
use revm::Database;
use store::window::BlockWrite;

use crate::dbstore::{BlockStore, Record};
use crate::node_engine::{Ex, Inner, Stats};
use crate::tree::Id;
use node::sae::{Overlay, Projection};

/// A verified-but-not-accepted SAE block: only its projection delta (the
/// overlay of its unaccepted ancestor chain plus its own txs), never executed
/// state. A child's Verify layers on this. Dropped on reject.
pub struct SaePending {
    pub overlay: Overlay,
    pub hash: B256,
    pub number: u64,
    pub time: u64,
}

/// A settled block's own attestations: its post-execution state root, and its
/// OWN receipts root and logs bloom (the header at `h+k` commits these three).
#[derive(Clone, Copy)]
pub struct Settled {
    pub root: B256,
    pub receipts_root: B256,
    pub bloom: Bloom,
}

/// The settled-root rendezvous: what the executor has settled, and the roots
/// accepted headers commit for those heights. Bounded to a window around the
/// settled head.
#[derive(Default)]
struct Gate {
    settled: BTreeMap<u64, Settled>,
    expected: BTreeMap<u64, B256>,
}

/// SAE engine state. Present (`NodeEngine::sae` is Some) only in SAE mode.
pub struct Sae {
    pub proj: Mutex<Projection>,
    /// The settlement window: the header at `h` commits the settled root of
    /// `h-k`. `k = 0` is the synchronous model (the native path handles that).
    pub k: u64,
    /// ACP-194 block gas capacity (`20s x target`) and byte size cap; u64::MAX /
    /// usize::MAX = off (a permissive default; a real chain sets them).
    pub capacity: u64,
    pub size_cap: usize,
    gate: Mutex<Gate>,
    /// Accept -> the executor. `None` after shutdown.
    pub exec_tx: Mutex<Option<SyncSender<Arc<Block>>>>,
    pub exec_thread: Mutex<Option<std::thread::JoinHandle<anyhow::Result<()>>>>,
    pub settled_head: AtomicU64,
    pub settled_tx: AtomicU64,
    pub settled_gas: AtomicU64,
    pub settled_time: AtomicU64,
}

impl Sae {
    pub fn new(k: u64, capacity: u64, size_cap: usize, genesis_root: B256) -> Sae {
        let mut gate = Gate::default();
        gate.settled.insert(0, Settled { root: genesis_root, receipts_root: alloy_trie::EMPTY_ROOT_HASH, bloom: Bloom::default() });
        Sae {
            proj: Mutex::new(Projection::new()),
            k,
            capacity,
            size_cap,
            gate: Mutex::new(gate),
            exec_tx: Mutex::new(None),
            exec_thread: Mutex::new(None),
            settled_head: AtomicU64::new(0),
            settled_tx: AtomicU64::new(0),
            settled_gas: AtomicU64::new(0),
            settled_time: AtomicU64::new(0),
        }
    }

    /// The settled attestations at `h` (None: not settled yet). BuildBlock at
    /// `h` reads `h-k` for its committed root / receiptsRoot / logsBloom.
    pub fn settled(&self, h: u64) -> Option<Settled> {
        self.gate.lock().unwrap().settled.get(&h).copied()
    }

    /// The settled root at `h` (None: not settled yet).
    pub fn settled_root(&self, h: u64) -> Option<B256> {
        self.gate.lock().unwrap().settled.get(&h).map(|s| s.root)
    }

    /// Seed the settled root at a recovered height (the rebuilt state root at
    /// the store head on open; there is no header carrying it, so the replay is
    /// the source). Also advances the settled head. ponytail: the block's OWN
    /// receipts root / bloom are not recoverable from its header (the header
    /// commits h-k's), so a restart cannot build until the executor re-settles
    /// the window; a follower still verifies, accepts and settles. Re-executing
    /// the last k settled blocks on open would seed the full window.
    pub fn seed_settled(&self, h: u64, root: B256) {
        self.gate.lock().unwrap().settled.insert(h, Settled { root, receipts_root: alloy_trie::EMPTY_ROOT_HASH, bloom: Bloom::default() });
        self.settled_head.store(h, Ordering::Relaxed);
        self.settled_time.store(0, Ordering::Relaxed);
    }

    /// (settled head, settlement lag in blocks) against the accepted head.
    pub fn status(&self, accepted_head: u64) -> (u64, u64) {
        let s = self.settled_head.load(Ordering::Relaxed);
        (s, accepted_head.saturating_sub(s))
    }

    /// Accept of the block at `h` (h > k): its header commits the settled root
    /// of `h-k`. Record it, and if `h-k` is already settled, gate now.
    pub(crate) fn accept_expected(&self, h: u64, header_root: B256) {
        let s = h - self.k;
        let mut g = self.gate.lock().unwrap();
        if let Some(r) = g.settled.get(&s).copied() {
            gate_or_halt(s, r.root, header_root);
        }
        g.expected.insert(s, header_root);
        prune(&mut g, self.settled_head.load(Ordering::Relaxed), self.k);
    }

    /// The executor settled `h`: record its attestations, gate the state root
    /// against the header that committed it (if already accepted).
    fn on_settle(&self, h: u64, s: Settled) {
        let mut g = self.gate.lock().unwrap();
        if let Some(r) = g.expected.get(&h).copied() {
            gate_or_halt(h, s.root, r);
        }
        g.settled.insert(h, s);
        prune(&mut g, h, self.k);
    }
}

/// `settled[h]` must equal the header's committed root for `h`; a mismatch is a
/// Byzantine settled root on the accepted chain (the sync checker's fatal exit).
fn gate_or_halt(h: u64, computed: B256, header: B256) {
    if computed != header {
        eprintln!("epochdb-rs: SAE settled root mismatch at height {h}: computed {computed}, the header of h+k committed {header}");
        std::process::exit(1);
    }
}

/// Keep only a window of `~4k` heights around the settled head (bounded work,
/// enough for the accept/settle rendezvous and a few blocks of BuildBlock reach).
fn prune(g: &mut Gate, settled_head: u64, k: u64) {
    let keep = settled_head.saturating_sub(4 * k.max(1) + 16);
    g.settled.retain(|h, _| *h >= keep);
    g.expected.retain(|h, _| *h >= keep);
}

/// nonce, balance of `addrs` at the SETTLED state, with the projected nonce
/// (settled nonce + the sender's accepted-but-unsettled txs) so the pool admits
/// against the projection: the same worst-case funds and projected-nonce rule
/// verify_light enforces. `be` is the executor's committed backend.
pub fn sae_accounts(proj: &Projection, be: &mut Backend, addrs: &[Address]) -> Vec<(u64, U256)> {
    addrs
        .iter()
        .map(|a| {
            let (nonce, balance) = be.basic(*a).unwrap().map_or((0, U256::ZERO), |i| (i.nonce, i.balance));
            (nonce + proj.unsettled_count(a), balance)
        })
        .collect()
}

/// The continuous executor: pulls accepted blocks in order (`settled_head+1`
/// onward), executes each `k` behind the accepted head, applies the settled
/// state (backend + the rolled Dirty), gates the settled root, retires the
/// projection, and writes the store row. One thread: execution and the root run
/// serially. ponytail: a second thread would pipeline the root behind the next
/// block's execution (the bench's sae_checker split); add it only if the root
/// is the settled-throughput bottleneck.
#[allow(clippy::too_many_arguments)]
pub fn sae_executor(
    rx: Receiver<Arc<Block>>,
    inner: Arc<Mutex<Inner>>,
    store: Arc<Mutex<Box<dyn BlockStore>>>,
    recent: Arc<Mutex<HashMap<Id, Arc<Block>>>>,
    sae: Arc<Sae>,
    stats: Arc<Stats>,
    mut parent_time: u64,
) -> anyhow::Result<()> {
    for b in rx {
        let h = b.header.number;
        // Under the engine lock: execute on the settled state, apply, root, roll.
        let (payload, root, balances) = {
            let mut g = inner.lock().unwrap();
            let inner = &mut *g;
            let Ex::Native(ex) = &mut inner.ex else { bail!("SAE mode needs the native state engine") };
            let roller = inner.roller.as_mut().expect("the native engine rolls");
            // Swap in a roll that finished in the background before this block.
            if let Some(r) = roller.poll_roll(false)? {
                let st = store.clone();
                roller.finish_roll(&mut ex.db_mut().backend, r, || {
                    st.lock().unwrap().sync()?;
                    Ok(())
                })?;
            }
            ex.db_mut().begin(None);
            let r = ex.execute_block(&b, parent_time).with_context(|| format!("SAE exec block {h}"))?;
            let p = ex.db_mut().finish(h, b.hash, b.header.time, r);
            let payload = p.payload.lock().unwrap().take().expect("payload");
            let be = &mut ex.db_mut().backend;
            be.apply_ws(&payload.ws);
            for (ch, c) in &p.code {
                be.code.insert(*ch, c.clone());
            }
            be.set_block_hash(h, b.hash);
            let root = {
                let mut d = roller.dirty.lock().unwrap();
                if payload.ws.is_empty() {
                    d.current_root()
                } else {
                    for (k, v) in &payload.ws {
                        d.apply(k, v).with_context(|| format!("SAE block {h}: apply write set"))?;
                    }
                    d.root().with_context(|| format!("SAE block {h}: settled root"))?
                }
            };
            let root = B256::from(root);
            // The senders' settled (nonce, balance) after this block, to refresh
            // the projection's baseline (the worst-case funds check reads it).
            let mut balances: HashMap<Address, (u64, U256)> = HashMap::new();
            for t in &b.txs {
                if let Some(a) = t.sender {
                    if let Some(i) = be.basic(a).unwrap() {
                        balances.insert(a, (i.nonce, i.balance));
                    }
                }
            }
            roller.maybe_roll(be, inner.roll_budget, h, root);
            (payload, root, balances)
        };
        // Off the engine lock: the block's OWN receipts root / bloom (the header
        // at h+k commits these with the settled root), gate, settle, store.
        let receipts_root = exec::exec::receipts_root(&payload.result.txs);
        sae.on_settle(h, Settled { root, receipts_root, bloom: payload.result.bloom });
        sae.proj.lock().unwrap().settle_block(h, &b.txs, Some(&balances));
        let mut result = payload.result;
        exec::exec::render_deferred(&mut result).with_context(|| format!("SAE block {h}: callTracer render"))?;
        let rows = BlockWrite::from_exec(&b, &result).with_context(|| format!("SAE block {h}: store rows"))?;
        let mut receipts = Vec::new();
        for t in &result.txs {
            t.receipt.encode_2718(&mut receipts);
        }
        let record = Record { height: h, id: b.hash.0, container: b.container.clone(), receipts, traces: Vec::new(), ws: payload.ws, code: payload.code, rows: Some(rows) };
        store.lock().unwrap().append(record).with_context(|| format!("SAE block {h}: store append"))?;
        recent.lock().unwrap().remove(&b.hash.0);
        sae.settled_head.store(h, Ordering::Relaxed);
        sae.settled_tx.fetch_add(b.txs.len() as u64, Ordering::Relaxed);
        sae.settled_gas.fetch_add(result.gas_used, Ordering::Relaxed);
        sae.settled_time.store(b.header.time, Ordering::Relaxed);
        stats.checked.fetch_add(1, Ordering::Relaxed);
        parent_time = b.header.time;
    }
    let mut s = store.lock().unwrap();
    s.sync()?;
    s.close()?;
    Ok(())
}

/// Parse the SAE config keys. `sae` on = SAE mode; `sae-settlement-blocks` = k.
pub fn config(conf: &serde_json::Value) -> Option<(u64, u64, usize)> {
    let on = matches!(conf.get("sae"), Some(serde_json::Value::Bool(true)))
        || matches!(conf.get("sae"), Some(serde_json::Value::String(s)) if s == "true" || s == "1")
        || matches!(conf.get("sae"), Some(serde_json::Value::Number(n)) if n.as_u64() == Some(1));
    if !on {
        return None;
    }
    let k = crate::node_engine::conf_u64(conf, "sae-settlement-blocks").unwrap_or(8);
    let capacity = crate::node_engine::conf_u64(conf, "sae-gas-capacity").unwrap_or(u64::MAX);
    let size_cap = crate::node_engine::conf_u64(conf, "sae-size-cap-kib").map_or(usize::MAX, |k| (k as usize) << 10);
    Some((k, capacity, size_cap))
}

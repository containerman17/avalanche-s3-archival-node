//! Replay a container dump through the executor and check every block against
//! its header (gasUsed, receiptsRoot, logsBloom) and the state root at
//! checkpoints (every --checkpoint blocks, every precompile activation height
//! and the block after it). Prints a bench line.
//!
//!   epochdb-exec --dump step-containers-1-50000.bin --genesis chain.json \
//!       --upgrade upgrade.json [--to N] [--checkpoint 1000] [--workers 4] \
//!       [--traces-out traces.jsonl --traces-to 2000] [--network 1]
//!
//! A later window (the dump starts past genesis) replays over the archive
//! node's state as of the block before it (no root checks then):
//!
//!   epochdb-exec --dump beam-warp-3227132-3232131.bin --genesis chain.json \
//!       --upgrade upgrade.json --from 3227132 --rpc https://build.onbeam.com/rpc \
//!       [--pchain https://avalanche-p-chain-rpc.publicnode.com/ext/bc/P] [--rpc-cache FILE]

use anyhow::{anyhow, bail, Context, Result};
use epochdb_exec::rpc::{RpcDb, RpcValidatorState};
use epochdb_exec::{oracle, Config, Executor, StateDb};
use revm::database::{CacheDB, EmptyDB};
use std::io::Write;
use std::time::{Duration, Instant};

fn arg(args: &[String], name: &str) -> Option<String> {
    args.iter().position(|a| a == name).and_then(|i| args.get(i + 1).cloned())
}

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let dump = arg(&args, "--dump").ok_or_else(|| anyhow!("--dump FILE"))?;
    let mut genesis = std::fs::read(arg(&args, "--genesis").ok_or_else(|| anyhow!("--genesis chain.json"))?)?;
    let upgrade = match arg(&args, "--upgrade") {
        Some(p) => std::fs::read(p)?,
        None => Vec::new(),
    };
    let mut network: u32 = arg(&args, "--network").map(|s| s.parse()).transpose()?.unwrap_or(1);
    let (mut blockchain_id, mut subnet_id) = (Default::default(), Default::default());
    // An epochdb chain descriptor (chain/chain.go: networkID + base64 genesisData) or the bare genesis.
    if let Ok(desc) = serde_json::from_slice::<serde_json::Value>(&genesis) {
        if let Some(gd) = desc.get("genesisData").and_then(|v| v.as_str()) {
            use base64::Engine;
            genesis = base64::engine::general_purpose::STANDARD.decode(gd).context("genesisData base64")?;
            if let Some(n) = desc.get("networkID").and_then(|v| v.as_u64()) {
                network = n as u32;
            }
            if let Some(id) = desc.get("blockchainID").and_then(|v| v.as_str()) {
                blockchain_id = epochdb_exec::config::cb58(id)?;
            }
            if let Some(id) = desc.get("subnetID").and_then(|v| v.as_str()) {
                subnet_id = epochdb_exec::config::cb58(id)?;
            }
        }
    }
    let from: u64 = arg(&args, "--from").map(|s| s.parse()).transpose()?.unwrap_or(1);
    let to: u64 = arg(&args, "--to").map(|s| s.parse()).transpose()?.unwrap_or(u64::MAX);
    let checkpoint: u64 = arg(&args, "--checkpoint").map(|s| s.parse()).transpose()?.unwrap_or(1000);
    let workers: usize = arg(&args, "--workers").map(|s| s.parse()).transpose()?.unwrap_or(4);
    let traces_to: u64 = arg(&args, "--traces-to").map(|s| s.parse()).transpose()?.unwrap_or(0);
    let traces_from: u64 = arg(&args, "--traces-from").map(|s| s.parse()).transpose()?.unwrap_or(1);
    let mut traces_out = arg(&args, "--traces-out").map(std::fs::File::create).transpose()?.map(std::io::BufWriter::new);
    let rpc = arg(&args, "--rpc");
    let pchain = arg(&args, "--pchain");
    let rpc_cache = arg(&args, "--rpc-cache");

    let cfg = Config::from_genesis(&genesis, &upgrade, network).context("config")?.with_chain(blockchain_id, subnet_id);
    eprintln!(
        "chain {} spec(genesis)={:?} durango={:?} etna={:?} granite={:?} precompiles genesis={} upgrades={} stateUpgrades={} blockchain={} subnet={}",
        cfg.chain_id,
        cfg.spec(cfg.genesis_timestamp),
        cfg.durango,
        cfg.etna,
        cfg.granite,
        cfg.genesis_precompiles.len(),
        cfg.precompile_upgrades.len(),
        cfg.state_upgrades.len(),
        cfg.blockchain_id,
        cfg.subnet_id
    );
    let vs = pchain.map(|u| Box::new(RpcValidatorState::new(&u, rpc_cache.as_deref())) as Box<dyn epochdb_exec::ValidatorState>);
    if from > 1 {
        let url = rpc.ok_or_else(|| anyhow!("--from past 1 needs --rpc (the archive node's state at --from minus one)"))?;
        let db = RpcDb::new(&url, from - 1, rpc_cache.as_deref());
        let (parent_time, parent_hash) = db.block_time_and_hash(from - 1);
        let mut ex = Executor::resume(cfg, CacheDB::new(db))?;
        ex.validator_state = vs;
        ex.set_block_hash(from - 1, parent_hash);
        eprintln!("window from {from}: parent time {parent_time} hash {parent_hash}");
        let r = run(&mut ex, &dump, from, to, u64::MAX, workers, traces_from, traces_to, &mut traces_out, parent_time, false);
        eprintln!("rpc calls: state {} validators {}", ex.db().db.rpc.calls.borrow(), 0);
        return r;
    }
    let parent_time = cfg.genesis_timestamp;
    let mut ex = Executor::new(cfg)?;
    ex.validator_state = vs;
    run(&mut ex, &dump, 1, to, checkpoint, workers, traces_from, traces_to, &mut traces_out, parent_time, true)
}

#[allow(clippy::too_many_arguments)]
fn run<D: StateDb + 'static>(
    ex: &mut Executor<D>,
    dump: &str,
    from: u64,
    to: u64,
    checkpoint: u64,
    workers: usize,
    traces_from: u64,
    traces_to: u64,
    traces_out: &mut Option<std::io::BufWriter<std::fs::File>>,
    mut parent_time: u64,
    roots: bool,
) -> Result<()> {
    let blocks = block::Blocks::open(dump, from, to)?;
    let t_start = Instant::now();
    let (mut t_exec, mut t_root, mut t_wait) = (Duration::ZERO, Duration::ZERO, Duration::ZERO);
    let (mut nblk, mut ntx, mut gas, mut last_h) = (0u64, 0u64, 0u64, 0u64);
    let mut last_report = Instant::now();
    let (mut rblk, mut rtx, mut rgas) = (0u64, 0u64, 0u64);
    let mut first = true;
    let mut check_next = false;
    let mut roots_checked = 0u64;
    let mut precompile_txs = 0u64;
    let mut it = block::recovered(blocks, workers);
    loop {
        let tw = Instant::now();
        let Some(b) = it.next() else { break };
        t_wait += tw.elapsed();
        let b = b.map_err(|e| anyhow!("block decode: {e:?}"))?;
        let h = &b.header;
        if first {
            ex.set_block_hash(h.number - 1, h.parent_hash);
            first = false;
        }
        let activation = !ex.cfg.activating(Some(parent_time), h.time).is_empty() || !ex.cfg.activating_state_upgrades(Some(parent_time), h.time).is_empty();
        if activation {
            eprintln!("h={} activation: {:?}", h.number, ex.cfg.activating(Some(parent_time), h.time).iter().map(|c| format!("{}{}", c.key, if c.disable { "(disable)" } else { "" })).collect::<Vec<_>>());
        }
        let t0 = Instant::now();
        let r = ex.execute_block(&b, parent_time).with_context(|| format!("block {}", h.number))?;
        t_exec += t0.elapsed();
        if let Some(w) = traces_out.as_mut() {
            if h.number >= traces_from && h.number <= traces_to {
                for t in &r.txs {
                    writeln!(w, "{{\"height\":{},\"tx\":\"{}\",\"result\":{}}}", h.number, t.hash, t.trace_json)?;
                }
                w.flush()?;
            }
        }
        if r.gas_used != h.gas_used {
            bail!("block {}: gasUsed {} != header {}", h.number, r.gas_used, h.gas_used);
        }
        if r.receipts_root != h.receipt_hash {
            bail!("block {}: receiptsRoot {} != header {}", h.number, r.receipts_root, h.receipt_hash);
        }
        if r.bloom != h.bloom {
            bail!("block {}: logsBloom differs from the header", h.number);
        }
        precompile_txs += b.txs.iter().filter(|t| t.to.is_some_and(|a| epochdb_exec::precompile::module_index(a).is_some())).count() as u64;
        ex.set_block_hash(h.number, b.hash);
        nblk += 1;
        ntx += r.txs.len() as u64;
        gas += r.gas_used;
        rblk += 1;
        rtx += r.txs.len() as u64;
        rgas += r.gas_used;
        parent_time = h.time;
        last_h = h.number;
        let periodic = h.number % checkpoint == 0;
        if roots && (periodic || activation || check_next) {
            let t1 = Instant::now();
            let root = state_root(ex);
            t_root += t1.elapsed();
            if root != h.root {
                bail!("block {}: stateRoot {root} != header {}", h.number, h.root);
            }
            roots_checked += 1;
            if !periodic {
                eprintln!("h={} root ok ({}, {:.0} ms)", h.number, if activation { "activation" } else { "activation+1" }, t1.elapsed().as_secs_f64() * 1e3);
            }
        }
        check_next = activation;
        if periodic {
            let dt = last_report.elapsed().as_secs_f64();
            eprintln!(
                "h={} {} window {:.0} blk/s {:.0} tx/s {:.1} mgas/s cum {:.1} mgas/s (evm {:.1}s trace {:.1}s commit {:.1}s) precompile txs {} predicates verified {} trusted {}",
                h.number,
                if roots { "root ok" } else { "receipts ok" },
                rblk as f64 / dt,
                rtx as f64 / dt,
                rgas as f64 / dt / 1e6,
                gas as f64 / t_exec.as_secs_f64() / 1e6,
                ex.t_evm.as_secs_f64(),
                ex.t_trace.as_secs_f64(),
                ex.t_commit.as_secs_f64(),
                precompile_txs,
                ex.predicates_verified,
                ex.predicates_trusted
            );
            last_report = Instant::now();
            (rblk, rtx, rgas) = (0, 0, 0);
        }
    }
    let wall = t_start.elapsed().as_secs_f64();
    let ex_s = t_exec.as_secs_f64();
    eprintln!(
        "done h={last_h} blocks={nblk} txs={ntx} gas={gas} roots_checked={roots_checked} precompile_txs={precompile_txs} predicates_verified={} predicates_trusted={} wall={wall:.2}s exec={ex_s:.2}s wait={:.2}s root={:.2}s | exec-thread: {:.0} blk/s {:.0} tx/s {:.1} mgas/s | wall: {:.0} blk/s {:.0} tx/s {:.1} mgas/s",
        ex.predicates_verified,
        ex.predicates_trusted,
        t_wait.as_secs_f64(),
        t_root.as_secs_f64(),
        nblk as f64 / ex_s,
        ntx as f64 / ex_s,
        gas as f64 / ex_s / 1e6,
        nblk as f64 / wall,
        ntx as f64 / wall,
        gas as f64 / wall / 1e6
    );
    Ok(())
}

/// The root oracle only exists for the in-memory state.
fn state_root<D: StateDb + 'static>(ex: &Executor<D>) -> alloy_primitives::B256 {
    let any: &dyn std::any::Any = ex.db();
    match any.downcast_ref::<CacheDB<EmptyDB>>() {
        Some(db) => oracle::state_root(db),
        None => alloy_primitives::B256::ZERO,
    }
}

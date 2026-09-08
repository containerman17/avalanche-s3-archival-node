//! Replay a container dump through the executor and check every block against
//! its header (gasUsed, receiptsRoot, logsBloom) and the state root at
//! checkpoints. Prints a bench line.
//!
//!   epochdb-exec --dump step-containers-1-50000.bin --genesis chain.json \
//!       --upgrade upgrade.json [--to N] [--checkpoint 1000] [--workers 4] \
//!       [--traces-out traces.jsonl --traces-to 2000] [--network 1]

use anyhow::{anyhow, bail, Context, Result};
use epochdb_exec::{oracle, Config, Executor};
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
    // An epochdb chain descriptor (chain/chain.go: networkID + base64 genesisData) or the bare genesis.
    if let Ok(desc) = serde_json::from_slice::<serde_json::Value>(&genesis) {
        if let Some(gd) = desc.get("genesisData").and_then(|v| v.as_str()) {
            use base64::Engine;
            genesis = base64::engine::general_purpose::STANDARD.decode(gd).context("genesisData base64")?;
            if let Some(n) = desc.get("networkID").and_then(|v| v.as_u64()) {
                network = n as u32;
            }
        }
    }
    let to: u64 = arg(&args, "--to").map(|s| s.parse()).transpose()?.unwrap_or(u64::MAX);
    let checkpoint: u64 = arg(&args, "--checkpoint").map(|s| s.parse()).transpose()?.unwrap_or(1000);
    let workers: usize = arg(&args, "--workers").map(|s| s.parse()).transpose()?.unwrap_or(4);
    let traces_to: u64 = arg(&args, "--traces-to").map(|s| s.parse()).transpose()?.unwrap_or(0);
    let mut traces_out = arg(&args, "--traces-out").map(std::fs::File::create).transpose()?.map(std::io::BufWriter::new);

    let cfg = Config::from_genesis(&genesis, &upgrade, network).context("config")?;
    eprintln!(
        "chain {} spec(genesis)={:?} durango={:?} etna={:?} precompile upgrades={}",
        cfg.chain_id,
        cfg.spec(cfg.genesis_timestamp),
        cfg.durango,
        cfg.etna,
        cfg.precompile_upgrades.len()
    );
    let mut parent_time = cfg.genesis_timestamp;
    let mut ex = Executor::new(cfg)?;

    let blocks = block::Blocks::open(&dump, 1, to)?;
    let t_start = Instant::now();
    let (mut t_exec, mut t_root, mut t_wait) = (Duration::ZERO, Duration::ZERO, Duration::ZERO);
    let (mut nblk, mut ntx, mut gas, mut last_h) = (0u64, 0u64, 0u64, 0u64);
    let mut last_report = Instant::now();
    let (mut rblk, mut rtx, mut rgas) = (0u64, 0u64, 0u64);
    let mut first = true;
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
        let t0 = Instant::now();
        let r = ex.execute_block(&b, parent_time).with_context(|| format!("block {}", h.number))?;
        t_exec += t0.elapsed();
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
        if let Some(w) = traces_out.as_mut() {
            if h.number <= traces_to {
                for t in &r.txs {
                    writeln!(w, "{{\"height\":{},\"tx\":\"{}\",\"result\":{}}}", h.number, t.hash, t.trace_json)?;
                }
            }
        }
        nblk += 1;
        ntx += r.txs.len() as u64;
        gas += r.gas_used;
        rblk += 1;
        rtx += r.txs.len() as u64;
        rgas += r.gas_used;
        parent_time = h.time;
        last_h = h.number;
        if h.number % checkpoint == 0 {
            let t1 = Instant::now();
            let root = oracle::state_root(ex.db());
            t_root += t1.elapsed();
            if root != h.root {
                bail!("block {}: stateRoot {root} != header {}", h.number, h.root);
            }
            let (na, ns) = oracle::size(ex.db());
            let dt = last_report.elapsed().as_secs_f64();
            eprintln!(
                "h={} root ok ({:.0} ms, {na} accts {ns} slots) window {:.0} blk/s {:.0} tx/s {:.1} mgas/s cum {:.1} mgas/s",
                h.number,
                t1.elapsed().as_secs_f64() * 1e3,
                rblk as f64 / dt,
                rtx as f64 / dt,
                rgas as f64 / dt / 1e6,
                gas as f64 / t_exec.as_secs_f64() / 1e6
            );
            last_report = Instant::now();
            (rblk, rtx, rgas) = (0, 0, 0);
        }
    }
    if last_h % checkpoint != 0 && last_h > 0 {
        eprintln!("h={last_h}: final root not checked (no header at hand after the loop); rerun with --checkpoint dividing --to");
    }
    let wall = t_start.elapsed().as_secs_f64();
    let ex_s = t_exec.as_secs_f64();
    eprintln!(
        "done h={last_h} blocks={nblk} txs={ntx} gas={gas} wall={wall:.2}s exec={ex_s:.2}s wait={:.2}s root={:.2}s | exec-thread: {:.0} blk/s {:.0} tx/s {:.1} mgas/s | wall: {:.0} blk/s {:.0} tx/s {:.1} mgas/s",
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

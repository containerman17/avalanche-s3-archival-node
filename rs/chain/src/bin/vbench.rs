//! The validator-shape bench and build oracle over a container dump: the
//! engine in NormalOp (state root inside verify), every block verified and
//! accepted through the block tree, and with --build every block first
//! rebuilt from its own tx list on its real parent (same timestamp, coinbase
//! and delay excess) and compared byte for byte with the real block. Latency
//! percentiles per block for verify and build. --synthetic N builds and
//! verifies one block of N signed transfers on a private genesis.
//!
//!   vbench --dump FILE --chain chain.json [--upgrade upgrade.json] --data DIR
//!          [--to N] [--build] [--quiet]
//!   vbench --synthetic 1000 --data DIR
//!   vbench --window --dump W.bin --chain chain.json --upgrade u.json --from H --rpc URL [--rpc-cache F] [--pchain URL]
//!       (a later window over the archive node's state: every block from H+1 rebuilt on the dump
//!        block before it; the state root is taken from the real header, everything else compared)
use std::time::Instant;

use alloy_primitives::{keccak256, Address, U256};
use anyhow::{anyhow, bail, Context as _, Result};
use bytes::Bytes;
use chain::build::Params;
use chain::{Engine, Init, NodeEngine, Tree};

fn arg(args: &[String], name: &str) -> Option<String> {
    args.iter().position(|a| a == name).and_then(|i| args.get(i + 1).cloned())
}

struct Lat(Vec<f64>);
impl Lat {
    fn add(&mut self, t: Instant) {
        self.0.push(t.elapsed().as_secs_f64() * 1e3);
    }
    fn line(&mut self, what: &str) -> String {
        if self.0.is_empty() {
            return format!("{what}: none");
        }
        self.0.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let n = self.0.len();
        let q = |p: f64| self.0[((n as f64 - 1.0) * p).round() as usize];
        format!("{what}: n={n} mean={:.3} p50={:.3} p99={:.3} max={:.3} ms", self.0.iter().sum::<f64>() / n as f64, q(0.5), q(0.99), self.0[n - 1])
    }
}

/// The first header field that differs, for a build mismatch report.
fn header_diff(a: &block::Header, b: &block::Header) -> String {
    macro_rules! f {
        ($($x:ident),*) => { $( if a.$x != b.$x { return format!("{}: built {:?} real {:?}", stringify!($x), a.$x, b.$x); } )* };
    }
    f!(parent_hash, uncle_hash, coinbase, root, tx_hash, receipt_hash, bloom, difficulty, number, gas_limit, gas_used, time, extra, mix_digest, nonce, base_fee, block_gas_cost, blob_gas_used, excess_blob_gas, parent_beacon_root, time_milliseconds, min_delay_excess);
    "headers equal".into()
}

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if let Some(out) = arg(&args, "--export-inner") {
        // --export-inner FILE --dump D [--to N]: the inner block RLP of every dump block as [u32 LE len][bytes].
        let dump = arg(&args, "--dump").ok_or_else(|| anyhow!("--dump FILE"))?;
        let to: u64 = arg(&args, "--to").map(|s| s.parse()).transpose()?.unwrap_or(u64::MAX);
        let mut w = std::io::BufWriter::new(std::fs::File::create(&out)?);
        let mut n = 0u64;
        for r in block::Dump::open(&dump)?.records(1, to) {
            let inner = block::pvm::unwrap(&r.container).map_err(|e| anyhow!("{e}"))?.inner;
            std::io::Write::write_all(&mut w, &(inner.len() as u32).to_le_bytes())?;
            std::io::Write::write_all(&mut w, &inner)?;
            n += 1;
        }
        std::io::Write::flush(&mut w)?;
        eprintln!("vbench: exported {n} inner blocks to {out}");
        return Ok(());
    }
    if let Some(hs) = arg(&args, "--probe") {
        // --probe h1,h2: height, time and the fork-relevant header fields of dump blocks.
        let dump = arg(&args, "--dump").ok_or_else(|| anyhow!("--dump FILE"))?;
        let d = block::Dump::open(&dump)?;
        for h in hs.split(',') {
            let h: u64 = h.parse()?;
            let Some(r) = d.records(h, h).next() else { println!("{h}: not in the dump"); continue };
            let b = block::decode_container(r.container).map_err(|e| anyhow!("{e}"))?;
            println!("{h}: time={} extra={} base_fee={:?} block_gas_cost={:?} blob={:?} ms={:?} mde={:?} txs={}", b.header.time, b.header.extra.len(), b.header.base_fee, b.header.block_gas_cost, b.header.excess_blob_gas, b.header.time_milliseconds, b.header.min_delay_excess, b.txs.len());
        }
        return Ok(());
    }
    if args.iter().any(|a| a == "--window") {
        return window(&args);
    }
    let data = arg(&args, "--data").ok_or_else(|| anyhow!("--data DIR"))?;
    if let Some(n) = arg(&args, "--synthetic") {
        return synthetic(n.parse()?, &data);
    }
    let dump = arg(&args, "--dump").ok_or_else(|| anyhow!("--dump FILE"))?;
    let desc: serde_json::Value = serde_json::from_slice(&std::fs::read(arg(&args, "--chain").ok_or_else(|| anyhow!("--chain chain.json"))?)?)?;
    let upgrade = match arg(&args, "--upgrade") {
        Some(p) => std::fs::read(p)?,
        None => Vec::new(),
    };
    let to: u64 = arg(&args, "--to").map(|s| s.parse()).transpose()?.unwrap_or(u64::MAX);
    let do_build = args.iter().any(|a| a == "--build");
    let quiet = args.iter().any(|a| a == "--quiet");
    use base64::Engine as _;
    let genesis = base64::engine::general_purpose::STANDARD.decode(desc["genesisData"].as_str().ok_or_else(|| anyhow!("chain.json: genesisData"))?)?;
    let init = Init {
        network_id: desc["networkID"].as_u64().unwrap_or(1) as u32,
        subnet_id: exec::config::cb58(desc["subnetID"].as_str().unwrap_or_default())?.0,
        chain_id: exec::config::cb58(desc["blockchainID"].as_str().unwrap_or_default())?.0,
        chain_data_dir: data.clone(),
        genesis_bytes: genesis,
        upgrade_bytes: upgrade,
        config_bytes: arg(&args, "--config").unwrap_or_else(|| r#"{"roll-budget-mb":256}"#.into()).into_bytes(),
    };
    let engine = NodeEngine::open(&init).map_err(|e| anyhow!("{e}"))?;
    run_dump(engine, &dump, to, do_build, quiet)
}

fn run_dump(engine: NodeEngine, dump: &str, to: u64, do_build: bool, quiet: bool) -> Result<()> {
    let tree = Tree::new(engine);
    tree.engine.set_state(true);
    let head = tree.engine.last_accepted();
    let from = head.height + 1;
    eprintln!("vbench: head {} ({}), dump from {from} to {}", head.height, head.hash, if to == u64::MAX { "end".into() } else { to.to_string() });
    let t_start = Instant::now();
    let (mut lv, mut lb, mut la) = (Lat(Vec::new()), Lat(Vec::new()), Lat(Vec::new()));
    let (mut built_ok, mut built_bad, mut built_skip, mut n) = (0u64, 0u64, 0u64, 0u64);
    let mut txs_total = 0u64;
    let mut recs = block::Dump::open(dump)?.records(from, to);
    let mut last_report = Instant::now();
    loop {
        let batch: Vec<Bytes> = recs.by_ref().take(256).map(|r| r.container).collect();
        if batch.is_empty() {
            break;
        }
        let blocks = tree.engine.parse_batch(batch).map_err(|e| anyhow!("parse: {e}"))?;
        for b in blocks {
            let parent = tree.engine.last_accepted();
            let pvm_h = b.pvm.as_ref().map(|p| p.pchain_height);
            if do_build {
                let inner = block::pvm::unwrap(&b.container).map_err(|e| anyhow!("{e}"))?.inner;
                let params = Params {
                    timestamp_ms: b.header.time_milliseconds.unwrap_or(b.header.time * 1000),
                    coinbase: b.header.coinbase,
                    desired_min_delay_excess: b.header.min_delay_excess,
                };
                let carries_predicates = b.txs.iter().any(|t| t.access_list.iter().any(|a| a.address == exec::precompile::WARP));
                let t0 = Instant::now();
                match tree.engine.build(None, &parent.header, &params, pvm_h, b.txs.clone()) {
                    Ok(out) => {
                        lb.add(t0);
                        if out.block.container[..] == inner[..] && out.block.hash == b.hash {
                            built_ok += 1;
                        } else {
                            built_bad += 1;
                            if built_bad <= 20 {
                                eprintln!("vbench: build mismatch at {}: {} (included {} of {}, reasons {:?})", b.height, header_diff(&out.block.header, &b.header), out.included.len(), b.txs.len(), out.reasons.iter().filter(|r| **r != exec::exec::SkipReason::Included).take(5).collect::<Vec<_>>());
                            }
                        }
                    }
                    Err(e) if carries_predicates => {
                        built_skip += 1;
                        if built_skip <= 5 {
                            eprintln!("vbench: build skipped at {} (warp predicates, no validator state): {e}", b.height);
                        }
                    }
                    Err(e) => {
                        built_bad += 1;
                        if built_bad <= 20 {
                            eprintln!("vbench: build failed at {}: {e}", b.height);
                        }
                    }
                }
            }
            let id = b.hash.0;
            txs_total += b.txs.len() as u64;
            let t0 = Instant::now();
            tree.verify(b, pvm_h).map_err(|e| anyhow!("verify: {e}"))?;
            lv.add(t0);
            let t0 = Instant::now();
            tree.accept(&id).map_err(|e| anyhow!("accept: {e}"))?;
            la.add(t0);
            n += 1;
            if !quiet && last_report.elapsed().as_secs() >= 10 {
                last_report = Instant::now();
                eprintln!("vbench: h={} blocks={n} txs={txs_total} built ok={built_ok} bad={built_bad} skipped={built_skip} {:.0} blk/s", tree.engine.last_accepted().height, n as f64 / t_start.elapsed().as_secs_f64());
            }
        }
    }
    let secs = t_start.elapsed().as_secs_f64();
    eprintln!("vbench: done blocks={n} txs={txs_total} in {secs:.1}s ({:.0} blk/s) head={}", n as f64 / secs, tree.engine.last_accepted().height);
    eprintln!("vbench: {}", lv.line("verify (execute + inline root)"));
    eprintln!("vbench: {}", la.line("accept"));
    if do_build {
        eprintln!("vbench: {}", lb.line("build"));
        eprintln!("vbench: build oracle: byte-identical {built_ok}, mismatched {built_bad}, skipped {built_skip} (warp predicates without a validator state)");
    }
    {
        let s = &tree.engine.stats;
        let ns = |a: &std::sync::atomic::AtomicU64| a.load(std::sync::atomic::Ordering::Relaxed) as f64 / 1e9;
        let g = tree.engine.inner.lock().unwrap();
        eprintln!("vbench: split verify={:.2}s (root {:.2}s) accept={:.2}s checker={:.2}s | exec thread evm={:.2}s trace(receipts+deferred take)={:.2}s commit={:.2}s", ns(&s.t_verify), ns(&s.t_root), ns(&s.t_accept), ns(&s.t_check), g.ex.t_evm.as_secs_f64(), g.ex.t_trace.as_secs_f64(), g.ex.t_commit.as_secs_f64());
    }
    tree.engine.shutdown();
    if built_bad > 0 {
        bail!("{built_bad} built blocks differ from the real ones");
    }
    Ok(())
}

/// A private chain: one funded key, mainnet's fork schedule, genesis at 0
/// (LONDON); the block is built at a Granite timestamp.
fn synthetic(n: usize, data: &str) -> Result<()> {
    let s = chain::synth::Signer::new([0x11; 32]);
    let init = Init { network_id: 1, subnet_id: [7; 32], chain_id: [9; 32], chain_data_dir: data.into(), genesis_bytes: chain::synth::genesis_json(s.address).into_bytes(), upgrade_bytes: Vec::new(), config_bytes: b"{}".to_vec() };
    let engine = NodeEngine::open(&init).map_err(|e| anyhow!("{e}"))?;
    let tree = Tree::new(engine);
    tree.engine.set_state(true);
    let genesis_blk = tree.engine.last_accepted();
    // n EIP-1559 transfers to distinct recipients, nonces 0..n.
    let txs: Vec<block::Tx> = (0..n as u64).map(|i| s.transfer(99999, i, 1_000_000_000, 50_000_000_000, 21_000, Address::from_slice(&keccak256(i.to_be_bytes())[12..]), U256::from(1_000_000_000_000u64))).collect();
    let params = Params { timestamp_ms: 1_770_000_000_123, coinbase: Address::from([0xcc; 20]), desired_min_delay_excess: None };
    let t0 = Instant::now();
    let out = tree.engine.build(None, &genesis_blk.header, &params, None, txs).map_err(|e| anyhow!("build: {e}"))?;
    let build_ms = t0.elapsed().as_secs_f64() * 1e3;
    let b = out.block.clone();
    eprintln!("vbench: synthetic block {} txs={} gas={} root={} extra={} bytes: build {build_ms:.2} ms", b.height, b.txs.len(), b.header.gas_used, b.header.root, b.header.extra.len());
    assert_eq!(out.included.len(), n, "every transfer included: {:?}", out.reasons.iter().filter(|r| **r != exec::exec::SkipReason::Included).count());
    assert_eq!(b.header.time_milliseconds, Some(1_770_000_000_123));
    assert_eq!(b.header.min_delay_excess, Some(chain::build::INITIAL_DELAY_EXCESS));
    // The verify path on the same bytes: parse, verify (root inline), accept.
    let parsed = tree.engine.parse(b.container.clone()).map_err(|e| anyhow!("{e}"))?;
    assert_eq!(parsed.hash, b.hash);
    let t0 = Instant::now();
    tree.verify(parsed, None).map_err(|e| anyhow!("verify: {e}"))?;
    let verify_ms = t0.elapsed().as_secs_f64() * 1e3;
    tree.accept(&b.hash.0).map_err(|e| anyhow!("accept: {e}"))?;
    eprintln!("vbench: synthetic verify (execute + inline root) {verify_ms:.2} ms; build {build_ms:.2} ms; accepted head {}", tree.engine.last_accepted().height);
    let accts = tree.engine.accounts(None, &[s.address]);
    assert_eq!(accts[0].0, n as u64);
    tree.engine.shutdown();
    Ok(())
}

/// The chain config of a chain.json descriptor (or a bare genesis).
fn config_of(args: &[String]) -> Result<exec::Config> {
    let mut genesis = std::fs::read(arg(args, "--chain").ok_or_else(|| anyhow!("--chain chain.json"))?)?;
    let upgrade = match arg(args, "--upgrade") {
        Some(p) => std::fs::read(p)?,
        None => Vec::new(),
    };
    let (mut network, mut bid, mut sid) = (1u32, alloy_primitives::B256::ZERO, alloy_primitives::B256::ZERO);
    if let Ok(desc) = serde_json::from_slice::<serde_json::Value>(&genesis) {
        if let Some(gd) = desc.get("genesisData").and_then(|v| v.as_str()) {
            use base64::Engine as _;
            genesis = base64::engine::general_purpose::STANDARD.decode(gd)?;
            network = desc["networkID"].as_u64().unwrap_or(1) as u32;
            bid = exec::config::cb58(desc["blockchainID"].as_str().unwrap_or_default())?;
            sid = exec::config::cb58(desc["subnetID"].as_str().unwrap_or_default())?;
        }
    }
    Ok(exec::Config::from_genesis(&genesis, &upgrade, network)?.with_chain(bid, sid))
}

/// The header-building oracle on a window without local state: the executor
/// over the archive node's state at --from minus one (exec::rpc::RpcDb), each
/// block from --from + 1 built from its txs on the previous dump block and
/// compared with the real block with the root copied in (the trie is not
/// here). The build IS the block's execution (same txs, same order), so the
/// state advances through it; a block the build cannot reproduce is executed
/// as is and reported.
fn window(args: &[String]) -> Result<()> {
    use revm::Database as _;
    let dump = arg(args, "--dump").ok_or_else(|| anyhow!("--dump FILE"))?;
    let from: u64 = arg(args, "--from").ok_or_else(|| anyhow!("--from H"))?.parse()?;
    let to: u64 = arg(args, "--to").map(|s| s.parse()).transpose()?.unwrap_or(u64::MAX);
    let url = arg(args, "--rpc").ok_or_else(|| anyhow!("--rpc URL"))?;
    let cache = arg(args, "--rpc-cache");
    let cfg = config_of(args)?;
    let db = exec::rpc::RpcDb::new(&url, from - 1, cache.as_deref());
    let (mut parent_time, parent_hash) = db.block_time_and_hash(from - 1);
    let mut ex = exec::Executor::resume(cfg.clone(), revm::database::CacheDB::new(db))?;
    if let Some(p) = arg(args, "--pchain") {
        ex.validator_state = Some(Box::new(exec::rpc::RpcValidatorState::new(&p, cache.as_deref())));
    }
    ex.set_block_hash(from - 1, parent_hash);
    let mut prev: Option<block::Block> = None;
    let (mut ok, mut bad, mut executed, mut skipped, mut lat) = (0u64, 0u64, 0u64, 0u64, Lat(Vec::new()));
    for r in block::Dump::open(&dump)?.records(from, to) {
        let mut b = block::decode_container(r.container).map_err(|e| anyhow!("{e}"))?;
        for t in &mut b.txs {
            t.sender = block::recover(t);
        }
        let inner = block::pvm::unwrap(&b.container).map_err(|e| anyhow!("{e}"))?.inner;
        let this_pchain = b.pvm.as_ref().map(|p| p.pchain_height);
        let epoch = b.pvm.as_ref().and_then(|p| p.epoch_pchain_height);
        let mut built = false;
        if let Some(p) = &prev {
            let params = Params { timestamp_ms: b.header.time_milliseconds.unwrap_or(b.header.time * 1000), coinbase: b.header.coinbase, desired_min_delay_excess: b.header.min_delay_excess };
            let (cfg_ref, db) = ex.cfg_and_db();
            let fc = chain::build::fee_config_at(cfg_ref, p.header.time, |slot| db.storage(exec::precompile::FEE_MANAGER, slot).unwrap());
            let rule = chain::build::coinbase_rule(cfg_ref, p.header.time, || db.storage(exec::precompile::REWARD_MANAGER, exec::rewardmanager::reward_address_slot()).unwrap());
            match chain::build::template(cfg_ref, &fc, &p.header, &params, rule) {
                Ok(h) => {
                    let t0 = Instant::now();
                    // The build only reproduces the block if every tx applies in order.
                    let r = ex.build_block(&h, p.header.time, this_pchain, epoch, &b.txs).with_context(|| format!("build {}", b.height))?;
                    lat.add(t0);
                    if r.included.len() != b.txs.len() {
                        let predicates = b.txs.iter().any(|t| t.access_list.iter().any(|a| a.address == exec::precompile::WARP));
                        eprintln!("vbench: window {}: build included {} of {} txs: {:?}{}", b.height, r.included.len(), b.txs.len(), r.reasons, if predicates { " (warp predicates: executed as is)" } else { "" });
                        if !predicates || !r.included.is_empty() {
                            bail!("block {} not reproducible: the state now differs", b.height);
                        }
                        // Nothing was committed (every tx popped): execute the block itself below.
                        skipped += 1;
                        let r = ex.execute_block(&b, parent_time).with_context(|| format!("block {}", b.height))?;
                        if r.gas_used != b.header.gas_used || r.receipts_root != b.header.receipt_hash {
                            bail!("block {}: execution differs from the header", b.height);
                        }
                        ex.set_block_hash(b.height, b.hash);
                        parent_time = b.header.time;
                        prev = Some(b);
                        continue;
                    }
                    let txs: Vec<&block::Tx> = b.txs.iter().collect();
                    let gas: Vec<u64> = r.result.txs.iter().map(|t| t.gas_used).collect();
                    if let Err(e) = chain::build::verify_block_fee(h.base_fee.unwrap(), h.block_gas_cost.unwrap_or_default(), &txs, &gas) {
                        eprintln!("vbench: window {}: block fee: {e}", b.height);
                    }
                    let (hdr, _, bytes) = chain::build::assemble(h, &txs, b.header.root, &r.result, &r.predicate_bytes).map_err(|e| anyhow!("{e}"))?;
                    if bytes[..] == inner[..] {
                        ok += 1;
                    } else {
                        bad += 1;
                        if bad <= 20 {
                            eprintln!("vbench: window {}: header differs: {}", b.height, header_diff(&hdr, &b.header));
                        }
                    }
                    built = true;
                }
                Err(e) => eprintln!("vbench: window {}: template: {e}", b.height),
            }
        }
        if !built {
            let r = ex.execute_block(&b, parent_time).with_context(|| format!("block {}", b.height))?;
            if r.gas_used != b.header.gas_used || r.receipts_root != b.header.receipt_hash {
                bail!("block {}: execution differs from the header", b.height);
            }
            executed += 1;
        }
        ex.set_block_hash(b.height, b.hash);
        parent_time = b.header.time;
        if b.height % 25 == 0 {
            eprintln!("vbench: window h={} ok={ok} bad={bad} executed={executed} rpc calls={}", b.height, ex.db().db.rpc.calls.borrow());
        }
        prev = Some(b);
    }
    eprintln!("vbench: window {from}..: rpc calls {}; rebuilt byte-identical (root copied) {ok}, mismatched {bad}, executed only {executed} (first block) + {skipped} (warp predicates the build could not verify); {}", ex.db().db.rpc.calls.borrow(), lat.line("build (rpc state)"));
    if bad > 0 {
        bail!("{bad} rebuilt blocks differ");
    }
    Ok(())
}

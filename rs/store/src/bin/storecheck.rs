//! rs/store driver and oracle CLI.
//!
//!   storecheck write   --dump F --genesis chain.json --upgrade upgrade.json --data DIR [--to N] [--flush-end] [--crash-at H] [--workers N]
//!   storecheck verify  --dump F --genesis chain.json --upgrade upgrade.json --data DIR [--to N] [--postings-every N]
//!   storecheck readall --data DIR --genesis chain.json [--to N]
//!   storecheck publish --data DIR --genesis chain.json      (EPOCHDB_S3_* in the environment)
//!   storecheck join    --data DIR --genesis chain.json      (EPOCHDB_S3_* in the environment)
//!   storecheck probe   --data DIR
use anyhow::{anyhow, bail, Context, Result};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Instant;
use store::db::DB;
use store::format::*;
use store::window::BlockWrite;

fn arg(args: &[String], name: &str) -> Option<String> {
    args.iter().position(|a| a == name).and_then(|i| args.get(i + 1).cloned())
}
fn flag(args: &[String], name: &str) -> bool {
    args.iter().any(|a| a == name)
}

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(|s| s.as_str()).unwrap_or("") {
        "write" => write(&args),
        "verify" => verify(&args),
        "readall" => readall(&args),
        "publish" => publish(&args),
        "join" => join(&args),
        "probe" => probe(&args),
        _ => Err(anyhow!("usage: storecheck write|verify|readall|publish|join|probe ...")),
    }
}

/// chain.json: an epochdb chain descriptor with base64 genesisData; the chain
/// root is sha256 of those bytes verbatim (dist/chainroot.go).
fn genesis(args: &[String]) -> Result<(Vec<u8>, u32, [u8; 32])> {
    let raw = std::fs::read(arg(args, "--genesis").ok_or_else(|| anyhow!("--genesis chain.json"))?)?;
    let desc: serde_json::Value = serde_json::from_slice(&raw)?;
    let gd = desc.get("genesisData").and_then(|v| v.as_str()).ok_or_else(|| anyhow!("chain.json has no genesisData"))?;
    use base64::Engine;
    let g = base64::engine::general_purpose::STANDARD.decode(gd)?;
    let network = desc.get("networkID").and_then(|v| v.as_u64()).unwrap_or(1) as u32;
    let root: [u8; 32] = Sha256::digest(&g).into();
    Ok((g, network, root))
}

fn open_db(args: &[String], read_only: bool) -> Result<(DB, PathBuf)> {
    let dir = PathBuf::from(arg(args, "--data").ok_or_else(|| anyhow!("--data DIR"))?);
    let (_, _, root) = genesis(args)?;
    let cas = store::casfs::Store::open(&dir)?;
    let db = if read_only { DB::open_read_only(&dir, cas, root)? } else { DB::open(&dir, cas, root)? };
    Ok((db, dir))
}

struct Exec {
    ex: exec::Executor,
    parent_time: u64,
    first: bool,
}

impl Exec {
    fn new(args: &[String]) -> Result<Exec> {
        let (g, network, _) = genesis(args)?;
        let upgrade = match arg(args, "--upgrade") {
            Some(p) => std::fs::read(p)?,
            None => Vec::new(),
        };
        let cfg = exec::Config::from_genesis(&g, &upgrade, network).context("config")?;
        let parent_time = cfg.genesis_timestamp;
        Ok(Exec { ex: exec::Executor::new(cfg)?, parent_time, first: true })
    }
    fn run(&mut self, b: &block::Block) -> Result<exec::BlockResult> {
        if self.first {
            self.ex.set_block_hash(b.header.number - 1, b.header.parent_hash);
            self.first = false;
        }
        let r = self.ex.execute_block(b, self.parent_time).with_context(|| format!("block {}", b.header.number))?;
        if r.gas_used != b.header.gas_used || r.receipts_root != b.header.receipt_hash || r.bloom != b.header.bloom {
            bail!("block {}: execution does not match the header", b.header.number);
        }
        self.ex.set_block_hash(b.header.number, b.hash);
        self.parent_time = b.header.time;
        Ok(r)
    }
}

fn blocks(args: &[String]) -> Result<block::Recovered> {
    let dump = arg(args, "--dump").ok_or_else(|| anyhow!("--dump FILE"))?;
    let to: u64 = arg(args, "--to").map(|s| s.parse()).transpose()?.unwrap_or(u64::MAX);
    let workers: usize = arg(args, "--workers").map(|s| s.parse()).transpose()?.unwrap_or(8);
    Ok(block::recovered(block::Blocks::open(&dump, 1, to)?, workers))
}

fn write(args: &[String]) -> Result<()> {
    let (mut db, dir) = open_db(args, false)?;
    let crash_at: u64 = arg(args, "--crash-at").map(|s| s.parse()).transpose()?.unwrap_or(0);
    let mut ex = Exec::new(args)?;
    let head = db.next_height();
    eprintln!("store: opened {} at next height {head}", dir.display());
    let t0 = Instant::now();
    let (mut n, mut ntx) = (0u64, 0u64);
    for b in blocks(args)? {
        let b = b.map_err(|e| anyhow!("block decode: {e:?}"))?;
        let r = ex.run(&b)?;
        if b.height < head {
            continue; // already stored: re-execution only rebuilds the in-memory state
        }
        let bw = BlockWrite::from_exec(&b, &r)?;
        db.write_block(&bw)?;
        n += 1;
        ntx += b.txs.len() as u64;
        if b.height % 256 == 0 {
            db.sync()?;
        }
        if b.height == crash_at {
            // A torn tail: the block's records are in the file (the buffer
            // flushes at every block end); cut the end record in half and die.
            let p = dir.join("window").join("window.log");
            let len = std::fs::metadata(&p)?.len();
            let f = std::fs::OpenOptions::new().write(true).open(&p)?;
            f.set_len(len - 10)?;
            f.sync_all()?;
            eprintln!("store: CRASH at block {crash_at}: window log truncated by 10 bytes, exiting");
            std::process::exit(3);
        }
    }
    if flag(args, "--flush-end") {
        db.flush()?;
    }
    db.sync()?;
    eprintln!("store: wrote {n} blocks {ntx} txs in {:.1}s, head {:?}, {} runs", t0.elapsed().as_secs_f64(), db.head(), db.man.runs.len());
    Ok(())
}

fn expect<T: PartialEq + std::fmt::Debug>(what: &str, got: T, want: T) -> Result<()> {
    if got != want {
        bail!("{what}: got {got:?}, want {want:?}");
    }
    Ok(())
}

/// Re-executes the dump and checks every row family the store answers
/// against what the executor produced and the dump's containers.
fn verify(args: &[String]) -> Result<()> {
    let (db, _) = open_db(args, true)?;
    let every: u64 = arg(args, "--postings-every").map(|s| s.parse()).transpose()?.unwrap_or(1);
    let mut ex = Exec::new(args)?;
    let head = db.head().ok_or_else(|| anyhow!("empty store"))?;
    let t0 = Instant::now();
    let mut next_tx = 0u64;
    let (mut nblk, mut ntx, mut nstate, mut npost, mut ncode) = (0u64, 0u64, 0u64, 0u64, 0u64);
    for b in blocks(args)? {
        let b = b.map_err(|e| anyhow!("block decode: {e:?}"))?;
        let r = ex.run(&b)?;
        let h = b.height;
        if h > head {
            break;
        }
        let bw = BlockWrite::from_exec(&b, &r)?;
        expect(&format!("hdr/{h}"), db.header_rlp(h)?, Some(b.header_rlp.to_vec()))?;
        expect(&format!("pvm/{h}"), db.pvm(h)?, Some(bw.pvm.clone()))?;
        expect(&format!("container {h}"), db.container_at(h)?, Some(b.container.to_vec()))?;
        let (first, count) = db.block_tx_range(h)?.ok_or_else(|| anyhow!("blk/{h} missing"))?;
        expect(&format!("blk/{h}"), (first, count), (next_tx, b.txs.len() as u32))?;
        expect(&format!("blkh {h}"), db.height_by_hash(b.hash.as_slice())?, Some(h))?;
        expect(&format!("cid {h}"), db.height_by_container_id(b.container_id.as_slice())?, Some(h))?;
        expect(&format!("txnum_at_end_of {h}"), db.txnum_at_end_of(h)?, Some(first + count as u64))?;
        for (i, t) in bw.txs.iter().enumerate() {
            let n = first + i as u64;
            expect(&format!("tx/{n}"), db.tx_rlp(n)?, Some(t.rlp.clone()))?;
            expect(&format!("rcpt/{n}"), db.receipt(n)?, Some(t.receipt.clone()))?;
            store::receipts::decode(&t.receipt)?;
            expect(&format!("itx/{n}"), db.frames(n)?, Some(t.frames.clone()))?;
            expect(&format!("txh {n}"), db.txnum_by_hash(&t.hash)?, Some(n))?;
            expect(&format!("height_of_tx {n}"), db.height_of_tx(n)?, Some(h))?;
            let mut last: HashMap<Vec<u8>, &Vec<u8>> = HashMap::new();
            for row in &t.state {
                last.insert(row.key_prefix(), &row.val);
            }
            for (k, v) in &last {
                let got = match k[27] {
                    b'a' => db.account_at(&k[6..26], n)?,
                    b'c' => db.code_hash_at(&k[6..26], n)?,
                    _ => db.storage_at(&k[6..26], &k[29..61], n)?,
                };
                expect(&format!("state row {} at {n}", hex::encode(k)), got, Some((*v).clone()))?;
                nstate += 1;
            }
            if n % every == 0 {
                let mut want: std::collections::BTreeMap<Vec<u8>, u8> = std::collections::BTreeMap::new();
                let mut role = |a: Option<&[u8; 20]>, r: u8| {
                    if let Some(a) = a {
                        *want.entry(addr_prefix(a)).or_insert(0) |= r;
                    }
                };
                role(t.sender.as_ref(), ROLE_SENDER);
                role(t.to.as_ref(), ROLE_RECIPIENT);
                role(t.created.as_ref(), ROLE_CREATED);
                for a in &t.frame_addrs {
                    role(Some(a), ROLE_FRAME);
                }
                for l in &t.logs {
                    role(Some(&l.emitter), ROLE_EMITTER);
                }
                for l in &t.logs {
                    let topic0: &[u8] = l.topics.first().map(|t| &t[..]).unwrap_or(&[0u8; 32]);
                    if !l.topics.is_empty() {
                        want.entry(sig_group(topic0)).or_insert(0);
                    }
                    want.entry(elog_group(&l.emitter, topic0)).or_insert(0);
                    for (i, tp) in l.topics.iter().enumerate().skip(1).take(3) {
                        *want.entry(tval_group(tp, topic0)).or_insert(0) |= 1 << (i - 1);
                        let sk = set_key(&l.topics[0], i as u8, tp, &l.emitter);
                        let mut found = false;
                        db.set_scan(&sk, |k| {
                            found = k == sk.as_slice();
                            false
                        })?;
                        if !found {
                            bail!("set row {} missing", hex::encode(&sk));
                        }
                    }
                }
                for (g, p) in want {
                    let mut got = None;
                    db.postings(&g, n, n, |gg, nn, pp| {
                        if gg == g.as_slice() && nn == n {
                            got = Some(pp);
                        }
                        true
                    })?;
                    expect(&format!("posting {} at {n}", String::from_utf8_lossy(&g[..4])), got, Some(p))?;
                    npost += 1;
                }
            }
            ntx += 1;
        }
        let tail_at = first + count as u64;
        let mut last: HashMap<Vec<u8>, &Vec<u8>> = HashMap::new();
        for row in &bw.tail {
            last.insert(row.key_prefix(), &row.val);
        }
        for (k, v) in &last {
            let got = match k[27] {
                b'a' => db.account_at(&k[6..26], tail_at)?,
                b'c' => db.code_hash_at(&k[6..26], tail_at)?,
                _ => db.storage_at(&k[6..26], &k[29..61], tail_at)?,
            };
            expect(&format!("tail row {} at {tail_at}", hex::encode(k)), got, Some((*v).clone()))?;
            nstate += 1;
        }
        for (ch, blob) in &bw.code {
            expect(&format!("code {}", hex::encode(ch)), db.code(ch)?, Some(blob.clone()))?;
            ncode += 1;
        }
        next_tx = tail_at + 1;
        nblk += 1;
        if h % 10000 == 0 {
            eprintln!("verify: h={h} ok ({:.0}s)", t0.elapsed().as_secs_f64());
        }
    }
    if nblk == head {
        expect("next_tx", db.next_tx(), next_tx)?;
    }
    let head = head.min(nblk);
    // the sequential readers agree with the point reads
    let mut n = 0u64;
    db.chain_rows(FAM_HDR, 1, head, |h, v| {
        n += 1;
        if n != h {
            bail!("chain_rows hdr: gap at {h}");
        }
        Ok(v.len() > 100)
    })?;
    expect("chain_rows hdr count", n, head)?;
    let mut m = 0u64;
    db.chain_rows(FAM_TX, 0, next_tx, |_, _| {
        m += 1;
        Ok(true)
    })?;
    expect("chain_rows tx count", m, ntx)?;
    eprintln!("verify OK: {nblk} blocks, {ntx} txs, {nstate} state rows, {npost} posting checks, {ncode} code blobs, {} runs, {:.1}s", db.man.runs.len(), t0.elapsed().as_secs_f64());
    Ok(())
}

/// Reads every row family of a store through the API, no dump needed (a
/// joined dir reads through the chunk cache).
fn readall(args: &[String]) -> Result<()> {
    let (db, _) = open_db(args, true)?;
    let head = db.head().ok_or_else(|| anyhow!("empty store"))?;
    let to: u64 = arg(args, "--to").map(|s| s.parse()).transpose()?.unwrap_or(head).min(head);
    let t0 = Instant::now();
    let (mut ntx, mut nlog, mut nbytes) = (0u64, 0u64, 0u64);
    for h in 1..=to {
        let hdr = db.header_rlp(h)?.ok_or_else(|| anyhow!("hdr/{h} missing"))?;
        let c = db.container_at(h)?.ok_or_else(|| anyhow!("container {h} missing"))?;
        nbytes += c.len() as u64;
        expect(&format!("blkh {h}"), db.height_by_hash(&state::keccak::keccak256(&hdr))?, Some(h))?;
        let (first, count) = db.block_tx_range(h)?.ok_or_else(|| anyhow!("blk/{h} missing"))?;
        for n in first..first + count as u64 {
            let raw = db.tx_rlp(n)?.ok_or_else(|| anyhow!("tx/{n} missing"))?;
            expect(&format!("txh {n}"), db.txnum_by_hash(&state::keccak::keccak256(store::container::tx_envelope(&raw)))?, Some(n))?;
            let rc = store::receipts::decode(&db.receipt(n)?.ok_or_else(|| anyhow!("rcpt/{n} missing"))?)?;
            nlog += rc.logs.len() as u64;
            let fr = db.frames(n)?.ok_or_else(|| anyhow!("itx/{n} missing"))?;
            let v: serde_json::Value = serde_json::from_slice(&fr)?;
            let from = v["from"].as_str().ok_or_else(|| anyhow!("itx/{n}: no from"))?;
            let sender = hex::decode(&from[2..])?;
            let mut hit = false;
            db.postings(&addr_prefix(&sender), n, n, |_, nn, p| {
                hit = nn == n && p & ROLE_SENDER != 0;
                true
            })?;
            if !hit {
                bail!("addr posting for the sender of tx {n} missing");
            }
            // the sender's account has a row at or below this tx
            if db.account_at(&sender, n)?.is_none() {
                bail!("no account row for the sender of tx {n}");
            }
            ntx += 1;
        }
    }
    eprintln!("readall OK: {to} blocks, {ntx} txs, {nlog} logs, {nbytes} container bytes, {} runs, {:.1}s", db.man.runs.len(), t0.elapsed().as_secs_f64());
    Ok(())
}

fn publish(args: &[String]) -> Result<()> {
    let (mut db, _) = open_db(args, false)?;
    if !db.cas.remote() {
        bail!("publish: EPOCHDB_S3_ENDPOINT is not set");
    }
    db.publish()?;
    let released = db.sync_artifacts()?;
    eprintln!("publish OK: {} runs in the manifest, {} artifacts uploaded and released", db.man.runs.len(), released.len());
    // still serving after the release: every run reads through the chunk cache now
    let head = db.head().ok_or_else(|| anyhow!("empty"))?;
    db.header_rlp(head)?.ok_or_else(|| anyhow!("head header unreadable after release"))?;
    Ok(())
}

fn join(args: &[String]) -> Result<()> {
    let dir = PathBuf::from(arg(args, "--data").ok_or_else(|| anyhow!("--data DIR"))?);
    let (_, _, root) = genesis(args)?;
    let cas = store::casfs::Store::open(&dir)?;
    store::db::join(&cas, &dir, root)?;
    let db = DB::open_read_only(&dir, cas, root)?;
    eprintln!("join OK: head {:?}, {} runs, next tx {}", db.head(), db.man.runs.len(), db.next_tx());
    Ok(())
}

fn probe(args: &[String]) -> Result<()> {
    let dir = PathBuf::from(arg(args, "--data").ok_or_else(|| anyhow!("--data"))?);
    let cas = store::casfs::Store::local(&dir)?;
    let man = store::db::Manifest::load(&dir)?;
    for r in &man.runs {
        let run = store::run::Run::open(&cas, &r.name)?;
        println!("run {} level {} tx [{},{}) blocks [{},{}]", r.name, r.level, run.footer.from_tx, run.footer.to_tx, run.footer.from_height, run.footer.to_height);
        for (i, s) in run.sec.iter().enumerate() {
            let mut n = 0u64;
            run.scan_range(SECTIONS[i], &[], None, |_, _| {
                n += 1;
                true
            })?;
            println!("  section {i}: {} bytes, {} data blocks, index {}, filter {} bytes, {n} rows", s.len, s.num_data_blocks(), if s.two_level { "two-level" } else { "single" }, s.filter.as_ref().map(|f| f.len()).unwrap_or(0));
        }
    }
    Ok(())
}

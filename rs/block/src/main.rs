//! blockcheck: the oracles over a dump. Parent-hash chain, header numbers,
//! container ids against the archive index, senders to a file, throughput.
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::time::Instant;

use block::{recovered, Blocks};

fn main() {
    let mut args = std::env::args().skip(1);
    let mut dump = None;
    let (mut from, mut to, mut workers) = (1u64, u64::MAX, 0usize);
    let (mut no_recover, mut index, mut senders, mut show) = (false, None, None, Vec::new());
    while let Some(a) = args.next() {
        match a.as_str() {
            "--from" => from = args.next().unwrap().parse().unwrap(),
            "--to" => to = args.next().unwrap().parse().unwrap(),
            "--workers" => workers = args.next().unwrap().parse().unwrap(),
            "--no-recover" => no_recover = true,
            "--index" => index = Some(args.next().unwrap()),
            "--senders" => senders = Some(args.next().unwrap()),
            "--show" => show = args.next().unwrap().split(',').map(|s| s.parse::<u64>().unwrap()).collect(),
            _ => dump = Some(a),
        }
    }
    let Some(dump) = dump else {
        eprintln!("usage: blockcheck <dump> [--from N] [--to N] [--workers N] [--no-recover] [--index FILE] [--senders FILE] [--show h1,h2]");
        std::process::exit(2);
    };
    if workers == 0 {
        workers = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(2).saturating_sub(2).max(1);
    }
    let index = index.map(|p| {
        let f = File::open(&p).expect("index");
        unsafe { memmap2::Mmap::map(&f).expect("mmap index") }
    });
    let mut senders = senders.map(|p| BufWriter::new(File::create(p).expect("senders file")));

    let blocks = Blocks::open(&dump, from, to).expect("open dump");
    let iter: Box<dyn Iterator<Item = Result<block::Block, block::Error>>> =
        if no_recover { Box::new(blocks) } else { Box::new(recovered(blocks, workers)) };

    let t0 = Instant::now();
    let (mut n, mut ntx, mut nosender, mut idmiss, mut bare, mut gas) = (0u64, 0u64, 0u64, 0u64, 0u64, 0u64);
    let mut types: HashMap<u8, u64> = HashMap::new();
    let mut prev: Option<(u64, alloy_primitives::B256)> = None;
    for b in iter {
        let b = match b {
            Ok(b) => b,
            Err(e) => {
                eprintln!("FAIL: {e}");
                std::process::exit(1);
            }
        };
        if let Some((ph, phash)) = prev {
            if ph + 1 != b.height || b.header.parent_hash != phash {
                eprintln!("FAIL: height {} parent {} does not link to height {} hash {}", b.height, b.header.parent_hash, ph, phash);
                std::process::exit(1);
            }
        }
        prev = Some((b.height, b.hash));
        if b.pvm.is_none() {
            bare += 1;
        }
        gas += b.header.gas_used;
        if let Some(idx) = &index {
            if !index_has(idx, &b.container_id.0, b.height) {
                idmiss += 1;
                if idmiss <= 10 {
                    eprintln!("index miss: height {} id {}", b.height, b.container_id);
                }
            }
        }
        if show.contains(&b.height) {
            println!("height {} hash {} container_id {} pvm {} txs {} len {}", b.height, b.hash, b.container_id, b.pvm.is_some(), b.txs.len(), b.container.len());
        }
        for (i, t) in b.txs.iter().enumerate() {
            *types.entry(t.tx_type).or_default() += 1;
            if t.sender.is_none() {
                nosender += 1;
            }
            if let Some(w) = &mut senders {
                writeln!(w, "{} {} {} {}", b.height, i, t.hash, t.sender.map(|a| a.to_string()).unwrap_or_default()).unwrap();
            }
        }
        ntx += b.txs.len() as u64;
        n += 1;
    }
    if let Some(w) = &mut senders {
        w.flush().unwrap();
    }
    let dt = t0.elapsed().as_secs_f64();
    println!(
        "blocks={n} txs={ntx} gas={gas} bare={bare} types={types:?} no_sender={nosender} index_miss={idmiss} workers={workers} recover={} secs={dt:.2} blk/s={:.0} tx/s={:.0}",
        !no_recover,
        n as f64 / dt,
        ntx as f64 / dt
    );
    if idmiss > 0 || (!no_recover && nosender > 0) {
        std::process::exit(1);
    }
}

/// index_has looks the id up in the archive server's index: [32B id][8B BE
/// height] records sorted by id.
fn index_has(idx: &[u8], id: &[u8; 32], height: u64) -> bool {
    let n = idx.len() / 40;
    let (mut lo, mut hi) = (0usize, n);
    while lo < hi {
        let mid = (lo + hi) / 2;
        if idx[mid * 40..mid * 40 + 32] < id[..] {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    lo < n && &idx[lo * 40..lo * 40 + 32] == id && u64::from_be_bytes(idx[lo * 40 + 32..lo * 40 + 40].try_into().unwrap()) == height
}

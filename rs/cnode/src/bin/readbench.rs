//! readbench --data <dir> --bootstrap <export dir> [--threads 8] [--seconds 30] [--http URL] [--ws URL]
//! Acceptance 4: random reads of hot keys (accounts and slots touched by the
//! last blocks in the history) from N threads while the node follows the tip.
//! Reports ns per read per thread and the applier's received->applied gap.
use cnode::{Config, Mode, Node};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

fn arg(a: &[String], k: &str) -> Option<String> {
    a.iter().position(|x| x == k).map(|i| a[i + 1].clone())
}

fn main() {
    let a: Vec<String> = std::env::args().collect();
    let threads: usize = arg(&a, "--threads").map_or(8, |s| s.parse().unwrap());
    let secs: u64 = arg(&a, "--seconds").map_or(30, |s| s.parse().unwrap());
    let cfg = Config {
        rpc_ws: arg(&a, "--ws").unwrap_or_else(|| "ws://127.0.0.1:9650/ext/bc/C/ws".into()),
        rpc_http: arg(&a, "--http").unwrap_or_else(|| "http://127.0.0.1:9650/ext/bc/C/rpc".into()),
        validator_ws: vec![],
        data_dir: PathBuf::from(arg(&a, "--data").expect("--data")),
        bootstrap_dir: PathBuf::from(arg(&a, "--bootstrap").expect("--bootstrap")),
        checker_lag_blocks: 60,
        snapshot_every_blocks: 4000,
        dump_every_blocks: 0,
    };
    let node = Arc::new(Node::open(cfg, Mode::Tip).expect("open"));
    // Hot keys: everything the last 50 blocks touched.
    let head = node.history.head().unwrap().unwrap();
    let mut accts: Vec<[u8; 32]> = Vec::new();
    let mut slots: Vec<([u8; 32], [u8; 32])> = Vec::new();
    for h in head.saturating_sub(50)..=head {
        for (k, _) in node.history.diff(h).unwrap().unwrap_or_default() {
            match k.len() {
                33 => accts.push(k[..32].try_into().unwrap()),
                65 => slots.push((k[..32].try_into().unwrap(), k[33..].try_into().unwrap())),
                _ => {}
            }
        }
    }
    println!("hot set: {} accounts, {} slots from blocks {}..={head}; {threads} threads for {secs} s while following", accts.len(), slots.len(), head.saturating_sub(50));
    let stop = Arc::new(AtomicBool::new(false));
    let mut hs = Vec::new();
    for th in 0..threads {
        let (node, stop, accts, slots) = (node.clone(), stop.clone(), accts.clone(), slots.clone());
        hs.push(std::thread::spawn(move || {
            let mut x = 0x9E3779B97F4A7C15u64 ^ (th as u64 + 1);
            let (mut n, mut stale, mut sink) = (0u64, 0u64, 0u64);
            let mut g = node.generation();
            let t = Instant::now();
            while !stop.load(Ordering::Relaxed) {
                for _ in 0..1000 {
                    x ^= x << 13;
                    x ^= x >> 7;
                    x ^= x << 17;
                    let r = if x & 1 == 0 {
                        node.account_by_hash(g, &accts[(x >> 1) as usize % accts.len()]).map(|a| a.map_or(0, |a| a.nonce))
                    } else {
                        let (a, s) = &slots[(x >> 1) as usize % slots.len()];
                        node.storage_by_hash(g, a, s).map(|v| v.as_limbs()[0])
                    };
                    match r {
                        Ok(v) => sink = sink.wrapping_add(v),
                        Err(_) => {
                            stale += 1;
                            g = node.generation();
                        }
                    }
                    n += 1;
                }
            }
            std::hint::black_box(sink);
            (n, stale, t.elapsed())
        }));
    }
    let rx = node.subscribe_blocks();
    let deadline = Instant::now() + Duration::from_secs(secs);
    let mut gaps = Vec::new();
    while let Ok(ev) = rx.recv_timeout(deadline.saturating_duration_since(Instant::now())) {
        gaps.push((ev.applied_ms - ev.received_ms, ev.exec_ms, ev.txs));
    }
    stop.store(true, Ordering::Relaxed);
    let mut total = 0u64;
    for h in hs {
        let (n, stale, d) = h.join().unwrap();
        total += n;
        println!("thread: {n} reads, {stale} stale, {:.1} ns/read", d.as_nanos() as f64 / n as f64);
    }
    println!("aggregate {:.1} M reads/s; {} blocks applied: gaps received->applied ms {:?}", total as f64 / secs as f64 / 1e6, gaps.len(), gaps);
    println!("status {:?} checker lag {}", node.status(), node.checker_lag());
}

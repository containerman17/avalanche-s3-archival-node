//! cnode-run --data <dir> --bootstrap <export dir> [--http URL] [--ws URL] [--validators ws1,ws2] [--at HEIGHT] [--roll-every N] [--dump-every N]
//! Runs the node as a process (the library's host is normally the bot) and
//! prints one status line per block and every 10 s.
use cnode::{Config, Mode, Node, Status};
use std::path::PathBuf;

fn arg(a: &[String], k: &str) -> Option<String> {
    a.iter().position(|x| x == k).map(|i| a[i + 1].clone())
}

fn main() {
    let a: Vec<String> = std::env::args().collect();
    let cfg = Config {
        rpc_ws: arg(&a, "--ws").unwrap_or_else(|| "ws://127.0.0.1:9650/ext/bc/C/ws".into()),
        rpc_http: arg(&a, "--http").unwrap_or_else(|| "http://127.0.0.1:9650/ext/bc/C/rpc".into()),
        validator_ws: arg(&a, "--validators").map_or(vec![], |v| v.split(',').map(String::from).collect()),
        data_dir: PathBuf::from(arg(&a, "--data").expect("--data")),
        bootstrap_dir: PathBuf::from(arg(&a, "--bootstrap").expect("--bootstrap")),
        checker_lag_blocks: 60,
        snapshot_every_blocks: arg(&a, "--roll-every").map_or(4000, |s| s.parse().unwrap()),
        dump_every_blocks: arg(&a, "--dump-every").map_or(2000, |s| s.parse().unwrap()),
    };
    let mode = match arg(&a, "--at") {
        Some(h) => Mode::AtHeight(h.parse().unwrap()),
        None => Mode::Tip,
    };
    let node = match Node::open(cfg, mode) {
        Ok(n) => n,
        Err(e) => {
            eprintln!("open: {e:#}");
            std::process::exit(2);
        }
    };
    eprintln!("open: {:?}", node.status());
    if let Mode::AtHeight(_) = mode {
        return;
    }
    let rx = node.subscribe_blocks();
    let mut last = std::time::Instant::now();
    let mut gaps: Vec<u64> = Vec::new();
    loop {
        match rx.recv_timeout(std::time::Duration::from_secs(10)) {
            Ok(ev) => {
                gaps.push(ev.applied_ms - ev.received_ms);
                if last.elapsed().as_secs() >= 10 {
                    gaps.sort_unstable();
                    let p = |q: f64| gaps[((gaps.len() - 1) as f64 * q) as usize];
                    eprintln!("block {} txs {} exec {} ms received->applied {} ms | last {} blocks: p50 {} p99 {} ms | checker lag {} blocks | {:?}", ev.height, ev.txs, ev.exec_ms, ev.applied_ms - ev.received_ms, gaps.len(), p(0.5), p(0.99), node.checker_lag(), node.status());
                    gaps.clear();
                    last = std::time::Instant::now();
                }
            }
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => eprintln!("idle: {:?} checker lag {}", node.status(), node.checker_lag()),
            Err(_) => break,
        }
        if let Status::Halted { .. } = node.status() {
            eprintln!("HALTED: {:?}", node.status());
            std::process::exit(3);
        }
    }
}

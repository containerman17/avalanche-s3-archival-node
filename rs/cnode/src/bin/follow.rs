//! follow <ws url> <http url> [seconds]: print every accepted head as it arrives
//! (receive time, height, txs, atomic extra data bytes, fetch time).
use cnode::feed;
use std::sync::mpsc;
use std::time::Duration;

fn main() {
    let a: Vec<String> = std::env::args().collect();
    let secs: u64 = a.get(3).map_or(30, |s| s.parse().unwrap());
    let (tx, rx) = mpsc::channel();
    let (ws, http) = (a[1].clone(), a[2].clone());
    std::thread::spawn(move || {
        if let Err(e) = feed::follow(&ws, &http, &tx) {
            eprintln!("feed: {e:#}");
        }
    });
    let deadline = std::time::Instant::now() + Duration::from_secs(secs);
    while let Ok(b) = rx.recv_timeout(deadline.saturating_duration_since(std::time::Instant::now())) {
        let txs = b.block["transactions"].as_array().map_or(0, |t| t.len());
        let extra = b.block["blockExtraData"].as_str().map_or(0, |s| (s.len() - 2) / 2);
        let ts = feed::hex_u64(&b.block["timestamp"]).unwrap_or(0);
        println!("{} h={} txs={} extdata={}B block_ts_lag={}ms fetched_in={}ms hash={}", b.received_ms, b.height, txs, extra, b.received_ms as i64 - (ts * 1000) as i64, feed::now_ms() - b.received_ms, &b.hash[..10]);
    }
}

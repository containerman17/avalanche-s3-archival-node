//! pendingcheck <seconds> <ws url>...: count pending txs seen per source, dedup across sources.
use cnode::mempool::Mempool;
fn main() {
    let a: Vec<String> = std::env::args().collect();
    let secs: u64 = a[1].parse().unwrap();
    let m = Mempool::new(60_000);
    for (i, ws) in a[2..].iter().enumerate() {
        let (m, ws) = (m.clone(), ws.clone());
        std::thread::spawn(move || m.run(i, ws));
    }
    let rx = m.subscribe();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(secs);
    let mut per = vec![0u64; a.len() - 2];
    let mut n = 0;
    while let Ok(ev) = rx.recv_timeout(deadline.saturating_duration_since(std::time::Instant::now())) {
        per[ev.source] += 1;
        n += 1;
        if n <= 3 {
            println!("{} src {} {} from {} to {} gas {}", ev.received_ms, ev.source, ev.hash, ev.tx["from"], ev.tx["to"], ev.tx["gas"]);
        }
    }
    println!("{secs}s: {n} distinct pending txs, first-seen per source {per:?}, flush bytes {}", m.flush().len());
}

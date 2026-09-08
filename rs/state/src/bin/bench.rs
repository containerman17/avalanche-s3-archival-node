//! Release-mode benches mirroring the Go ones.
//!
//!   bench latest <rows file>   run bytes/key, Get 1 thread and 16 threads, Merge, Overlay heap
//!   bench dirty                4M synthetic state (commit_test's benchState shape): Roll, Dirty 20k serial and parallel
use state::commit::dirty::{Dirty, SeekFn};
use state::commit::file::File;
use state::commit::roll::roll;
use state::overlay::Overlay;
use state::run::{Run, Writer};
use state::sample::*;
use state::view::{merge, View};
use state::RowsIter;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

fn tmpdir() -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!("rs-state-bench-{}", std::process::id()));
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn latest(rows_path: &Path) {
    let dir = tmpdir();
    let mut rows = read_rows(rows_path).unwrap();
    rows.sort_unstable_by(|a, b| a.0.cmp(&b.0));
    rows.dedup_by(|a, b| a.0 == b.0);
    let path = dir.join("a.run");
    let t = Instant::now();
    let mut w = Writer::create(&path).unwrap();
    for (k, v) in &rows {
        w.add(k, v).unwrap();
    }
    w.close().unwrap();
    let write_wall = t.elapsed();
    let r = Run::open(&path).unwrap();
    println!("run: {} entries, {} blocks, {:.2} B/key, written in {:.2?} ({:.2}M entries/s incl. fsync)", r.len(), r.blocks(), r.bytes() as f64 / rows.len() as f64, write_wall, rows.len() as f64 / write_wall.as_secs_f64() / 1e6);

    let mut rng = Rng::new(3);
    let keys: Vec<&[u8]> = (0..1 << 20).map(|_| rows[rng.below(rows.len())].0.as_slice()).collect();
    let n = 4_000_000usize;
    let t = Instant::now();
    let mut sink = 0usize;
    for i in 0..n {
        sink += r.get(keys[i & (keys.len() - 1)]).map_or(0, |v| v.len());
    }
    let wall = t.elapsed();
    println!("Get 1 thread: {:.0} ns/op ({sink})", wall.as_nanos() as f64 / n as f64);

    for threads in [8usize, 16] {
        let per = 2_000_000usize;
        let t = Instant::now();
        std::thread::scope(|s| {
            for th in 0..threads {
                let (r, keys) = (&r, &keys);
                s.spawn(move || {
                    let mut i = th * 7919;
                    let mut sink = 0usize;
                    for _ in 0..per {
                        sink += r.get(keys[i & (keys.len() - 1)]).map_or(0, |v| v.len());
                        i += 1;
                    }
                    std::hint::black_box(sink);
                });
            }
        });
        let wall = t.elapsed();
        println!("Get {threads} threads: {:.1} ns/op aggregate", wall.as_nanos() as f64 / (per * threads) as f64);
    }

    let mut o = Overlay::new();
    let mut want = 0;
    for (i, (k, v)) in rows.iter().enumerate() {
        if i % 100 == 0 {
            o.put(k, b"rewritten");
        }
        if i % 100 == 0 || !v.is_empty() {
            want += 1;
        }
    }
    let v = View::new(Some(&o), &[&r]);
    for round in 0..3 {
        let t = Instant::now();
        let m = merge(&dir.join("m.run"), &v, [0; 32]).unwrap();
        let wall = t.elapsed();
        assert_eq!(m.len(), want);
        println!("Merge round {round}: {:.2}M entries/s incl. fsync ({:.2?})", rows.len() as f64 / wall.as_secs_f64() / 1e6, wall);
    }

    let mut o = Overlay::new();
    let n = rows.len().min(1_000_000);
    for (k, v) in &rows[..n] {
        o.put(k, v);
    }
    let kv: usize = rows[..n].iter().map(|(k, v)| k.len() + v.len()).sum();
    println!("Overlay: {} entries, {:.1} B/entry heap ({:.1} B/entry key+value), accounted {:.1} B/entry", o.len(), o.heap_bytes() as f64 / n as f64, kv as f64 / n as f64, o.bytes() as f64 / n as f64);
    let _ = std::fs::remove_dir_all(&dir);
}

/// 4M keys shaped like mainnet C: ten contracts hold 94% of the slots (the
/// top one 41%), the rest are small accounts.
fn bench_state() -> Vec<Row> {
    let mut rng = Rng::new(9);
    let total = 4_000_000usize;
    let shares = [0.41, 0.20, 0.10, 0.07, 0.05, 0.04, 0.03, 0.02, 0.01, 0.01];
    let slots = (total as f64 * 0.95) as usize;
    let mut rows: Vec<Row> = Vec::with_capacity(total);
    let mut used = 0;
    let acctv = |rng: &mut Rng| {
        let nonce = (rng.below(100) + 1) as u64;
        let bal = rng.u64();
        contract_row(&nonce.to_be_bytes()[7..], &bal.to_be_bytes()[(bal.leading_zeros() / 8) as usize..], &EMPTY_CODE_HASH)
    };
    for s in shares {
        let h = rng.bytes(32);
        rows.push(([h.clone(), vec![0]].concat(), acctv(&mut rng)));
        let n = (slots as f64 * s) as usize;
        for _ in 0..n {
            let sh = rng.bytes(32);
            let w = rng.word();
            rows.push(([h.clone(), vec![1], sh].concat(), trim_word(&w)));
        }
        used += n + 1;
    }
    while used < total {
        let h = rng.bytes(32);
        rows.push(([h.clone(), vec![0]].concat(), acctv(&mut rng)));
        used += 1;
        let mut i = rng.below(3);
        while i > 0 && used < total {
            let sh = rng.bytes(32);
            let w = rng.word();
            rows.push(([h.clone(), vec![1], sh].concat(), trim_word(&w)));
            used += 1;
            i -= 1;
        }
    }
    rows.sort_unstable_by(|a, b| a.0.cmp(&b.0));
    rows.dedup_by(|a, b| a.0 == b.0);
    rows
}

fn seek_fn(rows: Arc<Vec<Row>>) -> Arc<SeekFn> {
    Arc::new(move |prefix: &[u8]| {
        let i = rows.partition_point(|r| r.0.as_slice() < prefix);
        rows.get(i).cloned()
    })
}

fn dirty() {
    let dir = tmpdir();
    let rows = Arc::new(bench_state());
    let path = dir.join("nodes");
    let t = Instant::now();
    let (_, st) = roll(&mut RowsIter::new(&rows), &path, [0; 32]).unwrap();
    let wall = t.elapsed();
    println!("roll 4M synthetic: keys {} nodes {} bytes {} in {:.2?}: {:.0} keys/s {:.2}M keccak/s {:.1} B/key", st.keys, st.nodes, st.bytes, wall, st.keys as f64 / wall.as_secs_f64(), (st.keys + st.nodes) as f64 / wall.as_secs_f64() / 1e6, st.bytes as f64 / st.keys as f64);
    let f = Arc::new(File::open(&path).unwrap());
    let mut rng = Rng::new(11);
    for workers in [1usize, 16, 1, 16, 1, 16] {
        let mut d = Dirty::new(f.clone(), seek_fn(rows.clone()));
        d.workers = workers;
        for j in 0..20_000u64 {
            let r = &rows[rng.below(rows.len())];
            if r.0.len() == 65 {
                let w = rng.word();
                d.apply(&r.0, &trim_word(&w)).unwrap();
            } else {
                let v = contract_row(&j.to_be_bytes()[(j.leading_zeros() / 8) as usize..], &[1], &EMPTY_CODE_HASH);
                d.apply(&r.0, &v).unwrap();
            }
        }
        let t = Instant::now();
        let root = d.root().unwrap();
        let wall = t.elapsed();
        println!("Dirty 20k workers={workers}: {:.1} ms, {:.0} B/key retained, {} nodes, root {:02x?}..", wall.as_secs_f64() * 1e3, d.bytes() as f64 / 20_000.0, d.nodes(), &root[..4]);
    }
    let _ = std::fs::remove_dir_all(&dir);
}

fn main() {
    let a: Vec<String> = std::env::args().collect();
    match a[1].as_str() {
        "latest" => latest(Path::new(&a[2])),
        "dirty" => dirty(),
        _ => panic!("bench latest <rows> | bench dirty"),
    }
}

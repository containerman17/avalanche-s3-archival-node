//! The Rust side of the cross-language oracle; exp/rscompat is the Go side.
//! Rows files are [klen u8][vlen u8][key][value].
//!
//!   gen <seed> <accounts> <ops> <dir>   rows.bin, updates.bin, rows2.bin (state after the updates)
//!   runwrite <rows> <out.run>
//!   runcheck <rows> <run>
//!   roll <rows> <out nodes>
//!   dirty <rows> <updates>
//!   noderoot <nodes>
//!   rollsample <statedump> <out nodes>   convert like exp/commitroot -sample, roll, time it
use state::commit::dirty::Dirty;
use state::commit::file::File;
use state::commit::roll::roll;
use state::run::{Run, Writer};
use state::sample::*;
use state::{KvIter, RowsIter};
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn seek_fn(rows: Arc<Vec<Row>>) -> Arc<state::commit::dirty::SeekFn> {
    Arc::new(move |prefix: &[u8]| {
        let i = rows.partition_point(|r| r.0.as_slice() < prefix);
        rows.get(i).cloned()
    })
}

fn main() {
    let a: Vec<String> = std::env::args().collect();
    match a[1].as_str() {
        "gen" => {
            let mut rng = Rng::new(a[2].parse().unwrap());
            let mut m = gen_state(&mut rng, a[3].parse().unwrap());
            let dir = Path::new(&a[5]);
            write_rows(&dir.join("rows.bin"), &flatten(&m)).unwrap();
            let ups = mutate(&mut m, &mut rng, &[0, 1, 2, 3, 4, 5, 6], a[4].parse().unwrap());
            write_rows(&dir.join("updates.bin"), &ups).unwrap();
            write_rows(&dir.join("rows2.bin"), &flatten(&m)).unwrap();
            println!("{} accounts, {} updates", m.len(), ups.len());
        }
        "runwrite" => {
            let mut rows = read_rows(Path::new(&a[2])).unwrap();
            rows.sort_unstable_by(|x, y| x.0.cmp(&y.0));
            let mut w = Writer::create(Path::new(&a[3])).unwrap();
            w.set_user_data(std::array::from_fn(|i| i as u8));
            for (k, v) in &rows {
                w.add(k, v).unwrap();
            }
            w.close().unwrap();
            println!("wrote {} entries", rows.len());
        }
        "runcheck" => {
            let mut rows = read_rows(Path::new(&a[2])).unwrap();
            rows.sort_unstable_by(|x, y| x.0.cmp(&y.0));
            let r = Run::open(Path::new(&a[3])).unwrap();
            assert_eq!(r.len(), rows.len(), "Len");
            for (k, v) in &rows {
                assert_eq!(r.get(k), Some(v.as_slice()), "Get {}", hex(k));
                let mut k2 = k.clone();
                *k2.last_mut().unwrap() ^= 1;
                if rows.binary_search_by(|r| r.0.cmp(&k2)).is_err() {
                    assert!(r.get(&k2).is_none(), "Get absent {}", hex(&k2));
                }
            }
            let mut it = r.iter(None, None);
            let mut n = 0;
            while it.next() {
                assert!(n < rows.len() && it.key() == rows[n].0 && it.value() == rows[n].1, "Iter entry {n}");
                n += 1;
            }
            assert_eq!(n, rows.len(), "Iter count");
            if rows.len() > 10 {
                let (lo, hi) = (&rows[rows.len() / 4].0, &rows[rows.len() / 2].0);
                let mut it = r.iter(Some(lo), Some(hi));
                let mut n = 0;
                while it.next() {
                    n += 1;
                }
                assert_eq!(n, rows.len() / 2 - rows.len() / 4, "bounded Iter");
            }
            println!("OK {} entries, user {}", r.len(), hex(&r.user_data()));
        }
        "roll" => {
            let rows = read_rows(Path::new(&a[2])).unwrap();
            let (root, st) = roll(&mut RowsIter::new(&rows), Path::new(&a[3]), { let mut u = [0u8; 32]; u[0] = 7; u }).unwrap();
            println!("root {} keys {} nodes {} bytes {}", hex(&root), st.keys, st.nodes, st.bytes);
        }
        "dirty" => {
            let rows = Arc::new(read_rows(Path::new(&a[2])).unwrap());
            let tmp = std::env::temp_dir().join(format!("rscompat-nodes-{}", std::process::id()));
            let (root, _) = roll(&mut RowsIter::new(&rows), &tmp, [0; 32]).unwrap();
            let f = Arc::new(File::open(&tmp).unwrap());
            let mut d = Dirty::new(f, seek_fn(rows.clone()));
            for (k, v) in read_rows(Path::new(&a[3])).unwrap() {
                d.apply(&k, &v).unwrap();
            }
            let got = d.root().unwrap();
            println!("rolled {}\ndirty {}\nretained {}", hex(&root), hex(&got), d.bytes());
            let _ = std::fs::remove_file(&tmp);
        }
        "noderoot" => {
            let f = File::open(Path::new(&a[2])).unwrap();
            println!("root {} nodes {} keys {} size {}", hex(&f.root()), f.node_count(), f.key_count(), f.size());
        }
        "rollsample" => {
            let t0 = Instant::now();
            let dump = read_rows(Path::new(&a[2])).unwrap();
            let rows = to_contract_rows(&dump);
            eprintln!("sample: {} rows converted and sorted in {:.2?}", rows.len(), t0.elapsed());
            let t1 = Instant::now();
            let (root, st) = roll(&mut RowsIter::new(&rows), Path::new(&a[3]), [0; 32]).unwrap();
            let wall = t1.elapsed();
            eprintln!(
                "roll: {} keys {} nodes {} bytes in {:.2?}: {:.0} keys/s {:.0} keccak/s {:.1} MB/s {:.1} B/key",
                st.keys, st.nodes, st.bytes, wall,
                st.keys as f64 / wall.as_secs_f64(), (st.keys + st.nodes) as f64 / wall.as_secs_f64(),
                st.bytes as f64 / wall.as_secs_f64() / 1e6, st.bytes as f64 / st.keys as f64
            );
            println!("root {} keys {} nodes {} bytes {}", hex(&root), st.keys, st.nodes, st.bytes);
        }
        _ => panic!("unknown mode"),
    }
}

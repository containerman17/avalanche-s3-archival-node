//! bench2 <contracts> <slots_per_contract> [hot_contracts]: run-file-over-mmap vs in-heap maps.
//! Keys are keccak-shaped (contract 32 B ++ slot 32 B). Patterns: cold (uniform over all keys) and
//! hot (hot_contracts contracts, half of their slots each, the DeFi shape). 1 thread and 8 threads.
use state::run::{Run, Writer};
use state::sample::Rng;
use std::time::Instant;

#[derive(Default, Clone)]
struct Take8;
struct Take8H(u64);
impl std::hash::Hasher for Take8H {
    fn finish(&self) -> u64 { self.0 }
    fn write(&mut self, b: &[u8]) { for c in b.chunks_exact(8) { self.0 ^= u64::from_le_bytes(c.try_into().unwrap()); } }
    fn write_usize(&mut self, _: usize) {}
}
impl std::hash::BuildHasher for Take8 { type Hasher = Take8H; fn build_hasher(&self) -> Take8H { Take8H(0) } }
type H = [u8; 32];
type Key = [u8; 64];
type Val = [u8; 32];
type Map<K, V> = hashbrown::HashMap<K, V, Take8>;

fn measure<F: Fn(&Key) -> usize + Sync>(name: &str, keys: &[Key], f: F) {
    let n = 4_000_000usize;
    let t = Instant::now();
    let mut sink = 0;
    for i in 0..n { sink += f(&keys[i % keys.len()]); }
    let one = t.elapsed().as_nanos() as f64 / n as f64;
    let per = 2_000_000usize;
    let t = Instant::now();
    std::thread::scope(|s| for th in 0..8 { let (f, keys) = (&f, &keys); s.spawn(move || { let mut i = th * 7919; let mut sink = 0; for _ in 0..per { sink += f(&keys[i % keys.len()]); i += 1; } std::hint::black_box(sink); }); });
    let eight = t.elapsed().as_nanos() as f64 / (per * 8) as f64;
    println!("{name:24} 1 thread {one:5.0} ns   8 threads {eight:5.1} ns/op aggregate   ({sink})");
}

fn main() {
    let a: Vec<String> = std::env::args().collect();
    let nc: usize = a[1].parse().unwrap();
    let ns: usize = a[2].parse().unwrap();
    let hc: usize = a.get(3).map_or(100, |s| s.parse().unwrap());
    let mut rng = Rng::new(7);
    let w = |rng: &mut Rng| { let mut h = [0u8; 32]; rng.fill(&mut h); h };
    let contracts: Vec<H> = (0..nc).map(|_| w(&mut rng)).collect();
    let mut rows: Vec<(Key, Val)> = Vec::with_capacity(nc * ns);
    for c in &contracts { for _ in 0..ns { let mut k = [0u8; 64]; k[..32].copy_from_slice(c); k[32..].copy_from_slice(&w(&mut rng)); rows.push((k, w(&mut rng))); } }
    rows.sort_unstable_by(|a, b| a.0.cmp(&b.0));
    assert!(rows.windows(2).all(|p| p[0].0 < p[1].0));
    let dir = std::env::temp_dir().join(format!("bench2-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("a.run");
    let t = Instant::now();
    let mut w = Writer::create(&path).unwrap();
    for (k, v) in &rows { w.add(k, v).unwrap(); }
    w.close().unwrap();
    let r = Run::open(&path).unwrap();
    println!("rows {} ({nc} contracts x {ns} slots) run {} B/key written {:.1?}", rows.len(), r.bytes() / rows.len(), t.elapsed());
    let t = Instant::now();
    let boxed: hashbrown::HashMap<Vec<u8>, Vec<u8>, foldhash::fast::RandomState> = rows.iter().map(|(k, v)| (k.to_vec(), v.to_vec())).collect();
    let flat: Map<Key, Val> = rows.iter().map(|(k, v)| (*k, *v)).collect();
    let mut nested: Map<H, Map<H, Val>> = Map::default();
    for (k, v) in &rows { nested.entry(k[..32].try_into().unwrap()).or_default().insert(k[32..].try_into().unwrap(), *v); }
    println!("maps built {:.1?}", t.elapsed());

    let cold: Vec<Key> = (0..1 << 18).map(|_| rows[rng.below(rows.len())].0).collect();
    let mut hot: Vec<Key> = Vec::new();
    for _ in 0..hc { let c = rng.below(nc); for _ in 0..ns / 2 { hot.push(rows[c * ns + rng.below(ns)].0); } }
    for i in (1..hot.len()).rev() { hot.swap(i, rng.below(i + 1)); }
    for (pat, keys) in [("cold", &cold), ("hot", &hot)] {
        println!("-- {pat}: {} keys, {} contracts", keys.len(), if pat == "hot" { hc } else { nc });
        measure("run file mmap", keys, |k| r.get(k).map_or(0, |v| v.len()));
        measure("map boxed foldhash", keys, |k| boxed.get(k.as_slice()).map_or(0, |v| v.len()));
        measure("map flat inline", keys, |k| flat.get(k).map_or(0, |v| v.len()));
        measure("map nested contract", keys, |k| nested.get::<H>(&k[..32].try_into().unwrap()).and_then(|m| m.get::<H>(&k[32..].try_into().unwrap())).map_or(0, |v| v.len()));
    }
    let _ = std::fs::remove_dir_all(&dir);
}

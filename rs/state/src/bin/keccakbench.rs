use std::time::Instant;
fn main() {
    let data = vec![0xabu8; 100];
    let n = 2_000_000u32;
    for (name, f) in [("tiny", state::keccak::keccak256 as fn(&[u8]) -> [u8; 32]), ("sha3", state::keccak::keccak256_sha3)] {
        let t = Instant::now();
        let mut acc = 0u8;
        for i in 0..n { let mut d = data.clone(); d[0] = i as u8; acc ^= f(&d)[0]; }
        println!("{name}: {:.0} ns/hash ({acc})", t.elapsed().as_nanos() as f64 / n as f64);
    }
    let big = vec![0x11u8; 532];
    let t = Instant::now();
    let mut acc = 0u8;
    for _ in 0..500_000 { acc ^= state::keccak::keccak256(&big)[0]; }
    println!("532B: {:.0} ns/hash ({acc})", t.elapsed().as_nanos() as f64 / 500_000.0);
}

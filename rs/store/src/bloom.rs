//! pebble's bloom.FilterPolicy(20) table filter (RocksDB full-filter format).

const CACHE_LINE_BITS: u32 = 512;

pub fn hash(b: &[u8]) -> u32 {
    const SEED: u32 = 0xbc9f1d34;
    const M: u32 = 0xc6a4a793;
    let mut h = SEED ^ (b.len() as u32).wrapping_mul(M);
    let mut b = b;
    while b.len() >= 4 {
        h = h.wrapping_add(u32::from_le_bytes([b[0], b[1], b[2], b[3]]));
        h = h.wrapping_mul(M);
        h ^= h >> 16;
        b = &b[4..];
    }
    match b.len() {
        3 => {
            h = h.wrapping_add((b[2] as i8 as i32 as u32) << 16);
            h = h.wrapping_add((b[1] as i8 as i32 as u32) << 8);
            h = h.wrapping_add(b[0] as i8 as i32 as u32);
            h = h.wrapping_mul(M);
            h ^= h >> 24;
        }
        2 => {
            h = h.wrapping_add((b[1] as i8 as i32 as u32) << 8);
            h = h.wrapping_add(b[0] as i8 as i32 as u32);
            h = h.wrapping_mul(M);
            h ^= h >> 24;
        }
        1 => {
            h = h.wrapping_add(b[0] as i8 as i32 as u32);
            h = h.wrapping_mul(M);
            h ^= h >> 24;
        }
        _ => {}
    }
    h
}

pub fn may_contain(f: &[u8], key: &[u8]) -> bool {
    if f.len() <= 5 {
        return false;
    }
    let n = f.len() - 5;
    let n_probes = f[n];
    let n_lines = u32::from_le_bytes(f[n + 1..n + 5].try_into().unwrap());
    let line_bits = 8 * (n as u32 / n_lines);
    let mut h = hash(key);
    let delta = h >> 17 | h << 15;
    let b = (h % n_lines) * line_bits;
    for _ in 0..n_probes {
        let bit = b + (h % line_bits);
        if f[(bit / 8) as usize] & (1 << (bit % 8)) == 0 {
            return false;
        }
        h = h.wrapping_add(delta);
    }
    true
}

pub struct FilterWriter {
    bits_per_key: usize,
    hashes: Vec<u32>,
    last: u32,
    /// Keys added (before the last-hash dedup), tableFilterWriter.count.
    pub count: usize,
}

impl FilterWriter {
    pub fn new(bits_per_key: usize) -> Self {
        FilterWriter { bits_per_key, hashes: Vec::new(), last: 0, count: 0 }
    }
    pub fn add_key(&mut self, key: &[u8]) {
        self.count += 1;
        let h = hash(key);
        if !self.hashes.is_empty() && h == self.last {
            return;
        }
        self.hashes.push(h);
        self.last = h;
    }
    pub fn finish(&self) -> Vec<u8> {
        let mut n_lines = 0usize;
        if !self.hashes.is_empty() {
            n_lines = (self.hashes.len() * self.bits_per_key + 511) / 512;
            if n_lines % 2 == 0 {
                n_lines += 1;
            }
        }
        let n_bytes = n_lines * 64;
        let mut f = vec![0u8; n_bytes + 5];
        if n_lines != 0 {
            let n_probes = ((self.bits_per_key as f64 * 0.69) as u32).clamp(1, 30);
            for &h0 in &self.hashes {
                let mut h = h0;
                let delta = h >> 17 | h << 15;
                let b = (h % n_lines as u32) * CACHE_LINE_BITS;
                for _ in 0..n_probes {
                    let bit = b + (h % CACHE_LINE_BITS);
                    f[(bit / 8) as usize] |= 1 << (bit % 8);
                    h = h.wrapping_add(delta);
                }
            }
            f[n_bytes] = n_probes as u8;
            f[n_bytes + 1..].copy_from_slice(&(n_lines as u32).to_le_bytes());
        }
        f
    }
}

//! store/ef: one posting chunk, plain Elias-Fano over txnums plus a fixed
//! width payload per entry. uvarint n | uvarint low | uvarint payloadBits |
//! upper bits (unary, byte padded) | low bits | payloads; all LSB-first.

use anyhow::{bail, Result};

pub const MAX_ENTRIES: usize = 4096;

struct BitWriter {
    buf: Vec<u8>,
    bits: usize,
    base: usize,
}
impl BitWriter {
    fn set(&mut self, i: usize) {
        while self.buf.len() <= self.base + i / 8 {
            self.buf.push(0);
        }
        self.buf[self.base + i / 8] |= 1 << (i % 8);
        if i + 1 > self.bits {
            self.bits = i + 1;
        }
    }
    fn pad(&mut self, n: usize) {
        while self.buf.len() < self.base + (n + 7) / 8 {
            self.buf.push(0);
        }
        self.bits = self.buf.len() * 8;
    }
    fn put(&mut self, x: u64, nbits: u32) {
        for k in 0..nbits {
            if self.bits % 8 == 0 {
                self.buf.push(0);
            }
            if x >> k & 1 == 1 {
                self.buf[self.bits / 8] |= 1 << (self.bits % 8);
            }
            self.bits += 1;
        }
    }
}

pub fn encode(base: u64, txnums: &[u64], payload: &[u8], payload_bits: u32) -> Vec<u8> {
    let n = txnums.len();
    let span = if n > 0 { txnums[n - 1] - base } else { 0 };
    let mut low = 0u32;
    if n > 0 && span / n as u64 > 0 {
        low = 64 - (span / n as u64).leading_zeros();
    }
    let mut out = Vec::new();
    crate::sst::put_uvarint(&mut out, n as u64);
    crate::sst::put_uvarint(&mut out, low as u64);
    crate::sst::put_uvarint(&mut out, payload_bits as u64);
    let upper_len = (span >> low) as usize + n;
    let base_off = out.len();
    let mut bw = BitWriter { buf: out, bits: 0, base: base_off };
    for (i, t) in txnums.iter().enumerate() {
        let hi = (t - base) >> low;
        bw.set(hi as usize + i);
    }
    bw.pad(upper_len);
    for t in txnums {
        bw.put((t - base) & ((1u64 << low) - 1), low);
    }
    if payload_bits > 0 {
        for p in payload.iter().take(n) {
            bw.put(*p as u64, payload_bits);
        }
    }
    bw.buf
}

struct BitReader<'a> {
    buf: &'a [u8],
    bit: usize,
}
impl BitReader<'_> {
    fn next(&mut self) -> Option<bool> {
        if self.bit / 8 >= self.buf.len() {
            return None;
        }
        let b = self.buf[self.bit / 8] >> (self.bit % 8) & 1 == 1;
        self.bit += 1;
        Some(b)
    }
    fn get(&mut self, nbits: u32) -> Option<u64> {
        let mut x = 0u64;
        for k in 0..nbits {
            if self.next()? {
                x |= 1 << k;
            }
        }
        Some(x)
    }
}

pub fn decode(base: u64, v: &[u8]) -> Result<(Vec<u64>, Vec<u8>)> {
    let mut pos = 0;
    let mut next = || -> Result<u64> {
        let (x, k) = crate::sst::uvarint(&v[pos..]).ok_or_else(|| anyhow::anyhow!("ef: corrupt chunk"))?;
        pos += k;
        Ok(x)
    };
    let n = next()? as usize;
    let low = next()? as u32;
    let pb = next()? as u32;
    if n > MAX_ENTRIES || low > 64 || pb > 8 {
        bail!("ef: corrupt chunk");
    }
    let mut r = BitReader { buf: v, bit: pos * 8 };
    let mut nums = vec![0u64; n];
    let mut hi = 0u64;
    for x in nums.iter_mut() {
        loop {
            match r.next() {
                None => bail!("ef: corrupt chunk"),
                Some(true) => break,
                Some(false) => hi += 1,
            }
        }
        *x = hi;
    }
    r.bit = (r.bit + 7) & !7;
    for x in nums.iter_mut() {
        let lo = r.get(low).ok_or_else(|| anyhow::anyhow!("ef: corrupt chunk"))?;
        *x = base + (*x << low) + lo;
    }
    let mut payload = Vec::new();
    if pb > 0 {
        for _ in 0..n {
            payload.push(r.get(pb).ok_or_else(|| anyhow::anyhow!("ef: corrupt chunk"))? as u8);
        }
    }
    Ok((nums, payload))
}

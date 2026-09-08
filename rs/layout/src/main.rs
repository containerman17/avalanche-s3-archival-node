//! Layout experiment for the latest-state run file (see rs/state/LAYOUT.md).
//! Every candidate builds a file from contract-form rows, opens it mmapped,
//! answers `get` with no cache layer, and scans it whole (the roll's need).
//! Nothing here is a store format; run.rs is the production layout (A).
//!
//!   layout <rows.bin> [--raw] [--rounds N] [--set A,B,C,D,E,F,S] [--only name-substr]
//!
//! Prints one Markdown table row per candidate.

use memmap2::Mmap;
use state::rlp;
use state::run;
use state::sample::{read_rows, to_contract_rows, write_rows, Row, Rng, EMPTY_CODE_HASH};
use state::KvIter;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Instant;

// ---------------------------------------------------------------- file sections

struct FileBuilder {
    buf: Vec<u8>,
    secs: Vec<(u64, u64)>,
}

impl FileBuilder {
    fn new() -> Self {
        FileBuilder { buf: vec![], secs: vec![] }
    }
    fn section(&mut self, b: &[u8]) {
        while self.buf.len() % 8 != 0 {
            self.buf.push(0);
        }
        self.secs.push((self.buf.len() as u64, b.len() as u64));
        self.buf.extend_from_slice(b);
    }
    fn finish(mut self) -> Vec<u8> {
        while self.buf.len() % 8 != 0 {
            self.buf.push(0);
        }
        for (o, l) in &self.secs {
            self.buf.extend_from_slice(&o.to_le_bytes());
            self.buf.extend_from_slice(&l.to_le_bytes());
        }
        self.buf.extend_from_slice(&(self.secs.len() as u64).to_le_bytes());
        self.buf
    }
}

fn sections(f: &[u8]) -> Vec<&[u8]> {
    let rd = |o: usize| u64::from_le_bytes(f[o..o + 8].try_into().unwrap()) as usize;
    let n = rd(f.len() - 8);
    let base = f.len() - 8 - 16 * n;
    (0..n).map(|i| &f[rd(base + 16 * i)..rd(base + 16 * i) + rd(base + 16 * i + 8)]).collect()
}

fn u64s(b: &[u8]) -> &[u64] {
    let (p, s, t) = unsafe { b.align_to::<u64>() };
    assert!(p.is_empty() && t.is_empty());
    s
}

fn u32s(b: &[u8]) -> &[u32] {
    let (p, s, t) = unsafe { b.align_to::<u32>() };
    assert!(p.is_empty() && t.is_empty());
    s
}

fn as_bytes<T: Copy>(v: &[T]) -> &[u8] {
    unsafe { std::slice::from_raw_parts(v.as_ptr() as *const u8, std::mem::size_of_val(v)) }
}

// ---------------------------------------------------------------- values (F)

#[derive(Clone, Copy, PartialEq, Debug)]
enum ValEnc {
    Raw,
    Packed,
    Dict,
}

/// Value codec. Packed: account RLP[nonce, balance, codeHash] becomes
/// [nlen<<1 | empty_code][blen][nonce][balance][codeHash unless empty].
/// Dict: on top of Packed, the top 127 stored values get vlen = 128 + i
/// and no bytes.
struct Vals {
    enc: ValEnc,
    dict: Vec<Vec<u8>>,
    idx: HashMap<Vec<u8>, u8>,
}

fn pack(k: &[u8], v: &[u8], out: &mut Vec<u8>) {
    out.clear();
    if k.len() != 33 || v.is_empty() {
        out.extend_from_slice(v);
        return;
    }
    let (c, _) = rlp::split_list(v).expect("account rlp");
    let (nonce, r) = rlp::split_string(c).unwrap();
    let (bal, r) = rlp::split_string(r).unwrap();
    let (code, _) = rlp::split_string(r).unwrap();
    let empty = code == EMPTY_CODE_HASH;
    out.push(((nonce.len() as u8) << 1) | empty as u8);
    out.push(bal.len() as u8);
    out.extend_from_slice(nonce);
    out.extend_from_slice(bal);
    if !empty {
        out.extend_from_slice(code);
    }
}

fn unpack<'a>(p: &[u8], out: &'a mut Vec<u8>) -> &'a [u8] {
    let (nl, empty, bl) = ((p[0] >> 1) as usize, p[0] & 1 == 1, p[1] as usize);
    let nonce = &p[2..2 + nl];
    let bal = &p[2 + nl..2 + nl + bl];
    let code: &[u8] = if empty { &EMPTY_CODE_HASH } else { &p[2 + nl + bl..2 + nl + bl + 32] };
    out.clear();
    out.extend_from_slice(&[0, 0]);
    rlp::put_bytes(out, nonce);
    rlp::put_bytes(out, bal);
    rlp::put_bytes(out, code);
    let body = out.len() - 2;
    if body < 56 {
        out[1] = 0xc0 + body as u8;
        &out[1..]
    } else {
        out[0] = 0xf8;
        out[1] = body as u8;
        &out[..]
    }
}

impl Vals {
    fn train(enc: ValEnc, rows: &[Row]) -> Vals {
        let mut dict = vec![];
        if enc == ValEnc::Dict {
            let mut cnt: HashMap<Vec<u8>, u64> = HashMap::new();
            let mut p = Vec::new();
            for (k, v) in rows {
                pack(k, v, &mut p);
                *cnt.entry(p.clone()).or_default() += 1;
            }
            let mut all: Vec<(Vec<u8>, u64)> = cnt.into_iter().filter(|(v, c)| *c > 1 && !v.is_empty()).collect();
            all.sort_by_key(|(v, c)| std::cmp::Reverse(v.len() as u64 * c));
            dict = all.into_iter().take(127).map(|(v, _)| v).collect();
        }
        Vals::with_dict(enc, dict)
    }
    fn with_dict(enc: ValEnc, dict: Vec<Vec<u8>>) -> Vals {
        let idx = dict.iter().enumerate().map(|(i, v)| (v.clone(), i as u8)).collect();
        Vals { enc, dict, idx }
    }
    fn to_bytes(&self) -> Vec<u8> {
        let mut b = vec![self.enc as u8, self.dict.len() as u8];
        for d in &self.dict {
            b.push(d.len() as u8);
            b.extend_from_slice(d);
        }
        b
    }
    fn from_bytes(b: &[u8]) -> Vals {
        let enc = match b[0] {
            0 => ValEnc::Raw,
            1 => ValEnc::Packed,
            _ => ValEnc::Dict,
        };
        let (mut dict, mut i) = (vec![], 2);
        for _ in 0..b[1] {
            let l = b[i] as usize;
            dict.push(b[i + 1..i + 1 + l].to_vec());
            i += 1 + l;
        }
        Vals::with_dict(enc, dict)
    }
    /// Stored form: the vlen byte and the bytes (in `out`).
    fn encode(&self, k: &[u8], v: &[u8], out: &mut Vec<u8>) -> u8 {
        match self.enc {
            ValEnc::Raw => {
                out.clear();
                out.extend_from_slice(v);
            }
            _ => pack(k, v, out),
        }
        if self.enc == ValEnc::Dict {
            if let Some(&i) = self.idx.get(out.as_slice()) {
                out.clear();
                return 128 + i;
            }
        }
        assert!(out.len() < 128, "value {} bytes", out.len());
        out.len() as u8
    }
    #[inline]
    fn decode<'a>(&'a self, k: &[u8], vl: u8, bytes: &'a [u8], sc: &'a mut Vec<u8>) -> &'a [u8] {
        let raw: &'a [u8] = if vl >= 128 { &self.dict[(vl - 128) as usize] } else { bytes };
        if self.enc == ValEnc::Raw || k.len() != 33 || raw.is_empty() {
            return raw;
        }
        unpack(raw, sc)
    }
}

// ---------------------------------------------------------------- index (C)

#[derive(Clone, Copy, PartialEq, Debug)]
enum Idx {
    /// u64 first-key prefixes, binary search at both levels (C2).
    Bin,
    /// contract table (distinct u64 prefixes -> first block) with
    /// interpolation search, interpolation on the slot prefix in ties (C1).
    Interp,
    /// binary at the top, interpolation in the tie range.
    Mixed,
}

#[inline]
fn code(k: &[u8]) -> (u64, u64) {
    let hi = u64::from_be_bytes(k[..8].try_into().unwrap());
    let lo = if k.len() >= 41 { u64::from_be_bytes(k[33..41].try_into().unwrap()) } else { 0 };
    (hi, lo)
}

/// Number of elements <= t (the array is sorted).
#[inline]
fn upper(a: &[u64], t: u64) -> usize {
    a.partition_point(|&x| x <= t)
}

/// Same as `upper` by interpolation steps, binary search on the last window.
fn interp_upper(a: &[u64], t: u64) -> usize {
    let (mut lo, mut hi) = (0usize, a.len());
    let mut it = 0;
    while hi - lo > 32 && it < 8 {
        let (alo, ahi) = (a[lo], a[hi - 1]);
        if t < alo {
            return lo;
        }
        if t >= ahi {
            return hi;
        }
        let pos = lo + (((t - alo) as u128 * (hi - lo - 1) as u128) / ((ahi - alo) as u128 + 1)) as usize;
        let pos = pos.clamp(lo, hi - 1);
        if a[pos] <= t {
            lo = pos + 1;
        } else {
            hi = pos;
        }
        it += 1;
    }
    lo + upper(&a[lo..hi], t)
}

struct Index<'a> {
    hi: &'a [u64],
    lo: &'a [u64],
    kind: Idx,
    /// distinct hi values and the first block of each (the contract table)
    dh: Vec<u64>,
    db: Vec<u32>,
}

impl<'a> Index<'a> {
    fn new(hi: &'a [u64], lo: &'a [u64], kind: Idx) -> Index<'a> {
        let (mut dh, mut db) = (vec![], vec![]);
        if kind == Idx::Interp {
            for (i, &h) in hi.iter().enumerate() {
                if dh.last() != Some(&h) {
                    dh.push(h);
                    db.push(i as u32);
                }
            }
        }
        Index { hi, lo, kind, dh, db }
    }
    fn ram(&self) -> usize {
        self.dh.len() * 12
    }
    /// The last block whose first key is <= key.
    #[inline]
    fn find(&self, k: &[u8]) -> Option<usize> {
        let (kh, kl) = code(k);
        let (s, e) = match self.kind {
            Idx::Interp => {
                let j = interp_upper(&self.dh, kh);
                if j == 0 {
                    return None;
                }
                let e = if j < self.dh.len() { self.db[j] as usize } else { self.hi.len() };
                if self.dh[j - 1] < kh {
                    return Some(e - 1);
                }
                (self.db[j - 1] as usize, e)
            }
            _ => {
                let e = upper(self.hi, kh);
                if e == 0 {
                    return None;
                }
                if self.hi[e - 1] < kh {
                    return Some(e - 1);
                }
                (self.hi[..e].partition_point(|&x| x < kh), e)
            }
        };
        let w = &self.lo[s..e];
        let t = s + if self.kind != Idx::Bin && w.len() > 32 { interp_upper(w, kl) } else { upper(w, kl) };
        t.checked_sub(1)
    }
}

fn index_bytes(hi: &[u64], lo: &[u64], fb: &mut FileBuilder) {
    fb.section(as_bytes(hi));
    fb.section(as_bytes(lo));
}

// ---------------------------------------------------------------- readers

struct Scratch {
    key: Vec<u8>,
    val: Vec<u8>,
    blk: Vec<u8>,
    zd: Option<zstd::bulk::Decompressor<'static>>,
}

impl Scratch {
    fn new() -> Scratch {
        Scratch { key: Vec::with_capacity(256), val: Vec::with_capacity(256), blk: Vec::with_capacity(1 << 16), zd: None }
    }
}

trait Reader: Sync {
    fn get<'a>(&'a self, key: &[u8], sc: &'a mut Scratch) -> Option<&'a [u8]>;
    fn scan(&self, sc: &mut Scratch, f: &mut dyn FnMut(&[u8], &[u8]));
    /// Decompresses block b alone (0 when blocks are stored plain).
    fn decompress(&self, _sc: &mut Scratch, _b: usize) -> usize {
        0
    }
    fn nblk(&self) -> usize;
    /// Bytes held outside the run's blocks and index (sidecar, contract table).
    fn ram(&self) -> usize {
        0
    }
}

// ---- A: the production run.rs

struct RunReader(run::Run);

impl Reader for RunReader {
    fn get<'a>(&'a self, key: &[u8], _sc: &'a mut Scratch) -> Option<&'a [u8]> {
        self.0.get(key)
    }
    fn scan(&self, _sc: &mut Scratch, f: &mut dyn FnMut(&[u8], &[u8])) {
        let mut it = self.0.iter(None, None);
        while it.next() {
            f(it.key(), it.value());
        }
    }
    fn nblk(&self) -> usize {
        self.0.blocks()
    }
}

// ---- B: front-coded blocks with restarts, my index

/// Block: [used u16][nres u16] entries [sh u8][un u8][vl u8][suffix][bytes]
/// from 4 to used, then nres u16 restart offsets. Restart entries have sh 0.
fn front_scan<'a>(b: &'a [u8], mut i: usize, end: usize, key: &[u8]) -> Option<(u8, &'a [u8])> {
    let mut eq = 0usize;
    while i < end {
        let (sh, un, vl) = (b[i] as usize, b[i + 1] as usize, b[i + 2]);
        i += 3;
        if sh < eq {
            return None;
        }
        let vb = if vl >= 128 { 0 } else { vl as usize };
        if sh == eq {
            let (suf, rest) = (&b[i..i + un], &key[eq..]);
            let n = suf.len().min(rest.len());
            let mut j = 0;
            while j < n && suf[j] == rest[j] {
                j += 1;
            }
            if (j < n && suf[j] > rest[j]) || (j == n && suf.len() > rest.len()) {
                return None;
            }
            if j == n && suf.len() == rest.len() {
                return Some((vl, &b[i + un..i + un + vb]));
            }
            eq += j;
        }
        i += un + vb;
    }
    None
}

fn front_get<'a>(b: &'a [u8], key: &[u8]) -> Option<(u8, &'a [u8])> {
    let used = u16::from_le_bytes([b[0], b[1]]) as usize;
    let nres = u16::from_le_bytes([b[2], b[3]]) as usize;
    let (mut i, mut end) = (4, used);
    if nres > 1 {
        let ra = &b[used..used + 2 * nres];
        let at = |m: usize| u16::from_le_bytes([ra[2 * m], ra[2 * m + 1]]) as usize;
        let (mut lo, mut hi) = (0, nres);
        while lo < hi {
            let m = (lo + hi) / 2;
            let o = at(m);
            if &b[o + 3..o + 3 + b[o + 1] as usize] <= key {
                lo = m + 1;
            } else {
                hi = m;
            }
        }
        if lo == 0 {
            return None;
        }
        i = at(lo - 1);
        if lo < nres {
            end = at(lo);
        }
    }
    front_scan(b, i, end, key)
}

struct FrontCfg {
    bs: usize,
    restart: usize,
}

/// Returns (blocks, hi, lo, per-entry file offset of its restart group).
fn build_front(rows: &[Row], vals: &Vals, cfg: &FrontCfg) -> (Vec<u8>, Vec<u64>, Vec<u64>, Vec<u64>) {
    let bs = cfg.bs;
    let (mut out, mut hi, mut lo, mut groups) = (Vec::new(), vec![], vec![], Vec::with_capacity(rows.len()));
    let mut group = 0u64;
    let mut blk: Vec<u8> = vec![0, 0, 0, 0];
    let mut res: Vec<u16> = vec![];
    let mut prev: &[u8] = &[];
    let mut n_in = 0usize;
    let mut vb = Vec::new();
    let flush = |blk: &mut Vec<u8>, res: &mut Vec<u16>, out: &mut Vec<u8>| {
        let used = blk.len();
        blk[..2].copy_from_slice(&(used as u16).to_le_bytes());
        blk[2..4].copy_from_slice(&(res.len() as u16).to_le_bytes());
        for &r in res.iter() {
            blk.extend_from_slice(&r.to_le_bytes());
        }
        blk.resize(bs, 0);
        out.extend_from_slice(blk);
        blk.truncate(4);
        res.clear();
    };
    for (k, v) in rows {
        let vl = vals.encode(k, v, &mut vb);
        let mut shared = 0;
        let restart = n_in == 0 || (cfg.restart > 0 && n_in % cfg.restart == 0);
        if !restart {
            while shared < prev.len() && shared < k.len() && prev[shared] == k[shared] {
                shared += 1;
            }
        }
        let need = blk.len() + 3 + k.len() - shared + vb.len() + 2 * (res.len() + restart as usize);
        if n_in > 0 && need > bs {
            flush(&mut blk, &mut res, &mut out);
            n_in = 0;
            shared = 0;
        }
        if n_in == 0 {
            let (h, l) = code(k);
            hi.push(h);
            lo.push(l);
        }
        if n_in == 0 || (cfg.restart > 0 && n_in % cfg.restart == 0) {
            res.push(blk.len() as u16);
            group = (out.len() + blk.len()) as u64;
        }
        groups.push(group);
        blk.extend_from_slice(&[shared as u8, (k.len() - shared) as u8, vl]);
        blk.extend_from_slice(&k[shared..]);
        blk.extend_from_slice(&vb);
        prev = k;
        n_in += 1;
    }
    if n_in > 0 {
        flush(&mut blk, &mut res, &mut out);
    }
    (out, hi, lo, groups)
}

struct FrontReader<'a> {
    blocks: &'a [u8],
    bs: usize,
    idx: Index<'a>,
    vals: Vals,
    /// E: open-addressing table of [fp 24][restart offset 40], 0 = empty.
    tab: &'a [u64],
}

#[inline]
fn mix(k: &[u8]) -> u64 {
    let (h, l) = code(k);
    let mut x = h ^ l.rotate_left(29) ^ (k.len() as u64);
    x ^= x >> 32;
    x = x.wrapping_mul(0x9E3779B97F4A7C15);
    x ^= x >> 29;
    x
}

impl<'a> FrontReader<'a> {
    #[inline]
    fn block(&self, b: usize) -> &'a [u8] {
        &self.blocks[b * self.bs..(b + 1) * self.bs]
    }
    fn get_tab<'s>(&'s self, key: &[u8], sc: &'s mut Scratch) -> Option<&'s [u8]> {
        let h = mix(key);
        let fp = (h & 0xFF_FFFF) | 1;
        let cap = self.tab.len();
        let mut i = ((h as u128 * cap as u128) >> 64) as usize;
        loop {
            let s = self.tab[i];
            if s == 0 {
                return None;
            }
            if s >> 40 == fp {
                let off = (s & 0xFF_FFFF_FFFF) as usize;
                let b = self.block(off / self.bs);
                let used = u16::from_le_bytes([b[0], b[1]]) as usize;
                if let Some((vl, bytes)) = front_scan(b, off % self.bs, used, key) {
                    return Some(self.vals.decode(key, vl, bytes, &mut sc.val));
                }
            }
            i += 1;
            if i == cap {
                i = 0;
            }
        }
    }
}

impl Reader for FrontReader<'_> {
    fn get<'s>(&'s self, key: &[u8], sc: &'s mut Scratch) -> Option<&'s [u8]> {
        if !self.tab.is_empty() {
            return self.get_tab(key, sc);
        }
        let b = self.idx.find(key)?;
        let (vl, bytes) = front_get(self.block(b), key)?;
        Some(self.vals.decode(key, vl, bytes, &mut sc.val))
    }
    fn scan(&self, sc: &mut Scratch, f: &mut dyn FnMut(&[u8], &[u8])) {
        let Scratch { key, val, .. } = sc;
        for b in 0..self.nblk() {
            let b = self.block(b);
            let used = u16::from_le_bytes([b[0], b[1]]) as usize;
            let mut i = 4;
            while i < used {
                let (sh, un, vl) = (b[i] as usize, b[i + 1] as usize, b[i + 2]);
                i += 3;
                key.truncate(sh);
                key.extend_from_slice(&b[i..i + un]);
                i += un;
                let vb = if vl >= 128 { 0 } else { vl as usize };
                let v = self.vals.decode(key, vl, &b[i..i + vb], val);
                i += vb;
                f(key, v);
            }
        }
    }
    fn nblk(&self) -> usize {
        self.blocks.len() / self.bs
    }
    fn ram(&self) -> usize {
        self.idx.ram() + self.tab.len() * 8
    }
}

// ---- D: fixed-width rows, compressed blocks

#[derive(Clone, Copy, PartialEq, Debug)]
enum Codec {
    Plain,
    Zstd(i32),
    Lz4,
}

struct FixedCfg {
    bs: usize,
    codec: Codec,
    dict: bool,
}

/// Block (uncompressed): [n u16][kw u8][vw u8] rows [kl u8][key kw][vl u8][value vw].
fn fixed_blocks(rows: &[Row], vals: &Vals, bs: usize) -> (Vec<Vec<u8>>, Vec<u64>, Vec<u64>) {
    let (mut blocks, mut hi, mut lo) = (vec![], vec![], vec![]);
    let mut i = 0;
    let mut enc: Vec<(u8, Vec<u8>)> = Vec::new();
    while i < rows.len() {
        let (mut j, mut kw, mut vw) = (i, 0usize, 0usize);
        enc.clear();
        while j < rows.len() {
            let mut vb = Vec::new();
            let vl = vals.encode(&rows[j].0, &rows[j].1, &mut vb);
            let (nkw, nvw) = (kw.max(rows[j].0.len()), vw.max(vb.len()));
            if j > i && 4 + (j - i + 1) * (2 + nkw + nvw) > bs {
                break;
            }
            kw = nkw;
            vw = nvw;
            enc.push((vl, vb));
            j += 1;
        }
        let mut b = Vec::with_capacity(bs);
        b.extend_from_slice(&((j - i) as u16).to_le_bytes());
        b.push(kw as u8);
        b.push(vw as u8);
        for (r, (vl, vb)) in rows[i..j].iter().zip(&enc) {
            b.push(r.0.len() as u8);
            b.extend_from_slice(&r.0);
            b.resize(b.len() + kw - r.0.len(), 0);
            b.push(*vl);
            b.extend_from_slice(vb);
            b.resize(b.len() + vw - vb.len(), 0);
        }
        let (h, l) = code(&rows[i].0);
        hi.push(h);
        lo.push(l);
        blocks.push(b);
        i = j;
    }
    (blocks, hi, lo)
}

fn train_dict(blocks: &[Vec<u8>]) -> Vec<u8> {
    let step = (blocks.len() / 20_000).max(1);
    let samples: Vec<&[u8]> = blocks.iter().step_by(step).map(|b| b.as_slice()).collect();
    zstd::dict::from_samples(&samples, 32 << 10).expect("dict")
}

fn fixed_get<'a>(b: &'a [u8], key: &[u8]) -> Option<(u8, &'a [u8])> {
    let n = u16::from_le_bytes([b[0], b[1]]) as usize;
    let (kw, vw) = (b[2] as usize, b[3] as usize);
    let rw = 2 + kw + vw;
    let row = |r: usize| &b[4 + r * rw..4 + (r + 1) * rw];
    let (mut lo, mut hi) = (0, n);
    while lo < hi {
        let m = (lo + hi) / 2;
        let r = row(m);
        if &r[1..1 + r[0] as usize] < key {
            lo = m + 1;
        } else {
            hi = m;
        }
    }
    if lo == n {
        return None;
    }
    let r = row(lo);
    if &r[1..1 + r[0] as usize] != key {
        return None;
    }
    let vl = r[1 + kw];
    let vb = if vl >= 128 { 0 } else { vl as usize };
    Some((vl, &r[2 + kw..2 + kw + vb]))
}

struct FixedReader<'a> {
    data: &'a [u8],
    offs: &'a [u32],
    idx: Index<'a>,
    vals: Vals,
    codec: Codec,
    dict: &'a [u8],
    zdict: Option<&'static zstd::dict::DecoderDictionary<'static>>,
    bs: usize,
}

impl FixedReader<'_> {
    /// Decompresses block b into sc.blk (or returns the plain block).
    #[inline]
    fn load<'s>(&'s self, sc: &'s mut Scratch, b: usize) -> &'s [u8] {
        let src = &self.data[self.offs[b] as usize..self.offs[b + 1] as usize];
        match self.codec {
            Codec::Plain => src,
            Codec::Zstd(_) => {
                let zd = sc.zd.get_or_insert_with(|| match self.zdict {
                    Some(d) => zstd::bulk::Decompressor::with_prepared_dictionary(d).unwrap(),
                    None => zstd::bulk::Decompressor::new().unwrap(),
                });
                sc.blk.resize(self.bs, 0);
                let n = zd.decompress_to_buffer(src, &mut sc.blk[..]).expect("zstd");
                &sc.blk[..n]
            }
            Codec::Lz4 => {
                let ul = u16::from_le_bytes([src[0], src[1]]) as usize;
                sc.blk.resize(ul, 0);
                if self.dict.is_empty() {
                    lz4_flex::block::decompress_into(&src[2..], &mut sc.blk).expect("lz4");
                } else {
                    lz4_flex::block::decompress_into_with_dict(&src[2..], &mut sc.blk, self.dict).expect("lz4");
                }
                &sc.blk[..]
            }
        }
    }
}

impl Reader for FixedReader<'_> {
    fn get<'s>(&'s self, key: &[u8], sc: &'s mut Scratch) -> Option<&'s [u8]> {
        let b = self.idx.find(key)?;
        let Scratch { val, .. } = sc;
        let val: &'s mut Vec<u8> = unsafe { &mut *(val as *mut Vec<u8>) };
        let blk = self.load(sc, b);
        let (vl, bytes) = fixed_get(blk, key)?;
        Some(self.vals.decode(key, vl, bytes, val))
    }
    fn scan(&self, sc: &mut Scratch, f: &mut dyn FnMut(&[u8], &[u8])) {
        let mut val = Vec::with_capacity(256);
        for b in 0..self.nblk() {
            let blk = self.load(sc, b);
            let n = u16::from_le_bytes([blk[0], blk[1]]) as usize;
            let (kw, vw) = (blk[2] as usize, blk[3] as usize);
            let rw = 2 + kw + vw;
            for r in 0..n {
                let r = &blk[4 + r * rw..4 + (r + 1) * rw];
                let k = &r[1..1 + r[0] as usize];
                let vl = r[1 + kw];
                let vb = if vl >= 128 { 0 } else { vl as usize };
                let v = self.vals.decode(k, vl, &r[2 + kw..2 + kw + vb], &mut val);
                f(k, v);
            }
        }
    }
    fn decompress(&self, sc: &mut Scratch, b: usize) -> usize {
        self.load(sc, b).len()
    }
    fn nblk(&self) -> usize {
        self.offs.len() - 1
    }
    fn ram(&self) -> usize {
        self.idx.ram() + self.dict.len()
    }
}

// ---------------------------------------------------------------- layouts

enum Layout {
    Run,
    Front { cfg: FrontCfg, idx: Idx, enc: ValEnc, sidecar: bool },
    Fixed { cfg: FixedCfg, idx: Idx, enc: ValEnc },
}

impl Layout {
    fn name(&self) -> String {
        match self {
            Layout::Run => "A run.rs 4K/p40".into(),
            Layout::Front { cfg, idx, enc, sidecar } => format!(
                "{} front {}K R{} {:?}{}",
                if *sidecar { "E" } else { "B" },
                cfg.bs / 1024,
                cfg.restart,
                idx,
                match enc {
                    ValEnc::Raw => "".into(),
                    e => format!(" {:?}", e),
                }
            ),
            Layout::Fixed { cfg, idx, enc } => format!(
                "D fixed {} {:?}{} {:?}{}",
                if cfg.bs < 1024 { format!("{}B", cfg.bs) } else { format!("{}K", cfg.bs / 1024) },
                cfg.codec,
                if cfg.dict { "+dict" } else { "" },
                idx,
                match enc {
                    ValEnc::Raw => "".into(),
                    e => format!(" {:?}", e),
                }
            ),
        }
    }

    fn build(&self, rows: &[Row], path: &Path) {
        match self {
            Layout::Run => {
                let mut w = run::Writer::create(path).unwrap();
                for (k, v) in rows {
                    w.add(k, v).unwrap();
                }
                w.close().unwrap();
            }
            Layout::Front { cfg, enc, sidecar, .. } => {
                let vals = Vals::train(*enc, rows);
                let (blocks, hi, lo, groups) = build_front(rows, &vals, cfg);
                let mut fb = FileBuilder::new();
                fb.section(&blocks);
                index_bytes(&hi, &lo, &mut fb);
                fb.section(&vals.to_bytes());
                let mut tab: Vec<u64> = vec![];
                if *sidecar {
                    // load factor 0.8: 10 B/key
                    let cap = rows.len() * 5 / 4 + 1;
                    tab = vec![0; cap];
                    for (i, (k, _)) in rows.iter().enumerate() {
                        let h = mix(k);
                        let fp = (h & 0xFF_FFFF) | 1;
                        let mut s = ((h as u128 * cap as u128) >> 64) as usize;
                        while tab[s] != 0 {
                            s += 1;
                            if s == cap {
                                s = 0;
                            }
                        }
                        tab[s] = fp << 40 | groups[i];
                    }
                }
                fb.section(as_bytes(&tab));
                std::fs::write(path, fb.finish()).unwrap();
            }
            Layout::Fixed { cfg, enc, .. } => {
                let vals = Vals::train(*enc, rows);
                let (blocks, hi, lo) = fixed_blocks(rows, &vals, cfg.bs);
                let dict = if cfg.dict { train_dict(&blocks) } else { vec![] };
                let mut data = Vec::with_capacity(blocks.len() * cfg.bs / 2);
                let mut offs: Vec<u32> = Vec::with_capacity(blocks.len() + 1);
                let mut zc = match cfg.codec {
                    Codec::Zstd(l) if cfg.dict => Some(zstd::bulk::Compressor::with_dictionary(l, &dict).unwrap()),
                    Codec::Zstd(l) => Some(zstd::bulk::Compressor::new(l).unwrap()),
                    _ => None,
                };
                if let Some(z) = zc.as_mut() {
                    z.include_checksum(false).unwrap();
                }
                let mut buf = Vec::with_capacity(cfg.bs * 2);
                for b in &blocks {
                    offs.push(data.len() as u32);
                    match cfg.codec {
                        Codec::Plain => data.extend_from_slice(b),
                        Codec::Zstd(_) => {
                            buf.clear();
                            zc.as_mut().unwrap().compress_to_buffer(b, &mut buf).unwrap();
                            data.extend_from_slice(&buf);
                        }
                        Codec::Lz4 => {
                            data.extend_from_slice(&(b.len() as u16).to_le_bytes());
                            let c = if cfg.dict { lz4_flex::block::compress_with_dict(b, &dict) } else { lz4_flex::block::compress(b) };
                            data.extend_from_slice(&c);
                        }
                    }
                }
                offs.push(data.len() as u32);
                let mut fb = FileBuilder::new();
                fb.section(&data);
                fb.section(as_bytes(&offs));
                index_bytes(&hi, &lo, &mut fb);
                fb.section(&vals.to_bytes());
                fb.section(&dict);
                std::fs::write(path, fb.finish()).unwrap();
            }
        }
    }

    fn open<'a>(&self, path: &Path, mm: &'a [u8]) -> Box<dyn Reader + 'a> {
        match self {
            Layout::Run => Box::new(RunReader(run::Run::open(path).unwrap())),
            Layout::Front { cfg, idx, .. } => {
                let s = sections(mm);
                Box::new(FrontReader { blocks: s[0], bs: cfg.bs, idx: Index::new(u64s(s[1]), u64s(s[2]), *idx), vals: Vals::from_bytes(s[3]), tab: u64s(s[4]) })
            }
            Layout::Fixed { cfg, idx, .. } => {
                let s = sections(mm);
                let dict = s[5];
                let zdict = match cfg.codec {
                    Codec::Zstd(_) if cfg.dict => Some(&*Box::leak(Box::new(zstd::dict::DecoderDictionary::copy(dict)))),
                    _ => None,
                };
                Box::new(FixedReader { data: s[0], offs: u32s(s[1]), idx: Index::new(u64s(s[2]), u64s(s[3]), *idx), vals: Vals::from_bytes(s[4]), codec: cfg.codec, dict, zdict, bs: cfg.bs })
            }
        }
    }
}

// ---------------------------------------------------------------- measurement

struct Res {
    bkey: f64,
    build: f64,
    get1: f64,
    p50: f64,
    p99: f64,
    get16: f64,
    miss: f64,
    scan: f64,
    decomp: f64,
    ram: usize,
    small1: f64,
    small16: f64,
    file: usize,
}

fn median(v: &mut [f64]) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

fn touch(mm: &[u8]) -> u64 {
    let mut s = 0u64;
    let mut i = 0;
    while i < mm.len() {
        s += mm[i] as u64;
        i += 4096;
    }
    s
}

fn get_loop(r: &dyn Reader, keys: &[&[u8]], n: usize, start: usize) -> f64 {
    let mut sc = Scratch::new();
    let mask = keys.len() - 1;
    let mut sink = 0usize;
    let t = Instant::now();
    for i in 0..n {
        sink += r.get(keys[(start + i) & mask], &mut sc).map_or(0, |v| v.len());
    }
    let d = t.elapsed();
    std::hint::black_box(sink);
    d.as_nanos() as f64 / n as f64
}

fn get_mt(r: &dyn Reader, keys: &[&[u8]], per: usize, threads: usize) -> f64 {
    let t = Instant::now();
    std::thread::scope(|s| {
        for th in 0..threads {
            s.spawn(move || get_loop(r, keys, per, th * 7919));
        }
    });
    t.elapsed().as_nanos() as f64 / (per * threads) as f64
}

fn percentiles(r: &dyn Reader, keys: &[&[u8]], n: usize) -> (f64, f64) {
    let mut sc = Scratch::new();
    let mask = keys.len() - 1;
    let mut ts: Vec<u32> = Vec::with_capacity(n);
    let mut sink = 0usize;
    for i in 0..n {
        let t = Instant::now();
        sink += r.get(keys[(i * 31) & mask], &mut sc).map_or(0, |v| v.len());
        ts.push(t.elapsed().as_nanos() as u32);
    }
    std::hint::black_box(sink);
    ts.sort_unstable();
    (ts[n / 2] as f64, ts[n * 99 / 100] as f64)
}

fn measure(l: &Layout, rows: &[Row], keys: &[&[u8]], misses: &[&[u8]], small: &[Row], small_keys: &[&[u8]], dir: &Path, rounds: usize, small_only_get: bool) -> Res {
    let path = dir.join("cand.run");
    let (mut build, mut get1, mut p50, mut p99, mut get16, mut miss, mut scan, mut decomp, mut small1, mut small16) =
        (vec![], vec![], vec![], vec![], vec![], vec![], vec![], vec![], vec![], vec![]);
    let (mut file, mut ram) = (0, 0);
    for round in 0..rounds {
        let t = Instant::now();
        l.build(rows, &path);
        build.push(t.elapsed().as_secs_f64());
        let f = std::fs::File::open(&path).unwrap();
        let mm = unsafe { Mmap::map(&f).unwrap() };
        file = mm.len();
        std::hint::black_box(touch(&mm));
        let r = l.open(&path, &mm);
        ram = r.ram();
        if round == 0 {
            // correctness: every sampled key answers its value, misses miss,
            // the scan returns every row in order
            let mut sc = Scratch::new();
            let mut rng = Rng::new(5);
            for _ in 0..20_000 {
                let (k, v) = &rows[rng.below(rows.len())];
                assert_eq!(r.get(k, &mut sc), Some(v.as_slice()), "{}: wrong value for {:02x?}", l.name(), k);
            }
            for m in misses.iter().take(20_000) {
                assert!(r.get(m, &mut sc).is_none(), "{}: hit on a missing key", l.name());
            }
            let mut i = 0usize;
            r.scan(&mut sc, &mut |k, v| {
                assert!(rows[i].0 == k && rows[i].1 == v, "{}: scan row {i} differs", l.name());
                i += 1;
            });
            assert_eq!(i, rows.len(), "{}: scan count", l.name());
        }
        get1.push(get_loop(&*r, keys, 2_000_000, 0));
        let (a, b) = percentiles(&*r, keys, 200_000);
        p50.push(a);
        p99.push(b);
        get16.push(get_mt(&*r, keys, 1_000_000, 16));
        miss.push(get_loop(&*r, misses, 1_000_000, 0));
        let mut sc = Scratch::new();
        let t = Instant::now();
        let mut n = 0usize;
        r.scan(&mut sc, &mut |_, v| n += v.len());
        std::hint::black_box(n);
        scan.push(rows.len() as f64 / t.elapsed().as_secs_f64() / 1e6);
        if matches!(l, Layout::Fixed { cfg, .. } if cfg.codec != Codec::Plain) {
            let nb = r.nblk();
            let mut rng = Rng::new(7);
            let bl: Vec<usize> = (0..200_000).map(|_| rng.below(nb)).collect();
            let t = Instant::now();
            let mut s = 0;
            for &b in &bl {
                s += r.decompress(&mut sc, b);
            }
            std::hint::black_box(s);
            decomp.push(t.elapsed().as_nanos() as f64 / bl.len() as f64);
        } else {
            decomp.push(0.0);
        }
        drop(r);
        drop(mm);
        if !small_only_get {
            // the L3-resident variant: the same layout over 200k rows
            let sp = dir.join("small.run");
            l.build(small, &sp);
            let f = std::fs::File::open(&sp).unwrap();
            let mm = unsafe { Mmap::map(&f).unwrap() };
            std::hint::black_box(touch(&mm));
            let r = l.open(&sp, &mm);
            get_loop(&*r, small_keys, 500_000, 0);
            small1.push(get_loop(&*r, small_keys, 2_000_000, 0));
            small16.push(get_mt(&*r, small_keys, 1_000_000, 16));
        } else {
            small1.push(0.0);
            small16.push(0.0);
        }
    }
    Res {
        bkey: file as f64 / rows.len() as f64,
        build: median(&mut build),
        get1: median(&mut get1),
        p50: median(&mut p50),
        p99: median(&mut p99),
        get16: median(&mut get16),
        miss: median(&mut miss),
        scan: median(&mut scan),
        decomp: median(&mut decomp),
        ram,
        small1: median(&mut small1),
        small16: median(&mut small16),
        file,
    }
}

fn miss_keys(rows: &[Row], n: usize) -> Vec<Vec<u8>> {
    let mut rng = Rng::new(17);
    (0..n)
        .map(|_| {
            let mut k = rows[rng.below(rows.len())].0.clone();
            if k.len() == 65 {
                // an unset slot of an existing contract
                let tail = k.len() - 8;
                rng.fill(&mut k[tail..]);
            } else {
                // an address never seen
                rng.fill(&mut k[..32]);
            }
            k
        })
        .collect()
}

fn candidates(set: &str) -> Vec<Layout> {
    let mut v = vec![];
    for s in set.split(',') {
        match s {
            "A" => v.push(Layout::Run),
            "B" => {
                for bs in [2048, 4096, 8192] {
                    for restart in [0, 8, 16, 32] {
                        v.push(Layout::Front { cfg: FrontCfg { bs, restart }, idx: Idx::Bin, enc: ValEnc::Raw, sidecar: false });
                    }
                }
            }
            "C" => {
                for bs in [4096, 8192] {
                    for idx in [Idx::Interp, Idx::Mixed] {
                        v.push(Layout::Front { cfg: FrontCfg { bs, restart: 16 }, idx, enc: ValEnc::Raw, sidecar: false });
                    }
                }
            }
            "D" => {
                for bs in [512, 1024, 4096] {
                    for codec in [Codec::Zstd(1), Codec::Zstd(3), Codec::Lz4] {
                        for dict in [false, true] {
                            v.push(Layout::Fixed { cfg: FixedCfg { bs, codec, dict }, idx: Idx::Bin, enc: ValEnc::Raw });
                        }
                    }
                }
                v.push(Layout::Fixed { cfg: FixedCfg { bs: 4096, codec: Codec::Plain, dict: false }, idx: Idx::Bin, enc: ValEnc::Raw });
            }
            "E" => {
                for restart in [8, 16] {
                    v.push(Layout::Front { cfg: FrontCfg { bs: 4096, restart }, idx: Idx::Bin, enc: ValEnc::Raw, sidecar: true });
                }
            }
            "F" => {
                for enc in [ValEnc::Packed, ValEnc::Dict] {
                    v.push(Layout::Front { cfg: FrontCfg { bs: 4096, restart: 16 }, idx: Idx::Bin, enc, sidecar: false });
                }
            }
            _ => panic!("unknown set {s}"),
        }
    }
    v
}

fn main() {
    let a: Vec<String> = std::env::args().collect();
    let rows_path = PathBuf::from(&a[1]);
    let arg = |name: &str| a.iter().position(|x| x == name).map(|i| a[i + 1].clone());
    let raw = a.iter().any(|x| x == "--raw");
    let rounds: usize = arg("--rounds").map_or(3, |s| s.parse().unwrap());
    let set = arg("--set").unwrap_or_else(|| "A,B,C,D,E,F".into());
    let only = arg("--only");
    let dir = std::env::temp_dir().join(format!("layout-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();

    let t = Instant::now();
    let rows: Vec<Row> = if raw {
        let mut rows = read_rows(&rows_path).unwrap();
        rows.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        rows.dedup_by(|a, b| a.0 == b.0);
        rows
    } else {
        let cache = rows_path.with_extension("contract.bin");
        if cache.exists() {
            read_rows(&cache).unwrap()
        } else {
            let rows = to_contract_rows(&read_rows(&rows_path).unwrap());
            write_rows(&cache, &rows).unwrap();
            rows
        }
    };
    let accts = rows.iter().filter(|r| r.0.len() == 33).count();
    let kv: usize = rows.iter().map(|(k, v)| k.len() + v.len()).sum();
    eprintln!("{} rows ({} accounts, {} slots), {:.1} B/key raw key+value, loaded in {:.1?}", rows.len(), accts, rows.len() - accts, kv as f64 / rows.len() as f64, t.elapsed());

    let mut rng = Rng::new(3);
    let keys: Vec<&[u8]> = (0..1 << 20).map(|_| rows[rng.below(rows.len())].0.as_slice()).collect();
    let misses_v = miss_keys(&rows, 1 << 20);
    let misses: Vec<&[u8]> = misses_v.iter().map(|k| k.as_slice()).collect();
    let mut small: Vec<Row> = (0..200_000).map(|_| rows[rng.below(rows.len())].clone()).collect();
    small.sort_unstable();
    small.dedup_by(|a, b| a.0 == b.0);
    let small_keys: Vec<&[u8]> = (0..1 << 18).map(|_| small[rng.below(small.len())].0.as_slice()).collect();

    let load = std::fs::read_to_string("/proc/loadavg").unwrap_or_default();
    println!("\nload at start: {}", load.trim());
    println!("| candidate | B/key | file MB | build s | Get 1T ns | p50 | p99 | Get 16T ns | miss ns | scan Mkeys/s | decomp ns | RAM MB | L3 Get 1T | L3 Get 16T |");
    println!("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|");
    for l in candidates(&set) {
        if let Some(o) = &only {
            if !l.name().contains(o.as_str()) {
                continue;
            }
        }
        let small_only = false;
        let r = measure(&l, &rows, &keys, &misses, &small, &small_keys, &dir, rounds, small_only);
        println!(
            "| {} | {:.2} | {:.0} | {:.2} | {:.0} | {:.0} | {:.0} | {:.1} | {:.0} | {:.1} | {:.0} | {:.1} | {:.0} | {:.1} |",
            l.name(),
            r.bkey,
            r.file as f64 / 1e6,
            r.build,
            r.get1,
            r.p50,
            r.p99,
            r.get16,
            r.miss,
            r.scan,
            r.decomp,
            r.ram as f64 / 1e6,
            r.small1,
            r.small16
        );
    }
    let load = std::fs::read_to_string("/proc/loadavg").unwrap_or_default();
    println!("\nload at end: {}", load.trim());
    let _ = std::fs::remove_dir_all(&dir);
}

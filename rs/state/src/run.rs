//! An immutable, mmapped, front-coded sorted run: format version 2 (the
//! layout LAYOUT.md recommends; not Go-compatible).
//!
//! File layout, little-endian throughout:
//!
//! - blocks: `nblk` x `BLOCK_SIZE`. A block is [used u16] then entries
//!   [shared u8][unshared u8][vlen u8][key suffix][stored value bytes], the
//!   first entry with shared = 0, zero padding to `BLOCK_SIZE`; an entry
//!   never straddles blocks. vlen 0..=127 is a literal length; vlen 128+i
//!   means dictionary value i and no bytes.
//! - dictionary: [count u8] then count x [len u8][bytes], padded to 8: the
//!   run's whole-value dictionary (stored forms, see below).
//! - index: hi[nblk] u64, lo[nblk] u64, impure[nimp] u64. hi is the
//!   big-endian first 8 bytes of a block's first key, lo its bytes 33..41
//!   (the slot-hash prefix of a storage key), both zero padded. impure lists
//!   the hi values whose blocks' first keys do not all share bytes 8..33
//!   (two contracts with the same 8 leading hash bytes), sorted.
//! - footer (`FOOTER_LEN` bytes): magic "epochrun", version u32, block size
//!   u32, entry count u64, block count u64, dictionary offset u64, index
//!   offset u64, nimp u64, 32 bytes of user data, crc32 over the dictionary,
//!   the index and the footer before the crc.
//!
//! Stored values: a value under a 33-byte key (an account) that is canonical
//! RLP[nonce, balance, codeHash] is packed as [nlen<<1 | empty][blen][nonce]
//! [balance][codeHash unless it is the empty-code hash]; any other value
//! under a 33-byte key is stored behind a 0xFF tag byte; values under other
//! keys are stored as given. `get` and the iterator return the original
//! bytes (a packed account is rebuilt in a scratch buffer).
//!
//! The dictionary is learned inside the Writer from the first `CENSUS_ROWS`
//! rows (buffered, then written through): the top 127 stored forms by
//! bytes saved. Keys are hashes, so the first rows are a uniform sample of
//! contracts; a value that is common later in the file is only stored
//! longer, never wrong.
//!
//! Search: the distinct hi values and their first block form the contract
//! table (built at open), searched by interpolation. Inside one hi group
//! the blocks are searched on lo, by interpolation for wide groups; that is
//! exact when the group's first keys share bytes 8..33, which the reader
//! confirms on the block it lands on, and impure groups are binary searched
//! on their stored first keys. An exact (hi, lo) tie is resolved on the
//! stored first key as well, so the index is never wrong for any key.

use crate::rlp;
use crate::sample::EMPTY_CODE_HASH;
use crate::KvIter;
use memmap2::Mmap;
use std::cell::UnsafeCell;
use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::ops::Range;
use std::path::Path;

pub const BLOCK_SIZE: usize = 2048;
pub const FOOTER_LEN: usize = 92;
pub const MAX_KEY: usize = 255;
/// Stored values use vlen 0..=127; 128..=254 are dictionary codes.
pub const MAX_VALUE: usize = 127;
const DICT_MAX: usize = 127;
/// Rows the Writer buffers to learn the dictionary before the first block.
pub const CENSUS_ROWS: usize = 1 << 18;
const MAGIC: &[u8; 8] = b"epochrun";
const VERSION: u32 = 2;
const RAW_TAG: u8 = 0xFF;
const _: () = assert!(cfg!(target_endian = "little"), "the index arrays are mapped in place");

fn bad(msg: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg)
}

/// N bytes of k from `from`, zero padded.
#[inline]
fn pfx<const N: usize>(k: &[u8], from: usize) -> [u8; N] {
    let mut p = [0u8; N];
    if k.len() > from {
        let n = (k.len() - from).min(N);
        p[..n].copy_from_slice(&k[from..from + n]);
    }
    p
}
#[inline]
fn hi_of(k: &[u8]) -> u64 {
    u64::from_be_bytes(pfx::<8>(k, 0))
}
#[inline]
fn lo_of(k: &[u8]) -> u64 {
    u64::from_be_bytes(pfx::<8>(k, 33))
}
#[inline]
fn mid_of(k: &[u8]) -> [u8; 25] {
    pfx::<25>(k, 8)
}

// ---------------------------------------------------------------- values

/// Rebuilds the RLP of a packed account into buf; returns its range.
fn unpack(p: &[u8], buf: &mut [u8; 128]) -> Range<usize> {
    let (nl, empty, bl) = ((p[0] >> 1) as usize, p[0] & 1 == 1, p[1] as usize);
    let nonce = &p[2..2 + nl];
    let bal = &p[2 + nl..2 + nl + bl];
    let code: &[u8] = if empty { &EMPTY_CODE_HASH } else { &p[2 + nl + bl..2 + nl + bl + 32] };
    let mut n = 2;
    for s in [nonce, bal, code] {
        if s.len() == 1 && s[0] < 0x80 {
            buf[n] = s[0];
            n += 1;
        } else {
            buf[n] = 0x80 + s.len() as u8;
            buf[n + 1..n + 1 + s.len()].copy_from_slice(s);
            n += 1 + s.len();
        }
    }
    let body = n - 2;
    if body < 56 {
        buf[1] = 0xc0 + body as u8;
        1..n
    } else {
        buf[0] = 0xf8;
        buf[1] = body as u8;
        0..n
    }
}

/// Packs an account value into out; false when v is not canonical
/// RLP[nonce, balance, codeHash] (checked by unpacking it again).
fn pack(v: &[u8], out: &mut Vec<u8>) -> bool {
    let Some((c, rest)) = rlp::split_list(v) else { return false };
    let Some((nonce, r)) = rlp::split_string(c) else { return false };
    let Some((bal, r)) = rlp::split_string(r) else { return false };
    let Some((code, r)) = rlp::split_string(r) else { return false };
    if !rest.is_empty() || !r.is_empty() || nonce.len() > 8 || bal.len() > 32 || code.len() != 32 {
        return false;
    }
    let empty = code == EMPTY_CODE_HASH;
    out.clear();
    out.push((nonce.len() as u8) << 1 | empty as u8);
    out.push(bal.len() as u8);
    out.extend_from_slice(nonce);
    out.extend_from_slice(bal);
    if !empty {
        out.extend_from_slice(code);
    }
    let mut buf = [0u8; 128];
    let r = unpack(out, &mut buf);
    &buf[r] == v
}

/// The stored form of value under key (before the dictionary).
fn store_form<'a>(key: &[u8], v: &'a [u8], buf: &'a mut Vec<u8>) -> &'a [u8] {
    if key.len() != 33 || v.is_empty() {
        return v;
    }
    if pack(v, buf) {
        return buf;
    }
    buf.clear();
    buf.push(RAW_TAG);
    buf.extend_from_slice(v);
    buf
}

/// The original value of a stored form; buf receives a rebuilt account.
#[inline]
fn load_form<'a>(key: &[u8], s: &'a [u8], buf: &'a mut [u8; 128]) -> &'a [u8] {
    if key.len() != 33 || s.is_empty() {
        return s;
    }
    if s[0] == RAW_TAG {
        return &s[1..];
    }
    let r = unpack(s, buf);
    &buf[r]
}

thread_local! {
    static SCRATCH: UnsafeCell<[u8; 128]> = const { UnsafeCell::new([0; 128]) };
}

/// The per-thread buffer `Run::get` rebuilds packed accounts in. The slice a
/// `get` returns for such a value is valid until this thread's next `get`
/// on any run: every caller decodes or copies it at once, and this keeps
/// `get` allocation free with an unchanged signature.
#[inline]
fn scratch<'a>() -> &'a mut [u8; 128] {
    SCRATCH.with(|s| unsafe { &mut *s.get() })
}

type Map<K, V> = hashbrown::HashMap<K, V, foldhash::fast::FixedState>;

struct Dict {
    vals: Vec<Vec<u8>>,
    idx: Map<Vec<u8>, u8>,
}

impl Dict {
    fn new(vals: Vec<Vec<u8>>) -> Dict {
        let idx = vals.iter().enumerate().map(|(i, v)| (v.clone(), i as u8)).collect();
        Dict { vals, idx }
    }
}

// ---------------------------------------------------------------- writer

/// Writer builds a run. `add` must be called in strictly ascending key order.
pub struct Writer {
    f: File,
    w: BufWriter<File>,
    blk: Vec<u8>,
    /// last key added (ordering) and last key emitted (front coding)
    prev: Vec<u8>,
    last: Vec<u8>,
    hi: Vec<u64>,
    lo: Vec<u64>,
    impure: Vec<u64>,
    group_mid: [u8; 25],
    n: u64,
    user: [u8; 32],
    /// rows buffered for the census: [klen u8][slen u8][key][stored form]
    pending: Vec<u8>,
    npending: usize,
    dict: Option<Dict>,
    vb: Vec<u8>,
}

impl Writer {
    pub fn create(path: &Path) -> io::Result<Writer> {
        let f = File::create(path)?;
        let w = BufWriter::with_capacity(1 << 20, f.try_clone()?);
        let mut blk = Vec::with_capacity(BLOCK_SIZE);
        blk.extend_from_slice(&[0, 0]);
        Ok(Writer {
            f,
            w,
            blk,
            prev: Vec::with_capacity(MAX_KEY),
            last: Vec::with_capacity(MAX_KEY),
            hi: vec![],
            lo: vec![],
            impure: vec![],
            group_mid: [0; 25],
            n: 0,
            user: [0; 32],
            pending: vec![],
            npending: 0,
            dict: None,
            vb: Vec::with_capacity(128),
        })
    }

    /// The 32-byte field stored in the footer (a height, a root).
    pub fn set_user_data(&mut self, u: [u8; 32]) {
        self.user = u;
    }

    pub fn add(&mut self, key: &[u8], value: &[u8]) -> io::Result<()> {
        if key.is_empty() || key.len() > MAX_KEY || value.len() > MAX_VALUE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("latest: key {} bytes, value {} bytes (limits 1..={MAX_KEY} and 0..={MAX_VALUE})", key.len(), value.len()),
            ));
        }
        if self.n > 0 && key <= self.prev.as_slice() {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, format!("latest: key {:x?} not after {:x?}", key, self.prev)));
        }
        let mut vb = std::mem::take(&mut self.vb);
        let stored = store_form(key, value, &mut vb);
        if stored.len() > MAX_VALUE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("latest: {}-byte value under a 33-byte key is not account RLP, {} bytes stored (limit {MAX_VALUE})", value.len(), stored.len()),
            ));
        }
        self.prev.clear();
        self.prev.extend_from_slice(key);
        self.n += 1;
        let r = if self.dict.is_some() {
            self.emit(key, stored)
        } else {
            self.pending.extend_from_slice(&[key.len() as u8, stored.len() as u8]);
            self.pending.extend_from_slice(key);
            self.pending.extend_from_slice(stored);
            self.npending += 1;
            if self.npending == CENSUS_ROWS {
                self.learn()
            } else {
                Ok(())
            }
        };
        self.vb = vb;
        r
    }

    /// Picks the dictionary from the buffered rows and writes them through.
    fn learn(&mut self) -> io::Result<()> {
        let pending = std::mem::take(&mut self.pending);
        let mut cnt: Map<&[u8], u32> = Map::default();
        for (_, s) in pending_rows(&pending) {
            if !s.is_empty() {
                *cnt.entry(s).or_default() += 1;
            }
        }
        let mut all: Vec<(&[u8], u32)> = cnt.into_iter().filter(|(_, c)| *c > 1).collect();
        all.sort_unstable_by(|a, b| (b.0.len() as u64 * b.1 as u64).cmp(&(a.0.len() as u64 * a.1 as u64)).then(a.0.cmp(b.0)));
        all.truncate(DICT_MAX);
        self.dict = Some(Dict::new(all.iter().map(|(v, _)| v.to_vec()).collect()));
        for (k, s) in pending_rows(&pending) {
            self.emit(k, s)?;
        }
        self.npending = 0;
        Ok(())
    }

    fn emit(&mut self, key: &[u8], stored: &[u8]) -> io::Result<()> {
        let dict = self.dict.as_ref().unwrap();
        let (vl, bytes): (u8, &[u8]) = match dict.idx.get(stored) {
            Some(&i) => (128 + i, &[]),
            None => (stored.len() as u8, stored),
        };
        let mut shared = 0;
        if self.blk.len() > 2 {
            while shared < self.last.len() && shared < key.len() && self.last[shared] == key[shared] {
                shared += 1;
            }
            if self.blk.len() + 3 + key.len() - shared + bytes.len() > BLOCK_SIZE {
                self.flush()?;
                shared = 0;
            }
        }
        if self.blk.len() == 2 {
            let h = hi_of(key);
            if self.hi.last() == Some(&h) {
                if mid_of(key) != self.group_mid && self.impure.last() != Some(&h) {
                    self.impure.push(h);
                }
            } else {
                self.group_mid = mid_of(key);
            }
            self.hi.push(h);
            self.lo.push(lo_of(key));
        }
        self.blk.extend_from_slice(&[shared as u8, (key.len() - shared) as u8, vl]);
        self.blk.extend_from_slice(&key[shared..]);
        self.blk.extend_from_slice(bytes);
        self.last.clear();
        self.last.extend_from_slice(key);
        Ok(())
    }

    fn flush(&mut self) -> io::Result<()> {
        let used = self.blk.len();
        if used == 2 {
            return Ok(());
        }
        self.blk[..2].copy_from_slice(&(used as u16).to_le_bytes());
        self.blk.resize(BLOCK_SIZE, 0);
        self.w.write_all(&self.blk)?;
        self.blk.truncate(2);
        Ok(())
    }

    /// Writes the last block, the dictionary, the index and the footer, and syncs.
    pub fn close(mut self) -> io::Result<()> {
        if self.dict.is_none() {
            self.learn()?;
        }
        self.flush()?;
        let dict = self.dict.take().unwrap();
        let mut ds = vec![dict.vals.len() as u8];
        for v in &dict.vals {
            ds.push(v.len() as u8);
            ds.extend_from_slice(v);
        }
        ds.resize((ds.len() + 7) & !7, 0);
        let nblk = self.hi.len() as u64;
        let doff = nblk * BLOCK_SIZE as u64;
        let ioff = doff + ds.len() as u64;
        let mut ft = [0u8; FOOTER_LEN];
        ft[0..8].copy_from_slice(MAGIC);
        ft[8..12].copy_from_slice(&VERSION.to_le_bytes());
        ft[12..16].copy_from_slice(&(BLOCK_SIZE as u32).to_le_bytes());
        ft[16..24].copy_from_slice(&self.n.to_le_bytes());
        ft[24..32].copy_from_slice(&nblk.to_le_bytes());
        ft[32..40].copy_from_slice(&doff.to_le_bytes());
        ft[40..48].copy_from_slice(&ioff.to_le_bytes());
        ft[48..56].copy_from_slice(&(self.impure.len() as u64).to_le_bytes());
        ft[56..88].copy_from_slice(&self.user);
        let mut h = crc32fast::Hasher::new();
        for part in [&ds[..], bytes_of(&self.hi), bytes_of(&self.lo), bytes_of(&self.impure), &ft[..88]] {
            h.update(part);
        }
        ft[88..92].copy_from_slice(&h.finalize().to_le_bytes());
        self.w.write_all(&ds)?;
        self.w.write_all(bytes_of(&self.hi))?;
        self.w.write_all(bytes_of(&self.lo))?;
        self.w.write_all(bytes_of(&self.impure))?;
        self.w.write_all(&ft)?;
        self.w.flush()?;
        self.f.sync_all()
    }
}

/// The (key, stored form) rows of the census buffer.
fn pending_rows(p: &[u8]) -> impl Iterator<Item = (&[u8], &[u8])> {
    let mut i = 0;
    std::iter::from_fn(move || {
        if i >= p.len() {
            return None;
        }
        let (kl, sl) = (p[i] as usize, p[i + 1] as usize);
        let r = (&p[i + 2..i + 2 + kl], &p[i + 2 + kl..i + 2 + kl + sl]);
        i += 2 + kl + sl;
        Some(r)
    })
}

fn bytes_of(v: &[u64]) -> &[u8] {
    unsafe { std::slice::from_raw_parts(v.as_ptr() as *const u8, v.len() * 8) }
}

fn u64s(b: &[u8]) -> &[u64] {
    let (p, s, t) = unsafe { b.align_to::<u64>() };
    assert!(p.is_empty() && t.is_empty(), "index not 8-aligned");
    s
}

/// Number of elements <= t (a is sorted).
#[inline]
fn upper(a: &[u64], t: u64) -> usize {
    a.partition_point(|&x| x <= t)
}

/// `upper` by at most 8 interpolation steps, then binary on the window.
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

// ---------------------------------------------------------------- reader

/// An open, immutable, mmapped run. Safe for concurrent readers.
pub struct Run {
    mm: Mmap,
    ioff: usize,
    nblk: usize,
    n: u64,
    user: [u8; 32],
    dict: Vec<Vec<u8>>,
    /// contract table: distinct hi values and the first block of each
    dh: Vec<u64>,
    db: Vec<u32>,
    impure: Vec<u64>,
}

impl Run {
    /// Maps the file read-only and validates the footer and the checksum.
    pub fn open(path: &Path) -> io::Result<Run> {
        let f = File::open(path)?;
        let size = f.metadata()?.len() as usize;
        if size < FOOTER_LEN {
            return Err(bad(format!("latest: {}: {size} bytes, no footer", path.display())));
        }
        let mm = unsafe { Mmap::map(&f)? };
        let ft: [u8; FOOTER_LEN] = mm[size - FOOTER_LEN..].try_into().unwrap();
        let u32at = |o: usize| u32::from_le_bytes(ft[o..o + 4].try_into().unwrap());
        let u64at = |o: usize| u64::from_le_bytes(ft[o..o + 8].try_into().unwrap());
        if &ft[..8] != MAGIC {
            return Err(bad(format!("latest: {}: bad magic", path.display())));
        }
        if u32at(8) != VERSION {
            return Err(bad(format!("latest: {}: run format version {}, this build reads version {VERSION}", path.display(), u32at(8))));
        }
        let (nblk, doff, ioff, nimp) = (u64at(24), u64at(32), u64at(40), u64at(48));
        if u32at(12) != BLOCK_SIZE as u32
            || nblk > (size / BLOCK_SIZE) as u64
            || doff != nblk * BLOCK_SIZE as u64
            || ioff < doff + 8
            || ioff % 8 != 0
            || ioff > size as u64
            || nimp > nblk
            || ioff + nblk * 16 + nimp * 8 + FOOTER_LEN as u64 != size as u64
        {
            return Err(bad(format!("latest: {}: bad footer", path.display())));
        }
        let (nblk, doff, ioff, nimp) = (nblk as usize, doff as usize, ioff as usize, nimp as usize);
        let mut h = crc32fast::Hasher::new();
        h.update(&mm[doff..size - FOOTER_LEN]);
        h.update(&ft[..88]);
        if h.finalize() != u32at(88) {
            return Err(bad(format!("latest: {}: checksum mismatch", path.display())));
        }
        let ds = &mm[doff..ioff];
        let mut dict = Vec::with_capacity(ds[0] as usize);
        let mut i = 1;
        for _ in 0..ds[0] {
            let l = *ds.get(i).ok_or_else(|| bad(format!("latest: {}: bad dictionary", path.display())))? as usize;
            if i + 1 + l > ds.len() {
                return Err(bad(format!("latest: {}: bad dictionary", path.display())));
            }
            dict.push(ds[i + 1..i + 1 + l].to_vec());
            i += 1 + l;
        }
        let mut user = [0u8; 32];
        user.copy_from_slice(&ft[56..88]);
        let hi = u64s(&mm[ioff..ioff + 8 * nblk]);
        let (mut dh, mut db) = (vec![], vec![]);
        for (b, &h) in hi.iter().enumerate() {
            if dh.last() != Some(&h) {
                dh.push(h);
                db.push(b as u32);
            }
        }
        let impure = u64s(&mm[ioff + 16 * nblk..ioff + 16 * nblk + 8 * nimp]).to_vec();
        Ok(Run { mm, ioff, nblk, n: u64at(16), user, dict, dh, db, impure })
    }

    pub fn len(&self) -> usize {
        self.n as usize
    }
    pub fn is_empty(&self) -> bool {
        self.n == 0
    }
    pub fn bytes(&self) -> usize {
        self.mm.len()
    }
    pub fn blocks(&self) -> usize {
        self.nblk
    }
    pub fn user_data(&self) -> [u8; 32] {
        self.user
    }
    /// The whole-value dictionary (stored forms), for inspection.
    pub fn dictionary(&self) -> &[Vec<u8>] {
        &self.dict
    }

    #[inline]
    fn block(&self, b: usize) -> &[u8] {
        &self.mm[b * BLOCK_SIZE..(b + 1) * BLOCK_SIZE]
    }
    #[inline]
    fn lo(&self) -> &[u64] {
        u64s(&self.mm[self.ioff + 8 * self.nblk..self.ioff + 16 * self.nblk])
    }
    /// Block b's first key, stored whole at the block start.
    #[inline]
    fn first_key(&self, b: usize) -> &[u8] {
        let blk = self.block(b);
        &blk[5..5 + blk[3] as usize]
    }

    /// Finds key. The value aliases the mapping or the dictionary, or, for
    /// a packed account, this thread's scratch (see `scratch`). No allocation.
    pub fn get(&self, key: &[u8]) -> Option<&[u8]> {
        let b = self.seek(key)?;
        let (vl, bytes) = scan_block(self.block(b), key)?;
        let s: &[u8] = if vl >= 128 { &self.dict[(vl - 128) as usize] } else { bytes };
        Some(load_form(key, s, scratch()))
    }

    /// The last block whose first key is <= key, or None.
    fn seek(&self, key: &[u8]) -> Option<usize> {
        let (kh, kl) = (hi_of(key), lo_of(key));
        let j = interp_upper(&self.dh, kh);
        if j == 0 {
            return None;
        }
        let e = if j < self.dh.len() { self.db[j] as usize } else { self.nblk };
        if self.dh[j - 1] < kh {
            return Some(e - 1);
        }
        let s = self.db[j - 1] as usize;
        if e - s > 1 && self.impure.binary_search(&kh).is_ok() {
            return self.seek_first_keys(s, e, key);
        }
        let lo = &self.lo()[s..e];
        let c = if lo.len() > 32 { interp_upper(lo, kl) } else { upper(lo, kl) };
        let t = s + c.max(1) - 1;
        let fk = self.first_key(t);
        match mid_of(key).cmp(&mid_of(fk)) {
            std::cmp::Ordering::Less => return s.checked_sub(1),
            std::cmp::Ordering::Greater => return Some(e - 1),
            std::cmp::Ordering::Equal => {}
        }
        if c == 0 {
            return s.checked_sub(1);
        }
        if lo[c - 1] != kl || fk <= key {
            return Some(t);
        }
        // Exact (hi, lo) tie and block t starts after key: binary search the
        // tied blocks on their stored first keys.
        let f = s + lo[..c].partition_point(|&x| x < kl);
        self.seek_first_keys(f, t, key)
    }

    /// The last block in [s, e) whose first key is <= key, else s - 1.
    fn seek_first_keys(&self, s: usize, e: usize, key: &[u8]) -> Option<usize> {
        let (mut lo, mut hi) = (s, e);
        while lo < hi {
            let m = (lo + hi) / 2;
            if self.first_key(m) <= key {
                lo = m + 1;
            } else {
                hi = m;
            }
        }
        lo.checked_sub(1)
    }

    /// Walks [lo, hi); None means unbounded.
    pub fn iter<'a>(&'a self, lo: Option<&[u8]>, hi: Option<&'a [u8]>) -> RunIter<'a> {
        let mut it = RunIter { r: self, lo: lo.map(|k| k.to_vec()), hi, b: -1, i: 0, used: 0, blk: &[], key: Vec::with_capacity(MAX_KEY), vbuf: Box::new([0; 128]), vr: 0..0 };
        if let Some(lo) = lo {
            if let Some(b) = self.seek(lo) {
                if b > 0 {
                    it.b = b as isize - 1;
                }
            }
        }
        it
    }
}

/// Walks the block's entries in order without materializing a key. `eq` is
/// how many leading bytes the previous key shares with key. The writer
/// stores the maximal shared prefix, so an entry that reuses more than eq
/// bytes of the previous key sorts before key, one that reuses fewer sorts
/// after it, and only an entry with shared == eq needs its suffix compared
/// against key[eq..]. Returns the vlen byte and the stored bytes.
fn scan_block<'a>(b: &'a [u8], key: &[u8]) -> Option<(u8, &'a [u8])> {
    let used = u16::from_le_bytes([b[0], b[1]]) as usize;
    let mut eq = 0usize;
    let mut i = 2;
    while i < used {
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

pub struct RunIter<'a> {
    r: &'a Run,
    lo: Option<Vec<u8>>,
    hi: Option<&'a [u8]>,
    b: isize,
    i: usize,
    used: usize,
    blk: &'a [u8],
    key: Vec<u8>,
    vbuf: Box<[u8; 128]>,
    vr: Range<usize>,
}

impl<'a> RunIter<'a> {
    fn step(&mut self) -> bool {
        while self.i >= self.used {
            self.b += 1;
            if self.b as usize >= self.r.nblk {
                return false;
            }
            self.blk = self.r.block(self.b as usize);
            self.used = u16::from_le_bytes([self.blk[0], self.blk[1]]) as usize;
            self.i = 2;
        }
        let b = self.blk;
        let (sh, un, vl) = (b[self.i] as usize, b[self.i + 1] as usize, b[self.i + 2]);
        self.i += 3;
        self.key.truncate(sh);
        self.key.extend_from_slice(&b[self.i..self.i + un]);
        self.i += un;
        let s: &[u8] = if vl >= 128 {
            &self.r.dict[(vl - 128) as usize]
        } else {
            let s = &b[self.i..self.i + vl as usize];
            self.i += vl as usize;
            s
        };
        if self.key.len() == 33 && !s.is_empty() && s[0] != RAW_TAG {
            self.vr = unpack(s, &mut self.vbuf);
        } else {
            let s = if self.key.len() == 33 && !s.is_empty() { &s[1..] } else { s };
            self.vbuf[..s.len()].copy_from_slice(s);
            self.vr = 0..s.len();
        }
        true
    }
}

impl KvIter for RunIter<'_> {
    fn next(&mut self) -> bool {
        while self.step() {
            if let Some(lo) = &self.lo {
                if self.key.as_slice() < lo.as_slice() {
                    continue;
                }
                self.lo = None;
            }
            if let Some(hi) = self.hi {
                if self.key.as_slice() >= hi {
                    self.b = self.r.nblk as isize;
                    self.i = 0;
                    self.used = 0;
                    return false;
                }
            }
            return true;
        }
        false
    }
    fn key(&self) -> &[u8] {
        &self.key
    }
    fn value(&self) -> &[u8] {
        &self.vbuf[self.vr.clone()]
    }
}

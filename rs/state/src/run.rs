//! An immutable, mmapped, front-coded sorted run, byte-compatible with Go's
//! latest/run.go.
//!
//! File layout: N blocks of `BLOCK_SIZE` bytes, then the index (N packed
//! first-key prefixes of `PREFIX_LEN` bytes, zero padded), then the footer.
//! A block is [used u16] then entries [shared u8][unshared u8][vlen u8]
//! [key suffix][value], the first entry of a block with shared = 0, and zero
//! padding to `BLOCK_SIZE`. An entry never straddles blocks.
//!
//! Footer (76 bytes, little-endian): magic "epochrun", version u32, block
//! size u32, entry count u64, block count u64, index offset u64, 32 bytes of
//! user data, crc32 over the index then the first 72 footer bytes.

use crate::KvIter;
use memmap2::Mmap;
use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::path::Path;

pub const BLOCK_SIZE: usize = 4096;
pub const PREFIX_LEN: usize = 40;
pub const FOOTER_LEN: usize = 76;
pub const MAX_KEY: usize = 255;
pub const MAX_VALUE: usize = 255;
const MAGIC: &[u8; 8] = b"epochrun";
const VERSION: u32 = 1;

fn bad(msg: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg)
}

/// Writer builds a run. `add` must be called in strictly ascending key order.
pub struct Writer {
    f: File,
    w: BufWriter<File>,
    blk: Vec<u8>,
    prev: Vec<u8>,
    index: Vec<u8>,
    n: u64,
    user: [u8; 32],
}

impl Writer {
    pub fn create(path: &Path) -> io::Result<Writer> {
        let f = File::create(path)?;
        let w = BufWriter::with_capacity(1 << 20, f.try_clone()?);
        let mut blk = Vec::with_capacity(BLOCK_SIZE);
        blk.extend_from_slice(&[0, 0]);
        Ok(Writer { f, w, blk, prev: Vec::with_capacity(MAX_KEY), index: vec![], n: 0, user: [0; 32] })
    }

    /// The 32-byte field stored in the footer (a height, a root).
    pub fn set_user_data(&mut self, u: [u8; 32]) {
        self.user = u;
    }

    pub fn add(&mut self, key: &[u8], value: &[u8]) -> io::Result<()> {
        if key.is_empty() || key.len() > MAX_KEY || value.len() > MAX_VALUE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("latest: key {} bytes, value {} bytes (limits 1..{MAX_KEY} and 0..{MAX_VALUE})", key.len(), value.len()),
            ));
        }
        if self.n > 0 && key <= self.prev.as_slice() {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, format!("latest: key {:x?} not after {:x?}", key, self.prev)));
        }
        let mut shared = 0;
        if self.blk.len() > 2 {
            while shared < self.prev.len() && shared < key.len() && self.prev[shared] == key[shared] {
                shared += 1;
            }
            if self.blk.len() + 3 + key.len() - shared + value.len() > BLOCK_SIZE {
                self.flush()?;
                shared = 0;
            }
        }
        if self.blk.len() == 2 {
            let mut p = [0u8; PREFIX_LEN];
            let n = key.len().min(PREFIX_LEN);
            p[..n].copy_from_slice(&key[..n]);
            self.index.extend_from_slice(&p);
        }
        self.blk.extend_from_slice(&[shared as u8, (key.len() - shared) as u8, value.len() as u8]);
        self.blk.extend_from_slice(&key[shared..]);
        self.blk.extend_from_slice(value);
        self.prev.clear();
        self.prev.extend_from_slice(key);
        self.n += 1;
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

    /// Writes the last block, the index and the footer, and syncs.
    pub fn close(mut self) -> io::Result<()> {
        self.flush()?;
        let nblk = (self.index.len() / PREFIX_LEN) as u64;
        let mut ft = [0u8; FOOTER_LEN];
        ft[0..8].copy_from_slice(MAGIC);
        ft[8..12].copy_from_slice(&VERSION.to_le_bytes());
        ft[12..16].copy_from_slice(&(BLOCK_SIZE as u32).to_le_bytes());
        ft[16..24].copy_from_slice(&self.n.to_le_bytes());
        ft[24..32].copy_from_slice(&nblk.to_le_bytes());
        ft[32..40].copy_from_slice(&(nblk * BLOCK_SIZE as u64).to_le_bytes());
        ft[40..72].copy_from_slice(&self.user);
        let crc = footer_crc(&self.index, &ft);
        ft[72..76].copy_from_slice(&crc.to_le_bytes());
        self.w.write_all(&self.index)?;
        self.w.write_all(&ft)?;
        self.w.flush()?;
        self.f.sync_all()
    }
}

fn footer_crc(index: &[u8], ft: &[u8; FOOTER_LEN]) -> u32 {
    let mut h = crc32fast::Hasher::new();
    h.update(index);
    h.update(&ft[..72]);
    h.finalize()
}

/// An open, immutable, mmapped run. Safe for concurrent readers.
pub struct Run {
    mm: Mmap,
    ioff: usize,
    nblk: usize,
    n: u64,
    user: [u8; 32],
}

impl Run {
    /// Maps the file read-only and validates the footer and the index checksum.
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
        let nblk = u64at(24);
        let ioff = u64at(32);
        if &ft[..8] != MAGIC
            || u32at(8) != VERSION
            || u32at(12) != BLOCK_SIZE as u32
            || nblk > (size / BLOCK_SIZE) as u64
            || ioff != nblk * BLOCK_SIZE as u64
            || ioff + nblk * PREFIX_LEN as u64 + FOOTER_LEN as u64 != size as u64
        {
            return Err(bad(format!("latest: {}: bad footer", path.display())));
        }
        let (ioff, nblk) = (ioff as usize, nblk as usize);
        if footer_crc(&mm[ioff..ioff + nblk * PREFIX_LEN], &ft) != u32at(72) {
            return Err(bad(format!("latest: {}: index checksum mismatch", path.display())));
        }
        let mut user = [0u8; 32];
        user.copy_from_slice(&ft[40..72]);
        Ok(Run { mm, ioff, nblk, n: u64at(16), user })
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

    #[inline]
    fn block(&self, b: usize) -> &[u8] {
        &self.mm[b * BLOCK_SIZE..(b + 1) * BLOCK_SIZE]
    }
    #[inline]
    fn prefix(&self, b: usize) -> &[u8] {
        &self.mm[self.ioff + b * PREFIX_LEN..self.ioff + (b + 1) * PREFIX_LEN]
    }
    /// Block b's first key, stored whole at the block start.
    #[inline]
    fn first_key(&self, b: usize) -> &[u8] {
        let blk = self.block(b);
        &blk[5..5 + blk[3] as usize]
    }

    /// Finds key; the value aliases the mapping. No allocation.
    pub fn get(&self, key: &[u8]) -> Option<&[u8]> {
        let b = self.seek(key)?;
        scan_block(self.block(b), key)
    }

    /// The last block whose first key is <= key, or None.
    fn seek(&self, key: &[u8]) -> Option<usize> {
        let mut kp = [0u8; PREFIX_LEN];
        let n = key.len().min(PREFIX_LEN);
        kp[..n].copy_from_slice(&key[..n]);
        // Random keys decide on the first 8 bytes nearly always, so compare
        // those as one word before the 40-byte memcmp.
        let kp0 = u64::from_be_bytes(kp[..8].try_into().unwrap());
        let (mut lo, mut hi) = (0usize, self.nblk);
        while lo < hi {
            let m = (lo + hi) / 2;
            let p = self.prefix(m);
            let p0 = u64::from_be_bytes(p[..8].try_into().unwrap());
            let le = if p0 != kp0 { p0 < kp0 } else { p <= &kp[..] };
            if le {
                lo = m + 1;
            } else {
                hi = m;
            }
        }
        if lo == 0 {
            return None;
        }
        let b = lo - 1;
        if self.prefix(b) != &kp[..] || self.first_key(b) <= key {
            return Some(b);
        }
        // The prefix ties and block b starts after key: find the first tied
        // block, then binary search the tied range on the full first keys.
        let (mut lo, mut hi) = (0usize, b);
        while lo < hi {
            let m = (lo + hi) / 2;
            if self.prefix(m) < &kp[..] {
                lo = m + 1;
            } else {
                hi = m;
            }
        }
        hi = b;
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

    /// Walks [lo, hi); None means unbounded. Values alias the mapping.
    pub fn iter<'a>(&'a self, lo: Option<&[u8]>, hi: Option<&'a [u8]>) -> RunIter<'a> {
        let mut it = RunIter { r: self, lo: lo.map(|k| k.to_vec()), hi, b: -1, i: 0, used: 0, blk: &[], key: Vec::with_capacity(MAX_KEY), val: &[] };
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
/// against key[eq..].
fn scan_block<'a>(b: &'a [u8], key: &[u8]) -> Option<&'a [u8]> {
    let used = u16::from_le_bytes([b[0], b[1]]) as usize;
    let mut eq = 0usize;
    let mut i = 2;
    while i < used {
        let (sh, un, vl) = (b[i] as usize, b[i + 1] as usize, b[i + 2] as usize);
        i += 3;
        if sh < eq {
            return None;
        }
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
                return Some(&b[i + un..i + un + vl]);
            }
            eq += j;
        }
        i += un + vl;
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
    val: &'a [u8],
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
        let (sh, un, vl) = (b[self.i] as usize, b[self.i + 1] as usize, b[self.i + 2] as usize);
        self.i += 3;
        self.key.truncate(sh);
        self.key.extend_from_slice(&b[self.i..self.i + un]);
        self.i += un;
        self.val = &b[self.i..self.i + vl];
        self.i += vl;
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
        self.val
    }
}

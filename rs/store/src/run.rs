//! A run (store/run.go): [chain SST][state SST][lookup SST][124-byte footer],
//! named by casfs. L0 runs use zstd-1 and live in <data>/runs; terminals use
//! zstd-9 and go through the spool.

use crate::casfs::{Hasher, Store};
use crate::format::*;
use crate::sst::{Opts, ReadAt, Sst, SstWriter};
use anyhow::{anyhow, bail, Result};
use std::fs;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

const RUN_MAGIC: &[u8; 8] = b"EPOCHRUN";
pub const FOOTER_SIZE: usize = 3 * 16 + 4 * 8 + 32 + 4 + 8;
pub const ZSTD_LEVEL: i32 = 9;
pub const L0_ZSTD_LEVEL: i32 = 1;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Footer {
    pub off: [u64; 3],
    pub len: [u64; 3],
    pub from_tx: u64,
    pub to_tx: u64,
    pub from_height: u64,
    pub to_height: u64,
    pub prev: [u8; 32],
    pub version: u32,
}

impl Footer {
    pub fn encode(&self) -> Vec<u8> {
        let mut b = Vec::with_capacity(FOOTER_SIZE);
        for i in 0..3 {
            b.extend_from_slice(&self.off[i].to_be_bytes());
            b.extend_from_slice(&self.len[i].to_be_bytes());
        }
        for v in [self.from_tx, self.to_tx, self.from_height, self.to_height] {
            b.extend_from_slice(&v.to_be_bytes());
        }
        b.extend_from_slice(&self.prev);
        b.extend_from_slice(&self.version.to_be_bytes());
        b.extend_from_slice(RUN_MAGIC);
        b
    }
    pub fn decode(b: &[u8]) -> Result<Footer> {
        if b.len() != FOOTER_SIZE {
            bail!("store: footer is {} bytes, want {FOOTER_SIZE}", b.len());
        }
        if &b[FOOTER_SIZE - 8..] != RUN_MAGIC {
            bail!("store: bad run magic");
        }
        let u = |i: usize| u64::from_be_bytes(b[i..i + 8].try_into().unwrap());
        let mut f = Footer::default();
        for i in 0..3 {
            f.off[i] = u(16 * i);
            f.len[i] = u(16 * i + 8);
        }
        f.from_tx = u(48);
        f.to_tx = u(56);
        f.from_height = u(64);
        f.to_height = u(72);
        f.prev.copy_from_slice(&b[80..112]);
        f.version = u32::from_be_bytes(b[112..116].try_into().unwrap());
        if f.version != STORAGE_VERSION {
            bail!("store: run is storage version {}, want {STORAGE_VERSION}", f.version);
        }
        Ok(f)
    }
}

/// Counts bytes and feeds the casfs hasher as pebble's writable does.
struct HashingSink<'a> {
    w: &'a mut BufWriter<fs::File>,
    h: &'a mut Hasher,
    n: u64,
}
impl Write for HashingSink<'_> {
    fn write(&mut self, p: &[u8]) -> std::io::Result<usize> {
        self.h.write(p);
        self.w.write_all(p)?;
        self.n += p.len() as u64;
        Ok(p.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.w.flush()
    }
}

pub fn writer_opts(s: Section, level: i32) -> Opts {
    Opts {
        block_size: s.block_size(),
        index_block_size: s.index_block_size(),
        filter: s.has_filter(),
        zstd_level: if level >= TERMINAL_LEVEL { ZSTD_LEVEL } else { L0_ZSTD_LEVEL },
        comparer: "epochdb.v0",
        merger: "nullptr",
    }
}

pub fn run_file_name(dir: &Path, level: i32, from_tx: u64, to_tx: u64) -> PathBuf {
    dir.join(format!("run-{}.tmp", run_label(level, from_tx, to_tx)))
}

/// Builds one run: three sections in order, each fed already sorted rows.
pub struct RunWriter {
    path: PathBuf,
    level: i32,
    f: BufWriter<fs::File>,
    h: Hasher,
    off: u64,
    footer: Footer,
    pub rows: [u64; 3],
}

impl RunWriter {
    pub fn new(path: PathBuf, prev: [u8; 32], level: i32) -> Result<RunWriter> {
        let f = BufWriter::with_capacity(1 << 20, fs::File::create(&path)?);
        let footer = Footer { prev, version: STORAGE_VERSION, ..Default::default() };
        Ok(RunWriter { path, level, f, h: Hasher::new(), off: 0, footer, rows: [0; 3] })
    }
    /// Writes section s from sorted rows. Sections must come in order.
    pub fn section<I: Iterator<Item = (Vec<u8>, Vec<u8>)>>(&mut self, s: Section, rows: I) -> Result<()> {
        let i = s as usize;
        if (i > 0 && self.footer.len[i - 1] == 0 && self.footer.off[i - 1] == 0 && i != 0 && self.off == 0) && false {
            unreachable!()
        }
        self.footer.off[i] = self.off;
        let mut n = 0u64;
        let len = {
            let mut sink = HashingSink { w: &mut self.f, h: &mut self.h, n: 0 };
            let mut w = SstWriter::new(&mut sink, writer_opts(s, self.level));
            for (k, v) in rows {
                w.set(&k, &v)?;
                n += 1;
            }
            w.finish()?
        };
        self.footer.len[i] = len;
        self.off += len;
        self.rows[i] = n;
        Ok(())
    }
    /// Footer, fsync, adopt: local dir for an L0 run, the spool for a terminal.
    pub fn finish(mut self, cas: &Store, from_tx: u64, to_tx: u64, from_height: u64, to_height: u64) -> Result<(String, Footer)> {
        self.footer.from_tx = from_tx;
        self.footer.to_tx = to_tx;
        self.footer.from_height = from_height;
        self.footer.to_height = to_height;
        let fb = self.footer.encode();
        self.h.write(&fb);
        self.f.write_all(&fb)?;
        self.f.flush()?;
        self.f.get_ref().sync_all()?;
        let label = run_label(self.level, from_tx, to_tx);
        let name = cas.adopt(&self.path, self.h, (self.level < TERMINAL_LEVEL).then_some(label.as_str()))?;
        Ok((name, self.footer))
    }
    pub fn abort(self) {
        let _ = fs::remove_file(&self.path);
    }
}

/// An open run: three SSTs over one artifact.
pub struct Run {
    pub name: String,
    pub footer: Footer,
    pub sec: [Sst; 3],
}

pub fn read_footer(blob: &dyn ReadAt) -> Result<Footer> {
    if blob.size() < FOOTER_SIZE as u64 {
        bail!("store: run is {} bytes, too small for a footer", blob.size());
    }
    let mut fb = vec![0u8; FOOTER_SIZE];
    blob.read_at(blob.size() - FOOTER_SIZE as u64, &mut fb)?;
    Footer::decode(&fb)
}

impl Run {
    pub fn open(cas: &Store, name: &str) -> Result<Run> {
        let blob: Arc<dyn ReadAt> = cas.open_blob(name)?;
        let footer = read_footer(blob.as_ref()).map_err(|e| anyhow!("store: run {name}: {e}"))?;
        let mut secs = Vec::with_capacity(3);
        for i in 0..3 {
            secs.push(Sst::open(blob.clone(), footer.off[i], footer.len[i]).map_err(|e| anyhow!("store: run {name} section {i}: {e}"))?);
        }
        let sec: [Sst; 3] = secs.try_into().ok().unwrap();
        Ok(Run { name: name.to_string(), footer, sec })
    }
    pub fn may_have(&self, s: Section, key: &[u8]) -> bool {
        self.sec[s as usize].may_contain(key)
    }
    pub fn get(&self, s: Section, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.sec[s as usize].get(key)
    }
    pub fn latest(&self, s: Section, prefix: &[u8], at: u64) -> Result<Option<(Vec<u8>, u64)>> {
        self.sec[s as usize].latest(prefix, at)
    }
    /// Calls f for every row in [lo, hi) (hi None = to the end); f returns
    /// false to stop.
    pub fn scan_range(&self, s: Section, lo: &[u8], hi: Option<&[u8]>, mut f: impl FnMut(&[u8], &[u8]) -> bool) -> Result<()> {
        let mut it = self.sec[s as usize].iter();
        let mut ok = if lo.is_empty() { it.first()? } else { it.seek_ge(lo)? };
        while ok {
            if let Some(hi) = hi {
                if it.key() >= hi {
                    return Ok(());
                }
            }
            if !f(it.key(), it.value()) {
                return Ok(());
            }
            ok = it.next()?;
        }
        Ok(())
    }
    /// Posting entries under prefix with txnum in [lo, hi] (postings.go ScanChunks).
    pub fn scan_chunks(&self, prefix: &[u8], lo: u64, hi: u64) -> Result<Vec<(Vec<u8>, u64, u8)>> {
        let gated = split(&suffixed(prefix, 0)) < prefix.len() + 8;
        if gated && !self.may_have(Section::Lookup, &suffixed(prefix, 0)) {
            return Ok(Vec::new());
        }
        let mut out = Vec::new();
        let mut pend: Option<(Vec<u8>, Vec<u8>)> = None;
        let mut err = None;
        let mut decode = |pend: &mut Option<(Vec<u8>, Vec<u8>)>, out: &mut Vec<(Vec<u8>, u64, u8)>| -> Result<()> {
            if let Some((k, v)) = pend.take() {
                let group = k[..k.len() - 8].to_vec();
                let (nums, pay) = crate::ef::decode(txnum_of(&k), &v)?;
                for (i, n) in nums.iter().enumerate() {
                    if *n < lo || *n > hi {
                        continue;
                    }
                    out.push((group.clone(), *n, pay.get(i).copied().unwrap_or(0)));
                }
            }
            Ok(())
        };
        self.scan_range(Section::Lookup, prefix, None, |k, v| {
            if !k.starts_with(prefix) || k.len() < prefix.len() + 8 {
                return false;
            }
            let first = txnum_of(k);
            let same = pend.as_ref().map(|(pk, _)| pk[..pk.len() - 8] == k[..k.len() - 8]).unwrap_or(false);
            if same && first <= lo {
                pend = None;
            } else if let Err(e) = decode(&mut pend, &mut out) {
                err = Some(e);
                return false;
            }
            if first > hi {
                return true;
            }
            pend = Some((k.to_vec(), v.to_vec()));
            true
        })?;
        if let Some(e) = err {
            return Err(e);
        }
        decode(&mut pend, &mut out)?;
        Ok(out)
    }
    /// Distinct group keys under prefix, in key order.
    pub fn scan_groups(&self, prefix: &[u8], mut f: impl FnMut(&[u8]) -> bool) -> Result<()> {
        let gated = split(&suffixed(prefix, 0)) < prefix.len() + 8;
        if gated && !self.may_have(Section::Lookup, &suffixed(prefix, 0)) {
            return Ok(());
        }
        let mut last: Vec<u8> = Vec::new();
        self.scan_range(Section::Lookup, prefix, None, |k, _| {
            if !k.starts_with(prefix) || k.len() < prefix.len() + 8 {
                return false;
            }
            let g = &k[..k.len() - 8];
            if g == last.as_slice() {
                return true;
            }
            last = g.to_vec();
            f(g)
        })
    }
    /// set/ keys under prefix, in key order.
    pub fn scan_set(&self, prefix: &[u8], mut f: impl FnMut(&[u8]) -> bool) -> Result<()> {
        if prefix.len() >= SET_SPLIT && !self.may_have(Section::Lookup, &prefix[..SET_SPLIT]) {
            return Ok(());
        }
        self.scan_range(Section::Lookup, prefix, None, |k, _| {
            if !k.starts_with(prefix) {
                return false;
            }
            f(k)
        })
    }
}

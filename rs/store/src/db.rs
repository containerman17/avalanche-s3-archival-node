//! The DB (store/db.go, join.go): the window over the unflushed blocks plus
//! the sealed runs, newest to oldest; one descent for everything.
//!
//! Deviations from the Go store (user ruling 2026-09-08: Go/Rust artifact
//! compatibility does not matter): the run cut seals SYNCHRONOUSLY on the
//! writer's thread; there is no terminal merge, every sealed run is final
//! (level 1) and goes to the spool, so a publish uploads every run and the
//! manifest lists them all.

use crate::casfs::{latest_pointer, Store};
use crate::format::*;
use crate::run::{read_footer, run_file_name, Footer, Run, RunWriter};
use crate::window::{BlockWrite, Memtable, FROZEN_LOG};
use anyhow::{anyhow, bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct RunRef {
    pub from_tx: u64,
    pub to_tx: u64,
    pub from_height: u64,
    pub to_height: u64,
    pub name: String,
    pub level: i32,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct Manifest {
    pub storage_version: u32,
    pub chain_root: String,
    pub runs: Vec<RunRef>,
}

const MANIFEST_FILE: &str = "manifest.json";

impl Manifest {
    pub fn load(dir: &Path) -> Result<Manifest> {
        let p = dir.join(MANIFEST_FILE);
        if !p.exists() {
            return Ok(Manifest { storage_version: STORAGE_VERSION, ..Default::default() });
        }
        let m: Manifest = serde_json::from_slice(&std::fs::read(&p)?).context("store: manifest")?;
        if m.storage_version != STORAGE_VERSION {
            bail!("store: manifest is storage version {}, this binary is {STORAGE_VERSION}", m.storage_version);
        }
        m.check_tiling()?;
        Ok(m)
    }
    fn check_tiling(&self) -> Result<()> {
        for w in self.runs.windows(2) {
            let (a, b) = (&w[0], &w[1]);
            if b.from_tx != a.to_tx || b.from_height != a.to_height + 1 {
                bail!("store: runs do not tile: {} ends at tx {} block {} and {} starts at tx {} block {}", a.name, a.to_tx, a.to_height, b.name, b.from_tx, b.from_height);
            }
        }
        Ok(())
    }
    /// tmp + fsync + rename + dir fsync: THE publish step for a run.
    pub fn save(&self, dir: &Path) -> Result<()> {
        let mut raw = serde_json::to_vec_pretty(self)?;
        raw.push(b'\n');
        crate::casfs::write_durable(&dir.join(MANIFEST_FILE), &[&raw])?;
        crate::casfs::sync_dir(dir)
    }
    pub fn head(&self) -> Option<&str> {
        self.runs.last().map(|r| r.name.as_str())
    }
}

fn decode_root(s: &str) -> Result<[u8; 32]> {
    let b = hex::decode(s).map_err(|_| anyhow!("store: {s:?} is not a 32-byte hash"))?;
    b.try_into().map_err(|_| anyhow!("store: {s:?} is not a 32-byte hash"))
}

pub struct DB {
    pub dir: PathBuf,
    pub cas: Store,
    pub chain_root: [u8; 32],
    mem: Memtable,
    pub man: Manifest,
    runs: Vec<Arc<Run>>,
    pub flush_txs: u64,
    pub flush_blocks: u64,
    published: Option<String>,
}

impl DB {
    pub fn open(dir: &Path, cas: Store, chain_root: [u8; 32]) -> Result<DB> {
        Self::open_mode(dir, cas, chain_root, false)
    }
    /// Opens beside a live writer: the window log's torn tail is not cut.
    pub fn open_read_only(dir: &Path, cas: Store, chain_root: [u8; 32]) -> Result<DB> {
        Self::open_mode(dir, cas, chain_root, true)
    }

    fn open_mode(dir: &Path, cas: Store, chain_root: [u8; 32], read_only: bool) -> Result<DB> {
        std::fs::create_dir_all(dir)?;
        let mut man = Manifest::load(dir)?;
        let root_hex = hex::encode(chain_root);
        if man.chain_root.is_empty() {
            man.chain_root = root_hex;
        } else if man.chain_root != root_hex {
            bail!("store: data dir holds chain root {}, refusing to open it as {root_hex}", man.chain_root);
        }
        let mut runs = Vec::with_capacity(man.runs.len());
        for r in &man.runs {
            runs.push(Arc::new(Run::open(&cas, &r.name)?));
        }
        let (base_tx, base_height) = match man.runs.last() {
            Some(r) => (r.to_tx, r.to_height + 1),
            None => (0, 0),
        };
        let mut db = DB { dir: dir.to_path_buf(), cas, chain_root, mem: Memtable::open(&dir.join("window").join("window.log"), read_only)?, man, runs, flush_txs: FLUSH_TXS, flush_blocks: FLUSH_BLOCKS, published: None };
        // A frozen log is a cut that did not finish: sealed first, its blocks come before the active log's.
        let fpath = dir.join("window").join(FROZEN_LOG);
        let (mut base_tx, mut base_height) = (base_tx, base_height);
        if fpath.exists() {
            let mut fm = Memtable::open(&fpath, read_only)?;
            fm.recover(base_tx, base_height)?;
            let (fbt, fnt, fbh, fnh, started) = fm.window();
            if started {
                base_tx = fnt;
                base_height = fnh;
                if !read_only {
                    db.seal(fm, fbt, fnt, fbh, fnh)?;
                }
            } else if !read_only {
                fm.remove()?;
            }
        }
        db.mem.recover(base_tx, base_height)?;
        Ok(db)
    }

    pub fn next_tx(&self) -> u64 {
        let (_, next, _, _, started) = self.mem.window();
        if started {
            return next;
        }
        self.man.runs.last().map(|r| r.to_tx).unwrap_or(0)
    }
    pub fn next_height(&self) -> u64 {
        let (_, _, _, next, started) = self.mem.window();
        if started {
            return next;
        }
        self.man.runs.last().map(|r| r.to_height + 1).unwrap_or(0)
    }
    pub fn head(&self) -> Option<u64> {
        self.next_height().checked_sub(1)
    }
    pub fn sync(&mut self) -> Result<()> {
        self.mem.sync()
    }

    /// Folds one block into the window and cuts a run when a trigger fires.
    pub fn write_block(&mut self, b: &BlockWrite) -> Result<()> {
        self.mem.add(b)?;
        self.maybe_flush()
    }

    pub fn maybe_flush(&mut self) -> Result<()> {
        let (bt, nt, bh, nh, started) = self.mem.window();
        if !started || (nt - bt < self.flush_txs && nh - bh < self.flush_blocks) {
            return Ok(());
        }
        self.cut_window()
    }

    /// Cuts the window into a run now (a stop, a test).
    pub fn flush(&mut self) -> Result<()> {
        self.cut_window()
    }

    fn cut_window(&mut self) -> Result<()> {
        let (bt, nt, bh, nh, started) = self.mem.window();
        if !started || nh == bh {
            return Ok(());
        }
        self.mem.sync()?;
        let dir = self.dir.join("window");
        let frozen = dir.join(FROZEN_LOG);
        std::fs::rename(&self.mem.path, &frozen)?;
        crate::casfs::sync_dir(&dir)?;
        let mut fresh = Memtable::open(&dir.join("window.log"), false)?;
        fresh.reset(nt, nh)?;
        let mut m = std::mem::replace(&mut self.mem, fresh);
        m.path = frozen;
        self.seal(m, bt, nt, bh, nh)
    }

    /// Writes the frozen window into a run, publishes it in the manifest,
    /// only then unlinks the log (publish before delete).
    fn seal(&mut self, m: Memtable, base_tx: u64, next_tx: u64, base_height: u64, next_height: u64) -> Result<()> {
        let prev = match self.man.runs.last() {
            Some(r) => decode_root(&r.name)?,
            None => self.chain_root,
        };
        let level = TERMINAL_LEVEL;
        let path = run_file_name(&self.cas.spool, level, base_tx, next_tx);
        let mut w = RunWriter::new(path, prev, level)?;
        if let Err(e) = write_sections(&mut w, &m) {
            w.abort();
            return Err(e);
        }
        let (name, _) = w.finish(&self.cas, base_tx, next_tx, base_height, next_height - 1)?;
        let run = Arc::new(Run::open(&self.cas, &name)?);
        self.man.runs.push(RunRef { from_tx: base_tx, to_tx: next_tx, from_height: base_height, to_height: next_height - 1, name: name.clone(), level });
        self.man.save(&self.dir)?;
        self.runs.push(run);
        m.remove()?;
        eprintln!("store: sealed run {name} [blocks {base_height}..{}]", next_height - 1);
        Ok(())
    }

    // -----------------------------------------------------------------------
    // reads

    fn chain_row(&self, fam: usize, n: u64) -> Result<Option<Vec<u8>>> {
        if let Some(v) = self.mem.chain_get(fam, n)? {
            return Ok(Some(v));
        }
        let by_height = fam_by_height(fam);
        for r in self.runs.iter().rev() {
            let f = &r.footer;
            let inside = if by_height { n >= f.from_height && n <= f.to_height } else { n >= f.from_tx && n < f.to_tx };
            if inside {
                return r.get(Section::Chain, &num_key(FAM_PREFIX[fam], n));
            }
        }
        Ok(None)
    }
    pub fn header_rlp(&self, height: u64) -> Result<Option<Vec<u8>>> {
        self.chain_row(FAM_HDR, height)
    }
    pub fn pvm(&self, height: u64) -> Result<Option<Vec<u8>>> {
        self.chain_row(FAM_PVM, height)
    }
    /// (first TxNum, tx count): the block owns [first, first+count].
    pub fn block_tx_range(&self, height: u64) -> Result<Option<(u64, u32)>> {
        let Some(v) = self.chain_row(FAM_BLK, height)? else { return Ok(None) };
        if v.len() != 12 {
            bail!("store: blk/{height} is {} bytes, want 12", v.len());
        }
        Ok(Some((u64::from_be_bytes(v[..8].try_into().unwrap()), u32::from_be_bytes(v[8..].try_into().unwrap()))))
    }
    /// The TxNum ceiling of a read at the END of height: its boundary slot.
    pub fn txnum_at_end_of(&self, height: u64) -> Result<Option<u64>> {
        Ok(self.block_tx_range(height)?.map(|(f, c)| f + c as u64))
    }
    pub fn tx_rlp(&self, txnum: u64) -> Result<Option<Vec<u8>>> {
        self.chain_row(FAM_TX, txnum)
    }
    pub fn frames(&self, txnum: u64) -> Result<Option<Vec<u8>>> {
        self.chain_row(FAM_ITX, txnum)
    }
    pub fn receipt(&self, txnum: u64) -> Result<Option<Vec<u8>>> {
        self.chain_row(FAM_RCPT, txnum)
    }
    /// The block's receipts blob as the producer handed it (empty if none).
    pub fn receipts_blob(&self, height: u64) -> Result<Option<Vec<u8>>> {
        self.chain_row(FAM_RCB, height)
    }
    /// The state engine's write set and code hashes of a block (recovery replay).
    pub fn write_set(&self, height: u64) -> Result<Option<(Vec<(Vec<u8>, Vec<u8>)>, Vec<[u8; 32]>)>> {
        match self.chain_row(FAM_WS, height)? {
            None => Ok(None),
            Some(b) if b.is_empty() => Ok(Some((Vec::new(), Vec::new()))),
            Some(b) => Ok(Some(crate::window::unframe_ws(&b)?)),
        }
    }
    /// TxNum of the idx-th tx of a block, None past its count.
    pub fn txnum_at(&self, height: u64, idx: u32) -> Result<Option<u64>> {
        Ok(self.block_tx_range(height)?.filter(|(_, c)| idx < *c).map(|(f, _)| f + idx as u64))
    }
    pub fn receipt_at(&self, height: u64, idx: u32) -> Result<Option<Vec<u8>>> {
        match self.txnum_at(height, idx)? {
            Some(n) => self.receipt(n),
            None => Ok(None),
        }
    }
    pub fn frames_at(&self, height: u64, idx: u32) -> Result<Option<Vec<u8>>> {
        match self.txnum_at(height, idx)? {
            Some(n) => self.frames(n),
            None => Ok(None),
        }
    }
    /// (height, index, txnum) of a tx hash.
    pub fn locate_tx(&self, hash: &[u8]) -> Result<Option<(u64, u32, u64)>> {
        let Some(n) = self.txnum_by_hash(hash)? else { return Ok(None) };
        let Some(h) = self.height_of_tx(n)? else { return Ok(None) };
        let (first, _) = self.block_tx_range(h)?.ok_or_else(|| anyhow!("store: blk/{h} missing"))?;
        Ok(Some((h, (n - first) as u32, n)))
    }
    pub fn receipt_by_hash(&self, hash: &[u8]) -> Result<Option<Vec<u8>>> {
        match self.txnum_by_hash(hash)? {
            Some(n) => self.receipt(n),
            None => Ok(None),
        }
    }
    pub fn frames_by_hash(&self, hash: &[u8]) -> Result<Option<Vec<u8>>> {
        match self.txnum_by_hash(hash)? {
            Some(n) => self.frames(n),
            None => Ok(None),
        }
    }
    /// Every tx element of a block plus every receipt row and trace, in order.
    pub fn block_txs(&self, height: u64) -> Result<Option<Vec<(Vec<u8>, Vec<u8>, Vec<u8>)>>> {
        let Some((first, count)) = self.block_tx_range(height)? else { return Ok(None) };
        let mut out = Vec::with_capacity(count as usize);
        for n in first..first + count as u64 {
            let miss = |w: &str| anyhow!("store: {w}/{n} of block {height} missing");
            out.push((self.tx_rlp(n)?.ok_or_else(|| miss("tx"))?, self.receipt(n)?.ok_or_else(|| miss("rcpt"))?, self.frames(n)?.ok_or_else(|| miss("itx"))?));
        }
        Ok(Some(out))
    }
    /// The verbatim container at height (Reassemble).
    pub fn container_at(&self, height: u64) -> Result<Option<Vec<u8>>> {
        let Some(hdr) = self.header_rlp(height)? else { return Ok(None) };
        let pvm = self.pvm(height)?.ok_or_else(|| anyhow!("store: block {height} has a header but no pvm row"))?;
        let (first, count) = self.block_tx_range(height)?.ok_or_else(|| anyhow!("store: block {height} has no blk row"))?;
        let mut txs = Vec::with_capacity(count as usize);
        for i in 0..count as u64 {
            txs.push(self.tx_rlp(first + i)?.ok_or_else(|| anyhow!("store: tx {} of block {height} is missing", first + i))?);
        }
        let refs: Vec<&[u8]> = txs.iter().map(|t| t.as_slice()).collect();
        Ok(Some(crate::container::reassemble(&pvm, &hdr, &refs)?))
    }

    fn lookup_num(&self, key: &[u8]) -> Result<Option<u64>> {
        if let Some(n) = self.mem.nums.get(key) {
            return Ok(Some(*n));
        }
        for r in self.runs.iter().rev() {
            if let Some(v) = r.get(Section::Lookup, key)? {
                if v.len() != 8 {
                    bail!("store: lookup row is {} bytes, want 8", v.len());
                }
                return Ok(Some(u64::from_be_bytes(v[..].try_into().unwrap())));
            }
        }
        Ok(None)
    }
    pub fn txnum_by_hash(&self, hash: &[u8]) -> Result<Option<u64>> {
        self.lookup_num(&txh_key(hash))
    }
    pub fn height_by_hash(&self, hash: &[u8]) -> Result<Option<u64>> {
        self.lookup_num(&blkh_key(hash))
    }
    pub fn height_by_container_id(&self, id: &[u8]) -> Result<Option<u64>> {
        self.lookup_num(&cid_key(id))
    }

    /// The newest value under prefix at or below TxNum at.
    fn latest(&self, prefix: &[u8], at: u64) -> Result<Option<Vec<u8>>> {
        if let Some((v, _)) = self.mem.latest_state(prefix, at) {
            return Ok(Some(v));
        }
        for r in self.runs.iter().rev() {
            if r.footer.from_tx > at {
                continue;
            }
            if let Some((v, _)) = r.latest(Section::State, prefix, at)? {
                return Ok(Some(v));
            }
        }
        Ok(None)
    }
    /// Account RLP at TxNum at; Some(empty) = deleted there, None = never written.
    pub fn account_at(&self, addr: &[u8], at: u64) -> Result<Option<Vec<u8>>> {
        self.latest(&account_prefix(addr), at)
    }
    pub fn storage_at(&self, addr: &[u8], slot: &[u8], at: u64) -> Result<Option<Vec<u8>>> {
        self.latest(&slot_prefix(addr, slot), at)
    }
    pub fn code_hash_at(&self, addr: &[u8], at: u64) -> Result<Option<Vec<u8>>> {
        self.latest(&coderef_prefix(addr), at)
    }
    pub fn code(&self, hash: &[u8]) -> Result<Option<Vec<u8>>> {
        if let Ok(h) = <[u8; 32]>::try_from(hash) {
            if let Some(b) = self.mem.code.get(&h) {
                return Ok(Some(b.clone()));
            }
        }
        let key = code_key(hash);
        for r in self.runs.iter().rev() {
            if let Some(v) = r.get(Section::State, &key)? {
                return Ok(Some(v));
            }
        }
        Ok(None)
    }

    /// One chain family over [from, to] in one sequential pass: the runs
    /// covering it oldest first, then the window.
    pub fn chain_rows(&self, fam: usize, from: u64, to: u64, mut f: impl FnMut(u64, &[u8]) -> Result<bool>) -> Result<()> {
        if to < from {
            return Ok(());
        }
        let by_height = fam_by_height(fam);
        let prefix = FAM_PREFIX[fam];
        let mut next = from;
        let mut stopped = false;
        for r in &self.runs {
            let ft = &r.footer;
            let (lo, hi) = if by_height { (ft.from_height, ft.to_height) } else { (ft.from_tx, ft.to_tx.wrapping_sub(1)) };
            if (ft.to_tx == ft.from_tx && !by_height) || hi < next || lo > to {
                continue;
            }
            let mut err = None;
            r.scan_range(Section::Chain, &num_key(prefix, next), Some(&num_key(prefix, to + 1)), |k, v| {
                let n = txnum_of(k);
                next = n + 1;
                match f(n, v) {
                    Ok(true) => true,
                    Ok(false) => {
                        stopped = true;
                        false
                    }
                    Err(e) => {
                        err = Some(e);
                        false
                    }
                }
            })?;
            if let Some(e) = err {
                return Err(e);
            }
            if stopped {
                return Ok(());
            }
        }
        for n in next..=to {
            if let Some(v) = self.mem.chain_get(fam, n)? {
                if !f(n, &v)? {
                    return Ok(());
                }
            }
        }
        Ok(())
    }

    /// Posting entries under prefix with TxNum in [lo, hi]: runs oldest
    /// first then the window, each source's entries TxNum-ascending.
    pub fn postings(&self, prefix: &[u8], lo: u64, hi: u64, mut f: impl FnMut(&[u8], u64, u8) -> bool) -> Result<()> {
        for r in &self.runs {
            if r.footer.to_tx <= lo || r.footer.from_tx > hi {
                continue;
            }
            let mut ents = r.scan_chunks(prefix, lo, hi)?;
            ents.sort_by_key(|e| e.1);
            for (g, n, p) in ents {
                if !f(&g, n, p) {
                    return Ok(());
                }
            }
        }
        let mut ents: Vec<_> = self.mem.post.iter().filter(|(g, n, _)| *n >= lo && *n <= hi && g.starts_with(prefix)).collect();
        ents.sort_by_key(|e| e.1);
        for (g, n, p) in ents {
            if !f(g, *n, *p) {
                return Ok(());
            }
        }
        Ok(())
    }
    /// Distinct posting groups under prefix, once per source.
    pub fn groups(&self, prefix: &[u8], mut f: impl FnMut(&[u8]) -> bool) -> Result<()> {
        for r in &self.runs {
            let mut stop = false;
            r.scan_groups(prefix, |g| {
                if !f(g) {
                    stop = true;
                }
                !stop
            })?;
            if stop {
                return Ok(());
            }
        }
        let mut seen = std::collections::HashSet::new();
        for (g, _, _) in &self.mem.post {
            if g.starts_with(prefix) && seen.insert(g.clone()) && !f(g) {
                return Ok(());
            }
        }
        Ok(())
    }
    /// Distinct set/ keys under prefix across every run and the window.
    pub fn set_scan(&self, prefix: &[u8], mut f: impl FnMut(&[u8]) -> bool) -> Result<()> {
        let mut seen = std::collections::HashSet::new();
        for r in &self.runs {
            let mut stop = false;
            r.scan_set(prefix, |k| {
                if seen.insert(k.to_vec()) && !f(k) {
                    stop = true;
                }
                !stop
            })?;
            if stop {
                return Ok(());
            }
        }
        let mut rows: Vec<&Vec<u8>> = self.mem.sets.iter().filter(|k| k.starts_with(prefix)).collect();
        rows.sort();
        for k in rows {
            if seen.insert(k.clone()) && !f(k) {
                return Ok(());
            }
        }
        Ok(())
    }
    /// The block a TxNum belongs to.
    pub fn height_of_tx(&self, txnum: u64) -> Result<Option<u64>> {
        let (mut lo, mut hi) = {
            let (bt, nt, bh, nh, started) = self.mem.window();
            if started && txnum >= bt && txnum < nt {
                (bh, nh - 1)
            } else {
                match self.man.runs.iter().find(|r| txnum >= r.from_tx && txnum < r.to_tx) {
                    Some(r) => (r.from_height, r.to_height),
                    None => return Ok(None),
                }
            }
        };
        while lo < hi {
            let mid = (lo + hi + 1) / 2;
            let Some((first, _)) = self.block_tx_range(mid)? else { return Ok(None) };
            if first <= txnum {
                lo = mid;
            } else {
                hi = mid - 1;
            }
        }
        Ok(Some(lo))
    }

    // -----------------------------------------------------------------------
    // publish and join

    /// Writes the manifest artifact and points `latest-<root>` at it; the
    /// bytes move on the next `sync_artifacts`.
    pub fn publish(&mut self) -> Result<()> {
        let Some(head) = self.man.head().map(|s| s.to_string()) else { return Ok(()) };
        if self.published.as_deref() == Some(&head) {
            return Ok(());
        }
        let raw = serde_json::to_vec(&self.man)?;
        let name = self.cas.put(&raw)?;
        self.cas.set_pointer(&latest_pointer(&self.chain_root), &format!("manifest {name}\n"))?;
        self.published = Some(head);
        Ok(())
    }
    /// Uploads the spool and reopens every run the upload released onto the
    /// chunk cache (an unlinked mapped file would keep its blocks until exit).
    pub fn sync_artifacts(&mut self) -> Result<Vec<String>> {
        let released = self.cas.sync()?;
        for i in 0..self.runs.len() {
            if released.contains(&self.runs[i].name) {
                let name = self.runs[i].name.clone();
                self.runs[i] = Arc::new(Run::open(&self.cas, &name).with_context(|| format!("store: run {name} was uploaded and unlinked but does not reopen"))?);
            }
        }
        Ok(released)
    }
    pub fn runs(&self) -> &[Arc<Run>] {
        &self.runs
    }
}

/// Makes dir able to serve a published chain: pointer, manifest, then the
/// footers walked backward to the chain root. A dir with runs is a no-op.
pub fn join(cas: &Store, dir: &Path, chain_root: [u8; 32]) -> Result<()> {
    std::fs::create_dir_all(dir)?;
    let local = Manifest::load(dir)?;
    if !local.runs.is_empty() {
        return Ok(());
    }
    let Some(ptr) = cas.get_pointer(&latest_pointer(&chain_root))? else {
        if !cas.remote() || std::env::var("EPOCHDB_NEW_CHAIN").as_deref() == Ok("1") {
            return Ok(());
        }
        if cas.prefix_has_objects()? {
            bail!("store: this chain has no published manifest pointer, but the bucket prefix ALREADY HOLDS OBJECTS: refusing (set EPOCHDB_NEW_CHAIN=1 for a new chain in a shared prefix)");
        }
        eprintln!("store: no pointer and an empty bucket prefix: a fresh chain");
        return Ok(());
    };
    let mut mname = None;
    for line in ptr.lines() {
        let f: Vec<&str> = line.split_whitespace().collect();
        match f.as_slice() {
            ["manifest", n] if crate::casfs::valid_hash(n) => mname = Some(n.to_string()),
            [] => {}
            _ => bail!("dist: bad latest pointer line {line:?}"),
        }
    }
    let mname = mname.ok_or_else(|| anyhow!("dist: latest pointer names no manifest"))?;
    let blob = cas.open_blob(&mname).with_context(|| format!("store: the pointer names manifest {mname}, which is not in the bucket"))?;
    let mut raw = vec![0u8; blob.size() as usize];
    blob.read_at(0, &mut raw)?;
    let man: Manifest = serde_json::from_slice(&raw).with_context(|| format!("store: manifest {mname}"))?;
    if man.storage_version != STORAGE_VERSION {
        bail!("store: published manifest is storage version {}, this binary is {STORAGE_VERSION}", man.storage_version);
    }
    if man.chain_root != hex::encode(chain_root) {
        bail!("store: published manifest belongs to chain root {}, this node is {}", man.chain_root, hex::encode(chain_root));
    }
    if man.runs.is_empty() {
        bail!("store: published manifest {mname} lists no runs");
    }
    walk_runs(cas, &man.runs, chain_root)?;
    eprintln!("store: joined a published chain: {} runs, blocks {}..{}, {} txs, head run {}", man.runs.len(), man.runs[0].from_height, man.runs.last().unwrap().to_height, man.runs.last().unwrap().to_tx, man.head().unwrap());
    man.save(dir)
}

/// Verifies a run list backward by prev-name to the chain root: footers only.
pub fn walk_runs(cas: &Store, runs: &[RunRef], chain_root: [u8; 32]) -> Result<()> {
    for i in (0..runs.len()).rev() {
        let r = &runs[i];
        let f: Footer = read_footer(cas.open_blob(&r.name)?.as_ref()).with_context(|| format!("store: run {}", r.name))?;
        if f.from_tx != r.from_tx || f.to_tx != r.to_tx || f.from_height != r.from_height || f.to_height != r.to_height {
            bail!("store: run {} covers tx [{},{}) blocks [{},{}] but the manifest claims tx [{},{}) blocks [{},{}]", r.name, f.from_tx, f.to_tx, f.from_height, f.to_height, r.from_tx, r.to_tx, r.from_height, r.to_height);
        }
        if i == 0 {
            if f.prev != chain_root {
                bail!("store: the oldest run {} links back to {}, but this chain's root is {}", r.name, hex::encode(f.prev), hex::encode(chain_root));
            }
            if f.from_tx != 0 {
                bail!("store: the oldest run {} starts at TxNum {}, so history below it is missing", r.name, f.from_tx);
            }
            continue;
        }
        let p = &runs[i - 1];
        if f.prev != decode_root(&p.name)? {
            bail!("store: run {} links back to {}, but the run before it is {}: the hash chain is broken", r.name, hex::encode(f.prev), p.name);
        }
        if p.to_tx != f.from_tx || p.to_height + 1 != f.from_height {
            bail!("store: run {} starts at tx {} block {} but {} ends at tx {} block {}: there is a hole", r.name, f.from_tx, f.from_height, p.name, p.to_tx, p.to_height);
        }
    }
    Ok(())
}

/// Streams the window into the three sections in key order.
fn write_sections(w: &mut RunWriter, m: &Memtable) -> Result<()> {
    let mut chain: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    for fam in 0..NUM_FAMS {
        m.each_chain(fam, |n, v| {
            chain.push((num_key(FAM_PREFIX[fam], n), v));
            Ok(())
        })?;
    }
    w.section(Section::Chain, chain.into_iter())?;

    let mut state: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    let mut hashes: Vec<&[u8; 32]> = m.code.keys().collect();
    hashes.sort();
    for h in hashes {
        state.push((code_key(h), m.code[h].clone()));
    }
    let mut prefixes: Vec<&Vec<u8>> = m.state.keys().collect();
    prefixes.sort();
    for p in prefixes {
        for (tn, v) in &m.state[p] {
            state.push((suffixed(p, *tn), v.clone()));
        }
    }
    w.section(Section::State, state.into_iter())?;

    let mut rows: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    let mut groups: BTreeMap<&[u8], Vec<(u64, u8)>> = BTreeMap::new();
    for (g, n, p) in &m.post {
        groups.entry(g.as_slice()).or_default().push((*n, *p));
    }
    for (g, ents) in groups.iter_mut() {
        ents.sort_by_key(|e| e.0);
        let mut nums: Vec<u64> = Vec::new();
        let mut pay: Vec<u8> = Vec::new();
        let bits = payload_bits(g);
        let flush = |nums: &mut Vec<u64>, pay: &mut Vec<u8>, rows: &mut Vec<(Vec<u8>, Vec<u8>)>| {
            if !nums.is_empty() {
                rows.push((suffixed(g, nums[0]), crate::ef::encode(nums[0], nums, pay, bits)));
                nums.clear();
                pay.clear();
            }
        };
        for (n, p) in ents.iter() {
            if let Some(last) = nums.last() {
                if last == n {
                    *pay.last_mut().unwrap() |= p;
                    continue;
                }
            }
            if nums.len() == crate::ef::MAX_ENTRIES {
                flush(&mut nums, &mut pay, &mut rows);
            }
            nums.push(*n);
            pay.push(*p);
        }
        flush(&mut nums, &mut pay, &mut rows);
    }
    for (k, n) in &m.nums {
        rows.push((k.clone(), n.to_be_bytes().to_vec()));
    }
    for k in &m.sets {
        rows.push((k.clone(), Vec::new()));
    }
    rows.sort_by(|a, b| a.0.cmp(&b.0));
    w.section(Section::Lookup, rows.into_iter())
}

//! The DB (store/db.go, join.go, merge.go): the window over the unflushed
//! blocks plus the sealed runs, newest to oldest; one descent for everything.
//!
//! Concurrency (the Go model): the run list is an immutable VERSION object
//! (`Arc<Version>`) that is replaced, never edited; a reader clones the Arc
//! once per read and the runs in it stay open (mapped) until the last holder
//! lets go, so a run retired by a merge closes when its last reader leaves.
//! The active window is behind a RwLock the writer holds only while it
//! appends one block; a cut moves it whole into `frozen` (an Arc the readers
//! share) and a thread seals it into an L0 run, so no reader ever waits for
//! a seal or a merge.
//!
//! Deviations from the Go store (user ruling 2026-09-08: Go/Rust artifact
//! compatibility does not matter): none in the write order; the merge does
//! not madvise its inputs out of the page cache.

use crate::casfs::{latest_pointer, Store};
use crate::format::*;
use crate::run::{read_footer, run_file_name, Footer, Run, RunWriter};
use crate::sst::SstIter;
use crate::window::{BlockWrite, Memtable, FROZEN_LOG};
use anyhow::{anyhow, bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

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
/// The terminal boundary in TxNum slots (merge.go TerminalTxs), pinned.
pub const TERMINAL_TXS: u64 = 8_000_000;

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
    /// The terminal prefix: what a publish lists (Go's publishable()).
    pub fn publishable(&self) -> Manifest {
        let n = self.runs.iter().take_while(|r| r.level >= TERMINAL_LEVEL).count();
        Manifest { storage_version: self.storage_version, chain_root: self.chain_root.clone(), runs: self.runs[..n].to_vec() }
    }
}

fn decode_root(s: &str) -> Result<[u8; 32]> {
    let b = hex::decode(s).map_err(|_| anyhow!("store: {s:?} is not a 32-byte hash"))?;
    b.try_into().map_err(|_| anyhow!("store: {s:?} is not a 32-byte hash"))
}

/// The run list and its manifest as one immutable object.
pub struct Version {
    pub man: Manifest,
    pub runs: Vec<Arc<Run>>,
}

/// What a read descends: the frozen window (if a cut is in flight) and the
/// version, captured in that order so a block moving frozen -> run is seen
/// in one of the two.
pub struct View {
    pub frozen: Option<Arc<Memtable>>,
    pub ver: Arc<Version>,
}

impl View {
    fn mems(&self) -> impl Iterator<Item = &Memtable> {
        self.frozen.iter().map(|m| m.as_ref())
    }
}

#[derive(Default)]
struct MergeState {
    running: Option<JoinHandle<Result<()>>>,
    /// Sticky: the next maybe_merge hands it to the writer.
    err: Option<String>,
}

/// Everything a seal or merge thread needs without the DB handle.
struct Inner {
    dir: PathBuf,
    cas: Arc<Store>,
    chain_root: [u8; 32],
    frozen: RwLock<Option<Arc<Memtable>>>,
    ver: RwLock<Arc<Version>>,
    published: Mutex<Option<String>>,
    merge: Mutex<MergeState>,
    terminal_txs: u64,
    /// A TEST HOOK (merge.go mergeCrash): the merge stops dead after the
    /// named stage ("written", "swapped"); None in every real open.
    merge_crash: Mutex<Option<&'static str>>,
}

pub struct DB {
    inner: Arc<Inner>,
    mem: RwLock<Memtable>,
    cut: Mutex<Option<JoinHandle<Result<()>>>>,
    read_only: bool,
    pub flush_txs: u64,
    pub flush_blocks: u64,
    pub flush_bytes: u64,
    /// How long `close` waits for a seal or merge in flight before it
    /// abandons it (`shutdown-grace-secs`); MAX = forever (the tools).
    pub close_grace: Duration,
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
        let terminal_txs = std::env::var("EPOCHDB_TERMINAL_TXS").ok().and_then(|v| v.parse().ok()).unwrap_or(TERMINAL_TXS);
        let inner = Arc::new(Inner {
            dir: dir.to_path_buf(),
            cas: Arc::new(cas),
            chain_root,
            frozen: RwLock::new(None),
            ver: RwLock::new(Arc::new(Version { man, runs })),
            published: Mutex::new(None),
            merge: Mutex::new(MergeState::default()),
            terminal_txs,
            merge_crash: Mutex::new(None),
        });
        let flush_bytes = std::env::var("EPOCHDB_WINDOW_MAX_BYTES").ok().and_then(|v| v.parse().ok()).unwrap_or(FLUSH_BYTES);
        let db = DB { inner, mem: RwLock::new(Memtable::open(&dir.join("window").join("window.log"), read_only)?), cut: Mutex::new(None), read_only, flush_txs: FLUSH_TXS, flush_blocks: FLUSH_BLOCKS, flush_bytes, close_grace: Duration::MAX };
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
                    db.inner.seal(Arc::new(fm), fbt, fnt, fbh, fnh)?;
                } else {
                    *db.inner.frozen.write().unwrap() = Some(Arc::new(fm));
                }
            } else if !read_only {
                fm.remove()?;
            }
        }
        db.mem.write().unwrap().recover(base_tx, base_height)?;
        Ok(db)
    }

    pub fn dir(&self) -> &Path {
        &self.inner.dir
    }
    pub fn cas(&self) -> &Arc<Store> {
        &self.inner.cas
    }
    pub fn chain_root(&self) -> [u8; 32] {
        self.inner.chain_root
    }
    /// The current run list and manifest, one Arc clone: the runs in it stay
    /// open for as long as the holder keeps it.
    pub fn version(&self) -> Arc<Version> {
        self.inner.ver.read().unwrap().clone()
    }
    pub fn manifest(&self) -> Manifest {
        self.version().man.clone()
    }
    pub fn runs(&self) -> Vec<Arc<Run>> {
        self.version().runs.clone()
    }
    fn view(&self) -> View {
        self.inner.view()
    }
    /// The window and the view captured together (the cut takes the window
    /// write lock, so the pair is one consistent moment): what a scan over
    /// the window needs. Point reads probe the window first and take the
    /// view after, which is consistent on its own (rows only ever move
    /// window -> frozen -> run).
    fn window_and_view(&self) -> (std::sync::RwLockReadGuard<'_, Memtable>, View) {
        let mem = self.mem.read().unwrap();
        let v = self.inner.view();
        (mem, v)
    }

    pub fn next_tx(&self) -> u64 {
        self.mem.read().unwrap().window().1
    }
    pub fn next_height(&self) -> u64 {
        self.mem.read().unwrap().window().3
    }
    pub fn head(&self) -> Option<u64> {
        self.next_height().checked_sub(1)
    }
    /// fsyncs the window log. The buffer is flushed under the lock, the
    /// fsync runs outside it through a second handle.
    pub fn sync(&self) -> Result<()> {
        let f = self.mem.write().unwrap().flush_and_dup()?;
        f.sync_data()?;
        Ok(())
    }

    /// Folds one block into the window and cuts a run when a trigger fires.
    /// One writer thread; the window lock is held for the append only.
    pub fn write_block(&self, b: &BlockWrite) -> Result<()> {
        self.mem.write().unwrap().add(b)?;
        self.maybe_flush()
    }

    pub fn maybe_flush(&self) -> Result<()> {
        let (bt, nt, bh, nh, started, bytes) = {
            let m = self.mem.read().unwrap();
            let (bt, nt, bh, nh, started) = m.window();
            (bt, nt, bh, nh, started, m.bytes())
        };
        if !started || (nt - bt < self.flush_txs && nh - bh < self.flush_blocks && bytes < self.flush_bytes) {
            return Ok(());
        }
        self.cut_window()
    }

    /// Cuts the window into a run now (a stop, a test).
    pub fn flush(&self) -> Result<()> {
        self.cut_window()
    }

    /// Waits for the in-flight cut, if any, and reports what it did.
    pub fn wait_cut(&self) -> Result<()> {
        let h = self.cut.lock().unwrap().take();
        match h {
            Some(h) => h.join().map_err(|_| anyhow!("store: the seal thread panicked"))?,
            None => Ok(()),
        }
    }
    /// Waits for the in-flight merge, if any, and reports the last merge's error.
    pub fn wait_merge(&self) -> Result<()> {
        self.inner.wait_merge()
    }
    /// Starts a terminal merge when the L0 tail has reached the boundary
    /// (a thread), and reports the LAST merge's error, which is sticky.
    pub fn maybe_merge(&self) -> Result<()> {
        self.inner.maybe_merge()
    }
    /// Waits for the cut and the merge up to `close_grace`, then abandons
    /// them: the seal's frozen log and the merge's inputs stay on disk and
    /// the next open re-seals / re-merges (the same recovery as a crash).
    /// The window log itself is fsynced by the caller's `sync`.
    pub fn close(&self) -> Result<()> {
        let deadline = Instant::now().checked_add(self.close_grace);
        loop {
            let busy = |h: &Option<JoinHandle<Result<()>>>| h.as_ref().is_some_and(|h| !h.is_finished());
            let (cut, merge) = (busy(&self.cut.lock().unwrap()), busy(&self.inner.merge.lock().unwrap().running));
            if !cut && !merge {
                break;
            }
            if deadline.is_some_and(|d| Instant::now() >= d) {
                eprintln!(
                    "store: close: abandoning the {} still running after {:.0}s (re-done at the next open)",
                    match (cut, merge) {
                        (true, true) => "seal and merge",
                        (true, false) => "seal",
                        _ => "merge",
                    },
                    self.close_grace.as_secs_f64()
                );
                drop(self.cut.lock().unwrap().take());
                drop(self.inner.merge.lock().unwrap().running.take());
                return Ok(());
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        let a = self.wait_cut();
        let b = self.wait_merge();
        a.and(b)
    }

    /// Renames the full log to the frozen name, opens a fresh window and
    /// seals the frozen one on a thread (the previous cut is waited for first).
    fn cut_window(&self) -> Result<()> {
        if self.read_only {
            bail!("store: read-only");
        }
        self.wait_cut()?;
        // The bulk of the log is fsynced outside the window lock; the seal
        // thread fsyncs the frozen file again before it writes the run.
        self.sync()?;
        let mut m = self.mem.write().unwrap();
        let (bt, nt, bh, nh, started) = m.window();
        if !started || nh == bh {
            return Ok(());
        }
        m.flush_and_dup()?;
        let dir = self.inner.dir.join("window");
        let frozen = dir.join(FROZEN_LOG);
        std::fs::rename(&m.path, &frozen)?;
        crate::casfs::sync_dir(&dir)?;
        let mut fresh = Memtable::open(&dir.join("window.log"), false)?;
        fresh.reset(nt, nh)?;
        let mut old = std::mem::replace(&mut *m, fresh);
        old.path = frozen;
        let old = Arc::new(old);
        // Under the window lock: a reader that missed the fresh window finds
        // the block in frozen.
        *self.inner.frozen.write().unwrap() = Some(old.clone());
        drop(m);
        let inner = self.inner.clone();
        *self.cut.lock().unwrap() = Some(std::thread::spawn(move || inner.seal(old, bt, nt, bh, nh)));
        Ok(())
    }

    // -----------------------------------------------------------------------
    // publish and join

    /// Writes the manifest artifact (the terminal prefix) and points
    /// `latest-<root>` at it; the bytes move on the next `sync_artifacts`.
    pub fn publish(&self) -> Result<()> {
        self.inner.publish()
    }
    /// Uploads the spool and reopens every run the upload released onto the
    /// chunk cache (an unlinked mapped file would keep its blocks until exit).
    pub fn sync_artifacts(&self) -> Result<Vec<String>> {
        let released = self.inner.cas.sync()?;
        if released.is_empty() {
            return Ok(released);
        }
        let mut g = self.inner.ver.write().unwrap();
        let cur = g.clone();
        let mut runs = cur.runs.clone();
        for r in runs.iter_mut() {
            if released.contains(&r.name) {
                let name = r.name.clone();
                *r = Arc::new(Run::open(&self.inner.cas, &name).with_context(|| format!("store: run {name} was uploaded and unlinked but does not reopen"))?);
            }
        }
        *g = Arc::new(Version { man: cur.man.clone(), runs });
        Ok(released)
    }
}

impl Drop for DB {
    fn drop(&mut self) {
        if let Err(e) = self.close() {
            eprintln!("store: close: {e:#}");
        }
    }
}

impl Inner {
    fn view(&self) -> View {
        let frozen = self.frozen.read().unwrap().clone();
        let ver = self.ver.read().unwrap().clone();
        View { frozen, ver }
    }

    /// Writes the frozen window into an L0 run, publishes it in the manifest
    /// (durable manifest, then the version swap), only then unlinks the log.
    fn seal(self: &Arc<Self>, m: Arc<Memtable>, base_tx: u64, next_tx: u64, base_height: u64, next_height: u64) -> Result<()> {
        let t0 = Instant::now();
        m.fsync()?;
        let prev = match self.ver.read().unwrap().man.runs.last() {
            Some(r) => decode_root(&r.name)?,
            None => self.chain_root,
        };
        let level = 0;
        let path = run_file_name(&self.cas.local, level, base_tx, next_tx);
        let mut w = RunWriter::new(path, prev, level)?;
        if let Err(e) = write_sections(&mut w, &m) {
            w.abort();
            return Err(e);
        }
        let (name, _) = w.finish(&self.cas, base_tx, next_tx, base_height, next_height - 1)?;
        let run = Arc::new(Run::open(&self.cas, &name)?);
        {
            let mut g = self.ver.write().unwrap();
            let mut man = g.man.clone();
            man.runs.push(RunRef { from_tx: base_tx, to_tx: next_tx, from_height: base_height, to_height: next_height - 1, name: name.clone(), level });
            man.save(&self.dir)?;
            let mut runs = g.runs.clone();
            runs.push(run);
            *g = Arc::new(Version { man, runs });
            *self.frozen.write().unwrap() = None;
        }
        m.remove()?;
        eprintln!("store: sealed run {name} [blocks {base_height}..{}] in {:.1}s", next_height - 1, t0.elapsed().as_secs_f64());
        self.maybe_merge()?;
        self.publish()
    }

    fn publish(&self) -> Result<()> {
        let mut published = self.published.lock().unwrap();
        let man = self.ver.read().unwrap().man.publishable();
        let Some(head) = man.head().map(|s| s.to_string()) else { return Ok(()) };
        if published.as_deref() == Some(&head) {
            return Ok(());
        }
        let raw = serde_json::to_vec(&man)?;
        let name = self.cas.put(&raw)?;
        self.cas.set_pointer(&latest_pointer(&self.chain_root), &format!("manifest {name}\n"))?;
        *published = Some(head);
        Ok(())
    }

    // -----------------------------------------------------------------------
    // the terminal merge (merge.go): the L0 runs since the last terminal up
    // to the one that crosses the boundary, 16-way merged into one terminal
    // run, off the writer thread; the span is a function of chain content.

    /// [start, end) of the next terminal, once the L0 tail holds one.
    fn merge_span(runs: &[RunRef], terminal_txs: u64) -> Option<(usize, usize)> {
        let mut start = runs.len();
        while start > 0 && runs[start - 1].level < TERMINAL_LEVEL {
            start -= 1;
        }
        (start..runs.len()).find(|&end| runs[end].to_tx - runs[start].from_tx >= terminal_txs).map(|end| (start, end + 1))
    }

    fn maybe_merge(self: &Arc<Self>) -> Result<()> {
        let mut st = self.merge.lock().unwrap();
        if let Some(e) = &st.err {
            bail!("store: the terminal merge failed, execution stops: {e}");
        }
        if let Some(h) = &st.running {
            if !h.is_finished() {
                return Ok(());
            }
            let h = st.running.take().unwrap();
            if let Err(e) = h.join().map_err(|_| anyhow!("the merge thread panicked"))? {
                st.err = Some(format!("{e:#}"));
                bail!("store: the terminal merge failed, execution stops: {e:#}");
            }
        }
        let ver = self.ver.read().unwrap().clone();
        let Some((start, end)) = Self::merge_span(&ver.man.runs, self.terminal_txs) else { return Ok(()) };
        let me = self.clone();
        st.running = Some(std::thread::spawn(move || {
            let r = me.merge(start, end).and_then(|_| me.publish());
            if let Err(e) = &r {
                eprintln!("store: the terminal merge failed, execution will stop at the next flush: {e:#}");
            }
            r
        }));
        Ok(())
    }

    fn wait_merge(&self) -> Result<()> {
        let h = self.merge.lock().unwrap().running.take();
        let r = match h {
            Some(h) => h.join().map_err(|_| anyhow!("store: the merge thread panicked"))?,
            None => Ok(()),
        };
        let mut st = self.merge.lock().unwrap();
        if let Err(e) = &r {
            st.err = Some(format!("{e:#}"));
        }
        if let Some(e) = &st.err {
            bail!("store: the terminal merge failed: {e}");
        }
        r
    }

    fn merge(&self, start: usize, end: usize) -> Result<()> {
        let t0 = Instant::now();
        // The merge is a reader too: the version holds its inputs open for
        // the whole pass, whatever the swap below retires.
        let ver = self.ver.read().unwrap().clone();
        if end > ver.runs.len() {
            bail!("store: merge span [{start},{end}) past {} runs", ver.runs.len());
        }
        let refs = ver.man.runs[start..end].to_vec();
        let inputs = ver.runs[start..end].to_vec();
        let prev = if start > 0 { decode_root(&ver.man.runs[start - 1].name)? } else { self.chain_root };
        for w in refs.windows(2) {
            if w[1].from_tx != w[0].to_tx || w[1].from_height != w[0].to_height + 1 {
                bail!("store: merge inputs are not contiguous: {:?} then {:?}", w[0], w[1]);
            }
        }
        let (from, to) = (refs[0].clone(), refs[refs.len() - 1].clone());
        let path = run_file_name(&self.cas.spool, TERMINAL_LEVEL, from.from_tx, to.to_tx);
        let mut w = RunWriter::new(path, prev, TERMINAL_LEVEL)?;
        for s in SECTIONS {
            let mut err = None;
            let it = MergeIter::new(&inputs, s, &mut err)?;
            if let Err(e) = w.section(s, it) {
                w.abort();
                return Err(e);
            }
            if let Some(e) = err {
                w.abort();
                return Err(e);
            }
        }
        let rows = w.rows;
        let (name, _) = w.finish(&self.cas, from.from_tx, to.to_tx, from.from_height, to.to_height)?;
        if *self.merge_crash.lock().unwrap() == Some("written") {
            bail!("store: merge crash hook: written");
        }
        // Verified: the merged run reopens and every row reads back out.
        let merged = Run::open(&self.cas, &name).with_context(|| format!("store: merged run {name} does not reopen"))?;
        let f = &merged.footer;
        if f.from_tx != from.from_tx || f.to_tx != to.to_tx || f.from_height != from.from_height || f.to_height != to.to_height {
            bail!("store: merged run {name} covers tx [{},{}) blocks [{},{}], want tx [{},{}) blocks [{},{}]", f.from_tx, f.to_tx, f.from_height, f.to_height, from.from_tx, to.to_tx, from.from_height, to.to_height);
        }
        for (i, s) in SECTIONS.iter().enumerate() {
            let mut n = 0u64;
            merged.scan_range(*s, &[], None, |_, _| {
                n += 1;
                true
            })?;
            if n != rows[i] {
                bail!("store: merged run {name} section {s:?} reads back {n} rows, {} went in", rows[i]);
            }
        }
        let merged = Arc::new(merged);
        // Publish: the manifest lands durably, then the version swaps. The
        // swap replaces the span and keeps every run appended meanwhile.
        {
            let mut g = self.ver.write().unwrap();
            let old = &g.man.runs;
            if end > old.len() || old[start].name != refs[0].name || old[end - 1].name != to.name {
                bail!("store: the merged span [{start},{end}) moved under the merge: the manifest holds {} runs and no longer starts at {}", old.len(), refs[0].name);
            }
            let mut man = g.man.clone();
            man.runs.splice(start..end, [RunRef { from_tx: from.from_tx, to_tx: to.to_tx, from_height: from.from_height, to_height: to.to_height, name: name.clone(), level: TERMINAL_LEVEL }]);
            man.save(&self.dir)?;
            let mut runs = g.runs.clone();
            runs.splice(start..end, [merged]);
            *g = Arc::new(Version { man, runs });
        }
        if *self.merge_crash.lock().unwrap() == Some("swapped") {
            bail!("store: merge crash hook: swapped");
        }
        // Only now may the inputs go: unlink; the mapping closes with the
        // last version holding it.
        for r in &refs {
            self.cas.drop_local(&r.name).with_context(|| format!("store: retire run {}", r.name))?;
        }
        eprintln!("store: merged {} L0 runs into terminal run {name} [tx {}..{}, blocks {}..{}]: {} chain + {} state + {} lookup rows in {:.1}s", inputs.len(), from.from_tx, to.to_tx, from.from_height, to.to_height, rows[0], rows[1], rows[2], t0.elapsed().as_secs_f64());
        Ok(())
    }
}

/// One input's position in one section.
struct Cursor<'a> {
    idx: usize,
    it: SstIter<'a>,
}

/// The k-way merge of one section: key order, a tie (only ever a code/ row,
/// content addressed) taken from the newest input once.
struct MergeIter<'a> {
    cur: Vec<Cursor<'a>>,
    last: Vec<u8>,
    started: bool,
    err: &'a mut Option<anyhow::Error>,
}

impl<'a> MergeIter<'a> {
    fn new(inputs: &'a [Arc<Run>], s: Section, err: &'a mut Option<anyhow::Error>) -> Result<MergeIter<'a>> {
        let mut cur = Vec::with_capacity(inputs.len());
        for (idx, r) in inputs.iter().enumerate() {
            let mut it = r.sec[s as usize].iter();
            if it.first()? {
                cur.push(Cursor { idx, it });
            }
        }
        Ok(MergeIter { cur, last: Vec::new(), started: false, err })
    }
}

impl Iterator for MergeIter<'_> {
    type Item = (Vec<u8>, Vec<u8>);
    fn next(&mut self) -> Option<(Vec<u8>, Vec<u8>)> {
        loop {
            // ponytail: a linear scan over at most 16 cursors, a heap when the fan-in grows
            let mut best: Option<usize> = None;
            for (i, c) in self.cur.iter().enumerate() {
                best = match best {
                    None => Some(i),
                    Some(b) => {
                        let o = c.it.key().cmp(self.cur[b].it.key());
                        if o.is_lt() || (o.is_eq() && c.idx > self.cur[b].idx) {
                            Some(i)
                        } else {
                            Some(b)
                        }
                    }
                };
            }
            let b = best?;
            let dup = self.started && self.cur[b].it.key() == self.last.as_slice();
            let out = if dup { None } else { Some((self.cur[b].it.key().to_vec(), self.cur[b].it.value().to_vec())) };
            if let Some((k, _)) = &out {
                self.last.clear();
                self.last.extend_from_slice(k);
                self.started = true;
            }
            match self.cur[b].it.next() {
                Ok(true) => {}
                Ok(false) => {
                    self.cur.swap_remove(b);
                }
                Err(e) => {
                    *self.err = Some(e);
                    self.cur.clear();
                    return None;
                }
            }
            if out.is_some() {
                return out;
            }
        }
    }
}

impl DB {
    // -----------------------------------------------------------------------
    // reads

    fn chain_row(&self, fam: usize, n: u64) -> Result<Option<Vec<u8>>> {
        if let Some(v) = self.mem.read().unwrap().chain_get(fam, n)? {
            return Ok(Some(v));
        }
        let v = self.view();
        for m in v.mems() {
            if let Some(v) = m.chain_get(fam, n)? {
                return Ok(Some(v));
            }
        }
        let by_height = fam_by_height(fam);
        for r in v.ver.runs.iter().rev() {
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
        if let Some(n) = self.mem.read().unwrap().nums.get(key) {
            return Ok(Some(*n));
        }
        let v = self.view();
        for m in v.mems() {
            if let Some(n) = m.nums.get(key) {
                return Ok(Some(*n));
            }
        }
        for r in v.ver.runs.iter().rev() {
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
        if let Some((v, _)) = self.mem.read().unwrap().latest_state(prefix, at) {
            return Ok(Some(v));
        }
        let view = self.view();
        for m in view.mems() {
            if let Some((v, _)) = m.latest_state(prefix, at) {
                return Ok(Some(v));
            }
        }
        for r in view.ver.runs.iter().rev() {
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
            if let Some(b) = self.mem.read().unwrap().code.get(&h) {
                return Ok(Some(b.clone()));
            }
        }
        let v = self.view();
        if let Ok(h) = <[u8; 32]>::try_from(hash) {
            for m in v.mems() {
                if let Some(b) = m.code.get(&h) {
                    return Ok(Some(b.clone()));
                }
            }
        }
        let key = code_key(hash);
        for r in v.ver.runs.iter().rev() {
            if let Some(v) = r.get(Section::State, &key)? {
                return Ok(Some(v));
            }
        }
        Ok(None)
    }

    /// Every code blob the store holds (the runs, then the windows): the
    /// engine's code table at open.
    pub fn each_code(&self, mut f: impl FnMut(&[u8; 32], &[u8]) -> Result<()>) -> Result<()> {
        let window: Vec<([u8; 32], Vec<u8>)> = {
            let (mem, v) = self.window_and_view();
            let mut out: Vec<_> = v.mems().chain(std::iter::once(&*mem)).flat_map(|m| m.code.iter().map(|(h, b)| (*h, b.clone()))).collect();
            out.sort();
            out.dedup_by(|a, b| a.0 == b.0);
            out
        };
        let v = self.view();
        let mut seen = std::collections::HashSet::new();
        for r in &v.ver.runs {
            let mut err = None;
            r.scan_range(Section::State, PREFIX_CODE, None, |k, val| {
                if !k.starts_with(PREFIX_CODE) {
                    return false;
                }
                let Ok(h) = <[u8; 32]>::try_from(&k[PREFIX_CODE.len()..]) else { return true };
                if seen.insert(h) {
                    if let Err(e) = f(&h, val) {
                        err = Some(e);
                        return false;
                    }
                }
                true
            })?;
            if let Some(e) = err {
                return Err(e);
            }
        }
        for (h, b) in &window {
            if seen.insert(*h) {
                f(h, b)?;
            }
        }
        Ok(())
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
        let v = self.view();
        for r in &v.ver.runs {
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
        drop(v);
        for n in next..=to {
            if let Some(row) = self.chain_row(fam, n)? {
                if !f(n, &row)? {
                    return Ok(());
                }
            }
        }
        Ok(())
    }

    /// Posting entries under prefix with TxNum in [lo, hi]: runs oldest
    /// first then the window, each source's entries TxNum-ascending.
    pub fn postings(&self, prefix: &[u8], lo: u64, hi: u64, mut f: impl FnMut(&[u8], u64, u8) -> bool) -> Result<()> {
        let (mem, v) = self.window_and_view();
        let window: Vec<(Vec<u8>, u64, u8)> = v.mems().chain(std::iter::once(&*mem)).flat_map(|m| m.post.iter().filter(|(g, n, _)| *n >= lo && *n <= hi && g.starts_with(prefix)).cloned()).collect();
        drop(mem);
        for r in &v.ver.runs {
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
        let mut ents = window;
        ents.sort_by_key(|e| e.1);
        for (g, n, p) in ents {
            if !f(&g, n, p) {
                return Ok(());
            }
        }
        Ok(())
    }
    /// Distinct posting groups under prefix, once per source.
    pub fn groups(&self, prefix: &[u8], mut f: impl FnMut(&[u8]) -> bool) -> Result<()> {
        let (mem, v) = self.window_and_view();
        let mut window: Vec<Vec<u8>> = v.mems().chain(std::iter::once(&*mem)).flat_map(|m| m.post.iter().filter(|(g, _, _)| g.starts_with(prefix)).map(|(g, _, _)| g.clone())).collect();
        drop(mem);
        window.sort();
        window.dedup();
        for r in &v.ver.runs {
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
        for g in &window {
            if !f(g) {
                return Ok(());
            }
        }
        Ok(())
    }
    /// Distinct set/ keys under prefix across every run and the window.
    pub fn set_scan(&self, prefix: &[u8], mut f: impl FnMut(&[u8]) -> bool) -> Result<()> {
        let mut seen = std::collections::HashSet::new();
        let (mem, v) = self.window_and_view();
        let mut window: Vec<Vec<u8>> = v.mems().chain(std::iter::once(&*mem)).flat_map(|m| m.sets.iter().filter(|k| k.starts_with(prefix)).cloned()).collect();
        drop(mem);
        window.sort();
        for r in &v.ver.runs {
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
        for k in window {
            if seen.insert(k.clone()) && !f(&k) {
                return Ok(());
            }
        }
        Ok(())
    }
    /// The block a TxNum belongs to.
    pub fn height_of_tx(&self, txnum: u64) -> Result<Option<u64>> {
        let (mut lo, mut hi) = {
            let win = self.mem.read().unwrap().window();
            let v = self.view();
            let wins: Vec<_> = v.mems().map(|m| m.window()).chain(std::iter::once(win)).collect();
            match wins.iter().find(|w| w.4 && txnum >= w.0 && txnum < w.1).map(|w| (w.2, w.3 - 1)) {
                Some(r) => r,
                None => match v.ver.man.runs.iter().find(|r| txnum >= r.from_tx && txnum < r.to_tx) {
                    Some(r) => (r.from_height, r.to_height),
                    None => return Ok(None),
                },
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
    // Chain rows stream out of the log one at a time (family order then
    // number order is key order): the window's raw chain bytes can be many
    // GB and used to sit in one Vec for the whole seal, which is what the
    // plugin's anon heap stepped up by at every seal.
    w.section_with(Section::Chain, |set| {
        for fam in 0..NUM_FAMS {
            m.each_chain(fam, |n, v| set(&num_key(FAM_PREFIX[fam], n), &v))?;
        }
        Ok(())
    })?;

    w.section_with(Section::State, |set| {
        let mut hashes: Vec<&[u8; 32]> = m.code.keys().collect();
        hashes.sort();
        for h in hashes {
            set(&code_key(h), &m.code[h])?;
        }
        m.each_state_sorted(|k, tn, v| set(&suffixed(k, tn), v))?;
        Ok(())
    })?;

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::window::{StateRow, TxWrite};
    use std::sync::atomic::{AtomicU64, Ordering};

    fn addr(h: u64, i: u64) -> [u8; 20] {
        let mut a = [0u8; 20];
        a[..8].copy_from_slice(&(h * 10 + i).to_be_bytes());
        a
    }
    /// Block h: 3 txs, each writing one account row and one slot, plus a
    /// tail row; the header bytes name the height.
    fn block(h: u64) -> BlockWrite {
        let mut b = BlockWrite { height: h, header_rlp: format!("hdr-{h:08}").into_bytes(), ..Default::default() };
        b.container_id = state::keccak::keccak256(&b.header_rlp);
        for i in 0..3u64 {
            let hash = state::keccak::keccak256(&(h * 3 + i).to_be_bytes());
            b.txs.push(TxWrite {
                hash,
                rlp: format!("tx-{h}-{i}").into_bytes(),
                receipt: crate::receipts::encode(1, 21000, 21000 * (i + 1), &[]),
                frames: b"{}".to_vec(),
                frame_addrs: vec![],
                state: vec![StateRow { kind: b'a', addr: addr(h, i), slot: [0; 32], val: vec![h as u8, i as u8] }, StateRow { kind: b's', addr: addr(h, i), slot: [1; 32], val: vec![9, h as u8] }],
                sender: Some(addr(h, i)),
                to: None,
                created: None,
                logs: vec![],
            });
        }
        b.tail.push(StateRow { kind: b'a', addr: addr(0, 0), slot: [0; 32], val: vec![h as u8] });
        b
    }
    /// Every row of block h at its own TxNum, through the public read API.
    fn check(db: &DB, h: u64) -> Result<()> {
        let want = block(h);
        let hdr = db.header_rlp(h)?.ok_or_else(|| anyhow!("hdr/{h} missing"))?;
        anyhow::ensure!(hdr == want.header_rlp, "hdr/{h}");
        anyhow::ensure!(db.height_by_hash(&state::keccak::keccak256(&hdr))? == Some(h), "blkh {h}");
        let (first, count) = db.block_tx_range(h)?.ok_or_else(|| anyhow!("blk/{h} missing"))?;
        anyhow::ensure!(count == 3 && first == (h - 1) * 4, "blk/{h}: {first} {count}");
        anyhow::ensure!(db.container_at(h)?.is_some(), "container {h}");
        for (i, t) in want.txs.iter().enumerate() {
            let n = first + i as u64;
            anyhow::ensure!(db.tx_rlp(n)? == Some(t.rlp.clone()), "tx/{n}");
            anyhow::ensure!(db.receipt(n)? == Some(t.receipt.clone()), "rcpt/{n}");
            anyhow::ensure!(db.txnum_by_hash(&t.hash)? == Some(n), "txh {n}");
            anyhow::ensure!(db.height_of_tx(n)? == Some(h), "height_of_tx {n}");
            anyhow::ensure!(db.account_at(&addr(h, i as u64), n)? == Some(t.state[0].val.clone()), "account {h}/{i}");
            anyhow::ensure!(db.storage_at(&addr(h, i as u64), &[1; 32], n)? == Some(t.state[1].val.clone()), "slot {h}/{i}");
            let mut hit = false;
            db.postings(&addr_prefix(&addr(h, i as u64)), n, n, |_, nn, p| {
                hit = nn == n && p & ROLE_SENDER != 0;
                true
            })?;
            anyhow::ensure!(hit, "addr posting {h}/{i}");
        }
        anyhow::ensure!(db.account_at(&addr(0, 0), first + 3)? == Some(vec![h as u8]), "tail {h}");
        Ok(())
    }

    /// The merge's write order survives a crash at each stage: after the
    /// terminal is written but before the manifest swap, the inputs are
    /// still the manifest and the retry produces the same terminal name;
    /// after the swap but before the inputs are unlinked, the manifest
    /// already names the terminal and the leftover inputs are harmless.
    #[test]
    fn merge_crash_points_leave_the_inputs() {
        let dir = std::env::temp_dir().join(format!("epochdb-mergecrash-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::env::set_var("EPOCHDB_TERMINAL_TXS", "800");
        let open = || {
            let mut db = DB::open(&dir, Store::local(&dir).unwrap(), [1u8; 32]).unwrap();
            db.flush_blocks = 40;
            db
        };
        let names = |sub: &str| -> Vec<String> {
            let mut v: Vec<String> = std::fs::read_dir(dir.join(sub)).unwrap().flatten().map(|e| e.file_name().to_string_lossy().into_owned()).filter(|n| !n.starts_with('.')).collect();
            v.sort();
            v
        };
        // 200 blocks = 5 L0 runs of 160 slots each; the boundary (800) is
        // reached on the 5th seal, so the merge would start there: hold it.
        let db = open();
        *db.inner.merge_crash.lock().unwrap() = Some("written");
        for h in 1..=200 {
            db.write_block(&block(h)).unwrap();
        }
        db.wait_cut().unwrap(); // the 5th seal is what starts the merge
        assert!(db.wait_merge().unwrap_err().to_string().contains("written"));
        let man = db.manifest();
        assert_eq!(man.runs.len(), 5, "the manifest still lists the inputs");
        assert!(man.runs.iter().all(|r| r.level == 0));
        assert_eq!(names("runs").len(), 5);
        assert_eq!(names("cas").len(), 1, "the terminal is in the spool, unreferenced");
        let stray = names("cas")[0].clone();
        drop(db);
        // Reopen: same span, same name, the merge completes; inputs gone.
        let db = open();
        for h in 1..=200 {
            check(&db, h).unwrap();
        }
        db.maybe_merge().unwrap();
        db.wait_merge().unwrap();
        let man = db.manifest();
        assert_eq!(man.runs.len(), 1);
        assert_eq!(man.runs[0].name, stray, "the retry recomputed the same terminal");
        assert_eq!((man.runs[0].from_height, man.runs[0].to_height, man.runs[0].level), (1, 200, 1));
        assert!(names("runs").is_empty());
        for h in 1..=200 {
            check(&db, h).unwrap();
        }
        // Crash after the swap: the manifest names the terminal, the inputs are leftovers.
        *db.inner.merge_crash.lock().unwrap() = Some("swapped");
        for h in 201..=400 {
            db.write_block(&block(h)).unwrap();
        }
        db.wait_cut().unwrap();
        assert!(db.wait_merge().unwrap_err().to_string().contains("swapped"));
        let man = db.manifest();
        assert_eq!(man.runs.len(), 2);
        assert_eq!(man.runs[1].level, 1);
        assert_eq!(names("runs").len(), 5, "inputs left behind by the crash");
        drop(db);
        let db = open();
        assert_eq!(db.manifest().runs.len(), 2);
        for h in 1..=400 {
            check(&db, h).unwrap();
        }
        assert_eq!(db.manifest().publishable().runs.len(), 2);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// 8 readers hammer every read path against a writer that appends,
    /// seals a run every 40 blocks and merges every 5 runs, while every
    /// answer for a block at or below the published head must be right and
    /// no read may wait on a seal or a merge.
    #[test]
    fn readers_never_block_on_the_writer() {
        let dir = std::env::temp_dir().join(format!("epochdb-hammer-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::env::set_var("EPOCHDB_TERMINAL_TXS", "800");
        let cas = Store::local(&dir).unwrap();
        let mut db = DB::open(&dir, cas, [1u8; 32]).unwrap();
        db.flush_blocks = 40;
        let db = Arc::new(db);
        let head = Arc::new(AtomicU64::new(0));
        let stop = Arc::new(AtomicU64::new(0));
        const N: u64 = 1200;
        let mut readers = Vec::new();
        for t in 0..8u64 {
            let (db, head, stop) = (db.clone(), head.clone(), stop.clone());
            readers.push(std::thread::spawn(move || -> Result<(u64, f64)> {
                let mut reads = 0u64;
                let mut worst = 0f64;
                let mut x = t + 1;
                while stop.load(Ordering::Relaxed) == 0 {
                    let h = head.load(Ordering::Relaxed);
                    if h == 0 {
                        continue;
                    }
                    x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                    let pick = 1 + (x >> 33) % h;
                    let t0 = Instant::now();
                    check(&db, pick)?;
                    worst = worst.max(t0.elapsed().as_secs_f64());
                    reads += 1;
                }
                Ok((reads, worst))
            }));
        }
        let mut seals = 0;
        for h in 1..=N {
            let before = db.manifest().runs.len();
            db.write_block(&block(h)).unwrap();
            if db.manifest().runs.len() != before {
                seals += 1;
            }
            head.store(h, Ordering::Relaxed);
            if h % 256 == 0 {
                db.sync().unwrap();
            }
        }
        db.flush().unwrap();
        db.close().unwrap();
        stop.store(1, Ordering::Relaxed);
        let mut total = 0;
        let mut worst = 0f64;
        for r in readers {
            let (n, w) = r.join().unwrap().unwrap();
            total += n;
            worst = worst.max(w);
        }
        let man = db.manifest();
        let terminals = man.publishable().runs.len();
        eprintln!("hammer: {total} reads on 8 threads, worst {:.1} ms, {} runs ({terminals} terminal), {} cuts seen by the writer", worst * 1e3, man.runs.len(), seals);
        assert!(total > 1000, "the readers barely ran: {total}");
        assert!(terminals >= 3, "expected terminals from the 800-slot boundary, got {terminals}");
        assert!(man.runs.len() < N as usize / 40, "no merge happened: {} runs", man.runs.len());
        for h in 1..=N {
            check(&db, h).unwrap();
        }
        // The merged corpus reads back exactly, its runs tile, and the
        // retired L0 files are gone.
        let names: Vec<_> = std::fs::read_dir(dir.join("runs")).unwrap().flatten().map(|e| e.file_name().to_string_lossy().into_owned()).collect();
        let l0 = man.runs.iter().filter(|r| r.level < TERMINAL_LEVEL).count();
        assert_eq!(names.len(), l0, "local dir holds {names:?}, manifest lists {l0} L0 runs");
        assert!(worst < 2.0, "a read waited {worst:.2}s");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The byte trigger: with the block and slot triggers out of reach, a
    /// window is cut as soon as its log passes `flush_bytes`, so after every
    /// block the live window holds fewer bytes than the cap. (No env here:
    /// the other tests set EPOCHDB_TERMINAL_TXS concurrently, so merges may
    /// or may not happen and the cuts are counted at the writer.)
    #[test]
    fn window_is_cut_by_bytes() {
        let dir = std::env::temp_dir().join(format!("epochdb-bytes-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let mut db = DB::open(&dir, Store::local(&dir).unwrap(), [1u8; 32]).unwrap();
        db.flush_bytes = 8192;
        const N: u64 = 300;
        let (mut cuts, mut largest) = (0, 0);
        for h in 1..=N {
            db.write_block(&block(h)).unwrap();
            cuts += db.cut.lock().unwrap().is_some() as usize;
            db.wait_cut().unwrap();
            let bytes = db.mem.read().unwrap().bytes();
            assert!(bytes < db.flush_bytes, "block {h}: window holds {bytes} bytes, cap {}", db.flush_bytes);
            largest = largest.max(bytes);
        }
        db.close().unwrap();
        assert!(cuts >= 20, "{cuts} cuts for {N} blocks");
        for h in 1..=N {
            check(&db, h).unwrap();
        }
        eprintln!("bytes: {cuts} cuts for {N} blocks, largest live window {largest} bytes under an {} cap", db.flush_bytes);
        let _ = std::fs::remove_dir_all(&dir);
    }
}

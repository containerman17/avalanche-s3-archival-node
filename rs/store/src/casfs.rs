//! casfs artifacts (github.com/containerman17/casfs chunked.go, casfs.go,
//! s3.go) and epochdb's dist layer over them (dist/dist.go): a stored object is
//! [content L][sha256 per 4MB chunk][u64 LE L]["CASFSv1\n"], named by
//! sha256(hash list). Spool `<data>/cas/<hash>` uploads; `<data>/runs/
//! <label>-<hash>` never leaves the machine; pointers live at `<data>/<name>`
//! and `<data>/cas/.pointers/<name>` until Sync uploads them.

use crate::sst::ReadAt;
use anyhow::{anyhow, bail, Context, Result};
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

pub const CHUNK_SIZE: u64 = 4 << 20;
const TRAILER_SIZE: u64 = 16;
const MAGIC: &[u8; 8] = b"CASFSv1\n";
const MAX_CHUNKS: u64 = 128 << 10;

pub struct Hasher {
    chunk: Sha256,
    in_chunk: u64,
    list: Vec<u8>,
    n: u64,
}

impl Default for Hasher {
    fn default() -> Self {
        Self::new()
    }
}

impl Hasher {
    pub fn new() -> Hasher {
        Hasher { chunk: Sha256::new(), in_chunk: 0, list: Vec::new(), n: 0 }
    }
    pub fn write(&mut self, mut p: &[u8]) {
        while !p.is_empty() {
            let take = (p.len() as u64).min(CHUNK_SIZE - self.in_chunk) as usize;
            self.chunk.update(&p[..take]);
            self.in_chunk += take as u64;
            self.n += take as u64;
            p = &p[take..];
            if self.in_chunk == CHUNK_SIZE {
                self.close_chunk();
            }
        }
    }
    fn close_chunk(&mut self) {
        let h = std::mem::replace(&mut self.chunk, Sha256::new()).finalize();
        self.list.extend_from_slice(&h);
        self.in_chunk = 0;
    }
    /// (name, tail)
    pub fn finish(mut self) -> Result<(String, Vec<u8>)> {
        if self.in_chunk > 0 {
            self.close_chunk();
        }
        if self.list.len() as u64 / 32 > MAX_CHUNKS {
            bail!("casfs: artifact over the {MAX_CHUNKS} chunk ceiling");
        }
        let mut tail = self.list.clone();
        tail.extend_from_slice(&self.n.to_le_bytes());
        tail.extend_from_slice(MAGIC);
        Ok((name_of(&self.list), tail))
    }
}

pub fn name_of(list: &[u8]) -> String {
    hex::encode(Sha256::digest(list))
}

fn nchunks(size: u64) -> u64 {
    (size + CHUNK_SIZE - 1) / CHUNK_SIZE
}

fn tail_len(stored: u64) -> Result<u64> {
    if stored < TRAILER_SIZE {
        bail!("casfs: a {stored} byte object is smaller than the trailer");
    }
    let meta = stored - TRAILER_SIZE;
    let n = (meta + CHUNK_SIZE + 32 - 1) / (CHUNK_SIZE + 32);
    if n > MAX_CHUNKS {
        bail!("casfs: a {stored} byte object is over the chunk ceiling");
    }
    Ok(n * 32 + TRAILER_SIZE)
}

/// (content length, hash list)
fn parse_tail(stored: u64, tail: &[u8]) -> Result<(u64, Vec<u8>)> {
    let want = tail_len(stored)?;
    if tail.len() as u64 != want {
        bail!("casfs: tail is {} bytes, want {want}", tail.len());
    }
    let (list, tr) = tail.split_at((want - TRAILER_SIZE) as usize);
    if &tr[8..] != MAGIC {
        bail!("casfs: not a casfs object (bad trailer magic)");
    }
    let size = u64::from_le_bytes(tr[..8].try_into().unwrap());
    if size != stored - want {
        bail!("casfs: trailer claims {size} bytes of content in a {stored} byte object");
    }
    if nchunks(size) * 32 != list.len() as u64 {
        bail!("casfs: chunk list length does not match the content length");
    }
    Ok((size, list.to_vec()))
}

/// Reads a spool/local file's tail: (content length, list).
pub fn read_tail(path: &Path) -> Result<(u64, Vec<u8>)> {
    let f = fs::File::open(path)?;
    let stored = f.metadata()?.len();
    let want = tail_len(stored)?;
    let mut tail = vec![0u8; want as usize];
    read_exact_at(&f, stored - want, &mut tail)?;
    parse_tail(stored, &tail)
}

fn read_exact_at(f: &fs::File, off: u64, buf: &mut [u8]) -> Result<()> {
    use std::os::unix::fs::FileExt;
    f.read_exact_at(buf, off)?;
    Ok(())
}

pub fn valid_hash(s: &str) -> bool {
    s.len() == 64 && s.bytes().all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
}

/// tmp + fsync + rename.
pub fn write_durable(path: &Path, parts: &[&[u8]]) -> Result<()> {
    let tmp = path.with_extension(format!("{}.tmp", path.extension().and_then(|e| e.to_str()).unwrap_or("")));
    let tmp = if path.extension().is_none() { PathBuf::from(format!("{}.tmp", path.display())) } else { tmp };
    {
        let mut f = fs::File::create(&tmp)?;
        for p in parts {
            f.write_all(p)?;
        }
        f.sync_all()?;
    }
    fs::rename(&tmp, path)?;
    Ok(())
}

pub fn sync_dir(dir: &Path) -> Result<()> {
    fs::File::open(dir)?.sync_all()?;
    Ok(())
}

// ---------------------------------------------------------------------------
// blobs

/// A whole-file mapping of a local artifact's CONTENT (L bytes, no tail).
pub struct MmapBlob {
    mm: memmap2::Mmap,
    size: u64,
}

impl ReadAt for MmapBlob {
    fn size(&self) -> u64 {
        self.size
    }
    fn read_at(&self, off: u64, buf: &mut [u8]) -> Result<()> {
        let end = off + buf.len() as u64;
        if end > self.size {
            bail!("casfs: range [{off},{end}) outside a {} byte artifact", self.size);
        }
        buf.copy_from_slice(&self.mm[off as usize..end as usize]);
        Ok(())
    }
}

pub fn mmap_artifact(path: &Path) -> Result<MmapBlob> {
    let (size, _) = read_tail(path)?;
    let f = fs::File::open(path)?;
    let mm = unsafe { memmap2::MmapOptions::new().len(size as usize).map(&f)? };
    Ok(MmapBlob { mm, size })
}

/// A remote artifact read through the chunk cache, verified chunk by chunk.
pub struct RemoteBlob {
    s3: Arc<S3>,
    cache: Arc<ChunkCache>,
    hash: String,
    size: u64,
    list: Vec<u8>,
}

impl ReadAt for RemoteBlob {
    fn size(&self) -> u64 {
        self.size
    }
    fn read_at(&self, off: u64, buf: &mut [u8]) -> Result<()> {
        if off + buf.len() as u64 > self.size {
            bail!("casfs: range outside artifact {}", self.hash);
        }
        let mut n = 0usize;
        while n < buf.len() {
            let cur = off + n as u64;
            let idx = cur / CHUNK_SIZE;
            let in_chunk = (cur % CHUNK_SIZE) as usize;
            let chunk = self.cache.chunk(&self.s3, &self.hash, self.size, &self.list, idx)?;
            let take = (buf.len() - n).min(chunk.len() - in_chunk);
            buf[n..n + take].copy_from_slice(&chunk[in_chunk..in_chunk + take]);
            n += take;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// the chunk cache: <cache>/<window>/<ns>/<hash>.<idx>, 20-minute UTC windows
// (casfs cache.go). Two watermarks in absolute free bytes on the cache
// filesystem: below `min_free` a fill is served from memory and not written
// (admission never evicts inline); below `evict_target` the worker drops the
// oldest whole windows until free + freed >= target, in one pass counting
// the bytes it freed itself. A chunk read from an old window is promoted
// into the current one, so old windows drain into husks; `sweep` expires
// windows past `max_age` and removes husks. The current window is never
// evicted.

const WINDOW_SECS: u64 = 1200;
const SETTLE_SECS: u64 = 60;
const POLL_SECS: u64 = 5;
const SWEEP_SECS: u64 = 300;
const TMP_MAX_AGE_SECS: u64 = 3600;
const DEFAULT_MAX_AGE_SECS: u64 = 30 * 24 * 3600;
const DEFAULT_FULL_PCT: u64 = 95;
const DEFAULT_EVICT_PCT: u64 = 90;

#[derive(Clone, Debug)]
pub struct CacheConfig {
    pub min_free: u64,
    pub evict_target: u64,
    pub max_age_secs: u64,
}

#[derive(Default, Debug)]
pub struct CacheStats {
    pub refusals: AtomicU64,
    pub evictions: AtomicU64,
    pub freed: AtomicU64,
    pub fills: AtomicU64,
}

type FreeFn = Box<dyn Fn(&Path) -> Result<u64> + Send + Sync>;

pub struct ChunkCache {
    root: PathBuf,
    ns: String,
    hot: Mutex<Vec<(String, Arc<Vec<u8>>)>>,
    cfg: CacheConfig,
    free: FreeFn,
    pub stats: CacheStats,
}

fn now_unix() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0)
}

fn window_name(unix: u64) -> String {
    let t = unix - unix % WINDOW_SECS;
    let days = t / 86400;
    let secs = t % 86400;
    // civil from days (Howard Hinnant)
    let z = days as i64 + 719468;
    let era = z.div_euclid(146097);
    let doe = z.rem_euclid(146097);
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!("{y:04}-{m:02}-{d:02}T{:02}-{:02}", secs / 3600, (secs % 3600) / 60)
}

/// A window name is exactly what window_name produces: fixed width, so a
/// string compare orders windows in time; anything else is not ours.
fn valid_window(name: &str) -> bool {
    let b = name.as_bytes();
    b.len() == 16 && b[4] == b'-' && b[7] == b'-' && b[10] == b'T' && b[13] == b'-' && name.chars().enumerate().all(|(i, c)| matches!(i, 4 | 7 | 10 | 13) || c.is_ascii_digit()) && matches!(&name[14..], "00" | "20" | "40")
}

/// Free bytes for an unprivileged writer on the filesystem holding path.
pub fn statfs_free(path: &Path) -> Result<u64> {
    statvfs(path).map(|(_, free)| free)
}

fn statvfs(path: &Path) -> Result<(u64, u64)> {
    use std::os::unix::ffi::OsStrExt;
    let c = std::ffi::CString::new(path.as_os_str().as_bytes())?;
    let mut st: libc::statvfs = unsafe { std::mem::zeroed() };
    if unsafe { libc::statvfs(c.as_ptr(), &mut st) } != 0 {
        return Err(std::io::Error::last_os_error()).with_context(|| format!("statvfs {}", path.display()));
    }
    Ok((st.f_blocks as u64 * st.f_frsize as u64, st.f_bavail as u64 * st.f_frsize as u64))
}

fn parse_secs(s: &str) -> Option<u64> {
    let s = s.trim();
    let (n, mul) = match s.chars().last()? {
        'h' => (&s[..s.len() - 1], 3600),
        'm' => (&s[..s.len() - 1], 60),
        's' => (&s[..s.len() - 1], 1),
        _ => (s, 1),
    };
    n.parse::<u64>().ok().map(|n| n * mul)
}

impl CacheConfig {
    /// The Go defaults: min_free 5% of the filesystem, evict_target 10%;
    /// EPOCHDB_CACHE_MIN_FREE (bytes) sets the floor with the target at
    /// twice it; EPOCHDB_CACHE_MAX_AGE (seconds, or Nh / Nm / Ns).
    pub fn from_env(root: &Path) -> Result<CacheConfig> {
        let min_free: u64 = std::env::var("EPOCHDB_CACHE_MIN_FREE").ok().and_then(|v| v.parse().ok()).unwrap_or(0);
        let max_age_secs = std::env::var("EPOCHDB_CACHE_MAX_AGE").ok().and_then(|v| parse_secs(&v)).unwrap_or(DEFAULT_MAX_AGE_SECS);
        let (min_free, evict_target) = if min_free > 0 {
            (min_free, 2 * min_free)
        } else {
            let (total, _) = statvfs(root)?;
            (total * (100 - DEFAULT_FULL_PCT) / 100, total * (100 - DEFAULT_EVICT_PCT) / 100)
        };
        Ok(CacheConfig { min_free, evict_target, max_age_secs })
    }
}

impl ChunkCache {
    pub fn new(root: PathBuf, ns: String) -> Result<ChunkCache> {
        fs::create_dir_all(&root)?;
        let cfg = CacheConfig::from_env(&root)?;
        Ok(Self::with_free(root, ns, cfg, Box::new(statfs_free)))
    }
    /// A cache over an injected free-space reading (tests: a small fake cap).
    pub fn with_free(root: PathBuf, ns: String, cfg: CacheConfig, free: FreeFn) -> ChunkCache {
        ChunkCache { root, ns, hot: Mutex::new(Vec::new()), cfg, free, stats: CacheStats::default() }
    }
    pub fn config(&self) -> &CacheConfig {
        &self.cfg
    }
    fn windows(&self) -> Vec<String> {
        let mut wins: Vec<String> = fs::read_dir(&self.root).ok().into_iter().flatten().flatten().map(|e| e.file_name().to_string_lossy().into_owned()).filter(|n| valid_window(n)).collect();
        wins.sort();
        wins
    }
    /// The chunk's path, promoted into the current window on the way past.
    fn find(&self, name: &str) -> Option<PathBuf> {
        let cur = window_name(now_unix());
        for w in self.windows().iter().rev() {
            let p = self.root.join(w).join(&self.ns).join(name);
            if p.is_file() {
                if *w != cur {
                    let dir = self.root.join(&cur).join(&self.ns);
                    let to = dir.join(name);
                    // A promotion that loses a race costs nothing: the old
                    // path stays readable until its window is dropped.
                    if fs::create_dir_all(&dir).is_ok() && fs::rename(&p, &to).is_ok() {
                        return Some(to);
                    }
                }
                return Some(p);
            }
        }
        None
    }
    /// Writes a fetched chunk into the current window, unless the disk is
    /// under the admission floor (a statfs error reads as full).
    fn admit(&self, name: &str, b: &[u8]) -> Result<()> {
        let free = match (self.free)(&self.root) {
            Ok(f) => f,
            Err(_) => {
                self.stats.refusals.fetch_add(1, Ordering::Relaxed);
                return Ok(());
            }
        };
        if free < self.cfg.min_free {
            self.stats.refusals.fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }
        self.admit_in(&window_name(now_unix()), name, b)
    }
    fn admit_in(&self, window: &str, name: &str, b: &[u8]) -> Result<()> {
        let dir = self.root.join(window).join(&self.ns);
        fs::create_dir_all(&dir)?;
        write_durable(&dir.join(name), &[b])?;
        self.stats.fills.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
    /// One cleaning pass: below the eviction target, the oldest window goes,
    /// again and again, until the target is reached or only the current
    /// window is left. Counts its own bytes, never re-reads statfs. Reports
    /// whether it freed anything (what the settle delay is for).
    pub fn step(&self) -> bool {
        let Ok(free) = (self.free)(&self.root) else { return false };
        let mut freed = 0u64;
        while free + freed < self.cfg.evict_target {
            let Some(n) = self.evict_oldest() else { break };
            freed += n;
            self.stats.freed.fetch_add(n, Ordering::Relaxed);
        }
        freed > 0
    }
    /// Removes the oldest non-current window whole; the bytes it freed.
    fn evict_oldest(&self) -> Option<u64> {
        let cur = window_name(now_unix());
        let w = self.windows().into_iter().next().filter(|w| *w < cur)?;
        let dir = self.root.join(&w);
        let n = dir_bytes(&dir);
        fs::remove_dir_all(&dir).ok()?;
        self.stats.evictions.fetch_add(1, Ordering::Relaxed);
        Some(n)
    }
    /// The time-based half: windows past max_age go by name, husks (older,
    /// empty) go, and tmp files a killed fill left behind are collected.
    pub fn sweep(&self) {
        let now = now_unix();
        let cur = window_name(now);
        let cutoff = window_name(now.saturating_sub(self.cfg.max_age_secs));
        for w in self.windows() {
            let dir = self.root.join(&w);
            if w < cutoff {
                let _ = fs::remove_dir_all(&dir);
                self.stats.evictions.fetch_add(1, Ordering::Relaxed);
                continue;
            }
            if w < cur && dir_bytes(&dir) == 0 {
                let _ = fs::remove_dir_all(&dir);
                continue;
            }
            for e in fs::read_dir(dir.join(&self.ns)).into_iter().flatten().flatten() {
                let p = e.path();
                let stale = p.extension().is_some_and(|x| x == "tmp") && e.metadata().and_then(|m| m.modified()).map(|t| t.elapsed().map(|d| d.as_secs() > TMP_MAX_AGE_SECS).unwrap_or(false)).unwrap_or(false);
                if stale {
                    let _ = fs::remove_file(p);
                }
            }
        }
    }
    /// The worker: sweeps every 5 minutes, cleans every 5 s, waits a minute
    /// after a pass that freed bytes (statfs lags a cohort of unlinks). Runs
    /// while the store holds the cache.
    fn spawn_worker(cache: &Arc<ChunkCache>) {
        let weak = Arc::downgrade(cache);
        std::thread::spawn(move || {
            let mut last_sweep = 0u64;
            loop {
                let Some(c) = weak.upgrade() else { return };
                if now_unix().saturating_sub(last_sweep) >= SWEEP_SECS {
                    c.sweep();
                    last_sweep = now_unix();
                }
                let d = if c.step() { SETTLE_SECS } else { POLL_SECS };
                drop(c);
                std::thread::sleep(std::time::Duration::from_secs(d));
            }
        });
    }
    /// One verified chunk, from RAM, the cache directory, or a ranged GET.
    fn chunk(&self, s3: &S3, hash: &str, size: u64, list: &[u8], idx: u64) -> Result<Arc<Vec<u8>>> {
        let name = format!("{hash}.{idx}");
        {
            let hot = self.hot.lock().unwrap();
            if let Some((_, b)) = hot.iter().find(|(n, _)| *n == name) {
                return Ok(b.clone());
            }
        }
        let clen = (size - idx * CHUNK_SIZE).min(CHUNK_SIZE);
        let b = match self.find(&name) {
            Some(p) => {
                let b = fs::read(p)?;
                if b.len() as u64 != clen || verify_chunk(hash, list, idx, &b).is_err() {
                    let b = s3.get_range(hash, idx * CHUNK_SIZE, clen)?;
                    verify_chunk(hash, list, idx, &b)?;
                    self.admit(&name, &b)?;
                    b
                } else {
                    b
                }
            }
            None => {
                let b = s3.get_range(hash, idx * CHUNK_SIZE, clen)?;
                verify_chunk(hash, list, idx, &b)?;
                self.admit(&name, &b)?;
                b
            }
        };
        let b = Arc::new(b);
        let mut hot = self.hot.lock().unwrap();
        // ponytail: 16-chunk RAM ring, the disk window tree is the real cache
        if hot.len() >= 16 {
            hot.remove(0);
        }
        hot.push((name, b.clone()));
        Ok(b)
    }
}

/// Bytes under a directory (what dropping it frees).
fn dir_bytes(dir: &Path) -> u64 {
    let mut n = 0;
    for e in fs::read_dir(dir).into_iter().flatten().flatten() {
        match e.metadata() {
            Ok(m) if m.is_dir() => n += dir_bytes(&e.path()),
            Ok(m) => n += m.len(),
            Err(_) => {}
        }
    }
    n
}

pub fn verify_chunk(hash: &str, list: &[u8], idx: u64, b: &[u8]) -> Result<()> {
    let off = (idx * 32) as usize;
    if off + 32 > list.len() {
        bail!("casfs: {hash}: chunk {idx} outside the list");
    }
    let sum = Sha256::digest(b);
    if sum[..] != list[off..off + 32] {
        bail!("casfs: {hash} chunk {idx}: content does not match the artifact's own list");
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// S3, path style, SigV4 as casfs signs it

pub struct S3Config {
    pub endpoint: String,
    pub bucket: String,
    pub prefix: String,
    pub access_key: String,
    pub secret_key: String,
    pub region: String,
}

pub struct S3 {
    cfg: S3Config,
    agent: ureq::Agent,
}

const EMPTY_SHA256: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

fn hmac_sha(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut m = Hmac::<Sha256>::new_from_slice(key).unwrap();
    m.update(data);
    m.finalize().into_bytes().to_vec()
}

fn amz_dates() -> (String, String) {
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs();
    let w = window_name(now - now % 1200 + 0); // reuse the civil conversion for Y-M-D
    let days = now / 86400;
    let secs = now % 86400;
    let ymd = &w[..10];
    let date = format!("{}{}{}", &ymd[0..4], &ymd[5..7], &ymd[8..10]);
    let _ = days;
    let amz = format!("{date}T{:02}{:02}{:02}Z", secs / 3600, (secs % 3600) / 60, secs % 60);
    (amz, date)
}

impl S3 {
    pub fn new(cfg: S3Config) -> S3 {
        S3 { cfg, agent: ureq::AgentBuilder::new().build() }
    }
    pub fn from_env() -> Result<Option<S3>> {
        let endpoint = std::env::var("EPOCHDB_S3_ENDPOINT").unwrap_or_default();
        if endpoint.is_empty() {
            return Ok(None);
        }
        let bucket = std::env::var("EPOCHDB_S3_BUCKET").unwrap_or_default();
        if bucket.is_empty() {
            bail!("EPOCHDB_S3_BUCKET is required with EPOCHDB_S3_ENDPOINT");
        }
        let access_key = std::env::var("EPOCHDB_S3_ACCESS_KEY").unwrap_or_default();
        let secret_key = std::env::var("EPOCHDB_S3_SECRET_KEY").unwrap_or_default();
        if access_key.is_empty() || secret_key.is_empty() {
            bail!("EPOCHDB_S3_ACCESS_KEY and EPOCHDB_S3_SECRET_KEY are required (no default credential chain here)");
        }
        let region = std::env::var("EPOCHDB_S3_REGION").ok().filter(|s| !s.is_empty()).unwrap_or_else(|| "auto".into());
        Ok(Some(S3::new(S3Config { endpoint, bucket, prefix: std::env::var("EPOCHDB_S3_PREFIX").unwrap_or_default(), access_key, secret_key, region })))
    }
    pub fn key(&self, hash: &str) -> String {
        format!("{}{hash}", self.cfg.prefix)
    }
    fn url(&self, key: &str) -> String {
        format!("{}/{}/{key}", self.cfg.endpoint.trim_end_matches('/'), self.cfg.bucket)
    }
    fn signed(&self, method: &str, key: &str, query: &str, payload_hash: &str) -> ureq::Request {
        let (amz, date) = amz_dates();
        let url = self.url(key);
        let host = url.split("//").nth(1).unwrap().split('/').next().unwrap().to_string();
        let path = format!("/{}/{key}", self.cfg.bucket);
        let signed = "host;x-amz-content-sha256;x-amz-date";
        let canon_headers = format!("host:{host}\nx-amz-content-sha256:{payload_hash}\nx-amz-date:{amz}\n");
        let canon = [method, &path, query, &canon_headers, signed, payload_hash].join("\n");
        let scope = format!("{date}/{}/s3/aws4_request", self.cfg.region);
        let sts = format!("AWS4-HMAC-SHA256\n{amz}\n{scope}\n{}", hex::encode(Sha256::digest(canon.as_bytes())));
        let mut k = hmac_sha(format!("AWS4{}", self.cfg.secret_key).as_bytes(), date.as_bytes());
        k = hmac_sha(&k, self.cfg.region.as_bytes());
        k = hmac_sha(&k, b"s3");
        k = hmac_sha(&k, b"aws4_request");
        let sig = hex::encode(hmac_sha(&k, sts.as_bytes()));
        let full = if query.is_empty() { url } else { format!("{url}?{query}") };
        self.agent
            .request(method, &full)
            .set("X-Amz-Content-Sha256", payload_hash)
            .set("X-Amz-Date", &amz)
            .set("Authorization", &format!("AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed}, Signature={sig}", self.cfg.access_key))
    }
    /// Object size, None on 404.
    pub fn head(&self, key: &str) -> Result<Option<u64>> {
        match self.signed("HEAD", key, "", EMPTY_SHA256).call() {
            Ok(r) => Ok(Some(r.header("Content-Length").and_then(|v| v.parse().ok()).ok_or_else(|| anyhow!("casfs: HEAD {key}: no Content-Length"))?)),
            Err(ureq::Error::Status(404, _)) => Ok(None),
            Err(e) => Err(anyhow!("casfs: HEAD {key}: {e}")),
        }
    }
    fn get_key_range(&self, key: &str, off: u64, n: u64) -> Result<Vec<u8>> {
        let r = self
            .signed("GET", key, "", EMPTY_SHA256)
            .set("Range", &format!("bytes={off}-{}", off + n - 1))
            .call()
            .map_err(|e| anyhow!("casfs: GET {key} [{off},{}): {e}", off + n))?;
        if r.status() != 206 {
            bail!("casfs: GET {key}: expected 206, got {}", r.status());
        }
        let mut b = Vec::with_capacity(n as usize);
        r.into_reader().read_to_end(&mut b)?;
        if b.len() as u64 != n {
            bail!("casfs: GET {key}: got {} bytes, want {n}", b.len());
        }
        Ok(b)
    }
    pub fn get_range(&self, hash: &str, off: u64, n: u64) -> Result<Vec<u8>> {
        self.get_key_range(&self.key(hash), off, n)
    }
    pub fn get_all(&self, key: &str) -> Result<Option<Vec<u8>>> {
        match self.signed("GET", key, "", EMPTY_SHA256).call() {
            Ok(r) => {
                let mut b = Vec::new();
                r.into_reader().read_to_end(&mut b)?;
                Ok(Some(b))
            }
            Err(ureq::Error::Status(404, _)) => Ok(None),
            Err(e) => Err(anyhow!("casfs: GET {key}: {e}")),
        }
    }
    pub fn put(&self, key: &str, body: &[u8]) -> Result<()> {
        let sum = hex::encode(Sha256::digest(body));
        self.signed("PUT", key, "", &sum).set("Content-Length", &body.len().to_string()).send_bytes(body).map_err(|e| anyhow!("casfs: PUT {key}: {e}"))?;
        Ok(())
    }
    pub fn any_object(&self) -> Result<bool> {
        let q = format!("list-type=2&max-keys=1&prefix={}", self.cfg.prefix.replace('/', "%2F"));
        let r = self.signed("GET", "", &q, EMPTY_SHA256).call().map_err(|e| anyhow!("casfs: list: {e}"))?;
        let body = r.into_string()?;
        Ok(body.contains("<Contents>"))
    }
    /// Remote identity: HEAD, then one ranged GET of the tail (plus the last
    /// content chunk, which every consumer's first read wants anyway).
    fn identity(&self, cache: &ChunkCache, hash: &str) -> Result<(u64, Vec<u8>)> {
        let key = self.key(hash);
        let stored = self.head(&key)?.ok_or_else(|| anyhow!("casfs: {hash} is not in the bucket"))?;
        let want = tail_len(stored)?;
        let last = nchunks(stored - want).checked_sub(1);
        let off = match last {
            Some(l) => l * CHUNK_SIZE,
            None => stored - want,
        };
        let buf = self.get_key_range(&key, off, stored - off)?;
        let (size, list) = parse_tail(stored, &buf[buf.len() - want as usize..])?;
        if name_of(&list) != hash {
            bail!("casfs: {hash}: the object under this name carries a list naming {}: REFUSED", name_of(&list));
        }
        if let Some(l) = last {
            let chunk = &buf[..buf.len() - want as usize];
            verify_chunk(hash, &list, l, chunk)?;
            let _ = cache.admit(&format!("{hash}.{l}"), chunk);
        }
        Ok((size, list))
    }
}

// ---------------------------------------------------------------------------
// the store (dist.Store)

pub struct Store {
    pub dir: PathBuf,
    pub spool: PathBuf,
    pub local: PathBuf,
    pub s3: Option<Arc<S3>>,
    cache: Arc<ChunkCache>,
}

impl Store {
    pub fn local(dir: &Path) -> Result<Store> {
        let spool = dir.join("cas");
        let local = dir.join("runs");
        fs::create_dir_all(&spool)?;
        fs::create_dir_all(&local)?;
        let cache_root = std::env::var("EPOCHDB_CACHE_DIR").ok().filter(|s| !s.is_empty()).map(PathBuf::from).unwrap_or_else(|| dir.join("cache"));
        let ns = dir.canonicalize().ok().and_then(|p| p.file_name().map(|s| s.to_string_lossy().into_owned())).unwrap_or_else(|| "default".into());
        let cache = Arc::new(ChunkCache::new(cache_root, ns)?);
        ChunkCache::spawn_worker(&cache);
        Ok(Store { dir: dir.to_path_buf(), spool, local, s3: None, cache })
    }
    /// Local, plus S3 when EPOCHDB_S3_ENDPOINT is set.
    pub fn open(dir: &Path) -> Result<Store> {
        let mut s = Store::local(dir)?;
        s.s3 = S3::from_env()?.map(Arc::new);
        Ok(s)
    }
    pub fn remote(&self) -> bool {
        self.s3.is_some()
    }
    pub fn cache(&self) -> &Arc<ChunkCache> {
        &self.cache
    }
    pub fn spool_path(&self, hash: &str) -> PathBuf {
        self.spool.join(hash)
    }
    pub fn local_path(&self, hash: &str) -> Option<PathBuf> {
        if !valid_hash(hash) {
            return None;
        }
        for e in fs::read_dir(&self.local).ok()?.flatten() {
            let n = e.file_name();
            if n.to_string_lossy().ends_with(hash) {
                return Some(e.path());
            }
        }
        None
    }
    /// Unlinks a run's local copy (an L0 in the local dir, or a spool copy):
    /// a mapping still open keeps reading until it closes.
    pub fn drop_local(&self, hash: &str) -> Result<()> {
        if let Some(p) = self.local_path(hash) {
            fs::remove_file(p)?;
        }
        let sp = self.spool_path(hash);
        if sp.is_file() {
            fs::remove_file(sp)?;
        }
        Ok(())
    }
    /// Seals a written file with its tail and renames it into the spool
    /// (terminal, uploads) or the local dir under `label-hash` (never uploads).
    pub fn adopt(&self, path: &Path, h: Hasher, local_label: Option<&str>) -> Result<String> {
        let (name, tail) = h.finish()?;
        {
            let mut f = fs::OpenOptions::new().append(true).open(path)?;
            f.write_all(&tail)?;
            f.sync_all()?;
        }
        let dst = match local_label {
            Some(l) => self.local.join(format!("{l}-{name}")),
            None => self.spool_path(&name),
        };
        fs::rename(path, &dst)?;
        Ok(name)
    }
    /// Stores b as an artifact (tmp+fsync+rename onto the spool path).
    pub fn put(&self, b: &[u8]) -> Result<String> {
        let mut h = Hasher::new();
        h.write(b);
        let (name, tail) = h.finish()?;
        write_durable(&self.spool_path(&name), &[b, &tail])?;
        Ok(name)
    }
    pub fn open_blob(&self, hash: &str) -> Result<Arc<dyn ReadAt>> {
        if let Some(p) = self.local_path(hash) {
            return Ok(Arc::new(mmap_artifact(&p).with_context(|| format!("open local {hash}"))?));
        }
        let sp = self.spool_path(hash);
        if sp.is_file() {
            return Ok(Arc::new(mmap_artifact(&sp).with_context(|| format!("open {hash}"))?));
        }
        let Some(s3) = &self.s3 else { bail!("casfs: artifact {hash} is not on this machine and no bucket is configured") };
        let (size, list) = s3.identity(&self.cache, hash)?;
        Ok(Arc::new(RemoteBlob { s3: s3.clone(), cache: self.cache.clone(), hash: hash.to_string(), size, list }))
    }
    /// Uploads the spool (content first, pointers last) and unlinks what the
    /// bucket confirms. Returns the released artifact names.
    pub fn sync(&self) -> Result<Vec<String>> {
        let Some(s3) = &self.s3 else { return Ok(Vec::new()) };
        let mut released = Vec::new();
        let mut names: Vec<String> = fs::read_dir(&self.spool)?.flatten().map(|e| e.file_name().to_string_lossy().into_owned()).filter(|n| valid_hash(n)).collect();
        names.sort();
        for hash in names {
            let p = self.spool_path(&hash);
            let stored = fs::metadata(&p)?.len();
            match s3.head(&s3.key(&hash))? {
                Some(sz) if sz == stored => {}
                Some(sz) => bail!("casfs: {hash} is in the bucket at {sz} bytes but the spool file is {stored}"),
                None => {
                    let (_, list) = read_tail(&p)?;
                    let body = fs::read(&p)?;
                    // The name is the integrity check: rebuild the list before a byte goes out.
                    let mut h = Hasher::new();
                    h.write(&body[..body.len() - list.len() - TRAILER_SIZE as usize]);
                    let (n, _) = h.finish()?;
                    if n != hash {
                        bail!("casfs: spool file {hash} hashes to {n}, refusing to upload it");
                    }
                    s3.put(&s3.key(&hash), &body)?;
                }
            }
            fs::remove_file(&p)?;
            released.push(hash);
        }
        let pdir = self.spool.join(".pointers");
        if pdir.is_dir() {
            for e in fs::read_dir(&pdir)?.flatten() {
                let name = e.file_name().to_string_lossy().into_owned();
                if name.ends_with(".tmp") {
                    continue;
                }
                let v = fs::read(e.path())?;
                s3.put(&format!("{}{name}", s3.cfg.prefix), &v)?;
                fs::remove_file(e.path())?;
            }
        }
        Ok(released)
    }
    // ---- pointers (dist.Store.SetPointer / GetPointer)
    pub fn set_pointer(&self, name: &str, value: &str) -> Result<()> {
        write_durable(&self.dir.join(name), &[value.as_bytes()])?;
        let pdir = self.spool.join(".pointers");
        fs::create_dir_all(&pdir)?;
        write_durable(&pdir.join(name), &[value.as_bytes()])
    }
    /// Local copy first, then the bucket. None = no such pointer.
    pub fn get_pointer(&self, name: &str) -> Result<Option<String>> {
        if let Ok(b) = fs::read(self.dir.join(name)) {
            return Ok(Some(String::from_utf8_lossy(&b).into_owned()));
        }
        let Some(s3) = &self.s3 else { return Ok(None) };
        let v = s3.get_all(&format!("{}{name}", s3.cfg.prefix))?;
        if let Some(v) = &v {
            let pdir = self.spool.join(".pointers");
            if !pdir.join(name).exists() {
                write_durable(&self.dir.join(name), &[v])?;
            }
        }
        Ok(v.map(|b| String::from_utf8_lossy(&b).into_owned()))
    }
    pub fn prefix_has_objects(&self) -> Result<bool> {
        match &self.s3 {
            Some(s3) => s3.any_object(),
            None => Ok(false),
        }
    }
}

pub fn latest_pointer(chain_root: &[u8; 32]) -> String {
    format!("latest-{}", hex::encode(chain_root))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A cache with a fake 10 KB filesystem: admission refuses under the
    /// floor, a pass drops the oldest windows until the target, the current
    /// window survives, a read promotes, sweep expires by age and drops husks.
    #[test]
    fn cache_admits_and_evicts_by_watermark() {
        let root = std::env::temp_dir().join(format!("epochdb-cache-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        let cap = 10_000u64;
        let cfg = CacheConfig { min_free: 2_000, evict_target: 4_000, max_age_secs: 7 * 86400 };
        let c = ChunkCache::with_free(root.clone(), "ns".into(), cfg, Box::new(move |p| Ok(cap.saturating_sub(dir_bytes(p)))));
        let now = now_unix();
        let old = |days: u64| window_name(now - days * 86400);
        let chunk = vec![7u8; 1_000];
        // Three old windows of 2 KB each, then the current one.
        for d in [3, 2, 1] {
            c.admit_in(&old(d), "a.0", &chunk).unwrap();
            c.admit_in(&old(d), "b.0", &chunk).unwrap();
        }
        c.admit("cur.0", &chunk).unwrap();
        assert_eq!(dir_bytes(&root), 7_000);
        // free = 3,000: over the floor, admitted; then 2,000, still admitted (>=); then refused.
        c.admit("cur.1", &chunk).unwrap();
        c.admit("cur.2", &chunk).unwrap();
        assert_eq!(dir_bytes(&root), 9_000);
        c.admit("cur.3", &chunk).unwrap();
        assert_eq!(dir_bytes(&root), 9_000, "under the floor the fill is not written");
        assert_eq!(c.stats.refusals.load(Ordering::Relaxed), 1);
        // A pass: free 1,000 < target 4,000: drops the two oldest windows (4 KB) and stops.
        assert!(c.step());
        assert_eq!(c.stats.evictions.load(Ordering::Relaxed), 2);
        assert_eq!(c.stats.freed.load(Ordering::Relaxed), 4_000);
        assert_eq!(c.windows(), vec![old(1), window_name(now)]);
        // A read from the old window promotes it into the current one.
        let p = c.find("a.0").unwrap();
        assert!(p.starts_with(root.join(window_name(now))));
        assert!(!root.join(old(1)).join("ns").join("a.0").exists());
        // Nothing to evict below the target but the current window: it stays.
        c.admit_in(&old(1), "z.0", &vec![1u8; 3_000]).unwrap();
        assert!(c.step());
        assert_eq!(c.windows(), vec![window_name(now)]);
        assert!(!c.step(), "only the current window is left: no progress, no theatre");
        // Sweep: a window past max_age goes by name whatever it holds; an old husk goes.
        c.admit_in(&old(8), "x.0", &chunk).unwrap();
        fs::create_dir_all(root.join(old(2)).join("ns")).unwrap();
        c.sweep();
        assert_eq!(c.windows(), vec![window_name(now)]);
        assert!(valid_window(&window_name(now)) && !valid_window("2026-09-09T01-15") && !valid_window("junk"));
        let _ = fs::remove_dir_all(&root);
    }
}

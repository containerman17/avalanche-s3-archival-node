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

pub struct ChunkCache {
    root: PathBuf,
    ns: String,
    hot: Mutex<Vec<(String, Arc<Vec<u8>>)>>,
}

fn window_name(unix: u64) -> String {
    let t = unix - unix % 1200;
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

impl ChunkCache {
    pub fn new(root: PathBuf, ns: String) -> ChunkCache {
        ChunkCache { root, ns, hot: Mutex::new(Vec::new()) }
    }
    fn find(&self, name: &str) -> Option<PathBuf> {
        let mut wins: Vec<_> = fs::read_dir(&self.root).ok()?.flatten().map(|e| e.file_name()).collect();
        wins.sort();
        for w in wins.iter().rev() {
            let p = self.root.join(w).join(&self.ns).join(name);
            if p.is_file() {
                return Some(p);
            }
        }
        None
    }
    fn admit(&self, name: &str, b: &[u8]) -> Result<()> {
        let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_secs();
        let dir = self.root.join(window_name(now)).join(&self.ns);
        fs::create_dir_all(&dir)?;
        write_durable(&dir.join(name), &[b])
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
        Ok(Store { dir: dir.to_path_buf(), spool, local, s3: None, cache: Arc::new(ChunkCache::new(cache_root, ns)) })
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

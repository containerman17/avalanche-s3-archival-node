//! The unflushed window (store/mem.go): one append-only log beside the runs,
//! replayed at open and cut at the last complete block; chain rows keep one
//! offset per row, state and lookup rows stay in RAM for the sort at flush.
//!
//! Records: [kind u8][num u64 BE][len u32 BE][payload]. Kinds: B H I P R T
//! chain rows (num = height or TxNum), S state (num = TxNum, payload =
//! [klen u16][key prefix][value]), L posting ([payload byte][group]), X a
//! suffix-free lookup row (payload = full key, num = value), K a set/ row, C
//! code ([hash 32][blob], num 0), E end of block (payload = next TxNum).

use crate::format::*;
use anyhow::{anyhow, bail, Context, Result};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

pub const FROZEN_LOG: &str = "window.frozen.log";
const REC_HEADER: usize = 1 + 8 + 4;
const FAM_REC: [u8; NUM_FAMS] = [b'B', b'H', b'I', b'P', b'Q', b'R', b'T', b'W'];

// ---------------------------------------------------------------------------
// what a block writes

#[derive(Debug, Clone)]
pub struct StateRow {
    /// b'a' account RLP, b'c' code hash, b's' slot value; empty = deleted/cleared.
    pub kind: u8,
    pub addr: [u8; 20],
    pub slot: [u8; 32],
    pub val: Vec<u8>,
}

impl StateRow {
    pub fn key_prefix(&self) -> Vec<u8> {
        match self.kind {
            b'a' => account_prefix(&self.addr),
            b'c' => coderef_prefix(&self.addr),
            _ => slot_prefix(&self.addr, &self.slot),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LogWrite {
    pub emitter: [u8; 20],
    pub topics: Vec<[u8; 32]>,
}

#[derive(Debug, Clone)]
pub struct TxWrite {
    pub hash: [u8; 32],
    pub rlp: Vec<u8>,
    pub receipt: Vec<u8>,
    /// callTracer JSON, verbatim.
    pub frames: Vec<u8>,
    pub frame_addrs: Vec<[u8; 20]>,
    pub state: Vec<StateRow>,
    pub sender: Option<[u8; 20]>,
    pub to: Option<[u8; 20]>,
    pub created: Option<[u8; 20]>,
    pub logs: Vec<LogWrite>,
}

#[derive(Debug, Clone, Default)]
pub struct BlockWrite {
    pub height: u64,
    /// What a peer names the block by: sha256 of the unsigned proposervm
    /// bytes (keccak of the header for a bare pre-fork block). The Go store
    /// hashes the whole container instead, which only agrees when the wrapper
    /// carries no signature.
    pub container_id: [u8; 32],
    pub header_rlp: Vec<u8>,
    pub pvm: Vec<u8>,
    pub txs: Vec<TxWrite>,
    pub code: Vec<([u8; 32], Vec<u8>)>,
    /// State written outside any tx; lands at the block's boundary slot.
    pub tail: Vec<StateRow>,
    /// The receipts blob as the plugin was handed it (rcb/ row); empty when
    /// the producer has none.
    pub receipts_blob: Vec<u8>,
    /// The state engine's write set + code hashes, framed by `frame_ws`
    /// (ws/ row); empty when the producer has none.
    pub ws: Vec<u8>,
}

/// ws/ row framing: [n u32][klen u32][key][vlen u32][val]... [m u32][hash 32]...
pub fn frame_ws(ws: &[(Vec<u8>, Vec<u8>)], code_hashes: &[[u8; 32]]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&(ws.len() as u32).to_le_bytes());
    for (k, v) in ws {
        out.extend_from_slice(&(k.len() as u32).to_le_bytes());
        out.extend_from_slice(k);
        out.extend_from_slice(&(v.len() as u32).to_le_bytes());
        out.extend_from_slice(v);
    }
    out.extend_from_slice(&(code_hashes.len() as u32).to_le_bytes());
    for h in code_hashes {
        out.extend_from_slice(h);
    }
    out
}

pub fn unframe_ws(b: &[u8]) -> Result<(Vec<(Vec<u8>, Vec<u8>)>, Vec<[u8; 32]>)> {
    let mut p = 0usize;
    let mut take = |n: usize| -> Result<&[u8]> {
        if b.len() < p + n {
            bail!("ws row: truncated");
        }
        p += n;
        Ok(&b[p - n..p])
    };
    let n = u32::from_le_bytes(take(4)?.try_into().unwrap()) as usize;
    let mut ws = Vec::with_capacity(n);
    for _ in 0..n {
        let kl = u32::from_le_bytes(take(4)?.try_into().unwrap()) as usize;
        let k = take(kl)?.to_vec();
        let vl = u32::from_le_bytes(take(4)?.try_into().unwrap()) as usize;
        let v = take(vl)?.to_vec();
        ws.push((k, v));
    }
    let m = u32::from_le_bytes(take(4)?.try_into().unwrap()) as usize;
    let mut hashes = Vec::with_capacity(m);
    for _ in 0..m {
        hashes.push(take(32)?.try_into().unwrap());
    }
    Ok((ws, hashes))
}

fn hex20(v: &serde_json::Value) -> Option<[u8; 20]> {
    let s = v.as_str()?.strip_prefix("0x")?;
    let b = hex::decode(s).ok()?;
    b.try_into().ok()
}

/// Every from/to the callTracer saw, top-level and nested (vmexec/frames.go).
pub fn frame_participants(trace_json: &[u8]) -> Result<Vec<[u8; 20]>> {
    let v: serde_json::Value = serde_json::from_slice(trace_json).context("trace json")?;
    let mut out = Vec::new();
    let mut seen = HashSet::new();
    fn walk(v: &serde_json::Value, out: &mut Vec<[u8; 20]>, seen: &mut HashSet<[u8; 20]>) {
        for f in ["from", "to"] {
            if let Some(a) = v.get(f).and_then(hex20) {
                if seen.insert(a) {
                    out.push(a);
                }
            }
        }
        if let Some(calls) = v.get("calls").and_then(|c| c.as_array()) {
            for c in calls {
                walk(c, out, seen);
            }
        }
    }
    walk(&v, &mut out, &mut seen);
    Ok(out)
}

fn exec_row(r: &exec::StateRow) -> StateRow {
    match r {
        exec::StateRow::Account { addr, val } => StateRow { kind: b'a', addr: addr.0 .0, slot: [0; 32], val: val.clone() },
        exec::StateRow::Slot { addr, slot, val } => StateRow { kind: b's', addr: addr.0 .0, slot: slot.0, val: val.clone() },
        exec::StateRow::CodeUse { addr, code_hash } => StateRow { kind: b'c', addr: addr.0 .0, slot: [0; 32], val: code_hash.0.to_vec() },
    }
}

impl BlockWrite {
    /// Everything the store keeps for one block, from the decoder's block and
    /// the executor's result: the node's checker builds this and hands it to
    /// `DB::write_block`.
    pub fn from_exec(b: &block::Block, r: &exec::BlockResult) -> Result<BlockWrite> {
        let u = block::pvm::unwrap(&b.container).map_err(|e| anyhow!("unwrap container: {e}"))?;
        let off = (u.inner.as_ptr() as usize).checked_sub(b.container.as_ptr() as usize).filter(|o| o + u.inner.len() <= b.container.len());
        let inner_off = match off {
            Some(o) => o,
            None => b.container.windows(u.inner.len()).position(|w| w == &u.inner[..]).ok_or_else(|| anyhow!("inner block is not inside the container"))?,
        };
        let tx_rlps = crate::container::tx_elements(&u.inner)?;
        if tx_rlps.len() != b.txs.len() {
            bail!("block {}: {} tx elements for {} decoded txs", b.height, tx_rlps.len(), b.txs.len());
        }
        let pvm = crate::container::split_container(&b.container, inner_off, u.inner.len(), &b.header_rlp, &tx_rlps)?;
        if r.txs.len() != b.txs.len() {
            bail!("block {}: {} tx results for {} txs", b.height, r.txs.len(), b.txs.len());
        }
        let mut txs = Vec::with_capacity(b.txs.len());
        for ((t, tr), elem) in b.txs.iter().zip(&r.txs).zip(&tx_rlps) {
            let logs: Vec<crate::receipts::LogIn> = tr
                .receipt
                .logs()
                .iter()
                .map(|l| crate::receipts::LogIn { address: l.address.as_slice(), topics: l.data.topics().iter().map(|t| t.as_slice()).collect(), data: &l.data.data })
                .collect();
            let receipt = crate::receipts::encode(tr.status as u64, tr.gas_used, tr.cumulative_gas_used, &logs);
            let created = match (t.to, t.sender) {
                (None, Some(s)) => Some(s.create(t.nonce).0 .0),
                _ => None,
            };
            txs.push(TxWrite {
                hash: t.hash.0,
                rlp: elem.to_vec(),
                receipt,
                frame_addrs: frame_participants(tr.trace_json.as_bytes())?,
                frames: tr.trace_json.as_bytes().to_vec(),
                state: tr.rows.iter().map(exec_row).collect(),
                sender: t.sender.map(|a| a.0 .0),
                to: t.to.map(|a| a.0 .0),
                created,
                logs: tr
                    .receipt
                    .logs()
                    .iter()
                    .map(|l| LogWrite { emitter: l.address.0 .0, topics: l.data.topics().iter().map(|t| t.0).collect() })
                    .collect(),
            });
        }
        let mut code = Vec::new();
        let mut seen = HashSet::new();
        for (h, c) in &r.code {
            if seen.insert(h.0) {
                code.push((h.0, c.to_vec()));
            }
        }
        Ok(BlockWrite { height: b.height, container_id: b.container_id.0, header_rlp: b.header_rlp.to_vec(), pvm, txs, code, tail: r.tail.iter().map(exec_row).collect(), receipts_blob: Vec::new(), ws: Vec::new() })
    }
}

// ---------------------------------------------------------------------------
// the memtable

#[derive(Default)]
struct ChainIndex {
    base: u64,
    set: bool,
    off: Vec<u64>,
    n: Vec<u32>,
}

impl ChainIndex {
    fn add(&mut self, num: u64, off: u64, n: usize) -> Result<()> {
        if !self.set {
            self.base = num;
            self.set = true;
        }
        if num != self.base + self.off.len() as u64 {
            bail!("store: chain row {num} is not contiguous after {}", self.base + self.off.len() as u64);
        }
        self.off.push(off);
        self.n.push(n as u32);
        Ok(())
    }
    fn find(&self, num: u64) -> Option<(u64, u32)> {
        if !self.set || num < self.base || num >= self.base + self.off.len() as u64 {
            return None;
        }
        let i = (num - self.base) as usize;
        if self.off[i] == 0 {
            return None;
        }
        Some((self.off[i], self.n[i]))
    }
    /// Parks a tx-keyed family past a boundary slot (absent = offset 0).
    fn pad_to(&mut self, next: u64) {
        if !self.set {
            self.base = next - 1;
            self.set = true;
        }
        while self.base + self.off.len() as u64 <= next - 1 {
            self.off.push(0);
            self.n.push(0);
        }
    }
    fn reset(&mut self) {
        *self = ChainIndex::default();
    }
}

pub struct Memtable {
    pub path: PathBuf,
    f: fs::File,
    w: BufWriter<fs::File>,
    read_only: bool,
    pos: u64,
    flushed: u64,
    pub base_tx: u64,
    pub next_tx: u64,
    pub base_height: u64,
    pub next_height: u64,
    pub started: bool,
    chain: [ChainIndex; NUM_FAMS],
    /// The state index: key hash -> newest `StateEnt`, versions chained
    /// newest first. Keys and values stay in the log (read back with pread,
    /// or mmap for the seal), so a row costs ~40 B resident on top of its
    /// log bytes; the old HashMap<Vec<u8>, Vec<(u64, Vec<u8>)>> cost 3.6x
    /// the raw log (see mem_tests).
    pub state: HashMap<u64, u32>,
    pub ents: Vec<StateEnt>,
    pub code: HashMap<[u8; 32], Vec<u8>>,
    pub nums: HashMap<Vec<u8>, u64>,
    pub post: Vec<(Vec<u8>, u64, u8)>,
    pub sets: HashSet<Vec<u8>>,
}

/// One version of one state row: the 'S' record body ([klen u16][key][val])
/// at `rec` in the log. `next` chains the older versions of the same key
/// hash (u32::MAX ends it).
#[derive(Clone, Copy)]
pub struct StateEnt {
    pub txnum: u64,
    rec: u32,
    len: u32,
    klen: u16,
    next: u32,
}

const NO_ENT: u32 = u32::MAX;

fn key_hash(k: &[u8]) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    k.hash(&mut h);
    h.finish()
}

fn rec_header(kind: u8, num: u64, n: u32) -> [u8; REC_HEADER] {
    let mut h = [0u8; REC_HEADER];
    h[0] = kind;
    h[1..9].copy_from_slice(&num.to_be_bytes());
    h[9..].copy_from_slice(&n.to_be_bytes());
    h
}

impl Memtable {
    pub fn open(path: &Path, read_only: bool) -> Result<Memtable> {
        if let Some(d) = path.parent() {
            fs::create_dir_all(d)?;
        }
        let f = fs::OpenOptions::new().read(true).write(true).create(true).open(path)?;
        let w = BufWriter::with_capacity(1 << 20, f.try_clone()?);
        let mut m = Memtable {
            path: path.to_path_buf(),
            f,
            w,
            read_only,
            pos: 0,
            flushed: 0,
            base_tx: 0,
            next_tx: 0,
            base_height: 0,
            next_height: 0,
            started: false,
            chain: Default::default(),
            state: HashMap::new(),
            ents: Vec::new(),
            code: HashMap::new(),
            nums: HashMap::new(),
            post: Vec::new(),
            sets: HashSet::new(),
        };
        m.clear();
        Ok(m)
    }

    fn clear(&mut self) {
        self.state = HashMap::new();
        self.ents = Vec::new();
        self.code.clear();
        self.nums.clear();
        self.post.clear();
        self.sets.clear();
        for c in &mut self.chain {
            c.reset();
        }
        self.pos = 0;
        self.flushed = 0;
        self.started = false;
    }

    fn flush_buf(&mut self) -> Result<()> {
        if self.flushed != self.pos {
            self.w.flush()?;
            self.flushed = self.pos;
        }
        Ok(())
    }

    /// Replays the log from where the sealed runs end, cutting a torn tail
    /// at the last complete block (writer only).
    pub fn recover(&mut self, base_tx: u64, base_height: u64) -> Result<()> {
        self.clear();
        self.base_tx = base_tx;
        self.next_tx = base_tx;
        self.base_height = base_height;
        self.next_height = base_height;
        self.w.flush()?;
        let mut r = std::io::BufReader::with_capacity(1 << 20, self.f.try_clone()?);
        r.seek(SeekFrom::Start(0))?;
        let mut off = 0u64;
        let mut good = 0u64;
        let mut hdr = [0u8; REC_HEADER];
        enum Pend {
            Chain(usize, u64, u64, usize),
            State(Vec<u8>, u64, u64, usize),
            Post(Vec<u8>, u64, u8),
            Num(Vec<u8>, u64),
            Set(Vec<u8>),
            Code([u8; 32], Vec<u8>),
        }
        let mut pend: Vec<Pend> = Vec::new();
        loop {
            if r.read_exact(&mut hdr).is_err() {
                break;
            }
            let kind = hdr[0];
            let num = u64::from_be_bytes(hdr[1..9].try_into().unwrap());
            let n = u32::from_be_bytes(hdr[9..].try_into().unwrap());
            if n > 1 << 30 {
                break;
            }
            let mut payload = vec![0u8; n as usize];
            if r.read_exact(&mut payload).is_err() {
                break;
            }
            let body = off + REC_HEADER as u64;
            off = body + n as u64;
            match kind {
                b'B' | b'H' | b'I' | b'P' | b'Q' | b'R' | b'T' | b'W' => {
                    let fam = FAM_REC.iter().position(|&k| k == kind).unwrap();
                    pend.push(Pend::Chain(fam, num, body, n as usize));
                }
                b'S' => {
                    if payload.len() < 2 {
                        break;
                    }
                    let kl = u16::from_be_bytes([payload[0], payload[1]]) as usize;
                    if payload.len() < 2 + kl {
                        break;
                    }
                    pend.push(Pend::State(payload[2..2 + kl].to_vec(), num, body, n as usize));
                }
                b'L' => {
                    if payload.is_empty() {
                        break;
                    }
                    pend.push(Pend::Post(payload[1..].to_vec(), num, payload[0]));
                }
                b'X' => pend.push(Pend::Num(payload, num)),
                b'K' => pend.push(Pend::Set(payload)),
                b'C' => {
                    if payload.len() < 32 {
                        break;
                    }
                    pend.push(Pend::Code(payload[..32].try_into().unwrap(), payload[32..].to_vec()));
                }
                b'E' => {
                    if n != 8 {
                        break;
                    }
                    // A block a run already holds is dropped: chain rows are written exactly once.
                    if num < self.base_height {
                        pend.clear();
                        good = off;
                        continue;
                    }
                    if !self.started {
                        self.base_height = num;
                        self.started = true;
                    }
                    let nt = u64::from_be_bytes(payload[..8].try_into().unwrap());
                    for p in pend.drain(..) {
                        match p {
                            Pend::Chain(fam, num, body, len) => self.chain[fam].add(num, body, len).context("store: window log replay")?,
                            Pend::State(k, tn, rec, len) => self.put_state(&k, tn, rec, len),
                            Pend::Post(g, tn, p) => self.post.push((g, tn, p)),
                            Pend::Num(k, v) => {
                                self.nums.insert(k, v);
                            }
                            Pend::Set(k) => {
                                self.sets.insert(k);
                            }
                            Pend::Code(h, b) => {
                                self.code.insert(h, b);
                            }
                        }
                    }
                    for fam in TX_KEYED_FAMS {
                        self.chain[fam].pad_to(nt);
                    }
                    self.next_height = num + 1;
                    self.next_tx = nt;
                    good = off;
                }
                _ => break,
            }
        }
        if !self.read_only {
            self.f.set_len(good)?;
            self.w = BufWriter::with_capacity(1 << 20, self.f.try_clone()?);
            self.w.seek(SeekFrom::Start(good))?;
        }
        self.pos = good;
        self.flushed = good;
        Ok(())
    }

    /// Starts a fresh, empty window at the given TxNum/height.
    pub fn reset(&mut self, base_tx: u64, base_height: u64) -> Result<()> {
        self.clear();
        self.base_tx = base_tx;
        self.next_tx = base_tx;
        self.base_height = base_height;
        self.next_height = base_height;
        self.f.set_len(0)?;
        self.f.sync_all()?;
        self.w = BufWriter::with_capacity(1 << 20, self.f.try_clone()?);
        self.w.seek(SeekFrom::Start(0))?;
        Ok(())
    }

    pub fn sync(&mut self) -> Result<()> {
        self.flush_buf()?;
        self.f.sync_data()?;
        Ok(())
    }
    /// fsyncs what was flushed (a frozen log, from the seal thread).
    pub fn fsync(&self) -> Result<()> {
        self.f.sync_data()?;
        Ok(())
    }
    /// Flushes the buffer and hands back a second handle to fsync outside
    /// the window lock.
    pub fn flush_and_dup(&mut self) -> Result<fs::File> {
        self.flush_buf()?;
        Ok(self.f.try_clone()?)
    }

    /// Unlinks the log (after the run it was sealed into is published); a
    /// reader still holding this memtable keeps reading the open file.
    pub fn remove(&self) -> Result<()> {
        match fs::remove_file(&self.path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    fn write(&mut self, kind: u8, num: u64, parts: &[&[u8]]) -> Result<(u64, usize)> {
        let n: usize = parts.iter().map(|p| p.len()).sum();
        self.w.write_all(&rec_header(kind, num, n as u32))?;
        let body = self.pos + REC_HEADER as u64;
        for p in parts {
            self.w.write_all(p)?;
        }
        self.pos = body + n as u64;
        Ok((body, n))
    }

    fn chain_row(&mut self, fam: usize, num: u64, val: &[u8]) -> Result<()> {
        let (off, n) = self.write(FAM_REC[fam], num, &[val])?;
        self.chain[fam].add(num, off, n)
    }

    fn num_row(&mut self, key: Vec<u8>, num: u64) -> Result<()> {
        self.write(b'X', num, &[&key])?;
        self.nums.insert(key, num);
        Ok(())
    }

    /// Indexes the 'S' record whose body ([klen][key][val], `len` bytes)
    /// starts at `rec`. A second write of the same key in the same tx
    /// replaces the first (same hash + txnum; a 64-bit collision inside one
    /// tx is ignored: the seal reads both records from the log anyway).
    fn put_state(&mut self, key: &[u8], txnum: u64, rec: u64, len: usize) {
        let rec = u32::try_from(rec).expect("store: window log past 4 GiB, lower window-max-bytes");
        let h = key_hash(key);
        let ent = StateEnt { txnum, rec, len: len as u32, klen: key.len() as u16, next: NO_ENT };
        match self.state.get(&h) {
            Some(&i) if self.ents[i as usize].txnum == txnum && self.ents[i as usize].klen == ent.klen => {
                let next = self.ents[i as usize].next;
                self.ents[i as usize] = StateEnt { next, ..ent };
            }
            prev => {
                let next = prev.copied().unwrap_or(NO_ENT);
                self.ents.push(StateEnt { next, ..ent });
                self.state.insert(h, self.ents.len() as u32 - 1);
            }
        }
    }

    /// The (key, value) of one indexed record, read from the log.
    fn state_rec(&self, e: &StateEnt) -> Result<(Vec<u8>, Vec<u8>)> {
        let mut body = vec![0u8; e.len as usize];
        self.f.read_exact_at(&mut body, e.rec as u64).with_context(|| format!("store: window state row at {}", e.rec))?;
        let val = body.split_off(2 + e.klen as usize);
        body.drain(..2);
        Ok((body, val))
    }

    /// Every state row of the window sorted by (key, txnum), the last write
    /// of a (key, txnum) pair winning, read through one mmap of the log.
    pub fn each_state_sorted(&self, mut f: impl FnMut(&[u8], u64, &[u8]) -> Result<()>) -> Result<()> {
        if self.ents.is_empty() {
            return Ok(());
        }
        let map = unsafe { memmap2::Mmap::map(&self.f)? };
        let row = |e: &StateEnt| -> (&[u8], &[u8]) {
            let b = &map[e.rec as usize..(e.rec + e.len) as usize];
            (&b[2..2 + e.klen as usize], &b[2 + e.klen as usize..])
        };
        let mut idx: Vec<u32> = (0..self.ents.len() as u32).collect();
        idx.sort_by(|&a, &b| {
            let (ea, eb) = (&self.ents[a as usize], &self.ents[b as usize]);
            row(ea).0.cmp(row(eb).0).then(ea.txnum.cmp(&eb.txnum)).then(a.cmp(&b))
        });
        for (n, &i) in idx.iter().enumerate() {
            let e = &self.ents[i as usize];
            if let Some(&j) = idx.get(n + 1) {
                let e2 = &self.ents[j as usize];
                if e2.txnum == e.txnum && row(e2).0 == row(e).0 {
                    continue; // superseded within the same tx
                }
            }
            let (k, v) = row(e);
            f(k, e.txnum, v)?;
        }
        Ok(())
    }

    fn state_row(&mut self, txnum: u64, r: &StateRow) -> Result<()> {
        let k = r.key_prefix();
        let kl = (k.len() as u16).to_be_bytes();
        let (body, n) = self.write(b'S', txnum, &[&kl, &k, &r.val])?;
        self.put_state(&k, txnum, body, n);
        Ok(())
    }

    /// Folds one block in. A block the store already holds is skipped; a gap
    /// or an out-of-order block is refused.
    pub fn add(&mut self, b: &BlockWrite) -> Result<()> {
        if b.height < self.next_height {
            return Ok(());
        }
        if !self.started {
            if self.next_height != 0 && b.height != self.next_height {
                bail!("store: block {} would start a fresh window over sealed runs ending at {}", b.height, self.next_height - 1);
            }
            self.base_height = b.height;
            self.next_height = b.height;
            self.started = true;
        }
        if b.height != self.next_height {
            bail!("store: block {} out of order, window expects {}", b.height, self.next_height);
        }
        let first = self.next_tx;
        let mut blk = [0u8; 12];
        blk[..8].copy_from_slice(&first.to_be_bytes());
        blk[8..].copy_from_slice(&(b.txs.len() as u32).to_be_bytes());
        self.chain_row(FAM_BLK, b.height, &blk)?;
        self.chain_row(FAM_HDR, b.height, &b.header_rlp)?;
        self.chain_row(FAM_PVM, b.height, &b.pvm)?;
        self.chain_row(FAM_RCB, b.height, &b.receipts_blob)?;
        self.chain_row(FAM_WS, b.height, &b.ws)?;
        self.num_row(blkh_key(&state::keccak::keccak256(&b.header_rlp)), b.height)?;
        self.num_row(cid_key(&b.container_id), b.height)?;
        for (h, blob) in &b.code {
            if self.code.contains_key(h) {
                continue;
            }
            self.write(b'C', 0, &[h, blob])?;
            self.code.insert(*h, blob.clone());
        }
        for (i, t) in b.txs.iter().enumerate() {
            let n = first + i as u64;
            self.chain_row(FAM_ITX, n, &t.frames)?;
            self.chain_row(FAM_RCPT, n, &t.receipt)?;
            self.chain_row(FAM_TX, n, &t.rlp)?;
            self.num_row(txh_key(&t.hash), n)?;
            let mut post: std::collections::BTreeMap<Vec<u8>, u8> = std::collections::BTreeMap::new();
            let mut role = |a: Option<&[u8; 20]>, r: u8| {
                if let Some(a) = a {
                    *post.entry(addr_prefix(a)).or_insert(0) |= r;
                }
            };
            role(t.sender.as_ref(), ROLE_SENDER);
            role(t.to.as_ref(), ROLE_RECIPIENT);
            role(t.created.as_ref(), ROLE_CREATED);
            for a in &t.frame_addrs {
                role(Some(a), ROLE_FRAME);
            }
            for l in &t.logs {
                role(Some(&l.emitter), ROLE_EMITTER);
            }
            for l in &t.logs {
                let topic0: &[u8] = l.topics.first().map(|t| &t[..]).unwrap_or(&[0u8; 32]);
                if !l.topics.is_empty() {
                    post.entry(sig_group(topic0)).or_insert(0);
                }
                post.entry(elog_group(&l.emitter, topic0)).or_insert(0);
                for (i, tp) in l.topics.iter().enumerate().skip(1).take(3) {
                    *post.entry(tval_group(tp, topic0)).or_insert(0) |= 1 << (i - 1);
                }
            }
            for (g, p) in post {
                self.write(b'L', n, &[&[p], &g])?;
                self.post.push((g, n, p));
            }
            for l in &t.logs {
                for (i, tp) in l.topics.iter().enumerate().skip(1).take(3) {
                    let k = set_key(&l.topics[0], i as u8, tp, &l.emitter);
                    if self.sets.contains(&k) {
                        continue;
                    }
                    self.write(b'K', n, &[&k])?;
                    self.sets.insert(k);
                }
            }
            for r in &t.state {
                self.state_row(n, r)?;
            }
        }
        let tail_at = first + b.txs.len() as u64;
        for r in &b.tail {
            self.state_row(tail_at, r)?;
        }
        for fam in TX_KEYED_FAMS {
            self.chain[fam].pad_to(tail_at + 1);
        }
        self.next_tx = tail_at + 1;
        self.next_height = b.height + 1;
        let end = self.next_tx.to_be_bytes();
        self.write(b'E', b.height, &[&end])?;
        self.flush_buf()
    }

    // ---- reads

    pub fn chain_get(&self, fam: usize, num: u64) -> Result<Option<Vec<u8>>> {
        let Some((off, n)) = self.chain[fam].find(num) else { return Ok(None) };
        if n == 0 {
            return Ok(Some(Vec::new()));
        }
        let mut p = vec![0u8; n as usize];
        self.f.read_exact_at(&mut p, off).with_context(|| format!("store: window row at {off} ({n} bytes)"))?;
        Ok(Some(p))
    }

    /// Every row of a chain family in number order (for the seal).
    pub fn each_chain(&self, fam: usize, mut f: impl FnMut(u64, Vec<u8>) -> Result<()>) -> Result<()> {
        let c = &self.chain[fam];
        for i in 0..c.off.len() {
            if c.off[i] == 0 {
                continue;
            }
            let mut p = vec![0u8; c.n[i] as usize];
            if c.n[i] > 0 {
                self.f.read_exact_at(&mut p, c.off[i])?;
            }
            f(c.base + i as u64, p)?;
        }
        Ok(())
    }

    /// The newest value of `prefix` written at or before `at`: one pread per
    /// version walked (the newest version usually matches first).
    pub fn latest_state(&self, prefix: &[u8], at: u64) -> Option<(Vec<u8>, u64)> {
        let mut i = *self.state.get(&key_hash(prefix))?;
        while i != NO_ENT {
            let e = &self.ents[i as usize];
            if e.txnum <= at && e.klen as usize == prefix.len() {
                if let Ok((k, v)) = self.state_rec(e) {
                    if k == prefix {
                        return Some((v, e.txnum));
                    }
                }
            }
            i = e.next;
        }
        None
    }

    pub fn window(&self) -> (u64, u64, u64, u64, bool) {
        (self.base_tx, self.next_tx, self.base_height, self.next_height, self.started)
    }
    /// Raw bytes of the window log so far (the byte flush trigger).
    pub fn bytes(&self) -> u64 {
        self.pos
    }
}

#[cfg(test)]
mod mem_tests {
    use super::*;

    fn rss_kb() -> u64 {
        let s = std::fs::read_to_string("/proc/self/status").unwrap();
        s.lines().find(|l| l.starts_with("RssAnon:")).unwrap().split_whitespace().nth(1).unwrap().parse().unwrap()
    }

    /// Resident bytes of the memtable per raw byte of the window log, on a
    /// slots-shaped load (50 sstores per tx, one contract per 64 txs).
    /// `cargo test -p epochdb-store --release memtable_resident -- --ignored --nocapture`
    #[test]
    #[ignore]
    fn memtable_resident_per_raw_byte() {
        let path = std::env::temp_dir().join(format!("epochdb-memres-{}", std::process::id())).join("window.log");
        let mut m = Memtable::open(&path, false).unwrap();
        m.reset(0, 0).unwrap();
        let before = rss_kb();
        let mut tn = 0u64;
        for h in 1..=400u64 {
            let mut b = BlockWrite { height: h, header_rlp: vec![0; 500], ..Default::default() };
            for t in 0..200u64 {
                let mut addr = [0u8; 20];
                addr[..8].copy_from_slice(&((h * 200 + t) / 64).to_be_bytes());
                let mut sender = [0u8; 20];
                sender[..8].copy_from_slice(&(h * 200 + t).to_be_bytes());
                let mut state = Vec::with_capacity(51);
                state.push(StateRow { kind: b'a', addr: sender, slot: [0; 32], val: vec![1; 70] });
                for s in 0..50u64 {
                    let mut slot = [0u8; 32];
                    slot[..8].copy_from_slice(&(tn * 50 + s).to_be_bytes());
                    state.push(StateRow { kind: b's', addr, slot, val: vec![7; 32] });
                }
                let mut hash = [0u8; 32];
                hash[..8].copy_from_slice(&tn.to_be_bytes());
                b.txs.push(TxWrite { hash, rlp: vec![0; 110], receipt: vec![0; 60], frames: Vec::new(), frame_addrs: Vec::new(), state, sender: Some(sender), to: Some(addr), created: None, logs: Vec::new() });
                tn += 1;
            }
            m.add(&b).unwrap();
        }
        let raw = m.bytes();
        let resident = (rss_kb() - before) << 10;
        eprintln!("memtable: raw log {} MB, resident {} MB, {:.2}x, {} state keys", raw >> 20, resident >> 20, resident as f64 / raw as f64, m.state.len());
        // reads still work through the index
        let mut addr = [0u8; 20];
        addr[..8].copy_from_slice(&(1u64 * 200 / 64).to_be_bytes());
        let mut slot = [0u8; 32];
        slot[..8].copy_from_slice(&0u64.to_be_bytes());
        assert_eq!(m.latest_state(&slot_prefix(&addr, &slot), u64::MAX).unwrap().0, vec![7; 32]);
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }
}

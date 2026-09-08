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
    pub state: HashMap<Vec<u8>, Vec<(u64, Vec<u8>)>>,
    pub code: HashMap<[u8; 32], Vec<u8>>,
    pub nums: HashMap<Vec<u8>, u64>,
    pub post: Vec<(Vec<u8>, u64, u8)>,
    pub sets: HashSet<Vec<u8>>,
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
            code: HashMap::new(),
            nums: HashMap::new(),
            post: Vec::new(),
            sets: HashSet::new(),
        };
        m.clear();
        Ok(m)
    }

    fn clear(&mut self) {
        self.state.clear();
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
            State(Vec<u8>, u64, Vec<u8>),
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
                    pend.push(Pend::State(payload[2..2 + kl].to_vec(), num, payload[2 + kl..].to_vec()));
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
                            Pend::State(k, tn, v) => self.put_state(k, tn, v),
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

    /// Closes the log and deletes it (after the run it was sealed into is published).
    pub fn remove(self) -> Result<()> {
        let p = self.path.clone();
        drop(self);
        match fs::remove_file(&p) {
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

    fn put_state(&mut self, key: Vec<u8>, txnum: u64, val: Vec<u8>) {
        let h = self.state.entry(key).or_default();
        if let Some(last) = h.last_mut() {
            if last.0 == txnum {
                last.1 = val;
                return;
            }
        }
        h.push((txnum, val));
    }

    fn state_row(&mut self, txnum: u64, r: &StateRow) -> Result<()> {
        let k = r.key_prefix();
        let kl = (k.len() as u16).to_be_bytes();
        self.write(b'S', txnum, &[&kl, &k, &r.val])?;
        self.put_state(k, txnum, r.val.clone());
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

    pub fn latest_state(&self, prefix: &[u8], at: u64) -> Option<(Vec<u8>, u64)> {
        let h = self.state.get(prefix)?;
        let i = h.partition_point(|(t, _)| *t <= at);
        if i == 0 {
            return None;
        }
        Some((h[i - 1].1.clone(), h[i - 1].0))
    }

    pub fn window(&self) -> (u64, u64, u64, u64, bool) {
        (self.base_tx, self.next_tx, self.base_height, self.next_height, self.started)
    }
}

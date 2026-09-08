//! INTERIM store: an append-only log of accepted blocks (`blocks.log`) and
//! one of deployed code (`code.log`) under the chain data dir, with the
//! height / id index rebuilt on open. rs/store replaces it; the engine only
//! needs `BlockStore` from it.
//!
//! blocks.log record: `[u32 len][u64 height][32 id][u32 crc32(payload)][payload]`,
//! payload = `[u32][container] [u32][receipts RLP] [u32 n]{[u32][callTracer JSON]}
//! [u32 n]{[u32][key][u32][value]} [u32 n]{[32 hash][u32][code]}`. Heights are
//! contiguous from 1. One write per record, fsync on demand (`sync`).
//!
//! Open scans record heads (one pread each) and stops on a CLEAN short tail
//! only: a record whose bytes are not all there, or whose crc fails as the
//! last record, is a torn tail and the file is truncated to the last whole
//! record; a short or bad record anywhere else, or any read error, is an
//! error (the wiki rule: never truncate on a read error).
//!
//! code.log record: `[32 hash][u32 len][code]`, same tail rule.
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::os::unix::fs::FileExt;
use std::path::Path;

use bytes::Bytes;

use crate::tree::Id;

const HEAD: usize = 4 + 8 + 32 + 4;

pub struct Record {
    pub height: u64,
    pub id: Id,
    pub container: Bytes,
    pub receipts: Vec<u8>,
    pub traces: Vec<String>,
    pub ws: Vec<(Vec<u8>, Vec<u8>)>,
    pub code: Vec<(alloy_primitives::B256, alloy_primitives::Bytes)>,
}

/// The accepted-block interface the engine needs from a store.
pub trait BlockStore: Send {
    /// The last logged height, 0 when empty.
    fn head(&self) -> u64;
    fn height_of(&self, id: &Id) -> Option<u64>;
    fn id_at(&self, height: u64) -> Option<Id>;
    /// The container bytes at a height (what the plugin was handed).
    fn container(&self, height: u64) -> io::Result<Option<Bytes>>;
    /// The whole record (recovery replay reads the write set).
    fn read(&self, height: u64) -> io::Result<Option<Record>>;
    /// Append the next height's record (height must be head + 1).
    fn append(&mut self, r: &Record) -> io::Result<()>;
    /// Make everything appended durable.
    fn sync(&self) -> io::Result<()>;
}

pub struct BlockLog {
    f: File,
    end: u64,
    /// (offset, id) by height - 1.
    idx: Vec<(u64, Id)>,
    by_id: HashMap<Id, u64>,
}

fn put(buf: &mut Vec<u8>, b: &[u8]) {
    buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
    buf.extend_from_slice(b);
}

fn take<'a>(p: &mut &'a [u8], what: &str) -> io::Result<&'a [u8]> {
    if p.len() < 4 {
        return Err(io::Error::new(io::ErrorKind::InvalidData, format!("blocks.log: short {what} length")));
    }
    let n = u32::from_le_bytes(p[..4].try_into().unwrap()) as usize;
    if p.len() < 4 + n {
        return Err(io::Error::new(io::ErrorKind::InvalidData, format!("blocks.log: short {what}")));
    }
    let (v, rest) = p[4..].split_at(n);
    *p = rest;
    Ok(v)
}

fn count(p: &mut &[u8], what: &str) -> io::Result<usize> {
    Ok(u32::from_le_bytes(take_n(p, 4, what)?.try_into().unwrap()) as usize)
}

fn take_n<'a>(p: &mut &'a [u8], n: usize, what: &str) -> io::Result<&'a [u8]> {
    if p.len() < n {
        return Err(io::Error::new(io::ErrorKind::InvalidData, format!("blocks.log: short {what}")));
    }
    let (v, rest) = p.split_at(n);
    *p = rest;
    Ok(v)
}

fn bad(msg: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg)
}

impl BlockLog {
    /// Opens (creating) the log and rebuilds the index; reports the torn
    /// bytes dropped, if any.
    pub fn open(path: &Path) -> io::Result<(BlockLog, u64)> {
        let f = OpenOptions::new().read(true).append(true).create(true).open(path)?;
        let size = f.metadata()?.len();
        let mut idx = Vec::new();
        let mut by_id = HashMap::new();
        let mut off = 0u64;
        let mut head = [0u8; HEAD];
        let mut torn = 0u64;
        while off < size {
            // A clean short tail: fewer bytes than a head, or a head whose
            // record runs past the end.
            if size - off < HEAD as u64 {
                torn = size - off;
                break;
            }
            f.read_exact_at(&mut head, off)?;
            let len = u32::from_le_bytes(head[..4].try_into().unwrap()) as u64;
            let height = u64::from_le_bytes(head[4..12].try_into().unwrap());
            let id: Id = head[12..44].try_into().unwrap();
            if off + HEAD as u64 + len > size {
                torn = size - off;
                break;
            }
            if height != idx.len() as u64 + 1 {
                return Err(bad(format!("blocks.log: record at {off} has height {height}, expected {}", idx.len() + 1)));
            }
            let next = off + HEAD as u64 + len;
            if next == size {
                // The last record is the one a crash can leave half-written
                // (a partial write of one record buffer): check its crc.
                let mut payload = vec![0u8; len as usize];
                f.read_exact_at(&mut payload, off + HEAD as u64)?;
                let crc = u32::from_le_bytes(head[44..48].try_into().unwrap());
                if crc32fast::hash(&payload) != crc {
                    torn = size - off;
                    break;
                }
            }
            idx.push((off, id));
            by_id.insert(id, height);
            off = next;
        }
        if torn > 0 {
            f.set_len(off)?;
            f.sync_all()?;
        }
        Ok((BlockLog { f, end: off, idx, by_id }, torn))
    }

    /// A second handle on the file for the fsync thread (the lock stays free).
    pub fn dup(&self) -> io::Result<File> {
        self.f.try_clone()
    }

    fn record_at(&self, off: u64) -> io::Result<Vec<u8>> {
        let mut head = [0u8; HEAD];
        self.f.read_exact_at(&mut head, off)?;
        let len = u32::from_le_bytes(head[..4].try_into().unwrap()) as usize;
        let mut buf = vec![0u8; len];
        self.f.read_exact_at(&mut buf, off + HEAD as u64)?;
        let crc = u32::from_le_bytes(head[44..48].try_into().unwrap());
        if crc32fast::hash(&buf) != crc {
            return Err(bad(format!("blocks.log: crc mismatch at {off}")));
        }
        Ok(buf)
    }

    fn offset(&self, height: u64) -> Option<u64> {
        if height == 0 {
            return None;
        }
        self.idx.get(height as usize - 1).map(|(o, _)| *o)
    }
}

impl BlockStore for BlockLog {
    fn head(&self) -> u64 {
        self.idx.len() as u64
    }

    fn height_of(&self, id: &Id) -> Option<u64> {
        self.by_id.get(id).copied()
    }

    fn id_at(&self, height: u64) -> Option<Id> {
        if height == 0 {
            return None;
        }
        self.idx.get(height as usize - 1).map(|(_, id)| *id)
    }

    fn container(&self, height: u64) -> io::Result<Option<Bytes>> {
        let Some(off) = self.offset(height) else { return Ok(None) };
        let mut lens = [0u8; HEAD + 4];
        self.f.read_exact_at(&mut lens, off)?;
        let clen = u32::from_le_bytes(lens[HEAD..].try_into().unwrap()) as usize;
        let mut c = vec![0u8; clen];
        self.f.read_exact_at(&mut c, off + HEAD as u64 + 4)?;
        Ok(Some(Bytes::from(c)))
    }

    fn read(&self, height: u64) -> io::Result<Option<Record>> {
        let Some(off) = self.offset(height) else { return Ok(None) };
        let (_, id) = self.idx[height as usize - 1];
        let buf = self.record_at(off)?;
        let mut p = &buf[..];
        let container = Bytes::from(take(&mut p, "container")?.to_vec());
        let receipts = take(&mut p, "receipts")?.to_vec();
        let n = count(&mut p, "trace count")?;
        let mut traces = Vec::with_capacity(n);
        for _ in 0..n {
            traces.push(String::from_utf8_lossy(take(&mut p, "trace")?).into_owned());
        }
        let n = count(&mut p, "row count")?;
        let mut ws = Vec::with_capacity(n);
        for _ in 0..n {
            let k = take(&mut p, "key")?.to_vec();
            let v = take(&mut p, "value")?.to_vec();
            ws.push((k, v));
        }
        let n = count(&mut p, "code count")?;
        let mut code = Vec::with_capacity(n);
        for _ in 0..n {
            let h = alloy_primitives::B256::from_slice(take_n(&mut p, 32, "code hash")?);
            code.push((h, alloy_primitives::Bytes::from(take(&mut p, "code")?.to_vec())));
        }
        Ok(Some(Record { height, id, container, receipts, traces, ws, code }))
    }

    fn append(&mut self, r: &Record) -> io::Result<()> {
        if r.height != self.head() + 1 {
            return Err(bad(format!("blocks.log: append height {} at head {}", r.height, self.head())));
        }
        let mut payload = Vec::with_capacity(r.container.len() + r.receipts.len() + 4096);
        put(&mut payload, &r.container);
        put(&mut payload, &r.receipts);
        payload.extend_from_slice(&(r.traces.len() as u32).to_le_bytes());
        for t in &r.traces {
            put(&mut payload, t.as_bytes());
        }
        payload.extend_from_slice(&(r.ws.len() as u32).to_le_bytes());
        for (k, v) in &r.ws {
            put(&mut payload, k);
            put(&mut payload, v);
        }
        payload.extend_from_slice(&(r.code.len() as u32).to_le_bytes());
        for (h, c) in &r.code {
            payload.extend_from_slice(h.as_slice());
            put(&mut payload, c);
        }
        let mut rec = Vec::with_capacity(HEAD + payload.len());
        rec.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        rec.extend_from_slice(&r.height.to_le_bytes());
        rec.extend_from_slice(&r.id);
        rec.extend_from_slice(&crc32fast::hash(&payload).to_le_bytes());
        rec.extend_from_slice(&payload);
        (&self.f).write_all(&rec)?;
        self.idx.push((self.end, r.id));
        self.by_id.insert(r.id, r.height);
        self.end += rec.len() as u64;
        Ok(())
    }

    fn sync(&self) -> io::Result<()> {
        self.f.sync_data()
    }
}

/// code.log: every deployed code by hash, read whole on open.
pub struct CodeLog {
    f: File,
}

impl CodeLog {
    pub fn open(path: &Path) -> io::Result<(CodeLog, Vec<(alloy_primitives::B256, alloy_primitives::Bytes)>, u64)> {
        let f = OpenOptions::new().read(true).append(true).create(true).open(path)?;
        let data = std::fs::read(path)?;
        let mut out = Vec::new();
        let mut off = 0usize;
        while off < data.len() {
            if data.len() - off < 36 {
                break;
            }
            let n = u32::from_le_bytes(data[off + 32..off + 36].try_into().unwrap()) as usize;
            if off + 36 + n > data.len() {
                break;
            }
            out.push((alloy_primitives::B256::from_slice(&data[off..off + 32]), alloy_primitives::Bytes::from(data[off + 36..off + 36 + n].to_vec())));
            off += 36 + n;
        }
        let torn = (data.len() - off) as u64;
        if torn > 0 {
            f.set_len(off as u64)?;
            f.sync_all()?;
        }
        Ok((CodeLog { f }, out, torn))
    }

    pub fn append(&mut self, code: &[(alloy_primitives::B256, alloy_primitives::Bytes)]) -> io::Result<()> {
        if code.is_empty() {
            return Ok(());
        }
        let mut buf = Vec::new();
        for (h, c) in code {
            buf.extend_from_slice(h.as_slice());
            put(&mut buf, c);
        }
        (&self.f).write_all(&buf)
    }

    pub fn sync(&self) -> io::Result<()> {
        self.f.sync_data()
    }

    pub fn dup(&self) -> io::Result<File> {
        self.f.try_clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(h: u64) -> Record {
        Record {
            height: h,
            id: [h as u8; 32],
            container: Bytes::from(vec![h as u8; 100]),
            receipts: vec![1, 2, 3],
            traces: vec!["{}".into(), "{\"a\":1}".into()],
            ws: vec![(vec![1u8; 33], vec![9u8; 10]), (vec![2u8; 65], vec![])],
            code: vec![(alloy_primitives::B256::repeat_byte(7), alloy_primitives::Bytes::from_static(b"\x60\x00"))],
        }
    }

    /// Append, reopen, read back; then tear the tail and reopen.
    #[test]
    fn roundtrip_and_torn_tail() {
        let dir = std::env::temp_dir().join(format!("epochdb-blocklog-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let p = dir.join("blocks.log");
        {
            let (mut l, torn) = BlockLog::open(&p).unwrap();
            assert_eq!(torn, 0);
            for h in 1..=5 {
                l.append(&rec(h)).unwrap();
            }
            assert_eq!(l.head(), 5);
            l.sync().unwrap();
        }
        let (l, torn) = BlockLog::open(&p).unwrap();
        assert_eq!(torn, 0);
        assert_eq!(l.head(), 5);
        assert_eq!(l.height_of(&[3u8; 32]), Some(3));
        assert_eq!(l.id_at(4), Some([4u8; 32]));
        let r = l.read(3).unwrap().unwrap();
        assert_eq!(r.container.len(), 100);
        assert_eq!(r.traces.len(), 2);
        assert_eq!(r.ws.len(), 2);
        assert_eq!(r.code[0].1.len(), 2);
        assert_eq!(l.container(5).unwrap().unwrap()[0], 5);
        let size = std::fs::metadata(&p).unwrap().len();
        drop(l);
        // Tear the last record: 7 bytes short.
        std::fs::OpenOptions::new().write(true).open(&p).unwrap().set_len(size - 7).unwrap();
        let (mut l, torn) = BlockLog::open(&p).unwrap();
        assert!(torn > 0);
        assert_eq!(l.head(), 4);
        l.append(&rec(5)).unwrap();
        assert_eq!(l.head(), 5);
        drop(l);
        let (l, torn) = BlockLog::open(&p).unwrap();
        assert_eq!((torn, l.head()), (0, 5));
        let _ = std::fs::remove_dir_all(&dir);
    }
}

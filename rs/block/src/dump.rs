//! The dump file: `[u64 LE height][u32 LE len][container]` records.
use std::fs::File;
use std::path::Path;

use bytes::Bytes;
use memmap2::Mmap;

pub struct Dump {
    data: Bytes,
}

impl Dump {
    pub fn open(path: impl AsRef<Path>) -> std::io::Result<Dump> {
        let f = File::open(path)?;
        // SAFETY: the dump is immutable once written (tmp + rename).
        let m = unsafe { Mmap::map(&f)? };
        let _ = m.advise(memmap2::Advice::Sequential);
        Ok(Dump { data: Bytes::from_owner(m) })
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// records iterates the heights [from, to], `to` inclusive.
    pub fn records(&self, from: u64, to: u64) -> Records {
        Records { data: self.data.clone(), pos: 0, from, to }
    }
}

pub struct Record {
    pub height: u64,
    pub container: Bytes,
}

/// Records walks the file; a `from` past the start skips records by their
/// length prefix.
// ponytail: the skip touches one page per record (no side index); add a
// height -> offset index if random windows deep in a big dump matter.
pub struct Records {
    data: Bytes,
    pos: usize,
    from: u64,
    to: u64,
}

impl Iterator for Records {
    type Item = Record;
    fn next(&mut self) -> Option<Record> {
        loop {
            let d = &self.data[..];
            if self.pos + 12 > d.len() {
                return None;
            }
            let h = u64::from_le_bytes(d[self.pos..self.pos + 8].try_into().unwrap());
            let n = u32::from_le_bytes(d[self.pos + 8..self.pos + 12].try_into().unwrap()) as usize;
            let start = self.pos + 12;
            if start + n > d.len() {
                return None;
            }
            self.pos = start + n;
            if h < self.from {
                continue;
            }
            if h > self.to {
                self.pos = d.len();
                return None;
            }
            return Some(Record { height: h, container: self.data.slice(start..start + n) });
        }
    }
}

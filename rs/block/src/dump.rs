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
        Records { data: self.data.clone(), pos: 0, from, to, released: 0 }
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
    released: usize,
}

/// Consumed pages are given back with MADV_DONTNEED every RELEASE_EVERY bytes,
/// RELEASE_LAG behind the cursor (blocks in flight in the recovery pipeline
/// still point into the mapping; a refault comes from the page cache), so the
/// mapping does not inflate the process RSS.
const RELEASE_EVERY: usize = 64 << 20;
const RELEASE_LAG: usize = 64 << 20;

impl Records {
    fn release(&mut self) {
        let end = self.pos.saturating_sub(RELEASE_LAG) & !4095;
        if end < self.released + RELEASE_EVERY {
            return;
        }
        // SAFETY: the range is inside the file mapping; DONTNEED on a shared
        // read-only file mapping only drops page table entries.
        unsafe {
            libc::madvise(self.data.as_ptr().add(self.released) as *mut libc::c_void, end - self.released, libc::MADV_DONTNEED);
        }
        self.released = end;
    }
}

impl Iterator for Records {
    type Item = Record;
    fn next(&mut self) -> Option<Record> {
        loop {
            let len = self.data.len();
            if self.pos + 12 > len {
                return None;
            }
            let d = &self.data[self.pos..self.pos + 12];
            let h = u64::from_le_bytes(d[..8].try_into().unwrap());
            let n = u32::from_le_bytes(d[8..12].try_into().unwrap()) as usize;
            let start = self.pos + 12;
            if start + n > len {
                return None;
            }
            self.pos = start + n;
            self.release();
            if h < self.from {
                continue;
            }
            if h > self.to {
                self.pos = len;
                return None;
            }
            return Some(Record { height: h, container: self.data.slice(start..start + n) });
        }
    }
}

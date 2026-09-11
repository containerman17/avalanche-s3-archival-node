//! The diff log: every block's rows as the applier put them into the hot
//! state, appended to flat files the out-of-process checker (`cnode-check`)
//! tails. Segments of `SEGMENT_BLOCKS` blocks under `<data>/difflog/`, named
//! by their first height (zero padded, so name order is height order).
//!
//! Record: u64 le height, 32 B header state root, u32 le len, `history::encode_rows` bytes.
//! The writer flushes after every record; a torn tail (a crash mid-write) is
//! left where it is: the writer starts a new segment on every open and the
//! reader drops a short record at the end of a segment that has a successor.
//! A height may appear twice (a crash between the log append and the history
//! write re-applies the block); the reader skips heights it has passed.

use alloy_primitives::B256;
use anyhow::{bail, Context, Result};
use std::fs::File;
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

pub const SEGMENT_BLOCKS: u64 = 1000;

const HEADER: usize = 8 + 32 + 4;

fn segment_path(dir: &Path, first: u64) -> PathBuf {
    dir.join(format!("{first:020}.log"))
}

/// The segments in `dir` as (first height, path), oldest first.
pub fn segments(dir: &Path) -> Result<Vec<(u64, PathBuf)>> {
    let mut out = Vec::new();
    let rd = match std::fs::read_dir(dir) {
        Ok(rd) => rd,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(out),
        Err(e) => return Err(e.into()),
    };
    for e in rd {
        let e = e?;
        let name = e.file_name().to_string_lossy().to_string();
        if let Some(h) = name.strip_suffix(".log").and_then(|s| s.parse::<u64>().ok()) {
            out.push((h, e.path()));
        }
    }
    out.sort();
    Ok(out)
}

/// Deletes every segment whose blocks are all at or below `below` (its
/// successor's first height is `below + 1` or less). The newest segment stays.
pub fn prune(dir: &Path, below: u64) -> Result<usize> {
    let segs = segments(dir)?;
    let mut n = 0;
    for w in segs.windows(2) {
        if w[1].0 <= below + 1 {
            std::fs::remove_file(&w[0].1)?;
            n += 1;
        }
    }
    Ok(n)
}

pub struct Writer {
    dir: PathBuf,
    cur: Option<(u64, BufWriter<File>)>,
}

impl Writer {
    pub fn open(dir: &Path) -> Result<Writer> {
        std::fs::create_dir_all(dir)?;
        Ok(Writer { dir: dir.to_path_buf(), cur: None })
    }

    pub fn append(&mut self, height: u64, root: B256, rows: &[u8]) -> Result<()> {
        if !matches!(&self.cur, Some((first, _)) if height - first < SEGMENT_BLOCKS) {
            let f = File::options().append(true).create(true).open(segment_path(&self.dir, height))?;
            self.cur = Some((height, BufWriter::with_capacity(1 << 20, f)));
        }
        let (_, w) = self.cur.as_mut().unwrap();
        w.write_all(&height.to_le_bytes())?;
        w.write_all(root.as_slice())?;
        w.write_all(&(rows.len() as u32).to_le_bytes())?;
        w.write_all(rows)?;
        w.flush()?;
        Ok(())
    }
}

pub struct Record {
    pub height: u64,
    pub root: B256,
    pub rows: Vec<(Vec<u8>, Vec<u8>)>,
}

/// Reads records above `after`, in height order, across segments; `next`
/// returns `None` when the log has nothing more yet (call again later).
pub struct Reader {
    dir: PathBuf,
    after: u64,
    cur: Option<(u64, File, u64)>,
}

impl Reader {
    pub fn new(dir: &Path, after: u64) -> Reader {
        Reader { dir: dir.to_path_buf(), after, cur: None }
    }

    /// The segment holding `after + 1`: the last one starting at or below it,
    /// else the first one.
    fn open_first(&mut self) -> Result<bool> {
        let segs = segments(&self.dir)?;
        let Some(&(first, ref path)) = segs.iter().rev().find(|(f, _)| *f <= self.after + 1).or(segs.first()) else { return Ok(false) };
        self.cur = Some((first, File::open(path)?, 0));
        Ok(true)
    }

    fn open_next(&mut self) -> Result<bool> {
        let cur_first = self.cur.as_ref().map(|(f, _, _)| *f).unwrap();
        let segs = segments(&self.dir)?;
        let Some((first, path)) = segs.into_iter().find(|(f, _)| *f > cur_first) else { return Ok(false) };
        self.cur = Some((first, File::open(&path)?, 0));
        Ok(true)
    }

    /// One complete record at the current offset, or `None` at a short read
    /// (the offset is left where it was).
    fn read_one(&mut self) -> Result<Option<Record>> {
        let (_, f, off) = self.cur.as_mut().unwrap();
        f.seek(SeekFrom::Start(*off))?;
        let mut head = [0u8; HEADER];
        if f.read(&mut head).map(|n| n < HEADER).unwrap_or(true) {
            return Ok(None);
        }
        let height = u64::from_le_bytes(head[..8].try_into().unwrap());
        let root = B256::from_slice(&head[8..40]);
        let len = u32::from_le_bytes(head[40..44].try_into().unwrap()) as usize;
        let mut body = vec![0u8; len];
        if f.read_exact(&mut body).is_err() {
            return Ok(None);
        }
        *off += (HEADER + len) as u64;
        Ok(Some(Record { height, root, rows: crate::history::decode_rows(&body) }))
    }

    pub fn next(&mut self) -> Result<Option<Record>> {
        loop {
            if self.cur.is_none() && !self.open_first()? {
                return Ok(None);
            }
            match self.read_one()? {
                Some(r) if r.height <= self.after => continue,
                Some(r) => {
                    if r.height != self.after + 1 {
                        bail!("difflog: block {} follows {} (gap)", r.height, self.after);
                    }
                    self.after = r.height;
                    return Ok(Some(r));
                }
                None => {
                    // End of what this segment holds: move on if a later one
                    // exists (this one is complete), else wait for more.
                    if !self.open_next()? {
                        return Ok(None);
                    }
                }
            }
        }
    }

    pub fn after(&self) -> u64 {
        self.after
    }
}

impl Record {
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
}

/// The last height a reader would reach from `after` (a cheap look for status lines).
pub fn head(dir: &Path, after: u64) -> Result<u64> {
    let mut r = Reader::new(dir, after);
    while r.next().context("difflog head")?.is_some() {}
    Ok(r.after())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::history::encode_rows;

    #[test]
    fn write_read_rotate_prune() {
        let dir = std::env::temp_dir().join(format!("cnode-difflog-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let mut w = Writer::open(&dir).unwrap();
        let row = |h: u64| vec![(vec![h as u8; 33], vec![1, 2, 3])];
        for h in 1..=2500u64 {
            w.append(h, B256::repeat_byte(h as u8), &encode_rows(&row(h))).unwrap();
        }
        assert_eq!(segments(&dir).unwrap().iter().map(|(f, _)| *f).collect::<Vec<_>>(), vec![1, 1001, 2001]);

        // Tail from 1200: lands in the second segment, crosses into the third, then waits.
        let mut r = Reader::new(&dir, 1200);
        let mut n = 0;
        while let Some(rec) = r.next().unwrap() {
            n += 1;
            assert_eq!(rec.height, 1200 + n);
            assert_eq!(rec.root, B256::repeat_byte(rec.height as u8));
            assert_eq!(rec.rows, row(rec.height));
        }
        assert_eq!(n, 1300);
        assert!(r.next().unwrap().is_none());
        // More appended later, including a duplicate height and a reopened writer (new segment).
        w.append(2500, B256::ZERO, &encode_rows(&row(2500))).unwrap();
        let mut w2 = Writer::open(&dir).unwrap();
        w2.append(2501, B256::ZERO, &encode_rows(&row(2501))).unwrap();
        assert_eq!(r.next().unwrap().unwrap().height, 2501);
        assert!(r.next().unwrap().is_none());
        assert_eq!(head(&dir, 0).unwrap(), 2501);

        // A gap is an error.
        w2.append(2503, B256::ZERO, &[]).unwrap();
        assert!(r.next().is_err());

        // Prune below 2100: the first two segments go (their blocks are all <= 2100).
        assert_eq!(prune(&dir, 2100).unwrap(), 2);
        assert_eq!(segments(&dir).unwrap().iter().map(|(f, _)| *f).collect::<Vec<_>>(), vec![2001, 2501]);
        let _ = std::fs::remove_dir_all(&dir);
    }
}

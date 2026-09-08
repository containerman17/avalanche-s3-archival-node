//! A View is overlays (newest first, may be empty) over runs, newest first;
//! `merge` writes its merged contents as a new run (the checkpoint).

use crate::overlay::Overlay;
use crate::run::{Run, Writer};
use crate::KvIter;
use std::io;
use std::path::Path;

pub struct View<'a> {
    overlays: Vec<&'a Overlay>,
    runs: Vec<&'a Run>,
}

impl<'a> View<'a> {
    pub fn new(overlay: Option<&'a Overlay>, runs: &[&'a Run]) -> View<'a> {
        View { overlays: overlay.into_iter().collect(), runs: runs.to_vec() }
    }

    /// Stacks several overlays (newest first) over runs: a fresh overlay over
    /// a frozen one being merged, over the base.
    pub fn multi(overlays: &[&'a Overlay], runs: &[&'a Run]) -> View<'a> {
        View { overlays: overlays.to_vec(), runs: runs.to_vec() }
    }

    /// Consults the overlays, then the runs in order. A tombstone or an empty
    /// value at any level ends the descent as not found.
    pub fn get(&self, key: &[u8]) -> Option<&'a [u8]> {
        for o in &self.overlays {
            if let Some(v) = o.get(key) {
                return if v.is_empty() { None } else { Some(v) };
            }
        }
        for r in &self.runs {
            if let Some(v) = r.get(key) {
                return if v.is_empty() { None } else { Some(v) };
            }
        }
        None
    }

    /// Merges all levels over [lo, hi), newest winning, tombstones and empty
    /// values dropped.
    pub fn iter(&self, lo: Option<&[u8]>, hi: Option<&'a [u8]>) -> MergeIter<'a> {
        let mut its: Vec<Box<dyn KvIter + 'a>> = Vec::with_capacity(self.overlays.len() + self.runs.len());
        for o in &self.overlays {
            its.push(Box::new(o.iter(lo, hi)));
        }
        for r in &self.runs {
            its.push(Box::new(r.iter(lo, hi)));
        }
        let live = its.iter_mut().map(|it| it.next()).collect();
        MergeIter { its, live, key: Vec::new(), val: Vec::new(), have: false }
    }
}

/// A k-way merge over a handful of levels. ponytail: linear scan of the
/// heads per step, a heap if k ever grows past a few runs.
pub struct MergeIter<'a> {
    its: Vec<Box<dyn KvIter + 'a>>,
    live: Vec<bool>,
    key: Vec<u8>,
    val: Vec<u8>,
    have: bool,
}

impl KvIter for MergeIter<'_> {
    fn next(&mut self) -> bool {
        loop {
            if self.have {
                for (i, it) in self.its.iter_mut().enumerate() {
                    if self.live[i] && it.key() == self.key.as_slice() {
                        self.live[i] = it.next();
                    }
                }
            }
            let mut best: Option<usize> = None;
            for (i, it) in self.its.iter().enumerate() {
                if self.live[i] && best.is_none_or(|b| it.key() < self.its[b].key()) {
                    best = Some(i);
                }
            }
            let Some(b) = best else {
                self.have = false;
                return false;
            };
            self.key.clear();
            self.key.extend_from_slice(self.its[b].key());
            self.val.clear();
            self.val.extend_from_slice(self.its[b].value());
            self.have = true;
            if !self.val.is_empty() {
                return true;
            }
        }
    }
    fn key(&self) -> &[u8] {
        &self.key
    }
    fn value(&self) -> &[u8] {
        &self.val
    }
}

/// Writes the view's merged contents as a new run at dst, streaming, and
/// opens it. `user` goes into the footer.
pub fn merge(dst: &Path, v: &View, user: [u8; 32]) -> io::Result<Run> {
    let res = (|| {
        let mut w = Writer::create(dst)?;
        w.set_user_data(user);
        let mut it = v.iter(None, None);
        while it.next() {
            w.add(it.key(), it.value())?;
        }
        w.close()
    })();
    if let Err(e) = res {
        let _ = std::fs::remove_file(dst);
        return Err(e);
    }
    Run::open(dst)
}

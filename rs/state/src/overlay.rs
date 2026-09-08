//! The writes since the last checkpoint: a byte slab behind a hash index of
//! slab offsets, so an entry costs its bytes plus one u64 per key. An empty
//! value is a tombstone: it shadows the runs below and `merge` drops the key.
//!
//! Unlike the Go Overlay there is no internal lock: `put` takes `&mut self`
//! and readers take `&self`; wrap it in a RwLock when writers and readers
//! run concurrently.
//!
//! A slab entry is [klen u8][vlen u8][vcap u8][key][value, padded to vcap];
//! a put of an existing key rewrites the value in place when it fits, else
//! appends a new entry and leaves the old one dead. ponytail: dead entries
//! are never reclaimed (a value outgrows its 16-byte slack rarely); compact
//! if the slab ever runs far past `bytes`.

use crate::run::{MAX_KEY, MAX_VALUE};
use crate::KvIter;
use hashbrown::HashTable;
use std::hash::BuildHasher;

/// The accounted per-entry cost on top of key+value bytes (Go's figure,
/// kept so `bytes()` means the same thing to a caller).
pub const ENTRY_OVERHEAD: usize = 64;

pub struct Overlay {
    table: HashTable<u64>,
    slab: Vec<u8>,
    bytes: usize,
    hasher: foldhash::fast::RandomState,
}

impl Default for Overlay {
    fn default() -> Self {
        Self::new()
    }
}

#[inline]
fn key_at(slab: &[u8], off: u64) -> &[u8] {
    let o = off as usize;
    &slab[o + 3..o + 3 + slab[o] as usize]
}

#[inline]
fn value_at(slab: &[u8], off: u64) -> &[u8] {
    let o = off as usize;
    let k = o + 3 + slab[o] as usize;
    &slab[k..k + slab[o + 1] as usize]
}

impl Overlay {
    pub fn new() -> Overlay {
        Overlay { table: HashTable::new(), slab: Vec::new(), bytes: 0, hasher: foldhash::fast::RandomState::default() }
    }

    fn find(&self, key: &[u8]) -> Option<u64> {
        let h = self.hasher.hash_one(key);
        self.table.find(h, |&off| key_at(&self.slab, off) == key).copied()
    }

    /// Copies key and value in. An empty value is a tombstone.
    pub fn put(&mut self, key: &[u8], value: &[u8]) {
        assert!(!key.is_empty() && key.len() <= MAX_KEY && value.len() <= MAX_VALUE, "latest: key {} bytes, value {} bytes", key.len(), value.len());
        let h = self.hasher.hash_one(key);
        let slot = self.table.find_mut(h, |&off| key_at(&self.slab, off) == key);
        let off = self.slab.len() as u64;
        match slot {
            Some(cur) => {
                let o = *cur as usize;
                self.bytes = self.bytes + value.len() - self.slab[o + 1] as usize;
                if value.len() <= self.slab[o + 2] as usize {
                    self.slab[o + 1] = value.len() as u8;
                    let v = o + 3 + key.len();
                    self.slab[v..v + value.len()].copy_from_slice(value);
                    return;
                }
                *cur = off;
            }
            None => {
                self.bytes += key.len() + value.len() + ENTRY_OVERHEAD;
                let slab = &self.slab;
                let hasher = &self.hasher;
                self.table.insert_unique(h, off, |&o| hasher.hash_one(key_at(slab, o)));
            }
        }
        let vcap = MAX_VALUE.min((value.len() + 15) & !15);
        self.slab.extend_from_slice(&[key.len() as u8, value.len() as u8, vcap as u8]);
        self.slab.extend_from_slice(key);
        self.slab.extend_from_slice(value);
        self.slab.resize(self.slab.len() + vcap - value.len(), 0);
    }

    /// The stored value; `Some(empty)` is a tombstone. The slice aliases the
    /// slab and stays valid until the next `put` of the same key.
    pub fn get(&self, key: &[u8]) -> Option<&[u8]> {
        self.find(key).map(|off| value_at(&self.slab, off))
    }

    pub fn len(&self) -> usize {
        self.table.len()
    }
    pub fn is_empty(&self) -> bool {
        self.table.is_empty()
    }

    /// The accounted size: key + value + ENTRY_OVERHEAD per entry.
    pub fn bytes(&self) -> usize {
        self.bytes
    }

    /// The real footprint: slab bytes plus the index.
    pub fn heap_bytes(&self) -> usize {
        self.slab.capacity() + self.table.capacity() * 9
    }

    /// Walks a sorted snapshot of the keys in [lo, hi) taken now; None means
    /// unbounded. Tombstones are included (empty values).
    pub fn iter<'a>(&'a self, lo: Option<&[u8]>, hi: Option<&[u8]>) -> OverlayIter<'a> {
        let mut offs: Vec<u64> = self
            .table
            .iter()
            .copied()
            .filter(|&off| {
                let k = key_at(&self.slab, off);
                lo.is_none_or(|lo| k >= lo) && hi.is_none_or(|hi| k < hi)
            })
            .collect();
        offs.sort_unstable_by(|&a, &b| key_at(&self.slab, a).cmp(key_at(&self.slab, b)));
        OverlayIter { slab: &self.slab, offs, i: usize::MAX }
    }
}

pub struct OverlayIter<'a> {
    slab: &'a [u8],
    offs: Vec<u64>,
    i: usize,
}

impl KvIter for OverlayIter<'_> {
    fn next(&mut self) -> bool {
        self.i = self.i.wrapping_add(1);
        self.i < self.offs.len()
    }
    fn key(&self) -> &[u8] {
        key_at(self.slab, self.offs[self.i])
    }
    fn value(&self) -> &[u8] {
        value_at(self.slab, self.offs[self.i])
    }
}

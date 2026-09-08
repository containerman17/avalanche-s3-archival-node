//! Rust port of epochdb's `latest` (front-coded sorted runs, overlay, view,
//! merge) and `commit` (Ethereum state root from the flat state: Roll, File,
//! Dirty) packages. The file formats are byte-compatible with the Go code.

pub mod keccak;
pub mod rlp;
pub mod run;
pub mod overlay;
pub mod view;
pub mod commit;
pub mod sample;

pub use keccak::Hash;

/// A sorted key/value cursor. `key` and `value` are valid until the next
/// `next`; values may alias a mapping or a slab.
pub trait KvIter {
    fn next(&mut self) -> bool;
    fn key(&self) -> &[u8];
    fn value(&self) -> &[u8];
}

/// Rows in memory, the test and bench shape.
pub struct RowsIter<'a> {
    rows: &'a [(Vec<u8>, Vec<u8>)],
    i: usize,
}

impl<'a> RowsIter<'a> {
    pub fn new(rows: &'a [(Vec<u8>, Vec<u8>)]) -> Self {
        RowsIter { rows, i: 0 }
    }
}

impl KvIter for RowsIter<'_> {
    fn next(&mut self) -> bool {
        self.i += 1;
        self.i <= self.rows.len()
    }
    fn key(&self) -> &[u8] {
        &self.rows[self.i - 1].0
    }
    fn value(&self) -> &[u8] {
        &self.rows[self.i - 1].1
    }
}

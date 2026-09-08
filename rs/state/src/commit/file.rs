//! An opened, mmapped node file written by `roll`.
//!
//! File layout (all integers little-endian):
//!
//! header: magic (8)
//! nodes:  tag (1: branch, 2: extension), uvarint blob length, blob,
//!         then 16 uvarint child offsets (branch) or 1 (extension);
//!         0 means the child is absent, embedded in the parent blob, or a leaf
//! index:  indexCount x {account hash 32, storage root 32, root offset 8},
//!         sorted by account hash, one entry per account whose storage root
//!         is an internal node
//! footer: magic 8, version 4, root offset 8, root hash 32, node count 8,
//!         key count 8, index offset 8, index count 8, user data 32, crc32 4

use super::{compact_to_hex, err, uvarint, Result, FOOTER_SIZE, INDEX_ENTRY, MAGIC, TAG_BRANCH, TAG_EXT, VERSION};
use crate::keccak::EMPTY_ROOT;
use crate::rlp;
use crate::Hash;
use memmap2::Mmap;
use std::path::Path;

#[derive(Clone, Copy, Debug)]
pub struct Footer {
    pub root_off: u64,
    pub root: Hash,
    pub nodes: u64,
    pub keys: u64,
    pub idx_off: u64,
    pub idx_n: u64,
    pub user: [u8; 32],
}

pub fn parse_footer(b: &[u8]) -> Result<Footer> {
    if b.len() != FOOTER_SIZE || &b[..8] != MAGIC || u32::from_le_bytes(b[8..12].try_into().unwrap()) != VERSION {
        return err("commit: not a commit node file or corrupted footer");
    }
    if crc32fast::hash(&b[..FOOTER_SIZE - 4]) != u32::from_le_bytes(b[FOOTER_SIZE - 4..].try_into().unwrap()) {
        return err("commit: not a commit node file or corrupted footer");
    }
    let u64at = |o: usize| u64::from_le_bytes(b[o..o + 8].try_into().unwrap());
    Ok(Footer {
        root_off: u64at(12),
        root: b[20..52].try_into().unwrap(),
        nodes: u64at(52),
        keys: u64at(60),
        idx_off: u64at(68),
        idx_n: u64at(76),
        user: b[84..116].try_into().unwrap(),
    })
}

pub struct File {
    m: Mmap,
    ft: Footer,
}

impl File {
    /// Mmaps a node file written by roll and checks its footer.
    pub fn open(path: &Path) -> Result<File> {
        let fd = std::fs::File::open(path)?;
        let size = fd.metadata()?.len();
        if size < (MAGIC.len() + FOOTER_SIZE) as u64 {
            return err(format!("commit: not a commit node file or corrupted footer: {}", path.display()));
        }
        let m = unsafe { Mmap::map(&fd)? };
        let ft = parse_footer(&m[size as usize - FOOTER_SIZE..]).map_err(|e| super::Error(format!("{e}: {}", path.display())))?;
        if &m[..8] != MAGIC || ft.root_off >= size || ft.idx_off < MAGIC.len() as u64 || ft.idx_off + ft.idx_n * INDEX_ENTRY as u64 != size - FOOTER_SIZE as u64 {
            return err(format!("commit: not a commit node file or corrupted footer: {}", path.display()));
        }
        Ok(File { m, ft })
    }

    /// The state root the file was rolled at.
    pub fn root(&self) -> Hash {
        self.ft.root
    }
    pub fn user_data(&self) -> [u8; 32] {
        self.ft.user
    }
    pub fn node_count(&self) -> u64 {
        self.ft.nodes
    }
    pub fn key_count(&self) -> u64 {
        self.ft.keys
    }
    pub fn size(&self) -> u64 {
        self.m.len() as u64
    }
    pub fn footer(&self) -> &Footer {
        &self.ft
    }

    /// The storage root of an account at the roll: EMPTY_ROOT when the
    /// account had no storage, and false when the root is a leaf (one slot)
    /// rather than an internal node, in which case the hash is unknown to
    /// the file and the caller derives it from the flat row.
    pub fn storage_root(&self, acct: &Hash) -> (Hash, bool) {
        match self.index(acct) {
            None => (EMPTY_ROOT, false),
            Some(e) => (e[32..64].try_into().unwrap(), true),
        }
    }

    fn index(&self, acct: &Hash) -> Option<&[u8]> {
        let idx = &self.m[self.ft.idx_off as usize..self.ft.idx_off as usize + self.ft.idx_n as usize * INDEX_ENTRY];
        let n = self.ft.idx_n as usize;
        let (mut lo, mut hi) = (0usize, n);
        while lo < hi {
            let m = (lo + hi) / 2;
            if &idx[m * INDEX_ENTRY..m * INDEX_ENTRY + 32] < &acct[..] {
                lo = m + 1;
            } else {
                hi = m;
            }
        }
        if lo == n || &idx[lo * INDEX_ENTRY..lo * INDEX_ENTRY + 32] != &acct[..] {
            return None;
        }
        Some(&idx[lo * INDEX_ENTRY..(lo + 1) * INDEX_ENTRY])
    }

    /// The RLP blob of the internal node at path (nibbles) in the trie of
    /// owner (the zero hash for the account trie, else the account hash).
    /// None when nothing is stored there: an absent, embedded, or leaf node.
    pub fn node(&self, owner: &Hash, mut path: &[u8]) -> Option<&[u8]> {
        let mut off = self.ft.root_off;
        if owner != &[0u8; 32] {
            let e = self.index(owner)?;
            off = u64::from_le_bytes(e[64..72].try_into().unwrap());
        }
        while off != 0 {
            let o = off as usize;
            let tag = self.m[o];
            let (n, k) = uvarint(&self.m[o + 1..]);
            let blob = &self.m[o + 1 + k..o + 1 + k + n as usize];
            let mut rest = &self.m[o + 1 + k + n as usize..];
            if path.is_empty() {
                return Some(blob);
            }
            match tag {
                TAG_BRANCH => {
                    for _ in 0..path[0] {
                        let (_, k) = uvarint(rest);
                        rest = &rest[k..];
                    }
                    off = uvarint(rest).0;
                    path = &path[1..];
                }
                TAG_EXT => {
                    let (content, _) = rlp::split_list(blob)?;
                    let (key, _) = rlp::split_string(content)?;
                    let nib = compact_to_hex(key);
                    if !path.starts_with(&nib) {
                        return None;
                    }
                    off = uvarint(rest).0;
                    path = &path[nib.len()..];
                }
                _ => return None,
            }
        }
        None
    }
}

//! Ethereum state roots (secure Merkle Patricia trie, geth semantics) from
//! a sorted flat latest state.
//!
//! `roll` writes the trie's INTERNAL nodes (branches and extensions) to one
//! immutable file in a single sequential pass; leaves are never stored, they
//! are the flat rows. Between rolls, `Dirty` recomputes the root from an
//! in-memory overlay of changed nodes over that file.
//!
//! Contract (shared with the latest store): keys are keccak(addr)+0x00 for
//! an account and keccak(addr)+0x01+keccak(slot) for a slot, sorted
//! ascending bytewise. An account value is RLP[nonce, balance, codeHash]
//! (no storage root: this module computes it); a slot value is the 32-byte
//! word with leading zeros trimmed. An empty value never reaches roll; in
//! Dirty::apply an empty value is a delete.

pub mod dirty;
pub mod file;
pub mod roll;
pub mod trie;

use crate::rlp;
use crate::Hash;
use std::fmt;

pub const MAGIC: &[u8; 8] = b"EPCHCMT1";
pub const VERSION: u32 = 1;
pub const TAG_BRANCH: u8 = 1;
pub const TAG_EXT: u8 = 2;
pub const INDEX_ENTRY: usize = 72;
pub const FOOTER_SIZE: usize = 8 + 4 + 8 + 32 + 8 + 8 + 8 + 8 + 32 + 4;

#[derive(Debug)]
pub struct Error(pub String);

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for Error {}
impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error(e.to_string())
    }
}

pub type Result<T> = std::result::Result<T, Error>;

pub fn err<T>(msg: impl Into<String>) -> Result<T> {
    Err(Error(msg.into()))
}

/// Hex-prefix decoding: compact key bytes to nibbles, terminator dropped.
pub fn compact_to_hex(compact: &[u8]) -> Vec<u8> {
    let mut nib = Vec::with_capacity(compact.len() * 2);
    for &b in compact {
        nib.push(b >> 4);
        nib.push(b & 15);
    }
    let chop = if nib[0] & 1 == 1 { 1 } else { 2 };
    nib.drain(..chop);
    nib
}

/// Appends the hex-prefix encoding of `nibbles`, terminator flag for a leaf.
pub fn put_compact(out: &mut Vec<u8>, nibbles: &[u8], leaf: bool) {
    let flag = if leaf { 0x20 } else { 0 };
    let (first, rest) = if nibbles.len() % 2 == 1 { (flag | 0x10 | nibbles[0], &nibbles[1..]) } else { (flag, nibbles) };
    out.push(first);
    for p in rest.chunks_exact(2) {
        out.push(p[0] << 4 | p[1]);
    }
}

pub fn hex_to_compact(nibbles: &[u8], leaf: bool) -> Vec<u8> {
    let mut out = Vec::with_capacity(nibbles.len() / 2 + 1);
    put_compact(&mut out, nibbles, leaf);
    out
}

/// The inverse of key_to_nibbles; an odd tail is padded with 0.
pub fn pack_nibbles(nib: &[u8]) -> Vec<u8> {
    let mut out = vec![0u8; nib.len().div_ceil(2)];
    for (i, &n) in nib.iter().enumerate() {
        out[i / 2] |= n << (4 * (1 - i % 2));
    }
    out
}

pub fn key_to_nibbles(key: &[u8]) -> Vec<u8> {
    let mut nib = Vec::with_capacity(key.len() * 2);
    for &b in key {
        nib.push(b >> 4);
        nib.push(b & 15);
    }
    nib
}

/// The account trie's leaf value RLP[nonce, balance, root, codeHash] built
/// from a contract row RLP[nonce, balance, codeHash] and the storage root.
pub fn account_leaf(row: &[u8], root: &Hash) -> Result<Vec<u8>> {
    let Some((content, _)) = rlp::split_list(row) else { return err("commit: account row is not a list") };
    let (Some((nonce, r)), ) = (rlp::split_string(content),) else { return err("commit: account row nonce") };
    let Some((bal, r)) = rlp::split_string(r) else { return err("commit: account row balance") };
    let Some((code, r)) = rlp::split_string(r) else { return err("commit: account row code hash") };
    // coreth (mainnet C) rows carry a 4th element, the IsMultiCoin bool, that
    // libevm appends to every account leaf as a 5th field; subnet-evm rows have none.
    let extra = if r.is_empty() { None } else { rlp::split_string(r).map(|(e, _)| e) };
    Ok(leaf_value(nonce, bal, root, code, extra))
}

pub fn leaf_value(nonce: &[u8], bal: &[u8], root: &Hash, code: &[u8], extra: Option<&[u8]>) -> Vec<u8> {
    let mut body = Vec::with_capacity(80);
    rlp::put_bytes(&mut body, nonce);
    rlp::put_bytes(&mut body, bal);
    rlp::put_bytes(&mut body, root);
    rlp::put_bytes(&mut body, code);
    if let Some(e) = extra {
        rlp::put_bytes(&mut body, e);
    }
    let mut out = Vec::with_capacity(body.len() + 2);
    rlp::put_header(&mut out, true, body.len());
    out.extend_from_slice(&body);
    out
}

/// The parsed fields of an account leaf RLP[nonce, balance, root, codeHash].
pub struct LeafFields {
    pub nonce: Vec<u8>,
    pub balance: Vec<u8>,
    pub root: Hash,
    pub code: Vec<u8>,
    /// The 5th leaf field when present (coreth's IsMultiCoin bool).
    pub extra: Option<Vec<u8>>,
}

pub fn parse_leaf(val: &[u8]) -> Result<LeafFields> {
    let Some((content, _)) = rlp::split_list(val) else { return err("commit: account leaf is not a list") };
    let Some((nonce, r)) = rlp::split_string(content) else { return err("commit: leaf nonce") };
    let Some((bal, r)) = rlp::split_string(r) else { return err("commit: leaf balance") };
    let Some((root, r)) = rlp::split_string(r) else { return err("commit: leaf root") };
    let Some((code, r)) = rlp::split_string(r) else { return err("commit: leaf code hash") };
    if root.len() != 32 {
        return err("commit: leaf root is not 32 bytes");
    }
    let extra = if r.is_empty() { None } else { rlp::split_string(r).map(|(e, _)| e.to_vec()) };
    Ok(LeafFields { nonce: nonce.to_vec(), balance: bal.to_vec(), root: root.try_into().unwrap(), code: code.to_vec(), extra })
}

/// Reads a Go binary.Uvarint: (value, bytes consumed).
pub fn uvarint(b: &[u8]) -> (u64, usize) {
    let mut x = 0u64;
    let mut s = 0;
    for (i, &c) in b.iter().enumerate() {
        if c < 0x80 {
            return (x | (c as u64) << s, i + 1);
        }
        x |= ((c & 0x7f) as u64) << s;
        s += 7;
    }
    (0, 0)
}

pub fn put_uvarint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push(v as u8 | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nibbles() {
        assert_eq!(key_to_nibbles(&[0xab, 0x01]), [0xa, 0xb, 0x0, 0x1]);
        assert_eq!(pack_nibbles(&[0xa, 0xb, 0x0]), [0xab, 0x00]);
        assert_eq!(hex_to_compact(&[1, 2, 3], true), [0x31, 0x23]);
        assert_eq!(hex_to_compact(&[1, 2], true), [0x20, 0x12]);
        assert_eq!(hex_to_compact(&[1, 2], false), [0x00, 0x12]);
        assert_eq!(hex_to_compact(&[1], false), [0x11]);
        assert_eq!(compact_to_hex(&[0x31, 0x23]), [1, 2, 3]);
        assert_eq!(compact_to_hex(&[0x00, 0x12]), [1, 2]);
        assert_eq!(compact_to_hex(&[0x11]), [1]);
        let mut v = vec![];
        put_uvarint(&mut v, 300);
        assert_eq!(v, [0xac, 0x02]);
        assert_eq!(uvarint(&v), (300, 2));
    }
}

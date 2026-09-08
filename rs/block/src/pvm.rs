//! The proposervm wrapper: avalanchego's vms/proposervm/block codec
//! (linearcodec: u16 codec version 0, u32 type id, fields in order; ints
//! big-endian, byte slices u32-length-prefixed, ids.ID 32 raw bytes).
//! Type 0 = statelessBlock, 1 = option, 2 = statelessGraniteBlock.
use bytes::Bytes;
use sha2::{Digest, Sha256};

use crate::Error;

#[derive(Debug, Clone)]
pub struct Pvm {
    pub parent_id: [u8; 32],
    pub timestamp: i64,
    pub pchain_height: u64,
    /// statelessGraniteBlock's Epoch.PChainHeight (the predicate context
    /// height under Granite); None before Granite.
    pub epoch_pchain_height: Option<u64>,
}

pub struct Unwrapped {
    /// The inner eth block RLP, exactly the list (trailing bytes stripped).
    pub inner: Bytes,
    /// The container id; None for a bare block (its id is the block hash).
    pub id: Option<[u8; 32]>,
    pub pvm: Option<Pvm>,
}

/// unwrap strips the proposervm wrapper, or trims a bare pre-fork block to
/// its RLP list (fetch/parse.go's rule).
pub fn unwrap(c: &Bytes) -> Result<Unwrapped, Error> {
    if let Some(u) = parse_pvm(c) {
        return Ok(u);
    }
    let mut p = &c[..];
    let h = alloy_rlp::Header::decode(&mut p)?;
    if !h.list {
        return Err("container is neither a proposervm block nor an RLP list".into());
    }
    Ok(Unwrapped { inner: c.slice(..h.length() + h.payload_length), id: None, pvm: None })
}

struct Reader<'a> {
    b: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        let s = self.b.get(self.pos..self.pos + n)?;
        self.pos += n;
        Some(s)
    }
    fn u16(&mut self) -> Option<u16> {
        self.take(2).map(|s| u16::from_be_bytes(s.try_into().unwrap()))
    }
    fn u32(&mut self) -> Option<u32> {
        self.take(4).map(|s| u32::from_be_bytes(s.try_into().unwrap()))
    }
    fn u64(&mut self) -> Option<u64> {
        self.take(8).map(|s| u64::from_be_bytes(s.try_into().unwrap()))
    }
    fn bytes(&mut self) -> Option<&'a [u8]> {
        let n = self.u32()? as usize;
        self.take(n)
    }
}

fn parse_pvm(c: &Bytes) -> Option<Unwrapped> {
    let mut r = Reader { b: c, pos: 0 };
    if r.u16()? != 0 {
        return None;
    }
    let ty = r.u32()?;
    match ty {
        0 | 2 => {
            let parent_id: [u8; 32] = r.take(32)?.try_into().unwrap();
            let timestamp = r.u64()? as i64;
            let pchain_height = r.u64()?;
            let _cert = r.bytes()?;
            let block = r.bytes()?;
            let mut epoch_pchain_height = None;
            if ty == 2 {
                epoch_pchain_height = Some(r.u64()?);
                r.u64()?;
                r.u64()?;
            }
            let sig = r.bytes()?;
            if r.pos != c.len() {
                return None;
            }
            // The id is sha256 of everything before the [u32 len][signature].
            let unsigned = c.len() - 4 - sig.len();
            Some(Unwrapped {
                inner: c.slice_ref(block),
                id: Some(Sha256::digest(&c[..unsigned]).into()),
                pvm: Some(Pvm { parent_id, timestamp, pchain_height, epoch_pchain_height }),
            })
        }
        1 => {
            let parent_id: [u8; 32] = r.take(32)?.try_into().unwrap();
            let block = r.bytes()?;
            if r.pos != c.len() {
                return None;
            }
            Some(Unwrapped {
                inner: c.slice_ref(block),
                id: Some(Sha256::digest(&c[..]).into()),
                pvm: Some(Pvm { parent_id, timestamp: 0, pchain_height: 0, epoch_pchain_height: None }),
            })
        }
        _ => None,
    }
}

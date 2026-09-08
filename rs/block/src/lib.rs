//! The block source of the Rust node: a container dump file
//! (`[u64 LE height][u32 LE len][container]`, heights ascending and
//! contiguous, written by cmd/epochdb-dump-containers) decoded into
//! subnet-evm blocks with recovered senders.
//!
//! Zero-copy end to end: the dump is mmapped and wrapped in one `Bytes`, and
//! every container, header RLP and raw tx is a slice of it.
pub mod dump;
pub mod eth;
pub mod pvm;
pub mod sender;

use std::path::Path;
use std::sync::mpsc;
use std::thread;

use alloy_primitives::{keccak256, B256};
use bytes::Bytes;
use rayon::prelude::*;

pub use dump::{Dump, Record, Records};
pub use eth::{AccessItem, Header, Tx};
pub use pvm::Pvm;
pub use sender::recover;

pub type Error = Box<dyn std::error::Error + Send + Sync>;

pub struct Block {
    pub height: u64,
    /// keccak(header_rlp), the eth block hash.
    pub hash: B256,
    /// What a peer names the container by: sha256 of the unsigned proposervm
    /// bytes, or the eth block hash for a bare pre-fork block.
    pub container_id: B256,
    pub header: Header,
    pub header_rlp: Bytes,
    pub txs: Vec<Tx>,
    /// The verbatim container (proposervm wrapper + inner block, or the bare
    /// block with its trailing bytes).
    pub container: Bytes,
    /// The proposervm wrapper's fields; None for a bare pre-fork block.
    pub pvm: Option<Pvm>,
}

/// decode parses one dump record into a Block, senders not recovered.
pub fn decode(r: Record) -> Result<Block, Error> {
    let b = decode_container(r.container).map_err(|e| format!("height {}: {e}", r.height))?;
    if b.height != r.height {
        return Err(format!("height {}: header says number {}", r.height, b.height).into());
    }
    Ok(b)
}

/// decode_container parses a container (or the bare inner block bytes a
/// plugin receives) into a Block, senders not recovered; the height is the
/// header's.
pub fn decode_container(container: Bytes) -> Result<Block, Error> {
    let u = pvm::unwrap(&container)?;
    let (header_rlp, txs) = eth::decode_block(&u.inner)?;
    let header = eth::decode_header(&header_rlp).map_err(|e| format!("header: {e}"))?;
    let hash = keccak256(&header_rlp);
    Ok(Block {
        height: header.number,
        hash,
        container_id: u.id.map(B256::from).unwrap_or(hash),
        header,
        header_rlp,
        txs,
        container,
        pvm: u.pvm,
    })
}

/// Blocks is the sequential decoder over a height window of a dump.
pub struct Blocks {
    recs: Records,
}

impl Blocks {
    /// open maps the dump and positions on `from`; `to` is inclusive
    /// (u64::MAX = to the end).
    pub fn open(path: impl AsRef<Path>, from: u64, to: u64) -> std::io::Result<Blocks> {
        Ok(Blocks { recs: Dump::open(path)?.records(from, to) })
    }
}

impl Iterator for Blocks {
    type Item = Result<Block, Error>;
    fn next(&mut self) -> Option<Self::Item> {
        self.recs.next().map(decode)
    }
}

/// CHUNK blocks per parallel batch; AHEAD batches buffered in front of the
/// consumer (the Go node's budget was 4,000 blocks).
const CHUNK: usize = 256;
const AHEAD: usize = 16;

/// recovered decodes and recovers senders on `workers` threads, running
/// ahead of the consumer, and yields the blocks in height order.
pub fn recovered(blocks: Blocks, workers: usize) -> Recovered {
    let workers = workers.max(1);
    let (tx, rx) = mpsc::sync_channel::<Vec<Result<Block, Error>>>(AHEAD);
    let mut recs = blocks.recs;
    thread::spawn(move || {
        let pool = rayon::ThreadPoolBuilder::new().num_threads(workers).build().expect("rayon pool");
        loop {
            let chunk: Vec<Record> = recs.by_ref().take(CHUNK).collect();
            if chunk.is_empty() {
                return;
            }
            let out: Vec<Result<Block, Error>> = pool.install(|| {
                chunk
                    .into_par_iter()
                    .map(|r| {
                        let mut b = decode(r)?;
                        for t in &mut b.txs {
                            t.sender = recover(t);
                        }
                        Ok(b)
                    })
                    .collect()
            });
            let failed = out.iter().any(|b| b.is_err());
            if tx.send(out).is_err() || failed {
                return;
            }
        }
    });
    Recovered { rx, cur: Vec::new().into_iter() }
}

pub struct Recovered {
    rx: mpsc::Receiver<Vec<Result<Block, Error>>>,
    cur: std::vec::IntoIter<Result<Block, Error>>,
}

impl Iterator for Recovered {
    type Item = Result<Block, Error>;
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(b) = self.cur.next() {
                return Some(b);
            }
            self.cur = self.rx.recv().ok()?.into_iter();
        }
    }
}

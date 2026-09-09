//! The engine's accepted-block store: rs/store's archival DB under
//! `<chainData>/store`. Every row family the executor produces is written,
//! so a record carries the per-tx rows (`BlockWrite::from_exec`, built by the
//! checker from the block and its exec result) beside the engine's write set
//! and receipts blob; recovery reads the write set back from the `ws/` row
//! and the code table from the `code/` rows.

use crate::tree::Id;
use bytes::Bytes;
use std::io;
use std::path::Path;
use std::sync::Arc;
use store::db::DB;
use store::window::{frame_ws, BlockWrite};

pub struct Record {
    pub height: u64,
    pub id: Id,
    pub container: Bytes,
    /// The EIP-2718 receipt list of the block (the rcb/ row).
    pub receipts: Vec<u8>,
    pub traces: Vec<String>,
    /// The state engine's ordered write set (contract keys).
    pub ws: Vec<(Vec<u8>, Vec<u8>)>,
    pub code: Vec<(alloy_primitives::B256, alloy_primitives::Bytes)>,
    /// The store's per-tx rows; None on a record read back.
    pub rows: Option<BlockWrite>,
}

/// The accepted-block interface the engine needs from a store.
pub trait BlockStore: Send {
    /// The last stored height, 0 when empty.
    fn head(&self) -> u64;
    fn height_of(&self, id: &Id) -> Option<u64>;
    fn id_at(&self, height: u64) -> Option<Id>;
    /// The container bytes at a height (what the plugin was handed).
    fn container(&self, height: u64) -> io::Result<Option<Bytes>>;
    /// The whole record (recovery replay reads the write set).
    fn read(&self, height: u64) -> io::Result<Option<Record>>;
    /// Append the next height's record (height must be head + 1).
    fn append(&mut self, r: Record) -> io::Result<()>;
    /// Make everything appended durable.
    fn sync(&self) -> io::Result<()>;
    /// Waits (bounded) for the store's background work (a seal, a merge) at shutdown.
    fn close(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub struct DbStore {
    pub db: Arc<DB>,
}

fn ioerr(e: anyhow::Error) -> io::Error {
    io::Error::other(format!("{e:#}"))
}

impl DbStore {
    /// Opens (creating) the store under `dir`; S3 from EPOCHDB_S3_* if set.
    /// `close_grace` bounds how long `close` waits for a seal or merge.
    pub fn open(dir: &Path, chain_root: [u8; 32], close_grace: std::time::Duration) -> anyhow::Result<DbStore> {
        let cas = store::casfs::Store::open(dir)?;
        let mut db = DB::open(dir, cas, chain_root)?;
        db.close_grace = close_grace;
        Ok(DbStore { db: Arc::new(db) })
    }
}

impl BlockStore for DbStore {
    fn head(&self) -> u64 {
        self.db.head().unwrap_or(0)
    }
    fn height_of(&self, id: &Id) -> Option<u64> {
        self.db.height_by_hash(id).ok().flatten()
    }
    fn id_at(&self, height: u64) -> Option<Id> {
        self.db.header_rlp(height).ok().flatten().map(|h| state::keccak::keccak256(&h))
    }
    fn container(&self, height: u64) -> io::Result<Option<Bytes>> {
        Ok(self.db.container_at(height).map_err(ioerr)?.map(Bytes::from))
    }
    fn read(&self, height: u64) -> io::Result<Option<Record>> {
        let Some(container) = self.container(height)? else { return Ok(None) };
        let id = self.id_at(height).ok_or_else(|| io::Error::other(format!("block {height}: no header")))?;
        let receipts = self.db.receipts_blob(height).map_err(ioerr)?.unwrap_or_default();
        let traces = self
            .db
            .block_txs(height)
            .map_err(ioerr)?
            .unwrap_or_default()
            .into_iter()
            .map(|(_, _, t)| String::from_utf8_lossy(&t).into_owned())
            .collect();
        let (ws, hashes) = self.db.write_set(height).map_err(ioerr)?.unwrap_or_default();
        let mut code = Vec::with_capacity(hashes.len());
        for h in hashes {
            let blob = self.db.code(&h).map_err(ioerr)?.ok_or_else(|| io::Error::other(format!("block {height}: code {} missing", hex::encode(h))))?;
            code.push((alloy_primitives::B256::from(h), alloy_primitives::Bytes::from(blob)));
        }
        Ok(Some(Record { height, id, container, receipts, traces, ws, code, rows: None }))
    }
    fn append(&mut self, r: Record) -> io::Result<()> {
        let Some(mut bw) = r.rows.filter(|b| b.height == r.height) else {
            return Err(io::Error::other(format!("block {}: the record carries no BlockWrite for it", r.height)));
        };
        if bw.header_rlp.is_empty() || state::keccak::keccak256(&bw.header_rlp) != r.id {
            return Err(io::Error::other(format!("block {}: the BlockWrite is not this record's block", r.height)));
        }
        bw.receipts_blob = r.receipts;
        let hashes: Vec<[u8; 32]> = r.code.iter().map(|(h, _)| h.0).collect();
        bw.ws = frame_ws(&r.ws, &hashes);
        for (h, c) in &r.code {
            if !bw.code.iter().any(|(x, _)| x == &h.0) {
                bw.code.push((h.0, c.to_vec()));
            }
        }
        self.db.write_block(&bw).map_err(ioerr)
    }
    fn sync(&self) -> io::Result<()> {
        self.db.sync().map_err(ioerr)
    }
    fn close(&mut self) -> io::Result<()> {
        self.db.close().map_err(ioerr)
    }
}

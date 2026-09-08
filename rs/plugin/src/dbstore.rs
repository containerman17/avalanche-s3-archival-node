//! `BlockStore` over rs/store's DB: the archival store in place of the
//! interim blocks.log. Every row family the executor produces is written,
//! so the store must see the per-tx rows: the engine stages a
//! `store::window::BlockWrite` (built with `BlockWrite::from_exec` at verify
//! time, when the exec result is at hand) and `append(&Record)` attaches the
//! record's receipts blob and write set to it. An append with nothing staged
//! for that height is refused rather than stored without state rows.

use crate::log::{BlockStore, Record};
use crate::tree::Id;
use bytes::Bytes;
use std::io;
use std::path::Path;
use store::db::DB;
use store::window::{frame_ws, BlockWrite};

pub struct DbStore {
    pub db: DB,
    staged: Option<BlockWrite>,
}

fn ioerr(e: anyhow::Error) -> io::Error {
    io::Error::other(e.to_string())
}

impl DbStore {
    /// Opens (creating) the store under `dir`; S3 from EPOCHDB_S3_* if set.
    pub fn open(dir: &Path, chain_root: [u8; 32]) -> anyhow::Result<DbStore> {
        let cas = store::casfs::Store::open(dir)?;
        Ok(DbStore { db: DB::open(dir, cas, chain_root)?, staged: None })
    }
    /// The per-tx rows of the next block to append (BlockWrite::from_exec).
    pub fn stage(&mut self, bw: BlockWrite) {
        self.staged = Some(bw);
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
        Ok(Some(Record { height, id, container, receipts, traces, ws, code }))
    }
    fn append(&mut self, r: &Record) -> io::Result<()> {
        let Some(mut bw) = self.staged.take().filter(|b| b.height == r.height) else {
            return Err(io::Error::other(format!("block {}: no BlockWrite staged for it (DbStore::stage(BlockWrite::from_exec(..)) before append)", r.height)));
        };
        if bw.header_rlp.is_empty() || state::keccak::keccak256(&bw.header_rlp) != r.id {
            return Err(io::Error::other(format!("block {}: the staged BlockWrite is not this record's block", r.height)));
        }
        bw.receipts_blob = r.receipts.clone();
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
        // ponytail: the window log is flushed at every block end and fsynced
        // by DB::sync, which needs &mut; the trait's &self sync is a no-op
        // until the plugin calls DbStore::db.sync() on its fsync cadence.
        Ok(())
    }
}

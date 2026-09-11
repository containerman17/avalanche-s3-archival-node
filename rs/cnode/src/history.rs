//! The history log: one redb file, three tables keyed by height. Blocks as
//! the RPC JSON coreth returned (transactions and blockExtraData included,
//! so a replay needs no network) with the receive time; state diffs as
//! contract rows; the mempool capture of the interval after the block.

use anyhow::Result;
use redb::{Database, ReadableTable, TableDefinition};
use std::path::Path;

const BLOCKS: TableDefinition<u64, &[u8]> = TableDefinition::new("blocks");
const DIFFS: TableDefinition<u64, &[u8]> = TableDefinition::new("diffs");
const MEMPOOL: TableDefinition<u64, &[u8]> = TableDefinition::new("mempool");

pub struct History {
    db: Database,
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct StoredBlock {
    pub received_ms: u64,
    pub applied_ms: u64,
    pub block: serde_json::Value,
}

pub fn encode_rows(rows: &[(Vec<u8>, Vec<u8>)]) -> Vec<u8> {
    let mut out = Vec::with_capacity(rows.iter().map(|(k, v)| 5 + k.len() + v.len()).sum());
    for (k, v) in rows {
        out.push(k.len() as u8);
        out.extend_from_slice(&(v.len() as u32).to_le_bytes());
        out.extend_from_slice(k);
        out.extend_from_slice(v);
    }
    out
}

pub fn decode_rows(mut b: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut out = Vec::new();
    while !b.is_empty() {
        let kl = b[0] as usize;
        let vl = u32::from_le_bytes(b[1..5].try_into().unwrap()) as usize;
        out.push((b[5..5 + kl].to_vec(), b[5 + kl..5 + kl + vl].to_vec()));
        b = &b[5 + kl + vl..];
    }
    out
}

/// Rows back into a hot-state diff (a restart replay). Account rows are
/// applied before slot rows, as `hot::rows` emitted them.
pub fn diff_from_rows(rows: &[(Vec<u8>, Vec<u8>)]) -> anyhow::Result<crate::hot::Diff> {
    use alloy_primitives::U256;
    let mut d = crate::hot::Diff::default();
    for (k, v) in rows {
        match k.len() {
            33 => d.accounts.push((k[..32].try_into().unwrap(), if v.is_empty() { None } else { Some(crate::import::parse_account_row(v)?) })),
            65 => d.storage.push((k[..32].try_into().unwrap(), k[33..65].try_into().unwrap(), U256::from_be_slice(v))),
            n => anyhow::bail!("history row with a {n}-byte key"),
        }
    }
    Ok(d)
}

impl History {
    pub fn open(path: &Path) -> Result<History> {
        let db = Database::create(path)?;
        let w = db.begin_write()?;
        w.open_table(BLOCKS)?;
        w.open_table(DIFFS)?;
        w.open_table(MEMPOOL)?;
        w.commit()?;
        Ok(History { db })
    }

    /// One block: its JSON with timestamps and its rows, in one transaction.
    pub fn put(&self, height: u64, block: &StoredBlock, rows: &[(Vec<u8>, Vec<u8>)]) -> Result<()> {
        let w = self.db.begin_write()?;
        w.open_table(BLOCKS)?.insert(height, serde_json::to_vec(block)?.as_slice())?;
        w.open_table(DIFFS)?.insert(height, encode_rows(rows).as_slice())?;
        w.commit()?;
        Ok(())
    }

    pub fn put_mempool(&self, height: u64, bytes: &[u8]) -> Result<()> {
        let w = self.db.begin_write()?;
        w.open_table(MEMPOOL)?.insert(height, bytes)?;
        w.commit()?;
        Ok(())
    }

    pub fn block(&self, height: u64) -> Result<Option<StoredBlock>> {
        let r = self.db.begin_read()?;
        let t = r.open_table(BLOCKS)?;
        Ok(match t.get(height)? {
            Some(v) => Some(serde_json::from_slice(v.value())?),
            None => None,
        })
    }

    pub fn diff(&self, height: u64) -> Result<Option<Vec<(Vec<u8>, Vec<u8>)>>> {
        let r = self.db.begin_read()?;
        let t = r.open_table(DIFFS)?;
        Ok(t.get(height)?.map(|v| decode_rows(v.value())))
    }

    pub fn mempool(&self, height: u64) -> Result<Option<Vec<u8>>> {
        let r = self.db.begin_read()?;
        let t = r.open_table(MEMPOOL)?;
        Ok(t.get(height)?.map(|v| v.value().to_vec()))
    }

    /// Highest block height stored.
    pub fn head(&self) -> Result<Option<u64>> {
        let r = self.db.begin_read()?;
        let t = r.open_table(BLOCKS)?;
        let last = t.last()?.map(|(k, _)| k.value());
        Ok(last)
    }
}

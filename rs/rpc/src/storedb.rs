//! `Store` over rs/store's `DB` (the v4 store): blocks reassembled from the
//! rows, receipts from rcpt/, frames from itx/, state by descent at the
//! block's boundary TxNum, logs candidates and postings from the lookup
//! section. One mutex around the DB.
// ponytail: one global lock over the DB; a reader snapshot the writer does not
// block is the store's open item.
use std::sync::{Arc, Mutex};

use alloy_primitives::{Address, Bytes, B256, U256};
use block::Block;
use store::db::DB;
use store::format::*;

use crate::{Account, BlockCache, Log, Receipt, Result, StateRead, Store};

pub struct StoreDb {
    pub db: Mutex<DB>,
    pub cfg: Arc<exec::Config>,
    cache: BlockCache,
}

impl StoreDb {
    pub fn new(db: DB, cfg: Arc<exec::Config>) -> StoreDb {
        StoreDb { db: Mutex::new(db), cfg, cache: BlockCache::new(512) }
    }

    /// The TxNum ceiling of the state after block h (its boundary slot).
    fn ceiling(&self, h: u64) -> Result<Option<u64>> {
        self.db.lock().unwrap().txnum_at_end_of(h)
    }
}

struct State<'a> {
    s: &'a StoreDb,
    /// None = the genesis state (nothing written yet).
    at: Option<u64>,
}

impl StateRead for State<'_> {
    fn account(&mut self, a: Address) -> Result<Option<Account>> {
        if let Some(at) = self.at {
            if let Some(v) = self.s.db.lock().unwrap().account_at(a.as_slice(), at)? {
                if v.is_empty() {
                    return Ok(None);
                }
                let mut p = &v[..];
                let (nonce, balance, code_hash) = (|| -> alloy_rlp::Result<(u64, U256, B256)> {
                    let h = alloy_rlp::Header::decode(&mut p)?;
                    if !h.list {
                        return Err(alloy_rlp::Error::UnexpectedString);
                    }
                    Ok((<u64 as alloy_rlp::Decodable>::decode(&mut p)?, <U256 as alloy_rlp::Decodable>::decode(&mut p)?, <B256 as alloy_rlp::Decodable>::decode(&mut p)?))
                })()
                .map_err(|e| anyhow::anyhow!("account rlp of {a}: {e}"))?;
                return Ok(Some(Account { nonce, balance, code_hash }));
            }
        }
        Ok(self.s.cfg.alloc.get(&a).map(|g| Account { nonce: g.nonce, balance: g.balance, code_hash: if g.code.is_empty() { alloy_primitives::KECCAK256_EMPTY } else { alloy_primitives::keccak256(&g.code) } }))
    }
    fn storage(&mut self, a: Address, slot: U256) -> Result<U256> {
        let key = B256::from(slot);
        if let Some(at) = self.at {
            if let Some(v) = self.s.db.lock().unwrap().storage_at(a.as_slice(), key.as_slice(), at)? {
                return Ok(U256::from_be_slice(&v));
            }
        }
        Ok(self.s.cfg.alloc.get(&a).and_then(|g| g.storage.get(&key)).map(|v| U256::from_be_bytes(v.0)).unwrap_or_default())
    }
    fn code(&mut self, h: B256) -> Result<Option<Bytes>> {
        if let Some(c) = self.s.db.lock().unwrap().code(h.as_slice())? {
            return Ok(Some(c.into()));
        }
        Ok(self.s.cfg.alloc.values().find(|g| !g.code.is_empty() && alloy_primitives::keccak256(&g.code) == h).map(|g| g.code.clone()))
    }
}

impl Store for StoreDb {
    fn head(&self) -> u64 {
        self.db.lock().unwrap().head().unwrap_or(0)
    }
    fn block(&self, h: u64) -> Result<Option<Arc<Block>>> {
        self.cache.get_or(h, || {
            let Some(c) = self.db.lock().unwrap().container_at(h)? else { return Ok(None) };
            Ok(Some(block::decode_container(c.into()).map_err(|e| anyhow::anyhow!("block {h}: {e}"))?))
        })
    }
    fn hash_at(&self, h: u64) -> Result<Option<B256>> {
        Ok(self.db.lock().unwrap().header_rlp(h)?.map(|r| alloy_primitives::keccak256(&r)))
    }
    fn height_by_hash(&self, hash: &B256) -> Result<Option<u64>> {
        self.db.lock().unwrap().height_by_hash(hash.as_slice())
    }
    fn tx_by_hash(&self, hash: &B256) -> Result<Option<(u64, usize)>> {
        let db = self.db.lock().unwrap();
        let Some(n) = db.txnum_by_hash(hash.as_slice())? else { return Ok(None) };
        let Some(h) = db.height_of_tx(n)? else { anyhow::bail!("tx {hash} is stored at TxNum {n}, which is in no block") };
        let (first, count) = db.block_tx_range(h)?.ok_or_else(|| anyhow::anyhow!("block {h} has no blk row"))?;
        if n < first || n >= first + count as u64 {
            anyhow::bail!("tx {hash} at TxNum {n} is outside block {h}'s range");
        }
        Ok(Some((h, (n - first) as usize)))
    }
    fn receipts(&self, h: u64) -> Result<Option<Vec<Receipt>>> {
        let db = self.db.lock().unwrap();
        let Some((first, count)) = db.block_tx_range(h)? else { return Ok(None) };
        let mut out = Vec::with_capacity(count as usize);
        for i in 0..count as u64 {
            let raw = db.receipt(first + i)?.ok_or_else(|| anyhow::anyhow!("tx {} has no stored receipt: this node never executed it", first + i))?;
            let r = store::receipts::decode(&raw)?;
            out.push(Receipt {
                status: r.status,
                gas_used: r.gas_used,
                cumulative_gas_used: r.cumulative_gas_used,
                logs: r.logs.into_iter().map(|l| Log { address: Address::from(l.address), topics: l.topics.into_iter().map(B256::from).collect(), data: l.data.into() }).collect(),
            });
        }
        Ok(Some(out))
    }
    fn traces(&self, h: u64) -> Result<Option<Vec<String>>> {
        let db = self.db.lock().unwrap();
        let Some((first, count)) = db.block_tx_range(h)? else { return Ok(None) };
        let mut out = Vec::with_capacity(count as usize);
        for i in 0..count as u64 {
            let raw = db.frames(first + i)?.ok_or_else(|| anyhow::anyhow!("tx {} has no stored frames", first + i))?;
            out.push(String::from_utf8(raw)?);
        }
        Ok(Some(out))
    }
    fn container(&self, h: u64) -> Result<Option<Bytes>> {
        Ok(self.db.lock().unwrap().container_at(h)?.map(Bytes::from))
    }
    fn state_at(&self, h: u64) -> Result<Box<dyn StateRead + '_>> {
        let at = if h == 0 { None } else { Some(self.ceiling(h)?.ok_or_else(|| anyhow::anyhow!("block {h} is not stored"))?) };
        Ok(Box::new(State { s: self, at }))
    }
    /// rpc/logs.go logCandidates: per dimension a TxNum set from the
    /// postings, intersected, mapped back to heights.
    fn log_candidates(&self, from: u64, to: u64, addrs: &[Address], topics: &[Vec<B256>]) -> Result<Option<Vec<u64>>> {
        let db = self.db.lock().unwrap();
        let Some((lo, _)) = db.block_tx_range(from)? else { anyhow::bail!("tx range of block {from}: not stored") };
        let Some((first, count)) = db.block_tx_range(to)? else { anyhow::bail!("tx range of block {to}: not stored") };
        let hi = if count == 0 { if first == 0 { return Ok(Some(Vec::new())) } else { first - 1 } } else { first + count as u64 - 1 };
        let topic0 = if topics.first().is_some_and(|t| t.len() == 1) { Some(topics[0][0]) } else { None };
        let mut sets: Vec<std::collections::HashSet<u64>> = Vec::new();
        let collect = |prefix: &[u8], mask: u8, set: &mut std::collections::HashSet<u64>| -> Result<()> {
            db.postings(prefix, lo, hi, |_, n, p| {
                if mask == 0 || p & mask != 0 {
                    set.insert(n);
                }
                true
            })
        };
        if !addrs.is_empty() {
            let mut set = Default::default();
            for a in addrs {
                let prefix = match topic0 {
                    Some(t0) => elog_group(a.as_slice(), t0.as_slice()),
                    None => elog_prefix(a.as_slice()),
                };
                collect(&prefix, 0, &mut set)?;
            }
            sets.push(set);
        }
        for (i, want) in topics.iter().enumerate() {
            if want.is_empty() || i > 3 {
                continue;
            }
            let mut set = Default::default();
            for t in want {
                let (prefix, mask) = if i == 0 {
                    (sig_group(t.as_slice()), 0)
                } else {
                    (match topic0 {
                        Some(t0) => tval_group(t.as_slice(), t0.as_slice()),
                        None => tval_prefix(t.as_slice()),
                    }, 1u8 << (i - 1))
                };
                collect(&prefix, mask, &mut set)?;
            }
            sets.push(set);
        }
        if sets.is_empty() {
            return Ok(None);
        }
        let smallest = sets.iter().min_by_key(|s| s.len()).unwrap();
        let mut heights = std::collections::BTreeSet::new();
        for n in smallest {
            if sets.iter().all(|s| s.contains(n)) {
                if let Some(h) = db.height_of_tx(*n)? {
                    if h >= from && h <= to {
                        heights.insert(h);
                    }
                }
            }
        }
        Ok(Some(heights.into_iter().collect()))
    }
    fn tx_range(&self, h: u64) -> Result<Option<(u64, u32)>> {
        self.db.lock().unwrap().block_tx_range(h)
    }
    fn height_of_tx(&self, txnum: u64) -> Result<Option<u64>> {
        self.db.lock().unwrap().height_of_tx(txnum)
    }
    fn next_tx(&self) -> u64 {
        self.db.lock().unwrap().next_tx()
    }
    fn postings(&self, prefix: &[u8], lo: u64, hi: u64, desc: bool, f: &mut dyn FnMut(&[u8], u64, u8) -> bool) -> Result<()> {
        let db = self.db.lock().unwrap();
        if !desc {
            return db.postings(prefix, lo, hi, |g, n, p| f(g, n, p));
        }
        // ponytail: descending = ascending collected and reversed; a reverse
        // chunk cursor when a hot address makes this scan matter.
        let mut all = Vec::new();
        db.postings(prefix, lo, hi, |g, n, p| {
            all.push((g.to_vec(), n, p));
            true
        })?;
        for (g, n, p) in all.into_iter().rev() {
            if !f(&g, n, p) {
                break;
            }
        }
        Ok(())
    }
    fn groups(&self, prefix: &[u8], f: &mut dyn FnMut(&[u8]) -> bool) -> Result<()> {
        self.db.lock().unwrap().groups(prefix, |g| f(g))
    }
    fn set_scan(&self, prefix: &[u8], f: &mut dyn FnMut(&[u8]) -> bool) -> Result<()> {
        self.db.lock().unwrap().set_scan(prefix, |g| f(g))
    }
}

//! INTERIM `rpc::Store` over the engine: blocks from the head window and
//! blocks.log, receipts and callTracer frames from the log records, a
//! tx-hash index built at open and kept at accept, state at the head only
//! (the flat state has no history; rs/store's descent serves the rest).
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use alloy_eips::eip2718::Decodable2718;
use alloy_primitives::{Address, Bytes, B256, U256};
use block::Block;
use revm::Database;
use rpc::{Account, BlockCache, Log, Receipt, Result, StateRead, Store};

use crate::log::BlockStore;
use crate::node_engine::Inner;
use crate::tree::Id;

pub struct PluginStore {
    pub genesis: Arc<Block>,
    pub head: Arc<Mutex<Arc<Block>>>,
    pub inner: Arc<Mutex<Inner>>,
    pub log: Arc<Mutex<Box<dyn BlockStore>>>,
    pub recent: Arc<Mutex<HashMap<Id, Arc<Block>>>>,
    /// tx hash -> (height, index).
    pub txs: Mutex<HashMap<B256, (u64, u32)>>,
    cache: BlockCache,
}

impl PluginStore {
    pub fn new(genesis: Arc<Block>, head: Arc<Mutex<Arc<Block>>>, inner: Arc<Mutex<Inner>>, log: Arc<Mutex<Box<dyn BlockStore>>>, recent: Arc<Mutex<HashMap<Id, Arc<Block>>>>) -> PluginStore {
        PluginStore { genesis, head, inner, log, recent, txs: Mutex::new(HashMap::new()), cache: BlockCache::new(256) }
    }

    pub fn index_block(&self, b: &Block) {
        let mut g = self.txs.lock().unwrap();
        for (i, t) in b.txs.iter().enumerate() {
            g.insert(t.hash, (b.height, i as u32));
        }
    }

    /// The tx index over every logged block (one decode per block).
    pub fn build_index(&self) -> Result<usize> {
        let head = self.log.lock().unwrap().head();
        let mut n = 0;
        for h in 1..=head {
            let c = self.log.lock().unwrap().container(h)?.ok_or_else(|| anyhow::anyhow!("blocks.log holds no block {h}"))?;
            let b = block::decode_container(c).map_err(|e| anyhow::anyhow!("block {h}: {e}"))?;
            let mut g = self.txs.lock().unwrap();
            for (i, t) in b.txs.iter().enumerate() {
                g.insert(t.hash, (h, i as u32));
            }
            n += b.txs.len();
        }
        Ok(n)
    }

    /// A block the engine still holds in memory (senders recovered).
    fn live_block(&self, h: u64) -> Option<Arc<Block>> {
        if h == 0 {
            return Some(self.genesis.clone());
        }
        let head = self.head.lock().unwrap().clone();
        if h == head.height {
            return Some(head);
        }
        self.recent.lock().unwrap().values().find(|b| b.height == h).cloned()
    }

    fn logged_block(&self, h: u64) -> Result<Option<Block>> {
        let Some(c) = self.log.lock().unwrap().container(h)? else { return Ok(None) };
        Ok(Some(block::decode_container(c).map_err(|e| anyhow::anyhow!("block {h}: {e}"))?))
    }
}

struct HeadState<'a>(&'a PluginStore);

impl StateRead for HeadState<'_> {
    fn account(&mut self, a: Address) -> Result<Option<Account>> {
        let mut g = self.0.inner.lock().unwrap();
        Ok(g.ex.db_mut().backend.basic(a).unwrap().map(|i| Account { nonce: i.nonce, balance: i.balance, code_hash: i.code_hash }))
    }
    fn storage(&mut self, a: Address, slot: U256) -> Result<U256> {
        let mut g = self.0.inner.lock().unwrap();
        Ok(g.ex.db_mut().backend.storage(a, slot).unwrap())
    }
    fn code(&mut self, h: B256) -> Result<Option<Bytes>> {
        let mut g = self.0.inner.lock().unwrap();
        Ok(g.ex.db_mut().backend.code.get(&h).map(|c| c.original_bytes()))
    }
}

fn not_indexed() -> anyhow::Error {
    anyhow::anyhow!("not available: the interim block log has no postings index (rs/store serves it)")
}

impl Store for PluginStore {
    fn head(&self) -> u64 {
        self.head.lock().unwrap().height
    }
    fn block(&self, h: u64) -> Result<Option<Arc<Block>>> {
        if let Some(b) = self.live_block(h) {
            return Ok(Some(b));
        }
        self.cache.get_or(h, || self.logged_block(h))
    }
    fn hash_at(&self, h: u64) -> Result<Option<B256>> {
        if h == 0 {
            return Ok(Some(self.genesis.hash));
        }
        let head = self.head.lock().unwrap().clone();
        if h > head.height {
            return Ok(None);
        }
        if h == head.height {
            return Ok(Some(head.hash));
        }
        if let Some(id) = self.log.lock().unwrap().id_at(h) {
            return Ok(Some(B256::from(id)));
        }
        Ok(self.recent.lock().unwrap().values().find(|b| b.height == h).map(|b| b.hash))
    }
    fn height_by_hash(&self, hash: &B256) -> Result<Option<u64>> {
        if *hash == self.genesis.hash {
            return Ok(Some(0));
        }
        if let Some(b) = self.recent.lock().unwrap().get(&hash.0) {
            return Ok(Some(b.height));
        }
        let head = self.head.lock().unwrap().clone();
        if head.hash == *hash {
            return Ok(Some(head.height));
        }
        Ok(self.log.lock().unwrap().height_of(&hash.0))
    }
    fn tx_by_hash(&self, hash: &B256) -> Result<Option<(u64, usize)>> {
        Ok(self.txs.lock().unwrap().get(hash).map(|(h, i)| (*h, *i as usize)))
    }
    fn receipts(&self, h: u64) -> Result<Option<Vec<Receipt>>> {
        let Some(r) = self.log.lock().unwrap().read(h)? else { return Ok(None) };
        let mut p = &r.receipts[..];
        let mut out = Vec::new();
        while !p.is_empty() {
            let env = alloy_consensus::ReceiptEnvelope::decode_2718(&mut p).map_err(|e| anyhow::anyhow!("block {h} receipt {}: {e}", out.len()))?;
            let rc = env.as_receipt().ok_or_else(|| anyhow::anyhow!("receipt without a body"))?;
            out.push(Receipt {
                status: rc.status.coerce_status() as u64,
                gas_used: 0,
                cumulative_gas_used: rc.cumulative_gas_used,
                logs: rc.logs.iter().map(|l| Log { address: l.address, topics: l.data.topics().to_vec(), data: l.data.data.clone() }).collect(),
            });
        }
        let mut prev = 0;
        for r in &mut out {
            r.gas_used = r.cumulative_gas_used - prev;
            prev = r.cumulative_gas_used;
        }
        Ok(Some(out))
    }
    fn traces(&self, h: u64) -> Result<Option<Vec<String>>> {
        Ok(self.log.lock().unwrap().read(h)?.map(|r| r.traces))
    }
    fn container(&self, h: u64) -> Result<Option<Bytes>> {
        if let Some(b) = self.live_block(h) {
            return Ok(Some(b.container.clone().into()));
        }
        Ok(self.log.lock().unwrap().container(h)?.map(Into::into))
    }
    fn state_at(&self, h: u64) -> Result<Box<dyn StateRead + '_>> {
        let head = self.head();
        if h != head {
            anyhow::bail!("historical state is not available (head {head}, asked {h})");
        }
        Ok(Box::new(HeadState(self)))
    }
    fn log_candidates(&self, _from: u64, _to: u64, _addrs: &[Address], _topics: &[Vec<B256>]) -> Result<Option<Vec<u64>>> {
        Ok(None)
    }
    fn tx_range(&self, _h: u64) -> Result<Option<(u64, u32)>> {
        Err(not_indexed())
    }
    fn height_of_tx(&self, _txnum: u64) -> Result<Option<u64>> {
        Err(not_indexed())
    }
    fn next_tx(&self) -> u64 {
        self.txs.lock().unwrap().len() as u64
    }
    fn postings(&self, _prefix: &[u8], _lo: u64, _hi: u64, _desc: bool, _f: &mut dyn FnMut(&[u8], u64, u8) -> bool) -> Result<()> {
        Err(not_indexed())
    }
    fn groups(&self, _prefix: &[u8], _f: &mut dyn FnMut(&[u8]) -> bool) -> Result<()> {
        Err(not_indexed())
    }
    fn set_scan(&self, _prefix: &[u8], _f: &mut dyn FnMut(&[u8]) -> bool) -> Result<()> {
        Err(not_indexed())
    }
}

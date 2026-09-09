//! `rpc::Store` over the engine: rs/rpc's `StoreDb` on the writer's own
//! `Arc<DB>` (blocks, receipts, frames, postings, state history by descent)
//! plus what the store does not hold yet: the accepted head and the blocks
//! the checker has not appended (at most CHECK_DEPTH), and the head state
//! read from the executor's backend, so `latest` is the accepted head.
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use alloy_primitives::{Address, Bytes, B256, U256};
use block::Block;
use rpc::storedb::StoreDb;
use rpc::{Account, Receipt, Result, StateRead, Store};
use store::db::DB;

use crate::node_engine::Inner;
use crate::tree::Id;

pub struct PluginStore {
    pub genesis: Arc<Block>,
    pub head: Arc<Mutex<Arc<Block>>>,
    pub inner: Arc<Mutex<Inner>>,
    pub recent: Arc<Mutex<HashMap<Id, Arc<Block>>>>,
    pub store: StoreDb,
}

impl PluginStore {
    pub fn new(genesis: Arc<Block>, head: Arc<Mutex<Arc<Block>>>, inner: Arc<Mutex<Inner>>, db: Arc<DB>, recent: Arc<Mutex<HashMap<Id, Arc<Block>>>>, cfg: Arc<exec::Config>) -> PluginStore {
        PluginStore { genesis, head, inner, recent, store: StoreDb::new(db, cfg) }
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
}

struct HeadState<'a>(&'a PluginStore);

impl StateRead for HeadState<'_> {
    fn account(&mut self, a: Address) -> Result<Option<Account>> {
        let mut g = self.0.inner.lock().unwrap();
        Ok(g.head_account(a).map(|i| Account { nonce: i.nonce, balance: i.balance, code_hash: i.code_hash }))
    }
    fn storage(&mut self, a: Address, slot: U256) -> Result<U256> {
        let mut g = self.0.inner.lock().unwrap();
        Ok(g.head_storage(a, slot))
    }
    fn code(&mut self, h: B256) -> Result<Option<Bytes>> {
        let mut g = self.0.inner.lock().unwrap();
        Ok(g.head_code(h).map(|c| c.original_bytes()))
    }
}

impl Store for PluginStore {
    fn head(&self) -> u64 {
        self.head.lock().unwrap().height
    }
    fn block(&self, h: u64) -> Result<Option<Arc<Block>>> {
        if let Some(b) = self.live_block(h) {
            return Ok(Some(b));
        }
        self.store.block(h)
    }
    fn hash_at(&self, h: u64) -> Result<Option<B256>> {
        if h > self.head() {
            return Ok(None);
        }
        if let Some(b) = self.live_block(h) {
            return Ok(Some(b.hash));
        }
        self.store.hash_at(h)
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
        self.store.height_by_hash(hash)
    }
    fn tx_by_hash(&self, hash: &B256) -> Result<Option<(u64, usize)>> {
        if let Some(r) = self.store.tx_by_hash(hash)? {
            return Ok(Some(r));
        }
        // The accepted blocks the checker has not appended yet.
        let head = self.head.lock().unwrap().clone();
        let recent = self.recent.lock().unwrap();
        for b in recent.values().chain(std::iter::once(&head)) {
            if let Some(i) = b.txs.iter().position(|t| t.hash == *hash) {
                return Ok(Some((b.height, i)));
            }
        }
        Ok(None)
    }
    fn receipts(&self, h: u64) -> Result<Option<Vec<Receipt>>> {
        self.store.receipts(h)
    }
    fn traces(&self, h: u64) -> Result<Option<Vec<String>>> {
        self.store.traces(h)
    }
    fn container(&self, h: u64) -> Result<Option<Bytes>> {
        if let Some(b) = self.live_block(h) {
            return Ok(Some(b.container.clone().into()));
        }
        self.store.container(h)
    }
    fn state_at(&self, h: u64) -> Result<Box<dyn StateRead + '_>> {
        if h == self.head() && h != 0 {
            return Ok(Box::new(HeadState(self)));
        }
        self.store.state_at(h)
    }
    fn log_candidates(&self, from: u64, to: u64, addrs: &[Address], topics: &[Vec<B256>]) -> Result<Option<Vec<u64>>> {
        if to > self.store.head() {
            // ponytail: a filter reaching into the unappended tail scans it.
            return Ok(None);
        }
        self.store.log_candidates(from, to, addrs, topics)
    }
    fn tx_range(&self, h: u64) -> Result<Option<(u64, u32)>> {
        self.store.tx_range(h)
    }
    fn height_of_tx(&self, txnum: u64) -> Result<Option<u64>> {
        self.store.height_of_tx(txnum)
    }
    fn next_tx(&self) -> u64 {
        self.store.next_tx()
    }
    fn postings(&self, prefix: &[u8], lo: u64, hi: u64, desc: bool, f: &mut dyn FnMut(&[u8], u64, u8) -> bool) -> Result<()> {
        self.store.postings(prefix, lo, hi, desc, f)
    }
    fn groups(&self, prefix: &[u8], f: &mut dyn FnMut(&[u8]) -> bool) -> Result<()> {
        self.store.groups(prefix, f)
    }
    fn set_scan(&self, prefix: &[u8], f: &mut dyn FnMut(&[u8]) -> bool) -> Result<()> {
        self.store.set_scan(prefix, f)
    }
}

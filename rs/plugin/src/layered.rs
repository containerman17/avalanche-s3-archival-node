//! Layered: the executor's `StateDb` in the plugin. Reads go through the
//! block being verified (`cur`), then its chain of verified-not-accepted
//! parents (`Pending`), then the accepted state (rs/node's `Backend`: fresh
//! overlay, frozen overlay, rolled run). A tx's commit lands in `cur` (the
//! block's ordered write set and a read map), never in the backend: accept
//! applies the write set to the backend later, reject drops it.
//!
//! An account delete tombstones every live slot under it in the read map
//! (as Backend::commit does in the overlay) but not in the write set: the
//! overlay apply and the recovery replay regenerate them, and Dirty wipes
//! the storage itself (the Go write set has the same shape).
use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::sync::{Arc, Mutex};

use alloy_primitives::{Address, Bytes, B256, U256};
use exec::exec::{account_rlp, trimmed};
use exec::StateDb;
use node::engine::Backend;
use revm::state::{AccountInfo, Bytecode, EvmState};
use revm::{Database, DatabaseCommit};

/// What accept hands the checker: the block's ordered write set, deployed
/// code and the executor's result (receipts, traces, state rows: the
/// store's rows are built from it off the verify path). Taken out of the
/// Pending once.
pub struct Payload {
    pub ws: Vec<(Vec<u8>, Vec<u8>)>,
    pub code: Vec<(B256, Bytes)>,
    pub result: exec::BlockResult,
}

/// A verified block's state on top of its parent's.
pub struct Pending {
    pub number: u64,
    pub hash: B256,
    pub time: u64,
    pub parent: Option<Arc<Pending>>,
    map: HashMap<Vec<u8>, Vec<u8>>,
    owners: HashSet<B256>,
    pub code: Vec<(B256, Bytecode)>,
    pub payload: Mutex<Option<Payload>>,
}

impl Pending {
    fn get(&self, key: &[u8]) -> Option<Option<&[u8]>> {
        if let Some(v) = self.map.get(key) {
            return Some(if v.is_empty() { None } else { Some(v) });
        }
        self.parent.as_ref().and_then(|p| p.get(key))
    }

    fn code(&self, h: &B256) -> Option<Bytecode> {
        if let Some((_, c)) = self.code.iter().find(|(x, _)| x == h) {
            return Some(c.clone());
        }
        self.parent.as_ref().and_then(|p| p.code(h))
    }

    fn block_hash(&self, n: u64) -> Option<B256> {
        if self.number == n {
            return Some(self.hash);
        }
        self.parent.as_ref().and_then(|p| p.block_hash(n))
    }

    /// Live slot keys under an account across the chain (gated by owners).
    fn slot_keys(&self, ah: &B256, out: &mut Vec<Vec<u8>>) {
        if self.owners.contains(ah) {
            for (k, v) in &self.map {
                if k.len() == 65 && &k[..32] == ah.as_slice() && !v.is_empty() {
                    out.push(k.clone());
                }
            }
        }
        if let Some(p) = &self.parent {
            p.slot_keys(ah, out);
        }
    }
}

#[derive(Default)]
struct Cur {
    map: HashMap<Vec<u8>, Vec<u8>>,
    owners: HashSet<B256>,
    ws: Vec<(Vec<u8>, Vec<u8>)>,
    code: Vec<(B256, Bytecode)>,
    new_code: Vec<(B256, Bytes)>,
}

pub struct Layered {
    pub backend: Backend,
    parent: Option<Arc<Pending>>,
    cur: Cur,
}

fn acct_key(ah: &B256) -> [u8; 33] {
    let mut k = [0u8; 33];
    k[..32].copy_from_slice(ah.as_slice());
    k
}

fn slot_key(ah: &B256, sh: &B256) -> [u8; 65] {
    let mut k = [0u8; 65];
    k[..32].copy_from_slice(ah.as_slice());
    k[32] = 1;
    k[33..].copy_from_slice(sh.as_slice());
    k
}

fn decode_account(mut v: &[u8]) -> AccountInfo {
    use alloy_rlp::Decodable;
    let h = alloy_rlp::Header::decode(&mut v).expect("account rlp");
    assert!(h.list, "account row is not a list");
    let nonce = u64::decode(&mut v).expect("account nonce");
    let balance = U256::decode(&mut v).expect("account balance");
    let code_hash = B256::decode(&mut v).expect("account code hash");
    AccountInfo { balance, nonce, code_hash, code: None, ..Default::default() }
}

impl Layered {
    pub fn new(backend: Backend) -> Layered {
        Layered { backend, parent: None, cur: Cur::default() }
    }

    /// Start a block on top of `parent` (None: the accepted head).
    pub fn begin(&mut self, parent: Option<Arc<Pending>>) {
        self.parent = parent;
        self.cur = Cur::default();
    }

    /// Close the block: its Pending, with the checker's payload inside.
    pub fn finish(&mut self, number: u64, hash: B256, time: u64, result: exec::BlockResult) -> Pending {
        let cur = std::mem::take(&mut self.cur);
        let parent = self.parent.take();
        Pending {
            number,
            hash,
            time,
            parent,
            map: cur.map,
            owners: cur.owners,
            code: cur.code,
            payload: Mutex::new(Some(Payload { ws: cur.ws, code: cur.new_code, result })),
        }
    }

    /// The live value: cur, the pending chain, the backend.
    pub fn get(&self, key: &[u8]) -> Option<&[u8]> {
        if let Some(v) = self.cur.map.get(key) {
            return if v.is_empty() { None } else { Some(v) };
        }
        if let Some(p) = &self.parent {
            if let Some(v) = p.get(key) {
                return v;
            }
        }
        self.backend.get(key)
    }

    fn put(&mut self, key: &[u8], val: &[u8]) {
        self.cur.map.insert(key.to_vec(), val.to_vec());
        self.cur.ws.push((key.to_vec(), val.to_vec()));
    }

    /// Tombstone every live slot under a deleted account in the read map.
    fn tombstone_slots(&mut self, ah: &B256) {
        let mut keys = self.backend.slot_keys(ah);
        if let Some(p) = &self.parent {
            p.slot_keys(ah, &mut keys);
        }
        if self.cur.owners.contains(ah) {
            for (k, v) in &self.cur.map {
                if k.len() == 65 && &k[..32] == ah.as_slice() && !v.is_empty() {
                    keys.push(k.clone());
                }
            }
        }
        for k in keys {
            self.cur.map.insert(k, Vec::new());
        }
    }

    fn has_code(&self, h: &B256) -> bool {
        self.cur.code.iter().any(|(x, _)| x == h) || self.parent.as_ref().is_some_and(|p| p.code(h).is_some()) || self.backend.code.contains_key(h)
    }
}

impl Database for Layered {
    type Error = Infallible;

    fn basic(&mut self, address: Address) -> Result<Option<AccountInfo>, Infallible> {
        let ah = self.backend.addr_hash(address);
        Ok(self.get(&acct_key(&ah)).map(decode_account))
    }

    fn code_by_hash(&mut self, code_hash: B256) -> Result<Bytecode, Infallible> {
        if let Some((_, c)) = self.cur.code.iter().find(|(x, _)| *x == code_hash) {
            return Ok(c.clone());
        }
        if let Some(c) = self.parent.as_ref().and_then(|p| p.code(&code_hash)) {
            return Ok(c);
        }
        Ok(self.backend.code.get(&code_hash).cloned().unwrap_or_default())
    }

    fn storage(&mut self, address: Address, index: U256) -> Result<U256, Infallible> {
        let ah = self.backend.addr_hash(address);
        let sh = self.backend.slot_hash(B256::from(index));
        Ok(self.get(&slot_key(&ah, &sh)).map(U256::from_be_slice).unwrap_or(U256::ZERO))
    }

    fn block_hash(&mut self, number: u64) -> Result<B256, Infallible> {
        if let Some(h) = self.parent.as_ref().and_then(|p| p.block_hash(number)) {
            return Ok(h);
        }
        Ok(self.backend.block_hashes.get(&number).copied().unwrap_or_default())
    }
}

/// Backend::commit's rows, into cur instead of the overlay. The rows of one
/// commit are sorted by key: revm's EvmState iterates in a per-process
/// random order, and the write set is stored (the ws/ row), so without the
/// sort two processes write different bytes for the same block.
impl DatabaseCommit for Layered {
    fn commit(&mut self, changes: EvmState) {
        let start = self.cur.ws.len();
        self.commit_rows(changes);
        self.cur.ws[start..].sort_by(|a, b| a.0.cmp(&b.0));
    }
}

impl Layered {
    fn commit_rows(&mut self, changes: EvmState) {
        for (addr, a) in changes {
            if !a.is_touched() {
                continue;
            }
            let ah = self.backend.addr_hash(addr);
            if a.is_selfdestructed() || a.is_empty() {
                self.put(&acct_key(&ah), &[]);
                self.tombstone_slots(&ah);
                continue;
            }
            for (k, slot) in &a.storage {
                if slot.is_changed() {
                    let sh = self.backend.slot_hash(B256::from(*k));
                    let v = trimmed(slot.present_value());
                    if !v.is_empty() {
                        self.cur.owners.insert(ah);
                    }
                    self.put(&slot_key(&ah, &sh), &v);
                }
            }
            self.put(&acct_key(&ah), &account_rlp(&a.info));
            if let Some(code) = a.info.code {
                if !a.info.code_hash.is_zero() && a.info.code_hash != alloy_primitives::KECCAK256_EMPTY && !self.has_code(&a.info.code_hash) {
                    self.cur.new_code.push((a.info.code_hash, code.original_bytes()));
                    self.cur.code.push((a.info.code_hash, code));
                }
            }
        }
    }
}

impl StateDb for Layered {
    /// Accepted blocks only; a pending block's hash is its Pending's.
    fn set_block_hash(&mut self, number: u64, hash: B256) {
        self.backend.set_block_hash(number, hash);
    }
}

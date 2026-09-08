//! Oracle feeders over JSON-RPC (blocking ureq, one retry ladder, a
//! JSON-lines disk cache keyed by method+params so a rerun costs nothing):
//! `RpcDb`, the state of a chain as of one block from an archive node
//! (eth_getBalance / eth_getTransactionCount / eth_getCode / eth_getStorageAt
//! / eth_getBlockByNumber) behind a `CacheDB`, so a later window replays
//! without the state before it; and `RpcValidatorState`, the P-chain's
//! platform.validatedBy + platform.getValidatorsAt for warp predicates.

use crate::exec::StateDb;
use crate::warp::{ValidatorState, WarpSet};
use alloy_primitives::{Address, B256, U256};
use revm::{
    database::CacheDB,
    state::{AccountInfo, Bytecode},
    DatabaseRef,
};
use serde_json::{json, Value};
use std::cell::RefCell;
use std::collections::HashMap;
use std::io::Write;

pub struct Rpc {
    url: String,
    cache: RefCell<HashMap<String, Value>>,
    cache_file: Option<RefCell<std::fs::File>>,
    pub calls: RefCell<u64>,
}

impl Rpc {
    pub fn new(url: &str, cache_path: Option<&str>) -> Rpc {
        let mut cache = HashMap::new();
        let cache_file = cache_path.map(|p| {
            if let Ok(s) = std::fs::read_to_string(p) {
                for line in s.lines() {
                    if let Ok(v) = serde_json::from_str::<Value>(line) {
                        if let (Some(k), Some(r)) = (v.get("k").and_then(Value::as_str), v.get("r")) {
                            cache.insert(k.to_string(), r.clone());
                        }
                    }
                }
            }
            RefCell::new(std::fs::OpenOptions::new().create(true).append(true).open(p).expect("rpc cache file"))
        });
        Rpc { url: url.to_string(), cache: RefCell::new(cache), cache_file, calls: RefCell::new(0) }
    }

    /// One call, cached by method+params; a transport or RPC error retries
    /// with backoff and panics after the ladder (the oracle cannot go on).
    pub fn call(&self, method: &str, params: Value) -> Value {
        let key = format!("{method} {params}");
        if let Some(v) = self.cache.borrow().get(&key) {
            return v.clone();
        }
        let body = json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params});
        let mut delay = 500u64;
        let mut last = String::new();
        for _ in 0..8 {
            *self.calls.borrow_mut() += 1;
            let resp = ureq::post(&self.url).set("content-type", "application/json").set("user-agent", "curl/8.5.0").send_json(body.clone());
            match resp.and_then(|r| r.into_json::<Value>().map_err(ureq::Error::from)) {
                Ok(v) => {
                    if let Some(e) = v.get("error") {
                        last = e.to_string();
                    } else if let Some(r) = v.get("result") {
                        self.cache.borrow_mut().insert(key.clone(), r.clone());
                        if let Some(f) = &self.cache_file {
                            let _ = writeln!(f.borrow_mut(), "{}", json!({"k": key, "r": r}));
                        }
                        return r.clone();
                    }
                }
                Err(e) => last = e.to_string(),
            }
            std::thread::sleep(std::time::Duration::from_millis(delay));
            delay = (delay * 2).min(16_000);
        }
        panic!("rpc {method} {params} failed: {last}");
    }
}

fn hex_u256(v: &Value) -> U256 {
    let s = v.as_str().unwrap_or("0x0");
    U256::from_str_radix(s.trim_start_matches("0x"), 16).unwrap_or_default()
}

/// The chain state as of `block` (the parent of the first replayed block).
pub struct RpcDb {
    pub rpc: Rpc,
    pub block: u64,
}

impl RpcDb {
    pub fn new(url: &str, block: u64, cache_path: Option<&str>) -> RpcDb {
        RpcDb { rpc: Rpc::new(url, cache_path), block }
    }

    fn tag(&self) -> String {
        format!("0x{:x}", self.block)
    }

    /// The header fields the replay needs of a block: (timestamp, hash).
    pub fn block_time_and_hash(&self, n: u64) -> (u64, B256) {
        let b = self.rpc.call("eth_getBlockByNumber", json!([format!("0x{n:x}"), false]));
        let t = hex_u256(&b["timestamp"]).to::<u64>();
        let h: B256 = b["hash"].as_str().unwrap().parse().unwrap();
        (t, h)
    }
}

impl DatabaseRef for RpcDb {
    type Error = std::convert::Infallible;

    fn basic_ref(&self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        let a = format!("{address:?}");
        let tag = self.tag();
        let balance = hex_u256(&self.rpc.call("eth_getBalance", json!([a, tag])));
        let nonce = hex_u256(&self.rpc.call("eth_getTransactionCount", json!([a, tag]))).to::<u64>();
        let code = self.rpc.call("eth_getCode", json!([a, tag]));
        let code: Vec<u8> = alloy_primitives::hex::decode(code.as_str().unwrap_or("0x")).unwrap_or_default();
        if balance.is_zero() && nonce == 0 && code.is_empty() {
            return Ok(None);
        }
        let mut info = AccountInfo { balance, nonce, ..Default::default() };
        if !code.is_empty() {
            let bc = Bytecode::new_raw(code.into());
            info.code_hash = bc.hash_slow();
            info.code = Some(bc);
        }
        Ok(Some(info))
    }

    fn code_by_hash_ref(&self, _code_hash: B256) -> Result<Bytecode, Self::Error> {
        // CacheDB records the code of every account it loads; nothing else asks by hash.
        Ok(Bytecode::default())
    }

    fn storage_ref(&self, address: Address, index: U256) -> Result<U256, Self::Error> {
        let v = self.rpc.call("eth_getStorageAt", json!([format!("{address:?}"), format!("0x{:064x}", index), self.tag()]));
        Ok(hex_u256(&v))
    }

    fn block_hash_ref(&self, number: u64) -> Result<B256, Self::Error> {
        Ok(self.block_time_and_hash(number).1)
    }
}

impl StateDb for CacheDB<RpcDb> {
    fn set_block_hash(&mut self, number: u64, hash: B256) {
        self.cache.block_hashes.insert(U256::from(number), hash);
    }
    fn forget(&mut self, addr: Address) {
        self.cache.accounts.insert(addr, revm::database::DbAccount::new_not_existing());
    }
}

/// avalanchego ids.ID as cb58.
pub fn cb58_encode(id: B256) -> String {
    use sha2::Digest;
    let mut raw = id.to_vec();
    let sum = sha2::Sha256::digest(&raw);
    raw.extend_from_slice(&sum[28..]);
    bs58::encode(raw).into_string()
}

/// snow.Context.ValidatorState over the P-chain API.
pub struct RpcValidatorState {
    pub rpc: Rpc,
    sets: HashMap<(u64, B256), Option<WarpSet>>,
}

impl RpcValidatorState {
    pub fn new(url: &str, cache_path: Option<&str>) -> RpcValidatorState {
        RpcValidatorState { rpc: Rpc::new(url, cache_path), sets: HashMap::new() }
    }
}

impl ValidatorState for RpcValidatorState {
    fn subnet_id(&mut self, chain_id: B256) -> Result<B256, String> {
        let r = self.rpc.call("platform.validatedBy", json!({"blockchainID": cb58_encode(chain_id)}));
        let s = r.get("subnetID").and_then(Value::as_str).ok_or_else(|| format!("validatedBy: {r}"))?;
        crate::config::cb58(s).map_err(|e| e.to_string())
    }

    fn validator_set(&mut self, pchain_height: u64, subnet_id: B256) -> Result<Option<WarpSet>, String> {
        if let Some(s) = self.sets.get(&(pchain_height, subnet_id)) {
            return Ok(s.clone());
        }
        let r = self.rpc.call("platform.getValidatorsAt", json!({"height": pchain_height, "subnetID": cb58_encode(subnet_id)}));
        let vdrs = r.as_object().ok_or_else(|| format!("getValidatorsAt: {r}"))?;
        let mut list = Vec::with_capacity(vdrs.len());
        for (_, v) in vdrs {
            let pk = v.get("publicKey").and_then(Value::as_str).map(|s| alloy_primitives::hex::decode(s).map_err(|e| e.to_string())).transpose()?;
            let w: u64 = v.get("weight").and_then(Value::as_str).ok_or("weight")?.parse().map_err(|e| format!("weight: {e}"))?;
            list.push((pk, w));
        }
        let set = if list.is_empty() { None } else { Some(WarpSet::flatten(list)?) };
        self.sets.insert((pchain_height, subnet_id), set.clone());
        Ok(set)
    }
}

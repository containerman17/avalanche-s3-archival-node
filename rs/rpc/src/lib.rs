//! epochdb-rpc: the JSON-RPC surface of the Rust node (eth_, debug_, net_,
//! web3_, txpool_, ots_, edb_), the Go node's `rpc` package as the spec for
//! the method set and the shapes, stock subnet-evm as the judge for eth_ and
//! debug_ (the RPC ORACLE RULE). Everything is served over the `Store` trait
//! (blocks, receipts, stored callTracer frames, historical state, postings)
//! plus rs/exec for anything that executes (eth_call, estimateGas, the
//! re-executing tracers). See REPORT.md.
pub mod call;
pub mod debug;
pub mod edb;
pub mod eth;
pub mod fee;
pub mod filters;
pub mod json;
pub mod ots;
pub mod storedb;
pub mod genesis;

use std::sync::{Arc, Mutex};

use alloy_primitives::{Address, Bytes, B256, U256};
use block::Block;
use serde_json::{json, Value};

pub type Result<T> = anyhow::Result<T>;

/// One stored receipt (the rcpt row): what execution produced, nothing derived.
#[derive(Clone, Debug)]
pub struct Receipt {
    pub status: u64,
    pub gas_used: u64,
    pub cumulative_gas_used: u64,
    pub logs: Vec<Log>,
}

#[derive(Clone, Debug)]
pub struct Log {
    pub address: Address,
    pub topics: Vec<B256>,
    pub data: Bytes,
}

/// An account as the state holds it (code by hash through `StateRead::code`).
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Account {
    pub nonce: u64,
    pub balance: U256,
    pub code_hash: B256,
}

/// The state as of the END of one block (its post-state).
pub trait StateRead: Send {
    fn account(&mut self, a: Address) -> Result<Option<Account>>;
    fn storage(&mut self, a: Address, slot: U256) -> Result<U256>;
    fn code(&mut self, h: B256) -> Result<Option<Bytes>>;
}

/// What the RPC layer reads. Heights are [0, head]; 0 is the genesis. A
/// `None` is "not on this chain"; an error is "could not read", never
/// collapsed into a None (the Go node's rule).
pub trait Store: Send + Sync {
    /// The last stored (executed) height.
    fn head(&self) -> u64;
    /// The block at h with senders recovered.
    fn block(&self, h: u64) -> Result<Option<Arc<Block>>>;
    fn hash_at(&self, h: u64) -> Result<Option<B256>>;
    fn height_by_hash(&self, hash: &B256) -> Result<Option<u64>>;
    /// (height, index in block) of a tx.
    fn tx_by_hash(&self, hash: &B256) -> Result<Option<(u64, usize)>>;
    fn receipts(&self, h: u64) -> Result<Option<Vec<Receipt>>>;
    /// The stored callTracer JSON of every tx of block h.
    fn traces(&self, h: u64) -> Result<Option<Vec<String>>>;
    /// The verbatim container bytes.
    fn container(&self, h: u64) -> Result<Option<Bytes>>;
    /// The state after block h.
    fn state_at(&self, h: u64) -> Result<Box<dyn StateRead + '_>>;
    /// The ascending candidate heights in [from, to] for a logs filter out
    /// of the postings; None when the store has no postings (scan the range).
    fn log_candidates(&self, from: u64, to: u64, addrs: &[Address], topics: &[Vec<B256>]) -> Result<Option<Vec<u64>>>;
    /// TxNum space (the Go store's): (first TxNum, tx count) of block h.
    fn tx_range(&self, h: u64) -> Result<Option<(u64, u32)>>;
    fn height_of_tx(&self, txnum: u64) -> Result<Option<u64>>;
    /// The TxNum the next block starts at.
    fn next_tx(&self) -> u64;
    /// Posting chunks under a key prefix (store::format), TxNums in [lo, hi],
    /// ascending or descending; f gets (key, txnum, payload) and returns false to stop.
    fn postings(&self, prefix: &[u8], lo: u64, hi: u64, desc: bool, f: &mut dyn FnMut(&[u8], u64, u8) -> bool) -> Result<()>;
    /// The distinct group keys under a prefix (elog/<emitter>/ -> topic0s).
    fn groups(&self, prefix: &[u8], f: &mut dyn FnMut(&[u8]) -> bool) -> Result<()>;
    /// The set/ rows under a prefix.
    fn set_scan(&self, prefix: &[u8], f: &mut dyn FnMut(&[u8]) -> bool) -> Result<()>;
}

pub struct RpcError {
    pub code: i64,
    pub message: String,
    pub data: Option<Value>,
}

impl From<String> for RpcError {
    fn from(message: String) -> RpcError {
        RpcError { code: -32000, message, data: None }
    }
}
impl From<&str> for RpcError {
    fn from(m: &str) -> RpcError {
        m.to_string().into()
    }
}
impl From<anyhow::Error> for RpcError {
    fn from(e: anyhow::Error) -> RpcError {
        format!("{e:#}").into()
    }
}

pub fn invalid(m: impl Into<String>) -> RpcError {
    RpcError { code: -32602, message: m.into(), data: None }
}

pub type RpcResult = std::result::Result<Value, RpcError>;

/// web3_clientVersion.
pub const CLIENT_VERSION: &str = "epochdb/v0.1.0";
/// geth's defaults: batch size and body cap.
pub const MAX_BATCH: usize = 1000;
pub const MAX_REQUEST_BYTES: usize = 10 << 20;

/// The server: one chain's store plus the chain config the executor runs with.
pub struct Server {
    pub store: Arc<dyn Store>,
    pub cfg: Arc<exec::Config>,
    pub genesis: Arc<Block>,
    /// The genesis `config` object (eth_getChainConfig).
    pub chain_config: Value,
    pub(crate) filters: Mutex<filters::Registry>,
    pub(crate) iface721: Mutex<alloy_primitives::map::HashMap<Address, String>>,
}

impl Server {
    pub fn new(store: Arc<dyn Store>, cfg: Arc<exec::Config>, genesis: Arc<Block>, chain_config: Value) -> Server {
        Server { store, cfg, genesis, chain_config, filters: Mutex::new(Default::default()), iface721: Mutex::new(Default::default()) }
    }

    pub fn head(&self) -> u64 {
        self.store.head()
    }

    /// A block by height with the JSON-RPC error shape.
    pub fn block_at(&self, h: u64) -> std::result::Result<Arc<Block>, RpcError> {
        if h == 0 {
            return Ok(self.genesis.clone());
        }
        self.store.block(h)?.ok_or_else(|| format!("block {h} is not stored").into())
    }

    /// The Go node's blockNumber: a tag, a hex number, a bare hash or the
    /// object form, resolved to a height within [0, head].
    pub fn block_number(&self, v: Option<&Value>) -> std::result::Result<u64, RpcError> {
        let head = self.head();
        let Some(v) = v else { return Ok(head) };
        match v {
            Value::Null => Ok(head),
            Value::Object(o) => {
                if let Some(h) = o.get("blockHash").filter(|v| !v.is_null()) {
                    let hash = json::parse_hash(Some(h))?;
                    return self.height_of_hash_tag(&hash);
                }
                if let Some(n) = o.get("blockNumber").filter(|v| !v.is_null()) {
                    return self.block_number(Some(n));
                }
                Err(invalid("block tag object needs blockNumber or blockHash"))
            }
            Value::String(s) => {
                if s.len() == 66 && s.starts_with("0x") {
                    let hash = json::parse_hash(Some(v))?;
                    return self.height_of_hash_tag(&hash);
                }
                match s.as_str() {
                    "latest" | "pending" | "" | "safe" | "finalized" => Ok(head),
                    "earliest" => Ok(0),
                    _ => {
                        let n = json::parse_qty(v).map_err(|e| invalid(format!("bad block number {s:?}: {}", e.message)))?;
                        if n > head {
                            return Err(invalid(format!("block {n} beyond head {head}")));
                        }
                        Ok(n)
                    }
                }
            }
            Value::Number(n) => {
                let n = n.as_u64().ok_or_else(|| invalid("bad block number"))?;
                if n > head {
                    return Err(invalid(format!("block {n} beyond head {head}")));
                }
                Ok(n)
            }
            _ => Err(invalid("bad block tag")),
        }
    }

    /// A block-hash tag: an unknown hash is an ERROR, not a null.
    pub fn height_of_hash_tag(&self, h: &B256) -> std::result::Result<u64, RpcError> {
        self.store.height_by_hash(h)?.ok_or_else(|| invalid(format!("block {h} is not on this chain")))
    }

    fn dispatch(&self, method: &str, params: &[Value]) -> RpcResult {
        let p = |i: usize| params.get(i).filter(|v| !v.is_null());
        match method {
            "eth_chainId" => Ok(json!(json::qty(self.cfg.chain_id))),
            "eth_blockNumber" => Ok(json!(json::qty(self.head()))),
            "net_version" => Ok(json!(self.cfg.chain_id.to_string())),
            "web3_clientVersion" => Ok(json!(CLIENT_VERSION)),
            "web3_sha3" => {
                let b: Bytes = p(0).and_then(Value::as_str).ok_or_else(|| invalid("need [data]"))?.parse().map_err(|e| invalid(format!("bad data: {e}")))?;
                Ok(json!(alloy_primitives::keccak256(&b)))
            }
            "eth_syncing" => Ok(json!(false)),
            "net_listening" => Ok(json!(false)),
            "net_peerCount" => Ok(json!("0x0")),
            "eth_accounts" => Ok(json!([])),
            "eth_coinbase" | "eth_etherbase" => Ok(json!("0x0100000000000000000000000000000000000000")),
            "eth_getUncleCountByBlockNumber" | "eth_getUncleCountByBlockHash" => Ok(json!("0x0")),
            "eth_getUncleByBlockNumberAndIndex" | "eth_getUncleByBlockHashAndIndex" => Ok(Value::Null),
            "eth_pendingTransactions" => Ok(json!([])),
            "eth_getProof" => Err("eth_getProof unsupported by design: epochdb stores no tries".into()),
            "debug_getBadBlocks" => Ok(json!([])),
            "debug_dumpBlock" | "debug_accountRange" | "debug_storageRangeAt" | "debug_intermediateRoots" => Err(format!("{method} unsupported by design: epochdb stores no tries").into()),
            "debug_preimage" => Err("debug_preimage unsupported by design: epochdb keeps no preimage table".into()),
            "debug_traceBadBlock" => Err("debug_traceBadBlock: no bad blocks are retained (replay is root-verified and halts instead)".into()),
            "debug_traceChain" => Err("debug_traceChain unsupported: trace a range with debug_traceBlockByNumber per block".into()),
            "debug_getModifiedAccountsByNumber" | "debug_getModifiedAccountsByHash" => Err(format!("{method}: storage v0 keys state by account, not by block, so there is no touched-account index (a new key family and a reindex, not a lookup)").into()),
            "txpool_status" | "txpool_content" | "txpool_contentFrom" | "txpool_inspect" => Ok(filters::empty_txpool(method)),
            "eth_sendRawTransaction" | "eth_sendTransaction" | "eth_fillTransaction" | "eth_resend" => Err(format!("{method}: this node does not submit transactions (an archive read server, no mempool)").into()),
            "eth_sign" | "eth_signTransaction" => Err(format!("{method}: no keystore on this node").into()),
            "eth_subscribe" | "eth_unsubscribe" => Err(format!("{method} requires the WebSocket transport").into()),
            "epochdb_head" => {
                let h = self.head();
                let b = self.block_at(h)?;
                Ok(json!({"number": json::qty(h), "hash": b.hash, "timestamp": json::qty(b.header.time), "accepted": json::qty(h), "settled": json::qty(h), "txs": json::qty(self.store.next_tx())}))
            }
            _ => {
                if let Some(r) = eth::dispatch(self, method, params) {
                    return r;
                }
                if let Some(r) = debug::dispatch(self, method, params) {
                    return r;
                }
                if let Some(r) = filters::dispatch(self, method, params) {
                    return r;
                }
                if let Some(r) = ots::dispatch(self, method, params) {
                    return r;
                }
                if let Some(r) = edb::dispatch(self, method, params) {
                    return r;
                }
                if let Some((ns, _)) = method.split_once('_') {
                    if matches!(ns, "personal" | "miner" | "admin" | "les" | "clique" | "ethash") {
                        return Err(format!("{method}: not an archive method (this node serves reads only)").into());
                    }
                }
                Err(RpcError { code: -32601, message: format!("method not found: {method}"), data: None })
            }
        }
    }

    /// One request object -> one response object (None for a notification).
    fn one(&self, r: &Value) -> Option<Value> {
        let id = r.get("id").cloned();
        let method = r.get("method").and_then(Value::as_str).unwrap_or("");
        if method.is_empty() {
            return Some(reply(id, Err(RpcError { code: -32600, message: "invalid request: no method".into(), data: None })));
        }
        let params: Vec<Value> = match r.get("params") {
            None | Some(Value::Null) => Vec::new(),
            Some(Value::Array(a)) => a.clone(),
            Some(_) => return Some(reply(id, Err(RpcError { code: -32600, message: "invalid request: params must be an array".into(), data: None }))),
        };
        let res = self.dispatch(method, &params);
        id.map(|id| reply(Some(id), res))
    }

    /// One HTTP body in, one body out (single or batch).
    pub fn handle(&self, body: &[u8]) -> Vec<u8> {
        if body.len() > MAX_REQUEST_BYTES {
            return reply(None, Err(RpcError { code: -32600, message: format!("request body exceeds the {MAX_REQUEST_BYTES}-byte limit"), data: None })).to_string().into_bytes();
        }
        let req: Value = match serde_json::from_slice(body) {
            Ok(v) => v,
            Err(err) => return reply(None, Err(RpcError { code: -32700, message: format!("parse error: {err}"), data: None })).to_string().into_bytes(),
        };
        let out = match &req {
            Value::Array(rs) => {
                if rs.is_empty() {
                    return reply(None, Err(RpcError { code: -32600, message: "invalid request: empty batch".into(), data: None })).to_string().into_bytes();
                }
                if rs.len() > MAX_BATCH {
                    return reply(None, Err(RpcError { code: -32600, message: format!("batch of {} requests exceeds the limit of {MAX_BATCH}", rs.len()), data: None })).to_string().into_bytes();
                }
                let replies: Vec<Value> = rs
                    .iter()
                    .filter_map(|r| match r {
                        Value::Object(_) => self.one(r),
                        _ => Some(reply(None, Err(RpcError { code: -32600, message: "invalid request: not an object".into(), data: None }))),
                    })
                    .collect();
                if replies.is_empty() {
                    return Vec::new();
                }
                Value::Array(replies)
            }
            Value::Object(_) => match self.one(&req) {
                Some(v) => v,
                None => return Vec::new(),
            },
            _ => reply(None, Err(RpcError { code: -32600, message: "invalid request: not an object".into(), data: None })),
        };
        out.to_string().into_bytes()
    }
}

pub fn reply(id: Option<Value>, res: RpcResult) -> Value {
    let id = id.unwrap_or(Value::Null);
    match res {
        Ok(v) => json!({"jsonrpc": "2.0", "id": id, "result": v}),
        Err(e) => {
            let mut ev = json!({"code": e.code, "message": e.message});
            if let Some(d) = e.data {
                ev["data"] = d;
            }
            json!({"jsonrpc": "2.0", "id": id, "error": ev})
        }
    }
}

/// A small block cache for Store impls (decoded, senders recovered).
pub struct BlockCache {
    map: Mutex<(alloy_primitives::map::HashMap<u64, Arc<Block>>, std::collections::VecDeque<u64>)>,
    cap: usize,
}

impl BlockCache {
    pub fn new(cap: usize) -> BlockCache {
        BlockCache { map: Mutex::new(Default::default()), cap }
    }
    pub fn get_or(&self, h: u64, f: impl FnOnce() -> Result<Option<Block>>) -> Result<Option<Arc<Block>>> {
        if let Some(b) = self.map.lock().unwrap().0.get(&h) {
            return Ok(Some(b.clone()));
        }
        let Some(mut b) = f()? else { return Ok(None) };
        for t in &mut b.txs {
            if t.sender.is_none() {
                t.sender = block::recover(t);
            }
        }
        let b = Arc::new(b);
        let mut g = self.map.lock().unwrap();
        if g.0.len() >= self.cap {
            if let Some(old) = g.1.pop_front() {
                g.0.remove(&old);
            }
        }
        g.0.insert(h, b.clone());
        g.1.push_back(h);
        Ok(Some(b))
    }
}

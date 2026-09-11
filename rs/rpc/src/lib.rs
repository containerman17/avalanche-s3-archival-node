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
pub mod tokens;
pub mod genesis;
pub mod ws;

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
    /// The SETTLED head: the last executed height (its state, receipts and
    /// traces are stored). Under SAE this is `k` blocks behind the accepted
    /// head; in the synchronous model it is the accepted head.
    fn head(&self) -> u64;
    /// The ACCEPTED head: the chain's height (eth_blockNumber). Under SAE it
    /// runs ahead of `head` (settled); its blocks exist (header + txs) but
    /// their execution results are not settled yet. Default = settled head
    /// (synchronous model, where accept implies execution).
    fn accepted_head(&self) -> u64 {
        self.head()
    }
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

#[derive(Debug)]
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

/// One admission answer of the mempool: the ABI's code (0 ok, 2 replaced,
/// anything else refused with `message`), and the tx hash.
#[derive(Clone, Copy, Debug)]
pub struct PoolAdd {
    pub code: u8,
    pub message: &'static str,
    pub hash: B256,
}

/// The transaction pool behind eth_sendRawTransaction, txpool_*,
/// eth_pendingTransactions and the "pending" nonce (the validator's engine
/// sets it; the archive read server has none and refuses submissions).
pub trait Mempool: Send + Sync {
    /// Admits tx envelopes (MarshalBinary form), one answer per input.
    fn add(&self, raws: Vec<Bytes>, local: bool) -> Vec<PoolAdd>;
    /// (pending, queued).
    fn status(&self) -> (usize, usize);
    /// (pending, queued) txs of one address or of all, address then nonce order.
    fn content(&self, addr: Option<Address>) -> (Vec<Arc<block::Tx>>, Vec<Arc<block::Tx>>);
    /// The state nonce plus the executable txs; None when the pool holds nothing of the address.
    fn pending_nonce(&self, addr: Address) -> Option<u64>;
}

/// subnet-evm's ErrUnfinalizedData: a height past the accepted head.
pub fn unfinalized() -> RpcError {
    "cannot query unfinalized data".into()
}

/// The SAE not-settled error code (server-defined range). A height that is
/// accepted but not yet settled (executed): its block exists, its execution
/// results (state, receipts, traces) are not available. Never a wrong value.
pub const NOT_SETTLED_CODE: i64 = -32011;

/// A height accepted but not settled yet, with the settlement lag.
pub fn not_settled(h: u64, settled: u64) -> RpcError {
    RpcError { code: NOT_SETTLED_CODE, message: format!("block {h} is accepted but not settled yet (settled head {settled}, lag {})", h.saturating_sub(settled)), data: None }
}

/// geth's "invalid argument N: ..." for a bad param.
pub fn bad_arg(i: usize, e: RpcError) -> RpcError {
    invalid(format!("invalid argument {i}: {}", e.message))
}

pub fn missing_arg(i: usize) -> RpcError {
    invalid(format!("missing value for required argument {i}"))
}

pub type RpcResult = std::result::Result<Value, RpcError>;

/// web3_clientVersion.
pub const CLIENT_VERSION: &str = "epochdb/v0.1.0";
/// geth's defaults: batch size and body cap.
pub const MAX_BATCH: usize = 1000;
pub const MAX_REQUEST_BYTES: usize = 10 << 20;

/// The server: one chain's store plus the chain config the executor runs with.
/// The hash, or libevm's refusal text.
fn pool_answer(a: PoolAdd) -> RpcResult {
    match a.code {
        0 | 2 => Ok(json!(a.hash)),
        _ => Err(a.message.into()),
    }
}

pub struct Server {
    pub store: Arc<dyn Store>,
    pub cfg: Arc<exec::Config>,
    pub genesis: Arc<Block>,
    /// The genesis `config` object (eth_getChainConfig).
    pub chain_config: Value,
    pub(crate) filters: Mutex<filters::Registry>,
    pub(crate) iface721: Mutex<alloy_primitives::map::HashMap<Address, String>>,
    /// Accepted heads for eth_subscribe (ws.rs); `publish` feeds it.
    pub heads: tokio::sync::broadcast::Sender<Arc<ws::Head>>,
    /// The validator's pool, set once after construction; unset = no mempool.
    pub mempool: std::sync::OnceLock<Arc<dyn Mempool>>,
}

impl Server {
    pub fn new(store: Arc<dyn Store>, cfg: Arc<exec::Config>, genesis: Arc<Block>, chain_config: Value, upgrades: Option<Value>) -> Server {
        let chain_config = stock_chain_config(&cfg, chain_config, upgrades);
        Server { store, cfg, genesis, chain_config, filters: Mutex::new(Default::default()), iface721: Mutex::new(Default::default()), heads: tokio::sync::broadcast::channel(ws::QUEUE).0, mempool: std::sync::OnceLock::new() }
    }

    /// The pool's RPC methods (dispatch routes them here when a mempool is set).
    fn pool_method(&self, pool: &dyn Mempool, method: &str, params: &[Value]) -> RpcResult {
        match method {
            "eth_sendRawTransaction" => {
                let raw = json::parse_bytes(params.first().filter(|v| !v.is_null()).ok_or_else(|| missing_arg(0))?).map_err(|e| bad_arg(0, e))?;
                pool_answer(pool.add(vec![raw], false)[0])
            }
            "txpool_status" => {
                let (p, q) = pool.status();
                Ok(json!({"pending": json::qty(p as u64), "queued": json::qty(q as u64)}))
            }
            "txpool_content" | "txpool_contentFrom" | "txpool_inspect" => {
                let addr = if method == "txpool_contentFrom" { Some(json::parse_addr(params.first()).map_err(|e| bad_arg(0, e))?) } else { None };
                let (p, q) = pool.content(addr);
                let render = |t: &block::Tx| -> Value {
                    if method == "txpool_inspect" {
                        json!(format!("{}: {} wei + {} gas x {} wei", t.to.map_or("contract creation".to_string(), |a| a.to_string()), t.value, t.gas_limit, t.gas_price))
                    } else {
                        json::pending_tx_json(t)
                    }
                };
                let group = |txs: Vec<Arc<block::Tx>>| -> Value {
                    if addr.is_some() {
                        let mut m = serde_json::Map::new();
                        for t in txs {
                            m.insert(t.nonce.to_string(), render(&t));
                        }
                        return Value::Object(m);
                    }
                    let mut m = serde_json::Map::new();
                    for t in txs {
                        let by = m.entry(t.sender.unwrap_or_default().to_string()).or_insert_with(|| json!({}));
                        by[t.nonce.to_string()] = render(&t);
                    }
                    Value::Object(m)
                };
                Ok(json!({"pending": group(p), "queued": group(q)}))
            }
            "eth_pendingTransactions" => {
                let (p, _) = pool.content(None);
                Ok(Value::Array(p.iter().map(|t| json::pending_tx_json(t)).collect()))
            }
            _ => Err(RpcError { code: -32601, message: format!("the method {method} does not exist/is not available"), data: None }),
        }
    }

    /// The accept path's hook: one accepted block with its receipts (the
    /// 2718 envelopes, concatenated) for every live subscription.
    pub fn publish(&self, block: Arc<Block>, receipts_rlp: &[u8]) {
        if self.heads.receiver_count() == 0 {
            return;
        }
        match ws::decode_receipts(receipts_rlp) {
            Ok(receipts) => {
                let _ = self.heads.send(Arc::new(ws::Head { block, receipts }));
            }
            Err(e) => eprintln!("rpc: head {} not published: {e:#}", block.height),
        }
    }

    /// The settled head (the executed height): `latest` state, and the ceiling
    /// for state / receipt / trace reads.
    pub fn head(&self) -> u64 {
        self.store.head()
    }

    /// The accepted head (the chain height): eth_blockNumber, and the ceiling
    /// for block / tx reads. Equals the settled head in the synchronous model.
    pub fn accepted_head(&self) -> u64 {
        self.store.accepted_head()
    }

    /// Reject a height that is accepted but not settled yet (its state /
    /// execution results are not available). A height <= the settled head
    /// passes; one above the accepted head is already refused as unfinalized
    /// by `block_number`.
    pub fn require_settled(&self, n: u64) -> std::result::Result<u64, RpcError> {
        let settled = self.head();
        if n > settled {
            return Err(not_settled(n, settled));
        }
        Ok(n)
    }

    /// A block by height with the JSON-RPC error shape.
    pub fn block_at(&self, h: u64) -> std::result::Result<Arc<Block>, RpcError> {
        if h == 0 {
            return Ok(self.genesis.clone());
        }
        self.store.block(h)?.ok_or_else(|| format!("block {h} is not stored").into())
    }

    /// The Go node's blockNumber: a tag, a hex number, a bare hash or the
    /// object form. Resolves to a height in [0, accepted], so a block / tx read
    /// reaches an accepted-but-unsettled height (its execution results are
    /// gated separately, see `require_settled`). The tags `latest` / `pending`
    /// / `safe` / `finalized` resolve to the SETTLED head, so a state read at a
    /// tag serves settled state and eth_call("latest") never hits the unsettled
    /// band; `eth_blockNumber` returns the accepted head on its own.
    pub fn block_number(&self, v: Option<&Value>) -> std::result::Result<u64, RpcError> {
        let head = self.head();
        let accepted = self.accepted_head();
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
                        let n = json::parse_qty(v)?;
                        if n > accepted {
                            return Err(unfinalized());
                        }
                        Ok(n)
                    }
                }
            }
            Value::Number(n) => {
                let n = n.as_u64().ok_or_else(|| invalid("bad block number"))?;
                if n > accepted {
                    return Err(unfinalized());
                }
                Ok(n)
            }
            _ => Err(invalid("bad block tag")),
        }
    }

    /// A block-hash tag: an unknown hash is an ERROR, not a null (stock's text).
    pub fn height_of_hash_tag(&self, h: &B256) -> std::result::Result<u64, RpcError> {
        self.store.height_by_hash(h)?.ok_or_else(|| "header for hash not found".into())
    }

    fn dispatch(&self, method: &str, params: &[Value]) -> RpcResult {
        let p = |i: usize| params.get(i).filter(|v| !v.is_null());
        match method {
            "eth_chainId" => Ok(json!(json::qty(self.cfg.chain_id))),
            "eth_blockNumber" => Ok(json!(json::qty(self.accepted_head()))),
            "net_version" => Ok(json!(self.cfg.chain_id.to_string())),
            "web3_clientVersion" => Ok(json!(CLIENT_VERSION)),
            "web3_sha3" => {
                let b: Bytes = json::parse_bytes(p(0).ok_or_else(|| missing_arg(0))?).map_err(|e| bad_arg(0, e))?;
                Ok(json!(alloy_primitives::keccak256(&b)))
            }
            "eth_syncing" => Ok(json!(false)),
            "net_listening" => Ok(json!(true)),
            "net_peerCount" => Ok(json!("0x0")),
            "eth_accounts" => Ok(json!([])),
            "eth_coinbase" | "eth_etherbase" => Ok(json!("0x0100000000000000000000000000000000000000")),
            "eth_getUncleCountByBlockNumber" | "eth_getUncleCountByBlockHash" => Ok(json!("0x0")),
            "eth_getUncleByBlockNumberAndIndex" | "eth_getUncleByBlockHashAndIndex" => Ok(Value::Null),
            m if self.mempool.get().is_some() && matches!(m, "eth_sendRawTransaction" | "txpool_status" | "txpool_content" | "txpool_contentFrom" | "txpool_inspect" | "eth_pendingTransactions") => self.pool_method(self.mempool.get().unwrap().as_ref(), m, params),
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
                let (accepted, settled) = (self.accepted_head(), self.head());
                // number = the accepted head (the chain height); hash / timestamp
                // of the accepted block. settled and lag surface the SAE gap.
                let b = self.block_at(accepted)?;
                Ok(json!({"number": json::qty(accepted), "hash": b.hash, "timestamp": json::qty(b.header.time), "accepted": json::qty(accepted), "settled": json::qty(settled), "lag": json::qty(accepted.saturating_sub(settled)), "txs": json::qty(self.store.next_tx())}))
            }
            // The SAE settled-head / lag surface: the accepted head is
            // eth_blockNumber, the settled head is the executed height.
            "edb_settledNumber" | "epochdb_settledNumber" => {
                let (accepted, settled) = (self.accepted_head(), self.head());
                Ok(json!({"settled": json::qty(settled), "accepted": json::qty(accepted), "lag": json::qty(accepted.saturating_sub(settled))}))
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
                Err(RpcError { code: -32601, message: format!("the method {method} does not exist/is not available"), data: None })
            }
        }
    }

    /// One request object -> one response object (None for a notification).
    /// `hook` sees the call first (the transport's own methods: eth_subscribe
    /// over a WebSocket connection).
    fn one(&self, r: &Value, hook: &mut dyn FnMut(&str, &[Value]) -> Option<RpcResult>) -> Option<Value> {
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
        let res = hook(method, &params).unwrap_or_else(|| self.dispatch(method, &params));
        id.map(|id| reply(Some(id), res))
    }

    /// The batch's eth_sendRawTransaction elements with a decodable
    /// parameter, admitted together: index in the batch -> the answer.
    fn pre_admit(&self, rs: &[Value]) -> std::collections::HashMap<usize, PoolAdd> {
        let mut out = std::collections::HashMap::new();
        let Some(pool) = self.mempool.get() else { return out };
        let mut raws = Vec::new();
        let mut at = Vec::new();
        for (i, r) in rs.iter().enumerate() {
            if r.get("method").and_then(Value::as_str) != Some("eth_sendRawTransaction") {
                continue;
            }
            let Some(p) = r.get("params").and_then(Value::as_array).and_then(|a| a.first()) else { continue };
            if let Ok(raw) = json::parse_bytes(p) {
                raws.push(raw);
                at.push(i);
            }
        }
        if raws.is_empty() {
            return out;
        }
        for (i, a) in at.into_iter().zip(pool.add(raws, false)) {
            out.insert(i, a);
        }
        out
    }

    /// One HTTP body in, one body out (single or batch).
    pub fn handle(&self, body: &[u8]) -> Vec<u8> {
        self.handle_with(body, &mut |_, _| None)
    }

    /// `handle` with a per-connection hook ahead of the dispatch.
    pub fn handle_with(&self, body: &[u8], hook: &mut dyn FnMut(&str, &[Value]) -> Option<RpcResult>) -> Vec<u8> {
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
                // Every eth_sendRawTransaction of the batch goes to the pool in one
                // call (one lock, one state read, senders recovered in parallel).
                let pre = self.pre_admit(rs);
                let replies: Vec<Value> = rs
                    .iter()
                    .enumerate()
                    .filter_map(|(i, r)| match r {
                        Value::Object(_) if pre.get(&i).is_some() => {
                            let res = pre[&i];
                            self.one(r, &mut |m, _| if m == "eth_sendRawTransaction" { Some(pool_answer(res)) } else { None })
                        }
                        Value::Object(_) => self.one(r, hook),
                        _ => Some(reply(None, Err(RpcError { code: -32600, message: "invalid request: not an object".into(), data: None }))),
                    })
                    .collect();
                if replies.is_empty() {
                    return Vec::new();
                }
                Value::Array(replies)
            }
            Value::Object(_) => match self.one(&req, hook) {
                Some(v) => v,
                None => return Vec::new(),
            },
            _ => reply(None, Err(RpcError { code: -32600, message: "invalid request: not an object".into(), data: None })),
        };
        out.to_string().into_bytes()
    }
}

/// params.ChainConfig as subnet-evm marshals it (eth_getChainConfig): the
/// genesis config with every block fork at 0, the libevm time forks, and the
/// Avalanche network upgrade timestamps; eip150Hash is not a field there.
fn stock_chain_config(cfg: &exec::Config, mut c: Value, upgrades: Option<Value>) -> Value {
    let Some(o) = c.as_object_mut() else { return c };
    if let Some(u) = upgrades.filter(|u| u.is_object()) {
        o.insert("upgrades".into(), u);
    }
    o.remove("eip150Hash");
    for k in ["homesteadBlock", "eip150Block", "eip155Block", "eip158Block", "byzantiumBlock", "constantinopleBlock", "petersburgBlock", "istanbulBlock", "muirGlacierBlock", "berlinBlock", "londonBlock"] {
        o.entry(k).or_insert(json!(0));
    }
    o.entry("subnetEVMTimestamp").or_insert(json!(cfg.subnet_evm));
    if let Some(t) = cfg.durango {
        o.insert("durangoTimestamp".into(), json!(t));
        o.insert("shanghaiTime".into(), json!(t));
    }
    if let Some(t) = cfg.etna {
        o.insert("etnaTimestamp".into(), json!(t));
        o.insert("cancunTime".into(), json!(t));
    }
    if let Some(t) = cfg.granite {
        o.insert("graniteTimestamp".into(), json!(t));
    }
    // Helicon is unscheduled on every network today (upgrade.UnscheduledActivationTime).
    o.insert("heliconTimestamp".into(), json!(253399622400u64));
    remarshal(&mut c);
    c
}

/// Stock re-marshals the parsed config: common.Address (allow-list roles,
/// initialMint keys, rewardAddress) as lowercase hex whatever case the genesis
/// wrote, and the fields without omitempty (warp's quorumNumerator and
/// requirePrimaryNetworkSigners, the reward manager's allowFeeRecipients) even
/// when the genesis left them out.
fn remarshal(v: &mut Value) {
    fn is_addr(s: &str) -> bool {
        s.len() == 42 && s.starts_with("0x") && s[2..].bytes().all(|b| b.is_ascii_hexdigit())
    }
    match v {
        Value::String(s) if is_addr(s) => *s = s.to_ascii_lowercase(),
        Value::Array(a) => a.iter_mut().for_each(remarshal),
        Value::Object(o) => {
            for (k, x) in std::mem::take(o) {
                let mut x = x;
                remarshal(&mut x);
                if let Some(m) = x.as_object_mut() {
                    match k.as_str() {
                        "warpConfig" => {
                            m.entry("quorumNumerator").or_insert(json!(0));
                            m.entry("requirePrimaryNetworkSigners").or_insert(json!(false));
                        }
                        "initialRewardConfig" => {
                            m.entry("allowFeeRecipients").or_insert(json!(false));
                        }
                        _ => {}
                    }
                }
                o.insert(if is_addr(&k) { k.to_ascii_lowercase() } else { k }, x);
            }
        }
        _ => {}
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

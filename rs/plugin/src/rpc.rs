//! The /rpc handler, minimal: eth_chainId, eth_blockNumber,
//! eth_getBlockByNumber / ByHash (the store), eth_getBalance /
//! eth_getTransactionCount / eth_getCode / eth_getStorageAt at latest (the
//! state engine), eth_call / eth_estimateGas at latest (the executor in call
//! mode, nothing committed). JSON-RPC 2.0, batches too. rs/rpc (P4) takes
//! this over; the shape to keep is `call(method, params) -> Result<Value, RpcError>`.
use std::sync::Arc;

use alloy_primitives::{Address, Bytes, B256, U256};
use block::Block;
use exec::CallMsg;
use revm::Database;
use serde_json::{json, Value};

use crate::node_engine::NodeEngine;

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

fn invalid(m: impl Into<String>) -> RpcError {
    RpcError { code: -32602, message: m.into(), data: None }
}

fn qty(v: u64) -> String {
    format!("0x{v:x}")
}

fn qty256(v: U256) -> String {
    format!("0x{v:x}")
}

fn hex(b: &[u8]) -> String {
    format!("0x{}", alloy_primitives::hex::encode(b))
}

fn parse_qty(v: &Value) -> Result<u64, RpcError> {
    let s = v.as_str().ok_or_else(|| invalid("expected a hex quantity"))?;
    u64::from_str_radix(s.trim_start_matches("0x"), 16).map_err(|e| invalid(e.to_string()))
}

fn parse_u256(v: &Value) -> Result<U256, RpcError> {
    let s = v.as_str().ok_or_else(|| invalid("expected a hex quantity"))?;
    U256::from_str_radix(s.trim_start_matches("0x"), 16).map_err(|e| invalid(e.to_string()))
}

fn parse_addr(v: Option<&Value>) -> Result<Address, RpcError> {
    v.and_then(Value::as_str).ok_or_else(|| invalid("missing address"))?.parse().map_err(|_| invalid("invalid address"))
}

fn parse_hash(v: Option<&Value>) -> Result<B256, RpcError> {
    v.and_then(Value::as_str).ok_or_else(|| invalid("missing hash"))?.parse().map_err(|_| invalid("invalid hash"))
}

/// geth's revert error: -32000 "execution reverted" for an empty revert,
/// else code 3 with the data and the Error(string) reason in the message.
fn revert_error(output: &[u8]) -> RpcError {
    if output.is_empty() {
        return RpcError { code: -32000, message: "execution reverted".into(), data: None };
    }
    let mut message = "execution reverted".to_string();
    if output.len() >= 68 && output[..4] == [0x08, 0xc3, 0x79, 0xa0] {
        let off = U256::from_be_slice(&output[4..36]).to::<usize>() + 4;
        if output.len() >= off + 32 {
            let n = U256::from_be_slice(&output[off..off + 32]).to::<usize>();
            if output.len() >= off + 32 + n {
                message = format!("execution reverted: {}", String::from_utf8_lossy(&output[off + 32..off + 32 + n]));
            }
        }
    }
    RpcError { code: 3, message, data: Some(json!(hex(output))) }
}

/// The executor's invalid-message errors in geth's words where the door's
/// clients match on them.
fn call_error(e: anyhow::Error, gas: u64) -> RpcError {
    let s = format!("{e:#}");
    if let Some(rest) = s.strip_prefix("Transaction(CallGasCostMoreThanGasLimit { initial_gas: ") {
        if let Some(want) = rest.split(',').next().and_then(|n| n.trim().parse::<u64>().ok()) {
            return format!("err: intrinsic gas too low: have {gas}, want {want} (supplied gas {gas})").into();
        }
    }
    s.into()
}

/// geth's DefaultRPCGasCap (subnet-evm: 50M).
const RPC_GAS_CAP: u64 = 50_000_000;
const TX_GAS: u64 = 21_000;
const CALL_STIPEND: u64 = 2300;
/// internal/ethapi estimateGasErrorRatio.
const ESTIMATE_ERROR_RATIO: f64 = 0.015;

fn tx_json(b: &Block, i: usize) -> Value {
    let t = &b.txs[i];
    // A block read back from the store has no senders recovered.
    let from = t.sender.or_else(|| block::recover(t)).unwrap_or_default();
    let mut v = json!({
        "blockHash": b.hash,
        "blockNumber": qty(b.height),
        "from": from,
        "gas": qty(t.gas_limit),
        "gasPrice": qty(t.gas_price as u64),
        "hash": t.hash,
        "input": hex(&t.input),
        "nonce": qty(t.nonce),
        "to": t.to,
        "transactionIndex": qty(i as u64),
        "value": qty256(t.value),
        "type": qty(t.tx_type as u64),
        "v": qty(t.v),
        "r": qty256(t.r),
        "s": qty256(t.s),
    });
    if let Some(c) = t.chain_id {
        v["chainId"] = json!(qty(c));
    }
    if t.tx_type >= 1 {
        v["accessList"] = json!(t.access_list.iter().map(|a| json!({"address": a.address, "storageKeys": a.storage_keys})).collect::<Vec<_>>());
        v["yParity"] = json!(qty(t.recid as u64));
    }
    if t.tx_type == 2 {
        v["maxFeePerGas"] = json!(qty(t.gas_price as u64));
        v["maxPriorityFeePerGas"] = json!(qty(t.gas_tip as u64));
    }
    v
}

fn block_json(b: &Block, full: bool) -> Value {
    let h = &b.header;
    let txs: Vec<Value> = if full { (0..b.txs.len()).map(|i| tx_json(b, i)).collect() } else { b.txs.iter().map(|t| json!(t.hash)).collect() };
    let mut v = json!({
        "number": qty(h.number),
        "hash": b.hash,
        "parentHash": h.parent_hash,
        "nonce": hex(h.nonce.as_slice()),
        "mixHash": h.mix_digest,
        "sha3Uncles": h.uncle_hash,
        "logsBloom": hex(h.bloom.as_slice()),
        "stateRoot": h.root,
        "miner": h.coinbase,
        "difficulty": qty256(h.difficulty),
        "extraData": hex(&h.extra),
        "size": qty(b.container.len() as u64),
        "gasLimit": qty(h.gas_limit),
        "gasUsed": qty(h.gas_used),
        "timestamp": qty(h.time),
        "transactionsRoot": h.tx_hash,
        "receiptsRoot": h.receipt_hash,
        // ponytail: subnet-evm blocks carry difficulty 1 each over a genesis of 0, so
        // td = height; a chain with another genesis difficulty needs the sum.
        "totalDifficulty": qty(h.number),
        "transactions": txs,
        "uncles": [],
    });
    if let Some(f) = h.base_fee {
        v["baseFeePerGas"] = json!(qty256(f));
    }
    if let Some(c) = h.block_gas_cost {
        v["blockGasCost"] = json!(qty256(c));
    }
    if let Some(x) = h.blob_gas_used {
        v["blobGasUsed"] = json!(qty(x));
    }
    if let Some(x) = h.excess_blob_gas {
        v["excessBlobGas"] = json!(qty(x));
    }
    if let Some(x) = h.parent_beacon_root {
        v["parentBeaconBlockRoot"] = json!(x);
    }
    v
}

impl NodeEngine {
    fn block_at(&self, h: u64) -> Result<Option<Arc<Block>>, RpcError> {
        if h == 0 {
            return Ok(Some(self.genesis.clone()));
        }
        let head = self.head.lock().unwrap().clone();
        if h > head.height {
            return Ok(None);
        }
        if h == head.height {
            return Ok(Some(head));
        }
        let id = self.block_id_at_height_engine(h).ok_or("block not found")?;
        Ok(<Self as crate::tree::Engine>::get_block(self, &id))
    }

    fn block_id_at_height_engine(&self, h: u64) -> Option<crate::tree::Id> {
        <Self as crate::tree::Engine>::block_id_at_height(self, h)
    }

    /// The head, and the check that a block tag means it (the interim store
    /// has no historical state).
    fn latest(&self, tag: Option<&Value>) -> Result<Arc<Block>, RpcError> {
        let head = self.head.lock().unwrap().clone();
        match tag {
            None => Ok(head),
            Some(Value::String(s)) if matches!(s.as_str(), "latest" | "pending" | "safe" | "finalized") => Ok(head),
            Some(v) => {
                let n = match v {
                    Value::Object(o) => o.get("blockNumber").map(parse_qty).transpose()?.ok_or_else(|| invalid("blockHash lookups are not supported"))?,
                    v => parse_qty(v)?,
                };
                if n == head.height {
                    Ok(head)
                } else {
                    Err(format!("historical state is not available (head {}, asked {n})", head.height).into())
                }
            }
        }
    }

    fn call_msg(&self, o: &Value, head: &Block) -> Result<CallMsg, RpcError> {
        let o = o.as_object().ok_or_else(|| invalid("call object expected"))?;
        let field = |k: &str| o.get(k).filter(|v| !v.is_null());
        let data = match field("input").or_else(|| field("data")) {
            Some(v) => v.as_str().ok_or_else(|| invalid("data"))?.parse::<Bytes>().map_err(|e| invalid(e.to_string()))?,
            None => Bytes::new(),
        };
        Ok(CallMsg {
            from: match field("from") {
                Some(v) => parse_addr(Some(v))?,
                None => Address::ZERO,
            },
            to: field("to").map(|v| parse_addr(Some(v))).transpose()?,
            gas: match field("gas") {
                Some(v) => parse_qty(v)?,
                None => head.header.gas_limit,
            }
            .min(RPC_GAS_CAP),
            gas_price: match field("gasPrice").or_else(|| field("maxFeePerGas")) {
                Some(v) => parse_qty(v)? as u128,
                None => 0,
            },
            value: match field("value") {
                Some(v) => parse_u256(v)?,
                None => U256::ZERO,
            },
            data,
        })
    }

    fn call(&self, method: &str, params: &[Value]) -> Result<Value, RpcError> {
        match method {
            "eth_chainId" => Ok(json!(qty(self.chain_id))),
            "eth_blockNumber" => Ok(json!(qty(self.head.lock().unwrap().height))),
            "eth_getBlockByNumber" => {
                let tag = params.first().and_then(Value::as_str).ok_or_else(|| invalid("missing block tag"))?;
                let full = params.get(1).and_then(Value::as_bool).unwrap_or(false);
                let head = self.head.lock().unwrap().height;
                let h = match tag {
                    "latest" | "pending" | "safe" | "finalized" => head,
                    "earliest" => 0,
                    _ => parse_qty(&params[0])?,
                };
                Ok(self.block_at(h)?.map(|b| block_json(&b, full)).unwrap_or(Value::Null))
            }
            "eth_getBlockByHash" => {
                let id = parse_hash(params.first())?;
                let full = params.get(1).and_then(Value::as_bool).unwrap_or(false);
                Ok(<Self as crate::tree::Engine>::get_block(self, &id.0).map(|b| block_json(&b, full)).unwrap_or(Value::Null))
            }
            "eth_getBalance" | "eth_getTransactionCount" | "eth_getCode" => {
                let addr = parse_addr(params.first())?;
                self.latest(params.get(1))?;
                let mut g = self.inner.lock().unwrap();
                let db = g.ex.db_mut();
                let acct = db.basic(addr).unwrap();
                Ok(match method {
                    "eth_getBalance" => json!(qty256(acct.map(|a| a.balance).unwrap_or_default())),
                    "eth_getTransactionCount" => json!(qty(acct.map(|a| a.nonce).unwrap_or(0))),
                    _ => json!(match acct {
                        Some(a) if !a.is_empty_code_hash() => hex(&db.code_by_hash(a.code_hash).unwrap().original_bytes()),
                        _ => "0x".to_string(),
                    }),
                })
            }
            "eth_getStorageAt" => {
                let addr = parse_addr(params.first())?;
                let slot = parse_u256(params.get(1).ok_or_else(|| invalid("missing slot"))?)?;
                self.latest(params.get(2))?;
                let v = self.inner.lock().unwrap().ex.db_mut().storage(addr, slot).unwrap();
                Ok(json!(B256::from(v)))
            }
            "eth_call" => {
                let head = self.latest(params.get(1))?;
                let msg = self.call_msg(params.first().ok_or_else(|| invalid("missing call object"))?, &head)?;
                let r = self.inner.lock().unwrap().ex.call(&head.header, &msg).map_err(|e| call_error(e, msg.gas))?;
                if r.revert {
                    return Err(revert_error(&r.output));
                }
                if let Some(h) = r.halt {
                    return Err(format!("execution halted: {h}").into());
                }
                Ok(json!(hex(&r.output)))
            }
            "eth_estimateGas" => {
                let head = self.latest(params.get(1))?;
                let mut msg = self.call_msg(params.first().ok_or_else(|| invalid("missing call object"))?, &head)?;
                let mut g = self.inner.lock().unwrap();
                // gasestimator.Estimate: hi = the message's gas (or the cap), capped by the
                // sender's balance at a non-zero gas price; the run at hi must succeed.
                let mut lo;
                let mut hi = msg.gas;
                if msg.gas_price > 0 {
                    let bal = g.ex.db_mut().basic(msg.from).unwrap().map(|a| a.balance).unwrap_or_default();
                    let avail = bal.saturating_sub(msg.value) / U256::from(msg.gas_price);
                    if avail < U256::from(hi) {
                        hi = avail.to::<u64>();
                    }
                }
                // A plain transfer to an account without code: 21000 is tried first.
                let plain = msg.data.is_empty() && msg.to.is_some_and(|to| !g.ex.db_mut().basic(to).unwrap().is_some_and(|a| !a.is_empty_code_hash()));
                let mut run = |gas: u64| -> Result<(bool, exec::CallOut), RpcError> {
                    msg.gas = gas;
                    let r = g.ex.call(&head.header, &msg).map_err(|e| call_error(e, gas))?;
                    let failed = r.revert || r.halt.is_some();
                    Ok((failed, r))
                };
                if plain && hi >= TX_GAS && !run(TX_GAS)?.0 {
                    return Ok(json!(qty(TX_GAS)));
                }
                let (failed, r) = run(hi)?;
                if failed {
                    if r.revert {
                        return Err(revert_error(&r.output));
                    }
                    return Err(format!("gas required exceeds allowance ({hi})").into());
                }
                // The unconstrained run's gas lower-bounds the limit; no refunds under subnet-evm.
                lo = r.gas_used - 1;
                let optimistic = (r.gas_used + CALL_STIPEND) * 64 / 63;
                if optimistic < hi {
                    if run(optimistic)?.0 {
                        lo = optimistic;
                    } else {
                        hi = optimistic;
                    }
                }
                while lo + 1 < hi {
                    if ((hi - lo) as f64) / (hi as f64) < ESTIMATE_ERROR_RATIO {
                        break;
                    }
                    let mut mid = (hi + lo) / 2;
                    if mid > lo * 2 {
                        mid = lo * 2;
                    }
                    if run(mid)?.0 {
                        lo = mid;
                    } else {
                        hi = mid;
                    }
                }
                Ok(json!(qty(hi)))
            }
            _ => Err(RpcError { code: -32601, message: format!("the method {method} does not exist/is not available"), data: None }),
        }
    }
}

/// One request body in, one response body out (single or batch).
pub fn handle(e: &NodeEngine, body: &[u8]) -> Vec<u8> {
    let req: Value = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(err) => return json!({"jsonrpc": "2.0", "id": null, "error": {"code": -32700, "message": err.to_string()}}).to_string().into_bytes(),
    };
    let one = |r: &Value| {
        let id = r.get("id").cloned().unwrap_or(Value::Null);
        let method = r.get("method").and_then(Value::as_str).unwrap_or("");
        let params = r.get("params").and_then(Value::as_array).cloned().unwrap_or_default();
        match e.call(method, &params) {
            Ok(v) => json!({"jsonrpc": "2.0", "id": id, "result": v}),
            Err(err) => {
                let mut ev = json!({"code": err.code, "message": err.message});
                if let Some(d) = err.data {
                    ev["data"] = d;
                }
                json!({"jsonrpc": "2.0", "id": id, "error": ev})
            }
        }
    };
    let out = match &req {
        Value::Array(rs) => Value::Array(rs.iter().map(one).collect()),
        r => one(r),
    };
    out.to_string().into_bytes()
}

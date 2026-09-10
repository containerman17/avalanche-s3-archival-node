//! Hex shapes (hexutil) and the coreth internal/ethapi JSON of blocks,
//! transactions, receipts and logs, duplicated field for field so the
//! answers byte-match stock subnet-evm.
use alloy_primitives::{Address, Bloom, Bytes, B256, U256};
use block::{Block, Tx};
use serde_json::{json, Value};

use crate::{invalid, Log, Receipt, RpcError};

pub fn qty(v: u64) -> String {
    format!("0x{v:x}")
}

pub fn qty128(v: u128) -> String {
    format!("0x{v:x}")
}

pub fn qty256(v: U256) -> String {
    format!("0x{v:x}")
}

pub fn hex(b: &[u8]) -> String {
    format!("0x{}", alloy_primitives::hex::encode(b))
}

pub fn parse_qty(v: &Value) -> Result<u64, RpcError> {
    match v {
        Value::Number(n) => n.as_u64().ok_or_else(|| invalid("expected a hex quantity")),
        Value::String(s) => {
            let h = s.strip_prefix("0x").ok_or_else(|| invalid("hex string without 0x prefix"))?;
            if h.is_empty() {
                return Err(invalid("hex string \"0x\""));
            }
            if h.len() > 1 && h.starts_with('0') {
                return Err(invalid("hex number with leading zero digits"));
            }
            if h.len() > 16 {
                return Err(invalid("hex number > 64 bits"));
            }
            u64::from_str_radix(h, 16).map_err(|_| invalid("invalid hex string"))
        }
        _ => Err(invalid("expected a hex quantity")),
    }
}

pub fn parse_u256(v: &Value) -> Result<U256, RpcError> {
    let s = v.as_str().ok_or_else(|| invalid("expected a hex quantity"))?;
    let h = s.strip_prefix("0x").ok_or_else(|| invalid("hex string without 0x prefix"))?;
    if h.is_empty() {
        return Err(invalid("hex string \"0x\""));
    }
    if h.len() > 1 && h.starts_with('0') {
        return Err(invalid("hex number with leading zero digits"));
    }
    if h.len() > 64 {
        return Err(invalid("hex number > 256 bits"));
    }
    U256::from_str_radix(h, 16).map_err(|_| invalid("invalid hex string"))
}

pub fn parse_bytes(v: &Value) -> Result<Bytes, RpcError> {
    v.as_str().ok_or_else(|| invalid("expected hex bytes"))?.parse().map_err(|e| invalid(format!("{e}")))
}

/// common.Address UnmarshalJSON: exactly 40 hex digits behind 0x.
pub fn parse_addr(v: Option<&Value>) -> Result<Address, RpcError> {
    let s = v.and_then(Value::as_str).ok_or_else(|| invalid("json: cannot unmarshal non-string into Go value of type common.Address"))?;
    let h = s.strip_prefix("0x").ok_or_else(|| invalid("hex string without 0x prefix"))?;
    if h.len() != 40 {
        return Err(invalid(format!("hex string has length {}, want 40 for common.Address", h.len())));
    }
    s.parse().map_err(|_| invalid("invalid hex string"))
}

/// common.Hash UnmarshalJSON: exactly 64 hex digits behind 0x.
pub fn parse_hash(v: Option<&Value>) -> Result<B256, RpcError> {
    let s = v.and_then(Value::as_str).ok_or_else(|| invalid("json: cannot unmarshal non-string into Go value of type common.Hash"))?;
    let h = s.strip_prefix("0x").ok_or_else(|| invalid("hex string without 0x prefix"))?;
    if h.len() != 64 {
        return Err(invalid(format!("hex string has length {}, want 64 for common.Hash", h.len())));
    }
    s.parse().map_err(|_| invalid("invalid hex string"))
}

/// A hash param from a 32-byte hex, geth's common.Hash UnmarshalJSON (exact length).
pub fn parse_hash32(v: Option<&Value>) -> Result<B256, RpcError> {
    let s = v.and_then(Value::as_str).ok_or_else(|| invalid("missing hash"))?;
    if s.len() != 66 {
        return Err(invalid(format!("hex string has length {}, want 64 for common.Hash", s.len().saturating_sub(2))));
    }
    s.parse().map_err(|_| invalid("invalid hash"))
}

/// The tx's effective gas price in its block (pre-AP3: the gas price).
pub fn effective_gas_price(t: &Tx, base_fee: Option<U256>) -> u128 {
    match base_fee {
        None => t.gas_price,
        Some(b) => {
            let b = b.to::<u128>();
            if t.tx_type == 2 {
                (t.gas_tip + b).min(t.gas_price)
            } else {
                t.gas_price
            }
        }
    }
}

/// newRPCTransaction for a mined tx.
pub fn tx_json(b: &Block, i: usize) -> Result<Value, RpcError> {
    let t = &b.txs[i];
    let from = t.sender.ok_or_else(|| bad_sender(t, b.height))?;
    let base = b.header.base_fee;
    let gas_price = if t.tx_type == 2 { effective_gas_price(t, base) } else { t.gas_price };
    let mut v = json!({
        "blockHash": b.hash,
        "blockNumber": qty(b.height),
        "from": from,
        "gas": qty(t.gas_limit),
        "gasPrice": qty128(gas_price),
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
    match t.tx_type {
        0 => {
            if let Some(c) = t.chain_id.filter(|c| *c != 0) {
                v["chainId"] = json!(qty(c));
            }
        }
        _ => {
            v["accessList"] = json!(t.access_list.iter().map(|a| json!({"address": a.address, "storageKeys": a.storage_keys})).collect::<Vec<_>>());
            v["chainId"] = json!(qty(t.chain_id.unwrap_or(0)));
            v["yParity"] = json!(qty(t.v.min(1)));
            if t.tx_type == 2 {
                v["maxFeePerGas"] = json!(qty128(t.gas_price));
                v["maxPriorityFeePerGas"] = json!(qty128(t.gas_tip));
            }
        }
    }
    Ok(v)
}

/// newRPCPendingTransaction: a pool tx (no block fields; gasPrice is the
/// fee cap, there is no base fee yet).
pub fn pending_tx_json(t: &Tx) -> Value {
    let mut v = json!({
        "blockHash": Value::Null,
        "blockNumber": Value::Null,
        "from": t.sender.unwrap_or_default(),
        "gas": qty(t.gas_limit),
        "gasPrice": qty128(t.gas_price),
        "hash": t.hash,
        "input": hex(&t.input),
        "nonce": qty(t.nonce),
        "to": t.to,
        "transactionIndex": Value::Null,
        "value": qty256(t.value),
        "type": qty(t.tx_type as u64),
        "v": qty(t.v),
        "r": qty256(t.r),
        "s": qty256(t.s),
    });
    match t.tx_type {
        0 => {
            if let Some(c) = t.chain_id.filter(|c| *c != 0) {
                v["chainId"] = json!(qty(c));
            }
        }
        _ => {
            v["accessList"] = json!(t.access_list.iter().map(|a| json!({"address": a.address, "storageKeys": a.storage_keys})).collect::<Vec<_>>());
            v["chainId"] = json!(qty(t.chain_id.unwrap_or(0)));
            v["yParity"] = json!(qty(t.v.min(1)));
            if t.tx_type == 2 {
                v["maxFeePerGas"] = json!(qty128(t.gas_price));
                v["maxPriorityFeePerGas"] = json!(qty128(t.gas_tip));
            }
        }
    }
    v
}

pub fn bad_sender(t: &Tx, h: u64) -> RpcError {
    format!("sender of tx {} in block {h} does not recover (corrupt container, or this node's chain id is not the one it was signed for)", t.hash).into()
}

/// The RLP element of a tx as it sits in a block's tx list (a typed tx is a
/// string wrapping its envelope).
fn tx_element_len(t: &Tx) -> usize {
    if t.tx_type == 0 {
        t.raw.len()
    } else {
        alloy_rlp::Header { list: false, payload_length: t.raw.len() }.length() + t.raw.len()
    }
}

/// The eth block RLP [header, txs, uncles] (what debug_getRawBlock hands out).
pub fn block_rlp(b: &Block) -> Vec<u8> {
    let txs: usize = b.txs.iter().map(tx_element_len).sum();
    let body = b.header_rlp.len() + alloy_rlp::Header { list: true, payload_length: txs }.length() + txs + 1;
    let mut out = Vec::with_capacity(body + 4);
    alloy_rlp::Header { list: true, payload_length: body }.encode(&mut out);
    out.extend_from_slice(&b.header_rlp);
    alloy_rlp::Header { list: true, payload_length: txs }.encode(&mut out);
    for t in &b.txs {
        if t.tx_type != 0 {
            alloy_rlp::Header { list: false, payload_length: t.raw.len() }.encode(&mut out);
        }
        out.extend_from_slice(&t.raw);
    }
    out.push(0xc0);
    out
}

/// types.Block.Size(): the RLP size of [header, txs, uncles].
pub fn block_size(b: &Block) -> u64 {
    let txs: usize = b.txs.iter().map(tx_element_len).sum();
    let body = b.header_rlp.len() + alloy_rlp::Header { list: true, payload_length: txs }.length() + txs + 1;
    (alloy_rlp::Header { list: true, payload_length: body }.length() + body) as u64
}

pub fn header_fields(b: &Block) -> Value {
    let h = &b.header;
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
        "gasLimit": qty(h.gas_limit),
        "gasUsed": qty(h.gas_used),
        "timestamp": qty(h.time),
        "transactionsRoot": h.tx_hash,
        "receiptsRoot": h.receipt_hash,
        // subnet-evm: difficulty 1 per block over a genesis of 0, so td = height.
        "totalDifficulty": qty(h.number),
    });
    if let Some(f) = h.base_fee {
        v["baseFeePerGas"] = json!(qty256(f));
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
    // customtypes HeaderExtra.PostRPCMarshal.
    if let Some(c) = h.block_gas_cost {
        v["blockGasCost"] = json!(qty256(c));
    }
    if let Some(x) = h.time_milliseconds {
        v["timestampMilliseconds"] = json!(qty(x));
    }
    if let Some(x) = h.min_delay_excess {
        v["minDelayExcess"] = json!(qty(x));
    }
    v
}

pub fn block_json(b: &Block, full: bool) -> Result<Value, RpcError> {
    let mut v = header_fields(b);
    v["size"] = json!(qty(block_size(b)));
    v["uncles"] = json!([]);
    v["transactions"] = if full { Value::Array((0..b.txs.len()).map(|i| tx_json(b, i)).collect::<Result<_, _>>()?) } else { json!(b.txs.iter().map(|t| t.hash).collect::<Vec<_>>()) };
    Ok(v)
}

pub fn log_bloom(logs: &[Log]) -> Bloom {
    let mut bloom = Bloom::default();
    for l in logs {
        bloom.accrue(alloy_primitives::BloomInput::Raw(l.address.as_slice()));
        for t in &l.topics {
            bloom.accrue(alloy_primitives::BloomInput::Raw(t.as_slice()));
        }
    }
    bloom
}

pub fn log_json(l: &Log, b: &Block, tx_index: usize, log_index: u64, removed: bool) -> Value {
    json!({
        "address": l.address,
        "topics": l.topics,
        "data": hex(&l.data),
        "blockNumber": qty(b.height),
        "transactionHash": b.txs[tx_index].hash,
        "transactionIndex": qty(tx_index as u64),
        "blockHash": b.hash,
        "logIndex": qty(log_index),
        "removed": removed,
    })
}

/// marshalReceipt; `first_log` is the block-wide index of the tx's first log.
pub fn receipt_json(b: &Block, i: usize, r: &Receipt, first_log: u64) -> Result<Value, RpcError> {
    let t = &b.txs[i];
    let from = t.sender.ok_or_else(|| bad_sender(t, b.height))?;
    let logs: Vec<Value> = r.logs.iter().enumerate().map(|(k, l)| log_json(l, b, i, first_log + k as u64, false)).collect();
    let mut v = json!({
        "blockHash": b.hash,
        "blockNumber": qty(b.height),
        "transactionHash": t.hash,
        "transactionIndex": qty(i as u64),
        "from": from,
        "to": t.to,
        "gasUsed": qty(r.gas_used),
        "cumulativeGasUsed": qty(r.cumulative_gas_used),
        "contractAddress": Value::Null,
        "logs": logs,
        "logsBloom": hex(log_bloom(&r.logs).as_slice()),
        "type": qty(t.tx_type as u64),
        "effectiveGasPrice": qty128(effective_gas_price(t, b.header.base_fee)),
        "status": qty(r.status),
    });
    if t.to.is_none() {
        v["contractAddress"] = json!(from.create(t.nonce));
    }
    Ok(v)
}

//! debug_ methods: the tracers (stored callTracer frames for the plain
//! callTracer, re-execution for everything else), the raw getters,
//! printBlock, getAccessibleState.
use alloy_primitives::{Bytes, B256};
use block::Block;
use serde_json::{json, Value};

use crate::call::{parse_trace_config, TraceCfg};
use crate::json::*;
use crate::{invalid, RpcError, RpcResult, Server};

pub fn dispatch(s: &Server, method: &str, params: &[Value]) -> Option<RpcResult> {
    Some(match method {
        "eth_call" => s.eth_call(params),
        "eth_estimateGas" => s.estimate_gas(params),
        "eth_callDetailed" => s.call_detailed(params),
        "eth_createAccessList" => s.create_access_list(params),
        "debug_traceCall" => s.debug_trace_call(params),
        "debug_traceTransaction" => s.debug_trace_transaction(params),
        "debug_traceBlockByNumber" => s.debug_trace_block(params),
        "debug_traceBlockByHash" => s.by_hash(params, |s, p| s.debug_trace_block(p), true),
        "debug_traceBlock" => s.debug_trace_block_rlp(params),
        "debug_getRawBlock" => s.debug_get_raw_block(params),
        "debug_getRawHeader" => s.debug_get_raw_header(params),
        "debug_getRawTransaction" => s.debug_get_raw_transaction(params),
        "debug_getRawReceipts" => s.debug_get_raw_receipts(params),
        "debug_printBlock" => s.print_block(params),
        "debug_getAccessibleState" => s.get_accessible_state(params),
        _ => return None,
    })
}

/// The plain callTracer (no onlyTopCall, no withLog): the stored frames answer.
fn is_plain_call_tracer(cfg: &TraceCfg) -> bool {
    if cfg.tracer != "callTracer" {
        return false;
    }
    match &cfg.trace {
        exec::Trace::Call(c) => !c.only_top_call.unwrap_or(false) && !c.with_log.unwrap_or(false),
        _ => false,
    }
}

impl Server {
    fn stored_traces(&self, b: &Block) -> Result<Vec<Value>, RpcError> {
        if b.txs.is_empty() {
            return Ok(Vec::new());
        }
        let ts = self.store.traces(b.height)?.ok_or_else(|| RpcError::from(format!("block {} has no stored frames", b.height)))?;
        if ts.len() != b.txs.len() {
            return Err(format!("block {} holds {} transactions but {} traces are stored", b.height, b.txs.len(), ts.len()).into());
        }
        ts.iter().map(|t| serde_json::from_str(t).map_err(|e| RpcError::from(format!("stored trace: {e}")))).collect()
    }

    /// Every tx's trace of b under cfg.
    pub fn traces_of(&self, b: &Block, cfg: &TraceCfg) -> Result<Vec<Value>, RpcError> {
        if is_plain_call_tracer(cfg) {
            return self.stored_traces(b);
        }
        self.trace_block(b, cfg)
    }

    pub fn debug_trace_transaction(&self, params: &[Value]) -> RpcResult {
        let hash = parse_hash32(params.first()).map_err(|e| invalid(format!("bad tx hash: {}", e.message)))?;
        let cfg = parse_trace_config(params.get(1))?;
        let Some((b, i)) = self.find_tx(&hash)? else { return Err(format!("transaction {hash} not found").into()) };
        let mut ts = self.traces_of(&b, &cfg)?;
        Ok(ts.swap_remove(i))
    }

    pub fn debug_trace_block(&self, params: &[Value]) -> RpcResult {
        if params.is_empty() {
            return Err(invalid("need [blockTag, traceConfig]"));
        }
        let n = self.block_number(params.first())?;
        let b = self.block_at(n)?;
        let cfg = parse_trace_config(params.get(1))?;
        let ts = self.traces_of(&b, &cfg)?;
        Ok(Value::Array(ts.into_iter().enumerate().map(|(i, r)| json!({"txHash": b.txs[i].hash, "result": r})).collect()))
    }

    /// debug_traceBlock(rlp): the block by its hash, which must be stored.
    fn debug_trace_block_rlp(&self, params: &[Value]) -> RpcResult {
        let raw = parse_bytes(params.first().ok_or_else(|| invalid("need [blockRlp, traceConfig]"))?)?;
        let b = block::decode_container(raw.into()).map_err(|e| invalid(format!("could not decode block: {e}")))?;
        let Some(n) = self.store.height_by_hash(&b.hash)? else { return Err(format!("block {} is not on this chain", b.hash).into()) };
        let mut p = params.to_vec();
        p[0] = json!(qty(n));
        self.debug_trace_block(&p)
    }

    fn debug_get_raw_block(&self, params: &[Value]) -> RpcResult {
        if params.is_empty() {
            return Err(invalid("need [blockTag]"));
        }
        let n = self.block_number(params.first())?;
        if n == 0 {
            return Ok(json!(hex(&self.genesis.container)));
        }
        let c = self.store.container(n)?.ok_or_else(|| RpcError::from(format!("block {n} is not stored")))?;
        Ok(json!(hex(&c)))
    }

    fn debug_get_raw_header(&self, params: &[Value]) -> RpcResult {
        if params.is_empty() {
            return Err(invalid("need [blockTag]"));
        }
        let n = self.block_number(params.first())?;
        Ok(json!(hex(&self.block_at(n)?.header_rlp)))
    }

    fn debug_get_raw_transaction(&self, params: &[Value]) -> RpcResult {
        let hash = parse_hash32(params.first()).map_err(|e| invalid(format!("bad tx hash: {}", e.message)))?;
        match self.find_tx(&hash)? {
            None => Ok(json!("0x")),
            Some((b, i)) => Ok(json!(hex(&b.txs[i].raw))),
        }
    }

    /// The consensus receipts (typed envelope RLP) from the stored rows.
    fn debug_get_raw_receipts(&self, params: &[Value]) -> RpcResult {
        if params.is_empty() {
            return Err(invalid("need [blockTag]"));
        }
        let n = self.block_number(params.first())?;
        let b = self.block_at(n)?;
        if n == 0 || b.txs.is_empty() {
            return Ok(json!([]));
        }
        let (rs, _) = self.block_receipts(&b)?;
        let out: Vec<String> = rs
            .iter()
            .enumerate()
            .map(|(i, r)| {
                let logs = r.logs.iter().map(|l| alloy_primitives::Log::new_unchecked(l.address, l.topics.clone(), l.data.clone())).collect();
                let rc = alloy_consensus::Receipt { status: alloy_consensus::Eip658Value::Eip658(r.status == 1), cumulative_gas_used: r.cumulative_gas_used, logs }.with_bloom();
                let env = alloy_consensus::ReceiptEnvelope::from_typed(alloy_consensus::TxType::try_from(b.txs[i].tx_type).unwrap_or(alloy_consensus::TxType::Legacy), rc);
                let mut buf = Vec::new();
                alloy_eips::eip2718::Encodable2718::encode_2718(&env, &mut buf);
                hex(&buf)
            })
            .collect();
        Ok(json!(out))
    }

    /// A block number spelled as a JSON number (geth's debug_printBlock,
    /// debug_getAccessibleState), a tag accepted too.
    fn block_number_param(&self, v: Option<&Value>) -> Result<u64, RpcError> {
        self.block_number(v)
    }

    /// coreth's spew dump of the parsed block; this node prints the same
    /// fields in a fixed text (a spew-identical dump is not reproducible).
    fn print_block(&self, params: &[Value]) -> RpcResult {
        let n = self.block_number_param(params.first())?;
        let b = self.block_at(n)?;
        let h = &b.header;
        let mut s = format!(
            "Block {} (hash {}):\n  parentHash {}\n  coinbase {}\n  root {}\n  txHash {}\n  receiptHash {}\n  difficulty {}\n  gasLimit {}\n  gasUsed {}\n  time {}\n  extra {}\n  baseFee {:?}\n  transactions {}\n",
            h.number, b.hash, h.parent_hash, h.coinbase, h.root, h.tx_hash, h.receipt_hash, h.difficulty, h.gas_limit, h.gas_used, h.time, hex(&h.extra), h.base_fee, b.txs.len()
        );
        for t in &b.txs {
            s.push_str(&format!("    {} type {} nonce {} to {:?} value {} gas {}\n", t.hash, t.tx_type, t.nonce, t.to, t.value, t.gas_limit));
        }
        Ok(json!(s))
    }

    fn get_accessible_state(&self, params: &[Value]) -> RpcResult {
        if params.len() < 2 {
            return Err(invalid("need [fromBlock, toBlock]"));
        }
        let from = self.block_number_param(params.first())?;
        let to = self.block_number_param(params.get(1))?;
        let first = if from > to { from } else { from.min(to) };
        Ok(json!(qty(first)))
    }
}

#[allow(dead_code)]
fn _unused(_: Bytes, _: B256) {}

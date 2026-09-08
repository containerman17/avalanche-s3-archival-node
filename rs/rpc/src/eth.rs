//! eth_ methods over the store: blocks, transactions, receipts, logs, the
//! fee surface and state reads. Executing methods are in call.rs.
use std::sync::Arc;

use alloy_primitives::{Address, B256, U256};
use block::Block;
use serde_json::{json, Value};

use crate::json::*;
use crate::{invalid, Log, RpcError, RpcResult, Server};

/// eth_getLogs range cap (blocks per query).
pub const GET_LOGS_MAX_RANGE: u64 = 10_000;

pub fn dispatch(s: &Server, method: &str, params: &[Value]) -> Option<RpcResult> {
    let p = |i: usize| params.get(i).filter(|v| !v.is_null());
    Some(match method {
        "eth_getBlockByNumber" => s.get_block_by_number(params),
        "eth_getBlockByHash" => s.by_hash(params, |s, p| s.get_block_by_number(p), false),
        "eth_getHeaderByNumber" => s.get_header_by_number(params),
        "eth_getHeaderByHash" => s.by_hash(params, |s, p| s.get_header_by_number(p), false),
        "eth_getBlockTransactionCountByNumber" => s.block_tx_count(params),
        "eth_getBlockTransactionCountByHash" => s.by_hash(params, |s, p| s.block_tx_count(p), false),
        "eth_getTransactionByBlockNumberAndIndex" => s.tx_by_block_and_index(params),
        "eth_getTransactionByBlockHashAndIndex" => s.by_hash(params, |s, p| s.tx_by_block_and_index(p), false),
        "eth_getRawTransactionByBlockNumberAndIndex" => s.raw_tx_by_block_and_index(params),
        "eth_getRawTransactionByBlockHashAndIndex" => s.by_hash(params, |s, p| s.raw_tx_by_block_and_index(p), false),
        "eth_getTransactionByHash" => s.get_transaction_by_hash(params),
        "eth_getRawTransactionByHash" => s.raw_tx_by_hash(params),
        "eth_getTransactionReceipt" => s.get_transaction_receipt(params),
        "eth_getBlockReceipts" => s.get_block_receipts(params),
        "eth_getLogs" => s.get_logs(params),
        "eth_getBalance" | "eth_getTransactionCount" | "eth_getCode" => s.account_field(method, params),
        "eth_getStorageAt" => s.get_storage_at(params),
        "eth_gasPrice" => s.gas_oracle().map(|v| json!(qty128(v))),
        "eth_maxPriorityFeePerGas" => s.suggest_tip().map(|v| json!(qty128(v))),
        "eth_baseFee" => s.base_fee_at(s.head()).map(|v| json!(qty128(v.unwrap_or(0)))),
        "eth_feeHistory" => s.fee_history(params),
        "eth_suggestPriceOptions" => s.suggest_price_options(),
        "eth_getChainConfig" | "debug_chainConfig" => Ok(s.chain_config.clone()),
        _ => {
            let _ = p;
            return None;
        }
    })
}

impl Server {
    /// *ByHash = its *ByNumber twin behind one blkh lookup; an unknown hash is
    /// null (strict: an error, debug_traceBlockByHash).
    pub fn by_hash(&self, params: &[Value], f: impl FnOnce(&Server, &[Value]) -> RpcResult, strict: bool) -> RpcResult {
        let hash = parse_hash(params.first()).map_err(|e| invalid(format!("bad block hash: {}", e.message)))?;
        let Some(n) = self.store.height_by_hash(&hash)? else {
            if strict {
                return Err(format!("block {hash} is not on this chain").into());
            }
            return Ok(Value::Null);
        };
        let mut p = params.to_vec();
        p[0] = json!(qty(n));
        f(self, &p)
    }

    fn full_tx_flag(params: &[Value], i: usize) -> Result<bool, RpcError> {
        match params.get(i) {
            None => Ok(false),
            Some(Value::Bool(b)) => Ok(*b),
            Some(_) => Err(invalid("bad fullTx flag")),
        }
    }

    fn get_block_by_number(&self, params: &[Value]) -> RpcResult {
        if params.is_empty() {
            return Err(invalid("need [blockTag, fullTx]"));
        }
        let n = match self.block_number(params.first()) {
            Ok(n) => n,
            // stock subnet-evm: a number past the head is null, not an error
            Err(e) if e.message.contains("beyond head") => return Ok(Value::Null),
            Err(e) => return Err(e),
        };
        let full = Self::full_tx_flag(params, 1)?;
        let b = self.block_at(n)?;
        block_json(&b, full)
    }

    fn get_header_by_number(&self, params: &[Value]) -> RpcResult {
        if params.is_empty() {
            return Err(invalid("need [blockTag]"));
        }
        let n = match self.block_number(params.first()) {
            Ok(n) => n,
            Err(e) if e.message.contains("beyond head") => return Ok(Value::Null),
            Err(e) => return Err(e),
        };
        let b = self.block_at(n)?;
        Ok(header_fields(&b))
    }

    fn block_param(&self, params: &[Value]) -> Result<Arc<Block>, RpcError> {
        if params.is_empty() {
            return Err(invalid("need [block...]"));
        }
        let n = self.block_number(params.first())?;
        self.block_at(n)
    }

    fn block_tx_count(&self, params: &[Value]) -> RpcResult {
        if params.is_empty() {
            return Err(invalid("need [block...]"));
        }
        let n = match self.block_number(params.first()) {
            Ok(n) => n,
            Err(e) if e.message.contains("beyond head") => return Ok(Value::Null),
            Err(e) => return Err(e),
        };
        Ok(json!(qty(self.block_at(n)?.txs.len() as u64)))
    }

    fn tx_index_param(params: &[Value]) -> Result<u64, RpcError> {
        let v = params.get(1).ok_or_else(|| invalid("need [block, txIndex]"))?;
        parse_qty(v).map_err(|e| invalid(format!("bad tx index: {}", e.message)))
    }

    fn tx_by_block_and_index(&self, params: &[Value]) -> RpcResult {
        let n = match self.block_number(params.first()) {
            Ok(n) => n,
            Err(e) if e.message.contains("beyond head") => return Ok(Value::Null),
            Err(e) => return Err(e),
        };
        let b = self.block_at(n)?;
        let i = Self::tx_index_param(params)? as usize;
        if i >= b.txs.len() {
            return Ok(Value::Null);
        }
        tx_json(&b, i)
    }

    fn raw_tx_by_block_and_index(&self, params: &[Value]) -> RpcResult {
        let b = self.block_param(params)?;
        let i = Self::tx_index_param(params)? as usize;
        if i >= b.txs.len() {
            return Ok(Value::Null);
        }
        Ok(json!(hex(&b.txs[i].raw)))
    }

    /// (block, index) of a tx hash; None = unknown tx.
    pub fn find_tx(&self, hash: &B256) -> Result<Option<(Arc<Block>, usize)>, RpcError> {
        let Some((h, i)) = self.store.tx_by_hash(hash)? else { return Ok(None) };
        let b = self.block_at(h)?;
        if i >= b.txs.len() || b.txs[i].hash != *hash {
            return Err(format!("tx {hash} is indexed at block {h} index {i}, which holds another tx").into());
        }
        Ok(Some((b, i)))
    }

    fn tx_hash_param(params: &[Value]) -> Result<B256, RpcError> {
        parse_hash32(params.first()).map_err(|e| invalid(format!("bad tx hash: {}", e.message)))
    }

    fn get_transaction_by_hash(&self, params: &[Value]) -> RpcResult {
        let hash = Self::tx_hash_param(params)?;
        match self.find_tx(&hash)? {
            None => Ok(Value::Null),
            Some((b, i)) => tx_json(&b, i),
        }
    }

    fn raw_tx_by_hash(&self, params: &[Value]) -> RpcResult {
        let hash = Self::tx_hash_param(params)?;
        match self.find_tx(&hash)? {
            None => Ok(Value::Null),
            Some((b, i)) => Ok(json!(hex(&b.txs[i].raw))),
        }
    }

    /// The block's stored receipts, and the block-wide log index each tx's
    /// logs start at.
    pub fn block_receipts(&self, b: &Block) -> Result<(Vec<crate::Receipt>, Vec<u64>), RpcError> {
        if b.txs.is_empty() {
            return Ok((Vec::new(), Vec::new()));
        }
        let rs = self.store.receipts(b.height)?.ok_or_else(|| format!("block {} has no stored receipts: this node never executed it", b.height))?;
        if rs.len() != b.txs.len() {
            return Err(format!("block {} holds {} transactions but {} receipts are stored", b.height, b.txs.len(), rs.len()).into());
        }
        let mut first = Vec::with_capacity(rs.len());
        let mut n = 0u64;
        for r in &rs {
            first.push(n);
            n += r.logs.len() as u64;
        }
        Ok((rs, first))
    }

    fn get_transaction_receipt(&self, params: &[Value]) -> RpcResult {
        let hash = Self::tx_hash_param(params)?;
        let Some((b, i)) = self.find_tx(&hash)? else { return Ok(Value::Null) };
        let (rs, first) = self.block_receipts(&b)?;
        receipt_json(&b, i, &rs[i], first[i])
    }

    fn get_block_receipts(&self, params: &[Value]) -> RpcResult {
        let n = match self.block_number(params.first()) {
            Ok(n) => n,
            Err(e) if e.message.contains("beyond head") => return Ok(Value::Null),
            Err(e) => return Err(e),
        };
        let b = self.block_at(n)?;
        let (rs, first) = self.block_receipts(&b)?;
        Ok(Value::Array((0..rs.len()).map(|i| receipt_json(&b, i, &rs[i], first[i])).collect::<Result<_, _>>()?))
    }

    // --- logs ---------------------------------------------------------------

    fn get_logs(&self, params: &[Value]) -> RpcResult {
        let f = params.first().and_then(Value::as_object).ok_or_else(|| invalid("need [filter]"))?;
        let (addrs, topics) = parse_log_matchers(f)?;
        if let Some(h) = f.get("blockHash").filter(|v| !v.is_null()) {
            if f.get("fromBlock").is_some_and(|v| !v.is_null()) || f.get("toBlock").is_some_and(|v| !v.is_null()) {
                return Err(invalid("cannot specify both BlockHash and FromBlock/ToBlock, choose one or the other"));
            }
            let hash = parse_hash(Some(h)).map_err(|e| invalid(format!("bad blockHash: {}", e.message)))?;
            let n = self.store.height_by_hash(&hash)?.ok_or_else(|| RpcError::from(format!("unknown block {hash}")))?;
            if n == 0 {
                return Ok(json!([]));
            }
            return Ok(Value::Array(self.run_get_logs(n, n, &addrs, &topics)?));
        }
        let (from, to) = self.parse_filter_range(f)?;
        Ok(Value::Array(self.run_get_logs(from, to, &addrs, &topics)?))
    }

    /// The [from, to] of a filter object, validated.
    pub fn parse_filter_range(&self, f: &serde_json::Map<String, Value>) -> Result<(u64, u64), RpcError> {
        let from = self.block_number(f.get("fromBlock"))?.max(1);
        let to = self.block_number(f.get("toBlock"))?;
        if to < from {
            return Err(invalid(format!("toBlock {to} below fromBlock {from}")));
        }
        if to - from + 1 > GET_LOGS_MAX_RANGE {
            return Err(invalid(format!("block range {} exceeds {GET_LOGS_MAX_RANGE}", to - from + 1)));
        }
        Ok((from, to))
    }

    pub fn run_get_logs(&self, from: u64, to: u64, addrs: &[Address], topics: &[Vec<B256>]) -> Result<Vec<Value>, RpcError> {
        let candidates = match self.store.log_candidates(from, to, addrs, topics)? {
            Some(c) => c,
            None => (from..=to).collect(),
        };
        let mut out = Vec::new();
        for n in candidates {
            let b = self.block_at(n)?;
            if b.txs.is_empty() || (addrs.is_empty() && topics.is_empty() && b.header.bloom.is_zero()) {
                continue;
            }
            if !bloom_may_match(&b.header.bloom, addrs, topics) {
                continue;
            }
            let (rs, first) = self.block_receipts(&b)?;
            for (i, r) in rs.iter().enumerate() {
                for (k, l) in r.logs.iter().enumerate() {
                    if log_matches(l, addrs, topics) {
                        out.push(log_json(l, &b, i, first[i] + k as u64, false));
                    }
                }
            }
        }
        Ok(out)
    }

    // --- state --------------------------------------------------------------

    fn account_field(&self, method: &str, params: &[Value]) -> RpcResult {
        if params.is_empty() {
            return Err(invalid("need [address, blockTag]"));
        }
        let addr = parse_addr(params.first()).map_err(|e| invalid(format!("bad address: {}", e.message)))?;
        let n = self.block_number(params.get(1))?;
        let mut st = self.store.state_at(n)?;
        let acct = st.account(addr)?;
        Ok(match method {
            "eth_getBalance" => json!(qty256(acct.map(|a| a.balance).unwrap_or_default())),
            "eth_getTransactionCount" => json!(qty(acct.map(|a| a.nonce).unwrap_or(0))),
            _ => json!(match acct {
                Some(a) if a.code_hash != alloy_primitives::KECCAK256_EMPTY && a.code_hash != B256::ZERO => hex(&st.code(a.code_hash)?.unwrap_or_default()),
                _ => "0x".to_string(),
            }),
        })
    }

    fn get_storage_at(&self, params: &[Value]) -> RpcResult {
        if params.len() < 2 {
            return Err(invalid("need [address, slot, blockTag]"));
        }
        let addr = parse_addr(params.first()).map_err(|e| invalid(format!("bad address: {}", e.message)))?;
        let slot_s = params[1].as_str().ok_or_else(|| invalid("bad slot"))?;
        // common.HexToHash: any hex, left-padded / left-truncated to 32 bytes.
        let raw = alloy_primitives::hex::decode(slot_s.trim_start_matches("0x")).or_else(|_| alloy_primitives::hex::decode(format!("0{}", slot_s.trim_start_matches("0x")))).map_err(|_| invalid("bad slot"))?;
        let slot = B256::left_padding_from(&raw[raw.len().saturating_sub(32)..]);
        let n = self.block_number(params.get(2))?;
        let v = self.store.state_at(n)?.storage(addr, slot.into())?;
        Ok(json!(B256::from(v)))
    }

    // --- fees ---------------------------------------------------------------

    pub fn base_fee_at(&self, n: u64) -> Result<Option<u128>, RpcError> {
        Ok(self.block_at(n)?.header.base_fee.map(|f| f.to::<u128>()))
    }

    /// The 60th percentile gas price of the txs in the last 20 non-empty
    /// blocks (up to 100 txs); the chain's minBaseFee with nothing to sample.
    pub fn gas_oracle(&self) -> Result<u128, RpcError> {
        let mut prices = Vec::new();
        let mut n = self.head();
        let mut scanned = 0;
        while n > 0 && scanned < 20 && prices.len() < 100 {
            let b = self.block_at(n)?;
            n -= 1;
            if b.txs.is_empty() {
                continue;
            }
            scanned += 1;
            prices.extend(b.txs.iter().map(|t| t.gas_price));
        }
        if prices.is_empty() {
            return Ok(self.cfg.fee_config.min_base_fee.to::<u128>());
        }
        prices.sort_unstable();
        Ok(prices[prices.len() * 60 / 100])
    }

    pub fn suggest_tip(&self) -> Result<u128, RpcError> {
        let price = self.gas_oracle()?;
        Ok(match self.base_fee_at(self.head())? {
            None => price,
            Some(b) => price.saturating_sub(b),
        })
    }

    fn suggest_price_options(&self) -> RpcResult {
        let tip = self.suggest_tip()?;
        let head = self.head();
        let Some(next) = self.next_base_fee(head)? else { return Ok(Value::Null) };
        let capped = tip.min(20_000_000_000);
        let slow = (capped * 95 / 100).max(1);
        let fast = tip * 105 / 100;
        let doubled = next * 2;
        let opt = |t: u128| json!({"maxPriorityFeePerGas": qty128(t), "maxFeePerGas": qty128(doubled + t)});
        Ok(json!({"slow": opt(slow), "normal": opt(capped), "fast": opt(fast)}))
    }

    /// subnet-evm's next base fee over the fee window in parent.Extra.
    pub fn next_base_fee(&self, n: u64) -> Result<Option<u128>, RpcError> {
        let b = self.block_at(n)?;
        Ok(self.next_base_fee_of(&b.header)?.map(|f| f.to::<u128>()))
    }

    fn fee_history(&self, params: &[Value]) -> RpcResult {
        if params.len() < 2 {
            return Err(invalid("need [blockCount, newestBlock, rewardPercentiles]"));
        }
        let mut count = parse_qty(&params[0]).map_err(|e| invalid(format!("bad blockCount: {}", e.message)))?;
        if count == 0 {
            return Err(invalid("bad blockCount \"0x0\""));
        }
        let newest = self.block_number(params.get(1))?;
        let percentiles: Vec<f64> = match params.get(2) {
            None | Some(Value::Null) => Vec::new(),
            Some(v) => serde_json::from_value(v.clone()).map_err(|e| invalid(format!("bad rewardPercentiles: {e}")))?,
        };
        count = count.min(1024);
        if !percentiles.is_empty() && count > 128 {
            return Err(invalid("reward percentiles capped at 128 blocks"));
        }
        let oldest = (newest + 1).saturating_sub(count);
        let mut base_fees = Vec::new();
        let mut ratios = Vec::new();
        let mut rewards = Vec::new();
        for n in oldest..=newest {
            let b = self.block_at(n)?;
            let bf = b.header.base_fee.map(|f| f.to::<u128>()).unwrap_or(0);
            base_fees.push(json!(qty128(bf)));
            ratios.push(if b.header.gas_limit > 0 { b.header.gas_used as f64 / b.header.gas_limit as f64 } else { 0.0 });
            if !percentiles.is_empty() {
                rewards.push(self.reward_row(&b, bf, &percentiles)?);
            }
        }
        let next = if newest < self.head() { self.base_fee_at(newest + 1)?.unwrap_or(0) } else { self.next_base_fee(newest)?.unwrap_or(0) };
        base_fees.push(json!(qty128(next)));
        let mut out = json!({"oldestBlock": qty(oldest), "baseFeePerGas": base_fees, "gasUsedRatio": ratios});
        if !percentiles.is_empty() {
            out["reward"] = Value::Array(rewards);
        }
        Ok(out)
    }

    fn reward_row(&self, b: &Block, base_fee: u128, percentiles: &[f64]) -> RpcResult {
        if b.height == 0 || b.txs.is_empty() {
            return Ok(json!(vec!["0x0"; percentiles.len()]));
        }
        let (rs, _) = self.block_receipts(b)?;
        let mut items: Vec<(u128, u64)> = Vec::with_capacity(b.txs.len());
        for (i, t) in b.txs.iter().enumerate() {
            if t.gas_price < base_fee {
                return Err(format!("block {} tx {}: effective tip against base fee {base_fee}: max fee per gas less than block base fee", b.height, t.hash).into());
            }
            items.push((effective_gas_price(t, Some(U256::from(base_fee))) - base_fee, rs[i].gas_used));
        }
        items.sort_by_key(|x| x.0);
        let mut cum = 0u64;
        let mut idx = 0;
        let mut row = Vec::with_capacity(percentiles.len());
        for p in percentiles {
            let threshold = (b.header.gas_used as f64 * p / 100.0) as u64;
            while idx < items.len() - 1 && cum + items[idx].1 < threshold {
                cum += items[idx].1;
                idx += 1;
            }
            row.push(json!(qty128(items[idx].0)));
        }
        Ok(Value::Array(row))
    }
}

/// The address / topic half of a filter object.
pub fn parse_log_matchers(f: &serde_json::Map<String, Value>) -> Result<(Vec<Address>, Vec<Vec<B256>>), RpcError> {
    let mut addrs = Vec::new();
    match f.get("address") {
        None | Some(Value::Null) => {}
        Some(Value::String(_)) => addrs.push(parse_addr(f.get("address")).map_err(|e| invalid(format!("bad address: {}", e.message)))?),
        Some(Value::Array(a)) => {
            for v in a {
                addrs.push(parse_addr(Some(v)).map_err(|e| invalid(format!("bad address: {}", e.message)))?);
            }
        }
        Some(_) => return Err(invalid("bad address")),
    }
    let mut topics: Vec<Vec<B256>> = Vec::new();
    if let Some(Value::Array(ts)) = f.get("topics") {
        for t in ts {
            topics.push(match t {
                Value::Null => Vec::new(),
                Value::String(_) => vec![parse_hash(Some(t)).map_err(|e| invalid(format!("bad topics entry: {}", e.message)))?],
                Value::Array(many) => many.iter().map(|v| parse_hash(Some(v)).map_err(|e| invalid(format!("bad topics entry: {}", e.message)))).collect::<Result<_, _>>()?,
                _ => return Err(invalid("bad topics entry")),
            });
        }
    }
    while topics.last().is_some_and(|t| t.is_empty()) {
        topics.pop();
    }
    Ok((addrs, topics))
}

pub fn log_matches(l: &Log, addrs: &[Address], topics: &[Vec<B256>]) -> bool {
    if !addrs.is_empty() && !addrs.contains(&l.address) {
        return false;
    }
    for (i, want) in topics.iter().enumerate() {
        if want.is_empty() {
            continue;
        }
        if i >= l.topics.len() || !want.contains(&l.topics[i]) {
            return false;
        }
    }
    true
}

fn bloom_may_match(bloom: &alloy_primitives::Bloom, addrs: &[Address], topics: &[Vec<B256>]) -> bool {
    if !addrs.is_empty() && !addrs.iter().any(|a| bloom.contains_input(alloy_primitives::BloomInput::Raw(a.as_slice()))) {
        return false;
    }
    for want in topics {
        if !want.is_empty() && !want.iter().any(|t| bloom.contains_input(alloy_primitives::BloomInput::Raw(t.as_slice()))) {
            return false;
        }
    }
    true
}

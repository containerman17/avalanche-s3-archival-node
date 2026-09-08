//! The Otterscan namespace (rpc/otterscan.go): every method is a read over
//! the lookup families that already exist (addr/, itx/, blk/, rcpt/, tx/).
//! Pagination is KEYSET, newest-first, and it is the posting order itself.
use std::collections::HashMap;
use std::sync::Arc;

use alloy_primitives::{Address, B256, U256};
use block::Block;
use serde_json::{json, Map, Value};
use store::format::{addr_prefix, ROLE_CREATED, ROLE_SENDER};

use crate::json::*;
use crate::{invalid, Receipt, RpcError, RpcResult, Server};

/// The Otterscan API version this server implements.
pub const API_LEVEL: u64 = 8;
/// One page cannot ask for unbounded work.
const MAX_PAGE_SIZE: u64 = 100;

pub fn dispatch(s: &Server, method: &str, params: &[Value]) -> Option<RpcResult> {
    Some(match method {
        "ots_getApiLevel" => Ok(json!(API_LEVEL)),
        "ots_searchTransactionsBefore" => s.ots_search_before(params),
        "ots_searchTransactionsAfter" => s.ots_search_after(params),
        "ots_getTransactionBySenderAndNonce" => s.ots_tx_by_sender_and_nonce(params),
        "ots_getContractCreator" => s.ots_contract_creator(params),
        "ots_getInternalOperations" => s.ots_internal_operations(params),
        "ots_getBlockDetails" => s.ots_block_details(params),
        "ots_getBlockTransactions" => s.ots_block_transactions(params),
        _ => return None,
    })
}

// --- parameters ---------------------------------------------------------------

fn addr_param(params: &[Value], i: usize) -> Result<Address, RpcError> {
    let Some(v) = params.get(i) else { return Err(invalid(format!("need an address at position {i}"))) };
    let s = v.as_str().ok_or_else(|| invalid(format!("bad address: json: cannot unmarshal {} into Go value of type common.Address", go_kind(v))))?;
    let h = s.strip_prefix("0x").ok_or_else(|| invalid("bad address: hex string without 0x prefix"))?;
    if h.len() != 40 {
        return Err(invalid(format!("bad address: hex string has length {}, want 40 for common.Address", h.len())));
    }
    s.parse().map_err(|_| invalid("bad address: invalid hex string"))
}

fn go_kind(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

/// A parameter Otterscan spells as a plain JSON number; the hex string form
/// is accepted too. Missing or null is 0.
fn uint_param(params: &[Value], i: usize, name: &str) -> Result<u64, RpcError> {
    let Some(v) = params.get(i).filter(|v| !v.is_null()) else { return Ok(0) };
    if let Some(n) = v.as_u64() {
        return Ok(n);
    }
    let s = v.as_str().ok_or_else(|| invalid(format!("bad {name}: json: cannot unmarshal {} into Go value of type string", go_kind(v))))?;
    hexutil_uint64(s).map_err(|e| invalid(format!("bad {name} {s:?}: {e}")))
}

/// hexutil.DecodeUint64 in its own words.
fn hexutil_uint64(s: &str) -> Result<u64, String> {
    let h = s.strip_prefix("0x").ok_or("hex string without 0x prefix")?;
    if h.is_empty() {
        return Err("empty hex string".into());
    }
    if h.len() > 1 && h.starts_with('0') {
        return Err("hex number with leading zero digits".into());
    }
    if h.len() > 16 {
        return Err("hex number > 64 bits".into());
    }
    u64::from_str_radix(h, 16).map_err(|_| "invalid hex string".to_string())
}

fn page_size(params: &[Value], i: usize) -> Result<usize, RpcError> {
    let n = uint_param(params, i, "pageSize")?;
    if n == 0 {
        return Ok(25); // Otterscan's own default
    }
    if n > MAX_PAGE_SIZE {
        return Err(invalid(format!("pageSize {n} exceeds the limit of {MAX_PAGE_SIZE}")));
    }
    Ok(n as usize)
}

fn tx_hash_param(params: &[Value]) -> Result<B256, RpcError> {
    let Some(v) = params.first() else { return Err(invalid("need [txHash]")) };
    let s = v.as_str().ok_or_else(|| invalid(format!("bad tx hash: json: cannot unmarshal {} into Go value of type common.Hash", go_kind(v))))?;
    let h = s.strip_prefix("0x").ok_or_else(|| invalid("bad tx hash: hex string without 0x prefix"))?;
    if h.len() != 64 {
        return Err(invalid(format!("bad tx hash: hex string has length {}, want 64 for common.Hash", h.len())));
    }
    s.parse().map_err(|_| invalid("bad tx hash: invalid hex string"))
}

// --- address history ----------------------------------------------------------

fn empty_page(first: bool, last: bool) -> Value {
    json!({"txs": [], "receipts": [], "firstPage": first, "lastPage": last})
}

/// One block assembled at most once per request: a page of 25 transactions
/// usually touches far fewer blocks, and receipts are block-wide.
#[derive(Default)]
struct BlockCache {
    blk: HashMap<u64, (Arc<Block>, Vec<Receipt>, Vec<u64>)>,
}

impl BlockCache {
    fn get(&mut self, s: &Server, n: u64) -> Result<&(Arc<Block>, Vec<Receipt>, Vec<u64>), RpcError> {
        if !self.blk.contains_key(&n) {
            let b = s.block_at(n)?;
            let (rs, first) = s.block_receipts(&b)?;
            self.blk.insert(n, (b, rs, first));
        }
        Ok(&self.blk[&n])
    }
}

impl Server {
    /// The highest TxNum this node can answer for.
    fn ots_head_tx(&self) -> Option<u64> {
        self.store.next_tx().checked_sub(1)
    }

    /// The TxNum a block starts at.
    fn ots_first_tx_of(&self, n: u64) -> Result<u64, RpcError> {
        Ok(self.store.tx_range(n)?.ok_or_else(|| invalid(format!("block {n} is not stored")))?.0)
    }

    fn ots_search_before(&self, params: &[Value]) -> RpcResult {
        let addr = addr_param(params, 0)?;
        let mut block_num = uint_param(params, 1, "blockNumber")?;
        let page_size = page_size(params, 2)?;
        let Some(head) = self.ots_head_tx() else { return Ok(empty_page(true, true)) };
        let mut hi_tx = head;
        if block_num > self.head() {
            block_num = 0; // a cursor above the head means "from the tip", not "none"
        }
        if block_num > 0 {
            let first = self.ots_first_tx_of(block_num)?;
            if first == 0 {
                return Ok(empty_page(false, true));
            }
            hi_tx = first - 1;
        }
        let (nums, more) = self.ots_walk(addr, 0, hi_tx, page_size, true)?;
        let mut res = self.ots_page(&nums)?;
        res["firstPage"] = json!(block_num == 0);
        res["lastPage"] = json!(!more);
        Ok(res)
    }

    fn ots_search_after(&self, params: &[Value]) -> RpcResult {
        let addr = addr_param(params, 0)?;
        let block_num = uint_param(params, 1, "blockNumber")?;
        let page_size = page_size(params, 2)?;
        let Some(head) = self.ots_head_tx() else { return Ok(empty_page(true, true)) };
        let mut lo_tx = 0;
        if block_num > 0 {
            if block_num >= self.head() {
                return Ok(empty_page(true, false));
            }
            lo_tx = self.ots_first_tx_of(block_num + 1)?;
        }
        let (mut nums, more) = self.ots_walk(addr, lo_tx, head, page_size, false)?;
        nums.reverse(); // the walk was ascending, the answer is newest-first
        let mut res = self.ots_page(&nums)?;
        res["firstPage"] = json!(!more);
        res["lastPage"] = json!(block_num == 0);
        Ok(res)
    }

    /// Up to page_size posting TxNums for addr in [lo, hi]; `more` is exact
    /// because the walk reads ONE row past the page.
    fn ots_walk(&self, addr: Address, lo: u64, hi: u64, page_size: usize, desc: bool) -> Result<(Vec<u64>, bool), RpcError> {
        if hi < lo {
            return Ok((Vec::new(), false));
        }
        let mut nums = Vec::new();
        let mut more = false;
        self.store.postings(&addr_prefix(addr.as_slice()), lo, hi, desc, &mut |_, n, _| {
            if nums.len() == page_size {
                more = true;
                return false;
            }
            nums.push(n);
            true
        })?;
        Ok((nums, more))
    }

    /// A list of TxNums as Otterscan's txs+receipts pair, in the order given.
    fn ots_page(&self, nums: &[u64]) -> RpcResult {
        let mut txs = Vec::with_capacity(nums.len());
        let mut receipts = Vec::with_capacity(nums.len());
        let mut cache = BlockCache::default();
        for &num in nums {
            let height = self.store.height_of_tx(num)?.ok_or_else(|| RpcError::from(format!("TxNum {num} is in no block")))?;
            let first = self.ots_first_tx_of(height)?;
            let (blk, rs, firstlog) = cache.get(self, height)?;
            let i = num.wrapping_sub(first) as usize;
            if num < first || i >= blk.txs.len() {
                return Err(format!("TxNum {num} is outside block {height}'s transactions").into());
            }
            txs.push(tx_json(blk, i)?);
            let mut m = receipt_json(blk, i, &rs[i], firstlog[i])?;
            // Otterscan's search receipts carry the block timestamp: the UI
            // shows a time per row and has no block object to read it from.
            m["timestamp"] = json!(qty(blk.header.time));
            receipts.push(m);
        }
        Ok(json!({"txs": txs, "receipts": receipts, "firstPage": false, "lastPage": false}))
    }

    // --- sender and nonce -----------------------------------------------------

    /// Which transaction did this account send with this nonce. The sender's
    /// postings are in TxNum order and a nonce only ever increases, so the
    /// walk stops at the first row whose nonce reaches the target.
    ///
    /// ponytail: linear over the sender's history; the fix if it ever matters
    /// is a sender/nonce lookup family, not a cache.
    fn ots_tx_by_sender_and_nonce(&self, params: &[Value]) -> RpcResult {
        let sender = addr_param(params, 0)?;
        let nonce = uint_param(params, 1, "nonce")?;
        let Some(head) = self.ots_head_tx() else { return Ok(Value::Null) };
        let mut cache = BlockCache::default();
        for (num, role) in self.collect_postings(&addr_prefix(sender.as_slice()), 0, head, false)? {
            if role & ROLE_SENDER == 0 {
                continue;
            }
            let (n, hash) = self.tx_at(num, &mut cache)?;
            if n < nonce {
                continue;
            }
            // at or past the target: nonces never come back down
            return Ok(if n == nonce { json!(hash) } else { Value::Null });
        }
        Ok(Value::Null) // this account never sent that nonce
    }

    /// Every posting under a prefix, materialised. The store holds its lock
    /// for the whole walk, so a callback must not read anything else.
    // ponytail: one address's postings in RAM; a chunk cursor when a hot
    // address makes that matter.
    pub(crate) fn collect_postings(&self, prefix: &[u8], lo: u64, hi: u64, desc: bool) -> Result<Vec<(u64, u8)>, RpcError> {
        let mut out = Vec::new();
        self.store.postings(prefix, lo, hi, desc, &mut |_, n, p| {
            out.push((n, p));
            true
        })?;
        Ok(out)
    }

    /// (nonce, hash) of one transaction by TxNum.
    fn tx_at(&self, num: u64, cache: &mut BlockCache) -> Result<(u64, B256), RpcError> {
        let (i, blk) = self.tx_index_at(num, cache)?;
        Ok((blk.txs[i].nonce, blk.txs[i].hash))
    }

    fn tx_index_at<'c>(&self, num: u64, cache: &'c mut BlockCache) -> Result<(usize, &'c Arc<Block>), RpcError> {
        let height = self.store.height_of_tx(num)?.ok_or_else(|| RpcError::from(format!("TxNum {num} is in no block")))?;
        let first = self.ots_first_tx_of(height)?;
        let (blk, _, _) = cache.get(self, height)?;
        let i = num.wrapping_sub(first) as usize;
        if num < first || i >= blk.txs.len() {
            return Err(format!("TxNum {num} is outside block {height}'s transactions").into());
        }
        Ok((i, blk))
    }

    // --- contract creator -----------------------------------------------------

    /// The transaction that deployed a contract and the account that sent it.
    /// A top-level creation carries RoleCreated; a factory deployment appears
    /// as a CREATE/CREATE2 frame in the itx/ row. A plain account answers
    /// null, and that check runs FIRST so an EOA costs one state read.
    fn ots_contract_creator(&self, params: &[Value]) -> RpcResult {
        let addr = addr_param(params, 0)?;
        let Some(head) = self.ots_head_tx() else { return Ok(Value::Null) };
        let has_code = {
            let mut st = self.store.state_at(self.head())?;
            match st.account(addr)? {
                None => false,
                Some(a) => a.code_hash != alloy_primitives::KECCAK256_EMPTY && a.code_hash != B256::ZERO,
            }
        };
        if !has_code {
            return Ok(Value::Null); // not a contract at the head
        }
        let mut cache = BlockCache::default();
        for (num, role) in self.collect_postings(&addr_prefix(addr.as_slice()), 0, head, false)? {
            let mut created = role & ROLE_CREATED != 0;
            if !created {
                // A factory deployment: the CREATE frame names the address.
                match self.frames_of(num)? {
                    None => continue,
                    Some(frames) => created = frames.iter().any(|f| (f.kind == "CREATE" || f.kind == "CREATE2") && f.to == addr && !f.failed),
                }
            }
            if !created {
                continue;
            }
            let (i, blk) = self.tx_index_at(num, &mut cache)?;
            let t = &blk.txs[i];
            let from = t.sender.ok_or_else(|| bad_sender(t, blk.height))?;
            return Ok(json!({"hash": t.hash, "creator": from}));
        }
        Ok(Value::Null)
    }

    // --- internal operations --------------------------------------------------

    /// The transaction's STORED call frames, flattened depth-first exactly as
    /// store.DecodeFrames does (the top-level frame is NOT one of them).
    fn frames_of(&self, num: u64) -> Result<Option<Vec<Frame>>, RpcError> {
        let Some(height) = self.store.height_of_tx(num)? else { return Ok(None) };
        let first = self.ots_first_tx_of(height)?;
        let Some(rows) = self.store.traces(height)? else { return Ok(None) };
        let i = num.wrapping_sub(first) as usize;
        if num < first || i >= rows.len() {
            return Ok(None);
        }
        if rows[i].is_empty() {
            return Ok(Some(Vec::new()));
        }
        let top: Value = serde_json::from_str(&rows[i]).map_err(|e| RpcError::from(format!("decode itx/{num}: frames: {e}")))?;
        let mut out = Vec::new();
        flatten(&top, &mut out);
        Ok(Some(out))
    }

    /// THE TRANSFER FILTER IS AT READ TIME: a DELEGATECALL frame carries the
    /// PARENT's value by capture, so only CALL frames report a transfer.
    /// SELF_DESTRUCT (kind 1) never appears: the tracer pin has no
    /// SELFDESTRUCT hook, a gap of the capture, not of this read.
    fn ots_internal_operations(&self, params: &[Value]) -> RpcResult {
        let hash = tx_hash_param(params)?;
        let Some((height, i)) = self.store.tx_by_hash(&hash)? else { return Ok(Value::Null) };
        let first = self.ots_first_tx_of(height)?;
        let Some(frames) = self.frames_of(first + i as u64)? else {
            return Err(format!("transaction {hash} has no stored call frames: this node never executed it").into());
        };
        let mut out = Vec::new();
        for f in &frames {
            let ty = match f.kind.as_str() {
                "CREATE" => 2,
                "CREATE2" => 3,
                "CALL" if f.value.is_some_and(|v| !v.is_zero()) => 0,
                _ => continue,
            };
            out.push(json!({"type": ty, "from": f.from, "to": f.to, "value": qty256(f.value.unwrap_or_default())}));
        }
        Ok(json!(out))
    }

    // --- block details --------------------------------------------------------

    /// The block WITHOUT its transaction list, plus issuance and total fees.
    ///
    /// ISSUANCE IS ZERO ON THESE CHAINS AND THAT IS A FACT, NOT A STUB:
    /// Avalanche pays no block reward and BURNS the base fee.
    fn ots_block_details(&self, params: &[Value]) -> RpcResult {
        let b = self.ots_block_param(params)?;
        let mut fields = block_json(&b, false)?;
        fields.as_object_mut().unwrap().remove("transactions");
        fields["transactionCount"] = json!(b.txs.len());
        let mut total = U256::ZERO;
        if !b.txs.is_empty() {
            let (rs, _) = self.block_receipts(&b)?;
            for (i, r) in rs.iter().enumerate() {
                total += U256::from(effective_gas_price(&b.txs[i], b.header.base_fee)) * U256::from(r.gas_used);
            }
        }
        Ok(json!({
            "block": fields,
            "issuance": {"blockReward": "0x0", "uncleReward": "0x0", "issuance": "0x0"},
            "totalFees": qty256(total),
        }))
    }

    fn ots_block_param(&self, params: &[Value]) -> Result<Arc<Block>, RpcError> {
        if params.is_empty() {
            return Err(invalid("need [block...]"));
        }
        let n = self.block_number(params.first())?;
        self.block_at(n)
    }

    /// One page of a block's transactions, in block order, with receipts.
    fn ots_block_transactions(&self, params: &[Value]) -> RpcResult {
        let b = self.ots_block_param(params)?;
        let page_number = uint_param(params, 1, "pageNumber")?;
        let page_size = page_size(params, 2)? as u64;
        let n = b.txs.len() as u64;
        let lo = page_number.saturating_mul(page_size).min(n);
        let hi = lo.saturating_add(page_size).min(n);

        let mut fields = block_json(&b, false)?;
        fields["transactionCount"] = json!(b.txs.len());
        let mut page = Vec::new();
        let mut out = Vec::new();
        if hi > lo {
            let (rs, firstlog) = self.block_receipts(&b)?;
            for i in lo..hi {
                let i = i as usize;
                page.push(tx_json(&b, i)?);
                out.push(receipt_json(&b, i, &rs[i], firstlog[i])?);
            }
        }
        fields["transactions"] = json!(page);
        Ok(json!({"fullblock": fields, "receipts": out}))
    }
}

/// One flattened callTracer frame (store.Frame's read-relevant fields).
pub struct Frame {
    pub kind: String,
    pub from: Address,
    pub to: Address,
    pub value: Option<U256>,
    pub failed: bool,
}

/// store.DecodeFrames: the NESTED calls, depth-first, the top frame skipped.
fn flatten(f: &Value, out: &mut Vec<Frame>) {
    let Some(calls) = f.get("calls").and_then(Value::as_array) else { return };
    for c in calls {
        out.push(Frame {
            kind: c.get("type").and_then(Value::as_str).unwrap_or("").to_string(),
            from: c.get("from").and_then(Value::as_str).and_then(|s| s.parse().ok()).unwrap_or(Address::ZERO),
            to: c.get("to").and_then(Value::as_str).and_then(|s| s.parse().ok()).unwrap_or(Address::ZERO),
            value: c.get("value").and_then(Value::as_str).and_then(|s| U256::from_str_radix(s.trim_start_matches("0x"), 16).ok()),
            failed: c.get("error").and_then(Value::as_str).is_some_and(|e| !e.is_empty()),
        });
        flatten(c, out);
    }
}

#[allow(dead_code)]
fn unused(_: &Map<String, Value>) {}

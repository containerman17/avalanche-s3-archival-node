//! THE POSTING-LIST LOG READS (rpc/tokens.go): the questions eth_getLogs
//! cannot answer in one bounded call (a wallet's token transfers over all of
//! history, a contract's events, the tokens a wallet ever touched). Keyset
//! paged over TxNum exactly like the ots_ search: cursor is the TxNum to
//! continue from (inclusive), 0 meaning the end the walk starts at, and the
//! page is cut on TRANSACTIONS, so it is never torn inside one.
use std::sync::Arc;

use alloy_primitives::{Address, B256, U256};
use block::Block;
use exec::{CallMsg, Trace};
use serde_json::{json, Value};
use store::format::*;

use crate::json::*;
use crate::{Log, Receipt, RpcError, RpcResult, Server};


/// hexutil.UnmarshalFixedJSON, error for error. `Err.0` is the message and
/// `Err.1` says whether it is a decError, which encoding/json re-renders as an
/// UnmarshalTypeError (the caller supplies the "into Go value of type ..." or
/// the struct-field half); a plain error is passed through verbatim.
pub fn unmarshal_fixed(v: &Value, n: usize) -> Result<Vec<u8>, (String, bool)> {
    let Some(s) = v.as_str() else { return Err(("non-string".into(), true)) };
    // hexutil.checkText: an empty string is accepted here and fails the length
    // check below, exactly as it does in Go.
    let raw = if s.is_empty() {
        ""
    } else {
        match s.strip_prefix("0x") {
            None => return Err(("hex string without 0x prefix".into(), true)),
            Some(h) => h,
        }
    };
    if raw.len() % 2 != 0 {
        return Err(("hex string of odd length".into(), true));
    }
    if raw.len() / 2 != n {
        return Err((format!("hex string has length {}, want {} for TYPE", raw.len(), 2 * n), false));
    }
    alloy_primitives::hex::decode(raw).map_err(|_| ("invalid hex string".to_string(), true))
}

/// One page's cap in transactions, as ots_ has one in rows.
pub const MAX_PAGE: usize = 1000;

/// The three token standards' event signatures.
pub fn sig_transfer() -> B256 {
    alloy_primitives::keccak256("Transfer(address,address,uint256)")
}
pub fn sig_transfer_single() -> B256 {
    alloy_primitives::keccak256("TransferSingle(address,address,address,uint256,uint256)")
}
pub fn sig_transfer_batch() -> B256 {
    alloy_primitives::keccak256("TransferBatch(address,address,address,uint256[],uint256[])")
}

/// What a standard name means in the generic reads: its signatures, where the
/// holder stands, and how many topics its event carries (Transfer with 3
/// topics is ERC-20, with 4 it is ERC-721).
pub struct Standard {
    pub sigs: Vec<B256>,
    pub positions: u8,
    pub topics: usize,
}

pub fn standard_of(name: &str) -> Result<Standard, RpcError> {
    Ok(match name {
        "erc20" => Standard { sigs: vec![sig_transfer()], positions: 1 | 2, topics: 3 },
        "erc721" => Standard { sigs: vec![sig_transfer()], positions: 1 | 2, topics: 4 },
        "erc1155" => Standard { sigs: vec![sig_transfer_single(), sig_transfer_batch()], positions: 2 | 4, topics: 4 },
        _ => return Err(format!("unknown token standard {name:?} (erc20, erc721, erc1155)").into()),
    })
}

impl Standard {
    pub fn matches(&self, l: &Log) -> bool {
        l.topics.len() == self.topics && self.sigs.contains(&l.topics[0])
    }
}

/// Whether value stands at one of the topic positions in `positions`
/// (1..3 as the store's Pos bits; 0 means any of the three).
pub fn topic_at(l: &Log, value: B256, positions: u8) -> bool {
    (1..l.topics.len().min(4)).any(|i| l.topics[i] == value && (positions == 0 || positions & (1 << (i - 1)) != 0))
}

/// One token a holder ever moved.
#[derive(PartialEq, Eq, PartialOrd, Ord)]
struct TokenContract {
    rank: u8,
    token: Address,
}

/// supportsInterface(0x80ac58cd): the ERC-165 selector and the ERC-721
/// interface id, ABI-padded.
fn erc165_supports_erc721() -> Vec<u8> {
    let mut d = vec![0x01, 0xff, 0xc9, 0xa7, 0x80, 0xac, 0x58, 0xcd];
    d.resize(36, 0);
    d
}

impl Server {
    /// The logs emitter emitted, optionally only with topic0.
    pub fn logs_by_emitter(&self, emitter: Address, topic0: Option<B256>, cursor: u64, limit: i64, desc: bool) -> RpcResult {
        let prefix = match topic0 {
            Some(t) => elog_group(emitter.as_slice(), t.as_slice()),
            None => elog_prefix(emitter.as_slice()),
        };
        self.paged_logs(&prefix, 0, cursor, limit, desc, &|l| l.address == emitter && topic0.is_none_or(|t| l.topics.first() == Some(&t)))
    }

    /// The logs where value stood at one of the indexed topic positions in
    /// `positions` (0 means any), optionally only under topic0.
    pub fn logs_by_topic_value(&self, value: B256, topic0: Option<B256>, positions: u8, cursor: u64, limit: i64, desc: bool) -> RpcResult {
        let prefix = match topic0 {
            Some(t) => tval_group(value.as_slice(), t.as_slice()),
            None => tval_prefix(value.as_slice()),
        };
        self.paged_logs(&prefix, positions, cursor, limit, desc, &|l| topic0.is_none_or(|t| l.topics.first() == Some(&t)) && topic_at(l, value, positions))
    }

    /// Every (position, emitter) value has stood under topic0 at topics 1..3:
    /// three set/ prefix scans, no receipt read. The signature is required by
    /// design (set/ is signature-first).
    pub fn topic_groups(&self, value: B256, topic0: B256) -> RpcResult {
        let mut out = Vec::new();
        for pos in 1u8..=3 {
            for e in self.set_emitters(topic0, pos, value)? {
                out.push(json!({"position": pos, "emitter": e}));
            }
        }
        // the Go node marshals a nil slice as null
        Ok(if out.is_empty() { Value::Null } else { json!(out) })
    }

    /// The emitters under one set/ (topic0, position, value) prefix, in key order.
    fn set_emitters(&self, topic0: B256, pos: u8, value: B256) -> Result<Vec<Address>, RpcError> {
        let prefix = set_prefix(topic0.as_slice(), pos, value.as_slice());
        let mut out = Vec::new();
        self.store.set_scan(&prefix, &mut |k| {
            out.push(Address::from_slice(&k[prefix.len()..]));
            true
        })?;
        Ok(out)
    }

    /// The standard's transfer events where holder is the sender or the
    /// receiver: = logs_by_topic_value(holder, sig, positions).
    pub fn token_transfers_by_holder(&self, holder: Address, standard: &str, cursor: u64, limit: i64, desc: bool) -> RpcResult {
        let st = standard_of(standard)?;
        let value = B256::left_padding_from(holder.as_slice());
        let prefix = if st.sigs.len() > 1 {
            tval_prefix(value.as_slice()) // both 1155 signatures; the filter picks
        } else {
            tval_group(value.as_slice(), st.sigs[0].as_slice())
        };
        self.paged_logs(&prefix, st.positions, cursor, limit, desc, &|l| st.matches(l) && topic_at(l, value, st.positions))
    }

    /// Every transfer event token emitted: = logs_by_emitter(token, sig).
    pub fn token_transfers_by_contract(&self, token: Address, standard: &str, cursor: u64, limit: i64, desc: bool) -> RpcResult {
        let st = standard_of(standard)?;
        let prefix = if st.sigs.len() > 1 { elog_prefix(token.as_slice()) } else { elog_group(token.as_slice(), st.sigs[0].as_slice()) };
        self.paged_logs(&prefix, 0, cursor, limit, desc, &|l| l.address == token && st.matches(l))
    }

    /// Every token contract holder ever sent or received under the three
    /// standards: set/ prefix scans, zero receipt reads. ERC-20 and ERC-721
    /// share the Transfer signature and are told apart by
    /// supportsInterface(0x80ac58cd) at latest. Sorted by standard, then by
    /// address.
    pub fn token_contracts(&self, holder: Address) -> RpcResult {
        let value = B256::left_padding_from(holder.as_slice());
        let mut out: Vec<TokenContract> = Vec::new();
        let push = |rank: u8, token: Address, out: &mut Vec<TokenContract>| {
            let k = TokenContract { rank, token };
            if !out.contains(&k) {
                out.push(k);
            }
        };
        let mut transfer = Vec::new();
        for pos in [1u8, 2] {
            transfer.extend(self.set_emitters(sig_transfer(), pos, value)?);
        }
        for t in &transfer {
            let rank = if self.transfer_standard(*t)? == "erc721" { 1 } else { 0 };
            push(rank, *t, &mut out);
        }
        for (sig, poss) in [(sig_transfer_single(), [2u8, 3]), (sig_transfer_batch(), [2, 3])] {
            for pos in poss {
                for e in self.set_emitters(sig, pos, value)? {
                    push(2, e, &mut out);
                }
            }
        }
        out.sort();
        let names = ["erc20", "erc721", "erc1155"];
        let rows: Vec<Value> = out.iter().map(|c| json!({"standard": names[c.rank as usize], "token": c.token})).collect();
        Ok(if rows.is_empty() { Value::Null } else { json!(rows) })
    }

    /// ERC-721 or ERC-20, by one supportsInterface(0x80ac58cd) eth_call at
    /// latest, cached per address for the life of the process (code does not
    /// change under a contract, and a proxy that flips is not worth a
    /// per-call read).
    // ponytail: probes run one at a time; the Go node fans them out 8 wide.
    fn transfer_standard(&self, token: Address) -> Result<&'static str, RpcError> {
        if let Some(s) = self.iface721.lock().unwrap().get(&token) {
            return Ok(if s == "erc721" { "erc721" } else { "erc20" });
        }
        let n = self.head();
        let mut std = "erc20";
        if n > 0 {
            let msg = CallMsg { from: Address::ZERO, to: Some(token), gas: 100_000, gas_price: 0, value: U256::ZERO, data: erc165_supports_erc721().into() };
            if let Ok(r) = self.run_call(n, &msg, Trace::Off, None, None) {
                if !r.revert && r.halt.is_none() && r.output.len() == 32 && r.output[31] == 1 && r.output[..31].iter().all(|b| *b == 0) {
                    std = "erc721";
                }
            }
        }
        self.iface721.lock().unwrap().insert(token, std.to_string());
        Ok(std)
    }

    /// The posting entries under prefix (payload & mask != 0 when mask is
    /// set), each candidate tx's logs read, those `keep` accepts kept, the
    /// page cut after `limit` transactions. `more` is exact: the walk reads
    /// one transaction past the page.
    fn paged_logs(&self, prefix: &[u8], mask: u8, cursor: u64, limit: i64, desc: bool, keep: &dyn Fn(&Log) -> bool) -> RpcResult {
        let Some(head) = self.store.next_tx().checked_sub(1) else { return Ok(json!({"logs": [], "more": false, "nextCursor": 0})) };
        let limit = if limit <= 0 || limit as usize > MAX_PAGE { MAX_PAGE } else { limit as usize };
        let (mut lo, mut hi) = (0u64, head);
        if desc {
            if cursor > 0 && cursor < hi {
                hi = cursor;
            }
        } else {
            lo = cursor;
        }
        let mut logs: Vec<Value> = Vec::new();
        let (mut more, mut cut) = (false, 0u64);
        let mut txs = 0usize;
        let mut last: Option<u64> = None;
        let mut w = TxLogs { s: self, at: None };
        for (txnum, payload) in self.collect_postings(prefix, lo, hi, desc)? {
            if mask != 0 && payload & mask == 0 {
                continue;
            }
            if last == Some(txnum) {
                continue; // a second group of the same tx
            }
            last = Some(txnum);
            let (blk, i, first_log) = w.at(txnum)?;
            let hit: Vec<Value> = blk.0.iter().enumerate().filter(|(_, l)| keep(l)).map(|(k, l)| log_json(l, &blk.1, i, first_log + k as u64, false)).collect();
            if hit.is_empty() {
                continue; // a candidate the exact filter rejects is not a page row
            }
            if txs == limit {
                more = true; // exact: this tx would be the next page's first
                break;
            }
            txs += 1;
            cut = txnum;
            logs.extend(hit);
        }
        let next = if more {
            if desc {
                cut.wrapping_sub(1)
            } else {
                cut + 1
            }
        } else {
            0
        };
        Ok(json!({"logs": logs, "more": more, "nextCursor": next}))
    }
}

/// One transaction's fully addressed logs, keeping the last block decoded: a
/// walk visits a block's transactions in a row.
struct TxLogs<'a> {
    s: &'a Server,
    at: Option<(u64, Arc<Block>, Vec<Receipt>, Vec<u64>)>,
}

impl TxLogs<'_> {
    /// (the tx's logs, its index in the block, its first block-wide log index).
    fn at(&mut self, txnum: u64) -> Result<((Vec<Log>, Arc<Block>), usize, u64), RpcError> {
        let need = match &self.at {
            Some((first, b, _, _)) => txnum < *first || txnum >= *first + b.txs.len() as u64,
            None => true,
        };
        if need {
            let h = self.s.store.height_of_tx(txnum)?.ok_or_else(|| RpcError::from(format!("TxNum {txnum} is in no block")))?;
            let (first, _) = self.s.store.tx_range(h)?.ok_or_else(|| RpcError::from(format!("tx range of block {h}: not stored")))?;
            let b = self.s.block_at(h)?;
            let (rs, firstlog) = self.s.block_receipts(&b)?;
            self.at = Some((first, b, rs, firstlog));
        }
        let (first, b, rs, firstlog) = self.at.as_ref().unwrap();
        let i = (txnum - first) as usize;
        Ok(((rs[i].logs.clone(), b.clone()), i, firstlog[i]))
    }
}

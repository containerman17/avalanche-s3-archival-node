//! The filter API (eth_newFilter and friends) and the empty txpool shapes.
//! Filters live in memory, keyed by a random id, and expire after 5 minutes
//! without a poll (geth's deadline).
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use alloy_primitives::{Address, B256};
#[allow(unused_imports)]
use crate::json::parse_hash;
use serde_json::{json, Value};

use crate::eth::parse_log_matchers;
use crate::{invalid, RpcResult, Server};

const DEADLINE: Duration = Duration::from_secs(5 * 60);

pub enum Kind {
    Logs { from: Option<Value>, to: Option<Value>, addrs: Vec<Address>, topics: Vec<Vec<B256>> },
    Blocks,
    PendingTxs,
}

pub struct Filter {
    pub kind: Kind,
    /// The next height a poll reports from.
    pub next: u64,
    pub last_poll: Instant,
}

#[derive(Default)]
pub struct Registry {
    pub filters: HashMap<String, Filter>,
}

static SEQ: AtomicU64 = AtomicU64::new(0);

/// rpc.NewID: 16 pseudo-random bytes as a hex quantity (leading zeros
/// trimmed, `0x0` for all zeros); filters and subscriptions share it.
pub fn new_id() -> String {
    let mut b = [0u8; 32];
    b[..8].copy_from_slice(&(std::process::id() as u64).to_be_bytes());
    b[8..16].copy_from_slice(&SEQ.fetch_add(1, Ordering::Relaxed).to_be_bytes());
    b[16..24].copy_from_slice(&std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_nanos() as u64).unwrap_or(0).to_be_bytes());
    let h = alloy_primitives::hex::encode(&alloy_primitives::keccak256(b)[..16]);
    let h = h.trim_start_matches('0');
    format!("0x{}", if h.is_empty() { "0" } else { h })
}

impl Registry {
    fn sweep(&mut self) {
        self.filters.retain(|_, f| f.last_poll.elapsed() < DEADLINE);
    }
}

pub fn empty_txpool(method: &str) -> Value {
    match method {
        "txpool_status" => json!({"pending": "0x0", "queued": "0x0"}),
        "txpool_contentFrom" => json!({"pending": {}, "queued": {}}),
        _ => json!({"pending": {}, "queued": {}}),
    }
}

pub fn dispatch(s: &Server, method: &str, params: &[Value]) -> Option<RpcResult> {
    Some(match method {
        "eth_newFilter" => s.new_filter(params),
        "eth_newBlockFilter" => s.install(Kind::Blocks),
        "eth_newPendingTransactionFilter" => s.install(Kind::PendingTxs),
        "eth_getFilterChanges" => s.get_filter_changes(params),
        "eth_getFilterLogs" => s.get_filter_logs(params),
        "eth_uninstallFilter" => s.uninstall_filter(params),
        _ => return None,
    })
}

impl Server {
    fn install(&self, kind: Kind) -> RpcResult {
        let mut g = self.filters.lock().unwrap();
        g.sweep();
        let id = new_id();
        g.filters.insert(id.clone(), Filter { kind, next: self.head() + 1, last_poll: Instant::now() });
        Ok(json!(id))
    }

    fn new_filter(&self, params: &[Value]) -> RpcResult {
        let f = params.first().and_then(Value::as_object).ok_or_else(|| invalid("need [filter]"))?;
        if f.get("blockHash").is_some_and(|v| !v.is_null()) {
            return Err(invalid("blockHash filter is for eth_getLogs, not for a stored filter over a range"));
        }
        let (addrs, topics) = parse_log_matchers(f)?;
        let (from, to) = self.parse_filter_range(f)?;
        let _ = (from, to);
        self.install(Kind::Logs { from: f.get("fromBlock").cloned(), to: f.get("toBlock").cloned(), addrs, topics })
    }

    fn filter_id(params: &[Value]) -> Result<String, crate::RpcError> {
        Ok(params.first().and_then(Value::as_str).ok_or_else(|| crate::missing_arg(0))?.to_string())
    }

    fn get_filter_changes(&self, params: &[Value]) -> RpcResult {
        let id = Self::filter_id(params)?;
        let head = self.head();
        let mut g = self.filters.lock().unwrap();
        g.sweep();
        let Some(f) = g.filters.get_mut(&id) else { return Err("filter not found".into()) };
        f.last_poll = Instant::now();
        let (from, to) = (f.next, head);
        if to < from {
            return Ok(json!([]));
        }
        f.next = to + 1;
        match &f.kind {
            Kind::PendingTxs => Ok(json!([])),
            Kind::Blocks => {
                drop(g);
                let mut out = Vec::new();
                for n in from..=to {
                    out.push(json!(self.store.hash_at(n)?.unwrap_or_default()));
                }
                Ok(Value::Array(out))
            }
            Kind::Logs { from: f0, to: t0, addrs, topics } => {
                let (addrs, topics, f0, t0) = (addrs.clone(), topics.clone(), f0.clone(), t0.clone());
                drop(g);
                // The filter's own range bounds the poll.
                let lo = self.block_number(f0.as_ref())?.max(1).max(from);
                let hi = self.block_number(t0.as_ref())?.min(to);
                if hi < lo {
                    return Ok(json!([]));
                }
                Ok(Value::Array(self.run_get_logs(lo, hi, &addrs, &topics)?))
            }
        }
    }

    fn get_filter_logs(&self, params: &[Value]) -> RpcResult {
        let id = Self::filter_id(params)?;
        let mut g = self.filters.lock().unwrap();
        g.sweep();
        let Some(f) = g.filters.get_mut(&id) else { return Err("filter not found".into()) };
        f.last_poll = Instant::now();
        let Kind::Logs { from, to, addrs, topics } = &f.kind else { return Err("filter not found".into()) };
        let (addrs, topics, from, to) = (addrs.clone(), topics.clone(), from.clone(), to.clone());
        drop(g);
        let lo = self.block_number(from.as_ref())?.max(1);
        let hi = self.block_number(to.as_ref())?;
        if hi < lo {
            return Ok(json!([]));
        }
        Ok(Value::Array(self.run_get_logs(lo, hi, &addrs, &topics)?))
    }

    fn uninstall_filter(&self, params: &[Value]) -> RpcResult {
        let id = Self::filter_id(params)?;
        Ok(json!(self.filters.lock().unwrap().filters.remove(&id).is_some()))
    }
}

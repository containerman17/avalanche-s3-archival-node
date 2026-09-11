//! SAE RPC contract: over a synthetic store whose settled head (S) is behind
//! its accepted head (A), the JSON-RPC surface serves the settled state for
//! `latest`, answers an accepted-but-unsettled height with the defined
//! not-settled error (never a value), and exposes the settled head and lag.
//!
//! A real SAE store (the engine's PluginStore reporting accepted_head > head)
//! is not built here; this drives the exact dispatch a live node runs, with the
//! store's recorded per-height account state standing in for a re-execution's
//! post-state at that height (the strong EVM-equality oracle needs the real
//! store, see REPORT.md).
use std::sync::Arc;

use alloy_primitives::{Address, Bytes, B256, B64, U256};
use bytes::Bytes as Raw;
use block::eth::Header;
use block::{Block, Tx};
use rpc::{Account, Receipt, Server, StateRead, Store, NOT_SETTLED_CODE};
use serde_json::{json, Value};

const S: u64 = 3; // settled head
const A: u64 = 6; // accepted head
const ADDR: Address = Address::new([0x11; 20]);

fn hash_of(h: u64) -> B256 {
    B256::from(U256::from(0x1000 + h))
}
fn tx_hash_of(h: u64) -> B256 {
    B256::from(U256::from(0x2000 + h))
}

fn header(number: u64) -> Header {
    Header {
        parent_hash: if number == 0 { B256::ZERO } else { hash_of(number - 1) },
        uncle_hash: B256::ZERO,
        coinbase: Address::ZERO,
        root: B256::from(U256::from(0x3000 + number)),
        tx_hash: B256::ZERO,
        receipt_hash: B256::ZERO,
        bloom: Default::default(),
        difficulty: U256::from(1),
        number,
        gas_limit: 8_000_000,
        gas_used: 21_000,
        time: 1_000 + number,
        extra: Bytes::new(),
        mix_digest: B256::ZERO,
        nonce: B64::ZERO,
        base_fee: Some(U256::from(25_000_000_000u64)),
        block_gas_cost: None,
        blob_gas_used: None,
        excess_blob_gas: None,
        parent_beacon_root: None,
        time_milliseconds: None,
        min_delay_excess: None,
    }
}

fn tx(h: u64) -> Tx {
    Tx {
        raw: Raw::from_static(&[0x02, 0x01, 0x02, 0x03]),
        hash: tx_hash_of(h),
        sender: Some(ADDR),
        tx_type: 2,
        chain_id: Some(123),
        nonce: h,
        gas_price: 25_000_000_000,
        gas_tip: 1,
        gas_limit: 21_000,
        to: Some(Address::new([0xff; 20])),
        value: U256::ZERO,
        input: Raw::new(),
        access_list: Vec::new(),
        v: 0,
        r: U256::ZERO,
        s: U256::ZERO,
        recid: 0,
        body_off: 0,
        sig_off: 0,
    }
}

fn block(h: u64) -> Arc<Block> {
    Arc::new(Block {
        height: h,
        hash: hash_of(h),
        container_id: hash_of(h),
        header: header(h),
        header_rlp: Raw::from_static(&[0xc0]),
        txs: vec![tx(h)],
        container: Raw::from_static(&[0x00, 0x01, 0x02]),
        pvm: None,
    })
}

/// The recorded post-state at a height: balance and nonce both equal the
/// height, so a settled read's value pins the height it was served from.
struct HState(u64);
impl StateRead for HState {
    fn account(&mut self, a: Address) -> rpc::Result<Option<Account>> {
        if a != ADDR {
            return Ok(None);
        }
        Ok(Some(Account { nonce: self.0, balance: U256::from(self.0), code_hash: alloy_primitives::KECCAK256_EMPTY }))
    }
    fn storage(&mut self, _: Address, _: U256) -> rpc::Result<U256> {
        Ok(U256::from(self.0))
    }
    fn code(&mut self, _: B256) -> rpc::Result<Option<Bytes>> {
        Ok(None)
    }
}

struct Synth;

impl Store for Synth {
    fn head(&self) -> u64 {
        S
    }
    fn accepted_head(&self) -> u64 {
        A
    }
    fn block(&self, h: u64) -> rpc::Result<Option<Arc<Block>>> {
        Ok((1..=A).contains(&h).then(|| block(h)))
    }
    fn hash_at(&self, h: u64) -> rpc::Result<Option<B256>> {
        Ok((h <= A).then(|| hash_of(h)))
    }
    fn height_by_hash(&self, hash: &B256) -> rpc::Result<Option<u64>> {
        Ok((0..=A).find(|h| hash_of(*h) == *hash))
    }
    fn tx_by_hash(&self, hash: &B256) -> rpc::Result<Option<(u64, usize)>> {
        Ok((1..=A).find(|h| tx_hash_of(*h) == *hash).map(|h| (h, 0)))
    }
    fn receipts(&self, h: u64) -> rpc::Result<Option<Vec<Receipt>>> {
        // Only settled blocks have stored receipts; the gate never lets an
        // unsettled height reach here, but be honest anyway.
        if h == 0 || h > S {
            return Ok(Some(Vec::new()).filter(|_| h == 0));
        }
        Ok(Some(vec![Receipt { status: 1, gas_used: 21_000, cumulative_gas_used: 21_000, logs: Vec::new() }]))
    }
    fn traces(&self, h: u64) -> rpc::Result<Option<Vec<String>>> {
        if h == 0 || h > S {
            return Ok(None);
        }
        Ok(Some(vec!["{\"type\":\"CALL\",\"from\":\"0x1111111111111111111111111111111111111111\",\"to\":\"0xffffffffffffffffffffffffffffffffffffffff\",\"gas\":\"0x5208\",\"gasUsed\":\"0x5208\",\"input\":\"0x\",\"value\":\"0x0\"}".to_string()]))
    }
    fn container(&self, h: u64) -> rpc::Result<Option<Bytes>> {
        Ok((1..=A).contains(&h).then(|| Bytes::from_static(&[0x00, 0x01, 0x02])))
    }
    fn state_at(&self, h: u64) -> rpc::Result<Box<dyn StateRead + '_>> {
        Ok(Box::new(HState(h)))
    }
    fn log_candidates(&self, _: u64, _: u64, _: &[Address], _: &[Vec<B256>]) -> rpc::Result<Option<Vec<u64>>> {
        Ok(None)
    }
    fn tx_range(&self, h: u64) -> rpc::Result<Option<(u64, u32)>> {
        Ok((1..=A).contains(&h).then(|| (h, 1)))
    }
    fn height_of_tx(&self, txnum: u64) -> rpc::Result<Option<u64>> {
        Ok(Some(txnum))
    }
    fn next_tx(&self) -> u64 {
        A + 1
    }
    fn postings(&self, _: &[u8], _: u64, _: u64, _: bool, _: &mut dyn FnMut(&[u8], u64, u8) -> bool) -> rpc::Result<()> {
        Ok(())
    }
    fn groups(&self, _: &[u8], _: &mut dyn FnMut(&[u8]) -> bool) -> rpc::Result<()> {
        Ok(())
    }
    fn set_scan(&self, _: &[u8], _: &mut dyn FnMut(&[u8]) -> bool) -> rpc::Result<()> {
        Ok(())
    }
}

fn server() -> Server {
    let genesis = b"{\"config\":{\"chainId\":123},\"alloc\":{}}";
    let cfg = Arc::new(exec::Config::from_genesis(genesis, b"", 1).unwrap());
    Server::new(Arc::new(Synth), cfg, block(0), json!({"chainId": 123}), None)
}

/// One JSON-RPC round trip through the real dispatch.
fn call(s: &Server, method: &str, params: Value) -> Value {
    let body = json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).to_string();
    serde_json::from_slice(&s.handle(body.as_bytes())).unwrap()
}
fn ok(v: &Value) -> Value {
    assert!(v.get("error").is_none(), "unexpected error: {v}");
    v["result"].clone()
}
fn err_code(v: &Value) -> i64 {
    v["error"]["code"].as_i64().unwrap_or_else(|| panic!("expected an error, got {v}"))
}
fn qty(h: u64) -> String {
    format!("0x{h:x}")
}

#[test]
fn heads_surface() {
    let s = server();
    // eth_blockNumber = the accepted head (the chain height).
    assert_eq!(ok(&call(&s, "eth_blockNumber", json!([]))), json!(qty(A)));
    // The settled-head / lag surface.
    let r = ok(&call(&s, "edb_settledNumber", json!([])));
    assert_eq!(r, json!({"settled": qty(S), "accepted": qty(A), "lag": qty(A - S)}));
    let h = ok(&call(&s, "epochdb_head", json!([])));
    assert_eq!(h["accepted"], json!(qty(A)));
    assert_eq!(h["settled"], json!(qty(S)));
    assert_eq!(h["lag"], json!(qty(A - S)));
    assert_eq!(h["number"], json!(qty(A)));
}

#[test]
fn state_latest_is_settled() {
    let s = server();
    // latest state = the settled head's state (balance == S here).
    assert_eq!(ok(&call(&s, "eth_getBalance", json!([ADDR, "latest"]))), json!(qty(S)));
    assert_eq!(ok(&call(&s, "eth_getTransactionCount", json!([ADDR, "latest"]))), json!(qty(S)));
    // A numeric height at or below the settled head serves that height.
    for h in 1..=S {
        assert_eq!(ok(&call(&s, "eth_getBalance", json!([ADDR, qty(h)]))), json!(qty(h)), "balance at {h}");
    }
    // eth_call runs against the settled head at latest.
    assert!(call(&s, "eth_call", json!([{"to": ADDR, "input": "0x"}, "latest"])).get("error").is_none());
}

#[test]
fn unsettled_height_is_refused_for_state() {
    let s = server();
    for h in (S + 1)..=A {
        for m in ["eth_getBalance", "eth_getTransactionCount", "eth_getCode"] {
            assert_eq!(err_code(&call(&s, m, json!([ADDR, qty(h)]))), NOT_SETTLED_CODE, "{m} at {h}");
        }
        assert_eq!(err_code(&call(&s, "eth_getStorageAt", json!([ADDR, "0x0", qty(h)]))), NOT_SETTLED_CODE);
        assert_eq!(err_code(&call(&s, "eth_call", json!([{"to": ADDR}, qty(h)]))), NOT_SETTLED_CODE);
        assert_eq!(err_code(&call(&s, "eth_estimateGas", json!([{"to": ADDR}, qty(h)]))), NOT_SETTLED_CODE);
    }
    // Above the accepted head is the past-the-head error, not not-settled.
    assert_eq!(err_code(&call(&s, "eth_getBalance", json!([ADDR, qty(A + 1)]))), -32000);
}

#[test]
fn unsettled_block_returns_header_but_not_results() {
    let s = server();
    for h in (S + 1)..=A {
        // The block itself (header + txs) is available at an unsettled height.
        let b = ok(&call(&s, "eth_getBlockByNumber", json!([qty(h), false])));
        assert_eq!(b["number"], json!(qty(h)));
        assert_eq!(b["hash"], json!(hash_of(h)));
        assert_eq!(b["transactions"], json!([tx_hash_of(h)]));
        // But its execution results answer not-settled.
        assert_eq!(err_code(&call(&s, "eth_getBlockReceipts", json!([qty(h)]))), NOT_SETTLED_CODE);
        assert_eq!(err_code(&call(&s, "eth_getTransactionReceipt", json!([tx_hash_of(h)]))), NOT_SETTLED_CODE);
        assert_eq!(err_code(&call(&s, "debug_traceBlockByNumber", json!([qty(h), {}]))), NOT_SETTLED_CODE);
        assert_eq!(err_code(&call(&s, "debug_traceTransaction", json!([tx_hash_of(h), {}]))), NOT_SETTLED_CODE);
    }
}

#[test]
fn settled_block_returns_results_fully() {
    let s = server();
    for h in 1..=S {
        let r = ok(&call(&s, "eth_getBlockReceipts", json!([qty(h)])));
        assert_eq!(r.as_array().unwrap().len(), 1, "receipts at {h}");
        let rc = ok(&call(&s, "eth_getTransactionReceipt", json!([tx_hash_of(h)])));
        assert_eq!(rc["blockNumber"], json!(qty(h)));
        assert_eq!(rc["status"], json!("0x1"));
        // The plain callTracer answers from the stored frames.
        let t = ok(&call(&s, "debug_traceBlockByNumber", json!([qty(h), {"tracer": "callTracer"}])));
        assert_eq!(t.as_array().unwrap().len(), 1, "traces at {h}");
    }
}

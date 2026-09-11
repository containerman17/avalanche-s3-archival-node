//! oracle <http url> [blocks per round] [rounds]: execute the latest blocks with their pre-state
//! read from the node at the parent block, then compare every touched
//! account and slot with the node at the block. Gas used and the receipts
//! root are compared inside `Applier::execute`. The node is pruned, so only
//! recent blocks work (about the last 128).
use alloy_primitives::{Address, B256, U256};
use cnode::exec::{Applier, Base};
use cnode::feed::rpc;
use revm::state::{AccountInfo, Bytecode};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Mutex;

struct RpcBase {
    http: String,
    block: Mutex<u64>,
    accts: Mutex<HashMap<Address, Option<AccountInfo>>>,
    slots: Mutex<HashMap<(Address, U256), U256>>,
    calls: Mutex<u64>,
}

impl RpcBase {
    fn call(&self, method: &str, params: Value) -> Value {
        *self.calls.lock().unwrap() += 1;
        rpc(&self.http, method, params).unwrap_or_else(|e| panic!("{method}: {e:#}"))
    }
    fn tag(&self) -> String {
        format!("0x{:x}", *self.block.lock().unwrap())
    }
    fn reset(&self, block: u64) {
        *self.block.lock().unwrap() = block;
        self.accts.lock().unwrap().clear();
        self.slots.lock().unwrap().clear();
    }
    fn account_at(&self, a: Address) -> Option<AccountInfo> {
        let tag = self.tag();
        let bal: U256 = self.call("eth_getBalance", json!([a, tag])).as_str().unwrap().parse().unwrap();
        let nonce = u64::from_str_radix(self.call("eth_getTransactionCount", json!([a, tag])).as_str().unwrap().trim_start_matches("0x"), 16).unwrap();
        let code: alloy_primitives::Bytes = self.call("eth_getCode", json!([a, tag])).as_str().unwrap().parse().unwrap();
        if bal.is_zero() && nonce == 0 && code.is_empty() {
            return None;
        }
        let code_hash = alloy_primitives::keccak256(&code);
        Some(AccountInfo { balance: bal, nonce, code_hash, code: Some(Bytecode::new_raw(code)), ..Default::default() })
    }
    fn storage_at(&self, a: Address, k: U256) -> U256 {
        self.call("eth_getStorageAt", json!([a, format!("0x{:064x}", k), self.tag()])).as_str().unwrap().parse().unwrap()
    }
}

impl Base for RpcBase {
    fn account(&self, a: Address) -> Option<(AccountInfo, bool)> {
        if let Some(v) = self.accts.lock().unwrap().get(&a) {
            return v.clone().map(|i| (i, false));
        }
        let v = self.account_at(a);
        self.accts.lock().unwrap().insert(a, v.clone());
        v.map(|i| (i, false))
    }
    fn storage(&self, a: Address, slot: U256) -> U256 {
        if let Some(v) = self.slots.lock().unwrap().get(&(a, slot)) {
            return *v;
        }
        let v = self.storage_at(a, slot);
        self.slots.lock().unwrap().insert((a, slot), v);
        v
    }
    fn code(&self, _h: B256) -> Option<Bytecode> {
        None
    }
    fn block_hash(&self, n: u64) -> B256 {
        self.call("eth_getBlockByNumber", json!([format!("0x{n:x}"), false]))["hash"].as_str().unwrap().parse().unwrap()
    }
}

fn main() {
    let a: Vec<String> = std::env::args().collect();
    let http = a[1].clone();
    let n: u64 = a.get(2).map_or(4, |s| s.parse().unwrap());
    let rounds: u64 = a.get(3).map_or(1, |s| s.parse().unwrap());
    let base = RpcBase { http: http.clone(), block: Mutex::new(0), accts: Mutex::new(HashMap::new()), slots: Mutex::new(HashMap::new()), calls: Mutex::new(0) };
    let mut ap = Applier::new(base);
    let mut bad = 0;
    let mut done = 0u64;
    let mut last_head = 0u64;
    for _round in 0..rounds {
    let head = u64::from_str_radix(rpc(&http, "eth_blockNumber", json!([])).unwrap().as_str().unwrap().trim_start_matches("0x"), 16).unwrap();
    if head < last_head + n {
        std::thread::sleep(std::time::Duration::from_secs((last_head + n - head) * 2));
    }
    last_head = head;
    for h in (head - n)..head {
        done += 1;
        ap.ex.db_mut().base.reset(h - 1);
        let json = rpc(&http, "eth_getBlockByNumber", json!([format!("0x{h:x}"), true])).unwrap();
        let t = std::time::Instant::now();
        let (b, r) = match ap.execute(&json) {
            Ok(x) => x,
            Err(e) => {
                println!("block {h}: EXEC FAILED: {e:#}");
                bad += 1;
                ap.ex.db_mut().take_diff();
                continue;
            }
        };
        let exec_ms = t.elapsed().as_millis();
        let (accts, slots) = ap.ex.db_mut().touched();
        ap.ex.db_mut().base.reset(h);
        let mut mism = 0;
        for (addr, info) in &accts {
            let want = ap.ex.db_mut().base.account_at(*addr);
            let ok = match (info, &want) {
                (None, None) => true,
                (Some(i), Some(w)) => i.balance == w.balance && i.nonce == w.nonce && i.code_hash == w.code_hash,
                _ => false,
            };
            if !ok {
                mism += 1;
                println!("  block {h} account {addr}: ours {:?} node {:?}", info.as_ref().map(|i| (i.nonce, i.balance, i.code_hash)), want.map(|i| (i.nonce, i.balance, i.code_hash)));
            }
        }
        for (addr, k, v) in &slots {
            let want = ap.ex.db_mut().base.storage_at(*addr, *k);
            if want != *v {
                mism += 1;
                println!("  block {h} slot {addr} {k:#x}: ours {v:#x} node {want:#x}");
            }
        }
        let extra = json["blockExtraData"].as_str().map_or(0, |s| (s.len() - 2) / 2);
        println!("block {h}: txs {} gas {} atomic_bytes {extra} touched {} accounts {} slots exec {exec_ms} ms rpc calls {} mismatches {mism}", b.txs.len(), r.gas_used, accts.len(), slots.len(), *ap.ex.db_mut().base.calls.lock().unwrap());
        bad += mism;
        ap.ex.db_mut().take_diff();
    }
    }
    println!("done: {done} blocks, {bad} problems");
    std::process::exit(if bad == 0 { 0 } else { 1 });
}

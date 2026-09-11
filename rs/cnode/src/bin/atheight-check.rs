//! atheight-check --data <dir> --bootstrap <export dir> --height H [--http URL] [--n 1000]
//! Acceptance 6: Mode::AtHeight(H) against eth_getBalance / eth_getTransactionCount /
//! eth_getStorageAt at H on the local node, for keys sampled from H's diff (hashed keys
//! only, so the addresses come from the block's transactions: from, to, and every
//! access-list entry; slots from the access lists). H must be inside the node's window.
use alloy_primitives::{Address, B256, U256};
use cnode::feed::rpc;
use cnode::hot::{addr_hash, slot_hash};
use cnode::{Config, Mode, Node};
use serde_json::json;
use std::collections::HashSet;
use std::path::PathBuf;

fn arg(a: &[String], k: &str) -> Option<String> {
    a.iter().position(|x| x == k).map(|i| a[i + 1].clone())
}

fn main() {
    let a: Vec<String> = std::env::args().collect();
    let h: u64 = arg(&a, "--height").expect("--height").parse().unwrap();
    let http = arg(&a, "--http").unwrap_or_else(|| "http://127.0.0.1:9650/ext/bc/C/rpc".into());
    let n: usize = arg(&a, "--n").map_or(1000, |s| s.parse().unwrap());
    let cfg = Config {
        rpc_ws: String::new(),
        rpc_http: http.clone(),
        validator_ws: vec![],
        data_dir: PathBuf::from(arg(&a, "--data").expect("--data")),
        bootstrap_dir: PathBuf::from(arg(&a, "--bootstrap").expect("--bootstrap")),
        checker_lag_blocks: 60,
        snapshot_every_blocks: 4000,
        dump_every_blocks: 0,
    };
    let t = std::time::Instant::now();
    let node = Node::open(cfg, Mode::AtHeight(h)).expect("open");
    let g = node.generation();
    println!("frozen at {} ({:?}) in {:.1?}", g.height, node.status(), t.elapsed());
    let tag = format!("0x{h:x}");
    let blk = node.history.block(h).unwrap().expect("block in history").block;
    let mut addrs: HashSet<Address> = HashSet::new();
    let mut slots: Vec<(Address, U256)> = Vec::new();
    for tx in blk["transactions"].as_array().unwrap() {
        for k in ["from", "to"] {
            if let Some(s) = tx[k].as_str() {
                addrs.insert(s.parse().unwrap());
            }
        }
        for e in tx["accessList"].as_array().unwrap_or(&vec![]) {
            let ad: Address = e["address"].as_str().unwrap().parse().unwrap();
            addrs.insert(ad);
            for k in e["storageKeys"].as_array().unwrap_or(&vec![]) {
                slots.push((ad, k.as_str().unwrap().parse::<B256>().unwrap().into()));
            }
        }
    }
    addrs.insert(blk["miner"].as_str().unwrap().parse().unwrap());
    let mut bad = 0;
    let mut checked = 0;
    for ad in addrs.iter().take(n) {
        let ours = node.account(g, *ad).unwrap();
        let bal: U256 = rpc(&http, "eth_getBalance", json!([ad, tag])).unwrap().as_str().unwrap().parse().unwrap();
        let nonce = u64::from_str_radix(rpc(&http, "eth_getTransactionCount", json!([ad, tag])).unwrap().as_str().unwrap().trim_start_matches("0x"), 16).unwrap();
        let (ob, on) = ours.map_or((U256::ZERO, 0), |a| (a.balance, a.nonce));
        checked += 1;
        if ob != bal || on != nonce {
            bad += 1;
            println!("account {ad}: ours {on}/{ob} node {nonce}/{bal}");
        }
    }
    for (ad, k) in slots.iter().take(n) {
        let ours = node.storage_by_hash(g, &addr_hash(ad), &slot_hash(&cnode::exec::mask(*k))).unwrap();
        let want: U256 = rpc(&http, "eth_getStorageAt", json!([ad, format!("0x{:064x}", k), tag])).unwrap().as_str().unwrap().parse().unwrap();
        checked += 1;
        if ours != want {
            bad += 1;
            println!("slot {ad} {k:#x}: ours {ours:#x} node {want:#x}");
        }
    }
    println!("height {h}: {checked} keys checked ({} accounts, {} slots), {bad} mismatches", addrs.len().min(n), slots.len().min(n));
    std::process::exit(if bad == 0 { 0 } else { 1 });
}

//! The engine on a private chain in a temp dir: siblings with the root
//! inside verify, the built block through verify, reject, reopen.
use std::sync::Arc;

use alloy_primitives::{keccak256, Address, U256};

use crate::build::Params;
use crate::synth::{genesis_json, Signer};
use crate::{Engine, Init, NodeEngine, Tree};

fn tmp(name: &str) -> String {
    let d = std::env::temp_dir().join(format!("epochdb-chain-test-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    d.to_string_lossy().into_owned()
}

fn open(dir: &str, signer: &Signer) -> Tree<NodeEngine> {
    let _g = crate::ENV_LOCK.lock().unwrap();
    let init = Init { network_id: 1, subnet_id: [7; 32], chain_id: [9; 32], chain_data_dir: dir.into(), genesis_bytes: genesis_json(signer.address).into_bytes(), upgrade_bytes: Vec::new(), config_bytes: b"{}".to_vec() };
    Tree::new(NodeEngine::open(&init).unwrap())
}

fn to(i: u64) -> Address {
    Address::from_slice(&keccak256(i.to_be_bytes())[12..])
}

/// Two children of the head built from different tx sets, each with its
/// own root computed at build; a grandchild on each; accept one branch,
/// reject the other; the accepted state is the branch's; a fresh open
/// recovers it.
#[test]
fn siblings_root_in_verify_accept_one_reject_other() {
    let s = Signer::new([0x42; 32]);
    let dir = tmp("siblings");
    let t = open(&dir, &s);
    t.engine.set_state(true);
    let g = t.engine.last_accepted();
    let tx = |nonce: u64, i: u64| s.transfer(99999, nonce, 1_000_000_000, 50_000_000_000, 21_000, to(i), U256::from(1_000_000_000_000u64));
    let ts = 1_770_000_000_000u64;
    let cb = Address::from([0xcc; 20]);
    let p = |ms: u64| Params { timestamp_ms: ms, coinbase: cb, desired_min_delay_excess: None };

    // A: 2 transfers; B: 1 transfer to a different recipient.
    let a = t.engine.build(None, &g.header, &p(ts), None, Some(vec![tx(0, 1), tx(1, 2)]), &[]).unwrap();
    let b = t.engine.build(None, &g.header, &p(ts), None, Some(vec![tx(0, 3)]), &[]).unwrap();
    assert_ne!(a.block.hash, b.block.hash);
    assert_ne!(a.block.header.root, b.block.header.root);
    assert!(a.pending.has_layer() && b.pending.has_layer());
    assert_eq!(a.included, vec![0, 1]);
    let (ha, hb) = (a.block.hash.0, b.block.hash.0);
    t.insert_verified(a.block.clone(), a.pending);
    t.insert_verified(b.block.clone(), b.pending);
    // Each branch's pending state: A spent two nonces, B one.
    assert_eq!(t.engine.accounts(t.pending(&ha).as_ref(), &[s.address])[0].0, 2);
    assert_eq!(t.engine.accounts(t.pending(&hb).as_ref(), &[s.address])[0].0, 1);
    assert_eq!(t.engine.accounts(None, &[s.address])[0].0, 0);

    // Grandchildren: on A the next nonce is 2, on B it is 1.
    let ca = t.engine.build(t.pending(&ha).as_ref(), &a.block.header, &p(ts + 2000), None, Some(vec![tx(2, 4)]), &[]).unwrap();
    let cb_ = t.engine.build(t.pending(&hb).as_ref(), &b.block.header, &p(ts + 2000), None, Some(vec![tx(1, 5), tx(0, 6)]), &[]).unwrap();
    assert_eq!(ca.included, vec![0]);
    assert_eq!(cb_.included, vec![0], "nonce 0 is too low on B: skipped, not popped");
    assert_eq!(cb_.reasons[1], exec::exec::SkipReason::NonceTooLow);
    let (hca, hcb) = (ca.block.hash.0, cb_.block.hash.0);
    t.insert_verified(ca.block.clone(), ca.pending);
    t.insert_verified(cb_.block.clone(), cb_.pending);

    // The built blocks verify as lookups (same bytes, same id).
    let again = t.engine.parse(a.block.container.clone()).unwrap();
    assert_eq!(again.hash, a.block.hash);
    t.verify(again, None).unwrap();

    // Accept A and its child, reject B's branch.
    t.accept(&ha).unwrap();
    t.reject(&hb);
    t.reject(&hcb);
    t.accept(&hca).unwrap();
    assert_eq!(t.engine.last_accepted().hash.0, hca);
    assert_eq!(t.engine.accounts(None, &[s.address])[0].0, 3);
    assert_eq!(t.engine.accounts(None, &[to(1), to(3), to(4)]).iter().map(|x| x.1).collect::<Vec<_>>(), vec![U256::from(1_000_000_000_000u64), U256::ZERO, U256::from(1_000_000_000_000u64)]);
    assert_eq!(t.verified_len(), 0);

    // A block re-parsed from bytes on the new head verifies with the root inline and accepts.
    let d = t.engine.build(None, &ca.block.header, &p(ts + 4000), None, Some(vec![tx(3, 7)]), &[]).unwrap();
    let parsed = t.engine.parse(d.block.container.clone()).unwrap();
    drop(d);
    let m = t.verify(parsed, None).unwrap();
    assert_eq!(m.height, 3);
    t.accept(&m.id).unwrap();

    // A block whose header claims a wrong root fails verify (not the process).
    let e = t.engine.build(None, &t.engine.last_accepted().header, &p(ts + 6000), None, Some(vec![tx(4, 8)]), &[]).unwrap();
    assert_eq!(e.included, vec![0], "reasons {:?} nonce {:?}", e.reasons, t.engine.accounts(None, &[s.address]));
    let mut hdr = e.block.header.clone();
    hdr.root = alloy_primitives::B256::repeat_byte(0xab);
    let (h2, _, bytes) = crate::build::assemble(hdr, &e.block.txs.iter().collect::<Vec<_>>(), alloy_primitives::B256::repeat_byte(0xab), &exec::BlockResult { gas_used: e.block.header.gas_used, receipts_root: e.block.header.receipt_hash, bloom: e.block.header.bloom, txs: Vec::new(), tail: Vec::new(), code: Vec::new() }, &[]).unwrap();
    assert_eq!(h2.gas_used, 21_000);
    let bad = t.engine.parse(bytes::Bytes::from(bytes)).unwrap();
    let err = t.verify(bad, None).unwrap_err().to_string();
    assert!(err.contains("receiptsRoot") || err.contains("state root mismatch"), "{err}");

    t.engine.shutdown();
    drop(t);
    let t = open(&dir, &s);
    assert_eq!(t.engine.last_accepted().height, 3);
    assert_eq!(t.engine.accounts(None, &[s.address])[0].0, 4);
    t.engine.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

/// Bootstrapping (checker one block behind) then NormalOp: the switch
/// drains the checker and the next verify computes the root itself.
#[test]
fn bootstrapping_then_normal_op() {
    let s = Signer::new([0x43; 32]);
    let dir = tmp("switch");
    let t = open(&dir, &s);
    let tx = |nonce: u64, i: u64| s.transfer(99999, nonce, 1_000_000_000, 50_000_000_000, 21_000, to(i), U256::from(7u64));
    // Build needs NormalOp; blocks for the bootstrapping phase come from a NormalOp engine on a second dir.
    let src = open(&tmp("switch-src"), &s);
    src.engine.set_state(true);
    let mut blocks = Vec::new();
    let mut parent = src.engine.last_accepted();
    for n in 0..3u64 {
        let b = src.engine.build(None, &parent.header, &Params { timestamp_ms: 1_770_000_000_000 + n * 2000, coinbase: Address::from([1; 20]), desired_min_delay_excess: None }, None, Some(vec![tx(n, n)]), &[]).unwrap();
        src.insert_verified(b.block.clone(), b.pending);
        src.accept(&b.block.hash.0).unwrap();
        parent = b.block.clone();
        blocks.push(b.block);
    }
    src.engine.shutdown();
    // Bootstrapping: two blocks through the checker path (no layer).
    for b in &blocks[..2] {
        let p = t.engine.parse(b.container.clone()).unwrap();
        t.verify(p, None).unwrap();
        assert!(!t.pending(&b.hash.0).unwrap().has_layer());
        t.accept(&b.hash.0).unwrap();
    }
    t.engine.set_state(true);
    let p = t.engine.parse(blocks[2].container.clone()).unwrap();
    t.verify(p, None).unwrap();
    assert!(t.pending(&blocks[2].hash.0).unwrap().has_layer(), "NormalOp verify carries the root layer");
    t.accept(&blocks[2].hash.0).unwrap();
    assert_eq!(t.engine.accounts(None, &[s.address])[0].0, 3);
    t.engine.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
    let _ = std::fs::remove_dir_all(tmp("switch-src"));
    let _ = Arc::new(0);
}

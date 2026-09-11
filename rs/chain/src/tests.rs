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
    let (h2, _, bytes) = crate::build::assemble(hdr, &e.block.txs.iter().collect::<Vec<_>>(), alloy_primitives::B256::repeat_byte(0xab), e.block.header.receipt_hash, e.block.header.tx_hash, &exec::BlockResult { gas_used: e.block.header.gas_used, receipts_root: e.block.header.receipt_hash, bloom: e.block.header.bloom, txs: Vec::new(), tail: Vec::new(), code: Vec::new() }, &[]).unwrap();
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

fn open_sae(dir: &str, signer: &Signer, k: u64) -> Tree<NodeEngine> {
    let _g = crate::ENV_LOCK.lock().unwrap();
    let config = format!(r#"{{"sae":true,"sae-settlement-blocks":{k}}}"#);
    let init = Init { network_id: 1, subnet_id: [7; 32], chain_id: [9; 32], chain_data_dir: dir.into(), genesis_bytes: genesis_json(signer.address).into_bytes(), upgrade_bytes: Vec::new(), config_bytes: config.into_bytes() };
    Tree::new(NodeEngine::open(&init).unwrap())
}

/// Wait until the SAE executor has settled through height `h` (the executor is
/// asynchronous; k blocks behind the accepted head).
fn wait_settled(t: &Tree<NodeEngine>, h: u64) {
    let sae = t.engine.sae.clone().expect("sae mode");
    for _ in 0..2000 {
        if sae.settled_head.load(std::sync::atomic::Ordering::Relaxed) >= h {
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    panic!("SAE settlement did not reach height {h} (settled head {})", sae.settled_head.load(std::sync::atomic::Ordering::Relaxed));
}

/// The SAE oracle in the plugin engine: a chain built without execution (the
/// header at h commits the settled root of h-k, gasUsed is worst-case
/// sum(gasLimit)), accepted on the vote path, settled asynchronously k blocks
/// behind. Every settled root must equal a full SYNCHRONOUS re-execution of the
/// same txs, and heights pipeline (verify_light lets a chain of verified-but-
/// not-accepted blocks stand > 1 deep with no executed state).
#[test]
fn sae_settled_roots_match_synchronous_reexecution() {
    let s = Signer::new([0x51; 32]);
    let ts = 1_770_000_000_000u64;
    let cb = Address::from([0xcc; 20]);
    let p = |ms: u64| Params { timestamp_ms: ms, coinbase: cb, desired_min_delay_excess: None };
    // Block h (1-indexed) sends one transfer of nonce h-1 to recipient to(h).
    let set = |h: u64| vec![s.transfer(99999, h - 1, 1_000_000_000, 50_000_000_000, 21_000, to(h), U256::from(1_000_000_000_000u64))];
    let n: u64 = 8;
    let k: u64 = 2;

    // The synchronous oracle: build + accept the same txs, record each height's
    // own post-execution root (the sync header's root).
    let sdir = tmp("sae-oracle-sync");
    let sync = open(&sdir, &s);
    sync.engine.set_state(true);
    let mut sync_root = vec![sync.engine.last_accepted().header.root]; // index by height, 0 = genesis
    for h in 1..=n {
        let parent = sync.engine.last_accepted();
        let b = sync.engine.build(None, &parent.header, &p(ts + h * 2000), None, Some(set(h)), &[]).unwrap();
        sync_root.push(b.block.header.root);
        let id = b.block.hash.0;
        sync.insert_verified(b.block, b.pending);
        sync.accept(&id).unwrap();
    }
    sync.engine.shutdown();

    // The SAE engine: build without execution, accept, settle behind.
    let edir = tmp("sae-oracle");
    let te = open_sae(&edir, &s, k);
    te.engine.set_state(true);
    let genesis_root = te.engine.last_accepted().header.root;
    for h in 1..=n {
        let parent = te.engine.last_accepted();
        let b = te.engine.build(None, &parent.header, &p(ts + h * 2000), None, Some(set(h)), &[]).unwrap();
        // The header commits the settled root of h-k (genesis for h <= k), and
        // worst-case gasUsed = sum(gasLimit).
        assert_eq!(b.block.header.gas_used, 21_000, "block {h} worst-case gasUsed");
        let want_root = if h <= k { genesis_root } else { sync_root[(h - k) as usize] };
        assert_eq!(b.block.header.root, want_root, "block {h} header commits settled root of h-k");
        // Verify (verify_light, no execution) then accept.
        let parsed = te.engine.parse(b.block.container.clone()).unwrap();
        te.verify(parsed, None).unwrap();
        te.accept(&b.block.hash.0).unwrap();
        // Building each block on the accepted head keeps the settlement window
        // small enough that h-k is settled by the time we build h.
        if h > k {
            wait_settled(&te, h - k);
        }
    }
    // Settle the tail and check every settled root against the sync oracle.
    wait_settled(&te, n);
    let sae = te.engine.sae.clone().unwrap();
    for h in 1..=n {
        assert_eq!(sae.settled_root(h), Some(sync_root[h as usize]), "SAE settled root at height {h} != synchronous re-execution");
    }
    assert_eq!(te.engine.last_accepted().height, n, "accepted head");
    assert_eq!(sae.settled_head.load(std::sync::atomic::Ordering::Relaxed), n, "settled head caught up");
    te.engine.shutdown();

    let _ = std::fs::remove_dir_all(&sdir);
    let _ = std::fs::remove_dir_all(&edir);
}

/// verify_light pipelines: a chain of verified-but-not-accepted SAE blocks
/// stands several deep with no executed state (heights in flight > 1, the SAE
/// point), then accepts in order.
#[test]
fn sae_pipelines_verified_but_not_accepted_blocks() {
    let s = Signer::new([0x71; 32]);
    let ts = 1_770_000_000_000u64;
    let cb = Address::from([0xcc; 20]);
    let p = |ms: u64| Params { timestamp_ms: ms, coinbase: cb, desired_min_delay_excess: None };
    let tx = |nonce: u64, i: u64| s.transfer(99999, nonce, 1_000_000_000, 50_000_000_000, 21_000, to(i), U256::from(1_000_000_000_000u64));
    let edir = tmp("sae-pipeline");
    let te = open_sae(&edir, &s, 3);
    te.engine.set_state(true);

    // Build a 4-deep chain, each on the previous block's pending state, and
    // verify every block WITHOUT accepting any: the projection overlay carries
    // the ancestors' nonces so verify_light succeeds with no executed state.
    let g = te.engine.last_accepted();
    let b1 = te.engine.build(None, &g.header, &p(ts + 2000), None, Some(vec![tx(0, 1)]), &[]).unwrap();
    te.insert_verified(b1.block.clone(), b1.pending);
    let b2 = te.engine.build(te.pending(&b1.block.hash.0).as_ref(), &b1.block.header, &p(ts + 4000), None, Some(vec![tx(1, 2)]), &[&b1.block.txs[0]]).unwrap();
    te.insert_verified(b2.block.clone(), b2.pending);
    let b3 = te.engine.build(te.pending(&b2.block.hash.0).as_ref(), &b2.block.header, &p(ts + 6000), None, Some(vec![tx(2, 3)]), &[&b1.block.txs[0], &b2.block.txs[0]]).unwrap();
    te.insert_verified(b3.block.clone(), b3.pending);
    assert_eq!(te.verified_len(), 3, "three heights in flight, none accepted, no executed state");

    // A sibling of b2 on b1 (a fork) also verifies against the same overlay.
    let b2b = te.engine.build(te.pending(&b1.block.hash.0).as_ref(), &b1.block.header, &p(ts + 4000), None, Some(vec![tx(1, 9)]), &[&b1.block.txs[0]]).unwrap();
    let sib = te.engine.parse(b2b.block.container.clone()).unwrap();
    drop(b2b);
    te.verify(sib, None).unwrap();
    assert_eq!(te.verified_len(), 4);

    // Accept the b1->b2->b3 line in order; the fork is rejected.
    te.accept(&b1.block.hash.0).unwrap();
    te.accept(&b2.block.hash.0).unwrap();
    te.accept(&b3.block.hash.0).unwrap();
    wait_settled(&te, 3);
    assert_eq!(te.engine.last_accepted().height, 3);
    let sae = te.engine.sae.clone().unwrap();
    assert!(sae.settled_root(3).is_some());
    te.engine.shutdown();
    let _ = std::fs::remove_dir_all(&edir);
}

/// Reproduces the fleet load-phase funder: one sender bursts consecutive nonces
/// through the pool, blocks are built from the pool and accepted, they settle k
/// behind, and then a fresh tx at the next nonce must be admitted (not "nonce
/// too low"). Pins the SAE pool admission against the projected nonce.
#[test]
fn sae_pool_admits_a_funder_burst_across_settlement() {
    use crate::pool::Code;
    let s = Signer::new([0x99; 32]);
    let ts = 1_770_000_000_000u64;
    let cb = Address::from([0xcc; 20]);
    let p = |ms: u64| Params { timestamp_ms: ms, coinbase: cb, desired_min_delay_excess: None };
    let raw = |nonce: u64| s.transfer(99999, nonce, 1_000_000_000, 50_000_000_000, 21_000, to(nonce + 1), U256::from(1_000_000u64)).raw;
    let edir = tmp("sae-pool-burst");
    let te = open_sae(&edir, &s, 4);
    te.engine.set_state(true);

    // Burst 6 consecutive nonces into the pool (as the funder does).
    let added = te.engine.pool_add((0..6).map(raw).collect(), true);
    for (i, a) in added.iter().enumerate() {
        assert_eq!(a.code, Code::Ok, "burst tx {i}: {}", a.message);
    }
    // Build from the pool and accept until the burst is mined.
    let mut h = 0u64;
    loop {
        let parent = te.engine.last_accepted();
        let b = te.engine.build(None, &parent.header, &p(ts + (h + 1) * 2000), None, None, &[]).unwrap();
        if b.included.is_empty() {
            break;
        }
        te.insert_verified(b.block.clone(), b.pending);
        te.accept(&b.block.hash.0).unwrap();
        h = b.block.height;
        if h >= 20 {
            break;
        }
    }
    wait_settled(&te, h);
    // The next nonce (6) must be admitted, not "nonce too low".
    let a = te.engine.pool_add(vec![raw(6)], true);
    assert_eq!(a[0].code, Code::Ok, "post-settlement tx nonce 6: {}", a[0].message);
    te.engine.shutdown();
    let _ = std::fs::remove_dir_all(&edir);
}

/// The functional-phase nonce-gap through the SAE pool: one sender mines nonces
/// 0..6, then a gap tx (nonce 8) is queued and a fill (nonce 7) must be admitted
/// (not "nonce too low"). Reproduces the network gap-fill single node.
#[test]
fn sae_pool_gap_then_fill() {
    use crate::pool::Code;
    let s = Signer::new([0xa1; 32]);
    let ts = 1_770_000_000_000u64;
    let cb = Address::from([0xcc; 20]);
    let p = |ms: u64| Params { timestamp_ms: ms, coinbase: cb, desired_min_delay_excess: None };
    let raw = |nonce: u64| s.transfer(99999, nonce, 1_000_000_000, 50_000_000_000, 21_000, to(nonce + 1), U256::from(1_000_000u64)).raw;
    let edir = tmp("sae-gap-fill");
    let te = open_sae(&edir, &s, 8);
    te.engine.set_state(true);
    // Mine nonces 0..6 one per block (as the functional transfers/deploy/calls).
    for n in 0..7u64 {
        let parent = te.engine.last_accepted();
        let a = te.engine.pool_add(vec![raw(n)], true);
        assert_eq!(a[0].code, Code::Ok, "add nonce {n}: {}", a[0].message);
        let b = te.engine.build(None, &parent.header, &p(ts + (n + 1) * 2000), None, None, &[]).unwrap();
        assert_eq!(b.included.len(), 1, "block for nonce {n}");
        te.insert_verified(b.block.clone(), b.pending);
        te.accept(&b.block.hash.0).unwrap();
    }
    wait_settled(&te, 7);
    // Gap tx (nonce 8) queues; fill (nonce 7) must be admitted.
    let g = te.engine.pool_add(vec![raw(8)], true);
    assert_eq!(g[0].code, Code::Ok, "gap tx: {}", g[0].message);
    let f = te.engine.pool_add(vec![raw(7)], true);
    assert_eq!(f[0].code, Code::Ok, "fill tx nonce 7: {}", f[0].message);
    te.engine.shutdown();
    let _ = std::fs::remove_dir_all(&edir);
}

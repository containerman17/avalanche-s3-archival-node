//! The C ABI of the epochdb engine (rs/ffi/ABI.md is the contract): a Go
//! validator shell drives parse / verify / accept / reject / build over cgo,
//! reads account state and the head header for its mempool, and forwards
//! JSON-RPC bodies. One `epochdb_engine` per chain; every function may be
//! called from any thread (the engine serializes execution behind its own
//! mutex, reads take snapshots). Panics never cross the boundary: each entry
//! point runs under `catch_unwind` and reports `EPOCHDB_EPANIC`. Memory
//! handed to C is always an `epochdb_buf`, freed with `epochdb_buf_free`.
#![allow(clippy::missing_safety_doc, non_camel_case_types)]

use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{Arc, Mutex};

use alloy_primitives::{Address, B256};
use bytes::Bytes;
use chain::build::Params;
use chain::node_engine::Pending;
use chain::{Engine, Id, Init, NodeEngine, Tree};

/// jemalloc for the engine's allocations (the plugin's choice too: glibc
/// kept the seals' transients); its symbols are prefixed, so the host's
/// malloc is untouched.
#[cfg(feature = "jemalloc")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use std::os::raw::c_int;

pub const EPOCHDB_OK: c_int = 0;
/// The operation failed; `epochdb_last_error` has the text.
pub const EPOCHDB_ERR: c_int = -1;
/// A null pointer, a bad length or an unknown enum value.
pub const EPOCHDB_EINVAL: c_int = -2;
/// The block id (or height) is unknown to the engine.
pub const EPOCHDB_ENOTFOUND: c_int = -3;
/// A Rust panic was caught at the boundary; the engine should be closed.
pub const EPOCHDB_EPANIC: c_int = -4;

/// A Rust-owned byte buffer. `ptr` is null when `len` is 0. Free exactly
/// once with `epochdb_buf_free`.
#[repr(C)]
pub struct epochdb_buf {
    pub ptr: *mut u8,
    pub len: usize,
}

#[repr(C)]
pub struct epochdb_block_meta {
    /// keccak(header): the block id.
    pub id: [u8; 32],
    pub parent: [u8; 32],
    pub height: u64,
    /// Unix seconds.
    pub timestamp: u64,
}

#[repr(C)]
pub struct epochdb_verify_out {
    pub state_root: [u8; 32],
    pub gas_used: u64,
    pub tx_count: u64,
}

#[repr(C)]
pub struct epochdb_build_out {
    /// The inner block RLP.
    pub block_bytes: epochdb_buf,
    pub id: [u8; 32],
    pub gas_used: u64,
    pub included_count: u64,
    /// One byte per candidate: 0 included, 1 nonce too low (skipped), 2 the
    /// tx failed (sender popped), 3 an earlier tx of the sender was popped,
    /// 4 no gas left for it (popped), 5 over the 1800 KiB size target
    /// (popped), 6 not reached (the loop stopped under 21,000 gas).
    pub skipped: epochdb_buf,
    /// 1 when the gas limit is not filled and every candidate was considered.
    pub needs_more: u8,
    /// Nanoseconds per phase: 0 candidate decode, 1 lock + parent + header
    /// template, 2 sender recovery, 3 execution, 4 fee check + finish, 5
    /// state root, 6 assemble + hash, 7 parsed-cache insert, 8 tree insert,
    /// 9 result copy into the out buffers.
    pub phase_ns: [u64; 10],
}

/// One open chain.
pub struct epochdb_engine {
    tree: Tree<NodeEngine>,
    err: Mutex<String>,
}

fn buf(v: Vec<u8>) -> epochdb_buf {
    if v.is_empty() {
        return epochdb_buf { ptr: std::ptr::null_mut(), len: 0 };
    }
    let b = v.into_boxed_slice();
    let len = b.len();
    epochdb_buf { ptr: Box::into_raw(b) as *mut u8, len }
}

unsafe fn slice<'a>(p: *const u8, len: usize) -> Option<&'a [u8]> {
    if len == 0 {
        return Some(&[]);
    }
    if p.is_null() {
        return None;
    }
    Some(std::slice::from_raw_parts(p, len))
}

unsafe fn id32(p: *const u8) -> Option<Id> {
    slice(p, 32).map(|s| s.try_into().unwrap())
}

impl epochdb_engine {
    fn fail(&self, code: c_int, msg: impl ToString) -> c_int {
        *self.err.lock().unwrap() = msg.to_string();
        code
    }

    /// Runs `f` with panics turned into EPOCHDB_EPANIC.
    fn guard(&self, f: impl FnOnce() -> Result<c_int, (c_int, String)>) -> c_int {
        match catch_unwind(AssertUnwindSafe(f)) {
            Ok(Ok(c)) => c,
            Ok(Err((c, m))) => self.fail(c, m),
            Err(p) => {
                let m = p.downcast_ref::<String>().cloned().or_else(|| p.downcast_ref::<&str>().map(|s| s.to_string())).unwrap_or_else(|| "panic".into());
                self.fail(EPOCHDB_EPANIC, format!("panic: {m}"))
            }
        }
    }

    /// A block by id: verified in the tree, parsed, or accepted.
    fn block(&self, id: &Id) -> Option<Arc<block::Block>> {
        if let Some(b) = self.tree.get_block(id) {
            return Some((*b).clone());
        }
        self.tree.engine.parsed(id)
    }

    /// The parent for build / account reads: None = the accepted head.
    fn parent(&self, id: &Id) -> Result<(Option<Arc<Pending>>, Arc<block::Block>), (c_int, String)> {
        let head = self.tree.engine.last_accepted();
        if *id == [0u8; 32] || *id == head.hash.0 {
            return Ok((None, head));
        }
        match (self.tree.pending(id), self.tree.get_block(id)) {
            (Some(p), Some(b)) => Ok((Some(p), (*b).clone())),
            _ => Err((EPOCHDB_ENOTFOUND, format!("block {} is neither the accepted head nor a verified block", chain::tree::hex(id)))),
        }
    }
}

fn ferr(m: impl ToString) -> (c_int, String) {
    (EPOCHDB_ERR, m.to_string())
}

/// Opens (or reopens) the chain under `data_dir`. Returns null on failure
/// with the message in `err` (free it). `config_json` takes the plugin's
/// keys (roll-budget-mb, roll-every-blocks, s3-*, min-delay-target, ...).
#[no_mangle]
pub unsafe extern "C" fn epochdb_open(
    data_dir: *const u8,
    data_dir_len: usize,
    genesis_json: *const u8,
    genesis_len: usize,
    upgrade_json: *const u8,
    upgrade_len: usize,
    config_json: *const u8,
    config_len: usize,
    chain_id: *const u8,
    subnet_id: *const u8,
    network_id: u32,
    err: *mut epochdb_buf,
) -> *mut epochdb_engine {
    let set_err = |m: String| {
        if !err.is_null() {
            *err = buf(m.into_bytes());
        }
        std::ptr::null_mut()
    };
    let r = catch_unwind(|| {
        let (Some(dir), Some(g), Some(u), Some(c), Some(cid), Some(sid)) = (slice(data_dir, data_dir_len), slice(genesis_json, genesis_len), slice(upgrade_json, upgrade_len), slice(config_json, config_len), id32(chain_id), id32(subnet_id)) else {
            return Err("epochdb_open: null argument".to_string());
        };
        let init = Init {
            network_id,
            subnet_id: sid,
            chain_id: cid,
            chain_data_dir: String::from_utf8(dir.to_vec()).map_err(|_| "data_dir is not UTF-8".to_string())?,
            genesis_bytes: g.to_vec(),
            upgrade_bytes: u.to_vec(),
            config_bytes: c.to_vec(),
        };
        let engine = NodeEngine::open(&init).map_err(|e| e.to_string())?;
        Ok(Box::into_raw(Box::new(epochdb_engine { tree: Tree::new(engine), err: Mutex::new(String::new()) })))
    });
    match r {
        Ok(Ok(p)) => p,
        Ok(Err(m)) => set_err(m),
        Err(_) => set_err("panic in epochdb_open".into()),
    }
}

/// Shutdown (rolls finished, the store synced and closed) and free.
#[no_mangle]
pub unsafe extern "C" fn epochdb_close(e: *mut epochdb_engine) {
    if e.is_null() {
        return;
    }
    let b = Box::from_raw(e);
    let _ = catch_unwind(AssertUnwindSafe(|| b.tree.engine.shutdown()));
    drop(b);
}

/// The text of the last error this engine reported (empty if none).
#[no_mangle]
pub unsafe extern "C" fn epochdb_last_error(e: *const epochdb_engine, out: *mut epochdb_buf) -> c_int {
    if e.is_null() || out.is_null() {
        return EPOCHDB_EINVAL;
    }
    *out = buf((*e).err.lock().unwrap().clone().into_bytes());
    EPOCHDB_OK
}

/// snow.State: 1 = bootstrapping (roots checked one block behind), 2 =
/// normal op (the root inside verify, build allowed).
#[no_mangle]
pub unsafe extern "C" fn epochdb_set_state(e: *mut epochdb_engine, state: u32) -> c_int {
    if e.is_null() {
        return EPOCHDB_EINVAL;
    }
    let en = &*e;
    en.guard(|| {
        match state {
            1 => en.tree.engine.set_state(false),
            2 => en.tree.engine.set_state(true),
            _ => return Err((EPOCHDB_EINVAL, format!("unknown state {state}"))),
        }
        Ok(EPOCHDB_OK)
    })
}

/// Decodes a block (the inner RLP; a proposervm container is accepted too),
/// recovers senders, caches it by id.
#[no_mangle]
pub unsafe extern "C" fn epochdb_parse(e: *mut epochdb_engine, block: *const u8, len: usize, out: *mut epochdb_block_meta) -> c_int {
    if e.is_null() || out.is_null() {
        return EPOCHDB_EINVAL;
    }
    let en = &*e;
    en.guard(|| {
        let bytes = slice(block, len).ok_or((EPOCHDB_EINVAL, "null block".to_string()))?;
        let b = en.tree.engine.parse(Bytes::copy_from_slice(bytes)).map_err(ferr)?;
        let m = en.tree.engine.meta(&b);
        *out = epochdb_block_meta { id: m.id, parent: m.parent, height: m.height, timestamp: m.timestamp };
        Ok(EPOCHDB_OK)
    })
}

/// Verify by id (parsed or built earlier). `pchain_height` 0 = none.
#[no_mangle]
pub unsafe extern "C" fn epochdb_verify(e: *mut epochdb_engine, id: *const u8, pchain_height: u64, out: *mut epochdb_verify_out) -> c_int {
    if e.is_null() {
        return EPOCHDB_EINVAL;
    }
    let en = &*e;
    en.guard(|| {
        let id = id32(id).ok_or((EPOCHDB_EINVAL, "null id".to_string()))?;
        let b = en.block(&id).ok_or((EPOCHDB_ENOTFOUND, format!("block {} was not parsed", chain::tree::hex(&id))))?;
        en.tree.verify(b.clone(), if pchain_height == 0 { None } else { Some(pchain_height) }).map_err(ferr)?;
        if !out.is_null() {
            let root = en.tree.pending(&id).and_then(|p| p.root()).unwrap_or(b.header.root);
            *out = epochdb_verify_out { state_root: root.0, gas_used: b.header.gas_used, tx_count: b.txs.len() as u64 };
        }
        Ok(EPOCHDB_OK)
    })
}

#[no_mangle]
pub unsafe extern "C" fn epochdb_accept(e: *mut epochdb_engine, id: *const u8) -> c_int {
    if e.is_null() {
        return EPOCHDB_EINVAL;
    }
    let en = &*e;
    en.guard(|| {
        let id = id32(id).ok_or((EPOCHDB_EINVAL, "null id".to_string()))?;
        en.tree.accept(&id).map_err(ferr)?;
        Ok(EPOCHDB_OK)
    })
}

#[no_mangle]
pub unsafe extern "C" fn epochdb_reject(e: *mut epochdb_engine, id: *const u8) -> c_int {
    if e.is_null() {
        return EPOCHDB_EINVAL;
    }
    let en = &*e;
    en.guard(|| {
        let id = id32(id).ok_or((EPOCHDB_EINVAL, "null id".to_string()))?;
        en.tree.reject(&id);
        Ok(EPOCHDB_OK)
    })
}

#[no_mangle]
pub unsafe extern "C" fn epochdb_last_accepted(e: *mut epochdb_engine, id: *mut u8, height: *mut u64) -> c_int {
    if e.is_null() || id.is_null() {
        return EPOCHDB_EINVAL;
    }
    let en = &*e;
    en.guard(|| {
        let b = en.tree.engine.last_accepted();
        std::ptr::copy_nonoverlapping(b.hash.as_ptr(), id, 32);
        if !height.is_null() {
            *height = b.height;
        }
        Ok(EPOCHDB_OK)
    })
}

#[no_mangle]
pub unsafe extern "C" fn epochdb_block_id_at_height(e: *mut epochdb_engine, height: u64, id: *mut u8) -> c_int {
    if e.is_null() || id.is_null() {
        return EPOCHDB_EINVAL;
    }
    let en = &*e;
    en.guard(|| {
        let x = en.tree.block_id_at_height(height).ok_or((EPOCHDB_ENOTFOUND, format!("no accepted block at height {height}")))?;
        std::ptr::copy_nonoverlapping(x.as_ptr(), id, 32);
        Ok(EPOCHDB_OK)
    })
}

/// The block's bytes as they were handed in (verified or accepted); the
/// genesis is assembled as `[header, [], []]`.
#[no_mangle]
pub unsafe extern "C" fn epochdb_get_block(e: *mut epochdb_engine, id: *const u8, out: *mut epochdb_buf) -> c_int {
    if e.is_null() || out.is_null() {
        return EPOCHDB_EINVAL;
    }
    let en = &*e;
    en.guard(|| {
        let id = id32(id).ok_or((EPOCHDB_EINVAL, "null id".to_string()))?;
        // Accepted, verified, dropped (rejected or superseded: the tree keeps
        // their bytes, avalanchego's rpcchainvm server looks a block up before
        // Reject and a "not found" for a block consensus knows shuts the chain
        // down), or still in the parsed cache.
        let b = en.block(&id).ok_or((EPOCHDB_ENOTFOUND, format!("block {} not found", chain::tree::hex(&id))))?;
        let bytes = if b.container.is_empty() {
            let mut body = b.header_rlp.to_vec();
            body.extend_from_slice(&[0xc0, 0xc0]);
            let mut out = Vec::with_capacity(body.len() + 4);
            alloy_rlp::Header { list: true, payload_length: body.len() }.encode(&mut out);
            out.extend_from_slice(&body);
            out
        } else {
            b.container.to_vec()
        };
        *out = buf(bytes);
        Ok(EPOCHDB_OK)
    })
}

/// Decodes an RLP list of tx envelopes (typed txs as byte strings, legacy as lists).
/// `senders`: 20 bytes per candidate, or empty; a zero address means unknown
/// (the engine recovers it). The Go pool recovered every candidate at
/// admission, so a build normally recovers nothing.
fn decode_candidates(b: &[u8], senders: &[u8]) -> Result<Vec<block::Tx>, String> {
    use alloy_rlp::Header as H;
    let mut p = b;
    let h = H::decode(&mut p).map_err(|e| e.to_string())?;
    if !h.list || h.payload_length != p.len() {
        return Err("candidates: not one RLP list".into());
    }
    let mut out = Vec::new();
    while !p.is_empty() {
        let start = p;
        let ih = H::decode(&mut p).map_err(|e| e.to_string())?;
        let total = ih.length() + ih.payload_length;
        if total > start.len() {
            return Err("candidates: truncated element".into());
        }
        let raw = if ih.list { &start[..total] } else { &start[ih.length()..total] };
        p = &start[total..];
        let mut t = block::eth::decode_tx(Bytes::copy_from_slice(raw)).map_err(|e| format!("candidate {}: {e}", out.len()))?;
        if let Some(a) = senders.get(out.len() * 20..out.len() * 20 + 20) {
            if a.iter().any(|b| *b != 0) {
                t.sender = Some(Address::from_slice(a));
            }
        }
        out.push(t);
    }
    if !senders.is_empty() && senders.len() != out.len() * 20 {
        return Err(format!("senders: {} bytes for {} candidates", senders.len(), out.len()));
    }
    Ok(out)
}

/// Builds a block on `parent_id` (zero or the head's id = the accepted head;
/// else a verified block) at `timestamp_ms` (Unix milliseconds) from `txs`
/// in the miner's order, with their senders (`senders`: 20 bytes each, or
/// null: the engine recovers them), or, with `txs` null / empty, from the
/// engine's own pool; the result is a verified pending block whose later
/// verify is a lookup. See ABI.md for the semantics.
#[no_mangle]
pub unsafe extern "C" fn epochdb_build(
    e: *mut epochdb_engine,
    parent_id: *const u8,
    timestamp_ms: u64,
    coinbase: *const u8,
    pchain_height: u64,
    txs: *const u8,
    txs_len: usize,
    senders: *const u8,
    senders_len: usize,
    out: *mut epochdb_build_out,
) -> c_int {
    if e.is_null() || out.is_null() {
        return EPOCHDB_EINVAL;
    }
    let en = &*e;
    en.guard(|| {
        let (Some(pid), Some(cb), Some(raw), Some(snd)) = (id32(parent_id), slice(coinbase, 20), slice(txs, txs_len), slice(senders, senders_len)) else {
            return Err((EPOCHDB_EINVAL, "null argument".to_string()));
        };
        let t0 = std::time::Instant::now();
        // No candidates handed in: the engine takes them from its pool.
        let candidates = if raw.is_empty() { None } else { Some(decode_candidates(raw, snd).map_err(ferr)?) };
        let t1 = std::time::Instant::now();
        let (parent, pb) = en.parent(&pid)?;
        // The unaccepted ancestors' txs: the pool still holds them (they leave
        // at accept), so the build must not offer them again.
        let mut chain: Vec<Arc<block::Block>> = Vec::new();
        let mut cur = pid;
        while en.tree.pending(&cur).is_some() {
            let Some(b) = en.tree.get_block(&cur) else { break };
            cur = b.header.parent_hash.0;
            chain.push((*b).clone());
        }
        let parent_txs: Vec<&block::Tx> = chain.iter().flat_map(|b| b.txs.iter()).collect();
        let params = Params { timestamp_ms, coinbase: Address::from_slice(cb), desired_min_delay_excess: en.tree.engine.desired_delay_excess };
        let r = en.tree.engine.build(parent.as_ref(), &pb.header, &params, if pchain_height == 0 { None } else { Some(pchain_height) }, candidates, &parent_txs).map_err(ferr)?;
        let t2 = std::time::Instant::now();
        let b = r.block.clone();
        en.tree.insert_verified(b.clone(), r.pending);
        let t3 = std::time::Instant::now();
        let mut phase_ns = [0u64; 10];
        phase_ns[0] = (t1 - t0).as_nanos() as u64;
        phase_ns[1..8].copy_from_slice(&r.phase_ns);
        phase_ns[8] = (t3 - t2).as_nanos() as u64;
        *out = epochdb_build_out {
            block_bytes: buf(b.container.to_vec()),
            id: b.hash.0,
            gas_used: b.header.gas_used,
            included_count: r.included.len() as u64,
            skipped: buf(r.reasons.iter().map(|x| *x as u8).collect()),
            needs_more: r.needs_more as u8,
            phase_ns,
        };
        (*out).phase_ns[9] = t3.elapsed().as_nanos() as u64;
        Ok(EPOCHDB_OK)
    })
}

/// An RLP list of byte strings, one tx envelope each (the pool's wire form
/// in both directions: what MarshalBinary / UnmarshalBinary handle).
fn decode_envelopes(b: &[u8]) -> Result<Vec<Bytes>, String> {
    use alloy_rlp::Header as H;
    let mut p = b;
    let h = H::decode(&mut p).map_err(|e| e.to_string())?;
    if !h.list || h.payload_length != p.len() {
        return Err("envelopes: not one RLP list".into());
    }
    let mut out = Vec::new();
    while !p.is_empty() {
        let s = H::decode_bytes(&mut p, false).map_err(|e| e.to_string())?;
        out.push(Bytes::copy_from_slice(s));
    }
    Ok(out)
}

fn encode_envelopes<'a>(txs: impl Iterator<Item = &'a [u8]>) -> Vec<u8> {
    let mut list = Vec::new();
    for t in txs {
        alloy_rlp::Header { list: false, payload_length: t.len() }.encode(&mut list);
        list.extend_from_slice(t);
    }
    let mut out = Vec::with_capacity(list.len() + 9);
    alloy_rlp::Header { list: true, payload_length: list.len() }.encode(&mut out);
    out.extend_from_slice(&list);
    out
}

/// Admits `txs` (an RLP list of tx envelopes) into the pool; `local` != 0
/// marks them local (only with `local-txs-enabled`). `out`: 33 bytes per
/// input, a code (0 ok, 1 known, 2 replaced, 3 underpriced, 4 nonce too
/// low, 5 insufficient funds, 6 over the block gas limit, 7 intrinsic gas,
/// 8 invalid signature / chain id, 9 pool full, 10 other) then the hash.
#[no_mangle]
pub unsafe extern "C" fn epochdb_pool_add(e: *mut epochdb_engine, txs: *const u8, len: usize, local: u8, out: *mut epochdb_buf) -> c_int {
    if e.is_null() || out.is_null() {
        return EPOCHDB_EINVAL;
    }
    let en = &*e;
    en.guard(|| {
        let raw = slice(txs, len).ok_or((EPOCHDB_EINVAL, "null txs".to_string()))?;
        let raws = decode_envelopes(raw).map_err(ferr)?;
        let res = en.tree.engine.pool_add(raws, local != 0);
        let mut v = Vec::with_capacity(33 * res.len());
        for a in res {
            v.push(a.code as u8);
            v.extend_from_slice(a.hash.as_slice());
        }
        *out = buf(v);
        Ok(EPOCHDB_OK)
    })
}

/// The pool's (pending, queued) counts.
#[no_mangle]
pub unsafe extern "C" fn epochdb_pool_status(e: *mut epochdb_engine, pending: *mut u64, queued: *mut u64) -> c_int {
    if e.is_null() || pending.is_null() || queued.is_null() {
        return EPOCHDB_EINVAL;
    }
    let en = &*e;
    en.guard(|| {
        let (p, q) = en.tree.engine.txpool.status();
        *pending = p as u64;
        *queued = q as u64;
        Ok(EPOCHDB_OK)
    })
}

/// `out` = 1 when the pool holds the tx with `hash`.
#[no_mangle]
pub unsafe extern "C" fn epochdb_pool_has(e: *mut epochdb_engine, hash: *const u8, out: *mut u8) -> c_int {
    if e.is_null() || out.is_null() {
        return EPOCHDB_EINVAL;
    }
    let en = &*e;
    en.guard(|| {
        let h = id32(hash).ok_or((EPOCHDB_EINVAL, "null hash".to_string()))?;
        *out = en.tree.engine.txpool.has(&B256::from(h)) as u8;
        Ok(EPOCHDB_OK)
    })
}

/// The pool's txs as an RLP list of envelopes, pending (address then nonce
/// order) then queued, of one address (`addr`: 20 bytes) or of all (null);
/// at most `limit` of each half (0 = all).
#[no_mangle]
pub unsafe extern "C" fn epochdb_pool_content(e: *mut epochdb_engine, addr: *const u8, limit: usize, out: *mut epochdb_buf) -> c_int {
    if e.is_null() || out.is_null() {
        return EPOCHDB_EINVAL;
    }
    let en = &*e;
    en.guard(|| {
        let a = if addr.is_null() { None } else { Some(Address::from_slice(slice(addr, 20).unwrap())) };
        let (p, q) = en.tree.engine.txpool.content(a, limit);
        *out = buf(encode_envelopes(p.iter().chain(q.iter()).map(|t| &t.raw[..])));
        Ok(EPOCHDB_OK)
    })
}

/// The pool's nonce for `addr` (its state nonce plus its executable txs);
/// EPOCHDB_ENOTFOUND when the pool holds nothing of it (use the state's).
#[no_mangle]
pub unsafe extern "C" fn epochdb_pool_nonce(e: *mut epochdb_engine, addr: *const u8, out: *mut u64) -> c_int {
    if e.is_null() || out.is_null() {
        return EPOCHDB_EINVAL;
    }
    let en = &*e;
    en.guard(|| {
        let a = slice(addr, 20).ok_or((EPOCHDB_EINVAL, "null address".to_string()))?;
        match en.tree.engine.txpool.pending_nonce(Address::from_slice(a)) {
            Some(n) => {
                *out = n;
                Ok(EPOCHDB_OK)
            }
            None => Err((EPOCHDB_ENOTFOUND, "address not in the pool".to_string())),
        }
    })
}

/// Blocks until the pool holds an executable tx (`out` = 1) or `timeout_ms`
/// passes (`out` = 0). Returns at once when it already does.
#[no_mangle]
pub unsafe extern "C" fn epochdb_pool_wait(e: *mut epochdb_engine, timeout_ms: u64, out: *mut u8) -> c_int {
    if e.is_null() || out.is_null() {
        return EPOCHDB_EINVAL;
    }
    let en = &*e;
    en.guard(|| {
        *out = en.tree.engine.txpool.wait(std::time::Duration::from_millis(timeout_ms)) as u8;
        Ok(EPOCHDB_OK)
    })
}

/// RLP `[[envelopes...], [hashes...]]`: every tx admitted (locally or from
/// gossip) since the previous call, oldest first (what the push gossiper
/// forwards), and every tx hash that left the pool since then (what the
/// push gossiper's Has stops answering). Empty buffer = neither.
#[no_mangle]
pub unsafe extern "C" fn epochdb_pool_drain_gossip(e: *mut epochdb_engine, out: *mut epochdb_buf) -> c_int {
    if e.is_null() || out.is_null() {
        return EPOCHDB_EINVAL;
    }
    let en = &*e;
    en.guard(|| {
        let (txs, gone) = en.tree.engine.txpool.drain_gossip();
        *out = buf(if txs.is_empty() && gone.is_empty() {
            Vec::new()
        } else {
            let mut body = encode_envelopes(txs.iter().map(|t| &t.raw[..]));
            body.extend_from_slice(&encode_envelopes(gone.iter().map(|h| &h[..])));
            let mut all = Vec::with_capacity(body.len() + 9);
            alloy_rlp::Header { list: true, payload_length: body.len() }.encode(&mut all);
            all.extend_from_slice(&body);
            all
        });
        Ok(EPOCHDB_OK)
    })
}

/// nonce (u64 LE) and balance (32 bytes BE) of `n` addresses at the state of
/// `block_id` (zero = the accepted head; a verified block's id = its pending state).
#[no_mangle]
pub unsafe extern "C" fn epochdb_account_state(e: *mut epochdb_engine, addrs: *const u8, n: usize, block_id: *const u8, out: *mut epochdb_buf) -> c_int {
    if e.is_null() || out.is_null() {
        return EPOCHDB_EINVAL;
    }
    let en = &*e;
    en.guard(|| {
        let (Some(a), Some(id)) = (slice(addrs, 20 * n), id32(block_id)) else {
            return Err((EPOCHDB_EINVAL, "null argument".to_string()));
        };
        let addrs: Vec<Address> = a.chunks(20).map(Address::from_slice).collect();
        let (parent, _) = en.parent(&id)?;
        let mut v = Vec::with_capacity(40 * n);
        for (nonce, bal) in en.tree.engine.accounts(parent.as_ref(), &addrs) {
            v.extend_from_slice(&nonce.to_le_bytes());
            v.extend_from_slice(&bal.to_be_bytes::<32>());
        }
        *out = buf(v);
        Ok(EPOCHDB_OK)
    })
}

/// The accepted head's header RLP (the genesis header before any accept).
#[no_mangle]
pub unsafe extern "C" fn epochdb_head_header(e: *mut epochdb_engine, out: *mut epochdb_buf) -> c_int {
    if e.is_null() || out.is_null() {
        return EPOCHDB_EINVAL;
    }
    let en = &*e;
    en.guard(|| {
        *out = buf(en.tree.engine.last_accepted().header_rlp.to_vec());
        Ok(EPOCHDB_OK)
    })
}

/// One JSON-RPC request body (single or batch) in, the response body out.
#[no_mangle]
pub unsafe extern "C" fn epochdb_rpc(e: *mut epochdb_engine, body: *const u8, len: usize, out: *mut epochdb_buf) -> c_int {
    if e.is_null() || out.is_null() {
        return EPOCHDB_EINVAL;
    }
    let en = &*e;
    en.guard(|| {
        let b = slice(body, len).ok_or((EPOCHDB_EINVAL, "null body".to_string()))?;
        *out = buf(en.tree.engine.rpc(b));
        Ok(EPOCHDB_OK)
    })
}

/// `{"height":..,"root-checked":..,"normal-op":..}`.
#[no_mangle]
pub unsafe extern "C" fn epochdb_health(e: *mut epochdb_engine, out: *mut epochdb_buf) -> c_int {
    if e.is_null() || out.is_null() {
        return EPOCHDB_EINVAL;
    }
    let en = &*e;
    en.guard(|| {
        let mut v = en.tree.engine.health().map_err(ferr)?;
        heap_stats(&mut v);
        *out = buf(v.to_string().into_bytes());
        Ok(EPOCHDB_OK)
    })
}

/// jemalloc's own accounting of the engine's heap (bytes), so the Go side
/// can split the process RSS between the pool and the engine.
#[cfg(feature = "jemalloc")]
fn heap_stats(v: &mut serde_json::Value) {
    use tikv_jemalloc_ctl::{epoch, stats};
    let _ = epoch::advance();
    if let Some(o) = v.as_object_mut() {
        o.insert("heap-allocated".into(), stats::allocated::read().unwrap_or(0).into());
        o.insert("heap-resident".into(), stats::resident::read().unwrap_or(0).into());
    }
}

#[cfg(not(feature = "jemalloc"))]
fn heap_stats(_: &mut serde_json::Value) {}

/// Frees a buffer returned by any function above (a null / empty one is fine).
#[no_mangle]
pub unsafe extern "C" fn epochdb_buf_free(b: *mut epochdb_buf) {
    if b.is_null() || (*b).ptr.is_null() {
        return;
    }
    drop(Box::from_raw(std::ptr::slice_from_raw_parts_mut((*b).ptr, (*b).len)));
    (*b).ptr = std::ptr::null_mut();
    (*b).len = 0;
}

const _: () = {
    // The types the header promises.
    let _ = std::mem::size_of::<epochdb_buf>();
    let _ = B256::ZERO;
};

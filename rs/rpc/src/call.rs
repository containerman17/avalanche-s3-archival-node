//! Everything that executes: eth_call, eth_estimateGas, eth_callDetailed,
//! eth_createAccessList, debug_traceCall and the re-executing block tracers.
//! One path: the state after block n out of the store behind a revm CacheDB
//! (`RpcDb`), rs/exec's `Executor` over it (subnet-evm rules, DoCall mode
//! for calls, execute_block for block replays), nothing committed.
use std::cell::RefCell;
use std::convert::Infallible;
use std::sync::Arc;

use alloy_primitives::{Address, Bytes, B256, U256};
use alloy_rpc_types_trace::geth::{CallConfig, GethDefaultTracingOptions, PreStateConfig};
use block::{Block, Header};
use exec::{CallMsg, CallOut, Executor, StateDb, Trace};
use revm::database::CacheDB;
use revm::state::{AccountInfo, Bytecode};
use revm::DatabaseRef;
use serde_json::{json, Map, Value};

use crate::json::*;
use crate::{invalid, RpcError, RpcResult, Server, StateRead, Store};

/// geth's DefaultRPCGasCap (subnet-evm: 50M).
pub const RPC_GAS_CAP: u64 = 50_000_000;
const TX_GAS: u64 = 21_000;
const CALL_STIPEND: u64 = 2300;
/// internal/ethapi estimateGasErrorRatio.
const ESTIMATE_ERROR_RATIO: f64 = 0.015;

/// The store's state as a revm DatabaseRef: read errors are recorded (a
/// geth statedb.Error()) and checked after the run, since revm's error type
/// here is Infallible.
pub struct HistView<'a> {
    st: RefCell<Box<dyn StateRead + 'a>>,
    store: &'a dyn Store,
    pub err: RefCell<Option<anyhow::Error>>,
}

impl HistView<'_> {
    fn catch<T: Default>(&self, r: anyhow::Result<T>) -> T {
        match r {
            Ok(v) => v,
            Err(e) => {
                let mut g = self.err.borrow_mut();
                if g.is_none() {
                    *g = Some(e);
                }
                T::default()
            }
        }
    }
}

impl DatabaseRef for HistView<'_> {
    type Error = Infallible;
    fn basic_ref(&self, a: Address) -> Result<Option<AccountInfo>, Infallible> {
        let r = self.st.borrow_mut().account(a);
        // code: None, not AccountInfo::default()'s Some(empty), or revm never loads the code.
        Ok(self.catch(r).map(|acc| AccountInfo { balance: acc.balance, nonce: acc.nonce, code_hash: if acc.code_hash == B256::ZERO { alloy_primitives::KECCAK256_EMPTY } else { acc.code_hash }, code: None, account_id: None }))
    }
    fn code_by_hash_ref(&self, h: B256) -> Result<Bytecode, Infallible> {
        let r = self.st.borrow_mut().code(h);
        Ok(Bytecode::new_raw(self.catch(r).unwrap_or_default()))
    }
    fn storage_ref(&self, a: Address, slot: U256) -> Result<U256, Infallible> {
        let r = self.st.borrow_mut().storage(a, slot);
        Ok(self.catch(r))
    }
    fn block_hash_ref(&self, n: u64) -> Result<B256, Infallible> {
        let r = self.store.hash_at(n);
        Ok(self.catch(r).unwrap_or_default())
    }
}

/// revm's CacheDB over the store view (writes stay in the cache).
pub struct RpcDb<'a>(pub CacheDB<HistView<'a>>);

impl<'a> std::ops::Deref for RpcDb<'a> {
    type Target = CacheDB<HistView<'a>>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl std::ops::DerefMut for RpcDb<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
impl revm::Database for RpcDb<'_> {
    type Error = Infallible;
    fn basic(&mut self, a: Address) -> Result<Option<AccountInfo>, Infallible> {
        self.0.basic(a)
    }
    fn code_by_hash(&mut self, h: B256) -> Result<Bytecode, Infallible> {
        self.0.code_by_hash(h)
    }
    fn storage(&mut self, a: Address, i: U256) -> Result<U256, Infallible> {
        self.0.storage(a, i)
    }
    fn block_hash(&mut self, n: u64) -> Result<B256, Infallible> {
        self.0.block_hash(n)
    }
}
impl revm::DatabaseCommit for RpcDb<'_> {
    fn commit(&mut self, changes: revm::state::EvmState) {
        self.0.commit(changes)
    }
}
impl StateDb for RpcDb<'_> {
    fn set_block_hash(&mut self, number: u64, hash: B256) {
        self.0.cache.block_hashes.insert(U256::from(number), hash);
    }
    fn forget(&mut self, addr: Address) {
        self.0.cache.accounts.insert(addr, revm::database::DbAccount::new_not_existing());
    }
}

/// geth's revert error: -32000 "execution reverted" for an empty revert,
/// else code 3 with the data and the Error(string) reason in the message.
pub fn revert_error(output: &[u8]) -> RpcError {
    if output.is_empty() {
        return RpcError { code: -32000, message: "execution reverted".into(), data: None };
    }
    RpcError { code: 3, message: revert_reason(output), data: Some(json!(hex(output))) }
}

pub fn revert_reason(output: &[u8]) -> String {
    let mut message = "execution reverted".to_string();
    if output.len() >= 68 && output[..4] == [0x08, 0xc3, 0x79, 0xa0] {
        let off = U256::from_be_slice(&output[4..36]).to::<usize>() + 4;
        if output.len() >= off + 32 {
            let n = U256::from_be_slice(&output[off..off + 32]).to::<usize>();
            if output.len() >= off + 32 + n {
                message = format!("execution reverted: {}", String::from_utf8_lossy(&output[off + 32..off + 32 + n]));
            }
        }
    }
    message
}

/// The executor's invalid-message errors in geth's words.
pub fn call_error(e: anyhow::Error, gas: u64, from: Address) -> RpcError {
    let s = format!("{e:#}");
    if let Some(rest) = s.strip_prefix("Transaction(CallGasCostMoreThanGasLimit { initial_gas: ") {
        if let Some(want) = rest.split(',').next().and_then(|n| n.trim().parse::<u64>().ok()) {
            return format!("err: intrinsic gas too low: have {gas}, want {want} (supplied gas {gas})").into();
        }
    }
    if let Some(rest) = s.strip_prefix("Transaction(LackOfFundForMaxFee { fee: ") {
        let nums: Vec<&str> = rest.split(|c: char| !c.is_ascii_digit()).filter(|x| !x.is_empty()).collect();
        if nums.len() >= 2 {
            return format!("err: insufficient funds for gas * price + value: address {} have {} want {} (supplied gas {gas})", from.to_checksum(None), nums[1], nums[0]).into();
        }
    }
    s.into()
}

/// geth's error text for a halt.
pub fn halt_error(h: &str) -> RpcError {
    match h {
        "OutOfGas" | "OutOfGas(Basic)" | "OutOfGas(Memory)" | "OutOfGas(MemoryLimit)" | "OutOfGas(Precompile)" | "OutOfGas(InvalidOperand)" | "OutOfGas(ReentrancySentry)" | "MemoryOOG" | "MemoryLimitOOG" | "PrecompileOOG" | "InvalidOperandOOG" | "ReentrancySentryOOG" => "out of gas".into(),
        "Revert" => "execution reverted".into(),
        "InvalidFEOpcode" => "invalid opcode: INVALID".into(),
        "OpcodeNotFound" => "invalid opcode".into(),
        "StackUnderflow" => "stack underflow".into(),
        "StackOverflow" => "stack limit reached 1024 (1023)".into(),
        "CallTooDeep" => "max call depth exceeded".into(),
        "CreateContractSizeLimit" => "max code size exceeded".into(),
        "CreateCollision" => "contract address collision".into(),
        "InvalidJump" => "invalid jump destination".into(),
        "PrecompileError" => "precompile error".into(),
        "NonceOverflow" => "nonce uint64 overflow".into(),
        "CreateContractStartingWithEF" => "invalid code: must not begin with 0xef".into(),
        "CreateInitCodeSizeLimit" => "max initcode size exceeded".into(),
        _ => format!("execution halted: {h}").into(),
    }
}

/// callArgs: from, to, gas, gasPrice / maxFeePerGas / maxPriorityFeePerGas, value, data / input.
pub struct CallArgs {
    pub msg: CallMsg,
    pub gas_given: bool,
    pub nonce: Option<u64>,
}

pub fn parse_call_args(o: &Value, default_gas: u64) -> Result<CallArgs, RpcError> {
    let o = o.as_object().ok_or_else(|| invalid("call object expected"))?;
    let field = |k: &str| o.get(k).filter(|v| !v.is_null());
    // libevm TransactionArgs.data(): input wins, silently.
    let data = match field("input").or_else(|| field("data")) {
        Some(v) => parse_bytes(v)?,
        None => Bytes::new(),
    };
    let gas_given = field("gas").is_some();
    let gas_price = match (field("gasPrice"), field("maxFeePerGas")) {
        (Some(_), Some(_)) => return Err("both gasPrice and (maxFeePerGas or maxPriorityFeePerGas) specified".into()),
        (Some(v), None) | (None, Some(v)) => parse_u256(v)?.to::<u128>(),
        _ => 0,
    };
    Ok(CallArgs {
        msg: CallMsg {
            from: match field("from") {
                Some(v) => parse_addr(Some(v))?,
                None => Address::ZERO,
            },
            to: field("to").map(|v| parse_addr(Some(v))).transpose()?,
            gas: match field("gas") {
                Some(v) => parse_qty(v)?,
                None => default_gas,
            },
            gas_price,
            value: match field("value") {
                Some(v) => parse_u256(v)?,
                None => U256::ZERO,
            },
            data,
        },
        gas_given,
        nonce: field("nonce").map(parse_qty).transpose()?,
    })
}

/// common.Hash out of a JSON string with encoding/json's error texts, for
/// the override maps (geth's OverrideAccount fields).
fn override_hash(v: &str, field: &str) -> Result<U256, RpcError> {
    let inner = match v.strip_prefix("0x") {
        None => "hex string without 0x prefix".to_string(),
        Some(h) if h.len() % 2 == 1 => "hex string of odd length".to_string(),
        Some(h) if h.len() != 64 => format!("hex string has length {}, want 64 for common.Hash", h.len()),
        Some(h) => match U256::from_str_radix(h, 16) {
            Ok(x) => return Ok(x),
            Err(_) => "invalid hex string".to_string(),
        },
    };
    Err(invalid(format!("invalid argument 2: json: cannot unmarshal {inner} into Go struct field OverrideAccount.{field} of type common.Hash")))
}

/// eth_call's state override object (geth StateOverride), applied onto the CacheDB.
pub fn apply_state_override(db: &mut RpcDb<'_>, ov: Option<&Value>) -> Result<(), RpcError> {
    let Some(ov) = ov.and_then(Value::as_object) else { return Ok(()) };
    for (addr, acc) in ov {
        let addr: Address = addr.parse().map_err(|_| invalid(format!("bad state override address {addr}")))?;
        let acc = acc.as_object().ok_or_else(|| invalid("bad state override"))?;
        let field = |k: &str| acc.get(k).filter(|v| !v.is_null());
        let mut info = revm::Database::basic(db, addr).unwrap().unwrap_or_default();
        if let Some(n) = field("nonce") {
            info.nonce = parse_qty(n)?;
        }
        if let Some(c) = field("code") {
            let code = Bytecode::new_raw(parse_bytes(c)?);
            info.code_hash = code.hash_slow();
            info.code = Some(code);
        }
        if let Some(b) = field("balance") {
            info.balance = parse_u256(b)?;
        }
        db.insert_account_info(addr, info);
        let (state, diff) = (field("state"), field("stateDiff"));
        if state.is_some() && diff.is_some() {
            return Err(invalid(format!("account {addr} has both 'state' and 'stateDiff'")));
        }
        if let Some(s) = state.and_then(Value::as_object) {
            // A whole-storage replacement: every stored slot goes, the given ones stay.
            let a = db.cache.accounts.entry(addr).or_default();
            a.storage.clear();
            a.account_state = revm::database::AccountState::StorageCleared;
            for (k, v) in s {
                let (k, v) = (override_hash(k, "state")?, override_hash(v.as_str().unwrap_or(""), "state")?);
                a.storage.insert(k, v);
            }
        }
        if let Some(s) = diff.and_then(Value::as_object) {
            for (k, v) in s {
                let (k, v) = (override_hash(k, "stateDiff")?, override_hash(v.as_str().unwrap_or(""), "stateDiff")?);
                db.insert_account_storage(addr, k, v).unwrap();
            }
        }
    }
    Ok(())
}

/// geth BlockOverrides onto a header copy.
pub fn apply_block_override(h: &mut Header, ov: Option<&Value>) -> Result<(), RpcError> {
    let Some(o) = ov.and_then(Value::as_object) else { return Ok(()) };
    let field = |k: &str| o.get(k).filter(|v| !v.is_null());
    if let Some(v) = field("number") {
        h.number = parse_qty(v)?;
    }
    if let Some(v) = field("difficulty") {
        h.difficulty = parse_u256(v)?;
    }
    if let Some(v) = field("time") {
        h.time = parse_qty(v)?;
    }
    if let Some(v) = field("gasLimit") {
        h.gas_limit = parse_qty(v)?;
    }
    if let Some(v) = field("coinbase") {
        h.coinbase = parse_addr(Some(v))?;
    }
    if let Some(v) = field("baseFee") {
        h.base_fee = Some(parse_u256(v)?);
    }
    Ok(())
}

pub struct TraceCfg {
    pub trace: Trace,
    pub tracer: String,
    pub raw: Value,
}

/// geth's TraceConfig: a named tracer with its tracerConfig, or the struct
/// logger with the logger.Config fields inline.
pub fn parse_trace_config(v: Option<&Value>) -> Result<TraceCfg, RpcError> {
    let raw = v.cloned().unwrap_or(Value::Null);
    let o = raw.as_object().cloned().unwrap_or_default();
    let name = o.get("tracer").and_then(Value::as_str).unwrap_or("").to_string();
    let tcfg = o.get("tracerConfig").cloned().unwrap_or(Value::Null);
    let trace = match name.as_str() {
        "" => {
            let mut opts: GethDefaultTracingOptions = serde_json::from_value(Value::Object(o.clone())).map_err(|e| invalid(format!("bad trace config: {e}")))?;
            if opts.enable_memory.is_none() && opts.disable_memory.is_none() {
                opts.enable_memory = Some(false);
            }
            Trace::Struct(opts)
        }
        "callTracer" => Trace::Call(if tcfg.is_null() { CallConfig::default() } else { serde_json::from_value(tcfg).map_err(|e| invalid(format!("bad tracerConfig: {e}")))? }),
        "prestateTracer" => Trace::PreState(if tcfg.is_null() { PreStateConfig::default() } else { serde_json::from_value(tcfg).map_err(|e| invalid(format!("bad tracerConfig: {e}")))? }),
        "noopTracer" => Trace::Noop,
        // 4byte is derived from a full call trace (call.rs four_byte).
        "4byteTracer" => Trace::Call(CallConfig::default()),
        "muxTracer" => Trace::Off,
        other => return Err(invalid(format!("tracer: tracer not found: {other}"))),
    };
    Ok(TraceCfg { trace, tracer: name, raw })
}

/// geth's 4byteTracer out of a full callTracer frame tree.
pub fn four_byte(frame: &Value, precompiles: &[Address]) -> Value {
    fn walk(f: &Value, top: bool, pre: &[Address], out: &mut std::collections::BTreeMap<String, u64>) {
        let typ = f.get("type").and_then(Value::as_str).unwrap_or("");
        let input = f.get("input").and_then(Value::as_str).unwrap_or("0x");
        let to: Option<Address> = f.get("to").and_then(Value::as_str).and_then(|s| s.parse().ok());
        let n = input.len().saturating_sub(2) / 2;
        // libevm's 4byte: CaptureStart stores the tx input whatever the kind,
        // CaptureEnter only CALL kinds and never a precompile.
        if n >= 4 && (top || (matches!(typ, "CALL" | "CALLCODE" | "DELEGATECALL" | "STATICCALL") && !to.is_some_and(|t| pre.contains(&t)))) {
            *out.entry(format!("{}-{}", &input[..10], n - 4)).or_default() += 1;
        }
        if let Some(calls) = f.get("calls").and_then(Value::as_array) {
            for c in calls {
                walk(c, false, pre, out);
            }
        }
    }
    let mut out = std::collections::BTreeMap::new();
    walk(frame, true, precompiles, &mut out);
    json!(out)
}

/// A trace's JSON after the tracer-specific post-processing. `created` is
/// the address a top-level creation deploys to, for geth's prestate quirk.
pub fn finish_trace(s: &Server, cfg: &TraceCfg, header: &Header, trace_json: &str, created: Option<Address>) -> Result<Value, RpcError> {
    let mut v: Value = serde_json::from_str(trace_json).map_err(|e| RpcError::from(format!("trace json: {e}")))?;
    Ok(match cfg.tracer.as_str() {
        "4byteTracer" => four_byte(&v, &s.active_precompiles(header.time)),
        "callTracer" => {
            // libevm's withLog entries carry no index / position.
            fn strip(f: &mut Value) {
                if let Some(logs) = f.get_mut("logs").and_then(Value::as_array_mut) {
                    for l in logs {
                        if let Some(o) = l.as_object_mut() {
                            o.remove("index");
                            o.remove("position");
                        }
                    }
                }
                if let Some(calls) = f.get_mut("calls").and_then(Value::as_array_mut) {
                    for c in calls {
                        strip(c);
                    }
                }
            }
            strip(&mut v);
            v
        }
        "" => {
            // libevm's struct logger: returnValue without 0x, no refund column.
            if let Some(r) = v.get("returnValue").and_then(Value::as_str).map(|r| r.trim_start_matches("0x").to_string()) {
                v["returnValue"] = json!(r);
            }
            if let Some(logs) = v.get_mut("structLogs").and_then(Value::as_array_mut) {
                for l in logs {
                    if let Some(o) = l.as_object_mut() {
                        o.remove("refund");
                        // storage keys and values without 0x; errors in geth's words.
                        if let Some(st) = o.get("storage").and_then(Value::as_object).cloned() {
                            o.insert("storage".into(), Value::Object(st.into_iter().map(|(k, v)| (k.trim_start_matches("0x").to_string(), json!(v.as_str().unwrap_or("").trim_start_matches("0x")))).collect()));
                        }
                        if let Some(e) = o.get("error").and_then(Value::as_str).map(str::to_string) {
                            let inner = e.strip_prefix("Some(").and_then(|x| x.strip_suffix(')')).unwrap_or(&e);
                            o.insert("error".into(), json!(halt_error(inner).message));
                        }
                    }
                }
            }
            v
        }
        "prestateTracer" => {
            // A stateful precompile reads its slots from Go, not through SLOAD:
            // geth's tracer never sees them.
            let modules: Vec<String> = exec::precompile::MODULES.iter().map(|(_, a)| format!("{a:#x}")).collect();
            for key in ["pre", "post"] {
                let m = if v.get(key).is_some() { v.get_mut(key) } else { None };
                if let Some(o) = m.and_then(Value::as_object_mut) {
                    for a in &modules {
                        if let Some(acc) = o.get_mut(a).and_then(Value::as_object_mut) {
                            acc.remove("storage");
                        }
                    }
                }
            }
            if v.get("pre").is_none() {
                if let Some(o) = v.as_object_mut() {
                    for a in &modules {
                        if let Some(acc) = o.get_mut(a).and_then(Value::as_object_mut) {
                            acc.remove("storage");
                        }
                    }
                }
            }
            // geth looks the created contract up after its nonce was set to 1
            // (EIP-161), so its pre-state reads {balance 0, nonce 1}.
            if let Some(c) = created {
                let key = format!("{c:#x}");
                let diff = v.get("pre").is_some();
                let pre = if diff { v.get_mut("pre") } else { Some(&mut v) };
                if let Some(pre) = pre.and_then(Value::as_object_mut) {
                    let e = pre.entry(key.clone()).or_insert_with(|| json!({}));
                    if let Some(o) = e.as_object_mut() {
                        o.entry("balance").or_insert(json!("0x0"));
                        o.insert("nonce".into(), json!(1));
                    }
                }
                if diff {
                    if let Some(post) = v.get_mut("post").and_then(Value::as_object_mut) {
                        if let Some(o) = post.get_mut(&key).and_then(Value::as_object_mut) {
                            if o.get("nonce") == Some(&json!(1)) {
                                o.remove("nonce");
                            }
                        }
                        if post.get(&key).and_then(Value::as_object).is_some_and(|o| o.is_empty()) {
                            post.remove(&key);
                        }
                    }
                }
            }
            v
        }
        _ => v,
    })
}

impl Server {
    /// The precompile addresses active at a time (geth's vm.ActivePrecompiles
    /// plus subnet-evm's stateful ones).
    pub fn active_precompiles(&self, time: u64) -> Vec<Address> {
        let spec = self.cfg.spec(time);
        let n = if spec >= revm::primitives::hardfork::SpecId::CANCUN { 10 } else { 9 };
        let mut v: Vec<Address> = (1..=n).map(|i| Address::from_word(B256::from(U256::from(i)))).collect();
        if self.cfg.is_granite(time) {
            v.push(Address::from_word(B256::from(U256::from(0x100))));
        }
        for (_, addr) in exec::precompile::MODULES.iter() {
            if self.cfg.precompile_enabled(*addr, time) {
                v.push(*addr);
            }
        }
        v
    }

    /// An executor over the state after block n (the block context is set
    /// per call / block by the executor itself).
    pub fn executor_at<'a>(&'a self, n: u64, trace: Trace) -> Result<Executor<RpcDb<'a>>, RpcError> {
        let st = self.store.state_at(n)?;
        let view = HistView { st: RefCell::new(st), store: self.store.as_ref(), err: RefCell::new(None) };
        let mut ex = Executor::open((*self.cfg).clone(), RpcDb(CacheDB::new(view)));
        ex.set_trace(trace);
        Ok(ex)
    }

    fn check_reads(ex: &Executor<RpcDb<'_>>) -> Result<(), RpcError> {
        if let Some(e) = ex.db().0.db.err.borrow_mut().take() {
            return Err(format!("{e:#}").into());
        }
        Ok(())
    }

    /// One call at the post-state of n under the header of n (with the
    /// optional state and block overrides), DoCall semantics.
    pub fn run_call(&self, n: u64, msg: &CallMsg, trace: Trace, state_ov: Option<&Value>, block_ov: Option<&Value>) -> Result<CallOut, RpcError> {
        let b = self.block_at(n)?;
        let mut header = b.header.clone();
        apply_block_override(&mut header, block_ov)?;
        let mut ex = self.executor_at(n, trace)?;
        apply_state_override(ex.db_mut(), state_ov)?;
        let r = ex.call(&header, msg).map_err(|e| call_error(e, msg.gas, msg.from))?;
        Self::check_reads(&ex)?;
        Ok(r)
    }

    fn call_block(&self, params: &[Value]) -> Result<(u64, Arc<Block>), RpcError> {
        let n = self.block_number(params.get(1))?;
        Ok((n, self.block_at(n)?))
    }

    pub fn eth_call(&self, params: &[Value]) -> RpcResult {
        let (n, _) = self.call_block(params)?;
        let mut args = parse_call_args(params.first().ok_or_else(|| invalid("need [callArgs, blockTag]"))?, RPC_GAS_CAP)?;
        args.msg.gas = args.msg.gas.min(RPC_GAS_CAP);
        let r = self.run_call(n, &args.msg, Trace::Off, params.get(2), params.get(3))?;
        if r.revert {
            return Err(revert_error(&r.output));
        }
        if let Some(h) = r.halt {
            return Err(halt_error(&h));
        }
        Ok(json!(hex(&r.output)))
    }

    pub fn call_detailed(&self, params: &[Value]) -> RpcResult {
        let (n, _) = self.call_block(params)?;
        let mut args = parse_call_args(params.first().ok_or_else(|| invalid("need [callArgs, blockTag, stateOverride]"))?, RPC_GAS_CAP)?;
        args.msg.gas = args.msg.gas.min(RPC_GAS_CAP);
        let r = self.run_call(n, &args.msg, Trace::Off, params.get(2), None)?;
        let (code, err) = if r.revert {
            if r.output.is_empty() { (0, "execution reverted".to_string()) } else { (3, "execution reverted".to_string()) }
        } else if let Some(h) = &r.halt {
            (0, halt_error(h).message)
        } else {
            (0, String::new())
        };
        Ok(json!({"gas": r.gas_used, "errCode": code, "err": err, "returnData": hex(&r.output)}))
    }

    /// gasestimator.Estimate: the optimistic 64/63 probe, then the binary
    /// search with the 1.5 percent error ratio and the mid <= 2 lo skew.
    pub fn estimate_gas(&self, params: &[Value]) -> RpcResult {
        let (n, b) = self.call_block(params)?;
        let mut args = parse_call_args(params.first().ok_or_else(|| invalid("need [callArgs, blockTag]"))?, b.header.gas_limit)?;
        let ov = params.get(2);
        let mut ex = self.executor_at(n, Trace::Off)?;
        apply_state_override(ex.db_mut(), ov)?;
        let msg = &mut args.msg;
        let mut lo;
        let mut hi = msg.gas.min(RPC_GAS_CAP);
        if msg.gas_price > 0 {
            let bal = revm::Database::basic(ex.db_mut(), msg.from).unwrap().map(|a| a.balance).unwrap_or_default();
            if bal < msg.value {
                return Err("insufficient funds for transfer".into());
            }
            let avail = (bal - msg.value) / U256::from(msg.gas_price);
            if avail < U256::from(hi) {
                hi = avail.to::<u64>();
            }
        }
        let plain = msg.data.is_empty() && msg.to.is_some_and(|to| !revm::Database::basic(ex.db_mut(), to).unwrap().is_some_and(|a| !a.is_empty_code_hash()));
        let header = &b.header;
        let mut run = |gas: u64| -> Result<(bool, CallOut), RpcError> {
            msg.gas = gas;
            let r = ex.call(header, msg).map_err(|e| call_error(e, gas, msg.from))?;
            Self::check_reads(&ex)?;
            let failed = r.revert || r.halt.is_some();
            Ok((failed, r))
        };
        if plain && hi >= TX_GAS && !run(TX_GAS)?.0 {
            return Ok(json!(qty(TX_GAS)));
        }
        let (failed, r) = run(hi)?;
        if failed {
            if r.revert {
                return Err(revert_error(&r.output));
            }
            return Err(format!("gas required exceeds allowance ({hi})").into());
        }
        lo = r.gas_used - 1;
        let optimistic = (r.gas_used + CALL_STIPEND) * 64 / 63;
        if optimistic < hi {
            if run(optimistic)?.0 {
                lo = optimistic;
            } else {
                hi = optimistic;
            }
        }
        while lo + 1 < hi {
            if ((hi - lo) as f64) / (hi as f64) < ESTIMATE_ERROR_RATIO {
                break;
            }
            let mut mid = (hi + lo) / 2;
            if mid > lo * 2 {
                mid = lo * 2;
            }
            if run(mid)?.0 {
                lo = mid;
            } else {
                hi = mid;
            }
        }
        Ok(json!(qty(hi)))
    }

    pub fn debug_trace_call(&self, params: &[Value]) -> RpcResult {
        let (n, b) = self.call_block(params)?;
        let mut args = parse_call_args(params.first().ok_or_else(|| invalid("need [callArgs, blockTag, traceConfig]"))?, RPC_GAS_CAP)?;
        args.msg.gas = args.msg.gas.min(RPC_GAS_CAP);
        let cfg = parse_trace_config(params.get(2))?;
        let (sov, bov) = (cfg.raw.get("stateOverrides").cloned(), cfg.raw.get("blockOverrides").cloned());
        let created = if args.msg.to.is_none() { Some(args.msg.from.create(self.store.state_at(n)?.account(args.msg.from)?.map(|a| a.nonce).unwrap_or(0))) } else { None };
        if cfg.tracer == "muxTracer" {
            let mut out = Map::new();
            for (name, c) in cfg.raw.get("tracerConfig").and_then(Value::as_object).cloned().unwrap_or_default() {
                let sub = parse_trace_config(Some(&json!({"tracer": name, "tracerConfig": c})))?;
                let r = self.run_call(n, &args.msg, sub.trace.clone(), sov.as_ref(), bov.as_ref())?;
                out.insert(name, finish_trace(self, &sub, &b.header, &r.trace_json, created)?);
            }
            return Ok(Value::Object(out));
        }
        let r = self.run_call(n, &args.msg, cfg.trace.clone(), sov.as_ref(), bov.as_ref())?;
        finish_trace(self, &cfg, &b.header, &r.trace_json, created)
    }

    /// eth_createAccessList: geth's fixed-point loop over the access-list
    /// tracer, here derived from the prestate trace (every account and slot
    /// the call touched) minus sender, destination and precompiles.
    pub fn create_access_list(&self, params: &[Value]) -> RpcResult {
        let (n, b) = self.call_block(params)?;
        let mut args = parse_call_args(params.first().ok_or_else(|| invalid("need [callArgs, blockTag]"))?, RPC_GAS_CAP)?;
        args.msg.gas = args.msg.gas.min(RPC_GAS_CAP);
        let from = args.msg.from;
        let to = match args.msg.to {
            Some(t) => t,
            None => {
                let nonce = match args.nonce {
                    Some(n) => n,
                    None => self.store.state_at(n)?.account(from)?.map(|a| a.nonce).unwrap_or(0),
                };
                from.create(nonce)
            }
        };
        let pre = self.active_precompiles(b.header.time);
        let r = self.run_call(n, &args.msg, Trace::PreState(PreStateConfig::default()), None, None)?;
        let v: Value = serde_json::from_str(&r.trace_json).map_err(|e| RpcError::from(e.to_string()))?;
        let mut al = Vec::new();
        for (addr, acc) in v.as_object().cloned().unwrap_or_default() {
            let a: Address = addr.parse().map_err(|_| RpcError::from("bad prestate address"))?;
            if a == from || a == to || a == b.header.coinbase || pre.contains(&a) {
                continue;
            }
            let mut keys: Vec<String> = acc.get("storage").and_then(Value::as_object).map(|m| m.keys().cloned().collect()).unwrap_or_default();
            keys.sort();
            al.push(json!({"address": a, "storageKeys": keys}));
        }
        let mut out = json!({"accessList": al, "gasUsed": qty(r.gas_used)});
        if r.revert {
            out["error"] = json!(revert_reason(&r.output));
        } else if let Some(h) = r.halt {
            out["error"] = json!(halt_error(&h).message);
        }
        Ok(out)
    }

    /// Re-executes block b from its parent's state under the tracer; one
    /// trace per tx (all of them, target or not: the block replays whole).
    pub fn trace_block(&self, b: &Block, cfg: &TraceCfg) -> Result<Vec<Value>, RpcError> {
        if b.height == 0 {
            return Err(invalid(format!("block 0 not traceable (head {})", self.head())));
        }
        let parent = self.block_at(b.height - 1)?;
        if cfg.tracer == "muxTracer" {
            let mut per: Vec<Map<String, Value>> = vec![Map::new(); b.txs.len()];
            for (name, c) in cfg.raw.get("tracerConfig").and_then(Value::as_object).cloned().unwrap_or_default() {
                let sub = parse_trace_config(Some(&json!({"tracer": name, "tracerConfig": c})))?;
                for (i, t) in self.trace_block(b, &sub)?.into_iter().enumerate() {
                    per[i].insert(name.clone(), t);
                }
            }
            return Ok(per.into_iter().map(Value::Object).collect());
        }
        let mut ex = self.executor_at(b.height - 1, cfg.trace.clone())?;
        // ponytail: the whole block is traced even for one target tx; render Off for
        // the others when tracing big blocks by tx matters.
        let r = ex.execute_block(b, parent.header.time).map_err(|e| RpcError::from(format!("block {}: {e:#}", b.height)))?;
        Self::check_reads(&ex)?;
        r.txs.iter().enumerate().map(|(i, t)| finish_trace(self, cfg, &b.header, &t.trace_json, b.txs[i].to.is_none().then(|| b.txs[i].sender.unwrap_or_default().create(b.txs[i].nonce)))).collect()
    }
}

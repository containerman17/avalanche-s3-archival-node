//! One subnet-evm block through revm, bit-exact with vmexec/genesis.go runEVM
//! (subnet-evm core.StateProcessor.Process minus engine.Finalize, which only
//! verifies the header's block gas cost and never touches state).
//!
//! What subnet-evm changes against vanilla London..Cancun, and where:
//! - no gas refunds at all (core/state_transition.go refundGas(subnetEVM),
//!   params/hooks_libevm.go ShouldRefundGas = !IsSubnetEVM): SevmHandler::refund
//! - the coinbase gets gasUsed * effectiveGasPrice, the base fee is NOT burned
//!   (state_transition.go TransitionDb: st.state.AddBalance(Coinbase, fee)):
//!   SevmHandler::reward_beneficiary
//! - coinbase = header.Coinbase (consensus/dummy Author); AllowFeeRecipients is a
//!   header validity rule only (verifyCoinbase), execution reads the header
//! - DIFFICULTY/PREVRANDAO: libevm's EVM has IsMerge only when Random != nil,
//!   which subnet-evm sets from Shanghai (= Durango) as the difficulty bytes
//!   (core/evm.go OverrideNewEVMArgs): spec LONDON before, SHANGHAI/CANCUN after
//!   with prevrandao = difficulty
//! - stateful precompiles at 0x0200..: PrecompileOverride (params/hooks_libevm.go)
//!   only for addresses whose config is active, never warm at tx start
//!   (ActivePrecompiles adds only P256Verify under Granite): SevmPrecompiles
//! - ApplyUpgrades before the first tx (core/state_processor_ext.go
//!   ApplyPrecompileActivations): nonce 1, code 0x01, module.Configure writes
//! - Durango: MaxInitCodeSize check (= EIP-3860 under SHANGHAI)
//! - nothing runs at block end; EIP-158 empty-account deletion is the journal's

use crate::config::{Config, PrecompileConfig, StateUpgrade};
use crate::precompile::{
    self, module_index, read_state_no_warm, Env, DEPLOYER_ALLOW_LIST, INVALIDATE_DELEGATE_UNIX, P256_VERIFY,
    TX_ALLOW_LIST, WARP,
};
use crate::warp::{self, ValidatorState};
use crate::{allowlist, feemanager, nativeminter, rewardmanager};
use alloy_consensus::{Eip658Value, Receipt, ReceiptEnvelope, TxType};
use alloy_eips::eip2718::Encodable2718;
use alloy_primitives::{Address, Bloom, Bytes, B256, U256};
use alloy_rpc_types_trace::geth::{CallConfig, CallFrame, GethDefaultTracingOptions, PreStateConfig};
use revm::bytecode::opcode;
use revm::interpreter::interpreter_types::{InputsTr, Jumps, LoopControl};
use revm::context::result::ResultAndState;
use revm::DatabaseRef;
use anyhow::{anyhow, Context as _, Result};
use revm::{
    context::{
        result::{EVMError, ExecutionResult, HaltReason, InvalidTransaction},
        transaction::{AccessList, AccessListItem},
        Block as _, BlockEnv, Cfg as _, CfgEnv, Context, ContextSetters, ContextTr, Evm, Journal, JournalTr,
        LocalContext, Transaction as _, TxEnv,
    },
    MainContext,
    database::{CacheDB, EmptyDB},
    handler::{
        evm::FrameTr, instructions::EthInstructions, EthFrame, EthPrecompiles, EvmTr, EvmTrError, FrameResult,
        Handler, PrecompileProvider,
    },
    inspector::{Inspector, InspectorEvmTr, InspectorHandler, JournalExt},
    interpreter::{
        interpreter::EthInterpreter, interpreter_action::FrameInit, CallInputs, CallOutcome, CallScheme, CreateInputs,
        CreateOutcome, Gas, InstructionResult, Interpreter, InterpreterResult,
    },
    context_interface::cfg::gas::InitialAndFloorGas,
    primitives::{hardfork::SpecId, AddressSet, Log, TxKind},
    state::{Account, AccountInfo, Bytecode, EvmState, EvmStorageSlot},
    Database, DatabaseCommit,
};
use revm_inspectors::tracing::{TracingInspector, TracingInspectorConfig};
use std::marker::PhantomData;

/// A post-image row in the store's contract form (vmexec/capture.go): account
/// value = RLP[nonce, balance, codeHash], slot value = left-trimmed word, empty
/// value = delete, code-use = the code hash an account now runs.
#[derive(Debug, Clone)]
pub enum StateRow {
    Account { addr: Address, val: Vec<u8> },
    Slot { addr: Address, slot: B256, val: Vec<u8> },
    CodeUse { addr: Address, code_hash: B256 },
}

/// A block-level StateDB write (module.Configure, stateupgrade.Configure).
enum Op {
    Touch(Address),
    Nonce(Address, u64),
    AddBalance(Address, U256),
    /// SetCode as is (a precompile's 0x01).
    Code(Address, Bytes),
    /// SetCode with the state upgrade's nonce rule (1 when 0).
    CodeNonce(Address, Bytes),
    Slot(Address, U256, U256),
}

impl Op {
    fn addr(&self) -> Address {
        match self {
            Op::Touch(a) | Op::Nonce(a, _) | Op::AddBalance(a, _) | Op::Code(a, _) | Op::CodeNonce(a, _) | Op::Slot(a, ..) => *a,
        }
    }
}

#[derive(Debug)]
pub struct TxResult {
    pub hash: B256,
    pub status: bool,
    pub gas_used: u64,
    pub cumulative_gas_used: u64,
    pub receipt: ReceiptEnvelope,
    /// libevm callTracer JSON (default config), the store's trace row;
    /// empty while `deferred` still holds the unrendered trace.
    pub trace_json: String,
    /// The callTracer's arena, rendered off the execution thread
    /// (`Executor::defer_call_trace`): `DeferredTrace::render` fills `trace_json`.
    pub deferred: Option<Box<DeferredTrace>>,
    pub rows: Vec<StateRow>,
}

/// A tx's callTracer capture plus what the render needs from the executor.
pub struct DeferredTrace {
    tracer: TracingInspector,
    errors: Vec<String>,
    not_entered: bool,
    gas_used: u64,
    gas_limit: u64,
    cfg: CallConfig,
}

impl std::fmt::Debug for DeferredTrace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DeferredTrace(gas_used={})", self.gas_used)
    }
}

impl DeferredTrace {
    pub fn render(self) -> Result<String> {
        if self.not_entered {
            let f = CallFrame { typ: "STOP".to_string(), gas_used: U256::from(self.gas_used), ..Default::default() };
            return serde_json::to_string(&f).context("trace json");
        }
        let mut tracer = self.tracer;
        tracer.set_transaction_gas_limit(self.gas_limit);
        let mut f = tracer.into_geth_builder().geth_call_traces(self.cfg, self.gas_used);
        prune_unentered(&mut f);
        name_precompile_errors(&mut f, &mut self.errors.iter());
        serde_json::to_string(&f).context("trace json")
    }
}

/// `TxResult::render_deferred` on every tx of a block result.
pub fn render_deferred(r: &mut BlockResult) -> Result<()> {
    for t in &mut r.txs {
        if let Some(d) = t.deferred.take() {
            t.trace_json = d.render()?;
        }
    }
    Ok(())
}

/// An eth_call / eth_estimateGas message.
#[derive(Debug, Clone)]
pub struct CallMsg {
    pub from: Address,
    pub to: Option<Address>,
    pub gas: u64,
    pub gas_price: u128,
    pub value: U256,
    pub data: Bytes,
}

/// What execute_block / call render per tx into `trace_json` (debug_ tracers).
#[derive(Clone, Debug)]
pub enum Trace {
    /// geth callTracer (the store's default row is `Call(CallConfig::default())`).
    Call(CallConfig),
    PreState(PreStateConfig),
    /// The struct logger (geth's default tracer).
    Struct(GethDefaultTracingOptions),
    /// noopTracer: `{}`.
    Noop,
    /// No rendering (eth_call).
    Off,
}

pub struct CallOut {
    /// The rendered trace under the executor's `Trace`, "" when Off.
    pub trace_json: String,
    pub gas_used: u64,
    pub output: Bytes,
    pub revert: bool,
    /// A halt (out of gas, invalid opcode, ...) by reason; None when the call
    /// returned or reverted.
    pub halt: Option<String>,
}

#[derive(Debug, Default)]
pub struct BlockResult {
    pub gas_used: u64,
    pub receipts_root: B256,
    pub bloom: Bloom,
    pub txs: Vec<TxResult>,
    /// Block-level writes outside any tx (precompile activations).
    pub tail: Vec<StateRow>,
    /// Code deployed in this block, by hash.
    pub code: Vec<(B256, Bytes)>,
}

/// The receipts trie root of a block's tx results in order (EMPTY_ROOT_HASH
/// for none).
pub fn receipts_root(txs: &[TxResult]) -> B256 {
    if txs.is_empty() {
        return alloy_trie::EMPTY_ROOT_HASH;
    }
    alloy_trie::root::ordered_trie_root_with_encoder(txs, |t, buf| t.receipt.encode_2718(buf))
}

/// ethparams.TxGas: the miner stops once less than this is left in the pool.
pub const TX_GAS: u64 = 21_000;
/// miner.targetTxsSize: the built block's tx bytes stay under this.
pub const TARGET_TXS_SIZE: usize = 1800 * 1024;

/// Why a build candidate was left out (the ABI's `skipped` codes).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum SkipReason {
    Included = 0,
    /// Nonce too low: skipped, the sender's later txs still considered (Shift).
    NonceTooLow = 1,
    /// The tx failed to apply (nonce too high, funds, intrinsic gas, fee cap, allow list, predicates): the sender was popped.
    Invalid = 2,
    /// An earlier tx of the sender was popped.
    SenderPopped = 3,
    /// The gas left in the pool is below the tx's gas limit: the sender was popped.
    NoGas = 4,
    /// The block's tx bytes would pass the 1800 KiB target: the sender was popped.
    Size = 5,
    /// The loop had stopped (under 21,000 gas left) before this candidate.
    NotReached = 6,
}

pub struct BuildResult {
    pub result: BlockResult,
    /// Candidate indexes included, in block order.
    pub included: Vec<usize>,
    pub reasons: Vec<SkipReason>,
    /// predicate.BlockResults bytes for header.Extra (Durango+); empty before.
    pub predicate_bytes: Vec<u8>,
}

struct BlockCtx {
    granite: bool,
    tx_allow_list: bool,
    warp_cfg: Option<PrecompileConfig>,
    context_height: Option<u64>,
    header_results: warp::BlockResults,
}

enum Mode<'a> {
    Verify,
    Build(&'a mut warp::BlockResults),
}

struct TxOut {
    result: TxResult,
    code: Vec<(B256, Bytes)>,
}

enum TxFail {
    NonceTooLow(String),
    Other(anyhow::Error),
}

impl TxFail {
    fn into_anyhow(self, number: u64, i: usize, hash: B256) -> anyhow::Error {
        match self {
            TxFail::NonceTooLow(m) => anyhow!("block {number} tx {i} ({hash}): {m}"),
            TxFail::Other(e) => anyhow!("block {number} tx {i} ({hash}): {e:#}"),
        }
    }
}

// ---------------------------------------------------------------------------
// Handler: subnet-evm's gas refund and fee rules.

pub struct SevmHandler<EVM, ERROR, FRAME> {
    /// What the warp predicates add to the intrinsic gas over the plain
    /// access-list cost of their entries (precompileconfig.AccessListGasWithPredicates).
    pub predicate_gas_delta: i128,
    _p: PhantomData<(EVM, ERROR, FRAME)>,
}

impl<EVM, ERROR, FRAME> Default for SevmHandler<EVM, ERROR, FRAME> {
    fn default() -> Self {
        SevmHandler { predicate_gas_delta: 0, _p: PhantomData }
    }
}

impl<EVM, ERROR, FRAME> Handler for SevmHandler<EVM, ERROR, FRAME>
where
    EVM: EvmTr<Context: ContextTr<Journal: JournalTr<State = EvmState>>, Frame = FRAME>,
    ERROR: EvmTrError<EVM>,
    FRAME: FrameTr<FrameResult = FrameResult, FrameInit = FrameInit>,
{
    type Evm = EVM;
    type Error = ERROR;
    type HaltReason = HaltReason;

    /// IntrinsicGas with the libevm AccessListGas hook: a warp access-list
    /// entry costs its PredicateGas instead of 2400 + 1900 per key.
    fn validate_initial_tx_gas(&self, evm: &mut EVM) -> Result<InitialAndFloorGas, ERROR> {
        let mut gas = {
            let ctx = evm.ctx_ref();
            let tx = ctx.tx();
            revm::handler::validation::validate_initial_tx_gas_with_gas_params(
                tx,
                ctx.cfg().spec().into(),
                ctx.cfg().gas_params(),
                ctx.cfg().is_eip7623_disabled(),
                ctx.cfg().is_amsterdam_eip8037_enabled(),
                ctx.cfg().tx_gas_limit_cap(),
                None,
            )?
        };
        if self.predicate_gas_delta != 0 {
            let adjusted = gas.initial_regular_gas as i128 + self.predicate_gas_delta;
            let limit = evm.ctx_ref().tx().gas_limit();
            if adjusted < 0 || adjusted > u64::MAX as i128 || adjusted as u64 > limit {
                return Err(InvalidTransaction::CallGasCostMoreThanGasLimit { initial_gas: adjusted.max(0) as u64, gas_limit: limit }.into());
            }
            gas.initial_regular_gas = adjusted as u64;
        }
        Ok(gas)
    }

    /// state_transition.go refundGas(subnetEVM=true): no refund counter at all.
    fn refund(&self, _evm: &mut EVM, exec_result: &mut FrameResult, _eip7702_refund: i64) -> Result<(), ERROR> {
        exec_result.gas_mut().set_refund(0);
        Ok(())
    }

    /// state_transition.go TransitionDb: fee = gasUsed * msg.GasPrice (the
    /// effective price), all of it to the coinbase.
    fn reward_beneficiary(&self, evm: &mut EVM, exec_result: &mut FrameResult) -> Result<(), ERROR> {
        let ctx = evm.ctx();
        let basefee = ctx.block().basefee() as u128;
        let price = ctx.tx().effective_gas_price(basefee);
        let used = exec_result.gas().total_gas_spent() as u128;
        let coinbase = ctx.block().beneficiary();
        {
            use revm::context::journaled_state::account::JournaledAccountTr;
            ctx.journal_mut().load_account_mut(coinbase)?.incr_balance(U256::from(price * used));
        }
        Ok(())
    }
}

impl<EVM, ERROR> InspectorHandler for SevmHandler<EVM, ERROR, EthFrame<EthInterpreter>>
where
    EVM: InspectorEvmTr<
        Context: ContextTr<Journal: JournalTr<State = EvmState>>,
        Frame = EthFrame<EthInterpreter>,
        Inspector: Inspector<<<Self as Handler>::Evm as EvmTr>::Context, EthInterpreter>,
    >,
    ERROR: EvmTrError<EVM>,
{
    type IT = EthInterpreter;
}

// ---------------------------------------------------------------------------
// Precompiles: the eth set, P256Verify under Granite, and every stateful
// module whose config is active (params/hooks_libevm.go PrecompileOverride).

pub struct SevmPrecompiles {
    eth: EthPrecompiles,
    warm: AddressSet,
    /// `warm` changed since revm last asked `set_spec`: revm re-warms the
    /// journal's precompile set only when set_spec answers true, so a rebuild
    /// from `set_granite` (P256Verify joins at the Granite block, the eth
    /// spec unchanged) must be reported through the next set_spec or 0x100
    /// stays cold in the journal (2600 instead of 100: beam 8,182,073 ran out
    /// of gas by exactly that).
    warm_changed: bool,
    /// Enabled modules by MODULES index.
    pub enabled: [bool; 6],
    pub block_time: u64,
    pub env: Env,
    /// The Go error text of every stateful module call that failed this tx, in
    /// order: libevm's callTracer renders `err.Error()` where revm-inspectors
    /// only knows "precompiled failed".
    pub errors: Vec<String>,
}

impl SevmPrecompiles {
    fn new(spec: SpecId) -> Self {
        let eth = EthPrecompiles::new(spec);
        let warm = eth.warm_addresses().clone();
        SevmPrecompiles { eth, warm, warm_changed: false, enabled: [false; 6], block_time: 0, env: Env::default(), errors: Vec::new() }
    }

    fn rebuild_warm(&mut self) {
        self.warm = self.eth.warm_addresses().clone();
        if self.env.granite {
            self.warm.insert(P256_VERIFY);
        }
        if self.env.coreth {
            for a in precompile::CORETH_DEPRECATED {
                self.warm.insert(a);
            }
        }
        self.warm_changed = true;
    }

    pub fn set_granite(&mut self, granite: bool) {
        if self.env.granite != granite {
            self.env.granite = granite;
            self.rebuild_warm();
        }
    }
}

impl<CTX: ContextTr> PrecompileProvider<CTX> for SevmPrecompiles {
    type Output = InterpreterResult;

    fn set_spec(&mut self, spec: <CTX::Cfg as revm::context::Cfg>::Spec) -> bool {
        let changed = <EthPrecompiles as PrecompileProvider<CTX>>::set_spec(&mut self.eth, spec);
        if changed {
            self.rebuild_warm();
        }
        std::mem::take(&mut self.warm_changed) || changed
    }

    fn run(&mut self, ctx: &mut CTX, inputs: &CallInputs) -> Result<Option<InterpreterResult>, String> {
        let addr = inputs.bytecode_address;
        if self.env.coreth && precompile::CORETH_DEPRECATED.contains(&addr) {
            // DeprecatedContract.Run: (nil, suppliedGas, ErrExecutionReverted).
            return Ok(Some(InterpreterResult::new(InstructionResult::Revert, Bytes::new(), Gas::new(inputs.gas_limit))));
        }
        if self.env.granite && addr == P256_VERIFY {
            let input = inputs.input.as_bytes(ctx);
            let mut gas = Gas::new(inputs.gas_limit);
            return Ok(Some(match revm::precompile::secp256r1::p256_verify_osaka(&input, inputs.gas_limit) {
                Ok(out) => {
                    let _ = gas.record_regular_cost(out.gas_used);
                    InterpreterResult::new(InstructionResult::Return, out.bytes, gas)
                }
                Err(_) => {
                    gas.spend_all();
                    InterpreterResult::new(InstructionResult::PrecompileOOG, Bytes::new(), gas)
                }
            }));
        }
        let Some(i) = module_index(addr).filter(|i| self.enabled[*i]) else {
            return <EthPrecompiles as PrecompileProvider<CTX>>::run(&mut self.eth, ctx, inputs);
        };
        // makePrecompile: DELEGATECALL/CALLCODE into a stateful precompile reverts
        // under Granite, and invalidates the tx (so the block) from InvalidateDelegateUnix.
        if matches!(inputs.scheme, CallScheme::DelegateCall | CallScheme::CallCode) {
            if self.env.granite {
                let mut gas = Gas::new(inputs.gas_limit);
                gas.spend_all();
                return Ok(Some(InterpreterResult::new(InstructionResult::Revert, Bytes::new(), gas)));
            }
            if self.block_time >= INVALIDATE_DELEGATE_UNIX {
                return Err(format!("precompile {addr} cannot be called with {:?} (InvalidateExecution: the tx is invalid)", inputs.scheme));
            }
        }
        let input: Vec<u8> = inputs.input.as_bytes(ctx).to_vec();
        let mut gas = Gas::new(inputs.gas_limit);
        let caller = inputs.caller;
        let read_only = inputs.is_static;
        // The account is warm (the CALL loaded it); load again so the journal has it.
        ctx.journal_mut().load_account(addr).map_err(|_| "load precompile account".to_string())?;
        let r = match i {
            0 | 2 => {
                // The two pure allow lists: only the shared functions.
                match precompile::split_selector(&input) {
                    Ok((sel, args)) => allowlist::call(ctx, &self.env, addr, sel, args, &mut gas, read_only, caller)
                        .unwrap_or_else(|| Err(precompile::invalid_selector(&sel))),
                    Err(h) => Err(h),
                }
            }
            1 => nativeminter::call(ctx, &self.env, &input, &mut gas, read_only, caller),
            3 => feemanager::call(ctx, &self.env, &input, &mut gas, read_only, caller),
            4 => rewardmanager::call(ctx, &self.env, &input, &mut gas, read_only, caller),
            _ => warp::call(ctx, &self.env, &input, &mut gas, read_only, caller),
        };
        if let Err(precompile::Halt::Err(m)) = &r {
            self.errors.push(m.clone());
        }
        Ok(Some(precompile::finish(ctx, gas, r)))
    }

    /// Only the eth precompiles (plus P256Verify under Granite) are warm at tx
    /// start (vm.ActivePrecompiles); the stateful modules are not.
    fn warm_addresses(&self) -> &AddressSet {
        &self.warm
    }

    fn contains(&self, address: &Address) -> bool {
        self.warm.contains(address) || module_index(*address).is_some_and(|i| self.enabled[i])
    }
}

// ---------------------------------------------------------------------------
// Inspector: the callTracer plus the deployer allow list (params/hooks_libevm.go
// CanCreateContract, called by libevm's evm.create after the caller's nonce
// bump: a create by a tx.origin without a role fails with all its gas).

pub struct SevmInspector {
    pub tracer: TracingInspector,
    pub deployer_allow_list: bool,
    /// A create the allow list refused gets no frame in libevm (evm.create
    /// returns before CaptureStart / CaptureEnter): the tracer is not told,
    /// and the create_end that follows is swallowed.
    skip_create_end: bool,
    /// The top-level create was refused: the tx never entered the EVM.
    pub not_entered: bool,
    /// Bookkeeping for the prestate / struct tracers, off for the stored row.
    pub oog_hook: bool,
    pending: Option<PendingOp>,
    /// Slots an errored SLOAD / SSTORE was the first to load (libevm's prestate
    /// CaptureState returns on err before lookupStorage), in order.
    pub oog_slots: Vec<(Address, U256)>,
    /// geth's full gasCost of each errored SLOAD / SSTORE (the interpreter logs
    /// static + dynamic cost with the error; revm spent what was left), in order.
    pub oog_costs: Vec<u64>,
}

struct PendingOp {
    addr: Address,
    key: U256,
    fresh: bool,
    cost: u64,
}

impl SevmInspector {
    fn new(cfg: TracingInspectorConfig) -> SevmInspector {
        SevmInspector {
            tracer: TracingInspector::new(cfg),
            deployer_allow_list: false,
            skip_create_end: false,
            not_entered: false,
            oog_hook: false,
            pending: None,
            oog_slots: Vec::new(),
            oog_costs: Vec::new(),
        }
    }

    /// Per-tx reset.
    fn reset(&mut self) {
        self.tracer.fuse();
        self.skip_create_end = false;
        self.not_entered = false;
        self.pending = None;
        self.oog_slots.clear();
        self.oog_costs.clear();
    }
}

impl<CTX: ContextTr<Journal: JournalTr<State = EvmState> + JournalExt>> Inspector<CTX, EthInterpreter> for SevmInspector {
    fn initialize_interp(&mut self, interp: &mut Interpreter<EthInterpreter>, context: &mut CTX) {
        self.tracer.initialize_interp(interp, context)
    }
    fn step(&mut self, interp: &mut Interpreter<EthInterpreter>, context: &mut CTX) {
        self.tracer.step(interp, context);
        if !self.oog_hook {
            return;
        }
        self.pending = None;
        let op = interp.bytecode.opcode();
        if !matches!(op, opcode::SLOAD | opcode::SSTORE) {
            return;
        }
        let st = interp.stack.data();
        let Some(&key) = st.last() else { return };
        let addr = interp.input.target_address();
        let slot = context.journal_ref().evm_state().get(&addr).and_then(|a| a.storage.get(&key));
        let fresh = slot.is_none();
        let (orig, cur) = match slot {
            Some(s) => (s.original_value(), s.present_value()),
            None => {
                let v = context.db_mut().storage(addr, key).unwrap_or_default();
                (v, v)
            }
        };
        // geth gasSStoreEIP2929 / SLOAD under EIP-2929: cold surcharge plus the
        // net-metered write cost; the reentrancy sentry fails before any cost.
        let cold = if fresh { 2100 } else { 0 };
        let cost = if op == opcode::SLOAD {
            if fresh { 2100 } else { 100 }
        } else if interp.gas.remaining() <= 2300 {
            0
        } else {
            let new = st.get(st.len().wrapping_sub(2)).copied().unwrap_or_default();
            cold + if cur == new {
                100
            } else if orig == cur {
                if orig.is_zero() { 20000 } else { 2900 }
            } else {
                100
            }
        };
        self.pending = Some(PendingOp { addr, key, fresh, cost });
    }
    fn step_end(&mut self, interp: &mut Interpreter<EthInterpreter>, context: &mut CTX) {
        self.tracer.step_end(interp, context);
        if let Some(p) = self.pending.take() {
            if interp.bytecode.instruction_result().is_some_and(|r| r.is_halt()) {
                if p.fresh {
                    self.oog_slots.push((p.addr, p.key));
                }
                self.oog_costs.push(p.cost);
            }
        }
    }
    fn log(&mut self, context: &mut CTX, log: Log) {
        self.tracer.log(context, log)
    }
    fn call(&mut self, context: &mut CTX, inputs: &mut CallInputs) -> Option<CallOutcome> {
        self.tracer.call(context, inputs)
    }
    fn call_end(&mut self, context: &mut CTX, inputs: &CallInputs, outcome: &mut CallOutcome) {
        self.tracer.call_end(context, inputs, outcome)
    }
    fn create(&mut self, context: &mut CTX, inputs: &mut CreateInputs) -> Option<CreateOutcome> {
        if !self.deployer_allow_list {
            return self.tracer.create(context, inputs);
        }
        let origin = context.tx().caller();
        let role = read_state_no_warm(context, DEPLOYER_ALLOW_LIST, allowlist::role_slot(origin));
        if allowlist::is_enabled(role) {
            return self.tracer.create(context, inputs);
        }
        self.skip_create_end = true;
        if context.journal().depth() == 0 {
            self.not_entered = true;
        }
        // libevm evm.create: the caller's nonce is bumped and the created address
        // warmed before the hook refuses with gas 0.
        {
            use revm::context::journaled_state::account::JournaledAccountTr;
            let nonce = match context.journal_mut().load_account_mut(inputs.caller()) {
                Ok(mut caller) => {
                    let n = caller.nonce();
                    caller.bump_nonce();
                    Some(n)
                }
                Err(_) => None,
            };
            if let Some(n) = nonce {
                let created = inputs.created_address(n);
                let _ = context.journal_mut().load_account(created);
            }
        }
        let mut gas = Gas::new(inputs.gas_limit());
        gas.spend_all();
        if context.journal().depth() == 0 {
            use revm::context::LocalContextTr;
            context.local_mut().set_precompile_error_context(format!("tx.origin {origin} is not authorized to deploy a contract"));
        }
        Some(CreateOutcome::new(InterpreterResult::new(InstructionResult::PrecompileError, Bytes::new(), gas), None))
    }
    fn create_end(&mut self, context: &mut CTX, inputs: &CreateInputs, outcome: &mut CreateOutcome) {
        if std::mem::take(&mut self.skip_create_end) {
            return;
        }
        self.tracer.create_end(context, inputs, outcome)
    }
    fn selfdestruct(&mut self, contract: Address, target: Address, value: U256) {
        <TracingInspector as Inspector<CTX, EthInterpreter>>::selfdestruct(&mut self.tracer, contract, target, value)
    }
}

/// libevm's Call / Create return before CaptureEnter on a depth, balance,
/// nonce or address-collision failure, so no frame exists for it; revm's
/// inspector hooks run before those checks. Only those checks end a frame with
/// these statuses (revm-inspectors' geth texts).
fn prune_unentered(f: &mut CallFrame) {
    f.calls.retain(|c| !matches!(c.error.as_deref(), Some("CallTooDeep" | "insufficient balance for transfer" | "CreateCollision" | "NonceOverflow")));
    for c in &mut f.calls {
        prune_unentered(c);
    }
}

/// Stateful module frames end in call_end order, which is the tree's post-order:
/// each "precompiled failed" on a module address takes the next recorded Go text.
fn name_precompile_errors<'a>(f: &mut CallFrame, errs: &mut impl Iterator<Item = &'a String>) {
    for c in &mut f.calls {
        name_precompile_errors(c, errs);
    }
    if f.error.as_deref() == Some("precompiled failed") && f.to.is_some_and(|t| module_index(t).is_some()) {
        if let Some(m) = errs.next() {
            f.error = Some(m.clone());
        }
    }
}

// ---------------------------------------------------------------------------

/// The state behind the executor: revm's read and commit traits plus what
/// the node's own backend needs to provide. The in-memory `Db` is the oracle
/// runner's; rs-node's flat state implements it over latest + commit.
pub trait StateDb: Database<Error = std::convert::Infallible> + DatabaseCommit {
    /// BLOCKHASH source: the hash of an executed block.
    fn set_block_hash(&mut self, number: u64, hash: B256);
    /// An account that a tx left self-destructed or EIP-158 empty: whatever
    /// the backend cached for it (storage included) must go.
    fn forget(&mut self, addr: Address) {
        let _ = addr;
    }
}

pub type Db = CacheDB<EmptyDB>;

impl StateDb for Db {
    fn set_block_hash(&mut self, number: u64, hash: B256) {
        self.cache.block_hashes.insert(U256::from(number), hash);
    }
    /// CacheDB keeps an EIP-158-deleted account as an empty touched entry;
    /// drop it (with its storage) so the state is the trie's.
    fn forget(&mut self, addr: Address) {
        self.cache.accounts.insert(addr, revm::database::DbAccount::new_not_existing());
    }
}

type Ctx<D> = Context<BlockEnv, TxEnv, CfgEnv, D, Journal<D>, (), LocalContext>;
type SevmEvm<D> = Evm<Ctx<D>, SevmInspector, EthInstructions<EthInterpreter, Ctx<D>>, SevmPrecompiles, EthFrame<EthInterpreter>>;
type Err = EVMError<std::convert::Infallible>;

pub struct Executor<D: StateDb = Db> {
    pub cfg: Config,
    evm: SevmEvm<D>,
    /// The host's validator state for warp predicate verification; None
    /// takes the header's predicate results on trust.
    pub validator_state: Option<Box<dyn ValidatorState>>,
    /// The previous block's proposervm P-chain height (the pre-Etna predicate
    /// context); None when unknown (a window's first block).
    pub prev_pchain_height: Option<u64>,
    /// Blocks whose predicates were verified against the validator state, and
    /// blocks that carried predicates but could only be trusted.
    pub predicates_verified: u64,
    pub predicates_trusted: u64,
    /// Time split of execute_block: EVM run (incl. sender-side validation), trace
    /// JSON, journal finalize + rows + DB commit.
    pub t_evm: std::time::Duration,
    pub t_trace: std::time::Duration,
    pub t_commit: std::time::Duration,
    pub trace: Trace,
    /// Under `Trace::Call`, hand the capture out in `TxResult::deferred`
    /// instead of rendering the JSON on this thread (the node's checker
    /// thread renders it before the store write).
    pub defer_call_trace: bool,
    /// Leave `BlockResult::receipts_root` zero: the caller computes it with
    /// `receipts_root` (the node overlaps it with the state root).
    pub defer_receipts_root: bool,
    /// The miner's block size target in bytes of tx RLP (subnet-evm: 1800 KiB);
    /// config key `block-size-target-kib` in the node.
    pub block_size_target: usize,
}

/// DatabaseRef over a `&mut Database` (the prestate render reads the pre-tx
/// state out of the executor's own db).
struct RefDb<'a, D>(std::cell::RefCell<&'a mut D>);
impl<D: Database> DatabaseRef for RefDb<'_, D> {
    type Error = D::Error;
    fn basic_ref(&self, a: Address) -> std::result::Result<Option<AccountInfo>, D::Error> {
        self.0.borrow_mut().basic(a)
    }
    fn code_by_hash_ref(&self, h: B256) -> std::result::Result<Bytecode, D::Error> {
        self.0.borrow_mut().code_by_hash(h)
    }
    fn storage_ref(&self, a: Address, i: revm::primitives::StorageKey) -> std::result::Result<revm::primitives::StorageValue, D::Error> {
        self.0.borrow_mut().storage(a, i)
    }
    fn block_hash_ref(&self, n: u64) -> std::result::Result<B256, D::Error> {
        self.0.borrow_mut().block_hash(n)
    }
}

impl Executor<Db> {
    /// The in-memory state (the oracle runner's).
    pub fn new(cfg: Config) -> Result<Executor<Db>> {
        Executor::with_db(cfg, CacheDB::new(EmptyDB::default()))
    }
}

impl<D: StateDb> Executor<D> {
    /// An executor over a db that already holds a state (a node reopening
    /// its rolled state): nothing is seeded.
    pub fn open(cfg: Config, db: D) -> Executor<D> {
        let spec = cfg.spec(cfg.genesis_timestamp);
        let ctx = Context::mainnet()
            .with_db(db)
            .with_cfg(CfgEnv::new_with_spec(spec))
            .modify_cfg_chained(|c| c.chain_id = cfg.chain_id);
        let trace_cfg = TracingInspectorConfig::from_geth_call_config(&CallConfig::default());
        let inspector = SevmInspector::new(trace_cfg);
        let mut precompiles = SevmPrecompiles::new(spec);
        precompiles.env.network_id = cfg.network_id;
        precompiles.env.blockchain_id = cfg.blockchain_id;
        let evm = Evm::new_with_inspector(ctx, inspector, EthInstructions::new_mainnet_with_spec(spec), precompiles);
        Executor {
            cfg,
            evm,
            validator_state: None,
            prev_pchain_height: None,
            predicates_verified: 0,
            predicates_trusted: 0,
            t_evm: Default::default(),
            t_trace: Default::default(),
            t_commit: Default::default(),
            trace: Trace::Call(CallConfig::default()),
            defer_call_trace: false,
            defer_receipts_root: false,
            block_size_target: TARGET_TXS_SIZE,
        }
    }

    /// Switches the rendered trace (and the inspector's capture config).
    pub fn set_trace(&mut self, t: Trace) {
        let cfg = match &t {
            Trace::Call(c) => TracingInspectorConfig::from_geth_call_config(c),
            Trace::PreState(c) => TracingInspectorConfig::from_geth_prestate_config(c),
            Trace::Struct(o) => TracingInspectorConfig::from_geth_config(o),
            Trace::Noop | Trace::Off => TracingInspectorConfig::none(),
        };
        self.evm.inspector.tracer = TracingInspector::new(cfg);
        self.evm.inspector.oog_hook = matches!(t, Trace::PreState(_) | Trace::Struct(_));
        self.trace = t;
    }

    /// The trace of the tx just run, under `self.trace`; `state` is the
    /// finalized journal (post-tx, not yet committed).
    fn render_trace(&mut self, res: &ExecutionResult, state: &EvmState, gas_used: u64, gas_limit: u64) -> Result<String> {
        // callTracer's root frame reports the tx gas limit as `gas` (CaptureTxStart), not the
        // post-intrinsic gas the top call started with.
        self.evm.inspector.tracer.set_transaction_gas_limit(gas_limit);
        // libevm never enters the EVM for a top-level create the deployer allow
        // list refuses or that collides (evm.create returns before CaptureStart):
        // the callTracer's zero frame with CaptureTxEnd's gasUsed, an empty struct
        // log that did not fail, no prestate.
        let not_entered = self.evm.inspector.not_entered
            || self.evm.inspector.tracer.traces().nodes().first().is_some_and(|n| n.trace.status == Some(InstructionResult::CreateCollision));
        Ok(match self.trace.clone() {
            Trace::Off => String::new(),
            Trace::Noop => "{}".to_string(),
            Trace::Call(_) if not_entered => {
                let f = CallFrame { typ: "STOP".to_string(), gas_used: U256::from(gas_used), ..Default::default() };
                serde_json::to_string(&f).context("trace json")?
            }
            Trace::Struct(_) if not_entered => format!(r#"{{"failed":false,"gas":{gas_used},"returnValue":"0x","structLogs":[]}}"#),
            Trace::PreState(c) if not_entered => if c.is_diff_mode() { r#"{"post":{},"pre":{}}"# } else { "{}" }.to_string(),
            Trace::Call(c) => {
                let mut f = self.evm.inspector.tracer.geth_builder().geth_call_traces(c, gas_used);
                prune_unentered(&mut f);
                name_precompile_errors(&mut f, &mut self.evm.precompiles.errors.iter());
                serde_json::to_string(&f).context("trace json")?
            }
            Trace::Struct(o) => {
                let ret = res.output().cloned().unwrap_or_default();
                let mut f = self.evm.inspector.tracer.geth_builder().geth_traces(gas_used, ret, o);
                let mut costs = self.evm.inspector.oog_costs.iter();
                for l in f.struct_logs.iter_mut().filter(|l| l.error.is_some() && matches!(&*l.op, "SLOAD" | "SSTORE")) {
                    if let Some(c) = costs.next() {
                        l.gas_cost = *c;
                    }
                }
                serde_json::to_string(&f).context("trace json")?
            }
            Trace::PreState(c) => {
                let mut state = state.clone();
                for (a, k) in &self.evm.inspector.oog_slots {
                    if let Some(acc) = state.get_mut(a) {
                        acc.storage.remove(k);
                    }
                }
                let ras = ResultAndState { result: res.clone(), state };
                let (ctx, insp) = (&mut self.evm.ctx, &self.evm.inspector);
                let db = RefDb(std::cell::RefCell::new(ctx.db_mut()));
                let f = insp.tracer.geth_builder().geth_prestate_traces(&ras, &c, &db).map_err(|e| anyhow!("prestate: {e:?}"))?;
                serde_json::to_string(&f).context("trace json")?
            }
        })
    }

    /// The tx's callTracer capture, taken out of the inspector for a later render.
    fn take_deferred(&mut self, gas_used: u64, gas_limit: u64) -> Option<Box<DeferredTrace>> {
        let Trace::Call(cfg) = &self.trace else { return None };
        let cfg = cfg.clone();
        let insp = &mut self.evm.inspector;
        let not_entered = insp.not_entered || insp.tracer.traces().nodes().first().is_some_and(|n| n.trace.status == Some(InstructionResult::CreateCollision));
        let tracer = std::mem::replace(&mut insp.tracer, TracingInspector::new(TracingInspectorConfig::from_geth_call_config(&cfg)));
        Some(Box::new(DeferredTrace { tracer, errors: std::mem::take(&mut self.evm.precompiles.errors), not_entered, gas_used, gas_limit, cfg }))
    }

    /// Seeds db with what the genesis materialises: the alloc and every
    /// precompile enabled at genesis (trieAlloc in vmexec/genesis.go).
    pub fn with_db(cfg: Config, db: D) -> Result<Executor<D>> {
        let mut ex = Executor::open(cfg, db);

        // Genesis.toBlock: the alloc, then ApplyPrecompileActivations with no parent
        // on top (an initialMint adds to an alloc balance).
        let alloc = ex.cfg.alloc.clone();
        let db = ex.db_mut();
        for (addr, ga) in alloc {
            let mut acct = Account::default();
            acct.mark_touch();
            acct.mark_created();
            acct.info = db.basic(addr).unwrap().unwrap_or_default();
            acct.info.balance = ga.balance;
            acct.info.nonce = ga.nonce;
            if !ga.code.is_empty() {
                let code = Bytecode::new_raw(ga.code.clone());
                acct.info.code_hash = code.hash_slow();
                acct.info.code = Some(code);
            }
            for (k, v) in ga.storage {
                let (k, v) = (U256::from_be_bytes(k.0), U256::from_be_bytes(v.0));
                acct.storage.insert(k, EvmStorageSlot::new_changed(U256::ZERO, v, Default::default()));
            }
            db.commit([(addr, acct)].into_iter().collect());
        }
        let acts: Vec<_> = ex.cfg.activating(None, ex.cfg.genesis_timestamp).into_iter().cloned().collect();
        for c in &acts {
            ex.activate(c, 0)?;
        }
        Ok(ex)
    }

    /// An executor over a state that already holds some block's post-state
    /// (a window replay): no genesis is materialised.
    pub fn resume(cfg: Config, db: D) -> Result<Executor<D>> {
        Ok(Executor::open(cfg, db))
    }

    /// mainnet C rules: the deprecated native-asset precompiles (warm, revert).
    pub fn set_coreth(&mut self) {
        self.evm.precompiles.env.coreth = true;
        self.evm.precompiles.rebuild_warm();
    }

    pub fn db(&self) -> &D {
        self.evm.ctx.db()
    }

    pub fn db_mut(&mut self) -> &mut D {
        self.evm.ctx.db_mut()
    }

    /// The config and the db at once (disjoint borrows).
    pub fn cfg_and_db(&mut self) -> (&Config, &mut D) {
        (&self.cfg, self.evm.ctx.db_mut())
    }

    /// BLOCKHASH source: hashes of executed blocks, keyed by number.
    pub fn set_block_hash(&mut self, number: u64, hash: B256) {
        self.db_mut().set_block_hash(number, hash);
    }

    /// ApplyPrecompileActivations for one config: disable = SelfDestruct (and
    /// Finalise, so a re-enable in the same block starts clean), else SetNonce 1,
    /// SetCode 0x01, module.Configure.
    fn activate(&mut self, c: &PrecompileConfig, block_number: u64) -> Result<(Vec<StateRow>, Vec<(B256, Bytes)>)> {
        let addr = c.address;
        if c.disable {
            let mut acct = Account::default();
            acct.mark_touch();
            acct.mark_selfdestruct();
            let db = self.db_mut();
            db.commit([(addr, acct)].into_iter().collect());
            db.forget(addr);
            return Ok((vec![StateRow::Account { addr, val: Vec::new() }], Vec::new()));
        }
        let mut ops = vec![Op::Nonce(addr, 1), Op::Code(addr, Bytes::from_static(&[1]))];
        let slots: Vec<(U256, U256)> = match module_index(addr) {
            Some(1) => {
                let (mint, slots) = nativeminter::configure(c);
                ops.extend(mint.into_iter().map(|(a, v)| Op::AddBalance(a, v)));
                slots
            }
            Some(3) => feemanager::configure(c, &self.cfg.fee_config, block_number)?,
            Some(4) => rewardmanager::configure(c, self.cfg.allow_fee_recipients),
            Some(5) => Vec::new(),
            _ => allowlist::configure(c),
        };
        ops.extend(slots.into_iter().map(|(k, v)| Op::Slot(addr, k, v)));
        Ok(self.apply_ops(ops))
    }

    /// stateupgrade.Configure: per account create if absent, AddBalance,
    /// SetCode (nonce 1 when 0, EIP-158 always on), SetState.
    fn apply_state_upgrade(&mut self, u: &StateUpgrade) -> (Vec<StateRow>, Vec<(B256, Bytes)>) {
        let mut ops = Vec::new();
        for (addr, a) in &u.accounts {
            ops.push(Op::Touch(*addr));
            if let Some(b) = a.balance_change {
                ops.push(Op::AddBalance(*addr, b));
            }
            if !a.code.is_empty() {
                ops.push(Op::CodeNonce(*addr, a.code.clone()));
            }
            for (k, v) in &a.storage {
                ops.push(Op::Slot(*addr, U256::from_be_bytes(k.0), U256::from_be_bytes(v.0)));
            }
        }
        self.apply_ops(ops)
    }

    /// Applies block-level state writes outside any tx (the StateDB calls of
    /// Configure), one commit per account in first-touch order.
    fn apply_ops(&mut self, ops: Vec<Op>) -> (Vec<StateRow>, Vec<(B256, Bytes)>) {
        let mut rows = Vec::new();
        let mut code_out = Vec::new();
        let mut order: Vec<Address> = Vec::new();
        for op in &ops {
            let a = op.addr();
            if !order.contains(&a) {
                order.push(a);
            }
        }
        let db = self.db_mut();
        for addr in order {
            let existed = db.basic(addr).unwrap();
            let mut acct = Account::default();
            acct.mark_touch();
            acct.info = existed.clone().unwrap_or_default();
            let mut touched_code = false;
            let mut any_write = false;
            for op in ops.iter().filter(|o| o.addr() == addr) {
                match op {
                    Op::Touch(_) => {}
                    Op::Nonce(_, n) => {
                        acct.info.nonce = *n;
                        any_write = true;
                    }
                    Op::AddBalance(_, v) => {
                        acct.info.balance = acct.info.balance.saturating_add(*v);
                        any_write = true;
                    }
                    Op::Code(_, c) | Op::CodeNonce(_, c) => {
                        if matches!(op, Op::CodeNonce(..)) && acct.info.nonce == 0 {
                            acct.info.nonce = 1;
                        }
                        let code = Bytecode::new_raw(c.clone());
                        acct.info.code_hash = code.hash_slow();
                        acct.info.code = Some(code);
                        touched_code = true;
                        any_write = true;
                    }
                    Op::Slot(_, k, v) => {
                        let prev = db.storage(addr, *k).unwrap();
                        acct.storage.insert(*k, EvmStorageSlot::new_changed(prev, *v, Default::default()));
                        rows.push(StateRow::Slot { addr, slot: B256::from(*k), val: trimmed(*v) });
                        any_write = true;
                    }
                }
            }
            // An account only created (or a zero add) stays empty and is not materialised (EIP-158).
            if acct.info.is_empty() && acct.storage.is_empty() {
                if existed.is_some() && any_write {
                    rows.push(StateRow::Account { addr, val: Vec::new() });
                    db.commit([(addr, acct)].into_iter().collect());
                    db.forget(addr);
                }
                continue;
            }
            if touched_code {
                rows.push(StateRow::CodeUse { addr, code_hash: acct.info.code_hash });
                code_out.push((acct.info.code_hash, acct.info.code.as_ref().unwrap().original_bytes()));
            }
            rows.push(StateRow::Account { addr, val: account_rlp(&acct.info) });
            db.commit([(addr, acct)].into_iter().collect());
        }
        (rows, code_out)
    }

    /// Execute one block on top of the current state. `parent_time` is the parent
    /// header's timestamp (what makes a precompile activate once).
    pub fn execute_block(&mut self, b: &block::Block, parent_time: u64) -> Result<BlockResult> {
        let h = &b.header;
        let time = h.time;
        let mut out = BlockResult::default();

        // ApplyUpgrades: precompile activations in module order, then the state upgrades.
        let acts: Vec<_> = self.cfg.activating(Some(parent_time), time).into_iter().cloned().collect();
        for c in &acts {
            let (rows, code) = self.activate(c, h.number)?;
            out.tail.extend(rows);
            out.code.extend(code);
        }
        let ups: Vec<_> = self.cfg.activating_state_upgrades(Some(parent_time), time).into_iter().cloned().collect();
        for u in &ups {
            let (rows, code) = self.apply_state_upgrade(u);
            out.tail.extend(rows);
            out.code.extend(code);
        }
        let this_pchain = b.pvm.as_ref().map(|p| p.pchain_height);
        if b.txs.is_empty() {
            out.receipts_root = alloy_trie::EMPTY_ROOT_HASH;
            self.prev_pchain_height = this_pchain;
            return Ok(out);
        }

        let bc = self.block_ctx(h, b.pvm.as_ref().map(|p| p.pchain_height), b.pvm.as_ref().and_then(|p| p.epoch_pchain_height))?;
        let mut cumulative = 0u64;
        let mut bloom = Bloom::default();
        for (i, t) in b.txs.iter().enumerate() {
            let o = self.apply_tx(h, i, t, &bc, cumulative, Mode::Verify).map_err(|e| e.into_anyhow(h.number, i, t.hash))?;
            cumulative = o.result.cumulative_gas_used;
            bloom |= o.result.receipt.logs_bloom();
            out.code.extend(o.code);
            out.txs.push(o.result);
        }
        out.gas_used = cumulative;
        out.bloom = bloom;
        if !self.defer_receipts_root {
            out.receipts_root = receipts_root(&out.txs);
        }
        self.prev_pchain_height = this_pchain;
        Ok(out)
    }

    /// The per-block context the txs share (set_block_env plus the predicate
    /// rules); `this_pchain` / `epoch_pchain` are the proposervm heights.
    fn block_ctx(&mut self, h: &block::Header, this_pchain: Option<u64>, epoch_pchain: Option<u64>) -> Result<BlockCtx> {
        let time = h.time;
        let enabled = self.set_block_env(h)?;
        let durango = self.cfg.is_durango(time);
        let granite = self.cfg.is_granite(time);
        // The header's predicate results (customheader.PredicateBytesFromExtra), and
        // the proposervm context height the predicates were verified at
        // (proposervm block.go: parent's height pre-Etna, own from Etna, the epoch's under Granite).
        let header_results = if durango { warp::parse_block_results_opt(warp::predicate_bytes_from_extra(&h.extra)).map_err(|e| anyhow!("block {}: predicate results: {e}", h.number))? } else { Default::default() };
        let context_height = if granite {
            epoch_pchain
        } else if self.cfg.is_etna(time) {
            this_pchain
        } else {
            self.prev_pchain_height
        };
        let warp_cfg = if enabled[5] { self.cfg.warp_config(time).cloned() } else { None };
        Ok(BlockCtx { granite, tx_allow_list: enabled[2], warp_cfg, context_height, header_results })
    }

    /// One tx on the current state in the block context of `h` (core.ApplyTransaction):
    /// preCheck, the predicate check, the EVM, the receipt, the rows, the commit.
    /// An Err leaves the state as it was (revm discards the journal).
    fn apply_tx(&mut self, h: &block::Header, i: usize, t: &block::Tx, bc: &BlockCtx, cumulative: u64, mode: Mode<'_>) -> std::result::Result<TxOut, TxFail> {
        let sender = t.sender.ok_or_else(|| TxFail::Other(anyhow!("sender not recovered")))?;
        let tx_env = TxEnv {
            tx_type: t.tx_type,
            caller: sender,
            gas_limit: t.gas_limit,
            gas_price: t.gas_price,
            gas_priority_fee: if t.tx_type == 2 { Some(t.gas_tip) } else { None },
            kind: match t.to {
                Some(a) => TxKind::Call(a),
                None => TxKind::Create,
            },
            value: t.value,
            data: Bytes::from(t.input.clone()),
            nonce: t.nonce,
            chain_id: t.chain_id,
            access_list: AccessList(
                t.access_list.iter().map(|a| AccessListItem { address: a.address, storage_keys: a.storage_keys.clone() }).collect(),
            ),
            ..Default::default()
        };
        if cumulative + t.gas_limit > h.gas_limit {
            return Err(TxFail::Other(anyhow!("gas limit reached (pool {}, tx {})", h.gas_limit - cumulative, t.gas_limit)));
        }
        // preCheck: the sender must be on the tx allow list while it is active
        // (an accepted block never carries an offender).
        if bc.tx_allow_list {
            let role = read_state_no_warm(&mut self.evm.ctx, TX_ALLOW_LIST, allowlist::role_slot(sender));
            if !allowlist::is_enabled(role) {
                return Err(TxFail::Other(anyhow!("cannot issue transaction from non-allow listed address: {sender}")));
            }
        }
        // CheckTxPredicates: the warp entries of the access list are predicates,
        // charged their PredicateGas and verified before execution.
        let mut handler = SevmHandler::<SevmEvm<D>, Err, EthFrame<EthInterpreter>>::default();
        let mut predicates: Vec<Vec<B256>> = Vec::new();
        let mut failed: Vec<u8> = Vec::new();
        if let Some(wc) = &bc.warp_cfg {
            let mut delta: i128 = 0;
            for a in t.access_list.iter().filter(|a| a.address == WARP) {
                let pg = warp::predicate_gas(&a.storage_keys, bc.granite).map_err(|e| TxFail::Other(anyhow!("{e}")))?;
                delta += pg as i128 - (2400 + 1900 * a.storage_keys.len() as i128);
                predicates.push(a.storage_keys.clone());
            }
            if !predicates.is_empty() {
                handler.predicate_gas_delta = delta;
                let ours = match (&mut self.validator_state, bc.context_height) {
                    (Some(vs), Some(height)) => {
                        let mut bad = Vec::new();
                        for (pi, p) in predicates.iter().enumerate() {
                            if warp::verify_predicate(vs.as_mut(), p, self.cfg.network_id, self.cfg.subnet_id, height, wc.quorum_numerator, wc.require_primary_network_signers).is_err() {
                                bad.push(pi);
                            }
                        }
                        Some(warp::bits_from_indices(&bad))
                    }
                    _ => None,
                };
                match mode {
                    Mode::Verify => {
                        failed = bc.header_results.get(&t.hash).and_then(|m| m.get(&WARP)).cloned().unwrap_or_default();
                        match ours {
                            Some(ours) if ours != failed => {
                                return Err(TxFail::Other(anyhow!("predicate results differ from the header (ours {:x?}, header {:x?}, pchain height {:?})", ours, failed, bc.context_height)));
                            }
                            Some(_) => self.predicates_verified += 1,
                            None => self.predicates_trusted += 1,
                        }
                    }
                    Mode::Build(results) => {
                        // The miner verifies every predicate itself; without a
                        // validator state the tx cannot be built into a block.
                        failed = ours.ok_or_else(|| TxFail::Other(anyhow!("warp predicates cannot be verified without a validator state (pchain height {:?})", bc.context_height)))?;
                        self.predicates_verified += 1;
                        results.insert(t.hash, [(WARP, failed.clone())].into_iter().collect());
                    }
                }
            }
        }
        self.evm.precompiles.env.predicates = predicates;
        self.evm.precompiles.env.failed = failed;
        let t0 = std::time::Instant::now();
        self.evm.ctx.set_tx(tx_env);
        self.evm.inspector.reset();
        self.evm.precompiles.errors.clear();
        let res: ExecutionResult = match handler.inspect_run(&mut self.evm) {
            Ok(r) => r,
            Err(EVMError::Transaction(InvalidTransaction::NonceTooLow { tx, state })) => return Err(TxFail::NonceTooLow(format!("nonce too low: address {sender}, tx: {tx} state: {state}"))),
            Err(e) => return Err(TxFail::Other(anyhow!("{e:?}"))),
        };
        let t1 = std::time::Instant::now();
        self.t_evm += t1 - t0;
        let state = self.evm.ctx.journal_mut().finalize();

        let gas_used = res.tx_gas_used();
        let cumulative = cumulative + gas_used;
        let status = res.is_success();
        let (trace_json, deferred) = if self.defer_call_trace && matches!(self.trace, Trace::Call(_)) {
            (String::new(), self.take_deferred(gas_used, t.gas_limit))
        } else {
            (self.render_trace(&res, &state, gas_used, t.gas_limit).map_err(TxFail::Other)?, None)
        };
        let logs = res.into_logs();
        let receipt = Receipt { status: Eip658Value::Eip658(status), cumulative_gas_used: cumulative, logs }.with_bloom();
        let tx_type = TxType::try_from(t.tx_type).map_err(|e| TxFail::Other(anyhow!("tx type {}: {e}", t.tx_type)))?;
        let receipt = ReceiptEnvelope::from_typed(tx_type, receipt);
        let t2 = std::time::Instant::now();
        self.t_trace += t2 - t1;

        let (rows, code) = state_rows(&state);
        self.commit(state);
        self.t_commit += t2.elapsed();
        let _ = i;
        Ok(TxOut { result: TxResult { hash: t.hash, status, gas_used, cumulative_gas_used: cumulative, receipt, trace_json, deferred, rows }, code })
    }

    /// The miner's commitTransactions (miner/worker.go) over `candidates` in
    /// the caller's order: a tx is included when it applies; a nonce-too-low
    /// tx is skipped (Shift); any other failure, a tx that no longer fits the
    /// gas pool or the 1800 KiB size target pops the sender (its later txs are
    /// skipped); the loop stops when less than 21,000 gas is left. `h` is the
    /// header template (gasLimit, baseFee, time, coinbase, number, extra =
    /// the fee window); `parent_time` drives the activations as in
    /// `execute_block`. The result carries the included txs' receipts in
    /// order, the predicate results bytes for the header's extra, and a reason
    /// per candidate.
    pub fn build_block(&mut self, h: &block::Header, parent_time: u64, this_pchain: Option<u64>, epoch_pchain: Option<u64>, candidates: &[block::Tx]) -> Result<BuildResult> {
        let time = h.time;
        let mut out = BlockResult::default();
        let acts: Vec<_> = self.cfg.activating(Some(parent_time), time).into_iter().cloned().collect();
        for c in &acts {
            let (rows, code) = self.activate(c, h.number)?;
            out.tail.extend(rows);
            out.code.extend(code);
        }
        let ups: Vec<_> = self.cfg.activating_state_upgrades(Some(parent_time), time).into_iter().cloned().collect();
        for u in &ups {
            let (rows, code) = self.apply_state_upgrade(u);
            out.tail.extend(rows);
            out.code.extend(code);
        }
        let durango = self.cfg.is_durango(time);
        let bc = self.block_ctx(h, this_pchain, epoch_pchain)?;
        let mut results: warp::BlockResults = Default::default();
        let mut cumulative = 0u64;
        let mut size = 0usize;
        let mut bloom = Bloom::default();
        let mut included = Vec::new();
        let mut reasons = vec![SkipReason::NotReached; candidates.len()];
        let mut popped: Vec<Address> = Vec::new();
        for (i, t) in candidates.iter().enumerate() {
            if h.gas_limit - cumulative < TX_GAS {
                break;
            }
            let Some(sender) = t.sender else {
                reasons[i] = SkipReason::Invalid;
                continue;
            };
            if popped.contains(&sender) {
                reasons[i] = SkipReason::SenderPopped;
                continue;
            }
            if h.gas_limit - cumulative < t.gas_limit {
                reasons[i] = SkipReason::NoGas;
                popped.push(sender);
                continue;
            }
            if size + t.raw.len() > self.block_size_target {
                reasons[i] = SkipReason::Size;
                popped.push(sender);
                continue;
            }
            let applied = self.apply_tx(h, included.len(), t, &bc, cumulative, Mode::Build(&mut results));
            if applied.is_err() {
                // revm's discard_tx reverts the journal but keeps the accounts
                // it loaded: the next tx (and the next block) would read them
                // instead of the db. Drop them.
                self.evm.ctx.journal_mut().clear();
            }
            match applied {
                Ok(o) => {
                    cumulative = o.result.cumulative_gas_used;
                    size += t.raw.len();
                    bloom |= o.result.receipt.logs_bloom();
                    out.code.extend(o.code);
                    out.txs.push(o.result);
                    included.push(i);
                    reasons[i] = SkipReason::Included;
                }
                Err(TxFail::NonceTooLow(_)) => reasons[i] = SkipReason::NonceTooLow,
                Err(TxFail::Other(_)) => {
                    reasons[i] = SkipReason::Invalid;
                    popped.push(sender);
                }
            }
        }
        out.gas_used = cumulative;
        out.bloom = bloom;
        if !self.defer_receipts_root {
            out.receipts_root = receipts_root(&out.txs);
        }
        self.prev_pchain_height = this_pchain;
        let predicate_bytes = if durango { warp::encode_block_results(&results) } else { Vec::new() };
        Ok(BuildResult { result: out, included, reasons, predicate_bytes })
    }

    /// The block context of h: spec, fee rules, active precompiles.
    fn set_block_env(&mut self, h: &block::Header) -> Result<[bool; 6]> {
        let time = h.time;
        let spec = self.cfg.spec(time);
        let basefee = h.base_fee.ok_or_else(|| anyhow!("block {} has no base fee", h.number))?;
        let mut block_env = BlockEnv {
            number: U256::from(h.number),
            beneficiary: h.coinbase,
            timestamp: U256::from(time),
            gas_limit: h.gas_limit,
            basefee: basefee.to::<u64>(),
            difficulty: h.difficulty,
            prevrandao: Some(B256::from(h.difficulty)),
            ..Default::default()
        };
        if spec.is_enabled_in(SpecId::CANCUN) {
            block_env.set_blob_excess_gas_and_price(h.excess_blob_gas.unwrap_or(0), revm::primitives::eip4844::BLOB_BASE_FEE_UPDATE_FRACTION_CANCUN);
        }
        self.evm.ctx.set_block(block_env);
        // The gas params table (EIP-3860 initcode words and the rest) is per spec: a bare
        // `c.spec = spec` would keep the genesis spec's table across Durango and Etna.
        self.evm.ctx.modify_cfg(|c| c.set_spec_and_mainnet_gas_params(spec));
        let durango = self.cfg.is_durango(time);
        let granite = self.cfg.is_granite(time);
        let mut enabled = [false; 6];
        for (i, (_, addr)) in precompile::MODULES.iter().enumerate() {
            enabled[i] = self.cfg.precompile_enabled(*addr, time);
        }
        let pc = &mut self.evm.precompiles;
        pc.enabled = enabled;
        pc.block_time = time;
        pc.env.durango = durango;
        pc.env.block_number = h.number;
        pc.set_granite(granite);
        self.evm.inspector.deployer_allow_list = enabled[0];
        Ok(enabled)
    }

    /// eth_call: one message against the current state in the block context
    /// of `head` (subnet-evm DoCall: no base fee, balance or nonce checks,
    /// EIP-3607 off), nothing committed. Invalid messages are Err.
    pub fn call(&mut self, head: &block::Header, msg: &CallMsg) -> Result<CallOut> {
        self.set_block_env(head)?;
        let tx_env = TxEnv {
            tx_type: 0,
            caller: msg.from,
            gas_limit: msg.gas,
            gas_price: msg.gas_price,
            gas_priority_fee: None,
            kind: match msg.to {
                Some(a) => TxKind::Call(a),
                None => TxKind::Create,
            },
            value: msg.value,
            data: msg.data.clone(),
            nonce: self.db_mut().basic(msg.from).unwrap().map_or(0, |a| a.nonce),
            chain_id: Some(self.cfg.chain_id),
            ..Default::default()
        };
        self.evm.ctx.modify_cfg(|c| {
            c.disable_base_fee = true;
            // geth's DoCall keeps buyGas's balance check (gas * price + value).
            c.disable_balance_check = false;
            c.disable_nonce_check = true;
            c.disable_eip3607 = true;
            c.disable_block_gas_limit = true;
        });
        self.evm.ctx.set_tx(tx_env);
        self.evm.inspector.reset();
        self.evm.precompiles.errors.clear();
        let res = SevmHandler::<SevmEvm<D>, Err, EthFrame<EthInterpreter>>::default().inspect_run(&mut self.evm);
        let state = self.evm.ctx.journal_mut().finalize();
        self.evm.ctx.journal_mut().clear();
        self.evm.ctx.modify_cfg(|c| {
            c.disable_base_fee = false;
            c.disable_balance_check = false;
            c.disable_nonce_check = false;
            c.disable_eip3607 = false;
            c.disable_block_gas_limit = false;
        });
        let res = res.map_err(|e| anyhow!("{e:?}"))?;
        let gas_used = res.tx_gas_used();
        let trace_json = self.render_trace(&res, &state, gas_used, msg.gas)?;
        Ok(match res {
            ExecutionResult::Success { output, .. } => CallOut { trace_json, gas_used, output: output.into_data(), revert: false, halt: None },
            ExecutionResult::Revert { output, .. } => CallOut { trace_json, gas_used, output, revert: true, halt: None },
            ExecutionResult::Halt { reason, .. } => CallOut { trace_json, gas_used, output: Bytes::new(), revert: false, halt: Some(format!("{reason:?}")) },
        })
    }

    /// Commit a tx's journal state; the backend forgets the accounts it left
    /// self-destructed or EIP-158 empty (see StateDb::forget).
    fn commit(&mut self, state: EvmState) {
        let dead: Vec<Address> = state
            .iter()
            .filter(|(_, a)| a.is_touched() && (a.is_selfdestructed() || a.is_empty()))
            .map(|(addr, _)| *addr)
            .collect();
        let db = self.db_mut();
        db.commit(state);
        for addr in dead {
            db.forget(addr);
        }
    }
}

/// RLP[nonce, balance, codeHash].
pub fn account_rlp(info: &AccountInfo) -> Vec<u8> {
    use alloy_rlp::Encodable;
    let mut out = Vec::with_capacity(80);
    let payload_length = info.nonce.length() + info.balance.length() + info.code_hash.length();
    alloy_rlp::Header { list: true, payload_length }.encode(&mut out);
    info.nonce.encode(&mut out);
    info.balance.encode(&mut out);
    info.code_hash.encode(&mut out);
    out
}

/// Left-trimmed big-endian word; empty for zero.
pub fn trimmed(v: U256) -> Vec<u8> {
    v.to_be_bytes_trimmed_vec()
}

/// The post-image rows a tx leaves, as the trie interceptor in vmexec/capture.go
/// records them at the tx's IntermediateRoot: every touched account (deleted when
/// empty under EIP-158 or self-destructed), every changed slot, and the code an
/// account was given.
fn state_rows(state: &EvmState) -> (Vec<StateRow>, Vec<(B256, Bytes)>) {
    let mut rows = Vec::new();
    let mut code = Vec::new();
    for (addr, a) in state {
        if !a.is_touched() {
            continue;
        }
        let addr = *addr;
        if a.is_selfdestructed() || a.is_empty() {
            rows.push(StateRow::Account { addr, val: Vec::new() });
            continue;
        }
        for (k, slot) in &a.storage {
            if slot.is_changed() {
                rows.push(StateRow::Slot { addr, slot: B256::from(*k), val: trimmed(slot.present_value()) });
            }
        }
        rows.push(StateRow::Account { addr, val: account_rlp(&a.info) });
        if a.is_created() && !a.info.is_empty_code_hash() {
            rows.push(StateRow::CodeUse { addr, code_hash: a.info.code_hash });
            if let Some(c) = &a.info.code {
                code.push((a.info.code_hash, c.original_bytes()));
            }
        }
    }
    (rows, code)
}

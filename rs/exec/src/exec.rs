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

use crate::config::{precompile_code_hash, Config, FEE_MANAGER};
use crate::feemanager;
use alloy_consensus::{Eip658Value, Receipt, ReceiptEnvelope, TxType};
use alloy_eips::eip2718::Encodable2718;
use alloy_primitives::{Address, Bloom, Bytes, B256, U256};
use alloy_rpc_types_trace::geth::CallConfig;
use anyhow::{anyhow, bail, Context as _, Result};
use revm::{
    context::{
        result::{EVMError, ExecutionResult, HaltReason},
        transaction::{AccessList, AccessListItem},
        Block as _, BlockEnv, CfgEnv, Context, ContextSetters, ContextTr, Evm, Journal, JournalTr, LocalContext,
        Transaction as _, TxEnv,
    },
    MainContext,
    database::{CacheDB, EmptyDB},
    handler::{
        evm::FrameTr, instructions::EthInstructions, EthFrame, EthPrecompiles, EvmTr, EvmTrError, FrameResult,
        Handler, PrecompileProvider,
    },
    inspector::{Inspector, InspectorEvmTr, InspectorHandler},
    interpreter::{interpreter::EthInterpreter, interpreter_action::FrameInit, CallInputs, InterpreterResult},
    primitives::{hardfork::SpecId, AddressSet, TxKind},
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

#[derive(Debug)]
pub struct TxResult {
    pub hash: B256,
    pub status: bool,
    pub gas_used: u64,
    pub cumulative_gas_used: u64,
    pub receipt: ReceiptEnvelope,
    /// libevm callTracer JSON (default config), the store's trace row.
    pub trace_json: String,
    pub rows: Vec<StateRow>,
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

// ---------------------------------------------------------------------------
// Handler: subnet-evm's gas refund and fee rules.

pub struct SevmHandler<EVM, ERROR, FRAME>(PhantomData<(EVM, ERROR, FRAME)>);

impl<EVM, ERROR, FRAME> Default for SevmHandler<EVM, ERROR, FRAME> {
    fn default() -> Self {
        SevmHandler(PhantomData)
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
// Precompiles: the eth set plus the FeeManager when its config is active.

pub struct SevmPrecompiles {
    eth: EthPrecompiles,
    pub fee_manager: bool,
    pub durango: bool,
    pub block_number: u64,
}

impl SevmPrecompiles {
    fn new(spec: SpecId) -> Self {
        SevmPrecompiles { eth: EthPrecompiles::new(spec), fee_manager: false, durango: false, block_number: 0 }
    }
}

impl<CTX: ContextTr> PrecompileProvider<CTX> for SevmPrecompiles {
    type Output = InterpreterResult;

    fn set_spec(&mut self, spec: <CTX::Cfg as revm::context::Cfg>::Spec) -> bool {
        <EthPrecompiles as PrecompileProvider<CTX>>::set_spec(&mut self.eth, spec)
    }

    fn run(&mut self, context: &mut CTX, inputs: &CallInputs) -> Result<Option<InterpreterResult>, String> {
        if self.fee_manager && inputs.bytecode_address == FEE_MANAGER {
            return feemanager::run(context, inputs, self.durango, self.block_number).map(Some);
        }
        <EthPrecompiles as PrecompileProvider<CTX>>::run(&mut self.eth, context, inputs)
    }

    /// Only the eth precompiles are warm at tx start (vm.ActivePrecompiles).
    fn warm_addresses(&self) -> &AddressSet {
        <EthPrecompiles as PrecompileProvider<CTX>>::warm_addresses(&self.eth)
    }

    fn contains(&self, address: &Address) -> bool {
        (self.fee_manager && *address == FEE_MANAGER) || <EthPrecompiles as PrecompileProvider<CTX>>::contains(&self.eth, address)
    }
}

// ---------------------------------------------------------------------------

pub type Db = CacheDB<EmptyDB>;
type Ctx = Context<BlockEnv, TxEnv, CfgEnv, Db, Journal<Db>, (), LocalContext>;
type SevmEvm = Evm<Ctx, TracingInspector, EthInstructions<EthInterpreter, Ctx>, SevmPrecompiles, EthFrame<EthInterpreter>>;
type Err = EVMError<std::convert::Infallible>;

pub struct Executor {
    pub cfg: Config,
    evm: SevmEvm,
}

impl Executor {
    /// A fresh state holding what the genesis materialises: the alloc and every
    /// precompile enabled at genesis (trieAlloc in vmexec/genesis.go).
    pub fn new(cfg: Config) -> Result<Executor> {
        let spec = cfg.spec(cfg.genesis_timestamp);
        let ctx = Context::mainnet()
            .with_db(CacheDB::new(EmptyDB::default()))
            .with_cfg(CfgEnv::new_with_spec(spec))
            .modify_cfg_chained(|c| c.chain_id = cfg.chain_id);
        let trace_cfg = TracingInspectorConfig::from_geth_call_config(&CallConfig::default());
        let evm = Evm::new_with_inspector(ctx, TracingInspector::new(trace_cfg), EthInstructions::new_mainnet_with_spec(spec), SevmPrecompiles::new(spec));
        let mut ex = Executor { cfg, evm };

        // Genesis.toBlock: ApplyPrecompileActivations with no parent, then the alloc on top.
        let acts: Vec<_> = ex.cfg.activating(None, ex.cfg.genesis_timestamp).into_iter().cloned().collect();
        for c in &acts {
            ex.activate(c, 0)?;
        }
        let alloc = ex.cfg.alloc.clone();
        let db = ex.db_mut();
        for (addr, ga) in alloc {
            let mut info = db.basic(addr).unwrap().unwrap_or_default();
            info.balance = ga.balance;
            info.nonce = ga.nonce;
            if !ga.code.is_empty() {
                let code = Bytecode::new_raw(ga.code.clone());
                info.code_hash = code.hash_slow();
                info.code = Some(code);
            }
            db.insert_account_info(addr, info);
            for (k, v) in ga.storage {
                db.insert_account_storage(addr, U256::from_be_bytes(k.0), U256::from_be_bytes(v.0)).unwrap();
            }
        }
        Ok(ex)
    }

    pub fn db(&self) -> &Db {
        self.evm.ctx.db()
    }

    pub fn db_mut(&mut self) -> &mut Db {
        self.evm.ctx.db_mut()
    }

    /// BLOCKHASH source: hashes of executed blocks, keyed by number.
    pub fn set_block_hash(&mut self, number: u64, hash: B256) {
        self.db_mut().cache.block_hashes.insert(U256::from(number), hash);
    }

    /// ApplyPrecompileActivations for one config: disable = SelfDestruct, else
    /// SetNonce 1, SetCode 0x01, Configure.
    fn activate(&mut self, c: &crate::config::PrecompileConfig, block_number: u64) -> Result<Vec<StateRow>> {
        let addr = c.address;
        let mut rows = Vec::new();
        let mut acct = Account::default();
        acct.mark_touch();
        if c.disable {
            acct.mark_selfdestruct();
            rows.push(StateRow::Account { addr, val: Vec::new() });
            self.db_mut().commit([(addr, acct)].into_iter().collect());
            return Ok(rows);
        }
        let writes = feemanager::configure(c, &self.cfg.fee_config, block_number)?;
        let db = self.db_mut();
        let mut info = db.basic(addr).unwrap().unwrap_or_default();
        info.nonce = 1;
        info.code_hash = precompile_code_hash();
        info.code = Some(Bytecode::new_raw(Bytes::from_static(&[1])));
        acct.info = info;
        for (slot, value) in writes {
            let prev = db.storage(addr, slot).unwrap();
            acct.storage.insert(slot, EvmStorageSlot::new_changed(prev, value, Default::default()));
            rows.push(StateRow::Slot { addr, slot: B256::from(slot), val: trimmed(value) });
        }
        rows.push(StateRow::Account { addr, val: account_rlp(&acct.info) });
        db.commit([(addr, acct)].into_iter().collect());
        Ok(rows)
    }

    /// Execute one block on top of the current state. `parent_time` is the parent
    /// header's timestamp (what makes a precompile activate once).
    pub fn execute_block(&mut self, b: &block::Block, parent_time: u64) -> Result<BlockResult> {
        let h = &b.header;
        let time = h.time;
        let mut out = BlockResult::default();

        let acts: Vec<_> = self.cfg.activating(Some(parent_time), time).into_iter().cloned().collect();
        for c in &acts {
            out.tail.extend(self.activate(c, h.number)?);
        }
        if b.txs.is_empty() {
            out.receipts_root = alloy_trie::EMPTY_ROOT_HASH;
            return Ok(out);
        }

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
        self.evm.ctx.modify_cfg(|c| c.spec = spec);
        let durango = self.cfg.is_durango(time);
        self.evm.precompiles.fee_manager = self.cfg.precompile_enabled(FEE_MANAGER, time);
        self.evm.precompiles.durango = durango;
        self.evm.precompiles.block_number = h.number;
        if durango && self.cfg.is_granite(time) {
            bail!("Granite rules (P256Verify, precompile delegatecall revert) are not implemented");
        }

        let mut cumulative = 0u64;
        let mut receipts = Vec::with_capacity(b.txs.len());
        let mut bloom = Bloom::default();
        for (i, t) in b.txs.iter().enumerate() {
            let sender = t.sender.ok_or_else(|| anyhow!("block {} tx {i}: sender not recovered", h.number))?;
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
                bail!("block {} tx {i}: gas limit reached (pool {}, tx {})", h.number, h.gas_limit - cumulative, t.gas_limit);
            }
            self.evm.ctx.set_tx(tx_env);
            self.evm.inspector.fuse();
            let res: ExecutionResult = SevmHandler::<SevmEvm, Err, EthFrame<EthInterpreter>>::default()
                .inspect_run(&mut self.evm)
                .map_err(|e| anyhow!("block {} tx {i} ({}): {e:?}", h.number, t.hash))?;
            let state = self.evm.ctx.journal_mut().finalize();

            let gas_used = res.tx_gas_used();
            cumulative += gas_used;
            let status = res.is_success();
            let logs = res.into_logs();
            let receipt = Receipt { status: Eip658Value::Eip658(status), cumulative_gas_used: cumulative, logs }.with_bloom();
            bloom |= receipt.logs_bloom;
            let tx_type = TxType::try_from(t.tx_type).map_err(|e| anyhow!("tx type {}: {e}", t.tx_type))?;
            let receipt = ReceiptEnvelope::from_typed(tx_type, receipt);

            // callTracer's root frame reports the tx gas limit as `gas` (CaptureTxStart), not the
            // post-intrinsic gas the top call started with.
            self.evm.inspector.set_transaction_gas_limit(t.gas_limit);
            let frame = self.evm.inspector.geth_builder().geth_call_traces(CallConfig::default(), gas_used);
            let trace_json = serde_json::to_string(&frame).context("trace json")?;

            let (rows, code) = state_rows(&state);
            out.code.extend(code);
            self.commit(state);
            receipts.push(receipt.clone());
            out.txs.push(TxResult { hash: t.hash, status, gas_used, cumulative_gas_used: cumulative, receipt, trace_json, rows });
        }
        out.gas_used = cumulative;
        out.bloom = bloom;
        out.receipts_root = alloy_trie::root::ordered_trie_root_with_encoder(&receipts, |r, buf| r.encode_2718(buf));
        Ok(out)
    }

    /// Commit a tx's journal state. CacheDB keeps an EIP-158-deleted account as an
    /// empty touched entry; drop it (with its storage) so the state is the trie's.
    fn commit(&mut self, state: EvmState) {
        let dead: Vec<Address> = state
            .iter()
            .filter(|(_, a)| a.is_touched() && (a.is_selfdestructed() || a.is_empty()))
            .map(|(addr, _)| *addr)
            .collect();
        let db = self.db_mut();
        db.commit(state);
        for addr in dead {
            db.cache.accounts.insert(addr, revm::database::DbAccount::new_not_existing());
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

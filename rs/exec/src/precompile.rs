//! What every stateful precompile shares (subnet-evm precompile/contract and
//! params/hooks_libevm.go makePrecompile): the module addresses, the gas
//! constants, the geth ABI unpack rules, state access over the revm journal,
//! log emission, and the error mapping. libevm's evm.call reverts the frame
//! and consumes ALL its gas on any precompile error (err !=
//! ErrExecutionReverted => gas = 0), so every failure here is a halt with
//! the gas spent; ErrExecutionReverted itself only comes from the Granite
//! delegatecall rule and is a plain revert with the gas spent too
//! (makePrecompile returns remainingGas 0).

use alloy_primitives::{keccak256, Address, Bytes, Log, LogData, B256, U256};
use revm::{
    context::{ContextTr, JournalTr, LocalContextTr},
    interpreter::{Gas, InstructionResult, InterpreterResult},
    state::EvmState,
    Database,
};

pub const WRITE_GAS: u64 = 20_000;
pub const READ_GAS: u64 = 5_000;
pub const LOG_GAS: u64 = 375;
pub const LOG_TOPIC_GAS: u64 = 375;
pub const LOG_DATA_GAS: u64 = 8;

const fn module_addr(last: u8) -> Address {
    Address::new([0x02, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, last])
}

pub const DEPLOYER_ALLOW_LIST: Address = module_addr(0x00);
pub const NATIVE_MINTER: Address = module_addr(0x01);
pub const TX_ALLOW_LIST: Address = module_addr(0x02);
pub const FEE_MANAGER: Address = module_addr(0x03);
pub const REWARD_MANAGER: Address = module_addr(0x04);
pub const WARP: Address = module_addr(0x05);

/// The registered modules in address order (modules.RegisteredModules), the
/// index being the module's slot in per-block state.
pub const MODULES: [(&str, Address); 6] = [
    ("contractDeployerAllowListConfig", DEPLOYER_ALLOW_LIST),
    ("contractNativeMinterConfig", NATIVE_MINTER),
    ("txAllowListConfig", TX_ALLOW_LIST),
    ("feeManagerConfig", FEE_MANAGER),
    ("rewardManagerConfig", REWARD_MANAGER),
    ("warpConfig", WARP),
];

pub fn module_index(addr: Address) -> Option<usize> {
    MODULES.iter().position(|(_, a)| *a == addr)
}

/// params.P256VerifyAddress, the Granite builtin.
pub const P256_VERIFY: Address = Address::new([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x01, 0x00]);

/// params/hooks_libevm.go InvalidateDelegateUnix.
pub const INVALIDATE_DELEGATE_UNIX: u64 = 1754107200;

/// What a precompile call sees of its block and tx (contract.AccessibleState:
/// the rules, the block context, the snow context, the tx's predicates).
#[derive(Debug, Clone, Default)]
pub struct Env {
    pub durango: bool,
    pub granite: bool,
    pub block_number: u64,
    pub network_id: u32,
    pub blockchain_id: B256,
    /// The tx's warp predicates in access-list order (extstate.StateDB.GetPredicate).
    pub predicates: Vec<Vec<B256>>,
    /// Failed predicate indices as a big-endian big.Int bitset (set.Bits.Bytes()).
    pub failed: Vec<u8>,
}

impl Env {
    /// set.Bits.Contains(i) over the failed bitset.
    pub fn predicate_failed(&self, i: usize) -> bool {
        let n = self.failed.len();
        let byte = i / 8;
        byte < n && (self.failed[n - 1 - byte] >> (i % 8)) & 1 == 1
    }
}

pub enum Halt {
    OutOfGas,
    Err(String),
}

pub fn deduct(gas: &mut Gas, cost: u64) -> Result<(), Halt> {
    if gas.record_regular_cost(cost) {
        Ok(())
    } else {
        Err(Halt::OutOfGas)
    }
}

pub fn sload<CTX: ContextTr>(ctx: &mut CTX, addr: Address, slot: U256) -> Result<U256, Halt> {
    ctx.journal_mut().sload(addr, slot).map(|l| l.data).map_err(|_| Halt::Err("state read failed".into()))
}

pub fn sstore<CTX: ContextTr>(ctx: &mut CTX, addr: Address, slot: U256, value: U256) -> Result<(), Halt> {
    ctx.journal_mut().sstore(addr, slot, value).map(|_| ()).map_err(|_| Halt::Err("state write failed".into()))
}

/// StateDB.GetState outside the EVM's access-list accounting (the tx allow
/// list check in preCheck, CanCreateContract): the journal's view when the
/// account is loaded in this tx, else the backend's, and nothing is warmed.
pub fn read_state_no_warm<CTX: ContextTr<Journal: JournalTr<State = EvmState>>>(ctx: &mut CTX, addr: Address, slot: U256) -> U256 {
    if let Some(a) = ctx.journal().evm_state().get(&addr) {
        if let Some(s) = a.storage.get(&slot) {
            return s.present_value();
        }
        if a.is_created() || a.is_selfdestructed() {
            return U256::ZERO;
        }
    }
    ctx.db_mut().storage(addr, slot).unwrap_or_default()
}

/// StateDB.AddLog (BlockNumber is the receipt's, not the log's own).
pub fn add_log<CTX: ContextTr>(ctx: &mut CTX, addr: Address, topics: Vec<B256>, data: Vec<u8>) {
    ctx.journal_mut().log(Log { address: addr, data: LogData::new_unchecked(topics, Bytes::from(data)) });
}

pub fn selector(sig: &str) -> [u8; 4] {
    keccak256(sig.as_bytes())[..4].try_into().unwrap()
}

pub fn event_sig(sig: &str) -> B256 {
    keccak256(sig.as_bytes())
}

pub fn topic_addr(a: Address) -> B256 {
    B256::left_padding_from(a.as_slice())
}

pub fn word(input: &[u8], i: usize) -> Option<U256> {
    input.get(i * 32..i * 32 + 32).map(U256::from_be_slice)
}

/// geth abi.Arguments.Unpack: an empty input with expected arguments, and an
/// input shorter than the static head, are errors; extra bytes are not.
fn head(input: &[u8], words: usize) -> Result<(), String> {
    if input.is_empty() {
        return Err("abi: attempting to unmarshal an empty string while arguments are expected".into());
    }
    if input.len() < words * 32 {
        return Err(format!("abi: cannot marshal in to go type: length insufficient {} require {}", input.len(), words * 32));
    }
    Ok(())
}

/// An `address` argument at word i (the low 20 bytes, upper bytes ignored).
pub fn abi_address(input: &[u8], i: usize, nwords: usize) -> Result<Address, String> {
    head(input, nwords)?;
    Ok(Address::from_slice(&input[i * 32 + 12..i * 32 + 32]))
}

pub fn abi_u256(input: &[u8], i: usize, nwords: usize) -> Result<U256, String> {
    head(input, nwords)?;
    Ok(word(input, i).unwrap())
}

/// A `uint32` argument: abi.ReadInteger rejects a word above MaxUint32.
pub fn abi_u32(input: &[u8], i: usize, nwords: usize) -> Result<u32, String> {
    let w = abi_u256(input, i, nwords)?;
    if w > U256::from(u32::MAX) {
        return Err("abi: improperly encoded uint32 value".into());
    }
    Ok(w.to::<u32>())
}

/// A dynamic `bytes` argument whose offset word is at word i
/// (abi.lengthPrefixPointsTo + the slice read).
pub fn abi_bytes<'a>(input: &'a [u8], i: usize, nwords: usize) -> Result<&'a [u8], String> {
    head(input, nwords)?;
    let len = input.len();
    let off = word(input, i).unwrap();
    let off_end = off.checked_add(U256::from(32)).ok_or("abi offset larger than int64")?;
    if off_end > U256::from(len) {
        return Err(format!("abi: cannot marshal in to go slice: offset {off_end} would go over slice boundary (len={len})"));
    }
    if off_end > U256::from(i64::MAX) {
        return Err(format!("abi offset larger than int64: {off_end}"));
    }
    let off_end = off_end.to::<usize>();
    let n = U256::from_be_slice(&input[off_end - 32..off_end]);
    let total = U256::from(off_end).checked_add(n).ok_or("abi: length larger than int64")?;
    if total > U256::from(i64::MAX) {
        return Err(format!("abi: length larger than int64: {total}"));
    }
    if total > U256::from(len) {
        return Err(format!("abi: cannot marshal in to go type: length insufficient {len} require {total}"));
    }
    Ok(&input[off_end..total.to::<usize>()])
}

/// abi.encode of one dynamic `bytes` value in place: length word then the
/// data right-padded to 32.
pub fn pack_bytes(out: &mut Vec<u8>, b: &[u8]) {
    out.extend_from_slice(&U256::from(b.len()).to_be_bytes::<32>());
    out.extend_from_slice(b);
    let pad = (32 - b.len() % 32) % 32;
    out.extend(std::iter::repeat(0u8).take(pad));
}

/// The frame result of a stateful precompile call.
pub fn finish<CTX: ContextTr>(ctx: &mut CTX, mut gas: Gas, r: Result<Bytes, Halt>) -> InterpreterResult {
    match r {
        Ok(output) => InterpreterResult::new(InstructionResult::Return, output, gas),
        Err(Halt::OutOfGas) => {
            gas.spend_all();
            InterpreterResult::new(InstructionResult::PrecompileOOG, Bytes::new(), gas)
        }
        Err(Halt::Err(msg)) => {
            if ctx.journal().depth() == 1 {
                ctx.local_mut().set_precompile_error_context(msg);
            }
            gas.spend_all();
            InterpreterResult::new(InstructionResult::PrecompileError, Bytes::new(), gas)
        }
    }
}

pub fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

/// contract.go Run: no fallback in any module, so an input shorter than the
/// selector is "missing function selector".
pub fn split_selector(input: &[u8]) -> Result<([u8; 4], &[u8]), Halt> {
    if input.len() < 4 {
        return Err(Halt::Err(format!("missing function selector to precompile - input length ({})", input.len())));
    }
    Ok((input[..4].try_into().unwrap(), &input[4..]))
}

pub fn invalid_selector(sel: &[u8; 4]) -> Halt {
    Halt::Err(format!("invalid function selector: 0x{}", hex(sel)))
}

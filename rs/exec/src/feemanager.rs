//! The FeeManager stateful precompile (subnet-evm precompile/contracts/feemanager
//! + precompile/allowlist), as a revm precompile over the journal.
//!
//! Storage layout in the precompile's own account (feemanager/contract.go):
//!   slot common.Hash{byte(i)} for i = 1..=8: gasLimit, targetBlockRate, minBaseFee,
//!     targetGas, baseFeeChangeDenominator, minBlockGasCost, maxBlockGasCost,
//!     blockGasCostStep (each a big-endian word),
//!   slot common.Hash{'l','c','a'}: the block number of the last change,
//!   slot BytesToHash(address): that address's allow-list role (0 none, 1 enabled,
//!     2 admin, 3 manager).
//!
//! Gas (precompile/contract/utils.go): 20_000 per written slot, 5_000 per read
//! slot; setFeeConfig 9 writes, getFeeConfig 8 reads, getFeeConfigLastChangedAt
//! 1 read, allow-list setters 1 write, readAllowList 1 read.
//!
//! Errors: any error from the contract makes libevm's Call revert the frame and
//! consume all its gas (evm.call: err != ErrExecutionReverted => gas = 0), so
//! every failure below is a halt with the gas spent.

use crate::config::{FeeConfig, PrecompileConfig, FEE_MANAGER};
use alloy_primitives::{keccak256, Address, Bytes, U256};
use revm::{
    context::{ContextTr, JournalTr, LocalContextTr},
    interpreter::{CallInputs, Gas, InstructionResult, InterpreterResult},
};
use std::sync::OnceLock;

const WRITE_GAS: u64 = 20_000;
const READ_GAS: u64 = 5_000;

pub const ROLE_NONE: U256 = U256::ZERO;
pub const ROLE_ENABLED: U256 = U256::from_limbs([1, 0, 0, 0]);
pub const ROLE_ADMIN: U256 = U256::from_limbs([2, 0, 0, 0]);
pub const ROLE_MANAGER: U256 = U256::from_limbs([3, 0, 0, 0]);

/// common.Hash{byte(i)}: the first byte of the word.
pub fn field_slot(i: u8) -> U256 {
    U256::from(i) << 248
}

/// common.Hash{'l','c','a'}.
pub fn last_changed_slot() -> U256 {
    U256::from(0x6c6361u64) << (29 * 8)
}

/// common.BytesToHash(address.Bytes()): the address left-padded.
pub fn role_slot(a: Address) -> U256 {
    U256::from_be_slice(a.as_slice())
}

struct Selectors {
    read_allow_list: [u8; 4],
    set_admin: [u8; 4],
    set_enabled: [u8; 4],
    set_none: [u8; 4],
    set_manager: [u8; 4],
    get_fee_config: [u8; 4],
    get_last_changed: [u8; 4],
    set_fee_config: [u8; 4],
}

fn sel(sig: &str) -> [u8; 4] {
    keccak256(sig.as_bytes())[..4].try_into().unwrap()
}

fn selectors() -> &'static Selectors {
    static S: OnceLock<Selectors> = OnceLock::new();
    S.get_or_init(|| Selectors {
        read_allow_list: sel("readAllowList(address)"),
        set_admin: sel("setAdmin(address)"),
        set_enabled: sel("setEnabled(address)"),
        set_none: sel("setNone(address)"),
        set_manager: sel("setManager(address)"),
        get_fee_config: sel("getFeeConfig()"),
        get_last_changed: sel("getFeeConfigLastChangedAt()"),
        set_fee_config: sel("setFeeConfig(uint256,uint256,uint256,uint256,uint256,uint256,uint256,uint256)"),
    })
}

fn is_enabled(role: U256) -> bool {
    role == ROLE_ENABLED || role == ROLE_ADMIN || role == ROLE_MANAGER
}

/// allowlist.Role.CanModify(from, target).
fn can_modify(caller: U256, from: U256, target: U256) -> bool {
    if caller == ROLE_ADMIN {
        true
    } else if caller == ROLE_MANAGER {
        (from == ROLE_ENABLED || from == ROLE_NONE) && (target == ROLE_ENABLED || target == ROLE_NONE)
    } else {
        false
    }
}

fn word(input: &[u8], i: usize) -> U256 {
    U256::from_be_slice(&input[i * 32..i * 32 + 32])
}

/// ABI `address` argument: the low 20 bytes of the word.
fn address_arg(input: &[u8], strict: bool) -> Result<Address, String> {
    if strict && input.len() != 32 {
        return Err(format!("invalid input length for modifying allow list: {}", input.len()));
    }
    if input.len() < 32 {
        return Err("abi: attempting to unmarshall an empty string while arguments are expected".into());
    }
    Ok(Address::from_slice(&input[12..32]))
}

/// One stateful call. `durango` selects the post-Durango ABI (non-strict input
/// lengths, setManager, events). Events are not implemented: a setter under
/// Durango is a hard error rather than a silently missing log.
pub fn run<CTX: ContextTr>(
    ctx: &mut CTX,
    inputs: &CallInputs,
    durango: bool,
    block_number: u64,
) -> Result<InterpreterResult, String> {
    let input: Vec<u8> = inputs.input.as_bytes(ctx).to_vec();
    let mut gas = Gas::new(inputs.gas_limit);
    let read_only = inputs.is_static;
    let caller = inputs.caller;
    let r = call(ctx, &input, &mut gas, read_only, caller, durango, block_number);
    Ok(match r {
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
    })
}

enum Halt {
    OutOfGas,
    Err(String),
}

fn deduct(gas: &mut Gas, cost: u64) -> Result<(), Halt> {
    if gas.record_regular_cost(cost) {
        Ok(())
    } else {
        Err(Halt::OutOfGas)
    }
}

fn sload<CTX: ContextTr>(ctx: &mut CTX, slot: U256) -> Result<U256, Halt> {
    ctx.journal_mut()
        .sload(FEE_MANAGER, slot)
        .map(|l| l.data)
        .map_err(|_| Halt::Err("state read failed".into()))
}

fn sstore<CTX: ContextTr>(ctx: &mut CTX, slot: U256, value: U256) -> Result<(), Halt> {
    ctx.journal_mut()
        .sstore(FEE_MANAGER, slot, value)
        .map(|_| ())
        .map_err(|_| Halt::Err("state write failed".into()))
}

fn call<CTX: ContextTr>(
    ctx: &mut CTX,
    input: &[u8],
    gas: &mut Gas,
    read_only: bool,
    caller: Address,
    durango: bool,
    block_number: u64,
) -> Result<Bytes, Halt> {
    // contract.go Run: no fallback, so an empty input is "missing selector".
    if input.len() < 4 {
        return Err(Halt::Err(format!("missing function selector to precompile - input length ({})", input.len())));
    }
    let s = selectors();
    let selector: [u8; 4] = input[..4].try_into().unwrap();
    let args = &input[4..];
    let strict = !durango;
    // The account is warm (the CALL loaded it); load again so the journal has it.
    ctx.journal_mut()
        .load_account(FEE_MANAGER)
        .map_err(|_| Halt::Err("load precompile account".into()))?;

    if selector == s.read_allow_list {
        deduct(gas, READ_GAS)?;
        let a = address_arg(args, strict).map_err(Halt::Err)?;
        let role = sload(ctx, role_slot(a))?;
        return Ok(Bytes::from(role.to_be_bytes::<32>()));
    }
    let setter = if selector == s.set_admin {
        Some(ROLE_ADMIN)
    } else if selector == s.set_enabled {
        Some(ROLE_ENABLED)
    } else if selector == s.set_none {
        Some(ROLE_NONE)
    } else if selector == s.set_manager {
        if !durango {
            return Err(Halt::Err(format!("invalid non-activated function selector: 0x{}", hex(&selector))));
        }
        Some(ROLE_MANAGER)
    } else {
        None
    };
    if let Some(target) = setter {
        deduct(gas, WRITE_GAS)?;
        let a = address_arg(args, strict).map_err(Halt::Err)?;
        if read_only {
            return Err(Halt::Err("write protection".into()));
        }
        let caller_role = sload(ctx, role_slot(caller))?;
        let from = sload(ctx, role_slot(a))?;
        if !can_modify(caller_role, from, target) {
            return Err(Halt::Err(format!("cannot modify allow list: modify address: {a}, from role: {from}, to role: {target}")));
        }
        if durango {
            return Err(Halt::Err("feemanager: Durango RoleSet event not implemented".into()));
        }
        sstore(ctx, role_slot(a), target)?;
        return Ok(Bytes::new());
    }
    if selector == s.get_fee_config {
        deduct(gas, READ_GAS * 8)?;
        let mut out = Vec::with_capacity(256);
        for i in 1..=8u8 {
            out.extend_from_slice(&sload(ctx, field_slot(i))?.to_be_bytes::<32>());
        }
        return Ok(Bytes::from(out));
    }
    if selector == s.get_last_changed {
        deduct(gas, READ_GAS)?;
        return Ok(Bytes::from(sload(ctx, last_changed_slot())?.to_be_bytes::<32>()));
    }
    if selector == s.set_fee_config {
        deduct(gas, WRITE_GAS * 9)?;
        if read_only {
            return Err(Halt::Err("write protection".into()));
        }
        if strict && args.len() != 256 {
            return Err(Halt::Err(format!("invalid input length for fee config Input: {}", args.len())));
        }
        if args.len() < 256 {
            return Err(Halt::Err("failed to unpack input".into()));
        }
        let words: [U256; 8] = std::array::from_fn(|i| word(args, i));
        let fc = FeeConfig::from_words(&words);
        let caller_role = sload(ctx, role_slot(caller))?;
        if !is_enabled(caller_role) {
            return Err(Halt::Err(format!("non-enabled cannot change fee config: {caller}")));
        }
        if durango {
            return Err(Halt::Err("feemanager: Durango FeeConfigChanged event not implemented".into()));
        }
        fc.verify().map_err(|e| Halt::Err(format!("cannot verify fee config: {e}")))?;
        for (i, w) in fc.words().iter().enumerate() {
            sstore(ctx, field_slot(i as u8 + 1), *w)?;
        }
        sstore(ctx, last_changed_slot(), U256::from(block_number))?;
        return Ok(Bytes::new());
    }
    Err(Halt::Err(format!("invalid function selector: 0x{}", hex(&selector))))
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

/// module.Configure at activation: the slots StoreFeeConfig (initial config or the
/// chain's) and AllowListConfig.Configure write, in that order.
pub fn configure(c: &PrecompileConfig, chain_fee: &FeeConfig, block_number: u64) -> anyhow::Result<Vec<(U256, U256)>> {
    let fc = c.initial_fee_config.as_ref().unwrap_or(chain_fee);
    fc.verify().map_err(|e| anyhow::anyhow!("cannot configure given initial fee config: cannot verify fee config: {e}"))?;
    let mut w = Vec::new();
    for (i, v) in fc.words().iter().enumerate() {
        w.push((field_slot(i as u8 + 1), *v));
    }
    w.push((last_changed_slot(), U256::from(block_number)));
    for a in &c.enabled {
        w.push((role_slot(*a), ROLE_ENABLED));
    }
    for a in &c.admins {
        w.push((role_slot(*a), ROLE_ADMIN));
    }
    for a in &c.managers {
        w.push((role_slot(*a), ROLE_MANAGER));
    }
    Ok(w)
}

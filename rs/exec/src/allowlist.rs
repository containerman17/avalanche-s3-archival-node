//! precompile/allowlist: the role slots every permissioned module shares, the
//! setAdmin/setEnabled/setManager/setNone/readAllowList functions, the
//! Durango RoleSet event, and AllowListConfig.Configure.
//!
//! Storage: slot BytesToHash(address) in the module's own account holds the
//! role (0 none, 1 enabled, 2 admin, 3 manager).

use crate::config::PrecompileConfig;
use crate::precompile::{
    abi_address, add_log, deduct, event_sig, selector, sload, sstore, topic_addr, Env, Halt, LOG_DATA_GAS, LOG_GAS,
    LOG_TOPIC_GAS, READ_GAS, WRITE_GAS,
};
use alloy_primitives::{Address, Bytes, B256, U256};
use revm::{context::ContextTr, interpreter::Gas};
use std::sync::OnceLock;

pub const ROLE_NONE: U256 = U256::ZERO;
pub const ROLE_ENABLED: U256 = U256::from_limbs([1, 0, 0, 0]);
pub const ROLE_ADMIN: U256 = U256::from_limbs([2, 0, 0, 0]);
pub const ROLE_MANAGER: U256 = U256::from_limbs([3, 0, 0, 0]);

pub const MODIFY_GAS: u64 = WRITE_GAS;
pub const READ_ALLOW_LIST_GAS: u64 = READ_GAS;
/// AllowListEventGasCost: base + 4 topics + 32 data bytes.
pub const EVENT_GAS: u64 = LOG_GAS + LOG_TOPIC_GAS * 4 + LOG_DATA_GAS * 32;

/// common.BytesToHash(address.Bytes()): the address left-padded.
pub fn role_slot(a: Address) -> U256 {
    U256::from_be_slice(a.as_slice())
}

pub fn is_enabled(role: U256) -> bool {
    role == ROLE_ENABLED || role == ROLE_ADMIN || role == ROLE_MANAGER
}

/// Role.CanModify(from, target).
pub fn can_modify(caller: U256, from: U256, target: U256) -> bool {
    if caller == ROLE_ADMIN {
        true
    } else if caller == ROLE_MANAGER {
        (from == ROLE_ENABLED || from == ROLE_NONE) && (target == ROLE_ENABLED || target == ROLE_NONE)
    } else {
        false
    }
}

pub fn role_name(r: U256) -> &'static str {
    if r == ROLE_NONE {
        "NoRole"
    } else if r == ROLE_ENABLED {
        "EnabledRole"
    } else if r == ROLE_ADMIN {
        "AdminRole"
    } else if r == ROLE_MANAGER {
        "ManagerRole"
    } else {
        "UnknownRole"
    }
}

pub fn get_role<CTX: ContextTr>(ctx: &mut CTX, precompile: Address, a: Address) -> Result<U256, Halt> {
    sload(ctx, precompile, role_slot(a))
}

struct Selectors {
    read_allow_list: [u8; 4],
    set_admin: [u8; 4],
    set_enabled: [u8; 4],
    set_none: [u8; 4],
    set_manager: [u8; 4],
    role_set: B256,
}

fn sels() -> &'static Selectors {
    static S: OnceLock<Selectors> = OnceLock::new();
    S.get_or_init(|| Selectors {
        read_allow_list: selector("readAllowList(address)"),
        set_admin: selector("setAdmin(address)"),
        set_enabled: selector("setEnabled(address)"),
        set_none: selector("setNone(address)"),
        set_manager: selector("setManager(address)"),
        role_set: event_sig("RoleSet(uint256,address,address,uint256)"),
    })
}

/// The address argument of the allow-list functions: exactly one word before
/// Durango (allowListInputLen), the geth ABI rules after.
fn address_arg(args: &[u8], strict: bool, what: &str) -> Result<Address, Halt> {
    if strict && args.len() != 32 {
        return Err(Halt::Err(format!("invalid input length for {what}: {}", args.len())));
    }
    abi_address(args, 0, 1).map_err(Halt::Err)
}

/// The allow-list functions of `precompile`; None when the selector is not one
/// of them (the module's own functions come next).
pub fn call<CTX: ContextTr>(
    ctx: &mut CTX,
    env: &Env,
    precompile: Address,
    sel: [u8; 4],
    args: &[u8],
    gas: &mut Gas,
    read_only: bool,
    caller: Address,
) -> Option<Result<Bytes, Halt>> {
    let s = sels();
    if sel == s.read_allow_list {
        return Some(read_allow_list(ctx, env, precompile, args, gas));
    }
    let target = if sel == s.set_admin {
        ROLE_ADMIN
    } else if sel == s.set_enabled {
        ROLE_ENABLED
    } else if sel == s.set_none {
        ROLE_NONE
    } else if sel == s.set_manager {
        // NewStatefulPrecompileFunctionWithActivator: setManager exists from Durango.
        if !env.durango {
            return Some(Err(Halt::Err(format!("invalid non-activated function selector: 0x{}", crate::precompile::hex(&sel)))));
        }
        ROLE_MANAGER
    } else {
        return None;
    };
    Some(set_role(ctx, env, precompile, target, args, gas, read_only, caller))
}

fn read_allow_list<CTX: ContextTr>(ctx: &mut CTX, env: &Env, precompile: Address, args: &[u8], gas: &mut Gas) -> Result<Bytes, Halt> {
    deduct(gas, READ_ALLOW_LIST_GAS)?;
    let a = address_arg(args, !env.durango, "read allow list")?;
    let role = get_role(ctx, precompile, a)?;
    Ok(Bytes::from(role.to_be_bytes::<32>()))
}

/// createAllowListRoleSetter.
fn set_role<CTX: ContextTr>(
    ctx: &mut CTX,
    env: &Env,
    precompile: Address,
    target: U256,
    args: &[u8],
    gas: &mut Gas,
    read_only: bool,
    caller: Address,
) -> Result<Bytes, Halt> {
    deduct(gas, MODIFY_GAS)?;
    let a = address_arg(args, !env.durango, "modifying allow list")?;
    if read_only {
        return Err(Halt::Err("write protection".into()));
    }
    let caller_role = get_role(ctx, precompile, caller)?;
    let from = get_role(ctx, precompile, a)?;
    if !can_modify(caller_role, from, target) {
        return Err(Halt::Err(format!(
            "cannot modify allow list: modify address: {caller}, from role: {}, to role: {}",
            role_name(from),
            role_name(target)
        )));
    }
    if env.durango {
        deduct(gas, EVENT_GAS)?;
        add_log(
            ctx,
            precompile,
            vec![sels().role_set, B256::from(target), topic_addr(a), topic_addr(caller)],
            from.to_be_bytes::<32>().to_vec(),
        );
    }
    sstore(ctx, precompile, role_slot(a), target)?;
    Ok(Bytes::new())
}

/// AllowListConfig.Configure: enabled, then admins, then managers.
pub fn configure(c: &PrecompileConfig) -> Vec<(U256, U256)> {
    let mut w = Vec::new();
    for a in &c.enabled {
        w.push((role_slot(*a), ROLE_ENABLED));
    }
    for a in &c.admins {
        w.push((role_slot(*a), ROLE_ADMIN));
    }
    for a in &c.managers {
        w.push((role_slot(*a), ROLE_MANAGER));
    }
    w
}

//! The FeeManager stateful precompile (subnet-evm precompile/contracts/feemanager).
//!
//! Storage layout in the precompile's own account (feemanager/contract.go):
//!   slot common.Hash{byte(i)} for i = 1..=8: gasLimit, targetBlockRate, minBaseFee,
//!     targetGas, baseFeeChangeDenominator, minBlockGasCost, maxBlockGasCost,
//!     blockGasCostStep (each a big-endian word),
//!   slot common.Hash{'l','c','a'}: the block number of the last change,
//!   slot BytesToHash(address): that address's allow-list role.
//!
//! Gas: 20_000 per written slot, 5_000 per read slot; setFeeConfig 9 writes,
//! getFeeConfig 8 reads, getFeeConfigLastChangedAt 1 read, plus the Durango
//! FeeConfigChanged event (GetFeeConfigGasCost + log base + 2 topics + 512
//! data bytes).

use crate::allowlist;
use crate::config::{FeeConfig, PrecompileConfig};
use crate::precompile::{
    add_log, deduct, event_sig, invalid_selector, selector, sload, split_selector, sstore, topic_addr, Env, Halt,
    FEE_MANAGER, LOG_DATA_GAS, LOG_GAS, LOG_TOPIC_GAS, READ_GAS, WRITE_GAS,
};
use alloy_primitives::{Address, Bytes, B256, U256};
use revm::{context::ContextTr, interpreter::Gas};
use std::sync::OnceLock;

pub const SET_FEE_CONFIG_GAS: u64 = WRITE_GAS * 9;
pub const GET_FEE_CONFIG_GAS: u64 = READ_GAS * 8;
pub const GET_LAST_CHANGED_GAS: u64 = READ_GAS;
/// FeeConfigChangedEventGasCost.
pub const EVENT_GAS: u64 = GET_FEE_CONFIG_GAS + LOG_GAS + LOG_TOPIC_GAS * 2 + 2 * 256 * LOG_DATA_GAS;

pub use crate::allowlist::{ROLE_ADMIN, ROLE_ENABLED, ROLE_MANAGER, ROLE_NONE};

/// common.Hash{byte(i)}: the first byte of the word.
pub fn field_slot(i: u8) -> U256 {
    U256::from(i) << 248
}

/// common.Hash{'l','c','a'}.
pub fn last_changed_slot() -> U256 {
    U256::from(0x6c6361u64) << (29 * 8)
}

pub fn role_slot(a: Address) -> U256 {
    allowlist::role_slot(a)
}

struct Selectors {
    get_fee_config: [u8; 4],
    get_last_changed: [u8; 4],
    set_fee_config: [u8; 4],
    changed: B256,
}

fn sels() -> &'static Selectors {
    static S: OnceLock<Selectors> = OnceLock::new();
    S.get_or_init(|| Selectors {
        get_fee_config: selector("getFeeConfig()"),
        get_last_changed: selector("getFeeConfigLastChangedAt()"),
        set_fee_config: selector("setFeeConfig(uint256,uint256,uint256,uint256,uint256,uint256,uint256,uint256)"),
        changed: event_sig(
            "FeeConfigChanged(address,(uint256,uint256,uint256,uint256,uint256,uint256,uint256,uint256),(uint256,uint256,uint256,uint256,uint256,uint256,uint256,uint256))",
        ),
    })
}

fn stored_fee_config<CTX: ContextTr>(ctx: &mut CTX) -> Result<[U256; 8], Halt> {
    let mut w = [U256::ZERO; 8];
    for i in 0..8u8 {
        w[i as usize] = sload(ctx, FEE_MANAGER, field_slot(i + 1))?;
    }
    Ok(w)
}

pub fn call<CTX: ContextTr>(ctx: &mut CTX, env: &Env, input: &[u8], gas: &mut Gas, read_only: bool, caller: Address) -> Result<Bytes, Halt> {
    let (sel, args) = split_selector(input)?;
    if let Some(r) = allowlist::call(ctx, env, FEE_MANAGER, sel, args, gas, read_only, caller) {
        return r;
    }
    let s = sels();
    if sel == s.get_fee_config {
        deduct(gas, GET_FEE_CONFIG_GAS)?;
        let mut out = Vec::with_capacity(256);
        for w in stored_fee_config(ctx)? {
            out.extend_from_slice(&w.to_be_bytes::<32>());
        }
        return Ok(Bytes::from(out));
    }
    if sel == s.get_last_changed {
        deduct(gas, GET_LAST_CHANGED_GAS)?;
        return Ok(Bytes::from(sload(ctx, FEE_MANAGER, last_changed_slot())?.to_be_bytes::<32>()));
    }
    if sel == s.set_fee_config {
        deduct(gas, SET_FEE_CONFIG_GAS)?;
        if read_only {
            return Err(Halt::Err("write protection".into()));
        }
        if !env.durango && args.len() != 256 {
            return Err(Halt::Err(format!("invalid input length for fee config Input: {}", args.len())));
        }
        if args.len() < 256 {
            return Err(Halt::Err(if args.is_empty() {
                "abi: attempting to unmarshal an empty string while arguments are expected".into()
            } else {
                format!("abi: cannot marshal in to go type: length insufficient {} require 256", args.len())
            }));
        }
        let words: [U256; 8] = std::array::from_fn(|i| U256::from_be_slice(&args[i * 32..i * 32 + 32]));
        let fc = FeeConfig::from_words(&words);
        let caller_role = allowlist::get_role(ctx, FEE_MANAGER, caller)?;
        if !allowlist::is_enabled(caller_role) {
            return Err(Halt::Err(format!("non-enabled cannot change fee config: {caller}")));
        }
        if env.durango {
            deduct(gas, EVENT_GAS)?;
            let old = stored_fee_config(ctx)?;
            let mut data = Vec::with_capacity(512);
            for w in old.iter().chain(fc.words().iter()) {
                data.extend_from_slice(&w.to_be_bytes::<32>());
            }
            add_log(ctx, FEE_MANAGER, vec![s.changed, topic_addr(caller)], data);
        }
        fc.verify().map_err(|e| Halt::Err(format!("cannot verify fee config: {e}")))?;
        for (i, w) in fc.words().iter().enumerate() {
            sstore(ctx, FEE_MANAGER, field_slot(i as u8 + 1), *w)?;
        }
        sstore(ctx, FEE_MANAGER, last_changed_slot(), U256::from(env.block_number))?;
        return Ok(Bytes::new());
    }
    Err(invalid_selector(&sel))
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
    w.extend(allowlist::configure(c));
    Ok(w)
}

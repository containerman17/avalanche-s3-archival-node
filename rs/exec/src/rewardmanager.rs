//! The RewardManager stateful precompile (precompile/contracts/rewardmanager).
//! It only records the block-builder policy the consensus engine verifies the
//! coinbase against (allowFeeRecipients / reward address / rewards disabled);
//! execution itself always pays header.Coinbase.
//!
//! Storage: slot common.Hash{'r','a','s','k'} holds common.Hash{'a','f','r','a','v'}
//! (fee recipients allowed), BytesToHash(BlackholeAddr) (rewards disabled) or
//! BytesToHash(rewardAddress); BytesToHash(address) the allow-list roles.

use crate::allowlist;
use crate::config::PrecompileConfig;
use crate::precompile::{
    abi_address, add_log, deduct, event_sig, invalid_selector, selector, sload, split_selector, sstore, topic_addr, Env,
    Halt, LOG_GAS, LOG_TOPIC_GAS, READ_GAS, REWARD_MANAGER, WRITE_GAS,
};
use alloy_primitives::{Address, Bytes, B256, U256};
use revm::{context::ContextTr, interpreter::Gas};
use std::sync::OnceLock;

pub const ALLOW_FEE_RECIPIENTS_GAS: u64 = WRITE_GAS + READ_GAS;
pub const ARE_FEE_RECIPIENTS_ALLOWED_GAS: u64 = READ_GAS;
pub const CURRENT_REWARD_ADDRESS_GAS: u64 = READ_GAS;
pub const DISABLE_REWARDS_GAS: u64 = WRITE_GAS + READ_GAS;
pub const SET_REWARD_ADDRESS_GAS: u64 = WRITE_GAS + READ_GAS;
pub const FEE_RECIPIENTS_ALLOWED_EVENT_GAS: u64 = LOG_GAS + LOG_TOPIC_GAS * 2;
pub const REWARD_ADDRESS_CHANGED_EVENT_GAS: u64 = LOG_GAS + LOG_TOPIC_GAS * 4 + READ_GAS;
pub const REWARDS_DISABLED_EVENT_GAS: u64 = LOG_GAS + LOG_TOPIC_GAS * 2;

/// common.Hash{'r','a','s','k'}.
pub fn reward_address_slot() -> U256 {
    U256::from(0x7261736bu64) << (28 * 8)
}

/// common.Hash{'a','f','r','a','v'}.
pub fn allow_fee_recipients_value() -> U256 {
    U256::from(0x6166726176u64) << (27 * 8)
}

/// constants.BlackholeAddr = 0x0100000000000000000000000000000000000000, as a word.
pub fn blackhole_value() -> U256 {
    U256::from(1u8) << (19 * 8)
}

struct Selectors {
    allow_fee_recipients: [u8; 4],
    are_fee_recipients_allowed: [u8; 4],
    current_reward_address: [u8; 4],
    disable_rewards: [u8; 4],
    set_reward_address: [u8; 4],
    ev_allowed: B256,
    ev_changed: B256,
    ev_disabled: B256,
}

fn sels() -> &'static Selectors {
    static S: OnceLock<Selectors> = OnceLock::new();
    S.get_or_init(|| Selectors {
        allow_fee_recipients: selector("allowFeeRecipients()"),
        are_fee_recipients_allowed: selector("areFeeRecipientsAllowed()"),
        current_reward_address: selector("currentRewardAddress()"),
        disable_rewards: selector("disableRewards()"),
        set_reward_address: selector("setRewardAddress(address)"),
        ev_allowed: event_sig("FeeRecipientsAllowed(address)"),
        ev_changed: event_sig("RewardAddressChanged(address,address,address)"),
        ev_disabled: event_sig("RewardsDisabled(address)"),
    })
}

/// GetStoredRewardAddress: (address, allowFeeRecipients).
fn stored<CTX: ContextTr>(ctx: &mut CTX) -> Result<(Address, bool), Halt> {
    let v = sload(ctx, REWARD_MANAGER, reward_address_slot())?;
    Ok((Address::from_slice(&v.to_be_bytes::<32>()[12..]), v == allow_fee_recipients_value()))
}

fn require_enabled<CTX: ContextTr>(ctx: &mut CTX, caller: Address, what: &str) -> Result<(), Halt> {
    let role = allowlist::get_role(ctx, REWARD_MANAGER, caller)?;
    if !allowlist::is_enabled(role) {
        return Err(Halt::Err(format!("non-enabled cannot call {what}: {caller}")));
    }
    Ok(())
}

pub fn call<CTX: ContextTr>(ctx: &mut CTX, env: &Env, input: &[u8], gas: &mut Gas, read_only: bool, caller: Address) -> Result<Bytes, Halt> {
    let (sel, args) = split_selector(input)?;
    if let Some(r) = allowlist::call(ctx, env, REWARD_MANAGER, sel, args, gas, read_only, caller) {
        return r;
    }
    let s = sels();
    if sel == s.allow_fee_recipients {
        deduct(gas, ALLOW_FEE_RECIPIENTS_GAS)?;
        if read_only {
            return Err(Halt::Err("write protection".into()));
        }
        require_enabled(ctx, caller, "allowFeeRecipients")?;
        if env.durango {
            deduct(gas, FEE_RECIPIENTS_ALLOWED_EVENT_GAS)?;
            add_log(ctx, REWARD_MANAGER, vec![s.ev_allowed, topic_addr(caller)], Vec::new());
        }
        sstore(ctx, REWARD_MANAGER, reward_address_slot(), allow_fee_recipients_value())?;
        return Ok(Bytes::new());
    }
    if sel == s.are_fee_recipients_allowed {
        deduct(gas, ARE_FEE_RECIPIENTS_ALLOWED_GAS)?;
        let (_, allowed) = stored(ctx)?;
        return Ok(Bytes::from(U256::from(allowed as u8).to_be_bytes::<32>()));
    }
    if sel == s.current_reward_address {
        deduct(gas, CURRENT_REWARD_ADDRESS_GAS)?;
        let (a, _) = stored(ctx)?;
        return Ok(Bytes::from(topic_addr(a).0));
    }
    if sel == s.disable_rewards {
        deduct(gas, DISABLE_REWARDS_GAS)?;
        if read_only {
            return Err(Halt::Err("write protection".into()));
        }
        require_enabled(ctx, caller, "disableRewards")?;
        if env.durango {
            deduct(gas, REWARDS_DISABLED_EVENT_GAS)?;
            add_log(ctx, REWARD_MANAGER, vec![s.ev_disabled, topic_addr(caller)], Vec::new());
        }
        sstore(ctx, REWARD_MANAGER, reward_address_slot(), blackhole_value())?;
        return Ok(Bytes::new());
    }
    if sel == s.set_reward_address {
        deduct(gas, SET_REWARD_ADDRESS_GAS)?;
        if read_only {
            return Err(Halt::Err("write protection".into()));
        }
        // Pre-Durango only the divisibility by 32 was enforced here.
        if !env.durango && args.len() % 32 != 0 {
            return Err(Halt::Err(format!("invalid input length for setting reward address: {}", args.len())));
        }
        let a = abi_address(args, 0, 1).map_err(Halt::Err)?;
        require_enabled(ctx, caller, "setRewardAddress")?;
        if a == Address::ZERO {
            return Err(Halt::Err("reward address cannot be empty".into()));
        }
        if env.durango {
            deduct(gas, REWARD_ADDRESS_CHANGED_EVENT_GAS)?;
            let (old, _) = stored(ctx)?;
            add_log(ctx, REWARD_MANAGER, vec![s.ev_changed, topic_addr(caller), topic_addr(old), topic_addr(a)], Vec::new());
        }
        sstore(ctx, REWARD_MANAGER, reward_address_slot(), U256::from_be_slice(a.as_slice()))?;
        return Ok(Bytes::new());
    }
    Err(invalid_selector(&sel))
}

/// module.Configure: the initial reward config (or the chain's
/// allowFeeRecipients, or rewards disabled), then the allow list.
pub fn configure(c: &PrecompileConfig, chain_allow_fee_recipients: bool) -> Vec<(U256, U256)> {
    let value = match &c.initial_reward {
        Some((allow, addr)) => {
            if *allow {
                allow_fee_recipients_value()
            } else if *addr == Address::ZERO {
                blackhole_value()
            } else {
                U256::from_be_slice(addr.as_slice())
            }
        }
        None if chain_allow_fee_recipients => allow_fee_recipients_value(),
        None => blackhole_value(),
    };
    let mut w = vec![(reward_address_slot(), value)];
    w.extend(allowlist::configure(c));
    w
}

//! The ContractNativeMinter stateful precompile
//! (precompile/contracts/nativeminter): mintNativeCoin(address,uint256) by an
//! enabled/admin/manager caller, the Durango NativeCoinMinted event, and the
//! initialMint at activation.

use crate::allowlist;
use crate::config::PrecompileConfig;
use crate::precompile::{
    abi_address, abi_u256, add_log, deduct, event_sig, invalid_selector, selector, split_selector, topic_addr, Env,
    Halt, LOG_DATA_GAS, LOG_GAS, LOG_TOPIC_GAS, NATIVE_MINTER,
};
use alloy_primitives::{Address, Bytes, B256, U256};
use revm::{
    context::{ContextTr, JournalTr},
    interpreter::Gas,
};
use std::sync::OnceLock;

pub const MINT_GAS: u64 = 30_000;
/// NativeCoinMintedEventGasCost: base + 3 topics + 32 data bytes.
pub const EVENT_GAS: u64 = LOG_GAS + LOG_TOPIC_GAS * 3 + LOG_DATA_GAS * 32;

struct Selectors {
    mint: [u8; 4],
    minted: B256,
}

fn sels() -> &'static Selectors {
    static S: OnceLock<Selectors> = OnceLock::new();
    S.get_or_init(|| Selectors { mint: selector("mintNativeCoin(address,uint256)"), minted: event_sig("NativeCoinMinted(address,address,uint256)") })
}

pub fn call<CTX: ContextTr>(ctx: &mut CTX, env: &Env, input: &[u8], gas: &mut Gas, read_only: bool, caller: Address) -> Result<Bytes, Halt> {
    let (sel, args) = split_selector(input)?;
    if let Some(r) = allowlist::call(ctx, env, NATIVE_MINTER, sel, args, gas, read_only, caller) {
        return r;
    }
    let s = sels();
    if sel != s.mint {
        return Err(invalid_selector(&sel));
    }
    deduct(gas, MINT_GAS)?;
    if read_only {
        return Err(Halt::Err("write protection".into()));
    }
    if !env.durango && args.len() != 64 {
        return Err(Halt::Err(format!("invalid input length for minting: {}", args.len())));
    }
    let to = abi_address(args, 0, 2).map_err(|e| Halt::Err(format!("failed to unpack input: {e}")))?;
    let amount = abi_u256(args, 1, 2).map_err(|e| Halt::Err(format!("failed to unpack input: {e}")))?;
    let caller_role = allowlist::get_role(ctx, NATIVE_MINTER, caller)?;
    if !allowlist::is_enabled(caller_role) {
        return Err(Halt::Err(format!("non-enabled cannot mint: {caller}")));
    }
    if env.durango {
        deduct(gas, EVENT_GAS)?;
        add_log(ctx, NATIVE_MINTER, vec![s.minted, topic_addr(caller), topic_addr(to)], amount.to_be_bytes::<32>().to_vec());
    }
    // CreateAccount if absent, then AddBalance: the journal's incr does both
    // (a zero add on an absent account leaves an empty touched account that
    // EIP-158 removes at the tx end, as in Go).
    ctx.journal_mut().balance_incr(to, amount).map_err(|_| Halt::Err("state write failed".into()))?;
    Ok(Bytes::new())
}

/// module.Configure: the initial mint (AddBalance per address), then the
/// allow list. Returned as (balance adds, role slots).
pub fn configure(c: &PrecompileConfig) -> (Vec<(Address, U256)>, Vec<(U256, U256)>) {
    (c.initial_mint.clone(), allowlist::configure(c))
}

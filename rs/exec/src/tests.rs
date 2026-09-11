//! Ports of the table-driven cases in subnet-evm's
//! precompile/allowlist/allowlisttest/test_allowlist.go and
//! precompile/contracts/*/contract_test.go: same input, same expected
//! output / gas / error class, over the revm journal instead of the StateDB.

use crate::allowlist::{self, ROLE_ADMIN, ROLE_ENABLED, ROLE_MANAGER, ROLE_NONE};
use crate::config::{Config, FeeConfig, PrecompileConfig};
use crate::precompile::{self, sload, sstore, Env, Halt, FEE_MANAGER, NATIVE_MINTER, REWARD_MANAGER, WARP};
use crate::{feemanager, nativeminter, rewardmanager, warp};
use alloy_primitives::{address, Address, Bytes, B256, U256};
use revm::{
    context::{Context, ContextTr, JournalTr},
    database::{CacheDB, EmptyDB},
    interpreter::Gas,
    MainContext,
};

const ADMIN: Address = address!("8db97C7cEcE249c2b98bDC0226Cc4C2A57BF52FC");
const ENABLED: Address = address!("0Fa8EA536Be85F32724D57A37758761B86416123");
const MANAGER: Address = address!("Ee7B4E4a08E1d0a1B26F1c37e69D3A4D3C6b1A7E");
const NOBODY: Address = address!("F60C45c607D0f41687c94C314d300f483661E13a");
const REWARD: Address = address!("0000000000000000000000000000000000000abc");

type Ctx = Context<revm::context::BlockEnv, revm::context::TxEnv, revm::context::CfgEnv, CacheDB<EmptyDB>>;

fn ctx() -> Ctx {
    Context::mainnet().with_db(CacheDB::new(EmptyDB::default()))
}

/// allowlisttest.SetDefaultRoles.
fn default_roles(c: &mut Ctx, precompile: Address) {
    c.journal_mut().load_account(precompile).unwrap();
    for (a, r) in [(ADMIN, ROLE_ADMIN), (ENABLED, ROLE_ENABLED), (MANAGER, ROLE_MANAGER)] {
        sstore(c, precompile, allowlist::role_slot(a), r).ok().unwrap();
    }
}

fn env(durango: bool) -> Env {
    Env { durango, granite: false, coreth: false, block_number: 7, network_id: 54321, blockchain_id: B256::repeat_byte(0xab), predicates: Vec::new(), failed: Vec::new() }
}

type ModuleFn = fn(&mut Ctx, &Env, &[u8], &mut Gas, bool, Address) -> Result<Bytes, Halt>;

fn allow_list_only(precompile: Address) -> impl Fn(&mut Ctx, &Env, &[u8], &mut Gas, bool, Address) -> Result<Bytes, Halt> {
    move |c, e, input, gas, ro, caller| {
        let (sel, args) = precompile::split_selector(input)?;
        allowlist::call(c, e, precompile, sel, args, gas, ro, caller).unwrap_or_else(|| Err(precompile::invalid_selector(&sel)))
    }
}

enum Want {
    Ok(Vec<u8>),
    Err(&'static str),
    Oog,
}

/// precompiletest.PrecompileTest.Run: the call, then the expected error class
/// or output and (on success) that the exact gas was consumed.
fn run(c: &mut Ctx, f: impl Fn(&mut Ctx, &Env, &[u8], &mut Gas, bool, Address) -> Result<Bytes, Halt>, e: &Env, caller: Address, input: &[u8], gas: u64, read_only: bool, want: Want) {
    c.journal_mut().load_account(FEE_MANAGER).unwrap();
    let mut g = Gas::new(gas);
    let r = f(c, e, input, &mut g, read_only, caller);
    match (want, r) {
        (Want::Ok(out), Ok(got)) => {
            assert_eq!(got.as_ref(), out.as_slice(), "output");
            assert_eq!(g.remaining(), 0, "exact gas");
        }
        (Want::Err(sub), Err(Halt::Err(msg))) => assert!(msg.contains(sub), "error {msg:?} does not contain {sub:?}"),
        (Want::Oog, Err(Halt::OutOfGas)) => {}
        (Want::Ok(_), Err(Halt::Err(m))) => panic!("expected success, got error {m}"),
        (Want::Ok(_), Err(Halt::OutOfGas)) => panic!("expected success, got out of gas"),
        (Want::Err(sub), Ok(o)) => panic!("expected error {sub:?}, got output {o}"),
        (Want::Err(sub), Err(Halt::OutOfGas)) => panic!("expected error {sub:?}, got out of gas"),
        (Want::Oog, other) => panic!("expected out of gas, got {:?}", other.map(|b| b.to_string()).map_err(|e| match e { Halt::Err(m) => m, Halt::OutOfGas => "oog".into() })),
    }
}

fn pack_addr(sel: &str, a: Address) -> Vec<u8> {
    let mut v = precompile::selector(sel).to_vec();
    v.extend_from_slice(precompile::topic_addr(a).as_slice());
    v
}

fn role_of(c: &mut Ctx, precompile: Address, a: Address) -> U256 {
    sload(c, precompile, allowlist::role_slot(a)).ok().unwrap()
}

fn setter(role: U256) -> &'static str {
    if role == ROLE_ADMIN {
        "setAdmin(address)"
    } else if role == ROLE_ENABLED {
        "setEnabled(address)"
    } else if role == ROLE_MANAGER {
        "setManager(address)"
    } else {
        "setNone(address)"
    }
}

// ---------------------------------------------------------------------------
// allowlist

#[test]
fn allowlist_role_transitions() {
    // (caller, target account, target role, durango, allowed)
    let cases: &[(Address, Address, U256, bool, bool)] = &[
        (ADMIN, NOBODY, ROLE_ADMIN, true, true),
        (ADMIN, NOBODY, ROLE_ENABLED, true, true),
        (ADMIN, ENABLED, ROLE_NONE, true, true),
        (ADMIN, NOBODY, ROLE_MANAGER, true, true),
        (NOBODY, NOBODY, ROLE_NONE, true, false),
        (NOBODY, NOBODY, ROLE_ENABLED, true, false),
        (NOBODY, NOBODY, ROLE_ADMIN, true, false),
        (NOBODY, NOBODY, ROLE_MANAGER, true, false),
        (ENABLED, NOBODY, ROLE_NONE, true, false),
        (ENABLED, NOBODY, ROLE_ENABLED, true, false),
        (ENABLED, NOBODY, ROLE_ADMIN, true, false),
        (ENABLED, NOBODY, ROLE_MANAGER, true, false),
        (MANAGER, NOBODY, ROLE_NONE, true, true),
        (MANAGER, NOBODY, ROLE_ENABLED, true, true),
        (MANAGER, NOBODY, ROLE_MANAGER, true, false),
        (MANAGER, NOBODY, ROLE_ADMIN, true, false),
        (MANAGER, ENABLED, ROLE_ADMIN, true, false),
        (MANAGER, ENABLED, ROLE_MANAGER, true, false),
        (MANAGER, ENABLED, ROLE_NONE, true, true),
        (MANAGER, ADMIN, ROLE_NONE, true, false),
        (MANAGER, ADMIN, ROLE_ENABLED, true, false),
        (MANAGER, ADMIN, ROLE_MANAGER, true, false),
        (MANAGER, MANAGER, ROLE_NONE, true, false),
        // pre-Durango
        (ADMIN, NOBODY, ROLE_ADMIN, false, true),
        (ADMIN, NOBODY, ROLE_ENABLED, false, true),
        (ADMIN, ENABLED, ROLE_NONE, false, true),
    ];
    for (caller, target, role, durango, allowed) in cases {
        let mut c = ctx();
        default_roles(&mut c, FEE_MANAGER);
        let e = env(*durango);
        let before = role_of(&mut c, FEE_MANAGER, *target);
        let gas = allowlist::MODIFY_GAS + if *durango { allowlist::EVENT_GAS } else { 0 };
        let want = if *allowed { Want::Ok(Vec::new()) } else { Want::Err("cannot modify allow list") };
        run(&mut c, allow_list_only(FEE_MANAGER), &e, *caller, &pack_addr(setter(*role), *target), gas, false, want);
        if *allowed {
            assert_eq!(role_of(&mut c, FEE_MANAGER, *target), *role, "{caller} sets {target} to {role}");
            let logs = c.journal().logs();
            if *durango {
                assert_eq!(logs.len(), 1);
                let l = &logs[0];
                assert_eq!(l.address, FEE_MANAGER);
                assert_eq!(l.topics(), &[precompile::event_sig("RoleSet(uint256,address,address,uint256)"), B256::from(*role), precompile::topic_addr(*target), precompile::topic_addr(*caller)]);
                assert_eq!(l.data.data.as_ref(), before.to_be_bytes::<32>());
            } else {
                assert!(logs.is_empty(), "no event before Durango");
            }
        } else {
            assert_eq!(role_of(&mut c, FEE_MANAGER, *target), before);
        }
    }
}

#[test]
fn allowlist_manager_pre_durango_and_errors() {
    // setManager is not activated before Durango, whoever calls.
    for caller in [NOBODY, ENABLED, ADMIN] {
        let mut c = ctx();
        default_roles(&mut c, FEE_MANAGER);
        run(&mut c, allow_list_only(FEE_MANAGER), &env(false), caller, &pack_addr("setManager(address)", NOBODY), allowlist::MODIFY_GAS, false, Want::Err("invalid non-activated function selector"));
    }
    // read only
    let mut c = ctx();
    default_roles(&mut c, FEE_MANAGER);
    run(&mut c, allow_list_only(FEE_MANAGER), &env(true), ADMIN, &pack_addr("setNone(address)", ENABLED), allowlist::MODIFY_GAS, true, Want::Err("write protection"));
    // insufficient gas
    run(&mut c, allow_list_only(FEE_MANAGER), &env(true), ADMIN, &pack_addr("setNone(address)", ENABLED), allowlist::MODIFY_GAS - 1, false, Want::Oog);
    // readAllowList by anyone, read only or not
    for (caller, target, role) in [(NOBODY, NOBODY, ROLE_NONE), (ADMIN, ADMIN, ROLE_ADMIN), (ENABLED, MANAGER, ROLE_MANAGER)] {
        run(&mut c, allow_list_only(FEE_MANAGER), &env(true), caller, &pack_addr("readAllowList(address)", target), allowlist::READ_ALLOW_LIST_GAS, true, Want::Ok(role.to_be_bytes::<32>().to_vec()));
    }
    run(&mut c, allow_list_only(FEE_MANAGER), &env(true), ADMIN, &pack_addr("readAllowList(address)", ADMIN), allowlist::READ_ALLOW_LIST_GAS - 1, false, Want::Oog);
    // strict input lengths before Durango, padded input accepted after
    let mut padded = pack_addr("setEnabled(address)", NOBODY);
    padded.extend_from_slice(&[0u8; 32]);
    run(&mut c, allow_list_only(FEE_MANAGER), &env(false), ADMIN, &padded, allowlist::MODIFY_GAS, false, Want::Err("invalid input length for modifying allow list"));
    run(&mut c, allow_list_only(FEE_MANAGER), &env(true), ADMIN, &padded, allowlist::MODIFY_GAS + allowlist::EVENT_GAS, false, Want::Ok(Vec::new()));
    // no selector, unknown selector
    run(&mut c, allow_list_only(FEE_MANAGER), &env(true), ADMIN, &[1, 2], 100_000, false, Want::Err("missing function selector"));
    run(&mut c, allow_list_only(FEE_MANAGER), &env(true), ADMIN, &[1, 2, 3, 4], 100_000, false, Want::Err("invalid function selector"));
}

#[test]
fn allowlist_initial_config() {
    let c = PrecompileConfig {
        key: "txAllowListConfig",
        address: precompile::TX_ALLOW_LIST,
        timestamp: 1,
        disable: false,
        admins: vec![ADMIN],
        enabled: vec![ENABLED],
        managers: vec![MANAGER],
        initial_fee_config: None,
        initial_mint: Vec::new(),
        initial_reward: None,
        quorum_numerator: 0,
        require_primary_network_signers: false,
    };
    let w = allowlist::configure(&c);
    assert_eq!(w, vec![(allowlist::role_slot(ENABLED), ROLE_ENABLED), (allowlist::role_slot(ADMIN), ROLE_ADMIN), (allowlist::role_slot(MANAGER), ROLE_MANAGER)]);
    assert_eq!(allowlist::role_slot(ADMIN), U256::from_be_slice(ADMIN.as_slice()));
}

// ---------------------------------------------------------------------------
// nativeminter

fn mint_input(to: Address, amount: U256) -> Vec<u8> {
    let mut v = precompile::selector("mintNativeCoin(address,uint256)").to_vec();
    v.extend_from_slice(precompile::topic_addr(to).as_slice());
    v.extend_from_slice(&amount.to_be_bytes::<32>());
    v
}

fn balance(c: &mut Ctx, a: Address) -> U256 {
    c.journal_mut().load_account(a).unwrap().info.balance
}

#[test]
fn nativeminter_cases() {
    let f: ModuleFn = nativeminter::call;
    let ev = precompile::event_sig("NativeCoinMinted(address,address,uint256)");
    // no role fails
    let mut c = ctx();
    default_roles(&mut c, NATIVE_MINTER);
    run(&mut c, f, &env(true), NOBODY, &mint_input(NOBODY, U256::from(1)), nativeminter::MINT_GAS, false, Want::Err("non-enabled cannot mint"));
    // enabled, manager, admin succeed and log
    for (caller, to) in [(ENABLED, ENABLED), (MANAGER, ENABLED), (ADMIN, ADMIN)] {
        let mut c = ctx();
        default_roles(&mut c, NATIVE_MINTER);
        run(&mut c, f, &env(true), caller, &mint_input(to, U256::from(1)), nativeminter::MINT_GAS + nativeminter::EVENT_GAS, false, Want::Ok(Vec::new()));
        assert_eq!(balance(&mut c, to), U256::from(1));
        let logs = c.journal().logs();
        assert_eq!(logs.len(), 1);
        assert_eq!(logs[0].topics(), &[ev, precompile::topic_addr(caller), precompile::topic_addr(to)]);
        assert_eq!(logs[0].data.data.as_ref(), U256::from(1).to_be_bytes::<32>());
    }
    // max amount
    let mut c = ctx();
    default_roles(&mut c, NATIVE_MINTER);
    run(&mut c, f, &env(true), ADMIN, &mint_input(ADMIN, U256::MAX), nativeminter::MINT_GAS + nativeminter::EVENT_GAS, false, Want::Ok(Vec::new()));
    assert_eq!(balance(&mut c, ADMIN), U256::MAX);
    // read only fails for every role
    for caller in [NOBODY, ENABLED, ADMIN] {
        run(&mut c, f, &env(true), caller, &mint_input(ADMIN, U256::from(1)), nativeminter::MINT_GAS, true, Want::Err("write protection"));
    }
    // insufficient gas (the event's gas is part of the cost after Durango)
    run(&mut c, f, &env(true), ADMIN, &mint_input(ENABLED, U256::from(1)), nativeminter::MINT_GAS + nativeminter::EVENT_GAS - 1, false, Want::Oog);
    // pre-Durango: no log, strict length
    let mut c = ctx();
    default_roles(&mut c, NATIVE_MINTER);
    run(&mut c, f, &env(false), ENABLED, &mint_input(ENABLED, U256::from(1)), nativeminter::MINT_GAS, false, Want::Ok(Vec::new()));
    assert!(c.journal().logs().is_empty());
    let mut padded = mint_input(ENABLED, U256::from(1));
    padded.extend_from_slice(&[0u8; 32]);
    run(&mut c, f, &env(false), ENABLED, &padded, nativeminter::MINT_GAS, false, Want::Err("invalid input length for minting"));
    run(&mut c, f, &env(true), ENABLED, &padded, nativeminter::MINT_GAS + nativeminter::EVENT_GAS, false, Want::Ok(Vec::new()));
    assert_eq!(balance(&mut c, ENABLED), U256::from(2));
    // the allow list functions are reachable through the module
    run(&mut c, f, &env(true), ADMIN, &pack_addr("readAllowList(address)", MANAGER), allowlist::READ_ALLOW_LIST_GAS, false, Want::Ok(ROLE_MANAGER.to_be_bytes::<32>().to_vec()));
    // initial mint
    let cfg = PrecompileConfig {
        key: "contractNativeMinterConfig",
        address: NATIVE_MINTER,
        timestamp: 1,
        disable: false,
        admins: vec![ADMIN],
        enabled: vec![],
        managers: vec![],
        initial_fee_config: None,
        initial_mint: vec![(ENABLED, U256::from(2))],
        initial_reward: None,
        quorum_numerator: 0,
        require_primary_network_signers: false,
    };
    let (mint, slots) = nativeminter::configure(&cfg);
    assert_eq!(mint, vec![(ENABLED, U256::from(2))]);
    assert_eq!(slots, vec![(allowlist::role_slot(ADMIN), ROLE_ADMIN)]);
}

// ---------------------------------------------------------------------------
// rewardmanager

fn sel(sig: &str) -> Vec<u8> {
    precompile::selector(sig).to_vec()
}

#[test]
fn rewardmanager_cases() {
    let f: ModuleFn = rewardmanager::call;
    let mut c = ctx();
    default_roles(&mut c, REWARD_MANAGER);
    // no role fails
    run(&mut c, f, &env(true), NOBODY, &sel("allowFeeRecipients()"), rewardmanager::ALLOW_FEE_RECIPIENTS_GAS, false, Want::Err("non-enabled cannot call allowFeeRecipients"));
    run(&mut c, f, &env(true), NOBODY, &pack_addr("setRewardAddress(address)", REWARD), rewardmanager::SET_REWARD_ADDRESS_GAS, false, Want::Err("non-enabled cannot call setRewardAddress"));
    run(&mut c, f, &env(true), NOBODY, &sel("disableRewards()"), rewardmanager::DISABLE_REWARDS_GAS, false, Want::Err("non-enabled cannot call disableRewards"));
    // allowFeeRecipients from enabled and manager, with the event
    for caller in [ENABLED, MANAGER] {
        let mut c = ctx();
        default_roles(&mut c, REWARD_MANAGER);
        run(&mut c, f, &env(true), caller, &sel("allowFeeRecipients()"), rewardmanager::ALLOW_FEE_RECIPIENTS_GAS + rewardmanager::FEE_RECIPIENTS_ALLOWED_EVENT_GAS, false, Want::Ok(Vec::new()));
        assert_eq!(sload(&mut c, REWARD_MANAGER, rewardmanager::reward_address_slot()).ok().unwrap(), rewardmanager::allow_fee_recipients_value());
        let logs = c.journal().logs();
        assert_eq!(logs.len(), 1);
        assert_eq!(logs[0].topics(), &[precompile::event_sig("FeeRecipientsAllowed(address)"), precompile::topic_addr(caller)]);
        assert!(logs[0].data.data.is_empty());
        run(&mut c, f, &env(true), NOBODY, &sel("areFeeRecipientsAllowed()"), rewardmanager::ARE_FEE_RECIPIENTS_ALLOWED_GAS, true, Want::Ok(U256::from(1).to_be_bytes::<32>().to_vec()));
    }
    // pre-Durango: no event
    let mut c = ctx();
    default_roles(&mut c, REWARD_MANAGER);
    run(&mut c, f, &env(false), ENABLED, &sel("allowFeeRecipients()"), rewardmanager::ALLOW_FEE_RECIPIENTS_GAS, false, Want::Ok(Vec::new()));
    assert!(c.journal().logs().is_empty());
    // setRewardAddress from enabled and manager
    for caller in [ENABLED, MANAGER] {
        let mut c = ctx();
        default_roles(&mut c, REWARD_MANAGER);
        run(&mut c, f, &env(true), caller, &pack_addr("setRewardAddress(address)", REWARD), rewardmanager::SET_REWARD_ADDRESS_GAS + rewardmanager::REWARD_ADDRESS_CHANGED_EVENT_GAS, false, Want::Ok(Vec::new()));
        let logs = c.journal().logs();
        assert_eq!(logs.len(), 1);
        assert_eq!(logs[0].topics(), &[precompile::event_sig("RewardAddressChanged(address,address,address)"), precompile::topic_addr(caller), precompile::topic_addr(Address::ZERO), precompile::topic_addr(REWARD)]);
        run(&mut c, f, &env(true), NOBODY, &sel("currentRewardAddress()"), rewardmanager::CURRENT_REWARD_ADDRESS_GAS, true, Want::Ok(precompile::topic_addr(REWARD).to_vec()));
        run(&mut c, f, &env(true), NOBODY, &sel("areFeeRecipientsAllowed()"), rewardmanager::ARE_FEE_RECIPIENTS_ALLOWED_GAS, true, Want::Ok(U256::ZERO.to_be_bytes::<32>().to_vec()));
    }
    // disableRewards from manager and enabled
    for caller in [MANAGER, ENABLED] {
        let mut c = ctx();
        default_roles(&mut c, REWARD_MANAGER);
        run(&mut c, f, &env(true), caller, &sel("disableRewards()"), rewardmanager::DISABLE_REWARDS_GAS + rewardmanager::REWARDS_DISABLED_EVENT_GAS, false, Want::Ok(Vec::new()));
        assert_eq!(sload(&mut c, REWARD_MANAGER, rewardmanager::reward_address_slot()).ok().unwrap(), rewardmanager::blackhole_value());
        let logs = c.journal().logs();
        assert_eq!(logs[0].topics(), &[precompile::event_sig("RewardsDisabled(address)"), precompile::topic_addr(caller)]);
        run(&mut c, f, &env(true), NOBODY, &sel("currentRewardAddress()"), rewardmanager::CURRENT_REWARD_ADDRESS_GAS, true, Want::Ok(precompile::topic_addr(address!("0100000000000000000000000000000000000000")).to_vec()));
    }
    // read only and gas
    let mut c = ctx();
    default_roles(&mut c, REWARD_MANAGER);
    run(&mut c, f, &env(true), ENABLED, &sel("allowFeeRecipients()"), rewardmanager::ALLOW_FEE_RECIPIENTS_GAS, true, Want::Err("write protection"));
    run(&mut c, f, &env(true), ENABLED, &pack_addr("setRewardAddress(address)", REWARD), rewardmanager::SET_REWARD_ADDRESS_GAS, true, Want::Err("write protection"));
    run(&mut c, f, &env(true), ENABLED, &pack_addr("setRewardAddress(address)", REWARD), rewardmanager::SET_REWARD_ADDRESS_GAS - 1, false, Want::Oog);
    run(&mut c, f, &env(true), ENABLED, &sel("allowFeeRecipients()"), rewardmanager::ALLOW_FEE_RECIPIENTS_GAS - 1, false, Want::Oog);
    run(&mut c, f, &env(true), ENABLED, &sel("currentRewardAddress()"), rewardmanager::CURRENT_REWARD_ADDRESS_GAS - 1, false, Want::Oog);
    run(&mut c, f, &env(true), ENABLED, &sel("areFeeRecipientsAllowed()"), rewardmanager::ARE_FEE_RECIPIENTS_ALLOWED_GAS - 1, false, Want::Oog);
    // empty reward address
    run(&mut c, f, &env(true), ENABLED, &pack_addr("setRewardAddress(address)", Address::ZERO), rewardmanager::SET_REWARD_ADDRESS_GAS, false, Want::Err("reward address cannot be empty"));
    // invalid length pre / post Durango (two extra bytes)
    let mut padded = pack_addr("setRewardAddress(address)", REWARD);
    padded.extend_from_slice(&[0, 0]);
    run(&mut c, f, &env(false), ENABLED, &padded, rewardmanager::SET_REWARD_ADDRESS_GAS, false, Want::Err("invalid input length for setting reward address"));
    run(&mut c, f, &env(true), ENABLED, &padded, rewardmanager::SET_REWARD_ADDRESS_GAS + rewardmanager::REWARD_ADDRESS_CHANGED_EVENT_GAS, false, Want::Ok(Vec::new()));
    // initial config
    let mut cfg = PrecompileConfig {
        key: "rewardManagerConfig",
        address: REWARD_MANAGER,
        timestamp: 1,
        disable: false,
        admins: vec![],
        enabled: vec![],
        managers: vec![],
        initial_fee_config: None,
        initial_mint: Vec::new(),
        initial_reward: Some((false, REWARD)),
        quorum_numerator: 0,
        require_primary_network_signers: false,
    };
    assert_eq!(rewardmanager::configure(&cfg, false)[0], (rewardmanager::reward_address_slot(), U256::from_be_slice(REWARD.as_slice())));
    cfg.initial_reward = Some((true, Address::ZERO));
    assert_eq!(rewardmanager::configure(&cfg, false)[0].1, rewardmanager::allow_fee_recipients_value());
    cfg.initial_reward = None;
    assert_eq!(rewardmanager::configure(&cfg, true)[0].1, rewardmanager::allow_fee_recipients_value());
    assert_eq!(rewardmanager::configure(&cfg, false)[0].1, rewardmanager::blackhole_value());
    assert_eq!(rewardmanager::reward_address_slot(), U256::from_be_slice(&{
        let mut b = [0u8; 32];
        b[..4].copy_from_slice(b"rask");
        b
    }));
    assert_eq!(rewardmanager::allow_fee_recipients_value(), U256::from_be_slice(&{
        let mut b = [0u8; 32];
        b[..5].copy_from_slice(b"afrav");
        b
    }));
}

// ---------------------------------------------------------------------------
// feemanager

fn test_fee_config() -> FeeConfig {
    FeeConfig {
        gas_limit: U256::from(8_000_000u64),
        target_block_rate: 2,
        min_base_fee: U256::from(25_000_000_000u64),
        target_gas: U256::from(15_000_000u64),
        base_fee_change_denominator: U256::from(36u64),
        min_block_gas_cost: U256::ZERO,
        max_block_gas_cost: U256::from(1_000_000u64),
        block_gas_cost_step: U256::from(200_000u64),
    }
}

fn set_fee_config_input(fc: &FeeConfig) -> Vec<u8> {
    let mut v = precompile::selector("setFeeConfig(uint256,uint256,uint256,uint256,uint256,uint256,uint256,uint256)").to_vec();
    for w in fc.words() {
        v.extend_from_slice(&w.to_be_bytes::<32>());
    }
    v
}

#[test]
fn feemanager_cases() {
    let f: ModuleFn = feemanager::call;
    let fc = test_fee_config();
    let mut c = ctx();
    default_roles(&mut c, FEE_MANAGER);
    run(&mut c, f, &env(true), NOBODY, &set_fee_config_input(&fc), feemanager::SET_FEE_CONFIG_GAS, false, Want::Err("non-enabled cannot change fee config"));
    for caller in [ENABLED, MANAGER, ADMIN] {
        let mut c = ctx();
        default_roles(&mut c, FEE_MANAGER);
        run(&mut c, f, &env(true), caller, &set_fee_config_input(&fc), feemanager::SET_FEE_CONFIG_GAS + feemanager::EVENT_GAS, false, Want::Ok(Vec::new()));
        let mut out = Vec::new();
        for w in fc.words() {
            out.extend_from_slice(&w.to_be_bytes::<32>());
        }
        run(&mut c, f, &env(true), NOBODY, &sel("getFeeConfig()"), feemanager::GET_FEE_CONFIG_GAS, true, Want::Ok(out.clone()));
        run(&mut c, f, &env(true), NOBODY, &sel("getFeeConfigLastChangedAt()"), feemanager::GET_LAST_CHANGED_GAS, true, Want::Ok(U256::from(7).to_be_bytes::<32>().to_vec()));
        let logs = c.journal().logs();
        assert_eq!(logs.len(), 1);
        assert_eq!(logs[0].topics()[1], precompile::topic_addr(caller));
        assert_eq!(logs[0].data.data.len(), 512);
        assert_eq!(&logs[0].data.data[..256], &[0u8; 256][..], "old config is zero");
        assert_eq!(&logs[0].data.data[256..], &out[..]);
    }
    // invalid config: minBlockGasCost > maxBlockGasCost
    let mut bad = fc.clone();
    bad.min_block_gas_cost = bad.max_block_gas_cost * U256::from(2);
    let mut c = ctx();
    default_roles(&mut c, FEE_MANAGER);
    run(&mut c, f, &env(true), ENABLED, &set_fee_config_input(&bad), feemanager::SET_FEE_CONFIG_GAS + feemanager::EVENT_GAS, false, Want::Err("minBlockGasCost cannot be greater than maxBlockGasCost"));
    // read only, gas
    run(&mut c, f, &env(true), ADMIN, &set_fee_config_input(&fc), feemanager::SET_FEE_CONFIG_GAS, true, Want::Err("write protection"));
    run(&mut c, f, &env(true), ADMIN, &set_fee_config_input(&fc), feemanager::SET_FEE_CONFIG_GAS - 1, false, Want::Oog);
    // padded input pre / post Durango
    let mut padded = set_fee_config_input(&fc);
    padded.extend_from_slice(&[0u8; 32]);
    run(&mut c, f, &env(false), ENABLED, &padded, feemanager::SET_FEE_CONFIG_GAS, false, Want::Err("invalid input length for fee config Input"));
    run(&mut c, f, &env(true), ENABLED, &padded, feemanager::SET_FEE_CONFIG_GAS + feemanager::EVENT_GAS, false, Want::Ok(Vec::new()));
    // no event pre-Durango
    let mut c = ctx();
    default_roles(&mut c, FEE_MANAGER);
    run(&mut c, f, &env(false), ENABLED, &set_fee_config_input(&fc), feemanager::SET_FEE_CONFIG_GAS, false, Want::Ok(Vec::new()));
    assert!(c.journal().logs().is_empty());
    // storage layout
    assert_eq!(feemanager::field_slot(1), U256::from_be_slice(&{
        let mut b = [0u8; 32];
        b[0] = 1;
        b
    }));
    assert_eq!(feemanager::last_changed_slot(), U256::from_be_slice(&{
        let mut b = [0u8; 32];
        b[..3].copy_from_slice(b"lca");
        b
    }));
    assert_eq!(feemanager::EVENT_GAS, 45_221);
    assert_eq!(allowlist::EVENT_GAS, 2_131);
    assert_eq!(nativeminter::EVENT_GAS, 1_756);
}

// ---------------------------------------------------------------------------
// warp

fn warp_env(predicates: Vec<Vec<B256>>, failed: Vec<u8>) -> Env {
    Env { durango: true, granite: false, coreth: false, block_number: 7, network_id: 54321, blockchain_id: B256::repeat_byte(0xab), predicates, failed }
}

#[test]
fn warp_send_and_get_blockchain_id() {
    let f: ModuleFn = warp::call;
    let mut c = ctx();
    let e = warp_env(Vec::new(), Vec::new());
    let g = warp::gas_config(false);
    run(&mut c, f, &e, NOBODY, &sel("getBlockchainID()"), g.get_blockchain_id, true, Want::Ok(B256::repeat_byte(0xab).to_vec()));
    run(&mut c, f, &e, NOBODY, &sel("getBlockchainID()"), g.get_blockchain_id - 1, false, Want::Oog);

    let payload = b"mcsorley".to_vec();
    let mut input = sel("sendWarpMessage(bytes)");
    input.extend_from_slice(&U256::from(0x20).to_be_bytes::<32>());
    precompile::pack_bytes(&mut input, &payload);
    let cost = g.send_base + g.per_message_byte * (input.len() as u64 - 4);
    run(&mut c, f, &e, ENABLED, &input, cost, true, Want::Err("write protection"));
    run(&mut c, f, &e, ENABLED, &input, g.send_base - 1, false, Want::Oog);
    run(&mut c, f, &e, ENABLED, &input, cost - 1, false, Want::Oog);
    run(&mut c, f, &e, ENABLED, &input[..4], g.send_base, false, Want::Err("invalid sendWarpMessage input"));
    let unsigned = warp::UnsignedMessage { network_id: 54321, source_chain_id: B256::repeat_byte(0xab), payload: warp::addressed_call_bytes(ENABLED.as_slice(), &payload) };
    run(&mut c, f, &e, ENABLED, &input, cost, false, Want::Ok(unsigned.id().to_vec()));
    let logs = c.journal().logs();
    assert_eq!(logs.len(), 1);
    assert_eq!(logs[0].address, WARP);
    assert_eq!(logs[0].topics(), &[precompile::event_sig("SendWarpMessage(address,bytes32,bytes)"), precompile::topic_addr(ENABLED), unsigned.id()]);
    let data = &logs[0].data.data;
    let msg = precompile::abi_bytes(data, 0, 1).unwrap();
    assert_eq!(msg, unsigned.bytes());
    // the byte layout of the unsigned message: version, networkID, chainID, len, payload
    let b = unsigned.bytes();
    assert_eq!(&b[..2], &[0, 0]);
    assert_eq!(&b[2..6], &54321u32.to_be_bytes());
    assert_eq!(&b[6..38], B256::repeat_byte(0xab).as_slice());
    assert_eq!(&b[38..42], &(unsigned.payload.len() as u32).to_be_bytes());
    match warp::parse_payload(&unsigned.payload).ok().unwrap() {
        warp::Payload::AddressedCall { source_address, payload: p } => {
            assert_eq!(source_address, ENABLED.as_slice());
            assert_eq!(p, payload);
        }
        _ => panic!("addressed call"),
    }
}

fn signed(unsigned: &warp::UnsignedMessage, signers: &[u8], sig: [u8; 96]) -> Vec<u8> {
    let mut b = unsigned.bytes();
    b.extend_from_slice(&0u32.to_be_bytes());
    b.extend_from_slice(&(signers.len() as u32).to_be_bytes());
    b.extend_from_slice(signers);
    b.extend_from_slice(&sig);
    b
}

fn get_input(sig: &str, index: u32) -> Vec<u8> {
    let mut v = sel(sig);
    v.extend_from_slice(&U256::from(index).to_be_bytes::<32>());
    v
}

#[test]
fn warp_get_verified_message() {
    let f: ModuleFn = warp::call;
    let g = warp::gas_config(false);
    let source_chain = B256::repeat_byte(0x11);
    let source_addr = address!("0000000000000000000000000000000000456789");
    let unsigned = warp::UnsignedMessage { network_id: 54321, source_chain_id: source_chain, payload: warp::addressed_call_bytes(source_addr.as_slice(), b"mcsorley") };
    let msg = signed(&unsigned, &[], [0u8; 96]);
    let pred = warp::predicate_chunks(&msg);
    assert_eq!(warp::predicate_bytes(&pred).unwrap(), msg, "predicate roundtrip");
    let want_ok = warp::pack_message_output(Some((source_chain, source_addr, b"mcsorley")));
    let want_invalid = warp::pack_message_output(None);
    let cost = g.get_verified_base + g.per_chunk * pred.len() as u64;

    let mut c = ctx();
    run(&mut c, f, &warp_env(vec![pred.clone()], Vec::new()), NOBODY, &get_input("getVerifiedWarpMessage(uint32)", 0), cost, false, Want::Ok(want_ok.clone()));
    // read only is fine
    run(&mut c, f, &warp_env(vec![pred.clone()], Vec::new()), NOBODY, &get_input("getVerifiedWarpMessage(uint32)", 0), cost, true, Want::Ok(want_ok.clone()));
    // out of bounds index: invalid output, only the base gas
    run(&mut c, f, &warp_env(vec![pred.clone()], Vec::new()), NOBODY, &get_input("getVerifiedWarpMessage(uint32)", 1), g.get_verified_base, false, Want::Ok(want_invalid.clone()));
    // non-zero index success and failure (index 1 failed = bit 1)
    run(&mut c, f, &warp_env(vec![pred.clone(), pred.clone()], Vec::new()), NOBODY, &get_input("getVerifiedWarpMessage(uint32)", 1), cost, false, Want::Ok(want_ok.clone()));
    run(&mut c, f, &warp_env(vec![pred.clone(), pred.clone()], vec![0b10]), NOBODY, &get_input("getVerifiedWarpMessage(uint32)", 1), g.get_verified_base, false, Want::Ok(want_invalid.clone()));
    run(&mut c, f, &warp_env(vec![pred.clone(), pred.clone()], vec![0b10]), NOBODY, &get_input("getVerifiedWarpMessage(uint32)", 0), cost, false, Want::Ok(want_ok.clone()));
    // no predicates at all
    run(&mut c, f, &warp_env(Vec::new(), Vec::new()), NOBODY, &get_input("getVerifiedWarpMessage(uint32)", 0), g.get_verified_base, true, Want::Ok(want_invalid.clone()));
    // gas
    run(&mut c, f, &warp_env(vec![pred.clone()], Vec::new()), NOBODY, &get_input("getVerifiedWarpMessage(uint32)", 0), g.get_verified_base - 1, false, Want::Oog);
    run(&mut c, f, &warp_env(vec![pred.clone()], Vec::new()), NOBODY, &get_input("getVerifiedWarpMessage(uint32)", 0), cost - 1, false, Want::Oog);
    // invalid predicate packing (an all-zero chunk), invalid warp message, invalid addressed payload
    let bad_pack = vec![B256::ZERO];
    run(&mut c, f, &warp_env(vec![bad_pack.clone()], Vec::new()), NOBODY, &get_input("getVerifiedWarpMessage(uint32)", 0), g.get_verified_base + g.per_chunk, false, Want::Err("cannot unpack predicate bytes"));
    let bad_msg = warp::predicate_chunks(&[1, 2, 3]);
    run(&mut c, f, &warp_env(vec![bad_msg.clone()], Vec::new()), NOBODY, &get_input("getVerifiedWarpMessage(uint32)", 0), g.get_verified_base + g.per_chunk * bad_msg.len() as u64, false, Want::Err("cannot unpack warp message"));
    let bad_payload = warp::predicate_chunks(&signed(&warp::UnsignedMessage { network_id: 54321, source_chain_id: source_chain, payload: vec![1, 2, 3] }, &[], [0u8; 96]));
    run(&mut c, f, &warp_env(vec![bad_payload.clone()], Vec::new()), NOBODY, &get_input("getVerifiedWarpMessage(uint32)", 0), g.get_verified_base + g.per_chunk * bad_payload.len() as u64, false, Want::Err("cannot unpack addressed payload"));
    // index errors: a uint64 word, MaxInt32 + 1, short input
    let mut big = sel("getVerifiedWarpMessage(uint32)");
    big.extend_from_slice(&U256::from(i64::MAX).to_be_bytes::<32>());
    run(&mut c, f, &warp_env(vec![pred.clone()], Vec::new()), NOBODY, &big, g.get_verified_base, false, Want::Err("invalid index to specify warp message"));
    run(&mut c, f, &warp_env(vec![pred.clone()], Vec::new()), NOBODY, &get_input("getVerifiedWarpMessage(uint32)", i32::MAX as u32 + 1), g.get_verified_base, false, Want::Err("larger than MaxInt32"));
    let short = get_input("getVerifiedWarpMessage(uint32)", 1);
    run(&mut c, f, &warp_env(vec![pred.clone()], Vec::new()), NOBODY, &short[..short.len() - 2], g.get_verified_base, false, Want::Err("invalid index to specify warp message"));

    // block hash variant
    let bh = B256::repeat_byte(0x22);
    let unsigned = warp::UnsignedMessage { network_id: 54321, source_chain_id: source_chain, payload: warp::hash_payload_bytes(bh) };
    let pred = warp::predicate_chunks(&signed(&unsigned, &[], [0u8; 96]));
    let cost = g.get_verified_base + g.per_chunk * pred.len() as u64;
    run(&mut c, f, &warp_env(vec![pred.clone()], Vec::new()), NOBODY, &get_input("getVerifiedWarpBlockHash(uint32)", 0), cost, false, Want::Ok(warp::pack_block_hash_output(Some((source_chain, bh)))));
    run(&mut c, f, &warp_env(vec![pred.clone()], vec![1]), NOBODY, &get_input("getVerifiedWarpBlockHash(uint32)", 0), g.get_verified_base, false, Want::Ok(warp::pack_block_hash_output(None)));
    run(&mut c, f, &warp_env(vec![bad_payload.clone()], Vec::new()), NOBODY, &get_input("getVerifiedWarpBlockHash(uint32)", 0), g.get_verified_base + g.per_chunk * bad_payload.len() as u64, false, Want::Err("cannot unpack block hash payload"));
    // the invalid outputs are what abi.PackOutput gives for a zero struct
    assert_eq!(want_invalid.len(), 6 * 32);
    assert_eq!(warp::pack_block_hash_output(None), vec![0u8; 96]);
}

#[test]
fn warp_predicate_codec_vectors() {
    // predicate.New / Bytes (predicate_test.go)
    assert_eq!(warp::predicate_chunks(&[]), vec![B256::right_padding_from(&[0xff])]);
    assert_eq!(warp::predicate_chunks(&[0x42]), vec![B256::right_padding_from(&[0x42, 0xff])]);
    assert_eq!(warp::predicate_chunks(&[0u8; 31]).len(), 1);
    assert_eq!(warp::predicate_chunks(&[0u8; 32]).len(), 2);
    for n in [0usize, 1, 31, 32, 33, 48, 63, 64, 65] {
        let b: Vec<u8> = (0..n).map(|i| (i as u8).wrapping_mul(7).wrapping_add(1)).collect();
        assert_eq!(warp::predicate_bytes(&warp::predicate_chunks(&b)).unwrap(), b);
    }
    assert!(warp::predicate_bytes(&[]).is_err());
    assert!(warp::predicate_bytes(&[B256::ZERO]).is_err());
    assert!(warp::predicate_bytes(&[B256::right_padding_from(&[0x42])]).is_err());
    assert!(warp::predicate_bytes(&[B256::right_padding_from(&[0xff]), B256::ZERO]).is_err());

    // predicate.BlockResults.Bytes (results_test.go single_tx_single_result)
    let mut b = vec![0u8, 0, 0, 0, 0, 1];
    let mut tx = [0u8; 32];
    tx[0] = 1;
    b.extend_from_slice(&tx);
    b.extend_from_slice(&[0, 0, 0, 1]);
    let mut addr = [0u8; 20];
    addr[0] = 2;
    b.extend_from_slice(&addr);
    b.extend_from_slice(&[0, 0, 0, 1, 0b00001110]);
    let r = warp::parse_block_results(&b).unwrap();
    let bits = &r[&B256::from(tx)][&Address::from(addr)];
    assert_eq!(bits, &[0b1110]);
    assert!(!warp::bits_contains(bits, 0) && warp::bits_contains(bits, 1) && warp::bits_contains(bits, 3) && !warp::bits_contains(bits, 4));
    assert_eq!(warp::bits_from_indices(&[1, 2, 3]), vec![0b1110]);
    assert_eq!(warp::bits_from_indices(&[]), Vec::<u8>::new());
    assert_eq!(warp::bits_from_indices(&[9]), vec![0b10, 0]);
    assert!(warp::parse_block_results(&[0, 0, 0, 0, 0, 0]).unwrap().is_empty());
    assert!(warp::parse_block_results(&[0, 0, 0, 0, 0, 0, 1]).is_err(), "trailing bytes");
    assert_eq!(warp::predicate_bytes_from_extra(&[0u8; 80]), &[] as &[u8]);
    assert_eq!(warp::predicate_bytes_from_extra(&[0u8; 86]).len(), 6);
}

#[test]
fn warp_predicate_gas() {
    let unsigned = warp::UnsignedMessage { network_id: 1, source_chain_id: B256::repeat_byte(1), payload: warp::addressed_call_bytes(&[1; 20], b"x") };
    let pred = warp::predicate_chunks(&signed(&unsigned, &[0b101], [0u8; 96]));
    let g = warp::gas_config(false);
    assert_eq!(warp::predicate_gas(&pred, false).unwrap(), g.verify_predicate_base + g.per_chunk * pred.len() as u64 + 2 * g.per_signer);
    let g = warp::gas_config(true);
    assert_eq!(warp::predicate_gas(&pred, true).unwrap(), g.verify_predicate_base + g.per_chunk * pred.len() as u64 + 2 * g.per_signer);
    // a leading zero byte in the signers bitset is not big.Int.Bytes()
    let pred = warp::predicate_chunks(&signed(&unsigned, &[0, 1], [0u8; 96]));
    assert!(warp::predicate_gas(&pred, false).unwrap_err().contains("bitset is invalid"));
    assert!(warp::predicate_gas(&warp::predicate_chunks(&[1, 2, 3]), false).unwrap_err().contains("cannot unpack warp message"));
}

struct FakeState(warp::WarpSet);

impl warp::ValidatorState for FakeState {
    fn subnet_id(&mut self, chain_id: B256) -> Result<B256, String> {
        Ok(if chain_id == B256::repeat_byte(0xc0) { warp::PRIMARY_NETWORK_ID } else { B256::repeat_byte(0x55) })
    }
    fn validator_set(&mut self, _h: u64, _subnet: B256) -> Result<Option<warp::WarpSet>, String> {
        Ok(Some(self.0.clone()))
    }
}

#[test]
fn warp_bls_verification() {
    use blst::min_pk::{AggregateSignature, SecretKey};
    let keys: Vec<SecretKey> = (0..4u8).map(|i| SecretKey::key_gen(&[i + 1; 32], &[]).unwrap()).collect();
    let unsigned = warp::UnsignedMessage { network_id: 1, source_chain_id: B256::repeat_byte(0xc0), payload: warp::addressed_call_bytes(&[1; 20], b"x") };
    let raw = unsigned.bytes();
    // weights 10, 20, 30, 40 (total 100), one node without a key adds 100 to the total
    let mut vdrs: Vec<(Option<Vec<u8>>, u64)> = keys.iter().enumerate().map(|(i, k)| (Some(k.sk_to_pk().compress().to_vec()), 10 * (i as u64 + 1))).collect();
    vdrs.push((None, 100));
    let set = warp::WarpSet::flatten(vdrs.clone()).unwrap();
    assert_eq!(set.total_weight, 200);
    assert_eq!(set.validators.len(), 4);
    assert!(set.validators.windows(2).all(|w| w[0].public_key < w[1].public_key), "sorted by key");
    // sign with the canonical indices of keys 2 and 3 (weights 30 + 40 = 70)
    let idx_of = |k: &SecretKey| set.validators.iter().position(|v| v.public_key == k.sk_to_pk().serialize().to_vec()).unwrap();
    let signers_keys = [&keys[2], &keys[3]];
    let sigs: Vec<_> = signers_keys.iter().map(|k| k.sign(&raw, warp::BLS_DST, &[])).collect();
    let agg = AggregateSignature::aggregate(&sigs.iter().collect::<Vec<_>>(), false).unwrap().to_signature().compress();
    let signers = warp::bits_from_indices(&[idx_of(&keys[2]), idx_of(&keys[3])]);
    let msg = warp::parse_message(&signed(&unsigned, &signers, agg)).unwrap();
    assert_eq!(msg.unsigned, unsigned);
    // 70 * 100 >= 200 * 33 passes at numerator 33, fails at 67
    assert!(warp::verify_signature(&msg, 1, &set, 33).is_ok());
    assert!(warp::verify_signature(&msg, 1, &set, 67).unwrap_err().contains("signature weight is insufficient"));
    assert!(warp::verify_signature(&msg, 2, &set, 33).unwrap_err().contains("wrong network ID"));
    // a wrong signer set for the signature (enough weight, wrong aggregate key)
    let wrong = warp::parse_message(&signed(&unsigned, &warp::bits_from_indices(&[idx_of(&keys[0]), idx_of(&keys[2]), idx_of(&keys[3])]), agg)).unwrap();
    assert!(warp::verify_signature(&wrong, 1, &set, 33).unwrap_err().contains("signature is invalid"));
    // an index past the set
    let far = warp::parse_message(&signed(&unsigned, &warp::bits_from_indices(&[4]), agg)).unwrap();
    assert!(warp::verify_signature(&far, 1, &set, 33).unwrap_err().contains("unknown validator"));
    // through verify_predicate: a primary-network source with requirePrimaryNetworkSigners=false
    // uses this chain's own subnet (the fake answers the same set either way)
    let pred = warp::predicate_chunks(&signed(&unsigned, &signers, agg));
    let mut vs = FakeState(set.clone());
    assert!(warp::verify_predicate(&mut vs, &pred, 1, B256::repeat_byte(0x55), 10, 33, false).is_ok());
    assert!(warp::verify_predicate(&mut vs, &pred, 1, B256::repeat_byte(0x55), 10, 0, false).unwrap_err().contains("cannot verify warp signature"));
}

// ---------------------------------------------------------------------------
// config: every fleet chain parses (EPOCHDB_V1_CONFIGS = the v1-configs dir)

#[test]
fn fleet_configs_parse() {
    let Ok(dir) = std::env::var("EPOCHDB_V1_CONFIGS") else {
        eprintln!("EPOCHDB_V1_CONFIGS unset, skipping");
        return;
    };
    use base64::Engine;
    let mut n = 0;
    let mut failures = Vec::new();
    for e in std::fs::read_dir(&dir).unwrap() {
        let p = e.unwrap().path();
        let Ok(chain) = std::fs::read(p.join("chain.json")) else { continue };
        let desc: serde_json::Value = serde_json::from_slice(&chain).unwrap();
        let genesis = base64::engine::general_purpose::STANDARD.decode(desc["genesisData"].as_str().unwrap()).unwrap();
        let upgrade = std::fs::read(p.join("upgrade.json")).unwrap_or_default();
        let network = desc["networkID"].as_u64().unwrap() as u32;
        match Config::from_genesis(&genesis, &upgrade, network) {
            Ok(c) => {
                n += 1;
                assert!(c.chain_id > 0);
                // every referenced module is one of the six
                for pc in c.genesis_precompiles.iter().chain(c.precompile_upgrades.iter()) {
                    assert!(precompile::module_index(pc.address).is_some(), "{}", pc.key);
                }
            }
            Err(err) => failures.push(format!("{}: {err}", p.file_name().unwrap().to_string_lossy())),
        }
    }
    eprintln!("{n} chains parsed; failures: {failures:?}");
    // pandasea's upgrade.json names txBlockListConfig, which this subnet-evm does not register either.
    assert!(failures.iter().all(|f| f.starts_with("pandasea:")), "{failures:?}");
}

#[test]
fn config_state_upgrades_and_forks() {
    let genesis = r#"{"config":{"chainId":1,"feeConfig":{"gasLimit":8000000,"minBaseFee":1,"targetGas":1,"baseFeeChangeDenominator":1,"minBlockGasCost":0,"maxBlockGasCost":1,"targetBlockRate":2,"blockGasCostStep":1},
        "etnaTimestamp":253399622400,"contractNativeMinterConfig":{"blockTimestamp":0,"adminAddresses":["0x8db97C7cEcE249c2b98bDC0226Cc4C2A57BF52FC"],"initialMint":{"0x0Fa8EA536Be85F32724D57A37758761B86416123":"0x10"}},
        "rewardManagerConfig":{"blockTimestamp":0,"initialRewardConfig":{"allowFeeRecipients":true}},"warpConfig":{"blockTimestamp":0,"quorumNumerator":67,"requirePrimaryNetworkSigners":true},
        "contractFeeManagerConfig":{"ignored":true}},"alloc":{},"timestamp":"0x0"}"#;
    let upgrade = r#"{"networkUpgradeOverrides":{"etnaTimestamp":1763398800},"stateUpgrades":[{"blockTimestamp":1679072400,"accounts":{"0x04b9da42306b023f3572e106b11d82aad9d32ebb":{"storage":{"0x0000000000000000000000000000000000000000000000000000000000000007":"0x000000000000000000000000000000000000000000cecb8f27f4200f3a000000"},"balanceChange":"0x1","code":"0x6001"}}}],
        "precompileUpgrades":[{"txAllowListConfig":{"blockTimestamp":1698760800,"adminAddresses":["0x8db97C7cEcE249c2b98bDC0226Cc4C2A57BF52FC"]}},{"txAllowListConfig":{"blockTimestamp":1698771600,"disable":true}}]}"#;
    let c = Config::from_genesis(genesis.as_bytes(), upgrade.as_bytes(), 1).unwrap();
    assert_eq!(c.etna, Some(1763398800), "override wins over the genesis value");
    assert_eq!(c.fortuna, Some(1746057600));
    assert_eq!(c.genesis_precompiles.len(), 3);
    assert_eq!(c.genesis_precompiles[0].initial_mint, vec![(ENABLED, U256::from(16))]);
    assert_eq!(c.genesis_precompiles[1].initial_reward, Some((true, Address::ZERO)));
    assert!(c.genesis_precompiles[2].require_primary_network_signers);
    assert_eq!(c.state_upgrades.len(), 1);
    let su = &c.state_upgrades[0];
    assert_eq!(su.timestamp, 1679072400);
    let a = &su.accounts[&address!("04b9da42306b023f3572e106b11d82aad9d32ebb")];
    assert_eq!(a.balance_change, Some(U256::from(1)));
    assert_eq!(a.code.as_ref(), &[0x60, 0x01]);
    assert_eq!(a.storage.len(), 1);
    assert_eq!(c.activating_state_upgrades(Some(1679072399), 1679072400).len(), 1);
    assert!(c.activating_state_upgrades(Some(1679072400), 1679072401).is_empty());
    assert!(c.precompile_enabled(precompile::TX_ALLOW_LIST, 1698760800));
    assert!(!c.precompile_enabled(precompile::TX_ALLOW_LIST, 1698771600));
    assert!(c.warp_config(0).is_some());
    assert!(Config::from_genesis(genesis.as_bytes(), br#"{"precompileUpgrades":[{"txBlockListConfig":{"blockTimestamp":1}}]}"#, 1).unwrap_err().to_string().contains("unknown precompile config"));
    assert_eq!(crate::config::cb58("2tmrrBo1Lgt1mzzvPSFt73kkQKFas5d1AP88tv9cicwoFp8BSn").unwrap().to_string(), "0xf94107902c8418dfcdf51d3f95429688abc7109e0f5b0e806c7e204d542e0761");
    assert_eq!(crate::rpc::cb58_encode(crate::config::cb58("2tmrrBo1Lgt1mzzvPSFt73kkQKFas5d1AP88tv9cicwoFp8BSn").unwrap()), "2tmrrBo1Lgt1mzzvPSFt73kkQKFas5d1AP88tv9cicwoFp8BSn");
    assert_eq!(crate::rpc::cb58_encode(B256::ZERO), "11111111111111111111111111111111LpoYY");
}

/// libevm never enters the EVM for a create the deployer allow list refuses
/// (evm.create returns before CaptureStart / CaptureEnter): the callTracer
/// keeps its zero frame, the struct logger an empty log that did not fail, the
/// prestate nothing; a refused CREATE at depth leaves no child frame. Module
/// failures carry the Go error text. Beam blocks 0x86 / 0x88 and 0x7f27.
#[test]
fn refused_create_and_module_error_frames() {
    use crate::exec::{CallMsg, Executor, Trace};
    use alloy_rpc_types_trace::geth::{CallConfig, GethDefaultTracingOptions, PreStateConfig};
    // FACTORY: PUSH1 0 PUSH1 0 PUSH1 0 CREATE STOP
    const FACTORY: Address = address!("00000000000000000000000000000000000000fa");
    let genesis = format!(
        r#"{{"config":{{"chainId":1,"feeConfig":{{"gasLimit":8000000,"minBaseFee":25000000000,"targetGas":15000000,"baseFeeChangeDenominator":36,"minBlockGasCost":0,"maxBlockGasCost":1000000,"targetBlockRate":2,"blockGasCostStep":200000}},
        "contractDeployerAllowListConfig":{{"blockTimestamp":0,"adminAddresses":["{ADMIN}"]}}}},
        "alloc":{{"{ADMIN}":{{"balance":"0x1000000000000000000"}},"{NOBODY}":{{"balance":"0x1000000000000000000"}},"{FACTORY}":{{"code":"0x6000600060006000f000"}}}},"timestamp":"0x0"}}"#
    );
    let cfg = Config::from_genesis(genesis.as_bytes(), b"{}", 1).unwrap();
    let mut ex = Executor::new(cfg).unwrap();
    let head = block::Header {
        parent_hash: B256::ZERO, uncle_hash: B256::ZERO, coinbase: Address::ZERO, root: B256::ZERO, tx_hash: B256::ZERO, receipt_hash: B256::ZERO,
        bloom: Default::default(), difficulty: U256::from(1), number: 1, gas_limit: 8_000_000, gas_used: 0, time: 1, extra: Bytes::new(),
        mix_digest: B256::ZERO, nonce: Default::default(), base_fee: Some(U256::from(25_000_000_000u64)), block_gas_cost: None, blob_gas_used: None,
        excess_blob_gas: None, parent_beacon_root: None, time_milliseconds: None, min_delay_excess: None,
    };
    let msg = |from: Address, to: Option<Address>, data: &str| CallMsg { from, to, gas: 100_000, gas_price: 0, value: U256::ZERO, data: data.parse().unwrap() };
    let create = msg(NOBODY, None, "0x60006000f3");

    ex.set_trace(Trace::Call(CallConfig::default()));
    let out = ex.call(&head, &create).unwrap();
    assert_eq!(out.gas_used, 100_000, "all gas consumed");
    assert_eq!(out.trace_json, r#"{"from":"0x0000000000000000000000000000000000000000","gas":"0x0","gasUsed":"0x186a0","input":"0x","type":"STOP"}"#);
    ex.set_trace(Trace::Struct(GethDefaultTracingOptions::default()));
    assert_eq!(ex.call(&head, &create).unwrap().trace_json, r#"{"failed":false,"gas":100000,"returnValue":"0x","structLogs":[]}"#);
    ex.set_trace(Trace::PreState(PreStateConfig::default()));
    assert_eq!(ex.call(&head, &create).unwrap().trace_json, "{}");
    ex.set_trace(Trace::PreState(PreStateConfig { diff_mode: Some(true), ..Default::default() }));
    assert_eq!(ex.call(&head, &create).unwrap().trace_json, r#"{"post":{},"pre":{}}"#);

    // The admin's create runs: a real CREATE frame.
    ex.set_trace(Trace::Call(CallConfig::default()));
    let ok = ex.call(&head, &msg(ADMIN, None, "0x60006000f3")).unwrap();
    assert!(ok.trace_json.contains(r#""type":"CREATE""#) && ok.halt.is_none(), "{}", ok.trace_json);

    // A refused CREATE at depth: the factory's frame has no child and no error.
    let nested = ex.call(&head, &msg(NOBODY, Some(FACTORY), "0x")).unwrap();
    let v: serde_json::Value = serde_json::from_str(&nested.trace_json).unwrap();
    assert!(v.get("calls").is_none() && v.get("error").is_none(), "{}", nested.trace_json);
    assert_eq!(v["type"], "CALL");
    let nested_ok = ex.call(&head, &msg(ADMIN, Some(FACTORY), "0x")).unwrap();
    let v: serde_json::Value = serde_json::from_str(&nested_ok.trace_json).unwrap();
    assert_eq!(v["calls"][0]["type"], "CREATE", "{}", nested_ok.trace_json);

    // setEnabled(NOBODY) by NOBODY on the allow list: libevm's err.Error() in the frame.
    let denied = ex.call(&head, &msg(NOBODY, Some(precompile::DEPLOYER_ALLOW_LIST), &format!("0x0aaf7043000000000000000000000000{}", precompile::hex(NOBODY.as_slice())))).unwrap();
    let v: serde_json::Value = serde_json::from_str(&denied.trace_json).unwrap();
    assert_eq!(v["error"], format!("cannot modify allow list: modify address: {NOBODY}, from role: NoRole, to role: EnabledRole"), "{}", denied.trace_json);
}

/// The two beam bugs the live sync found, on a long-lived Executor (the
/// plugin's): after `set_block_env` crosses into Granite the journal must
/// learn that 0x100 (P256Verify) is a precompile, so a STATICCALL to it costs
/// the warm 100, not the cold 2600 (beam 8,182,073 ran out of gas by exactly
/// that); and warp's getBlockchainID() answers the configured blockchain id
/// (beam 3,423,561 stored 32 zero bytes).
#[test]
fn granite_flip_warms_p256_and_get_blockchain_id_answers_the_chain() {
    use crate::exec::{CallMsg, Executor, Trace};
    // CALLER: STATICCALL(0xffff, 0x100, 0, 0, 0, 0) STOP
    const CALLER: Address = address!("00000000000000000000000000000000000000ca");
    let genesis = format!(
        r#"{{"config":{{"chainId":1,"feeConfig":{{"gasLimit":8000000,"minBaseFee":25000000000,"targetGas":15000000,"baseFeeChangeDenominator":36,"minBlockGasCost":0,"maxBlockGasCost":1000000,"targetBlockRate":2,"blockGasCostStep":200000}},
        "warpConfig":{{"blockTimestamp":0}}}},
        "alloc":{{"{NOBODY}":{{"balance":"0x1000000000000000000"}},"{CALLER}":{{"code":"0x600060006000600061010061fffffa00"}}}},"timestamp":"0x0"}}"#
    );
    let chain = B256::repeat_byte(0xc4);
    let cfg = Config::from_genesis(genesis.as_bytes(), b"{}", 1).unwrap().with_chain(chain, B256::repeat_byte(0x5b));
    let granite = cfg.granite.unwrap();
    assert!(cfg.is_etna(granite - 1), "the eth spec must not change at the flip");
    let head = |time: u64| block::Header {
        parent_hash: B256::ZERO, uncle_hash: B256::ZERO, coinbase: Address::ZERO, root: B256::ZERO, tx_hash: B256::ZERO, receipt_hash: B256::ZERO,
        bloom: Default::default(), difficulty: U256::from(1), number: 1, gas_limit: 8_000_000, gas_used: 0, time, extra: Bytes::new(),
        mix_digest: B256::ZERO, nonce: Default::default(), base_fee: Some(U256::from(25_000_000_000u64)), block_gas_cost: None, blob_gas_used: None,
        excess_blob_gas: None, parent_beacon_root: None, time_milliseconds: None, min_delay_excess: None,
    };
    let msg = |to: Address, data: Vec<u8>| CallMsg { from: NOBODY, to: Some(to), gas: 100_000, gas_price: 0, value: U256::ZERO, data: data.into() };

    // A fresh executor that starts under Granite: the oracle.
    let mut fresh = Executor::new(cfg.clone()).unwrap();
    fresh.set_trace(Trace::Off);
    let warm = fresh.call(&head(granite), &msg(CALLER, vec![])).unwrap().gas_used;
    // 21000 + 6 pushes + STATICCALL 100 (warm) + P256Verify 6900.
    assert_eq!(warm, 21_000 + 6 * 3 + 100 + 6_900, "fresh executor under Granite");

    // The long-lived one: a block before the flip (0x100 is an empty account,
    // cold 2600), then a block under it.
    let mut ex = Executor::new(cfg.clone()).unwrap();
    ex.set_trace(Trace::Off);
    let cold = ex.call(&head(granite - 1), &msg(CALLER, vec![])).unwrap().gas_used;
    assert_eq!(cold, 21_000 + 6 * 3 + 2_600, "before Granite 0x100 is a cold empty account");
    let after = ex.call(&head(granite), &msg(CALLER, vec![])).unwrap().gas_used;
    assert_eq!(after, warm, "after the flip the long-lived executor must charge the warm precompile access");

    // getBlockchainID() through the executor answers the configured id.
    let out = ex.call(&head(granite), &msg(WARP, sel("getBlockchainID()"))).unwrap();
    assert!(out.halt.is_none() && !out.revert, "halt {:?} revert {}", out.halt, out.revert);
    assert_eq!(out.output.as_ref(), chain.as_slice());
    let zero = Executor::new(Config::from_genesis(genesis.as_bytes(), b"{}", 1).unwrap()).unwrap().call(&head(granite), &msg(WARP, sel("getBlockchainID()"))).unwrap();
    assert_eq!(zero.output.as_ref(), B256::ZERO.as_slice(), "without with_chain the answer is zeros: what the plugin used to run with");
}

/// A hand-built transfer for the build tests: the sender is given (no
/// signature needed), `raw` only matters for the size rule and the hash.
fn fake_tx(sender: Address, nonce: u64, gas_limit: u64, gas_price: u128, to: Address, value: U256, raw_len: usize) -> block::Tx {
    let mut raw = vec![0xf8u8; raw_len.max(8)];
    raw[1..8].copy_from_slice(&nonce.to_be_bytes()[1..]);
    raw[0] = sender.0[0];
    let raw = ::bytes::Bytes::from(raw);
    block::Tx { hash: alloy_primitives::keccak256(&raw), raw, sender: Some(sender), tx_type: 0, chain_id: Some(1), nonce, gas_price, gas_tip: gas_price, gas_limit, to: Some(to), value, input: ::bytes::Bytes::new(), access_list: Vec::new(), v: 37, r: U256::ZERO, s: U256::ZERO, recid: 0, body_off: 0, sig_off: 0 }
}

fn build_header(gas_limit: u64) -> block::Header {
    block::Header {
        parent_hash: B256::ZERO, uncle_hash: B256::ZERO, coinbase: REWARD, root: B256::ZERO, tx_hash: B256::ZERO, receipt_hash: B256::ZERO,
        bloom: Default::default(), difficulty: U256::from(1), number: 1, gas_limit, gas_used: 0, time: 10, extra: Bytes::from(vec![0u8; 80]),
        mix_digest: B256::ZERO, nonce: Default::default(), base_fee: Some(U256::from(25_000_000_000u64)), block_gas_cost: Some(U256::ZERO), blob_gas_used: None,
        excess_blob_gas: None, parent_beacon_root: None, time_milliseconds: None, min_delay_excess: None,
    }
}

/// miner.commitTransactions over a candidate list: nonce too low is
/// skipped (Shift), any other failure pops the sender (its later txs are
/// skipped), a tx over the gas left or the size target pops the sender, the
/// loop stops under 21,000 gas; what is included applied in order and the
/// receipts root / gasUsed follow.
#[test]
fn build_block_skip_and_pop_rules() {
    use crate::exec::{Executor, SkipReason::*};
    let genesis = format!(
        r#"{{"config":{{"chainId":1,"feeConfig":{{"gasLimit":8000000,"minBaseFee":25000000000,"targetGas":15000000,"baseFeeChangeDenominator":36,"minBlockGasCost":0,"maxBlockGasCost":1000000,"targetBlockRate":2,"blockGasCostStep":200000}}}},
        "alloc":{{"{ADMIN}":{{"balance":"0x1000000000000000000"}},"{ENABLED}":{{"balance":"0x1000000000000000000"}},"{MANAGER}":{{"balance":"0x1000000000000000000"}},"{NOBODY}":{{"balance":"0x0"}}}},"timestamp":"0x0"}}"#
    );
    let cfg = Config::from_genesis(genesis.as_bytes(), b"{}", 1).unwrap();
    let mut ex = Executor::new(cfg.clone()).unwrap();
    let price = 30_000_000_000u128;
    let one = U256::from(1u64);
    let cands = vec![
        fake_tx(ADMIN, 0, 21_000, price, REWARD, one, 100),   // 0 included
        fake_tx(ADMIN, 0, 21_000, price, REWARD, one, 100),   // 1 nonce too low: skipped, ADMIN stays
        fake_tx(ADMIN, 1, 21_000, price, REWARD, one, 100),   // 2 included
        fake_tx(ENABLED, 5, 21_000, price, REWARD, one, 100), // 3 nonce too high: popped
        fake_tx(ENABLED, 0, 21_000, price, REWARD, one, 100), // 4 sender popped
        fake_tx(NOBODY, 0, 21_000, price, REWARD, one, 100),  // 5 insufficient funds: popped
        fake_tx(MANAGER, 0, 21_000, price, REWARD, one, 100), // 6 included
        fake_tx(ADMIN, 2, 21_000, 1, REWARD, one, 100),       // 7 fee cap below base fee: popped
        fake_tx(ADMIN, 3, 21_000, price, REWARD, one, 100),   // 8 sender popped
        fake_tx(MANAGER, 1, 21_000, price, REWARD, one, 100), // 9 included
    ];
    let h = build_header(8_000_000);
    let r = ex.build_block(&h, 0, None, None, &cands).unwrap();
    assert_eq!(r.included, vec![0, 2, 6, 9]);
    assert_eq!(r.reasons, vec![Included, NonceTooLow, Included, Invalid, SenderPopped, Invalid, Included, Invalid, SenderPopped, Included]);
    assert_eq!(r.result.gas_used, 4 * 21_000);
    assert_eq!(r.result.txs.len(), 4);
    assert_eq!(r.result.txs[3].cumulative_gas_used, 84_000);
    assert!(r.predicate_bytes.is_empty(), "pre-Durango: no predicate results");
    use revm::Database as _;
    assert_eq!(ex.db_mut().basic(ADMIN).unwrap().unwrap().nonce, 2);
    assert_eq!(ex.db_mut().basic(REWARD).unwrap().unwrap().balance, U256::from(4u64) + U256::from(4 * 21_000) * U256::from(price));

    // The gas pool: a 50,000-gas tx when 40,000 are left pops its sender; the
    // loop stops once under 21,000 remain, the rest is not reached.
    let mut ex = Executor::new(cfg.clone()).unwrap();
    let cands = vec![
        fake_tx(ADMIN, 0, 21_000, price, REWARD, one, 100),
        fake_tx(ADMIN, 1, 21_000, price, REWARD, one, 100),
        fake_tx(ENABLED, 0, 50_000, price, REWARD, one, 100), // 40,000 left: popped
        fake_tx(ENABLED, 1, 21_000, price, REWARD, one, 100), // sender popped
        fake_tx(MANAGER, 0, 21_000, price, REWARD, one, 100), // included (19,000 left)
        fake_tx(MANAGER, 1, 21_000, price, REWARD, one, 100), // not reached
    ];
    let r = ex.build_block(&build_header(82_000), 0, None, None, &cands).unwrap();
    assert_eq!(r.reasons, vec![Included, Included, NoGas, SenderPopped, Included, NotReached]);
    assert_eq!(r.result.gas_used, 63_000);

    // The 1800 KiB size target: a tx that would pass it pops its sender.
    let mut ex = Executor::new(cfg.clone()).unwrap();
    let cands = vec![
        fake_tx(ADMIN, 0, 21_000, price, REWARD, one, 1800 * 1024 - 50),
        fake_tx(ENABLED, 0, 21_000, price, REWARD, one, 100), // over the target: popped
        fake_tx(MANAGER, 0, 21_000, price, REWARD, one, 40),  // fits
    ];
    let r = ex.build_block(&build_header(8_000_000), 0, None, None, &cands).unwrap();
    assert_eq!(r.reasons, vec![Included, Size, Included]);

    // Durango: the predicate results bytes are the empty codec map.
    let mut d = Config::from_genesis(genesis.as_bytes(), b"{}", 1).unwrap();
    d.durango = Some(5);
    let mut ex = Executor::new(d).unwrap();
    let r = ex.build_block(&build_header(8_000_000), 0, None, None, &[fake_tx(ADMIN, 0, 21_000, price, REWARD, one, 100)]).unwrap();
    assert_eq!(r.predicate_bytes, vec![0, 0, 0, 0, 0, 0]);
    // encode_block_results sorts by tx hash then address and round-trips through the parser.
    let mut br = crate::warp::BlockResults::default();
    br.insert(B256::repeat_byte(2), [(crate::precompile::WARP, vec![1u8])].into_iter().collect());
    br.insert(B256::repeat_byte(1), [(crate::precompile::WARP, vec![])].into_iter().collect());
    let enc = crate::warp::encode_block_results(&br);
    assert_eq!(&enc[..6], &[0, 0, 0, 0, 0, 2]);
    assert_eq!(&enc[6..38], B256::repeat_byte(1).as_slice());
    assert_eq!(crate::warp::parse_block_results(&enc).unwrap(), br);
}

/// The deferred callTracer render equals the synchronous one.
#[test]
fn deferred_call_trace_renders_the_same_json() {
    use crate::exec::Executor;
    let genesis = format!(
        r#"{{"config":{{"chainId":1,"feeConfig":{{"gasLimit":8000000,"minBaseFee":25000000000,"targetGas":15000000,"baseFeeChangeDenominator":36,"minBlockGasCost":0,"maxBlockGasCost":1000000,"targetBlockRate":2,"blockGasCostStep":200000}}}},
        "alloc":{{"{ADMIN}":{{"balance":"0x1000000000000000000"}}}},"timestamp":"0x0"}}"#
    );
    let cfg = Config::from_genesis(genesis.as_bytes(), b"{}", 1).unwrap();
    let mut h = build_header(8_000_000);
    h.gas_used = 42_000;
    let txs = vec![fake_tx(ADMIN, 0, 21_000, 30_000_000_000, REWARD, U256::from(1u64), 100), fake_tx(ADMIN, 1, 21_000, 30_000_000_000, NOBODY, U256::from(2u64), 100)];
    let b = block::Block { height: 1, hash: B256::repeat_byte(9), container_id: B256::ZERO, header: h, header_rlp: ::bytes::Bytes::new(), txs, container: ::bytes::Bytes::new(), pvm: None };
    let mut sync = Executor::new(cfg.clone()).unwrap();
    let r1 = sync.execute_block(&b, 0).unwrap();
    let mut lazy = Executor::new(cfg).unwrap();
    lazy.defer_call_trace = true;
    let mut r2 = lazy.execute_block(&b, 0).unwrap();
    assert!(r2.txs.iter().all(|t| t.trace_json.is_empty() && t.deferred.is_some()));
    crate::exec::render_deferred(&mut r2).unwrap();
    for (a, c) in r1.txs.iter().zip(&r2.txs) {
        assert_eq!(a.trace_json, c.trace_json);
        assert!(c.deferred.is_none());
    }
    assert!(r1.txs[0].trace_json.contains(r#""type":"CALL""#));
}

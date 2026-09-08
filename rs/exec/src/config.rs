//! Chain configuration: genesis JSON `config` + upgrade.json + the network's
//! upgrade schedule. Mirrors vmexec/genesis.go parseGenesis and subnet-evm
//! params/extras (SetDefaults, Override, GetActivatingPrecompileConfigs).

use alloy_primitives::{keccak256, Address, Bytes, B256, U256};
use anyhow::{anyhow, bail, Context, Result};
use revm::primitives::hardfork::SpecId;
use serde_json::Value;
use std::collections::BTreeMap;

/// subnet-evm commontype.FeeConfig.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeeConfig {
    pub gas_limit: U256,
    pub target_block_rate: u64,
    pub min_base_fee: U256,
    pub target_gas: U256,
    pub base_fee_change_denominator: U256,
    pub min_block_gas_cost: U256,
    pub max_block_gas_cost: U256,
    pub block_gas_cost_step: U256,
}

impl FeeConfig {
    fn from_json(v: &Value) -> Result<FeeConfig> {
        let f = |k: &str| -> Result<U256> {
            let x = v.get(k).ok_or_else(|| anyhow!("feeConfig.{k} missing"))?;
            json_u256(x).with_context(|| format!("feeConfig.{k}"))
        };
        Ok(FeeConfig {
            gas_limit: f("gasLimit")?,
            target_block_rate: f("targetBlockRate")?.to::<u64>(),
            min_base_fee: f("minBaseFee")?,
            target_gas: f("targetGas")?,
            base_fee_change_denominator: f("baseFeeChangeDenominator")?,
            min_block_gas_cost: f("minBlockGasCost")?,
            max_block_gas_cost: f("maxBlockGasCost")?,
            block_gas_cost_step: f("blockGasCostStep")?,
        })
    }

    /// commontype.FeeConfig.Verify (the checks setFeeConfig applies).
    pub fn verify(&self) -> Result<()> {
        if self.gas_limit.is_zero() {
            bail!("gasLimit cannot be less than or equal to 0");
        }
        if self.target_block_rate == 0 {
            bail!("targetBlockRate cannot be less than or equal to 0");
        }
        if self.target_gas.is_zero() {
            bail!("targetGas cannot be less than or equal to 0");
        }
        if self.base_fee_change_denominator.is_zero() {
            bail!("baseFeeChangeDenominator cannot be less than or equal to 0");
        }
        if self.min_block_gas_cost > self.max_block_gas_cost {
            bail!("minBlockGasCost cannot be greater than maxBlockGasCost");
        }
        if self.max_block_gas_cost > U256::from(u64::MAX) {
            bail!("maxBlockGasCost is not a valid uint64");
        }
        Ok(())
    }

    /// Storage words in feemanager key order 1..=8.
    pub fn words(&self) -> [U256; 8] {
        [
            self.gas_limit,
            U256::from(self.target_block_rate),
            self.min_base_fee,
            self.target_gas,
            self.base_fee_change_denominator,
            self.min_block_gas_cost,
            self.max_block_gas_cost,
            self.block_gas_cost_step,
        ]
    }

    pub fn from_words(w: &[U256; 8]) -> FeeConfig {
        FeeConfig {
            gas_limit: w[0],
            target_block_rate: w[1].to::<u64>(),
            min_base_fee: w[2],
            target_gas: w[3],
            base_fee_change_denominator: w[4],
            min_block_gas_cost: w[5],
            max_block_gas_cost: w[6],
            block_gas_cost_step: w[7],
        }
    }
}

/// One precompile config (genesis or upgrade.json entry). Only feeManagerConfig
/// is modelled; every other module key is a hard error at parse time.
#[derive(Debug, Clone)]
pub struct PrecompileConfig {
    pub key: &'static str,
    pub address: Address,
    pub timestamp: u64,
    pub disable: bool,
    pub admins: Vec<Address>,
    pub enabled: Vec<Address>,
    pub managers: Vec<Address>,
    pub initial_fee_config: Option<FeeConfig>,
}

pub const FEE_MANAGER: Address = Address::new([
    0x02, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x03,
]);

const MODULE_KEYS: &[(&str, [u8; 20])] = &[
    ("contractDeployerAllowListConfig", [0x02, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x00]),
    ("contractNativeMinterConfig", [0x02, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x01]),
    ("txAllowListConfig", [0x02, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x02]),
    ("feeManagerConfig", [0x02, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x03]),
    ("rewardManagerConfig", [0x02, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x04]),
    ("warpConfig", [0x02, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x05]),
];

#[derive(Debug, Clone, Default)]
pub struct GenesisAccount {
    pub balance: U256,
    pub nonce: u64,
    pub code: Bytes,
    pub storage: BTreeMap<B256, B256>,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub chain_id: u64,
    pub fee_config: FeeConfig,
    pub allow_fee_recipients: bool,
    /// Timestamp forks (subnetEVM is always 0 on the networks we run).
    pub subnet_evm: u64,
    pub durango: Option<u64>,
    pub etna: Option<u64>,
    pub granite: Option<u64>,
    /// Genesis precompile configs, in module address order.
    pub genesis_precompiles: Vec<PrecompileConfig>,
    /// upgrade.json precompileUpgrades, in file order.
    pub precompile_upgrades: Vec<PrecompileConfig>,
    pub alloc: BTreeMap<Address, GenesisAccount>,
    pub genesis_timestamp: u64,
}

/// avalanchego upgrade/upgrade.go: Mainnet and Fuji schedules (Unix seconds).
/// Any other network id is the Default config: everything initially active.
fn network_upgrades(network_id: u32) -> (Option<u64>, Option<u64>, Option<u64>) {
    match network_id {
        1 => (Some(1709740800), Some(1734368400), Some(1763568000)),
        5 => (Some(1707840000), Some(1732550400), Some(1761750000)),
        _ => (Some(1607144400), Some(1607144400), Some(1607144400)),
    }
}

impl Config {
    pub fn from_genesis(genesis_json: &[u8], upgrade_json: &[u8], network_id: u32) -> Result<Config> {
        let g: Value = serde_json::from_slice(genesis_json).context("genesis json")?;
        let cfg = g.get("config").ok_or_else(|| anyhow!("genesis has no config"))?;
        let chain_id = cfg.get("chainId").and_then(Value::as_u64).ok_or_else(|| anyhow!("config.chainId"))?;
        // Every block-number fork must be 0 (SetEthUpgrades fills the missing ones with 0).
        for k in [
            "homesteadBlock", "eip150Block", "eip155Block", "eip158Block", "byzantiumBlock",
            "constantinopleBlock", "petersburgBlock", "istanbulBlock", "muirGlacierBlock",
            "berlinBlock", "londonBlock",
        ] {
            if let Some(v) = cfg.get(k) {
                if v.as_u64() != Some(0) {
                    bail!("config.{k} = {v}: only block-number forks at 0 are supported");
                }
            }
        }
        let fee_config = match cfg.get("feeConfig") {
            Some(v) if v.is_object() && !v.as_object().unwrap().is_empty() => FeeConfig::from_json(v)?,
            _ => FeeConfig {
                // extras.DefaultFeeConfig
                gas_limit: U256::from(8_000_000u64),
                target_block_rate: 2,
                min_base_fee: U256::from(25_000_000_000u64),
                target_gas: U256::from(15_000_000u64),
                base_fee_change_denominator: U256::from(36u64),
                min_block_gas_cost: U256::ZERO,
                max_block_gas_cost: U256::from(1_000_000u64),
                block_gas_cost_step: U256::from(200_000u64),
            },
        };
        let allow_fee_recipients = cfg.get("allowFeeRecipients").and_then(Value::as_bool).unwrap_or(false);

        // NetworkUpgrades.SetDefaults: nil or 0 in the genesis means the network default.
        let (d_durango, d_etna, d_granite) = network_upgrades(network_id);
        let pick = |key: &str, default: Option<u64>| -> Option<u64> {
            match cfg.get(key).and_then(Value::as_u64) {
                None | Some(0) => default,
                Some(t) => Some(t),
            }
        };
        let subnet_evm = cfg.get("subnetEVMTimestamp").and_then(Value::as_u64).unwrap_or(0);
        let mut durango = pick("durangoTimestamp", d_durango);
        let mut etna = pick("etnaTimestamp", d_etna);
        let mut granite = pick("graniteTimestamp", d_granite);

        let mut genesis_precompiles = Vec::new();
        for (key, addr) in MODULE_KEYS {
            if let Some(v) = cfg.get(*key) {
                genesis_precompiles.push(parse_precompile(key, Address::new(*addr), v)?);
            }
        }

        let mut precompile_upgrades = Vec::new();
        if !upgrade_json.is_empty() {
            let u: Value = serde_json::from_slice(upgrade_json).context("upgrade json")?;
            if let Some(o) = u.get("networkUpgradeOverrides") {
                // NetworkUpgrades.Override: set fields present (non-null).
                if let Some(t) = o.get("durangoTimestamp").and_then(Value::as_u64) { durango = Some(t); }
                if let Some(t) = o.get("etnaTimestamp").and_then(Value::as_u64) { etna = Some(t); }
                if let Some(t) = o.get("graniteTimestamp").and_then(Value::as_u64) { granite = Some(t); }
            }
            if u.get("stateUpgrades").map(|s| !s.as_array().map_or(true, Vec::is_empty)).unwrap_or(false) {
                bail!("upgrade.json stateUpgrades are not implemented");
            }
            if let Some(arr) = u.get("precompileUpgrades").and_then(Value::as_array) {
                for entry in arr {
                    let obj = entry.as_object().ok_or_else(|| anyhow!("precompileUpgrade is not an object"))?;
                    if obj.len() != 1 {
                        bail!("PrecompileUpgrade must have exactly one key, got {}", obj.len());
                    }
                    let (k, v) = obj.iter().next().unwrap();
                    let (key, addr) = MODULE_KEYS
                        .iter()
                        .find(|(mk, _)| mk == k)
                        .ok_or_else(|| anyhow!("unknown precompile config: {k}"))?;
                    precompile_upgrades.push(parse_precompile(key, Address::new(*addr), v)?);
                }
            }
        }

        let mut alloc = BTreeMap::new();
        if let Some(a) = g.get("alloc").and_then(Value::as_object) {
            for (addr, acct) in a {
                let address: Address = addr.parse().with_context(|| format!("alloc address {addr}"))?;
                let mut ga = GenesisAccount::default();
                if let Some(b) = acct.get("balance") {
                    ga.balance = json_u256(b).with_context(|| format!("alloc {addr} balance"))?;
                }
                if let Some(n) = acct.get("nonce") {
                    ga.nonce = json_u256(n)?.to::<u64>();
                }
                if let Some(c) = acct.get("code").and_then(Value::as_str) {
                    ga.code = c.parse::<Bytes>().with_context(|| format!("alloc {addr} code"))?;
                }
                if let Some(s) = acct.get("storage").and_then(Value::as_object) {
                    for (k, v) in s {
                        ga.storage.insert(k.parse()?, v.as_str().ok_or_else(|| anyhow!("storage value"))?.parse()?);
                    }
                }
                alloc.insert(address, ga);
            }
        }
        let genesis_timestamp = g.get("timestamp").map(json_u256).transpose()?.map(|t| t.to::<u64>()).unwrap_or(0);

        Ok(Config {
            chain_id,
            fee_config,
            allow_fee_recipients,
            subnet_evm,
            durango,
            etna,
            granite,
            genesis_precompiles,
            precompile_upgrades,
            alloc,
            genesis_timestamp,
        })
    }

    pub fn is_durango(&self, time: u64) -> bool {
        self.durango.is_some_and(|t| t <= time)
    }
    pub fn is_etna(&self, time: u64) -> bool {
        self.etna.is_some_and(|t| t <= time)
    }
    pub fn is_granite(&self, time: u64) -> bool {
        self.granite.is_some_and(|t| t <= time)
    }

    /// The revm spec for a block time. params.SetEthUpgrades: Shanghai = Durango,
    /// Cancun = Etna; the block-number forks are all 0 so London is the floor.
    /// libevm's EVM only sees IsMerge when Random is set, which subnet-evm does
    /// from Shanghai on, so pre-Durango is exactly LONDON (DIFFICULTY = header).
    pub fn spec(&self, time: u64) -> SpecId {
        if self.is_etna(time) {
            SpecId::CANCUN
        } else if self.is_durango(time) {
            SpecId::SHANGHAI
        } else {
            SpecId::LONDON
        }
    }

    /// GetActivatingPrecompileConfigs for every module, in module address order:
    /// configs whose timestamp is in (parent, block] (IsForkTransition).
    pub fn activating(&self, parent_time: Option<u64>, time: u64) -> Vec<&PrecompileConfig> {
        let transition = |t: u64| parent_time.map_or(true, |p| t > p) && t <= time;
        let mut out = Vec::new();
        for (_, addr) in MODULE_KEYS {
            let addr = Address::new(*addr);
            for c in self.genesis_precompiles.iter().filter(|c| c.address == addr) {
                if transition(c.timestamp) {
                    out.push(c);
                }
            }
            for c in self.precompile_upgrades.iter().filter(|c| c.address == addr) {
                if transition(c.timestamp) {
                    out.push(c);
                }
            }
        }
        out
    }

    /// IsPrecompileEnabled: the most recent config at `time` is not a disable.
    pub fn precompile_enabled(&self, addr: Address, time: u64) -> bool {
        self.activating(None, time)
            .into_iter()
            .filter(|c| c.address == addr)
            .last()
            .is_some_and(|c| !c.disable)
    }
}

fn parse_precompile(key: &'static str, address: Address, v: &Value) -> Result<PrecompileConfig> {
    if key != "feeManagerConfig" {
        bail!("precompile {key} is not implemented (only feeManagerConfig)");
    }
    let timestamp = v.get("blockTimestamp").and_then(Value::as_u64).ok_or_else(|| anyhow!("{key}: blockTimestamp missing"))?;
    let addrs = |k: &str| -> Result<Vec<Address>> {
        v.get(k)
            .and_then(Value::as_array)
            .map(|a| a.iter().map(|x| Ok(x.as_str().ok_or_else(|| anyhow!("{k}"))?.parse::<Address>()?)).collect())
            .unwrap_or(Ok(Vec::new()))
    };
    Ok(PrecompileConfig {
        key,
        address,
        timestamp,
        disable: v.get("disable").and_then(Value::as_bool).unwrap_or(false),
        admins: addrs("adminAddresses")?,
        enabled: addrs("enabledAddresses")?,
        managers: addrs("managerAddresses")?,
        initial_fee_config: v.get("initialFeeConfig").map(FeeConfig::from_json).transpose()?,
    })
}

/// geth math.HexOrDecimal256 / json numbers.
fn json_u256(v: &Value) -> Result<U256> {
    match v {
        Value::Number(n) => {
            let s = n.to_string();
            U256::from_str_radix(&s, 10).map_err(|e| anyhow!("{s}: {e}"))
        }
        Value::String(s) => {
            if let Some(h) = s.strip_prefix("0x").or_else(|| s.strip_prefix("0X")) {
                U256::from_str_radix(h, 16).map_err(|e| anyhow!("{s}: {e}"))
            } else {
                U256::from_str_radix(s, 10).map_err(|e| anyhow!("{s}: {e}"))
            }
        }
        _ => bail!("not a number: {v}"),
    }
}

/// keccak(0x01): the code hash of every activated stateful precompile.
pub fn precompile_code_hash() -> B256 {
    keccak256([1u8])
}

#[cfg(test)]
mod tests {
    use super::*;

    const STEP: &str = r#"{"config":{"chainId":1234,"homesteadBlock":0,"subnetEVMTimestamp":0,
        "feeConfig":{"gasLimit":20000000,"minBaseFee":1000000000,"targetGas":100000000,"baseFeeChangeDenominator":48,
        "minBlockGasCost":0,"maxBlockGasCost":10000000,"targetBlockRate":2,"blockGasCostStep":500000},"allowFeeRecipients":true},
        "alloc":{"0x7212Ac7f1146e5e59a6d58B0de00E73CA7ea57C9":{"balance":"0x1027e72f1f12813088000000"}},"timestamp":"0x0"}"#;
    const UPGRADE: &str = r#"{"precompileUpgrades":[{"feeManagerConfig":{"adminAddresses":["0xf15a56af154fb241db6167758c14e3970de018bf"],
        "blockTimestamp":1675252800,"initialFeeConfig":{"gasLimit":20000000,"targetBlockRate":2,"minBaseFee":1000000000,
        "targetGas":37500000,"baseFeeChangeDenominator":48,"minBlockGasCost":0,"maxBlockGasCost":10000000,"blockGasCostStep":500000}}}]}"#;

    #[test]
    fn step_config() {
        let c = Config::from_genesis(STEP.as_bytes(), UPGRADE.as_bytes(), 1).unwrap();
        assert_eq!(c.chain_id, 1234);
        assert!(c.allow_fee_recipients);
        assert_eq!(c.fee_config.target_gas, U256::from(100_000_000u64));
        assert_eq!(c.spec(1661675311), SpecId::LONDON);
        assert_eq!(c.spec(1709740800), SpecId::SHANGHAI);
        assert_eq!(c.spec(1734368400), SpecId::CANCUN);
        assert!(c.activating(Some(1675252799), 1675252800).len() == 1);
        assert!(c.activating(Some(1675252800), 1675252802).is_empty());
        assert!(!c.precompile_enabled(FEE_MANAGER, 1675252799));
        assert!(c.precompile_enabled(FEE_MANAGER, 1675252800));
        let w = crate::feemanager::configure(&c.precompile_upgrades[0], &c.fee_config, 7).unwrap();
        assert_eq!(w[3], (U256::from(4u8) << 248, U256::from(37_500_000u64)));
        assert_eq!(w[8].0, U256::from_str_radix("6c63610000000000000000000000000000000000000000000000000000000000", 16).unwrap());
        assert_eq!(w[8].1, U256::from(7u64));
        assert_eq!(w[9], (U256::from_be_slice(c.precompile_upgrades[0].admins[0].as_slice()), crate::feemanager::ROLE_ADMIN));
        assert_eq!(precompile_code_hash().to_string(), "0x5fe7f977e71dba2ea1a68e21057beebb9be2ac30c6410aa38d4f3fbe41dcffd2");
    }
}

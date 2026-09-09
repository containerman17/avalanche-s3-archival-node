//! The genesis block from the genesis JSON, as subnet-evm's
//! core.Genesis.toBlock builds its header: the root over the alloc plus the
//! precompiles active at time 0 (rs/exec's seeded state, alloy-trie full
//! recompute), then the header field defaults and the subnet-evm extras
//! (baseFee = minBaseFee under SubnetEVM, blockGasCost 0 under Etna, the
//! EIP-4844/4788 zeros under Cancun). keccak(header RLP) is the id the host
//! knows height 0 by: block 1's parentHash.
use alloy_primitives::{keccak256, Address, Bloom, Bytes, B256, B64, U256};
use block::{Block, Header};
use exec::{oracle, Config, Executor};
use serde_json::Value;

pub type Error = Box<dyn std::error::Error + Send + Sync>;

const EMPTY_ROOT: B256 = alloy_trie::EMPTY_ROOT_HASH;
const EMPTY_UNCLES: B256 = alloy_primitives::b256!("1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347");
/// ethparams.GenesisGasLimit and GenesisDifficulty.
const GENESIS_GAS_LIMIT: u64 = 4_712_388;
const GENESIS_DIFFICULTY: u64 = 131_072;
/// acp226.InitialDelayExcess (2000 ms).
pub const ACP226_INITIAL_DELAY_EXCESS: u64 = 7_970_124;

/// acp226 DelayExcess.Delay(): e^(excess / 2^20) ms by gas.CalculatePrice's
/// integer series.
pub fn acp226_delay_ms(excess: u64) -> u64 {
    const D: u64 = 1 << 20;
    let (n, d) = (U256::from(excess), U256::from(D));
    let mut output = U256::ZERO;
    let mut acc = d;
    let mut i = 1u64;
    while !acc.is_zero() {
        output += acc;
        acc = acc * n / d / U256::from(i);
        i += 1;
    }
    u64::try_from(output / d).unwrap_or(u64::MAX)
}

/// acp226.DesiredDelayExcess: the smallest excess whose delay reaches `ms`.
pub fn acp226_desired_delay_excess(ms: u64) -> u64 {
    let (mut lo, mut hi) = (0u64, 46_516_320u64);
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        if acp226_delay_ms(mid) >= ms {
            hi = mid;
        } else {
            lo = mid + 1;
        }
    }
    lo
}

/// geth math.HexOrDecimal64 / HexOrDecimal256, or a JSON number.
fn num(v: Option<&Value>) -> Result<Option<U256>, Error> {
    let Some(v) = v else { return Ok(None) };
    Ok(Some(match v {
        Value::Null => return Ok(None),
        Value::Number(n) => U256::from_str_radix(&n.to_string(), 10)?,
        Value::String(s) => match s.strip_prefix("0x").or_else(|| s.strip_prefix("0X")) {
            Some(h) => U256::from_str_radix(h, 16)?,
            None => U256::from_str_radix(s, 10)?,
        },
        _ => return Err(format!("genesis: {v} is not a number").into()),
    }))
}

fn u64_of(v: Option<&Value>) -> Result<Option<u64>, Error> {
    Ok(num(v)?.map(|x| x.to::<u64>()))
}

/// The genesis block of `cfg`'s chain: no txs, empty container bytes (what
/// the Go follower answers for height 0 too).
pub fn block(cfg: &Config, genesis_json: &[u8]) -> Result<Block, Error> {
    let g: Value = serde_json::from_slice(genesis_json)?;
    let root = oracle::state_root(Executor::new(cfg.clone()).map_err(|e| e.to_string())?.db());
    let time = cfg.genesis_timestamp;
    // Granite at genesis: TimeMilliseconds = time * 1000 and the initial
    // delay excess (acp226.InitialDelayExcess, or DesiredDelayExcess of the
    // config's initialMinDelayMS).
    let granite = cfg.is_granite(time);
    let initial_min_delay_ms = g.get("config").and_then(|c| c.get("initialMinDelayMS")).and_then(Value::as_u64).unwrap_or(0);
    let extra: Bytes = match g.get("extraData") {
        Some(Value::String(s)) => s.parse()?,
        _ => Bytes::new(),
    };
    let subnet_evm = cfg.subnet_evm <= time;
    let base_fee = if subnet_evm { Some(num(g.get("baseFeePerGas"))?.unwrap_or(cfg.fee_config.min_base_fee)) } else { None };
    let cancun = cfg.is_etna(time);
    let h = Header {
        parent_hash: g.get("parentHash").and_then(Value::as_str).map(|s| s.parse()).transpose()?.unwrap_or_default(),
        uncle_hash: EMPTY_UNCLES,
        coinbase: g.get("coinbase").and_then(Value::as_str).map(|s| s.parse::<Address>()).transpose()?.unwrap_or_default(),
        root,
        tx_hash: EMPTY_ROOT,
        receipt_hash: EMPTY_ROOT,
        bloom: Bloom::default(),
        difficulty: num(g.get("difficulty"))?.unwrap_or(U256::from(GENESIS_DIFFICULTY)),
        number: u64_of(g.get("number"))?.unwrap_or(0),
        gas_limit: match u64_of(g.get("gasLimit"))? {
            Some(0) | None => GENESIS_GAS_LIMIT,
            Some(l) => l,
        },
        gas_used: u64_of(g.get("gasUsed"))?.unwrap_or(0),
        time,
        extra,
        mix_digest: g.get("mixHash").and_then(Value::as_str).map(|s| s.parse()).transpose()?.unwrap_or_default(),
        nonce: B64::from(u64_of(g.get("nonce"))?.unwrap_or(0).to_be_bytes()),
        base_fee,
        block_gas_cost: if cancun { Some(U256::ZERO) } else { None },
        blob_gas_used: if cancun { Some(u64_of(g.get("blobGasUsed"))?.unwrap_or(0)) } else { None },
        excess_blob_gas: if cancun { Some(u64_of(g.get("excessBlobGas"))?.unwrap_or(0)) } else { None },
        parent_beacon_root: if cancun { Some(B256::ZERO) } else { None },
        time_milliseconds: if granite { Some(time * 1000) } else { None },
        min_delay_excess: if granite { Some(if initial_min_delay_ms != 0 { acp226_desired_delay_excess(initial_min_delay_ms) } else { ACP226_INITIAL_DELAY_EXCESS }) } else { None },
    };
    let header_rlp = bytes::Bytes::from(block::eth::encode_header(&h)?);
    let hash = keccak256(&header_rlp);
    Ok(Block { height: h.number, hash, container_id: hash, header: h, header_rlp, txs: Vec::new(), container: bytes::Bytes::new(), pvm: None })
}

/// Step's genesis JSON (the fixture of ws.rs and rs/chain's tests).
#[doc(hidden)]
pub const STEP_GENESIS: &str = r#"{"config":{"chainId":1234,"homesteadBlock":0,"eip150Block":0,"eip155Block":0,"eip158Block":0,"byzantiumBlock":0,
            "constantinopleBlock":0,"petersburgBlock":0,"istanbulBlock":0,"muirGlacierBlock":0,"subnetEVMTimestamp":0,
            "feeConfig":{"gasLimit":20000000,"minBaseFee":1000000000,"targetGas":100000000,"baseFeeChangeDenominator":48,
            "minBlockGasCost":0,"maxBlockGasCost":10000000,"targetBlockRate":2,"blockGasCostStep":500000},"allowFeeRecipients":true},
            "nonce":"0x0","timestamp":"0x0","extraData":"0x00","gasLimit":"0x1312d00","difficulty":"0x0",
            "mixHash":"0x0000000000000000000000000000000000000000000000000000000000000000","coinbase":"0x0000000000000000000000000000000000000000",
            "alloc":{"7212Ac7f1146e5e59a6d58B0de00E73CA7ea57C9":{"balance":"0x1027e72f1f12813088000000"}},
            "number":"0x0","gasUsed":"0x0","parentHash":"0x0000000000000000000000000000000000000000000000000000000000000000"}"#;

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// Step's genesis: the hash must be block 1's parentHash.
    #[test]
    fn step_genesis_hash() {
        let g = STEP_GENESIS;
        let cfg = Config::from_genesis(g.as_bytes(), b"", 1).unwrap();
        let b = block(&cfg, g.as_bytes()).unwrap();
        assert_eq!(b.header.root.to_string(), "0x51736d52ef12525c8a48a4d2215b34a7573e871efb62008ac8b45c25590f0d21");
        assert_eq!(b.header.base_fee, Some(U256::from(1_000_000_000u64)));
        let again = block::eth::decode_header(&b.header_rlp).unwrap();
        assert_eq!(again.gas_limit, 20_000_000);
        assert_eq!(b.hash.to_string(), STEP_GENESIS_HASH);
    }

    /// Block 1's parentHash in the Step dump (checked by the harness too).
    const STEP_GENESIS_HASH: &str = "0x628a4aba6699f2af07d1c66cce4ae6b3c17982987fd57995ddfe571c5cb1b0c1";
}

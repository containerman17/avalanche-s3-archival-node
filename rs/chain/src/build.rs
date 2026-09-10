//! The header a subnet-evm miner builds (miner/worker.go commitNewWork +
//! consensus/dummy FinalizeAndAssemble + plugin/evm/customheader): every
//! field is a function of the parent header, the fee config and coinbase
//! rule read at the parent's state, the caller's timestamp and coinbase,
//! and what execution produced. Wall clock and validator identity never
//! enter here: the caller picks the timestamp (customheader.GetNextTimestamp
//! and VerifyTime are the shell's), the coinbase, and the desired ACP-226
//! delay excess (the node's `desiredDelayExcess` config).
use alloy_primitives::{Address, Bloom, B256, B64, U256};
use alloy_trie::EMPTY_ROOT_HASH;
use block::{Header, Tx};
use exec::config::FeeConfig;
use exec::Config;

pub const EMPTY_UNCLES: B256 = alloy_primitives::b256!("1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347");
/// constants.BlackholeAddr.
pub const BLACKHOLE: Address = Address::new([1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
const WINDOW_LEN: usize = 10;
/// acp226.InitialDelayExcess and MaxDelayExcessDiff.
pub const INITIAL_DELAY_EXCESS: u64 = rpc::genesis::ACP226_INITIAL_DELAY_EXCESS;
const MAX_DELAY_EXCESS_DIFF: u64 = 200;

/// What the caller decides.
pub struct Params {
    /// Unix milliseconds; `time` is the floor to seconds, `timeMilliseconds`
    /// is set under Granite.
    pub timestamp_ms: u64,
    pub coinbase: Address,
    /// acp226 desired delay excess (None: keep the parent's).
    pub desired_min_delay_excess: Option<u64>,
}

/// BlockChain.GetCoinbaseAt: (configured address, fee recipients allowed).
pub fn coinbase_rule(cfg: &Config, parent_time: u64, reward_slot: impl FnOnce() -> U256) -> (Address, bool) {
    if parent_time < cfg.subnet_evm {
        return (BLACKHOLE, false);
    }
    if !cfg.precompile_enabled(exec::precompile::REWARD_MANAGER, parent_time) {
        return if cfg.allow_fee_recipients { (Address::ZERO, true) } else { (BLACKHOLE, false) };
    }
    let v = reward_slot();
    (Address::from_slice(&v.to_be_bytes::<32>()[12..]), v == exec::rewardmanager::allow_fee_recipients_value())
}

/// BlockChain.GetFeeConfigAt with the FeeManager's stored words when it is
/// enabled at the parent (`slot(i)` reads its storage word i = 1..=8).
pub fn fee_config_at(cfg: &Config, parent_time: u64, mut slot: impl FnMut(U256) -> U256) -> FeeConfig {
    if cfg.precompile_enabled(exec::precompile::FEE_MANAGER, parent_time) {
        let mut w = [U256::ZERO; 8];
        for (i, x) in w.iter_mut().enumerate() {
            *x = slot(exec::feemanager::field_slot(i as u8 + 1));
        }
        FeeConfig::from_words(&w)
    } else {
        cfg.fee_config.clone()
    }
}

/// customheader.feeWindow: the parent's window plus its gasUsed, shifted by
/// the elapsed seconds (subnetevm.Window).
pub fn fee_window(parent: &Header, timestamp: u64) -> Result<[u64; WINDOW_LEN], String> {
    if parent.number == 0 {
        return Ok([0; WINDOW_LEN]);
    }
    if parent.extra.len() < 8 * WINDOW_LEN {
        return Err(format!("insufficient length for window: expected at least {} bytes but got {} bytes", 8 * WINDOW_LEN, parent.extra.len()));
    }
    if timestamp < parent.time {
        return Err(format!("invalid timestamp: timestamp {timestamp} prior to parent timestamp {}", parent.time));
    }
    let mut w = [0u64; WINDOW_LEN];
    for (i, x) in w.iter_mut().enumerate() {
        *x = u64::from_be_bytes(parent.extra[i * 8..i * 8 + 8].try_into().unwrap());
    }
    w[WINDOW_LEN - 1] = w[WINDOW_LEN - 1].saturating_add(parent.gas_used);
    let n = (timestamp - parent.time) as usize;
    if n >= WINDOW_LEN {
        return Ok([0; WINDOW_LEN]);
    }
    w.rotate_left(n);
    for x in w[WINDOW_LEN - n..].iter_mut() {
        *x = 0;
    }
    Ok(w)
}

pub fn window_bytes(w: &[u64; WINDOW_LEN]) -> Vec<u8> {
    w.iter().flat_map(|x| x.to_be_bytes()).collect()
}

/// customheader.BlockGasCost: nil before SubnetEVM (never here), 0 under
/// Granite, else parentCost +/- step * |targetBlockRate - elapsed| clamped.
pub fn block_gas_cost(cfg: &Config, fc: &FeeConfig, parent: &Header, timestamp: u64) -> U256 {
    if cfg.is_granite(timestamp) {
        return U256::ZERO;
    }
    let Some(parent_cost) = parent.block_gas_cost else { return fc.min_block_gas_cost };
    let step = fc.block_gas_cost_step.to::<u64>();
    let elapsed = timestamp.saturating_sub(parent.time);
    let deviation = fc.target_block_rate.abs_diff(elapsed);
    let change = step.checked_mul(deviation).unwrap_or(u64::MAX);
    let (min, max) = (fc.min_block_gas_cost.to::<u64>(), fc.max_block_gas_cost.to::<u64>());
    let parent_cost = parent_cost.to::<u64>();
    let cost = if elapsed > fc.target_block_rate { parent_cost.checked_sub(change).unwrap_or(min) } else { parent_cost.checked_add(change).unwrap_or(max) };
    U256::from(cost.clamp(min, max))
}

/// customheader.MinDelayExcess under Granite: the parent's excess (the
/// initial one when the parent is pre-Granite) moved toward the desired value
/// by at most acp226.MaxDelayExcessDiff.
pub fn min_delay_excess(cfg: &Config, parent: &Header, desired: Option<u64>) -> Result<u64, String> {
    let mut e = if cfg.is_granite(parent.time) { parent.min_delay_excess.ok_or_else(|| format!("parent min delay excess should not be nil: {}", parent.number))? } else { INITIAL_DELAY_EXCESS };
    if let Some(d) = desired {
        let change = e.abs_diff(d).min(MAX_DELAY_EXCESS_DIFF);
        e = if e < d { e + change } else { e - change };
    }
    Ok(e)
}

pub use rpc::genesis::{acp226_delay_ms as delay_ms, acp226_desired_delay_excess as desired_delay_excess};

/// commitNewWork + Prepare: the header before execution (extra = the fee
/// window; root, txHash, receiptsRoot, bloom, gasUsed come after).
/// `coinbase_rule` is GetCoinbaseAt(parent).
pub fn template(cfg: &Config, fc: &FeeConfig, parent: &Header, p: &Params, coinbase_rule: (Address, bool)) -> Result<Header, String> {
    let timestamp = p.timestamp_ms / 1000;
    let parent_ms = parent.time_milliseconds.unwrap_or(parent.time * 1000);
    if p.timestamp_ms < parent_ms {
        return Err(format!("block timestamp is too old: {} < parent {parent_ms}", p.timestamp_ms));
    }
    if timestamp < cfg.subnet_evm {
        return Err("building before SubnetEVM is not supported".into());
    }
    if p.coinbase == Address::ZERO {
        return Err("cannot mine without etherbase".into());
    }
    let (configured, allow) = coinbase_rule;
    let coinbase = if !allow && p.coinbase != configured { configured } else { p.coinbase };
    let window = fee_window(parent, timestamp)?;
    let base_fee = rpc::fee::next_base_fee(fc, parent, timestamp).map_err(|e| e.message)?;
    let cancun = cfg.is_etna(timestamp);
    let granite = cfg.is_granite(timestamp);
    Ok(Header {
        parent_hash: parent.hash(),
        uncle_hash: EMPTY_UNCLES,
        coinbase,
        root: B256::ZERO,
        tx_hash: EMPTY_ROOT_HASH,
        receipt_hash: EMPTY_ROOT_HASH,
        bloom: Bloom::default(),
        difficulty: U256::from(1),
        number: parent.number + 1,
        gas_limit: fc.gas_limit.to::<u64>(),
        gas_used: 0,
        time: timestamp,
        extra: window_bytes(&window).into(),
        mix_digest: B256::ZERO,
        nonce: B64::ZERO,
        base_fee: Some(base_fee),
        block_gas_cost: Some(block_gas_cost(cfg, fc, parent, timestamp)),
        // EIP-4844 with no blob txs: excess = max(parent.excess + parent.used - target, 0) = 0 always.
        blob_gas_used: if cancun { Some(0) } else { None },
        excess_blob_gas: if cancun { Some(0) } else { None },
        parent_beacon_root: if cancun { Some(B256::ZERO) } else { None },
        time_milliseconds: if granite { Some(p.timestamp_ms) } else { None },
        min_delay_excess: if granite { Some(min_delay_excess(cfg, parent, p.desired_min_delay_excess)?) } else { None },
    })
}

/// customheader.VerifyBlockFee: the effective tips must buy the block gas cost.
pub fn verify_block_fee(base_fee: U256, block_gas_cost: U256, txs: &[&Tx], gas_used: &[u64]) -> Result<(), String> {
    if block_gas_cost.is_zero() {
        return Ok(());
    }
    let mut total = U256::ZERO;
    for (t, g) in txs.iter().zip(gas_used) {
        let cap = U256::from(t.gas_price);
        if cap < base_fee {
            return Err("max fee per gas less than block base fee".into());
        }
        let tip = U256::from(t.gas_tip).min(cap - base_fee);
        total += tip * U256::from(*g);
    }
    let bought = total / base_fee;
    if bought < block_gas_cost {
        return Err(format!("insufficient gas to cover the block cost: expected {block_gas_cost} but got {bought}"));
    }
    Ok(())
}

/// types.NewBlock: the header completed from execution, the block RLP
/// `[header, txs, []]` and its hash.
pub fn assemble(mut h: Header, txs: &[&Tx], root: B256, receipts_root: B256, tx_root: B256, r: &exec::BlockResult, predicate_bytes: &[u8]) -> Result<(Header, Vec<u8>, Vec<u8>), String> {
    if !predicate_bytes.is_empty() {
        let mut e = h.extra.to_vec();
        e.extend_from_slice(predicate_bytes);
        h.extra = e.into();
    }
    h.root = root;
    h.gas_used = r.gas_used;
    h.bloom = r.bloom;
    h.receipt_hash = receipts_root;
    h.tx_hash = tx_root;
    let header_rlp = block::eth::encode_header(&h).map_err(|e| e.to_string())?;
    let mut body = Vec::with_capacity(header_rlp.len() + txs.iter().map(|t| t.raw.len() + 4).sum::<usize>() + 8);
    body.extend_from_slice(&header_rlp);
    let mut list = Vec::with_capacity(txs.iter().map(|t| t.raw.len() + 4).sum());
    for t in txs {
        if t.tx_type == 0 {
            list.extend_from_slice(&t.raw);
        } else {
            alloy_rlp::Header { list: false, payload_length: t.raw.len() }.encode(&mut list);
            list.extend_from_slice(&t.raw);
        }
    }
    alloy_rlp::Header { list: true, payload_length: list.len() }.encode(&mut body);
    body.extend_from_slice(&list);
    body.push(0xc0);
    let mut out = Vec::with_capacity(body.len() + 4);
    alloy_rlp::Header { list: true, payload_length: body.len() }.encode(&mut out);
    out.extend_from_slice(&body);
    Ok((h, header_rlp, out))
}

/// The transactions trie root of `txs` in order (the header's txHash).
pub fn tx_root(txs: &[&Tx]) -> B256 {
    if txs.is_empty() {
        return EMPTY_ROOT_HASH;
    }
    alloy_trie::root::ordered_trie_root_with_encoder(txs, |t, buf| buf.extend_from_slice(&t.raw))
}

trait HeaderHash {
    fn hash(&self) -> B256;
}
impl HeaderHash for Header {
    fn hash(&self) -> B256 {
        alloy_primitives::keccak256(block::eth::encode_header(self).expect("header encodes"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fc() -> FeeConfig {
        FeeConfig { gas_limit: U256::from(20_000_000), target_block_rate: 2, min_base_fee: U256::from(25_000_000_000u64), target_gas: U256::from(15_000_000), base_fee_change_denominator: U256::from(36), min_block_gas_cost: U256::ZERO, max_block_gas_cost: U256::from(1_000_000), block_gas_cost_step: U256::from(200_000) }
    }
    fn parent(cost: Option<u64>, time: u64) -> Header {
        Header { parent_hash: B256::ZERO, uncle_hash: EMPTY_UNCLES, coinbase: Address::ZERO, root: B256::ZERO, tx_hash: EMPTY_ROOT_HASH, receipt_hash: EMPTY_ROOT_HASH, bloom: Bloom::default(), difficulty: U256::from(1), number: 10, gas_limit: 20_000_000, gas_used: 100_000, time, extra: vec![0u8; 80].into(), mix_digest: B256::ZERO, nonce: B64::ZERO, base_fee: Some(U256::from(25_000_000_000u64)), block_gas_cost: cost.map(U256::from), blob_gas_used: None, excess_blob_gas: None, parent_beacon_root: None, time_milliseconds: None, min_delay_excess: None }
    }

    /// blockgascost.BlockGasCost's table (cost_test.go): parent 500,000, step 200,000, target rate 2.
    #[test]
    fn block_gas_cost_table() {
        let cfg = Config::from_genesis(rpc::genesis::STEP_GENESIS.as_bytes(), b"", 1).unwrap();
        let f = fc();
        let p = parent(Some(500_000), 1000);
        assert_eq!(block_gas_cost(&cfg, &f, &p, 1000), U256::from(900_000)); // elapsed 0: +2*200k
        assert_eq!(block_gas_cost(&cfg, &f, &p, 1001), U256::from(700_000));
        assert_eq!(block_gas_cost(&cfg, &f, &p, 1002), U256::from(500_000));
        assert_eq!(block_gas_cost(&cfg, &f, &p, 1003), U256::from(300_000));
        assert_eq!(block_gas_cost(&cfg, &f, &p, 1010), U256::ZERO);
        assert_eq!(block_gas_cost(&cfg, &f, &parent(Some(950_000), 1000), 1000), U256::from(1_000_000));
        assert_eq!(block_gas_cost(&cfg, &f, &parent(None, 1000), 1000), U256::ZERO);
        // Granite: 0.
        assert_eq!(block_gas_cost(&cfg, &f, &p, 1_763_568_000), U256::ZERO);
    }

    #[test]
    fn fee_window_shifts_and_adds_parent_gas() {
        let mut p = parent(None, 1000);
        p.extra = window_bytes(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]).into();
        assert_eq!(fee_window(&p, 1000).unwrap(), [1, 2, 3, 4, 5, 6, 7, 8, 9, 100_010]);
        assert_eq!(fee_window(&p, 1003).unwrap(), [4, 5, 6, 7, 8, 9, 100_010, 0, 0, 0]);
        assert_eq!(fee_window(&p, 1010).unwrap(), [0; 10]);
        assert!(fee_window(&p, 999).is_err());
    }

    #[test]
    fn delay_excess_moves_toward_desired_by_200() {
        let cfg = Config::from_genesis(rpc::genesis::STEP_GENESIS.as_bytes(), b"", 1).unwrap();
        let p = parent(None, 1000); // pre-Granite parent
        assert_eq!(min_delay_excess(&cfg, &p, None).unwrap(), INITIAL_DELAY_EXCESS);
        assert_eq!(min_delay_excess(&cfg, &p, Some(0)).unwrap(), INITIAL_DELAY_EXCESS - 200);
        assert_eq!(min_delay_excess(&cfg, &p, Some(INITIAL_DELAY_EXCESS + 50)).unwrap(), INITIAL_DELAY_EXCESS + 50);
        let mut g = parent(None, 1_763_568_100);
        g.min_delay_excess = Some(5_000);
        assert_eq!(min_delay_excess(&cfg, &g, Some(10_000)).unwrap(), 5_200);
        g.min_delay_excess = None;
        assert!(min_delay_excess(&cfg, &g, None).is_err());
    }

    /// acp226_test.go's table.
    #[test]
    fn acp226_delay_and_desired_excess() {
        assert_eq!(delay_ms(0), 1);
        assert_eq!(delay_ms(4_828_872), 100);
        assert_eq!(delay_ms(6_516_490), 500);
        assert_eq!(delay_ms(7_243_307), 1000);
        assert_eq!(delay_ms(INITIAL_DELAY_EXCESS), 2000);
        assert_eq!(delay_ms(9_657_742), 10000);
        assert_eq!(desired_delay_excess(2000), INITIAL_DELAY_EXCESS);
        assert_eq!(desired_delay_excess(1000), 7_243_307);
        assert_eq!(desired_delay_excess(100), 4_828_872);
    }

    #[test]
    fn coinbase_rule_without_reward_manager() {
        let cfg = Config::from_genesis(rpc::genesis::STEP_GENESIS.as_bytes(), b"", 1).unwrap();
        assert_eq!(coinbase_rule(&cfg, 5, || U256::ZERO), (Address::ZERO, true));
        let mut c2 = cfg.clone();
        c2.allow_fee_recipients = false;
        assert_eq!(coinbase_rule(&c2, 5, || U256::ZERO), (BLACKHOLE, false));
    }
}

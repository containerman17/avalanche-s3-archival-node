//! subnet-evm's next base fee (customheader.EstimateNextBaseFee over the
//! fee window in parent.Extra), at the parent's own timestamp, with the fee
//! config the FeeManager precompile holds at the parent when it is enabled
//! (what stock reads; the Go node reads the genesis fee config, a deviation
//! reported in REPORT.md).
use alloy_primitives::U256;
use block::Header;
use exec::config::FeeConfig;
use serde_json::{json, Value};

use crate::{RpcError, RpcResult, Server, StateRead};

const WINDOW_LEN: usize = 10;

fn add(sum: u64, v: u64) -> u64 {
    sum.checked_add(v).unwrap_or(u64::MAX)
}

impl Server {
    /// The fee config in force when building on `parent`.
    pub fn fee_config_at(&self, parent: &Header, st: &mut dyn StateRead) -> Result<FeeConfig, RpcError> {
        Ok(self.fee_config_and_last_changed(parent, st)?.0)
    }

    /// BlockChain.GetFeeConfigAt: (config, lastChangedAt); lastChangedAt is
    /// None before SubnetEVM, 0 without the FeeManager, else its stored value.
    fn fee_config_and_last_changed(&self, parent: &Header, st: &mut dyn StateRead) -> Result<(FeeConfig, Option<U256>), RpcError> {
        if parent.time < self.cfg.subnet_evm {
            // params.DefaultFeeConfig
            return Ok((FeeConfig { gas_limit: U256::from(8_000_000), target_block_rate: 2, min_base_fee: U256::from(25_000_000_000u64), target_gas: U256::from(15_000_000), base_fee_change_denominator: U256::from(36), min_block_gas_cost: U256::ZERO, max_block_gas_cost: U256::from(1_000_000), block_gas_cost_step: U256::from(200_000) }, None));
        }
        if !self.cfg.precompile_enabled(exec::precompile::FEE_MANAGER, parent.time) {
            return Ok((self.cfg.fee_config.clone(), Some(U256::ZERO)));
        }
        // feemanager storage keys are 1..=8.
        let mut w = [U256::ZERO; 8];
        for (i, x) in w.iter_mut().enumerate() {
            *x = st.storage(exec::precompile::FEE_MANAGER, exec::feemanager::field_slot(i as u8 + 1))?;
        }
        let lca = st.storage(exec::precompile::FEE_MANAGER, exec::feemanager::last_changed_slot())?;
        Ok((FeeConfig::from_words(&w), Some(lca)))
    }

    /// eth_feeConfig [blockNrOrHash]: ethapi FeeConfigResult.
    pub fn fee_config(&self, params: &[Value]) -> RpcResult {
        let n = self.block_number(params.first())?;
        let b = self.block_at(n)?;
        let mut st = self.store.state_at(n)?;
        let (fc, lca) = self.fee_config_and_last_changed(&b.header, st.as_mut())?;
        let num = |v: U256| json!(u64::try_from(v).unwrap_or(u64::MAX));
        let mut out = json!({"feeConfig": {
            "gasLimit": num(fc.gas_limit), "targetBlockRate": fc.target_block_rate, "minBaseFee": num(fc.min_base_fee),
            "targetGas": num(fc.target_gas), "baseFeeChangeDenominator": num(fc.base_fee_change_denominator),
            "minBlockGasCost": num(fc.min_block_gas_cost), "maxBlockGasCost": num(fc.max_block_gas_cost), "blockGasCostStep": num(fc.block_gas_cost_step),
        }});
        if let Some(l) = lca {
            out["lastChangedAt"] = num(l);
        }
        Ok(out)
    }

    /// EstimateNextBaseFee at the parent's own timestamp.
    pub fn next_base_fee_of(&self, parent: &Header) -> Result<Option<U256>, RpcError> {
        self.next_base_fee_at(parent, parent.time)
    }

    /// EstimateNextBaseFee(parent, timestamp) with the fee config in force
    /// at `parent`; None before SubnetEVM.
    pub fn next_base_fee_at(&self, parent: &Header, timestamp: u64) -> Result<Option<U256>, RpcError> {
        if parent.time < self.cfg.subnet_evm {
            return Ok(None);
        }
        let mut st = self.store.state_at(parent.number)?;
        let fc = self.fee_config_at(parent, st.as_mut())?;
        Ok(Some(next_base_fee(&fc, parent, timestamp)?))
    }
}

/// customheader.EstimateNextBaseFee / baseFeeFromWindow: the parent's fee
/// window plus its gasUsed, shifted by the elapsed seconds (the ms clock floors
/// to seconds), the delta multiplied by the whole windows elapsed on the way
/// down, clamped to the min base fee. Vectors from the Go library in
/// `exp/feecheck synth`.
pub fn next_base_fee(fc: &FeeConfig, parent: &Header, timestamp: u64) -> Result<U256, RpcError> {
    let timestamp = timestamp.max(parent.time);
    if parent.number == 0 {
        return Ok(fc.min_base_fee);
    }
    let parent_base = parent.base_fee.ok_or_else(|| RpcError::from(format!("block {} has no base fee", parent.number)))?;
    if parent.extra.len() < 8 * WINDOW_LEN {
        return Err(format!("insufficient length for window: expected at least {} bytes but got {} bytes", 8 * WINDOW_LEN, parent.extra.len()).into());
    }
    let mut window = [0u64; WINDOW_LEN];
    for (i, w) in window.iter_mut().enumerate() {
        *w = u64::from_be_bytes(parent.extra[i * 8..i * 8 + 8].try_into().unwrap());
    }
    window[WINDOW_LEN - 1] = add(window[WINDOW_LEN - 1], parent.gas_used);
    let elapsed = timestamp - parent.time;
    if elapsed as usize >= WINDOW_LEN {
        window = [0; WINDOW_LEN];
    } else {
        window.rotate_left(elapsed as usize);
        for w in window[WINDOW_LEN - elapsed as usize..].iter_mut() {
            *w = 0;
        }
    }
    let total: u64 = window.iter().fold(0, |s, v| add(s, *v));
    let target = fc.target_gas.to::<u64>();
    if total == target {
        // exact target: the base fee stays, unclamped (legacy)
        return Ok(parent_base);
    }
    let (diff, up) = if total > target { (total - target, true) } else { (target - total, false) };
    let mut delta = (U256::from(diff) * parent_base / U256::from(target) / fc.base_fee_change_denominator).max(U256::from(1));
    let base = if up {
        parent_base.saturating_add(delta)
    } else {
        let windows = elapsed / WINDOW_LEN as u64;
        if windows > 1 {
            delta *= U256::from(windows);
        }
        parent_base.saturating_sub(delta)
    };
    Ok(base.max(fc.min_base_fee))
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{Address, Bytes, B256};

    /// `go run ./exp/feecheck synth`: customheader.EstimateNextBaseFee on the
    /// same parents at the same offsets (the clock at parent.time + off, .999 s).
    #[test]
    fn next_base_fee_matches_go() {
        let fc = FeeConfig { gas_limit: U256::from(20_000_000), target_block_rate: 2, min_base_fee: U256::from(25_000_000_000u64), target_gas: U256::from(15_000_000), base_fee_change_denominator: U256::from(36), min_block_gas_cost: U256::ZERO, max_block_gas_cost: U256::from(1_000_000), block_gas_cost_step: U256::from(200_000) };
        let window = |v: &[u64]| -> Bytes {
            let mut out = vec![0u8; 80];
            for (i, x) in v.iter().enumerate() {
                out[i * 8..i * 8 + 8].copy_from_slice(&x.to_be_bytes());
            }
            out.into()
        };
        let parent = |base_fee: u64, gas_used: u64, extra: Bytes| Header {
            parent_hash: B256::ZERO, uncle_hash: B256::ZERO, coinbase: Address::ZERO, root: B256::ZERO, tx_hash: B256::ZERO, receipt_hash: B256::ZERO,
            bloom: Default::default(), difficulty: U256::from(1), number: 1000, gas_limit: 20_000_000, gas_used, time: 1_700_000_000, extra,
            mix_digest: B256::ZERO, nonce: Default::default(), base_fee: Some(U256::from(base_fee)), block_gas_cost: None, blob_gas_used: None,
            excess_blob_gas: None, parent_beacon_root: None, time_milliseconds: None, min_delay_excess: None,
        };
        let cases = [
            ("busy", parent(300_000_000_000, 8_000_000, window(&[1_000_000, 2_000_000, 3_000_000, 4_000_000, 5_000_000, 6_000_000, 7_000_000, 8_000_000, 9_000_000, 10_000_000]))),
            ("quiet", parent(300_000_000_000, 21_000, window(&[0, 0, 0, 0, 0, 0, 0, 0, 0, 100_000]))),
            ("exact", parent(300_000_000_000, 5_000_000, window(&[0, 0, 0, 0, 0, 0, 0, 0, 0, 10_000_000]))),
            ("tiny", parent(25_000_000_001, 21_000, window(&[0, 0, 0, 0, 0, 0, 0, 0, 0, 14_978_999]))),
            ("floor", parent(25_000_000_000, 0, window(&[]))),
        ];
        let want: &[(&str, u64, u64)] = &[
            ("busy", 0, 326666666666), ("busy", 1, 326111111111), ("busy", 3, 323333333333), ("busy", 9, 301666666666), ("busy", 10, 291666666667), ("busy", 11, 291666666667),
            ("busy", 19, 291666666667), ("busy", 20, 283333333334), ("busy", 25, 283333333334), ("busy", 100, 216666666670), ("busy", 1000, 25000000000),
            ("quiet", 0, 291733888889), ("quiet", 1, 291733888889), ("quiet", 3, 291733888889), ("quiet", 9, 291733888889), ("quiet", 10, 291666666667), ("quiet", 11, 291666666667),
            ("quiet", 19, 291666666667), ("quiet", 20, 283333333334), ("quiet", 25, 283333333334), ("quiet", 100, 216666666670), ("quiet", 1000, 25000000000),
            ("exact", 0, 300000000000), ("exact", 1, 300000000000), ("exact", 3, 300000000000), ("exact", 9, 300000000000), ("exact", 10, 291666666667), ("exact", 11, 291666666667),
            ("exact", 19, 291666666667), ("exact", 20, 283333333334), ("exact", 25, 283333333334), ("exact", 100, 216666666670), ("exact", 1000, 25000000000),
            ("tiny", 0, 25000000000), ("tiny", 1, 25000000000), ("tiny", 1000, 25000000000),
            ("floor", 0, 25000000000), ("floor", 1, 25000000000), ("floor", 10, 25000000000), ("floor", 1000, 25000000000),
        ];
        for (name, off, fee) in want {
            let p = &cases.iter().find(|(n, _)| n == name).unwrap().1;
            assert_eq!(next_base_fee(&fc, p, p.time + off).ok(), Some(U256::from(*fee)), "{name} +{off}");
        }
    }
}

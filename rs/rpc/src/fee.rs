//! subnet-evm's next base fee (customheader.EstimateNextBaseFee over the
//! fee window in parent.Extra), at the parent's own timestamp, with the fee
//! config the FeeManager precompile holds at the parent when it is enabled
//! (what stock reads; the Go node reads the genesis fee config, a deviation
//! reported in REPORT.md).
use alloy_primitives::U256;
use block::Header;
use exec::config::FeeConfig;

use crate::{RpcError, Server, StateRead};

const WINDOW_LEN: usize = 10;

fn add(sum: u64, v: u64) -> u64 {
    sum.checked_add(v).unwrap_or(u64::MAX)
}

impl Server {
    /// The fee config in force when building on `parent`.
    pub fn fee_config_at(&self, parent: &Header, st: &mut dyn StateRead) -> Result<FeeConfig, RpcError> {
        if !self.cfg.precompile_enabled(exec::precompile::FEE_MANAGER, parent.time) {
            return Ok(self.cfg.fee_config.clone());
        }
        let mut w = [U256::ZERO; 8];
        for (i, x) in w.iter_mut().enumerate() {
            *x = st.storage(exec::precompile::FEE_MANAGER, exec::feemanager::field_slot(i as u8))?;
        }
        Ok(FeeConfig::from_words(&w))
    }

    /// EstimateNextBaseFee at the parent's own timestamp.
    pub fn next_base_fee_of(&self, parent: &Header) -> Result<Option<U256>, RpcError> {
        self.next_base_fee_at(parent, parent.time)
    }

    /// EstimateNextBaseFee(parent, timestamp): the window shifted by the
    /// elapsed seconds; None before SubnetEVM.
    pub fn next_base_fee_at(&self, parent: &Header, timestamp: u64) -> Result<Option<U256>, RpcError> {
        let cfg = &self.cfg;
        let timestamp = timestamp.max(parent.time);
        if parent.time < cfg.subnet_evm {
            return Ok(None);
        }
        let mut st = self.store.state_at(parent.number)?;
        let fc = self.fee_config_at(parent, st.as_mut())?;
        if parent.number == 0 {
            return Ok(Some(fc.min_base_fee));
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
        let mut base = parent_base;
        if total != target {
            let (diff, up) = if total > target { (total - target, true) } else { (target - total, false) };
            let mut delta = (U256::from(diff) * parent_base / U256::from(target) / fc.base_fee_change_denominator).max(U256::from(1));
            if !up {
                let windows = elapsed / WINDOW_LEN as u64;
                if windows > 1 {
                    delta *= U256::from(windows);
                }
            }
            base = if up { base.saturating_add(delta) } else { base.saturating_sub(delta) };
        }
        Ok(Some(base.max(fc.min_base_fee)))
    }
}

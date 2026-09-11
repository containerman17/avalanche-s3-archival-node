//! SAE (ACP-194) projection and light verify: the admission side of Streaming
//! Asynchronous Execution. No execution here; the executor produces the settled
//! state, this tracks the accepted-but-unsettled backlog so `accept` and
//! `verify_light` never read unsettled state and never execute. See rs/SAE.md.
//!
//! Per sender: the SETTLED nonce and balance, plus the running count and
//! worst-case cost of that sender's accepted-but-unsettled txs. The invariant
//! `projected_nonce = settled_nonce + unsettled_count` is preserved across
//! accept (advances) and settle (retires). A sender first seen at accept with
//! no baseline is seeded to the tx's own nonce (its correct chain nonce at that
//! height); genesis-alloc accounts are seeded at open. A sender is dropped once
//! it has no unsettled tx left, so the map is bounded by the in-flight set; a
//! reappearance re-seeds correctly.

use std::collections::HashMap;

use alloy_primitives::{Address, U256};
use block::Tx;

/// Per sender, (projected nonce, cumulative worst-case cost) across an
/// unaccepted ancestor chain: the block tree's verified-but-not-accepted delta
/// on top of the projection, so `verify_light_over` pipelines siblings without
/// executed state. An empty overlay = the parent is the accepted head.
pub type Overlay = HashMap<Address, (u64, U256)>;

/// tx worst-case cost: gasLimit * feeCap + value (feeCap = the tx's max fee per
/// gas). Saturating: an overflow reads as unpayable and fails the funds check.
pub fn cost_of(t: &Tx) -> U256 {
    U256::from(t.gas_limit).saturating_mul(U256::from(t.gas_price)).saturating_add(t.value)
}

/// The ACP-194 per-tx charged-gas floor (MinimumGasConsumption): a tx is
/// charged at least ceil(limit/2) by the settled gas clock even if it uses
/// less. Not an admission rule (worst case is the full limit); exposed for the
/// gas clock (P2).
pub fn charged_gas_floor(gas_limit: u64) -> u64 {
    (gas_limit + 1) / 2
}

/// Why verify_light refused a block (valid/invalid only, with the offending
/// position for the log).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reject {
    /// tx `index`'s signature did not recover (no sender).
    BadSig(usize),
    /// tx `index` has nonce `got`, its projected nonce is `want`.
    NonceGap { index: usize, want: u64, got: u64 },
    /// tx `index` overruns the sender's worst-case funds over the unsettled sequence.
    Underfunded(usize),
    /// sum of tx gas limits exceeds the block gas capacity (20s x target).
    OverCapacity { used: u64, capacity: u64 },
    /// block byte size exceeds the size cap.
    OverSize { size: usize, cap: usize },
}

#[derive(Clone)]
struct Sender {
    settled_nonce: u64,
    settled_balance: U256,
    unsettled_count: u64,
    unsettled_cost: U256,
}

/// The projection over the accepted-but-unsettled chain.
#[derive(Default)]
pub struct Projection {
    senders: HashMap<Address, Sender>,
    pub settled_head: u64,
    pub accepted_head: u64,
}

impl Projection {
    pub fn new() -> Projection {
        Projection::default()
    }

    /// Seed a sender's settled baseline (genesis alloc at open, or the
    /// executor's post-settle account state).
    pub fn set_settled(&mut self, addr: Address, nonce: u64, balance: U256) {
        let s = self.senders.entry(addr).or_insert(Sender { settled_nonce: nonce, settled_balance: balance, unsettled_count: 0, unsettled_cost: U256::ZERO });
        s.settled_nonce = nonce;
        s.settled_balance = balance;
    }

    /// The projected nonce of a sender (settled nonce + its unsettled txs).
    pub fn projected_nonce(&self, addr: &Address) -> Option<u64> {
        self.senders.get(addr).map(|s| s.settled_nonce + s.unsettled_count)
    }

    /// The pool's admission baseline for a tracked sender: (projected nonce,
    /// settled balance), the same pair verify_light checks against. None if the
    /// sender is not tracked, OR its settled_balance is still the U256::MAX
    /// placeholder (accepted but not yet settled with a real balance): those
    /// callers must read the real backend balance to keep the funds gate honest.
    /// Serving this from the projection lets pool.add avoid the engine lock the
    /// continuous executor holds for a whole block.
    pub fn settled_baseline(&self, addr: &Address) -> Option<(u64, U256)> {
        let s = self.senders.get(addr)?;
        if s.settled_balance == U256::MAX {
            return None;
        }
        Some((s.settled_nonce + s.unsettled_count, s.settled_balance))
    }

    /// The count of a sender's accepted-but-unsettled txs (0 if untracked): the
    /// gap between its settled nonce and its projected nonce, which the pool
    /// adds to the settled-state nonce to admit against the projected nonce.
    pub fn unsettled_count(&self, addr: &Address) -> u64 {
        self.senders.get(addr).map_or(0, |s| s.unsettled_count)
    }

    /// The settled head / accepted head the projection has advanced to.
    pub fn heads(&self) -> (u64, u64) {
        (self.settled_head, self.accepted_head)
    }

    /// Number of senders currently tracked (in-flight set; a leak watch).
    pub fn tracked(&self) -> usize {
        self.senders.len()
    }

    /// verify_light over an overlay of unaccepted ancestor blocks (the block
    /// tree's verified-but-not-accepted chain): the same checks, but a sender's
    /// baseline is advanced by the ancestors' txs already, so a chain of
    /// verified blocks pipelines to OptimalProcessing depth without touching
    /// executed state. `parent` is the ancestor chain's overlay (empty when the
    /// block's parent is the accepted head); the returned overlay is `parent`
    /// plus this block's txs, to pass to a child's verify. Read-only on the
    /// projection. The plugin's Verify uses this; `verify_light` is the empty-
    /// overlay case (the bench's single-stream feed).
    ///
    /// An overlay entry is (projected nonce, cumulative worst-case cost) for a
    /// sender across the unsettled backlog plus the ancestor chain. Balance is
    /// read from the projection's settled baseline (a sender with no baseline is
    /// unfunded-unknown = U256::MAX, the funds check skipped, exactly as
    /// verify_light seeds a new sender).
    pub fn verify_light_over(&self, txs: &[Tx], parent: &Overlay, size_bytes: usize, capacity: u64, size_cap: usize) -> Result<Overlay, Reject> {
        let mut ov = parent.clone();
        let mut total_gas: u64 = 0;
        for (i, t) in txs.iter().enumerate() {
            let sender = t.sender.ok_or(Reject::BadSig(i))?;
            let (proj_nonce, cost_acc) = *ov.entry(sender).or_insert_with(|| match self.senders.get(&sender) {
                Some(s) => (s.settled_nonce + s.unsettled_count, s.unsettled_cost),
                None => (t.nonce, U256::ZERO),
            });
            if t.nonce != proj_nonce {
                return Err(Reject::NonceGap { index: i, want: proj_nonce, got: t.nonce });
            }
            let balance = self.senders.get(&sender).map_or(U256::MAX, |s| s.settled_balance);
            let committed = cost_acc.saturating_add(cost_of(t));
            if committed > balance {
                return Err(Reject::Underfunded(i));
            }
            ov.insert(sender, (proj_nonce + 1, committed));
            total_gas = total_gas.saturating_add(t.gas_limit);
        }
        if total_gas > capacity {
            return Err(Reject::OverCapacity { used: total_gas, capacity });
        }
        if size_bytes > size_cap {
            return Err(Reject::OverSize { size: size_bytes, cap: size_cap });
        }
        Ok(ov)
    }

    /// verify_light: signatures, projected nonce per tx position, worst-case
    /// funds cumulative over each sender's unsettled sequence, block gas
    /// capacity and byte size. NO execution, NO unsettled state read. Read-only
    /// (does not advance the projection; call `accept_block` after). Valid = Ok.
    pub fn verify_light(&self, txs: &[Tx], size_bytes: usize, capacity: u64, size_cap: usize) -> Result<(), Reject> {
        // Per sender, the baseline (settled nonce/balance + already-accepted
        // unsettled txs) captured ONCE on first encounter, plus the running
        // count and cost of that sender's earlier txs in THIS block. An unseen
        // sender is seeded to its FIRST tx's nonce here (verify_light is
        // read-only, so the seed must live in `local`, not in `self.senders`).
        struct Local {
            base_nonce: u64,
            base_count: u64,
            base_cost: U256,
            base_balance: U256,
            extra_count: u64,
            extra_cost: U256,
        }
        let mut local: HashMap<Address, Local> = HashMap::new();
        let mut total_gas: u64 = 0;
        for (i, t) in txs.iter().enumerate() {
            let sender = t.sender.ok_or(Reject::BadSig(i))?;
            let e = local.entry(sender).or_insert_with(|| match self.senders.get(&sender) {
                Some(s) => Local { base_nonce: s.settled_nonce, base_count: s.unsettled_count, base_cost: s.unsettled_cost, base_balance: s.settled_balance, extra_count: 0, extra_cost: U256::ZERO },
                None => Local { base_nonce: t.nonce, base_count: 0, base_cost: U256::ZERO, base_balance: U256::MAX, extra_count: 0, extra_cost: U256::ZERO },
            });
            let projected = e.base_nonce + e.base_count + e.extra_count;
            if t.nonce != projected {
                return Err(Reject::NonceGap { index: i, want: projected, got: t.nonce });
            }
            let cost = cost_of(t);
            let committed = e.base_cost.saturating_add(e.extra_cost).saturating_add(cost);
            if committed > e.base_balance {
                return Err(Reject::Underfunded(i));
            }
            e.extra_count += 1;
            e.extra_cost = e.extra_cost.saturating_add(cost);
            total_gas = total_gas.saturating_add(t.gas_limit);
        }
        if total_gas > capacity {
            return Err(Reject::OverCapacity { used: total_gas, capacity });
        }
        if size_bytes > size_cap {
            return Err(Reject::OverSize { size: size_bytes, cap: size_cap });
        }
        Ok(())
    }

    /// Advance the projection by an accepted block's txs (projected nonce and
    /// worst-case cost per sender). A new sender is seeded to its first tx's nonce.
    pub fn accept_block(&mut self, height: u64, txs: &[Tx]) {
        for t in txs {
            let Some(sender) = t.sender else { continue };
            let s = self.senders.entry(sender).or_insert(Sender { settled_nonce: t.nonce, settled_balance: U256::MAX, unsettled_count: 0, unsettled_cost: U256::ZERO });
            s.unsettled_count += 1;
            s.unsettled_cost = s.unsettled_cost.saturating_add(cost_of(t));
        }
        self.accepted_head = height;
    }

    /// Retire a settled block's txs: advance the settled nonce, drop the
    /// worst-case cost, and drop a sender that has no unsettled tx left (bounds
    /// the map to the in-flight set). `balances`, when given, overwrites the
    /// settled balance from the executor's post-settle state.
    pub fn settle_block(&mut self, height: u64, txs: &[Tx], balances: Option<&HashMap<Address, (u64, U256)>>) {
        for t in txs {
            let Some(sender) = t.sender else { continue };
            if let Some(s) = self.senders.get_mut(&sender) {
                s.unsettled_count = s.unsettled_count.saturating_sub(1);
                s.unsettled_cost = s.unsettled_cost.saturating_sub(cost_of(t));
                s.settled_nonce += 1;
            }
        }
        if let Some(b) = balances {
            for (addr, (nonce, bal)) in b {
                if let Some(s) = self.senders.get_mut(addr) {
                    s.settled_nonce = *nonce;
                    s.settled_balance = *bal;
                }
            }
        }
        // Drop senders with nothing in flight.
        self.senders.retain(|_, s| s.unsettled_count > 0);
        self.settled_head = height;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn addr(b: u8) -> Address {
        Address::from([b; 20])
    }

    /// A minimal tx: only the fields verify_light / the projection read.
    fn tx(sender: u8, nonce: u64, gas_limit: u64, gas_price: u128, value: u64) -> Tx {
        Tx {
            raw: Default::default(),
            hash: Default::default(),
            sender: Some(addr(sender)),
            tx_type: 2,
            chain_id: Some(1),
            nonce,
            gas_price,
            gas_tip: gas_price,
            gas_limit,
            to: Some(addr(0xff)),
            value: U256::from(value),
            input: Default::default(),
            access_list: Vec::new(),
            v: 0,
            r: U256::ZERO,
            s: U256::ZERO,
            recid: 0,
            body_off: 0,
            sig_off: 0,
        }
    }

    const CAP: u64 = u64::MAX;
    const SIZE: usize = usize::MAX;

    #[test]
    fn projected_nonce_advances_on_accept_and_retires_on_settle() {
        let mut p = Projection::new();
        p.set_settled(addr(1), 5, U256::MAX);
        // Two blocks, one tx of sender 1 each.
        let b1 = [tx(1, 5, 21_000, 1, 0)];
        let b2 = [tx(1, 6, 21_000, 1, 0)];
        assert!(p.verify_light(&b1, 0, CAP, SIZE).is_ok());
        p.accept_block(1, &b1);
        assert_eq!(p.projected_nonce(&addr(1)), Some(6));
        assert!(p.verify_light(&b2, 0, CAP, SIZE).is_ok());
        p.accept_block(2, &b2);
        assert_eq!(p.projected_nonce(&addr(1)), Some(7));
        assert_eq!((p.accepted_head, p.settled_head), (2, 0));
        // Settle block 1: settled nonce 5 -> 6, one unsettled left.
        p.settle_block(1, &b1, None);
        assert_eq!(p.projected_nonce(&addr(1)), Some(7));
        assert_eq!(p.settled_head, 1);
        // Settle block 2: last unsettled retires, sender dropped.
        p.settle_block(2, &b2, None);
        assert_eq!(p.projected_nonce(&addr(1)), None);
        assert_eq!(p.tracked(), 0);
    }

    /// One sender funds 20 consecutive nonces, all accepted, then settled a few
    /// at a time: projected_nonce is invariant across every settle (settled nonce
    /// advances by exactly the count the settle retires), and ends at 20. Pins
    /// the "nonce too low" class of bug (the expected nonce over-advancing).
    #[test]
    fn projected_nonce_is_invariant_across_accept_then_settle() {
        let mut p = Projection::new();
        p.set_settled(addr(1), 0, U256::MAX);
        // 20 blocks, one tx of sender 1 each (nonces 0..20).
        let blocks: Vec<Vec<Tx>> = (0..20).map(|i| vec![tx(1, i, 21_000, 1, 0)]).collect();
        for (i, b) in blocks.iter().enumerate() {
            p.accept_block(i as u64 + 1, b);
        }
        assert_eq!(p.projected_nonce(&addr(1)), Some(20));
        assert_eq!(p.unsettled_count(&addr(1)), 20);
        // Settle in chunks of 3; projected stays 20 the whole way, unsettled
        // shrinks by exactly the settled count, settled nonce grows to match.
        let mut settled = 0u64;
        for (i, b) in blocks.iter().enumerate() {
            // The executor overwrites the settled nonce from the post-block
            // backend state: after settling block i+1, sender 1's nonce is i+1.
            let bal: std::collections::HashMap<Address, (u64, U256)> = [(addr(1), (i as u64 + 1, U256::MAX))].into_iter().collect();
            p.settle_block(i as u64 + 1, b, Some(&bal));
            settled += 1;
            if settled < 20 {
                // Still in flight: projected is invariant, unsettled shrinks by
                // exactly the settled count.
                assert_eq!(p.projected_nonce(&addr(1)), Some(20), "projected nonce moved at settle {}", i + 1);
                assert_eq!(p.unsettled_count(&addr(1)), 20 - settled);
            }
        }
        // Fully settled: the sender drops (nothing in flight), projected == chain nonce.
        assert_eq!(p.projected_nonce(&addr(1)), None);
        assert_eq!(p.settled_head, 20);
    }

    #[test]
    fn worst_case_funds_span_the_unsettled_sequence() {
        let mut p = Projection::new();
        // Balance covers exactly two 50-gas-price x 21k transfers of value 1.
        let cost = 21_000u128 * 50 + 1;
        p.set_settled(addr(2), 0, U256::from(2 * cost));
        // First two txs (across two blocks) fit; the third overruns.
        let a = [tx(2, 0, 21_000, 50, 1)];
        assert!(p.verify_light(&a, 0, CAP, SIZE).is_ok());
        p.accept_block(1, &a);
        let b = [tx(2, 1, 21_000, 50, 1)];
        assert!(p.verify_light(&b, 0, CAP, SIZE).is_ok());
        p.accept_block(2, &b);
        // Third tx: settled balance already committed to the first two.
        let c = [tx(2, 2, 21_000, 50, 1)];
        assert_eq!(p.verify_light(&c, 0, CAP, SIZE), Err(Reject::Underfunded(0)));
        // Two txs in one block, the second overruns.
        let mut q = Projection::new();
        q.set_settled(addr(3), 0, U256::from(2 * cost));
        let two = [tx(3, 0, 21_000, 50, 1), tx(3, 1, 21_000, 50, 1)];
        assert!(q.verify_light(&two, 0, CAP, SIZE).is_ok());
        let three = [tx(3, 0, 21_000, 50, 1), tx(3, 1, 21_000, 50, 1), tx(3, 2, 21_000, 50, 1)];
        assert_eq!(q.verify_light(&three, 0, CAP, SIZE), Err(Reject::Underfunded(2)));
    }

    #[test]
    fn settled_baseline_serves_tracked_senders_and_misses_placeholders() {
        let mut p = Projection::new();
        // Untracked sender: miss.
        assert_eq!(p.settled_baseline(&addr(1)), None);
        // Tracked with a real settled balance: (projected nonce, balance).
        p.set_settled(addr(1), 5, U256::from(100));
        let b = [tx(1, 5, 21_000, 1, 0)];
        p.accept_block(1, &b);
        assert_eq!(p.settled_baseline(&addr(1)), Some((6, U256::from(100))));
        // Accepted but not yet settled with a real balance (U256::MAX placeholder): miss.
        let c = [tx(2, 0, 21_000, 1, 0)];
        p.accept_block(2, &c);
        assert_eq!(p.projected_nonce(&addr(2)), Some(1));
        assert_eq!(p.settled_baseline(&addr(2)), None);
    }

    #[test]
    fn verify_light_rejects_a_nonce_gap() {
        let mut p = Projection::new();
        p.set_settled(addr(4), 10, U256::MAX);
        // Gap against the settled nonce.
        let gap = [tx(4, 11, 21_000, 1, 0)];
        assert_eq!(p.verify_light(&gap, 0, CAP, SIZE), Err(Reject::NonceGap { index: 0, want: 10, got: 11 }));
        // Gap within one block (second tx skips a nonce).
        let within = [tx(4, 10, 21_000, 1, 0), tx(4, 12, 21_000, 1, 0)];
        assert_eq!(p.verify_light(&within, 0, CAP, SIZE), Err(Reject::NonceGap { index: 1, want: 11, got: 12 }));
    }

    #[test]
    fn verify_light_rejects_an_over_capacity_block() {
        let p = Projection::new();
        let txs = [tx(5, 0, 8_000_000, 1, 0), tx(6, 0, 8_000_000, 1, 0)];
        assert_eq!(p.verify_light(&txs, 0, 15_000_000, SIZE), Err(Reject::OverCapacity { used: 16_000_000, capacity: 15_000_000 }));
        // Under capacity passes (unseen senders seed to their own nonce).
        assert!(p.verify_light(&txs, 0, 16_000_000, SIZE).is_ok());
        // Over the byte size cap.
        assert_eq!(p.verify_light(&txs, 2000, 16_000_000, 1000), Err(Reject::OverSize { size: 2000, cap: 1000 }));
    }

    #[test]
    fn verify_light_handles_a_new_sender_with_several_txs_in_one_block() {
        // A brand-new sender (absent from the projection) with a contiguous run
        // in one block: each tx must project off the FIRST tx's nonce, not its
        // own. Regression for the per-tx re-seed bug.
        let p = Projection::new();
        let run = [tx(8, 12, 21_000, 1, 0), tx(8, 13, 21_000, 1, 0), tx(8, 14, 21_000, 1, 0)];
        assert!(p.verify_light(&run, 0, CAP, SIZE).is_ok());
        // A gap within a new sender's run is still caught.
        let gap = [tx(8, 12, 21_000, 1, 0), tx(8, 14, 21_000, 1, 0)];
        assert_eq!(p.verify_light(&gap, 0, CAP, SIZE), Err(Reject::NonceGap { index: 1, want: 13, got: 14 }));
    }

    /// The overlay pipelines a chain of verified-but-not-accepted blocks: each
    /// child verifies against its ancestors' projection delta, not the accepted
    /// state, so heights in flight exceed 1 without executed state. The result
    /// matches accepting the ancestors then verify_light on the child.
    #[test]
    fn verify_light_over_pipelines_the_unaccepted_chain() {
        let mut p = Projection::new();
        p.set_settled(addr(1), 5, U256::MAX);
        p.set_settled(addr(2), 0, U256::from(10u64) * U256::from(21_000u64 * 50 + 1));
        // Block h+1 (sender 1 nonce 5, sender 2 nonce 0), verified on the head.
        let b1 = [tx(1, 5, 21_000, 1, 0), tx(2, 0, 21_000, 50, 1)];
        let ov1 = p.verify_light_over(&b1, &Overlay::new(), 0, CAP, SIZE).unwrap();
        assert_eq!(ov1[&addr(1)].0, 6);
        assert_eq!(ov1[&addr(2)].0, 1);
        // Block h+2 on top of the still-unaccepted h+1: nonces continue.
        let b2 = [tx(1, 6, 21_000, 1, 0), tx(2, 1, 21_000, 50, 1)];
        let ov2 = p.verify_light_over(&b2, &ov1, 0, CAP, SIZE).unwrap();
        assert_eq!(ov2[&addr(1)].0, 7);
        assert_eq!(ov2[&addr(2)].0, 2);
        // A gap against the ancestor chain is caught (h+2 must start at 6, not 7).
        let gap = [tx(1, 7, 21_000, 1, 0)];
        assert_eq!(p.verify_light_over(&gap, &ov1, 0, CAP, SIZE), Err(Reject::NonceGap { index: 0, want: 6, got: 7 }));
        // The overlay carries the funds cost: sender 2's balance covers 10 txs;
        // after 2 in flight, the cumulative cost is 2 units, not reset per block.
        assert_eq!(ov2[&addr(2)].1, U256::from(2u64) * U256::from(21_000u64 * 50 + 1));
        // Accepting the ancestors then verify_light on the child agrees with the
        // overlay path (the projection advanced by accept == the overlay delta).
        p.accept_block(1, &b1);
        let ov2b = p.verify_light_over(&b2, &Overlay::new(), 0, CAP, SIZE).unwrap();
        assert_eq!(ov2b[&addr(1)].0, ov2[&addr(1)].0);
        assert_eq!(ov2b[&addr(2)].0, ov2[&addr(2)].0);
    }

    #[test]
    fn verify_light_rejects_a_bad_signature() {
        let p = Projection::new();
        let mut t = tx(7, 0, 21_000, 1, 0);
        t.sender = None;
        assert_eq!(p.verify_light(&[t], 0, CAP, SIZE), Err(Reject::BadSig(0)));
    }

    /// Recovery: re-executing the accepted-but-unsettled backlog from the last
    /// settled state rebuilds the projection to the same projected nonces.
    #[test]
    fn recovery_rebuilds_the_projection_from_the_backlog() {
        // A clean run: accept 1..5, settle 1..2 (k = 3), for two senders.
        let blocks: Vec<Vec<Tx>> = (0..5).map(|i| vec![tx(1, i, 21_000, 1, 0), tx(2, i, 21_000, 1, 0)]).collect();
        let mut clean = Projection::new();
        clean.set_settled(addr(1), 0, U256::MAX);
        clean.set_settled(addr(2), 0, U256::MAX);
        for (i, b) in blocks.iter().enumerate() {
            clean.accept_block(i as u64 + 1, b);
        }
        clean.settle_block(1, &blocks[0], None);
        clean.settle_block(2, &blocks[1], None);
        let want1 = clean.projected_nonce(&addr(1));
        let want2 = clean.projected_nonce(&addr(2));
        assert_eq!((want1, want2), (Some(5), Some(5)));

        // Crash and reopen: durable state is the settled head (2) and its
        // settled nonces; the backlog 3..5 is re-executed forward.
        let mut recovered = Projection::new();
        // Settled state at height 2: each sender's nonce advanced by 2.
        recovered.set_settled(addr(1), 2, U256::MAX);
        recovered.set_settled(addr(2), 2, U256::MAX);
        recovered.settled_head = 2;
        for i in 2..5 {
            // verify_light must pass on replay (the backlog is a valid chain).
            assert!(recovered.verify_light(&blocks[i], 0, CAP, SIZE).is_ok(), "replay block {}", i + 1);
            recovered.accept_block(i as u64 + 1, &blocks[i]);
        }
        assert_eq!(recovered.projected_nonce(&addr(1)), want1);
        assert_eq!(recovered.projected_nonce(&addr(2)), want2);
        assert_eq!(recovered.accepted_head, 5);
    }
}

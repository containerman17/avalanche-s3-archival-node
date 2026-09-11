//! The hot state: the latest flat EVM state in memory, keyed by keccak of the
//! address and of the slot (the bootstrap export has no preimages), one map per
//! contract for its slots (measured 2026-09-12 on bot384: 11 ns hot, 56 ns cold,
//! against 109/229 for one flat map, see the design doc).
//!
//! One writer (the block applier), any number of readers. `papaya` gives
//! lock-free reads that never block the writer; the seqlock `seq` tells a
//! reader whether the value it got belongs to the generation it asked for:
//! `seq` is odd while an apply is in progress, so a reader that checks
//! `seq == g.seq` before and after its lookup either saw generation `g` whole
//! or reports `Stale`.

use crate::{Generation, Stale};
use alloy_primitives::{keccak256, Address, B256, U256};
use papaya::HashMap;
use std::hash::{BuildHasher, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

/// keccak256 output of an address or a slot.
pub type H = [u8; 32];

/// Keys are keccak output: xor-fold the 8-byte words and call that the hash.
#[derive(Default, Clone)]
pub struct Take8;
pub struct Take8Hasher(u64);
impl Hasher for Take8Hasher {
    fn finish(&self) -> u64 {
        self.0
    }
    fn write(&mut self, b: &[u8]) {
        for c in b.chunks_exact(8) {
            self.0 ^= u64::from_le_bytes(c.try_into().unwrap());
        }
    }
    fn write_usize(&mut self, _: usize) {}
}
impl BuildHasher for Take8 {
    type Hasher = Take8Hasher;
    fn build_hasher(&self) -> Take8Hasher {
        Take8Hasher(0)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Account {
    pub nonce: u64,
    pub balance: U256,
    pub code_hash: B256,
    /// coreth's IsMultiCoin extra: part of the trie leaf, sticky per account.
    pub multicoin: bool,
}

pub type SlotMap = HashMap<H, U256, Take8>;

/// One block's post-values. `None` account = deleted (its storage goes too).
/// A zero slot value = deleted slot.
#[derive(Default, Debug)]
pub struct Diff {
    pub accounts: Vec<(H, Option<Account>)>,
    pub storage: Vec<(H, H, U256)>,
    pub code: Vec<(B256, Arc<[u8]>)>,
}

pub struct HotState {
    accounts: HashMap<H, Account, Take8>,
    storage: HashMap<H, Arc<SlotMap>, Take8>,
    code: HashMap<B256, Arc<[u8]>, Take8>,
    seq: AtomicU64,
    /// height and hash of the current generation; written under the odd seq.
    at: RwLock<(u64, B256)>,
}

impl HotState {
    pub fn new(height: u64, hash: B256) -> Self {
        HotState {
            accounts: HashMap::with_hasher(Take8),
            storage: HashMap::with_hasher(Take8),
            code: HashMap::with_hasher(Take8),
            seq: AtomicU64::new(0),
            at: RwLock::new((height, hash)),
        }
    }

    pub fn generation(&self) -> Generation {
        loop {
            let seq = self.seq.load(Ordering::Acquire);
            let (height, hash) = *self.at.read().unwrap();
            if seq % 2 == 0 && self.seq.load(Ordering::Acquire) == seq {
                return Generation { height, hash, seq };
            }
            std::hint::spin_loop();
        }
    }

    fn check(&self, g: Generation) -> Result<(), Stale> {
        if self.seq.load(Ordering::Acquire) == g.seq {
            Ok(())
        } else {
            Err(Stale)
        }
    }

    pub fn account(&self, g: Generation, addr_hash: &H) -> Result<Option<Account>, Stale> {
        self.check(g)?;
        let v = self.accounts.pin().get(addr_hash).copied();
        self.check(g)?;
        Ok(v)
    }

    pub fn storage(&self, g: Generation, addr_hash: &H, slot_hash: &H) -> Result<U256, Stale> {
        self.check(g)?;
        let v = self
            .storage
            .pin()
            .get(addr_hash)
            .and_then(|m| m.pin().get(slot_hash).copied())
            .unwrap_or(U256::ZERO);
        self.check(g)?;
        Ok(v)
    }

    pub fn code(&self, hash: &B256) -> Option<Arc<[u8]>> {
        self.code.pin().get(hash).cloned()
    }

    pub fn len(&self) -> (usize, usize, usize) {
        (self.accounts.len(), self.storage.pin().iter().map(|(_, m)| m.len()).sum(), self.code.len())
    }

    /// Bulk load (import): no generation dance, the state is not readable yet.
    pub fn put_account(&self, addr_hash: H, a: Account) {
        self.accounts.pin().insert(addr_hash, a);
    }
    pub fn put_slot(&self, addr_hash: H, slot_hash: H, v: U256) {
        let st = self.storage.pin();
        let m = match st.get(&addr_hash) {
            Some(m) => m.clone(),
            None => st.get_or_insert_with(addr_hash, || Arc::new(SlotMap::with_hasher(Take8))).clone(),
        };
        m.pin().insert(slot_hash, v);
    }
    pub fn put_code(&self, hash: B256, code: Arc<[u8]>) {
        self.code.pin().insert(hash, code);
    }

    /// The single writer applies one block: seq goes odd, the maps change,
    /// the generation advances, seq goes even.
    pub fn apply(&self, height: u64, hash: B256, d: &Diff) {
        let s = self.seq.fetch_add(1, Ordering::AcqRel);
        debug_assert!(s % 2 == 0, "apply while applying");
        for (h, c) in &d.code {
            self.code.pin().insert(*h, c.clone());
        }
        {
            let acc = self.accounts.pin();
            let st = self.storage.pin();
            for (k, a) in &d.accounts {
                match a {
                    Some(a) => {
                        acc.insert(*k, *a);
                    }
                    None => {
                        acc.remove(k);
                        st.remove(k);
                    }
                }
            }
        }
        for (a, k, v) in &d.storage {
            if v.is_zero() {
                if let Some(m) = self.storage.pin().get(a) {
                    m.pin().remove(k);
                }
            } else {
                self.put_slot(*a, *k, *v);
            }
        }
        *self.at.write().unwrap() = (height, hash);
        self.seq.fetch_add(1, Ordering::AcqRel);
    }
}

/// The contract rows of a diff in rs/state's key form (the checker's and the
/// history log's shape): account = keccak(addr)+0x00 -> RLP[nonce, balance,
/// codeHash, multicoin], slot = keccak(addr)+0x01+keccak(slot) -> trimmed word,
/// empty value = delete. Account rows first (a delete must precede its slots' recreate).
pub fn rows(d: &Diff) -> Vec<(Vec<u8>, Vec<u8>)> {
    use alloy_rlp::Encodable;
    let mut out = Vec::with_capacity(d.accounts.len() + d.storage.len());
    for (h, a) in &d.accounts {
        let mut k = Vec::with_capacity(33);
        k.extend_from_slice(h);
        k.push(0);
        let v = match a {
            None => Vec::new(),
            Some(a) => {
                let mut v = Vec::with_capacity(80);
                let len = a.nonce.length() + a.balance.length() + a.code_hash.length() + a.multicoin.length();
                alloy_rlp::Header { list: true, payload_length: len }.encode(&mut v);
                a.nonce.encode(&mut v);
                a.balance.encode(&mut v);
                a.code_hash.encode(&mut v);
                a.multicoin.encode(&mut v);
                v
            }
        };
        out.push((k, v));
    }
    for (a, s, v) in &d.storage {
        let mut k = Vec::with_capacity(65);
        k.extend_from_slice(a);
        k.push(1);
        k.extend_from_slice(s);
        out.push((k, if v.is_zero() { Vec::new() } else { v.to_be_bytes_trimmed_vec() }));
    }
    out
}

pub fn addr_hash(a: &Address) -> H {
    keccak256(a.as_slice()).0
}
pub fn slot_hash(k: &U256) -> H {
    keccak256(k.to_be_bytes::<32>()).0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generations_and_stale() {
        let hs = HotState::new(10, B256::ZERO);
        let a = addr_hash(&Address::repeat_byte(1));
        let k = slot_hash(&U256::from(7));
        hs.put_account(a, Account { nonce: 1, balance: U256::from(5), code_hash: B256::ZERO, multicoin: false });
        hs.put_slot(a, k, U256::from(9));
        let g = hs.generation();
        assert_eq!(g.seq, 0);
        assert_eq!(hs.account(g, &a).unwrap().unwrap().nonce, 1);
        assert_eq!(hs.storage(g, &a, &k).unwrap(), U256::from(9));
        assert_eq!(hs.storage(g, &a, &[0u8; 32]).unwrap(), U256::ZERO);

        let d = Diff {
            accounts: vec![(a, Some(Account { nonce: 2, balance: U256::from(6), code_hash: B256::ZERO, multicoin: true }))],
            storage: vec![(a, k, U256::ZERO), (a, [3u8; 32], U256::from(4))],
            code: vec![],
        };
        let r = rows(&d);
        assert_eq!(r.len(), 3);
        assert_eq!(r[0].0.len(), 33);
        assert_eq!(r[0].1, alloy_primitives::hex!("e40206a00000000000000000000000000000000000000000000000000000000000000000" "01").to_vec());
        assert_eq!(r[1].0.len(), 65);
        assert!(r[1].1.is_empty());
        assert_eq!(r[2].1, vec![4]);
        hs.apply(11, B256::repeat_byte(0xaa), &d);
        assert_eq!(hs.account(g, &a), Err(Stale));
        let g2 = hs.generation();
        assert_eq!((g2.height, g2.seq), (11, 2));
        assert_eq!(hs.account(g2, &a).unwrap().unwrap().nonce, 2);
        assert_eq!(hs.storage(g2, &a, &k).unwrap(), U256::ZERO);
        assert_eq!(hs.storage(g2, &a, &[3u8; 32]).unwrap(), U256::from(4));

        hs.apply(12, B256::ZERO, &Diff { accounts: vec![(a, None)], ..Default::default() });
        let g3 = hs.generation();
        assert_eq!(hs.account(g3, &a).unwrap(), None);
        assert_eq!(hs.storage(g3, &a, &[3u8; 32]).unwrap(), U256::ZERO);
        assert_eq!(hs.len(), (0, 0, 0));
    }
}

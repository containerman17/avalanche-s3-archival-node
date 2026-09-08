//! The state root over the whole in-memory state: a full secure-trie recompute
//! with alloy-trie (keccak(addr) -> RLP account with keccak(slot) storage tries).
//! Checkpoint oracle only; the node's root is commit's.

use crate::exec::Db;
use alloy_primitives::{B256, U256};
use alloy_trie::{root::{state_root_unhashed, storage_root_unhashed}, TrieAccount};
use revm::database::AccountState;

pub fn state_root(db: &Db) -> B256 {
    let accounts = db.cache.accounts.iter().filter_map(|(addr, a)| {
        if a.account_state == AccountState::NotExisting || a.info.is_empty() {
            return None;
        }
        let storage_root = storage_root_unhashed(
            a.storage.iter().filter(|(_, v)| !v.is_zero()).map(|(k, v)| (B256::from(*k), *v)),
        );
        Some((
            *addr,
            TrieAccount { nonce: a.info.nonce, balance: a.info.balance, storage_root, code_hash: a.info.code_hash },
        ))
    });
    state_root_unhashed(accounts)
}

/// Number of accounts and non-zero slots in the state (bench line).
pub fn size(db: &Db) -> (usize, usize) {
    let mut n = 0;
    let mut s = 0;
    for a in db.cache.accounts.values() {
        if a.account_state == AccountState::NotExisting || a.info.is_empty() {
            continue;
        }
        n += 1;
        s += a.storage.values().filter(|v| **v != U256::ZERO).count();
    }
    (n, s)
}

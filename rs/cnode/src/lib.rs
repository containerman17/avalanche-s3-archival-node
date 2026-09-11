//! cnode: an in-process follower of Avalanche mainnet C-chain for one host process.
//! Design of record: ~/dotfiles/projects/archival-node/assets/defi_node_handover_task.md
//! Running notes: NOTES.md next to this crate.

pub mod checker;
pub mod exec;
pub mod feed;
pub mod history;
pub mod hot;
pub mod import;
pub mod mempool;
pub mod node;
pub use node::{BlockEvent, Node};

use alloy_primitives::B256;
use std::path::PathBuf;

/// A block generation: what the hot state is at, plus a counter that changes
/// on every apply. Readers tag their work with it and compare.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Generation {
    pub height: u64,
    pub hash: B256,
    /// Even while a generation is readable, odd while the applier is writing
    /// the next one (a seqlock; see `hot::HotState`).
    pub seq: u64,
}

/// The generation a read started against is no longer the current one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Stale;

impl std::fmt::Display for Stale {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("stale generation")
    }
}
impl std::error::Error for Stale {}

#[derive(Clone, Debug)]
pub struct Config {
    pub rpc_ws: String,
    pub rpc_http: String,
    pub validator_ws: Vec<String>,
    pub data_dir: PathBuf,
    /// The cmd/cnode-export output: the import when data_dir is empty, and
    /// code.bin on every restart (the rolled run holds no code).
    pub bootstrap_dir: PathBuf,
    pub checker_lag_blocks: u64,
    pub snapshot_every_blocks: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mode {
    Tip,
    AtHeight(u64),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Status {
    Following { height: u64, lag_ms: u64 },
    Frozen { height: u64 },
    Halted { height: u64, expected_root: B256, got_root: B256 },
}

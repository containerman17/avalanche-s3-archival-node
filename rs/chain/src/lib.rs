//! epochdb-chain: the subnet-evm engine behind both shells, the rpcchainvm
//! plugin (`rs/plugin`) and the C ABI (`rs/ffi`): parse, verify (the state
//! root inline in NormalOp), accept, reject, build, the block tree of
//! verified-not-accepted blocks, the archival store and the RPC surface.
pub mod build;
pub mod config;
pub mod dbstore;
pub mod layered;
pub mod node_engine;
pub mod pool;
pub mod rpc_store;
pub mod synth;
pub mod tree;

#[cfg(test)]
mod tests;

/// Tests that touch the process environment (config::apply sets EPOCHDB_*
/// variables the store reads) serialize on this.
#[cfg(test)]
pub(crate) static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

pub use node_engine::{Init, NodeEngine};
pub use tree::{Engine, Error, Id, Meta, Tree};

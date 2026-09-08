//! epochdb-exec: subnet-evm block execution on revm, bit-exact with the Go node
//! (libevm + subnet-evm). See REPORT.md.

pub mod config;
pub mod exec;
pub mod feemanager;
pub mod oracle;

pub use config::Config;
pub use exec::{BlockResult, Executor, StateRow, TxResult};

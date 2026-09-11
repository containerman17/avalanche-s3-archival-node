//! epochdb-node: the executor's flat state backend (engine), the roll and
//! manifest helpers, and the in-process benchmark loop (bench, the
//! `epochdb-rs --dump` mode). The plugin crate builds the VM on top.
pub mod bench;
pub mod engine;
pub mod firewood;
pub mod sae;

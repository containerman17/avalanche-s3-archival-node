//! epochdb-rs: no arguments = rpcchainvm plugin mode (handshake with the
//! runtime engine, serve, exit on Shutdown + SIGTERM); --version prints the
//! version line avalanchego's plugin check reads.
use plugin::engine::TrivialEngine;
use plugin::vm::{serve, Init};
use plugin::VERSION;

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.iter().any(|a| a == "--version" || a == "version") {
        println!("{VERSION}");
        return;
    }
    if !args.is_empty() {
        eprintln!("usage: epochdb-rs [--version]   (no arguments: rpcchainvm plugin mode)");
        std::process::exit(2);
    }
    let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build().expect("tokio");
    let factory = Box::new(|init: &Init| {
        // Config bytes (JSON): {"genesis-id": "0x<32 bytes>"} is the id the
        // host knows height 0 by (avalanchego bootstraps from it; the
        // executor will compute the genesis header itself, the trivial
        // engine cannot). Other keys are ignored.
        let cfg: serde_json::Value = serde_json::from_slice(&init.config_bytes).unwrap_or(serde_json::Value::Null);
        let mut genesis_id = [0u8; 32];
        if let Some(s) = cfg.get("genesis-id").and_then(|v| v.as_str()) {
            let s = s.trim_start_matches("0x");
            if s.len() != 64 {
                return Err("config genesis-id: expected 32 bytes of hex".into());
            }
            for i in 0..32 {
                genesis_id[i] = u8::from_str_radix(&s[2 * i..2 * i + 2], 16)?;
            }
        }
        eprintln!("epochdb-rs: chain {} data {} config {}", plugin::tree::hex(&init.chain_id), init.chain_data_dir, String::from_utf8_lossy(&init.config_bytes));
        TrivialEngine::new(genesis_id, &init.genesis_bytes)
    });
    if let Err(e) = rt.block_on(serve(factory)) {
        eprintln!("epochdb-rs: {e}");
        std::process::exit(1);
    }
}

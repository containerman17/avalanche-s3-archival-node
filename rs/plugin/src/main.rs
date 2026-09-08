//! epochdb-rs: no arguments = rpcchainvm plugin mode (handshake with the
//! runtime engine, serve, exit on Shutdown + SIGTERM); `--dump ...` = the
//! in-process benchmark node (rs/node's bench, the A/B tool); --version
//! prints the version line avalanchego's plugin check reads.
use plugin::node_engine::NodeEngine;
use plugin::vm::{serve, Init};
use plugin::VERSION;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.iter().skip(1).any(|a| a == "--version" || a == "version") {
        println!("{VERSION}");
        return;
    }
    if args.iter().any(|a| a == "--dump") {
        if let Err(e) = node::bench::main(args) {
            eprintln!("epochdb-rs: {e:#}");
            std::process::exit(1);
        }
        return;
    }
    if args.len() > 1 {
        eprintln!("usage: epochdb-rs [--version | --dump FILE --genesis chain.json --data DIR ...]   (no arguments: rpcchainvm plugin mode)");
        std::process::exit(2);
    }
    let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build().expect("tokio");
    let factory = Box::new(|init: &Init| {
        eprintln!("epochdb-rs: chain {} data {} config {}", plugin::tree::hex(&init.chain_id), init.chain_data_dir, String::from_utf8_lossy(&init.config_bytes));
        NodeEngine::open(init)
    });
    if let Err(e) = rt.block_on(serve(factory)) {
        eprintln!("epochdb-rs: {e}");
        std::process::exit(1);
    }
}

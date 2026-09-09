//! epochdb-rs: no arguments = rpcchainvm plugin mode (handshake with the
//! runtime engine, serve, exit on Shutdown + SIGTERM); `--dump ...` = the
//! in-process benchmark node (rs/node's bench, the A/B tool); --version
//! prints the version line avalanchego's plugin check reads.
use plugin::node_engine::NodeEngine;
use plugin::vm::{serve, Init};
use plugin::VERSION;

/// jemalloc: glibc's malloc kept what the seals and merges freed (the plugin's
/// anon heap ran at 5 to 7 GB over 1 GB of live state on Step 1M); jemalloc
/// returns dirty pages after its decay and behaves the same under musl.
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

/// One line of jemalloc's own accounting beside the kernel's RssAnon, so live
/// memory (allocated) and what the allocator holds (resident) can be told apart.
fn heap_line() -> String {
    use tikv_jemalloc_ctl::{epoch, stats};
    let mb = |r: Result<usize, _>| r.map(|b| b >> 20).unwrap_or(0);
    let _ = epoch::advance();
    let anon = std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| s.lines().find(|l| l.starts_with("RssAnon:")).and_then(|l| l.split_whitespace().nth(1)).and_then(|k| k.parse::<u64>().ok()))
        .map(|k| k >> 10)
        .unwrap_or(0);
    format!(
        "epochdb-rs: heap allocated={}MB active={}MB resident={}MB retained={}MB rss-anon={anon}MB",
        mb(stats::allocated::read()),
        mb(stats::active::read()),
        mb(stats::resident::read()),
        mb(stats::retained::read())
    )
}

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
    std::thread::spawn(|| loop {
        std::thread::sleep(std::time::Duration::from_secs(60));
        eprintln!("{}", heap_line());
    });
    let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build().expect("tokio");
    let factory = Box::new(|init: &Init| {
        eprintln!("epochdb-rs: chain {} data {} config {}", plugin::tree::hex(&init.chain_id), init.chain_data_dir, plugin::config::redacted(&init.config_bytes));
        NodeEngine::open(init)
    });
    if let Err(e) = rt.block_on(serve(factory)) {
        eprintln!("epochdb-rs: {e}");
        std::process::exit(1);
    }
}

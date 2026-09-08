//! epochdb-rpc-serve: the RPC surface over a store dir, plain HTTP (JSON-RPC
//! POST, keep-alive), for the differential and throughput runs.
//!   epochdb-rpc-serve --data DIR --genesis chain.json --upgrade upgrade.json --http 127.0.0.1:19905
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};

fn arg(args: &[String], k: &str) -> Option<String> {
    args.iter().position(|a| a == k).and_then(|i| args.get(i + 1).cloned())
}

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let dir = arg(&args, "--data").ok_or_else(|| anyhow!("--data DIR"))?;
    let chain: serde_json::Value = serde_json::from_slice(&std::fs::read(arg(&args, "--genesis").ok_or_else(|| anyhow!("--genesis chain.json"))?)?)?;
    let upgrade = arg(&args, "--upgrade").map(std::fs::read).transpose()?.unwrap_or_default();
    let http = arg(&args, "--http").unwrap_or_else(|| "127.0.0.1:19905".into());
    use base64::Engine;
    let g = base64::engine::general_purpose::STANDARD.decode(chain.get("genesisData").and_then(|v| v.as_str()).ok_or_else(|| anyhow!("chain.json has no genesisData"))?)?;
    let network = chain.get("networkID").and_then(|v| v.as_u64()).unwrap_or(1) as u32;
    let root: [u8; 32] = <sha2::Sha256 as sha2::Digest>::digest(&g).into();
    let cfg = exec::Config::from_genesis(&g, &upgrade, network).context("config")?;
    let genesis = Arc::new(rpc::genesis::block(&cfg, &g).map_err(|e| anyhow!("genesis: {e}"))?);
    let db = store::db::DB::open_read_only(std::path::Path::new(&dir), store::casfs::Store::open(std::path::Path::new(&dir))?, root)?;
    let chain_config = serde_json::from_slice::<serde_json::Value>(&g)?.get("config").cloned().unwrap_or_default();
    let cfg = Arc::new(cfg);
    let upgrades = serde_json::from_slice::<serde_json::Value>(&upgrade).ok();
    let server = Arc::new(rpc::Server::new(Arc::new(rpc::storedb::StoreDb::new(db, cfg.clone())), cfg, genesis, chain_config, upgrades));
    eprintln!("epochdb-rpc-serve: head {} at http://{http}", server.head());
    let l = TcpListener::bind(&http)?;
    for conn in l.incoming() {
        let Ok(conn) = conn else { continue };
        let server = server.clone();
        std::thread::spawn(move || {
            let _ = conn.set_nodelay(true);
            let mut r = BufReader::new(conn.try_clone().unwrap());
            let mut w = conn;
            loop {
                let mut line = String::new();
                if r.read_line(&mut line).unwrap_or(0) == 0 {
                    return;
                }
                let mut len = 0usize;
                let mut close = false;
                loop {
                    let mut h = String::new();
                    if r.read_line(&mut h).unwrap_or(0) == 0 {
                        return;
                    }
                    let h = h.trim_end();
                    if h.is_empty() {
                        break;
                    }
                    let (k, v) = h.split_once(':').unwrap_or((h, ""));
                    match k.to_ascii_lowercase().as_str() {
                        "content-length" => len = v.trim().parse().unwrap_or(0),
                        "connection" if v.trim().eq_ignore_ascii_case("close") => close = true,
                        _ => {}
                    }
                }
                let mut body = vec![0u8; len];
                if r.read_exact(&mut body).is_err() {
                    return;
                }
                let out = if line.starts_with("POST") { server.handle(&body) } else { b"{\"jsonrpc\":\"2.0\",\"id\":null,\"error\":{\"code\":-32600,\"message\":\"POST a JSON-RPC request\"}}".to_vec() };
                let head = format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n", out.len());
                if w.write_all(head.as_bytes()).and_then(|_| w.write_all(&out)).is_err() || close {
                    return;
                }
            }
        });
    }
    Ok(())
}

//! Block source: `newHeads` over the local avalanchego's WebSocket, then
//! `eth_getBlockByHash` with full transactions over HTTP. The receive time
//! (ms since the Unix epoch) is taken the moment the head notification is
//! read, before anything else.

use anyhow::{anyhow, Context, Result};
use serde_json::{json, Value};
use std::sync::mpsc::Sender;
use std::time::{SystemTime, UNIX_EPOCH};

pub struct RawBlock {
    pub received_ms: u64,
    pub height: u64,
    pub hash: String,
    /// The `eth_getBlockByHash(hash, true)` result as coreth returns it,
    /// `blockExtraData` (the atomic txs) included.
    pub block: Value,
}

pub fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64
}

pub fn rpc(http: &str, method: &str, params: Value) -> Result<Value> {
    let resp: Value = ureq::post(http)
        .send_json(json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}))
        .with_context(|| format!("{method} at {http}"))?
        .into_json()?;
    if let Some(e) = resp.get("error") {
        return Err(anyhow!("{method}: {e}"));
    }
    resp.get("result").cloned().ok_or_else(|| anyhow!("{method}: no result"))
}

pub fn block_by_hash(http: &str, hash: &str) -> Result<Value> {
    let b = rpc(http, "eth_getBlockByHash", json!([hash, true]))?;
    if b.is_null() {
        return Err(anyhow!("eth_getBlockByHash {hash}: null"));
    }
    Ok(b)
}

pub fn hex_u64(v: &Value) -> Result<u64> {
    let s = v.as_str().ok_or_else(|| anyhow!("not a hex string: {v}"))?;
    Ok(u64::from_str_radix(s.trim_start_matches("0x"), 16)?)
}

/// Blocks until the socket drops; the caller reconnects. Every head becomes a
/// `RawBlock` on `out` in arrival order.
pub fn follow(ws: &str, http: &str, out: &Sender<RawBlock>) -> Result<()> {
    let (mut sock, _) = tungstenite::connect(ws).with_context(|| format!("connect {ws}"))?;
    sock.send(tungstenite::Message::Text(
        json!({"jsonrpc": "2.0", "id": 1, "method": "eth_subscribe", "params": ["newHeads"]}).to_string().into(),
    ))?;
    loop {
        let msg = sock.read()?;
        let received_ms = now_ms();
        let text = match msg {
            tungstenite::Message::Text(t) => t,
            tungstenite::Message::Ping(p) => {
                sock.send(tungstenite::Message::Pong(p))?;
                continue;
            }
            tungstenite::Message::Close(_) => return Err(anyhow!("closed")),
            _ => continue,
        };
        let v: Value = serde_json::from_str(&text)?;
        let Some(head) = v.pointer("/params/result") else { continue };
        let hash = head["hash"].as_str().ok_or_else(|| anyhow!("head without hash: {head}"))?.to_string();
        let height = hex_u64(&head["number"])?;
        let block = block_by_hash(http, &hash)?;
        if out.send(RawBlock { received_ms, height, hash, block }).is_err() {
            return Ok(());
        }
    }
}

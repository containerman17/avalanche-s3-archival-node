//! WebSocket JSON-RPC (the /ws handler): the HTTP dispatch over one
//! connection (single and batch) plus eth_subscribe / eth_unsubscribe, after
//! libevm's rpc/websocket.go, rpc/subscription.go and rpc/handler.go and
//! subnet-evm's eth/filters/api.go (accepted heads and accepted logs; the
//! pending-tx stream exists but a follower has no mempool, so it is silent).
//! The transport is any byte stream: a TCP socket (bin/serve.rs) or the
//! hijacked connection the host streams over rpcchainvm (the plugin's ghttp).
use std::collections::HashMap;
use std::pin::Pin;
use std::time::Duration;

use alloy_eips::eip2718::Decodable2718;
use alloy_primitives::{Address, B256};
use block::Block;
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::broadcast::error::RecvError;
use tokio_tungstenite::tungstenite::handshake::derive_accept_key;
use tokio_tungstenite::tungstenite::protocol::{Message, Role, WebSocketConfig};
use tokio_tungstenite::WebSocketStream;

use crate::eth::{log_matches, parse_log_matchers};
use crate::json::{header_fields, log_json, parse_qty, tx_json};
use crate::{filters, invalid, Log, Receipt, RpcError, RpcResult, Server};

/// wsDefaultReadLimit: the largest message a client may send.
pub const READ_LIMIT: usize = 32 << 20;
/// wsPingInterval / wsPongTimeout / defaultWriteTimeout.
const PING_INTERVAL: Duration = Duration::from_secs(30);
const PONG_TIMEOUT: Duration = Duration::from_secs(30);
const WRITE_TIMEOUT: Duration = Duration::from_secs(10);
/// maxClientSubscriptionBuffer: heads a client may fall behind before the
/// connection is dropped (the queue overflow policy).
pub const QUEUE: usize = 20000;

/// One accepted block with its receipts, what every subscription reads.
pub struct Head {
    pub block: Arc<Block>,
    pub receipts: Vec<Receipt>,
}

/// The 2718 receipt envelopes of a block, concatenated (the executor's row).
pub fn decode_receipts(mut p: &[u8]) -> crate::Result<Vec<Receipt>> {
    let mut out = Vec::new();
    while !p.is_empty() {
        let env = alloy_consensus::ReceiptEnvelope::decode_2718(&mut p).map_err(|e| anyhow::anyhow!("receipt {}: {e}", out.len()))?;
        let rc = env.as_receipt().ok_or_else(|| anyhow::anyhow!("receipt without a body"))?;
        out.push(Receipt {
            status: rc.status.coerce_status() as u64,
            gas_used: 0,
            cumulative_gas_used: rc.cumulative_gas_used,
            logs: rc.logs.iter().map(|l| Log { address: l.address, topics: l.data.topics().to_vec(), data: l.data.data.clone() }).collect(),
        });
    }
    Ok(out)
}

/// A byte stream the server can serve (type-erased for the plugin).
pub trait Stream: AsyncRead + AsyncWrite + Send + Unpin {}
impl<T: AsyncRead + AsyncWrite + Send + Unpin> Stream for T {}
pub type BoxStream = Pin<Box<dyn Stream>>;

/// gorilla's Upgrader.Upgrade checks, in its order, over the request line and
/// headers (`header` looks a key up case-insensitively). Ok = the 101 response
/// bytes; Err = (status, reason) for `http.Error(w, StatusText(status), status)`
/// with `Sec-Websocket-Version: 13` set (what gorilla's returnError sends).
pub fn handshake(method: &str, header: &dyn Fn(&str) -> Option<String>) -> Result<String, (u16, &'static str)> {
    let has_token = |k: &str, want: &str| header(k).is_some_and(|v| v.split(',').any(|t| t.trim().eq_ignore_ascii_case(want)));
    if !has_token("Connection", "upgrade") {
        return Err((400, "websocket: the client is not using the websocket protocol: 'upgrade' token not found in 'Connection' header"));
    }
    if !has_token("Upgrade", "websocket") {
        return Err((400, "websocket: the client is not using the websocket protocol: 'websocket' token not found in 'Upgrade' header"));
    }
    if method != "GET" {
        return Err((405, "websocket: the client is not using the websocket protocol: request method is not GET"));
    }
    if !has_token("Sec-Websocket-Version", "13") {
        return Err((400, "websocket: unsupported version: 13 not found in 'Sec-Websocket-Version' header"));
    }
    // Origins: stock passes "*" to the handler, every origin is allowed.
    let key = header("Sec-Websocket-Key").unwrap_or_default();
    use base64::Engine;
    if key.is_empty() || base64::engine::general_purpose::STANDARD.decode(key.as_bytes()).map(|b| b.len()) != Ok(16) {
        return Err((400, "websocket: not a websocket handshake: 'Sec-WebSocket-Key' header must be Base64 encoded value of 16-byte in length"));
    }
    Ok(format!("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {}\r\n\r\n", derive_accept_key(key.as_bytes())))
}

/// net/http's StatusText for the codes the handshake can answer.
pub fn status_text(code: u16) -> &'static str {
    match code {
        400 => "Bad Request",
        403 => "Forbidden",
        405 => "Method Not Allowed",
        _ => "Internal Server Error",
    }
}

enum Kind {
    Heads,
    Logs { from: Option<i64>, to: Option<i64>, addrs: Vec<Address>, topics: Vec<Vec<B256>>, min_topics: usize },
    /// newAcceptedTransactions (fires) and newPendingTransactions (never does).
    AcceptedTxs { full: bool },
    PendingTxs,
}

struct Sub {
    kind: Kind,
    /// The head when the subscription was made: heads at or below it were
    /// accepted before the client asked and are not delivered.
    since: u64,
}

/// One connection's subscriptions.
#[derive(Default)]
struct Conn {
    subs: HashMap<String, Sub>,
}

/// rpc.BlockNumber: tags to their negative sentinels, hex to a height.
fn block_number(v: &Value) -> Result<i64, String> {
    match v.as_str() {
        Some("earliest") => Ok(0),
        Some("latest") => Ok(-2),
        Some("pending") => Ok(-1),
        Some("finalized") => Ok(-3),
        Some("safe") => Ok(-4),
        Some(_) => {
            let n = parse_qty(v).map_err(|e| e.message)?;
            if n > i64::MAX as u64 {
                return Err("block number larger than int64".into());
            }
            Ok(n as i64)
        }
        None => Err(format!("json: cannot unmarshal {} into Go value of type string", json_type(v))),
    }
}

fn json_type(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

impl Conn {
    /// eth_subscribe: handler.handleSubscribe + the FilterAPI methods.
    fn subscribe(&mut self, head: u64, params: &[Value]) -> RpcResult {
        let name = params.first().and_then(Value::as_str).ok_or_else(|| invalid("expected subscription name as first argument"))?;
        let args = match name {
            "newHeads" => 0,
            "logs" | "newPendingTransactions" | "newAcceptedTransactions" => 1,
            _ => return Err(RpcError { code: -32601, message: format!("no \"{name}\" subscription in eth namespace"), data: None }),
        };
        if params.len() > 1 + args {
            return Err(invalid(format!("too many arguments, want at most {}", 1 + args)));
        }
        let kind = match name {
            "newHeads" => Kind::Heads,
            "logs" => {
                let f = params.get(1).ok_or_else(|| invalid("missing value for required argument 1"))?;
                let f = match f {
                    Value::Null => &serde_json::Map::new(),
                    Value::Object(o) => o,
                    _ => return Err(invalid(format!("invalid argument 1: json: cannot unmarshal {} into Go value of type filters.input", json_type(f)))),
                };
                let set = |k: &str| f.get(k).filter(|v| !v.is_null());
                if set("blockHash").is_some() && (set("fromBlock").is_some() || set("toBlock").is_some()) {
                    return Err(invalid("invalid argument 1: cannot specify both BlockHash and FromBlock/ToBlock, choose one or the other"));
                }
                let from = set("fromBlock").map(block_number).transpose().map_err(|e| invalid(format!("invalid argument 1: {e}")))?;
                let to = set("toBlock").map(block_number).transpose().map_err(|e| invalid(format!("invalid argument 1: {e}")))?;
                if let Some(Value::Array(a)) = f.get("address") {
                    if a.len() > 1000 {
                        return Err(invalid("invalid argument 1: exceed max addresses"));
                    }
                }
                let (addrs, topics) = parse_log_matchers(f).map_err(|e| invalid(format!("invalid argument 1: {}", e.message)))?;
                // filterLogs skips a log with fewer topics than the filter lists
                // (trailing wildcards included; parse_log_matchers drops those).
                let min_topics = f.get("topics").and_then(Value::as_array).map_or(0, Vec::len);
                // EventSystem.SubscribeAcceptedLogs' accepted combinations.
                let (fb, tb) = (from.unwrap_or(-2), to.unwrap_or(-2));
                let ok = (fb == -1 && tb == -1) || (fb == -2 && tb == -2) || (fb >= 0 && tb >= 0 && tb >= fb) || (fb >= -2 && tb == -1) || (fb >= 0 && tb == -2);
                if !ok {
                    return Err("invalid from and to block combination: from > to".into());
                }
                Kind::Logs { from, to, addrs, topics, min_topics }
            }
            _ => {
                let full = match params.get(1) {
                    None | Some(Value::Null) => false,
                    Some(Value::Bool(b)) => *b,
                    Some(v) => return Err(invalid(format!("invalid argument 1: json: cannot unmarshal {} into Go value of type bool", json_type(v)))),
                };
                if name == "newPendingTransactions" {
                    Kind::PendingTxs
                } else {
                    Kind::AcceptedTxs { full }
                }
            }
        };
        let id = filters::new_id();
        self.subs.insert(id.clone(), Sub { kind, since: head });
        Ok(json!(id))
    }

    /// eth_unsubscribe: handler.unsubscribe.
    fn unsubscribe(&mut self, params: &[Value]) -> RpcResult {
        let id = match params.first() {
            None | Some(Value::Null) => return Err(invalid("missing value for required argument 0")),
            Some(Value::String(s)) => s,
            Some(v) => return Err(invalid(format!("invalid argument 0: json: cannot unmarshal {} into Go value of type rpc.ID", json_type(v)))),
        };
        if params.len() > 1 {
            return Err(invalid("too many arguments, want at most 1"));
        }
        if self.subs.remove(id).is_none() {
            return Err("subscription not found".into());
        }
        Ok(json!(true))
    }

    /// The notifications one accepted head produces, in delivery order.
    fn notifications(&self, h: &Head) -> Vec<String> {
        let b = &h.block;
        let mut out = Vec::new();
        let mut ids: Vec<&String> = self.subs.keys().collect();
        ids.sort();
        for id in ids {
            let sub = &self.subs[id];
            if b.height <= sub.since {
                continue;
            }
            let mut push = |v: Value| out.push(json!({"jsonrpc": "2.0", "method": "eth_subscription", "params": {"subscription": id, "result": v}}).to_string());
            match &sub.kind {
                Kind::Heads => push(head_json(b)),
                Kind::PendingTxs => {}
                Kind::AcceptedTxs { full } => {
                    for (i, t) in b.txs.iter().enumerate() {
                        push(if *full { tx_json(b, i).unwrap_or(Value::Null) } else { json!(t.hash) });
                    }
                }
                Kind::Logs { from, to, addrs, topics, min_topics } => {
                    // filterLogs: the range bounds apply to live logs, tags do not.
                    if from.is_some_and(|f| f >= 0 && f as u64 > b.height) || to.is_some_and(|t| t >= 0 && (t as u64) < b.height) {
                        continue;
                    }
                    let mut idx = 0u64;
                    for (i, r) in h.receipts.iter().enumerate() {
                        for l in &r.logs {
                            if l.topics.len() >= *min_topics && log_matches(l, addrs, topics) {
                                push(log_json(l, b, i, idx, false));
                            }
                            idx += 1;
                        }
                    }
                }
            }
        }
        out
    }
}

/// The newHeads payload: subnet-evm's HeaderSerializable JSON (the header
/// with its hash; every optional field present, null when unset; no
/// totalDifficulty / size / transactions).
pub fn head_json(b: &Block) -> Value {
    let mut f = header_fields(b);
    let f = f.as_object_mut().unwrap();
    // HeaderSerializable's field order (byte-equal with stock's notification).
    let mut o = serde_json::Map::new();
    for k in [
        "parentHash", "sha3Uncles", "miner", "stateRoot", "transactionsRoot", "receiptsRoot", "logsBloom", "difficulty", "number", "gasLimit", "gasUsed", "timestamp", "extraData", "mixHash", "nonce",
        "baseFeePerGas", "blockGasCost", "blobGasUsed", "excessBlobGas", "parentBeaconBlockRoot", "timestampMilliseconds", "minDelayExcess", "hash",
    ] {
        o.insert(k.to_string(), f.remove(k).unwrap_or(Value::Null));
    }
    Value::Object(o)
}

/// Serve one upgraded connection until it closes: requests are answered in
/// order through the HTTP dispatch (eth_subscribe / eth_unsubscribe
/// intercepted), accepted heads fan out to the live subscriptions, an idle
/// ping every 30 s with a 30 s pong deadline, 10 s per write; a client that
/// falls QUEUE heads behind is disconnected.
pub async fn serve<S: AsyncRead + AsyncWrite + Unpin>(s: &Server, stream: S) {
    let cfg = WebSocketConfig::default().max_message_size(Some(READ_LIMIT)).max_frame_size(Some(READ_LIMIT));
    let mut ws = WebSocketStream::from_raw_socket(stream, Role::Server, Some(cfg)).await;
    let mut heads = s.heads.subscribe();
    let mut conn = Conn::default();
    let ping_at = tokio::time::sleep(PING_INTERVAL);
    tokio::pin!(ping_at);
    let pong_by = tokio::time::sleep(Duration::from_secs(30 * 86400));
    tokio::pin!(pong_by);
    let mut pong_armed = false;
    loop {
        let out: Vec<String> = tokio::select! {
            msg = ws.next() => {
                let data = match msg {
                    Some(Ok(Message::Text(t))) => t.as_bytes().to_vec(),
                    Some(Ok(Message::Binary(b))) => b.to_vec(),
                    Some(Ok(Message::Pong(_))) => { pong_armed = false; continue }
                    Some(Ok(Message::Ping(_))) | Some(Ok(Message::Frame(_))) => continue,
                    Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
                };
                // ServeCodec: a message that is not JSON gets the parse error and ends the connection.
                let parse_error = serde_json::from_slice::<Value>(&data).is_err();
                let body = tokio::task::block_in_place(|| s.handle_with(&data, &mut |m, p| match m {
                    "eth_subscribe" => Some(conn.subscribe(s.accepted_head(), p)),
                    "eth_unsubscribe" => Some(conn.unsubscribe(p)),
                    _ => None,
                }));
                if body.is_empty() { continue }
                if parse_error {
                    let _ = tokio::time::timeout(WRITE_TIMEOUT, ws.send(Message::text(String::from_utf8(body).unwrap_or_default() + "\n"))).await;
                    break;
                }
                vec![String::from_utf8(body).unwrap_or_default()]
            }
            h = heads.recv() => match h {
                Ok(h) => conn.notifications(&h),
                // Lagged: the client fell QUEUE heads behind; Closed: the server is gone.
                Err(RecvError::Lagged(_)) | Err(RecvError::Closed) => break,
            },
            _ = &mut ping_at => {
                if tokio::time::timeout(Duration::from_secs(5), ws.send(Message::Ping(Default::default()))).await.map_err(|_| ()).and_then(|r| r.map_err(|_| ())).is_err() { break }
                ping_at.as_mut().reset(tokio::time::Instant::now() + PING_INTERVAL);
                pong_by.as_mut().reset(tokio::time::Instant::now() + PONG_TIMEOUT);
                pong_armed = true;
                continue
            }
            _ = &mut pong_by, if pong_armed => break,
        };
        for mut m in out {
            // json.Encoder.Encode: every message stock sends ends in a newline (byte-equal notifications).
            m.push('\n');
            if tokio::time::timeout(WRITE_TIMEOUT, ws.feed(Message::text(m))).await.map_err(|_| ()).and_then(|r| r.map_err(|_| ())).is_err() {
                let _ = ws.close(None).await;
                return;
            }
        }
        if tokio::time::timeout(WRITE_TIMEOUT, ws.flush()).await.map_err(|_| ()).and_then(|r| r.map_err(|_| ())).is_err() {
            return;
        }
        // A write delays the next idle ping (writeJSON's pingReset).
        ping_at.as_mut().reset(tokio::time::Instant::now() + PING_INTERVAL);
    }
    // Close (or answer the client's Close) and drive the handshake to its end.
    let _ = tokio::time::timeout(WRITE_TIMEOUT, async {
        let _ = ws.close(None).await;
        while let Some(Ok(_)) = ws.next().await {}
    })
    .await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_consensus::{Eip658Value, ReceiptEnvelope, ReceiptWithBloom};
    use alloy_eips::eip2718::Encodable2718;
    use alloy_primitives::{address, b256, Bytes};

    /// A Store that holds nothing (the ws layer only asks for the head).
    struct Empty;
    impl crate::Store for Empty {
        fn head(&self) -> u64 {
            0
        }
        fn block(&self, _: u64) -> crate::Result<Option<Arc<Block>>> {
            Ok(None)
        }
        fn hash_at(&self, _: u64) -> crate::Result<Option<B256>> {
            Ok(None)
        }
        fn height_by_hash(&self, _: &B256) -> crate::Result<Option<u64>> {
            Ok(None)
        }
        fn tx_by_hash(&self, _: &B256) -> crate::Result<Option<(u64, usize)>> {
            Ok(None)
        }
        fn receipts(&self, _: u64) -> crate::Result<Option<Vec<Receipt>>> {
            Ok(None)
        }
        fn traces(&self, _: u64) -> crate::Result<Option<Vec<String>>> {
            Ok(None)
        }
        fn container(&self, _: u64) -> crate::Result<Option<Bytes>> {
            Ok(None)
        }
        fn state_at(&self, _: u64) -> crate::Result<Box<dyn crate::StateRead + '_>> {
            unimplemented!()
        }
        fn log_candidates(&self, _: u64, _: u64, _: &[Address], _: &[Vec<B256>]) -> crate::Result<Option<Vec<u64>>> {
            Ok(None)
        }
        fn tx_range(&self, _: u64) -> crate::Result<Option<(u64, u32)>> {
            Ok(None)
        }
        fn height_of_tx(&self, _: u64) -> crate::Result<Option<u64>> {
            Ok(None)
        }
        fn next_tx(&self) -> u64 {
            0
        }
        fn postings(&self, _: &[u8], _: u64, _: u64, _: bool, _: &mut dyn FnMut(&[u8], u64, u8) -> bool) -> crate::Result<()> {
            Ok(())
        }
        fn groups(&self, _: &[u8], _: &mut dyn FnMut(&[u8]) -> bool) -> crate::Result<()> {
            Ok(())
        }
        fn set_scan(&self, _: &[u8], _: &mut dyn FnMut(&[u8]) -> bool) -> crate::Result<()> {
            Ok(())
        }
    }

    const ADDR: Address = address!("0x00000000000000000000000000000000000000aa");
    const TOPIC: B256 = b256!("0x00000000000000000000000000000000000000000000000000000000000000bb");

    /// The Step genesis as block `height`, plus one receipt with one log.
    fn fixture(height: u64) -> (Arc<exec::Config>, Arc<Block>, Vec<u8>) {
        let g = crate::genesis::STEP_GENESIS.as_bytes();
        let cfg = Arc::new(exec::Config::from_genesis(g, b"", 1).unwrap());
        let mut b = crate::genesis::block(&cfg, g).unwrap();
        b.height = height;
        b.header.number = height;
        // EIP-155's example tx: the log's transactionHash comes from here.
        let raw = alloy_primitives::hex::decode("f86c098504a817c800825208943535353535353535353535353535353535353535880de0b6b3a76400008025a028ef61340bd939bc2195fe537567866003e1a15d3c71ff63e1590620aa636276a067cbe9d8997f761aecb703304b3800ccf555c9f3dc64214b297fb1966a3b6d83").unwrap();
        b.txs.push(block::eth::decode_tx(bytes::Bytes::from(raw)).unwrap());
        let log = alloy_primitives::Log::new_unchecked(ADDR, vec![TOPIC], Bytes::from_static(b"\x01"));
        let rc = alloy_consensus::Receipt { status: Eip658Value::Eip658(true), cumulative_gas_used: 21000, logs: vec![log] };
        let mut rlp = Vec::new();
        ReceiptEnvelope::Legacy(ReceiptWithBloom::from(rc)).encode_2718(&mut rlp);
        (cfg, Arc::new(b), rlp)
    }

    #[test]
    fn handshake_is_gorillas() {
        // RFC 6455 section 1.3's example key and accept value.
        let hdr = |k: &str| match k {
            "Connection" => Some("keep-alive, Upgrade".to_string()),
            "Upgrade" => Some("websocket".to_string()),
            "Sec-Websocket-Version" => Some("13".to_string()),
            "Sec-Websocket-Key" => Some("dGhlIHNhbXBsZSBub25jZQ==".to_string()),
            _ => None,
        };
        let resp = handshake("GET", &hdr).unwrap();
        assert!(resp.starts_with("HTTP/1.1 101 Switching Protocols\r\n"));
        assert!(resp.contains("Sec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\n"));
        assert_eq!(handshake("POST", &hdr).unwrap_err().0, 405);
        assert_eq!(handshake("GET", &|k| if k == "Upgrade" { None } else { hdr(k) }).unwrap_err().0, 400);
        assert_eq!(handshake("GET", &|k| if k == "Sec-Websocket-Key" { Some("short".into()) } else { hdr(k) }).unwrap_err().0, 400);
    }

    #[test]
    fn subscriptions_fan_out_and_unsubscribe() {
        let (_, b, rlp) = fixture(5);
        let head = Head { block: b.clone(), receipts: decode_receipts(&rlp).unwrap() };
        let mut c = Conn::default();
        let heads = c.subscribe(4, &[json!("newHeads")]).unwrap();
        let heads = heads.as_str().unwrap().to_string();
        assert!(heads.starts_with("0x") && heads.len() <= 34 && !heads[2..].starts_with('0'), "{heads}");
        let logs_hit = c.subscribe(4, &[json!("logs"), json!({"address": ADDR, "topics": [TOPIC]})]).unwrap();
        let logs_miss = c.subscribe(4, &[json!("logs"), json!({"address": "0x00000000000000000000000000000000000000cc"})]).unwrap();
        let late = c.subscribe(5, &[json!("newHeads")]).unwrap(); // made at head 5: block 5 is not new to it
        let pending = c.subscribe(4, &[json!("newPendingTransactions")]).unwrap();
        assert_eq!(c.subscribe(4, &[json!("bogus")]).err().unwrap().code, -32601);
        assert_eq!(c.subscribe(4, &[json!("logs"), json!({"fromBlock": "0x10", "toBlock": "0x5"})]).err().unwrap().message, "invalid from and to block combination: from > to");
        let out: Vec<Value> = c.notifications(&head).iter().map(|s| serde_json::from_str(s).unwrap()).collect();
        assert_eq!(out.len(), 2, "{out:?}");
        let by_sub = |id: &Value| out.iter().find(|n| n["params"]["subscription"] == *id).unwrap();
        let h = by_sub(&json!(heads));
        assert_eq!(h["method"], "eth_subscription");
        assert_eq!(h["params"]["result"]["hash"], json!(b.hash));
        assert_eq!(h["params"]["result"]["number"], "0x5");
        assert!(h["params"]["result"].get("totalDifficulty").is_none());
        assert!(h["params"]["result"].get("size").is_none());
        let l = by_sub(&logs_hit);
        assert_eq!(l["params"]["result"]["address"], json!(ADDR));
        assert_eq!(l["params"]["result"]["logIndex"], "0x0");
        assert_eq!(l["params"]["result"]["removed"], false);
        assert!(!out.iter().any(|n| n["params"]["subscription"] == logs_miss || n["params"]["subscription"] == late || n["params"]["subscription"] == pending));
        assert_eq!(c.unsubscribe(&[json!(heads)]).unwrap(), json!(true));
        assert_eq!(c.unsubscribe(&[json!(heads)]).err().unwrap().message, "subscription not found");
        assert_eq!(c.unsubscribe(&[json!(5)]).err().unwrap().message, "invalid argument 0: json: cannot unmarshal number into Go value of type rpc.ID");
        assert_eq!(c.notifications(&head).len(), 1);
    }

    /// The frame codec end to end: a tungstenite client on the other end of
    /// a duplex pipe talks JSON-RPC, subscribes, gets a published head, closes.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn serve_over_a_pipe() {
        let (cfg, genesis, rlp) = fixture(0);
        let b1 = fixture(1).1;
        let server = Arc::new(Server::new(Arc::new(Empty), cfg, genesis, json!({}), None));
        let (a, b) = tokio::io::duplex(1 << 16);
        let srv = server.clone();
        let task = tokio::spawn(async move { serve(&srv, a).await });
        let mut client = WebSocketStream::from_raw_socket(b, Role::Client, None).await;
        async fn call<S: AsyncRead + AsyncWrite + Unpin>(client: &mut WebSocketStream<S>, req: Value) -> Value {
            client.send(Message::text(req.to_string())).await.unwrap();
            match client.next().await.unwrap().unwrap() {
                Message::Text(t) => serde_json::from_str::<Value>(&t).unwrap(),
                m => panic!("{m:?}"),
            }
        }
        assert_eq!(call(&mut client, json!({"jsonrpc":"2.0","id":1,"method":"eth_chainId","params":[]})).await["result"], "0x4d2");
        let batch = call(&mut client, json!([{"jsonrpc":"2.0","id":2,"method":"eth_blockNumber"},{"jsonrpc":"2.0","id":3,"method":"eth_chainId"}])).await;
        assert_eq!(batch.as_array().unwrap().len(), 2);
        let sid = call(&mut client, json!({"jsonrpc":"2.0","id":4,"method":"eth_subscribe","params":["newHeads"]})).await["result"].clone();
        assert!(sid.as_str().unwrap().starts_with("0x"));
        // Accept: the hook fires while a subscriber is listening.
        while server.heads.receiver_count() == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        server.publish(b1.clone(), &rlp);
        let n = match client.next().await.unwrap().unwrap() {
            Message::Text(t) => serde_json::from_str::<Value>(&t).unwrap(),
            m => panic!("{m:?}"),
        };
        assert_eq!(n["method"], "eth_subscription");
        assert_eq!(n["params"]["subscription"], sid);
        assert_eq!(n["params"]["result"]["hash"], json!(b1.hash));
        assert_eq!(call(&mut client, json!({"jsonrpc":"2.0","id":5,"method":"eth_unsubscribe","params":[sid]})).await["result"], true);
        server.publish(b1.clone(), &rlp);
        // Nothing arrives for the dropped subscription: the next reply is ours.
        assert_eq!(call(&mut client, json!({"jsonrpc":"2.0","id":6,"method":"eth_chainId","params":[]})).await["id"], 6);
        client.close(None).await.unwrap();
        assert!(matches!(client.next().await, Some(Ok(Message::Close(_))) | None));
        tokio::time::timeout(Duration::from_secs(5), task).await.unwrap().unwrap();
    }
}

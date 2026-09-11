//! Mempool capture: `newPendingTransactions` with full bodies from each
//! validator's WebSocket, first-seen time in ms, deduplicated by hash.
//! Passive: nothing here feeds the EVM. The applier calls `flush(height)`
//! after each block to move the interval's arrivals into the history.

use crate::feed::now_ms;
use alloy_primitives::B256;
use serde_json::{json, Value};
use std::collections::{HashMap, VecDeque};
use std::sync::mpsc::Sender;
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug)]
pub struct PendingEvent {
    pub received_ms: u64,
    /// Index into Config::validator_ws.
    pub source: usize,
    pub hash: B256,
    pub tx: Arc<Value>,
}

#[derive(Default)]
struct Inner {
    seen: HashMap<B256, u64>,
    /// Everything seen, oldest first; trimmed to `keep_ms`.
    recent: VecDeque<PendingEvent>,
    /// Arrivals since the last flush.
    interval: Vec<PendingEvent>,
    subscribers: Vec<Sender<PendingEvent>>,
}

pub struct Mempool {
    inner: Mutex<Inner>,
    keep_ms: u64,
}

impl Mempool {
    pub fn new(keep_ms: u64) -> Arc<Mempool> {
        Arc::new(Mempool { inner: Mutex::new(Inner::default()), keep_ms })
    }

    fn record(&self, source: usize, tx: Value) {
        let received_ms = now_ms();
        let Some(hash) = tx["hash"].as_str().and_then(|h| h.parse::<B256>().ok()) else { return };
        let mut m = self.inner.lock().unwrap();
        if m.seen.contains_key(&hash) {
            return;
        }
        m.seen.insert(hash, received_ms);
        let ev = PendingEvent { received_ms, source, hash, tx: Arc::new(tx) };
        m.subscribers.retain(|s| s.send(ev.clone()).is_ok());
        m.interval.push(ev.clone());
        m.recent.push_back(ev);
        let cutoff = received_ms.saturating_sub(self.keep_ms);
        while m.recent.front().is_some_and(|e| e.received_ms < cutoff) {
            let e = m.recent.pop_front().unwrap();
            m.seen.remove(&e.hash);
        }
    }

    /// The last `keep_ms` of arrivals, oldest first.
    pub fn pending(&self) -> Vec<PendingEvent> {
        self.inner.lock().unwrap().recent.iter().cloned().collect()
    }

    pub fn subscribe(&self) -> std::sync::mpsc::Receiver<PendingEvent> {
        let (tx, rx) = std::sync::mpsc::channel();
        self.inner.lock().unwrap().subscribers.push(tx);
        rx
    }

    /// The interval's arrivals as JSON lines `{"ms","source","tx"}`, and a reset.
    pub fn flush(&self) -> Vec<u8> {
        let evs = std::mem::take(&mut self.inner.lock().unwrap().interval);
        let mut out = Vec::new();
        for e in evs {
            out.extend_from_slice(json!({"ms": e.received_ms, "source": e.source, "tx": &*e.tx}).to_string().as_bytes());
            out.push(b'\n');
        }
        out
    }

    /// One subscription, reconnecting forever.
    pub fn run(self: Arc<Self>, source: usize, ws: String) {
        loop {
            match self.subscribe_once(source, &ws) {
                Ok(()) => eprintln!("mempool[{source}]: {ws} closed; reconnecting"),
                Err(e) => eprintln!("mempool[{source}]: {ws}: {e:#}; reconnecting"),
            }
            std::thread::sleep(std::time::Duration::from_secs(2));
        }
    }

    fn subscribe_once(&self, source: usize, ws: &str) -> anyhow::Result<()> {
        let (mut sock, _) = tungstenite::connect(ws)?;
        sock.send(tungstenite::Message::Text(
            json!({"jsonrpc": "2.0", "id": 1, "method": "eth_subscribe", "params": ["newPendingTransactions", true]}).to_string().into(),
        ))?;
        loop {
            let text = match sock.read()? {
                tungstenite::Message::Text(t) => t,
                tungstenite::Message::Ping(p) => {
                    sock.send(tungstenite::Message::Pong(p))?;
                    continue;
                }
                tungstenite::Message::Close(_) => return Ok(()),
                _ => continue,
            };
            let mut v: Value = serde_json::from_str(&text)?;
            if let Some(err) = v.get("error") {
                anyhow::bail!("subscribe: {err}");
            }
            if let Some(tx) = v.pointer_mut("/params/result") {
                if tx.is_object() {
                    self.record(source, tx.take());
                }
            }
        }
    }
}

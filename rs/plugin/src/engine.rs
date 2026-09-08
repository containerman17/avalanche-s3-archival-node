//! TrivialEngine: decodes subnet-evm blocks (rs/block), tracks ids and
//! heights in memory, executes nothing. The harness measures the protocol
//! round trips with it; rs/node's executor replaces it behind `Engine`.
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use alloy_primitives::keccak256;
use bytes::Bytes;
use serde_json::{json, Value};

use crate::tree::{hex, Engine, Error, Id, Meta};

pub struct TrivialBlock {
    pub meta: Meta,
    pub bytes: Bytes,
    pub header: Option<block::Header>,
    pub tx_hashes: Vec<[u8; 32]>,
}

struct Chain {
    by_id: HashMap<Id, Arc<TrivialBlock>>,
    /// by_height[h] is the accepted id at height h; [0] is the genesis.
    by_height: Vec<Id>,
}

pub struct TrivialEngine {
    chain_id: u64,
    chain: Mutex<Chain>,
}

impl TrivialEngine {
    /// genesis_id: the id the host knows height 0 by (avalanchego bootstraps
    /// from it; a fresh harness run never checks it). genesis_json: the
    /// chain's genesis, for the chain id.
    pub fn new(genesis_id: Id, genesis_json: &[u8]) -> Result<TrivialEngine, Error> {
        let g: Value = serde_json::from_slice(genesis_json)?;
        let chain_id = g
            .pointer("/config/chainId")
            .and_then(Value::as_u64)
            .ok_or("genesis: config.chainId missing")?;
        let timestamp = g.get("timestamp").map(json_u64).transpose()?.unwrap_or(0);
        let genesis = Arc::new(TrivialBlock {
            meta: Meta { id: genesis_id, parent: [0; 32], height: 0, timestamp },
            bytes: Bytes::new(),
            header: None,
            tx_hashes: Vec::new(),
        });
        let mut by_id = HashMap::new();
        by_id.insert(genesis_id, genesis);
        Ok(TrivialEngine { chain_id, chain: Mutex::new(Chain { by_id, by_height: vec![genesis_id] }) })
    }

    fn block_json(&self, b: &TrivialBlock) -> Value {
        let Some(h) = &b.header else {
            return json!({"number": "0x0", "hash": hex(&b.meta.id), "parentHash": hex(&[0; 32]), "timestamp": qty(b.meta.timestamp), "transactions": []});
        };
        json!({
            "number": qty(h.number),
            "hash": hex(&b.meta.id),
            "parentHash": hex(&h.parent_hash.0),
            "sha3Uncles": hex(&h.uncle_hash.0),
            "miner": format!("{}", h.coinbase),
            "stateRoot": hex(&h.root.0),
            "transactionsRoot": hex(&h.tx_hash.0),
            "receiptsRoot": hex(&h.receipt_hash.0),
            "difficulty": format!("0x{:x}", h.difficulty),
            "gasLimit": qty(h.gas_limit),
            "gasUsed": qty(h.gas_used),
            "timestamp": qty(h.time),
            "extraData": format!("{}", h.extra),
            "nonce": hex_bytes(h.nonce.as_slice()),
            "mixHash": hex(&h.mix_digest.0),
            "baseFeePerGas": h.base_fee.map(|v| format!("0x{v:x}")),
            "blockGasCost": h.block_gas_cost.map(|v| format!("0x{v:x}")),
            "size": qty(b.bytes.len() as u64),
            "transactions": b.tx_hashes.iter().map(hex).collect::<Vec<_>>(),
            "uncles": [],
        })
    }

    fn call(&self, method: &str, params: &[Value]) -> Result<Value, String> {
        let chain = self.chain.lock().unwrap();
        let head = chain.by_height.len() as u64 - 1;
        match method {
            "eth_chainId" => Ok(Value::String(qty(self.chain_id))),
            "eth_blockNumber" => Ok(Value::String(qty(head))),
            "eth_getBlockByNumber" => {
                let tag = params.first().and_then(Value::as_str).ok_or("missing block tag")?;
                let h = match tag {
                    "latest" | "pending" | "safe" | "finalized" => head,
                    "earliest" => 0,
                    _ => u64::from_str_radix(tag.trim_start_matches("0x"), 16).map_err(|e| e.to_string())?,
                };
                Ok(chain.by_height.get(h as usize).and_then(|id| chain.by_id.get(id)).map(|b| self.block_json(b)).unwrap_or(Value::Null))
            }
            "eth_getBlockByHash" => {
                let s = params.first().and_then(Value::as_str).ok_or("missing hash")?;
                let id = parse_id(s)?;
                Ok(chain.by_id.get(&id).map(|b| self.block_json(b)).unwrap_or(Value::Null))
            }
            _ => Err(format!("the method {method} does not exist/is not available")),
        }
    }
}

fn json_u64(v: &Value) -> Result<u64, Error> {
    match v {
        Value::Number(n) => n.as_u64().ok_or_else(|| "not a u64".into()),
        Value::String(s) => Ok(u64::from_str_radix(s.trim_start_matches("0x"), 16)?),
        _ => Err("not a number".into()),
    }
}

fn qty(v: u64) -> String {
    format!("0x{v:x}")
}

fn hex_bytes(b: &[u8]) -> String {
    let mut s = String::from("0x");
    for x in b {
        s.push_str(&format!("{x:02x}"));
    }
    s
}

fn parse_id(s: &str) -> Result<Id, String> {
    let s = s.trim_start_matches("0x");
    if s.len() != 64 {
        return Err("hash must be 32 bytes".into());
    }
    let mut id = [0u8; 32];
    for i in 0..32 {
        id[i] = u8::from_str_radix(&s[2 * i..2 * i + 2], 16).map_err(|e| e.to_string())?;
    }
    Ok(id)
}

impl Engine for TrivialEngine {
    type Block = Arc<TrivialBlock>;
    type Pending = ();

    /// The inner block RLP (what a plugin receives) or a whole container
    /// (what a host may pass): rs/block's unwrap reads both.
    fn parse(&self, bytes: Bytes) -> Result<Self::Block, Error> {
        let u = block::pvm::unwrap(&bytes)?;
        let (header_rlp, txs) = block::eth::decode_block(&u.inner)?;
        let header = block::eth::decode_header(&header_rlp)?;
        let id: Id = keccak256(&header_rlp).0;
        let meta = Meta { id, parent: header.parent_hash.0, height: header.number, timestamp: header.time };
        let tx_hashes = txs.iter().map(|t| t.hash.0).collect();
        Ok(Arc::new(TrivialBlock { meta, bytes, header: Some(header), tx_hashes }))
    }

    fn meta(&self, b: &Self::Block) -> Meta {
        b.meta.clone()
    }

    fn bytes(&self, b: &Self::Block) -> Bytes {
        b.bytes.clone()
    }

    fn verify(&self, b: &Self::Block, _parent: Option<&Arc<()>>, _pch: Option<u64>) -> Result<(), Error> {
        let head = self.chain.lock().unwrap().by_height.len() as u64;
        if b.meta.height < head {
            return Err(format!("block {} is below the accepted head {}", b.meta.height, head - 1).into());
        }
        Ok(())
    }

    fn accept(&self, b: &Self::Block, _: &()) -> Result<(), Error> {
        let mut c = self.chain.lock().unwrap();
        if b.meta.height != c.by_height.len() as u64 {
            return Err(format!("block {} accepted at head {}", b.meta.height, c.by_height.len() - 1).into());
        }
        c.by_height.push(b.meta.id);
        c.by_id.insert(b.meta.id, b.clone());
        Ok(())
    }

    fn last_accepted(&self) -> Self::Block {
        let c = self.chain.lock().unwrap();
        c.by_id[c.by_height.last().unwrap()].clone()
    }

    fn get_block(&self, id: &Id) -> Option<Self::Block> {
        self.chain.lock().unwrap().by_id.get(id).cloned()
    }

    fn block_id_at_height(&self, height: u64) -> Option<Id> {
        self.chain.lock().unwrap().by_height.get(height as usize).copied()
    }

    /// JSON-RPC 2.0, single requests and batches.
    fn rpc(&self, body: &[u8]) -> Vec<u8> {
        let req: Value = match serde_json::from_slice(body) {
            Ok(v) => v,
            Err(e) => return json!({"jsonrpc": "2.0", "id": null, "error": {"code": -32700, "message": e.to_string()}}).to_string().into_bytes(),
        };
        let one = |r: &Value| {
            let id = r.get("id").cloned().unwrap_or(Value::Null);
            let method = r.get("method").and_then(Value::as_str).unwrap_or("");
            let params = r.get("params").and_then(Value::as_array).cloned().unwrap_or_default();
            match self.call(method, &params) {
                Ok(v) => json!({"jsonrpc": "2.0", "id": id, "result": v}),
                Err(m) => json!({"jsonrpc": "2.0", "id": id, "error": {"code": -32601, "message": m}}),
            }
        };
        let out = match &req {
            Value::Array(rs) => Value::Array(rs.iter().map(one).collect()),
            r => one(r),
        };
        out.to_string().into_bytes()
    }
}

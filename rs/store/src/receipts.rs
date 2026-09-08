//! The rcpt/<txnum> row (store/receipts.go EncodeTxReceipt): uvarint status |
//! uvarint gasUsed | uvarint cumulativeGasUsed | uvarint nLogs | per log:
//! addr20 | uvarint nTopics | topics | uvarint dataLen | data.

use crate::sst::{put_uvarint, uvarint};
use anyhow::{anyhow, bail, Result};

pub struct LogIn<'a> {
    pub address: &'a [u8],
    pub topics: Vec<&'a [u8]>,
    pub data: &'a [u8],
}

pub fn encode(status: u64, gas_used: u64, cumulative: u64, logs: &[LogIn]) -> Vec<u8> {
    let mut out = Vec::new();
    put_uvarint(&mut out, status);
    put_uvarint(&mut out, gas_used);
    put_uvarint(&mut out, cumulative);
    put_uvarint(&mut out, logs.len() as u64);
    for l in logs {
        out.extend_from_slice(l.address);
        put_uvarint(&mut out, l.topics.len() as u64);
        for t in &l.topics {
            out.extend_from_slice(t);
        }
        put_uvarint(&mut out, l.data.len() as u64);
        out.extend_from_slice(l.data);
    }
    out
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredLog {
    pub address: [u8; 20],
    pub topics: Vec<[u8; 32]>,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Receipt {
    pub status: u64,
    pub gas_used: u64,
    pub cumulative_gas_used: u64,
    pub logs: Vec<StoredLog>,
}

pub fn decode(rec: &[u8]) -> Result<Receipt> {
    let mut pos = 0;
    let mut next = |what: &str| -> Result<u64> {
        let (v, k) = uvarint(&rec[pos..]).ok_or_else(|| anyhow!("tx receipt: bad {what}"))?;
        pos += k;
        Ok(v)
    };
    let status = next("status")?;
    let gas_used = next("gas used")?;
    let cumulative_gas_used = next("cumulative gas")?;
    let n = next("log count")?;
    let mut logs = Vec::with_capacity(n as usize);
    for _ in 0..n {
        if pos + 20 > rec.len() {
            bail!("tx receipt: truncated addr");
        }
        let address: [u8; 20] = rec[pos..pos + 20].try_into().unwrap();
        pos += 20;
        let (nt, k) = uvarint(&rec[pos..]).ok_or_else(|| anyhow!("tx receipt: bad topic count"))?;
        pos += k;
        let mut topics = Vec::with_capacity(nt as usize);
        for _ in 0..nt {
            if pos + 32 > rec.len() {
                bail!("tx receipt: truncated topics");
            }
            topics.push(rec[pos..pos + 32].try_into().unwrap());
            pos += 32;
        }
        let (dl, k) = uvarint(&rec[pos..]).ok_or_else(|| anyhow!("tx receipt: bad data len"))?;
        pos += k;
        if pos + dl as usize > rec.len() {
            bail!("tx receipt: truncated data");
        }
        let data = rec[pos..pos + dl as usize].to_vec();
        pos += dl as usize;
        logs.push(StoredLog { address, topics, data });
    }
    Ok(Receipt { status, gas_used, cumulative_gas_used, logs })
}

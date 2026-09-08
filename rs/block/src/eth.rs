//! subnet-evm block RLP: the header with its extra fields (BlockGasCost after
//! BaseFee, then the EIP-4844/4788 and Granite optionals) and the
//! transactions (legacy, EIP-2930, EIP-1559).
use alloy_primitives::{Address, Bloom, B256, B64, U256};
use alloy_rlp::{Decodable, Header as RlpHeader};
use bytes::Bytes;

use crate::Error;

#[derive(Debug, Clone)]
pub struct Header {
    pub parent_hash: B256,
    pub uncle_hash: B256,
    pub coinbase: Address,
    pub root: B256,
    pub tx_hash: B256,
    pub receipt_hash: B256,
    pub bloom: Bloom,
    pub difficulty: U256,
    pub number: u64,
    pub gas_limit: u64,
    pub gas_used: u64,
    pub time: u64,
    pub extra: alloy_primitives::Bytes,
    pub mix_digest: B256,
    pub nonce: B64,
    pub base_fee: Option<U256>,
    pub block_gas_cost: Option<U256>,
    pub blob_gas_used: Option<u64>,
    pub excess_blob_gas: Option<u64>,
    pub parent_beacon_root: Option<B256>,
    pub time_milliseconds: Option<u64>,
    pub min_delay_excess: Option<u64>,
}

fn opt<T: Decodable>(p: &mut &[u8]) -> Result<Option<T>, Error> {
    if p.is_empty() {
        Ok(None)
    } else {
        Ok(Some(T::decode(p)?))
    }
}

fn list(p: &mut &[u8]) -> Result<RlpHeader, Error> {
    let h = RlpHeader::decode(p)?;
    if !h.list {
        return Err("expected an RLP list".into());
    }
    Ok(h)
}

pub fn decode_header(rlp: &[u8]) -> Result<Header, Error> {
    let mut p = rlp;
    let h = list(&mut p)?;
    if h.payload_length != p.len() {
        return Err("header RLP has trailing bytes".into());
    }
    let hd = Header {
        parent_hash: B256::decode(&mut p)?,
        uncle_hash: B256::decode(&mut p)?,
        coinbase: Address::decode(&mut p)?,
        root: B256::decode(&mut p)?,
        tx_hash: B256::decode(&mut p)?,
        receipt_hash: B256::decode(&mut p)?,
        bloom: Bloom::decode(&mut p)?,
        difficulty: U256::decode(&mut p)?,
        number: u64::decode(&mut p)?,
        gas_limit: u64::decode(&mut p)?,
        gas_used: u64::decode(&mut p)?,
        time: u64::decode(&mut p)?,
        extra: alloy_primitives::Bytes::decode(&mut p)?,
        mix_digest: B256::decode(&mut p)?,
        nonce: B64::decode(&mut p)?,
        base_fee: opt(&mut p)?,
        block_gas_cost: opt(&mut p)?,
        blob_gas_used: opt(&mut p)?,
        excess_blob_gas: opt(&mut p)?,
        parent_beacon_root: opt(&mut p)?,
        time_milliseconds: opt(&mut p)?,
        min_delay_excess: opt(&mut p)?,
    };
    if !p.is_empty() {
        return Err(format!("header RLP: {} unknown trailing bytes", p.len()).into());
    }
    Ok(hd)
}

/// encode_header is the inverse of decode_header: the optional tail is
/// written through the last field that is Some (an earlier None in front of
/// a Some is an error, subnet-evm never produces one).
pub fn encode_header(h: &Header) -> Result<Vec<u8>, Error> {
    use alloy_rlp::Encodable;
    let mut body = Vec::with_capacity(600);
    h.parent_hash.encode(&mut body);
    h.uncle_hash.encode(&mut body);
    h.coinbase.encode(&mut body);
    h.root.encode(&mut body);
    h.tx_hash.encode(&mut body);
    h.receipt_hash.encode(&mut body);
    h.bloom.encode(&mut body);
    h.difficulty.encode(&mut body);
    h.number.encode(&mut body);
    h.gas_limit.encode(&mut body);
    h.gas_used.encode(&mut body);
    h.time.encode(&mut body);
    h.extra.encode(&mut body);
    h.mix_digest.encode(&mut body);
    h.nonce.encode(&mut body);
    let tail: [Option<Vec<u8>>; 7] = [
        h.base_fee.map(|v| { let mut b = Vec::new(); v.encode(&mut b); b }),
        h.block_gas_cost.map(|v| { let mut b = Vec::new(); v.encode(&mut b); b }),
        h.blob_gas_used.map(|v| { let mut b = Vec::new(); v.encode(&mut b); b }),
        h.excess_blob_gas.map(|v| { let mut b = Vec::new(); v.encode(&mut b); b }),
        h.parent_beacon_root.map(|v| { let mut b = Vec::new(); v.encode(&mut b); b }),
        h.time_milliseconds.map(|v| { let mut b = Vec::new(); v.encode(&mut b); b }),
        h.min_delay_excess.map(|v| { let mut b = Vec::new(); v.encode(&mut b); b }),
    ];
    let last = tail.iter().rposition(Option::is_some).map_or(0, |i| i + 1);
    for (i, t) in tail.iter().take(last).enumerate() {
        body.extend_from_slice(t.as_ref().ok_or_else(|| format!("header optional field {i} is None before a later Some"))?);
    }
    let mut out = Vec::with_capacity(body.len() + 3);
    RlpHeader { list: true, payload_length: body.len() }.encode(&mut out);
    out.extend_from_slice(&body);
    Ok(out)
}

#[derive(Debug, Clone)]
pub struct AccessItem {
    pub address: Address,
    pub storage_keys: Vec<B256>,
}

#[derive(Debug, Clone)]
pub struct Tx {
    /// The envelope: the RLP list for a legacy tx, type byte + payload for a
    /// typed one (what eth_getRawTransaction returns).
    pub raw: Bytes,
    /// keccak(raw).
    pub hash: B256,
    /// Recovered by `sender::recover`; None until then, or when the
    /// signature does not recover.
    pub sender: Option<Address>,
    pub tx_type: u8,
    /// None for a pre-EIP-155 legacy tx.
    pub chain_id: Option<u64>,
    pub nonce: u64,
    /// gasPrice for legacy and EIP-2930, maxFeePerGas for EIP-1559.
    pub gas_price: u128,
    /// maxPriorityFeePerGas for EIP-1559, else gas_price.
    pub gas_tip: u128,
    pub gas_limit: u64,
    /// None = contract creation.
    pub to: Option<Address>,
    pub value: U256,
    pub input: Bytes,
    pub access_list: Vec<AccessItem>,
    pub v: u64,
    pub r: U256,
    pub s: U256,
    /// The recovery id: v - 27 or v - chainId*2 - 35 for legacy, yParity for typed.
    pub recid: u8,
    /// Offsets in raw of the list payload and of the v / yParity item, so the
    /// signing hash is the fields between, re-wrapped, no re-encoding.
    pub body_off: usize,
    pub sig_off: usize,
}

fn decode_to(p: &mut &[u8]) -> Result<Option<Address>, Error> {
    let s = RlpHeader::decode_bytes(p, false)?;
    match s.len() {
        0 => Ok(None),
        20 => Ok(Some(Address::from_slice(s))),
        n => Err(format!("tx to: {n} bytes").into()),
    }
}

fn decode_input(raw: &Bytes, p: &mut &[u8]) -> Result<Bytes, Error> {
    Ok(raw.slice_ref(RlpHeader::decode_bytes(p, false)?))
}

fn decode_access_list(p: &mut &[u8]) -> Result<Vec<AccessItem>, Error> {
    let h = list(p)?;
    let (mut q, rest) = p.split_at(h.payload_length);
    *p = rest;
    let mut out = Vec::new();
    while !q.is_empty() {
        let ih = list(&mut q)?;
        let (mut item, rest) = q.split_at(ih.payload_length);
        q = rest;
        let address = Address::decode(&mut item)?;
        let kh = list(&mut item)?;
        let mut keys = &item[..kh.payload_length];
        let mut storage_keys = Vec::with_capacity(kh.payload_length / 33);
        while !keys.is_empty() {
            storage_keys.push(B256::decode(&mut keys)?);
        }
        out.push(AccessItem { address, storage_keys });
    }
    Ok(out)
}

pub fn decode_tx(raw: Bytes) -> Result<Tx, Error> {
    let hash = alloy_primitives::keccak256(&raw);
    let first = *raw.first().ok_or("empty tx")?;
    let tx_type = if first >= 0xc0 { 0 } else { first };
    let mut p = if tx_type == 0 { &raw[..] } else { &raw[1..] };
    let h = list(&mut p)?;
    if h.payload_length != p.len() {
        return Err("tx RLP has trailing bytes".into());
    }
    let body_off = raw.len() - p.len();
    let mut chain_id = None;
    let (nonce, gas_price, gas_tip, gas_limit, to, value, input, access_list);
    match tx_type {
        0 => {
            nonce = u64::decode(&mut p)?;
            gas_price = u128::decode(&mut p)?;
            gas_tip = gas_price;
            gas_limit = u64::decode(&mut p)?;
            to = decode_to(&mut p)?;
            value = U256::decode(&mut p)?;
            input = decode_input(&raw, &mut p)?;
            access_list = Vec::new();
        }
        1 => {
            chain_id = Some(u64::decode(&mut p)?);
            nonce = u64::decode(&mut p)?;
            gas_price = u128::decode(&mut p)?;
            gas_tip = gas_price;
            gas_limit = u64::decode(&mut p)?;
            to = decode_to(&mut p)?;
            value = U256::decode(&mut p)?;
            input = decode_input(&raw, &mut p)?;
            access_list = decode_access_list(&mut p)?;
        }
        2 => {
            chain_id = Some(u64::decode(&mut p)?);
            nonce = u64::decode(&mut p)?;
            gas_tip = u128::decode(&mut p)?;
            gas_price = u128::decode(&mut p)?;
            gas_limit = u64::decode(&mut p)?;
            to = decode_to(&mut p)?;
            value = U256::decode(&mut p)?;
            input = decode_input(&raw, &mut p)?;
            access_list = decode_access_list(&mut p)?;
        }
        t => return Err(format!("tx type {t:#x} not supported").into()),
    }
    let sig_off = raw.len() - p.len();
    let v = u64::decode(&mut p)?;
    let r = U256::decode(&mut p)?;
    let s = U256::decode(&mut p)?;
    if !p.is_empty() {
        return Err("tx RLP: bytes after the signature".into());
    }
    let recid = if tx_type != 0 {
        if v > 1 {
            return Err(format!("typed tx yParity {v}").into());
        }
        v as u8
    } else if v == 27 || v == 28 {
        (v - 27) as u8
    } else if v >= 35 {
        chain_id = Some((v - 35) / 2);
        ((v - 35) % 2) as u8
    } else {
        return Err(format!("legacy tx v {v}").into());
    };
    Ok(Tx {
        raw,
        hash,
        sender: None,
        tx_type,
        chain_id,
        nonce,
        gas_price,
        gas_tip,
        gas_limit,
        to,
        value,
        input,
        access_list,
        v,
        r,
        s,
        recid,
        body_off,
        sig_off,
    })
}

/// decode_block splits the inner block RLP `[header, txs, uncles]` into the
/// header's raw RLP and the decoded transactions. Uncles are not decoded.
pub fn decode_block(inner: &Bytes) -> Result<(Bytes, Vec<Tx>), Error> {
    let mut p = &inner[..];
    let h = list(&mut p)?;
    if h.payload_length != p.len() {
        return Err("block RLP has trailing bytes".into());
    }
    let start = p;
    let hh = list(&mut p)?;
    let hlen = hh.length() + hh.payload_length;
    let header_rlp = inner.slice_ref(&start[..hlen]);
    p = &start[hlen..];
    let th = list(&mut p)?;
    let (mut q, _uncles) = p.split_at(th.payload_length);
    let mut txs = Vec::new();
    while !q.is_empty() {
        let start = q;
        let ih = RlpHeader::decode(&mut q)?;
        let total = ih.length() + ih.payload_length;
        let raw = if ih.list { &start[..total] } else { &start[ih.length()..total] };
        q = &start[total..];
        txs.push(decode_tx(inner.slice_ref(raw))?);
    }
    Ok((header_rlp, txs))
}

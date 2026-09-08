//! Container reassembly (store/container.go): the pvm row is a template
//! [4B len(A)] A [4B len(B)] B C around the header RLP and the tx RLPs, or
//! empty for the bare three-field pre-proposervm block.

use anyhow::{anyhow, bail, Result};
use state::rlp;

/// The bare [header, [txs], []] block from stored bytes.
fn bare_block(header_rlp: &[u8], txs: &[&[u8]]) -> Vec<u8> {
    let txs_len: usize = txs.iter().map(|t| t.len()).sum();
    let inner_len = header_rlp.len() + rlp::header_len(txs_len) + txs_len + 1;
    let mut out = Vec::with_capacity(rlp::header_len(inner_len) + inner_len);
    rlp::put_header(&mut out, true, inner_len);
    out.extend_from_slice(header_rlp);
    rlp::put_header(&mut out, true, txs_len);
    for t in txs {
        out.extend_from_slice(t);
    }
    out.push(0xc0);
    out
}

pub fn reassemble(pvm: &[u8], header_rlp: &[u8], txs: &[&[u8]]) -> Result<Vec<u8>> {
    if pvm.is_empty() {
        return Ok(bare_block(header_rlp, txs));
    }
    if pvm.len() < 8 {
        bail!("store: pvm row is {} bytes, too short for a template", pvm.len());
    }
    let la = u32::from_be_bytes(pvm[..4].try_into().unwrap()) as usize;
    if 4 + la + 4 > pvm.len() {
        bail!("store: pvm row: bad prefix length {la}");
    }
    let a = &pvm[4..4 + la];
    let lb = u32::from_be_bytes(pvm[4 + la..8 + la].try_into().unwrap()) as usize;
    if 8 + la + lb > pvm.len() {
        bail!("store: pvm row: bad tx-list header length {lb}");
    }
    let (b, c) = (&pvm[8 + la..8 + la + lb], &pvm[8 + la + lb..]);
    let mut out = Vec::with_capacity(pvm.len() + header_rlp.len() + txs.iter().map(|t| t.len()).sum::<usize>());
    out.extend_from_slice(a);
    out.extend_from_slice(header_rlp);
    out.extend_from_slice(b);
    for t in txs {
        out.extend_from_slice(t);
    }
    out.extend_from_slice(c);
    Ok(out)
}

/// The pvm row for a container whose inner eth block sits at
/// [inner_off, inner_off+inner_len). Checks its own work: the pieces must
/// reassemble to the container byte for byte.
pub fn split_container(raw: &[u8], inner_off: usize, inner_len: usize, header_rlp: &[u8], txs: &[&[u8]]) -> Result<Vec<u8>> {
    if bare_block(header_rlp, txs) == raw {
        return Ok(Vec::new());
    }
    let inner = &raw[inner_off..inner_off + inner_len];
    let (content, rest) = rlp::split_list(inner).ok_or_else(|| anyhow!("store: block is not an RLP list"))?;
    let hs = inner.len() - rest.len() - content.len();
    let (_, _, after_header) = rlp::split(content).ok_or_else(|| anyhow!("store: block header element"))?;
    let he = inner.len() - after_header.len();
    let (txs_content, after_txs) = rlp::split_list(after_header).ok_or_else(|| anyhow!("store: block transaction list"))?;
    let ts = inner.len() - after_txs.len() - txs_content.len();
    let te = ts + txs_content.len();
    let (hs, he, ts, te) = (inner_off + hs, inner_off + he, inner_off + ts, inner_off + te);
    let (a, b, c) = (&raw[..hs], &raw[he..ts], &raw[te..]);
    let mut pvm = Vec::with_capacity(8 + a.len() + b.len() + c.len());
    pvm.extend_from_slice(&(a.len() as u32).to_be_bytes());
    pvm.extend_from_slice(a);
    pvm.extend_from_slice(&(b.len() as u32).to_be_bytes());
    pvm.extend_from_slice(b);
    pvm.extend_from_slice(c);
    let out = reassemble(&pvm, header_rlp, txs)?;
    if out != raw {
        bail!("store: container does not round trip ({} bytes in, {} out)", raw.len(), out.len());
    }
    Ok(pvm)
}

/// The transaction elements of an inner eth block, each as its full RLP
/// bytes (a typed tx keeps its string header): what a tx/ row stores.
pub fn tx_elements(inner: &[u8]) -> Result<Vec<&[u8]>> {
    let (content, _) = rlp::split_list(inner).ok_or_else(|| anyhow!("store: block is not an RLP list"))?;
    let (_, _, after_header) = rlp::split(content).ok_or_else(|| anyhow!("store: block header element"))?;
    let (mut txs, _) = rlp::split_list(after_header).ok_or_else(|| anyhow!("store: block transaction list"))?;
    let mut out = Vec::new();
    while !txs.is_empty() {
        let (_, _, rest) = rlp::split(txs).ok_or_else(|| anyhow!("store: transaction element"))?;
        out.push(&txs[..txs.len() - rest.len()]);
        txs = rest;
    }
    Ok(out)
}

/// The envelope of a stored tx element (eth_getRawTransaction's bytes,
/// what the tx hash is over): the element itself for a legacy tx, the
/// string's content for a typed one.
pub fn tx_envelope(elem: &[u8]) -> &[u8] {
    if elem.first().map(|b| *b >= 0xc0).unwrap_or(true) {
        return elem;
    }
    rlp::split_string(elem).map(|(c, _)| c).unwrap_or(elem)
}

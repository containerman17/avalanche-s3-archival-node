//! Sender recovery: the signing hash by signer rule (pre-155 legacy, EIP-155,
//! typed) and secp256k1 (the C library) public key recovery.
use std::sync::LazyLock;

use alloy_primitives::{keccak256, Address, B256};
use alloy_rlp::{Encodable, Header as RlpHeader};
use secp256k1::ecdsa::{RecoverableSignature, RecoveryId};
use secp256k1::{Message, Secp256k1, VerifyOnly};

use crate::eth::Tx;

static SECP: LazyLock<Secp256k1<VerifyOnly>> = LazyLock::new(Secp256k1::verification_only);

/// sighash is what the sender signed: the unsigned fields re-wrapped in a
/// list (plus chainId, 0, 0 for EIP-155), behind the type byte for typed txs.
pub fn sighash(tx: &Tx) -> B256 {
    let fields = &tx.raw[tx.body_off..tx.sig_off];
    let mut tail = Vec::new();
    if tx.tx_type == 0 {
        if let Some(id) = tx.chain_id {
            id.encode(&mut tail);
            tail.extend_from_slice(&[0x80, 0x80]);
        }
    }
    let mut out = Vec::with_capacity(fields.len() + tail.len() + 16);
    if tx.tx_type != 0 {
        out.push(tx.tx_type);
    }
    RlpHeader { list: true, payload_length: fields.len() + tail.len() }.encode(&mut out);
    out.extend_from_slice(fields);
    out.extend_from_slice(&tail);
    keccak256(&out)
}

/// recover is the tx sender, None when the signature does not recover.
// ponytail: no low-s / r,s < N range check beyond what libsecp256k1 rejects;
// the chain's blocks are already valid.
pub fn recover(tx: &Tx) -> Option<Address> {
    let mut sig = [0u8; 64];
    sig[..32].copy_from_slice(&tx.r.to_be_bytes::<32>());
    sig[32..].copy_from_slice(&tx.s.to_be_bytes::<32>());
    let rs = RecoverableSignature::from_compact(&sig, RecoveryId::try_from(tx.recid as i32).ok()?).ok()?;
    let pk = SECP.recover_ecdsa(Message::from_digest(sighash(tx).0), &rs).ok()?;
    let pk = pk.serialize_uncompressed();
    Some(Address::from_slice(&keccak256(&pk[1..])[12..]))
}

//! Avalanche Warp Messaging (precompile/contracts/warp + core/predicate_check.go
//! + avalanchego vms/platformvm/warp): the precompile (getBlockchainID,
//! sendWarpMessage with its log, getVerifiedWarpMessage /
//! getVerifiedWarpBlockHash over the tx's pre-verified predicates), the
//! predicate codec (access-list storage keys -> message bytes), the header's
//! predicate results codec, the predicate gas that replaces the access-list
//! gas of the warp entries, and the BLS verification against a validator set
//! the host provides through `ValidatorState`.
//!
//! avalanchego linearcodec: u16 codec version 0, then the struct fields in
//! order: u32 big-endian, ids.ID 32 raw bytes, []byte u32-length-prefixed,
//! interfaces a u32 type id first. Warp message = UnsignedMessage{networkID
//! u32, sourceChainID, payload []byte} + Signature(type 0 BitSetSignature
//! {signers []byte, signature [96]byte}). Payload = type 0 Hash{hash} or
//! type 1 AddressedCall{sourceAddress []byte, payload []byte}.

use crate::precompile::{
    abi_bytes, abi_u32, add_log, deduct, event_sig, invalid_selector, pack_bytes, selector, split_selector, topic_addr,
    Env, Halt, LOG_DATA_GAS, LOG_GAS, LOG_TOPIC_GAS, WARP, WRITE_GAS,
};
use alloy_primitives::{Address, Bytes, B256, U256};
use revm::{context::ContextTr, interpreter::Gas};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::OnceLock;

pub const DEFAULT_QUORUM_NUMERATOR: u64 = 67;
pub const QUORUM_DENOMINATOR: u64 = 100;
const ADD_WARP_MESSAGE_BASE_GAS: u64 = 20_000;

/// warp.GasConfig: the pre-Granite and Granite costs.
#[derive(Debug, Clone, Copy)]
pub struct GasConfig {
    pub get_blockchain_id: u64,
    pub get_verified_base: u64,
    pub per_signer: u64,
    pub per_chunk: u64,
    pub verify_predicate_base: u64,
    pub send_base: u64,
    pub per_message_byte: u64,
}

pub fn gas_config(granite: bool) -> GasConfig {
    let send_base = LOG_GAS + 3 * LOG_TOPIC_GAS + ADD_WARP_MESSAGE_BASE_GAS + WRITE_GAS;
    if granite {
        GasConfig { get_blockchain_id: 200, get_verified_base: 750, per_signer: 250, per_chunk: 512, verify_predicate_base: 125_000, send_base, per_message_byte: LOG_DATA_GAS }
    } else {
        GasConfig { get_blockchain_id: 2, get_verified_base: 2, per_signer: 500, per_chunk: 3_200, verify_predicate_base: 200_000, send_base, per_message_byte: LOG_DATA_GAS }
    }
}

// ---------------------------------------------------------------------------
// Codec.

struct Reader<'a> {
    b: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], String> {
        let s = self.b.get(self.pos..self.pos + n).ok_or("insufficient length for input")?;
        self.pos += n;
        Ok(s)
    }
    fn u16(&mut self) -> Result<u16, String> {
        Ok(u16::from_be_bytes(self.take(2)?.try_into().unwrap()))
    }
    fn u32(&mut self) -> Result<u32, String> {
        Ok(u32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn bytes(&mut self) -> Result<&'a [u8], String> {
        let n = self.u32()? as usize;
        self.take(n)
    }
    fn done(&self) -> Result<(), String> {
        if self.pos != self.b.len() {
            return Err("extra space after unmarshalling".into());
        }
        Ok(())
    }
}

fn put_bytes(out: &mut Vec<u8>, b: &[u8]) {
    out.extend_from_slice(&(b.len() as u32).to_be_bytes());
    out.extend_from_slice(b);
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnsignedMessage {
    pub network_id: u32,
    pub source_chain_id: B256,
    pub payload: Vec<u8>,
}

impl UnsignedMessage {
    pub fn bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(42 + self.payload.len());
        out.extend_from_slice(&0u16.to_be_bytes());
        out.extend_from_slice(&self.network_id.to_be_bytes());
        out.extend_from_slice(self.source_chain_id.as_slice());
        put_bytes(&mut out, &self.payload);
        out
    }
    /// sha256 of the bytes.
    pub fn id(&self) -> B256 {
        B256::from_slice(&Sha256::digest(self.bytes()))
    }
}

#[derive(Debug, Clone)]
pub struct Message {
    pub unsigned: UnsignedMessage,
    /// BitSetSignature.Signers (a big-endian big.Int bitset).
    pub signers: Vec<u8>,
    pub signature: [u8; 96],
}

pub fn parse_message(b: &[u8]) -> Result<Message, String> {
    let mut r = Reader { b, pos: 0 };
    if r.u16()? != 0 {
        return Err("unknown codec version".into());
    }
    let network_id = r.u32()?;
    let source_chain_id = B256::from_slice(r.take(32)?);
    let payload = r.bytes()?.to_vec();
    if r.u32()? != 0 {
        return Err("unknown signature type".into());
    }
    let signers = r.bytes()?.to_vec();
    let signature: [u8; 96] = r.take(96)?.try_into().unwrap();
    r.done()?;
    Ok(Message { unsigned: UnsignedMessage { network_id, source_chain_id, payload }, signers, signature })
}

pub enum Payload {
    Hash(B256),
    AddressedCall { source_address: Vec<u8>, payload: Vec<u8> },
}

pub fn parse_payload(b: &[u8]) -> Result<Payload, String> {
    let mut r = Reader { b, pos: 0 };
    if r.u16()? != 0 {
        return Err("unknown codec version".into());
    }
    let p = match r.u32()? {
        0 => Payload::Hash(B256::from_slice(r.take(32)?)),
        1 => {
            let source_address = r.bytes()?.to_vec();
            let payload = r.bytes()?.to_vec();
            Payload::AddressedCall { source_address, payload }
        }
        t => return Err(format!("unknown payload type {t}")),
    };
    r.done()?;
    Ok(p)
}

pub fn addressed_call_bytes(source_address: &[u8], payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(14 + source_address.len() + payload.len());
    out.extend_from_slice(&0u16.to_be_bytes());
    out.extend_from_slice(&1u32.to_be_bytes());
    put_bytes(&mut out, source_address);
    put_bytes(&mut out, payload);
    out
}

pub fn hash_payload_bytes(h: B256) -> Vec<u8> {
    let mut out = Vec::with_capacity(38);
    out.extend_from_slice(&0u16.to_be_bytes());
    out.extend_from_slice(&0u32.to_be_bytes());
    out.extend_from_slice(h.as_slice());
    out
}

/// set.Bits over a big-endian big.Int byte string.
pub fn bits_count(b: &[u8]) -> usize {
    b.iter().map(|x| x.count_ones() as usize).sum()
}

pub fn bits_contains(b: &[u8], i: usize) -> bool {
    let n = b.len();
    let byte = i / 8;
    byte < n && (b[n - 1 - byte] >> (i % 8)) & 1 == 1
}

pub fn bits_bitlen(b: &[u8]) -> usize {
    for (i, x) in b.iter().enumerate() {
        if *x != 0 {
            return (b.len() - i - 1) * 8 + (8 - x.leading_zeros() as usize);
        }
    }
    0
}

/// big.Int.Bytes(): no leading zero bytes.
pub fn bits_normalized(b: &[u8]) -> bool {
    b.first().map_or(true, |x| *x != 0)
}

pub fn bits_from_indices(idx: &[usize]) -> Vec<u8> {
    let Some(max) = idx.iter().max() else { return Vec::new() };
    let mut out = vec![0u8; max / 8 + 1];
    let n = out.len();
    for i in idx {
        out[n - 1 - i / 8] |= 1 << (i % 8);
    }
    out
}

/// predicate.Predicate.Bytes(): the chunks concatenated, right zero-trimmed,
/// the 0xff delimiter dropped; excess chunks are an error.
pub fn predicate_bytes(chunks: &[B256]) -> Result<Vec<u8>, String> {
    let mut padded = Vec::with_capacity(chunks.len() * 32);
    for c in chunks {
        padded.extend_from_slice(c.as_slice());
    }
    let trimmed_len = padded.iter().rposition(|b| *b != 0).map_or(0, |p| p + 1);
    if trimmed_len == 0 {
        return Err(format!("no delimiter found: length ({})", chunks.len()));
    }
    let expected = (trimmed_len + 31) / 32;
    if expected != chunks.len() {
        return Err(format!("predicate included excess padding: got length ({}), expected length ({expected})", chunks.len()));
    }
    if padded[trimmed_len - 1] != 0xff {
        return Err("wrong delimiter".into());
    }
    padded.truncate(trimmed_len - 1);
    Ok(padded)
}

/// predicate.New: the bytes chunked into 32-byte words with the delimiter.
pub fn predicate_chunks(b: &[u8]) -> Vec<B256> {
    let mut v = b.to_vec();
    v.push(0xff);
    let mut out = Vec::with_capacity(v.len() / 32 + 1);
    for c in v.chunks(32) {
        out.push(B256::right_padding_from(c));
    }
    out
}

/// The header's predicate results (customheader.PredicateBytesFromExtra,
/// predicate.ParseBlockResults): tx hash -> precompile -> failed bitset.
pub type BlockResults = HashMap<B256, HashMap<Address, Vec<u8>>>;

pub fn parse_block_results(b: &[u8]) -> Result<BlockResults, String> {
    let mut r = Reader { b, pos: 0 };
    if r.u16()? != 0 {
        return Err("unknown codec version".into());
    }
    let n = r.u32()? as usize;
    let mut out = HashMap::with_capacity(n);
    for _ in 0..n {
        let tx = B256::from_slice(r.take(32)?);
        let m = r.u32()? as usize;
        let mut per = HashMap::with_capacity(m);
        for _ in 0..m {
            let addr = Address::from_slice(r.take(20)?);
            let bits = r.bytes()?.to_vec();
            per.insert(addr, bits);
        }
        out.insert(tx, per);
    }
    r.done()?;
    Ok(out)
}

/// subnetevm.WindowSize: the fee window prefix of header.Extra.
pub const EXTRA_WINDOW_SIZE: usize = 80;

pub fn predicate_bytes_from_extra(extra: &[u8]) -> &[u8] {
    if extra.len() <= EXTRA_WINDOW_SIZE {
        &[]
    } else {
        &extra[EXTRA_WINDOW_SIZE..]
    }
}

// ---------------------------------------------------------------------------
// Predicate gas and verification (Config.PredicateGas / VerifyPredicate).

/// PredicateGas: base + per chunk + per signer; a message that does not parse
/// invalidates the tx.
pub fn predicate_gas(chunks: &[B256], granite: bool) -> Result<u64, String> {
    let g = gas_config(granite);
    let mut total = g.verify_predicate_base.checked_add(g.per_chunk.checked_mul(chunks.len() as u64).ok_or("overflow")?).ok_or("overflow")?;
    let raw = predicate_bytes(chunks).map_err(|e| format!("cannot unpack predicate bytes: {e}"))?;
    let msg = parse_message(&raw).map_err(|e| format!("cannot unpack warp message: {e}"))?;
    parse_payload(&msg.unsigned.payload).map_err(|e| format!("cannot unpack warp message payload: {e}"))?;
    if !bits_normalized(&msg.signers) {
        return Err("cannot fetch num signers from warp message: bitset is invalid".into());
    }
    let signers = bits_count(&msg.signers) as u64;
    total = total.checked_add(signers.checked_mul(g.per_signer).ok_or("overflow calculating warp signers gas cost")?).ok_or("overflow")?;
    Ok(total)
}

/// One validator of a canonical (flattened) warp set: the BLS public key in
/// its 96-byte uncompressed form and the summed weight of the nodes behind it.
#[derive(Debug, Clone)]
pub struct WarpValidator {
    pub public_key: Vec<u8>,
    pub weight: u64,
}

/// validators.WarpSet: sorted by public key bytes; TotalWeight counts the
/// validators without a key too.
#[derive(Debug, Clone, Default)]
pub struct WarpSet {
    pub validators: Vec<WarpValidator>,
    pub total_weight: u64,
}

impl WarpSet {
    /// validators.FlattenValidatorSet over (compressed public key, weight).
    pub fn flatten(vdrs: impl IntoIterator<Item = (Option<Vec<u8>>, u64)>) -> Result<WarpSet, String> {
        let mut total: u64 = 0;
        let mut by_key: HashMap<Vec<u8>, u64> = HashMap::new();
        for (pk, w) in vdrs {
            total = total.checked_add(w).ok_or("weight overflowed")?;
            let Some(pk) = pk else { continue };
            let key = blst::min_pk::PublicKey::uncompress(&pk).map_err(|e| format!("bad public key: {e:?}"))?;
            let ser = key.serialize().to_vec();
            *by_key.entry(ser).or_insert(0) += w;
        }
        let mut validators: Vec<WarpValidator> = by_key.into_iter().map(|(public_key, weight)| WarpValidator { public_key, weight }).collect();
        validators.sort_by(|a, b| a.public_key.cmp(&b.public_key));
        Ok(WarpSet { validators, total_weight: total })
    }
}

/// What the host answers for predicate verification (snow.Context.ValidatorState).
pub trait ValidatorState {
    /// GetSubnetID(chainID).
    fn subnet_id(&mut self, chain_id: B256) -> Result<B256, String>;
    /// GetWarpValidatorSets(height)[subnetID]; None when the subnet cannot sign.
    fn validator_set(&mut self, pchain_height: u64, subnet_id: B256) -> Result<Option<WarpSet>, String>;
}

pub const PRIMARY_NETWORK_ID: B256 = B256::ZERO;
pub const PLATFORM_CHAIN_ID: B256 = B256::ZERO;

pub const BLS_DST: &[u8] = b"BLS_SIG_BLS12381G2_XMD:SHA-256_SSWU_RO_POP_";

/// BitSetSignature.Verify over the canonical set.
pub fn verify_signature(msg: &Message, network_id: u32, set: &WarpSet, quorum_num: u64) -> Result<(), String> {
    if msg.unsigned.network_id != network_id {
        return Err("wrong network ID".into());
    }
    if !bits_normalized(&msg.signers) {
        return Err("bitset is invalid".into());
    }
    if bits_bitlen(&msg.signers) > set.validators.len() {
        return Err(format!("unknown validator: NumIndices ({}) >= NumFilteredValidators ({})", bits_bitlen(&msg.signers) as i64 - 1, set.validators.len()));
    }
    let signers: Vec<&WarpValidator> = set.validators.iter().enumerate().filter(|(i, _)| bits_contains(&msg.signers, *i)).map(|(_, v)| v).collect();
    let mut sig_weight: u64 = 0;
    for v in &signers {
        sig_weight = sig_weight.checked_add(v.weight).ok_or("weight overflowed")?;
    }
    let scaled_total = (set.total_weight as u128) * (quorum_num as u128);
    let scaled_sig = (sig_weight as u128) * (QUORUM_DENOMINATOR as u128);
    if scaled_total > scaled_sig {
        return Err(format!("signature weight is insufficient: {quorum_num}*{} > {QUORUM_DENOMINATOR}*{sig_weight}", set.total_weight));
    }
    let sig = blst::min_pk::Signature::uncompress(&msg.signature).map_err(|e| format!("failed to parse signature: {e:?}"))?;
    sig.validate(false).map_err(|e| format!("failed to parse signature: {e:?}"))?;
    if signers.is_empty() {
        return Err("no public keys".into());
    }
    let keys: Vec<blst::min_pk::PublicKey> = signers
        .iter()
        .map(|v| blst::min_pk::PublicKey::deserialize(&v.public_key).map_err(|e| format!("bad public key: {e:?}")))
        .collect::<Result<_, _>>()?;
    let refs: Vec<&blst::min_pk::PublicKey> = keys.iter().collect();
    let agg = blst::min_pk::AggregatePublicKey::aggregate(&refs, false).map_err(|e| format!("failed to aggregate public keys: {e:?}"))?;
    let pk = agg.to_public_key();
    let unsigned = msg.unsigned.bytes();
    if sig.verify(false, &unsigned, BLS_DST, &[], &pk, false) != blst::BLST_ERROR::BLST_SUCCESS {
        return Err("signature is invalid".into());
    }
    Ok(())
}

/// Config.VerifyPredicate for one predicate of the tx. `subnet_id` is this
/// chain's subnet, `pchain_height` the proposervm context height.
pub fn verify_predicate(
    vs: &mut dyn ValidatorState,
    chunks: &[B256],
    network_id: u32,
    subnet_id: B256,
    pchain_height: u64,
    quorum_numerator: u64,
    require_primary_network_signers: bool,
) -> Result<(), String> {
    let raw = predicate_bytes(chunks).map_err(|e| format!("cannot unpack predicate bytes: {e}"))?;
    let msg = parse_message(&raw).map_err(|e| format!("cannot parse warp message: {e}"))?;
    let quorum = if quorum_numerator != 0 { quorum_numerator } else { DEFAULT_QUORUM_NUMERATOR };
    let mut source_subnet = vs.subnet_id(msg.unsigned.source_chain_id).map_err(|e| format!("cannot retrieve validator set: {e}"))?;
    if source_subnet == PRIMARY_NETWORK_ID && (!require_primary_network_signers || msg.unsigned.source_chain_id == PLATFORM_CHAIN_ID) {
        source_subnet = subnet_id;
    }
    let set = vs
        .validator_set(pchain_height, source_subnet)
        .map_err(|e| format!("cannot retrieve validator set: {e}"))?
        .ok_or_else(|| format!("cannot retrieve validator set: {source_subnet} source subnet not found"))?;
    verify_signature(&msg, network_id, &set, quorum).map_err(|e| format!("cannot verify warp signature: {e}"))
}

// ---------------------------------------------------------------------------
// The precompile.

struct Selectors {
    get_blockchain_id: [u8; 4],
    get_verified_block_hash: [u8; 4],
    get_verified_message: [u8; 4],
    send: [u8; 4],
    ev_send: B256,
}

fn sels() -> &'static Selectors {
    static S: OnceLock<Selectors> = OnceLock::new();
    S.get_or_init(|| Selectors {
        get_blockchain_id: selector("getBlockchainID()"),
        get_verified_block_hash: selector("getVerifiedWarpBlockHash(uint32)"),
        get_verified_message: selector("getVerifiedWarpMessage(uint32)"),
        send: selector("sendWarpMessage(bytes)"),
        ev_send: event_sig("SendWarpMessage(address,bytes32,bytes)"),
    })
}

/// abi.encode((bytes32,address,bytes), bool): the invalid output has an empty message.
pub fn pack_message_output(msg: Option<(B256, Address, &[u8])>) -> Vec<u8> {
    let mut out = Vec::with_capacity(224);
    out.extend_from_slice(&U256::from(0x40).to_be_bytes::<32>());
    out.extend_from_slice(&U256::from(msg.is_some() as u8).to_be_bytes::<32>());
    let (chain, sender, payload) = msg.unwrap_or((B256::ZERO, Address::ZERO, &[]));
    out.extend_from_slice(chain.as_slice());
    out.extend_from_slice(topic_addr(sender).as_slice());
    out.extend_from_slice(&U256::from(0x60).to_be_bytes::<32>());
    pack_bytes(&mut out, payload);
    out
}

/// abi.encode((bytes32,bytes32), bool).
pub fn pack_block_hash_output(v: Option<(B256, B256)>) -> Vec<u8> {
    let mut out = Vec::with_capacity(96);
    let (chain, hash) = v.unwrap_or((B256::ZERO, B256::ZERO));
    out.extend_from_slice(chain.as_slice());
    out.extend_from_slice(hash.as_slice());
    out.extend_from_slice(&U256::from(v.is_some() as u8).to_be_bytes::<32>());
    out
}

pub fn call<CTX: ContextTr>(ctx: &mut CTX, env: &Env, input: &[u8], gas: &mut Gas, read_only: bool, caller: Address) -> Result<Bytes, Halt> {
    let (sel, args) = split_selector(input)?;
    let s = sels();
    let g = gas_config(env.granite);
    if sel == s.get_blockchain_id {
        deduct(gas, g.get_blockchain_id)?;
        return Ok(Bytes::from(env.blockchain_id.0));
    }
    if sel == s.send {
        deduct(gas, g.send_base)?;
        let payload_gas = g.per_message_byte.checked_mul(args.len() as u64).ok_or(Halt::OutOfGas)?;
        deduct(gas, payload_gas)?;
        if read_only {
            return Err(Halt::Err("write protection".into()));
        }
        let payload = abi_bytes(args, 0, 1).map_err(|e| Halt::Err(format!("invalid sendWarpMessage input: {e}")))?;
        let unsigned = UnsignedMessage { network_id: env.network_id, source_chain_id: env.blockchain_id, payload: addressed_call_bytes(caller.as_slice(), payload) };
        let id = unsigned.id();
        let mut data = Vec::new();
        data.extend_from_slice(&U256::from(0x20).to_be_bytes::<32>());
        pack_bytes(&mut data, &unsigned.bytes());
        add_log(ctx, WARP, vec![s.ev_send, topic_addr(caller), id], data);
        return Ok(Bytes::from(id.0));
    }
    let block_hash = if sel == s.get_verified_message {
        false
    } else if sel == s.get_verified_block_hash {
        true
    } else {
        return Err(invalid_selector(&sel));
    };
    // handleWarpMessage
    deduct(gas, g.get_verified_base)?;
    let index = abi_u32(args, 0, 1).map_err(|e| Halt::Err(format!("invalid index to specify warp message: {e}")))?;
    if index > i32::MAX as u32 {
        return Err(Halt::Err("invalid index to specify warp message: larger than MaxInt32".into()));
    }
    let index = index as usize;
    let pred = env.predicates.get(index);
    let valid = pred.is_some() && !env.predicate_failed(index);
    let Some(pred) = pred.filter(|_| valid) else {
        return Ok(Bytes::from(if block_hash { pack_block_hash_output(None) } else { pack_message_output(None) }));
    };
    let msg_gas = g.per_chunk.checked_mul(pred.len() as u64).ok_or(Halt::OutOfGas)?;
    deduct(gas, msg_gas)?;
    let raw = predicate_bytes(pred).map_err(|e| Halt::Err(format!("cannot unpack predicate bytes: {e}")))?;
    let msg = parse_message(&raw).map_err(|e| Halt::Err(format!("cannot unpack warp message: {e}")))?;
    let payload = parse_payload(&msg.unsigned.payload);
    Ok(Bytes::from(if block_hash {
        match payload {
            Ok(Payload::Hash(h)) => pack_block_hash_output(Some((msg.unsigned.source_chain_id, h))),
            Ok(_) => return Err(Halt::Err("cannot unpack block hash payload: wrong payload type".into())),
            Err(e) => return Err(Halt::Err(format!("cannot unpack block hash payload: {e}"))),
        }
    } else {
        match payload {
            Ok(Payload::AddressedCall { source_address, payload }) => {
                // common.BytesToAddress: the low 20 bytes of a longer slice, left-padded otherwise.
                let addr = if source_address.len() >= 20 {
                    Address::from_slice(&source_address[source_address.len() - 20..])
                } else {
                    Address::left_padding_from(&source_address)
                };
                pack_message_output(Some((msg.unsigned.source_chain_id, addr, &payload)))
            }
            Ok(_) => return Err(Halt::Err("cannot unpack addressed payload: wrong payload type".into())),
            Err(e) => return Err(Halt::Err(format!("cannot unpack addressed payload: {e}"))),
        }
    }))
}

/// parse_block_results of a possibly absent byte string (no results = empty).
pub fn parse_block_results_opt(b: &[u8]) -> Result<BlockResults, String> {
    if b.is_empty() {
        return Ok(BlockResults::default());
    }
    parse_block_results(b)
}

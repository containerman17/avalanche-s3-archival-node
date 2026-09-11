//! Block execution for mainnet C: rs/exec's revm executor (the handler rules
//! coreth and subnet-evm share: zero refunds, the whole fee to the coinbase,
//! warp, P256VERIFY) under a coreth `Config`, plus what coreth adds on top
//! (`CORETH_RULES.md`): the deprecated native-asset precompiles, the storage
//! key mask (bit 248 cleared), the sticky multicoin flag on account leaves,
//! and the atomic transactions applied after the block's txs.
//!
//! `Db<B>` is the executor's state: a block-scoped overlay of post-values over
//! a `Base` (the hot state, or an RPC at the parent block for the oracle).
//! After the block the overlay IS the diff.

use crate::hot::{addr_hash, slot_hash, Account, Diff, HashCache, HotState};
use alloy_primitives::{Address, Bytes, B256, U256};
use anyhow::{anyhow, bail, Context, Result};
use exec::config::{Config, FeeConfig, PrecompileConfig};
use exec::exec::{Executor, StateDb, Trace};
use exec::config::WARP;
use revm::state::{AccountInfo, Bytecode, EvmState};
use revm::{Database, DatabaseCommit};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::sync::Arc;

pub const CHAIN_ID: u64 = 43114;
/// extstate normalizeStateKey: key[0] &^= 0x01.
pub fn mask(slot: U256) -> U256 {
    let bit: U256 = U256::from(1u8) << 248;
    slot & !bit
}
const X2C: u64 = 1_000_000_000;

pub trait Base: Send + Sync {
    /// The account and its multicoin flag.
    fn account(&self, a: Address) -> Option<(AccountInfo, bool)>;
    /// `slot` is already masked.
    fn storage(&self, a: Address, slot: U256) -> U256;
    fn code(&self, h: B256) -> Option<Bytecode>;
    fn block_hash(&self, n: u64) -> B256;
}

/// The hot state as a base, plus the BLOCKHASH ring the applier maintains.
pub struct HotBase {
    pub hot: Arc<HotState>,
    pub cache: Arc<HashCache>,
    pub hashes: std::sync::Mutex<HashMap<u64, B256>>,
}

impl Base for HotBase {
    fn account(&self, a: Address) -> Option<(AccountInfo, bool)> {
        let acc = self.hot.account_raw(&self.cache.addr(&a))?;
        Some((AccountInfo { balance: acc.balance, nonce: acc.nonce, code_hash: acc.code_hash, code: None, ..Default::default() }, acc.multicoin))
    }
    fn storage(&self, a: Address, slot: U256) -> U256 {
        self.hot.storage_raw(&self.cache.addr(&a), &self.cache.slot(&slot))
    }
    fn code(&self, h: B256) -> Option<Bytecode> {
        self.hot.code(&h).map(|c| Bytecode::new_raw(Bytes::copy_from_slice(&c)))
    }
    fn block_hash(&self, n: u64) -> B256 {
        self.hashes.lock().unwrap().get(&n).copied().unwrap_or_else(|| panic!("BLOCKHASH {n}: not in the ring"))
    }
}

/// The hot state at one generation, for `simulate`: a read that finds the
/// generation gone marks `stale` and answers a default; the caller drops the
/// result.
pub struct GenBase {
    pub hot: Arc<HotState>,
    pub cache: Arc<HashCache>,
    pub g: crate::Generation,
    pub stale: std::sync::atomic::AtomicBool,
}

impl Base for GenBase {
    fn account(&self, a: Address) -> Option<(AccountInfo, bool)> {
        match self.hot.account(self.g, &self.cache.addr(&a)) {
            Ok(v) => v.map(|acc| (AccountInfo { balance: acc.balance, nonce: acc.nonce, code_hash: acc.code_hash, code: None, ..Default::default() }, acc.multicoin)),
            Err(_) => {
                self.stale.store(true, std::sync::atomic::Ordering::Release);
                None
            }
        }
    }
    fn storage(&self, a: Address, slot: U256) -> U256 {
        self.hot.storage(self.g, &self.cache.addr(&a), &self.cache.slot(&slot)).unwrap_or_else(|_| {
            self.stale.store(true, std::sync::atomic::Ordering::Release);
            U256::ZERO
        })
    }
    fn code(&self, h: B256) -> Option<Bytecode> {
        self.hot.code(&h).map(|c| Bytecode::new_raw(Bytes::copy_from_slice(&c)))
    }
    fn block_hash(&self, _n: u64) -> B256 {
        // ponytail: BLOCKHASH in a simulation answers zero; wire the ring through when a strategy needs it.
        B256::ZERO
    }
}

pub struct Db<B> {
    pub base: B,
    /// Post-values this block; None = deleted.
    acct: HashMap<Address, Option<(AccountInfo, bool)>>,
    /// Accounts whose storage was cleared this block (delete or create), in order of first wipe.
    wiped: HashSet<Address>,
    slots: HashMap<(Address, U256), U256>,
    code: HashMap<B256, Bytecode>,
}

impl<B: Base> Db<B> {
    pub fn new(base: B) -> Self {
        Db { base, acct: HashMap::new(), wiped: HashSet::new(), slots: HashMap::new(), code: HashMap::new() }
    }

    fn load(&self, a: Address) -> Option<(AccountInfo, bool)> {
        match self.acct.get(&a) {
            Some(v) => v.clone(),
            None => self.base.account(a),
        }
    }

    fn delete(&mut self, a: Address) {
        self.acct.insert(a, None);
        self.wiped.insert(a);
        self.slots.retain(|(x, _), _| *x != a);
    }

    /// Atomic import: `AddBalance(addr, amount * 1e9)`.
    pub fn add_balance(&mut self, a: Address, navax: u64) {
        let (mut info, multi) = self.load(a).unwrap_or_default();
        info.balance += U256::from(navax) * U256::from(X2C);
        self.acct.insert(a, Some((info, multi)));
    }
    /// Atomic export: `SubBalance`; `bump_nonce` once per distinct address after.
    pub fn sub_balance(&mut self, a: Address, navax: u64) -> Result<()> {
        let (mut info, multi) = self.load(a).unwrap_or_default();
        let wei = U256::from(navax) * U256::from(X2C);
        if info.balance < wei {
            bail!("export from {a}: balance {} below {wei}", info.balance);
        }
        info.balance -= wei;
        self.acct.insert(a, Some((info, multi)));
        Ok(())
    }
    pub fn bump_nonce(&mut self, a: Address) {
        let (mut info, multi) = self.load(a).unwrap_or_default();
        info.nonce += 1;
        self.acct.insert(a, Some((info, multi)));
    }

    /// The block's diff in the hot state's shape; the overlay is cleared.
    pub fn take_diff(&mut self) -> Diff {
        let mut d = Diff::default();
        for (a, v) in self.acct.drain() {
            let h = addr_hash(&a);
            if self.wiped.contains(&a) {
                d.accounts.push((h, None));
            }
            if let Some((info, multi)) = v {
                d.accounts.push((h, Some(Account { nonce: info.nonce, balance: info.balance, code_hash: info.code_hash, multicoin: multi })));
            }
        }
        for (a, c) in self.code.drain() {
            d.code.push((a, Arc::from(c.original_byte_slice())));
        }
        for ((a, k), v) in self.slots.drain() {
            d.storage.push((addr_hash(&a), slot_hash(&k), v));
        }
        self.wiped.clear();
        d
    }

    /// The overlay as (address, slot) pairs, for the oracle's comparison.
    pub fn touched(&self) -> (Vec<(Address, Option<AccountInfo>)>, Vec<(Address, U256, U256)>) {
        let a = self.acct.iter().map(|(a, v)| (*a, v.as_ref().map(|(i, _)| i.clone()))).collect();
        let s = self.slots.iter().map(|((a, k), v)| (*a, *k, *v)).collect();
        (a, s)
    }
}

impl<B: Base> Database for Db<B> {
    type Error = Infallible;
    fn basic(&mut self, a: Address) -> Result<Option<AccountInfo>, Infallible> {
        Ok(self.load(a).map(|(mut i, _)| {
            if i.code.is_none() && !i.is_empty_code_hash() {
                i.code = self.code.get(&i.code_hash).cloned().or_else(|| self.base.code(i.code_hash));
            }
            i
        }))
    }
    fn code_by_hash(&mut self, h: B256) -> Result<Bytecode, Infallible> {
        Ok(self.code.get(&h).cloned().or_else(|| self.base.code(h)).unwrap_or_default())
    }
    fn storage(&mut self, a: Address, slot: U256) -> Result<U256, Infallible> {
        let k = mask(slot);
        if let Some(v) = self.slots.get(&(a, k)) {
            return Ok(*v);
        }
        if self.wiped.contains(&a) {
            return Ok(U256::ZERO);
        }
        Ok(self.base.storage(a, k))
    }
    fn block_hash(&mut self, n: u64) -> Result<B256, Infallible> {
        Ok(self.base.block_hash(n))
    }
}

impl<B: Base> DatabaseCommit for Db<B> {
    /// state_rows' classification: a touched account is deleted when
    /// self-destructed or EIP-158 empty (unless multicoin), else its info and
    /// changed slots are the post-values; a created account starts with
    /// empty storage.
    fn commit(&mut self, state: EvmState) {
        for (a, acc) in state {
            if !acc.is_touched() {
                continue;
            }
            let multi = self.load(a).map_or(false, |(_, m)| m);
            if acc.is_selfdestructed() || (acc.is_empty() && !multi) {
                self.delete(a);
                continue;
            }
            if acc.is_created() {
                self.wiped.insert(a);
                self.slots.retain(|(x, _), _| *x != a);
            }
            let mut info = acc.info;
            if let Some(c) = info.code.take() {
                if !c.is_empty() {
                    self.code.entry(info.code_hash).or_insert(c);
                }
            }
            for (k, slot) in acc.storage {
                if slot.is_changed() {
                    self.slots.insert((a, mask(k)), slot.present_value);
                }
            }
            self.acct.insert(a, Some((info, multi)));
        }
    }
}

impl<B: Base> StateDb for Db<B> {
    fn set_block_hash(&mut self, _number: u64, _hash: B256) {}
    fn forget(&mut self, addr: Address) {
        self.delete(addr);
    }
}

/// coreth mainnet: params/config_extra.go SetEthUpgrades and the warp
/// precompile at Durango (vm.go parseGenesis). No allow lists, no fee
/// manager, no native minter, no reward manager.
pub fn coreth_config() -> Config {
    let blockchain_id: B256 = {
        let raw = bs58::decode("2q9e4r6Mu3U68nU1fYjgbR6JvwrRx36CohpAX5UQxse55x1Q5").into_vec().expect("chain id");
        B256::from_slice(&raw[..32])
    };
    Config {
        chain_id: CHAIN_ID,
        fee_config: FeeConfig {
            gas_limit: U256::from(15_000_000u64),
            target_block_rate: 2,
            min_base_fee: U256::from(25_000_000_000u64),
            target_gas: U256::from(15_000_000u64),
            base_fee_change_denominator: U256::from(36u64),
            min_block_gas_cost: U256::ZERO,
            max_block_gas_cost: U256::from(1_000_000u64),
            block_gas_cost_step: U256::from(200_000u64),
        },
        allow_fee_recipients: false,
        subnet_evm: 0,
        durango: Some(1709740800),
        etna: Some(1734368400),
        fortuna: Some(1744124400),
        granite: Some(1763568000),
        genesis_precompiles: vec![PrecompileConfig {
            key: "warpConfig",
            address: WARP,
            timestamp: 1709740800,
            disable: false,
            admins: vec![],
            enabled: vec![],
            managers: vec![],
            initial_fee_config: None,
            initial_mint: vec![],
            initial_reward: None,
            quorum_numerator: 0,
            require_primary_network_signers: true,
        }],
        precompile_upgrades: vec![],
        state_upgrades: vec![],
        alloc: Default::default(),
        genesis_timestamp: 0,
        network_id: 1,
        blockchain_id,
        subnet_id: B256::ZERO,
        extra_prefix: 24,
    }
}

pub fn hex_u64(v: &Value) -> Result<u64> {
    let s = v.as_str().ok_or_else(|| anyhow!("expected a hex string, got {v}"))?;
    Ok(u64::from_str_radix(s.trim_start_matches("0x"), 16)?)
}
fn hex_u128(v: &Value) -> Result<u128> {
    let s = v.as_str().ok_or_else(|| anyhow!("expected a hex string, got {v}"))?;
    Ok(u128::from_str_radix(s.trim_start_matches("0x"), 16)?)
}
fn parse<T: std::str::FromStr>(v: &Value, what: &str) -> Result<T>
where
    T::Err: std::fmt::Display,
{
    let s = v.as_str().ok_or_else(|| anyhow!("{what}: expected a string, got {v}"))?;
    s.parse::<T>().map_err(|e| anyhow!("{what}: {e}"))
}
fn opt_u64(v: &Value) -> Result<Option<u64>> {
    if v.is_null() {
        Ok(None)
    } else {
        Ok(Some(hex_u64(v)?))
    }
}

/// The `eth_getBlockByHash(hash, true)` result as rs/exec's block (senders
/// from the `from` field, so no signature recovery).
pub fn block_from_json(b: &Value) -> Result<block::Block> {
    let header = block::eth::Header {
        parent_hash: parse(&b["parentHash"], "parentHash")?,
        uncle_hash: parse(&b["sha3Uncles"], "sha3Uncles")?,
        coinbase: parse(&b["miner"], "miner")?,
        root: parse(&b["stateRoot"], "stateRoot")?,
        tx_hash: parse(&b["transactionsRoot"], "transactionsRoot")?,
        receipt_hash: parse(&b["receiptsRoot"], "receiptsRoot")?,
        bloom: parse(&b["logsBloom"], "logsBloom")?,
        difficulty: parse(&b["difficulty"], "difficulty")?,
        number: hex_u64(&b["number"])?,
        gas_limit: hex_u64(&b["gasLimit"])?,
        gas_used: hex_u64(&b["gasUsed"])?,
        time: hex_u64(&b["timestamp"])?,
        extra: parse(&b["extraData"], "extraData")?,
        mix_digest: parse(&b["mixHash"], "mixHash")?,
        nonce: parse(&b["nonce"], "nonce")?,
        base_fee: Some(parse(&b["baseFeePerGas"], "baseFeePerGas")?),
        block_gas_cost: if b["blockGasCost"].is_null() { None } else { Some(parse(&b["blockGasCost"], "blockGasCost")?) },
        blob_gas_used: opt_u64(&b["blobGasUsed"])?,
        excess_blob_gas: opt_u64(&b["excessBlobGas"])?,
        parent_beacon_root: if b["parentBeaconBlockRoot"].is_null() { None } else { Some(parse(&b["parentBeaconBlockRoot"], "parentBeaconBlockRoot")?) },
        time_milliseconds: opt_u64(&b["timestampMilliseconds"])?,
        min_delay_excess: opt_u64(&b["minDelayExcess"])?,
    };
    let mut txs = Vec::new();
    for (i, t) in b["transactions"].as_array().ok_or_else(|| anyhow!("transactions"))?.iter().enumerate() {
        let tx_type = if t["type"].is_null() { 0 } else { hex_u64(&t["type"])? as u8 };
        if tx_type > 2 {
            bail!("tx {i}: type {tx_type} is not a mainnet C tx type");
        }
        let gas_price = if tx_type == 2 { hex_u128(&t["maxFeePerGas"])? } else { hex_u128(&t["gasPrice"])? };
        let gas_tip = if tx_type == 2 { hex_u128(&t["maxPriorityFeePerGas"])? } else { gas_price };
        let mut access_list = Vec::new();
        if let Some(al) = t["accessList"].as_array() {
            for e in al {
                let mut storage_keys = Vec::new();
                for k in e["storageKeys"].as_array().unwrap_or(&vec![]) {
                    storage_keys.push(parse(k, "storageKey")?);
                }
                access_list.push(block::eth::AccessItem { address: parse(&e["address"], "accessList.address")?, storage_keys });
            }
        }
        txs.push(block::eth::Tx {
            raw: alloy_rlp::Bytes::new(),
            hash: parse(&t["hash"], "tx hash")?,
            sender: Some(parse(&t["from"], "from")?),
            tx_type,
            chain_id: if t["chainId"].is_null() { None } else { Some(hex_u64(&t["chainId"])?) },
            nonce: hex_u64(&t["nonce"])?,
            gas_price,
            gas_tip,
            gas_limit: hex_u64(&t["gas"])?,
            to: if t["to"].is_null() { None } else { Some(parse(&t["to"], "to")?) },
            value: parse(&t["value"], "value")?,
            input: parse::<Bytes>(&t["input"], "input")?.0,
            access_list,
            v: 0,
            r: U256::ZERO,
            s: U256::ZERO,
            recid: 0,
            body_off: 0,
            sig_off: 0,
        });
    }
    Ok(block::Block {
        height: header.number,
        hash: parse(&b["hash"], "hash")?,
        container_id: B256::ZERO,
        header,
        header_rlp: alloy_rlp::Bytes::new(),
        txs,
        container: alloy_rlp::Bytes::new(),
        pvm: None,
    })
}

// ---------------------------------------------------------------------------
// Atomic transactions: avalanchego linearcodec (plugin/evm/atomic/codec.go),
// only the fields that move EVM state are kept, the rest is skipped over.

pub enum AtomicTx {
    /// (address, amount in nAVAX, assetID)
    Import(Vec<(Address, u64, B256)>),
    /// (address, amount in nAVAX, assetID, nonce)
    Export(Vec<(Address, u64, B256, u64)>),
}

struct Cur<'a>(&'a [u8]);
impl<'a> Cur<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        if self.0.len() < n {
            bail!("atomic tx: truncated ({} bytes left, {n} wanted)", self.0.len());
        }
        let (a, b) = self.0.split_at(n);
        self.0 = b;
        Ok(a)
    }
    fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_be_bytes(self.take(2)?.try_into().unwrap()))
    }
    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_be_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn id(&mut self) -> Result<B256> {
        Ok(B256::from_slice(self.take(32)?))
    }
    fn addr(&mut self) -> Result<Address> {
        Ok(Address::from_slice(self.take(20)?))
    }
}

/// ExtractAtomicTxs with batch = true (post-AP5): `[]*Tx`.
pub fn decode_atomic(ext: &[u8]) -> Result<Vec<AtomicTx>> {
    if ext.is_empty() {
        return Ok(vec![]);
    }
    let mut c = Cur(ext);
    let ver = c.u16()?;
    if ver != 0 {
        bail!("atomic txs: codec version {ver}");
    }
    let n = c.u32()?;
    let mut out = Vec::with_capacity(n as usize);
    for _ in 0..n {
        let kind = c.u32()?;
        let _network = c.u32()?;
        let _blockchain = c.id()?;
        let _chain = c.id()?;
        match kind {
            0 => {
                // ImportedInputs []*TransferableInput
                let ni = c.u32()?;
                for _ in 0..ni {
                    let _txid = c.id()?;
                    let _idx = c.u32()?;
                    let _asset = c.id()?;
                    let t = c.u32()?;
                    if t != 5 {
                        bail!("atomic import: input type {t}");
                    }
                    let _amt = c.u64()?;
                    let nsig = c.u32()?;
                    c.take(4 * nsig as usize)?;
                }
                let no = c.u32()?;
                let mut outs = Vec::with_capacity(no as usize);
                for _ in 0..no {
                    outs.push((c.addr()?, c.u64()?, c.id()?));
                }
                out.push(AtomicTx::Import(outs));
            }
            1 => {
                let ni = c.u32()?;
                let mut ins = Vec::with_capacity(ni as usize);
                for _ in 0..ni {
                    ins.push((c.addr()?, c.u64()?, c.id()?, c.u64()?));
                }
                // ExportedOutputs []*TransferableOutput
                let no = c.u32()?;
                for _ in 0..no {
                    let _asset = c.id()?;
                    let t = c.u32()?;
                    if t != 7 {
                        bail!("atomic export: output type {t}");
                    }
                    let _amt = c.u64()?;
                    let _locktime = c.u64()?;
                    let _threshold = c.u32()?;
                    let na = c.u32()?;
                    c.take(20 * na as usize)?;
                }
                out.push(AtomicTx::Export(ins));
            }
            k => bail!("atomic tx type {k}"),
        }
        // Creds []verify.Verifiable: secp256k1fx.Credential = [][65]byte
        let nc = c.u32()?;
        for _ in 0..nc {
            let t = c.u32()?;
            if t != 9 {
                bail!("atomic cred type {t}");
            }
            let ns = c.u32()?;
            c.take(65 * ns as usize)?;
        }
    }
    if !c.0.is_empty() {
        bail!("atomic txs: {} trailing bytes", c.0.len());
    }
    Ok(out)
}

pub const AVAX_ASSET: &str = "FvwEAhmxKfeiG8SnEvq42hc6whRyY3EFYAvebMqDNDGCgxN5Z";

/// onExtraStateChange: EVMStateTransfer of each atomic tx in order.
pub fn apply_atomic<B: Base>(db: &mut Db<B>, txs: &[AtomicTx], avax: B256) -> Result<()> {
    for t in txs {
        match t {
            AtomicTx::Import(outs) => {
                for (a, amt, asset) in outs {
                    if *asset != avax {
                        bail!("import of a non-AVAX asset {asset} to {a}: multicoin is not implemented (Banff forbids it)");
                    }
                    db.add_balance(*a, *amt);
                }
            }
            AtomicTx::Export(ins) => {
                let mut seen = HashSet::new();
                for (a, amt, asset, _nonce) in ins {
                    if *asset != avax {
                        bail!("export of a non-AVAX asset {asset} from {a}");
                    }
                    db.sub_balance(*a, *amt)?;
                    seen.insert(*a);
                }
                let mut addrs: Vec<Address> = seen.into_iter().collect();
                addrs.sort();
                for a in addrs {
                    db.bump_nonce(a);
                }
            }
        }
    }
    Ok(())
}

/// One block on a base: the txs through revm, then the atomic txs. Returns
/// the executor's block result (gas used, receipts root) with the diff left
/// in the executor's Db.
pub struct Applier<B: Base> {
    pub ex: Executor<Db<B>>,
    avax: B256,
}

impl<B: Base> Applier<B> {
    pub fn new(base: B) -> Applier<B> {
        let mut ex = Executor::open(coreth_config(), Db::new(base));
        ex.set_coreth();
        ex.trace = Trace::Noop;
        let raw = bs58::decode(AVAX_ASSET).into_vec().expect("asset id");
        Applier { ex, avax: B256::from_slice(&raw[..32]) }
    }

    pub fn execute(&mut self, json: &Value) -> Result<(block::Block, exec::BlockResult)> {
        let b = block_from_json(json).context("block json")?;
        let r = self.ex.execute_block(&b, b.header.time).with_context(|| format!("execute block {}", b.height))?;
        if r.gas_used != b.header.gas_used {
            bail!("block {}: gas used {} but the header says {}", b.height, r.gas_used, b.header.gas_used);
        }
        if r.receipts_root != b.header.receipt_hash {
            bail!("block {}: receipts root {} but the header says {}", b.height, r.receipts_root, b.header.receipt_hash);
        }
        let ext: Bytes = match json["blockExtraData"].as_str() {
            Some(s) => s.parse().map_err(|e| anyhow!("blockExtraData: {e}"))?,
            None => Bytes::new(),
        };
        let atomic = decode_atomic(&ext).with_context(|| format!("block {} atomic txs", b.height))?;
        apply_atomic(self.ex.db_mut(), &atomic, self.avax)?;
        Ok((b, r))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A real block (95033472, six txs and one atomic tx of 311 bytes) parses
    /// into the executor's block and its atomic tx decodes.
    #[test]
    fn real_block_parses() {
        let v: Value = serde_json::from_str(include_str!("block_95033472.json")).unwrap();
        let b = block_from_json(&v).unwrap();
        assert_eq!((b.height, b.txs.len(), b.header.gas_used), (95033472, 6, 0x69364));
        assert!(b.txs.iter().all(|t| t.sender.is_some()));
        assert_eq!(b.header.extra.len(), 30);
        let ext: Bytes = v["blockExtraData"].as_str().unwrap().parse().unwrap();
        let txs = decode_atomic(&ext).unwrap();
        assert_eq!(txs.len(), 1);
        let avax: B256 = B256::from_slice(&bs58::decode(AVAX_ASSET).into_vec().unwrap()[..32]);
        match &txs[0] {
            AtomicTx::Import(outs) => {
                assert!(!outs.is_empty());
                assert!(outs.iter().all(|(_, amt, asset)| *asset == avax && *amt > 0));
            }
            AtomicTx::Export(ins) => {
                assert!(!ins.is_empty());
                assert!(ins.iter().all(|(_, amt, asset, _)| *asset == avax && *amt > 0));
            }
        }
        assert!(decode_atomic(&ext[..ext.len() - 1]).is_err());
    }

    #[test]
    fn mask_clears_bit_248() {
        let bit: U256 = U256::from(1u8) << 248;
        assert_eq!(mask(bit | U256::from(5u8)), U256::from(5u8));
        assert_eq!(mask(U256::MAX), U256::MAX & !bit);
        assert_eq!(mask(U256::from(5u8)), U256::from(5u8));
    }
}

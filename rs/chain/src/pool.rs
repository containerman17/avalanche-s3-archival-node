//! The transaction pool, in the engine (libevm's legacypool rules, without
//! its per-head O(pending) reset and O(n log n) re-heap).
//!
//! Per sender a `BTreeMap<nonce, tx>` split into executable (contiguous from
//! the sender's state nonce at the accepted head) and queued. Two ordered
//! indexes, maintained per insert / remove and never rebuilt: `heads`, one
//! key per sender with an executable head (tip cap desc, arrival asc: the
//! build's order), and `priced`, one key per remote tx (tip cap asc: what a
//! full pool evicts first). A head change touches only the senders the block
//! touched (its senders and, when they hold txs here, its recipients): their
//! nonce and balance are re-read, mined and unpayable txs dropped, the
//! executable prefix recomputed. Everything else stays where it is.
//!
//! Admission = libevm's ValidateTransaction + ValidateTransactionWithState
//! (see `validate` and `Inner::insert`); the deviations are listed in
//! cmd/epochdb-validator/E2E.md ("Mempool in the engine").
//!
//! Locking: `Inner` behind one mutex, held for microseconds per tx; sender
//! recovery and the stateless checks run before it, in parallel on rayon;
//! state reads run before it too (the engine's execution mutex, one batch
//! per call) and are validated against `gen`, which every head change bumps.
use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap, HashMap, HashSet, VecDeque};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use alloy_primitives::{Address, B256, U256};
use block::Tx;
use bytes::Bytes;
use rayon::prelude::*;

/// legacypool.txMaxSize.
pub const TX_MAX_SIZE: usize = 128 * 1024;
/// params.MaxInitCodeSize (Shanghai = Durango).
pub const MAX_INIT_CODE: usize = 49152;
/// The gossip backlog kept when nobody drains it (before NormalOp).
const GOSSIP_MAX: usize = 100_000;
/// How often the lifetime sweep runs (on a head change).
const SWEEP_EVERY: Duration = Duration::from_secs(30);

/// The per-tx admission result (the ABI's codes).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum Code {
    Ok = 0,
    Known = 1,
    Replaced = 2,
    Underpriced = 3,
    NonceLow = 4,
    Funds = 5,
    GasLimit = 6,
    Intrinsic = 7,
    Invalid = 8,
    Full = 9,
    Other = 10,
}

/// One admission answer: the code, libevm's message for it, the tx hash
/// (keccak of the bytes handed in, even when they did not decode).
#[derive(Clone, Copy, Debug)]
pub struct Added {
    pub code: Code,
    pub message: &'static str,
    pub hash: B256,
}

impl Added {
    pub fn ok(&self) -> bool {
        matches!(self.code, Code::Ok | Code::Replaced)
    }
}

/// subnet-evm's pool config keys (plugin/evm/config), with its defaults.
#[derive(Clone, Debug)]
pub struct Config {
    pub price_limit: u128,
    pub price_bump: u64,
    pub account_slots: usize,
    pub global_slots: usize,
    pub account_queue: usize,
    pub global_queue: usize,
    pub lifetime: Duration,
    /// `local-txs-enabled`: false makes every tx remote (legacypool NoLocals).
    pub locals: bool,
    pub allow_unprotected: bool,
}

impl Default for Config {
    fn default() -> Config {
        Config { price_limit: 1, price_bump: 10, account_slots: 16, global_slots: 4096 + 1024, account_queue: 64, global_queue: 1024, lifetime: Duration::from_secs(600), locals: false, allow_unprotected: false }
    }
}

impl Config {
    /// The chain config JSON (the plugin's Initialize config bytes).
    pub fn from_json(conf: &serde_json::Value) -> Config {
        let mut c = Config::default();
        let num = |k: &str| match conf.get(k) {
            Some(serde_json::Value::Number(n)) => n.as_u64(),
            Some(serde_json::Value::String(s)) => s.trim().parse().ok(),
            _ => None,
        };
        if let Some(v) = num("tx-pool-price-limit") {
            c.price_limit = v as u128;
        }
        if let Some(v) = num("tx-pool-price-bump") {
            c.price_bump = v;
        }
        if let Some(v) = num("tx-pool-account-slots") {
            c.account_slots = v as usize;
        }
        if let Some(v) = num("tx-pool-global-slots") {
            c.global_slots = v as usize;
        }
        if let Some(v) = num("tx-pool-account-queue") {
            c.account_queue = v as usize;
        }
        if let Some(v) = num("tx-pool-global-queue") {
            c.global_queue = v as usize;
        }
        // Duration: a JSON number is nanoseconds (subnet-evm's Duration), a string is Go's form ("10m").
        match conf.get("tx-pool-lifetime") {
            Some(serde_json::Value::Number(n)) => c.lifetime = Duration::from_nanos(n.as_u64().unwrap_or(600_000_000_000)),
            Some(serde_json::Value::String(s)) => {
                if let Some(d) = go_duration(s) {
                    c.lifetime = d;
                }
            }
            _ => {}
        }
        c.locals = conf.get("local-txs-enabled").and_then(|v| v.as_bool()).unwrap_or(false);
        c.allow_unprotected = conf.get("allow-unprotected-txs").and_then(|v| v.as_bool()).unwrap_or(false);
        c
    }
}

/// Go's time.ParseDuration for the forms a config carries ("10m", "1h30m", "90s").
fn go_duration(s: &str) -> Option<Duration> {
    let mut total = 0f64;
    let mut num = String::new();
    let mut i = 0;
    let b = s.as_bytes();
    while i < b.len() {
        let c = b[i] as char;
        if c.is_ascii_digit() || c == '.' {
            num.push(c);
            i += 1;
            continue;
        }
        let mut unit = String::new();
        while i < b.len() && !(b[i] as char).is_ascii_digit() {
            unit.push(b[i] as char);
            i += 1;
        }
        let n: f64 = num.parse().ok()?;
        num.clear();
        total += n * match unit.as_str() {
            "ns" => 1e-9,
            "us" | "µs" => 1e-6,
            "ms" => 1e-3,
            "s" => 1.0,
            "m" => 60.0,
            "h" => 3600.0,
            _ => return None,
        };
    }
    if !num.is_empty() {
        return None;
    }
    Some(Duration::from_secs_f64(total))
}

/// The head-dependent rules: the block gas limit and the fee config's
/// minimum base fee (both from `GetFeeConfigAt(head)`), the head's time
/// (fork rules).
#[derive(Clone, Copy, Debug, Default)]
pub struct Head {
    pub gas_limit: u64,
    pub min_base_fee: u128,
    pub time: u64,
}

struct Entry {
    tx: Arc<Tx>,
    /// Arrival order.
    seq: u64,
    /// tx.Cost(): gas limit x fee cap + value.
    cost: U256,
}

struct Account {
    txs: BTreeMap<u64, Entry>,
    /// The sender's nonce and balance at the accepted head.
    nonce: u64,
    balance: U256,
    /// The executable prefix: `exec` txs, nonces nonce..nonce+exec.
    exec: usize,
    exec_cost: U256,
    /// This sender's key in `heads` (Some when exec > 0).
    head_key: Option<HeadKey>,
    /// Last activity (the lifetime rule).
    beat: Instant,
    local: bool,
}

impl Account {
    fn queued(&self) -> usize {
        self.txs.len() - self.exec
    }
}

/// `heads`: highest tip cap first, oldest first among equals.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct HeadKey {
    tip: Reverse<u128>,
    seq: u64,
    sender: Address,
}

/// `priced`: the cheapest remote tx first, the newest among equals.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct PricedKey {
    tip: u128,
    seq: Reverse<u64>,
    hash: B256,
}

struct Inner {
    accounts: HashMap<Address, Account>,
    by_hash: HashMap<B256, (Address, u64)>,
    heads: BTreeSet<HeadKey>,
    priced: BTreeSet<PricedKey>,
    pending: usize,
    total: usize,
    seq: u64,
    /// Bumped by every head change; admission re-reads a sender's state when
    /// the head moved between its read and its insert.
    gen: u64,
    head: Head,
    gossip: VecDeque<Arc<Tx>>,
    last_sweep: Instant,
}

pub struct Pool {
    pub cfg: Config,
    chain: Arc<exec::Config>,
    inner: Mutex<Inner>,
    cv: Condvar,
}

/// tx.Cost(): None on overflow (refused as unpayable).
fn cost_of(t: &Tx) -> Option<U256> {
    U256::from(t.gas_limit).checked_mul(U256::from(t.gas_price))?.checked_add(t.value)
}

/// core.IntrinsicGas: 21,000 (53,000 for a create), 16 / 4 per calldata
/// byte, 2,400 per access-list address and 1,900 per key, 2 per initcode
/// word from Durango (Shanghai). A warp access-list entry costs its
/// predicate gas in libevm's hook; here the plain access-list cost, the
/// build applies the exact rule.
pub fn intrinsic_gas(t: &Tx, durango: bool) -> Option<u64> {
    let mut gas: u64 = if t.to.is_none() { 53_000 } else { 21_000 };
    let nz = t.input.iter().filter(|b| **b != 0).count() as u64;
    let z = t.input.len() as u64 - nz;
    gas = gas.checked_add(nz.checked_mul(16)?)?.checked_add(z.checked_mul(4)?)?;
    if t.to.is_none() && durango {
        gas = gas.checked_add(((t.input.len() as u64).div_ceil(32)).checked_mul(2)?)?;
    }
    let keys: u64 = t.access_list.iter().map(|a| a.storage_keys.len() as u64).sum();
    gas = gas.checked_add((t.access_list.len() as u64).checked_mul(2400)?)?.checked_add(keys.checked_mul(1900)?)?;
    Some(gas)
}

/// libevm's stateless checks (ValidateTransaction), and the pool's price
/// floor. The sender is not recovered here.
fn validate(t: &Tx, head: &Head, cfg: &Config, chain: &exec::Config, local: bool) -> Result<(), (Code, &'static str)> {
    if t.tx_type > 2 {
        return Err((Code::Other, "transaction type not supported"));
    }
    if t.raw.len() > TX_MAX_SIZE {
        return Err((Code::Other, "oversized data"));
    }
    match t.chain_id {
        Some(c) if c != chain.chain_id => return Err((Code::Invalid, "invalid sender")),
        None if !cfg.allow_unprotected => return Err((Code::Invalid, "only replay-protected (EIP-155) transactions allowed over RPC")),
        _ => {}
    }
    if t.to.is_none() && chain.is_durango(head.time) && t.input.len() > MAX_INIT_CODE {
        return Err((Code::Other, "max initcode size exceeded"));
    }
    if t.gas_limit > head.gas_limit {
        return Err((Code::GasLimit, "exceeds block gas limit"));
    }
    if t.gas_price < t.gas_tip {
        return Err((Code::Other, "max priority fee per gas higher than max fee per gas"));
    }
    match intrinsic_gas(t, chain.is_durango(head.time)) {
        Some(g) if g <= t.gas_limit => {}
        _ => return Err((Code::Intrinsic, "intrinsic gas too low")),
    }
    if !local && t.gas_tip < cfg.price_limit {
        return Err((Code::Underpriced, "transaction underpriced"));
    }
    if t.gas_price < head.min_base_fee {
        return Err((Code::Underpriced, "transaction underpriced"));
    }
    Ok(())
}

impl Pool {
    pub fn new(cfg: Config, chain: Arc<exec::Config>, head: Head) -> Pool {
        Pool {
            cfg,
            chain,
            inner: Mutex::new(Inner {
                accounts: HashMap::new(),
                by_hash: HashMap::new(),
                heads: BTreeSet::new(),
                priced: BTreeSet::new(),
                pending: 0,
                total: 0,
                seq: 0,
                gen: 0,
                head,
                gossip: VecDeque::new(),
                last_sweep: Instant::now(),
            }),
            cv: Condvar::new(),
        }
    }

    /// Admits `raws` (tx envelopes, MarshalBinary form). `read` answers
    /// (nonce, balance) at the accepted head for senders the pool does not
    /// hold yet; it runs outside the pool lock and is retried when a head
    /// change lands in between.
    pub fn add(&self, raws: Vec<Bytes>, local: bool, read: &dyn Fn(&[Address]) -> Vec<(u64, U256)>) -> Vec<Added> {
        let local = local && self.cfg.locals;
        let head = self.inner.lock().unwrap().head;
        let prep = |raw: Bytes| -> Result<Tx, Added> {
            let hash = alloy_primitives::keccak256(&raw);
            let mut t = block::eth::decode_tx(raw).map_err(|_| Added { code: Code::Other, message: "invalid transaction: does not decode", hash })?;
            validate(&t, &head, &self.cfg, &self.chain, local).map_err(|(code, message)| Added { code, message, hash })?;
            t.sender = block::recover(&t);
            if t.sender.is_none() {
                return Err(Added { code: Code::Invalid, message: "invalid sender", hash });
            }
            Ok(t)
        };
        let prepared: Vec<Result<Tx, Added>> = if raws.len() < 32 { raws.into_iter().map(prep).collect() } else { raws.into_par_iter().map(prep).collect() };
        let mut out: Vec<Added> = Vec::with_capacity(prepared.len());
        let mut txs: Vec<(usize, Tx)> = Vec::new();
        for (i, r) in prepared.into_iter().enumerate() {
            match r {
                Ok(t) => {
                    out.push(Added { code: Code::Ok, message: "", hash: t.hash });
                    txs.push((i, t));
                }
                Err(a) => out.push(a),
            }
        }
        if txs.is_empty() {
            return out;
        }
        let now = Instant::now();
        loop {
            let (gen, need) = {
                let g = self.inner.lock().unwrap();
                let mut need: Vec<Address> = Vec::new();
                let mut seen = HashSet::new();
                for (_, t) in &txs {
                    let a = t.sender.unwrap();
                    if !g.accounts.contains_key(&a) && seen.insert(a) {
                        need.push(a);
                    }
                }
                (g.gen, need)
            };
            let states = if need.is_empty() { Vec::new() } else { read(&need) };
            let mut g = self.inner.lock().unwrap();
            if g.gen != gen {
                continue; // a head landed in between: the states may be stale
            }
            for (a, (nonce, balance)) in need.iter().zip(states) {
                g.accounts.entry(*a).or_insert_with(|| Account { txs: BTreeMap::new(), nonce, balance, exec: 0, exec_cost: U256::ZERO, head_key: None, beat: now, local });
            }
            let mut promoted = false;
            let senders: Vec<Address> = txs.iter().map(|(_, t)| t.sender.unwrap()).collect();
            for (i, t) in txs.drain(..) {
                let (code, message) = g.insert(t, local, &self.cfg, now, &mut promoted);
                out[i].code = code;
                out[i].message = message;
            }
            // A sender left with no tx (every one refused, or its last one evicted) holds no slot.
            for a in senders {
                g.prune(&a);
            }
            if promoted {
                self.cv.notify_all();
            }
            return out;
        }
    }

    /// A block was accepted: its txs leave, the senders it touched are
    /// re-read (nonce, balance) and re-settled, the head rules move.
    pub fn on_accept(&self, block_txs: &[Tx], head: Head, read: &mut dyn FnMut(&[Address]) -> Vec<(u64, U256)>) {
        let mut g = self.inner.lock().unwrap();
        g.gen += 1;
        g.head = head;
        let mut touched: Vec<Address> = Vec::new();
        let mut seen = HashSet::new();
        for t in block_txs {
            g.remove_hash(&t.hash);
            if let Some(a) = t.sender {
                if g.accounts.contains_key(&a) && seen.insert(a) {
                    touched.push(a);
                }
            }
            if let Some(a) = t.to {
                if g.accounts.contains_key(&a) && seen.insert(a) {
                    touched.push(a);
                }
            }
        }
        if !touched.is_empty() {
            let states = read(&touched);
            for (a, (nonce, balance)) in touched.iter().zip(states) {
                if let Some(acct) = g.accounts.get_mut(a) {
                    acct.nonce = nonce;
                    acct.balance = balance;
                }
                g.settle(a);
                g.prune(a);
            }
        }
        let now = Instant::now();
        if now.duration_since(g.last_sweep) >= SWEEP_EVERY {
            g.last_sweep = now;
            g.expire(now, self.cfg.lifetime);
        }
        drop(g);
        self.cv.notify_all();
    }

    /// The build's candidates: the executable heads by effective tip
    /// (min(tip cap, fee cap - base fee)) desc then arrival, each sender's
    /// txs in nonce order behind its head, until the summed gas limits
    /// reach `gas_budget` or the summed sizes reach `max_bytes`; a sender
    /// whose head cannot pay the base fee is left out. `skip` (sender -> the
    /// nonce after its txs in the pending parent chain) starts a sender past
    /// what the block's unaccepted ancestors already hold. Exact miner
    /// order from the tip-cap index: a head whose effective tip is below its
    /// tip cap waits in a side heap until the index reaches its level.
    pub fn candidates(&self, base_fee: u128, gas_budget: u64, max_bytes: usize, skip: &HashMap<Address, u64>) -> Vec<Tx> {
        #[derive(PartialEq, Eq)]
        struct Cand {
            eff: u128,
            seq: Reverse<u64>,
            sender: Address,
            nonce: u64,
        }
        impl Ord for Cand {
            fn cmp(&self, o: &Self) -> std::cmp::Ordering {
                (self.eff, self.seq).cmp(&(o.eff, o.seq)).then_with(|| self.sender.cmp(&o.sender))
            }
        }
        impl PartialOrd for Cand {
            fn partial_cmp(&self, o: &Self) -> Option<std::cmp::Ordering> {
                Some(self.cmp(o))
            }
        }
        let g = self.inner.lock().unwrap();
        let eff = |e: &Entry| -> Option<u128> {
            if e.tx.gas_price < base_fee {
                return None;
            }
            Some(e.tx.gas_tip.min(e.tx.gas_price - base_fee))
        };
        let mut out = Vec::new();
        let (mut gas, mut size) = (0u64, 0usize);
        let mut heads = g.heads.iter().peekable();
        let mut heap: BinaryHeap<Cand> = BinaryHeap::new();
        loop {
            while let Some(k) = heads.peek() {
                if heap.peek().is_some_and(|top| k.tip.0 < top.eff) {
                    break;
                }
                let k = *heads.next().unwrap();
                let acct = &g.accounts[&k.sender];
                let first = skip.get(&k.sender).map_or(acct.nonce, |n| (*n).max(acct.nonce));
                if first >= acct.nonce + acct.exec as u64 {
                    continue; // everything executable is already in the parent chain
                }
                let e = &acct.txs[&first];
                if let Some(eff) = eff(e) {
                    heap.push(Cand { eff, seq: Reverse(e.seq), sender: k.sender, nonce: first });
                }
            }
            let Some(c) = heap.pop() else { break };
            let acct = &g.accounts[&c.sender];
            let e = &acct.txs[&c.nonce];
            out.push((*e.tx).clone());
            gas += e.tx.gas_limit;
            size += e.tx.raw.len();
            if gas >= gas_budget || size >= max_bytes {
                break;
            }
            let next = c.nonce + 1;
            if next < acct.nonce + acct.exec as u64 {
                let n = &acct.txs[&next];
                if let Some(eff) = eff(n) {
                    heap.push(Cand { eff, seq: Reverse(n.seq), sender: c.sender, nonce: next });
                }
            }
        }
        out
    }

    /// (pending, queued).
    pub fn status(&self) -> (usize, usize) {
        let g = self.inner.lock().unwrap();
        (g.pending, g.total - g.pending)
    }

    pub fn has(&self, hash: &B256) -> bool {
        self.inner.lock().unwrap().by_hash.contains_key(hash)
    }

    /// The pool's nonce for `addr`: the state nonce plus the executable
    /// txs; None when the pool holds nothing of the address.
    pub fn pending_nonce(&self, addr: Address) -> Option<u64> {
        self.inner.lock().unwrap().accounts.get(&addr).map(|a| a.nonce + a.exec as u64)
    }

    /// (pending, queued) txs of one address, or of every address (address
    /// order, then nonce), at most `limit` of each half (0 = all).
    pub fn content(&self, addr: Option<Address>, limit: usize) -> (Vec<Arc<Tx>>, Vec<Arc<Tx>>) {
        let g = self.inner.lock().unwrap();
        let limit = if limit == 0 { usize::MAX } else { limit };
        let mut pending = Vec::new();
        let mut queued = Vec::new();
        let mut push = |a: &Account| {
            for (i, e) in a.txs.values().enumerate() {
                let dst = if i < a.exec { &mut pending } else { &mut queued };
                if dst.len() < limit {
                    dst.push(e.tx.clone());
                }
            }
        };
        match addr {
            Some(a) => {
                if let Some(acct) = g.accounts.get(&a) {
                    push(acct);
                }
            }
            None => {
                let mut addrs: Vec<&Address> = g.accounts.keys().collect();
                addrs.sort();
                for a in addrs {
                    push(&g.accounts[a]);
                }
            }
        }
        (pending, queued)
    }

    /// Blocks until the pool holds an executable tx or `timeout` passes;
    /// true when it does.
    pub fn wait(&self, timeout: Duration) -> bool {
        let g = self.inner.lock().unwrap();
        let (g, _) = self.cv.wait_timeout_while(g, timeout, |g| g.pending == 0).unwrap();
        g.pending > 0
    }

    /// The senders the pool already recovered for `hashes` (admission
    /// recovers every tx once); None for a tx the pool does not hold. A
    /// peer's block is mostly the pool's own txs, so its parse recovers only
    /// the rest.
    pub fn senders(&self, hashes: &[B256]) -> Vec<Option<Address>> {
        let g = self.inner.lock().unwrap();
        hashes.iter().map(|h| g.by_hash.get(h).map(|(a, _)| *a)).collect()
    }

    /// Every tx admitted since the last drain (local and remote: the push
    /// gossiper forwards both), oldest first.
    pub fn drain_gossip(&self) -> Vec<Arc<Tx>> {
        self.inner.lock().unwrap().gossip.drain(..).collect()
    }

    /// Drops the queued txs of senders idle for longer than the lifetime
    /// (legacypool's heartbeat rule; senders with executable txs are kept).
    pub fn expire(&self, now: Instant) {
        self.inner.lock().unwrap().expire(now, self.cfg.lifetime);
    }

    pub fn gen(&self) -> u64 {
        self.inner.lock().unwrap().gen
    }
}

impl Inner {
    /// Removes one tx by hash (the account is re-settled by the caller).
    fn remove_hash(&mut self, hash: &B256) -> bool {
        let Some((sender, nonce)) = self.by_hash.remove(hash) else { return false };
        let Some(acct) = self.accounts.get_mut(&sender) else { return false };
        if let Some(e) = acct.txs.remove(&nonce) {
            self.priced.remove(&PricedKey { tip: e.tx.gas_tip, seq: Reverse(e.seq), hash: e.tx.hash });
            self.total -= 1;
            if nonce < acct.nonce + acct.exec as u64 {
                // Inside the executable prefix: the prefix now ends here.
                let was = acct.exec;
                acct.exec = (nonce - acct.nonce) as usize;
                acct.exec_cost = acct.txs.values().take(acct.exec).fold(U256::ZERO, |s, e| s + e.cost);
                self.pending -= was - acct.exec;
                if acct.exec == 0 {
                    if let Some(k) = acct.head_key.take() {
                        self.heads.remove(&k);
                    }
                }
            }
        }
        true
    }

    /// Recomputes the sender's executable prefix from its state nonce:
    /// mined (nonce below), unpayable and over-gas txs go, the prefix is
    /// the contiguous run from the nonce, the head key follows. An emptied
    /// sender is removed.
    fn settle(&mut self, sender: &Address) {
        let head = self.head;
        let Some(acct) = self.accounts.get_mut(sender) else { return };
        let mut drop: Vec<(u64, B256, u128, u64)> = Vec::new();
        for (n, e) in &acct.txs {
            if *n < acct.nonce || e.cost > acct.balance || e.tx.gas_limit > head.gas_limit {
                drop.push((*n, e.tx.hash, e.tx.gas_tip, e.seq));
            }
        }
        for (n, hash, tip, seq) in drop {
            acct.txs.remove(&n);
            self.by_hash.remove(&hash);
            self.priced.remove(&PricedKey { tip, seq: Reverse(seq), hash });
            self.total -= 1;
        }
        let was = acct.exec;
        let mut exec = 0usize;
        let mut cost = U256::ZERO;
        for (n, e) in &acct.txs {
            if *n != acct.nonce + exec as u64 {
                break;
            }
            exec += 1;
            cost += e.cost;
        }
        acct.exec = exec;
        acct.exec_cost = cost;
        self.pending = self.pending + exec - was;
        let key = if exec > 0 {
            let e = &acct.txs[&acct.nonce];
            Some(HeadKey { tip: Reverse(e.tx.gas_tip), seq: e.seq, sender: *sender })
        } else {
            None
        };
        if key != acct.head_key {
            if let Some(k) = acct.head_key.take() {
                self.heads.remove(&k);
            }
            if let Some(k) = key {
                self.heads.insert(k);
            }
            acct.head_key = key;
        }
    }

    /// legacypool.add for one validated tx whose sender's account exists.
    fn insert(&mut self, t: Tx, local: bool, cfg: &Config, now: Instant, promoted: &mut bool) -> (Code, &'static str) {
        if self.by_hash.contains_key(&t.hash) {
            return (Code::Known, "already known");
        }
        let sender = t.sender.unwrap();
        let Some(cost) = cost_of(&t) else { return (Code::Funds, "insufficient funds for gas * price + value") };
        let (replacing, gapped, queued, exec) = {
            let acct = &self.accounts[&sender];
            if t.nonce < acct.nonce {
                return (Code::NonceLow, "nonce too low");
            }
            if acct.balance < cost {
                return (Code::Funds, "insufficient funds for gas * price + value");
            }
            let old = acct.txs.get(&t.nonce);
            if let Some(o) = old {
                // list.Add: both the fee cap and the tip must beat the old by the bump.
                let bump = |x: u128| x.saturating_mul(100 + cfg.price_bump as u128) / 100;
                if t.gas_price <= o.tx.gas_price || t.gas_tip <= o.tx.gas_tip || t.gas_price < bump(o.tx.gas_price) || t.gas_tip < bump(o.tx.gas_tip) {
                    return (Code::Underpriced, "replacement transaction underpriced");
                }
            }
            // Funds for the whole executable sequence plus this tx (minus the tx it replaces).
            let exec_end = acct.nonce + acct.exec as u64;
            let spent = match old {
                Some(o) if t.nonce < exec_end => acct.exec_cost - o.cost,
                _ => acct.exec_cost,
            };
            if spent.checked_add(cost).is_none_or(|need| acct.balance < need) {
                return (Code::Funds, "insufficient funds for gas * price + value");
            }
            (old.is_some(), t.nonce > exec_end, acct.queued(), acct.exec)
        };
        if !replacing && !local {
            if gapped && queued >= cfg.account_queue {
                return (Code::Full, "txpool is full");
            }
            if !gapped && self.pending >= cfg.global_slots && exec >= cfg.account_slots {
                return (Code::Full, "txpool is full");
            }
            if self.total >= cfg.global_slots + cfg.global_queue {
                // Full: the cheapest remote tx makes room when it is cheaper than this one.
                let Some(cheapest) = self.priced.iter().next().copied() else { return (Code::Full, "txpool is full") };
                if cheapest.tip >= t.gas_tip {
                    return (Code::Underpriced, "transaction underpriced");
                }
                let victim = self.by_hash.get(&cheapest.hash).map(|(a, _)| *a);
                self.remove_hash(&cheapest.hash);
                if let Some(a) = victim {
                    self.settle(&a);
                    if a != sender {
                        self.prune(&a);
                    }
                }
            }
        }
        self.seq += 1;
        let seq = self.seq;
        let tx = Arc::new(t);
        let acct = self.accounts.get_mut(&sender).expect("sender account");
        acct.beat = now;
        if local {
            acct.local = true;
        }
        if let Some(o) = acct.txs.remove(&tx.nonce) {
            self.by_hash.remove(&o.tx.hash);
            self.priced.remove(&PricedKey { tip: o.tx.gas_tip, seq: Reverse(o.seq), hash: o.tx.hash });
            self.total -= 1;
            if tx.nonce < acct.nonce + acct.exec as u64 {
                // Replacing inside the prefix: settle recounts it below.
                self.pending -= acct.exec;
                acct.exec = 0;
                acct.exec_cost = U256::ZERO;
            }
        }
        if !acct.local {
            self.priced.insert(PricedKey { tip: tx.gas_tip, seq: Reverse(seq), hash: tx.hash });
        }
        self.by_hash.insert(tx.hash, (sender, tx.nonce));
        acct.txs.insert(tx.nonce, Entry { tx: tx.clone(), seq, cost });
        self.total += 1;
        let before = self.pending;
        self.settle(&sender);
        if self.pending > before || replacing {
            *promoted = true;
        }
        if self.gossip.len() >= GOSSIP_MAX {
            self.gossip.pop_front();
        }
        self.gossip.push_back(tx);
        if replacing { (Code::Replaced, "") } else { (Code::Ok, "") }
    }

    /// Drops an account that holds no tx.
    fn prune(&mut self, a: &Address) {
        if self.accounts.get(a).is_some_and(|x| x.txs.is_empty()) {
            self.accounts.remove(a);
        }
    }

    fn expire(&mut self, now: Instant, lifetime: Duration) {
        let idle: Vec<Address> = self.accounts.iter().filter(|(_, a)| a.exec == 0 && now.duration_since(a.beat) > lifetime).map(|(a, _)| *a).collect();
        for a in idle {
            let hashes: Vec<B256> = self.accounts[&a].txs.values().map(|e| e.tx.hash).collect();
            for h in hashes {
                self.remove_hash(&h);
            }
            self.accounts.remove(&a);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{keccak256, U256};
    use secp256k1::{Message, Secp256k1, SecretKey};

    const CHAIN_ID: u64 = 99999;

    fn chain() -> Arc<exec::Config> {
        let g = br#"{"config":{"chainId":99999,"feeConfig":{"gasLimit":20000000,"minBaseFee":1000000000,"targetGas":100000000,"baseFeeChangeDenominator":48,"minBlockGasCost":0,"maxBlockGasCost":10000000,"targetBlockRate":2,"blockGasCostStep":500000}},"alloc":{},"timestamp":"0x0"}"#;
        Arc::new(exec::Config::from_genesis(g, b"{}", 12345).unwrap())
    }

    fn head() -> Head {
        Head { gas_limit: 20_000_000, min_base_fee: 1_000_000_000, time: 1_700_000_000 }
    }

    struct Key(SecretKey);

    fn key(i: u8) -> Key {
        Key(SecretKey::from_byte_array([i; 32]).unwrap())
    }

    fn addr_of(k: &Key) -> Address {
        let pk = k.0.public_key(&Secp256k1::new()).serialize_uncompressed();
        Address::from_slice(&keccak256(&pk[1..])[12..])
    }

    /// A signed EIP-1559 transfer of 21,000 gas.
    fn sign(k: &Key, nonce: u64, tip: u128, cap: u128, value: u64) -> Bytes {
        sign_full(k, nonce, tip, cap, 21_000, value, Some(Address::from([9u8; 20])), &[])
    }

    fn sign_full(k: &Key, nonce: u64, tip: u128, cap: u128, gas: u64, value: u64, to: Option<Address>, data: &[u8]) -> Bytes {
        use alloy_rlp::Encodable;
        let mut body = Vec::new();
        CHAIN_ID.encode(&mut body);
        nonce.encode(&mut body);
        tip.encode(&mut body);
        cap.encode(&mut body);
        gas.encode(&mut body);
        match to {
            Some(a) => a.encode(&mut body),
            None => body.push(0x80),
        }
        U256::from(value).encode(&mut body);
        alloy_rlp::Header { list: false, payload_length: data.len() }.encode(&mut body);
        body.extend_from_slice(data);
        body.push(0xc0); // access list
        let mut unsigned = vec![2u8];
        alloy_rlp::Header { list: true, payload_length: body.len() }.encode(&mut unsigned);
        unsigned.extend_from_slice(&body);
        let h = keccak256(&unsigned);
        let sig = Secp256k1::new().sign_ecdsa_recoverable(Message::from_digest(h.0), &k.0);
        let (rid, rs) = sig.serialize_compact();
        (i32::from(rid) as u64).encode(&mut body);
        U256::from_be_slice(&rs[..32]).encode(&mut body);
        U256::from_be_slice(&rs[32..]).encode(&mut body);
        let mut out = vec![2u8];
        alloy_rlp::Header { list: true, payload_length: body.len() }.encode(&mut out);
        out.extend_from_slice(&body);
        Bytes::from(out)
    }

    const GWEI: u128 = 1_000_000_000;
    const ETH: u128 = 1_000_000_000_000_000_000;

    /// A state: every address has the given nonce and balance.
    fn state(nonce: u64, balance: u128) -> impl Fn(&[Address]) -> Vec<(u64, U256)> {
        move |addrs| addrs.iter().map(|_| (nonce, U256::from(balance))).collect()
    }

    fn pool(cfg: Config) -> Pool {
        Pool::new(cfg, chain(), head())
    }

    fn codes(v: &[Added]) -> Vec<Code> {
        v.iter().map(|a| a.code).collect()
    }

    #[test]
    fn nonce_order_gaps_and_promotion() {
        let p = pool(Config { account_slots: 100, account_queue: 100, global_slots: 1000, global_queue: 1000, ..Default::default() });
        let k = key(1);
        let st = state(0, 10 * ETH);
        // 2 and 0 arrive: 0 executable, 2 queued; then 1 fills the gap.
        let r = p.add(vec![sign(&k, 2, GWEI, 50 * GWEI, 1), sign(&k, 0, GWEI, 50 * GWEI, 1)], false, &st);
        assert_eq!(codes(&r), [Code::Ok, Code::Ok]);
        assert_eq!(p.status(), (1, 1));
        assert_eq!(p.pending_nonce(addr_of(&k)), Some(1));
        let r = p.add(vec![sign(&k, 1, GWEI, 50 * GWEI, 1)], false, &st);
        assert_eq!(codes(&r), [Code::Ok]);
        assert_eq!(p.status(), (3, 0));
        assert_eq!(p.pending_nonce(addr_of(&k)), Some(3));
        let c = p.candidates(GWEI, 1_000_000, 1 << 20, &HashMap::new());
        assert_eq!(c.iter().map(|t| t.nonce).collect::<Vec<_>>(), [0, 1, 2]);
        // Known, nonce too low after a head that mined 0 and 1.
        let r = p.add(vec![sign(&k, 2, GWEI, 50 * GWEI, 1)], false, &st);
        assert_eq!(codes(&r), [Code::Known]);
        let mined: Vec<Tx> = c[..2].to_vec();
        p.on_accept(&mined, head(), &mut state(2, 10 * ETH));
        assert_eq!(p.status(), (1, 0));
        let r = p.add(vec![sign(&k, 1, GWEI, 50 * GWEI, 7)], false, &st);
        assert_eq!(codes(&r), [Code::NonceLow]);
        assert!(!p.has(&mined[0].hash));
        assert!(p.has(&c[2].hash));
    }

    #[test]
    fn replacement_needs_the_bump() {
        let p = pool(Config::default());
        let k = key(2);
        let st = state(0, 10 * ETH);
        p.add(vec![sign(&k, 0, GWEI, 50 * GWEI, 1)], false, &st);
        let r = p.add(vec![sign(&k, 0, GWEI + 1, 50 * GWEI + 1, 2)], false, &st);
        assert_eq!(codes(&r), [Code::Underpriced]);
        let r = p.add(vec![sign(&k, 0, 2 * GWEI, 60 * GWEI, 2)], false, &st);
        assert_eq!(codes(&r), [Code::Replaced]);
        assert_eq!(p.status(), (1, 0));
        let c = p.candidates(GWEI, 1_000_000, 1 << 20, &HashMap::new());
        assert_eq!(c.len(), 1);
        assert_eq!(c[0].value, U256::from(2));
    }

    #[test]
    fn cost_check_spans_the_executable_sequence() {
        let p = pool(Config { account_slots: 100, global_slots: 1000, ..Default::default() });
        let k = key(3);
        // Balance covers exactly two 50 gwei x 21k transfers of value 1.
        let cost = 21_000 * 50 * GWEI + 1;
        let st = state(0, 2 * cost);
        let r = p.add(vec![sign(&k, 0, GWEI, 50 * GWEI, 1), sign(&k, 1, GWEI, 50 * GWEI, 1), sign(&k, 2, GWEI, 50 * GWEI, 1)], false, &st);
        assert_eq!(codes(&r), [Code::Ok, Code::Ok, Code::Funds]);
        // A tx nobody can pay alone.
        let r = p.add(vec![sign(&k, 5, GWEI, 50 * GWEI, 3 * cost as u64)], false, &st);
        assert_eq!(codes(&r), [Code::Funds]);
    }

    #[test]
    fn stateless_rules() {
        let p = pool(Config::default());
        let k = key(4);
        let st = state(0, 10 * ETH);
        let r = p.add(
            vec![
                sign_full(&k, 0, GWEI, 50 * GWEI, 30_000_000, 1, Some(Address::from([9; 20])), &[]), // over the block gas limit
                sign_full(&k, 0, GWEI, 50 * GWEI, 20_000, 1, Some(Address::from([9; 20])), &[]),     // under intrinsic
                sign(&k, 0, 0, 50 * GWEI, 1),                                                        // tip under the price limit
                sign(&k, 0, GWEI / 4, GWEI / 2, 1),                                                  // fee cap under the min base fee
                Bytes::from_static(&[0x02, 0xc0]),                                                   // garbage
                sign(&k, 0, GWEI, 50 * GWEI, 1),
            ],
            false,
            &st,
        );
        assert_eq!(codes(&r), [Code::GasLimit, Code::Intrinsic, Code::Underpriced, Code::Underpriced, Code::Other, Code::Ok]);
        assert_eq!(r[5].hash, keccak256(sign(&k, 0, GWEI, 50 * GWEI, 1)));
        // A create with initcode: 53,000 + 16 per byte + 2 per word.
        let data = vec![1u8; 100];
        let t = block::eth::decode_tx(sign_full(&k, 1, GWEI, 50 * GWEI, 100_000, 0, None, &data)).unwrap();
        assert_eq!(intrinsic_gas(&t, true), Some(53_000 + 1600 + 8));
        assert_eq!(intrinsic_gas(&t, false), Some(53_000 + 1600));
    }

    #[test]
    fn caps_and_eviction() {
        let p = pool(Config { account_slots: 2, account_queue: 1, global_slots: 3, global_queue: 1, ..Default::default() });
        let st = state(0, 10 * ETH);
        let (a, b, c) = (key(5), key(6), key(7));
        // a: 2 executable + 1 queued (the second queued one is over the account queue).
        let r = p.add(vec![sign(&a, 0, GWEI, 50 * GWEI, 1), sign(&a, 1, GWEI, 50 * GWEI, 1), sign(&a, 5, GWEI, 50 * GWEI, 1), sign(&a, 6, GWEI, 50 * GWEI, 1)], false, &st);
        assert_eq!(codes(&r), [Code::Ok, Code::Ok, Code::Ok, Code::Full]);
        assert_eq!(p.status(), (2, 1));
        // b fills the pool (3 pending + 1 queued = the global cap).
        let r = p.add(vec![sign(&b, 0, 2 * GWEI, 50 * GWEI, 1)], false, &st);
        assert_eq!(codes(&r), [Code::Ok]);
        assert_eq!(p.status(), (3, 1));
        // c at 1 gwei: not better than the cheapest -> underpriced; at 3 gwei it evicts a's cheapest (the newest 1 gwei tx, nonce 5).
        let r = p.add(vec![sign(&c, 0, GWEI, 50 * GWEI, 1)], false, &st);
        assert_eq!(codes(&r), [Code::Underpriced]);
        let r = p.add(vec![sign(&c, 0, 3 * GWEI, 50 * GWEI, 1)], false, &st);
        assert_eq!(codes(&r), [Code::Ok]);
        assert_eq!(p.status(), (4, 0));
        assert_eq!(p.pending_nonce(addr_of(&a)), Some(2));
        // Pending over the global slots: a third executable tx of a over its account slots is refused.
        let r = p.add(vec![sign(&a, 2, GWEI, 50 * GWEI, 1)], false, &st);
        assert_eq!(codes(&r), [Code::Full]);
    }

    #[test]
    fn build_order_by_effective_tip_then_arrival_and_base_fee_change() {
        let p = pool(Config { account_slots: 100, global_slots: 1000, ..Default::default() });
        let st = state(0, 10 * ETH);
        let (a, b, c) = (key(8), key(9), key(10));
        // a: tip 5, cap 6 gwei (effective tip shrinks as the base fee grows). b: tip 3, cap 50. c: tip 3, cap 50, later.
        p.add(vec![sign(&a, 0, 5 * GWEI, 6 * GWEI, 1), sign(&a, 1, 5 * GWEI, 6 * GWEI, 1)], false, &st);
        p.add(vec![sign(&b, 0, 3 * GWEI, 50 * GWEI, 1)], false, &st);
        p.add(vec![sign(&c, 0, 3 * GWEI, 50 * GWEI, 1)], false, &st);
        let order = |base: u128| p.candidates(base, 10_000_000, 1 << 20, &HashMap::new()).iter().map(|t| (t.sender.unwrap(), t.nonce)).collect::<Vec<_>>();
        let (aa, ba, ca) = (addr_of(&a), addr_of(&b), addr_of(&c));
        assert_eq!(order(GWEI), [(aa, 0), (aa, 1), (ba, 0), (ca, 0)]);
        // Base fee 4 gwei: a's effective tip is 2, below b and c (3); b before c by arrival.
        assert_eq!(order(4 * GWEI), [(ba, 0), (ca, 0), (aa, 0), (aa, 1)]);
        // Base fee 7 gwei: a cannot pay it and is left out.
        assert_eq!(order(7 * GWEI), [(ba, 0), (ca, 0)]);
        // The gas budget cuts the list.
        assert_eq!(p.candidates(GWEI, 30_000, 1 << 20, &HashMap::new()).len(), 2);
        assert_eq!(p.candidates(GWEI, 10_000_000, 100, &HashMap::new()).len(), 1);
    }

    #[test]
    fn accept_touches_only_its_senders_and_rejected_blocks_re_include() {
        let p = pool(Config { account_slots: 100, global_slots: 1000, ..Default::default() });
        let st = state(0, 10 * ETH);
        let (a, b) = (key(11), key(12));
        p.add(vec![sign(&a, 0, GWEI, 50 * GWEI, 1), sign(&a, 1, GWEI, 50 * GWEI, 1), sign(&b, 0, GWEI, 50 * GWEI, 1)], false, &st);
        let c1 = p.candidates(GWEI, 10_000_000, 1 << 20, &HashMap::new());
        assert_eq!(c1.len(), 3);
        // A rejected block: nothing changed, the same candidates come back.
        let c2 = p.candidates(GWEI, 10_000_000, 1 << 20, &HashMap::new());
        assert_eq!(c1.iter().map(|t| t.hash).collect::<Vec<_>>(), c2.iter().map(|t| t.hash).collect::<Vec<_>>());
        // A block with a's first tx only: a re-read (nonce 1), b untouched (the reader must not see it).
        let mined = vec![c1.iter().find(|t| t.sender == Some(addr_of(&a)) && t.nonce == 0).unwrap().clone()];
        let mut asked = Vec::new();
        p.on_accept(&mined, head(), &mut |addrs| {
            asked.extend_from_slice(addrs);
            addrs.iter().map(|_| (1u64, U256::from(10 * ETH))).collect()
        });
        assert_eq!(asked, [addr_of(&a)]);
        assert_eq!(p.status(), (2, 0));
        assert_eq!(p.pending_nonce(addr_of(&a)), Some(2));
        // A head that says a's nonce is 5: its remaining tx is old and goes.
        p.on_accept(&mined, head(), &mut state(5, 10 * ETH));
        assert_eq!(p.status(), (1, 0));
        assert_eq!(p.pending_nonce(addr_of(&a)), None);
        // A balance drop makes b's tx unpayable at the next touch.
        let bt = p.candidates(GWEI, 10_000_000, 1 << 20, &HashMap::new());
        p.on_accept(&bt, head(), &mut state(0, 0));
        assert_eq!(p.status(), (0, 0));
    }

    #[test]
    fn candidates_skip_the_pending_parents_txs() {
        let p = pool(Config { account_slots: 100, global_slots: 1000, ..Default::default() });
        let st = state(0, 10 * ETH);
        let (a, b) = (key(14), key(15));
        p.add(vec![sign(&a, 0, GWEI, 50 * GWEI, 1), sign(&a, 1, GWEI, 50 * GWEI, 1), sign(&a, 2, GWEI, 50 * GWEI, 1), sign(&b, 0, GWEI, 50 * GWEI, 1)], false, &st);
        // The unaccepted parent holds a's 0 and 1 and b's 0: only a's 2 is left.
        let skip = HashMap::from([(addr_of(&a), 2u64), (addr_of(&b), 1u64)]);
        let c = p.candidates(GWEI, 10_000_000, 1 << 20, &skip);
        assert_eq!(c.iter().map(|t| (t.sender.unwrap(), t.nonce)).collect::<Vec<_>>(), [(addr_of(&a), 2)]);
        // A stale skip (below the state nonce) changes nothing.
        let skip = HashMap::from([(addr_of(&a), 0u64)]);
        assert_eq!(p.candidates(GWEI, 10_000_000, 1 << 20, &skip).len(), 4);
    }

    #[test]
    fn lifetime_and_gossip_and_wait() {
        let p = pool(Config { lifetime: Duration::from_secs(1), ..Default::default() });
        let st = state(0, 10 * ETH);
        let k = key(13);
        assert!(!p.wait(Duration::from_millis(1)));
        p.add(vec![sign(&k, 3, GWEI, 50 * GWEI, 1)], false, &st); // queued only
        assert_eq!(p.status(), (0, 1));
        assert_eq!(p.drain_gossip().len(), 1);
        assert!(p.drain_gossip().is_empty());
        p.expire(Instant::now() + Duration::from_secs(2));
        assert_eq!(p.status(), (0, 0));
        p.add(vec![sign(&k, 0, GWEI, 50 * GWEI, 1)], false, &st);
        assert!(p.wait(Duration::from_millis(1)));
        let (pend, q) = p.content(None, 0);
        assert_eq!((pend.len(), q.len()), (1, 0));
        assert_eq!(p.content(Some(Address::ZERO), 0).0.len(), 0);
    }

    #[test]
    fn config_keys() {
        let c = Config::from_json(&serde_json::json!({"tx-pool-account-slots": 1000, "tx-pool-global-slots": "200000", "tx-pool-lifetime": "1h30m", "local-txs-enabled": true}));
        assert_eq!((c.account_slots, c.global_slots, c.lifetime, c.locals, c.price_bump), (1000, 200000, Duration::from_secs(5400), true, 10));
        assert_eq!(Config::from_json(&serde_json::json!({"tx-pool-lifetime": 600_000_000_000u64})).lifetime, Duration::from_secs(600));
    }
}

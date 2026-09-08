//! Rows files ([klen u8][vlen u8][key][value], exp/statedump's framing) and
//! the statedump to contract-form conversion exp/commitroot -sample does.
use crate::keccak::keccak256;
use crate::rlp;
use std::io;

pub type Row = (Vec<u8>, Vec<u8>);

pub fn read_rows(path: &std::path::Path) -> io::Result<Vec<Row>> {
    let raw = std::fs::read(path)?;
    let mut rows = Vec::new();
    let mut i = 0;
    while i + 2 <= raw.len() {
        let (kl, vl) = (raw[i] as usize, raw[i + 1] as usize);
        i += 2;
        rows.push((raw[i..i + kl].to_vec(), raw[i + kl..i + kl + vl].to_vec()));
        i += kl + vl;
    }
    Ok(rows)
}

pub fn write_rows(path: &std::path::Path, rows: &[Row]) -> io::Result<()> {
    let mut out = Vec::new();
    for (k, v) in rows {
        out.extend_from_slice(&[k.len() as u8, v.len() as u8]);
        out.extend_from_slice(k);
        out.extend_from_slice(v);
    }
    std::fs::write(path, out)
}

/// keccak256 of no bytes: the code hash of an account without code.
pub const EMPTY_CODE_HASH: [u8; 32] = [
    0xc5, 0xd2, 0x46, 0x01, 0x86, 0xf7, 0x23, 0x3c, 0x92, 0x7e, 0x7d, 0xb2, 0xdc, 0xc7, 0x03, 0xc0,
    0xe5, 0x00, 0xb6, 0x53, 0xca, 0x82, 0x27, 0x3b, 0x7b, 0xfa, 0xd8, 0x04, 0x5d, 0x85, 0xa4, 0x70,
];

/// The contract account row RLP[nonce, balance, codeHash]; nonce and
/// balance as minimal big-endian bytes.
pub fn contract_row(nonce: &[u8], balance: &[u8], code_hash: &[u8]) -> Vec<u8> {
    let mut body = Vec::with_capacity(48);
    rlp::put_bytes(&mut body, nonce);
    rlp::put_bytes(&mut body, balance);
    rlp::put_bytes(&mut body, code_hash);
    let mut out = Vec::with_capacity(body.len() + 3);
    rlp::put_header(&mut out, true, body.len());
    out.extend_from_slice(&body);
    out
}

/// Converts a statedump (raw addr+'a' / addr+'s'+slot keys, store row
/// values, grouped by address) to sorted contract-form rows the way
/// exp/commitroot -sample does: empty values skipped, a placeholder account
/// row when a contract's slots come without one.
pub fn to_contract_rows(dump: &[Row]) -> Vec<Row> {
    let mut rows = Vec::with_capacity(dump.len());
    let mut last_addr: &[u8] = &[];
    let mut last_hash = [0u8; 32];
    let mut have_acct = false;
    for (k, v) in dump {
        assert!(k.len() >= 21, "bad key {:x?}", k);
        if &k[..20] != last_addr {
            last_addr = &k[..20];
            last_hash = keccak256(last_addr);
            have_acct = false;
        }
        if v.is_empty() {
            continue;
        }
        let mut acct_key = last_hash.to_vec();
        acct_key.push(0);
        match (k.len(), k[20]) {
            (21, b'a') => {
                let (content, _) = rlp::split_list(v).expect("account rlp");
                let (nonce, r) = rlp::split_string(content).unwrap();
                let (bal, r) = rlp::split_string(r).unwrap();
                let (_root, r) = rlp::split_string(r).unwrap();
                let (code, _) = rlp::split_string(r).unwrap();
                rows.push((acct_key, contract_row(nonce, bal, code)));
                have_acct = true;
            }
            (53, b's') => {
                if !have_acct {
                    rows.push((acct_key, contract_row(&[1], &[], &EMPTY_CODE_HASH)));
                    have_acct = true;
                }
                let mut sk = last_hash.to_vec();
                sk.push(1);
                sk.extend_from_slice(&keccak256(&k[21..]));
                rows.push((sk, v.clone()));
            }
            _ => panic!("bad key {:x?}", k),
        }
    }
    rows.sort_unstable_by(|a, b| a.0.cmp(&b.0));
    rows
}

/// xorshift64*: the deterministic source for tests and benches.
pub struct Rng(pub u64);

impl Rng {
    pub fn new(seed: u64) -> Rng {
        Rng(seed.wrapping_mul(0x9E3779B97F4A7C15) | 1)
    }
    pub fn u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545F4914F6CDD1D)
    }
    pub fn below(&mut self, n: usize) -> usize {
        (self.u64() % n as u64) as usize
    }
    pub fn fill(&mut self, b: &mut [u8]) {
        for c in b.chunks_mut(8) {
            let x = self.u64().to_le_bytes();
            c.copy_from_slice(&x[..c.len()]);
        }
    }
    pub fn bytes(&mut self, n: usize) -> Vec<u8> {
        let mut v = vec![0u8; n];
        self.fill(&mut v);
        v
    }
    /// A slot word shaped like commit_test's randWord: one byte, eight
    /// bytes, or a full random word.
    pub fn word(&mut self) -> [u8; 32] {
        let mut h = [0u8; 32];
        match self.below(4) {
            0 => h[31] = self.below(255) as u8 + 1,
            1 => h[24..].copy_from_slice(&(self.u64() | 1).to_be_bytes()),
            _ => self.fill(&mut h),
        }
        h
    }
}

/// The test model of a state: accounts by address.
#[derive(Clone, Default)]
pub struct Acct {
    pub nonce: u64,
    pub bal: u64,
    pub code: Option<Vec<u8>>,
    pub slots: std::collections::BTreeMap<[u8; 32], [u8; 32]>,
}

pub type Model = std::collections::BTreeMap<[u8; 20], Acct>;

pub fn acct_key(addr: &[u8; 20]) -> Vec<u8> {
    let mut k = keccak256(addr).to_vec();
    k.push(0);
    k
}

pub fn slot_key(addr: &[u8; 20], slot: &[u8; 32]) -> Vec<u8> {
    let mut k = keccak256(addr).to_vec();
    k.push(1);
    k.extend_from_slice(&keccak256(slot));
    k
}

fn be_trim(v: u64) -> Vec<u8> {
    let b = v.to_be_bytes();
    b[(v.leading_zeros() / 8) as usize..].to_vec()
}

pub fn trim_word(w: &[u8; 32]) -> Vec<u8> {
    let i = w.iter().position(|&x| x != 0).unwrap_or(32);
    w[i..].to_vec()
}

pub fn acct_val(a: &Acct) -> Vec<u8> {
    let ch = match &a.code {
        Some(c) if !c.is_empty() => keccak256(c),
        _ => EMPTY_CODE_HASH,
    };
    contract_row(&be_trim(a.nonce), &be_trim(a.bal), &ch)
}

/// n accounts shaped like commit_test's genState: half without storage, a
/// few big contracts.
pub fn gen_state(rng: &mut Rng, n: usize) -> Model {
    let mut m = Model::new();
    for i in 0..n {
        let mut addr = [0u8; 20];
        rng.fill(&mut addr);
        let mut a = Acct { nonce: rng.below(1000) as u64 + 1, bal: rng.u64(), ..Default::default() };
        let r = rng.below(100);
        let ns = if r < 50 {
            0
        } else if r < 80 {
            rng.below(4) + 1
        } else if r < 98 {
            rng.below(64) + 1
        } else {
            rng.below(3000) + 1
        };
        if ns > 0 {
            a.code = Some(vec![0x60, i as u8]);
            for j in 0..ns {
                let mut s = [0u8; 32];
                if rng.below(2) == 0 {
                    s[24..].copy_from_slice(&(j as u64).to_be_bytes());
                } else {
                    rng.fill(&mut s);
                }
                a.slots.insert(s, rng.word());
            }
        }
        m.insert(addr, a);
    }
    m
}

pub fn flatten(m: &Model) -> Vec<Row> {
    let mut rows = Vec::new();
    for (addr, a) in m {
        rows.push((acct_key(addr), acct_val(a)));
        for (s, v) in &a.slots {
            rows.push((slot_key(addr, s), trim_word(v)));
        }
    }
    rows.sort_unstable_by(|a, b| a.0.cmp(&b.0));
    rows
}

/// Plays `ops` random writes of the given kinds (commit_test's runDirty:
/// 0 account fields, 1 delete with slots, 2 new account, 3/4 slot writes,
/// 5 slot clears, 6 delete then recreate) into the model and returns the
/// contract writes in apply order.
pub fn mutate(m: &mut Model, rng: &mut Rng, kinds: &[usize], ops: usize) -> Vec<Row> {
    let mut out = Vec::new();
    let mut addrs: Vec<[u8; 20]> = m.keys().copied().collect();
    for _ in 0..ops {
        if addrs.is_empty() {
            break;
        }
        let addr = addrs[rng.below(addrs.len())];
        if !m.contains_key(&addr) {
            continue;
        }
        match kinds[rng.below(kinds.len())] {
            0 => {
                let a = m.get_mut(&addr).unwrap();
                a.nonce += 1;
                a.bal = rng.u64();
                out.push((acct_key(&addr), acct_val(a)));
            }
            1 => {
                m.remove(&addr);
                out.push((acct_key(&addr), vec![]));
            }
            2 => {
                let mut n = Acct { nonce: 1, bal: rng.u64(), ..Default::default() };
                let mut na = [0u8; 20];
                rng.fill(&mut na);
                out.push((acct_key(&na), acct_val(&n)));
                if rng.below(2) == 0 {
                    for _ in 0..rng.below(5) + 1 {
                        let (s, v) = (rng.word(), rng.word());
                        n.slots.insert(s, v);
                        out.push((slot_key(&na, &s), trim_word(&v)));
                    }
                }
                m.insert(na, n);
                addrs.push(na);
            }
            3 | 4 => {
                let a = m.get_mut(&addr).unwrap();
                for _ in 0..rng.below(20) + 1 {
                    let s = if !a.slots.is_empty() && rng.below(2) == 0 {
                        let i = rng.below(a.slots.len());
                        *a.slots.keys().nth(i).unwrap()
                    } else {
                        rng.word()
                    };
                    let v = rng.word();
                    a.slots.insert(s, v);
                    out.push((slot_key(&addr, &s), trim_word(&v)));
                }
            }
            5 => {
                let a = m.get_mut(&addr).unwrap();
                let keys: Vec<[u8; 32]> = a.slots.keys().copied().collect();
                for s in keys {
                    if rng.below(2) == 0 {
                        a.slots.remove(&s);
                        out.push((slot_key(&addr, &s), vec![]));
                    }
                }
            }
            _ => {
                out.push((acct_key(&addr), vec![]));
                let a = m.get_mut(&addr).unwrap();
                a.nonce = 1;
                a.slots.clear();
                a.code = None;
                out.push((acct_key(&addr), acct_val(a)));
                let (s, v) = (rng.word(), rng.word());
                a.slots.insert(s, v);
                out.push((slot_key(&addr, &s), trim_word(&v)));
            }
        }
    }
    out
}

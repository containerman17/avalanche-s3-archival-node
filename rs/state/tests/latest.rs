//! Port of latest/latest_test.go.
use state::overlay::{Overlay, ENTRY_OVERHEAD};
use state::run::{Run, Writer, BLOCK_SIZE, CENSUS_ROWS, FOOTER_LEN};
use state::sample::{contract_row, EMPTY_CODE_HASH};
use state::sample::Rng;
use state::view::{merge, View};
use state::KvIter;
use std::collections::{BTreeMap, HashSet};
use std::path::Path;

type Entry = (Vec<u8>, Vec<u8>);

/// n distinct random keys (lengths drawn from klens) with random values
/// 0..=126 bytes (127 fits only account RLP under a 33-byte key), one in
/// ten empty, sorted.
fn gen_entries(rng: &mut Rng, n: usize, klens: &[usize]) -> Vec<Entry> {
    let mut seen = HashSet::new();
    let mut es = Vec::new();
    while es.len() < n {
        let kl = klens[rng.below(klens.len())];
        let k = rng.bytes(kl);
        if !seen.insert(k.clone()) {
            continue;
        }
        let vl = if rng.below(10) == 0 { 0 } else { rng.below(127) };
        es.push((k, rng.bytes(vl)));
    }
    es.sort();
    es
}

fn write_run(path: &Path, es: &[Entry], user: [u8; 32]) -> Run {
    let mut w = Writer::create(path).unwrap();
    w.set_user_data(user);
    for (k, v) in es {
        w.add(k, v).unwrap();
    }
    w.close().unwrap();
    Run::open(path).unwrap()
}

fn collect(it: &mut dyn KvIter) -> Vec<Entry> {
    let mut out = vec![];
    while it.next() {
        out.push((it.key().to_vec(), it.value().to_vec()));
    }
    out
}

fn tmp(name: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!("rs-state-test-{}", std::process::id()));
    std::fs::create_dir_all(&d).unwrap();
    d.join(name)
}

#[test]
fn round_trip() {
    let mut rng = Rng::new(1);
    let es = gen_entries(&mut rng, 20000, &[1, 8, 33, 65, 255]);
    let mut user = [0u8; 32];
    rng.fill(&mut user);
    let r = write_run(&tmp("a.run"), &es, user);
    assert_eq!(r.len(), es.len());
    assert_eq!(r.user_data(), user);
    assert!(r.bytes() > 0);
    for (k, v) in &es {
        assert_eq!(r.get(k), Some(v.as_slice()));
    }
    for _ in 0..20000 {
        let kl = 1 + rng.below(70);
        let k = rng.bytes(kl);
        if es.binary_search_by(|e| e.0.cmp(&k)).is_ok() {
            continue;
        }
        assert!(r.get(&k).is_none(), "Get absent {:x?}", k);
    }
    assert_eq!(collect(&mut r.iter(None, None)), es);
}

#[test]
fn writer_rejects() {
    let path = tmp("rej.run");
    let mut w = Writer::create(&path).unwrap();
    w.add(b"b", b"1").unwrap();
    assert!(w.add(b"b", b"2").is_err(), "duplicate");
    assert!(w.add(b"a", b"2").is_err(), "out of order");
    assert!(w.add(b"", b"2").is_err(), "empty key");
    assert!(w.add(&[0u8; 256], b"").is_err(), "256-byte key");
    assert!(w.add(b"c", &[0u8; 128]).is_err(), "128-byte value");
    // A 33-byte key takes a 127-byte value only when it is account RLP: any
    // other value goes behind a tag byte and no longer fits.
    let e = w.add(&[b'c'; 33], &[0u8; 127]).unwrap_err();
    assert!(e.to_string().contains("not account RLP"), "{e}");
    w.add(&[b'c'; 33], &[0u8; 126]).unwrap();
    w.add(&[b'c'; 255], &[0u8; 127]).unwrap();
    w.close().unwrap();
    let r = Run::open(&path).unwrap();
    assert_eq!(r.len(), 3);
    assert_eq!(r.get(&[b'c'; 33]), Some(&[0u8; 126][..]));
    assert_eq!(r.get(&[b'c'; 255]), Some(&[0u8; 127][..]));
    drop(r);
    // An empty run is valid.
    Writer::create(&path).unwrap().close().unwrap();
    let r2 = Run::open(&path).unwrap();
    assert_eq!(r2.len(), 0);
    assert!(collect(&mut r2.iter(None, None)).is_empty());
    assert!(r2.get(b"a").is_none());
}

#[test]
fn corrupt() {
    let mut rng = Rng::new(2);
    let es = gen_entries(&mut rng, 500, &[33]);
    let path = tmp("c.run");
    write_run(&path, &es, [0; 32]);
    let good = std::fs::read(&path).unwrap();
    let n = good.len();
    let flip = |off: usize| {
        let mut b = good.clone();
        b[off] ^= 1;
        b
    };
    let cases: Vec<(&str, Vec<u8>)> = vec![
        ("magic", flip(n - FOOTER_LEN)),
        ("version", flip(n - FOOTER_LEN + 8)),
        ("entry count", flip(n - FOOTER_LEN + 16)),
        ("block count", flip(n - FOOTER_LEN + 24)),
        ("dict offset", flip(n - FOOTER_LEN + 32)),
        ("index offset", flip(n - FOOTER_LEN + 40)),
        ("impure count", flip(n - FOOTER_LEN + 48)),
        ("user data", flip(n - FOOTER_LEN + 56)),
        ("checksum", flip(n - 1)),
        ("index byte", flip(n - FOOTER_LEN - 1)),
        ("truncated", good[..n - 1].to_vec()),
        ("short", good[..10].to_vec()),
        ("extra byte", [good.clone(), vec![0]].concat()),
    ];
    for (what, b) in cases {
        std::fs::write(&path, &b).unwrap();
        assert!(Run::open(&path).is_err(), "{what}: opened");
    }
    // An older format version is refused by name, before the checksum.
    let mut b = good.clone();
    b[n - FOOTER_LEN + 8] = 1;
    std::fs::write(&path, &b).unwrap();
    let e = Run::open(&path).err().unwrap().to_string();
    assert!(e.contains("version 1") && e.contains("reads version 2"), "{e}");
}

fn in_range(k: &[u8], lo: Option<&[u8]>, hi: Option<&[u8]>) -> bool {
    lo.is_none_or(|lo| k >= lo) && hi.is_none_or(|hi| k < hi)
}

#[test]
fn iter_bounds() {
    let mut rng = Rng::new(3);
    let es = gen_entries(&mut rng, 5000, &[4, 33]);
    let r = write_run(&tmp("b.run"), &es, [0; 32]);
    let mut o = Overlay::new();
    for (k, v) in &es {
        o.put(k, v);
    }
    let live: Vec<Entry> = es.iter().filter(|e| !e.1.is_empty()).cloned().collect();
    let v = View::new(Some(&o), &[&r]);
    let bound = |rng: &mut Rng| -> Option<Vec<u8>> {
        match rng.below(4) {
            0 => None,
            1 => Some(es[rng.below(es.len())].0.clone()),
            _ => {
                let n = 1 + rng.below(40);
                Some(rng.bytes(n))
            }
        }
    };
    for _ in 0..300 {
        let (lo, hi) = (bound(&mut rng), bound(&mut rng));
        let (lo, hi) = (lo.as_deref(), hi.as_deref());
        let want: Vec<Entry> = es.iter().filter(|e| in_range(&e.0, lo, hi)).cloned().collect();
        let want_live: Vec<Entry> = live.iter().filter(|e| in_range(&e.0, lo, hi)).cloned().collect();
        assert_eq!(collect(&mut r.iter(lo, hi)), want, "run");
        assert_eq!(collect(&mut o.iter(lo, hi)), want, "overlay");
        assert_eq!(collect(&mut v.iter(lo, hi)), want_live, "view");
    }
}

#[test]
fn merge_levels() {
    let mut rng = Rng::new(4);
    let old = gen_entries(&mut rng, 6000, &[33, 65]);
    let mut reference: BTreeMap<Vec<u8>, Vec<u8>> = old.iter().cloned().collect();
    let mut newer: Vec<Entry> = vec![];
    for (k, _) in &old {
        if rng.below(3) == 0 {
            let n = rng.below(40);
            newer.push((k.clone(), rng.bytes(n)));
        }
    }
    newer.extend(gen_entries(&mut rng, 3000, &[33, 65]));
    newer.sort();
    newer.dedup_by(|a, b| a.0 == b.0);
    for (k, v) in &newer {
        reference.insert(k.clone(), v.clone());
    }
    let r0 = write_run(&tmp("0.run"), &old, [0; 32]);
    let r1 = write_run(&tmp("1.run"), &newer, [0; 32]);
    let mut o = Overlay::new();
    for es in [&old, &newer] {
        for (k, _) in es.iter() {
            match rng.below(6) {
                0 => {
                    o.put(k, b"");
                    reference.insert(k.clone(), vec![]);
                }
                1 => {
                    o.put(k, b"overlay");
                    reference.insert(k.clone(), b"overlay".to_vec());
                }
                _ => {}
            }
        }
    }
    for (k, v) in gen_entries(&mut rng, 1000, &[33, 65]) {
        o.put(&k, &v);
        reference.insert(k, v);
    }
    o.put(b"never existed", b"");
    reference.insert(b"never existed".to_vec(), vec![]);
    let want: Vec<Entry> = reference.iter().filter(|(_, v)| !v.is_empty()).map(|(k, v)| (k.clone(), v.clone())).collect();

    let v = View::new(Some(&o), &[&r1, &r0]);
    for (k, rv) in &reference {
        let got = v.get(k);
        assert_eq!(got.is_some(), !rv.is_empty(), "View.get {:x?}", k);
        if let Some(g) = got {
            assert_eq!(g, rv.as_slice());
        }
    }
    assert!(v.get(b"absent").is_none());
    assert_eq!(collect(&mut v.iter(None, None)), want, "View.iter");
    let mut user = [0u8; 32];
    user[..3].copy_from_slice(&[1, 2, 3]);
    let m = merge(&tmp("m.run"), &v, user).unwrap();
    assert_eq!(m.len(), want.len());
    assert_eq!(m.user_data(), user);
    assert_eq!(collect(&mut m.iter(None, None)), want, "merged");
    for (k, rv) in &reference {
        let got = m.get(k);
        assert_eq!(got.is_some(), !rv.is_empty(), "merged get {:x?}", k);
        if let Some(g) = got {
            assert_eq!(g, rv.as_slice());
        }
    }
    assert_eq!(collect(&mut View::new(None, &[&m]).iter(None, None)), want, "no overlay");
}

#[test]
fn overlay() {
    let mut o = Overlay::new();
    o.put(b"k", b"v1");
    o.put(b"k", b"v22");
    o.put(b"d", b"");
    assert_eq!(o.len(), 2);
    assert_eq!(o.bytes(), (1 + 3 + ENTRY_OVERHEAD) + (1 + ENTRY_OVERHEAD));
    assert_eq!(o.get(b"k"), Some(&b"v22"[..]));
    assert_eq!(o.get(b"d"), Some(&b""[..]));
    assert!(o.get(b"x").is_none());
    // A value outgrowing its slack relocates; the old slot stays dead.
    o.put(b"k", &[7u8; 100]);
    assert_eq!(o.get(b"k"), Some(&[7u8; 100][..]));
    assert_eq!(o.len(), 2);
    assert_eq!(collect(&mut o.iter(None, None)), vec![(b"d".to_vec(), vec![]), (b"k".to_vec(), vec![7u8; 100])]);
}

/// The state engine's keys: account = hash + 0x00, slot = hash + 0x01 +
/// hash. One contract has enough slots to span many blocks; another set
/// shares 60 bytes so the index prefixes tie and the full-key search is
/// exercised.
#[test]
fn key_shapes() {
    let mut rng = Rng::new(5);
    let h = |rng: &mut Rng| state::keccak::keccak256(&rng.bytes(32)).to_vec();
    let mut es: Vec<Entry> = vec![];
    let mut big_addr = vec![];
    for c in 0..300 {
        let addr = h(&mut rng);
        es.push(([addr.clone(), vec![0]].concat(), b"account".to_vec()));
        let mut slots = 1 + rng.below(20);
        if c == 0 {
            slots = 20000;
            big_addr = addr.clone();
        }
        for _ in 0..slots {
            let k = [addr.clone(), vec![1], h(&mut rng)].concat();
            let v = k[40..44].to_vec();
            es.push((k, v));
        }
    }
    let common = h(&mut rng);
    for _ in 0..5000 {
        let k = [common.clone(), vec![1], common[..27].to_vec(), h(&mut rng)[..5].to_vec()].concat();
        let v = k[62..].to_vec();
        es.push((k, v));
    }
    es.sort();
    es.dedup_by(|a, b| a.0 == b.0);
    let r = write_run(&tmp("k.run"), &es, [0; 32]);
    for (k, v) in &es {
        assert!(k.len() == 33 || k.len() == 65);
        assert_eq!(r.get(k), Some(v.as_slice()));
        for d in [0xffu8, 0x01] {
            let mut k2 = k.clone();
            *k2.last_mut().unwrap() ^= d;
            if es.binary_search_by(|e| e.0.cmp(&k2)).is_err() {
                assert!(r.get(&k2).is_none(), "Get absent {:x?}", k2);
            }
        }
    }
    assert_eq!(collect(&mut r.iter(None, None)), es);
    let lo = [big_addr.clone(), vec![1]].concat();
    let hi = [big_addr.clone(), vec![2]].concat();
    let got = collect(&mut r.iter(Some(&lo), Some(&hi)));
    let want: Vec<Entry> = es.iter().filter(|e| e.0.len() == 65 && e.0[..32] == big_addr[..]).cloned().collect();
    assert_eq!(got, want, "contract range");
}

/// Format details: every block starts with shared = 0 and its whole first
/// key, no entry straddles a block, the index holds each block's hi and lo
/// prefixes, the footer counts match. Keys are not 33 bytes here so stored
/// values are the given bytes.
#[test]
fn block_layout() {
    let mut rng = Rng::new(6);
    let es = gen_entries(&mut rng, 3000, &[8, 65, 255]);
    let path = tmp("l.run");
    let r = write_run(&path, &es, [9; 32]);
    let raw = std::fs::read(&path).unwrap();
    let nblk = r.blocks();
    let ft = &raw[raw.len() - FOOTER_LEN..];
    let u64at = |o: usize| u64::from_le_bytes(ft[o..o + 8].try_into().unwrap()) as usize;
    assert_eq!(&ft[..8], b"epochrun");
    assert_eq!(u32::from_le_bytes(ft[8..12].try_into().unwrap()), 2);
    assert_eq!(u32::from_le_bytes(ft[12..16].try_into().unwrap()), BLOCK_SIZE as u32);
    assert_eq!(u64at(16), es.len());
    assert_eq!(u64at(24), nblk);
    let (doff, ioff, nimp) = (u64at(32), u64at(40), u64at(48));
    assert_eq!(doff, nblk * BLOCK_SIZE);
    let ndict = raw[doff] as usize;
    assert_eq!(ndict, r.dictionary().len());
    assert!(ioff >= doff + 8 && ioff % 8 == 0);
    assert_eq!(nimp, 0);
    assert_eq!(raw.len(), ioff + 16 * nblk + FOOTER_LEN);
    assert_eq!(&ft[56..88], &[9u8; 32]);
    let mut n = 0;
    let mut prev: Vec<u8> = vec![];
    for b in 0..nblk {
        let blk = &raw[b * BLOCK_SIZE..(b + 1) * BLOCK_SIZE];
        let used = u16::from_le_bytes([blk[0], blk[1]]) as usize;
        assert!(used <= BLOCK_SIZE && used > 2);
        assert_eq!(blk[2], 0, "block {b} first entry shares bytes");
        assert!(blk[used..].iter().all(|&x| x == 0), "block {b} padding");
        let first = &blk[5..5 + blk[3] as usize];
        let pad8 = |k: &[u8], from: usize| {
            let mut p = [0u8; 8];
            if k.len() > from {
                let m = (k.len() - from).min(8);
                p[..m].copy_from_slice(&k[from..from + m]);
            }
            u64::from_be_bytes(p)
        };
        let idx = |o: usize| u64::from_le_bytes(raw[o..o + 8].try_into().unwrap());
        assert_eq!(idx(ioff + 8 * b), pad8(first, 0), "hi of block {b}");
        assert_eq!(idx(ioff + 8 * nblk + 8 * b), pad8(first, 33), "lo of block {b}");
        let mut i = 2;
        while i < used {
            let (sh, un, code) = (blk[i] as usize, blk[i + 1] as usize, blk[i + 2]);
            let vl = if code >= 128 { 0 } else { code as usize };
            assert!(i + 3 + un + vl <= used, "entry straddles block {b}");
            prev.truncate(sh);
            prev.extend_from_slice(&blk[i + 3..i + 3 + un]);
            assert_eq!(prev, es[n].0);
            let v: &[u8] = if code >= 128 { &r.dictionary()[(code - 128) as usize] } else { &blk[i + 3 + un..i + 3 + un + vl] };
            assert_eq!(v, es[n].1.as_slice());
            i += 3 + un + vl;
            n += 1;
        }
    }
    assert_eq!(n, es.len());
}

fn acct(nonce: u64, bal: u64, code: &[u8; 32]) -> Vec<u8> {
    let be = |v: u64| v.to_be_bytes()[(v.leading_zeros() / 8) as usize..].to_vec();
    contract_row(&be(nonce), &be(bal), code)
}

/// Account values are packed in the file and come back as the RLP given;
/// non-RLP values under 33-byte keys and non-canonical RLP survive too.
#[test]
fn packed_accounts() {
    let mut rng = Rng::new(7);
    let mut es: Vec<Entry> = vec![];
    for i in 0..2000u64 {
        let k = [rng.bytes(32), vec![0]].concat();
        let v = match i % 5 {
            0 => acct(rng.below(300) as u64, rng.u64(), &EMPTY_CODE_HASH),
            1 => acct(0, 0, &EMPTY_CODE_HASH),
            2 => acct(u64::MAX, rng.u64(), &rng.bytes(32).try_into().unwrap()),
            3 => b"account".to_vec(),
            _ => vec![0xc3, 0x80, 0x80, 0x80], // RLP with a non-minimal nonce: not canonical
        };
        es.push((k, v));
    }
    es.sort();
    let path = tmp("p.run");
    let r = write_run(&path, &es, [0; 32]);
    for (k, v) in &es {
        assert_eq!(r.get(k), Some(v.as_slice()));
    }
    assert_eq!(collect(&mut r.iter(None, None)), es);
    // The file stores packed accounts: 2 header bytes + nonce + balance, no
    // empty code hash, no RLP headers; the 0-nonce 0-balance account is 2 bytes.
    let raw = std::fs::read(&path).unwrap();
    let blk = &raw[..BLOCK_SIZE];
    let (un, vl) = (blk[3] as usize, blk[4]);
    let stored = &blk[5 + un..5 + un + if vl >= 128 { 0 } else { vl as usize }];
    let stored = if vl >= 128 { r.dictionary()[(vl - 128) as usize].as_slice() } else { stored };
    let v0 = &es[0].1;
    if v0 == b"account" {
        assert_eq!(stored, [&[0xff][..], v0].concat());
    } else if v0.len() == 4 {
        assert_eq!(stored, [&[0xff][..], v0].concat());
    } else {
        assert!(stored.len() < v0.len(), "{:x?} stored as {:x?}", v0, stored);
        assert!(stored[0] < 0xc0);
    }
    assert!(r.bytes() < es.iter().map(|e| e.0.len() + e.1.len()).sum::<usize>(), "smaller than raw");
}

/// The dictionary: repeated values cost no bytes, and a value that is only
/// common past the census rows is still stored (longer) and read back.
#[test]
fn dictionary() {
    let mut rng = Rng::new(8);
    let n = CENSUS_ROWS + 20000;
    let mut es: Vec<Entry> = Vec::with_capacity(n);
    let common = acct(1, 5_000_000, &EMPTY_CODE_HASH);
    let mut keys: Vec<Vec<u8>> = (0..n).map(|i| if i % 4 == 0 { [rng.bytes(32), vec![0]].concat() } else { [rng.bytes(32), vec![1], rng.bytes(32)].concat() }).collect();
    keys.sort();
    keys.dedup();
    for (i, k) in keys.into_iter().enumerate() {
        let v = if k.len() == 33 {
            common.clone()
        } else {
            match i % 3 {
                0 => vec![1],
                1 => vec![(i % 7) as u8 + 1],
                _ => rng.bytes(20),
            }
        };
        es.push((k, v));
    }
    es.sort();
    let path = tmp("d.run");
    let r = write_run(&path, &es, [0; 32]);
    assert!(!r.dictionary().is_empty() && r.dictionary().len() <= 127, "{} entries", r.dictionary().len());
    assert!(r.dictionary().iter().any(|v| v == &[1u8]), "the 1-byte slot value is in the dictionary");
    for (k, v) in &es {
        assert_eq!(r.get(k), Some(v.as_slice()));
    }
    assert_eq!(collect(&mut r.iter(None, None)), es);
    // Coded rows carry no value bytes: the first block has some.
    let raw = std::fs::read(&path).unwrap();
    let blk = &raw[..BLOCK_SIZE];
    let used = u16::from_le_bytes([blk[0], blk[1]]) as usize;
    let (mut i, mut coded, mut rows) = (2, 0, 0);
    while i < used {
        let (un, vl) = (blk[i + 1] as usize, blk[i + 2]);
        coded += (vl >= 128) as usize;
        rows += 1;
        i += 3 + un + if vl >= 128 { 0 } else { vl as usize };
    }
    assert!(coded * 2 > rows, "{coded} of {rows} rows in block 0 use the dictionary");
    // Values seen only after the census rows are not in the dictionary but
    // are stored and read back: check the tail explicitly.
    for (k, v) in &es[es.len() - 1000..] {
        assert_eq!(r.get(k), Some(v.as_slice()));
    }
}

/// LAYOUT.md's exact-tie rule: several consecutive blocks whose first keys
/// share both 8-byte index prefixes (one contract, slot hashes with the
/// same 8 leading bytes). Without the stored-first-key comparison the index
/// picks the last tied block for every key and misses the earlier ones.
#[test]
fn prefix_ties() {
    let mut rng = Rng::new(9);
    let contract = rng.bytes(32);
    let slot_pfx = rng.bytes(8);
    let mut es: Vec<Entry> = vec![];
    // ~60 rows per 2 KB block: 400 rows span 6 or 7 tied blocks.
    for _ in 0..400 {
        let k = [contract.clone(), vec![1], slot_pfx.clone(), rng.bytes(24)].concat();
        es.push((k, rng.bytes(8)));
    }
    // Neighbours before and after the tied group.
    for _ in 0..100 {
        let k = [rng.bytes(32), vec![0]].concat();
        es.push((k, acct(1, 1, &EMPTY_CODE_HASH)));
    }
    es.sort();
    es.dedup_by(|a, b| a.0 == b.0);
    let path = tmp("t.run");
    let r = write_run(&path, &es, [0; 32]);
    assert!(r.blocks() >= 6, "{} blocks", r.blocks());
    // Count the blocks whose (hi, lo) index entry equals the previous one.
    let raw = std::fs::read(&path).unwrap();
    let ft = &raw[raw.len() - FOOTER_LEN..];
    let ioff = u64::from_le_bytes(ft[40..48].try_into().unwrap()) as usize;
    let at = |o: usize| u64::from_le_bytes(raw[o..o + 8].try_into().unwrap());
    let nblk = r.blocks();
    let tied = (1..nblk).filter(|&b| at(ioff + 8 * b) == at(ioff + 8 * (b - 1)) && at(ioff + 8 * nblk + 8 * b) == at(ioff + 8 * nblk + 8 * (b - 1))).count();
    assert!(tied >= 5, "{tied} tied blocks");
    for (k, v) in &es {
        assert_eq!(r.get(k), Some(v.as_slice()), "{:x?}", k);
        for d in [1u8, 0xff] {
            let mut k2 = k.clone();
            *k2.last_mut().unwrap() ^= d;
            if es.binary_search_by(|e| e.0.cmp(&k2)).is_err() {
                assert!(r.get(&k2).is_none(), "absent {:x?}", k2);
            }
        }
    }
    // A key that sorts between two tied blocks' first keys but before the
    // whole group, and one after it.
    let lo = [contract.clone(), vec![1], slot_pfx.clone(), vec![0; 24]].concat();
    let hi = [contract.clone(), vec![1], slot_pfx.clone(), vec![0xff; 24]].concat();
    assert!(r.get(&lo).is_none() && r.get(&hi).is_none());
    let want: Vec<Entry> = es.iter().filter(|e| e.0.len() == 65).cloned().collect();
    assert_eq!(collect(&mut r.iter(Some(&lo), Some(&hi))), want);
    assert_eq!(collect(&mut r.iter(None, None)), es);
}

/// Two contracts whose hashes share the 8 leading bytes (an impure index
/// group): their slot prefixes interleave, so the group is searched on the
/// stored first keys. A third contract with the same prefix and only a few
/// slots sits inside another contract's block.
#[test]
fn impure_groups() {
    let mut rng = Rng::new(10);
    let pfx = rng.bytes(8);
    let mut cs: Vec<Vec<u8>> = (0..3).map(|_| [pfx.clone(), rng.bytes(24)].concat()).collect();
    cs.sort();
    let mut es: Vec<Entry> = vec![];
    for (i, c) in cs.iter().enumerate() {
        es.push(([c.clone(), vec![0]].concat(), acct(i as u64 + 1, 7, &EMPTY_CODE_HASH)));
        let slots = if i == 1 { 3 } else { 500 };
        for _ in 0..slots {
            es.push(([c.clone(), vec![1], rng.bytes(32)].concat(), rng.bytes(4)));
        }
    }
    // Other contracts around them.
    for _ in 0..50 {
        let c = rng.bytes(32);
        es.push(([c.clone(), vec![0]].concat(), acct(1, 1, &EMPTY_CODE_HASH)));
        for _ in 0..rng.below(40) {
            es.push(([c.clone(), vec![1], rng.bytes(32)].concat(), vec![1]));
        }
    }
    es.sort();
    es.dedup_by(|a, b| a.0 == b.0);
    let path = tmp("i.run");
    let r = write_run(&path, &es, [0; 32]);
    let raw = std::fs::read(&path).unwrap();
    let nimp = u64::from_le_bytes(raw[raw.len() - FOOTER_LEN + 48..raw.len() - FOOTER_LEN + 56].try_into().unwrap());
    assert_eq!(nimp, 1, "one impure group");
    for (k, v) in &es {
        assert_eq!(r.get(k), Some(v.as_slice()), "{:x?}", k);
        let mut k2 = k.clone();
        *k2.last_mut().unwrap() ^= 1;
        if es.binary_search_by(|e| e.0.cmp(&k2)).is_err() {
            assert!(r.get(&k2).is_none(), "absent {:x?}", k2);
        }
    }
    for c in &cs {
        let (lo, hi) = ([c.clone(), vec![1]].concat(), [c.clone(), vec![2]].concat());
        let want: Vec<Entry> = es.iter().filter(|e| e.0.len() == 65 && e.0[..32] == c[..]).cloned().collect();
        assert_eq!(collect(&mut r.iter(Some(&lo), Some(&hi))), want);
        assert!(r.get(&[c.clone(), vec![1], vec![0; 32]].concat()).is_none());
    }
    assert_eq!(collect(&mut r.iter(None, None)), es);
}

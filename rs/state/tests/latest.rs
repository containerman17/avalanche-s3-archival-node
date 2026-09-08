//! Port of latest/latest_test.go.
use state::overlay::{Overlay, ENTRY_OVERHEAD};
use state::run::{Run, Writer, FOOTER_LEN};
use state::sample::Rng;
use state::view::{merge, View};
use state::KvIter;
use std::collections::{BTreeMap, HashSet};
use std::path::Path;

type Entry = (Vec<u8>, Vec<u8>);

/// n distinct random keys (lengths drawn from klens) with random values
/// 0..255 bytes, one in ten empty, sorted.
fn gen_entries(rng: &mut Rng, n: usize, klens: &[usize]) -> Vec<Entry> {
    let mut seen = HashSet::new();
    let mut es = Vec::new();
    while es.len() < n {
        let kl = klens[rng.below(klens.len())];
        let k = rng.bytes(kl);
        if !seen.insert(k.clone()) {
            continue;
        }
        let vl = if rng.below(10) == 0 { 0 } else { rng.below(256) };
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
    assert!(w.add(b"c", &[0u8; 256]).is_err(), "256-byte value");
    w.add(&[b'c'; 255], &[0u8; 255]).unwrap();
    w.close().unwrap();
    let r = Run::open(&path).unwrap();
    assert_eq!(r.len(), 2);
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
        ("index offset", flip(n - FOOTER_LEN + 32)),
        ("user data", flip(n - FOOTER_LEN + 40)),
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
/// key, no entry straddles a block, the index holds each block's first 40
/// bytes, the footer counts match.
#[test]
fn block_layout() {
    let mut rng = Rng::new(6);
    let es = gen_entries(&mut rng, 3000, &[33, 65, 255]);
    let path = tmp("l.run");
    let r = write_run(&path, &es, [9; 32]);
    let raw = std::fs::read(&path).unwrap();
    let nblk = r.blocks();
    assert_eq!(raw.len(), nblk * 4096 + nblk * 40 + FOOTER_LEN);
    let mut n = 0;
    let mut prev: Vec<u8> = vec![];
    for b in 0..nblk {
        let blk = &raw[b * 4096..(b + 1) * 4096];
        let used = u16::from_le_bytes([blk[0], blk[1]]) as usize;
        assert!(used <= 4096 && used > 2);
        assert_eq!(blk[2], 0, "block {b} first entry shares bytes");
        assert!(blk[used..].iter().all(|&x| x == 0), "block {b} padding");
        let first = &blk[5..5 + blk[3] as usize];
        let mut p = [0u8; 40];
        let m = first.len().min(40);
        p[..m].copy_from_slice(&first[..m]);
        assert_eq!(&raw[nblk * 4096 + b * 40..nblk * 4096 + (b + 1) * 40], &p[..], "index prefix of block {b}");
        let mut i = 2;
        while i < used {
            let (sh, un, vl) = (blk[i] as usize, blk[i + 1] as usize, blk[i + 2] as usize);
            assert!(i + 3 + un + vl <= used, "entry straddles block {b}");
            prev.truncate(sh);
            prev.extend_from_slice(&blk[i + 3..i + 3 + un]);
            assert_eq!(prev, es[n].0);
            assert_eq!(&blk[i + 3 + un..i + 3 + un + vl], es[n].1.as_slice());
            i += 3 + un + vl;
            n += 1;
        }
    }
    assert_eq!(n, es.len());
    let ft = &raw[raw.len() - FOOTER_LEN..];
    assert_eq!(&ft[..8], b"epochrun");
    assert_eq!(u64::from_le_bytes(ft[16..24].try_into().unwrap()), es.len() as u64);
    assert_eq!(u64::from_le_bytes(ft[24..32].try_into().unwrap()), nblk as u64);
    assert_eq!(&ft[40..72], &[9u8; 32]);
}

//! Port of commit/commit_test.go: roll against re-roll, Dirty rounds against
//! a fresh roll of the mutated state, the file walk, the corrupt footer.
use state::commit::dirty::{Dirty, SeekFn};
use state::commit::file::File;
use state::commit::roll::{roll, Stats};
use state::commit::{compact_to_hex, parse_leaf};
use state::keccak::{keccak256, EMPTY_ROOT};
use state::sample::*;
use state::{rlp, Hash, RowsIter};
use std::path::PathBuf;
use std::sync::Arc;

fn tmp(name: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("rs-state-ctest-{}", std::process::id()));
    std::fs::create_dir_all(&d).unwrap();
    d.join(name)
}

fn seek_fn(rows: Arc<Vec<Row>>) -> Arc<SeekFn> {
    Arc::new(move |prefix: &[u8]| {
        let i = rows.partition_point(|r| r.0.as_slice() < prefix);
        rows.get(i).cloned()
    })
}

fn roll_tmp(name: &str, rows: &[Row]) -> (Hash, Stats, Arc<File>) {
    let path = tmp(name);
    let mut user = [0u8; 32];
    user[0] = 7;
    let (root, st) = roll(&mut RowsIter::new(rows), &path, user).unwrap();
    let f = File::open(&path).unwrap();
    assert_eq!(f.root(), root);
    assert_eq!(f.user_data(), user);
    (root, st, Arc::new(f))
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

#[test]
fn roll_empty_and_dirty_from_empty() {
    let (root, _, f) = roll_tmp("empty", &[]);
    assert_eq!(root, EMPTY_ROOT);
    let rows = Arc::new(vec![]);
    let mut d = Dirty::new(f, seek_fn(rows));
    let mut m = Model::new();
    let mut rng = Rng::new(2);
    let mut addr = [0u8; 20];
    rng.fill(&mut addr);
    let a = Acct { nonce: 1, bal: 5, ..Default::default() };
    d.apply(&acct_key(&addr), &acct_val(&a)).unwrap();
    m.insert(addr, a);
    let got = d.root().unwrap();
    let (want, _, _) = roll_tmp("one", &flatten(&m));
    assert_eq!(got, want);
}

fn run_dirty(seed: u64, kinds: &[usize], rounds: usize, ops: usize, workers: usize) {
    let mut rng = Rng::new(seed);
    let mut m = gen_state(&mut rng, 3000);
    let flat = Arc::new(flatten(&m));
    let (root, _, f) = roll_tmp(&format!("d{seed}"), &flat);
    let mut d = Dirty::new(f.clone(), seek_fn(flat.clone()));
    d.workers = workers;
    let mut prev = root;
    for round in 0..rounds {
        let ups = mutate(&mut m, &mut rng, kinds, ops);
        for (k, v) in &ups {
            d.apply(k, v).unwrap();
        }
        let got = d.root().unwrap();
        let (want, _, _) = roll_tmp(&format!("d{seed}r{round}"), &flatten(&m));
        assert_eq!(hex(&got), hex(&want), "seed {seed} round {round}");
        assert!(got != prev || ups.is_empty());
        prev = got;
    }
    // A fresh Dirty over the same file must not depend on retained state.
    let mut d2 = Dirty::new(f.clone(), seek_fn(flat));
    assert_eq!(d2.root().unwrap(), f.root());
}

#[test]
fn dirty_matches_reroll() {
    run_dirty(3, &[0, 1, 2, 3, 4, 5, 6], 6, 300, 4);
}

#[test]
fn dirty_op_kinds() {
    for kind in 0..=6 {
        run_dirty(100 + kind as u64, &[kind], 3, 40, 1);
    }
}

/// The cases the task names: a new key that splits a leaf, a delete that
/// collapses a branch, a storage root going to empty, and an account with a
/// single slot (leaf-as-root: the file has no index entry for it).
#[test]
fn dirty_edge_cases() {
    let mut rng = Rng::new(77);
    let mut m = Model::new();
    let mut addrs = vec![];
    for _ in 0..40 {
        let mut a = [0u8; 20];
        rng.fill(&mut a);
        addrs.push(a);
        m.insert(a, Acct { nonce: 1, bal: 10, ..Default::default() });
    }
    // Two slots: one branch under the storage root; one slot: a leaf root;
    // three slots: room for a collapse.
    let (two, one, three) = (addrs[0], addrs[1], addrs[2]);
    let (s1, s2, s3, s4) = (rng.word(), rng.word(), rng.word(), rng.word());
    for a in [two, one, three] {
        m.get_mut(&a).unwrap().code = Some(vec![1]);
    }
    m.get_mut(&two).unwrap().slots.extend([(s1, rng.word()), (s2, rng.word())]);
    m.get_mut(&one).unwrap().slots.insert(s3, rng.word());
    m.get_mut(&three).unwrap().slots.extend([(s1, rng.word()), (s2, rng.word()), (s4, rng.word())]);
    let flat = Arc::new(flatten(&m));
    let (_, _, f) = roll_tmp("edge", &flat);
    assert!(!f.storage_root(&keccak256(&one).try_into().unwrap()).1, "single slot root must not be indexed");
    assert!(f.storage_root(&keccak256(&two).try_into().unwrap()).1);
    let mut d = Dirty::new(f.clone(), seek_fn(flat.clone()));
    let check = |d: &mut Dirty, m: &Model, what: &str| {
        let got = d.root().unwrap();
        let (want, _, _) = roll_tmp(&format!("edge-{what}"), &flatten(m));
        assert_eq!(hex(&got), hex(&want), "{what}");
    };
    // 1. new slot splits the single-slot leaf root; new account splits an account leaf.
    let v = rng.word();
    m.get_mut(&one).unwrap().slots.insert(s4, v);
    d.apply(&slot_key(&one, &s4), &trim_word(&v)).unwrap();
    let mut na = [0u8; 20];
    rng.fill(&mut na);
    let a = Acct { nonce: 3, bal: 4, ..Default::default() };
    d.apply(&acct_key(&na), &acct_val(&a)).unwrap();
    m.insert(na, a);
    check(&mut d, &m, "split");
    // 2. delete collapses: three slots down to one (branch to leaf root), then to none (empty root).
    m.get_mut(&three).unwrap().slots.remove(&s1);
    d.apply(&slot_key(&three, &s1), &[]).unwrap();
    m.get_mut(&three).unwrap().slots.remove(&s2);
    d.apply(&slot_key(&three, &s2), &[]).unwrap();
    check(&mut d, &m, "collapse");
    m.get_mut(&three).unwrap().slots.remove(&s4);
    d.apply(&slot_key(&three, &s4), &[]).unwrap();
    check(&mut d, &m, "empty-root");
    // 3. the single-slot account loses its slot, and the two-slot one goes to one.
    m.get_mut(&one).unwrap().slots.clear();
    d.apply(&slot_key(&one, &s3), &[]).unwrap();
    d.apply(&slot_key(&one, &s4), &[]).unwrap();
    m.get_mut(&two).unwrap().slots.remove(&s1);
    d.apply(&slot_key(&two, &s1), &[]).unwrap();
    check(&mut d, &m, "to-empty-and-leaf");
    // 4. delete an account whose leaf collapses a branch in the account trie.
    m.remove(&na);
    d.apply(&acct_key(&na), &[]).unwrap();
    check(&mut d, &m, "acct-delete");
}

/// Walks every internal node: a child referenced by hash must be found at
/// its path with that hash, or be a leaf the overlay rebuilds from the flat
/// rows with that same hash.
#[test]
fn file_nodes() {
    let mut rng = Rng::new(4);
    let m = gen_state(&mut rng, 2000);
    let flat = Arc::new(flatten(&m));
    let (_, st, f) = roll_tmp("walk", &flat);
    let d = Dirty::new(f.clone(), seek_fn(flat.clone()));
    let mut internal = 0u64;
    let mut leaves = 0u64;
    fn walk(f: &File, d: &Dirty, owner: &Hash, path: &[u8], want: Hash, internal: &mut u64, leaves: &mut u64) {
        let Some(blob) = f.node(owner, path) else {
            *leaves += 1;
            let blob = d.leaf(owner, path).unwrap_or_else(|e| panic!("{}/{:x?}: {e}", hex(owner), path));
            assert_eq!(keccak256(&blob), want, "{}/{:x?}: rebuilt leaf hash mismatch", hex(owner), path);
            return;
        };
        *internal += 1;
        assert_eq!(keccak256(blob), want, "{}/{:x?}: node hash mismatch", hex(owner), path);
        let (content, _) = rlp::split_list(blob).unwrap();
        let n = rlp::count_values(content).unwrap();
        if n == 17 {
            let mut rest = content;
            for i in 0..16u8 {
                let (kind, item, r) = rlp::split(rest).unwrap();
                rest = r;
                if kind == rlp::Kind::Str && item.len() == 32 {
                    walk(f, d, owner, &[path, &[i]].concat(), item.try_into().unwrap(), internal, leaves);
                }
            }
            return;
        }
        let (key, rest) = rlp::split_string(content).unwrap();
        let (kind, item, _) = rlp::split(rest).unwrap();
        if kind == rlp::Kind::Str && item.len() == 32 {
            walk(f, d, owner, &[path, &compact_to_hex(key)[..]].concat(), item.try_into().unwrap(), internal, leaves);
        }
    }
    walk(&f, &d, &[0; 32], &[], f.root(), &mut internal, &mut leaves);
    for (addr, a) in &m {
        if a.slots.len() < 2 {
            continue;
        }
        let h: Hash = keccak256(addr);
        let (root, ok) = f.storage_root(&h);
        assert!(ok, "{}: no storage root", hex(&h));
        walk(&f, &d, &h, &[], root, &mut internal, &mut leaves);
    }
    assert_eq!(internal, st.nodes);
    assert_eq!(internal, f.node_count());
    assert!(leaves > 0);
    // Storage root of a one-slot account equals the hash of its leaf.
    for (addr, a) in &m {
        if a.slots.len() == 1 {
            let h: Hash = keccak256(addr);
            let (_, ok) = f.storage_root(&h);
            assert!(!ok);
            let leaf = d.leaf(&[0; 32], &state::commit::key_to_nibbles(&h)).unwrap();
            let (content, _) = rlp::split_list(&leaf).unwrap();
            let (_, rest) = rlp::split_string(content).unwrap();
            let (val, _) = rlp::split_string(rest).unwrap();
            let lf = parse_leaf(val).unwrap();
            let slot_leaf = d.leaf(&h, &[]).unwrap();
            assert_eq!(lf.root, keccak256(&slot_leaf));
            break;
        }
    }
}

#[test]
fn open_refuses_corrupt_footer() {
    let mut rng = Rng::new(5);
    let flat = flatten(&gen_state(&mut rng, 50));
    let path = tmp("corrupt");
    roll(&mut RowsIter::new(&flat), &path, [0; 32]).unwrap();
    let good = std::fs::read(&path).unwrap();
    let n = good.len();
    let cases: Vec<Vec<u8>> = vec![
        { let mut b = good.clone(); b[n - 40] ^= 1; b },  // user data
        { let mut b = good.clone(); b[n - 100] ^= 1; b }, // root offset
        good[..n - 1].to_vec(),                            // truncated
        { let mut b = good.clone(); b[0] ^= 1; b },       // header magic
    ];
    for (i, bad) in cases.iter().enumerate() {
        let p = tmp(&format!("corrupt.bad{i}"));
        std::fs::write(&p, bad).unwrap();
        assert!(File::open(&p).is_err(), "mutation {i}: opened");
    }
}

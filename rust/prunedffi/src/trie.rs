//! Arena Merkle Patricia trie (geth secure-trie semantics) with path copying.
//!
//! Nodes live in one `Vec<Node>` of fixed 144-byte POD records. A node is
//! never mutated once hashed, so a revision is just a root index into the
//! shared arena and old roots stay readable until their replaced nodes are
//! released. Building a proposal copies the path from the root to every
//! touched leaf; nodes created by the build in progress (`hlen == 0`) are
//! edited in place.
//!
//! Keys are keccak(address) for accounts and keccak(slot) for storage, 64
//! nibbles each, so branches never carry values. A leaf and an extension
//! store a full 32-byte sample key; the nibbles they cover are implied by the
//! depth at which they hang.

use rayon::prelude::*;
use tiny_keccak::{Hasher, Keccak};

pub const NONE: u32 = u32::MAX;
pub const FREE: u8 = 0;
pub const BRANCH: u8 = 1;
pub const EXT: u8 = 2;
pub const ACCOUNT: u8 = 3;
pub const SLOT: u8 = 4;

pub type Hash = [u8; 32];

/// keccak256(rlp("")) = keccak256(0x80), the root of an empty trie.
pub const EMPTY_ROOT: Hash = [
    0x56, 0xe8, 0x1f, 0x17, 0x1b, 0xcc, 0x55, 0xa6, 0xff, 0x83, 0x45, 0xe6, 0x92, 0xc0, 0xf8, 0x6e,
    0x5b, 0x48, 0xe0, 0x1b, 0x99, 0x6c, 0xad, 0xc0, 0x01, 0x62, 0x2f, 0xb5, 0xe3, 0x63, 0xb4, 0x21,
];

pub fn keccak(data: &[u8]) -> Hash {
    let mut k = Keccak::v256();
    k.update(data);
    let mut out = [0u8; 32];
    k.finalize(&mut out);
    out
}

/// Account row as the Go side sends it: nonce (8, big-endian), balance (32,
/// big-endian), code hash (32).
pub const ROW: usize = 72;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct Node {
    pub hash: Hash,
    /// 0: not hashed yet; 1..=31: `hash[..hlen]` holds the embedded RLP; 32: a hash.
    pub hlen: u8,
    pub kind: u8,
    /// Extension: nibble count. Slot: value length.
    pub len: u8,
    pad: u8,
    /// Extension: child. Account: storage root (NONE when empty).
    pub link: u32,
    /// Leaf key, or any key below an extension.
    pub key: Hash,
    /// Branch: 16 little-endian u32 children. Account: row. Slot: value bytes.
    pub data: [u8; ROW],
}

pub const NODE_SIZE: usize = 144;
const _: () = assert!(std::mem::size_of::<Node>() == NODE_SIZE);

impl Node {
    fn blank(kind: u8) -> Node {
        Node { hash: [0; 32], hlen: 0, kind, len: 0, pad: 0, link: NONE, key: [0; 32], data: [0; ROW] }
    }
    pub fn branch() -> Node {
        let mut n = Node::blank(BRANCH);
        n.data[..64].fill(0xff);
        n
    }
    pub fn ext(key: &Hash, len: usize, child: u32) -> Node {
        let mut n = Node::blank(EXT);
        n.key = *key;
        n.len = len as u8;
        n.link = child;
        n
    }
    pub fn account(key: &Hash, row: &[u8; ROW], storage: u32) -> Node {
        let mut n = Node::blank(ACCOUNT);
        n.key = *key;
        n.data = *row;
        n.link = storage;
        n
    }
    pub fn slot(key: &Hash, value: &[u8]) -> Node {
        let mut n = Node::blank(SLOT);
        n.key = *key;
        n.len = value.len() as u8;
        n.data[..value.len()].copy_from_slice(value);
        n
    }
    #[inline]
    pub fn child(&self, i: usize) -> u32 {
        u32::from_le_bytes(self.data[i * 4..i * 4 + 4].try_into().unwrap())
    }
    #[inline]
    pub fn set_child(&mut self, i: usize, c: u32) {
        self.data[i * 4..i * 4 + 4].copy_from_slice(&c.to_le_bytes());
    }
    pub fn is_leaf(&self) -> bool {
        self.kind == ACCOUNT || self.kind == SLOT
    }
    pub fn value(&self) -> &[u8] {
        &self.data[..self.len as usize]
    }
}

#[inline]
fn nib(key: &Hash, i: usize) -> u8 {
    (key[i >> 1] >> ((1 - (i & 1)) * 4)) & 15
}

fn set_nib(key: &mut Hash, i: usize, v: u8) {
    let shift = (1 - (i & 1)) * 4;
    key[i >> 1] = (key[i >> 1] & !(15 << shift)) | (v << shift);
}

/// Number of equal nibbles of a and b starting at depth, at most max.
fn common(a: &Hash, b: &Hash, depth: usize, max: usize) -> usize {
    let mut n = 0;
    while n < max && nib(a, depth + n) == nib(b, depth + n) {
        n += 1;
    }
    n
}

#[derive(Default)]
pub struct Arena {
    pub nodes: Vec<Node>,
    pub free: Vec<u32>,
}

impl Arena {
    pub fn alloc(&mut self, n: Node) -> u32 {
        if let Some(i) = self.free.pop() {
            self.nodes[i as usize] = n;
            return i;
        }
        self.nodes.push(n);
        (self.nodes.len() - 1) as u32
    }
    pub fn release(&mut self, i: u32) {
        let n = &mut self.nodes[i as usize];
        debug_assert!(n.kind != FREE, "double release of node {i}");
        n.kind = FREE;
        n.hlen = 0;
        self.free.push(i);
    }
    pub fn bytes(&self) -> usize {
        self.nodes.len() * NODE_SIZE
    }

    /// Finds the leaf for key under root.
    pub fn get(&self, mut n: u32, key: &Hash) -> Option<&Node> {
        let mut depth = 0;
        loop {
            if n == NONE {
                return None;
            }
            let node = &self.nodes[n as usize];
            match node.kind {
                BRANCH => {
                    n = node.child(nib(key, depth) as usize);
                    depth += 1;
                }
                EXT => {
                    let len = node.len as usize;
                    if common(key, &node.key, depth, len) != len {
                        return None;
                    }
                    depth += len;
                    n = node.link;
                }
                _ => return if node.key == *key { Some(node) } else { None },
            }
        }
    }

    /// Hash of the subtree at n as a trie root (always a hash, never embedded).
    pub fn root_hash(&self, n: u32) -> Hash {
        if n == NONE {
            return EMPTY_ROOT;
        }
        let node = &self.nodes[n as usize];
        debug_assert!(node.hlen != 0, "root_hash on an unhashed node");
        if node.hlen == 32 {
            node.hash
        } else {
            keccak(&node.hash[..node.hlen as usize])
        }
    }

    /// Rebuilds the free list as every node not reachable from root.
    pub fn rebuild_free(&mut self, root: u32) {
        let mut live = vec![0u64; self.nodes.len().div_ceil(64)];
        let mut stack = Vec::new();
        if root != NONE {
            stack.push(root);
        }
        while let Some(n) = stack.pop() {
            live[n as usize / 64] |= 1 << (n % 64);
            let node = &self.nodes[n as usize];
            match node.kind {
                BRANCH => {
                    for i in 0..16 {
                        let c = node.child(i);
                        if c != NONE {
                            stack.push(c);
                        }
                    }
                }
                EXT => stack.push(node.link),
                ACCOUNT if node.link != NONE => stack.push(node.link),
                _ => {}
            }
        }
        self.free.clear();
        for i in (0..self.nodes.len()).rev() {
            if live[i / 64] & (1 << (i % 64)) == 0 {
                self.nodes[i].kind = FREE;
                self.nodes[i].hlen = 0;
                self.free.push(i as u32);
            }
        }
    }
}

/// One proposal under construction. `fresh` is every node it allocated,
/// `garbage` every node it made unreachable from its root (fresh nodes it
/// discarded again appear in both; a rejected proposal releases `fresh`, an
/// accepted one releases `garbage` when it leaves the retention window).
pub struct Build<'a> {
    pub arena: &'a mut Arena,
    pub fresh: Vec<u32>,
    pub garbage: Vec<u32>,
}

impl<'a> Build<'a> {
    pub fn new(arena: &'a mut Arena) -> Self {
        Build { arena, fresh: Vec::new(), garbage: Vec::new() }
    }

    fn node(&self, n: u32) -> Node {
        self.arena.nodes[n as usize]
    }
    fn is_fresh(&self, n: u32) -> bool {
        self.arena.nodes[n as usize].hlen == 0
    }
    fn alloc(&mut self, n: Node) -> u32 {
        let i = self.arena.alloc(n);
        self.fresh.push(i);
        i
    }
    fn discard(&mut self, n: u32) {
        self.garbage.push(n);
    }
    /// Discards n and every node below it.
    pub fn discard_tree(&mut self, n: u32) {
        let mut stack = vec![n];
        while let Some(n) = stack.pop() {
            if n == NONE {
                continue;
            }
            self.garbage.push(n);
            let node = self.node(n);
            match node.kind {
                BRANCH => stack.extend((0..16).map(|i| node.child(i)).filter(|&c| c != NONE)),
                EXT => stack.push(node.link),
                ACCOUNT => stack.push(node.link),
                _ => {}
            }
        }
    }
    /// A node that must change: edited in place when fresh, else replaced.
    fn own(&mut self, n: u32, mut node: Node) -> u32 {
        node.hlen = 0;
        if self.is_fresh(n) {
            self.arena.nodes[n as usize] = node;
            return n;
        }
        self.discard(n);
        self.alloc(node)
    }

    /// Inserts or replaces leaf (whose key is leaf.key) under n at depth.
    pub fn put(&mut self, n: u32, depth: usize, leaf: Node) -> u32 {
        if n == NONE {
            return self.alloc(leaf);
        }
        let key = leaf.key;
        let node = self.node(n);
        match node.kind {
            BRANCH => {
                let i = nib(&key, depth) as usize;
                let c = self.put(node.child(i), depth + 1, leaf);
                let mut nn = node;
                nn.set_child(i, c);
                self.own(n, nn)
            }
            EXT => {
                let plen = node.len as usize;
                let cp = common(&key, &node.key, depth, plen);
                if cp == plen {
                    let c = self.put(node.link, depth + plen, leaf);
                    let mut nn = node;
                    nn.link = c;
                    return self.own(n, nn);
                }
                let rest = plen - cp - 1;
                let tail = if rest == 0 {
                    node.link
                } else {
                    self.alloc(Node::ext(&node.key, rest, node.link))
                };
                let mut br = Node::branch();
                br.set_child(nib(&node.key, depth + cp) as usize, tail);
                let l = self.alloc(leaf);
                br.set_child(nib(&key, depth + cp) as usize, l);
                self.discard(n);
                let b = self.alloc(br);
                if cp > 0 {
                    self.alloc(Node::ext(&key, cp, b))
                } else {
                    b
                }
            }
            _ => {
                if node.key == key {
                    return self.own(n, leaf);
                }
                let cp = common(&key, &node.key, depth, 64 - depth);
                // The old leaf moves deeper, which changes its encoding.
                let old = self.own(n, node);
                let mut br = Node::branch();
                br.set_child(nib(&node.key, depth + cp) as usize, old);
                let l = self.alloc(leaf);
                br.set_child(nib(&key, depth + cp) as usize, l);
                let b = self.alloc(br);
                if cp > 0 {
                    self.alloc(Node::ext(&key, cp, b))
                } else {
                    b
                }
            }
        }
    }

    /// Removes key under n at depth. None when the key is absent.
    pub fn del(&mut self, n: u32, key: &Hash, depth: usize) -> Option<u32> {
        if n == NONE {
            return None;
        }
        let node = self.node(n);
        match node.kind {
            BRANCH => {
                let i = nib(key, depth) as usize;
                let c = self.del(node.child(i), key, depth + 1)?;
                let mut nn = node;
                nn.set_child(i, c);
                let kids: Vec<usize> = (0..16).filter(|&j| nn.child(j) != NONE).collect();
                if kids.len() >= 2 {
                    return Some(self.own(n, nn));
                }
                // One child left: the branch collapses into it.
                self.discard(n);
                let j = kids[0];
                let only = nn.child(j);
                let on = self.node(only);
                Some(match on.kind {
                    BRANCH => {
                        let mut k = *key;
                        set_nib(&mut k, depth, j as u8);
                        self.alloc(Node::ext(&k, 1, only))
                    }
                    EXT => self.own(only, Node::ext(&on.key, 1 + on.len as usize, on.link)),
                    _ => self.own(only, on),
                })
            }
            EXT => {
                let plen = node.len as usize;
                if common(key, &node.key, depth, plen) != plen {
                    return None;
                }
                let c = self.del(node.link, key, depth + plen)?;
                let cn = self.node(c);
                self.discard(n);
                Some(match cn.kind {
                    BRANCH => self.alloc(Node::ext(&node.key, plen, c)),
                    EXT => self.own(c, Node::ext(&cn.key, plen + cn.len as usize, cn.link)),
                    _ => self.own(c, cn),
                })
            }
            _ => {
                if node.key != *key {
                    return None;
                }
                if node.kind == ACCOUNT {
                    self.discard_tree(node.link);
                }
                self.discard(n);
                Some(NONE)
            }
        }
    }

    /// Hashes every unhashed node reachable from root.
    pub fn hash(&mut self, root: u32) {
        if root != NONE {
            hash_node(P(self.arena.nodes.as_mut_ptr()), root, 0);
        }
    }
}

#[derive(Clone, Copy)]
struct P(*mut Node);
unsafe impl Send for P {}
unsafe impl Sync for P {}

/// Fixed RLP scratch buffer: a branch is at most 16 * 33 + 1 payload bytes.
struct Buf {
    b: [u8; 640],
    n: usize,
}

impl Buf {
    fn new() -> Buf {
        Buf { b: [0; 640], n: 0 }
    }
    fn as_slice(&self) -> &[u8] {
        &self.b[..self.n]
    }
    fn push(&mut self, x: u8) {
        self.b[self.n] = x;
        self.n += 1;
    }
    fn extend(&mut self, s: &[u8]) {
        self.b[self.n..self.n + s.len()].copy_from_slice(s);
        self.n += s.len();
    }
    fn header(&mut self, base: u8, len: usize) {
        if len < 56 {
            self.push(base + len as u8);
        } else if len < 256 {
            self.push(base + 56);
            self.push(len as u8);
        } else {
            self.push(base + 57);
            self.push((len >> 8) as u8);
            self.push(len as u8);
        }
    }
    fn put_str(&mut self, s: &[u8]) {
        if s.len() == 1 && s[0] < 0x80 {
            self.push(s[0]);
        } else {
            self.header(0x80, s.len());
            self.extend(s);
        }
    }
    fn put_uint(&mut self, be: &[u8]) {
        let start = be.iter().position(|&b| b != 0).unwrap_or(be.len());
        self.put_str(&be[start..]);
    }
    fn put_ref(&mut self, child: &Node) {
        if child.hlen == 32 {
            self.push(0xa0);
            self.extend(&child.hash);
        } else {
            self.extend(&child.hash[..child.hlen as usize]);
        }
    }
    fn list(payload: &Buf) -> Buf {
        let mut out = Buf::new();
        out.header(0xc0, payload.n);
        out.extend(payload.as_slice());
        out
    }
}

/// Hex-prefix encoding of key nibbles [depth, depth+len).
fn hp(key: &Hash, depth: usize, len: usize, term: bool) -> ([u8; 33], usize) {
    let mut out = [0u8; 33];
    let flag = if term { 0x20 } else { 0 };
    let mut o = 0;
    let mut i = depth;
    if len % 2 == 1 {
        out[0] = 0x10 | flag | nib(key, i);
        i += 1;
    } else {
        out[0] = flag;
    }
    o += 1;
    while i < depth + len {
        out[o] = nib(key, i) << 4 | nib(key, i + 1);
        o += 1;
        i += 2;
    }
    (out, o)
}

fn account_rlp(row: &[u8; ROW], storage_root: &Hash) -> Buf {
    let mut v = Buf::new();
    v.put_uint(&row[..8]);
    v.put_uint(&row[8..40]);
    v.put_str(storage_root);
    v.put_str(&row[40..72]);
    Buf::list(&v)
}

fn hash_node(p: P, n: u32, depth: usize) {
    // SAFETY: an unhashed node has exactly one unhashed parent, so each
    // node is written by one task, and children are finished (joined) before
    // the parent reads their hash. Hashed nodes are never written.
    let node = unsafe { &mut *p.0.add(n as usize) };
    if node.hlen != 0 {
        return;
    }
    let at = |c: u32| unsafe { &*p.0.add(c as usize) };
    let mut payload = Buf::new();
    match node.kind {
        BRANCH => {
            let mut todo = [0u32; 16];
            let mut k = 0;
            for i in 0..16 {
                let c = node.child(i);
                if c != NONE && at(c).hlen == 0 {
                    todo[k] = c;
                    k += 1;
                }
            }
            if k > 1 && depth < 3 {
                todo[..k].par_iter().for_each(|&c| hash_node(p, c, depth + 1));
            } else {
                for &c in &todo[..k] {
                    hash_node(p, c, depth + 1);
                }
            }
            for i in 0..16 {
                let c = node.child(i);
                if c == NONE {
                    payload.push(0x80);
                } else {
                    payload.put_ref(at(c));
                }
            }
            payload.push(0x80);
        }
        EXT => {
            let len = node.len as usize;
            hash_node(p, node.link, depth + len);
            let (path, plen) = hp(&node.key, depth, len, false);
            payload.put_str(&path[..plen]);
            payload.put_ref(at(node.link));
        }
        ACCOUNT => {
            let root = if node.link == NONE {
                EMPTY_ROOT
            } else {
                hash_node(p, node.link, 0);
                let s = at(node.link);
                if s.hlen == 32 {
                    s.hash
                } else {
                    keccak(&s.hash[..s.hlen as usize])
                }
            };
            let (path, plen) = hp(&node.key, depth, 64 - depth, true);
            payload.put_str(&path[..plen]);
            payload.put_str(account_rlp(&node.data, &root).as_slice());
        }
        SLOT => {
            let (path, plen) = hp(&node.key, depth, 64 - depth, true);
            payload.put_str(&path[..plen]);
            let mut v = Buf::new();
            v.put_str(node.value());
            payload.put_str(v.as_slice());
        }
        _ => unreachable!("hashing a free node"),
    }
    let rlp = Buf::list(&payload);
    if rlp.n < 32 {
        node.hash[..rlp.n].copy_from_slice(rlp.as_slice());
        node.hlen = rlp.n as u8;
    } else {
        node.hash = keccak(rlp.as_slice());
        node.hlen = 32;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn h(n: u64) -> Hash {
        let mut k = [0u8; 32];
        k[24..].copy_from_slice(&n.to_be_bytes());
        k
    }
    fn row(balance: u64) -> [u8; ROW] {
        let mut r = [0u8; ROW];
        r[32..40].copy_from_slice(&balance.to_be_bytes());
        r[40..72].copy_from_slice(&keccak(&[]));
        r
    }
    fn hex(s: &str) -> Hash {
        let mut out = [0u8; 32];
        for i in 0..32 {
            out[i] = u8::from_str_radix(&s[2 * i..2 * i + 2], 16).unwrap();
        }
        out
    }

    #[test]
    fn empty_root_constant() {
        assert_eq!(keccak(&[0x80]), EMPTY_ROOT);
    }

    // Expected roots come from libevm's trie.New over the same keys, with
    // account values RLP[nonce, balance, storageRoot, keccak("")] and slot
    // values RLP(bytes).
    #[test]
    fn reference_roots() {
        let mut a = Arena::default();
        let mut b = Build::new(&mut a);
        let r1 = b.put(NONE, 0, Node::account(&h(1), &row(10), NONE));
        b.hash(r1);
        assert_eq!(a.root_hash(r1), hex(VEC_ONE_ACCOUNT));

        let mut b = Build::new(&mut a);
        let mut s = NONE;
        for (k, v) in [(1u64, &[9u8][..]), (2, &[1, 2, 3][..]), (300, &[0xff; 32][..])] {
            s = b.put(s, 0, Node::slot(&h(k), v));
        }
        let mut root = r1;
        for n in 1..=5u64 {
            let storage = if n == 1 { s } else { NONE };
            root = b.put(root, 0, Node::account(&h(n), &row(n * 100), storage));
        }
        b.hash(root);
        assert_eq!(a.root_hash(root), hex(VEC_FIVE_ACCOUNTS_STORAGE));
        // Path copying keeps the old root intact.
        assert_eq!(a.root_hash(r1), hex(VEC_ONE_ACCOUNT));

        let mut b = Build::new(&mut a);
        let root2 = b.del(root, &h(3), 0).unwrap();
        let root2 = b.del(root2, &h(4), 0).unwrap();
        b.hash(root2);
        assert!(b.del(root2, &h(3), 0).is_none());
        assert_eq!(a.root_hash(root2), hex(VEC_AFTER_DELETES));
    }

    #[test]
    fn delete_everything_is_empty_and_order_independent() {
        let mut a = Arena::default();
        let mut b = Build::new(&mut a);
        let keys: Vec<Hash> = (0..500u64).map(|i| keccak(&i.to_be_bytes())).collect();
        let mut root = NONE;
        for k in &keys {
            root = b.put(root, 0, Node::slot(k, &k[..5]));
        }
        b.hash(root);
        let forward = a.root_hash(root);
        let mut b = Build::new(&mut a);
        let mut rev = NONE;
        for k in keys.iter().rev() {
            rev = b.put(rev, 0, Node::slot(k, &k[..5]));
        }
        b.hash(rev);
        assert_eq!(a.root_hash(rev), forward);
        let mut b = Build::new(&mut a);
        let mut r = root;
        for (i, k) in keys.iter().enumerate() {
            r = b.del(r, k, 0).unwrap();
            if i % 97 == 0 {
                b.hash(r);
            }
        }
        assert_eq!(r, NONE);
        for k in &keys {
            assert!(a.get(root, k).is_some());
        }
    }

    #[test]
    fn hashes_match_incremental_and_fresh_builds() {
        let mut a = Arena::default();
        let keys: Vec<Hash> = (0..300u64).map(|i| keccak(&i.to_le_bytes())).collect();
        let mut b = Build::new(&mut a);
        let mut root = NONE;
        for (i, k) in keys.iter().enumerate() {
            root = b.put(root, 0, Node::slot(k, &[(i % 200) as u8 + 1]));
            if i % 50 == 49 {
                b.hash(root);
            }
        }
        let mut r2 = root;
        for k in &keys[100..200] {
            r2 = b.del(r2, k, 0).unwrap();
        }
        b.hash(r2);
        let incremental = a.root_hash(r2);
        let mut fresh = Arena::default();
        let mut b = Build::new(&mut fresh);
        let mut r = NONE;
        for (i, k) in keys.iter().enumerate() {
            if !(100..200).contains(&i) {
                r = b.put(r, 0, Node::slot(k, &[(i % 200) as u8 + 1]));
            }
        }
        b.hash(r);
        assert_eq!(fresh.root_hash(r), incremental);
    }

    #[test]
    fn rebuild_free_reclaims_unreachable() {
        let mut a = Arena::default();
        let mut b = Build::new(&mut a);
        let r1 = b.put(NONE, 0, Node::account(&h(1), &row(1), NONE));
        b.hash(r1);
        let mut b = Build::new(&mut a);
        let r2 = b.put(r1, 0, Node::account(&h(1), &row(2), NONE));
        b.hash(r2);
        a.rebuild_free(r2);
        assert_eq!(a.free.len(), 1);
        assert_eq!(a.nodes[r1 as usize].kind, FREE);
        assert_eq!(a.get(r2, &h(1)).unwrap().data, row(2));
    }

    const VEC_ONE_ACCOUNT: &str = "a01de97711c1387f1f3b5fab69cb6e0ed7ed269eb2839915574917904dcbdaae";
    const VEC_FIVE_ACCOUNTS_STORAGE: &str = "fe9b59ea2e7d98a1e7a12254c4bd3602d27e6d07d6a95ceb1840f85a7b1a0fdf";
    const VEC_AFTER_DELETES: &str = "2b782a5ebb28c04123c82a517b13740241c416b39c423e96f86ca98bdcf592b2";
}

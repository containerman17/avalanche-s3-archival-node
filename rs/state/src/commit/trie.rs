//! A mutable Merkle Patricia trie with geth's node semantics, resolved
//! lazily by path from a `NodeReader`, committed into a set of (path, blob)
//! for every dirty node that is stored separately (>= 32 bytes, or the
//! root). Smaller nodes are embedded in their parent's RLP.

use super::{compact_to_hex, err, key_to_nibbles, put_compact, Error, Result};
use crate::keccak::{keccak256, EMPTY_ROOT};
use crate::rlp;
use crate::Hash;
use std::borrow::Cow;

pub trait NodeReader: Sync {
    /// The RLP blob of the node at path (nibbles) in owner's trie.
    fn node(&self, owner: &Hash, path: &[u8]) -> Result<Cow<'_, [u8]>>;
}

/// hash: known for a node read from the reader and unchanged since;
/// dirty: changed this round, must be stored on commit.
#[derive(Clone, Copy, Default)]
struct Flags {
    hash: Option<Hash>,
    dirty: bool,
}

const DIRTY: Flags = Flags { hash: None, dirty: true };
const CLEAN: Flags = Flags { hash: None, dirty: false };

enum Node {
    Empty,
    Hash(Hash),
    Leaf { key: Vec<u8>, val: Vec<u8>, flags: Flags },
    Ext { key: Vec<u8>, child: Box<Node>, flags: Flags },
    Branch { kids: Box<[Node; 16]>, flags: Flags },
}

fn empty16() -> Box<[Node; 16]> {
    Box::new(std::array::from_fn(|_| Node::Empty))
}

/// The committed nodes of one trie: (path, blob) per stored dirty node.
pub struct NodeSet {
    pub owner: Hash,
    pub nodes: Vec<(Vec<u8>, Vec<u8>)>,
}

pub struct Trie<'a> {
    root: Node,
    cx: Cx<'a>,
    prefix: Vec<u8>,
}

struct Cx<'a> {
    owner: Hash,
    reader: &'a dyn NodeReader,
}

impl<'a> Trie<'a> {
    pub fn new(reader: &'a dyn NodeReader, owner: Hash, root: Hash) -> Trie<'a> {
        let root = if root == EMPTY_ROOT { Node::Empty } else { Node::Hash(root) };
        Trie { root, cx: Cx { owner, reader }, prefix: Vec::with_capacity(64) }
    }

    /// The value stored at key (bytes), resolving nodes on the way.
    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let nib = key_to_nibbles(key);
        let root = std::mem::replace(&mut self.root, Node::Empty);
        self.prefix.clear();
        let (v, root) = get(&self.cx, root, &mut self.prefix, &nib)?;
        self.root = root;
        Ok(v)
    }

    pub fn update(&mut self, key: &[u8], value: Vec<u8>) -> Result<()> {
        if value.is_empty() {
            return self.delete(key);
        }
        let nib = key_to_nibbles(key);
        let root = std::mem::replace(&mut self.root, Node::Empty);
        self.prefix.clear();
        self.root = insert(&self.cx, root, &mut self.prefix, &nib, value)?;
        Ok(())
    }

    pub fn delete(&mut self, key: &[u8]) -> Result<()> {
        let nib = key_to_nibbles(key);
        let root = std::mem::replace(&mut self.root, Node::Empty);
        self.prefix.clear();
        self.root = delete(&self.cx, root, &mut self.prefix, &nib)?.1;
        Ok(())
    }

    /// Applies ops (key bytes, value; an empty value deletes) and commits,
    /// the 16 subtries under the root branch spread over `workers` threads
    /// (the update walk, then the hashing). Falls back to the serial path
    /// when the root is not a branch, or when the deletes leave it with
    /// fewer than two children and it must collapse.
    pub fn commit_par(mut self, ops: &[(Vec<u8>, Vec<u8>)], workers: usize) -> Result<(Hash, NodeSet)> {
        let root = std::mem::replace(&mut self.root, Node::Empty);
        let root = match root {
            Node::Hash(h) => resolve(&self.cx, &[], h)?,
            n => n,
        };
        let (mut kids, root_hash) = match root {
            Node::Branch { kids, flags } if workers > 1 => (kids, flags.hash),
            n => {
                self.root = n;
                for (k, v) in ops {
                    self.update(k, v.clone())?;
                }
                return Ok(self.commit());
            }
        };
        let mut groups: [Vec<(Vec<u8>, &[u8])>; 16] = std::array::from_fn(|_| Vec::new());
        for (k, v) in ops {
            let nib = key_to_nibbles(k);
            groups[nib[0] as usize].push((nib, v));
        }
        struct Slot {
            kid: Node,
            changed: bool,
            deleted: bool,
            out: Option<(Enc, NodeSet)>,
        }
        let slots: Vec<std::sync::Mutex<Slot>> = kids.iter_mut().map(|k| std::sync::Mutex::new(Slot { kid: std::mem::replace(k, Node::Empty), changed: false, deleted: false, out: None })).collect();
        let n = workers.min(16);
        let next = std::sync::atomic::AtomicUsize::new(0);
        let commit_next = std::sync::atomic::AtomicUsize::new(0);
        let barrier = std::sync::Barrier::new(n);
        let par_commit = std::sync::atomic::AtomicBool::new(false);
        let errors: std::sync::Mutex<Vec<Error>> = std::sync::Mutex::new(Vec::new());
        let cx = &self.cx;
        let owner = self.cx.owner;
        std::thread::scope(|s| {
            for _ in 0..n {
                s.spawn(|| {
                    let mut prefix = Vec::with_capacity(64);
                    loop {
                        let i = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        if i >= 16 {
                            break;
                        }
                        let mut slot = slots[i].lock().unwrap();
                        let mut kid = std::mem::replace(&mut slot.kid, Node::Empty);
                        for (nib, v) in &groups[i] {
                            prefix.clear();
                            prefix.push(i as u8);
                            let r = if v.is_empty() {
                                delete(cx, kid, &mut prefix, &nib[1..]).map(|(d, n)| {
                                    slot.deleted |= d;
                                    slot.changed |= d;
                                    n
                                })
                            } else {
                                slot.changed = true;
                                insert(cx, kid, &mut prefix, &nib[1..], v.to_vec())
                            };
                            match r {
                                Ok(n) => kid = n,
                                Err(e) => {
                                    errors.lock().unwrap().push(e);
                                    kid = Node::Empty;
                                    break;
                                }
                            }
                        }
                        slot.kid = kid;
                    }
                    if barrier.wait().is_leader() {
                        let alive = slots.iter().filter(|s| !matches!(s.lock().unwrap().kid, Node::Empty)).count();
                        par_commit.store(alive >= 2 && errors.lock().unwrap().is_empty(), std::sync::atomic::Ordering::Relaxed);
                    }
                    barrier.wait();
                    if !par_commit.load(std::sync::atomic::Ordering::Relaxed) {
                        return;
                    }
                    loop {
                        let i = commit_next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        if i >= 16 {
                            break;
                        }
                        let mut slot = slots[i].lock().unwrap();
                        let kid = std::mem::replace(&mut slot.kid, Node::Empty);
                        let mut set = NodeSet { owner, nodes: Vec::new() };
                        prefix.clear();
                        prefix.push(i as u8);
                        let e = commit(kid, &mut prefix, &mut set);
                        slot.out = Some((e, set));
                    }
                });
            }
        });
        if let Some(e) = errors.into_inner().unwrap().pop() {
            return Err(e);
        }
        let mut slots: Vec<Slot> = slots.into_iter().map(|m| m.into_inner().unwrap()).collect();
        let changed = slots.iter().any(|s| s.changed);
        if !par_commit.into_inner() {
            let deleted = slots.iter().any(|s| s.deleted);
            for (k, s) in kids.iter_mut().zip(slots.iter_mut()) {
                *k = std::mem::replace(&mut s.kid, Node::Empty);
            }
            self.root = if deleted {
                reduce(&self.cx, kids, &mut Vec::new())?
            } else {
                Node::Branch { kids, flags: if changed { DIRTY } else { Flags { hash: root_hash, dirty: false } } }
            };
            return Ok(self.commit());
        }
        let mut set = NodeSet { owner, nodes: Vec::new() };
        let mut body = Vec::with_capacity(17 * 33);
        for s in slots.iter_mut() {
            let (e, sub) = s.out.take().unwrap();
            put_enc(&mut body, &e);
            set.nodes.extend(sub.nodes);
        }
        body.push(0x80);
        let root = match (changed, root_hash) {
            (false, Some(h)) => h,
            _ => match finish(list(&body), true, &[], &mut set) {
                Enc::Hash(h) => h,
                _ => unreachable!("the root is always hashed"),
            },
        };
        Ok((root, set))
    }

    /// Hashes the trie and collects every stored dirty node.
    pub fn commit(mut self) -> (Hash, NodeSet) {
        let mut set = NodeSet { owner: self.cx.owner, nodes: Vec::new() };
        self.prefix.clear();
        let root = match commit(self.root, &mut self.prefix, &mut set) {
            Enc::Empty => EMPTY_ROOT,
            Enc::Hash(h) => h,
            Enc::Raw(_) => unreachable!("the root is always hashed"),
        };
        (root, set)
    }
}

fn resolve(cx: &Cx, prefix: &[u8], hash: Hash) -> Result<Node> {
    let blob = cx.reader.node(&cx.owner, prefix)?;
    if blob.is_empty() {
        return err(format!("commit: missing trie node {} at {:x?}", hex(&hash), prefix));
    }
    debug_assert_eq!(keccak256(&blob), hash, "node hash mismatch at {:x?}", prefix);
    let mut n = decode(&blob).ok_or_else(|| super::Error(format!("commit: bad node RLP at {:x?} owner {} blob {}", prefix, hex(&cx.owner), hex(&blob))))?;
    match &mut n {
        Node::Leaf { flags, .. } | Node::Ext { flags, .. } | Node::Branch { flags, .. } => flags.hash = Some(hash),
        _ => {}
    }
    Ok(n)
}

/// Decodes a node RLP; embedded children are decoded inline.
fn decode(blob: &[u8]) -> Option<Node> {
    let (content, _) = rlp::split_list(blob)?;
    let n = rlp::count_values(content)?;
    if n == 17 {
        let mut kids = empty16();
        let mut rest = content;
        for kid in kids.iter_mut() {
            let (k, item, r) = rlp::split(rest)?;
            let raw = &rest[..rest.len() - r.len()];
            *kid = decode_ref(k, item, raw)?;
            rest = r;
        }
        return Some(Node::Branch { kids, flags: CLEAN });
    }
    if n != 2 {
        return None;
    }
    let (compact, rest) = rlp::split_string(content)?;
    if compact.is_empty() {
        return None;
    }
    let key = compact_to_hex(compact);
    if compact[0] & 0x20 != 0 {
        let (val, _) = rlp::split_string(rest)?;
        return Some(Node::Leaf { key, val: val.to_vec(), flags: CLEAN });
    }
    let (k, item, r) = rlp::split(rest)?;
    let raw = &rest[..rest.len() - r.len()];
    Some(Node::Ext { key, child: Box::new(decode_ref(k, item, raw)?), flags: CLEAN })
}

fn decode_ref(kind: rlp::Kind, item: &[u8], raw: &[u8]) -> Option<Node> {
    match kind {
        rlp::Kind::List => decode(raw),
        rlp::Kind::Str if item.is_empty() => Some(Node::Empty),
        rlp::Kind::Str if item.len() == 32 => Some(Node::Hash(item.try_into().unwrap())),
        _ => None,
    }
}

fn common_prefix(a: &[u8], b: &[u8]) -> usize {
    let n = a.len().min(b.len());
    let mut i = 0;
    while i < n && a[i] == b[i] {
        i += 1;
    }
    i
}

fn get(cx: &Cx, n: Node, prefix: &mut Vec<u8>, key: &[u8]) -> Result<(Option<Vec<u8>>, Node)> {
    match n {
        Node::Empty => Ok((None, Node::Empty)),
        Node::Hash(h) => {
            let r = resolve(cx, prefix, h)?;
            get(cx, r, prefix, key)
        }
        Node::Leaf { key: lk, val, flags } => {
            let v = if lk == key { Some(val.clone()) } else { None };
            Ok((v, Node::Leaf { key: lk, val, flags }))
        }
        Node::Ext { key: ek, child, flags } => {
            if key.len() < ek.len() || key[..ek.len()] != ek[..] {
                return Ok((None, Node::Ext { key: ek, child, flags }));
            }
            let plen = prefix.len();
            prefix.extend_from_slice(&ek);
            let (v, c) = get(cx, *child, prefix, &key[ek.len()..])?;
            prefix.truncate(plen);
            Ok((v, Node::Ext { key: ek, child: Box::new(c), flags }))
        }
        Node::Branch { mut kids, flags } => {
            if key.is_empty() {
                return Ok((None, Node::Branch { kids, flags }));
            }
            let i = key[0] as usize;
            let c = std::mem::replace(&mut kids[i], Node::Empty);
            prefix.push(key[0]);
            let (v, c) = get(cx, c, prefix, &key[1..])?;
            prefix.pop();
            kids[i] = c;
            Ok((v, Node::Branch { kids, flags }))
        }
    }
}

fn insert(cx: &Cx, n: Node, prefix: &mut Vec<u8>, key: &[u8], value: Vec<u8>) -> Result<Node> {
    match n {
        Node::Empty => Ok(Node::Leaf { key: key.to_vec(), val: value, flags: DIRTY }),
        Node::Hash(h) => {
            let r = resolve(cx, prefix, h)?;
            insert(cx, r, prefix, key, value)
        }
        Node::Leaf { key: lk, val: lv, .. } => {
            let m = common_prefix(&lk, key);
            if m == lk.len() && m == key.len() {
                return Ok(Node::Leaf { key: lk, val: value, flags: DIRTY });
            }
            if m == lk.len() || m == key.len() {
                return err("commit: keys of different lengths in one trie");
            }
            let mut kids = empty16();
            kids[lk[m] as usize] = Node::Leaf { key: lk[m + 1..].to_vec(), val: lv, flags: DIRTY };
            kids[key[m] as usize] = Node::Leaf { key: key[m + 1..].to_vec(), val: value, flags: DIRTY };
            let branch = Node::Branch { kids, flags: DIRTY };
            Ok(if m == 0 { branch } else { Node::Ext { key: key[..m].to_vec(), child: Box::new(branch), flags: DIRTY } })
        }
        Node::Ext { key: ek, child, .. } => {
            let m = common_prefix(&ek, key);
            if m == ek.len() {
                let plen = prefix.len();
                prefix.extend_from_slice(&key[..m]);
                let c = insert(cx, *child, prefix, &key[m..], value)?;
                prefix.truncate(plen);
                return Ok(Node::Ext { key: ek, child: Box::new(c), flags: DIRTY });
            }
            let mut kids = empty16();
            kids[ek[m] as usize] = if m + 1 == ek.len() { *child } else { Node::Ext { key: ek[m + 1..].to_vec(), child, flags: DIRTY } };
            kids[key[m] as usize] = Node::Leaf { key: key[m + 1..].to_vec(), val: value, flags: DIRTY };
            let branch = Node::Branch { kids, flags: DIRTY };
            Ok(if m == 0 { branch } else { Node::Ext { key: key[..m].to_vec(), child: Box::new(branch), flags: DIRTY } })
        }
        Node::Branch { mut kids, .. } => {
            if key.is_empty() {
                return err("commit: key ends at a branch");
            }
            let i = key[0] as usize;
            let c = std::mem::replace(&mut kids[i], Node::Empty);
            prefix.push(key[0]);
            kids[i] = insert(cx, c, prefix, &key[1..], value)?;
            prefix.pop();
            Ok(Node::Branch { kids, flags: DIRTY })
        }
    }
}

fn concat(a: &[u8], b: &[u8]) -> Vec<u8> {
    let mut v = Vec::with_capacity(a.len() + b.len());
    v.extend_from_slice(a);
    v.extend_from_slice(b);
    v
}

fn delete(cx: &Cx, n: Node, prefix: &mut Vec<u8>, key: &[u8]) -> Result<(bool, Node)> {
    match n {
        Node::Empty => Ok((false, Node::Empty)),
        Node::Hash(h) => {
            let r = resolve(cx, prefix, h)?;
            delete(cx, r, prefix, key)
        }
        Node::Leaf { key: lk, val, flags } => {
            if lk == key {
                Ok((true, Node::Empty))
            } else {
                Ok((false, Node::Leaf { key: lk, val, flags }))
            }
        }
        Node::Ext { key: ek, child, flags } => {
            let m = common_prefix(&ek, key);
            if m < ek.len() {
                return Ok((false, Node::Ext { key: ek, child, flags }));
            }
            let plen = prefix.len();
            prefix.extend_from_slice(&ek);
            let (d, c) = delete(cx, *child, prefix, &key[m..])?;
            prefix.truncate(plen);
            if !d {
                return Ok((false, Node::Ext { key: ek, child: Box::new(c), flags }));
            }
            Ok((
                true,
                match c {
                    Node::Leaf { key: ck, val, .. } => Node::Leaf { key: concat(&ek, &ck), val, flags: DIRTY },
                    Node::Ext { key: ck, child: cc, .. } => Node::Ext { key: concat(&ek, &ck), child: cc, flags: DIRTY },
                    other => Node::Ext { key: ek, child: Box::new(other), flags: DIRTY },
                },
            ))
        }
        Node::Branch { mut kids, flags } => {
            if key.is_empty() {
                return Ok((false, Node::Branch { kids, flags }));
            }
            let i = key[0] as usize;
            let c = std::mem::replace(&mut kids[i], Node::Empty);
            prefix.push(key[0]);
            let (d, nn) = delete(cx, c, prefix, &key[1..])?;
            prefix.pop();
            kids[i] = nn;
            if !d {
                return Ok((false, Node::Branch { kids, flags }));
            }
            if !matches!(kids[i], Node::Empty) {
                return Ok((true, Node::Branch { kids, flags: DIRTY }));
            }
            Ok((true, reduce(cx, kids, prefix)?))
        }
    }
}

/// The dirty branch of kids, or what it collapses to when at most one child
/// is left: a leaf or extension absorbing the child's nibble, or Empty.
fn reduce(cx: &Cx, mut kids: Box<[Node; 16]>, prefix: &mut Vec<u8>) -> Result<Node> {
    let mut pos = None;
    let mut count = 0;
    for (j, k) in kids.iter().enumerate() {
        if !matches!(k, Node::Empty) {
            count += 1;
            pos = Some(j);
        }
    }
    if count >= 2 {
        return Ok(Node::Branch { kids, flags: DIRTY });
    }
    let Some(pos) = pos else { return Ok(Node::Empty) };
    let mut child = std::mem::replace(&mut kids[pos], Node::Empty);
    if let Node::Hash(h) = child {
        prefix.push(pos as u8);
        child = resolve(cx, prefix, h)?;
        prefix.pop();
    }
    Ok(match child {
        Node::Leaf { key: ck, val, .. } => Node::Leaf { key: concat(&[pos as u8], &ck), val, flags: DIRTY },
        Node::Ext { key: ck, child: cc, .. } => Node::Ext { key: concat(&[pos as u8], &ck), child: cc, flags: DIRTY },
        other => Node::Ext { key: vec![pos as u8], child: Box::new(other), flags: DIRTY },
    })
}

enum Enc {
    Empty,
    Hash(Hash),
    Raw(Vec<u8>),
}

fn put_enc(out: &mut Vec<u8>, e: &Enc) {
    match e {
        Enc::Empty => out.push(0x80),
        Enc::Hash(h) => {
            out.push(0xa0);
            out.extend_from_slice(h);
        }
        Enc::Raw(r) => out.extend_from_slice(r),
    }
}

fn list(body: &[u8]) -> Vec<u8> {
    let mut blob = Vec::with_capacity(body.len() + 3);
    rlp::put_header(&mut blob, true, body.len());
    blob.extend_from_slice(body);
    blob
}

fn commit(n: Node, prefix: &mut Vec<u8>, set: &mut NodeSet) -> Enc {
    match n {
        Node::Empty => Enc::Empty,
        Node::Hash(h) => Enc::Hash(h),
        Node::Leaf { key, val, flags } => {
            if let Some(h) = flags.hash {
                return Enc::Hash(h);
            }
            let mut body = Vec::with_capacity(key.len() / 2 + val.len() + 6);
            let mut ck = Vec::with_capacity(key.len() / 2 + 1);
            put_compact(&mut ck, &key, true);
            rlp::put_bytes(&mut body, &ck);
            rlp::put_bytes(&mut body, &val);
            finish(list(&body), flags.dirty, prefix, set)
        }
        Node::Ext { key, child, flags } => {
            if let Some(h) = flags.hash {
                return Enc::Hash(h);
            }
            let plen = prefix.len();
            prefix.extend_from_slice(&key);
            let c = commit(*child, prefix, set);
            prefix.truncate(plen);
            let mut body = Vec::with_capacity(key.len() / 2 + 40);
            let mut ck = Vec::with_capacity(key.len() / 2 + 1);
            put_compact(&mut ck, &key, false);
            rlp::put_bytes(&mut body, &ck);
            put_enc(&mut body, &c);
            finish(list(&body), flags.dirty, prefix, set)
        }
        Node::Branch { kids, flags } => {
            if let Some(h) = flags.hash {
                return Enc::Hash(h);
            }
            let mut body = Vec::with_capacity(17 * 33);
            for (i, kid) in kids.into_iter().enumerate() {
                prefix.push(i as u8);
                let e = commit(kid, prefix, set);
                prefix.pop();
                put_enc(&mut body, &e);
            }
            body.push(0x80);
            finish(list(&body), flags.dirty, prefix, set)
        }
    }
}

fn finish(blob: Vec<u8>, dirty: bool, prefix: &[u8], set: &mut NodeSet) -> Enc {
    if blob.len() < 32 && !prefix.is_empty() {
        return Enc::Raw(blob);
    }
    let h = keccak256(&blob);
    if dirty {
        set.nodes.push((prefix.to_vec(), blob));
    }
    Enc::Hash(h)
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

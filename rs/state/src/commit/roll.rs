//! Roll: builds every storage trie and the account trie from a sorted flat
//! state in one pass with a stack trie (the port of geth's StackTrie, so the
//! nodes come out in the same post-order) and writes the internal nodes.

use super::{account_leaf, err, put_compact, put_uvarint, Result, FOOTER_SIZE, INDEX_ENTRY, MAGIC, TAG_BRANCH, TAG_EXT, VERSION};
use crate::keccak::{keccak256, EMPTY_ROOT};
use crate::rlp;
use crate::{Hash, KvIter};
use std::io::{BufWriter, Write};
use std::path::Path;

#[derive(Clone, Copy, Debug, Default)]
pub struct Stats {
    /// Leaves fed to the tries (one keccak each).
    pub keys: u64,
    /// Internal nodes written (one keccak each).
    pub nodes: u64,
    /// File size.
    pub bytes: u64,
}

/// Where the node writer puts a hashed node: the file, or nowhere (a
/// standalone hash, as `Dirty` uses for a single-slot storage root).
pub trait NodeSink {
    /// Called for every hashed node (>= 32 bytes, or the root) with its
    /// type, its children's file offsets (16 for a branch, 1 for an
    /// extension, none for a leaf) and its RLP. Returns the file offset the
    /// parent records, 0 for leaves and unwritten nodes.
    fn emit(&mut self, path_len: usize, typ: Typ, child_offs: &[u64], blob: &[u8]) -> u64;
}

pub struct NoSink;
impl NodeSink for NoSink {
    fn emit(&mut self, _: usize, _: Typ, _: &[u64], _: &[u8]) -> u64 {
        0
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Typ {
    Empty,
    Branch,
    Ext,
    Leaf,
    Hashed,
}

/// A stack trie node: an arena slot. `val` is the leaf value, then after
/// hashing the 32-byte hash or the < 32 byte embedded encoding; `off` is
/// the file offset of a hashed internal node.
pub struct StNode {
    pub typ: Typ,
    pub key: Vec<u8>,
    pub val: Vec<u8>,
    pub children: [u32; 16],
    pub off: u64,
}

impl StNode {
    fn reset(&mut self) {
        self.typ = Typ::Empty;
        self.key.clear();
        self.val.clear();
        self.children = [0; 16];
        self.off = 0;
    }
}

/// Streaming trie hasher over ascending keys; slot 0 of the arena is the
/// "no child" sentinel.
pub struct StackTrie {
    nodes: Vec<StNode>,
    free: Vec<u32>,
    root: u32,
    last: Option<Vec<u8>>,
    path: Vec<u8>,
    enc: Vec<u8>,
}

impl Default for StackTrie {
    fn default() -> Self {
        Self::new()
    }
}

impl StackTrie {
    pub fn new() -> StackTrie {
        let mut t = StackTrie { nodes: Vec::with_capacity(256), free: vec![], root: 0, last: None, path: Vec::with_capacity(80), enc: Vec::with_capacity(600) };
        t.alloc(); // sentinel 0
        t.root = t.alloc();
        t
    }

    pub fn reset(&mut self) {
        for n in &mut self.nodes {
            n.reset();
        }
        self.free.clear();
        self.free.extend((2..self.nodes.len() as u32).rev());
        self.root = 1;
        self.last = None;
    }

    fn alloc(&mut self) -> u32 {
        if let Some(i) = self.free.pop() {
            return i;
        }
        self.nodes.push(StNode { typ: Typ::Empty, key: Vec::new(), val: Vec::new(), children: [0; 16], off: 0 });
        (self.nodes.len() - 1) as u32
    }

    fn release(&mut self, i: u32) {
        self.nodes[i as usize].reset();
        self.free.push(i);
    }

    fn new_leaf(&mut self, key: &[u8], val: &[u8]) -> u32 {
        let i = self.alloc();
        let n = &mut self.nodes[i as usize];
        n.typ = Typ::Leaf;
        n.key.extend_from_slice(key);
        n.val.extend_from_slice(val);
        i
    }

    /// Inserts key (bytes) with a non-empty value; keys must ascend.
    pub fn update(&mut self, key: &[u8], value: &[u8], sink: &mut dyn NodeSink) -> Result<()> {
        if value.is_empty() {
            return err("trying to insert empty (deletion)");
        }
        let k = super::key_to_nibbles(key);
        if let Some(last) = &self.last {
            if last >= &k {
                return err("non-ascending key order");
            }
        }
        self.last = Some(k.clone());
        self.path.clear();
        let root = self.root;
        self.insert(root, &k, value, 0, sink);
        Ok(())
    }

    fn insert(&mut self, st: u32, key: &[u8], value: &[u8], plen: usize, sink: &mut dyn NodeSink) {
        self.path.truncate(plen);
        match self.nodes[st as usize].typ {
            Typ::Branch => {
                let idx = key[0] as usize;
                // Unresolve elder siblings.
                for i in (0..idx).rev() {
                    let c = self.nodes[st as usize].children[i];
                    if c != 0 {
                        if self.nodes[c as usize].typ != Typ::Hashed {
                            self.path.push(i as u8);
                            self.hash(c, plen + 1, sink);
                            self.path.truncate(plen);
                        }
                        break;
                    }
                }
                let c = self.nodes[st as usize].children[idx];
                if c == 0 {
                    let leaf = self.new_leaf(&key[1..], value);
                    self.nodes[st as usize].children[idx] = leaf;
                } else {
                    self.path.push(key[0]);
                    self.insert(c, &key[1..], value, plen + 1, sink);
                }
            }
            Typ::Ext => {
                let diff = diff_index(&self.nodes[st as usize].key, key);
                let klen = self.nodes[st as usize].key.len();
                if diff == klen {
                    self.path.extend_from_slice(&key[..diff]);
                    let c = self.nodes[st as usize].children[0];
                    self.insert(c, &key[diff..], value, plen + diff, sink);
                    return;
                }
                let n;
                if diff < klen - 1 {
                    // Break on a non-last nibble: an intermediate extension.
                    let child = self.nodes[st as usize].children[0];
                    n = self.alloc();
                    let tail = self.nodes[st as usize].key[diff + 1..].to_vec();
                    let nn = &mut self.nodes[n as usize];
                    nn.typ = Typ::Ext;
                    nn.key.extend_from_slice(&tail);
                    nn.children[0] = child;
                    let p: Vec<u8> = self.nodes[st as usize].key[..diff + 1].to_vec();
                    self.path.extend_from_slice(&p);
                    self.hash(n, plen + diff + 1, sink);
                    self.path.truncate(plen);
                } else {
                    n = self.nodes[st as usize].children[0];
                    let p: Vec<u8> = self.nodes[st as usize].key.clone();
                    self.path.extend_from_slice(&p);
                    self.hash(n, plen + klen, sink);
                    self.path.truncate(plen);
                }
                let p;
                if diff == 0 {
                    self.nodes[st as usize].children[0] = 0;
                    self.nodes[st as usize].typ = Typ::Branch;
                    p = st;
                } else {
                    p = self.alloc();
                    self.nodes[p as usize].typ = Typ::Branch;
                    self.nodes[st as usize].children[0] = p;
                }
                let o = self.new_leaf(&key[diff + 1..], value);
                let orig_idx = self.nodes[st as usize].key[diff] as usize;
                let new_idx = key[diff] as usize;
                self.nodes[p as usize].children[orig_idx] = n;
                self.nodes[p as usize].children[new_idx] = o;
                self.nodes[st as usize].key.truncate(diff);
            }
            Typ::Leaf => {
                let diff = diff_index(&self.nodes[st as usize].key, key);
                assert!(diff < self.nodes[st as usize].key.len(), "trying to insert into existing key");
                let p;
                if diff == 0 {
                    self.nodes[st as usize].typ = Typ::Branch;
                    p = st;
                } else {
                    self.nodes[st as usize].typ = Typ::Ext;
                    p = self.alloc();
                    self.nodes[p as usize].typ = Typ::Branch;
                    self.nodes[st as usize].children[0] = p;
                }
                let orig_idx = self.nodes[st as usize].key[diff] as usize;
                let (okey, oval) = {
                    let n = &self.nodes[st as usize];
                    (n.key[diff + 1..].to_vec(), std::mem::take(&mut self.nodes[st as usize].val))
                };
                let orig = self.new_leaf(&okey, &oval);
                self.nodes[p as usize].children[orig_idx] = orig;
                let pre: Vec<u8> = self.nodes[st as usize].key[..diff + 1].to_vec();
                self.path.extend_from_slice(&pre);
                self.hash(orig, plen + diff + 1, sink);
                self.path.truncate(plen);
                let new_idx = key[diff] as usize;
                let leaf = self.new_leaf(&key[diff + 1..], value);
                self.nodes[p as usize].children[new_idx] = leaf;
                self.nodes[st as usize].key.truncate(diff);
            }
            Typ::Empty => {
                let n = &mut self.nodes[st as usize];
                n.typ = Typ::Leaf;
                n.key.extend_from_slice(key);
                n.val.extend_from_slice(value);
            }
            Typ::Hashed => panic!("trying to insert into hash"),
        }
    }

    /// Converts st into a hashed node: val holds the 32-byte hash, or the
    /// < 32 byte encoding of a non-root node. self.path[..plen] is st's path.
    fn hash(&mut self, st: u32, plen: usize, sink: &mut dyn NodeSink) {
        self.path.truncate(plen);
        match self.nodes[st as usize].typ {
            Typ::Hashed => return,
            Typ::Empty => {
                let n = &mut self.nodes[st as usize];
                n.val.clear();
                n.val.extend_from_slice(&EMPTY_ROOT);
                n.key.clear();
                n.typ = Typ::Hashed;
                return;
            }
            Typ::Branch => {
                for i in 0..16 {
                    let c = self.nodes[st as usize].children[i];
                    if c == 0 {
                        continue;
                    }
                    self.path.push(i as u8);
                    self.hash(c, plen + 1, sink);
                    self.path.truncate(plen);
                }
                let mut body = std::mem::take(&mut self.enc);
                body.clear();
                for i in 0..16 {
                    let c = self.nodes[st as usize].children[i];
                    if c == 0 {
                        body.push(0x80);
                    } else {
                        put_child(&mut body, &self.nodes[c as usize].val);
                    }
                }
                body.push(0x80);
                let blob = finish_list(&body);
                self.enc = body;
                let mut offs = [0u64; 16];
                for i in 0..16 {
                    let c = self.nodes[st as usize].children[i];
                    if c != 0 {
                        offs[i] = self.nodes[c as usize].off;
                    }
                }
                self.finish(st, plen, Typ::Branch, &offs, blob, sink);
                for i in 0..16 {
                    let c = self.nodes[st as usize].children[i];
                    if c != 0 {
                        self.nodes[st as usize].children[i] = 0;
                        self.release(c);
                    }
                }
            }
            Typ::Ext => {
                let c = self.nodes[st as usize].children[0];
                let k: Vec<u8> = self.nodes[st as usize].key.clone();
                self.path.extend_from_slice(&k);
                self.hash(c, plen + k.len(), sink);
                self.path.truncate(plen);
                let mut body = std::mem::take(&mut self.enc);
                body.clear();
                let mut ck = Vec::with_capacity(k.len() / 2 + 1);
                put_compact(&mut ck, &k, false);
                rlp::put_bytes(&mut body, &ck);
                put_child(&mut body, &self.nodes[c as usize].val);
                let blob = finish_list(&body);
                self.enc = body;
                let offs = [self.nodes[c as usize].off];
                self.finish(st, plen, Typ::Ext, &offs, blob, sink);
                self.nodes[st as usize].children[0] = 0;
                self.release(c);
            }
            Typ::Leaf => {
                let mut body = std::mem::take(&mut self.enc);
                body.clear();
                let mut ck = Vec::with_capacity(33);
                put_compact(&mut ck, &self.nodes[st as usize].key, true);
                rlp::put_bytes(&mut body, &ck);
                rlp::put_bytes(&mut body, &self.nodes[st as usize].val);
                let blob = finish_list(&body);
                self.enc = body;
                self.finish(st, plen, Typ::Leaf, &[], blob, sink);
            }
        }
    }

    fn finish(&mut self, st: u32, plen: usize, typ: Typ, offs: &[u64], blob: Vec<u8>, sink: &mut dyn NodeSink) {
        let n = &mut self.nodes[st as usize];
        n.key.clear();
        if blob.len() < 32 && plen > 0 {
            n.typ = Typ::Hashed;
            n.val = blob;
            return;
        }
        let h = keccak256(&blob);
        let off = sink.emit(plen, typ, offs, &blob);
        let n = &mut self.nodes[st as usize];
        n.typ = Typ::Hashed;
        n.val.clear();
        n.val.extend_from_slice(&h);
        n.off = off;
    }

    /// Hashes what is left and returns the root hash.
    pub fn hash_root(&mut self, sink: &mut dyn NodeSink) -> Hash {
        self.path.clear();
        let root = self.root;
        self.hash(root, 0, sink);
        self.nodes[root as usize].val[..32].try_into().unwrap()
    }

    /// The file offset the root was written at (0 when it was a leaf).
    pub fn root_off(&self) -> u64 {
        self.nodes[self.root as usize].off
    }
}

#[inline]
fn diff_index(nkey: &[u8], key: &[u8]) -> usize {
    let mut i = 0;
    while i < nkey.len() && nkey[i] == key[i] {
        i += 1;
    }
    i
}

/// A child reference: the embedded encoding as is, or the hash as a string.
#[inline]
fn put_child(out: &mut Vec<u8>, val: &[u8]) {
    if val.len() < 32 {
        out.extend_from_slice(val);
    } else {
        out.push(0xa0);
        out.extend_from_slice(val);
    }
}

fn finish_list(body: &[u8]) -> Vec<u8> {
    let mut blob = Vec::with_capacity(body.len() + 3);
    rlp::put_header(&mut blob, true, body.len());
    blob.extend_from_slice(body);
    blob
}

/// Builds every storage trie and the account trie from `it` in one pass,
/// writes the internal nodes to path, and returns the state root. user_data
/// is stored verbatim in the footer.
pub fn roll(it: &mut dyn KvIter, path: &Path, user_data: [u8; 32]) -> Result<(Hash, Stats)> {
    let res = roll_inner(it, path, user_data);
    if res.is_err() {
        let _ = std::fs::remove_file(path);
    }
    res
}

/// The file writer: the node sink of both tries. Leaves are skipped, so
/// every offset recorded is one the parent references by hash.
struct Sink {
    w: BufWriter<std::fs::File>,
    off: u64,
    nodes: u64,
    buf: Vec<u8>,
}

impl NodeSink for Sink {
    fn emit(&mut self, _plen: usize, typ: Typ, child_offs: &[u64], blob: &[u8]) -> u64 {
        let tag = match typ {
            Typ::Branch => TAG_BRANCH,
            Typ::Ext => TAG_EXT,
            _ => return 0,
        };
        let off = self.off;
        self.buf.clear();
        self.buf.push(tag);
        put_uvarint(&mut self.buf, blob.len() as u64);
        self.buf.extend_from_slice(blob);
        for &c in child_offs {
            put_uvarint(&mut self.buf, c);
        }
        self.w.write_all(&self.buf).expect("commit: write");
        self.off += self.buf.len() as u64;
        self.nodes += 1;
        off
    }
}

fn roll_inner(it: &mut dyn KvIter, path: &Path, user_data: [u8; 32]) -> Result<(Hash, Stats)> {
    let f = std::fs::File::create(path)?;
    let mut w = BufWriter::with_capacity(1 << 20, f.try_clone()?);
    w.write_all(MAGIC)?;
    let mut sink = Sink { w, off: MAGIC.len() as u64, nodes: 0, buf: Vec::with_capacity(640) };
    let mut acc = StackTrie::new();
    let mut st = StackTrie::new();
    let mut have_st = false;
    let mut keys = 0u64;
    let mut index: Vec<u8> = Vec::new();
    let mut cur: Option<(Hash, Vec<u8>)> = None; // account hash, contract row
    let mut val = Vec::with_capacity(40);

    // flush closes the current account: its storage root, then its leaf.
    let flush = |cur: &mut Option<(Hash, Vec<u8>)>, st: &mut StackTrie, have_st: &mut bool, acc: &mut StackTrie, sink: &mut Sink, index: &mut Vec<u8>, keys: &mut u64| -> Result<()> {
        let Some((hash, row)) = cur.take() else { return Ok(()) };
        let mut root = EMPTY_ROOT;
        if *have_st {
            root = st.hash_root(sink);
            let off = st.root_off();
            if off != 0 {
                index.extend_from_slice(&hash);
                index.extend_from_slice(&root);
                index.extend_from_slice(&off.to_le_bytes());
            }
            st.reset();
            *have_st = false;
        }
        let leaf = account_leaf(&row, &root)?;
        *keys += 1;
        acc.update(&hash, &leaf, sink)
    };
    while it.next() {
        let k = it.key();
        if k.len() == 33 && k[32] == 0 {
            flush(&mut cur, &mut st, &mut have_st, &mut acc, &mut sink, &mut index, &mut keys)?;
            let mut h = [0u8; 32];
            h.copy_from_slice(&k[..32]);
            cur = Some((h, it.value().to_vec()));
        } else if k.len() == 65 && k[32] == 1 {
            match &cur {
                Some((h, _)) if &h[..] == &k[..32] => {}
                _ => return err(format!("commit: slot row {} has no account row", hex(k))),
            }
            have_st = true;
            val.clear();
            rlp::put_bytes(&mut val, it.value());
            keys += 1;
            st.update(&k[33..], &val, &mut sink).map_err(|e| super::Error(format!("commit: slot {}: {e}", hex(k))))?;
        } else {
            return err(format!("commit: malformed key {}", hex(k)));
        }
    }
    flush(&mut cur, &mut st, &mut have_st, &mut acc, &mut sink, &mut index, &mut keys)?;
    let root = acc.hash_root(&mut sink);
    let root_off = acc.root_off();

    let idx_off = sink.off;
    let idx_n = (index.len() / INDEX_ENTRY) as u64;
    sink.w.write_all(&index)?;
    sink.off += index.len() as u64;
    let mut ft = Vec::with_capacity(FOOTER_SIZE);
    ft.extend_from_slice(MAGIC);
    ft.extend_from_slice(&VERSION.to_le_bytes());
    ft.extend_from_slice(&root_off.to_le_bytes());
    ft.extend_from_slice(&root);
    ft.extend_from_slice(&sink.nodes.to_le_bytes());
    ft.extend_from_slice(&keys.to_le_bytes());
    ft.extend_from_slice(&idx_off.to_le_bytes());
    ft.extend_from_slice(&idx_n.to_le_bytes());
    ft.extend_from_slice(&user_data);
    let crc = crc32fast::hash(&ft);
    ft.extend_from_slice(&crc.to_le_bytes());
    sink.w.write_all(&ft)?;
    sink.off += ft.len() as u64;
    sink.w.flush()?;
    f.sync_all()?;
    Ok((root, Stats { keys, nodes: sink.nodes, bytes: sink.off }))
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

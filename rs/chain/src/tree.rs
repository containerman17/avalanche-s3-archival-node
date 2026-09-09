//! The block tree behind the VM service: verified-but-not-accepted blocks
//! with their pending state, layered on the accepted head. Consensus verifies
//! siblings, accepts one and rejects the other; the engine only ever sees a
//! block executed on top of its parent's state.
//!
//! One VM call at a time (avalanchego holds ctx.Lock around every call), so
//! the whole tree sits behind one mutex.
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use bytes::Bytes;

pub type Id = [u8; 32];
pub type Error = Box<dyn std::error::Error + Send + Sync>;

/// What every block answers over rpcchainvm.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Meta {
    pub id: Id,
    pub parent: Id,
    pub height: u64,
    /// Unix seconds.
    pub timestamp: u64,
}

/// Engine is the seam for the executor: rs/node's real one, or `TrivialEngine`.
pub trait Engine: Send + Sync + 'static {
    /// A parsed block: cheap to clone (share the bytes).
    type Block: Send + Sync + 'static;
    /// What `verify` computed and `accept` applies: the block's write set,
    /// receipts and traces, layered on the parent's `Pending` when the parent
    /// is not accepted yet. Dropped on reject.
    type Pending: Send + Sync + 'static;

    fn parse(&self, bytes: Bytes) -> Result<Self::Block, Error>;
    /// BatchedParseBlock: the engine may decode the batch in parallel.
    fn parse_batch(&self, raws: Vec<Bytes>) -> Result<Vec<Self::Block>, Error> {
        raws.into_iter().map(|r| self.parse(r)).collect()
    }
    fn meta(&self, b: &Self::Block) -> Meta;
    fn bytes(&self, b: &Self::Block) -> Bytes;
    /// Execute `b` on top of its parent's state: `parent` is the parent's
    /// pending state, or None when the parent is the accepted head.
    fn verify(&self, b: &Self::Block, parent: Option<&Arc<Self::Pending>>, pchain_height: Option<u64>) -> Result<Self::Pending, Error>;
    /// Apply the pending state; the block becomes the accepted head.
    fn accept(&self, b: &Self::Block, p: &Self::Pending) -> Result<(), Error>;
    fn last_accepted(&self) -> Self::Block;
    /// Accepted blocks only (the tree answers for the verified ones).
    fn get_block(&self, id: &Id) -> Option<Self::Block>;
    fn block_id_at_height(&self, height: u64) -> Option<Id>;
    /// One JSON-RPC request body in, one response body out (the /rpc handler).
    fn rpc(&self, body: &[u8]) -> Vec<u8>;
    /// The RPC server behind /ws (None: no websocket handler is mounted).
    fn ws_server(&self) -> Option<&rpc::Server> {
        None
    }
    fn health(&self) -> Result<serde_json::Value, Error> {
        Ok(serde_json::json!({"height": self.meta(&self.last_accepted()).height}))
    }
    fn shutdown(&self) {}
    /// SetState: true in NormalOp (the tip), false while bootstrapping.
    fn set_state(&self, normal: bool) {
        let _ = normal;
    }
}

struct Verified<E: Engine> {
    block: Arc<E::Block>,
    pending: Arc<E::Pending>,
}

pub struct Tree<E: Engine> {
    pub engine: E,
    verified: Mutex<HashMap<Id, Verified<E>>>,
}

impl<E: Engine> Tree<E> {
    pub fn new(engine: E) -> Tree<E> {
        Tree { engine, verified: Mutex::new(HashMap::new()) }
    }

    pub fn parse(&self, bytes: Bytes) -> Result<E::Block, Error> {
        self.engine.parse(bytes)
    }

    pub fn last_accepted(&self) -> E::Block {
        self.engine.last_accepted()
    }

    /// Verified blocks first, then the engine's accepted ones.
    pub fn get_block(&self, id: &Id) -> Option<Arc<E::Block>> {
        if let Some(v) = self.verified.lock().unwrap().get(id) {
            return Some(v.block.clone());
        }
        self.engine.get_block(id).map(Arc::new)
    }

    pub fn block_id_at_height(&self, h: u64) -> Option<Id> {
        self.engine.block_id_at_height(h)
    }

    fn is_accepted(&self, m: &Meta) -> bool {
        self.engine.block_id_at_height(m.height) == Some(m.id)
    }

    /// Verify: idempotent for a block already verified or accepted; otherwise
    /// the parent must be the accepted head or a verified block.
    pub fn verify(&self, b: E::Block, pchain_height: Option<u64>) -> Result<Meta, Error> {
        let m = self.engine.meta(&b);
        let mut verified = self.verified.lock().unwrap();
        if verified.contains_key(&m.id) || self.is_accepted(&m) {
            return Ok(m);
        }
        let head = self.engine.meta(&self.engine.last_accepted());
        let parent = if m.parent == head.id {
            None
        } else if let Some(p) = verified.get(&m.parent) {
            Some(p.pending.clone())
        } else {
            return Err(format!(
                "block {} {} parent {} is neither the accepted head {} {} nor a verified block",
                m.height,
                hex(&m.id),
                hex(&m.parent),
                head.height,
                hex(&head.id)
            )
            .into());
        };
        let pending = self.engine.verify(&b, parent.as_ref(), pchain_height)?;
        verified.insert(m.id, Verified { block: Arc::new(b), pending: Arc::new(pending) });
        Ok(m)
    }

    pub fn accept(&self, id: &Id) -> Result<(), Error> {
        let mut verified = self.verified.lock().unwrap();
        let Some(v) = verified.remove(id) else {
            if let Some(b) = self.engine.get_block(id) {
                if self.is_accepted(&self.engine.meta(&b)) {
                    return Ok(());
                }
            }
            return Err(format!("block {} accepted before it was verified", hex(id)).into());
        };
        let m = self.engine.meta(&v.block);
        let head = self.engine.meta(&self.engine.last_accepted());
        if m.parent != head.id {
            verified.insert(*id, v);
            return Err(format!("block {} {} accepted but its parent is not the accepted head {} {}", m.height, hex(&m.id), head.height, hex(&head.id)).into());
        }
        if let Err(e) = self.engine.accept(&v.block, &v.pending) {
            verified.insert(*id, v);
            return Err(e);
        }
        Ok(())
    }

    /// The pending state of a verified (not yet accepted) block.
    pub fn pending(&self, id: &Id) -> Option<Arc<E::Pending>> {
        self.verified.lock().unwrap().get(id).map(|v| v.pending.clone())
    }

    /// A block the engine built (already executed): verified from here on.
    pub fn insert_verified(&self, b: E::Block, pending: E::Pending) {
        let m = self.engine.meta(&b);
        self.verified.lock().unwrap().insert(m.id, Verified { block: Arc::new(b), pending: Arc::new(pending) });
    }

    /// Reject drops the pending state; unknown ids are fine (already dropped).
    pub fn reject(&self, id: &Id) {
        self.verified.lock().unwrap().remove(id);
    }

    pub fn verified_len(&self) -> usize {
        self.verified.lock().unwrap().len()
    }
}

pub fn hex(id: &Id) -> String {
    let mut s = String::with_capacity(66);
    s.push_str("0x");
    for b in id {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    /// A key-value chain: a block is `[parent 32][height 8][ts 8][k=v,k=v...]`
    /// and its id is the sha256 of its bytes. Pending is the block's writes
    /// layered on its parent's; the accepted state is a flat map.
    struct Kv {
        state: Mutex<(BTreeMap<String, String>, Vec<Blk>)>,
    }
    #[derive(Clone)]
    struct Blk {
        m: Meta,
        writes: Vec<(String, String)>,
        bytes: Bytes,
    }
    struct Pend {
        parent: Option<Arc<Pend>>,
        writes: BTreeMap<String, String>,
    }
    impl Pend {
        fn get(&self, k: &str) -> Option<String> {
            self.writes.get(k).cloned().or_else(|| self.parent.as_ref().and_then(|p| p.get(k)))
        }
    }
    fn mk(parent: Id, height: u64, kv: &str) -> Bytes {
        let mut b = Vec::new();
        b.extend_from_slice(&parent);
        b.extend_from_slice(&height.to_le_bytes());
        b.extend_from_slice(&height.to_le_bytes());
        b.extend_from_slice(kv.as_bytes());
        Bytes::from(b)
    }
    fn idof(b: &[u8]) -> Id {
        use sha2::Digest;
        sha2::Sha256::digest(b).into()
    }
    impl Engine for Kv {
        type Block = Blk;
        type Pending = Pend;
        fn parse(&self, bytes: Bytes) -> Result<Blk, Error> {
            let parent: Id = bytes[..32].try_into()?;
            let height = u64::from_le_bytes(bytes[32..40].try_into()?);
            let timestamp = u64::from_le_bytes(bytes[40..48].try_into()?);
            let writes = std::str::from_utf8(&bytes[48..])?
                .split(',')
                .filter(|s| !s.is_empty())
                .map(|s| {
                    let (k, v) = s.split_once('=').unwrap();
                    (k.to_string(), v.to_string())
                })
                .collect();
            Ok(Blk { m: Meta { id: idof(&bytes), parent, height, timestamp }, writes, bytes })
        }
        fn meta(&self, b: &Blk) -> Meta {
            b.m.clone()
        }
        fn bytes(&self, b: &Blk) -> Bytes {
            b.bytes.clone()
        }
        fn verify(&self, b: &Blk, parent: Option<&Arc<Pend>>, _: Option<u64>) -> Result<Pend, Error> {
            // "state seen by this block": every write of the form k=+x appends to what the parent chain shows.
            let st = self.state.lock().unwrap();
            let mut writes = BTreeMap::new();
            for (k, v) in &b.writes {
                let seen = parent
                    .and_then(|p| p.get(k))
                    .or_else(|| st.0.get(k).cloned())
                    .unwrap_or_default();
                writes.insert(k.clone(), format!("{seen}{v}"));
            }
            Ok(Pend { parent: parent.cloned(), writes })
        }
        fn accept(&self, b: &Blk, p: &Pend) -> Result<(), Error> {
            let mut st = self.state.lock().unwrap();
            st.0.extend(p.writes.clone());
            st.1.push(b.clone());
            Ok(())
        }
        fn last_accepted(&self) -> Blk {
            self.state.lock().unwrap().1.last().unwrap().clone()
        }
        fn get_block(&self, id: &Id) -> Option<Blk> {
            self.state.lock().unwrap().1.iter().find(|b| &b.m.id == id).cloned()
        }
        fn block_id_at_height(&self, h: u64) -> Option<Id> {
            self.state.lock().unwrap().1.get(h as usize).map(|b| b.m.id)
        }
        fn rpc(&self, _: &[u8]) -> Vec<u8> {
            Vec::new()
        }
    }

    fn genesis() -> Kv {
        let g = Blk { m: Meta { id: [0; 32], parent: [0; 32], height: 0, timestamp: 0 }, writes: vec![], bytes: Bytes::new() };
        Kv { state: Mutex::new((BTreeMap::new(), vec![g])) }
    }

    #[test]
    fn siblings_accept_one_reject_other() {
        let t = Tree::new(genesis());
        // Two children of the genesis: A writes x=A, B writes x=B.
        let a = t.parse(mk([0; 32], 1, "x=A")).unwrap();
        let b = t.parse(mk([0; 32], 1, "x=B")).unwrap();
        let (ma, mb) = (t.verify(a, None).unwrap(), t.verify(b, None).unwrap());
        assert_ne!(ma.id, mb.id);
        assert_eq!(t.verified_len(), 2);
        // A grandchild of each sibling sees its own parent's write, not the other's.
        let ca = t.parse(mk(ma.id, 2, "x=1")).unwrap();
        let cb = t.parse(mk(mb.id, 2, "x=2")).unwrap();
        let (mca, mcb) = (t.verify(ca, None).unwrap(), t.verify(cb, None).unwrap());
        assert_eq!(t.verified_len(), 4);
        // Accept A, reject B (and B's child, as consensus would).
        t.accept(&ma.id).unwrap();
        t.reject(&mb.id);
        t.reject(&mcb.id);
        assert_eq!(t.engine.state.lock().unwrap().0["x"], "A");
        assert_eq!(t.last_accepted().m.id, ma.id);
        assert_eq!(t.block_id_at_height(1), Some(ma.id));
        // A's child was verified on top of A's pending state: x = "A1".
        t.accept(&mca.id).unwrap();
        assert_eq!(t.engine.state.lock().unwrap().0["x"], "A1");
        assert_eq!(t.verified_len(), 0);
        // The next block sees the accepted state: x = "A1" then appends.
        let d = t.parse(mk(mca.id, 3, "x=!")).unwrap();
        let md = t.verify(d, None).unwrap();
        t.accept(&md.id).unwrap();
        assert_eq!(t.engine.state.lock().unwrap().0["x"], "A1!");
        // A block on B's branch cannot be verified any more: B is gone.
        let orphan = t.parse(mk(mb.id, 2, "x=9")).unwrap();
        assert!(t.verify(orphan, None).is_err());
        // Accept before verify is refused; verifying an accepted block again is a no-op.
        let e = t.parse(mk(md.id, 4, "y=0")).unwrap();
        let me = t.engine.meta(&e);
        assert!(t.accept(&me.id).is_err());
        let again = t.parse(t.engine.bytes(&t.get_block(&md.id).unwrap())).unwrap();
        assert_eq!(t.verify(again, None).unwrap(), md);
        assert_eq!(t.verified_len(), 0);
    }
}

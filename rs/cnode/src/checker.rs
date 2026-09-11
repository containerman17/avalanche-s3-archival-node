//! The root checker: rs/state's trie over the flat state in contract-key form.
//! Seeded from the import (a sorted run rolled into a trie file), then every
//! block's rows go into `Dirty` and its root is compared to the header's.
//! Every `every_blocks` blocks the rows since the last roll are merged into a
//! new run and rolled on a background thread; the new run is also the restart
//! snapshot (import::load_run). Nothing here is on the applier's path.
//!
//! Files in `dir`: run.N, trie.N, MANIFEST {gen, height, root}.

use alloy_primitives::B256;
use anyhow::{anyhow, bail, Context, Result};
use state::commit::dirty::{Dirty, SeekFn};
use state::commit::file::File;
use state::commit::roll::roll;
use state::overlay::Overlay;
use state::run::{Run, Writer};
use state::view::{merge, View};
use state::KvIter;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{sync_channel, Receiver};
use std::sync::Arc;
use std::time::Instant;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Mismatch {
    pub height: u64,
    pub expected: B256,
    pub got: B256,
}

#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize)]
pub struct Manifest {
    pub gen: u64,
    pub height: u64,
    pub root: B256,
}

pub struct Checker {
    dir: PathBuf,
    gen: u64,
    run: Arc<Run>,
    dirty: Dirty,
    /// Rows since the newest roll started, replayed onto the fresh Dirty when it lands.
    fresh: Overlay,
    /// The roll in flight: its rows (already in the run being written) and its result channel.
    rolling: Option<(Arc<Overlay>, Receiver<Result<(Run, File), String>>)>,
    pub height: u64,
    pub root: B256,
    blocks_since_roll: u64,
}

fn run_path(dir: &Path, gen: u64) -> PathBuf {
    dir.join(format!("run.{gen}"))
}
fn trie_path(dir: &Path, gen: u64) -> PathBuf {
    dir.join(format!("trie.{gen}"))
}
/// The 32-byte user field of run and trie: height and the first 24 bytes of the root.
fn user_data(h: u64, root: &B256) -> [u8; 32] {
    let mut u = [0u8; 32];
    u[..8].copy_from_slice(&h.to_le_bytes());
    u[8..].copy_from_slice(&root.as_slice()[..24]);
    u
}
fn seek_fn(run: Arc<Run>) -> Arc<SeekFn> {
    Arc::new(move |prefix: &[u8]| {
        let mut it = run.iter(Some(prefix), None);
        if it.next() {
            Some((it.key().to_vec(), it.value().to_vec()))
        } else {
            None
        }
    })
}

fn write_manifest(dir: &Path, m: &Manifest) -> Result<()> {
    let tmp = dir.join("MANIFEST.tmp");
    std::fs::write(&tmp, serde_json::to_vec(m)?)?;
    std::fs::File::open(&tmp)?.sync_all()?;
    std::fs::rename(&tmp, dir.join("MANIFEST"))?;
    std::fs::File::open(dir)?.sync_all()?;
    Ok(())
}

pub fn read_manifest(dir: &Path) -> Result<Option<Manifest>> {
    match std::fs::read(dir.join("MANIFEST")) {
        Ok(b) => Ok(Some(serde_json::from_slice(&b)?)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
}

impl Checker {
    /// Writes run.0 from sorted contract rows, rolls trie.0, and requires its
    /// root to be `expected` (the header's state root at `height`).
    pub fn seed(dir: &Path, rows: &mut dyn KvIter, height: u64, expected: B256) -> Result<Checker> {
        std::fs::create_dir_all(dir)?;
        let user = user_data(height, &expected);
        let t = Instant::now();
        let mut w = Writer::create(&run_path(dir, 0))?;
        w.set_user_data(user);
        let mut n = 0u64;
        while rows.next() {
            w.add(rows.key(), rows.value())?;
            n += 1;
        }
        w.close()?;
        let run = Run::open(&run_path(dir, 0))?;
        eprintln!("checker: run.0 written, {n} rows, {} B, {:.1?}", run.bytes(), t.elapsed());
        let t = Instant::now();
        let (root, stats) = roll(&mut run.iter(None, None), &trie_path(dir, 0), user).map_err(|e| anyhow!("roll: {e}"))?;
        eprintln!("checker: trie.0 rolled, root {}, {:?}, {:.1?}", B256::from(root), stats, t.elapsed());
        if B256::from(root) != expected {
            bail!("checker: imported state at height {height} rolls to root {} but the header says {expected}", B256::from(root));
        }
        write_manifest(dir, &Manifest { gen: 0, height, root: expected })?;
        Self::open_at(dir, Manifest { gen: 0, height, root: expected })
    }

    /// Opens the rolled pair the manifest names.
    pub fn open(dir: &Path) -> Result<(Checker, Manifest)> {
        let m = read_manifest(dir)?.ok_or_else(|| anyhow!("checker: no MANIFEST in {}", dir.display()))?;
        Ok((Self::open_at(dir, m)?, m))
    }

    fn open_at(dir: &Path, m: Manifest) -> Result<Checker> {
        let run = Arc::new(Run::open(&run_path(dir, m.gen)).context("run")?);
        let file = Arc::new(File::open(&trie_path(dir, m.gen)).map_err(|e| anyhow!("trie: {e}"))?);
        if B256::from(file.root()) != m.root || run.user_data() != user_data(m.height, &m.root) {
            bail!("checker: MANIFEST says gen {} height {} root {} but the files disagree", m.gen, m.height, m.root);
        }
        let dirty = Dirty::new(file, seek_fn(run.clone()));
        Ok(Checker { dir: dir.to_path_buf(), gen: m.gen, run, dirty, fresh: Overlay::new(), rolling: None, height: m.height, root: m.root, blocks_since_roll: 0 })
    }

    /// The snapshot the checker stands on: rows at `snapshot_height()`.
    pub fn run(&self) -> Arc<Run> {
        self.run.clone()
    }
    pub fn snapshot_height(&self) -> u64 {
        u64::from_le_bytes(self.run.user_data()[..8].try_into().unwrap())
    }

    /// Applies one block's rows and compares the root. On a mismatch the
    /// checker is left as is (halted by the caller).
    pub fn apply(&mut self, height: u64, rows: &[(Vec<u8>, Vec<u8>)], expected: B256) -> std::result::Result<(), Mismatch> {
        for (k, v) in rows {
            self.dirty.apply(k, v).expect("contract row");
            self.fresh.put(k, v);
        }
        let got = B256::from(self.dirty.root().expect("root"));
        if got != expected {
            return Err(Mismatch { height, expected, got });
        }
        self.height = height;
        self.root = expected;
        self.blocks_since_roll += 1;
        Ok(())
    }

    /// Feeds rows without checking (a restart replaying already-checked blocks).
    pub fn replay(&mut self, height: u64, rows: &[(Vec<u8>, Vec<u8>)], root: B256) {
        for (k, v) in rows {
            self.dirty.apply(k, v).expect("contract row");
            self.fresh.put(k, v);
        }
        self.height = height;
        self.root = root;
        self.blocks_since_roll += 1;
    }

    /// Starts a roll when due, and finishes one that landed. Call after every apply.
    pub fn maybe_roll(&mut self, every_blocks: u64) -> Result<()> {
        if self.rolling.is_none() && self.blocks_since_roll >= every_blocks && self.fresh.len() > 0 {
            let frozen = Arc::new(std::mem::replace(&mut self.fresh, Overlay::new()));
            let gen = self.gen + 1;
            let (dir, base, user) = (self.dir.clone(), self.run.clone(), user_data(self.height, &self.root));
            let (tx, rx) = sync_channel(1);
            let fz = frozen.clone();
            std::thread::spawn(move || {
                let r = (|| {
                    let t = Instant::now();
                    let run = merge(&run_path(&dir, gen), &View::new(Some(&fz), &[&base]), user).map_err(|e| format!("merge: {e}"))?;
                    let (root, _) = roll(&mut run.iter(None, None), &trie_path(&dir, gen), user).map_err(|e| format!("roll: {e}"))?;
                    let file = File::open(&trie_path(&dir, gen)).map_err(|e| format!("open trie: {e}"))?;
                    eprintln!("checker: rolled gen {gen}, {} rows, root {}, {:.1?}", run.len(), B256::from(root), t.elapsed());
                    Ok((run, file))
                })();
                let _ = tx.send(r);
            });
            self.rolling = Some((frozen, rx));
            self.blocks_since_roll = 0;
        }
        let Some((_, rx)) = &self.rolling else { return Ok(()) };
        let Ok(r) = rx.try_recv() else { return Ok(()) };
        let (run, file) = r.map_err(|e| anyhow!("checker roll: {e}"))?;
        let (frozen, _) = self.rolling.take().unwrap();
        let rolled_root = B256::from(file.root());
        let rolled_h = u64::from_le_bytes(run.user_data()[..8].try_into().unwrap());
        self.gen += 1;
        self.run = Arc::new(run);
        self.dirty = Dirty::new(Arc::new(file), seek_fn(self.run.clone()));
        // The rows applied while the roll ran go onto the fresh Dirty; the
        // root must come back to where we are.
        let fresh = std::mem::replace(&mut self.fresh, Overlay::new());
        let mut it = fresh.iter(None, None);
        while it.next() {
            self.dirty.apply(it.key(), it.value()).expect("contract row");
            self.fresh.put(it.key(), it.value());
        }
        drop(frozen);
        if self.fresh.len() > 0 {
            let got = B256::from(self.dirty.root().expect("root"));
            if got != self.root {
                bail!("checker: after the roll to gen {} the replayed rows give root {got}, the checked root is {}", self.gen, self.root);
            }
        }
        write_manifest(&self.dir, &Manifest { gen: self.gen, height: rolled_h, root: rolled_root })?;
        for g in 0..self.gen {
            let _ = std::fs::remove_file(run_path(&self.dir, g));
            let _ = std::fs::remove_file(trie_path(&self.dir, g));
        }
        Ok(())
    }

    pub fn rolling(&self) -> bool {
        self.rolling.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hot::{rows, Account, Diff};
    use alloy_primitives::U256;
    use state::RowsIter;

    fn acct(i: u8, nonce: u64, multi: bool) -> ([u8; 32], Account) {
        ([i; 32], Account { nonce, balance: U256::from(1000u64 * i as u64), code_hash: alloy_primitives::KECCAK256_EMPTY, multicoin: multi })
    }

    fn sorted(d: &Diff) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut r = rows(d);
        r.sort();
        r
    }

    /// The Dirty path (seed + apply) and the roll path (seed from the post
    /// state) must agree, across a roll, with multicoin leaves and deletes.
    #[test]
    fn dirty_and_roll_agree() {
        let dir = std::env::temp_dir().join(format!("cnode-checker-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let (a1, acc1) = acct(1, 1, false);
        let (a2, acc2) = acct(2, 5, true);
        let (a3, acc3) = acct(3, 0, false);
        let base = Diff { accounts: vec![(a1, Some(acc1)), (a2, Some(acc2)), (a3, Some(acc3))], storage: vec![(a2, [9; 32], U256::from(7)), (a2, [8; 32], U256::from(1))], code: vec![] };
        let base_rows = sorted(&base);
        let root0 = {
            let (r, _) = roll(&mut RowsIter::new(&base_rows), &dir.join("probe"), [0; 32]).unwrap();
            B256::from(r)
        };
        let mut c = Checker::seed(&dir.join("a"), &mut RowsIter::new(&base_rows), 100, root0).unwrap();
        assert!(Checker::seed(&dir.join("bad"), &mut RowsIter::new(&base_rows), 100, B256::ZERO).is_err());

        // Block 101: a2 changes (keeps multicoin), a3 deleted, a slot deleted, a new account with slots.
        let (a4, acc4) = acct(4, 2, false);
        let d1 = Diff {
            accounts: vec![(a2, Some(Account { nonce: 6, ..acc2 })), (a3, None), (a4, Some(acc4))],
            storage: vec![(a2, [9; 32], U256::ZERO), (a4, [1; 32], U256::from(0xabcdu64))],
            code: vec![],
        };
        let post1 = Diff { accounts: vec![(a1, Some(acc1)), (a2, Some(Account { nonce: 6, ..acc2 })), (a4, Some(acc4))], storage: vec![(a2, [8; 32], U256::from(1)), (a4, [1; 32], U256::from(0xabcdu64))], code: vec![] };
        let root1 = B256::from(roll(&mut RowsIter::new(&sorted(&post1)), &dir.join("probe1"), [0; 32]).unwrap().0);
        assert_ne!(root0, root1);
        assert_eq!(c.apply(101, &rows(&d1), B256::repeat_byte(1)), Err(Mismatch { height: 101, expected: B256::repeat_byte(1), got: root1 }));
        // A failed apply leaves the rows in; re-checking with the right root passes.
        assert_eq!(c.apply(101, &[], root1), Ok(()));

        // Roll now, with block 102 applied while the roll is in flight.
        c.maybe_roll(1).unwrap();
        assert!(c.rolling());
        let d2 = Diff { accounts: vec![(a1, Some(Account { nonce: 9, ..acc1 }))], storage: vec![], code: vec![] };
        let post2 = Diff { accounts: vec![(a1, Some(Account { nonce: 9, ..acc1 })), (a2, Some(Account { nonce: 6, ..acc2 })), (a4, Some(acc4))], storage: post1.storage.clone(), code: vec![] };
        let root2 = B256::from(roll(&mut RowsIter::new(&sorted(&post2)), &dir.join("probe2"), [0; 32]).unwrap().0);
        assert_eq!(c.apply(102, &rows(&d2), root2), Ok(()));
        while c.rolling() {
            c.maybe_roll(1).unwrap();
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        let m = read_manifest(&dir.join("a")).unwrap().unwrap();
        assert_eq!((m.gen, m.height, m.root), (1, 101, root1));
        assert_eq!(c.snapshot_height(), 101);
        assert_eq!(c.apply(103, &[], root2), Ok(()));
        // Reopen from the manifest: the snapshot is at 101, replaying 102 gets back to root2.
        let (mut c2, _) = Checker::open(&dir.join("a")).unwrap();
        assert_eq!(c2.root, root1);
        assert_eq!(c2.apply(102, &rows(&d2), root2), Ok(()));
        let _ = std::fs::remove_dir_all(&dir);
    }
}

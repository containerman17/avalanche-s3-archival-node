//! Restart snapshots: the hot maps written out by a forked child (copy on
//! write, so the applier never pauses), unsorted, in the bootstrap export's
//! own format (accounts.bin, storage.bin, code.bin, meta.json) under
//! `<data>/dumps/<height>/`. `import::load` reads records in any order; the
//! checker sorts a dump before seeding its trie from it. A dump is written
//! to `<height>.tmp` and renamed when complete, so a directory without `.tmp`
//! is whole.
//!
//! Fork in a multithreaded process: the child touches only the papaya maps
//! (lock free) and plain file writes; glibc's malloc is fork safe. It never
//! prints (stderr's lock may be held by another thread of the parent).

use crate::hot::HotState;
use crate::import::Meta;
use anyhow::{Context, Result};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

pub struct Dumper {
    dir: PathBuf,
    child: Option<i32>,
}

impl Dumper {
    pub fn new(dir: &Path) -> Result<Dumper> {
        std::fs::create_dir_all(dir)?;
        // Children are reaped by the kernel; the parent only asks whether one is still alive.
        unsafe { libc::signal(libc::SIGCHLD, libc::SIG_IGN) };
        Ok(Dumper { dir: dir.to_path_buf(), child: None })
    }

    /// Whether the last dump child is still running.
    pub fn busy(&mut self) -> bool {
        match self.child {
            Some(pid) if Path::new(&format!("/proc/{pid}")).exists() => true,
            _ => {
                self.child = None;
                false
            }
        }
    }

    /// Forks a child that writes the hot maps as they are right now. Returns
    /// false (and does nothing) while an earlier child is still writing.
    pub fn start(&mut self, hot: &HotState, meta: Meta) -> Result<bool> {
        if self.busy() {
            return Ok(false);
        }
        let tmp = self.dir.join(format!("{}.tmp", meta.height));
        let fin = self.dir.join(meta.height.to_string());
        let _ = std::fs::remove_dir_all(&tmp);
        std::fs::create_dir_all(&tmp)?;
        let pid = unsafe { libc::fork() };
        if pid < 0 {
            anyhow::bail!("fork: {}", std::io::Error::last_os_error());
        }
        if pid == 0 {
            let code = match write(hot, &tmp, meta).and_then(|()| std::fs::rename(&tmp, &fin).map_err(Into::into)) {
                Ok(()) => 0,
                Err(_) => 1,
            };
            unsafe { libc::_exit(code) };
        }
        self.child = Some(pid);
        Ok(true)
    }
}

fn write(hot: &HotState, dir: &Path, mut meta: Meta) -> Result<()> {
    let open = |name: &str| -> Result<BufWriter<std::fs::File>> { Ok(BufWriter::with_capacity(4 << 20, std::fs::File::create(dir.join(name))?)) };
    let mut accounts = 0u64;
    let mut w = open("accounts.bin")?;
    let mut err = None;
    hot.for_each_account(|h, a| {
        if err.is_some() {
            return;
        }
        let mut rec = [0u8; 105];
        rec[..32].copy_from_slice(h);
        rec[32..40].copy_from_slice(&a.nonce.to_le_bytes());
        rec[40..72].copy_from_slice(&a.balance.to_be_bytes::<32>());
        rec[72..104].copy_from_slice(a.code_hash.as_slice());
        rec[104] = a.multicoin as u8;
        err = w.write_all(&rec).err();
        accounts += 1;
    });
    err.map_or(Ok(()), Err).context("accounts.bin")?;
    w.flush()?;

    let mut slots = 0u64;
    let mut w = open("storage.bin")?;
    let mut err = None;
    hot.for_each_slot(|h, k, v| {
        if err.is_some() || v.is_zero() {
            return;
        }
        let mut rec = [0u8; 96];
        rec[..32].copy_from_slice(h);
        rec[32..64].copy_from_slice(k);
        rec[64..96].copy_from_slice(&v.to_be_bytes::<32>());
        err = w.write_all(&rec).err();
        slots += 1;
    });
    err.map_or(Ok(()), Err).context("storage.bin")?;
    w.flush()?;

    let mut codes = 0u64;
    let mut w = open("code.bin")?;
    let mut err = None;
    hot.for_each_code(|h, code| {
        if err.is_some() {
            return;
        }
        err = w.write_all(h.as_slice()).and_then(|()| w.write_all(&(code.len() as u32).to_le_bytes())).and_then(|()| w.write_all(code)).err();
        codes += 1;
    });
    err.map_or(Ok(()), Err).context("code.bin")?;
    w.flush()?;

    meta.accounts = accounts;
    meta.slots = slots;
    meta.codes = codes;
    meta.sorted = false;
    std::fs::write(dir.join("meta.json"), serde_json::to_vec_pretty(&meta)?)?;
    std::fs::File::open(dir)?.sync_all()?;
    Ok(())
}

/// The complete dumps in `dir` as (height, path), oldest first.
pub fn list(dir: &Path) -> Result<Vec<(u64, PathBuf)>> {
    let mut out = Vec::new();
    let rd = match std::fs::read_dir(dir) {
        Ok(rd) => rd,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(out),
        Err(e) => return Err(e.into()),
    };
    for e in rd {
        let e = e?;
        if let Ok(h) = e.file_name().to_string_lossy().parse::<u64>() {
            if e.path().join("meta.json").exists() {
                out.push((h, e.path()));
            }
        }
    }
    out.sort();
    Ok(out)
}

/// The newest complete dump at or below `at_or_below` (any, when None).
pub fn newest(dir: &Path, at_or_below: Option<u64>) -> Result<Option<(u64, PathBuf)>> {
    Ok(list(dir)?.into_iter().filter(|(h, _)| at_or_below.map_or(true, |c| *h <= c)).last())
}

/// Keeps the newest `keep` complete dumps; a `.tmp` of a dead child is
/// removed by the next `start` at that height, or by hand.
pub fn prune(dir: &Path, keep: usize) -> Result<()> {
    let all = list(dir)?;
    for (_, p) in all.iter().take(all.len().saturating_sub(keep)) {
        std::fs::remove_dir_all(p)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hot::Account;
    use alloy_primitives::{B256, U256};
    use state::KvIter;

    /// A forked dump loads back whole, and sorts into an export the checker's
    /// seed iterator accepts in key order.
    #[test]
    fn fork_dump_round_trip_and_sort() {
        let dir = std::env::temp_dir().join(format!("cnode-dump-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let hot = HotState::new(7, B256::ZERO);
        for i in 0..300u32 {
            let h = alloy_primitives::keccak256(i.to_le_bytes()).0;
            hot.put_account(h, Account { nonce: i as u64, balance: U256::from(i), code_hash: B256::repeat_byte(i as u8), multicoin: i % 3 == 0 });
            for k in 0..(i % 5) {
                hot.put_slot(h, alloy_primitives::keccak256([i as u8, k as u8]).0, U256::from(k + 1));
            }
        }
        hot.put_code(alloy_primitives::keccak256([0x60, 0x00]), std::sync::Arc::from(vec![0x60, 0x00]));
        let mut d = Dumper::new(&dir.join("dumps")).unwrap();
        let meta = Meta { height: 7, hash: B256::repeat_byte(9), state_root: B256::repeat_byte(8), head_height: 7, accounts: 0, slots: 0, codes: 0, sorted: false };
        assert!(d.start(&hot, meta).unwrap());
        while d.busy() {
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        let (h, p) = newest(&dir.join("dumps"), None).unwrap().unwrap();
        assert_eq!(h, 7);
        assert!(newest(&dir.join("dumps"), Some(6)).unwrap().is_none());
        let m = crate::import::read_meta(&p).unwrap();
        assert_eq!((m.accounts, m.slots, m.codes, m.sorted), (300, 600, 1, false));

        let back = HotState::new(0, B256::ZERO);
        assert_eq!(crate::import::load(&p, &back).unwrap(), (300, 600, 1));
        let h5 = alloy_primitives::keccak256(6u32.to_le_bytes()).0;
        assert_eq!(back.account_raw(&h5).unwrap().nonce, 6);
        assert_eq!(back.storage_raw(&h5, &alloy_primitives::keccak256([6u8, 0u8]).0), U256::from(1));

        let sorted = dir.join("sorted");
        let m2 = crate::import::sort_export(&p, &sorted).unwrap();
        assert!(m2.sorted);
        let mut it = crate::import::ExportIter::open(&sorted).unwrap();
        let mut prev: Vec<u8> = Vec::new();
        while it.next() {
            assert!(it.key() > prev.as_slice(), "export not in key order");
            prev = it.key().to_vec();
        }
        assert_eq!(it.rows, 900);
        let _ = std::fs::remove_dir_all(&dir);
    }
}

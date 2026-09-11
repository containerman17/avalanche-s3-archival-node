//! Loads the bootstrap export (cmd/cnode-export) into a `HotState`.
//! Files: meta.json, accounts.bin (105 B records), storage.bin (96 B records),
//! code.bin ([32 B hash][u32 le len][bytes]).

use crate::hot::{Account, HotState};
use alloy_primitives::{B256, U256};
use anyhow::{bail, Context, Result};
use std::io::{BufReader, Read};
use std::path::Path;
use std::sync::Arc;

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct Meta {
    pub height: u64,
    pub hash: B256,
    pub state_root: B256,
    pub head_height: u64,
    pub accounts: u64,
    pub slots: u64,
    pub codes: u64,
    /// accounts.bin and storage.bin in key order (cmd/cnode-sync writes them
    /// so; a fork dump does not). `ExportIter` needs sorted files.
    #[serde(default = "yes")]
    pub sorted: bool,
}

fn yes() -> bool {
    true
}

/// Writes `src` (an unsorted dump) as a sorted export into `dst`: records
/// sharded by their first key byte into 256 files, each sorted in memory and
/// appended in order; code.bin hard linked, meta.json with `sorted: true`.
pub fn sort_export(src: &Path, dst: &Path) -> Result<Meta> {
    let mut meta = read_meta(src)?;
    let _ = std::fs::remove_dir_all(dst);
    std::fs::create_dir_all(dst)?;
    for (name, size) in [("accounts.bin", 105usize), ("storage.bin", 96)] {
        let shard_dir = dst.join(format!("{name}.shards"));
        std::fs::create_dir_all(&shard_dir)?;
        let mut shards: Vec<std::io::BufWriter<std::fs::File>> = (0..256)
            .map(|i| Ok(std::io::BufWriter::with_capacity(1 << 20, std::fs::File::create(shard_dir.join(format!("{i:02x}")))?)))
            .collect::<Result<_>>()?;
        records(&src.join(name), size, |b| {
            use std::io::Write;
            shards[b[0] as usize].write_all(b).expect("shard write");
        })?;
        for mut s in shards {
            use std::io::Write;
            s.flush()?;
        }
        let mut out = std::io::BufWriter::with_capacity(4 << 20, std::fs::File::create(dst.join(name))?);
        for i in 0..256 {
            let b = std::fs::read(shard_dir.join(format!("{i:02x}")))?;
            let mut idx: Vec<usize> = (0..b.len() / size).collect();
            idx.sort_unstable_by(|x, y| b[x * size..x * size + 64.min(size - 32)].cmp(&b[y * size..y * size + 64.min(size - 32)]));
            use std::io::Write;
            for j in idx {
                out.write_all(&b[j * size..(j + 1) * size])?;
            }
        }
        use std::io::Write;
        out.flush()?;
        std::fs::remove_dir_all(&shard_dir)?;
    }
    if std::fs::hard_link(src.join("code.bin"), dst.join("code.bin")).is_err() {
        std::fs::copy(src.join("code.bin"), dst.join("code.bin"))?;
    }
    meta.sorted = true;
    std::fs::write(dst.join("meta.json"), serde_json::to_vec_pretty(&meta)?)?;
    Ok(meta)
}

pub fn read_meta(dir: &Path) -> Result<Meta> {
    let f = std::fs::File::open(dir.join("meta.json")).context("meta.json")?;
    Ok(serde_json::from_reader(f)?)
}

fn records(path: &Path, size: usize, mut f: impl FnMut(&[u8])) -> Result<u64> {
    let mut r = BufReader::with_capacity(1 << 20, std::fs::File::open(path).with_context(|| path.display().to_string())?);
    let mut buf = vec![0u8; size];
    let mut n = 0u64;
    loop {
        match r.read_exact(&mut buf) {
            Ok(()) => {
                f(&buf);
                n += 1;
            }
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(n),
            Err(e) => return Err(e.into()),
        }
    }
}

/// Fills `hs` from the export in `dir`; returns (accounts, slots, codes) loaded.
pub fn load(dir: &Path, hs: &HotState) -> Result<(u64, u64, u64)> {
    let accounts = records(&dir.join("accounts.bin"), 105, |b| {
        hs.put_account(
            b[..32].try_into().unwrap(),
            Account {
                nonce: u64::from_le_bytes(b[32..40].try_into().unwrap()),
                balance: U256::from_be_slice(&b[40..72]),
                code_hash: B256::from_slice(&b[72..104]),
                multicoin: b[104] != 0,
            },
        );
    })?;
    let slots = records(&dir.join("storage.bin"), 96, |b| {
        hs.put_slot(b[..32].try_into().unwrap(), b[32..64].try_into().unwrap(), U256::from_be_slice(&b[64..96]));
    })?;
    let mut r = BufReader::with_capacity(1 << 20, std::fs::File::open(dir.join("code.bin")).context("code.bin")?);
    let mut codes = 0u64;
    loop {
        let mut head = [0u8; 36];
        match r.read_exact(&mut head) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e.into()),
        }
        let len = u32::from_le_bytes(head[32..36].try_into().unwrap()) as usize;
        let mut code = vec![0u8; len];
        r.read_exact(&mut code)?;
        let hash = B256::from_slice(&head[..32]);
        if alloy_primitives::keccak256(&code) != hash {
            bail!("code.bin: hash mismatch for {hash}");
        }
        hs.put_code(hash, Arc::from(code));
        codes += 1;
    }
    Ok((accounts, slots, codes))
}

// ---------------------------------------------------------------------------
// The export as sorted contract rows (the checker's seed), and a rolled run
// back into a HotState (restart, AtHeight).

use crate::hot::rows as diff_rows;
use crate::hot::Diff;
use state::KvIter;

fn account_row(a: &Account) -> Vec<u8> {
    diff_rows(&Diff { accounts: vec![([0u8; 32], Some(*a))], ..Default::default() }).pop().unwrap().1
}

/// accounts.bin and storage.bin merged in contract-key order: for each account
/// hash h, `h||0x00` then its slots `h||0x01||slot` (both files are sorted by hash).
pub struct ExportIter {
    acc: BufReader<std::fs::File>,
    st: BufReader<std::fs::File>,
    acc_rec: Option<[u8; 105]>,
    st_rec: Option<[u8; 96]>,
    key: Vec<u8>,
    val: Vec<u8>,
    pub rows: u64,
}

impl ExportIter {
    pub fn open(dir: &Path) -> Result<ExportIter> {
        let mut it = ExportIter {
            acc: BufReader::with_capacity(1 << 20, std::fs::File::open(dir.join("accounts.bin"))?),
            st: BufReader::with_capacity(1 << 20, std::fs::File::open(dir.join("storage.bin"))?),
            acc_rec: None,
            st_rec: None,
            key: Vec::with_capacity(65),
            val: Vec::with_capacity(80),
            rows: 0,
        };
        it.acc_rec = read_rec::<105>(&mut it.acc)?;
        it.st_rec = read_rec::<96>(&mut it.st)?;
        Ok(it)
    }
}

fn read_rec<const N: usize>(r: &mut BufReader<std::fs::File>) -> Result<Option<[u8; N]>> {
    let mut b = [0u8; N];
    match r.read_exact(&mut b) {
        Ok(()) => Ok(Some(b)),
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => Ok(None),
        Err(e) => Err(e.into()),
    }
}

impl KvIter for ExportIter {
    fn next(&mut self) -> bool {
        // An account row sorts before its own slots (0x00 < 0x01) and before
        // any later hash; a slot row of an earlier hash comes first.
        let take_acc = match (&self.acc_rec, &self.st_rec) {
            (None, None) => return false,
            (Some(_), None) => true,
            (None, Some(_)) => false,
            (Some(a), Some(s)) => a[..32] <= s[..32],
        };
        self.key.clear();
        self.val.clear();
        if take_acc {
            let b = self.acc_rec.take().unwrap();
            self.key.extend_from_slice(&b[..32]);
            self.key.push(0);
            let a = Account {
                nonce: u64::from_le_bytes(b[32..40].try_into().unwrap()),
                balance: U256::from_be_slice(&b[40..72]),
                code_hash: B256::from_slice(&b[72..104]),
                multicoin: b[104] != 0,
            };
            self.val = account_row(&a);
            self.acc_rec = read_rec::<105>(&mut self.acc).expect("accounts.bin");
        } else {
            let b = self.st_rec.take().unwrap();
            self.key.extend_from_slice(&b[..32]);
            self.key.push(1);
            self.key.extend_from_slice(&b[32..64]);
            self.val = U256::from_be_slice(&b[64..96]).to_be_bytes_trimmed_vec();
            self.st_rec = read_rec::<96>(&mut self.st).expect("storage.bin");
        }
        self.rows += 1;
        true
    }
    fn key(&self) -> &[u8] {
        &self.key
    }
    fn value(&self) -> &[u8] {
        &self.val
    }
}

/// Parses a contract account row RLP[nonce, balance, codeHash, multicoin].
pub fn parse_account_row(v: &[u8]) -> Result<Account> {
    use alloy_rlp::Decodable;
    let mut p = v;
    let h = alloy_rlp::Header::decode(&mut p)?;
    if !h.list {
        bail!("account row is not a list");
    }
    let nonce = u64::decode(&mut p)?;
    let balance = U256::decode(&mut p)?;
    let code_hash = B256::decode(&mut p)?;
    let multicoin = if p.is_empty() { false } else { bool::decode(&mut p)? };
    Ok(Account { nonce, balance, code_hash, multicoin })
}

/// Fills `hs` from a rolled run (the checker's snapshot). Code is not in the
/// run: the caller loads it from the history's code table or the export.
pub fn load_run(run: &state::run::Run, hs: &HotState) -> Result<(u64, u64)> {
    let mut it = run.iter(None, None);
    let (mut accounts, mut slots) = (0u64, 0u64);
    while it.next() {
        let k = it.key();
        match k.len() {
            33 => {
                hs.put_account(k[..32].try_into().unwrap(), parse_account_row(it.value())?);
                accounts += 1;
            }
            65 => {
                hs.put_slot(k[..32].try_into().unwrap(), k[33..65].try_into().unwrap(), U256::from_be_slice(it.value()));
                slots += 1;
            }
            n => bail!("run row with a {n}-byte key"),
        }
    }
    Ok((accounts, slots))
}

/// Loads code.bin only (the hot state's code table on a restart).
pub fn load_code(dir: &Path, hs: &HotState) -> Result<u64> {
    let mut r = BufReader::with_capacity(1 << 20, std::fs::File::open(dir.join("code.bin")).context("code.bin")?);
    let mut codes = 0u64;
    loop {
        let mut head = [0u8; 36];
        match r.read_exact(&mut head) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e.into()),
        }
        let len = u32::from_le_bytes(head[32..36].try_into().unwrap()) as usize;
        let mut code = vec![0u8; len];
        r.read_exact(&mut code)?;
        hs.put_code(B256::from_slice(&head[..32]), Arc::from(code));
        codes += 1;
    }
    Ok(codes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn export_iter_orders_and_round_trips() {
        let dir = std::env::temp_dir().join(format!("cnode-import-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let mut acc = Vec::new();
        for (h, nonce, multi) in [(1u8, 7u64, false), (3, 0, true)] {
            let mut r = [0u8; 105];
            r[..32].copy_from_slice(&[h; 32]);
            r[32..40].copy_from_slice(&nonce.to_le_bytes());
            r[71] = 9;
            r[72..104].copy_from_slice(alloy_primitives::KECCAK256_EMPTY.as_slice());
            r[104] = multi as u8;
            acc.extend_from_slice(&r);
        }
        std::fs::write(dir.join("accounts.bin"), &acc).unwrap();
        let mut st = Vec::new();
        for (h, s, v) in [(1u8, 2u8, 5u8), (1, 4, 6), (2, 1, 1)] {
            let mut r = [0u8; 96];
            r[..32].copy_from_slice(&[h; 32]);
            r[32..64].copy_from_slice(&[s; 32]);
            r[95] = v;
            st.extend_from_slice(&r);
        }
        std::fs::write(dir.join("storage.bin"), &st).unwrap();
        let mut it = ExportIter::open(&dir).unwrap();
        let mut keys = Vec::new();
        let mut vals = Vec::new();
        while it.next() {
            keys.push(it.key().to_vec());
            vals.push(it.value().to_vec());
        }
        assert_eq!(keys.len(), 5);
        assert!(keys.windows(2).all(|w| w[0] < w[1]));
        assert_eq!(keys[0].len(), 33);
        assert_eq!(keys[1].len(), 65);
        assert_eq!(keys[3].len(), 65);
        assert_eq!(keys[4], [[3u8; 32].to_vec(), vec![0]].concat());
        let a = parse_account_row(&vals[4]).unwrap();
        assert_eq!((a.nonce, a.balance, a.multicoin), (0, U256::from(9), true));
        assert_eq!(vals[3], vec![1]);

        // Roll the rows into a run and load it back.
        let mut w = state::run::Writer::create(&dir.join("run.0")).unwrap();
        for (k, v) in keys.iter().zip(&vals) {
            w.add(k, v).unwrap();
        }
        w.close().unwrap();
        let run = state::run::Run::open(&dir.join("run.0")).unwrap();
        let hs = HotState::new(0, B256::ZERO);
        assert_eq!(load_run(&run, &hs).unwrap(), (2, 3));
        let g = hs.generation();
        assert_eq!(hs.account(g, &[1u8; 32]).unwrap().unwrap().nonce, 7);
        assert_eq!(hs.storage(g, &[1u8; 32], &[4u8; 32]).unwrap(), U256::from(6));
        let _ = std::fs::remove_dir_all(&dir);
    }
}

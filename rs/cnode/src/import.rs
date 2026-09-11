//! Loads the bootstrap export (cmd/cnode-export) into a `HotState`.
//! Files: meta.json, accounts.bin (104 B records), storage.bin (96 B records),
//! code.bin ([32 B hash][u32 le len][bytes]).

use crate::hot::{Account, HotState};
use alloy_primitives::{B256, U256};
use anyhow::{bail, Context, Result};
use std::io::{BufReader, Read};
use std::path::Path;
use std::sync::Arc;

#[derive(serde::Deserialize, Debug, Clone)]
pub struct Meta {
    pub height: u64,
    pub hash: B256,
    pub state_root: B256,
    pub head_height: u64,
    pub accounts: u64,
    pub slots: u64,
    pub codes: u64,
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
    let accounts = records(&dir.join("accounts.bin"), 104, |b| {
        hs.put_account(
            b[..32].try_into().unwrap(),
            Account {
                nonce: u64::from_le_bytes(b[32..40].try_into().unwrap()),
                balance: U256::from_be_slice(&b[40..72]),
                code_hash: B256::from_slice(&b[72..104]),
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

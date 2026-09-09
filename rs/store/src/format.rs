//! The key space (store/format.go): prefixes, key builders, Split, sections
//! and the pinned per-section constants.

pub const STORAGE_VERSION: u32 = 4;

pub const FLUSH_TXS: u64 = 500_000;
pub const FLUSH_BLOCKS: u64 = 50_000;
/// The third flush trigger: raw bytes of the window log (`window-max-bytes`).
/// Bounds the final seal a shutdown may abandon, the re-seal at open, and
/// the memtable's resident memory (~1.3x the log). 128 MiB: a validator's
/// RSS matters more than seal frequency (one L0 run per 128 MiB of rows).
pub const FLUSH_BYTES: u64 = 1 << 27;
pub const TERMINAL_LEVEL: i32 = 1;

pub const PREFIX_BLK: &[u8] = b"blk/";
pub const PREFIX_HDR: &[u8] = b"hdr/";
pub const PREFIX_ITX: &[u8] = b"itx/";
pub const PREFIX_PVM: &[u8] = b"pvm/";
pub const PREFIX_RCPT: &[u8] = b"rcpt/";
pub const PREFIX_TX: &[u8] = b"tx/";
/// The block's receipts blob (EIP-2718 receipt list) verbatim, what the
/// plugin was handed; and the state engine's write set of the block
/// (hashed keys) plus the code hashes it deployed, for recovery replay.
pub const PREFIX_RCB: &[u8] = b"rcb/";
pub const PREFIX_WS: &[u8] = b"ws/";
pub const PREFIX_CODE: &[u8] = b"code/";
pub const PREFIX_STATE: &[u8] = b"state/";
pub const PREFIX_TXH: &[u8] = b"txh/";
pub const PREFIX_BLKH: &[u8] = b"blkh/";
pub const PREFIX_CID: &[u8] = b"cid/";
pub const PREFIX_ADDR: &[u8] = b"addr/";
pub const PREFIX_ELOG: &[u8] = b"elog/";
pub const PREFIX_TVAL: &[u8] = b"tval/";
pub const PREFIX_SIG: &[u8] = b"sig/";
pub const PREFIX_SET: &[u8] = b"set/";

pub const ROLE_SENDER: u8 = 1;
pub const ROLE_RECIPIENT: u8 = 2;
pub const ROLE_CREATED: u8 = 4;
pub const ROLE_EMITTER: u8 = 8;
pub const ROLE_FRAME: u8 = 16;

/// Chain families in key-prefix order (mem.go famBlk..famTx).
pub const FAM_BLK: usize = 0;
pub const FAM_HDR: usize = 1;
pub const FAM_ITX: usize = 2;
pub const FAM_PVM: usize = 3;
pub const FAM_RCB: usize = 4;
pub const FAM_RCPT: usize = 5;
pub const FAM_TX: usize = 6;
pub const FAM_WS: usize = 7;
pub const NUM_FAMS: usize = 8;
pub const FAM_PREFIX: [&[u8]; NUM_FAMS] = [PREFIX_BLK, PREFIX_HDR, PREFIX_ITX, PREFIX_PVM, PREFIX_RCB, PREFIX_RCPT, PREFIX_TX, PREFIX_WS];
pub const TX_KEYED_FAMS: [usize; 3] = [FAM_ITX, FAM_RCPT, FAM_TX];

pub fn fam_by_height(fam: usize) -> bool {
    !TX_KEYED_FAMS.contains(&fam)
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Section {
    Chain = 0,
    State = 1,
    Lookup = 2,
}
pub const SECTIONS: [Section; 3] = [Section::Chain, Section::State, Section::Lookup];

impl Section {
    pub fn block_size(self) -> usize {
        match self {
            Section::Chain => 128 << 10,
            Section::State => 32 << 10,
            Section::Lookup => 8 << 10,
        }
    }
    pub fn index_block_size(self) -> usize {
        (2 * self.block_size()).max(64 << 10)
    }
    pub fn has_filter(self) -> bool {
        self != Section::Chain
    }
}

pub fn num_key(prefix: &[u8], n: u64) -> Vec<u8> {
    let mut k = Vec::with_capacity(prefix.len() + 8);
    k.extend_from_slice(prefix);
    k.extend_from_slice(&n.to_be_bytes());
    k
}
pub fn blk_key(h: u64) -> Vec<u8> {
    num_key(PREFIX_BLK, h)
}
pub fn hdr_key(h: u64) -> Vec<u8> {
    num_key(PREFIX_HDR, h)
}
pub fn itx_key(n: u64) -> Vec<u8> {
    num_key(PREFIX_ITX, n)
}
pub fn pvm_key(h: u64) -> Vec<u8> {
    num_key(PREFIX_PVM, h)
}
pub fn rcpt_key(n: u64) -> Vec<u8> {
    num_key(PREFIX_RCPT, n)
}
pub fn tx_key(n: u64) -> Vec<u8> {
    num_key(PREFIX_TX, n)
}
pub fn cat(prefix: &[u8], rest: &[u8]) -> Vec<u8> {
    let mut k = Vec::with_capacity(prefix.len() + rest.len());
    k.extend_from_slice(prefix);
    k.extend_from_slice(rest);
    k
}
pub fn txh_key(h: &[u8]) -> Vec<u8> {
    cat(PREFIX_TXH, h)
}
pub fn blkh_key(h: &[u8]) -> Vec<u8> {
    cat(PREFIX_BLKH, h)
}
pub fn cid_key(id: &[u8]) -> Vec<u8> {
    cat(PREFIX_CID, id)
}
pub fn code_key(h: &[u8]) -> Vec<u8> {
    cat(PREFIX_CODE, h)
}

pub fn account_prefix(addr: &[u8]) -> Vec<u8> {
    let mut k = cat(PREFIX_STATE, addr);
    k.extend_from_slice(b"/a/");
    k
}
pub fn coderef_prefix(addr: &[u8]) -> Vec<u8> {
    let mut k = cat(PREFIX_STATE, addr);
    k.extend_from_slice(b"/c/");
    k
}
pub fn slot_prefix(addr: &[u8], slot: &[u8]) -> Vec<u8> {
    let mut k = cat(PREFIX_STATE, addr);
    k.extend_from_slice(b"/s/");
    k.extend_from_slice(slot);
    k.push(b'/');
    k
}

fn join(prefix: &[u8], parts: &[&[u8]]) -> Vec<u8> {
    let mut k = prefix.to_vec();
    for p in parts {
        k.extend_from_slice(p);
        k.push(b'/');
    }
    k
}
pub fn addr_prefix(addr: &[u8]) -> Vec<u8> {
    join(PREFIX_ADDR, &[addr])
}
pub fn elog_prefix(emitter: &[u8]) -> Vec<u8> {
    join(PREFIX_ELOG, &[emitter])
}
pub fn elog_group(emitter: &[u8], topic0: &[u8]) -> Vec<u8> {
    join(PREFIX_ELOG, &[emitter, topic0])
}
pub fn tval_prefix(value: &[u8]) -> Vec<u8> {
    join(PREFIX_TVAL, &[value])
}
pub fn tval_group(value: &[u8], topic0: &[u8]) -> Vec<u8> {
    join(PREFIX_TVAL, &[value, topic0])
}
pub fn sig_group(topic0: &[u8]) -> Vec<u8> {
    join(PREFIX_SIG, &[topic0])
}
pub fn set_prefix(topic0: &[u8], pos: u8, value: &[u8]) -> Vec<u8> {
    join(PREFIX_SET, &[topic0, &[pos], value])
}
pub fn set_key(topic0: &[u8], pos: u8, value: &[u8], emitter: &[u8]) -> Vec<u8> {
    let mut k = set_prefix(topic0, pos, value);
    k.extend_from_slice(emitter);
    k
}
pub const SET_SPLIT: usize = 4 + 32 + 1 + 1 + 1 + 32 + 1;
pub const SET_KEY_LEN: usize = SET_SPLIT + 20;

pub fn suffixed(prefix: &[u8], txnum: u64) -> Vec<u8> {
    let mut k = prefix.to_vec();
    k.extend_from_slice(&txnum.to_be_bytes());
    k
}
pub fn txnum_of(key: &[u8]) -> u64 {
    u64::from_be_bytes(key[key.len() - 8..].try_into().unwrap())
}

pub fn payload_bits(group: &[u8]) -> u32 {
    if group.starts_with(PREFIX_ADDR) {
        5
    } else if group.starts_with(PREFIX_TVAL) {
        3
    } else {
        0
    }
}

/// The bloom prefix length of a key (format.go split).
pub fn split(key: &[u8]) -> usize {
    let n = key.len();
    if key.starts_with(PREFIX_STATE) {
        if n > 27 && key[27] == b's' {
            return if n >= 62 + 8 { 62 } else { n };
        }
        return if n >= 29 + 8 { 29 } else { n };
    }
    if key.starts_with(PREFIX_ADDR) || key.starts_with(PREFIX_ELOG) {
        if n >= 26 + 8 {
            return 26;
        }
    } else if key.starts_with(PREFIX_TVAL) {
        if n >= 38 + 8 {
            return 38;
        }
    } else if key.starts_with(PREFIX_SIG) {
        if n >= 37 + 8 {
            return 37;
        }
    } else if key.starts_with(PREFIX_SET) && n >= SET_SPLIT + 20 {
        return SET_SPLIT;
    }
    n
}

pub fn run_label(level: i32, from_tx: u64, to_tx: u64) -> String {
    let kind = if level >= TERMINAL_LEVEL { "t".to_string() } else { format!("l{level}") };
    format!("{kind}-{from_tx:016}-{to_tx:016}")
}

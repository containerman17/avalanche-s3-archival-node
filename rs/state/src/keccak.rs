//! keccak256 as the Ethereum trie uses it (pad 0x01, not SHA3's 0x06).

pub type Hash = [u8; 32];

#[cfg(feature = "asm")]
pub fn keccak256(data: &[u8]) -> Hash {
    use keccak_asm::Digest;
    let mut h = keccak_asm::Keccak256::new();
    h.update(data);
    h.finalize().into()
}

#[cfg(not(feature = "asm"))]
pub fn keccak256(data: &[u8]) -> Hash {
    use tiny_keccak::Hasher;
    let mut h = tiny_keccak::Keccak::v256();
    h.update(data);
    let mut out = [0u8; 32];
    h.finalize(&mut out);
    out
}

/// keccak256 of RLP(""), the root of an empty trie.
pub const EMPTY_ROOT: Hash = [
    0x56, 0xe8, 0x1f, 0x17, 0x1b, 0xcc, 0x55, 0xa6, 0xff, 0x83, 0x45, 0xe6, 0x92, 0xc0, 0xf8, 0x6e,
    0x5b, 0x48, 0xe0, 0x1b, 0x99, 0x6c, 0xad, 0xc0, 0x01, 0x62, 0x2f, 0xb5, 0xe3, 0x63, 0xb4, 0x21,
];

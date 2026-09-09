//! Signed transfers for tests and the bench: one secp256k1 key, EIP-1559
//! envelopes, nothing a real chain needs.
use alloy_primitives::{keccak256, Address, U256};
use bytes::Bytes;
use secp256k1::{Message, PublicKey, Secp256k1, SecretKey};

pub struct Signer {
    secp: Secp256k1<secp256k1::All>,
    sk: SecretKey,
    pub address: Address,
}

impl Signer {
    pub fn new(seed: [u8; 32]) -> Signer {
        let secp = Secp256k1::new();
        let sk = SecretKey::from_byte_array(seed).expect("seed");
        let pk = PublicKey::from_secret_key(&secp, &sk).serialize_uncompressed();
        Signer { secp, sk, address: Address::from_slice(&keccak256(&pk[1..])[12..]) }
    }

    /// A signed EIP-1559 transfer: 0x02 || rlp([chainId, nonce, tip, cap, gas, to, value, data, [], yParity, r, s]).
    #[allow(clippy::too_many_arguments)]
    pub fn transfer(&self, chain_id: u64, nonce: u64, tip: u128, cap: u128, gas: u64, to: Address, value: U256) -> block::Tx {
        use alloy_rlp::Encodable;
        let mut body = Vec::new();
        chain_id.encode(&mut body);
        nonce.encode(&mut body);
        tip.encode(&mut body);
        cap.encode(&mut body);
        gas.encode(&mut body);
        to.encode(&mut body);
        value.encode(&mut body);
        alloy_primitives::Bytes::new().encode(&mut body);
        body.push(0xc0);
        let mut unsigned = vec![2u8];
        alloy_rlp::Header { list: true, payload_length: body.len() }.encode(&mut unsigned);
        unsigned.extend_from_slice(&body);
        let sig = self.secp.sign_ecdsa_recoverable(Message::from_digest(keccak256(&unsigned).0), &self.sk);
        let (rec, rs) = sig.serialize_compact();
        (i32::from(rec) as u64).encode(&mut body);
        U256::from_be_slice(&rs[..32]).encode(&mut body);
        U256::from_be_slice(&rs[32..]).encode(&mut body);
        let mut out = vec![2u8];
        alloy_rlp::Header { list: true, payload_length: body.len() }.encode(&mut out);
        out.extend_from_slice(&body);
        let mut t = block::eth::decode_tx(Bytes::from(out)).expect("own tx decodes");
        t.sender = block::recover(&t);
        debug_assert_eq!(t.sender, Some(self.address));
        t
    }
}

/// A private chain's genesis JSON: mainnet's fork schedule (network 1),
/// genesis at 0 (LONDON), one funded account, a 100M gas limit.
pub fn genesis_json(funded: Address) -> String {
    format!(
        r#"{{"config":{{"chainId":99999,"homesteadBlock":0,"eip150Block":0,"eip155Block":0,"eip158Block":0,"byzantiumBlock":0,"constantinopleBlock":0,"petersburgBlock":0,"istanbulBlock":0,"muirGlacierBlock":0,"berlinBlock":0,"londonBlock":0,"subnetEVMTimestamp":0,
        "feeConfig":{{"gasLimit":100000000,"minBaseFee":25000000000,"targetGas":150000000,"baseFeeChangeDenominator":36,"minBlockGasCost":0,"maxBlockGasCost":1000000,"targetBlockRate":2,"blockGasCostStep":200000}},"allowFeeRecipients":true}},
        "nonce":"0x0","timestamp":"0x0","extraData":"0x00","gasLimit":"0x5f5e100","difficulty":"0x0","mixHash":"0x0000000000000000000000000000000000000000000000000000000000000000","coinbase":"0x0000000000000000000000000000000000000000",
        "alloc":{{"{funded:x}":{{"balance":"0x1027e72f1f12813088000000"}}}},"number":"0x0","gasUsed":"0x0","parentHash":"0x0000000000000000000000000000000000000000000000000000000000000000"}}"#
    )
}

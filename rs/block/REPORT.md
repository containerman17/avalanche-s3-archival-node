# rs-block report (2026-09-08, branch rs-block, not pushed)

## Part 1: the dump

Tool: `cmd/epochdb-dump-containers/main.go` (new command, no existing Go file touched). It opens the manifest's runs read-only with `store.OpenRunVersion(cas, name, 3)`, walks blk/hdr/pvm/tx by range scans (the same `scanBlocks` as epochdb-archive-serve) and writes `store.Reassemble(pvm, hdr, txs)` per height:

    [u64 LE height][u32 LE len][container bytes] ...   heights ascending, contiguous

Usage (same environment as archive-serve: `EPOCHDB_CACHE_DIR`, `EPOCHDB_S3_*`; `--dir` basename must be `data` so the chunk-cache namespace matches):

    epochdb-dump-containers --dir <dir>/data --manifest <dir>/data/manifest.json --version 3 --from 1 --to 1000000 --out step-containers-1-1000000.bin

Static build (blst and secp256k1 need cgo): `CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build -tags netgo,osusergo -ldflags '-linkmode external -extldflags "-static"' -o epochdb-dump-containers ./cmd/epochdb-dump-containers`.

Files:
- Tokyo box (`ec2-user@18.183.176.223`): `/data/epochdb-v0/tmp/rs/step-containers-1-1000000.bin` (3,176,734,443 bytes, 1M blocks, 4,087,552 txs, 451.7 Ggas), `step-containers-1-50000.bin` (157,527,739 bytes), `chain.json`, `upgrade.json`. Also `/data/epochdb-v0/tmp/rs/data/` (my own data dir: manifest + chain + upgrade copies, resident sidecars), `run.sh`, `dump.log`, the dump binary at `/data/epochdb-v0/tmp/epochdb-dump-containers`, and the musl `blockcheck`. Nothing under the archive server's dir was touched; it ran as root (the shared chunk cache is root-owned) under nice 10 / ionice idle. The 1M dump took 4 s (chunks were already cached by the archive server), 228k blk/s.
- This machine: `/tmp/claude-1000/-home-ilia-epochdb/222c563c-789d-46b6-8726-b8af4b2a6f62/scratchpad/rs/step/` has the 50k file, chain.json, upgrade.json and a README (announced there for rs-exec).

Heights 1..1,000,000 are all in the first terminal run (1..1,233,822). Height 1 is a bare pre-fork block; every block from 2 on is a proposervm statelessBlock (type 0).

## Part 2: the crate `rs/block` (package `epochdb-block`, lib name `block`)

Standalone crate, own Cargo.lock, `target/` ignored. Deps: alloy-primitives (rlp, asm-keccak), alloy-rlp, bytes, memmap2, rayon, secp256k1 0.31 (C, recovery), sha2.

Modules:
- `dump`: `Dump::open(path)` mmaps the file into one `bytes::Bytes` (`Bytes::from_owner`), `dump.records(from, to)` iterates `Record { height, container: Bytes }` zero-copy (`to` inclusive, `u64::MAX` = to the end; a `from` deep in the file skips records by their length prefix).
- `pvm`: the avalanchego linearcodec port: `[u16 version 0][u32 type]`, type 0 statelessBlock (parentID 32, timestamp i64, pChainHeight u64, cert bytes, block bytes, signature bytes), type 2 granite (same plus epoch u64,u64,i64 before the signature), type 1 option. Container id = sha256(bytes without the trailing `[u32 len][signature]`), option = sha256(all). Not a codec block: bare RLP list, trailing bytes stripped, id = keccak(header).
- `eth`: `decode_block(inner) -> (header_rlp: Bytes, Vec<Tx>)`, `decode_header(rlp) -> Header` with subnet-evm's optionals in order BaseFee, BlockGasCost, BlobGasUsed, ExcessBlobGas, ParentBeaconRoot, TimeMilliseconds, MinDelayExcess. Tx types 0 (legacy, EIP-155 or pre-155), 1 (EIP-2930), 2 (EIP-1559); anything else errors with the type byte. `Tx.raw` is the envelope (legacy list, or type byte + payload, what eth_getRawTransaction returns), `Tx.hash = keccak(raw)`, `input` and `raw` are slices of the mmap.
- `sender`: `sighash(tx)` re-wraps the unsigned fields (`raw[body_off..sig_off]`) in a new list header, appends chainId,0,0 for EIP-155, prefixes the type byte for typed txs; `recover(tx)` is libsecp256k1 `recover_ecdsa` then keccak of the 64-byte public key.
- `lib`: `Block { height, hash, container_id, header, header_rlp, txs, container, pvm: Option<Pvm { parent_id, timestamp, pchain_height }> }`, `decode(record)`, `Blocks::open(dump, from, to)` (sequential, senders None), `recovered(blocks, workers) -> Recovered` (a thread decodes and recovers chunks of 256 blocks on a rayon pool of `workers` threads, 16 chunks buffered ahead, height order preserved).
- `blockcheck` binary (`src/main.rs`): the oracle runner. `blockcheck <dump> [--from N] [--to N] [--workers N] [--no-recover] [--index FILE] [--senders FILE] [--show h1,h2]`. Exit 1 on any decode error, parent-hash break, header number mismatch, index miss, or unrecovered sender.
- `tools/senders_oracle.py <senders.txt> <rpc url> [blocks] [step]`: batches eth_getBlockByNumber(full) and compares block hash, tx hash and `from` (case-insensitive; the door blocks Python's User-Agent, the script sends curl's).

## Oracles

- Parent-hash chain and header numbers: every block of the 1M dump links to the previous one's keccak(header RLP) (checked on the box), and the 50k file locally. So keccak(header RLP) == block hash holds chain-wide (the door also returned the same block hashes for the 500 sampled blocks).
- Container id: `--index /data/epochdb-v0/tmp/archive/step/index` on the box: all 1,000,000 ids found at their height, 0 misses (the index is the Go server's `proposerblock.ParseWithoutVerification(c).ID()`).
- Senders: 500 blocks (every 97th of 1..50000), 3,445 txs, hash and from equal to the door's eth_getBlockByNumber, 0 mismatches. Recovery never failed on any of the 4,087,552 txs of the 1M dump.
- Tx types in 1..1M: 3,741,310 legacy, 346,242 EIP-1559, no EIP-2930, nothing else.

## Throughput (decode + recovery, blocks/s and txs/s)

- Local (i7-10700K, 8 cores / 16 threads), 50k file (343,731 txs): 14 workers 25.8k blk/s, 177k tx/s (1.94 s); 1 worker 3.7k blk/s, 25.6k tx/s (39 us per recovery); decode only, sequential: 58k blk/s, 402k tx/s (0.86 s; 41k blk/s before asm-keccak).
- Box (Xeon 8559C, 8 vCPU, live fleet on it, nice 10), 1M file, 6 workers, musl static binary: 18.1k blk/s, 74k tx/s, 55.2 s for 1,000,000 blocks / 4,087,552 txs. That binary was built before asm-keccak (recovery-bound, so the difference is small).
- Reference: Go's sender pool spent 17.1 s CPU per 30 s wall on Step.

## Deviations and notes

- `pvm_bytes` from the API sketch is `container: Bytes` (the whole verbatim container) plus the parsed `pvm: Option<Pvm>`; the wrapper's prefix/suffix alone are not useful to the executor.
- The dump is written whole to a `.tmp` and renamed; no per-record checksum (the container ids and hashes are the check).
- Uncles are not decoded (subnet-evm has none); `--show` and the index check only exist in blockcheck.
- No low-s check in `recover` (libsecp256k1 rejects out-of-range r/s; the chain's txs are already valid), no unit tests: blockcheck plus the RPC oracle are the runnable checks.
- musl target: `rustup target add x86_64-unknown-linux-musl` and `apt-get install musl-tools` were enough; `cargo build --release --target x86_64-unknown-linux-musl` builds secp256k1-sys's C with musl-gcc.

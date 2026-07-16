# epochdb design

## Principle
Bodies + headers + proposervm extras are the only ground truth. State history, receipts, logs, all indexes are caches rebuildable by re-execution (~5-6k blk/s). Re-executing one block is the universal primitive: receipts, traces, and getLogs results are computed on demand, never stored.

## Target architecture (v2, after measurement)
- Immutable per-epoch files, self-described by filename: epoch_<startblock>_<blockcount>.epoch. No catalogue; startup = readdir + read footers.
- Epoch file sections: bodies (frame-per-block zstd with a published global dict), headers, proposervm extras, state write-log SST of (account,key,^block)->NEW value rows, EF fp48 tx index, log posting lists (position-agnostic addr/topic -> EF block lists), per-epoch key bloom, footer (offsets, version, signature).
- Tx index: Elias-Fano over sorted 48-bit hash fingerprints (~20 bits/tx in RAM) + 27-bit block numbers on disk (~47 bits/tx total, ~7GB at 1.2B mainnet txs). Verify by decoding candidate block. No MPHF (EF matches its size, simpler).
- Compression: ONE global zstd dict published as an artifact, pinned zstd version, so independently built epoch files are bit-identical (torrent/S3 distributable, verifiable by re-cook).
- No database anywhere. Hot period = append-only logs + RAM maps; sealing an epoch = rewriting those logs into sorted compressed sections. Everything mmap'd; recent blooms pinned, old ones page-cached.
- LRU value cache key->(lastWriteBlock,value), ~1M entries, absorbs tip-heavy eth_call traffic.
- Historical state read at block N: LRU, then hot RAM map, then per-epoch blooms newest-to-oldest from epoch(N), one SST seek on first hit.
- getLogs: intersect posting lists -> candidate blocks -> re-execute -> exact filter. Range capped at 10k blocks.
- Tip following: lag N blocks behind accepted tips from 2-3 cross-checked public RPCs. No consensus, no P-chain.
- Ingest: naked p2p GetAncestors racing peers (deforestationdb-proven), no full node.
- eth_getProof over history: impossible by construction (no tries), documented as unsupported.

## v1 scope (this build)
Measurement build on Fuji. NO sealing, NO compression, NO EF, NO blooms. Append-only files + RAM maps. Deliverables: p2p fetch of full Fuji history, forward replay with per-block state root verification (Firewood frontier), write capture to append-only writelog + code-by-hash store, historical overlay serving eth_call / eth_getBalance / eth_getStorageAt / eth_getCode at any block, A/B bench vs public Fuji archive RPC.
v1 must produce the unknown numbers: body compression ratio with a trained dict (offline experiment), total write-log size, unique (epoch,key) bloom pair count.

## Reference
Proven prior art: ~/deforestationdb (fetch+execute+overlay, 0 mismatches on 15k A/B checks vs archive RPC). Reference dep versions from its go.mod: go 1.25.8; github.com/ava-labs/avalanchego v1.14.2; github.com/ava-labs/avalanchego/graft/coreth v1.14.2; github.com/ava-labs/avalanchego/graft/evm v1.14.2; github.com/ava-labs/libevm v1.13.14-0.4.0.rc.2; github.com/ava-labs/firewood-go-ethhash/ffi v0.3.1 (indirect there).

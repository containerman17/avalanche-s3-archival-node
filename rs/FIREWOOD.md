# Firewood as the state engine: what it is, how it was proven, what it costs

Branch `rust-firewood` (on `rust` = f7e83a2). Firewood = ava-labs `firewood` v0.8.0, pure Rust, feature `ethhash`, a git dependency pinned to the `v0.8.0` tag (crates.io has no release past 0.3.1). The engine is `rs/node/src/firewood.rs`; the design is in `README.md` ("Firewood state engine"). Everything below was measured on the local i7-10700K (8c/16t), WSL2, 2026-09-09 JST, with the release profile (`debug = 1`, fat LTO), one run each unless stated.

## The contract Firewood wants (checked against libevm's `graft/evm/firewood` and Firewood's own hasher)

| what | key | value | delete |
|---|---|---|---|
| account | keccak(addr), 32 B | RLP[nonce, balance, storageRoot, codeHash]; field 2 is spliced by Firewood at hash time (`storage/src/hashers/ethhash.rs`, `replace_list_field(.., 2, ..)`), so we write the empty root; a read-back returns what was written, never trust field 2 | `DeleteRange { prefix: keccak(addr) }`, which takes the storage with it |
| slot | keccak(addr) ++ keccak(slot), 64 B | RLP(left-trimmed word) | `Delete { key }` for a zero word (an empty Put stores an empty value, it is not a delete) |
| empty trie | | root `0x56e81f17...b421` = keccak(0x80); `Db::root_hash()` answers it for a new db | |

Ops apply in order (serial path), or per first nibble in order (parallel path: 16 workers, one mpsc channel each), so the wipes go first and a recreate's puts follow. Our write set keeps its own form (33 B / 65 B keys, empty = delete); `Layer::ops` translates once per block. Unit test `root_matches_alloy_trie` proves the contract against alloy-trie's secure-trie root for two accounts, two slots and a prefix delete.

## Oracles

All bench runs: `epochdb-rs --dump DUMP --genesis chain.json --upgrade upgrade.json --data D --state firewood --workers 14 [--root-inline] [--fw-cache-mb N]`. A root mismatch on any block exits 1 from the checker; `root-checked=` on the `split total` line is the count of blocks whose Firewood root equalled the header's.

| oracle | shape | result |
|---|---|---|
| Step 50k | pipelined | `blocks=50000 root-checked=50000`, 12 s, cum 1,980 mgas/s, RSS 336 MB, propose 6.2 s + commit 2.6 s, 32,142,102 B on disk |
| Step 50k | `--root-inline` | `root-checked=50000`, 17 s, executor waited 9.8 s for the checker |
| Step 50k, `--fw-kv-cache-mb 512` | pipelined | `root-checked=50000`, trie reads 566,898 -> 190,818 (kv hits 482,437), same root `0x0ecabf24...` |
| Step 1M, `--fw-cache-mb 4096` | pipelined | `blocks=1000000 root-checked=1000000`, 685 s, cum 660.7 mgas/s, peak RSS 3,850 MB, propose 310 s + commit 59 s, 13,771,290 trie reads, 622,483,264 B on disk (one file `firewood.db`) |
| Step 1M, `--fw-cache-mb 4096` | `--root-inline` | `root-checked=1000000`, 962 s, cum 470.2 mgas/s, peak RSS 3,835 MB, propose 292 s + commit 51 s, the executor waited 391.6 s for the checker, 19,321,611 trie reads, same root `0xb0a501d5...9445` and 622,483,264 B |
| beam 1M, `--fw-cache-mb 4096` | pipelined | `root-checked=1000000`, 391 s, cum 178.3 mgas/s (exec-thread 657.5), peak RSS 1,143 MB, propose 219.7 s + commit 150.9 s (1M commits, each waiting for the persist thread), 6,333,014 trie reads, 263,577,758 B on disk; genesis root `0xe3f8738b...bb60` |
| beam 1M, `--fw-cache-mb 4096` | `--root-inline` | `root-checked=1000000`, 378 s, cum 184.5 mgas/s, peak RSS 1,144 MB, propose 167.6 s + commit 54.5 s, the executor waited 265.2 s, 9,393,203 trie reads, same root `0xc0df2d77...c74e`, 263,577,758 B |

Genesis roots: Step `0x51736d52...0d21` from the alloc into the first proposal (equal to the alloy-trie recompute); beam's from its chain.json header.

Under the Go harness (`cmd/epochdb-host-bench`, `--batch 256`, `--config '{"state-sync-enabled":false,"state-engine":"firewood","firewood-kv-cache-mb":256}'`), Step 50k:

- fresh: 45 s, 1,107 blk/s, `root-checked=50000`, `check ... match=true` and `check genesis ... match=true`; `Shutdown took 3.22s` (the final commit and Firewood's close).
- restart on the same data: `recovered: firewood at 50000 (root 0x0ecabf24...), head 50000, rows replayed 0, 23 code blobs, root ok, in 6 ms`.
- `kill -9` of the plugin 15 s in (head 14,004): `recovered: firewood at 13984 (root 0x0dd08461...), head 14004, rows replayed 305, root ok, in 187 ms`, the run continued to 50,000 with both checks `match=true`; `storecheck verify --inner --postings-every 5` on that store: `verify OK: 50000 blocks, 343731 txs, 1024939 state rows, 104006 posting checks`.

Firewood's persisted revision was 20 blocks behind the store's head after the kill: the checker commits the proposal chain every 32 blocks right after `store.sync()`, so the persisted root is always at or behind the store (the replay covers the rest). The other direction (Firewood ahead of the store) cannot be recovered without a rollback API, which is why the commit waits for the fsync.

## Roll equivalent and disk

Firewood has no roll: every commit is a revision, old revisions past `max_revisions` (128) are reaped and their nodes' space goes to the free list, so the file holds the current state plus the last 128 deltas. What replaces our files: `run.N` (the sorted flat state) -> Firewood's node store (trie nodes in one file, branch nodes with child addresses and hashes, leaves with the value); `trie.N` (the rolled hash file) -> the same nodes (hashes live in the branch children); MANIFEST -> Firewood's header (persisted root address) + the store's headers to find the height.

| dump | native `run.N` + `trie.N` (+ MANIFEST) | Firewood `firewood.db` |
|---|---|---|
| Step, 1M blocks | not measured whole (the A/B stops at 180 s); at 894,408 blocks (5 rolls, `--roll-budget 256`) 345,569,787 B; `node/REPORT.md`'s roll at 495k: run 111 MB + trie 95 MB | 622,483,264 B (one file, free-list space and the last 128 revisions included) |
| Step, 691k blocks (the 180 s A/B) | 345,569,787 B at 894k (above) | 546,675,328 B at 691k |
| beam, 1M blocks | 51,780,163 B (run.1 20.1 MB + trie.1 31.7 MB) | 263,577,758 B |

Firewood's file is 1.8x (Step) to 5x (beam) ours: a trie node per key with 16 child addresses and hashes per branch, plus the reaped revisions' free space, against our sorted flat run plus a hash file with only the branch hashes.

## A/B (same machine, same hour, sequential)

Step 1M, `--duration 180 --workers 14`, native with `--roll-budget 256`; beam 1M whole dump. Load average before each run below 3. Firewood settings: `max_revisions` 128, `deferred_persistence_commit_count` 1, `use_parallel` `BatchSize(8)` (its default), node cache as listed.

All seven runs back to back at 13:30-13:55 JST with the validator load tests paused (load average 2.0-3.4 at each start, the idle avalanchego test nodes only), Step 1M, `--duration 180 --workers 14`, one run each; blocks = executed and root-checked in the 180 s (both shapes check every block: pipelined one block behind, inline before the next block).

| engine | shape | blocks in 180 s | cum mgas/s | blk/s | peak RSS | RssAnon 10 s -> 180 s | split (s) | disk |
|---|---|---|---|---|---|---|---|---|
| native, `--roll-budget 256` | pipelined | 894,408 | 1,335 | 4,969 | 753 MB | 181 -> 257 (peak 385) | evm 139.8 trace 15.7 commit 4.8; checker apply 1.4 root 67.2; 5 rolls | 345.6 MB |
| native | `--root-inline` | 780,742 | 991 | 4,337 | 657 MB | 104 -> 161 | evm 58.0 trace 7.0 commit 4.4; root 54.2; executor waited 88.9 | 341.5 MB |
| firewood, cache 4 GB | pipelined | 691,486 | 879 | 3,842 | 3,291 MB | 257 -> 3,092 | evm 74.2 trace 5.8 commit 5.5; checker propose 127.2 commit 37.5 | 546.7 MB |
| firewood, cache 4 GB | `--root-inline` | 520,821 | 614 | 2,893 | 1,817 MB | 194 -> 1,704 | evm 36.9 trace 3.4 commit 3.8; propose 73.8 commit 21.6; executor waited 119.6 | 297.5 MB |
| firewood, cache 4 GB, `--fw-kv-cache-mb 512` | pipelined | 686,284 | 874 | 3,813 | 3,431 MB | 288 -> 3,245 | evm 63.2 (kv hits 3,353,236, trie reads 4,604,650 -> 2,814,464, 2 clears); propose 124.1 commit 40.0 | 545.5 MB |
| firewood, cache 192 MB (default) | pipelined | 694,362 | 882 | 3,858 | 531 MB | 253 -> 241 (peak 338) | evm 74.0; propose 124.6 commit 39.0 | 547.3 MB |
| firewood, cache 4 GB, keccak-asm build | `--root-inline` | 530,206 | 624 | 2,946 | 1,811 MB | 199 -> 1,736 | evm 38.7; propose 70.9 commit 22.0; executor waited 117.3 | 303.4 MB |

Reading it:

- Pipelined, Firewood reaches 66 percent of native's cum mgas/s (879 vs 1,335) and 77 percent of its blk/s; inline 62 percent (614 vs 991). In both shapes the checker is the bound: propose + commit = 165 s of the 180 s pipelined (native's Dirty root: 69 s), so the executor idles 40 percent of the time (evm 74 s), where native's executor runs 140 s of the 180.
- The 4 GB node cache buys nothing on Step 1M (691k vs 694k blocks at 192 MB): the whole trie is ~550 MB on disk and sits in the page cache either way; the cache only saves deserialization. What the 4 GB cache does is fill RSS: RssAnon grows 257 -> 3,092 MB in 180 s (about 1 GB per 100k blocks of new nodes) and plateaus at 3.6 GB in the 1M runs. At 192 MB RssAnon is flat (253 -> 241 MB, peak 338), so there is no leak in the pure-Rust use over 700k blocks: the Go-side observation (a few hundred MB/h under cgo) is not reproduced here; what grows is the bounded cache.
- The kv read cache saves the executor 11 s of evm time (trie reads down 39 percent, 3.35M hits) but the run is checker-bound, so the block count is unchanged; it pays off only in the inline shape or once propose gets faster. 2 clears in 180 s at 512 MB (the bound counts key + value + 64 B per entry).
- keccak-asm: propose 73.8 -> 70.9 s (4 percent) on the mixed 1M window, 8 percent on Step 50k; the hasher is not the lever it looked like.
- Beam 1M whole dump (above, oracles): native 97 s pipelined / 180 s inline vs Firewood 391 s / 378 s. Beam's blocks are tiny (1.36 txs on average) so the per-proposal and per-commit fixed cost dominates: 1M commits at 55-150 s (each commit hands the revision to the persist thread and, with `deferred_persistence_commit_count` 1, waits for the previous one to be written), 1M proposals at 168-220 s. Native's Dirty root over the same blocks: 59-65 s.

## Where Firewood spends its time (measured, not reasoned)

### Serial vs parallel proposals

Step 50k `--root-inline` (small blocks: 6.9 txs and ~20 state rows per block on average), one run each, `--fw-parallel`:

| use_parallel | propose (50k blocks) | commit | per block |
|---|---|---|---|
| `never` | 4.49 s | 2.13 s | 90 us propose |
| `auto` = `BatchSize(8)` (Firewood's default, what every other number here used) | 5.69 s | 2.03 s | 114 us |
| `always` | 14.89 s | 2.39 s | 298 us |

The parallel path (`ParallelMerkle`: the root forced into a branch, one worker per first nibble fed over an mpsc channel, the coordinator re-hashes the root) costs ~200 us of dispatch per proposal, which a 20-op batch never earns back; `auto` pays it on every block with 8+ ops. On the contract-heavy Step tail (hundreds of rows per block) the parallel path is what makes propose keep up (see the 1M split: propose 310 s over 1M blocks, 6-9 s per 10 s window at 700-800k while the EVM took 8.5 s).

### The hasher: software `sha3` vs `keccak-asm` (candidate upstream PR)

Firewood's ethhash hashes with `sha3::Keccak256` (the `sha3` crate's portable keccak-f[1600], the `keccak::backends::soft::keccak_p` symbol in the profile). A worktree of Firewood v0.8.0 (`~/.herdr/worktrees/firewood/keccak-asm`, branch `keccak-asm`) swaps `storage`'s three hashing sites to `keccak_asm::Keccak256` (the same crate alloy's `asm-keccak` uses; `sha3` 0.12 has no asm feature). The diff is `$S/rs/firewood/firewood-keccak-asm.patch` (storage/Cargo.toml + 4 files, 33 lines; a `From<digest::Output<Keccak256>> for TrieHash` bridges digest 0.10's `GenericArray` to Firewood's `hybrid-array`). The bench binary for the experiment was built with a temporary path dependency on that worktree; the committed `Cargo.toml` keeps the pinned git dependency.

Step 50k `--root-inline`, two runs each, same binary otherwise:

| hasher | propose | commit | executor waited |
|---|---|---|---|
| `sha3` soft (v0.8.0) | 5.89 s / 5.78 s | 2.01 / 1.98 s | 10.34 / 10.16 s |
| `keccak-asm` | 5.37 s / 5.36 s | 1.94 / 1.95 s | 9.58 / 9.62 s |

8 percent off propose on small blocks (the profile's 30 percent keccak share is of the checker thread's samples, of which propose is ~70 percent; keccak-asm is about 2x the soft backend on 136-byte inputs, so ~10 percent was the ceiling). On the mixed Step 1M 180 s window (the A/B's last row) propose went 73.8 -> 70.9 s (4 percent), the kv-cache-free executor unchanged. A clear but small win; the patch is upstream-ready as written (`$S/rs/firewood/firewood-keccak-asm.patch`), the larger levers are elsewhere (below).

### Per-block fixed cost

`fw_micro` (ignored unit test, `cargo test --release -p epochdb-node -- --ignored --nocapture fw_micro`): propose + commit of a 1 / 20 / 200-op batch over a 200,000-account state, 2,000 blocks each:

| ops per block | `never` (serial) | `auto` (parallel from 8 ops) |
|---|---|---|
| 1 | 18.7 us | 18.1 us |
| 20 | 390 us | 882 us |
| 200 | 3,202 us | 3,734 us |

Measured at load average ~40 (the validator tests were running), so the absolute values are 2-4x a quiet box (the bench saw 90 us per 20-op block serial at load 2); the shape holds: ~18 us per proposal + commit is the floor (a `NodeStore` per proposal, the root re-hash, the commit's handoff to the persist thread), the parallel path adds ~500 us of dispatch per proposal at 20 ops and is still slower at 200 ops on one 8-core box, and the per-op cost is 16-20 us serial (a 200k-account trie is 5 levels deep: ~5 node copies, ~5 hashes and one leaf per op).

### Profile (perf, 499 Hz, dwarf call graphs) of Step 50k `--root-inline`

Thread split of the whole process: the executor thread 18.3 percent of samples (EVM + secp256k1 sender recovery is spread over the 14 pool threads at ~4 percent each), the checker 10.6 percent, Firewood's persist worker 8.7 percent. Inside the checker thread (propose + commit, 100 percent = its own samples):

| bucket | share | what |
|---|---|---|
| keccak | 30 percent | `sha3::Keccak256::finalize_into` 14.9 + `keccak::backends::soft::keccak_p` 14.0 + `update` 1.2: Firewood hashes with the `sha3` crate's SOFTWARE keccak (no asm feature); our own hashing (alloy `asm-keccak`) is elsewhere |
| node clone / alloc / drop | 31 percent | `BranchNode::clone_one` 15.9 (a proposal copies every branch node on the path before it edits it), `SmallVec` clones, `Children` / `Child` drops, jemalloc |
| ethhash encoding | 15 percent | `hashednode::hash_node` 5.1, `BranchNode::children_hashes` 2.5, `rlp::encode_list` 2.0, `write_bytes` 0.8, `fix_account_storage_root_value` 0.4 (the account special case is small) |
| node reads | 3 percent | `read_cached_node` 3.3, `as_shared_node`: Step 50k fits the node cache |
| trie mutation | 2.5 percent | `Merkle::insert` / `remove_prefix` |
| dispatch / sync | 1.7 percent | rayon / crossbeam / channels |
| commit | 1.2 percent | `Proposal::commit` itself; the write side runs on the persist worker |
| ours | 1.5 percent | `Layer::ops` 0.9, `Committer::propose` 0.65 |

The persist worker (commit's write side): `process_unpersisted_nodes` + `write_batch` 46 percent (node serialization into the file, free-list allocation), node clones and drops 30 percent, `insert_into_cache` 5 percent, `rapidhash` (the cache's hasher) 3 percent.

Per-block fixed overhead vs per-node: see `fw_micro` above; on Step 50k the checker spent 90 us per block serial for ~20 ops, i.e. hashing ~20 leaves plus their ~5-deep branch paths (~100 nodes), 0.9 us per node all-in.

### Profile of the contract-heavy window (Step 700k-880k, `--root-inline`, 4 GB cache)

`perf record -F 499 -p PID -- sleep 240` attached to the 1M inline run once it passed 700,000 (blocks 708k-883k in the window, 382,441 samples; the validator load tests were running, load ~7, relative shares only). Thread roles by top symbol:

| role | share of all samples | threads |
|---|---|---|
| executor (EVM, trace JSON, our layer maps) | 40.5 percent | 1 |
| Firewood propose: the checker + `ParallelMerkle`'s rayon workers | 33.0 percent | 13 |
| sender recovery pool (secp256k1) | 12.2 percent | 14 |
| Firewood persist worker (commit's write side) | 11.7 percent | 1 |

Inside the propose side (checker + workers, 100 percent = their samples), the buckets the coordinator asked for:

| bucket | share | symbols | fixable by |
|---|---|---|---|
| worker dispatch and idle spinning | 32.2 percent | `crossbeam_epoch::pin` / `try_advance` 7.5, `Stealer::steal` 1.9, `futex::Mutex::lock_contended` 1.8, `rayon_core::wait_until_cold`, channel sends: 16 workers fed one op at a time over mpsc channels, spinning between batches | a PR: persistent workers that take whole sub-batches, or one worker per touched subtrie only (`UseParallel::Never` is faster below ~100 ops) |
| keccak itself | 17.6 percent | `keccak::backends::soft::keccak_p` 12.9, `Keccak256::finalize_into` 9.5 (software backend) | a PR: `keccak-asm` (measured 4-8 percent of propose, this table says the ceiling on this window is ~9 percent) |
| node copies in `read_for_update` | 13.8 percent | `Child::clone` 11.8, `BranchNode::clone_one`, `Box<BranchNode>::clone` 3.3, `memmove` 5.3: a proposal copies every branch node on every touched path (16 `Child` entries with their hashes) before it edits one child | a PR: copy-on-write per child / `Arc::unwrap_or_clone` only when shared, or edit in place inside one proposal |
| allocation / drop | 12.5 percent | jemalloc `edata_heap_remove_first` 3.2, `drop_glue::<Child>`, `SmallVec::from_iter` | follows from the copies above |
| ethhash encoding | 10.4 percent | `hash_node` 3.7, `children_hashes` 2.1, `rlp::encode_list` 1.9, `nibbles_to_eth_compact`, `fix_account_storage_root_value` (the account special case is under 1 percent) | small; the RLP is what Ethereum hashing is |
| node reads / deserialize | 3.4 percent | `read_cached_node` 4.0 (cache hits; the 4 GB cache holds the tail's nodes) | n/a here |
| trie mutation (`Merkle::insert` / `remove_prefix`) | 2.9 percent | | n/a |

The commit side (persist worker, 11.7 percent of all samples): `process_unpersisted_nodes` 28.7 + `write_batch` and the persist loop 20.7 = 53 percent serialization and writes, `drop_glue::<Child>` and jemalloc 28 percent (the persisted nodes' in-memory copies are freed here), `insert_into_cache` + `rapidhash` 7 percent. With `deferred_persistence_commit_count` 1 every commit waits for the previous revision's write; this is the durability model (a revision per block, persisted in order), and raising the count trades crash-replay depth for commit latency (the replay is ours, 305 rows in 187 ms after the kill -9 above). Native's equivalent cost is the roll (merge + trie file) every 200k blocks, off the checker entirely.

Per-block fixed cost vs per-node: `fw_micro` puts the floor at ~18 us per proposal + commit (a `NodeStore` per proposal, root re-hash, the persist handoff) and 16-20 us per op serial on a 5-deep trie; on the contract-heavy window the propose side is per-node bound (copies + hashing + dispatch), on beam it is per-block bound (1.36 txs per block, 1M commits = 55-150 s).

## Memory

RssAnon sampled every 10 s from `/proc/self/status` (the `anon=` field of the bench line). Step 1M pipelined, 4 GB node cache: 258 MB at 10 s, 994 MB at 70 s, 1,795 MB at 130 s, 3,098 MB at 190 s, 3,390 MB at 310 s, 3,543 MB at 430 s, 3,620 MB at 550 s, 3,626 MB at 670 s; second-half slope 2,093 MB/h, last 120 s flat within 20 MB. That curve is the node cache filling to its 4 GB limit (the hot state of Step 1M is ~600 MB of nodes, the cache keeps written nodes only by default), not the Go-side leak: the 192 MB run below shows the plateau where the cache is bounded. The A/B's 192 MB row: RssAnon 253 MB at 10 s, 241 MB at 180 s (peak 338 MB) over 694k blocks, slope within noise.

## Deviations and open items

- Firewood's `Db` is leaked to `&'static` (a `Proposal<'db>` borrows it); `Committer::close` reclaims and closes it. One Firewood db per process.
- Reads that miss every in-memory layer walk the trie (`DbView::val`); the kv cache is optional and off by default; its eviction is a whole clear at the byte bound (`ponytail:` LRU when the clears show).
- The bench and the plugin commit differently: the bench commits every block (Firewood's shape), the plugin every 32 blocks after the store fsync (durability ordering). `--fw-deferred` (deferred persistence) is exposed but every number here used 1.
- Firewood's hashing uses software keccak; an `asm` keccak in Firewood would take ~30 percent off propose on small blocks.
- Not done: Firewood under a real avalanchego; the RPC `eth_getProof` could now be answered (Firewood has proofs) but is not wired.

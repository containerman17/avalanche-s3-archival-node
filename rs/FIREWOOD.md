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
| beam 1M | `--root-inline` | BEAM_INLINE |

Genesis roots: Step `0x51736d52...0d21` from the alloc into the first proposal (equal to the alloy-trie recompute); beam's from its chain.json header.

Under the Go harness (`cmd/epochdb-host-bench`, `--batch 256`, `--config '{"state-sync-enabled":false,"state-engine":"firewood","firewood-kv-cache-mb":256}'`), Step 50k:

- fresh: 45 s, 1,107 blk/s, `root-checked=50000`, `check ... match=true` and `check genesis ... match=true`; `Shutdown took 3.22s` (the final commit and Firewood's close).
- restart on the same data: `recovered: firewood at 50000 (root 0x0ecabf24...), head 50000, rows replayed 0, 23 code blobs, root ok, in 6 ms`.
- `kill -9` of the plugin 15 s in (head 14,004): `recovered: firewood at 13984 (root 0x0dd08461...), head 14004, rows replayed 305, root ok, in 187 ms`, the run continued to 50,000 with both checks `match=true`; `storecheck verify --inner --postings-every 5` on that store: `verify OK: 50000 blocks, 343731 txs, 1024939 state rows, 104006 posting checks`.

Firewood's persisted revision was 20 blocks behind the store's head after the kill: the checker commits the proposal chain every 32 blocks right after `store.sync()`, so the persisted root is always at or behind the store (the replay covers the rest). The other direction (Firewood ahead of the store) cannot be recovered without a rollback API, which is why the commit waits for the fsync.

## Roll equivalent and disk

Firewood has no roll: every commit is a revision, old revisions past `max_revisions` (128) are reaped and their nodes' space goes to the free list, so the file holds the current state plus the last 128 deltas. What replaces our files: `run.N` (the sorted flat state) -> Firewood's node store (trie nodes in one file, branch nodes with child addresses and hashes, leaves with the value); `trie.N` (the rolled hash file) -> the same nodes (hashes live in the branch children); MANIFEST -> Firewood's header (persisted root address) + the store's headers to find the height.

| after Step 1M | native (`run.N` + `trie.N`, from `node/REPORT.md` at the 495k roll: 111 MB + 95 MB, whole 1M measured below) | Firewood `firewood.db` |
|---|---|---|
| bytes | NATIVE_DISK | 622,483,264 (one file; free-list space included) |

## A/B (same machine, same hour, sequential)

Step 1M, `--duration 180 --workers 14`, native with `--roll-budget 256`; beam 1M whole dump. Load average before each run below 3. Firewood settings: `max_revisions` 128, `deferred_persistence_commit_count` 1, `use_parallel` `BatchSize(8)` (its default), node cache as listed.

AB_TABLE

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

8 percent off propose on small blocks (the profile's 30 percent keccak share is of the checker thread's samples, of which propose is ~70 percent; keccak-asm is about 2x the soft backend on 136-byte inputs, so ~10 percent was the ceiling). ASM_TAIL

### Per-block fixed cost

`fw_micro` (ignored unit test, `cargo test --release -p epochdb-node -- --ignored --nocapture fw_micro`): propose + commit of a 1 / 20 / 200-op batch over a 200,000-account state, 2,000 blocks each:

FW_MICRO

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

## Memory

RssAnon sampled every 10 s from `/proc/self/status` (the `anon=` field of the bench line). Step 1M pipelined, 4 GB node cache: 258 MB at 10 s, 994 MB at 70 s, 1,795 MB at 130 s, 3,098 MB at 190 s, 3,390 MB at 310 s, 3,543 MB at 430 s, 3,620 MB at 550 s, 3,626 MB at 670 s; second-half slope 2,093 MB/h, last 120 s flat within 20 MB. That curve is the node cache filling to its 4 GB limit (the hot state of Step 1M is ~600 MB of nodes, the cache keeps written nodes only by default), not the Go-side leak: the 192 MB run below shows the plateau where the cache is bounded. RSS192

## Deviations and open items

- Firewood's `Db` is leaked to `&'static` (a `Proposal<'db>` borrows it); `Committer::close` reclaims and closes it. One Firewood db per process.
- Reads that miss every in-memory layer walk the trie (`DbView::val`); the kv cache is optional and off by default; its eviction is a whole clear at the byte bound (`ponytail:` LRU when the clears show).
- The bench and the plugin commit differently: the bench commits every block (Firewood's shape), the plugin every 32 blocks after the store fsync (durability ordering). `--fw-deferred` (deferred persistence) is exposed but every number here used 1.
- Firewood's hashing uses software keccak; an `asm` keccak in Firewood would take ~30 percent off propose on small blocks.
- Not done: Firewood under a real avalanchego; the RPC `eth_getProof` could now be answered (Firewood has proofs) but is not wired.

# rs/state: Rust port of `latest` and `commit`

Branch `rs-state`, crate `rs/state` (lib name `state`), edition 2021, stable
cargo 1.97. Byte-compatible with the Go run files and node files. No Go
file was edited; the Go side of the oracle is the new `exp/rscompat`.

## Files

| File | What |
|---|---|
| `src/lib.rs` | `KvIter` trait (Go's Iterator: next/key/value), `RowsIter` over in-memory rows |
| `src/keccak.rs` | keccak256 via `keccak-asm` (default feature `asm`, XKCP assembly); `tiny-keccak` fallback with `--no-default-features` |
| `src/rlp.rs` | split/count/encode helpers for the trie (no alloy-rlp) |
| `src/run.rs` | `Writer` (front-coded 4 KB blocks, 40-byte prefix index, 76-byte footer with crc32) and `Run` (mmap, `get`, `iter(lo, hi)`) |
| `src/overlay.rs` | `Overlay`: byte slab + hashbrown `HashTable<u64>` of slab offsets, tombstone = empty value, sorted `iter` snapshot |
| `src/view.rs` | `View` (overlays newest first over runs), `get`, k-way `MergeIter`, `merge(dst, view, user) -> Run` |
| `src/commit/mod.rs` | constants, hex-prefix/nibble helpers, account row <-> leaf RLP, uvarint, `Error` |
| `src/commit/roll.rs` | `StackTrie` (arena port of geth's stacktrie, same emission order) and `roll(it, path, user) -> (root, Stats)` |
| `src/commit/file.rs` | `File`: mmap, footer parse, `root`, `storage_root(acct) -> (Hash, bool)`, `node(owner, path)` |
| `src/commit/trie.rs` | mutable MPT with lazy resolve by path through `NodeReader`, insert/delete with geth's collapse rules, `commit -> (root, NodeSet)` |
| `src/commit/dirty.rs` | `Dirty`: retained-node slab keyed by (owner, path), `apply`, `root()` with a scoped-thread pool for storage tries, `leaf()` fabricated from the rolled rows via `SeekFn` |
| `src/sample.rs` | rows files (`[klen u8][vlen u8][key][value]`), statedump -> contract-form conversion (as `exp/commitroot -sample`), xorshift `Rng`, synthetic state model (`gen_state`, `mutate`, `flatten`) |
| `src/bin/rscompat.rs` | Rust side of the oracle: `gen`, `runwrite`, `runcheck`, `roll`, `dirty`, `noderoot`, `rollsample` |
| `src/bin/bench.rs` | `bench latest <rows>` and `bench dirty` |
| `tests/latest.rs` | port of `latest_test.go` plus `block_layout` (format details) |
| `tests/commit.rs` | port of `commit_test.go` (Dirty rounds vs re-roll, file walk, corrupt footer) plus `dirty_edge_cases` |
| `../../exp/rscompat/main.go` | Go side of the oracle (uses `latest` and `commit` unchanged) |

## API summary

```rust
// latest
let mut w = run::Writer::create(path)?; w.set_user_data(u); w.add(k, v)?; w.close()?;
let r = run::Run::open(path)?; r.get(k) -> Option<&[u8]>; r.iter(lo, hi) -> RunIter (KvIter)
let mut o = overlay::Overlay::new(); o.put(k, v); o.get(k) -> Option<&[u8]> (Some(empty) = tombstone); o.iter(lo, hi)
let v = view::View::new(Some(&o), &[&newer_run, &older_run]); v.get(k); v.iter(lo, hi)
let merged: Run = view::merge(dst, &v, user)?;

// commit
let (root, stats) = commit::roll::roll(&mut it, path, user)?;        // it: &mut dyn KvIter over contract-form rows
let f = Arc::new(commit::file::File::open(path)?);                     // f.root(), f.storage_root(&acct), f.node(&owner, path)
let mut d = commit::dirty::Dirty::new(f, seek);                        // seek: Arc<dyn Fn(&[u8]) -> Option<(Vec<u8>, Vec<u8>)> + Send + Sync>
d.workers = 16; d.apply(key, value)?; let root = d.root()?; d.bytes(); d.reset(new_file);
```

Keys/values are the Go contract: account `keccak(addr)+0x00` with
RLP[nonce, balance, codeHash], slot `keccak(addr)+0x01+keccak(slot)` with the
left-trimmed word, empty value = delete.

## Oracle results (all pass)

Synthetic state (`rscompat gen 1 3000 400`: 2973 accounts, 107,916 rows, 1725 updates):

- Run written by Go (`exp/rscompat -mode runwrite`), opened by Rust (`rscompat runcheck`): every `get`, absent neighbours, full and bounded `iter` agree. Rust-written run opened by Go the same way. The two run files are byte-identical.
- Roll: Go and Rust node files byte-identical (6,160,728 bytes, 37,500 nodes), root `f28ace81...`.
- Dirty: Go `Dirty.Root` == Rust `Dirty::root` == Go re-Roll of the mutated rows == Rust re-roll, root `a104a0f8...`. The update mix covers account field writes, deletes with slots, new accounts with slots, slot writes, slot clears, delete-then-recreate.

Real samples from the Tokyo box (`/data/epochdb-v0/tmp`):

- `cstate.bin` (3,592,482 contract rows after conversion): node files byte-identical (209,772,653 bytes, 1,319,013 nodes), root `68e994ad...`.
- `cstate_recent_full.bin` (13,551,760 rows): node files byte-identical (776,495,559 bytes, 4,736,663 nodes), root `7aedf4d1...` (the header root exp/commitroot checks on the box).

Rust tests (`cargo test --release`, 16 tests): footer parse and every corrupt-footer flip, block layout (first entry shared=0, no straddling, index prefixes, padding), key shapes with 60-byte-equal prefixes (index ties), iter bounds, merge with tombstones, overlay in-place rewrite; commit: roll vs re-roll over 6 rounds of mixed writes, each op kind alone, edge cases (leaf split, branch collapse to leaf and to empty root, single-slot leaf-as-root account, account delete collapsing the account trie), file walk (every hashed child found at its path or rebuilt as a leaf with the same hash), corrupt footer.

## Numbers vs Go (i7-10700K, 8C/16T, same box the Go numbers came from)

Go numbers are today's runs on this box with the same inputs (`go test ./latest -bench Rows` with `LATEST_ROWS=cstate.bin`, `go test ./commit -bench 'Dirty|Roll4M'`, `exp/commitroot -sample`); the figures the task quoted are in parentheses where they differ.

| Metric | Go | Rust | Notes |
|---|---|---|---|
| Run bytes/key, cstate.bin 4M raw keys | 60.83 | 60.83 | files byte-identical; the quoted 34.2 B/key was a different sample |
| Run write incl. fsync | (Merge path only) | 3.2M to 13.4M entries/s | page-cache write-back dominates the spread |
| Get, 1 thread, random keys | 769 ns (756) | 658 ns | 0 allocations both |
| Get, 16 threads aggregate | 83.1 ns (83) | 68.0 ns | 8 threads: 89 ns |
| Merge (every 100th key rewritten in the overlay) incl. fsync | 4.18M entries/s (3.9M to 8.9M) | 3.3M to 10.5M entries/s | same fsync variance on both sides; best case 2.5x |
| Overlay real heap per entry (1M cstate rows, 79.9 B key+value) | ~204 B | 125.6 B | slab + 9 B/slot hash index |
| Roll, cstate.bin 3.59M keys | 427k keys/s (that run overlapped with `cargo test`) | 904k keys/s | identical 209.8 MB file, 58.4 B/key |
| Roll, cstate_recent_full.bin 13.55M keys | 524k keys/s, 25.8 s, 4.6 GB RSS (509k) | 1,020k keys/s, 13.3 s | identical 776.5 MB file, 57.3 B/key |
| Roll, 4M synthetic C-shaped state | 521k keys/s, 7.67 s | 990k keys/s, 4.04 s | 1.47M nodes, 58.3 B/key |
| Dirty.Root 20k updates, 1 worker | 338 ms (259) | 189 to 251 ms | first Rust round includes page-faulting the node file |
| Dirty.Root 20k updates, 16 workers | 225 ms (138) | 127 to 148 ms | one contract holds 41% of the updates, so one job bounds the parallel phase |
| Retained per dirty key | 922 B (787), slab only, map excluded | 952 to 956 B, slab incl. owner+path keys | 3.3 stored nodes per update |

Keccak: `keccak-asm` (XKCP) 292 ns per 100-byte hash and 1.19 us per 532-byte branch on this CPU, `tiny-keccak` 390 ns; the Go roll runs at 0.66M to 0.71M keccak/s, the Rust roll at 1.35M to 1.38M keccak/s.

## Deviations from the Go code (formats are identical)

- `Overlay` has no internal RwLock: `put` is `&mut self`, `get` is `&self`; wrap in a lock when a writer and readers run concurrently. Index is a hashbrown `HashTable` of slab offsets hashed with foldhash instead of Go's fixed-72-byte-array map plus a string map, so long keys need no second map.
- `Dirty` does not record deleted-node markers (geth's tracer). Nothing can reference a stale path: every change rewrites the ancestors, and a path that reappears is rewritten or embedded. The retained slab is keyed by an interned owner id plus the path bytes (`[owner u32][plen u8][path][cap u16][len u16][blob]`), Go keys a map by owner hash plus a packed u64 path.
- `Dirty::root` merges the storage node sets after the account trie commits (borrow rules); the result is the same since owners differ.
- Roll keeps the storage-root index in memory (72 bytes per account with an internal storage root) instead of Go's side file; 13.55M-key roll peaks well under Go's 4.6 GB RSS anyway because rows are the only large thing held.
- The roll hashes nodes through a `NodeSink` trait instead of a writer callback; `NoSink` is what `Dirty::leaf` uses to hash a single-slot storage root, matching Go's `trie.NewStackTrie(nil)`.
- Node hash verification on resolve is a `debug_assert` (Go's path reader does not verify either).

## Reproduce

```sh
cd rs/state && cargo test --release
cargo build --release
S=<scratch>; R=target/release/rscompat
$R gen 1 3000 400 $S
go run ../../exp/rscompat -mode runwrite -rows $S/rows.bin -out $S/go.run && $R runcheck $S/rows.bin $S/go.run
$R runwrite $S/rows.bin $S/rs.run && go run ../../exp/rscompat -mode runcheck -rows $S/rows.bin -run $S/rs.run && cmp $S/go.run $S/rs.run
go run ../../exp/rscompat -mode roll -rows $S/rows.bin -out $S/go.nodes && $R roll $S/rows.bin $S/rs.nodes && cmp $S/go.nodes $S/rs.nodes
go run ../../exp/rscompat -mode dirty -rows $S/rows.bin -updates $S/updates.bin; $R dirty $S/rows.bin $S/updates.bin; $R roll $S/rows2.bin $S/rs2.nodes
# real sample (scp cstate.bin from the box first)
go run ../../exp/commitroot -sample $S/cstate.bin -out $S/go-cstate.nodes; $R rollsample $S/cstate.bin $S/rs-cstate.nodes; cmp $S/go-cstate.nodes $S/rs-cstate.nodes
# benches
target/release/bench latest $S/cstate.bin; target/release/bench dirty
cargo build --release --target x86_64-unknown-linux-musl   # static binary for the box
```

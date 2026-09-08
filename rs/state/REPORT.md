# rs/state: Rust port of `latest` and `commit`

Branch `rs-state`, crate `rs/state` (lib name `state`), edition 2021, stable
cargo 1.97. Byte-compatible with the Go node files; the run file is format
v2 since branch rs-runfmt (see the last section) and no longer Go-readable.
No Go file was edited; the Go side of the oracle is the new `exp/rscompat`.

## Files

| File | What |
|---|---|
| `src/lib.rs` | `KvIter` trait (Go's Iterator: next/key/value), `RowsIter` over in-memory rows |
| `src/keccak.rs` | keccak256 via `keccak-asm` (default feature `asm`, XKCP assembly); `tiny-keccak` fallback with `--no-default-features` |
| `src/rlp.rs` | split/count/encode helpers for the trie (no alloy-rlp) |
| `src/run.rs` | `Writer` (front-coded 2 KB blocks, 16-byte block index, packed accounts, per-run value dictionary, 92-byte footer with crc32) and `Run` (mmap, contract-table interpolation search, `get`, `iter(lo, hi)`) |
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
| Run bytes/key, cstate.bin 4M raw keys | 60.83 | 60.83 | files byte-identical (format v1); the quoted 34.2 B/key was a different sample |
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

## Run format v2 (branch rs-runfmt, 2026-09-09)

The run file switched to the layout LAYOUT.md recommends ("F3 front 2K R0
Interp Subst" without the code-hash table). Rulings applied: nothing on the
Tokyo box, the format is free to change (no Go compatibility), no cache
layer. `Writer`, `Run`, `Overlay`, `View`, `merge` and `KvIter` keep their
signatures; rs/node and rs/exec did not change. Only `rs/state` changed
(`src/run.rs`, `tests/latest.rs`, this file). The version field is 2 and a
reader that meets a v1 file (76-byte footer) fails with `run format version
1, this build reads version 2`.

### Format

- Blocks of 2 KB, front-coded, no restart points: [used u16] then entries
  [shared u8][unshared u8][vlen u8][key suffix][stored value]; the first
  entry stores its whole key. vlen 0..=127 is a literal length; 128+i means
  dictionary value i and no bytes. `MAX_VALUE` is 127: `add` refuses longer
  values with an error naming the limit (accounts are at most 82 bytes,
  slots 32).
- Stored values. A value under a 33-byte key (an account) that is canonical
  RLP[nonce, balance, codeHash] is packed as [nlen<<1 | empty][blen][nonce]
  [balance][codeHash unless it is the empty-code hash]; the writer unpacks
  it again and compares before trusting the packing, so a non-canonical RLP
  or any other value under a 33-byte key is stored behind a 0xFF tag byte
  (which means a 127-byte non-account value under a 33-byte key does not
  fit: refused with "not account RLP"; nothing real hits it). Packing is
  transparent: `Writer::add` packs, `Run::get` and the iterator return the
  original bytes. `get` rebuilds a packed account into a per-thread
  128-byte scratch (a `thread_local`), so it stays allocation free with the
  same `Option<&[u8]>` signature; the returned slice for a packed account is
  valid until that thread's next `get`, which every caller respects (rs/node
  decodes it at once, `View::get` passes it straight through). Values that
  are not packed alias the mapping or the dictionary as before.
- Dictionary: the top 127 stored forms by bytes saved (length x count,
  count > 1), stored in the file after the blocks as [count u8] then
  [len u8][bytes] each. It is learned inside the Writer from the first
  `CENSUS_ROWS` = 262,144 rows: they are buffered (about 20 MB at most),
  counted in a hash map, then written through, and every later row is
  streamed. This was chosen over a second pass in `merge` because it costs
  no extra read of the view (a census pass over the merge would be another
  full k-way iteration, about 85 s per roll on C at the measured 10M
  entries/s) and works for every Writer caller; carrying the previous run's
  dictionary was rejected because the first run has none and a stale
  dictionary never refreshes. Keys are hashes, so the first rows are a
  uniform sample of contracts; the cost of the bias is measured below
  (37.64 vs LAYOUT's full-census 37.40 B/key on cstate, 35.83 vs 35.34 on
  cstate_recent_full). A value that is only common later is stored longer,
  never wrong.
- Index: hi[nblk] u64 (big-endian first 8 bytes of each block's first key),
  lo[nblk] u64 (its bytes 33..41, the slot-hash prefix), both zero padded,
  16 B per block, mapped in place. At open the distinct hi values and
  their first block become the contract table (12 B per distinct prefix,
  0.4 MB for cstate). A `get` interpolates on the contract table (at most 8
  steps, then binary on a window of 32), then on the lo values of the hi
  group (binary below 33 blocks), then scans one block.
- Exactness. The prefixes are 8 bytes, so the index must be made exact:
  1. Exact (hi, lo) tie (LAYOUT's Risks): when the landing block's index
     entry equals the key's prefixes, its stored first key is compared;
     if it sorts after the key the tied range is binary searched on stored
     first keys. Unit test `prefix_ties`: 400 slots of one contract whose
     slot hashes share 8 leading bytes span 6 or 7 tied blocks (the test
     counts them in the index); without the rule every key outside the last
     tied block is a miss.
  2. Deviation from LAYOUT.md, which only names the exact tie: the lo search
     assumes every block-first key of a hi group is one contract (shares
     bytes 8..33). Two contracts whose hashes share 8 leading bytes break
     that (their lo sequences interleave, and a binary search over an
     unsorted window can land anywhere). The writer records such hi values
     in an `impure` list in the file (nimp in the footer, expected empty:
     a few entries on C at most) and the reader binary searches those
     groups on stored first keys. The reader also compares bytes 8..33 of
     the key with the landing block's first key (the same cache line the
     scan starts on): a key whose contract is not the group's is routed to
     the block before or after the group. So the index is exact for any
     key bytes, not only keccak shapes. Unit test `impure_groups`: three
     contracts with the same 8-byte prefix (two with 500 slots, one with 3
     slots that sits inside another contract's block), one impure entry in
     the footer, every key found, every neighbour missed, bounded iteration
     per contract correct.
- Footer, 92 bytes: magic "epochrun", version u32 = 2, block size u32,
  entry count u64, block count u64, dictionary offset u64, index offset
  u64, nimp u64, 32 bytes of user data, crc32 over the dictionary, the
  index, the impure list and the footer before the crc (checked at open).

### Oracles (all green)

- `cargo test --release`: 20 tests. New in `tests/latest.rs`: `packed_accounts`
  (empty-code, coded, max-nonce, non-RLP and non-canonical values under
  33-byte keys round-trip, and the stored form is shorter), `dictionary`
  (a run larger than the census, coded rows carry no bytes, values past the
  census are read back), `prefix_ties`, `impure_groups`, the v1-version
  refusal and the 127-byte rules in `writer_rejects`; `corrupt` flips every
  footer field of the new layout; `block_layout` checks the 2 KB blocks and
  the hi/lo entries against the rows.
- `rscompat runwrite` + `runcheck` on the synthetic rows (every get, absent
  neighbours, full and bounded iter): OK. The Go-written v1 run is refused
  with the version message. `runcheck` on the real corpora written with the
  new format: cstate.contract.bin 3,592,482 entries OK (135,227,020 bytes),
  cstate_recent_full.contract.bin 13,551,760 entries OK (485,536,372 bytes).
- Roll: `rscompat roll` node file byte-identical to `go.nodes` (root
  `f28ace81...`); Dirty root `a104a0f8...` equals the re-roll, whose node
  file is byte-identical to `go2.nodes`. `rollsample` on both corpora:
  node files byte-identical to the Go ones (`68e994ad...`, `7aedf4d1...`).
  The node file format did not change; only the run did.
- rs/node 50k Step run, `--roll-budget 8 --workers 14`, fresh data dir:
  exit 0, `root-checked=50000`, every roll's root equal to the verified one
  (a mismatch exits 1); see the log lines below.

### Numbers before and after

Same binary pair (`bench latest` from this branch's parent commit and from
rs-runfmt), same contract-form rows, interleaved old/new per corpus on the
same load. `bench latest` rewrites every 100th key in the overlay for the
merge rounds and reads 1M random present keys for the Gets.

| corpus | metric | v1 (before) | v2 (after) |
|---|---|---|---|
| cstate.bin, 3.59M contract rows (47% accounts) | B/key (file bytes / rows) | 55.17 (47,921 blocks) | 37.64 (65,515 blocks), -32% |
| | Get 1 thread, ns | 719 | 641, -11% |
| | Get 16 threads, ns aggregate | 90.3 | 61.4, -32% |
| | Get 8 threads, ns aggregate | 101.0 | 80.7 |
| | write incl. fsync, M entries/s | 7.10 | 6.48 (census buffer plus a smaller file) |
| | merge, 3 rounds, M entries/s incl. fsync | 4.55 to 5.09 | 5.00 to 5.36 |
| cstate_recent_full.bin, 13.55M rows (99% slots, one contract 43%) | B/key | 36.72 (120,324 blocks) | 35.83 (235,239 blocks), -2.4% |
| | Get 1 thread, ns | 852 | 891 (first round 883 vs 873: noise, the load rose from 3.7 to 6.1 during this pair) |
| | Get 16 threads, ns aggregate | 99.6 | 85.0, -15% |
| | Get 8 threads, ns aggregate | 111.1 | 113.9 |
| | write incl. fsync, M entries/s | 6.58 | 4.76 |
| | merge, 3 rounds, M entries/s incl. fsync | 5.11 to 6.15 | 2.67 to 3.95 (first round 3.90 to 6.52 against 4.13 to 10.44: page-cache write-back variance, see the Go table above) |

Load average 3.7 to 6.7 during the interleaved round (other agents'
builds), 11.5 during the node run. Sizes are exact; treat Get differences
under 10% as noise. Against LAYOUT.md's projection (-12% / -17% single
thread, -23% / -21% 16-way, 37.4 / 35.3 B/key) the size and the 16-way
Gets landed as projected on cstate and the slot-heavy corpus is within
noise on single-thread Get; the sampled dictionary costs 0.24 / 0.49 B/key
against the full census.

rs/node 50k run (`run-50k-v2b.log`, load 11.5, so the wall times are not
comparable with rs/node/REPORT.md's 6 s):
```
epochdb-rs: roll 1 done: height=17546 keys=52120 nodes=18005 run=4MB trie=3MB merge=71ms roll=112ms total=196ms replayed=3768 overlay=1MB dirty=15MB->0MB
epochdb-rs: roll 2 done: height=31462 keys=104207 nodes=40281 run=7MB trie=6MB merge=158ms roll=334ms total=508ms replayed=8672 overlay=1MB dirty=21MB->0MB
epochdb-rs: roll 3 done: height=45339 keys=156299 nodes=60479 run=11MB trie=9MB merge=183ms roll=372ms total=580ms replayed=10732 overlay=2MB dirty=23MB->0MB
bench exit t=17 h=50000 blk=50000 tx=343731 mgas/s=900.87 cum=1407.77 wait=0.4 full=0 rss=224 overlay=2 dirty=8 rolls=3 rolling=false
split total read=0.37s evm=7.05s trace=0.59s commit=0.68s | checker apply=0.17s root=4.65s write=0.00s | exec-thread 2624.1 mgas/s | blocks=50000 root-checked=50000 rolls=3
```
The same keys, node counts and roll heights as the v1 run in
rs/node/REPORT.md; the run files are 4 / 7 / 11 MB against 4 / 7 / 10.

The first round (load 10 to 11 from other agents' builds, after run only)
gave cstate 37.64 B/key, 719 / 72.4 ns, merge 5.0M entries/s and
cstate_recent_full 35.83 B/key, 873 / 89.6 ns, merge 3.9 to 6.5M entries/s.

### Reproduce

```sh
cd rs && cargo test --release -p epochdb-state && cargo build --release
S=<scratch>/rs   # cstate.contract.bin, cstate_recent_full.contract.bin (written by rs/layout beside the dumps)
target/release/bench latest $S/cstate.contract.bin
target/release/bench latest $S/cstate_recent_full.contract.bin
R=target/release/rscompat; Q=$S/oracle
$R runwrite $Q/rows.bin /tmp/rs.run && $R runcheck $Q/rows.bin /tmp/rs.run
$R runwrite $S/cstate.contract.bin /tmp/c.run && $R runcheck $S/cstate.contract.bin /tmp/c.run
$R roll $Q/rows.bin /tmp/rs.nodes && cmp $Q/go.nodes /tmp/rs.nodes
$R dirty $Q/rows.bin $Q/updates.bin; $R roll $Q/rows2.bin /tmp/rs2.nodes && cmp $Q/go2.nodes /tmp/rs2.nodes
$R rollsample $S/cstate.bin /tmp/rs-cstate.nodes && cmp $S/go-cstate.nodes /tmp/rs-cstate.nodes
cd <scratch>/rs/node && target/release/epochdb-rs --dump ../step/step-containers-1-50000.bin --genesis ../step/chain.json --upgrade ../step/upgrade.json --data data-50k-v2 --workers 14 --roll-budget 8
```

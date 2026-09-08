# rs/store: epochdb storage v4 in Rust

Branch `rs-store` (rs-state + rs-block + rs-exec + rs-node + rs-vm merged; `rs/Cargo.toml` workspace members include `store` and `plugin`). Crate `epochdb-store` (lib `store`, bin `storecheck`). No Go file was edited. Started 2026-09-08 21:00 JST; an API outage cut the session between ~22:45 and 00:20 JST.

Rulings applied in order: (1) port the Go v4 store read+write; (2) user ruling mid-task: Go/Rust artifact compatibility does NOT matter, the Go format is the spec for WHAT is stored, deviate where Rust benefits, oracles are self-consistency, crash recovery and MinIO. The pebble-compatible reader/writer was already built and verified against the Go corpus when the ruling landed (Rust opens every Go data-50k section, row counts match, zstd bytes reproduce), so it stays as the container format; nothing after that was spent on byte identity.

## Format inventory (what is stored, and the Go source it comes from)

Data dir: `manifest.json` (store/db.go Manifest: storage_version 4, chain_root, runs[from_tx,to_tx,from_height,to_height,name,level]), `window/window.log` (+ `window.frozen.log` while a cut is in flight), `cas/<hash>` sealed runs and manifest artifacts (spool, uploads), `cas/.pointers/<name>` and `<data>/latest-<chainroot>` pointers, `cache/<window>/<ns>/<hash>.<idx>` chunk cache. `runs/` exists but is unused (see deviations).

A run (store/run.go): `[chain SST][state SST][lookup SST][124-byte footer: off/len x3, from_tx, to_tx, from_height, to_height, prev(32), version, "EPOCHRUN"]`, then the casfs tail (chunked.go: sha256 per 4MB chunk, u64 LE content length, "CASFSv1\n"); name = sha256(hash list). prev = previous run's name, the first run's prev = chain root = sha256(genesisData).

Sections are Pebblev2 row-block sstables as pebble v2.1.6 writes them under store/format.go writerOptions (src/sst.rs): restart interval 16, CRC32c (pebble's masked crc), blocks zstd (libzstd 1.5.7 via the zstd crate, uvarint length prefix, stored raw when the reduction is under 12%), 128KB/32KB/8KB data blocks (chain/state/lookup), index block 2x with a 64KB floor, two-level index past that, table bloom filter 20 bits/key over Split(key) for state and lookup (src/bloom.rs, pebble/bloom), properties + metaindex + 53-byte footer. Level: zstd-9 for every sealed run (Go's terminal level).

Row families (store/format.go key schema, store/mem.go WriteBlock for what a block writes), TxNum space per DESIGN: block occupies slots [first, first+count], first+count is its boundary slot:

| key | value | Go source |
|---|---|---|
| `blk/<height>` | first TxNum (8B BE) + tx count (4B BE) | mem.go add |
| `hdr/<height>` | header RLP verbatim | mem.go |
| `pvm/<height>` | container template `[len A]A[len B]B C` or empty for a bare block | container.go SplitContainer |
| `tx/<txnum>` | the tx element as it sits in the block's tx list (typed txs keep their RLP string header) | mem.go, container.go Reassemble |
| `rcb/<height>` | the block's receipts blob as the plugin was handed it (EIP-2718 receipt list), empty when the producer has none | rs/plugin log.rs Record.receipts |
| `ws/<height>` | the state engine's write set (hashed keys) + the code hashes it deployed, `window::frame_ws` framing, for the plugin's recovery replay | rs/plugin log.rs Record.ws/code |
| `rcpt/<txnum>` | uvarint status, gasUsed, cumulativeGasUsed, nLogs, logs (addr, topics, data) | receipts.go EncodeTxReceipt |
| `itx/<txnum>` | callTracer JSON verbatim | frames.go |
| `code/<codehash>` | code blob (state section) | db.go writeSections |
| `state/<addr>/a/<txnum>` | account RLP, empty = deleted | mem.go putState |
| `state/<addr>/c/<txnum>` | code hash | capture.go UpdateContractCode |
| `state/<addr>/s/<slot>/<txnum>` | left-trimmed slot value, empty = cleared | capture.go |
| `txh/<txhash>` | txnum (8B) | mem.go numRow |
| `blkh/<blockhash>` | height; keccak(header RLP) at write time | mem.go |
| `cid/<containerID>` | height; the id a peer names (see deviations) | mem.go |
| `addr/<addr>/<txnum>` | EF chunk, payload 5 role bits (sender, recipient, created, emitter, frame participant) | postings.go, ef/ef.go |
| `elog/<emitter>/<topic0>/<txnum>`, `sig/<topic0>/<txnum>` | EF chunk, no payload | mem.go logPostings |
| `tval/<value>/<topic0>/<txnum>` | EF chunk, payload 3 position bits | mem.go logPostings |
| `set/<topic0>/<pos>/<value>/<emitter>` | empty | mem.go logSets |

Window log (store/mem.go): records `[kind][num u64 BE][len u32 BE][payload]`, kinds B H I P Q R T W (chain rows: blk hdr itx pvm rcb rcpt tx ws), S (state: `[klen u16][prefix][value]`), L (posting), X (suffix-free lookup row), K (set row), C (code), E (end of block, payload = next TxNum). Replay applies a block's records only when its E record is present; the torn tail is truncated at the last complete block (writer only); blocks below the sealed floor are dropped; the buffer is flushed at every block end and fsynced by the caller (the driver syncs every 256 blocks). Flush triggers: 500,000 slots or 50,000 blocks, at a block boundary.

## Code map

`src/format.rs` keys, Split, sections, constants. `src/bloom.rs` pebble table filter. `src/sst.rs` Pebblev2 reader + writer, block cache. `src/casfs.rs` Hasher/tail/name, spool and local dirs, mmap blobs, S3 (SigV4 exactly as casfs signs: path style, 3 signed headers, unsigned Range, region "auto"; HEAD/GET/PUT/ListObjectsV2), chunk cache with the Go window naming, pointers, `sync` (content first, pointers last, release after confirm). `src/run.rs` footer, RunWriter, Run (get, latest, scan_range, scan_chunks, scan_groups, scan_set). `src/ef.rs` Elias-Fano chunks. `src/receipts.rs` rcpt codec. `src/container.rs` template split/reassemble, tx elements/envelope. `src/window.rs` BlockWrite (+ `from_exec`), Memtable, log replay. `src/db.rs` DB, seal, read API, publish/sync/join/walk_runs. `src/bin/storecheck.rs` driver and oracles. `rs/plugin/src/dbstore.rs` (new file in the plugin crate) `DbStore`, the `BlockStore` impl over the DB. ~4,200 lines.

## API (shaped for the RPC layer)

- `BlockWrite::from_exec(&block::Block, &exec::BlockResult) -> BlockWrite`: everything the store keeps for a block (header RLP, pvm template, container id, per tx: element RLP, hash, receipt row, callTracer JSON, frame participants, state rows, sender/to/created, logs; code; tail rows). Owned and Send, so the node's executor thread builds it and the checker thread calls `DB::write_block(&bw)` where it appends to the history file today (same inputs: header RLP, receipts, traces, state rows, code, plus the block for the tx rows).
- `DB::open(dir, casfs::Store, chain_root)` / `open_read_only`; `write_block`, `sync` (fsync the window), `flush` (cut now), `head`, `next_tx`, `next_height`.
- By height: `header_rlp`, `pvm`, `block_tx_range`, `txnum_at_end_of` (the state ceiling of a block), `container_at` (Reassemble, for Get/GetAncestors). By TxNum: `tx_rlp`, `receipt` (+ `receipts::decode`), `frames`, `height_of_tx`. By hash: `txnum_by_hash`, `height_by_hash`, `height_by_container_id`. State history descent: `account_at`, `storage_at`, `code_hash_at` (newest row at or below a TxNum: window, then runs newest first, bloom gated, `Latest` = SeekLT on the suffixed prefix), `code`. Range readers: `chain_rows(fam, from, to)` (runs oldest first then the window, one block decode per block), `postings(prefix, lo, hi)`, `groups`, `set_scan`.
- Publish/join: `publish` (manifest artifact + `latest-<root>` pointer, local only), `sync_artifacts` (upload spool, reopen released runs onto the chunk cache), `db::join(cas, dir, root)` (pointer, manifest, footers walked backward to the chain root, every range boundary checked, refuses a populated prefix without a pointer), `walk_runs`.
- RPC accessors: `receipt_at(height, idx)`, `frames_at(height, idx)`, `txnum_at(height, idx)`, `receipt_by_hash`, `frames_by_hash`, `locate_tx(hash) -> (height, index, txnum)`, `block_txs(height) -> [(tx element, receipt row, trace)]`, `receipts_blob(height)`, `write_set(height)`; `container::tx_envelope` turns a stored element into eth_getRawTransaction bytes.
- `plugin::dbstore::DbStore` implements rs-vm's `BlockStore` (head, height_of by block hash, id_at = keccak(header), container, read = the full Record rebuilt from rcb/ + itx/ + ws/ + code/ rows, append, sync). `append(&Record)` needs the per-tx rows, which the plugin's `Payload` does not carry (it holds only the block-level engine write set), so the engine stages `BlockWrite::from_exec(&block, &exec_result)` at verify time (`DbStore::stage`) and `append` attaches the record's receipts blob and write set to it; an append with nothing staged for that height is refused rather than stored without state rows. That two-line change in `node_engine.rs` (build the BlockWrite where the Payload is built, stage it before append) is the plugin agent's; not made here.
- p2p serving support (brief step 4): `height_by_container_id` + `container_at` are exactly what fetch/handler.go `ancestorsOf` needs (newest-first walk by decrementing height, byte and count caps are the caller's); no network code.

## Oracle results

All on the local i7, Step dump `$SCRATCH/rs/step/step-containers-1-50000.bin` (and the 1M file), `storecheck` release build.

1. Self-consistency, 1..50,000 (`storecheck write` then `storecheck verify`, which re-executes the dump and checks the store against the executor's output and the dump): PASS. 50,000 blocks, 343,731 txs, TxNum range [0, 393,731) (identical to the Go-written data-50k), 1,024,939 state rows (every row of every tx read back through `account_at`/`storage_at`/`code_hash_at` at its own TxNum, tail rows at the boundary slot), 517,825 posting checks (addr roles, elog, sig, tval position bits for EVERY tx, plus the set/ row of every log topic), 48 code blobs, every container byte-equal after Reassemble, blkh and cid rows for every block, txh and height_of_tx for every tx, chain_rows counts equal to the point reads. Write 16.0 s (executor included), verify 73 s. One sealed run, 101.6 MB (chain 63.9 MB, state 19.8 MB, lookup 18.0 MB).
2. Window-log crash recovery: `write --crash-at 30000` writes blocks 1..30000, truncates the log 10 bytes inside block 30000's end record and exits 3 (268 MB window log). Reopen: replay stops at the last complete block, next height 30000; the resumed write re-executes 1..29999 without writing (the store's floor), writes 30000..50000, seals. The sealed run's casfs name `6e273c9e...` equals the clean write's: byte-identical corpus, no loss, no duplication. `verify` over the recovered dir: PASS (same counts as above).
3. MinIO (docker `minio/minio` on 127.0.0.1:9100, fresh random keys, bucket `epochdb`, prefix `epochdb-step-rs/`; no real credentials anywhere): `publish` uploads the run (single PUT, 101.6 MB), the manifest artifact and the pointer `latest-b3efbfc5...` (content first, pointers last), releases the local spool copies (`cas/` empty after) and the process keeps serving from the chunk cache. `join` into an empty dir: pointer, manifest, footer walk (3.4 s), `readall` over the joined dir reads every block's header, container (reassembled), blk row, every tx's element, receipt, trace, txh, and the sender's addr posting and account row: PASS, 50,000 blocks, 343,731 txs, 156.9 MB of containers, 40 s, 24 chunk files in the cache (the whole run plus the manifest), every chunk verified against the list before use.
4. 1M: `write` of Step 1..1,000,000 into `$SCRATCH/rs/store/data-1m`: 20 runs of 50k blocks, 4,087,552 txs, 2.2 GB of sealed runs, 24m49s wall (executor included; blocks 700k+ are the executor-bound tail). `readall` over it (every block's header, container, blk row, every tx's element, receipt, trace, txh, sender posting and account row, no dump): PASS, 1,000,000 blocks, 4,087,552 txs, 624,935 logs, 3.16 GB of containers, 597 s. `verify --postings-every 20` (re-execution, every family, postings for every 20th tx): PASS: 1000000 blocks, 4087552 txs, 23074800 state rows, 819320 posting checks, 168 code blobs, 20 runs, 1588.4s
5. Regression after the rcb/ and ws/ families were added: write + verify (postings every 5th tx) of 1..50k PASS (chain section 1,281,193 rows, the two new rows per block included).

Read speed after the decoded-block cache: verify's mix of point reads runs ~700 blocks/s (75 s for 50k blocks, ~1.5M point reads plus re-execution); before the cache it was under 15 blocks/s (every point read decoded a whole block, and height_of_tx does 16 of them).

## Deviations from the Go store

- No terminal merge, no L0 level: every sealed run is final (manifest level 1), goes to the spool and uploads on `sync_artifacts`; the published manifest lists every run. The Go 16-way merge into 8M-slot terminals (merge.go) is not ported; the run count grows with the chain (20 runs per 1M Step blocks).
- The run cut seals synchronously on the writer's thread (Go seals on a goroutine behind a frozen log). The frozen-log recovery path is kept: a crash inside a cut is sealed first at the next open.
- `cid/` stores the block's real container id (sha256 of the UNSIGNED proposervm bytes, keccak of the header pre-fork, from rs/block); Go hashes the whole reassembled container, which only agrees when the wrapper is unsigned.
- Chunk cache: no eviction, no min-free admission; a 16-chunk RAM ring in front of the window tree.
- No resident sidecars, no flat/code caches, no jumpdest cache, no read-only cohabitation lock (`.epochdb.lock`).
- The state section has ~14% more rows than the Go corpus for the same blocks (1,024,962 vs 899,581 for 1..50k): the Rust executor emits a post-image row for every touched account; Go's trie interceptor only sees accounts whose trie node changed. Rows are self-consistent; not a store defect.
- S3: single PUT only (no multipart; MinIO takes 5 GB per PUT), static keys mandatory, no default credential chain.

## Open items

- Terminal merge (or any run compaction) when the run count matters (mainnet: ~3,200 runs of 500k slots).
- Chunk cache eviction (statfs watermark, max age) before a joined node runs unattended.
- Zero-copy reads: uncompressed blocks are copied out of the mmap and every block is decoded into owned entries; a proper LRU over decoded blocks and a restart-point cursor would cut the RPC path's allocation.
- Concurrency: `DB` is single-threaded (`&mut self` for writes, `&self` reads); the node needs a reader snapshot the writer does not block, as Go's version object does.
- Plugin wiring: stage the `BlockWrite` in `node_engine.rs` where the `Payload` is built and swap `BlockLog` for `DbStore` (two lines, plugin agent); the interim `code.log` becomes redundant (code/ rows + `write_set` hashes).
- `BlockStore::sync` takes `&self`; the DB's fsync needs `&mut`. The window buffer is flushed at every block end, so durability is one `DbStore::db.sync()` call on the plugin's fsync cadence (256 blocks in the node); wire it or make the trait take `&mut self`.
- Index artifacts beside runs: the run footer names three sections by offset, so a later index build (postings/blooms as separate artifacts) is a separate casfs object listed by the manifest, no run rewrite; not started, the current runs already carry the lookup section.

# PHASE 2: plugin wiring, reader snapshots, the terminal merge, chunk cache eviction (branch rs-storewire)

Branch `rs-storewire` on top of rs-store (62470a4), 2026-09-09 01:30-03:00 JST, local i7-10700K, nothing on the Tokyo box, no Go file edited, MinIO in docker with fresh random keys (`$S/minio/wire-env.sh`). Logs under `$S/wire-50k`, `$S/wire-crash`, `$S/wire-1m`.

## 1. Plugin wiring (rs/plugin/src/dbstore.rs, node_engine.rs, layered.rs)

- `BlockLog` and `CodeLog` are gone (log.rs deleted); `Record` and the `BlockStore` trait live in dbstore.rs. `DbStore { db: Arc<DB> }` opens `<chainData>/store` with chain root = sha256(genesis bytes). `Record` carries `rows: Option<BlockWrite>`; `append(Record)` (by value now) attaches the receipts blob and the framed write set + code hashes and calls `DB::write_block`; a record without rows is refused.
- Where the rows are built: the `Payload` keeps the executor's whole `BlockResult` (receipts, traces, state rows, code) instead of pre-encoded receipts and traces, and the CHECKER thread builds `BlockWrite::from_exec(&block, &result)` plus the EIP-2718 receipts blob right before the append. Deviation from the brief ("where the Payload is built", i.e. verify): verify sits on the host's serialized VM path, the checker is off it (depth 4), and from_exec parses every callTracer JSON for the frame participants; the checker's total went from 6.5 s to 6.3-7.0 s per 50k blocks with the blocks.log writes gone, verify stayed at 5.4-6.2 s.
- Sync: `DB::sync` takes `&self` (buffer flushed under the window lock, fdatasync outside it through a second handle), so `BlockStore::sync(&self)` is the real fsync; the flusher thread calls `db.sync()` every 256 blocks, `swap_roll` calls `store.sync()` before MANIFEST; the checker calls the new `BlockStore::close` (waits for a seal or merge in flight) after draining.
- Recovery: `DB::each_code` loads every `code/` row (runs, frozen, window) into the Backend's code table at open (what code.log did); the replay reads `ws/` rows through `DbStore::read` as before. The recovered line now prints the code blob and run counts.
- Determinism fix found by the crash test: revm's `EvmState` iterates in a per-process random order, and the write set is stored (`ws/` row), so a recovered process wrote different `ws/` bytes for the same blocks and the sealed run's casfs name differed from the clean run's (chain section 1,375 bytes apart, same row count; `storecheck rowsum` diff showed family 7 only). `Layered::commit` now sorts each commit's rows by key (distinct keys per commit, so the replay is unchanged). After it the recovered corpus is byte-identical to the fresh one (below).
- The engine's block lookups (`get_block`, `block_id_at_height`: the p2p container path and the RPC's block reads) go through `NodeEngine::db` (the `Arc<DB>`) directly, not through the writer's `Mutex<Box<dyn BlockStore>>`, so they never wait for the checker's append or the cut; the mutex is for append, sync and the recovery read only. rs-rpc can take the same Arc.
- The harness (like avalanchego's proposervm) hands the VM the UNWRAPPED inner block, so the store's `pvm/` rows are empty and `container_at` returns the inner bytes, which is what the plugin was handed. `storecheck verify --inner` compares against the unwrapped dump (senders copied from the recovered block); without it `pvm/2` fails on the first wrapped block.

Results (harness `go run ./cmd/epochdb-host-bench --dump step-50k --vm rs/target/release/epochdb-rs --batch 256 --config '{"state-sync-enabled":false,"roll-budget-mb":8}'`):

1. Fresh 50k (`$S/wire-50k/run2.log`, `run3.log`, `run4.log` = the final binary: 39 s, 1,280 blk/s, verify PASS 47 s): `exit: blocks=50000 txs=343731 gas=21828575876 root-checked=50000 rolls=4 head=50000`, rolls at 17,546 / 31,462 / 45,339 / 50,000 as before, both check lines `match=true`, the window cut at 50,000 sealed L0 run `4b688741...` [blocks 1..50000] in 3.3-3.5 s on the seal thread; 39-41 s wall, 1,203-1,263 blk/s (46 s and 1,078 blk/s with blocks.log: the checker no longer writes 319 MB of traces uncompressed). vm_rss 351 MB (the memtable holds 50k blocks of state and posting rows in RAM; 133 MB with blocks.log). `storecheck verify --inner --postings-every 5` over `<chainData>/store`: PASS, 50,000 blocks, 343,731 txs, 1,024,939 state rows, 104,006 posting checks, 48 code blobs, 1 run, 47 s.
2. Crash outside a roll (`$S/wire-crash/kill1.log`, `resume1.log`): kill -9 on `roll 2 done` (height 31,869 stored, the host died on Accept 31,870). Restart: `recovered: rolled at 31462 (gen 2), head 31869, rows replayed 6146, 19 code blobs, 0 runs, root ok, in 495 ms` (the window log replay of 31,869 blocks, 365 MB, is the 495 ms), the run continued to 50,000, both checks `match=true`.
3. Crash inside a roll (`kill2.log`, `resume2.log`): kill -9 on `roll 4 start` at 50,000 (`-s 0.005`), which is also inside the store's cut (the frozen log existed). Restart: `swept run.4: not named by the manifest`, the frozen log sealed first (`sealed run 4b688741... [blocks 1..50000]`, the SAME name as the fresh run's: byte-identical corpus after recovery), `recovered: rolled at 45339 (gen 3), head 50000 0xa9a6c28a..., rows replayed 64431, 23 code blobs, 1 runs, root ok, in 4185 ms`, both checks `match=true`. `storecheck verify --inner --postings-every 5` over the recovered store: PASS, the same counts (1,024,939 state rows, 104,006 posting checks, 48 code blobs, 1 run), 72 s.

## 2. Reader snapshots (rs/store/src/db.rs)

The Go model, ported: `Version { man, runs: Vec<Arc<Run>> }` is an immutable object behind `RwLock<Arc<Version>>`, replaced by the seal and the merge, never edited; a reader clones the Arc once per read and the runs in it stay mapped until the last holder drops (Arc is the refcount, a retired run closes when its last reader leaves, nothing waits). The active window is `RwLock<Memtable>`, held by the writer for one block's append only; the cut moves the memtable whole into `frozen: RwLock<Option<Arc<Memtable>>>` under the window write lock and a thread seals it (L0 run, zstd-1, `runs/` dir), so no read waits for a seal or a merge. `write_block`, `sync`, `flush`, `publish`, `sync_artifacts` all take `&self` now (one writer thread by contract); `DB::open` still returns `DB`, the read method signatures are unchanged (rs-rpc's `Mutex<DB>` keeps working; `db.man` is `db.manifest()`, `db.cas` is `db.cas()`); `DB::version()` is the snapshot handle. `Drop`/`close` wait for the cut and the merge.

Capture order is the whole correctness argument: rows move window -> frozen -> run and never back. Point reads probe the window first and take the view (frozen, then version) after. Scans (postings, groups, set_scan, each_code) take the window lock and the view together, copy the window entries out, then scan the runs. The first version took the view before the window and the hammer caught it in one run (`height_of_tx 389`: a block the cut had just moved). The cut's fsync runs outside the window lock (`DB::sync` first, the seal thread fsyncs the frozen file again); before that a reader could wait a whole fdatasync of a 300 MB log.

Proof: `db::tests::readers_never_block_on_the_writer` (release): 8 threads hammer every read path (header, blkh, blk range, container, tx, receipt, txh, height_of_tx, account_at, storage_at, tail row, addr posting) on random blocks at or below the writer's published head while the writer appends 1,200 synthetic blocks, cuts every 40 blocks (25-29 seals) and merges at an 800-slot boundary (6 terminals); every answer must be exact, then the whole corpus is re-read and the retired L0 files must be gone. Three runs: 17,912 / 20,099 / 21,843 reads, worst single read 11.7 / 14.7 / 87 ms (the 87 ms run was before the cut's fsync moved out of the lock).

## 3. Terminal merge (rs/store/src/db.rs, port of store/merge.go)

- Seal writes L0 runs (level 0, zstd-1, `<data>/runs/l0-<from>-<to>-<hash>`, never uploaded); `Inner::maybe_merge` at the tail of every seal starts one merge thread when the L0 tail since the last terminal crosses `terminal_txs` (TERMINAL_TXS = 8,000,000 slots pinned; `EPOCHDB_TERMINAL_TXS` lowers it for the oracle, read at open). The span is a function of content: [first L0 after the last terminal, the first run that carries the span past the boundary]. One merge at a time; the error is sticky and the next seal returns it (execution stops). `wait_merge` / `close` for shutdown.
- `merge`: holds its own `Arc<Version>` for the whole pass (inputs cannot close under it), checks the inputs tile, `MergeIter` k-way merges each section in key order (ties, only ever `code/` rows, taken from the newest input once) into a `RunWriter` at TERMINAL_LEVEL (zstd-9) in the spool, `finish` (fsync + adopt), reopen, verify the footer range and every section's row count against what went in, then under the version lock: check the span is still where it was (`old[start].name`, `old[end-1].name`), splice the manifest (keeping every run appended meanwhile), `man.save` (durable), swap the version; only then `cas.drop_local` on every input; then `publish`. A crash before the manifest save leaves the inputs and a stray `run-*.tmp` in the spool; a crash after it leaves inputs nothing references.
- `publish` lists the terminal prefix only (`Manifest::publishable`, Go's publishable()), serialized by the `published` mutex (the merge thread and the caller both publish). `join` and `walk_runs` are unchanged (the published manifest is terminals only, prev-linked back to the chain root).

Oracle (`$S/wire-1m`): `EPOCHDB_TERMINAL_TXS=2000000 storecheck write` of Step 1..1,000,000 (fresh dir, executor included): 1M_WRITE. Merges ran on their own thread while the writer kept executing and sealing:
- terminal 1: `merged 8 L0 runs into terminal run f84ebcbb... [tx 0..2129804, blocks 1..400000]: 7189412 chain + 5877195 state + 2554618 lookup rows in 39.0s` (three more L0 runs were sealed while it ran),
- terminal 2: `merged 9 L0 runs into terminal run 3b0578e5... [tx 2129804..4152834, blocks 400001..850000]: 6969090 chain + 8149683 state + 2846016 lookup rows in 83.0s`.
MERGE_AFTER

Crash points (`db::tests::merge_crash_points_leave_the_inputs`, the `merge_crash` hook is Go's `mergeCrash`: None in every real open): 200 synthetic blocks at an 800-slot boundary with the merge stopped after "written" (terminal fsynced and adopted into the spool, manifest not yet swapped): the manifest still lists the 5 L0 inputs, `runs/` still holds them, the spool holds the unreferenced terminal; reopen reads every block, `maybe_merge` recomputes the same span and the retry lands under the SAME casfs name, the inputs are then gone. Stopped after "swapped" (manifest saved and version swapped, inputs not yet unlinked): the manifest names the terminal, the 5 leftover input files are harmless, reopen reads every block and `publishable()` lists both terminals.

## 4. Chunk cache eviction (rs/store/src/casfs.rs)

Ported from casfs cache.go as the wiki note describes it: two watermarks in absolute free bytes on the cache filesystem (`statvfs`), `min_free` (default 5 percent of the filesystem, `EPOCHDB_CACHE_MIN_FREE` bytes) is the ADMISSION floor: under it a fill is served from memory and not written, never evicted inline (a statfs error reads as full); `evict_target` (default 10 percent, twice min_free when set) is where the worker cleans: one pass drops the oldest whole windows until free + freed >= target, counting the bytes it freed itself (no statfs re-read, so a queue of husks costs rmdirs, not minutes), never the current window. A chunk found in an older window is promoted (renamed) into the current one. `sweep` every 5 minutes: windows older than `max_age` (default 30 days, `EPOCHDB_CACHE_MAX_AGE` seconds or Nh/Nm/Ns) go by name, empty older windows go, stale `.tmp` files go. The worker thread polls every 5 s and settles 60 s after a pass that freed bytes; it holds a Weak to the cache and exits with the store. Stats (refusals, evictions, freed, fills) on `ChunkCache::stats`.

Test `casfs::tests::cache_admits_and_evicts_by_watermark`: a fake 10 KB filesystem (the free function is injected: cap minus the bytes under the cache root), floor 2 KB, target 4 KB: three old windows of 2 KB, fills in the current window until the floor refuses (refusals = 1, nothing written), one pass drops exactly the two oldest windows (evictions 2, freed 4,000) and stops, a read promotes a chunk out of the surviving old window into the current one, a later pass empties everything but the current window and then reports no progress, sweep expires a window past max_age by name and removes a husk. Window names are validated (`2026-09-09T01-15` is rejected).

## 5. MinIO round trip and the workspace

MINIO_RESULT

`cargo test --workspace` (debug): 21 tests pass (state 6 + 8, exec 2, block 1, plugin 2: the sibling tree test and the Step genesis hash, store 2: the cache watermark test and the reader hammer). The blocks.log round-trip test went with log.rs.

## Deviations and what is left

- The merge does not madvise/fadvise its inputs and output out of the page cache (Go's `coldWhileMerging`); on a box where execution's state reads live in the cache this costs hit rate during the 40-80 s of a merge.
- `MergeIter` picks the smallest key by a linear scan over the cursors (at most 16); a heap when the fan-in grows.
- The chunk cache's fills go through `write_durable` (tmp + fsync + rename) as before; the RAM ring in front of it is still 16 chunks.
- L0 runs are zstd-1 and stay local; a chain that never earns a terminal publishes nothing (Go's intent). Step 1M is 5.09M slots, under the 8M boundary, so the oracle lowered it to 2M through `EPOCHDB_TERMINAL_TXS`.
- Not done: the zero-copy read path, the read-only cohabitation lock, index artifacts beside runs (unchanged from PHASE 1's open items).

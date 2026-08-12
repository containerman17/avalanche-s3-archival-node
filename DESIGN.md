# epochdb design

THE single design file, a SNAPSHOT OF THE PRESENT (user ruling 2026-08-04). It describes the system AS IT IS and changes IN PLACE: no build diary, no commit hashes, no "superseded" blocks. Statuses are **TODO**, **IN PROGRESS**, **DONE**. History of this file is `git log -p -- DESIGN.md`. Chronological rulings and dead ends live in `~/dotfiles/devlogs/epochdb/`; durable gotchas in `~/dotfiles/wiki/epochdb/`. Where a rule exists because the user said so, the user's words and attribution stay attached. Only measurements that are DESIGN INPUTS live here.

Repo: `~/epochdb`, PUBLIC: https://github.com/containerman17/epochdb. Sibling library, also public: https://github.com/containerman17/casfs.

## Where the project stands (2026-08-12)

THE STORAGE LAYER IS BEING REBUILT (user rulings 2026-08-10..12). The product proven at tag `pre-clickhouse` was right; the wrong turn was /sql and the frames cascade ("the mistake was adding /sql method and going deeper instead of simply adding extra RPC methods"). The 21-section epoch format, the Elias-Fano tx index, the per-epoch key blooms, the /sql door and stored call frames are all REPLACED by one engine: a deterministic LSM of standard SSTables (below), storage version 0. A ClickHouse pivot was considered in full and rejected. The fleet is STOPPED, data dirs intact on the Tokyo box, mainnet C at 43.6M of 92.1M blocks. V1 corpora stay frozen under their S3 prefixes as validation artifacts, nothing more. NO CROSS-COMPATIBILITY MACHINERY (user ruling 2026-08-12: "clean slate"): the v6 reader is NOT kept, no adapter reads old corpora, and every chain re-syncs clean from p2p.

THE GOAL (user, 2026-08-10, restated from origin): the fastest and most compact node, existing for DeFi work. Two hard requirements: (1) blazingly fast in-process EVM calls at the current block (RPC is too slow; 200-400k calls/s on the target 96-core box); (2) state at a fixed historical block for simulations, extracted once, speed of extraction secondary. The target dev machine holds 200-500GB, so READ-THROUGH FROM S3 IS THE MAIN INSTRUMENT, not full local copies. DISTRIBUTION IS A BYPRODUCT, NOT THE GOAL (user ruling 2026-08-10): never complicate a design to preserve distribution traits; determinism survives because it is nearly free and is also the debugging story.

NEXT STEPS, in order:
1. Build storage v0: runs, manifest, memtable, flush, blooms. **DONE** except read-through from S3, the deterministic merger and the join path, which are the next step.
2. Prove on numine (mainnet L1, 1.06M txs; clean p2p sync measured 7m30s to tip). Verify per the three-layer stack below; the oracle is an archival subnet-evm node we sync ourselves, NEVER the pre-clickhouse binary (user ruling 2026-08-12: it was built without verification, so verifying against it is verifying slop against slop). **TODO**
3. RPC parity plus the Otterscan methods from day one ("it forces required indexes", user 2026-08-12), gRPC and the plain-HTTP adapter. **TODO**
4. Rebuild chains smallest-first under NEW S3 prefixes, clean p2p syncs from genesis. Mainnet C last (~2 weeks serial at the measured 1,100-1,300 tx/s, execution-bound; fetch runs concurrent). **TODO**

## Principles and standing rules [DONE]

- BODIES + HEADERS + PROPOSERVM EXTRAS ARE THE ONLY GROUND TRUTH. Re-execution rebuilds everything else. We STORE logs and receipts rather than re-derive them (user ruling 2026-07-20).
- NO STORED TRACES (user ruling 2026-08-12, reversing 2026-08-08). Frames were judged the costliest complexity in the system: a tracer seam in every execution path including SAE (a standing avalanchego fork), the biggest table everywhere, liveness coupled to capture. Any single tx's trace is recomputable on demand in milliseconds: replay the block's prefix against state at block start, which per-tx state history makes a seek plus a short replay. Consequence accepted: "history of address X" means X as sender, recipient or event emitter, never X touched deep inside a call tree. Otterscan-level, one notch below Glacier, said plainly.
- ALL DATA MUST ALWAYS BE SAFE (user ruling 2026-08-09). Nothing that exists is deleted or overwritten until its replacement is written, fsynced and verified. Derived state that cannot be built consistently refuses queries loudly; it never serves a hole.
- BYTE-IDENTITY ACROSS INDEPENDENT BUILDERS IS THE CORE PROMISE. Every artifact is a pure function of chain content. This rejects adaptive boundaries, configured thresholds, randomized construction and embedded signatures, everywhere and forever. AI-NATIVE COROLLARY (user, 2026-08-12: "no human will ever read the code"): reproducibility is the debugging story; "corrupt or correct" collapses to recompute-and-diff, which an agent can run unattended.
- NO DATABASE ANYWHERE. The hot window is RAM maps; everything durable is immutable SSTable runs plus append-only staging. Pebble is linked as a LIBRARY for its sstable reader/writer and block cache; no live key-value store runs anywhere.
- FIREWOOD IS VERIFY-ONLY WITH ZERO READERS (user ruling 2026-07-29: "Firewood is just a dumb hash verifying machine"). No read path touches it. `eth_getProof` and state-range debug methods stay deleted.
- ONE CHAIN PER PROCESS (user ruling 2026-08-04). One node type, no personas (2026-07-30): local disk decides only how FAST cold history answers, never WHAT a node can answer.
- NO EXTRA VALIDATION OF ALREADY-VALIDATED DATA (user ruling 2026-07-29).
- AGENT POLICY (user ruling 2026-07-29): subagents on this project run OPUS, pinned per dispatch, never fable.

## Chain identity [DONE]

A CHAIN IS NAMED BY ITS BLOCKCHAIN ID (user ruling 2026-07-31): `--chain C|<blockchainID>` beside `--network fuji|mainnet`. Everything a descriptor carried is ON-CHAIN: the blockchainID is its CreateChainTx txID, one `platform.getTx` returns `genesisData` and `subnetID`. `C` is coreth; everything else is subnet-evm. The data dir's vmkind stamp refuses the other kind forever. First resolution caches to `<data>/chain.json`; the cache wins over flags, and a cache naming another chain is refused.

THE CHAIN ROOT IS THE TRUST ANCHOR: `root = sha256(genesisData)`, the P-chain bytes VERBATIM (user rulings 2026-07-31, 2026-08-05). UPGRADE BYTES ARE NOT IN IT: upgrades apply inside blocks, never to genesis, so a wrong upgrade.json diverges at its activation height and hard-stops; a corpus built with a wrong one cannot exist. upgrade.json is the ONE off-chain input: `<data>/upgrade.json`, read verbatim every start, never cached. The C-chain genesis bytes are byte-identical to avalanchego's embedded copy; a test pins both network hashes.

MULTI-CHAIN: one binary, one format, trust anchored per chain. Every operator runs their own publisher and bucket; a private chain inherits S3 access control natively.

## Storage v0: a deterministic LSM of SSTables [DONE for the local engine; merger and read-through TODO]

THE WHOLE ENGINE IN FOUR RULES. (1) A RUN is one immutable file: a chain SST section, a state SST section, a lookup SST section, a footer with section offsets and the hash link. Runs are named by their TxNum range. (2) The MEMTABLE covers the unflushed window and serves it. (3) FLUSH every 500,000 txs or 50,000 blocks, whichever first, aligned to a block boundary (user ruling 2026-08-12). (4) Eight same-level runs MERGE into one next-level run at the tx boundary known in advance; only merged levels upload to S3. Everything below is elaboration.

THE ONE DIMENSION IS TxNum (Erigon's pattern, adopted 2026-08-12): a global monotone transaction number. The logical clock (flush and merge triggers), the run names, the keys and the indexes are all the same integer. A run named [from, to) contains keys `tx/from..tx/to`, so the manifest range check alone routes tx-by-number with no bloom and no search.

KEY SCHEMA, chain section (height/TxNum-ordered, arrives sorted, streams to disk during execution, big compression blocks, NO bloom: existence is answered by the run name before any file opens):
- `hdr/<height>` -> header RLP verbatim; `pvm/<height>` -> the proposervm wrapper bytes verbatim (a couple hundred bytes per block, and the difference between an archive and almost-an-archive); `blk/<height>` -> first TxNum of the block.
- `tx/<txnum>` -> tx RLP verbatim; `rcpt/<txnum>` -> receipt + full logs, epoch encoding carried over. VALUE GRANULARITY RULE: the value of a row is the smallest unit ever served alone; a whole block is a contiguous key range, one sequential scan. NO CONTAINER DUPLICATION (user ruling 2026-08-12: "no data duplication, you can assemble it, it's pretty damn fast"): proposervm extras stored beside the header, containers reassembled on demand; stored bytes stay the exact consensus RLP so header hashes and tx roots recompute from what we store.
KEY SCHEMA, state section (keys sort by account/slot, not by txnum, so this section is sorted at flush like lookup, and it MERGES; prefix bloom at 20 bits/key excluding the txnum suffix):
- `state/<account>/<slot>/<txnum>` -> post-tx value, PER-TX granularity (user ruling 2026-08-12: downgrade to per-block is a pure lossy transform consumers can run themselves). A slot's key range IS its time series: one contiguous scan yields every value it ever had, including intra-block ticks, which is where DeFi opportunities live and die. Cleared storage is a ZERO-VALUE ROW at its txnum; the EVM defines cleared as zero, so no tombstone mechanism exists (user ruling 2026-08-12). Account rows and code rows (`'c'` rows, invariant kept from V1: whichever run answers an account read also carries its code) live in the same family. Keys stay UNHASHED `kind|addr|slot`: clustering per contract is what read-through caching feeds on (user, 2026-08-11: DeFi happens in ~8,000 contracts; the cache converges on their regions), and SST prefix compression makes the shared prefix nearly free.

KEY SCHEMA, lookup section (arrives unsorted, sorted in memory at flush inside the bounded window, small blocks, blooms):
- `txh/<txhash>` -> txnum. Whole-key bloom; a hash lives in exactly one run, so nearly every probe is a miss, the bloom's best case.
- `addr/<address>/<txnum>` -> role bits (sender, recipient, created, log emitter). Populated from execution outputs, no ECDSA recovery at flush (the EVM already recovered the sender).
- `topic/<value>/<txnum>` and `logaddr/<address>/<txnum>` -> posting rows for getLogs, tx-granular (the V1 block-granular lists measured 5.1x read amplification; TxNum granularity is the fix).
- Prefix blooms via Pebble's `Comparer.Split`: the TxNum suffix stays OUT of the bloom (the CockroachDB MVCC pattern), so "does this address/key exist in this run" answers regardless of position. BLOOM POLICY: only families whose queries can miss; hot topics waste their bits by always hitting, accepted, unknowable at write time.

COMPRESSION AND BLOCK SIZE ARE PER-SECTION FORMAT CONSTANTS, measured then pinned (user direction 2026-08-12). Block sizes are pinned: chain 128KB blocks with a 256KB index (write once, stream reads), state 16KB, lookup 8KB (point reads decompress one block). THE CODEC IS SNAPPY, NOT THE EXPECTED ZSTD, and the reason is a library fact rather than a measurement: pebble reaches zstd through `github.com/DataDog/zstd`, avalanchego pins that module at v1.5.2, and its Decompress sizes its own destination from a >= 1MB hint and therefore always reallocates, which pebble rejects as a corrupt block. Every zstd run would be unreadable by the binary that wrote it. Worse for the core promise, pebble picks DataDog under cgo and klauspost without it, and the two produce different bytes from the same rows. Snappy is pure Go, one code path, deterministic everywhere. Moving to zstd needs pebble v2 (a different module path, so it links beside the one libevm's ethdb wrapper needs) plus the measurement pass; it is a storage version bump and an IO-class reindex, which the upgrade path already covers. Three sections is the count because there are three tuning profiles, not a byte-layout limit: prefixes are free and more sections cost one footer entry each.

DETERMINISM PINS: Pebble's `TableFormat` (Pebblev2), writer options, comparer and per-section profiles pinned exactly; nothing carries a wall clock (pebble writes a zero creation time, and the run footer holds no timestamp at all); the module version is pinned like a format constant, and libevm's own ethdb wrapper pins it too, so a pebble bump is a two-repo decision. Same sorted rows in = identical bytes out. Proven at unit level: the same blocks through the writer twice produce byte-identical run files and the same casfs name, and re-flushing the same staging range reproduces the same run. THE PINNED zstd CLI REQUIREMENT DIES WITH THE DICT TRAINING: block compression is the library's, pinned by module version.

MERGES ARE DETERMINISTIC AND OPTIONAL TO CONSUME: a merge is a pure function of its input runs, so a consumer holding the small runs computes the merged run locally, byte-identical, and checks the hash: compaction without re-download. Chain sections never merge above the flush level in spirit (their ranges never overlap; a merged run's chain section is pure concatenation); merging exists for the lookup and state families, whose keys overlap across runs. Mainnet settles around 25-30 live runs instead of thousands.

THE MANIFEST is the existing two-level casfs structure (user ruling 2026-08-12: "this is just a list of hashes... something we already implemented"): a run's name is sha256 of its chunk-hash list; the manifest lists live runs by tx range and name; each run's footer embeds the previous run's name and the first embeds the CHAIN ROOT. One head hash still authenticates all of history. The pointer stays a hint, never an authority; S3 stays untrusted (the name commits to every 4MB range).

STAGING IS FIRST-CLASS, ON DISK, AND MEMORY-BOUNDED (user directive 2026-08-12: "not an afterthought... nothing like memory explodes"). Downloaded-not-yet-executed blocks live in the height-bucketed arrival segments on disk, the fetcher their sole writer, DISK-UNBOUNDED BY DIRECTIVE (2026-08-08: a machine that cannot hold a chain's staging is an undersized machine; the from-genesis fetch is a producer-side job). SIZING SAID PLAINLY: a from-genesis mainnet C producer holds weeks of unexecuted raw containers at peak, ~0.6-0.7TB, planned for, not discovered. MEMORY IS THE OPPOSITE: strictly bounded regardless of chain length. The executor consumes buckets through a fixed read-ahead window; fetch bookkeeping must be O(window), never O(chain) (V1's known-deferred unbounded byID map, ~250 B/block = ~23GB at mainnet scale, is now a REQUIREMENT to not have: anything per-block beyond the window lives in the bucket files and is re-read, not remembered). Flush retires staging behind the executed point, same publish-before-delete order the seal used: cut, publish into the live set by atomic snapshot swap, only then unlink and raise the fetch floor. CRASH RECOVERY IS RE-EXECUTION out of staging, and THE UNFLUSHED WINDOW IS DURABLE (amendment 2026-08-12, found while building it): Firewood cannot be rolled back, so a restart that found the state layer 50,000 blocks behind Firewood could neither rewind Firewood to the flush boundary nor re-execute forward from it. The window therefore streams into one append-only log beside the runs, the same first-class on-disk staging pattern the fetcher uses, fsynced on the executor's own cadence and replayed back into RAM at open, cut at the last COMPLETE block. That is not a WAL in the redo sense (nothing replays it into a database; it IS the memtable's backing file) and it is what keeps the state layer at or ahead of Firewood, which is the invariant the reconcile walk-back has always needed. Data is safe because staging retires only after flush. The mainnet rebuild is a from-genesis p2p producer sync, so this staging peak applies for real, not hypothetically.

STORAGE VERSIONING AND REINDEX (user ruling 2026-08-12): this design is STORAGE VERSION 0, recorded in the manifest. Upgrades between versions exist from v0 on: a deterministic transform over runs, genesis-first where the hash chain demands it. With no stored traces every family derives from chain-section bytes, so EVERY reindex is IO-class: stream, recompute, rewrite, no EVM. A new index family is a new key prefix plus a version bump plus a reindex pass.

## The read path [IN PROGRESS: the descent and the lookup families serve; read-through from S3 is TODO]

ONE DESCENT FOR EVERYTHING: memtable, then live runs newest-to-oldest, bloom-gated, first hit wins. State at block N seeks `state/<key>/` at the largest txnum at or below N's end (or below tx k for mid-block reads). The descent's bottom is what genesis MATERIALISED, not the alloc (subnet-evm activations included; `exec.Genesis` carries `TrieAlloc`). Code by hash: memtable, then bloom-gated `'c'` rows, then genesis: ONE function, all callers.

READ-THROUGH IS THE MAIN INSTRUMENT (user ruling 2026-08-11: the target machine cannot hold 700GB, "that is practically impossible financially right now"). A node declares a local budget; blooms, SST index blocks and the manifest are ALWAYS RESIDENT LOCALLY (a few GB at mainnet), data blocks read through the windowed chunk cache from S3. A cold state read costs local bloom probes plus exactly ONE remote GET; the second touch is free. Contract clustering makes the cache converge on the operator's actual working set with zero configuration.

getLogs: intersect `topic/` and `logaddr/` posting rows, tx-granular, read the named `rcpt/` rows, exact-filter. The memtable serves the unflushed window, so sealed-plus-tail is seamless by construction, not by a special index. Aggregate and time-dimension queries are out of scope for the node; serious analytics is an external system fed by exports, said plainly (ruling carried from 2026-08-09).

## Execution and state history [carries over, one amendment]

The follower/executor/RPC process model, the VM seam (one interface, two per-kind structs, `--chain` picks), SAE (gas clock ring, settlement labels, derived base fee, no upstream C-chain state sync), ACP-77 subnet fetching, the snowman fetcher with cross-checked validator sets, the Firewood cache clamp, the anon budget and the known ~2GB/h Firewood Rust-side growth (12-point dataset in the wiki, upstream filing on the table): ALL UNCHANGED from the pre-clickhouse design. The one amendment: state capture appends PER-TX rows (was per-block), same hook, same "append to the state layer BEFORE committing Firewood" order. THE DRAIN IS A PER-TX `IntermediateRoot`: `Finalise` already runs per transaction inside ApplyTransaction, and IntermediateRoot adds the trie update pass that pushes post-images through the interceptor, with the account trie's Hash short-circuited so Firewood is never asked for a root nobody wants. geth itself calls it per transaction on pre-Byzantium chains. ONE EXCEPTION, named: on the SAE path `saexec.Execute` owns the transaction loop below the seam and offers no per-tx boundary, so an SAE block's post-images land at its last TxNum. Helicon is not scheduled on mainnet in this dep set; giving SAE per-tx rows means a boundary inside saexec, which is the class of seam the frames ruling killed. State written outside any transaction (a fork activation, coreth's atomic transfers, block finalisation) lands at the block's last TxNum, or at the TxNum the block would have started at when it has none. SAE needs NO tracer seam anymore: frames are dead, and on-demand tracing re-executes through the rpc seam that already exists.

THE FRONTIER BUILD survives with its input renamed: a k-way merge over the runs' state family (latest value per key), streamed into Firewood in batches onto the VM's own committed genesis, 2 revisions, torn-build self-healing. The join/start decision tree (pointer resolution, backward walk by prev-name, hard error over silent re-replay) is unchanged.

IN-MEMORY CURRENT STATE FOR THE FAST PATH: the goal's 200-400k in-process calls/s run against flat RAM maps of current state maintained by the follower, not against the descent. The descent is the historical and cold path.

## Entry points and adapters [TODO except JSON-RPC]

ONE CORE QUERY LAYER; every wire format is a thin peer adapter on top of it, none stacked on another (user ruling 2026-08-12: JSON-RPC is wasteful, so nothing routes through it).

1. NATIVE GO LIBRARY, the fast path: starting as a library starts the SAME full node (follower, executor, flush, everything), plus raw zero-serialization access to current state, historical state and in-process EVM calls (user ruling 2026-08-12). The algorithm is Go and links this. A starter script and the library are the same lifecycle.
2. gRPC, THE PRIMARY REMOTE API (user ruling 2026-08-12: "gRPC should be the best way to access it"). For same-box separate processes it costs 20-50us CPU per unary call, 8-20 cores at 400k calls/s: that is why the library exists; gRPC is for everything slower than in-process.
3. JSON-RPC over HTTP/WS: full coreth parity as shipped (78 methods, the refusal classes, every bug-ruling in the RPC surface section), PLUS the Otterscan `ots_` namespace from day one: paginated address history, contract creator, tx-by-sender-and-nonce, block details. Internal-operations methods are served by on-demand re-execution, not stored frames.
5. P2P BLOCK SERVING, day one (user directive 2026-08-12): PUBLIC ARCHIVE is a stated purpose (priority ten, capability one). Chains lose early blocks (small validator sets, lost disks); this node answers GetAncestors/Get with VERBATIM containers reassembled from `hdr` + `pvm` + `tx` rows, so a recovering operator lists us as a bootstrap beacon and pulls the chain back. CONTAINER REASSEMBLY IS A TESTED INVARIANT FROM DAY ONE: reassembled bytes byte-equal the fetched container, round-tripped in CI and spot-checked during sync. Epoch-level replication needs no protocol: runs are immutable S3 objects, `aws s3 cp` IS the replication mechanism (user, 2026-08-12).
4. PLAIN HTTP: method name in the path, parameters accepted in ANY form (GET query, POST form, POST JSON), because it is debuggable from a browser and curl. An adapter over the core layer, unbranded.

THE /sql DOOR IS REMOVED, with go-mysql-server, the plan cache, and the hot-tail log index (superseded by the memtable). The published-schema-file idea (llms.txt-style discovery, no versioned contract, ~100-row budgets) is RETAINED as the pattern for whatever the adapters expose.

## RPC surface [DONE, carries over verbatim in code]

Full parity minus the refusal classes (user ruling 2026-08-01: "whatever API methods are not implemented, implement them"). All standing bug-rulings carry: overrides honoured field-for-field; batching with geth's caps refused BY NAME; `eth_getLogs` blockHash form, unknown hash is an ERROR never `[]`; marshalling through each VM's own `PostRPCMarshal`; fee answers real on both kinds; RE-EXECUTION MUST NOT SWALLOW STATEDB READ ERRORS; NULL MEANS "NOT ON THIS CHAIN", NEVER "I COULD NOT READ IT"; subscriptions end the connection rather than skip a promised notification. Refusal classes unchanged: no tries, no preimages, no mempool/keystore/miner, no retained bad blocks, no traceChain. RPC labels under SAE: `pending` = last accepted, `latest` = last executed, `safe`/`finalized` = last settled.

## Verification: three layers, no self-trust [layer 1 DONE, layers 2 and 3 TODO]

NEVER VERIFY AGAINST OUR OWN PRIOR BINARY (user ruling 2026-08-12). (1) CRYPTOGRAPHIC SELF-VERIFICATION, the strongest oracle: every run recomputes header hash chain, txRoot, receiptsRoot, logsBloom from stored bytes, and state roots against a throwaway Firewood fed from the state family: consensus math, not implementation. A check that cannot run is a FAILURE, never a pass; the anchor is mandatory. (2) THE OFFICIAL CLIENT: an archival avalanchego/subnet-evm node we sync ourselves, diffed on eth_* including historical state (the method the flat-history avalanchego PR proved: 5,685 queries, 0 mismatches). (3) PUBLIC RPC SPOT CHECKS where one exists (mainnet C bodies/receipts/logs; its public endpoint has no archive state, so state rides on layers 1 and 2).

## The windowed shared chunk cache [DONE, unchanged]

Plain 4MB chunk files under 20-minute UTC window dirs; the window IS the LRU, recency in the filesystem, shared across processes, eviction is `unlink` of the oldest non-current window under a statfs watermark, admission control never eviction, fsync-then-rename fills, ranged GETs checked against Content-Range, promote-on-read once per window, singleflight per chunk. Full rationale in git history of this section; nothing about the new engine changes it, it caches run chunks exactly as it cached epoch chunks.

## Distribution and casfs [DONE, byproduct]

One publisher, many read-only consumers; object storage R2/B2-class with zero egress; a node's local disk is a cache of the bucket. casfs naming unchanged: files named by sha256 of their per-chunk hash list, spool-directory durability, parallel 1MB sub-range fills, multipart parsed for errors-under-200. Torrent alignment notes and R2 economics stand as recorded (4MiB pieces map 1:1 to chunks; a 700GB sync is ~6.5 cents of R2 reads). V1/V2 PREFIX POLICY: V1 corpora frozen under their prefixes; storage v0 publishes under NEW prefixes; nothing moves.

## Sizing inputs

- MAINNET SCALE: ~0.95-1.1B distinct state keys, ~1.6-1.8B total writes, 92.1M blocks, ~1.5B txs. PER-TX STATE ROWS add the within-block write-collision rate over per-block: low single-digit percent expected on Avalanche (fees are burned, so no per-tx coinbase write; measure at rebuild).
- STATE VS TRIE: Firewood is ~86% merkle overhead over flat state; current state 130-157GB; flat history was 175GB per-block at mainnet-43M, per-tx somewhat above.
- RUN COUNT: flush at 500k txs = ~3,000 L0 runs over mainnet history, merged 8x per level to ~25-30 live runs; blooms and SST indexes resident at a few GB.
- REPLAY WALL CLOCK, measured i7i.2xlarge: mainnet from genesis ~2 weeks serial at 1,100-1,300 tx/s; first 500M txs 3.3 days; numine (1.03M blocks) 7m30s to tip on the dev box.
- The old cook's 60s bucket rewrite (~100-150GB/day SSD at tip pace) DIES with the cook: flush writes each byte once per level, bounded by the merge fan-in.

## The fleet memory budget [DONE, dormant while the fleet is stopped]

Unchanged: the box slice (`MemoryHigh` ~70%, `MemoryMax` ~85%, sshd always runs), the heavy-slot flock queue for frontier builds, per-process grants derived from the container ceiling (GOMEMLIMIT 7/10, exec state cache 1/8, Firewood node cache 1/8), page cache deliberately unallocated as the read cache.

## Operations

- BUILD/RUN: `go run ./cmd/epochdb serve [--data <dir>] [--network mainnet] [--chain C|<blockchainID>] [--port] [--verify] [--tip-override <containerID>] [--pprof ...]`. ALWAYS PASS `--network mainnet`; the default is fuji.
- CONFIG IS ENVIRONMENT ONLY: `EPOCHDB_S3_*`, `EPOCHDB_CACHE_*`, `EPOCHDB_FETCH_CONCURRENCY`, `EPOCHDB_FIREWOOD_CACHE`, `EPOCHDB_HEAVY_SLOTS`, `EPOCHDB_NEW_CHAIN`, `GOMEMLIMIT`, `GOGC`. Credentials: static keys win, else the AWS SDK default chain; HTTP client and SigV4 signer stay hand-rolled.
- MEMORY DEFAULTS: GOGC=50, GODEBUG=madvdontneed=1 baked into the image (docker replaces GODEBUG wholesale; the binary warns when missing).
- PACKAGING: per-commit images ARE the release channel; vet+test then `ghcr.io/containerman17/epochdb` as `latest` and `sha-<short>`; cgo mandatory (firewood); runtime `distroless/cc-debian13`. The pinned zstd CLI layer goes away with storage v0.
- SOLE WRITER: flock on `<data>/.epochdb.lock`; read-only openers cohabit freely.
- NEVER MINT CLOUD CREDENTIALS FOR A TEST; reuse what exists; MinIO for S3 tests.
- THE TOKYO BOX (i7i.2xlarge, 61.78GiB, 1.8TB): fleet STOPPED 2026-08-09, data intact, crons paused; wiki has access and deployment shape.

## Open items

- The deterministic MERGER (fan-in 8, upload from level 1): **TODO**. Runs are identified by tx range and the manifest is merge-ready; nothing merges yet.
- READ-THROUGH FROM S3 for runs, the join/start decision tree over run footers, and the frontier build over the runs' state family: **TODO**.
- RPC parity restore: **IN PROGRESS**. Everything keyed by BLOCK HASH refuses with a named -32000 until a hash index exists.
- The archival-oracle node for verification layer 2: **TODO** alongside the numine proof.
- Deterministic merge constants (fan-in 8, upload from level 1) to be validated against real run counts during the rebuild.
- eth_sendRawTransaction / mempool relay: **TODO**, out of scope until designed.
- PEER-TO-PEER CHUNK SERVING between consensus-connected nodes: **TODO**.
- `avax_*` atomic tx lookup and an atomic-tx family: **TODO** (GetAncestors serving moved to day-one scope, entry point 5).
- REWRITING FIREWOOD IN GO, parked; the ~2GB/h Rust-side growth is the standing reason the option stays alive; upstream filing on the table.
- Fork upgrades are policy, not debt: headers plus proposervm extras must stay sufficient to reassemble VERBATIM containers; never break atomic fidelity.
- STILL UNEXERCISED: R2 with the default credential chain, throttling, cold ranged-GET latency at scale.
- 2026-08-12: delete the 410G `epochdb-fuji/` raw backup in S3 (operator reminder, awaiting user confirmation).

## Reference

Prior art: `~/deforestationdb` (fetch + execute + overlay, 0 mismatches on 15k A/B checks); the flat-state-history avalanchego PR #5624 (the overlay design and its verification method); Erigon (TxNum); tag `pre-clickhouse` (the last epoch-native build, kept as reference only).

Dep versions of record: go 1.26; github.com/containerman17/casfs; avalanchego / graft coreth / evm / subnet-evm v1.15.0-fuji; libevm v1.13.15-0.20260721184559; firewood-go-ethhash/ffi v0.8.0; cockroachdb/pebble (sstable library, version pinned as a format constant once chosen). go-mysql-server is REMOVED. Second brain: `~/dotfiles/agents/second-brain/projects/epochdb.md`.

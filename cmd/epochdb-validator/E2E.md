# epochdb-validator: oracle results

The validator is the Go shell in `validator/` (avalanchego rpcchainvm 45 plugin: libevm txpool, tx gossip over the p2p
gossip SDK with subnet-evm's wire format, BuildBlock orchestration, `/rpc`) over the Rust engine `rs/ffi`
(`libepochdb_engine.a`, C ABI in `rs/ffi/ABI.md`): parse, verify with the state root inline, accept, build, account
reads, JSON-RPC. Oracle: a local tmpnet subnet with 5 validators, 3 running this plugin and 2 running stock subnet-evm
v1.14.2, one genesis, same VM id (`cmd/epochdb-validator/e2e`). Machine: 16 cores, WSL2, shared with two other
agents' workloads during every run below (load 5-45), so the latencies are upper bounds.

## How to run

```
cd rs && cargo build --release -p epochdb-ffi                     # rs/target/release/libepochdb_engine.a
go test ./validator/                                              # in-process tests against the real engine
go test -tags epochdb_stub ./validator/                            # the same plumbing against the canned C stub
go build -o $P/ours/srEXiWaHuhNyGwPUi444Tu47ZEDwxTWrbQiuD7FmgSAQ6X7Dy ./cmd/epochdb-validator   # $P/stock holds stock subnet-evm
go run ./cmd/epochdb-validator/e2e --avalanchego ~/avalanchego/build/avalanchego --ours $P/ours --stock $P/stock \
   [--ours-n 3 --stock-n 2] [--load 10m --rate 300 --keys 200 --workers 8 --batch 200] [--stress] [--logs DIR] [--keep]
```

The harness: 5 nodes (per-node plugin dir), a subnet-evm genesis with ewoq funded (chainId 99999, 20 M gas, 2 s
ACP-226 delay; `--stress` = 500 M gas, targetGas x100, `initialMinDelayMS` 1), chain config with the pool caps raised
(`tx-pool-account-slots` 1000, `tx-pool-account-queue` 2000, `tx-pool-global-slots` 200k, `tx-pool-global-queue`
400k, `min-delay-target`, the stock `eth-apis` incl. `internal-tx-pool`). Functional phase: transfers through both
node kinds, a contract deploy (stock), a call (ours), a failing call (stock), a nonce gap (queued on ours, filled from
stock); every height compared across all 5 nodes (`eth_getBlockByNumber` full and `eth_getBlockReceipts`, canonical
JSON); proposer counts from the plugin's `"proposer": "self"` accept line; stock logs scanned for invalid-block lines.
Load phase: N keys funded, `workers` goroutines sign on the fly and post JSON-RPC batches round-robin to all nodes
at `rate` tx/s; a sample every 10 s (head, last block fill, pool, plugin RSS from /proc, Go heap / GC share /
verify and build p50 p99 from `/ext/metrics`); at the end every load-phase height is compared again.

## In-process (real engine, one sender, `go test -run TestLatencyByBlockSize`)

| txs in block | engine build | BuildBlock wall | Verify of own block | crossings |
|---|---|---|---|---|
| 50 | 2.4 ms | 2.6 ms | 4.5 us (lookup) | verify 1, accept 1, build 1, account_state 1, head_header 1 |
| 200 | 11.5 ms | 12.6 ms | 8.7 us | same |
| 1000 | 46 ms | 50 ms | 6.9 us | same |

Go heap 2-4 MB, GC CPU < 0.4% (the test itself signs the txs). `TestPoolDrainsUnderChurn`: 200 senders, 12
rounds, nonces arriving out of order through RPC and the gossip door (7200 txs): the pool drains every block, engine
nonce == pool nonce for every sampled sender, 0 account read errors. `TestAccountStateAtOldID`: the engine answers
ENOTFOUND for an accepted non-head id (Go falls back to the head). `TestPoolChainGetBlock`: the engine's stored bytes
decode as a libevm block.

## Run 1: functional + 10 min at 300 tx/s (3 ours + 2 stock, 200 keys, commit 1a803f0)

- Functional: 8 blocks, every check passed, blocks identical on 5 nodes.
- Load: 176,136 txs offered, 0 refused; 302 blocks (9..310), 176,336 txs, all byte-identical on 5 nodes; ours built
  156, stock 154; no invalid-block lines in stock logs.
- Per 2 s block ~600 txs = 12.5 M of 20 M gas (the single-goroutine generator was the limit, 294 tx/s delivered).
- Our plugin: RSS 220-250 MB flat over the 10 min, Go heap 9-12 MB, GC 0.5% of CPU; verify avg 2.7 ms; engine
  build avg 46 ms (includes up to 6 build retries per height while a built block waits for consensus, same 100 ms
  retry as subnet-evm); crossings per accepted block ~16: parse 3-5 (avalanchego re-parses gossiped/queried
  blocks), verify 1-2 (competing blocks), accept 1, build 1-6, account_state 1, head_header 1, rpc 0-11 (the
  harness), other 1-3.

## Run 2: 10 min at 2000 tx/s offered (3 ours + 2 stock, 1000 keys, 8 workers x 250-tx batches, commit 3f8f35e)

- Functional phase passed. Load: 1,011,500 txs offered and accepted by the RPC door (0 refused), the chain took
  54,487 of them.
- First 3 minutes: FULL blocks, 952 txs = 20.0 M gas every 2 s (chain rate ~480 tx/s, 10 Mgas/s); blocks 9..98
  byte-identical on 5 nodes, ours built 60 / stock 38; verify p50 1.1 ms p99 19.5 ms (n=100), engine build p50 4.5 ms
  p99 476 ms (n=56).
- Then a STALL at height 98 (t=190 s): every node's pool reported pending 0 while the queue grew 175k -> 500k; no
  node built again; the last four blocks included every candidate (14, 208, 314, 9 txs), no engine, plugin or stock
  errors. Our Go heap grew to 2.8 GB and GC to 33% of CPU because RPC txs were admitted as LOCAL, which exempts them
  from the pool's caps. About 12k pending txs left the pools unmined within ~10 s (a drop, not a lag).
- Fixes since (399d1da): RPC txs admitted as remote (caps and "txpool is full" backpressure apply); the e2e chain
  config raises the caps; account read failures are logged and counted (`account_errors` in the per-block line, an
  account that turns empty at a new head is logged with its previous nonce/balance); libevm's pool drop reasons
  ("Removed old/unpayable ...", "Discarding ...", reset failures) are forwarded to stderr (first 500). The in-process
  churn test above did not reproduce a wrong nonce or balance view. Re-run pending a machine slot (Run 3).

## Run 3: all-ours 3 nodes, 10 min at 2000 tx/s offered (bf263f8, localized archive, fixed admission, raised caps)

- No tx was dropped: the drop-reason forwarding and the empty-account warning stayed silent, `account_errors` 0
  on every block. So run 2's "12k txs vanished" was not a pool view bug; see below.
- Blocks 9..135 (first 4.5 min): FULL, 952 txs = 20.0 M gas every 2 s; verify p50 5 ms p99 17 ms, engine build p50
  60 ms p99 99 ms (candidates = 1.5x the gas limit, ~1400 txs executed per build).
- Then the chain crawled (135 -> 142 in 5 min) with 100k txs PENDING and the RPC door slowing from 2000 to 40 tx/s.
  Root cause (read from libevm): the builder called `pool.Sync()` before every pending check and every BuildBlock;
  in libevm's TxPool that is the simulator hook and it FORCES a full pool reset (demote + promote of every tx under
  the pool's write lock), and `hasPending`/`pendingSize` used `Pending()`, which copies every pending tx under the
  same lock. With 100k pending, every wake became a multi-second reset; adds starved; WaitForEvent sat in Sync. Run
  2's stall was the same mechanism plus the unbounded local queue. Fixed at 9a563ca: no Sync in the build path (a
  build racing the async reset hands the engine a few already-mined txs, which it skips, code 1), pending checks
  via `Stats()`.
- Go heap 100-300 MB with 100k pending txs (the pool itself, ~1-2 KB per tx), GC 2% of CPU, RSS 350-750 MB.

## Run 4: --stress genesis (500 M gas, 1 ms min delay), all-ours 3 nodes, 5 min at 3145 tx/s offered (9a563ca)

- No stall. 39 load blocks (9..47) byte-identical on 3 nodes: 364,532 txs on chain = 1215 tx/s, 65 Mgas/s.
- Blocks: 16,029 txs = 336.6 M gas (67% of the limit): the engine's miner size target (1800 KiB of tx bytes) caps a
  transfer-only block, not gas. One block per ~11 s.
- Engine verify p50 0.9 ms p99 196 ms (16k-tx blocks). Engine build p50 1.7 s, p99 above the histogram's 5 s bucket
  ("took 7.8 s" in the log): the Go side handed 107k candidates per build (1.5x the gas limit in 21k-gas txs, then
  re-sent 2x on `needs_more`, which the size cap also sets), 12 MB of RLP and 90k engine pops per build. Fixed after
  the run: candidates are capped by bytes too (1800 KiB + 1/8) and there is no second round after a size pop
  (code 5 in `skipped`). Expect build cost to follow the 16k included txs (~50 ms per 1000 in-process).
- Pool 480k pending (per-account cap 1000 x 1000 keys; the global pending cap is soft in geth's pool: it only trims
  accounts above their slot limit). Go heap 0.5-0.9 GB, GC 1.5%, plugin RSS 3.4-4.0 GB (pool + engine).
- Send failures at the end: "already known" (the generator's batch resend after a slow RPC), harmless.

## Run 5 (for the record): all-ours 3 nodes, 10 min, 2000 tx/s offered, default genesis + raised caps (2f9e8d1)

- No stall: 246 load blocks (9..254) byte-identical on 3 nodes, 185,328 txs on chain (309 tx/s over the 10 min; a
  full block every ~2.4 s), 753 txs/block average = 79% of the 20 M limit (most blocks 952 txs = 20.0 M, some short
  ones when two nodes built on the same parent), 0 refused sends, 0 drops (drop-reason and empty-account warnings
  silent, `account_errors` 0). Offered rate fell to 1265 tx/s because RPC round trips got longer as the pool grew.
- Engine verify p50 2.8 ms p99 9.9 ms (n=282); engine build p50 92 ms p99 430 ms (n=255; ~1400 candidates, 952
  included per build).
- Pool 577k pending at the end (per-account caps 1000 x 1000 keys); Go heap 0.6-0.9 GB (the pool), GC 1.7% of CPU;
  plugin RSS 1.6-1.85 GB growing with the pool.

## Run 6 (for the record): --stress genesis, all-ours 3 nodes, 5 min, 2682 tx/s offered (2f9e8d1)

- No stall: 94 load blocks (9..102) byte-identical on 3 nodes, 420,668 txs on chain = 1402 tx/s, 75 Mgas/s; block
  size alternates between 16,029 txs = 336.6 M gas (the 1800 KiB size cap, 67% of the gas limit) and small blocks
  (a build racing the pool's reset gets mostly already-mined candidates: "included 2005, candidates 18034"), 4475
  txs/block average.
- Engine verify p50 1.0 ms p99 179 ms (16k-tx blocks); engine build p50 650 ms p99 1.06 s with 18k candidates
  (~40 us per included tx, down from 1.7 s p50 / 7.8 s max with 107k candidates in run 4).
- Pool 350k pending; Go heap 0.3-0.5 GB, GC 1.3%; plugin RSS 1.7-2.0 GB. Per-block crossings 50-200: parse (each
  node re-parses the others' blocks several times), account_state 1, header 1, and build retries every 100 ms while
  a built block waits for consensus (the min delay is 1 ms here, so the retry loop dominates).

## Run 7: why stress blocks came every 2 s, and sub-second blocks (e2e 2a7f1e4-ish, plugin 2f9e8d1)

- Diagnosis (read-only): nothing in the shell or avalanchego paced the 2 s. The stress genesis had `"timestamp":
  "0x0"`; both stock's `core/genesis.go` and the engine's genesis builder write the `initialMinDelayMS` seed into the
  genesis header only when the genesis time is Granite-active, so at a 1970 genesis the header carries no excess, the
  first Granite block starts at `acp226.InitialDelayExcess` (~2000 ms) and ACP-226 moves it at most 200 units per
  block toward the `min-delay-target` (about 40k blocks to reach 1 ms). The shell's WaitForEvent waits exactly
  `parent.MinDelayExcess.Delay()`, so every block came 2 s after its parent regardless of load (the same gotcha as
  `wiki/avalanchego_why_initialmindelayms_seed_silently_fails_on_1970_genesis.md`). The proposervm min block delay
  was already 0 s (tmpnet `DefaultE2EFlags`), and the build retry is 100 ms.
- Fix (harness): `--stress` stamps the genesis timestamp with `now`; the e2e prints the mean block interval.
- Result (all-ours 3 nodes, 3 min, 3917 tx/s offered): 908 load blocks (9..916) identical on 3 nodes, 588,272 txs =
  3268 tx/s on chain, 195 ms between blocks, 648 txs / 13.6 M gas per block (the generator, not the chain, was the
  limit: pool mostly drained). Verify p50 0.8 ms p99 41 ms; build p50 21 ms p99 891 ms; Go heap 15-175 MB, GC
  0.4%, RSS 0.23-1.1 GB (pool grew to 90k late in the run).

## Memory: where 2-4.5 GB of plugin RSS went (branch rs-mem off rust 38a82fa)

Evidence from live processes (slots workload, 50 sstores per tx, 2000-tx blocks of 100k slot writes on the
--stress genesis; `epochdb_health` now reports jemalloc `heap-allocated`/`heap-resident`): the Go heap was 4-11 MB
throughout, the engine's state (overlay 3-60 MB, dirty 75-86 MB) small, and the whole growth was live Rust heap,
~8 MB per block, in three places:

1. The store window memtable copied every state row of the live window (up to 1 GiB of raw log) into
   `HashMap<Vec<u8>, Vec<(u64, Vec<u8>)>>`: measured 3.56x the raw log resident (450 MB log -> 1.6 GB). Fixed:
   the memtable indexes rows in the log (key hash -> chained `(txnum, offset, len)` entries, 24 B each; reads
   pread the record, the seal mmaps the log once and sorts) -> 0.58x (450 MB -> 262 MB); `window-max-bytes` default
   1 GiB -> 128 MiB, so a window costs ~75 MB resident and one L0 seal per 128 MiB of rows (seals took 0.7-1.2 s).
   Reads pay one pread per version walked (page cache); the e2e functional checks and the Go tests pass on it.
2. `layered::Pending.parent` was a strong `Arc`: every verified block chained back to genesis and kept every
   accepted block's write set (100k slots x ~150 B = 15 MB) and trie layer alive forever. Fixed: the parent is a
   `Weak`; the tree owns a pending block until accept/reject, and once the parent is accepted (its writes are in the
   backend) children read through to the backend.
3. Verified blocks consensus never rejected (a built block superseded before it was proposed, a 100 ms build
   retry on the same parent) stayed in the tree with their write sets; the node taking the RPC load built the most
   and grew 5x faster than the others. Fixed: accept drops every verified block at or below the accepted height.
   Also: the parsed-block cache (container + decoded txs, 5-10 MB per slots block) is swept on every accept
   instead of every 256 blocks.

Before/after at the same height on the same 3-node --stress L1 (node taking the RPC load, jemalloc allocated /
RssAnon): height ~187: 1.32 GB / 2.0 GB before -> 140 MB / 415 MB after; height 431-453: 3.9 GB / 4.3 GB before
-> (not reached after: the generator, built for 2 s blocks, stalls on sub-second ones) 191 blocks: 139 MB / 273 MB,
flat across the three nodes (135-140 MB allocated). The compare tab's original observation (2.0-2.6 GB at 273
blocks with a 410 MB window) is the sum of the three.

## Summary for a validator (this machine, 16 cores shared with other agents' jobs)

- Correctness: every block built by ours was accepted by stock and vice versa; `eth_getBlockByNumber` and
  `eth_getBlockReceipts` identical on all nodes at every height in every run (1.1 M txs over runs 1-6); no
  invalid-block lines in stock logs.
- Engine cost at the 20 M gas / 2 s default chain: verify p50 3-5 ms, build p50 60-90 ms per full 952-tx block,
  under 5% of the block interval. At 500 M gas: 16k-tx blocks verify in ~180 ms p99 and build in ~650 ms.
- Go side: heap follows the pool (about 1.5 KB per pending tx), 10 MB at 300 tx/s with a drained pool, 0.5-0.9 GB
  with 350k-580k pending; GC 0.5-2% of CPU; RSS = pool + engine (240 MB drained, 1.8-2 GB with a 500k pool).
- Crossings per accepted block with the pool drained: ~16 (parse 3-5, verify 1-2, accept 1, build 1-6, account 1,
  header 1, other 1-3); admission crossings only for a sender's first tx after a head change.

## Validator-side lessons (the pool under load)

1. `txpool.TxPool.Sync()` is the simulator's hook: it forces a full pool reset (demote + promote of every tx under the
   write lock). Never call it on the build path; the async reset after a head change is fine, the engine skips the
   few already-mined txs a racing build includes.
2. `Pending()` copies every pending tx into LazyTransactions under the pool's write lock; use it once per build, never
   for "is there anything pending" (use `Stats()`, O(accounts)) nor per gossip event.
3. Local admission (`pool.Add(txs, local=true)`) exempts a sender from every cap; an RPC flood then grows the queue
   and the Go heap without bound (2.8 GB, GC 33% in run 2). Admit RPC txs as remote; the chain config's
   `tx-pool-account-slots/queue` are the levers, the global pending cap is soft.
4. Candidate volume is the build cost: cap by bytes (the miner's 1800 KiB target) as well as gas, and do not re-run
   the build on `needs_more` when the block was size-capped.

## Deviations and open items

1. Pool: libevm's `core/txpool` instead of subnet-evm's. subnet-evm/core links firewood's Rust staticlib and two Rust
   runtimes cannot share one binary (`rust_eh_personality` and the allocator shims collide). Same lineage; missing
   `SetMinFee` (a tx under the chain's min base fee is held until it expires, the build filter skips it) and the
   fee-config gas limit check at admission. The gossip set is a 90-line adaptation of `plugin/evm/eth_gossiper.go`
   (same wire format, bloom, push/pull), for the same reason.
2. blst/secp256k1/jemalloc/Rust runtime symbols: the archive is localized by `rs/ffi/localize.sh` (17 `epochdb_*`
   globals only), so it links next to avalanchego's bls and firewood without `--allow-multiple-definition`.
3. `/ws` is not mounted (the engine's ws server has no socket door through the FFI).
4. `txpool_content` / `eth_pendingTransactions` return libevm's tx JSON (hash, no `from`), not ethapi's RPCTransaction.
5. Coinbase: `allowFeeRecipients=false` -> blackhole, else the config's `feeRecipient`; a RewardManager precompile is
   not read. `GetFeeConfigAt`-style runtime fee config changes (FeeManager) are not read by the Go side (the engine
   enforces them at build/verify).
6. tmpnet has no ConvertSubnetToL1 helper: the "L1" is a 5-validator permissioned subnet.
7. Build retries: like subnet-evm, WaitForEvent re-arms 100 ms after a build whose block is not yet accepted, so a
   height can cost up to 6 engine builds (~10 ms each at 600 txs). A cheap improvement is to skip the retry while our
   last built block is still preferred.
8. Engine build vs verify: build executes up to 1.5x the gas limit of candidates (the miner's over-provisioning) and
   averaged 46 ms against 2.7 ms verify at ~600 txs in Run 1 (p50 4.5 ms in Run 2, p50 60 ms at full 952-tx blocks
   in Run 3). A profile on the rs side of build vs verify for the same block is worth it; the Go side's share is the
   RLP of the candidates (~110 B per transfer).
9. Pool memory: geth's pool keeps every pending tx decoded (~1-2 KB each); per-account slots are the effective cap.
   Chain configs for a validator should keep `tx-pool-account-slots` small (16 default) unless a few senders are meant
   to burst.

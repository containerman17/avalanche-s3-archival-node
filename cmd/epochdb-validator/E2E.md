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
   [--ours-n 3 --stock-n 2] [--load 10m --rate 300 --keys 200 --workers 8 --batch 200] [--stress] [--account-slots 1000] [--logs DIR] [--keep]
go run ./cmd/epochdb-validator/admitload --rpc <node>/ext/bc/<chain>/rpc --keys 1024 --batch 500 --workers 8 --dur 30s   # admission rate at one node
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

## BuildBlock profile (branch rs-buildprof off rust db5aa14)

Why build cost 60-92 ms p50 per 952-tx block when vbench builds a 1,000-transfer block in 8 ms in-process: vbench's
synthetic txs carry their sender (`synth.rs` sets it), the FFI build did not. `NodeEngine::build` recovered every
candidate's secp256k1 sender sequentially (`block::recover`, 37 us each with libsecp256k1) before executing, and the
Go pool had already recovered the same senders at admission (cached in the tx). Candidates are 1.5x the gas limit,
and under a 100 ms block cadence about half of them are already mined (the pool's reset lags the head), so a build
recovered 2-4x the included count and then skipped half with code 1.

Instrumentation (kept): every build logs `validator: built` with the tx counts (candidates, included, pending
accounts, rounds, one counter per skip code) and a monotonic phase split, Go side (`t_head` header lookups + fee
math + Granite wait, `t_pending` Pending(), `t_order` heap build, `t_take` selection, `t_rlp` candidate RLP + sender
list, `t_cgo` the crossing) and engine side from `epochdb_build_out.phase_ns` (`t_eng_decode`, `t_eng_template`,
`t_eng_recover`, `t_eng_exec`, `t_eng_finish`, `t_eng_root`, `t_eng_assemble`, `t_eng_cache`, `t_eng_tree`,
`t_eng_outbuf`, `t_eng_copyout` = Go's copy of the result buffers, `t_eng_other` = the crossing minus the sum).
Accept of a self-built block logs `buildToAccept` (build return to Accept start). `"pprof-addr": "127.0.0.1:0"` in the
chain config serves Go pprof and logs the bound port (`validator: pprof`); the e2e chain config sets it.

Fix: Go hands the pool's recovered senders across the boundary (`epochdb_build` gained `senders, senders_len`: 20
bytes per candidate; the engine recovers only a candidate whose 20 bytes are zero, or every candidate when NULL).
The miner ordering, the skip codes and the crossing count are unchanged.

In-process (`go test -run TestLatencyByBlockSize -v`, one sender, 500 M gas genesis, pool caps raised), engine
call = `t_cgo`:

| txs | engine call before | of which recover | after | recover after | exec | assemble | decode | Go rlp+senders | Go Pending |
|---|---|---|---|---|---|---|---|---|---|
| 50 | 2.5 ms | 1.9 ms | 0.49 ms | 0.4 us | 0.29 ms | 0.09 ms | 0.05 ms | 0.06 ms | 0.04 ms |
| 200 | 13.9 ms | 10.7 ms | 1.7 ms | 0.5 us | 1.0 ms | 0.34 ms | 0.25 ms | 0.18 ms | 0.11 ms |
| 1000 | 44.1 ms | 36.6 ms | 7.4 ms | 2.4 us | 4.7 ms | 1.6 ms | 0.8 ms | 3.0 ms | 2.1 ms |
| 5000 | 212 ms | 179 ms | 35.6 ms | 11 us | 22.8 ms | 8.2 ms | 3.8 ms | 4.6 ms | 2.4 ms |

BuildBlock wall at 1000 txs: 45.8 ms -> 13.5 ms; Verify of the own block stays a lookup (7 us). Go heap 2-13 MB,
GC CPU under 0.5% in both.

3-node all-ours --stress L1 (genesis stamped now, 1 ms min delay, pool caps raised), the e2e's batched generator at
`--rate 4000 --keys 1000 --workers 8 --batch 250`, 3 min, machine otherwise idle (load < 1). Phase percentiles
over every build of the three nodes (1807 builds before; blocks came every ~100 ms, so a block held 387 txs on
average and 1012 at the p90; 286 builds included 800+ txs):

| phase (ms) | before p50 | before p99 | after p50 | after p99 |
|---|---|---|---|---|
| took (BuildBlock wall) | 41.0 | 300 | 3.6 | 12.9 |
| t_pending | 1.2 | 65 | 0.5 | 5.8 |
| t_order + t_take | 0.3 | 3.4 | 0.2 | 1.3 |
| t_rlp (+ senders after) | 0.7 | 6.2 | 0.3 | 1.9 |
| t_cgo (the crossing) | 37.1 | 241 | 2.0 | 8.9 |
| t_eng_decode | 0.5 | 5.2 | 0.2 | 1.2 |
| t_eng_recover | 32.8 | 223 | 0.0 | 0.0 |
| t_eng_exec | 1.7 | 11.0 | 0.9 | 4.7 |
| t_eng_root | 0.6 | 2.4 | 0.5 | 1.8 |
| t_eng_assemble | 0.4 | 3.3 | 0.2 | 1.7 |
| tree + cache + outbuf + copyout + other | 0.1 | 0.5 | 0.0 | 0.3 |
| buildToAccept | 86 | 803 | 32 | 144 |

The after run had 3084 builds (the chain sped up: 59 ms between blocks instead of 102, so a block held 234 txs on
average, 688 at the p90; 60 builds included 800+ txs). Per candidate the crossing went from 43 us to 8.5 us p50; the
skip of an already-mined candidate now costs its decode only. Builds with 800+ included txs: `took` p50 85 ms p99
523 ms before -> p50 10.2 ms p99 14.1 ms after (t_cgo 74.7 -> 7.3 ms p50, exec 5.5 -> 3.7 ms).

Chain-level, before -> after (same generator, 3 min, `/ext/metrics` of node 0 at the end): engine build p50 29.5 ms
p99 350 ms (n=849) -> p50 1.1 ms p99 9.7 ms (n=1898); engine verify p50 0.9 ms p99 10.0 ms -> p50 0.7 ms p99 8.4 ms
(build is within 2x of verify at the p50 and p99); mean block interval 102 ms -> 59 ms; on chain 695,500 txs in 1799
blocks (3858 tx/s, generator-limited, 0 refused) -> 721,000 txs in 3076 blocks (4000 tx/s, generator-limited, 0
refused); block fill 387 txs = 8.1 M gas -> 234 txs = 4.9 M gas (the same tx stream over more blocks); candidates per
build p50 845 (386 already mined) -> 344 (17 already mined; the pool's reset keeps up with the shorter build); Go heap
13-23 MB, GC 0.3% of CPU, plugin RSS 220-235 MB -> Go heap 14-17 MB, GC 0.3%, RSS 193-204 MB; crossings per block
69.8 -> 38.8 (fewer 100 ms build retries: buildToAccept p50 86 ms -> 32 ms). Blocks identical on the 3 nodes in both
runs.

perf (`perf record -g -F 499` on one plugin for 60 s of the load, before): 26% of the plugin's CPU was
`rustsecp256k1_v0_11_ecdsa_recover` (the engine: build candidates plus parse of the other nodes' blocks), 22% Go's
`secp256k1_*` (the pool's admission recovery, once per tx), 12% `legacypool.runReorg` (5% `truncatePending`),
13% gRPC serving; `buildBlock` itself 1% (the Rust frames do not unwind into the cgo caller, so the split above
comes from the phase log, not perf). After: `rustsecp256k1_v0_11_ecdsa_recover` 11.6% (parse of the other two nodes' blocks only), Go's
`secp256k1_*` recovery 22% (admission), `runReorg` 12%, gRPC 17%. Go pprof (60 s CPU, after): `runtime.cgocall` 38%
of samples, of which 62% is the pool's `secp256k1_ext_ecdsa_recover` at admission, 27% `epochdb_parse`, 4.8%
`epochdb_build`, 4.4% `epochdb_verify`, 1.3% `epochdb_account_state`; `buildBlock` 2.9% inclusive (Pending 0.9%,
RLP + senders 0.5%); heap in use 15 MB (the gossip bloom set's linked hashmap 3 MB, gRPC buffers 2 MB).

Found on the way (default 20 M gas / 2 s genesis, fixed in the same branch): all three nodes build height h in
the same millisecond after the 2 s Granite wait, so competing blocks at one height are the norm there. Accept
swept the losing sibling from the tree and from the parsed cache (the rs-mem memory fix), and avalanchego's
rpcchainvm server looks a block up (`VM.GetBlock`) before it calls `Reject` on it: the "not found" shut all three
chains down at height 2 ("not found while processing sync message: chits"). The parsed cache now keeps the accepted
height's siblings one block longer and `epochdb_get_block` answers a block that is still in the parsed cache.
The --stress runs never hit it because the 1 ms delay spreads the builds out.

Default genesis (20 M gas, 2 s), all-ours 3 nodes, 3 min at 2000 tx/s offered (run 5's settings), after: 105 load
blocks (9..113) identical on 3 nodes, 74,297 txs, 708 txs/block (74% of the limit: most blocks 952 txs = 20.0 M, short
ones when a node built on a stale parent), 2083 ms between blocks, 0 refused. Engine build p50 4.0 ms p99 19.5 ms
(n=140; run 5 on the same settings: p50 92 ms p99 430 ms), verify p50 1.0 ms p99 14.2 ms. Per full 952-tx build
(79 builds): `t_cgo` p50 12.7 ms p99 22.2 ms (decode 2.8, exec 5.9, root 1.5, assemble 1.6), candidates 4287 (round
1 = 1.5x the gas limit, round 2 after `needs_more`: 1823 of them already mined, the pool's reset lags a retry),
`t_rlp` 3.5 ms, and `t_pending` p50 114 ms p90 213 ms p99 1.6 s: `Pending()` copies every pending tx into a
LazyTransaction (184k pending at t=120 s, 1000 keys x the raised per-account cap), and its p99 is the pool's write
lock during a reorg of that many txs. That is now the Go side's dominant build cost under a deep pool; it is geth's
miner design (`Pending` has no limit) and the lever is the chain config's `tx-pool-account-slots` (lesson 9), not the
build code. `took` p50 1.9 s here is the Granite 2 s wait inside BuildBlock (the `t_head` lap), not work. Go heap
251 MB at t=120 s (184k pending), 426 MB at the end, GC 0.4-0.7%, RSS 532-801 MB.

Not the cause (ruled out by the split): `Pending()` copies (1.2 ms p50 with ~600 pending accounts and a drained
pool; the p99 of 65 ms is the pool's write lock during a reorg), the ordering heap (0.1 ms), candidate RLP (0.7 ms),
the result copy-out (< 0.1 ms), tree registration (< 0.1 ms), header/root work (0.6 ms; verify computes the same
root once more for a foreign block, a self-built block is a lookup).

Open items: (1) the 100 ms retry still re-runs the whole build while a built block waits for consensus
(buildToAccept p50 86 ms, p90 334 ms before), and each retry re-selects, re-encodes and re-executes the same
candidates; a retry now costs ~5 ms at 400 txs, so it stays as in subnet-evm. (2) Half of the candidates under a
100 ms cadence are already mined (skipNonceLow p50 386 of 845 candidates): the pool's async reset lags the head;
the engine's skip costs only the decode now (0.6 us per candidate). (3) The Go pool's own recovery at admission
(22% of the plugin's CPU at 3900 tx/s) is once per tx and stays. (4) `Pending()` under a deep pool (above): 114 ms
p50 per build at 184k pending; a pool-side cap on what a build asks for would need a Pending variant libevm does
not have.

## Blocks consensus still names (branch rs-getblock off rs-buildprof 8287607)

The log line, twice on the compare tab's 2-validator --stress L1 at height ~280-285 and on every 2 s chain under load:

```
FATAL <... Chain> handler/handler.go:337 shutting down chain {"reason": "received an unexpected error",
  "error": "rpc error: code = Unknown desc = not found while processing sync message: chits from NodeID-..."}
runtime engine: received shutdown signal: terminated
```

Cause (confirmed on db5aa14 with `TestSiblingStaysRetrievableAfterAccept`, which fails there with `GetBlock(loser) =
not found after the sibling's accept`, and on the network: 3 ours on the default 2 s genesis at 2000 tx/s, two of
three nodes died at height 38 after 9 competing builds): the rs-mem memory fix made accept drop every verified
block at or below the accepted height from the tree and sweep the parsed cache the same way, so a sibling that lost
to the accepted block was gone from every lookup. Consensus still names it by id: avalanchego's rpcchainvm server
calls `VM.GetBlock` before `Reject` (vm_server.go BlockReject), our `GetBlock` turned every engine error into
`database.ErrNotFound`, the client passes a Go error from the server back as gRPC `Unknown`, and the snowman
handler treats any error while processing a chits message as fatal. Only a `database.ErrNotFound` from the
`GetBlock` enum path is tolerated, and only for blocks the engine never issued (chits: `issueFromByID` requests the
block from the peer). Stock subnet-evm never loses a block consensus saw.

Fix: the tree separates a block's bytes from its pending state. Accept still drops the pending state (write set,
trie layer) of everything at or below the accepted height and reject still drops the rejected block's, but the
blocks move to a FIFO map bounded to 1024 blocks or 64 MiB of block bytes (`DROPPED_MAX_*` in tree.rs; the
decoded txs of a slots block roughly double what the bound counts) that `get_block` answers after the verified
set and before the store. Verify of a dropped block is a clean "was rejected: a sibling at its height was
accepted" error, reject of a dropped or unknown id is a no-op success, `epochdb_get_block` answers any block the
tree or the parsed cache has. Go: `engine.getBlock` maps only `EPOCHDB_ENOTFOUND` to not-found, `VM.GetBlock`
returns `database.ErrNotFound` for that alone and logs any other engine error with the id (`validator: GetBlock
failed`) and returns it. Tests: tree.rs `accept_keeps_the_losers_retrievable_without_state` (two siblings plus a
verified grandchild on the loser; get_block/meta for every id, verify of the loser errors, reject twice is a
no-op, pending state gone) and `dropped_cache_is_bounded`; validator `TestSiblingStaysRetrievableAfterAccept`
(two builds on one parent through the real cgo path).

After, same shapes: 2 s genesis, 3 ours, 3 min at 2000 tx/s: 125 blocks accepted, 134 built (9 losing siblings
kept and rejected), no FATAL, no `GetBlock failed`. --stress, 2 ours, 4 min at 4000 tx/s: 3147 blocks (~13/s),
3147 built across the two nodes, 0 refused txs, identical on both nodes; plugin RSS 156 MB at height 144, 194-199
MB at 1136, 205-207 MB at 2223, 206-231 MB at 3143 (flat; the rs-mem figure was ~270 MB at 191 blocks of heavier
slots load); jemalloc from `epochdb_health` at height ~150: heap-allocated 31-35 MB, heap-resident 122-135 MB.
The 2 s chain's 650-750 MB RSS is the Go pool holding a 250k-tx backlog (Go heap 278-454 MB), as in run 5.

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

## Run 9: the settle rounds that "lost" 400 txs (2 all-ours --stress nodes, plugin 8287607; branch go-pooldrop)

- Symptom (two networks, 224437 and 231202): the epochdb-host settle generator (1024 senders, 400 settle txs of 12 M
  gas per round) stopped with `no block past 365 for 30s, pool has 0 txs` after round 3 (round 5 on the second
  network); no block after 365 on either node, `txpool_status` 0/0 on both.
- Nothing was dropped. Blocks 334..365 hold exactly 1200 = 3 x 400 settle txs (400 at 344, 800 at 355, 1200 at 365;
  2000 = 5 x 400 through 390 on the second network); the next round was never submitted, the generator failed inside
  `drain()` after the last one. Every settle sender holds 19999.7 ETH, base fee flat at the 1 gwei floor, fee cap
  1000 gwei, 12 M tx gas vs a 500 M limit, `account_errors` 0 on every accepted line. The pool's drop lines could not
  say either way: the 500-line Trace cap was used up 5 s into the run, mostly by `Removed old/unpayable queued
  transactions count=0`.
- Mechanism: the engine answers `eth_blockNumber` as soon as `epochdb_accept` returns; the pool's reset ran later on
  libevm's `TxPool.loop` after Accept's ChainHeadEvent (header decode, a 1024-account refresh, a goroutine hop, the
  reset itself). `drain()` polls the height every 50 ms and calls `txpool_status` at once; on the block that mined
  the last tx of a round it read the mined txs as still pending and waited for a block no tx justified. The same lag
  woke `WaitForEvent` (Stats() > 0) into builds whose every candidate the engine skipped as nonce-low; those returned
  `errNoTxs` with no log line and no retry gap: `epochdb_build_seconds_count` 11595 vs `epochdb_build_txs_count` 175
  on K7 (9155 / 191 on Ny; 4870 / 198 and 5483 / 192 on the second network).
- Fix (validator only): `Block.Accept` calls `chain.headMoving()` before `eng.accept`; `setHead` runs the subpool's
  `Reset` on a goroutine and calls `headMoved()` when it lands (`validator: pool reset {height, took, old, ...}`
  at Info, the removals by libevm's reason: `old` = mined, anything else a drop; totals in
  `epochdb_pool_removed_total{reason}` and health `removed`). The builder sees no pending txs while a move is in
  flight and is woken when it lands; every pool RPC read (`txpool_status`, `txpool_content*`,
  `eth_pendingTransactions`, `eth_getTransactionCount "pending"`) waits for it. `buildBlock` arms the 100 ms retry
  gap before the empty check too (`epochdb_build_empty_total`). Not synchronous: the reset walks every pending
  account, 570 ms at 104k pending (1000 senders x 104 txs; `EPOCHDB_DEEP=1 go test -run TestAcceptCostDeepPool`);
  Accept stays at 1.7-2.9 ms there, the pool settles 350-530 ms later.
- Evidence: `TestPoolHeadMovesWithAccept` against 8287607 reads `txpool_status` pending=7 right after Accept in 4 of
  5 runs; with the fix 0. On a fresh 2-node --stress network, a probe that sends 400 transfers per round, polls the
  height every 2 ms and reads `txpool_status` at once (classifying against the blocks' tx counts afterwards): before
  1 read of 98 counted 22 mined txs as pending, 3.1 engine builds per block; after 0 of 74, slowest reset 1 ms.
  Empty builds remain (2.6 per block, bounded by the retry gap): consensus builds on a preferred, not yet accepted
  parent while the pool sits at the accepted head, so that parent's txs are candidates the engine skips.
- Generator side (epochdb-host `drain`/`awaitBlock`, other worktree): when no block comes and `pending()` reads 0,
  return instead of failing; the count it saw was a stale one.

## Admission: one pool call per JSON-RPC batch (branch go-admit off rust 18ecf06)

Why a 2-node --stress network mined 4.4-5.3k tx/s while the engine builds a 16k-tx block in 211 ms: `answerPool`
called `pool.Add([]{tx})` once per `eth_sendRawTransaction`, and every call recovered the sender, took legacypool's
global lock, read the sender through the account cache and requested a promotion round; a single issuer topped out
at ~7.8k tx/s on ours and on stock alike. Now `serveRPC` collects the batch's sends, decodes them, recovers the senders
on all cores (`recoverSenders`; `types.Sender` caches in the tx so the pool's own recovery outside its lock is a hit),
reads the senders' accounts in one crossing (`accountCache.warm`), then one `pool.Add(txs, false, false)` (one lock,
one promotion round: `sync=false` leaves the round to the reorg loop, which merges the rounds of concurrent calls)
and one `push.Add(txs...)`. Answers keep the batch's order and one bad element refuses itself only
(`TestBatchAdmission`). The inbound gossip door (`gossipSet.Add`) stays per tx: the gossip SDK calls it per element.

Found on the way, fixed first (fda567f): `poolChain` kept a 16-entry root -> block id map and evicted a random
entry per accept; a reset lagging a few heads (four accepts in 25 ms) could ask `StateAt` for an evicted root,
libevm logged `Failed to reset txpool state` and kept its old nonce view (6k txs mined, then no landings for 15 s).
`StateAt` now returns the head reader for any root (the engine has no readable state for a rolled-past block anyway,
the head's nonces are what the next reset would serve) and the account cache is keyed by address only: a lagging
reset also stops crossing once per account (3841 single-account crossings under the pool lock at 1024 senders, a
27 s reset when the engine was busy, builder and admitters waiting). `TestResetAtLaggedHead`; health `poolErrors`.

Admission directly (`go run ./cmd/epochdb-validator/admitload --keys 1024 --batch 500 --workers 8 --dur 30s` at node
0 of a 2-node all-ours --stress network from `e2e --keep`, presigned 21k-gas transfers, machine shared):

| | before (18ecf06) | after (37dbd3f) | after + libevm cached sender |
|---|---|---|---|
| admitted tx/s over 30 s | 6,794 | 12,421 (17,679 in the first 5 s, pool shallow) | 18,438 (35,117 in the first 5 s) |
| batch of 500: p50 / p99 | 506 ms / 1.15 s | 214 ms / 985 ms | 35 ms / 1.95 s |
| refused | 0 | 0 | 0 |
| pool at the end | 459 pending (chain kept up) | 189k pending (chain did not) | 419k pending |
| `pool.Add` of 1000 pre-warmed txs, in-process | 46 ms | 46 ms | 6-14 ms (box load) |

Chain level (`e2e --ours-n 2 --stock-n 0 --stress --load 2m --rate 40000 --keys 1024 --workers 8 --batch 250`,
workers sticky per node, node 0's `/ext/metrics` at the end):

| | before | after | after, `--account-slots 64` |
|---|---|---|---|
| offered (0 refused) | 9,067 tx/s (admission-bound) | 16,069 tx/s | 20,812 tx/s |
| mined | 1,089,024 txs = 9.07k tx/s | 1,097,626 = 9.1k tx/s | 803,971 = 6.7k tx/s |
| block fill / interval | 520 txs, 59 ms | 8,643 txs (16k and 2k alternating), 836 ms | 7,882 txs, 851 ms |
| engine build p50 / p99 | 6.9 / 38.8 ms | 42.9 / 340 ms | 35.4 / 194 ms |
| engine verify p50 / p99 | 0.8 / 16.4 ms | 0.9 / 188 ms | 0.8 / 184 ms |
| pool at the end | 1.3k pending | 408k pending | 65k pending + 400k queued |
| Go heap / GC / RSS | 25 MB / 0.7% / 280 MB | 700 MB / 1.4% / 1.5 GB | 1.2 GB / 1.4% / 3.1 GB |

Before, admission was the ceiling: the generator got 9k tx/s in and the chain mined all of it with the pool drained.
After, the chain still mines ~9.1k tx/s and the surplus deepens the pool: legacypool's reset walks every pending
account (1-1.7 s per head at 150-250k pending, 4 s at 400k), the builder waits for it (`hasPending` is false while
the head moves), so blocks come every ~0.8 s; `Pending()` at 400k took 3.6 s. A smaller per-account cap alone moves
the surplus into the queue (2000 per account, 400k global) with the same reset cost; both caps have to be low enough
that admission refuses (`txpool is full`) for the engine to set the pace. (An earlier run with the generator
alternating nodes per batch made every second nonce of a key cross by gossip; once admission outran gossip the queue
hit 400k, `truncateQueue` evicted the oldest queued txs and broke the sequences for good: 2000 pending, blocks of
2000 then 3 txs, 63k txs mined in 2 min. A client sends a key's txs to one node; the generator now does too.)

Where the next admission ceiling is (Go pprof, 15 s at ~15k tx/s, 33.6 s of CPU): `pool.Add` of 1000 presigned,
pre-recovered, pre-warmed txs takes 46 ms under the lock (`TestAdmitBatchCost`, in-process: 46 us per tx, a 21k
tx/s ceiling per node). About 30 us of that is libevm's `ValidateTransactionWithState`
(`core/txpool/validation.go:203`) calling `signer.Sender(tx)`, which bypasses the tx's cached sender and recovers the
secp256k1 key a second time, under the lock (upstream geth calls `types.Sender(signer, tx)` there); that is a
one-line fix in the libevm fork the module replaces to. Landed in the fork as `containerman17/cached-sender`
c05bef53c (`types.Sender(signer, tx)` at validation.go:203; go.mod's replace now resolves to it): `pool.Add` of
1000 = 6-14 ms (7-14 us per tx, the box shared), and the same admitload run admitted 35,117 tx/s over the first 5 s
(batch of 500 p50 35 ms) before the pool deepened past 135k, 18,438 tx/s over 31 s (p99 1.95 s; 419k pending at the
end, 0 refused). Its profile is the pool's depth: `runReorg` 11.6 s of 15 s, 10.7 s of it `pricedList.Reheap`
(`SetBaseFee` at every head re-heaps the whole pool: O(n log n) `EffectiveGasTip` big.Int compares), the rest
parallel recovery outside the lock. So after this commit and that fix the ceiling per node is the pool's depth, not
admission: legacypool's per-head `Reheap` and the O(pending) reset grow with what the chain has not mined yet, and
the pool caps (`tx-pool-account-slots` x senders, `tx-pool-account-queue`, the global slots and queue) must be small
enough that admission refuses (`txpool is full`) instead of queueing; with the e2e's raised caps (1000/2000 per
account, 200k/400k global) the 16-20k tx/s offered here piled up 400k txs and the chain mined 9.1k tx/s. Without the
fork fix the next costs under the lock are the second recovery (30 us), the priced heap push (~5 us per tx) and the
same `Reheap` and reset. Sender recovery
on the Go side is 47% of the plugin's CPU at 15k tx/s (16 s per 225k txs, ~70 us each in libevm's cgo secp256k1)
but runs on all cores outside the lock (6-9 ms per 1000), so moving it into the engine (`epochdb_recover_senders`)
would save CPU, not serial time; not built. JSON decode, gossip and the account crossings do not show (warm crosses
once per batch of new senders, 0.1-1 ms). Batch size: 500 measured; the path has no per-batch count cap (16 MB body),
so 1000 halves the per-batch fixed costs (lock handshake, promotion request, HTTP/gRPC).

## Summary for a validator (this machine, 16 cores shared with other agents' jobs)

- Correctness: every block built by ours was accepted by stock and vice versa; `eth_getBlockByNumber` and
  `eth_getBlockReceipts` identical on all nodes at every height in every run (1.1 M txs over runs 1-6); no
  invalid-block lines in stock logs.
- Engine cost at the 20 M gas / 2 s default chain: verify p50 3-5 ms; build was p50 60-90 ms per full 952-tx
  block until the BuildBlock profile below removed the engine's duplicate sender recovery (7 ms per 1000 txs
  in-process now). At 500 M gas: 16k-tx blocks verify in ~180 ms p99 and built in ~650 ms before that fix.
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
5. The pool's head moves after Accept, asynchronously; anyone who has seen the new height (the engine answers
   `eth_blockNumber` first) must not read the pool until the reset landed. `Block.Accept` marks the move before the
   engine accepts, `setHead` runs the reset on a goroutine and clears it; the builder's `hasPending` is false while
   marked, the pool RPC reads `settle()` first (Run 9). libevm's ChainHeadEvent is not sent: its loop gives no
   landing signal, and `pool.Sync()` (tests only) resets on the wrong old head.

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
7. Build retries: like subnet-evm, WaitForEvent re-arms 100 ms after a build whose block is not yet accepted (since
   Run 9 also after a build that skipped every candidate), so a height can cost up to 6 engine builds (~10 ms each
   at 600 txs). A cheap improvement is to skip the retry while our last built block is still preferred; a bigger one
   is subnet-evm's: move the pool with SetPreference, not Accept, so a preferred parent's txs are no candidates.
8. Engine build vs verify: resolved by the BuildBlock profile section (the engine recovered every candidate's
   sender again; Go now hands the pool's senders over). Build is within 2x of verify at the p50 and p99.
9. Pool memory: geth's pool keeps every pending tx decoded (~1-2 KB each); per-account slots are the effective cap.
   Chain configs for a validator should keep `tx-pool-account-slots` small (16 default) unless a few senders are meant
   to burst.

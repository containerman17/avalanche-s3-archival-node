# epochdb-validator: oracle results

The validator is the Go shell in `validator/` (avalanchego rpcchainvm 45 plugin: tx gossip over the p2p gossip SDK
with subnet-evm's wire format, BuildBlock timing, `/rpc` forwarded whole) over the Rust engine `rs/ffi`
(`libepochdb_engine.a`, C ABI in `rs/ffi/ABI.md`): parse, verify with the state root inline, accept, build, the
transaction pool (since rs-mempool, "Mempool in the engine" below; the runs before it used libevm's legacypool in Go),
account reads, JSON-RPC. Oracle: a local tmpnet subnet with 5 validators, 3 running this plugin and 2 running stock subnet-evm
v1.14.2, one genesis, same VM id (`cmd/epochdb-validator/e2e`). Machine: 16 cores, WSL2, shared with two other
agents' workloads during every run below (load 5-45), so the latencies are upper bounds.

## How to run

```
cd rs && cargo build --release -p epochdb-ffi && ffi/localize.sh target/release/libepochdb_engine.a   # rs/target/release/libepochdb_engine.a
go test -count=1 ./validator/                                     # in-process tests against the real engine (the cgo archive is invisible to the test cache)
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

## Mempool in the engine (branch rs-mempool off go-admit a6b3c85, 2026-09-10 JST)

The pool moved from Go (libevm legacypool) into the engine: `rs/chain/src/pool.rs`, wired into `NodeEngine` (open,
accept, build) and the Rust JSON-RPC, exposed through `epochdb_pool_*` (ABI.md). What legacypool cost per head was
O(pending): `reset` walked every pending account (1-1.7 s at 150-400k pending), `pricedList.Reheap` re-heaped the
whole pool at every base fee change (10.7 s of a 15 s profile), `Pending()` copied everything under the same lock, and
the builder sometimes ran before the reset landed. Now:

- Per sender a `BTreeMap<nonce, tx>` split at the sender's state nonce into the executable prefix and the queue;
  `heads` (a `BTreeSet` keyed tip cap desc, arrival, one entry per sender with an executable head) and `priced` (tip
  cap asc, one entry per remote tx) are updated per insert / remove, never rebuilt. A head change
  (`Pool::on_accept`, inside `epochdb_accept` under the execution mutex) removes the block's txs, re-reads the nonce
  and balance of the block's senders and of its recipients that hold txs here, re-settles those senders (mined,
  unpayable and over-gas txs drop, the prefix is recomputed, the head key follows) and moves the head rules (block gas
  limit, fee config min base fee). No other sender is touched: `TestAcceptCostDeepPool` measures Accept at 120k
  pending; the pool part is the block's senders.
- Admission (`Pool::add`): decode + libevm's stateless checks + sender recovery in parallel on rayon (from 32 txs),
  the unseen senders' state in ONE read under the execution mutex, validated against a head generation the accept
  bumps (a head landing in between re-reads), then the pool lock per tx (known, nonce, cost alone and with the
  executable sequence, replacement bump, caps, eviction). `epochdb_pool_add(1000 presigned transfers)` = 7-8 ms in
  process (`TestAdmitBatchCost`; libevm: 46 ms, 6-14 ms with its cached sender AFTER the caller recovered and warmed).
- Build: `epochdb_build` with no candidates asks the pool (`Pool::candidates`): the `heads` index walked in tip-cap
  order with a side heap for heads whose effective tip (min(tip, fee cap - base fee)) is below their tip cap, so the
  order is the miner's exactly, per-sender nonce order behind each head, cut at 1.5x the gas limit and the 1800 KiB
  target + 1/8 (as Go did). A sender's txs already in the block's unaccepted ancestors are skipped (the ffi walks the
  tree), so a build on the preferred block right after the one that made it offers only what is new. Built blocks do
  not remove anything: a rejected block's txs are simply still there.
- RPC: the engine's JSON-RPC answers `eth_sendRawTransaction` (every send of a batch in ONE pool call, answers in
  order, one bad element refuses itself), `txpool_status/content/contentFrom/inspect` (geth's shape, with `from`),
  `eth_pendingTransactions`, `eth_getTransactionCount(addr, "pending")` (state nonce + executable txs); Go forwards
  the body whole. `epochdb_pool_wait` blocks WaitForEvent until an executable tx is pending; since the pool moves
  inside Accept a mined tx never counts.
- Gossip (Go, `validator/gossip.go`): the same wire format (id = tx hash, payload = the envelope), no decode in Go.
  An inbound push message is admitted in one `epochdb_pool_add`; the push loop drains the pool's admissions
  (`epochdb_pool_drain_gossip`) into the push gossiper and the bloom filter every 100 ms; pull responses go through
  the SDK's per-element `Add` (one crossing per tx, 1 s period), `Has` (one crossing per tx the push gossiper is about
  to send) and `Iterate` (`epochdb_pool_content`, capped at 50k) are the SDK's contract. Stock nodes exchange txs
  with ours unchanged (the 3+2 run below).
- Go: no libevm pool, no `poolChain`, no account cache, no reset goroutine, no drop-log counters; `validator/pool.go`
  is gone, `vm.go` lost 200 lines. The Go heap is the gossip SDK's.

Admission rules kept from libevm (`ValidateTransaction` + `ValidateTransactionWithState`, legacypool `add`): tx types
0/1/2 only, 128 KiB max, chain id must be ours (a legacy tx without EIP-155 needs `allow-unprotected-txs`), initcode
<= 49152 for creates from Durango, gas limit <= the block gas limit of the head's fee config, fee cap >= tip, intrinsic
gas (21,000 / 53,000 + calldata + access list + initcode words from Durango), tip >= `tx-pool-price-limit` (locals
exempt), nonce >= state nonce, balance >= cost and >= the executable sequence's cost + cost (minus the replaced tx's),
replacement needs fee cap AND tip both > old and >= old x (100 + `tx-pool-price-bump`) / 100, nonce gaps allowed
(queued), a full pool (`tx-pool-global-slots` + `tx-pool-global-queue`) evicts its cheapest remote tx for a dearer
newcomer or refuses "transaction underpriced", `tx-pool-lifetime` drops the txs of a sender idle that long with no
executable tx, `local-txs-enabled` false makes everything remote. Error texts are libevm's.

Deviations from libevm (each deliberate):

1. Fee cap must be >= the fee config's min base fee at the head (subnet-evm's `SetMinFee`; libevm's pool lacked it and
   the Go shell held such txs until expiry).
2. `tx-pool-price-limit` is enforced as a tip floor (stock does; the Go shell set the gas tip to 0).
3. Slots are counted per tx, not per 32 KiB (`numSlots`); a 128 KiB tx takes 1, not 4.
4. Per-account caps refuse the newcomer ("txpool is full") instead of admitting and dropping the sender's highest
   nonce later: the queue cap (`tx-pool-account-queue`) always, the pending cap (`tx-pool-account-slots`) only while
   the global pending set is at `tx-pool-global-slots` (as legacypool's `truncatePending`, which trims other offenders).
5. Eviction picks the cheapest remote tx by tip cap (legacypool: effective tip at the current base fee; its
   `changesSinceReorg` throttle is not reproduced). `ErrFutureReplacePending` IS reproduced since go-gap: a queued
   (gapped) arrival never evicts an executable tx, it is refused "future transaction tries to replace pending" (code 9).
6. Unpayable and over-gas txs of a sender are dropped when that sender is next touched by a block (its own tx, or a
   value transfer to it), not at every head; base-fee changes never drop anything (legacypool keeps them too, the
   miner filter skips them).
7. Lifetime sweeps run on a head change at most every 30 s over senders with no executable tx (legacypool: the
   queue-only heartbeat rule, every minute).
8. Intrinsic gas at admission is the plain access-list cost; a warp predicate's `PredicateGas` is applied by the build
   (which pops the sender) and the verify, not the pool.
9. `priority-regossip-addresses` are not honoured (there is no local sender set; `local-txs-enabled` marks RPC
   senders local only if the shell passes local = 1, which it does not).
10. A build on a preferred, unaccepted parent: candidates skip the parent chain's txs per sender (legacypool + the Go
    shell offered them again and the engine skipped them as nonce-low).

Oracles run: `cargo test -p epochdb-chain pool::` (nonce order and gap promotion, replacement bump, sequence cost,
stateless rules and intrinsic gas, caps and eviction, effective-tip order under a base fee change, accept touching only
its senders and rejected blocks re-including, candidates skipping the pending parent, lifetime / gossip / wait, config
keys); `go test -count=1 ./validator/` (`TestBuildVerifyAccept`, `TestPoolHeadMovesWithAccept` incl. a rejected
sibling, `TestBatchAdmission`, `TestAdmitBatchCost`, `TestPoolDrainsUnderChurn`, `TestLatencyByBlockSize`,
`TestSiblingStaysRetrievableAfterAccept`, `TestRPCForward`, `TestAccountStateAtOldID`; the reset / lagged-head /
removal-reason tests are gone with the code they tested), `-tags epochdb_stub` for the plumbing.

Compatibility (3 ours + 2 stock, `e2e --load 2m --rate 300 --keys 200`, commit 6057fb6): blocks 1..71 identical on
5 nodes, 37,000 txs, 587 txs/block, 62% of the 20 M limit, proposers ours 38 / stock 33, txs sent to stock mined by
ours and vice versa (functional phase incl. the nonce gap filled from stock), no invalid-block lines in the stock logs,
0 refused; ours: Go heap 11 MB, GC 0.000, RSS 142 MB, verify p50 2.2 ms, build p50 3.4 ms.

Measurements (b3eb879 + the pool status in the built line; 2 all-ours validators on this box, 16 cores, the compare
tab idle; `e2e --ours-n 2 --stock-n 0 --keys 1024 --workers 8 --batch 1000`, generator workers sticky per node):

| run | offered / admitted | mined | blocks | build p50 / p99 | peer verify p50 / p99 | Go heap / GC / RSS |
|---|---|---|---|---|---|---|
| admitload at one node, --stress (1024 senders, 8 x 1000-tx batches, 30 s) | 78,848 tx/s admitted over the first 5 s, 54,230 over the next 5 s, then the 600k cap (200k slots + 400k queue) refused; 26.5k tx/s over 30 s incl. the refused stretch; batch of 1000 p50 46 ms p99 182 ms | | head +18 in 30 s (the other node got only what gossip carried, 1.8k tx/s, and built 180-tx blocks) | | | RSS 1.7 GB at 486k pending |
| --stress, 60k offered, 2 min | 17.6k tx/s accepted by the RPC (pool full after 20 s, the rest "txpool is full") | 1,197,074 txs = 10.0k tx/s | 90 blocks, 13,301 txs/block avg (full blocks 16,029 = the 1800 KiB target), 1,382 ms apart | 130 / 480 ms | 0.8 / 193 ms (n=104) | 315 MB / 0.3% / 2.0 GB |
| --stress, 32k offered, 2 min | 15.8k tx/s accepted | 796,624 = 6.6k tx/s | 79 blocks, 10,084 txs/block, 1,277 ms apart | 108 / 450 ms | 0.9 / 189 ms | 388 MB / 0.2% / 2.7 GB |
| default genesis (20 M gas, 2 s ACP-226 delay), 60k offered, 2 min | 9.1k tx/s accepted | 60,048 = 500 tx/s | 73 blocks, 823 txs/block, 2,010 ms apart (the genesis paces at 2 s; not a pool number) | 12 / 78 ms | 0.8 / 16 ms | 338 MB / 0.2% / 1.1 GB |

Admission is no longer the ceiling (78.8k tx/s at one node vs the 35k of go-admit's first seconds and libevm's 18k over
30 s), and neither is the pool's head move (Accept incl. the pool: 32 ms avg over 159 accepts of 16k-tx blocks) nor
the engine (build 130 ms, peer verify 190 ms for a 16k-tx block). The chain mines ~10k tx/s because acceptance is
BURSTY: both nodes extend a shared chain of up to 10 unaccepted blocks (each built on the preferred block within
100-250 ms of the previous one), then consensus accepts the whole chain within a second and nothing is accepted for
6-16 s (`buildToAccept` 6.3 / 8.2 / 10.6 / 16.1 s on 16k-tx blocks; accept-gap analysis of the 32k run: p50 80 ms,
p90 4.5 s, max 24.9 s, the 27 gaps over 2 s sum to 238 s of the 257 s; avalanchego's chain health check fired "block
processing too long: 30.6 s > 30 s"). The pool is not involved: `pending` in the built line stays 100-500k, candidates
are 18k every build, skipNonceLow is 0. Candidates for the cause at the time (settled by "Consensus cadence" below: the third one, avalanchego's 512 KiB/s
per-peer inbound bandwidth throttle): the snowman poll cadence with 1.8 MB blocks (each PushQuery answer waits for the peer's parse +
verify under ctx.Lock, 250-400 ms, times the virtuous commit threshold, so a 10-deep chain finalizes at once), the
proposervm's 5 s proposer windows (`errProposerWindowNotStarted`; ACP-226's 1 ms delay does not remove them), and the
2 MiB per-node at-large inbound throttle against 1.85 MB blocks. The generator itself adds noise once the pool is full:
every refused 1000-tx batch makes it re-read the pending nonce of up to 128 keys (x_rpc 200k per node per run).

Where the next ceilings are, in order: (1) consensus finalization latency of heavy blocks (above); (2) the per-block VM
path serialized under ctx.Lock on each node: build 130 + peer verify 190 + parse ~50 (16k recoveries on rayon) +
accept 32 ms = ~400 ms per 16k-tx block = 40k tx/s if consensus kept up; execution is 8-12 us per transfer in both
build and verify; (3) the miner's 1800 KiB size target caps a block at 16,029 transfers (336.6 M of 500 M gas), a
subnet-evm rule we keep; (4) gossip: the push gossiper carries ~1.8k tx/s to the peer (20 KiB per 100 ms tick), so a
node that receives no RPC load builds 180-tx blocks; the per-tx `Has` crossings of the push gossiper (x_pool 15-80k
per block interval at 600k pending) are microseconds each but the largest crossing count; (5) memory: 600k pooled txs
= 1.7-2.7 GB RSS (the pool keeps each tx decoded plus its envelope; the parsed-block cache holds the unaccepted chain),
and the gossip SDK's push tracking is the Go heap (300-450 MB). The 50k target needs (1) and (2) fixed: with the block
cap at 16k txs, 50k tx/s is 3.1 blocks/s, i.e. a build + peer verify + accept + finalization round under 320 ms.

## Consensus cadence: why acceptance came in bursts (branch rs-consensus off rs-mempool 0fe90b4, 2026-09-10 JST)

Setup: 2 all-ours validators, `--stress`, `--load 90s --rate 60000 --keys 1024 --workers 8 --batch 1000`, avalanchego
1.14.2 with `--node-log-level verbo` (every consensus vote and every network message logged; note that verbo writes
the whole message as base64, so 8 MB x 7 rotated files keep only the last ~15 s), and the harness now dumps the
snowman / request / throttler counters of every node (`consensusMetrics`) and the accept-gap distribution read
from the chain logs (`acceptGaps`). Extra avalanchego flags go in with `--node-flags k=v,k=v`.

Timeline of one burst (node Gbqa = builder of 127-131, node 5wdc = its peer; all times 11:14 JST):

| t | event |
|---|---|
| 36.56-36.87 | 40 polls finish on Gbqa within 300 ms (the peer's Chits arrive in one burst), heights 122-125 accepted |
| 36.97 | Gbqa is the slot-0 proposer of 126..131: builds 127 at 37.08, 128 at 37.31, 129 at 37.54, 130 at 37.77, 131 at 37.98 (116-124 ms each, one per ~230 ms, 16,029 txs each) |
| 37.12, 37.34, 37.58, 37.81, 38.02 | Gbqa sends each block as a PushQuery to 5wdc: 1.09-1.10 MB on the wire (1.65 MB of block, zstd) |
| 38.83, 40.96, 43.11, 45.28, 47.42 | 5wdc's read loop hands the five PushQuery bodies to the handler: exactly 2.14 s apart = 1.1 MB / 512 KiB/s. Between them the loop reads 2-8 messages per second (219 in the second before) |
| 39.33 onwards, every ~230 ms | Gbqa's pull queries to 5wdc time out (2 s, the `network-minimum-timeout` floor): `query_failed`, `poll finished votes=4..13` (< alpha 15), `no progress was made after processing pending blocks {numProcessing: 6}`; 27 such polls until 51.7. Every one clears the confidence of the whole processing chain (`RecordUnsuccessfulPoll`), so beta = 20 successful polls must start over |
| 38.02 | Gbqa is not the slot-0 proposer of 132: `slot time {"delay": "10s"}`, `Waiting until we should build a block 8.98s`; 5wdc is, but has not received 131 yet, so nobody builds until 47.1 (Gbqa, slot 2) |
| 51.13 | 5wdc reads the last block (132, 0.8 MB); it verifies 126-132 (~250 ms each under ctx.Lock), its preference reaches the tip, and 20 polls succeed within 1 s |
| 52.37-52.58 | 126..132 accepted on both nodes: 7 blocks in 210 ms after 15.5 s of nothing |

Cause: avalanchego's per-peer inbound bandwidth throttler (`network/throttling/bandwidth_throttler.go`, a token bucket
of `throttler-inbound-bandwidth-refill-rate` 512 KiB/s with a `throttler-inbound-bandwidth-max-burst-size` 2 MiB
burst) runs in the peer's read loop BEFORE the message body is read, so every message from that peer (Chits,
PullQuery, AppGossip) waits behind a block that waits for tokens. Metrics over the 2.5 min run:
`bandwidth_throttler_inbound_acquire_latency_sum` 114 s on one node and 25 s on the other, `requests_timeouts` 379 /
573, `polls_failed` 289 / 259 against ~700 successful, issue-to-accept latency (`blks_accepted_sum/count`) 6.8 s
average, 21 blocks rejected. Not the cause: the at-large / validator byte throttler (`byte_throttler_inbound_acquire_latency_sum` 6 ms
total, 6 MiB at-large and 32 MiB validator allocations never ran out), the handler queue (`unprocessed_msgs` 0), the
CPU / disk throttlers (no waits), the VM (build 116-124 ms, peer verify p99 195 ms, accept 32 ms), the proposervm
windows on their own (the slot-0 proposer builds within 100-250 ms of its parent; the 5 s slots only bite while the
proposer has not received the parent, which the throttle caused). Inbound volume that has to fit in 512 KiB/s: one
node received 62.7 MB of PushQuery + 18.2 MB of tx gossip in ~150 s = 540 KB/s, i.e. the budget exactly.

Fix (deployment requirement for every validator of a chain with MB blocks): raise the bandwidth throttle, e.g.
`--throttler-inbound-bandwidth-refill-rate=33554432 --throttler-inbound-bandwidth-max-burst-size=67108864` (32 MiB/s,
64 MiB burst; the harness passes them with `--node-flags`). Nothing else changed. Same 90 s run:

| | before (defaults) | after (32 MiB/s) |
|---|---|---|
| offered / refused | 18.3k tx/s (pool full after 20 s, 2.4 M refused) | 40.2k tx/s (130k refused near the end) |
| load blocks, txs | 9..67, 728,758 txs, 12,352 txs/block, 1,575 ms apart | 9..202, 2,892,367 txs, 14,909 txs/block, 496 ms apart |
| mined | ~8k tx/s (10k in the 2 min run) | ~30k tx/s over the accept span (2.89 M in 102 s) |
| accept gaps (both nodes) | p50 45 ms, p90 15.5 s, max 15.5 s in the 16 s window the logs kept; earlier 2 min run: p90 4.5 s, max 24.9 s, gaps > 2 s = 238 of 257 s | p50 438 / 441 ms, p90 873 / 978 ms, p99 1.6 s, max 2.1 / 2.2 s; gaps > 2 s sum 2.1 / 4.4 s of 102 s |
| issue-to-accept (avalanchego) | 6.8 s avg, health "block processing too long" | 2.1 s avg, blks_processing 6-7 (a 6-7 deep pipeline at 2 blocks/s) |
| request timeouts / failed polls | 379-573 / 259-289 | 3-74 / 9-16 |
| bandwidth throttler wait | 114 s / 25 s | 18 ms / 16 ms |
| rejected blocks | 21 | 0 |
| build p50 / verify p99 | 130 ms / 194 ms | 149 ms / 196 ms |

Ceiling with the fix: the chain accepts 2.0 blocks/s x 14.9k txs = 30k tx/s with full blocks at 16,029 txs, and the
per-height serialized path is now the bound: build 149 ms p50 + compress 15 ms + peer parse ~50 + verify ~200 ms
under ctx.Lock, plus the 100 ms WaitForEvent retry cadence, i.e. ~450-500 ms per height, the measured 496 ms.
50k tx/s = 3.1 blocks/s needs that under 320 ms (lower verify: the peer re-executes 16k transfers, 8-12 us each;
or a bigger block). Message size is not the next bound: a full 1800 KiB block is 1.65 MB raw and 1.1 MB compressed
against the 2 MiB `network-max-message-size`; the 1800 KiB miner target (subnet-evm parity) is the one that caps a
block at 16k transfers = 337 M of 500 M gas, and lifting it to 2.5 MB of tx bytes would put the compressed block at
~1.7 MB, still under the limit, for 25k txs per block. With 2 validators the proposervm slots stay as they are: the
slot-0 proposer of every height is one of the two (seeded by height), so the other node waits 5 s only if the
proposer has nothing to build or has not seen the parent; at 40k tx/s to both nodes neither happened (0 rejected
blocks, 188 "Waiting until we should build" lines per node are the non-proposer's normal wait for its slot, cut short
when the proposer's block arrives).

## Per-height cycle: where a 16k-tx height spends its time (branch rs-cycle off rust 7d9a694, 2026-09-10 JST)

Setup: 2 all-ours validators, `--stress`, `--load 60s --keys 1024 --workers 8 --batch 1000`, `--node-log-level debug`,
`--node-flags throttler-inbound-bandwidth-refill-rate=33554432,throttler-inbound-bandwidth-max-burst-size=67108864,log-rotater-max-size=512`,
box shared (another user's clickhouse at 100% of one core, other tabs idle-ish; 8 physical cores / 16 threads).
The shell now logs `validator: parsed` (height, bytes, took), `validator: verified` (height, txs, took),
`validator: wake` (WaitForEvent returning PendingTxs: the pool wait and the retry gap it slept), `validator: getblock`,
and `took` on the accepted line; avalanchego's debug log adds `sent message` / `forwarding sync message to consensus`
(messageOp push_query / chits), the proposervm's `built block` (blkID, height) and snowman's `adding block`.
`cmd/epochdb-validator/e2e/timeline.py LOGS_DIR [from to]` joins the two nodes' chain logs on one clock: the proposer of
h is the node whose `built block` id at h the other node added to consensus (both nodes build most heights: the
`proposer: self` field used to name only the LAST built id, so a node that built h+1 on its own unaccepted h never
logged itself as h's proposer; it is a set now). Segment medians / p90 over the load heights, all at 40k tx/s offered
on the same plugin build (7d9a694 + the log lines):

| segment | median | p90 |
|---|---|---|
| build (BuildBlock wall, 12.7k txs/block avg) | 146 ms | 201 |
| build done -> PushQuery sent (proposervm wrap, gRPC copy of 1.8 MB, zstd) | 38 | 60 |
| PushQuery sent -> peer handler recv | 3 | 101 |
| peer recv -> parse start (gRPC copy into the plugin) | 7 | 13 |
| parse (decode + 16k sender recoveries on the rayon pool) | 103 | 144 |
| parse done -> verify start (a second parse of the same bytes) | 15 | 28 |
| verify (execute + receipts root + state root) | 96 | 117 |
| verify done -> chits sent | 3 | 245 |
| chits sent -> chits recv at the proposer | 32 | 247 |
| chits recv -> accept(h) at the proposer | 1629 | 2469 |
| build start - accept(h-1) | -1880 | -464 |
| accept took (proposer / peer) | 21 / 8 | 45 / 22 |

Consensus is a PIPELINE: a block is built ~1.9 s before its parent is accepted and accepted ~2.1 s after it was
built (6-7 blocks processing), so `accept(h-1) -> wake` is not a segment of the cycle and the 100 ms retry gap
rarely bit (wake gap p50 26 ms on one node, 0 on the other). What bounds the height rate is the VM-lock work per
height: the proposer's build + wrap (~185 ms), then, when the next height's slot-0 proposer is the other node (the
proposervm seeds it by height, so about every second height), that node's parse + verify (~215 ms) before it can
build; when the same node proposes twice the two overlap. Hence 313 ms median between accepts at 40k offered
(365 ms per block over the run) and 479 ms at 60k offered with full blocks (base60k run).

Found and cut (each measured in-process with `vbench --synthetic 15364` on an idle box, 500 M gas genesis, then in
the 2-node run):

1. Parse decoded all 16k txs BEFORE looking the block up in the parsed cache, and the same bytes reach parse 2-3
   times per height (the proposervm's inner parse of its own block, the snowman PushQuery, `GetBlock` from the
   rpcchainvm server): 9-13 ms each on the critical path. The cache key is now `block::container_hash` (header
   only): a hit costs 0.5 ms (`getblock` p50 10.4 -> 0.5 ms, `parse done -> verify start` 15 -> 4 ms).
2. Parse decoded serially (9 ms), then recovered on the pool. `block::decode_container_par` splits the tx RLP,
   decodes per tx in parallel, takes the senders the pool already recovered at admission (`Pool::senders` by tx
   hash, 1-2 ms for 16k) and recovers only the rest. In this harness the peer holds ~5% of the block's txs (a
   generator worker sticks to one node and the push gossiper carries little), so recovery stays: 16k x ~40 us of
   libsecp256k1 = 640 ms of CPU, 72 ms on 14 threads of 8 physical cores when idle, 90-140 ms when the box is
   saturated. Under 15 ms is not reachable on this box; on a chain where gossip delivers the txs first the lookup
   removes the recovery.
3. The receipts trie was computed twice in a build (exec's `build_block`, then `assemble` again) and the tx trie
   once, all serial: `assemble` 28 ms in-process, 35 ms in the run. exec leaves the receipts root out
   (`defer_receipts_root`) and the engine computes the state root, the receipts root and the transactions root in one
   `rayon::join` in both verify and build (`eng_exec` 96 -> 74 ms, `eng_assemble` 35 -> 4.4 ms, `eng_root` 3.6 -> 25 ms
   because it now also spans the two other tries; the build crossing 168 -> 139 ms median at 60k). Verify also
   checks the header's transactionsRoot now (it was not checked; the receipts and state roots covered the txs
   indirectly). In-process: build 143 -> 105 ms, verify 103 -> 89 ms, parse 78 -> 72 ms (decode parallel; recovery is
   the rest).
4. WaitForEvent's 100 ms retry gap applied to every build within 100 ms of the previous one whatever the parent.
   It now applies only to a REPEATED build on the same preferred parent; a new preferred block (ours or a peer's)
   builds at once (the candidates skip the unaccepted ancestors' txs, so nothing is re-offered). Builds per accepted
   block stayed ~1.03 (no dropped or rejected blocks in any run).
5. `network-compression-type=none` (avalanchego flag): `build done -> PushQuery sent` 36 -> 15 ms; the wire carries
   1.65 MB instead of 1.1 MB per block, nothing else changes. A deployment choice for a LAN-class validator set.

Same 60 s runs, before (7d9a694) and after, 60k tx/s offered (the pool fills, blocks are full):

| | base 60k | after 60k | after 60k + compression none |
|---|---|---|---|
| offered / refused | 45.1k tx/s / 55k | 46.7k / 28k | 48.6k / 5.5k |
| load blocks, txs | 136 blocks, 1,939,779 txs, 14,263/block | 152, 2,149,810, 14,143/block | 161, 2,222,129, 13,802/block |
| ms between blocks / tx/s mined over the accept span | 479 / 27.4k | 435 / 30.1k | 416 / 30.9k |
| accept gaps p50 / p90 / p99 (two nodes) | 433 / 951 / 1513 and 416 / 909 / 1723 ms | 347 / 974 / 1872 and 361 / 995 / 1986 | 331 / 945 / 1615 and 307 / 907 / 1680 |
| engine build crossing median (16,029-tx builds) | 168 ms (exec 96, candidates 21, root 3.6, assemble 35) | 139 (exec 74, candidates 24, roots 25, assemble 4.4) | 138 |
| timeline: build wall / wrap / parse / verify median | n/a (no log lines) | 142 / 36 / 137 / 95 ms | 143 / 15 / 139 / 95 |
| plugin RSS at t=60 s | 2467 / 2467 MB | 2222 / 2319 | 1984 / 2418 |

At 40k offered both builds mine everything offered (2,393,024 txs each); after: 272 ms between blocks with 10.2k
txs/block against 365 ms with 12.7k (the chain drains the pool faster, so blocks are smaller), accept gaps p50 201
ms against 291. `cargo test --workspace --release` 75 passed, `go test -count=1 ./validator/` and `-tags epochdb_stub` ok.

What remains per 16k-tx height, for the parallel-execution round: the peer's parse ~90-140 ms (sender recovery,
CPU-bound: 640 ms of CPU per block; the pool lookup removes it only where gossip delivered the txs first), verify
exec ~74 ms (16k transfers at ~4.6 us each on one thread) plus 25 ms of tries, the proposer's build exec ~74 ms plus
the pool's candidate selection ~24 ms (24k candidates cloned out of the pool for a 16k block) plus 25 ms of tries,
and the proposervm wrap + gRPC copies (~15 ms without compression, ~7 ms into the peer plugin). Consensus itself
adds no idle time between heights at 2 validators: the proposervm's 5 s slot waits are the non-proposer's and are
cut short by the proposer's block. The box is the other bound: at 60k offered the two plugins and the generator use
13-16 of 16 threads (admission recovers every tx once per node, 46k tx/s x 40 us = 1.8 cores per node).

## Ingest dedup, the small-batch floor and push gossip capacity (branch rs-ingest off rust e6cdf12, 2026-09-11 JST)

The 5-validator EC2 profile of e6cdf12 (c7a.8xlarge, compare tab) said blocks were small (173 txs p50) because the
pool held only 700-1000 pending while 8-16k txs were in flight: ingest did not keep up, 45% of a plugin's cycles were
libsecp256k1 recovery. Cause: `Pool::add` decoded and RECOVERED every tx before any hash check, and every tx reaches
a node once by RPC and ~4x by push gossip.

Changes (each its own commit on `rs-ingest`):

1. `Pool::add` (84474a0): the hash (keccak of the envelope) comes first; one brief lock partitions the batch into
   known (pending in `by_hash`, mined in the last 200k txs kept in a `seen` ring filled by `on_accept`, or repeated
   inside the batch) and new; the lock is released; only the new txs are decoded, validated and recovered, on the
   pool's OWN rayon pool (`ingest-threads`, default min(cores / 2, 8)) instead of the global one, so ingest no longer
   shares threads with verify and build (the engine's main pool takes `workers`, default cores - 2; on a box running
   N plugins set `workers` = vCPU / N and `ingest-threads` = workers / 2). Known txs answer code 1 without a
   recovery. Counters in `epochdb_health`: `pool-dup` (txs answered Known by hash), `pool-recovered`, `pool-lock-ms`
   (lock held for the admission pass), `pool-add-ms` (wall inside add); the built line carries `poolDup`. In
   process (`cargo test -p epochdb-chain --lib ingest_dedup -- --nocapture`): `pool.add(1000)` new 8.1 ms, the same
   1000 again 0.40 ms, a stream with every tx 5 times interleaved 2.5 ms per 1000 with exactly 1000 recoveries in
   all; the admission lock 1.1 ms total for 6000 txs.
   Byzantine-safety rule kept: a recovery is skipped only when the keccak of the FULL signed envelope matches a tx
   we recovered ourselves (`by_hash`, the pool's own admissions, also what `Pool::senders` hands to block parse) or a
   tx of a block WE accepted after our own verify (`seen`, which answers "already known" only and never marks a tx
   verified). Per-tx results stay independent, verify is untouched, no consensus parameter changed.
2. Per-batch ingest timing on the RPC door (4ccbc8c): health `ingest-batches / -txs / -read-ms / -rpc-ms
   (the epochdb_rpc crossing: JSON + hex decode and pool.add) / -add-ms (= pool-add-ms) / -decode-ms (rpc minus add)
   / -rpc-p50-ms / -rpc-p99-ms / -inflight / -concurrency-max`, one `ingest=` field on the built line. Nothing on
   the Go side serializes batches: no mutex around the cgo call, ghttp `HandleSimple` runs per gRPC stream; the shared
   points are the pool lock (1-5 us per tx) and the engine's execution mutex for the state read of unseen senders.
3. The small-batch floor (114700e). EC2 with the client's 64 connections per node sliced the stream into ~6-tx
   bodies: `pool.add` cost ~0.75 ms per call (125 us per tx, against 8 us at 1000 per batch). The pool has no such
   floor (`small_batch_wall`: 1 tx 46 us, 6 txs 240 us = one inline libsecp256k1 recovery each, 64 txs 1.0 ms, 1000
   txs 7.9 ms; 6 known txs 2 us). The floor was `head_accounts`: a sender whose last tx was mined had its pool account
   pruned, so its next tx read (nonce, balance) under the engine's execution mutex, which verify and build hold for
   most of a busy height. Emptied sender accounts are now KEPT as the sender's state cache (re-read on every block
   that touches them like any held sender, swept after 60 s idle). `TestAdmitSmallBatches` through the engine: 6
   new senders 268 us, 6 cached senders after a block 258 us, 6 known 7 us. What remains per new tx is the recovery
   (~40 us of CPU on this box, parallel across the caller goroutines); a brand-new sender still costs one engine
   read per batch.
4. Push gossip capacity (7ae2314 -> 0d39cc2 -> e0cf7ad). The SDK's `PushGossiper` sends ONE message of
   `TargetMessageSize` (20 KiB, ~180 transfers) per `Gossip` call and the push loop called it once per 100 ms: 1.8k
   tx/s from a node to its peers whatever its pool held, so a node fed by gossip alone built 175-tx blocks. Three
   lessons, in order: (a) 7ae2314 called Gossip once per 64 KiB of newly drained bytes: the first tick after the RPC
   node filled its pool shipped 400k txs (44 MB per peer) at once, consensus messages queued behind the gossip,
   avalanchego's health said "block processing too long: 1m7s > 30s" (no disconnect) and the 3-node chain stalled at
   height 24 for the rest of the run. (b) 0d39cc2, one 64 KiB round per 25 ms tick (2.6 MB/s per peer), stalled the
   same way at avalanchego's DEFAULT per-peer inbound bandwidth throttle (`throttler-inbound-bandwidth-refill-rate`
   512 KiB/s, burst 2 MiB): a steady rate above the throttle delays every message from that peer, consensus included.
   With the throttle raised (refill 32 MiB/s, burst 64 MiB) the same plugin mined 1,429,733 txs in 60 s with RPC into
   one node. (c) e0cf7ad: the plugin cannot see the node's throttle, so the DEFAULT is the SDK's rate (20 KiB per
   `push-gossip-frequency` 100 ms, stock behaviour, safe at default flags) and the fast rate is a chain config choice.
   The Go bloom prefilter of inbound push messages (84474a0) is removed: a bloom filter answers Has for 1.00% of txs
   it never saw at target size (`TestGossipBloomFalsePositives`), 5% before a reset, which would silently drop
   first-time txs; the pool's exact hash check answers duplicates at 0.4 us each. With 5 equal validators
   `Top(0.9)` takes all five (0.8 < 0.9 pulls the fifth) and `Validators: 100` samples the rest.

   DEPLOYMENT RULE: to raise gossip past ~4k transfers/s per peer set BOTH, on every validator: chain config
   `"push-gossip-frequency":"25ms","push-gossip-target-bytes":65536` (2.6 MB/s per peer at most, ~23k transfers/s)
   AND node flags `throttler-inbound-bandwidth-refill-rate=33554432 throttler-inbound-bandwidth-max-burst-size=67108864`.
   The gossip keys without the node flags stall the chain. The e2e runs that shape with
   `--chain-config-extra '{"push-gossip-frequency":"25ms","push-gossip-target-bytes":65536}' --node-flags throttler-inbound-bandwidth-refill-rate=33554432,throttler-inbound-bandwidth-max-burst-size=67108864`
   and takes `--rpc-nodes N` to feed the load to the first N nodes only.
5. `rpc-direct-addr` (bf3b6dc, 20a233a): MEASUREMENT ONLY, off by default, no harness sets it. A plain net/http
   server inside the plugin process serving the same `/rpc` handler without avalanchego's HTTP server -> gRPC ghttp
   hop, to split a client's round trip (EC2: ~24 ms per 5-tx request, of which 0.1 ms in the plugin handler) into
   the hop and the plugin. It bypasses avalanchego's HTTP auth, API throttling and TLS: it binds loopback or private
   (RFC 1918 / link-local) addresses only unless `rpc-direct-allow-public: true` is set too, and logs a WARN saying
   so every time it is on. Never on a production node.

Measurements, 3 all-ours validators on this box (16 threads shared, so accept gaps > 2 s summed 40-55 s of every 70 s
span in the fan-out runs and the box, not the plugin, bounds them; the before/after pairs are like for like),
`e2e --ours-n 3 --stock-n 0 --stress --load 60s --rate 40000 --keys 1024 --workers 8 --batch 1000`:

| run | mined (load blocks) | txs/block | ms between blocks | peers' blocks / pending at build | plugin CPU (peak min) |
|---|---|---|---|---|---|
| before e6cdf12, fan-out (a worker per node) | 680,717 | 11,159 | 1022 | all three 16,029 at 94-217k pending | 2.1-2.4 cores each |
| 84474a0, fan-out | 634,672 | 10,578 | 777 | all three 16,029 at 145-300k pending; poolDup 288-616k per node | 1.9-2.3 cores each |
| before e6cdf12, RPC into ONE node | 390,088 | 4,816 | 791 | peers 179 txs at 179 pending; peers recovered 280k each | RPC node 3.7, peers 1.0 |
| 4ccbc8c, RPC into one node | 392,395 | 3,964 | 616 | peers 178 txs at 179 pending; RPC node poolDup 117k, peers 8-26k | 3.2 / 0.9 / 0.9 |
| 0d39cc2 or e0cf7ad + gossip keys, default node flags | STALL at height 24-25 | | | consensus starved behind gossip | |
| 0d39cc2, raised node throttle | 1,429,733 | 6,499 | 306 | peers 1,140-1,150 txs (max 16,029) at 2.3-3.4k pending; each peer recovered 1.39M | 3.5 / 2.8 / 2.8 |
| e0cf7ad + gossip keys, raised node throttle | 1,375,449 | 5,707 | 274 | peers ~1,150 txs; each peer recovered 1.32M | |

Dedup counters, RPC-into-one shape (e0cf7ad + keys, 60 s): RPC node pool-recovered 1.99M (every RPC tx once, incl.
the 1.3M a full pool then refused), pool-dup 2.47M (gossip echoes of its own txs), pool-lock-ms 23.7 s over 4.5M
txs = 5 us per tx under the lock at 600k pending; peers pool-recovered 1.32M, pool-dup 1.25M, lock 1.4-1.6 s (1.1 us
per tx). perf on the RPC node (20 s during load, 199 Hz): libsecp256k1 72% of cycles before, 69% after (that node
recovers every RPC tx once by design and the duplicates it no longer recovers were the gossip echoes; the saving
shows on the peers as CPU per tx admitted), keccak 3%, rayon 1.3% -> 0.6%, `Inner::settle` 6.5% -> 8.3% (it walks
the sender's whole BTreeMap per insert, O(txs per sender) at 580 pending per sender: the next pool cost).

EC2 scaling control (compare tab, 5 validators, 1000-tx batches, 4ccbc8c): client fanning every tx to every node
31.7k mined/s, RPC into ONE node 20.2k, round-robin one node per tx 13.2k; node count x mined/s stayed ~43-53k
box-wide with the fan-out client, which smells like the client's total send rate, so the single-entry-node number is
the one that matters. The earlier 1.9k single-entry run was the entry node choking on 6-tx batches (item 3), not
push gossip.

Open items: a full pool still decodes and recovers txs it then refuses (the RPC node recovered 1.3M refused txs
here; refusing on size before the recovery needs the tip, i.e. the decode but not the recovery: reorder validate ->
cap check -> recover); two concurrent adds of the same new tx both recover it (an in-flight set); `Inner::settle`
per insert; a brand-new sender's state read still takes the engine's execution mutex once per batch (a lock-free
head snapshot, or batching the reads of concurrent calls); the gossip rate is a manual pairing of a chain config
key with a node flag, the plugin cannot check the node's throttle.

## Accept-window pool crossings and the proposer's block-sent lap (branch go-accept off rust e751275, 2026-09-11 JST)

Fleet round 2 (5 validators, ~7.4k-tx blocks) showed `x_pool` = 8942 per accept-to-accept window on a peer, one
`epochdb_pool_*` crossing per tx. Source: the SDK's `PushGossiper.gossip` asks `set.Has(id)` for EVERY tx it is
about to push (each new tx once, each regossip round again) and `gossipSet.Has` was `epochdb_pool_has`, one cgo
call per tx. The count is bounded by the push rate, not the block: 16k-tx blocks here at the SDK default rate gave
x_pool p50 4032 / 3369 per window, the fleet's 25 ms / 64 KiB gossip keys gave 8.9k.

Change: the pool records every hash that leaves `by_hash` (mined by `on_accept`, replaced, unpayable or expired in
`settle` / `expire`, evicted) in a `gone` deque next to the `gossip` deque, and `epochdb_pool_drain_gossip` returns
both under ONE lock as RLP `[[envelopes...], [hashes...]]`; Go keeps `gossipSet.held` = drained ids minus gone ids
(adds before deletes, so a tx admitted and dropped between two drains cancels out) and answers `Has` from it, no
crossing. `epochdb_pool_has` stays in the ABI, unused by the plugin. `TestGossipHasFollowsPool` covers admit ->
held, accept -> gone after the next drain. Verification and consensus untouched.

Also: `validator: block-sent {height, id, t_since_built_ms}` on the proposer, logged once per height from the
Verify of a block we built: rpcchainvm's BuildBlock response already carries the bytes (there is no later Bytes()
fetch), and BlockVerify (re-parse + verify of our own block) is the last VM call before consensus adds the block and
PushQueries it, so it is the closest observable "sent" point. Peer timeline per height stays parsed -> verified ->
accepted. `parsed` is logged TWICE per height on a peer: rpcchainvm ParseBlock (the PushQuery/Put bytes, through
proposervm's inner-block parse) and BlockVerify, which re-parses the bytes it was handed before Verify; on the
proposer only BlockVerify parses. `getblock` once per height is BlockAccept fetching the block by id.

Measured, 2 all-ours --stress validators on this box (16 threads shared), `e2e --ours-n 2 --stock-n 0 --stress
--load 45s --rate 40000 --keys 1024 --workers 8 --batch 1000`, heights of >= 3k txs (p50 16,029 txs, 69-71 heights):

| build | x_pool per accept window p50 / max | x_total p50 | accept took p50 / p90 |
|---|---|---|---|
| e751275 node A / B | 4032 / 95,559 and 3369 / 60,811 | 4036 / 3375 | 32.8 / 85.9 ms and 24.2 / 65.9 ms |
| go-accept node A / B | 3 / 564 and 2 / 662 | 9 / 8 | 29.4 / 78.5 ms and 27.2 / 78.4 ms |

The remaining x_pool are the RPC door's batch adds (one per 1000-tx batch), the tick's drain and the status reads.
`accept took` did NOT move: the Has crossings ran in the push loop's goroutine, never inside Accept, so accept's
27-33 ms for 16k txs (fleet: 11 ms for 7.4k) is the engine's own `epochdb_accept` (state commit + `on_accept` over
the block's txs) plus one head header read; the "under 3 ms" target needs engine work, not Go. `block-sent` came
p50 10-11 ms (max 45-88 ms) after `built` on both nodes.

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

1. Pool: the engine's own (`rs/chain/src/pool.rs`, "Mempool in the engine"; the libevm pool of the earlier runs is
   gone). The gossip set is a 140-line adaptation of `plugin/evm/eth_gossiper.go` (same wire format, bloom,
   push/pull) over the engine's pool: subnet-evm/core links firewood's Rust staticlib and two Rust runtimes cannot
   share one binary.
2. blst/secp256k1/jemalloc/Rust runtime symbols: the archive is localized by `rs/ffi/localize.sh` (17 `epochdb_*`
   globals only), so it links next to avalanchego's bls and firewood without `--allow-multiple-definition`.
3. `/ws` is not mounted (the engine's ws server has no socket door through the FFI).
4. Resolved by the engine pool: `txpool_content` / `eth_pendingTransactions` return geth's RPCTransaction shape (with
   `from`, null block fields).
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

## Build pipelining: wakes that build nothing, and a wake that fires only for free txs (branch go-pipeline off rust f270a0c, 2026-09-11 JST)

Fleet round 3 (5 validators): `validator: wake` 713 vs `built` 386 on a proposer, heights in flight p50 0, accepted ->
next built 68 ms p50. How avalanchego drives the wake (read from 1.14.3 source, nothing of it changed): the handler's
`NotificationForwarder` calls `WaitForEvent` once, forwards the `PendingTxs` as one `Notify`, and re-subscribes only
after the `ChangeNotifier` (wrapping the proposervm) fires `OnChange`, i.e. after every `BuildBlock` attempt (dropped
or not) and every changed `SetPreference`; `OnChange` also CANCELS a WaitForEvent in progress, so a preference change
already interrupted our retry-gap sleep through the gRPC context. proposervm's own `WaitForEvent` only calls ours when
`timeToBuild` (the Windower's slot for THIS node on the current preferred parent, `proposervm-min-block-delay` 0 from
tmpnet) is due, and its `BuildBlock` drops with `errUnexpectedProposer` ("build block dropped" debug line, snowman's
`blks_built_failed` counter) when the slot moved between the wake and the call. So one wake pairs with at most one
BuildBlock, and a wake with no block is one of:

| `validator: build-skip` reason | where | means |
|---|---|---|
| `proposervm-window` | next WaitForEvent entry | PendingTxs returned, no BuildBlock reached the VM before the next subscription: proposervm dropped it (a) |
| `engine-empty` | BuildBlock | `epochdb_build` included nothing: every candidate held by the parent chain or skipped (b) |
| `engine-error` | BuildBlock | `epochdb_build` failed |
| `retry-gap-wait` / `min-delay-wait` | WaitForEvent | the 100 ms gap / Granite delay was cut by a preference change (`by`: preference or cancel, `waited`: time lost) |

Counters (`wakes`, `builds`, `blocks`, `skip*`) ride on every `validator: built` line and in the health check under
`build`. Fleet reading for (a): the plugin's `skipProposervmWindow`, or avalanchego's `avalanche_<chain>_blks_built_failed`
minus the plugin's `epochdb_build_empty_total` (a dropped BuildBlock never reaches the VM, an empty one does).

Change (7e8fcec): `epochdb_pool_wait(parent_id)`: the pool's wait predicate (`Pool::wait_free`, `Inner::has_free`)
counts only executable txs the unaccepted chain under the preferred block does not hold, the same skip set the build
uses (`node_engine::held_nonces`, shared), so the wake fires only when a build would include something; the retry-gap
and min-delay waits also end on our own `SetPreference` (a channel the builder closes) and re-evaluate against the
new parent; pool wait slice 200 -> 50 ms. Retry-gap semantics unchanged (100 ms after a build on the same parent).

Local proof, 3 all-ours --stress validators on this 16-thread box, `e2e --ours-n 3 --stock-n 0 --stress --load 60s
--rate 40000 --keys 1024 --workers 8 --batch 1000 --chain-config-extra '{"push-gossip-frequency":"25ms",
"push-gossip-target-bytes":262144}' --node-flags throttler-inbound-bandwidth-refill-rate=33554432,
throttler-inbound-bandwidth-max-burst-size=67108864 --node-log-level debug`, load heights 9..155, per node:

| build | wakes / built per node | build-skip | proposervm "build block dropped" | in flight after accept p50 / p90 / max | accepted(h-1) -> built(h) p50 | blocks/s | txs/block | mined/s |
|---|---|---|---|---|---|---|---|---|
| f270a0c (before) | 62/52, 43/41, 68/55 | not classified; snowman "failed building block" = 14, 6, 23, all `no transactions to build with` (engine-empty) | 0, 0, 0 | 3 / 7 / 10 | -1.5 to -1.8 s (built before the parent accepted) | 1.72 | 14,082 | 24.2k |
| 7e8fcec minus the SetPreference channel | 46/46, 42/42, 60/61 | 0 engine-empty, 0 gap cuts, 2 proposervm-window (functional phase) | 0, 0, 0 | 3 / 6 / 9 | -1.6 s | 1.72 | 14,001 | 24.1k |
| 7e8fcec | 52/52, 48/48, 46/46 | 0 engine-empty, 0 gap cuts, 1 proposervm-window in the load phase = the one "build block dropped" avalanchego logged on that node | 0, 0, 1 | 4 / 6 / 11 | -1.7 to -2.3 s | 1.58 | 13,751 | 21.8k (run-to-run noise on this shared box: 641 vs 608 ms between blocks, CPU-bound 14k-tx blocks) |

Reading: on this box (b) is everything: every wasted wake before was an empty engine build (14 + 6 + 23 of 173 wakes,
32%), and with the free-tx wait wakes == builds == blocks; proposervm dropped nothing here because a 3-node chain with
600 ms blocks rarely moves the slot between wake and build. The fleet's 5-node round-3 pattern (wakes without any
BuildBlock) is (a), now countable as `skipProposervmWindow` without debug logs. This box cannot reproduce the fleet's
"in flight p50 0": builds start 1.5-2 s BEFORE the parent is accepted here (in flight p50 3-4) because verify + accept
of 14k-tx blocks, not the wake, paces it; the fleet's 4k-tx / 30-50 ms regime needs the fleet run for the before/after
on accepted -> next built. Nothing in verification or consensus changed; proposervm's window rules are untouched.
`go test -count=1 ./validator/` (real and `-tags epochdb_stub`), `cargo test -p epochdb-chain` pass.

## Block fill policy: hold a fresh parent until the block is worth a poll (branch go-fill off rust 6847dd0, 2026-09-11 JST)

Fleet round 5 (16-vCPU validators, go-pipeline): the proposer built at the first free tx; blocks shrank (5121 vs 7808
included at 16k in flight), blocks/s rose 4 -> 5-6 and mined/s FELL 29% because polls per block stayed at 22. More
smaller blocks lose at a fixed poll cost per block. Chain config keys (read by the Go shell like the gossip keys):

| key | default | means |
|---|---|---|
| `build-fill-target` | 0 | free executable txs the pool must hold before we propose on a FRESH preferred parent (0 = off) |
| `build-fill-wait-ms` | 0 | longest hold since the parent became preferred; then we propose with what there is (0 = off) |
| `build-fill-adaptive` | false | target = the last block's included count, floored at `build-fill-target` (a block the gas/bytes limits cut is that size already, so the cap is implicit) |

Semantics (`fillPolicy` in `validator/build.go`, inside `waitForEvent` after the free-tx wait): fresh parent = not the
parent we last built on. If free < target and the wait since the preference moved is not up, re-check every 5 ms
(`epochdb_pool_wait` now returns the free COUNT, one crossing per check) or at once on a preference change, which also
resets the clock. A repeated build on the same parent keeps the 100 ms retry gap and never fill-waits. A lone tx is
therefore mined within `build-fill-wait-ms` of the preference move, or at once when the parent has been preferred
longer than that (idle chain). The `built` and `wake` lines carry `fillWaited` and `freeAtBuild`. Nothing in
verification or consensus changed; the policy only delays when WE propose. Recommended fleet start: `"build-fill-target":
8000, "build-fill-wait-ms": 150`, or `"build-fill-adaptive": true` with the same floor and wait.
`TestBuildFillPolicy` covers the three cases (lone tx waits ~wait, repeated parent takes the gap, target reached adds
no wait).

Local proof (3 all-ours --stress validators on this 16-thread box, the go-pipeline recipe: `e2e --ours-n 3 --stock-n 0
--stress --load 60s --rate 40000 --keys 1024 --workers 8 --batch 1000 --chain-config-extra '{"push-gossip-frequency":
"25ms","push-gossip-target-bytes":262144[,"build-fill-target":8000,"build-fill-wait-ms":150]}' --node-flags
throttler-inbound-bandwidth-refill-rate=33554432,throttler-inbound-bandwidth-max-burst-size=67108864`), load heights
9.., per node (own blocks = the blocks that node proposed):

| policy | wakes / built per node | blocks/s | txs/block | mined/s | own blocks included p50 / min | fillWaited p50 / p90 / max (blocks > 0) | free at build p50 | in flight after accept p50 |
|---|---|---|---|---|---|---|---|---|
| off (309b0d5) | 40/40, 50/50, 56/56 | 1.76 | 14,305 | 25.2k | 16,029 / 24-100 | 0 / 0 / 0 (0 of 146) | 181k-204k | 3 |
| target 8000, wait 150 ms | 52/53, 54/53, 52/52 | 1.72 | 14,209 | 24.4k | 16,029 / 24-100 | 0 / 0-5 ms / 30-125 ms (8 of 158) | 76k-144k | 3 |

Reading: this box cannot show the fleet's regime. The pool here holds 76k-200k free txs at every build (37k tx/s
offered into 600 ms CPU-bound 16k-tx blocks), so the 8000 target is met at once and the policy engaged on 8 of 158
blocks, at the load's edges (the first blocks and the tail, where the pool drained; the max hold was 125 ms, under
the 150 ms cap: never a stall), for the same blocks/s, txs/block and mined/s within run noise (blocks identical on the
3 nodes both runs; the off run's 81,556 refused sends are the generator hitting the pool caps at 373k pending, not the
proposer). What it proves: the policy costs nothing when the pool is deep, holds a thin parent at most `wait`, and
the repeated-parent gap is untouched (0 gap cuts, 0 engine-empty). The fleet's 4-5k-tx blocks at 16k in flight are
where `free < 8000` at a fresh parent is the norm, and only the fleet run shows whether holding 150 ms brings the
included count back to ~8k and mined/s above 37.5k; if the hold is too short there, raise `build-fill-wait-ms` before
the target, and `build-fill-adaptive` tracks the last block instead of a fixed number.

## Pool lock starvation, empty builds and the 7500-tx cap (branch go-stall off rust dce2af4, 2026-09-11 JST)

Fleet round 7 (5 x 16-vCPU validators, fill 8000 / 150 ms, account slots 4096 / global 131072, 64k in flight): accept
gaps of 18-19 s on every node with the chain frozen at 1090 and the proposer's `wake` line at `poolWait 18.1 s, free
7337` (13.8 s and 9.0 s on two others); engine-empty builds with `candidates 0` while the pool held 32k pending and
10-16k free (13-22 per node per run, 60 in the ERC20 run, each with the 99 ms retry gap); ERC20 blocks of exactly
7500 txs = 325 M gas of a 500 M limit.

Instrumentation (kept): every pool lock acquisition goes through `Pool::lock(who)`, which prints
`epochdb-rs: pool: <who> waited N for the lock` / `<who> held the lock N` when either passes 50 ms (`SLOW_LOCK`);
`wait_free_count` prints its lock wait, predicate time and free_count time when the call overruns its slice;
`epochdb_pool_wait` prints when the pending-chain lookup (the tree lock, held by verify and accept) took over 50 ms.

Local before (dce2af4 + the timers, 3 all-ours --stress nodes, `e2e --load 90s --rate 40000 --keys 1024 --workers 8
--batch 1000 --account-slots 4096`, fill 8000/150, gossip 25 ms / 262144, global slots 131072, 16k-tx blocks, pool
190k-509k): `add-insert held the lock` 87-113 times per node, p50 58 ms, max 153 ms (one 1000-tx JSON-RPC batch);
`on_accept held` p50 59 ms, max 123 ms; every other caller waited 60-80 ms p50, up to 285 ms (`add-filter`,
`add-need`, `drain_gossip`, `status`, `candidates`, `on_accept`); the wait's own predicate was 2 us over 1 check and
free_count 200 us over 1024 skip senders; the pending-chain lookup never passed 50 ms. No multi-second wait here.

(1) Root cause of the insert hold: `Inner::insert` ended in `settle(sender)`, which rescans EVERY tx of the sender
(the drop pass: nonce below, unpayable, over-gas; then the executable prefix from the state nonce), so one insert cost
O(the sender's depth) and a 1000-tx batch of 1024 senders 400 deep cost ~800k BTreeMap steps under the lock. The
fleet's deeper senders (account slots 4096) and four RPC connections re-taking the lock back to back starve the
builder's `wait_free_count` (std's mutex is unfair: a thread that just released and re-locks wins) for as long as the
batches keep coming: the 9-18 s `poolWait`, ending with free 7-12k as soon as one acquisition got through. Fix:
`insert` calls `promote(sender)`, which extends the prefix from its current end over the now-contiguous nonces and
refreshes the head key, O(txs promoted); a replacement inside the prefix moves its cost in place. `settle` (drop pass
+ `promote`) stays for the paths where the sender's state moved: `on_accept` (per touched sender) and the full-pool
eviction. `on_accept` stays O(block): 16k `remove_hash` (hash map + priced BTreeSet removals) plus the touched
senders' settle, 58 ms p50 per 16k-tx block here, once per block.

(2) Root cause of the empty builds: `wait_free_count` and `candidates` used the same skip set (the unaccepted chain's
held nonces) but `candidates` also leaves out a sender whose head cannot pay the block's base fee (`eff()` None when
fee cap < base fee) and the free count did not, so a base fee above the txs' fee caps (a burst of 300 M gas blocks on
a fee window tuned for far less) made the wait fire, the build include nothing, and the retry gap tick every 100 ms
until the base fee decayed (the 2.2-2.6 s accept gaps). Fix: `epochdb_pool_wait` prices the block a build on the
preferred parent would pay now (`Pool::next_base_fee` = `rpc::fee::next_base_fee` with the accepted head's fee
config, kept in `pool::Head.fee`, and the parent header) and `has_free` / `free_count` count a sender only when its
first free tx pays it (`free_of`), the same rule `candidates` starts a sender with. A later build pays the same or
less (the window shifts with time), so a wake never has less than it was promised.

(3) Root cause of 7500: `candidates` cut the list when the summed DECLARED gas limits reached 1.5x the block gas
limit; ERC20 transfers sent with a 100k limit use 43k, so 750 M / 100k = 7500 candidates, executed to 322 M gas,
and the block stopped there with the pool full. Fix: the budget counts each tx's intrinsic gas (`intrinsic_gas`, a
lower bound of what it uses), so a list cut at 1.5x the gas limit always fills the block; the byte cap
(1.125 x the size target) is unchanged. `gas_budget_counts_intrinsic_gas_not_the_declared_limit`: 100 senders
declaring 100k for 21k transfers, budget 1.5 M: 72 candidates, not 15.

Local after (all three fixes, same recipe with `--keys 256 --load 180s` and the fleet's `tx-pool-global-queue` 1024, so
the pool sat at 60-117k pending like the fleet's 36-59k, 14.3-14.6k txs/block, heights 9..356 in 3m07s), per node:

| | before (dce2af4, 90 s, 1024 keys) | after (go-stall, 180 s, 256 keys) |
|---|---|---|
| `add-insert held the lock` > 50 ms | 87 / 111 / 113 (p50 58 ms, max 153 ms) | 0 / 0 / 0 |
| other callers waited > 50 ms (add-filter + add-need + drain_gossip + status) | 1012 / 1225 / 1170 | 59 / 91 / 48 |
| `on_accept held` > 50 ms | 44 / 34 / 40 (max 123 ms) | 29 / 39 / 37 (max 108 ms; O(block), once per 16k-tx block) |
| `wake` poolWait, load heights | p50 8-15 ms, p90 69-120 ms | p50 5 ms, p90 35-69 ms, max 137 ms (the two 215 / 249 ms are heads 9 and 11, the functional phase's near-empty pool) |
| engine-empty / any build-skip | 0 / 0 | 0 / 0 (137 + 106 + 104 wakes = 356 blocks) |
| accept gaps | p99 4.7-5.0 s, max 6.6 s, sum of gaps > 2 s 34-35 s per 100 s | p99 2.0-3.4 s, max 3.0-4.2 s, sum 10-25 s per 187 s |

The accept gaps left are this shared 16-thread box verifying 16k-tx blocks (verify p99 450 ms, three nodes plus the
generator), not the pool: no pool wait passed 140 ms in the load phase and no lock was held over 112 ms. The 7500
cap cannot show with 21k transfers (declared = intrinsic); the unit test carries the ERC20 shape. Not reproduced here:
a 16 s freeze with `blks_processing 0` on every node at the same height. One candidate the fleet logs can settle that
this box cannot: a roll (`epochdb-rs: roll N start` / `roll N done ... total=`), whose `finish_roll` (overlay replay
into Dirty, the store sync, `flush_dirty`'s root) runs inside Accept under the engine mutex AND the tree lock that
`epochdb_pool_wait` takes for the pending chain, at the same height on every node (the 2 GB budget fills at the same
rate everywhere). Grep the round-7 plugin logs around height 1090 for `epochdb-rs: roll`. `cargo test -p
epochdb-chain`, `go test -count=1 ./validator/` pass.

## Run 10: the re-pricing client's pending-0 / queued-N stall (branch go-gap off rust 1c9d734, 2026-09-11 JST)

The fleet (round 9) saw 30-53k QUEUED txs with ZERO executable on every node for 10-18 s under a client that re-reads
`eth_gasPrice` every second and prices at 1.5x it: every sender's next nonce simply absent. Reproduced locally with a
new `e2e --gas-ramp 1.5` (the generator re-reads `SuggestGasPrice` from node 0 once a second and signs every tx with
fee cap = tip = ramp x that price): 3 all-ours `--stress` nodes, the fleet's pool caps (`--account-slots 4096`,
`--chain-config-extra` global-slots/global-queue 131072, account-queue 4096), 8 workers x 500-tx batches, ~40k tx/s
offered, 3 min. The stall reproduces: `validator: pool quiet ... pending 0, queued` up to 142029 fired 1090 times
across the nodes, acceptance p99 13-16 s (max 22 s), blocks stayed full when they came (12926 tx/block avg). The
client's `eth_gasPrice` ran 4.5 -> 6.75 -> 10.1 gwei over the run while base fee sat at the 1 gwei floor: the runaway
oracle loop the fleet noted, client-side, not touched.

Two mechanisms, both surfaced by the new diagnostic (`epochdb_pool_gaps`: per stuck sender the pool nonce, the state
nonce, the lowest queued nonce, and each gap nonce's fate from a nonce-keyed per-sender ring; `pool-removed` in health
counts removals by reason):

1. A QUEUED arrival evicting an executable head. In a full pool the globally cheapest tx (lowest tip) is, under a
   client whose later nonces are pricier, some sender's oldest = its executable head. A gapped newcomer evicting it
   manufactured a permanent nonce gap, and each further gapped arrival knocked out one more head: pending collapses.
   libevm refuses this (`ErrFutureReplacePending`); `pool.rs` did not. FIXED (ea1ee21, merged): a gapped arrival whose
   victim is executable is refused "future transaction tries to replace pending" (code 9, stricter than before, never
   looser). The client saw this message 158 times in the run; `pool-removed.evicted-full` still reached 154182 (the
   allowed evictions of other senders' cheapest heads by an executable newcomer, exactly as libevm).

2. The dominant remaining cause, and NOT ours to change: a sender's next needed nonce, priced BELOW a full pool's
   cheapest tx, is refused "transaction underpriced" (the client's 705 underpriced errors). Under the re-pricing ramp
   a sender's oldest (lowest) nonce carries its lowest price, so at the global cap it is the one tx the full pool will
   not admit, while the sender's pricier higher nonces evict cheaper txs and pile up queued: pending 0, queued N. This
   is legacypool-faithful (a full pool rejects an underpriced newcomer, `ErrUnderpriced`), so admission keeps it;
   loosening it would drop below libevm. It heals on re-send once the pool drains (the round-9 control at constant
   price: 46k resubmits, no stall), because the same bytes are re-evaluated (nothing remembers a rejected hash; the
   `seen` ring holds only mined hashes). The unit test `gap_report_retains_the_low_nonce_under_churn` reproduces the
   lockout and the ring retention; `queued_arrival_never_evicts_an_executable_head` covers the fix.

Answers to the audit (pool.rs at 1c9d734): (a) no price ordering within a sender's sequence, replacement compares only
the same nonce (fee cap AND tip > old and >= old x 1.10); (b) a hash rejected for a transient reason and re-sent
unchanged is re-evaluated (the filter skips only `by_hash` and the mined-only `seen` ring), not swallowed; (c)
eviction picks the lowest-tip remote tx regardless of nonce, so a lower nonce WAS droppable while higher ones stayed,
now blocked for a gapped newcomer (fix 1); (d) the cost check spans the executable run plus the newcomer minus any tx
it replaces, so a rising-price sequence is refused Funds, not silently gapped; (e) `promote` has no price test, a
cheaper next nonce still promotes. `cargo test -p epochdb-chain` (28), `go test -count=1 ./validator/` pass.

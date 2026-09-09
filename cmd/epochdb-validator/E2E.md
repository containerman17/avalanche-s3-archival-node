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

## Run 3: TODO (all-ours 3-node, 10 min at 2000 tx/s, fixed pool admission) and Run 4: TODO (--stress genesis, 5 min)

## Deviations and open items

1. Pool: libevm's `core/txpool` instead of subnet-evm's. subnet-evm/core links firewood's Rust staticlib and two Rust
   runtimes cannot share one binary (`rust_eh_personality` and the allocator shims collide). Same lineage; missing
   `SetMinFee` (a tx under the chain's min base fee is held until it expires, the build filter skips it) and the
   fee-config gas limit check at admission. The gossip set is a 90-line adaptation of `plugin/evm/eth_gossiper.go`
   (same wire format, bloom, push/pull), for the same reason.
2. blst: avalanchego's bls (supranational blst via cgo) and the engine's `blst` crate both define the assembly symbols;
   linked with `-Wl,--allow-multiple-definition`. Open item for rs/ffi: localize blst symbols in the staticlib.
3. `/ws` is not mounted (the engine's ws server has no socket door through the FFI).
4. `txpool_content` / `eth_pendingTransactions` return libevm's tx JSON (hash, no `from`), not ethapi's RPCTransaction.
5. Coinbase: `allowFeeRecipients=false` -> blackhole, else the config's `feeRecipient`; a RewardManager precompile is
   not read. `GetFeeConfigAt`-style runtime fee config changes (FeeManager) are not read by the Go side (the engine
   enforces them at build/verify).
6. tmpnet has no ConvertSubnetToL1 helper: the "L1" is a 5-validator permissioned subnet.
7. Build retries: like subnet-evm, WaitForEvent re-arms 100 ms after a build whose block is not yet accepted, so a
   height can cost up to 6 engine builds (~10 ms each at 600 txs). A cheap improvement is to skip the retry while our
   last built block is still preferred.
8. Engine build vs verify: at ~600 txs build averaged 46 ms against 2.7 ms verify in Run 1 (p50 4.5 ms in Run 2, so
   the average is dominated by a p99 tail of ~0.5 s); worth a profile on the rs side.

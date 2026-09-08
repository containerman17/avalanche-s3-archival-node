# epochdb-rs end to end: beam from genesis to the tip under the real Go host

Branch `rust-e2e` (from `rust` = 4f91788). 2026-09-09 JST, local i7-10700K 8C/16T, 25 GB, WSL2, other agents' work running beside it (load 2 to 22). Everything below ran on this machine; nothing touched the Tokyo box.

Chain: beam mainnet, blockchainID `2tmrrBo1Lgt1mzzvPSFt73kkQKFas5d1AP88tv9cicwoFp8BSn`, subnet `eYwmVU67LmSfZb1RwqCMhBYkFyG8ftxn6jAwqzFmxC9STBWLC`, chainId 4337, 233 validators, ~9.555M blocks at the tip. Public reference RPC `https://build.onbeam.com/rpc` (Cloudflare in front: refuses Python's default user agent with `403 error code: 1010`, so the probes send `User-Agent: curl/8.5.0`).

## What was proven

| claim | evidence |
|---|---|
| live fetch feeds the plugin | `cmd/epochdb-host` (fetch package, 54 to 56 archival peers) fed 9,554,972 blocks from genesis over rpcchainvm to `epochdb-rs`, `wait=0.0s full=9.8s` on every 10 s line: the ring stayed full, the plugin was the limiter, never the fetch |
| every block root-checks | the checker compared every block's computed root with the header root: `root-checked=3423561`, `3689016`, `1350295` on the exit lines of the three long runs, plus the recovery `root ok` on every restart; one mismatch found and fixed (below) |
| the store seals runs | `store: sealed run <sha> [blocks N..N+50000] in 1.0-2.4s` every 50,000 blocks, 191 seals in total; two terminal merges (`merged 69 L0 runs into terminal run 15dbe7... [tx 0..8038087, blocks 1..3450000]` in 278.6 s, `merged 73 L0 runs ... [blocks 3450001..7100000]` in 235.6 s) while syncing |
| the plugin follows the tip | `epochdb-host: caught up at height=9554955, plugin in NormalOp`, `epochdb-rs: state STATE_NORMAL_OP`, the tip roll, then one block per Accept as beam produced them; 20 samples over 10 minutes against `build.onbeam.com` `eth_blockNumber`: 19 equal, 1 one block behind at the sampling instant (below) |
| RPC through the host mount | 53 of 53 probes byte-equal to build.onbeam.com (blocks, receipts, txs, callTracer, balances, nonces, code, storage, logs, block 1,000,000 and balances at that height) |
| `/ws` newHeads at the tip | 11 + 3 newHeads received through the host's `/ws` mount, each 1.2 to 3.0 s after the block's timestamp, every hash equal to `eth_getBlockByNumber` |
| kill -9 at the tip recovers and follows | `kill -9` of plugin and host at 9,554,969, restart: `recovered: rolled at 9554955 (gen 1), head 9554969 ..., rows replayed 81, ..., root ok, in 339 ms`, caught up at 9,554,970, three more newHeads, 5 minutes of tip follow equal to build.onbeam.com |
| kill -9 mid-sync recovers | twice (at 4,232,319 and 4,487,634): 34.7M and 36.2M rows replayed in 89 and 93 s, `root ok`, the sync went on |

## Commands

Build (`rs/target/release/epochdb-rs`, `epochdb-exec`):

```
cd rs && cargo build --release -p epochdb-plugin -p epochdb-exec
```

The host (one process, the fetch package as the block source; `S` is the scratch dir, `$S/rs/e2e/data/` held `chain.json` and `upgrade.json` copied from `$S/rs/beam/data/` so `chain.Resolve` did not need the P-chain):

```
go run ./cmd/epochdb-host --chain 2tmrrBo1Lgt1mzzvPSFt73kkQKFas5d1AP88tv9cicwoFp8BSn --network mainnet \
  --vm rs/target/release/epochdb-rs --data $S/rs/e2e/data --node https://api.avax.network \
  --http 127.0.0.1:19960 --p2p-port 19961
```

`--p2p-port` is required: the host derives its NodeID from `staker.crt` under `--data`, and the fetch package persists `staker.key/.crt` only when `ListenPort > 0`. Without it the host exits at startup with `staking identity: open .../staker.crt: no such file or directory` (run 0, 04:38 JST). Documented, not changed (cmd/epochdb-host config handling is rs-ops's).

The host's config bytes are hardcoded `{"state-sync-enabled":false}`, so the plugin ran with its defaults: roll budget 2048 MB while bootstrapping, 128 MB at the tip, 14 workers.

Probes (`rs/scripts/e2e/`, the run logs and captures are under `$S/rs/e2e/`):

```
rs/scripts/e2e/tipfollow.sh http://127.0.0.1:19960/ext/bc/<id>/rpc https://build.onbeam.com/rpc 600 30
python3 rs/scripts/e2e/rpccmp.py http://127.0.0.1:19960/ext/bc/<id>/rpc https://build.onbeam.com/rpc 5
python3 rs/scripts/e2e/wsheads.py 127.0.0.1 19960 /ext/bc/<id>/ws 12 http://127.0.0.1:19960/ext/bc/<id>/rpc
rs/scripts/e2e/oracles.sh          # beam 1..1M: in-process bench, then the harness
```

## The runs

Seven launches of the same command on the same `--data`; the host resumes from the plugin's LastAccepted. Bench lines are the host's (`blk` and `tx` per 10 s window, `mgas/s` in the window, `cum` since the first block, `wait` = verify loop starved, `full` = host ring at capacity, host and plugin RSS).

| run | from | to | end | why it ended |
|---|---|---|---|---|
| 1 | 0 | 3,423,561 | 45 min, avg 1,264 blk/s | state root mismatch at 3,423,561 (bug 1) |
| 2 | 3,423,560 | 4,232,319 | 10 min | kill -9 (deliberate, after rebuilding with the roll fix) |
| 3 | 4,237,036 | 4,487,634 | 3 min | kill -9 (deliberate, second roll fix) |
| 4 | 4,493,056 | 8,182,072 | 44 min, avg 1,393 blk/s | `gasUsed 81396 != header 80494` at 8,182,073 (bug 2) |
| 5 | 8,182,072 | 9,532,367 | 16.5 min, avg 1,365 blk/s | SIGTERM from outside (another agent's `pkill epochdb-host` on this box; the host shut down cleanly, `Verify: context canceled`) |
| 6 | 9,532,367 | 9,554,969 | tip reached 07:19:53 JST, followed 13 min | kill -9 at the tip (deliberate) |
| 7 | 9,554,969 | 9,554,972 | followed 8 min, stopped with SIGTERM | the end of the test |

Bench lines, one every 5 minutes (full logs: `$S/rs/e2e/host-run{1..7}.log`):

```
run 1 (genesis)
04:39:43 bench t=0s h=0 blk=0 tx=0 mgas/s=0.0 cum=0.0 wait=0.0s full=0.0s host_rss=160MB vm_rss=11MB
04:44:33 bench t=275s h=352156 blk=14586 tx=21563 mgas/s=52.9 cum=93.8 wait=0.0s full=9.7s host_rss=1894MB vm_rss=1208MB
04:49:33 bench t=575s h=747833 blk=12761 tx=14264 mgas/s=131.8 cum=87.4 wait=0.0s full=9.6s host_rss=1857MB vm_rss=1426MB
04:54:33 bench t=875s h=1104914 blk=14135 tx=16121 mgas/s=97.3 cum=90.9 wait=0.0s full=9.8s host_rss=1896MB vm_rss=1732MB
04:59:33 bench t=1175s h=1507269 blk=13316 tx=15507 mgas/s=146.5 cum=99.3 wait=0.0s full=9.5s host_rss=1934MB vm_rss=2261MB
05:04:33 bench t=1475s h=1887026 blk=13154 tx=15200 mgas/s=161.4 cum=103.1 wait=0.0s full=9.6s host_rss=1903MB vm_rss=2393MB
05:09:33 bench t=1775s h=2259236 blk=13575 tx=15514 mgas/s=105.2 cum=110.3 wait=0.0s full=9.6s host_rss=1940MB vm_rss=2934MB
05:14:33 bench t=2075s h=2621508 blk=13230 tx=16246 mgas/s=140.8 cum=110.3 wait=0.0s full=9.8s host_rss=1930MB vm_rss=3014MB
05:19:33 bench t=2375s h=3010514 blk=12541 tx=18641 mgas/s=186.1 cum=118.5 wait=0.0s full=9.9s host_rss=1956MB vm_rss=3699MB
05:24:33 bench t=2675s h=3383150 blk=12581 tx=18411 mgas/s=218.3 cum=126.7 wait=0.0s full=10.0s host_rss=1961MB vm_rss=4340MB
05:25:06 bench exit t=2708s h=3423561 blk=3423561 tx=4548711 mgas/s=127.7 cum=127.7 wait=0.5s full=2546.8s host_rss=1885MB vm_rss=0MB
run 2 (after the with_chain fix; 28.1M rows replayed in 62.8 s)
05:37:08 bench t=0s h=3423560 blk=0 tx=0 mgas/s=0.0 cum=0.0 wait=0.0s full=0.0s host_rss=183MB vm_rss=6000MB
05:41:58 bench t=262s h=3747348 blk=13763 tx=17505 mgas/s=185.0 cum=188.9 wait=0.0s full=9.8s host_rss=1296MB vm_rss=11665MB
05:46:58 bench t=562s h=4174118 blk=15091 tx=16543 mgas/s=56.8 cum=146.6 wait=0.0s full=9.9s host_rss=1170MB vm_rss=11882MB
run 4 (34.7M then 36.2M rows replayed in 89 and 93 s for runs 3 and 4)
05:54:59 bench t=0s h=4493056 blk=0 tx=0 mgas/s=0.0 cum=0.0 wait=0.0s full=0.0s host_rss=171MB vm_rss=5773MB
05:59:49 bench t=277s h=4890777 blk=14587 tx=16673 mgas/s=72.3 cum=92.6 wait=0.0s full=10.0s host_rss=1152MB vm_rss=6416MB
06:04:49 bench t=577s h=5296571 blk=13474 tx=15977 mgas/s=167.1 cum=99.3 wait=0.0s full=9.6s host_rss=1251MB vm_rss=7126MB
06:09:49 bench t=877s h=5673606 blk=12494 tx=16459 mgas/s=238.2 cum=113.5 wait=0.0s full=9.8s host_rss=1243MB vm_rss=7669MB
06:14:49 bench t=1177s h=6101868 blk=14291 tx=16588 mgas/s=103.9 cum=114.3 wait=0.0s full=9.8s host_rss=1231MB vm_rss=8240MB
06:19:49 bench t=1477s h=6530588 blk=14308 tx=17190 mgas/s=98.5 cum=112.0 wait=0.0s full=9.9s host_rss=1196MB vm_rss=8260MB
06:24:49 bench t=1777s h=6963018 blk=14525 tx=17168 mgas/s=102.8 cum=109.7 wait=0.0s full=9.7s host_rss=1253MB vm_rss=8276MB
06:29:49 bench t=2077s h=7382454 blk=14301 tx=16760 mgas/s=63.4 cum=105.9 wait=0.0s full=9.7s host_rss=1186MB vm_rss=11807MB
06:34:49 bench t=2377s h=7791831 blk=14603 tx=18424 mgas/s=84.9 cum=102.2 wait=0.0s full=9.7s host_rss=1216MB vm_rss=11326MB
06:39:21 bench exit t=2649s h=8182072 blk=3689016 tx=4372358 mgas/s=101.0 cum=101.0 wait=0.0s full=2582.8s host_rss=1203MB vm_rss=11146MB
run 5 (after the P256Verify fix; 57.6M rows replayed in 164 s)
06:57:48 bench t=0s h=8182072 blk=0 tx=0 mgas/s=0.0 cum=0.0 wait=0.0s full=0.0s host_rss=194MB vm_rss=8374MB
07:02:38 bench t=284s h=8574155 blk=14851 tx=17248 mgas/s=79.3 cum=76.1 wait=0.0s full=9.9s host_rss=1193MB vm_rss=8684MB
07:07:38 bench t=584s h=8990134 blk=13924 tx=16309 mgas/s=64.9 cum=74.7 wait=0.0s full=10.0s host_rss=1205MB vm_rss=8833MB
07:12:38 bench t=884s h=9391841 blk=12746 tx=15697 mgas/s=125.9 cum=84.8 wait=0.0s full=9.8s host_rss=1266MB vm_rss=7728MB
07:14:24 bench exit t=989s h=9532367 blk=1350295 tx=1690634 mgas/s=88.1 cum=88.1 wait=2.1s full=892.0s host_rss=818MB vm_rss=7781MB
run 6 (65.0M rows replayed in 177 s; the tip)
07:19:38 bench t=0s h=9533216 blk=849 tx=954 mgas/s=7.5 cum=125.6 wait=0.0s full=0.0s host_rss=213MB vm_rss=9605MB
07:24:28 bench t=290s h=9554958 blk=0 tx=0 mgas/s=0.0 cum=6.1 wait=10.0s full=0.0s host_rss=277MB vm_rss=8906MB
07:29:28 bench t=590s h=9554961 blk=0 tx=0 mgas/s=0.0 cum=3.0 wait=10.0s full=0.0s host_rss=234MB vm_rss=8644MB
07:32:58 bench t=800s h=9554969 blk=1 tx=1 mgas/s=0.0 cum=2.2 wait=10.0s full=0.0s host_rss=234MB vm_rss=8582MB
run 7 (after kill -9 at the tip; 81 rows replayed in 339 ms)
07:36:50 bench t=0s h=9554969 blk=0 tx=0 mgas/s=0.0 cum=0.0 wait=0.0s full=0.0s host_rss=158MB vm_rss=348MB
07:41:00 bench t=233s h=9554972 blk=0 tx=0 mgas/s=0.0 cum=0.0 wait=10.0s full=0.0s host_rss=149MB vm_rss=486MB
07:41:26 bench exit t=260s h=9554972 blk=3 tx=4 mgas/s=0.0 cum=0.0 wait=260.2s full=0.0s host_rss=149MB vm_rss=486MB
```

Throughput: 1,250 to 1,500 blk/s on beam under rpcchainvm (12,500 to 15,300 blocks per 10 s window), the known round-trip ceiling; `full` at 9.6 to 10.0 s of every window says the host ring was always full, so the fetch (4,400 blk/s standalone) was never the limiter. Plugin time split of run 4 (3.69M blocks in 2,649 s): `evm=138s trace=33s commit=26s | parse-batch=45s parse=28s verify=248s accept=86s checker=419s`, exec thread 1,359 mgas/s. Genesis to tip, syncing time only: 45 + 10 + 3 + 44 + 16.5 + 5 min = about 2 h 04 min of syncing plus 8 min of recovery replays.

## Bug 1: state root mismatch at 3,423,561 (fixed)

```
epochdb-rs: block 3423561: state root mismatch: computed 0xfd25e7b8d5f786a87740e31766230c0998a7dd8d4227889a7dc8c85639dcffdc, header 0x1df822919eca832b62f977bae459f4bdc3e6fefa44b316f885efdf3cef991c73
epochdb-host: FATAL: height 3423562: Accept: rpc error: code = Unavailable desc = error reading from server: connection reset by peer
```

The block holds one tx, `0x892e131243d807a21ddfe1d253072535fdef854d3896df83441341d2169f3d92`, a contract creation whose constructor STATICCALLs the warp precompile `0x0200000000000000000000000000000000000005` with selector `0x4213cf78` = `getBlockchainID()` and stores the answer in an immutable (build.onbeam.com callTracer: the inner call returns `0xf94107902c8418dfcdf51d3f95429688abc7109e0f5b0e806c7e204d542e0761`, the chain's blockchainID; prestateTracer diffMode: only the deployed code, three storage slots, sender nonce and balance, coinbase balance change).

Cause: `exec::Config::from_genesis` leaves `blockchain_id` and `subnet_id` at zero; only `Config::with_chain(blockchain_id, subnet_id)` sets them, and only the `epochdb-exec` bin called it. `rs/plugin/src/node_engine.rs` (`NodeEngine::open_inner`) and `rs/node/src/bench.rs` built the Config without it, so under the plugin `getBlockchainID()` answered 32 zero bytes. Gas and receipts are unaffected (the executor's gasUsed / receiptsRoot / logsBloom checks passed), the deployed code differs by 32 bytes, so the account's code hash and the state root differ. The same zero `subnet_id` would have made every warp predicate verification sign over the wrong subnet.

Fix (commit cfa23b4): `node_engine.rs` passes `.with_chain(B256::from(init.chain_id), B256::from(init.subnet_id))` from the Initialize request (the snow context's ids); `bench.rs` reads `blockchainID` / `subnetID` from chain.json through `exec::config::cb58`, as the exec bin does. Run 2 restarted from the store (head 3,423,560, `rows replayed 28111272, ..., root ok`) and executed 3,423,561 with the right root.

Which oracle would have caught it: none of the ones run so far. The beam 1..1M oracles were run through `epochdb-exec` (has `with_chain`), the fork windows too, and Step has no warp precompile. The plugin oracles (Step 50k, beam 50k under the harness) end long before the first `getBlockchainID()` call on beam (3.42M). Only a plugin path oracle over a range with a stored `getBlockchainID()` catches it: this sync, or the harness on a dump that crosses 3,423,561.

## Bug 2: `gasUsed 81396 != header 80494` at 8,182,073 (fixed)

```
epochdb-host: FATAL: height 8182073: Verify: rpc error: code = Unknown desc = block 8182073: gasUsed 81396 != header 80494
```

One tx, `0x1a11dc9caf547be00f6319b048ac05fe0c59a528037e963001c85ce3a9f9a7c6` (gas limit 0x13df4 = 81396, so the plugin ran it out of gas): a WebAuthn signature check, two SHA256 calls and one STATICCALL to `0x0000000000000000000000000000000000000100` (P256Verify, the Granite precompile; build.onbeam.com structLog: the STATICCALL op cost 39452 = 100 warm access + 39352 forwarded, the precompile used 6900).

Cause: `rs/exec/src/exec.rs` `SevmPrecompiles::set_granite` rebuilds the warm precompile set with P256Verify at the Granite flip (6,970,224 on beam), but revm (`revm-handler` `pre_execution.rs`) re-warms the journal's precompile address set only when `PrecompileProvider::set_spec` answers true, and the eth spec did not change at that block. So in a long-lived Executor the journal never learned that 0x100 is a precompile: each call paid 2600 (cold) instead of 100, this tx's tight limit ran out by exactly that, and gasUsed was the limit. A fresh Executor never sees it: its first `set_spec` answers true after `set_granite` already ran. That is why `epochdb-exec` on a one-block dump of 8,182,073 (`$S/rs/e2e/p256/`, fetched with `epochdb-dump-fetch --from 8182073 --to 8182073 --anchor 0x852c9f...`) executes it correctly: `gas=80494`, the P256 call `gasUsed 6900`, output `0x...01`.

Fix (commit cfa23b4): `SevmPrecompiles` keeps a `warm_changed` flag set by `rebuild_warm`, and `set_spec` returns `take(warm_changed) || changed`. Run 5 restarted from the store (head 8,182,072, `rows replayed 57634039, ..., root ok`) and executed 8,182,073 (root-checked, along with the 1,350,294 blocks after it).

Which oracle would have caught it: only a plugin path (long-lived Executor) crossing Granite into a P256Verify call. The Granite window (6,970,224..6,975,223) has no P256 call (`precompile_txs=0`), and the exec bin's fresh Executor hides the bug anyway.

## Memory and the roll budget (finding, partly fixed)

Recovery said `rolled at 0 (gen 0)` at every restart: the state engine did not roll once in 9.55M blocks under the default budget, because `maybe_roll` compared `overlay.bytes()` (logical key + value bytes) with 2048 MB, and beam's rows are small. Under the harness the roll fires because the harness oracles pass `roll-budget-mb: 8`. The tip roll (budget 0 on SetState(NormalOp)) then reported the real sizes:

```
epochdb-rs: roll 1 start: height=9554955 overlay=4166822 keys/564MB dirty=609MB
epochdb-rs: roll 1 done: height=9554955 keys=3622673 nodes=1291438 run=147MB trie=209MB merge=6267ms roll=3691ms total=120509ms replayed=0 overlay=0MB dirty=609MB->0MB
```

So at the tip, 9.55M blocks in, the whole overlay was 564 MB logical (4.17M keys) and Dirty 609 MB, together 1.17 GB, under the 2 GB budget. The plugin's RSS at that moment (`/proc/<pid>/status`, plugin pid, at 9,554,958):

| part | size |
|---|---|
| RssAnon (heap: overlay 564 MB + its index, Dirty 609 MB, the store's open L0 window, executor, code table, gRPC buffers) | 2,991 MB |
| RssFile (mmapped store runs under `chainData/store/cas`, 6.0 GB on disk, page cache, reclaimable) | 6,048 MB |
| store window on disk (`chainData/store/window`, the unsealed L0 rows) | 29 MB |
| vmstate after the tip roll (`run.1` 147 MB + `trie.1` 209 MB) | 339 MB |
| host process | 234 to 280 MB at the tip, 1.1 to 1.9 GB while syncing (the 100k-item ring plus the fetch window) |

After the tip roll: RssAnon 2,991 MB, unchanged, so the roll freed nothing the allocator gave back; after the kill -9 and restart at the tip the plugin ran at 348 to 486 MB RSS (215 MB anon). The `vm_rss` of 11.1 to 11.9 GB during runs 2 and 4 was RssAnon 8.6 to 8.8 GB (measured at 4.23M and 8.06M) plus 2.7 to 3.3 GB of mmapped runs; run 5, restarted at 8.18M, ran the same stretch at RssAnon 3.6 to 3.9 GB. The anon heap of a long run therefore holds 5 GB more than overlay + Dirty account for, growing in steps (5.26 GB flat from 6.2M to 7.0M, then 8.67 GB at 8.06M) that line up with the store's L0 seals and the terminal merges (`merged 73 L0 runs ... in 235.6s`: their sort buffers and postings builders are the suspect, freed to the allocator but not to the OS). It never threatened the box (25 GB, 11 to 15 GB available throughout), but it is the open item: a heap profile of a long plugin run around a terminal merge, and glibc malloc trim or a different allocator.

Change made (commit cfa23b4, `rs/node/src/engine.rs`): `maybe_roll` budgets `overlay.bytes() + Dirty::bytes()` (try_lock; a miss is checked again next block). On beam it still did not fire before the tip (1.17 GB under 2 GB); on a chain with a bigger state it rolls earlier than before. Not changed: the budget itself (2 GB is Go's `SyncRoll`), and nothing in the host (its config bytes are rs-ops's).

## Tip follow

`tipfollow.sh`, 30 s samples, local `/rpc` vs `build.onbeam.com`, after `caught up at height=9554955` (07:19:53 JST):

```
07:22:17 JST local=9554958 remote=9554958 remote-local=0
07:22:49 JST local=9554958 remote=9554958 remote-local=0
07:23:19 JST local=9554958 remote=9554958 remote-local=0
07:23:50 JST local=9554958 remote=9554958 remote-local=0
07:24:21 JST local=9554958 remote=9554958 remote-local=0
07:24:51 JST local=9554958 remote=9554958 remote-local=0
07:25:22 JST local=9554958 remote=9554958 remote-local=0
07:25:52 JST local=9554958 remote=9554958 remote-local=0
07:26:23 JST local=9554958 remote=9554958 remote-local=0
07:26:53 JST local=9554958 remote=9554958 remote-local=0
07:27:24 JST local=9554960 remote=9554960 remote-local=0
07:27:54 JST local=9554961 remote=9554961 remote-local=0
07:28:26 JST local=9554961 remote=9554961 remote-local=0
07:28:57 JST local=9554961 remote=9554961 remote-local=0
07:29:28 JST local=9554961 remote=9554961 remote-local=0
07:29:58 JST local=9554963 remote=9554964 remote-local=1
07:30:29 JST local=9554964 remote=9554964 remote-local=0
07:30:59 JST local=9554964 remote=9554964 remote-local=0
07:31:30 JST local=9554964 remote=9554964 remote-local=0
07:32:01 JST local=9554964 remote=9554964 remote-local=0
```

Beam builds a block only when it has transactions, so the tip moves in bursts (minutes without a block, then 2 to 3 blocks a few seconds apart). The one sample with `remote-local=1` (07:29:58) is block 9,554,964, whose newHead arrived locally at 07:29:58 with `age=2.6s` (below): the local and the remote probes in that sample were about a second apart on both sides of an arriving block. After the kill -9 and restart (run 7), 10 more samples over 5 minutes, all `remote-local=0` (`$S/rs/e2e/tipfollow2.log`).

## RPC comparisons

`rpccmp.py`, local `/rpc` against build.onbeam.com at remote head 9,554,958 (local head 9,554,958), heights 9,554,918..9,554,922 (40 below the head so both sides surely had them), every result compared as parsed JSON, 53 of 53 equal:

| probe | method | equal |
|---|---|---|
| chainId | `eth_chainId` | yes |
| block 9554918..9554922, full transactions | `eth_getBlockByNumber(h, true)` x5 | 5/5 |
| the same by hash | `eth_getBlockByHash(hash, false)` x5 | 5/5 |
| tx counts | `eth_getBlockTransactionCountByNumber` x5 | 5/5 |
| block receipts | `eth_getBlockReceipts` x5 | 5/5 |
| receipts of 5 txs (0x47ca4198..., 0x73d3847a..., 0xd2ddcc84..., 0xe60bb497..., 0xbc343b89...) | `eth_getTransactionReceipt` x5 | 5/5 |
| the same txs | `eth_getTransactionByHash` x5 | 5/5 |
| callTracer of 4 txs (re-executed locally) | `debug_traceTransaction(hash, {tracer: callTracer})` x4 | 4/4 |
| balances and nonces of 4 senders at 9554922 | `eth_getBalance`, `eth_getTransactionCount` x8 | 8/8 |
| code and slot 0 of 3 contracts at 9554922 | `eth_getCode`, `eth_getStorageAt` x6 | 6/6 |
| logs over 9554918..9554922 | `eth_getLogs` | yes |
| block 1,000,000 header | `eth_getBlockByNumber(0xf4240, false)` | yes |
| balances of 2 senders at height 1,000,000 | `eth_getBalance(addr, 0xf4240)` x2 | 2/2 |

Historical state (`eth_getBalance` at 1,000,000, 8.5M blocks below the head) is served from the store's runs. The full transcript is `$S/rs/e2e/rpccmp.log`.

## `/ws`

`wsheads.py` opened `ws://127.0.0.1:19960/ext/bc/<id>/ws` through the host's mount (the rpcchainvm hijack path), `eth_subscribe newHeads`, and for every notification asked `/rpc` for the block by number. `age` is the arrival time minus the block's timestamp (the validators' clock):

```
subscribed {'jsonrpc': '2.0', 'id': 1, 'result': '0x8bb2b6ab035b730ad6ce2f6e52d00660'}
07:27:12 JST newHead number=9554959 hash=0x5d7f434d... ts=1788906430 age=2.5s rpc_hash_match=True
07:27:20 JST newHead number=9554960 hash=0x4557fb0f... ts=1788906439 age=1.7s rpc_hash_match=True
07:27:29 JST newHead number=9554961 hash=0x2d6e88e2... ts=1788906447 age=2.9s rpc_hash_match=True
07:29:31 JST newHead number=9554962 hash=0x431f87c1... ts=1788906570 age=1.8s rpc_hash_match=True
07:29:41 JST newHead number=9554963 hash=0xa9a9cc84... ts=1788906578 age=3.0s rpc_hash_match=True
07:29:58 JST newHead number=9554964 hash=0x455a7bc2... ts=1788906596 age=2.6s rpc_hash_match=True
07:32:15 JST newHead number=9554965 hash=0x7ea8b845... ts=1788906733 age=2.8s rpc_hash_match=True
07:32:16 JST newHead number=9554966 hash=0xa50c4f8d... ts=1788906735 age=1.8s rpc_hash_match=True
07:32:20 JST newHead number=9554967 hash=0xa9fb3523... ts=1788906739 age=1.9s rpc_hash_match=True
07:32:34 JST newHead number=9554968 hash=0x3c0b7701... ts=1788906752 age=2.4s rpc_hash_match=True
07:32:51 JST newHead number=9554969 hash=0x822d81ae... ts=1788906769 age=2.8s rpc_hash_match=True
```

The capture asked for 12 heads; 11 had arrived when the kill -9 came and the client got `EOFError` (the connection died with the plugin). After the restart, a new subscription:

```
subscribed {'jsonrpc': '2.0', 'id': 1, 'result': '0xd225a124e7e4ab9eb49fab8cee0be10'}
07:37:06 JST newHead number=9554970 hash=0x02e6c783... ts=1788907024 age=2.4s rpc_hash_match=True
07:37:07 JST newHead number=9554971 hash=0x4d7a4193... ts=1788907026 age=1.2s rpc_hash_match=True
07:37:16 JST newHead number=9554972 hash=0x727f5006... ts=1788907034 age=2.6s rpc_hash_match=True
unsubscribe {"jsonrpc":"2.0","id":2,"result":true}
```

The 1.2 to 3.0 s is the fetch package's consensus poll to accept the block (the validators' frontier) plus Verify and Accept; the plugin's own newHeads latency from Accept is sub-millisecond (rs/rpc REPORT).

## Crash and restart

Mid-sync, twice (both `kill -9` of the plugin and the host at once):

```
05:47:41 JST kill -9 at h=4232319 (bench), plugin RSS 12.2 GB
epochdb-rs: recovered: rolled at 0 (gen 0), head 4237036 0x5badbf8e..., rows replayed 34672905, 2841 code blobs, 16 runs, root ok, in 89207 ms
epochdb-host: plugin last accepted height=4237036 id=hNowzuYe..., fetching from 4237037
05:53:05 JST kill -9 at h=4487634
epochdb-rs: recovered: rolled at 0 (gen 0), head 4493056 0xffdfba72..., rows replayed 36155039, 2872 code blobs, 21 runs, root ok, in 93197 ms
```

The store's head was 4,700 to 5,400 blocks past the last bench line (3 to 4 s of blocks in flight), every one of them recovered: the store's fsync cadence (every 256 blocks on the flusher thread, torn tail dropped) lost nothing that a bench line had reported. Replays of the two process exits (bug 1 at 3.42M, 28.1M rows in 62.8 s; bug 2 at 8.18M, 57.6M rows in 164 s) and of the SIGTERM at 9.53M (65.0M rows in 177 s) went the same way. The replay is from the manifest height, which was 0 the whole sync (no roll, above), so it read the whole store every time: 370k rows/s.

At the tip:

```
07:32:59 JST kill -9 378831 (plugin) 378721 (host) at h=9554969, plugin RSS 8.8 GB (anon 3.0 GB)
epochdb-rs: recovered: rolled at 9554955 (gen 1), head 9554969 0x822d81ae..., rows replayed 81, 3180 code blobs, 51 runs, root ok, in 339 ms
epochdb-rs: initialized, last accepted height=9554969
epochdb-host: plugin last accepted height=9554969 id=zLDYB8VY..., fetching from 9554970   (07:33:10 JST, 11 s after the kill, go run's compile included)
epochdb-rs: state STATE_NORMAL_OP
epochdb-host: caught up at height=9554970, plugin in NormalOp                                (07:37:06 JST, the next block beam produced)
epochdb-rs: roll 2 done: height=9554970 keys=3622682 nodes=1291440 run=147MB trie=209MB merge=527ms roll=4724ms total=10182ms replayed=5 overlay=0MB dirty=0MB->0MB
```

then the three newHeads and the 5-minute tip follow above. The manifest written by the tip roll is what made this recovery 81 rows instead of 65M.

## Oracles re-run on the fixed code

All on the binaries of commit cfa23b4, CPU shared with the tip-following host.

1. beam 1..1M, in-process bench (`epochdb-rs --dump $S/rs/beam/beam-containers-1-1000000.bin --genesis chain.json --upgrade upgrade.json --data D --to 1000000 --workers 12`): `blocks=1000000 root-checked=1000000 rolls=0`, 67 s, cum 1,060 mgas/s, rss 545 MB (`$S/rs/e2e/oracle-bench.log`).
2. beam 1..1M under the harness (`go run ./cmd/epochdb-host-bench --dump ... --vm rs/target/release/epochdb-rs --data D --http 127.0.0.1:19970 --batch 256 --config '{"state-sync-enabled":false}' --to 1000000`): `bench exit t=668s h=1000000 blk=1000000 ... blk/s=1496.7`, plugin `root-checked=1000000 rolls=1` (the tip roll: `overlay=1115938 keys/149MB dirty=284MB`), `check eth_blockNumber=1000000 want=1000000 hash=0xd53a08bd... keccak(header)=0xd53a08bd... match=true`, `check genesis ... match=true` (`$S/rs/e2e/oracle-harness.log`).
3. The fork windows with `epochdb-exec` (state from build.onbeam.com through the saved rpc caches, `--checkpoint 1`), the same ranges as before, all `exit 0`, gas / receipts / bloom of every block equal to the header: durango 1,901,030..1,901,229 (200 blocks), warpon2 4,029,216..4,029,265 (50, the warp activation), warpblk 4,029,316..4,029,320 (`predicates_verified=2`), invdelblk 5,598,098 (`predicates_verified=1`), graniteblk 6,970,756 (`$S/rs/e2e/oracle-exec-*.log`). These start a fresh Executor per window so they exercise neither bug; listed for regression.
4. `cargo test --release -p epochdb-exec -p epochdb-node -p epochdb-plugin`: 16 passed.
5. The sync itself: 9,554,972 blocks root-checked through the plugin, the two blocks that failed now pass.

## Deviations from the plan

- Not seeded from a dump: the host has no dump source and the fetch was never the bottleneck (4,400 blk/s standalone vs 1,300 to 1,500 through rpcchainvm), so the whole chain came from the validators. `epochdb-host-bench` on the 1M dump would have saved nothing.
- Seven launches instead of one: two for the bugs, two deliberate kill -9 mid-sync (one of them also carried the roll change), one external SIGTERM, one deliberate kill -9 at the tip. Every restart resumed from the store on the same `--data`.
- Run 5's end: an external `SIGTERM` at 07:14:24 JST (another agent's process cleanup on this shared box; a second `./epochdb-host` was running here at the time). The host shut down cleanly (`stopped at height=9532367`), the plugin exited with its exit line, and run 6 replayed 65.0M rows in 177 s and reached the tip 5 minutes later.
- `pgrep -f epochdb-host` matches the shell running it; the first kill -9 attempt killed my own shell. Use `pgrep -x`.

## Open items

- Anon heap of a long plugin run: 8.6 to 8.8 GB at 4.2M and 8.1M blocks against 1.2 GB of overlay + Dirty; suspect the store's L0 seals and terminal merges (`merged 73 L0 runs ... 235.6s`) leaving freed memory with the allocator. Profile, then malloc trim or jemalloc/mimalloc. Also the tip roll's `total=120509ms` is wall time to the next Accept (the roll waits for a block to swap in), not work: `merge=6267ms roll=3691ms`.
- Roll budget: 2048 MB of overlay + Dirty never triggers on beam; a wall-clock or block-count trigger (Go rolls by budget too, but its rows are larger) would put a manifest down every hour and cut a crash replay from 3 minutes to seconds. The tip roll already does this once.
- `cmd/epochdb-host` needs `--p2p-port` for its staking identity (above); either persist the identity without a listener or say so in the flag's help.
- No P256Verify or `getBlockchainID()` fixture in the unit tests: add a two-block `exec` test that flips Granite between blocks and calls 0x100 (warm cost 100), and a `node_engine` test asserting `cfg.blockchain_id == init.chain_id`.
- `eth_blockNumber` and the RPC probes ran against 5 recent blocks and one old height; the wide differential (`rpccmp2.py --scan`) against stock remains the harness oracle in the README.
- Everything else from `rs/README.md` open items stands (real avalanchego, `EPOCHDB_*` through config bytes, metrics).

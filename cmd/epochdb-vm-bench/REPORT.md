# epochdb-vm-bench: epochdb-vm fed from a container dump file

Branch `go-bench`, 2026-09-08 (JST). The Go side of the local A/B against the Rust node: the same executor, store writes, checker goroutine, roll budgets and bench line as `cmd/epochdb-vm`, with the p2p fetcher replaced by a mmapped dump file. Nothing in the existing Go tree was edited (copy-on-write): the command is `cmd/epochdb-vm/main.go` copied and trimmed, plus `dump.go`.

## What changed vs epochdb-vm

`cmd/epochdb-vm-bench/main.go` (diff against `cmd/epochdb-vm/main.go`):

- Flags dropped: `--p2p-port`, `--peers`, `--node`, `--vdr-sources`, `--per-peer` (fetch and p2p only).
- Flags added: `--dump <file>` (required), `--from` (default 1, the first height the dump serves), `--to` (default 0 = the dump's last height; `--stop` and `--stop-at` are aliases). `--to` is the executor's `StopAt`, so the run ends there.
- Flags kept as they are: `--chain`, `--network`, `--data`, `--port`, `--pprof`, `--roll-budget` / `--roll-budget-sync`, `--roll-budget-tip`, `--tip-lag`, `--gogc` / `--gogc-sync`, `--gogc-tip`.
- `chain.Resolve` is called with no P-chain sources and the command refuses to start unless `<data>/chain.json` exists. `chain.Resolve` reads `chain.json` (the cached genesisData, subnetID, networkID, vmKind) and `upgrade.json` from the data dir and touches no network when the cache is present (`chain/chain.go`, `resolveL1` -> `fromCache`). Copy both files from the dump dir.
- `dist.Local(dataDir)` instead of `dist.Open(dataDir)`: the artifact store never talks to S3, whatever `EPOCHDB_S3_*` says. `store.Join` on a local store and a fresh dir is a no-op (`freshOrBroken` returns nil when `!cas.Remote()`), `store.Open` is unchanged, and the sealed runs land under `<data>/runs` and `<data>/cas` locally. No stub was needed: nothing insisted on an RPC node or S3.
- `vmexec.New` gets the dump source as `Blocks`, `StopAt: src.Last()` and `Budget.Accepted: src.Last` (the dump's last height plays the fetcher's accepted head, so the tip gate and both memory profiles behave as in epochdb-vm: catch-up until `tip-lag` blocks before the end, tip profile after). Everything else in the `vmexec.Config` is identical.
- The executor's return now ends the process (epochdb-vm keeps serving RPC after `--stop`); the exit line is tagged `bench exit` (epochdb-vm tags it `exit`).
- The bench struct lost the fetcher: `full=` is printed as `0` (a file is never full). The rest of the line is verbatim: `bench t= h= blk= tx= mgas/s= cum= wait= full= rss= overlay= dirty= rolls= rolling=` every 10 s. `wait=` is still the executor's time waiting on the source.
- `/status` reports `accepted` and `fetched` as the dump's last height and `queueBytes: 0`. The RPC server, `EnableLive`, pprof and the data-dir flock are unchanged.

`cmd/epochdb-vm-bench/dump.go`, the `vmexec.BlockSource`:

- Format `[u64 LE height][u32 LE len][container bytes]`, heights ascending and contiguous from 1. The file is `syscall.Mmap`ped read-only and scanned once on open into `off []int64` (record start per height, 8 MB for 1M records); a gap, a wrong height or a truncated record refuses the file.
- `GetByHeight(n)`: `ok=false` past `--to`, an error below `--from`, otherwise a copy of the container bytes (the executor holds the container until the checker wrote the block, so the copy mirrors the fetcher's heap bytes and lets the mapping be released).
- Consumed pages are given back with `MADV_DONTNEED` every 64 MB (during the index scan and while serving), so the mapping does not inflate `rss=`: a plain mmap of the 3.18 GB file would have counted every touched page in the bench's RSS. The page cache keeps the file.
- `dump_test.go` covers the index, `--from`/`--to` bounds and the gap refusal (`go test ./cmd/epochdb-vm-bench/`).

Build check: `go vet ./cmd/epochdb-vm-bench/`, `go test ./cmd/epochdb-vm-bench/`, `gofmt -l` clean.

## Inputs on this machine

```
SCRATCH=/tmp/claude-1000/-home-ilia-epochdb/222c563c-789d-46b6-8726-b8af4b2a6f62/scratchpad
$SCRATCH/rs/step/step-containers-1-1000000.bin   3,176,734,443 bytes, heights 1..1,000,000
$SCRATCH/rs/step/step-containers-1-50000.bin     157,527,739 bytes, heights 1..50,000
$SCRATCH/rs/step/chain.json, upgrade.json
```

Step: blockchain `2jRZvKtXY5nyWTqRwFh1KMHGrCRxJoULu4r2CsayWRnjdDGbV1`, network mainnet, chainId 1234. Machine: i7-10700K 8C/16T, 25 GB RAM, WSL2 (Linux 6.6), Go 1.26.4. No cgroup memory ceiling: `vmexec.setMemLimit` is a no-op and the store's flat cache takes its 1 GB default.

## Commands (repeat these for the A/B)

```
SCRATCH=/tmp/claude-1000/-home-ilia-epochdb/222c563c-789d-46b6-8726-b8af4b2a6f62/scratchpad
cd ~/.herdr/worktrees/epochdb/go-bench
go build -o $SCRATCH/go-bench/epochdb-vm-bench ./cmd/epochdb-vm-bench   # a binary in the scratch dir only

# 50k correctness run (fresh data dir)
mkdir -p $SCRATCH/go-bench/data-50k && cp $SCRATCH/rs/step/chain.json $SCRATCH/rs/step/upgrade.json $SCRATCH/go-bench/data-50k/
cd $SCRATCH/go-bench && /usr/bin/time -v ./epochdb-vm-bench \
  --chain 2jRZvKtXY5nyWTqRwFh1KMHGrCRxJoULu4r2CsayWRnjdDGbV1 --network mainnet \
  --data $SCRATCH/go-bench/data-50k --port 19650 \
  --dump $SCRATCH/rs/step/step-containers-1-50000.bin > $SCRATCH/go-bench/run-50k.log 2>&1

# 180 s run on the 1M file (fresh data dir; SIGINT at 182 s so the t=180 line prints, then "bench exit")
mkdir -p $SCRATCH/go-bench/data-1m && cp $SCRATCH/rs/step/chain.json $SCRATCH/rs/step/upgrade.json $SCRATCH/go-bench/data-1m/
cd $SCRATCH/go-bench && GOMEMLIMIT=10GiB /usr/bin/time -v timeout -s INT 182 ./epochdb-vm-bench \
  --chain 2jRZvKtXY5nyWTqRwFh1KMHGrCRxJoULu4r2CsayWRnjdDGbV1 --network mainnet \
  --data $SCRATCH/go-bench/data-1m --port 19650 \
  --dump $SCRATCH/rs/step/step-containers-1-1000000.bin > $SCRATCH/go-bench/run-1m-180s.log 2>&1
```

Flags left at their defaults: `--roll-budget 2048MB` (catch-up), `--roll-budget-tip 128MB`, `--tip-lag 5000`, `--gogc 400` (catch-up, set by `applyProfile` because `GOGC` is unset in the environment), `--gogc-tip 100`. `GOMEMLIMIT=10GiB` was set for the 1M run only, as the backstop the container ceiling would provide on the box (this machine has none, and GOGC=400 with no limit could take the heap past the 12 GB budget). Nothing else in the environment: no `EPOCHDB_S3_*`, no `EPOCHDB_FLAT_CACHE`.

## 50k correctness run

All 50,000 blocks executed and root-checked (vmexec's checker compares every non-empty block's computed root with the header root and `log.Fatalf`s on a mismatch; the run finished with exit 0 and sealed one run). Wall clock 22.4 s including the index build and the seal; the executor ran 19 s.

```
vmexec: genesis state ok: root=51736d52ef12525c8a48a4d2215b34a7573e871efb62008ac8b45c25590f0d21 accounts=1 keys=1 nodes=0 run=4212B trie=128B in 8ms
epochdb-vm-bench: 2jRZvKtXY5nyWTqRwFh1KMHGrCRxJoULu4r2CsayWRnjdDGbV1 on :19650 chainId=1234 dump=.../step-containers-1-50000.bin heights=1..50000 roll-budget=2048MB/128MB tip-lag=5000
vmexec: budget catch-up (start): gogc=400 roll-budget=2048MB rss=109MB
bench t=10 h=13206 blk=13206 tx=184435 mgas/s=1275.33 cum=1417.05 wait=0.0 full=0 rss=853 overlay=5 dirty=8 rolls=0 rolling=false
vmexec: height=13207 blk/s=1320 tx/s=18436 mgas/s=1276.51 chain=0.0MB state=0.0MB lookup=0.0MB runs=0 overlay=6MB dirty=9MB rolls=0
vmexec: split read=0.04s evm=9.29s | checker hash=1.21s write=1.44s of 10.0s
vmexec: budget tip (lag=3702): gogc=100 roll-budget=128MB rss=1696MB
vmexec: roll 1 start: height=46298 overlay=159895 keys/26MB dirty=42MB runs=1
vmexec: roll 1 done: height=46298 keys=159880 nodes=61730 run=11MB trie=10MB merge=203ms roll=399ms total=611ms replayed=6946 overlay=1MB dirty=43MB->0MB
vmexec: reached --stop height 50000
store: cut window [tx 0..393731, blocks 1..50000], sealing on a goroutine
epochdb-vm-bench: executor finished
epochdb-vm-bench: executor done, flushing
bench exit t=19 h=50000 blk=50000 tx=343731 mgas/s=976.39 cum=1193.17 wait=0.3 full=0 rss=698 overlay=2 dirty=6 rolls=1 rolling=false
store: sealed run f4b395804fbc12d84786c3fab54e437064d01fae37edb6e4bb264c6c53b8d0ed [blocks 1..50000]
/usr/bin/time: Elapsed 0:22.38, User 50.47 s, System 4.98 s, 247% CPU, Maximum resident set size 1,736,156 kB
```

(This run was made before the exit path cancelled the context, so the log also has one stray `bench t=20` line after `bench exit`; the committed binary stops the ticker first.)

## 180 s run on the 1M file

Fresh data dir, `GOMEMLIMIT=10GiB`, `GOGC` unset (the binary's own catch-up profile set GOGC=400 at start and stayed there: lag to the dump's end never fell inside `--tip-lag`). SIGINT at 182 s; the executor stopped with `context canceled`, no root mismatch, exit 0. Every bench line of the run:

```
epochdb-vm-bench: 2jRZvKtXY5nyWTqRwFh1KMHGrCRxJoULu4r2CsayWRnjdDGbV1 on :19650 chainId=1234 dump=.../step-containers-1-1000000.bin heights=1..1000000 roll-budget=2048MB/128MB tip-lag=5000
vmexec: budget catch-up (start): gogc=400 roll-budget=2048MB rss=130MB
bench t=10 h=15266 blk=15266 tx=197811 mgas/s=1486.42 cum=1651.59 wait=0.1 full=0 rss=907 overlay=6 dirty=10 rolls=0 rolling=false
bench t=20 h=61952 blk=61952 tx=394120 mgas/s=934.19 cum=1274.01 wait=0.5 full=0 rss=2274 overlay=33 dirty=53 rolls=0 rolling=false
bench t=30 h=108311 blk=108311 tx=576144 mgas/s=889.89 cum=1141.56 wait=0.9 full=0 rss=2584 overlay=58 dirty=92 rolls=0 rolling=false
bench t=40 h=153846 blk=153846 tx=757996 mgas/s=883.14 cum=1075.30 wait=1.2 full=0 rss=3278 overlay=83 dirty=132 rolls=0 rolling=false
bench t=50 h=197858 blk=197858 tx=931033 mgas/s=856.85 cum=1030.72 wait=1.6 full=0 rss=3701 overlay=106 dirty=171 rolls=0 rolling=false
bench t=60 h=240000 blk=240000 tx=1099437 mgas/s=823.52 cum=995.60 wait=2.0 full=0 rss=4334 overlay=129 dirty=210 rolls=0 rolling=false
bench t=70 h=279672 blk=279672 tx=1254092 mgas/s=770.27 cum=962.94 wait=2.4 full=0 rss=4851 overlay=150 dirty=246 rolls=0 rolling=false
bench t=80 h=332758 blk=332758 tx=1465183 mgas/s=766.76 cum=938.11 wait=2.6 full=0 rss=5818 overlay=166 dirty=271 rolls=0 rolling=false
bench t=90 h=371279 blk=371279 tx=1616617 mgas/s=742.88 cum=916.17 wait=3.0 full=0 rss=6695 overlay=187 dirty=305 rolls=0 rolling=false
bench t=100 h=407552 blk=407552 tx=1759652 mgas/s=700.10 cum=894.35 wait=3.3 full=0 rss=7188 overlay=207 dirty=336 rolls=0 rolling=false
bench t=110 h=448131 blk=448131 tx=1922190 mgas/s=786.69 cum=884.47 wait=3.6 full=0 rss=6965 overlay=230 dirty=372 rolls=0 rolling=false
bench t=120 h=488609 blk=488609 tx=2081291 mgas/s=784.14 cum=876.04 wait=4.0 full=0 rss=7892 overlay=252 dirty=409 rolls=0 rolling=false
bench t=130 h=530028 blk=530028 tx=2247551 mgas/s=804.19 cum=870.47 wait=4.4 full=0 rss=8577 overlay=275 dirty=448 rolls=0 rolling=false
bench t=140 h=570112 blk=570112 tx=2405837 mgas/s=773.95 cum=863.52 wait=4.7 full=0 rss=8957 overlay=297 dirty=486 rolls=0 rolling=false
bench t=150 h=577792 blk=577792 tx=2436380 mgas/s=916.83 cum=867.10 wait=4.8 full=0 rss=9215 overlay=333 dirty=541 rolls=0 rolling=false
bench t=160 h=578304 blk=578304 tx=2439196 mgas/s=724.49 cum=858.13 wait=4.8 full=0 rss=9846 overlay=357 dirty=575 rolls=0 rolling=false
bench t=170 h=609203 blk=609203 tx=2561347 mgas/s=597.23 cum=842.70 wait=5.1 full=0 rss=10088 overlay=374 dirty=603 rolls=0 rolling=false
bench t=180 h=651770 blk=651770 tx=2729122 mgas/s=818.46 cum=841.34 wait=5.4 full=0 rss=9945 overlay=397 dirty=643 rolls=0 rolling=false
bench exit t=181 h=655904 blk=655904 tx=2746004 mgas/s=829.93 cum=841.28 wait=5.5 full=0 rss=9813 overlay=400 dirty=646 rolls=0 rolling=false
/usr/bin/time: Elapsed 3:03.22 (index build + 181 s run + flush), User 494.67 s, System 56.93 s, 301% CPU, Maximum resident set size 10,491,028 kB
```

Numbers from the first executed block (the `t=180` line; `first` is the bench tick that first saw a block, within 1 s of start):

- Blocks: 651,770 in 180 s = **3,621 blk/s**; at `bench exit` (t=181) 655,904 blocks, 3,624 blk/s.
- Txs: 2,729,122 in 180 s = **15,162 tx/s**.
- Gas: **cum 841.34 mgas/s** (t=180), 841.28 at exit; about 151 Ggas executed. The 10 s windows ran 1486 (first 10 s, the light early blocks) down to 597 mgas/s, mostly 740 to 890.
- `wait=` 5.4 s cumulative at t=180 (3% of the run): the executor's wait on the prefetch channel, which here is sender recovery on the pool, not the file (`split read=` stayed at 0.01 to 0.4 s per 10 s).
- Peak RSS: **10,491 MB** (`/usr/bin/time`), 10,088 MB on the bench line at t=170. This is GOGC=400 headroom growing into the 10 GiB soft limit, not live data: overlay was 400 MB and dirty 646 MB at exit, the flat cache is 1 GB. From t=160 the limit was in force and the GC spent more (the 597 mgas/s window at t=170 also overlaps a run seal). The box run has the same shape under its 7/10 cgroup ceiling.
- Rolls: **0**. The 2 GB catch-up roll budget was never reached in 180 s (overlay 400 MB at exit), so the run never exercised a roll or a swap; the 50k run did (1 roll at the tip, 611 ms).
- Per-window blk/s and tx/s derived from consecutive bench lines:

```
t= 10  blk/s= 1527  tx/s= 19781  mgas/s=1486.42
t= 20  blk/s= 4669  tx/s= 19631  mgas/s= 934.19
t= 30  blk/s= 4636  tx/s= 18202  mgas/s= 889.89
t= 40  blk/s= 4554  tx/s= 18185  mgas/s= 883.14
t= 50  blk/s= 4401  tx/s= 17304  mgas/s= 856.85
t= 60  blk/s= 4214  tx/s= 16840  mgas/s= 823.52
t= 70  blk/s= 3967  tx/s= 15466  mgas/s= 770.27
t= 80  blk/s= 5309  tx/s= 21109  mgas/s= 766.76
t= 90  blk/s= 3852  tx/s= 15143  mgas/s= 742.88
t=100  blk/s= 3627  tx/s= 14304  mgas/s= 700.10
t=110  blk/s= 4058  tx/s= 16254  mgas/s= 786.69
t=120  blk/s= 4048  tx/s= 15910  mgas/s= 784.14
t=130  blk/s= 4142  tx/s= 16626  mgas/s= 804.19
t=140  blk/s= 4008  tx/s= 15829  mgas/s= 773.95
t=150  blk/s=  768  tx/s=  3054  mgas/s= 916.83   (heights 570k..578k are gas-heavy blocks: few blocks, full gas)
t=160  blk/s=   51  tx/s=   282  mgas/s= 724.49   (same stretch, 512 blocks in 10 s)
t=170  blk/s= 3090  tx/s= 12215  mgas/s= 597.23   (memory limit reached, seal of 550001..600000)
t=180  blk/s= 4257  tx/s= 16778  mgas/s= 818.46
```

Executor split at the end of the run (per 10 s): `read=0.38s evm=7.66s | checker hash=7.07s write=2.09s`. The checker's `Dirty.Root` share grows with the dirty slab (10 -> 646 MB, no roll to reset it), which is the slope in the mgas/s column.

The data dir after the run holds 1.3 GB (13 sealed runs of 50k blocks plus the open window), all local under `<data>/runs` and `<data>/cas`.

## Repeat protocol for the A/B

Same machine, sequential windows, fresh data dir each time (`rm -rf $SCRATCH/go-bench/data-1m && mkdir -p ... && cp chain.json upgrade.json`), the exact command above, `GOMEMLIMIT=10GiB`, averages over the 180 s (`cum=` on the `t=180` line, blk and tx on that line divided by 180), peak RSS from `/usr/bin/time -v`. Logs of these runs: `$SCRATCH/go-bench/run-50k.log`, `$SCRATCH/go-bench/run-1m-180s.log`.

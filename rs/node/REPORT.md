# rs/node: epochdb-rs, the benchmark-grade Rust node, and the local A/B against epochdb-vm

Branch `rs-node` (rs-state + rs-block + rs-exec merged, one workspace `rs/Cargo.toml`), 2026-09-08 22:15 JST. Binary `epochdb-rs` (`rs/node`). No Go file was edited. Machine: i7-10700K 8C/16T, 25 GB RAM, WSL2; nothing ran on the Tokyo box.

## Architecture (10 lines)

1. `block::Blocks::open` + `recovered(blocks, --workers)`: the dump is mmapped, blocks decoded and senders recovered on a rayon pool 16 chunks of 256 blocks ahead of the executor; consumed pages are released with `MADV_DONTNEED` 64 MB behind the cursor so the mapping does not count in `rss=` (the Go bench copies containers out and releases the same way).
2. `exec::Executor<D: StateDb>` (rs/exec made generic over the state, `StateDb = revm Database + DatabaseCommit + set_block_hash + forget`) runs one block at a time on the main thread: revm with the subnet-evm handler, receipts, callTracer JSON, per-tx journal commit.
3. `engine::Backend` is the `StateDb`: reads go fresh overlay -> frozen overlay -> rolled run (`state::overlay::Overlay`, `state::run::Run`), keys = keccak(addr)+0x00 -> RLP[nonce, balance, codeHash] and keccak(addr)+0x01+keccak(slot) -> trimmed word; keccak(addr) and keccak(slot) are memoised in two maps of at most 65,536 entries (flatdb.go's hashCache). Code is an in-memory table by code hash (genesis alloc + every deployment; persisted only into the history file). `basic` of a missing key is `None`.
4. A tx's commit writes its post-image rows into the fresh overlay and appends them to the block's ordered write set; a self-destructed or EIP-158-empty account becomes an account delete plus a tombstone for every live slot under it, found by a view scan gated by the owners sets and a run seek, exactly `engine.tombstoneSlots`.
5. The checker thread (queue depth 4, one block behind): `Dirty::apply` of the write set, `Dirty::root`, compared with `header.stateRoot`; a mismatch prints the height and both roots and exits 1. `Dirty`'s seek reads the rolled run only. Empty write sets compare the current root.
6. `maybe_roll`: when the fresh overlay's accounted bytes pass `--roll-budget`, the overlay is frozen (shared with a roll thread) and a fresh one starts; the roll thread does `view::merge` of frozen over the run into `run.N+1` and `commit::roll` into `trie.N+1`.
7. `swap_roll` at the next block boundary once the roll is done: the checker is parked on a sync item, the rolled root must equal the verified header root at the roll height (else exit 1), `MANIFEST` {gen, height, root} is written temp + fsync + rename + dir fsync, `Dirty` is rebuilt on the new file with the fresh overlay's entries replayed, the backend swaps to the new run, the old pair is unlinked, the checker resumes (swapRoll/finishRoll).
8. `--history FILE`: the checker appends per block the header RLP, the receipts RLP (EIP-2718 encoded), every tx's callTracer JSON, every state row (key, value) and the code deployed, each u32-length-prefixed, through a 1 MB BufWriter; every 256 blocks it flushes and a flusher thread fsyncs (WriteBlock + flushEvery parity in bytes, not a store).
9. A bench thread prints epochdb-vm's line every 10 s and at exit (`rss`, `overlay`, `dirty` in MB as the Go line prints them) plus a `split` line, and stops the executor `--duration` seconds after the first executed block.
10. Genesis: the alloc through the same `commit`, merged into `run.0`, rolled into `trie.0`, the rolled root checked against a full alloy-trie recompute of the alloc (`51736d52...`, Go's `genesis state ok` root).

Threads: recovery pool (`--workers`) ahead, one executor, one checker, one roll thread while rolling, one fsync thread, one bench thread. `Dirty` sits behind a mutex the checker holds per block and the executor takes only during a swap (the checker is parked then). Release profile: `lto = "fat"`, `codegen-units = 1`, keccak-asm in state, block and exec.

## Flags

`--dump FILE --genesis chain.json --upgrade upgrade.json --data DIR` (the `vmstate` dir under it is wiped at start), `--from 1` (must be 1: the state starts at genesis, recovery is out of scope), `--to N`, `--stop-at N`, `--duration S` (stop S seconds after the first executed block), `--workers N` (recovery pool, default 4), `--roll-budget MB` (default 2048, Go's catch-up profile), `--history FILE`, `--network ID` (1 unless the chain descriptor carries it). Dirty's hashing workers = available parallelism (16), used only past 256 pending slots (see the fix below).

## Oracle results

50k file, `--roll-budget 8` (three rolls at 17,546 / 31,462 / 45,339), `--workers 14`, exit 0:

- every one of the 50,000 blocks has a write set and was root-checked (`root-checked=50000`); gasUsed, receiptsRoot and logsBloom equal the header on every block (the executor checks them before handing the block over);
- all 3 rolls produced the verified root (`roll root mismatch` exits 1 otherwise), MANIFEST ends at `{"gen":3,"height":45339,"root":"0x9180f537...7ccfd086"}`;
- the final state root at 50,000 equals the header (the checker verified block 50,000 before exit);
- the same run with `--history`: identical roots and rolls, 287,657,908 bytes of history, 6.05 s wall, 240 MB peak RSS (before the Dirty fix 10.6 s, checker root 7.61 s; after it 2.52 s).

```
epochdb-rs: genesis state ok: root=0x51736d52ef12525c8a48a4d2215b34a7573e871efb62008ac8b45c25590f0d21 accounts=1 keys=1 nodes=0 run=4212B trie=128B
epochdb-rs: chainId=1234 dump=/tmp/claude-1000/-home-ilia-epochdb/222c563c-789d-46b6-8726-b8af4b2a6f62/scratchpad/rs/step/step-containers-1-50000.bin heights=1..end roll-budget=8MB workers=14 dirty-workers=16 history=/tmp/claude-1000/-home-ilia-epochdb/222c563c-789d-46b6-8726-b8af4b2a6f62/scratchpad/rs/node/history-50k.bin
epochdb-rs: roll 1 start: height=17546 overlay=52137 keys/8MB dirty=14MB
epochdb-rs: roll 1 done: height=17546 keys=52120 nodes=18005 run=4MB trie=3MB merge=20ms roll=59ms total=84ms replayed=4720 overlay=1MB dirty=16MB->0MB
epochdb-rs: roll 2 start: height=31462 overlay=52112 keys/8MB dirty=19MB
epochdb-rs: roll 2 done: height=31462 keys=104207 nodes=40281 run=7MB trie=6MB merge=32ms roll=105ms total=144ms replayed=7496 overlay=1MB dirty=21MB->0MB
epochdb-rs: roll 3 start: height=45339 overlay=52126 keys/8MB dirty=20MB
epochdb-rs: roll 3 done: height=45339 keys=156299 nodes=60479 run=10MB trie=9MB merge=32ms roll=168ms total=214ms replayed=13597 overlay=2MB dirty=24MB->0MB
bench exit t=6 h=50000 blk=50000 tx=343731 mgas/s=3609.18 cum=4331.64 wait=0.1 full=0 rss=225 overlay=2 dirty=8 rolls=3 rolling=false
split total read=0.06s evm=3.75s trace=0.32s commit=0.39s | checker apply=0.08s root=2.52s write=0.22s | exec-thread 4885.5 mgas/s | blocks=50000 root-checked=50000 rolls=3
```

1M file, `--duration 180 --history --roll-budget 256 --workers 14`: 896,445 and 892,125 blocks root-checked in the two timed runs (and 810,538 in the pre-fix run), 1 roll each at 495,426 (1,666,841 keys, 583,142 nodes, run 111 MB, trie 95 MB) with the rolled root equal to the verified one, no mismatch, exit 0.

## The Dirty::root fix (rs/state, this branch)

The first 1M run put the checker on the wall: `root=145 s` of 181 s while the EVM thread was busy 65 s (it stalled on the check queue). `Dirty::root` spawned a scoped thread per storage job on every call, tens of microseconds each, for blocks with a handful of slot updates. It now hashes the storage tries inline below 256 pending slots (`PAR_MIN_SLOTS`) and fans out above. 50k checker root 7.61 s -> 2.52 s; 1M run cum 1057 -> 1330-1348 mgas/s. The state crate's 16 tests still pass.

## A/B, same machine, same hour, sequential (22:00-22:11 JST)

Both sides: the 1M dump, fresh data dir, 180 s from the first executed block, roll budget 256 MB (so the roll at 495k happens inside the window), consumed dump pages released, total RSS on the bench line. Go additionally `GOMEMLIMIT=10GiB` as in its report; the load average before each run was 2.1 to 2.8 (the idle avalanchego test nodes and herdr).

Rust (run twice with the fixed binary; `run-1m-b.log`, `run-1m-c.log`):
```
cd $SCRATCH/rs/node && rm -rf data-1m history-1m.bin && /usr/bin/time -v \
  ~/.herdr/worktrees/epochdb/rs-node/rs/target/release/epochdb-rs \
  --dump $SCRATCH/rs/step/step-containers-1-1000000.bin --genesis $SCRATCH/rs/step/chain.json \
  --upgrade $SCRATCH/rs/step/upgrade.json --data $SCRATCH/rs/node/data-1m \
  --workers 14 --roll-budget 256 --history $SCRATCH/rs/node/history-1m.bin --duration 180
```
Go (`cmd/epochdb-vm-bench` from branch go-bench, the binary its REPORT.md built; `go-bench/run-1m-180s-256mb.log`):
```
cd $SCRATCH/go-bench && rm -rf data-1m && mkdir -p data-1m && cp $SCRATCH/rs/step/chain.json $SCRATCH/rs/step/upgrade.json data-1m/ && \
  GOMEMLIMIT=10GiB /usr/bin/time -v timeout -s INT 182 ./epochdb-vm-bench \
  --chain 2jRZvKtXY5nyWTqRwFh1KMHGrCRxJoULu4r2CsayWRnjdDGbV1 --network mainnet \
  --data $SCRATCH/go-bench/data-1m --port 19650 --roll-budget 268435456 \
  --dump $SCRATCH/rs/step/step-containers-1-1000000.bin
```
(`--roll-budget` is bytes on the Go side. The Go index build took the first 3 s of the 182 s, so its last tick is `t=170` and the `bench exit` line at `t=179` is its 180 s figure.)

| | Go epochdb-vm-bench | Rust run 2 | Rust run 3 | Rust / Go |
|---|---|---|---|---|
| cum mgas/s at the end | 821.64 (t=179) | 1329.58 (t=180) | 1347.90 (t=180) | 1.63x |
| blocks | 630,534 | 891,289 | 895,592 | |
| blk/s | 3,522 | 4,952 | 4,976 | 1.41x |
| txs | 2,644,939 | 3,438,491 | 3,452,729 | |
| tx/s | 14,776 | 19,103 | 19,182 | 1.30x |
| peak RSS (`/usr/bin/time`) | 8,428 MB | 1,346 MB | 1,338 MB | 0.16x |
| rss on the last bench line | 7,215 MB | 1,027 MB | 1,048 MB | |
| rolls | 1 (at 495,427: merge 2.74 s + roll 3.06 s = 5.83 s) | 1 (495,426: 2.36 + 2.07 = 4.55 s) | 1 (1.97 + 1.69 = 3.79 s) | |
| wait on the block source | 5.4 s | 0.3 s | 0.3 s | |
| user + sys CPU | 492 + 49 s | 488 + 30 s | 489 + 28 s | |

Like for like on the same blocks: Go reached 630k in 179 s; Rust passed 644k-650k at t=60 with cum 2,527-2,545 mgas/s, so blocks 1..650k took Rust 60 s and Go 180 s (3.0x). Past 700k the chain turns into a few 0.8-1.4 Mgas txs per block with 64 nested STATICCALLs each, where the Rust executor does 600-700 mgas/s single-threaded; the Rust 180 s window spends its second half there, which is what pulls its cum from 2,545 down to 1,348. The Go side never got that far in the window. The Go peak RSS is GOGC=400 headroom under the 10 GiB limit, not live data; the Rust process has no such headroom (overlay 217 MB + dirty 426 MB + the code table and caches).

Per-window (blk/s and tx/s derived from consecutive bench lines; the first row is from t=0):

Go:
| t | h | blk/s | tx/s | mgas/s window |
|---|---|---|---|---|
| 10 | 13173 | 1317 | 18311 | 1210.93 |
| 20 | 55709 | 4254 | 18467 | 1085.66 |
| 30 | 101883 | 4617 | 18309 | 890.24 |
| 40 | 146617 | 4473 | 17770 | 865.62 |
| 50 | 188910 | 4229 | 16746 | 825.71 |
| 60 | 232210 | 4330 | 17179 | 843.47 |
| 70 | 271910 | 3970 | 15618 | 770.87 |
| 80 | 325633 | 5372 | 21302 | 781.68 |
| 90 | 364234 | 3860 | 15183 | 742.90 |
| 100 | 404736 | 4050 | 15981 | 782.80 |
| 110 | 441592 | 3686 | 14739 | 713.58 |
| 120 | 478976 | 3738 | 14752 | 725.49 |
| 130 | 512211 | 3324 | 13253 | 645.04 |
| 140 | 548023 | 3581 | 14292 | 693.77 |
| 150 | 577427 | 2940 | 11550 | 751.31 |
| 160 | 577952 | 52 | 276 | 854.83 |
| 170 | 591771 | 1382 | 5592 | 722.93 |

Rust run 2:
| t | h | blk/s | tx/s | mgas/s window |
|---|---|---|---|---|
| 10 | 104503 | 10450 | 56099 | 3229.11 |
| 20 | 237697 | 13319 | 52924 | 2592.21 |
| 30 | 383558 | 14586 | 57498 | 2561.17 |
| 40 | 511376 | 12782 | 50741 | 2476.27 |
| 50 | 577794 | 6642 | 26376 | 2055.58 |
| 60 | 644005 | 6621 | 26214 | 1986.53 |
| 70 | 706197 | 6219 | 19132 | 1217.11 |
| 80 | 751450 | 4525 | 10896 | 1026.24 |
| 90 | 788081 | 3663 | 9364 | 926.98 |
| 100 | 806263 | 1818 | 5211 | 695.53 |
| 110 | 819863 | 1360 | 4846 | 718.69 |
| 120 | 829129 | 927 | 4012 | 696.54 |
| 130 | 840764 | 1164 | 3877 | 571.20 |
| 140 | 852036 | 1127 | 3777 | 539.75 |
| 150 | 866547 | 1451 | 4818 | 657.02 |
| 160 | 874931 | 838 | 2721 | 613.25 |
| 170 | 882940 | 801 | 2623 | 600.45 |
| 180 | 891289 | 835 | 2721 | 628.75 |

Rust run 3:
| t | h | blk/s | tx/s | mgas/s window |
|---|---|---|---|---|
| 10 | 109049 | 10905 | 57903 | 3321.48 |
| 20 | 237908 | 12886 | 51204 | 2507.90 |
| 30 | 387677 | 14977 | 59072 | 2635.71 |
| 40 | 514341 | 12666 | 50302 | 2454.01 |
| 50 | 577852 | 6351 | 25188 | 2094.54 |
| 60 | 649756 | 7190 | 28459 | 1999.54 |
| 70 | 708478 | 5872 | 17429 | 1173.53 |
| 80 | 754746 | 4627 | 11125 | 1029.47 |
| 90 | 789515 | 3477 | 8927 | 900.80 |
| 100 | 807384 | 1787 | 5220 | 705.63 |
| 110 | 820660 | 1328 | 4792 | 716.80 |
| 120 | 830081 | 942 | 4014 | 694.38 |
| 130 | 843722 | 1364 | 4571 | 673.87 |
| 140 | 858822 | 1510 | 4973 | 671.85 |
| 150 | 870530 | 1171 | 3884 | 660.88 |
| 160 | 878902 | 837 | 2743 | 620.52 |
| 170 | 887256 | 835 | 2731 | 632.80 |
| 180 | 895592 | 834 | 2734 | 629.06 |

Full bench lines, Go:
```
epochdb-vm-bench: 2jRZvKtXY5nyWTqRwFh1KMHGrCRxJoULu4r2CsayWRnjdDGbV1 on :19650 chainId=1234 dump=/tmp/claude-1000/-home-ilia-epochdb/222c563c-789d-46b6-8726-b8af4b2a6f62/scratchpad/rs/step/step-containers-1-1000000.bin heights=1..1000000 roll-budget=256MB/128MB tip-lag=5000
vmexec: budget catch-up (start): gogc=400 roll-budget=256MB rss=125MB
bench t=10 h=13173 blk=13173 tx=183109 mgas/s=1210.93 cum=1345.56 wait=0.0 full=0 rss=876 overlay=5 dirty=8 rolls=0 rolling=false
bench t=20 h=55709 blk=55709 tx=367777 mgas/s=1085.66 cum=1208.78 wait=0.4 full=0 rss=1938 overlay=29 dirty=47 rolls=0 rolling=false
bench t=30 h=101883 blk=101883 tx=550866 mgas/s=890.24 cum=1098.93 wait=0.8 full=0 rss=2604 overlay=54 dirty=86 rolls=0 rolling=false
bench t=40 h=146617 blk=146617 tx=728562 mgas/s=865.62 cum=1039.11 wait=1.2 full=0 rss=2901 overlay=79 dirty=126 rolls=0 rolling=false
bench t=50 h=188910 blk=188910 tx=896018 mgas/s=825.71 cum=995.56 wait=1.6 full=0 rss=3824 overlay=101 dirty=163 rolls=0 rolling=false
bench t=60 h=232210 blk=232210 tx=1067806 mgas/s=843.47 cum=969.78 wait=1.9 full=0 rss=4216 overlay=125 dirty=202 rolls=0 rolling=false
bench t=70 h=271910 blk=271910 tx=1223984 mgas/s=770.87 cum=940.95 wait=2.3 full=0 rss=4806 overlay=146 dirty=239 rolls=0 rolling=false
bench t=80 h=325633 blk=325633 tx=1437007 mgas/s=781.68 cum=920.79 wait=2.6 full=0 rss=5758 overlay=162 dirty=265 rolls=0 rolling=false
bench t=90 h=364234 blk=364234 tx=1588834 mgas/s=742.90 cum=900.80 wait=3.0 full=0 rss=6616 overlay=183 dirty=299 rolls=0 rolling=false
bench t=100 h=404736 blk=404736 tx=1748648 mgas/s=782.80 cum=888.88 wait=3.3 full=0 rss=6301 overlay=205 dirty=333 rolls=0 rolling=false
bench t=110 h=441592 blk=441592 tx=1896039 mgas/s=713.58 cum=872.80 wait=3.7 full=0 rss=7525 overlay=226 dirty=366 rolls=0 rolling=false
bench t=120 h=478976 blk=478976 tx=2043562 mgas/s=725.49 cum=860.42 wait=4.0 full=0 rss=7589 overlay=246 dirty=400 rolls=0 rolling=false
vmexec: roll 1 start: height=495427 overlay=1668257 keys/268MB dirty=436MB runs=1
vmexec: roll 1 done: height=495427 keys=1666845 nodes=583143 run=111MB trie=95MB merge=2.738s roll=3.058s total=5.831s replayed=61378 overlay=10MB dirty=452MB->0MB
bench t=130 h=512211 blk=512211 tx=2176096 mgas/s=645.04 cum=843.73 wait=4.3 full=0 rss=7813 overlay=9 dirty=431 rolls=0 rolling=true
bench t=140 h=548023 blk=548023 tx=2319012 mgas/s=693.77 cum=832.94 wait=4.6 full=0 rss=4544 overlay=29 dirty=73 rolls=1 rolling=false
bench t=150 h=577427 blk=577427 tx=2434515 mgas/s=751.31 cum=827.46 wait=4.9 full=0 rss=4496 overlay=53 dirty=118 rolls=1 rolling=false
bench t=160 h=577952 blk=577952 tx=2437278 mgas/s=854.83 cum=829.18 wait=4.9 full=0 rss=5463 overlay=87 dirty=169 rolls=1 rolling=false
bench t=170 h=591771 blk=591771 tx=2493198 mgas/s=722.93 cum=822.89 wait=5.1 full=0 rss=6766 overlay=108 dirty=203 rolls=1 rolling=false
bench exit t=179 h=630534 blk=630534 tx=2644939 mgas/s=798.90 cum=821.64 wait=5.4 full=0 rss=7215 overlay=130 dirty=244 rolls=1 rolling=false
```

Full bench lines, Rust run 2:
```
epochdb-rs: genesis state ok: root=0x51736d52ef12525c8a48a4d2215b34a7573e871efb62008ac8b45c25590f0d21 accounts=1 keys=1 nodes=0 run=4212B trie=128B
epochdb-rs: chainId=1234 dump=/tmp/claude-1000/-home-ilia-epochdb/222c563c-789d-46b6-8726-b8af4b2a6f62/scratchpad/rs/step/step-containers-1-1000000.bin heights=1..end roll-budget=256MB workers=14 dirty-workers=16 history=/tmp/claude-1000/-home-ilia-epochdb/222c563c-789d-46b6-8726-b8af4b2a6f62/scratchpad/rs/node/history-1m.bin
bench t=10 h=104503 blk=104503 tx=560992 mgas/s=3229.11 cum=3594.74 wait=0.1 full=0 rss=327 overlay=56 dirty=94 rolls=0 rolling=false
bench t=20 h=237697 blk=237697 tx=1090235 mgas/s=2592.21 cum=3067.16 wait=0.1 full=0 rss=494 overlay=128 dirty=221 rolls=0 rolling=false
bench t=30 h=383558 blk=383558 tx=1665220 mgas/s=2561.17 cum=2892.70 wait=0.1 full=0 rss=675 overlay=194 dirty=335 rolls=0 rolling=false
epochdb-rs: roll 1 start: height=495426 overlay=1668263 keys/268MB dirty=464MB
bench t=40 h=511376 blk=511376 tx=2172627 mgas/s=2476.27 cum=2785.93 wait=0.2 full=0 rss=839 overlay=8 dirty=458 rolls=0 rolling=true
epochdb-rs: roll 1 done: height=495426 keys=1666841 nodes=583142 run=111MB trie=95MB merge=2356ms roll=2071ms total=4554ms replayed=175861 overlay=28MB dirty=514MB->0MB
bench t=50 h=577794 blk=577794 tx=2436388 mgas/s=2055.58 cum=2636.89 wait=0.2 full=0 rss=863 overlay=77 dirty=159 rolls=1 rolling=false
bench t=60 h=644005 blk=644005 tx=2698531 mgas/s=1986.53 cum=2526.62 wait=0.2 full=0 rss=1076 overlay=137 dirty=271 rolls=1 rolling=false
bench t=70 h=706197 blk=706197 tx=2889852 mgas/s=1217.11 cum=2336.83 wait=0.2 full=0 rss=1118 overlay=161 dirty=319 rolls=1 rolling=false
bench t=80 h=751450 blk=751450 tx=2998807 mgas/s=1026.24 cum=2170.95 wait=0.2 full=0 rss=1184 overlay=179 dirty=352 rolls=1 rolling=false
bench t=90 h=788081 blk=788081 tx=3092450 mgas/s=926.98 cum=2031.20 wait=0.2 full=0 rss=1249 overlay=191 dirty=377 rolls=1 rolling=false
bench t=100 h=806263 blk=806263 tx=3144556 mgas/s=695.53 cum=1896.30 wait=0.2 full=0 rss=1244 overlay=196 dirty=385 rolls=1 rolling=false
bench t=110 h=819863 blk=819863 tx=3193011 mgas/s=718.69 cum=1788.27 wait=0.2 full=0 rss=1229 overlay=200 dirty=392 rolls=1 rolling=false
bench t=120 h=829129 blk=829129 tx=3233126 mgas/s=696.54 cum=1696.54 wait=0.2 full=0 rss=1261 overlay=202 dirty=397 rolls=1 rolling=false
bench t=130 h=840764 blk=840764 tx=3271893 mgas/s=571.20 cum=1609.29 wait=0.2 full=0 rss=1234 overlay=205 dirty=401 rolls=1 rolling=false
bench t=140 h=852036 blk=852036 tx=3309663 mgas/s=539.75 cum=1532.36 wait=0.3 full=0 rss=1271 overlay=207 dirty=406 rolls=1 rolling=false
bench t=150 h=866547 blk=866547 tx=3357843 mgas/s=657.02 cum=1473.61 wait=0.3 full=0 rss=1255 overlay=211 dirty=413 rolls=1 rolling=false
bench t=160 h=874931 blk=874931 tx=3385051 mgas/s=613.25 cum=1419.51 wait=0.3 full=0 rss=1281 overlay=212 dirty=416 rolls=1 rolling=false
bench t=170 h=882940 blk=882940 tx=3411277 mgas/s=600.45 cum=1371.05 wait=0.3 full=0 rss=1307 overlay=214 dirty=419 rolls=1 rolling=false
bench t=180 h=891289 blk=891289 tx=3438491 mgas/s=628.75 cum=1329.58 wait=0.3 full=0 rss=1271 overlay=216 dirty=423 rolls=1 rolling=false
bench exit t=181 h=892125 blk=892125 tx=3441253 mgas/s=629.64 cum=1325.65 wait=0.3 full=0 rss=1027 overlay=216 dirty=423 rolls=1 rolling=false
split total read=0.27s evm=130.73s trace=15.49s commit=5.93s | checker apply=1.66s root=66.89s write=11.86s | exec-thread 1568.6 mgas/s | blocks=892125 root-checked=892125 rolls=1
```

Full bench lines, Rust run 3:
```
epochdb-rs: genesis state ok: root=0x51736d52ef12525c8a48a4d2215b34a7573e871efb62008ac8b45c25590f0d21 accounts=1 keys=1 nodes=0 run=4212B trie=128B
epochdb-rs: chainId=1234 dump=/tmp/claude-1000/-home-ilia-epochdb/222c563c-789d-46b6-8726-b8af4b2a6f62/scratchpad/rs/step/step-containers-1-1000000.bin heights=1..end roll-budget=256MB workers=14 dirty-workers=16 history=/tmp/claude-1000/-home-ilia-epochdb/222c563c-789d-46b6-8726-b8af4b2a6f62/scratchpad/rs/node/history-1m.bin
bench t=10 h=109049 blk=109049 tx=579034 mgas/s=3321.48 cum=3694.11 wait=0.1 full=0 rss=280 overlay=58 dirty=98 rolls=0 rolling=false
bench t=20 h=237908 blk=237908 tx=1091077 mgas/s=2507.90 cum=3069.73 wait=0.1 full=0 rss=494 overlay=128 dirty=221 rolls=0 rolling=false
bench t=30 h=387677 blk=387677 tx=1681799 mgas/s=2635.71 cum=2920.03 wait=0.1 full=0 rss=627 overlay=196 dirty=339 rolls=0 rolling=false
epochdb-rs: roll 1 start: height=495426 overlay=1668263 keys/268MB dirty=464MB
bench t=40 h=514341 blk=514341 tx=2184819 mgas/s=2454.01 cum=2800.54 wait=0.2 full=0 rss=852 overlay=10 dirty=461 rolls=0 rolling=true
epochdb-rs: roll 1 done: height=495426 keys=1666841 nodes=583142 run=111MB trie=95MB merge=1970ms roll=1693ms total=3785ms replayed=153419 overlay=25MB dirty=508MB->0MB
bench t=50 h=577852 blk=577852 tx=2436703 mgas/s=2094.54 cum=2656.42 wait=0.2 full=0 rss=872 overlay=81 dirty=167 rolls=1 rolling=false
bench t=60 h=649756 blk=649756 tx=2721290 mgas/s=1999.54 cum=2545.11 wait=0.2 full=0 rss=1029 overlay=140 dirty=279 rolls=1 rolling=false
bench t=70 h=708478 blk=708478 tx=2895579 mgas/s=1173.53 cum=2346.38 wait=0.2 full=0 rss=1150 overlay=163 dirty=323 rolls=1 rolling=false
bench t=80 h=754746 blk=754746 tx=3006827 mgas/s=1029.47 cum=2179.71 wait=0.2 full=0 rss=1187 overlay=180 dirty=356 rolls=1 rolling=false
bench t=90 h=789515 blk=789515 tx=3096094 mgas/s=900.80 cum=2036.03 wait=0.2 full=0 rss=1246 overlay=192 dirty=379 rolls=1 rolling=false
bench t=100 h=807384 blk=807384 tx=3148299 mgas/s=705.63 cum=1901.67 wait=0.2 full=0 rss=1240 overlay=196 dirty=387 rolls=1 rolling=false
bench t=110 h=820660 blk=820660 tx=3196224 mgas/s=716.80 cum=1792.98 wait=0.2 full=0 rss=1224 overlay=200 dirty=394 rolls=1 rolling=false
bench t=120 h=830081 blk=830081 tx=3236365 mgas/s=694.38 cum=1700.67 wait=0.2 full=0 rss=1256 overlay=202 dirty=398 rolls=1 rolling=false
bench t=130 h=843722 blk=843722 tx=3282078 mgas/s=673.87 cum=1621.08 wait=0.3 full=0 rss=1236 overlay=205 dirty=404 rolls=1 rolling=false
bench t=140 h=858822 blk=858822 tx=3331807 mgas/s=671.85 cum=1552.80 wait=0.3 full=0 rss=1284 overlay=209 dirty=410 rolls=1 rolling=false
bench t=150 h=870530 blk=870530 tx=3370651 mgas/s=660.88 cum=1492.94 wait=0.3 full=0 rss=1260 overlay=212 dirty=416 rolls=1 rolling=false
bench t=160 h=878902 blk=878902 tx=3398084 mgas/s=620.52 cum=1438.08 wait=0.3 full=0 rss=1286 overlay=213 dirty=419 rolls=1 rolling=false
bench t=170 h=887256 blk=887256 tx=3425391 mgas/s=632.80 cum=1390.43 wait=0.3 full=0 rss=1249 overlay=215 dirty=423 rolls=1 rolling=false
bench t=180 h=895592 blk=895592 tx=3452729 mgas/s=629.06 cum=1347.90 wait=0.3 full=0 rss=1271 overlay=217 dirty=426 rolls=1 rolling=false
bench exit t=181 h=896445 blk=896445 tx=3455478 mgas/s=620.27 cum=1343.80 wait=0.3 full=0 rss=1048 overlay=217 dirty=426 rolls=1 rolling=false
split total read=0.26s evm=131.88s trace=15.34s commit=5.87s | checker apply=1.64s root=66.73s write=8.75s | exec-thread 1580.5 mgas/s | blocks=896445 root-checked=896445 rolls=1
```

Rust run 1, the pre-fix binary (thread-per-job `Dirty::root`), same flags, for the record:
```
epochdb-rs: genesis state ok: root=0x51736d52ef12525c8a48a4d2215b34a7573e871efb62008ac8b45c25590f0d21 accounts=1 keys=1 nodes=0 run=4212B trie=128B
epochdb-rs: chainId=1234 dump=/tmp/claude-1000/-home-ilia-epochdb/222c563c-789d-46b6-8726-b8af4b2a6f62/scratchpad/rs/step/step-containers-1-1000000.bin heights=1..end roll-budget=256MB workers=14 dirty-workers=16 history=/tmp/claude-1000/-home-ilia-epochdb/222c563c-789d-46b6-8726-b8af4b2a6f62/scratchpad/rs/node/history-1m.bin
bench t=10 h=47272 blk=47272 tx=332707 mgas/s=2128.07 cum=2366.07 wait=0.0 full=0 rss=215 overlay=25 dirty=42 rolls=0 rolling=false
bench t=20 h=99925 blk=99925 tx=543153 mgas/s=1018.63 cum=1656.84 wait=0.0 full=0 rss=310 overlay=53 dirty=90 rolls=0 rolling=false
bench t=30 h=151299 blk=151299 tx=747764 mgas/s=994.73 cum=1428.51 wait=0.1 full=0 rss=393 overlay=81 dirty=138 rolls=0 rolling=false
bench t=40 h=201337 blk=201337 tx=945068 mgas/s=974.02 cum=1311.97 wait=0.1 full=0 rss=412 overlay=108 dirty=186 rolls=0 rolling=false
bench t=50 h=251342 blk=251342 tx=1144578 mgas/s=975.35 cum=1243.29 wait=0.1 full=0 rss=487 overlay=135 dirty=234 rolls=0 rolling=false
bench t=60 h=303226 blk=303226 tx=1348436 mgas/s=885.02 cum=1182.58 wait=0.1 full=0 rss=563 overlay=157 dirty=273 rolls=0 rolling=false
bench t=70 h=359230 blk=359230 tx=1569491 mgas/s=942.23 cum=1147.75 wait=0.1 full=0 rss=644 overlay=180 dirty=313 rolls=0 rolling=false
bench t=80 h=409248 blk=409248 tx=1766479 mgas/s=965.36 cum=1124.67 wait=0.1 full=0 rss=733 overlay=208 dirty=359 rolls=0 rolling=false
bench t=90 h=460271 blk=460271 tx=1969891 mgas/s=988.23 cum=1109.34 wait=0.2 full=0 rss=748 overlay=236 dirty=407 rolls=0 rolling=false
epochdb-rs: roll 1 start: height=495426 overlay=1668263 keys/268MB dirty=464MB
bench t=100 h=510207 blk=510207 tx=2167872 mgas/s=969.09 cum=1095.17 wait=0.2 full=0 rss=875 overlay=8 dirty=456 rolls=0 rolling=true
epochdb-rs: roll 1 done: height=495426 keys=1666841 nodes=583142 run=111MB trie=95MB merge=2492ms roll=1800ms total=4348ms replayed=77646 overlay=12MB dirty=486MB->0MB
bench t=110 h=557764 blk=557764 tx=2357401 mgas/s=920.81 cum=1079.18 wait=0.2 full=0 rss=580 overlay=34 dirty=89 rolls=1 rolling=false
bench t=120 h=578120 blk=578120 tx=2438219 mgas/s=1658.10 cum=1127.82 wait=0.2 full=0 rss=807 overlay=94 dirty=191 rolls=1 rolling=false
bench t=130 h=622349 blk=622349 tx=2612288 mgas/s=1067.76 cum=1123.17 wait=0.2 full=0 rss=945 overlay=125 dirty=252 rolls=1 rolling=false
bench t=140 h=671395 blk=671395 tx=2806454 mgas/s=947.06 cum=1110.50 wait=0.2 full=0 rss=965 overlay=152 dirty=306 rolls=1 rolling=false
bench t=150 h=717808 blk=717808 tx=2917973 mgas/s=982.44 cum=1101.90 wait=0.2 full=0 rss=1096 overlay=167 dirty=335 rolls=1 rolling=false
bench t=160 h=758015 blk=758015 tx=3014691 mgas/s=871.78 cum=1087.43 wait=0.2 full=0 rss=1109 overlay=181 dirty=362 rolls=1 rolling=false
bench t=170 h=791968 blk=791968 tx=3102233 mgas/s=906.03 cum=1076.70 wait=0.3 full=0 rss=1101 overlay=193 dirty=384 rolls=1 rolling=false
bench t=180 h=809070 blk=809070 tx=3154206 mgas/s=718.22 cum=1056.67 wait=0.3 full=0 rss=1157 overlay=196 dirty=392 rolls=1 rolling=false
bench exit t=181 h=810538 blk=810538 tx=3159416 mgas/s=782.70 cum=1055.14 wait=0.3 full=0 rss=1022 overlay=197 dirty=393 rolls=1 rolling=false
split total read=0.26s evm=65.31s trace=8.20s commit=5.48s | checker apply=1.55s root=145.49s write=6.52s | exec-thread 2404.9 mgas/s | blocks=810538 root-checked=810538 rolls=1
```

## Time split (Rust run 3, 181 s wall)

Executor thread: block wait 0.26 s, evm 131.88 s, callTracer JSON 15.34 s, commit (rows + overlay put + tombstone scans) 5.87 s: 153 s busy, 1,580 mgas/s per executor second. Checker thread: Dirty apply 1.64 s, Dirty root 66.73 s, history write 8.75 s. By window: at t=10 (transfer-heavy blocks, 11k blk/s) `evm=5.88s root=5.69s` per 10 s, the two threads balanced; at t=120+ (the contract tail, 1k blk/s) `evm=8.6s trace=1.0s root=1.0s`, the executor is the wall. Sender recovery is never the wait (0.3 s). Where the time goes next: revm frame cost on the 64-STATICCALL txs plus the always-on TracingInspector (evm), then the trace JSON, then Dirty root in the early stretch (5.7-6.8 s per 10 s at 100k-400k, which would become the wall again with a faster executor).

## Deviations from the Go node

- `Dirty::root` hashes storage tries inline below 256 pending slots (Go's goroutine pool is always on); the roll and the file formats are unchanged and the roots equal.
- Code is an in-memory table by hash (Go: the store's ethdb behind libevm's cachingDB), written into the history file only.
- The history file is raw and uncompressed (13.0 GB for 896k blocks, 14.5 KB per block, most of it the tail's callTracer JSON) while Go's store compresses sealed runs (1.3 GB for 650k blocks); the write cost was 8.75 s of 181 s on the checker thread. Parity is in what is written, not in bytes on disk.
- The executor checks gasUsed, receiptsRoot and logsBloom against the header on every block (vmexec does not).
- Empty write sets are still root-compared (current root vs header); vmexec skips empty blocks entirely.
- No tip profile, no GOGC, no memory limit: one fixed `--roll-budget`; no `--stop`-then-serve, the process exits.
- BLOCKHASH is served from a 256-entry map of executed hashes (Go: recent headers, then the store).
- State rows in the history are the block's ordered write set in contract-key form plus deployed code (Go's per-tx address-form rows keyed by the store), in revm's journal order (Go's interceptor order, map-ordered on both sides).
- Containers stay zero-copy slices of the mapping (Go copies them out); page release is 64 MB behind the recovery cursor, a refault comes from the page cache.
- The bench line prints `rss`, `overlay`, `dirty` in MB like the Go line (the task text said bytes; the Go meaning won).
- `--from` other than 1 is refused; recovery, RPC, p2p and the store are out of scope (round 1).

## Open items

- The executor thread is the wall on the contract tail (evm 8.6 s of 10 s): profile revm's frame path on the 64-STATICCALL txs and the inspector hooks; a `perf`-less split is in place, the next step is a sampling profile.
- Trace JSON is 10 percent of the executor thread; rendering it on the checker (Go moved encoding/json there) is a free 10 percent on the tail.
- Dirty root at 100k-400k is 60 percent of the checker's time; the account trie commit per block and node keccaks are the next target there.
- History volume: compress or drop the trace bodies for a bytes-on-disk parity with the Go store.
- musl static build for the box (`cargo build --release --target x86_64-unknown-linux-musl`) not done this round (box off limits); the state and block crates already build for it.
- The Go `t=180` tick is missing from its 256 MB run because the index build ate 3 s of the 182 s timeout; use `timeout 185` next time.

## Files

`rs/Cargo.toml` (workspace, release profile), `rs/node/Cargo.toml`, `rs/node/src/main.rs` (pipeline, checker, history, bench, roll swap), `rs/node/src/engine.rs` (Backend: revm Database/DatabaseCommit over overlay + run, tombstones, hash caches, code table; roll thread, MANIFEST, seek), `rs/exec/src/exec.rs` (`StateDb` trait, `Executor<D>`, `with_db`), `rs/block/src/dump.rs` (page release), `rs/state/src/commit/dirty.rs` (inline small roots). Logs: `$SCRATCH/rs/node/run-50k*.log`, `run-1m-{a,b,c}.log`, `$SCRATCH/go-bench/run-1m-180s-256mb.log` with `SCRATCH=/tmp/claude-1000/-home-ilia-epochdb/222c563c-789d-46b6-8726-b8af4b2a6f62/scratchpad`.

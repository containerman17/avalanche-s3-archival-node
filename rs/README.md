# epochdb-rs

The Rust rewrite of the epochdb follower: one cargo workspace under `rs/`, one binary `epochdb-rs` that is an avalanchego rpcchainvm plugin (protocol 45) with the executor, the state engine, the store and the JSON-RPC / WebSocket surface in-process. Branch `rust` is every `rs-*` branch merged. Each crate keeps its own `REPORT.md` with the design, the oracle numbers and the Go source it was ported from; this file is the map.

## Crate map

| crate (package) | lib / bins | what it is | report |
|---|---|---|---|
| `state` (`epochdb-state`) | lib `state`; bins `bench`, `rscompat` | the `latest` state engine: overlay, frozen window, run files (format v2), the Dirty trie and its root, the roll | `state/REPORT.md`, `state/LAYOUT.md` |
| `block` (`epochdb-block`) | lib `block`; bin `blockcheck` | proposervm container unwrap, subnet-evm block / header / tx decode and encode, sender recovery, the container dump reader | `block/REPORT.md` |
| `exec` (`epochdb-exec`) | lib `exec`; bin `epochdb-exec` | subnet-evm block execution on revm: fee rules, the stateful precompile set (allow lists, FeeManager, RewardManager, NativeMinter, warp), state upgrades, forks through Granite, the tracers (callTracer, prestate, struct, 4byte, mux) | `exec/REPORT.md` |
| `node` (`epochdb-node`) | lib `node` | the in-process bench (`epochdb-rs --dump`): dump -> executor -> checker thread (root one block behind) -> roll; `Backend` = overlay + frozen + run | `node/REPORT.md` |
| `store` (`epochdb-store`) | lib `store`; bin `storecheck` | storage v4: window log, L0 seal, terminal merge, Pebblev2 sstable sections, Elias-Fano postings, casfs (local spool, S3, chunk cache with eviction), reader snapshots | `store/REPORT.md` |
| `rpc` (`epochdb-rpc`) | lib `rpc`; bin `epochdb-rpc-serve` | eth_ / debug_ / ots_ / edb_ / net_ / web3_ / txpool_ over a `Store` trait, eth_call and estimateGas through the executor, re-executing tracers, filters, the fee oracle, `/ws` with eth_subscribe | `rpc/REPORT.md` |
| `plugin` (`epochdb-plugin`) | lib `plugin`; bin `epochdb-rs` | the rpcchainvm plugin: `vm.proto` server, block tree, `NodeEngine` (executor + roller + checker + DbStore), ghttp `/rpc` and `/ws` (hijack path), genesis header | `plugin/REPORT.md` |
| `layout` (`epochdb-layout`) | bin `layout` | the run-file layout experiment that chose format v2 | `state/LAYOUT.md` |

Dependency order: `state` <- `node`; `block` <- `exec` <- `node`, `store`, `rpc` <- `plugin`. The protos under `plugin/proto/` are avalanchego `v1.14.3-0.20260804141953-6dc4c3b395b6` verbatim (`RPCChainVMProtocol = 45`), compiled by `plugin/build.rs` (needs `protoc`).

## Build

```
cd rs
cargo build --release                       # every bin into rs/target/release/
cargo test --workspace --release            # 44 tests
cargo clippy --workspace                    # warnings only
```

Static binary for a box without a matching glibc:

```
rustup target add x86_64-unknown-linux-musl   # once; musl-tools (musl-gcc) must be installed
cargo build --release --target x86_64-unknown-linux-musl -p epochdb-plugin
./target/x86_64-unknown-linux-musl/release/epochdb-rs --version    # epochdb-rs/0.1.0 [rpcchainvm=45]
```

The release profile keeps debug info (`debug = 1`, fat LTO), so the binary is ~78 MB; strip it if size matters.

## The binary

`epochdb-rs` with no arguments is the plugin: it expects `AVALANCHE_VM_RUNTIME_ENGINE_ADDR` in the environment (avalanchego's subprocess runtime sets it), dials it with `Runtime.Initialize{protocol_version: 45}` and serves `vm.VM`. Without the variable it exits 1 like the Go plugin.

`epochdb-rs --version` prints `epochdb-rs/0.1.0 [rpcchainvm=45]`.

`epochdb-rs --dump FILE --genesis chain.json --upgrade upgrade.json --data DIR [--to N] [--duration S] [--workers 14] [--roll-budget MB] [--history FILE]` is the in-process bench (rs/node): no gRPC, no store, the executor against a container dump, roots checked one block behind, a bench line every 10 s in the Go `epochdb-vm-bench` shape.

### Under `epochdb-host-bench` (the local harness)

`cmd/epochdb-host-bench` is avalanchego's own rpcchainvm client, factory and runtime manager with a container dump as the block source. The dump dir must hold `chain.json` (the chain package's cache file: `genesisData`, `blockchainID`, `subnetID`, `networkID`) and `upgrade.json`.

```
go run ./cmd/epochdb-host-bench --dump step-containers-1-50000.bin --vm rs/target/release/epochdb-rs \
  --data /tmp/rs-data --http 127.0.0.1:19902 --batch 256 \
  --config '{"state-sync-enabled":false,"roll-budget-mb":8}' [--to N] [--serve] [--feed-delay 500ms]
```

`--batch 256` = BatchedParseBlock (1 = ParseBlock per block, what a bootstrapping avalanchego does); `--serve` keeps the HTTP mount up after the dump; `--feed-delay` paces the feed for websocket clients. Handlers mount at `http://<http>/ext/bc/<blockchainID>/rpc` and `/ws`. The harness prints a `check` line at the end (eth_blockNumber and the head hash through `/rpc` against keccak(header) of the dump's last container, `match=true`) and a `check genesis` line. A restart on the same `--data` resumes from the plugin's LastAccepted.

`cmd/epochdb-host` is the same host with the fetch package (live peers) as the block source: `--chain <blockchainID> --vm rs/target/release/epochdb-rs --node <rpc uris> --data DIR`.

### Under a stock avalanchego

A chain's plugin is the file named by the chain's VM id in avalanchego's plugin dir (`--plugin-dir`, default `~/.avalanchego/plugins`). For a subnet-evm chain the VM id is `srEXiWaHuhNyGwPUi444Tu47ZEDwxTWrbQiuD7FmgSAQ6X7Dy`, so:

```
cp rs/target/x86_64-unknown-linux-musl/release/epochdb-rs ~/.avalanchego/plugins/srEXiWaHuhNyGwPUi444Tu47ZEDwxTWrbQiuD7FmgSAQ6X7Dy
```

and avalanchego launches it for every chain of that VM it tracks (`--track-subnets`). The chain config is `~/.avalanchego/configs/chains/<blockchainID>/config.json`; its bytes are what the plugin receives as `config_bytes`. The plugin's data goes under avalanchego's chain data dir (`<data-dir>/chainData/<blockchainID>/`): `vmstate/` (run + trie + MANIFEST of the state engine) and `store/` (the epochdb store). The plugin is a follower: BuildBlock is refused, state sync answers "not implemented", `/rpc` and `/ws` are mounted at `/ext/bc/<blockchainID>/`. Not yet done under a real avalanchego (only the harness, which is the same client code); the difference under a real node is consensus calling Reject and verifying siblings, which `plugin::tree`'s test covers.

### Configuration

Config bytes (the chain config JSON):

| key | meaning |
|---|---|
| `state-sync-enabled` | ignored by the plugin (it never state-syncs); the harness passes `false` so stock subnet-evm executes every block under the same config |
| `roll-budget-mb` | overlay bytes before a roll while bootstrapping (default 2048 = `SyncRoll`); the tip budget after SetState(NormalOp) is `min(roll-budget-mb, 128)` |

Unknown keys are ignored, so a stock subnet-evm config works as is.

Environment variables (read by `rs/store`, `casfs.rs` and `db.rs`):

| variable | meaning |
|---|---|
| `EPOCHDB_S3_ENDPOINT`, `EPOCHDB_S3_BUCKET`, `EPOCHDB_S3_ACCESS_KEY`, `EPOCHDB_S3_SECRET_KEY` | the casfs remote (SigV4 path style); all four required together, static keys only, no default credential chain. Unset = local only |
| `EPOCHDB_S3_PREFIX`, `EPOCHDB_S3_REGION` | key prefix (default none) and region (default `auto`) |
| `EPOCHDB_CACHE_DIR` | chunk cache root (default `<store>/cache`) |
| `EPOCHDB_CACHE_MIN_FREE` | admission floor in bytes of free space on the cache filesystem (default 5 percent of it); the eviction target is twice it |
| `EPOCHDB_CACHE_MAX_AGE` | cache window age limit, seconds or `Nh`/`Nm`/`Ns` (default 30 days) |
| `EPOCHDB_TERMINAL_TXS` | TxNum slots per terminal run (default 8,000,000); lowered for the merge oracle |
| `EPOCHDB_NEW_CHAIN=1` | let `join` start a chain that has no `latest-<chainroot>` pointer on the remote |
| `EPOCHDB_RPC_NOW=<unix s>` | pin the RPC's wall clock (eth_gasPrice / maxPriorityFeePerGas) for a deterministic differential |
| `EPOCHDB_V1_CONFIGS` | dir of fleet v1 chain configs for `exec`'s `fleet_configs_parse` test (skipped when unset) |

Gotcha: avalanchego's subprocess runtime (and therefore the harness, which uses it) forwards only `GRPC_*` and `GODEBUG*` variables to the plugin, plus `AVALANCHE_VM_RUNTIME_ENGINE_ADDR`. So under a host the `EPOCHDB_*` variables are NOT seen by the plugin; they work for `storecheck`, `epochdb-rpc-serve` and the bench. Carrying S3 / cache settings through the config bytes is open (below).

## Oracles

`S` below is the scratch dir with the dumps (`step-containers-1-50000.bin`, `-1-1000000.bin`, `beam-containers-1-1000000.bin`, each with `chain.json` and `upgrade.json` beside it) and the stock plugin `subnet-evm-linux-amd64-v1.14.2` saved as `plugins/srEXiWaHuhNyGwPUi444Tu47ZEDwxTWrbQiuD7FmgSAQ6X7Dy`.

1. Step 50k under the harness (root-checked every block, rolls, `check ... match=true` twice):
   `go run ./cmd/epochdb-host-bench --dump $S/rs/step/step-containers-1-50000.bin --vm rs/target/release/epochdb-rs --data D --http 127.0.0.1:19951 --batch 256 --config '{"state-sync-enabled":false,"roll-budget-mb":8}'`
   then `rs/target/release/storecheck verify --dump ... --genesis chain.json --upgrade upgrade.json --data D/chainData/store --inner --postings-every 5` (`--inner`: the host hands the plugin the unwrapped block, so `pvm/` rows are empty).
2. beam 50k: the same with `--dump $S/rs/beam/beam-containers-1-1000000.bin --to 50000` (precompiles at genesis, activations at 2203 / 44322 / 44349 / 44383); verify with `--to 50000`.
3. Crash: run 1, `kill -9` the plugin on a log pattern (`roll 2 done`, or `roll N start` with `tail -F -s 0.005` to land inside a roll), rerun with the same `--data`: the `recovered:` line, `root ok`, the run continues, the sealed run's casfs name equals the fresh run's.
4. RPC differential: stock under the harness with `--serve` and `{"state-sync-enabled":false,"pruning-enabled":false,"eth-apis":[eth, eth-filter, net, web3, internal-*, debug, debug-tracer, debug-file-tracer, debug-handler]}` (the default config prunes state and serves no debug_), epochdb-rs under the harness with `--serve`, then `python3 rpccmp2.py http://stock/ext/bc/<id>/rpc http://rs/ext/bc/<id>/rpc --scan 1500` (Step) / `--scan 3000` (beam). Expected 869 of 875 and 914 of 920, differences listed below.
5. `/ws`: both plugins with `--serve --feed-delay 500ms --to 50500` from a 50k data dir; `python3 rs/rpc/scripts/ws_e2e.py capture ws://.../ext/bc/<id>/ws 150 OUT.json` on each, `ws_e2e.py diff A.json B.json` (0 differences after masking subscription ids), `ws_e2e.py static URL`, `ws_e2e.py latency CAP.json harness.log`.
6. Bench: `epochdb-rs --dump $S/rs/step/step-containers-1-1000000.bin --genesis ... --upgrade ... --data D --duration 120 --roll-budget 256 --workers 14`; compare cum mgas/s and rss with `node/REPORT.md`.
7. Unit tests: `cargo test --workspace --release`; state's `tests/latest.rs` and `tests/commit.rs` are the format oracles; exec's tests cover the fee rules, the precompiles and the tracer shapes; store's the reader hammer, merge crash points and cache watermarks; rpc's the fee window against Go vectors (`go run ./exp/feecheck synth`) and the ws session.
8. Store on S3: `docker run minio/minio` with random keys, `EPOCHDB_S3_*` set, `storecheck publish` then `storecheck join` into an empty dir and `storecheck readall`.
9. Go side: `go vet ./...`, `gofmt -l` on the new Go dirs (`cmd/epochdb-host-bench`, `cmd/epochdb-vm-bench`, `cmd/epochdb-dump-fetch`, `exp/feecheck`, `exp/rpcoracle`); `exp/rpcoracle` is the read-only Go node for the ots_/edb_ differential (`otscmp.py`).

Results on branch `rust` (2026-09-09 JST, local i7-10700K, load 13-19 from a parallel 1M run): Step 50k 822 blk/s (1,200-1,280 unloaded), rolls at 17,546 / 31,462 / 45,339 / 50,000, root-checked 50,000, both checks `match=true`, verify PASS (1,024,939 state rows, 104,006 posting checks); beam 50k 993 blk/s, root-checked 50,000, verify PASS (273,547 state rows, 231 code blobs); crash at 31,837 recovered in 404 ms and finished with the same sealed run `4b688741...`; differential 869/875 and 914/920 through the plugin's `/rpc`; `/ws` 192 of 192 notifications byte-equal (149 newHeads + 43 logs), latency median 0.58 ms from Accept start (stock 1.69); bench 120 s: 798,690 blocks, cum 1,534 mgas/s, peak RSS 1,282 MB (rs-node's 180 s figure: 1,348 mgas/s, 1,338 MB; the cum falls past 700k where the chain turns contract-heavy).

## Remaining RPC differences vs stock subnet-evm v1.14.2 (by construction)

`web3_clientVersion` (`epochdb/v0.1.0` vs `v1.14.2`); `debug_traceBlockByNumber` with an unknown tracer name (stock runs it as a JS tracer and answers a per-tx `ReferenceError`, rs -32602); `eth_getProof` (no tries here); `personal_listAccounts` (stock `[]`, rs -32601); `eth_newFilter` / `eth_newBlockFilter` ids (random). Message-only differences with the same code: parse and not-found texts carry the detail, `eth_sendRawTransaction` (no mempool), `eth_estimateGas` fee-cap wording.

## Deviations (consolidated from the REPORTs)

From the Go node / the Go formats:

- Run file format v2 (rs/state): not the Go `latest` run format; a v1 file fails to open with `run format version 1, this build reads version 2`. Overlay / Dirty / roll produce the same roots.
- Store artifacts are not byte-compatible with the Go store (ruling: the Go format is the spec for WHAT is stored). Sections are still Pebblev2 sstables; `cid/` holds the real container id (sha256 of the unsigned proposervm bytes); ~14 percent more state rows than the Go capture (a post-image row for every touched account); L0 runs are zstd-1 and local, terminals zstd-9 and published; the merge does not fadvise its inputs; S3 single PUT only.
- Recovery replays the store's `ws/` rows (contract-key form, sorted per commit) block by block; empty write sets are still root-compared; shutdown waits for a roll in flight (Go abandons it).
- Code lives in an in-memory table by hash loaded from `code/` rows at open.
- The executor checks gasUsed, receiptsRoot and logsBloom against the header on every block (vmexec does not); BLOCKHASH from a 256-entry map; deployer-allow-list refusal is checked before revm's depth / balance checks (a refused create at depth 1024 consumes all gas; never seen).
- Predicates are taken from `header.Extra` when no ValidatorState is given.
- Fees follow stock, not the Go node (stock's gasprice oracle, feeHistory without the trailing projection); past-the-head heights answer stock's -32000; eth_getLogs has no range cap; default call gas is the 50M cap; `debug_getRawReceipts` is built from stored rows.
- `debug_printBlock` is a fixed field text, not spew; flatCallTracer and JS tracers are not served.

From avalanchego's `vm_server.go`:

- No `grpc.health.v1` service, `Gather` returns no metrics, `NewHTTPHandler` answers none, Initialize does not validate the BLS key / node id / upgrades, plain errors are `Unknown`, logging is stderr lines.
- `/ws`: after a non-JSON message we write the error and a close frame (stock drops the TCP connection); `newPendingTransactions` never fires (no mempool); origins unchecked, no compression, replies in request order; ping / pong deadline enforced from our side only.

## Open items (deduplicated)

- Run under a real avalanchego (consensus Reject, siblings, a validator set) and on the Tokyo box; metrics in `Gather`.
- `EPOCHDB_*` settings do not reach the plugin under a host (env filter above): carry S3 / cache / terminal settings in the config bytes.
- Throughput under rpcchainvm is round-trip bound (~1,100-1,280 blk/s on Step: Verify + Accept per block); the executor is busy 10-15 percent of the time.
- Executor: the contract-heavy tail (600-750 mgas/s single-threaded) is unprofiled; trace JSON rendering could move to the checker; Dirty root at 100k-400k is 60 percent of the checker.
- Store: zero-copy reads (decoded-block LRU, restart-point cursor), the read-only cohabitation lock, index artifacts beside runs, `MergeIter` heap for a fan-in above 16, RAM ring of 16 chunks in front of the chunk cache.
- RPC: flatCallTracer and JS tracers; eth_createAccessList from the prestate (equal on the probes, untested on calls touching other contracts); ERC-721 / ERC-1155 paths in `tokens.rs` need a corpus with 4-topic Transfer / TransferSingle logs; a single-tx re-executing trace re-executes the whole block; descending postings are the ascending scan reversed; the Go node's `edb_getTopicGroups` / `edb_getTokenContracts` answer `null` for empty (reported, not changed).
- `/ws`: reorgs (`removed: true`) unimplemented (the follower never reorgs); subscription ids seeded from pid + counter + clock, not `crypto/rand`; `epochdb-rpc-serve` handles an upgrade only on a connection's first request.
- exec: FeeManager Durango events and `setManager`, Granite / P256Verify, beacon root are hard errors (Step and beam 1..1M never reach them); `WarpSet::flatten` costs about a second per 1,000 validators, cached per (height, subnet).
- block: no low-s check in recovery; no per-record checksum in the dump.

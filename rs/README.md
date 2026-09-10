# epochdb-rs

The Rust rewrite of the epochdb node: one cargo workspace under `rs/` with the executor, two state engines (the flat native one and Firewood), the store and the JSON-RPC / WebSocket surface, and one engine crate (`rs/chain`) behind two shells: `epochdb-rs`, the Rust follower plugin (avalanchego rpcchainvm protocol 45, in-process), and `epochdb-validator`, the Go validator shell (`validator/`, txpool + gossip + BuildBlock) over the engine's C ABI (`rs/ffi`). Branch `rust` is every `rs-*` branch merged (`rs-ffi`, `go-validator` and `rust-firewood` last). Each crate keeps its own `REPORT.md` with the design, the oracle numbers and the Go source it was ported from; this file is the map.

## Crate map

| crate (package) | lib / bins | what it is | report |
|---|---|---|---|
| `state` (`epochdb-state`) | lib `state`; bins `bench`, `rscompat` | the `latest` state engine: overlay, frozen window, run files (format v2), the Dirty trie and its root, the roll | `state/REPORT.md`, `state/LAYOUT.md` |
| `block` (`epochdb-block`) | lib `block`; bin `blockcheck` | proposervm container unwrap, subnet-evm block / header / tx decode and encode, sender recovery, the container dump reader | `block/REPORT.md` |
| `exec` (`epochdb-exec`) | lib `exec`; bin `epochdb-exec` | subnet-evm block execution on revm: fee rules, the stateful precompile set (allow lists, FeeManager, RewardManager, NativeMinter, warp), state upgrades, forks through Granite, the tracers (callTracer, prestate, struct, 4byte, mux) | `exec/REPORT.md` |
| `node` (`epochdb-node`) | lib `node` | the in-process bench (`epochdb-rs --dump`): dump -> executor -> checker thread (root one block behind) -> roll; `Backend` = overlay + frozen + run; `firewood.rs` = the Firewood state engine (`--state firewood`, `state-engine: firewood`) | `node/REPORT.md`, `FIREWOOD.md` |
| `store` (`epochdb-store`) | lib `store`; bin `storecheck` | storage v4: window log, L0 seal, terminal merge, Pebblev2 sstable sections, Elias-Fano postings, casfs (local spool, S3, chunk cache with eviction), reader snapshots | `store/REPORT.md` |
| `rpc` (`epochdb-rpc`) | lib `rpc`; bin `epochdb-rpc-serve` | eth_ / debug_ / ots_ / edb_ / net_ / web3_ / txpool_ over a `Store` trait, eth_call and estimateGas through the executor, re-executing tracers, filters, the fee oracle, `/ws` with eth_subscribe | `rpc/REPORT.md` |
| `chain` (`epochdb-chain`) | lib `chain`; bin `vbench` | the engine both shells share: `NodeEngine` (executor over `Ex::Native` (Layered + roller + Dirty) or `Ex::Firewood`, checker + DbStore), `Tree` of verified-not-accepted blocks, the state root inside verify in NormalOp (native), `build` (subnet-evm miner semantics + customheader; native only, candidates from the pool), `pool` (the transaction pool: libevm legacypool's admission rules, per-sender nonce maps, incremental tip and price indexes, head moves per touched sender), the RPC store adapter; `vbench` is the validator-shape bench and build oracle | `ffi/ABI.md`, `../cmd/epochdb-validator/E2E.md` (Mempool in the engine) |
| `plugin` (`epochdb-plugin`) | lib `plugin`; bin `epochdb-rs` | the rpcchainvm plugin over `chain`: `vm.proto` server, ghttp `/rpc` and `/ws` (hijack path) | `plugin/REPORT.md` |
| `ffi` (`epochdb-ffi`) | staticlib `libepochdb_engine.a` + `ffi/epochdb_engine.h` | the C ABI over `chain` for the Go validator shell (parse / verify / accept / reject / build / pool_* / account_state / rpc) | `ffi/ABI.md`, `ffi/README.md` |
| `layout` (`epochdb-layout`) | bin `layout` | the run-file layout experiment that chose format v2 | `state/LAYOUT.md` |
| `validator/` + `cmd/epochdb-validator` (Go, outside the workspace) | bin `epochdb-validator`; `cmd/epochdb-validator/e2e`, `/admitload`, `/stub` | the validator shell over `ffi`: tx gossip (subnet-evm wire format, over the engine's pool), BuildBlock timing and `epochdb_build`, `/rpc` forwarded whole; `e2e` is the tmpnet oracle (ours + stock validators on one chain), `admitload` the admission-rate probe, `stub` a canned C engine for the Go tests | `cmd/epochdb-validator/E2E.md` |

Dependency order: `state` <- `node`; `block` <- `exec` <- `node`, `store`, `rpc` <- `chain` <- `plugin`, `ffi` <- `validator` (Go). The protos under `plugin/proto/` are avalanchego `v1.14.3-0.20260804141953-6dc4c3b395b6` verbatim (`RPCChainVMProtocol = 45`), compiled by `plugin/build.rs` (needs `protoc`).

## Build

```
cd rs
cargo build --release                       # every Rust bin into rs/target/release/
cargo test --workspace --release            # the unit and oracle tests
cargo clippy --workspace                    # warnings only
```

The binaries:

| binary | build | what it is |
|---|---|---|
| `epochdb-rs` (`rs/plugin`) | `cargo build --release -p epochdb-plugin` | no arguments: the rpcchainvm follower plugin (`AVALANCHE_VM_RUNTIME_ENGINE_ADDR` set by avalanchego or the hosts); `--dump ...`: the in-process bench (rs/node); `--version` |
| `epochdb-validator` (Go, `cmd/epochdb-validator`) | `cargo build --release -p epochdb-ffi && ffi/localize.sh target/release/libepochdb_engine.a`, then `cd .. && go build ./cmd/epochdb-validator` | the validator plugin: the same engine linked through `libepochdb_engine.a` (cgo, glibc), plus gossip and BuildBlock (the pool is the engine's); `--version`. `go test ./validator/` runs the in-process tests against the real archive, `-tags epochdb_stub` against the canned C engine |
| `epochdb-rpc-serve` (`rs/rpc`) | `cargo build --release -p epochdb-rpc` | the JSON-RPC / `/ws` server over a store dir on its own (no plugin): `--data DIR --genesis chain.json --upgrade upgrade.json --http ADDR` |
| `storecheck` (`rs/store`) | `cargo build --release -p epochdb-store` | store tools: `verify` (rows against a dump), `write`, `merge`, `probe`, `rowsum`, `publish` / `join` / `readall` (casfs, S3) |
| `epochdb-exec` (`rs/exec`) | `cargo build --release -p epochdb-exec` | the executor alone over a dump: re-executes blocks and compares gasUsed / receiptsRoot / traces against an RPC (`--rpc`, `--rpc-cache`) |
| `vbench` (`rs/chain`) | `cargo build --release -p epochdb-chain` | the validator-shape bench and build oracle over the library engine (`--build`, `--window`, `--synthetic`, `--export-inner`) |
| `libepochdb_engine.a` + `ffi/epochdb_engine.h` (`rs/ffi`) | `cargo build --release -p epochdb-ffi`, then `ffi/localize.sh` (mandatory before a Go link; `ffi/README.md`) | the C ABI over `chain`; `ffi/smoke/run.sh <dump> <chain.json> <upgrade.json> <workdir> [n]` is the C smoke test |

### The Firewood option

Every engine consumer takes a state-engine switch: `--state firewood` on the bench (`epochdb-rs --dump ...`; `--fw-cache-mb`, `--fw-kv-cache-mb`, `--fw-parallel`, `--fw-deferred`, `--fw-revisions`), `"state-engine":"firewood"` in the plugin's config bytes (with `firewood-cache-mb`, `firewood-kv-cache-mb`), and the same config bytes through the FFI (`epochdb_open`) for `epochdb-validator`. Under Firewood the state root comes from the checker thread's proposal after accept (no inline root in verify, so `epochdb_build` answers an error: block building needs the native engine); the native default keeps the inline root in NormalOp. Details and numbers: the "Firewood state engine" section below and `FIREWOOD.md`.

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

`cmd/epochdb-host` is the same host with the fetch package (live peers) as the block source: `--chain <blockchainID> --vm rs/target/release/epochdb-rs --node <rpc uris> --data DIR`. `--p2p-port` is optional: without it the host is fetch-only (no inbound listener) and creates the `staker.key/.crt` identity under `--data` itself, so the NodeID is stable either way.

### Under a stock avalanchego

A chain's plugin is the file named by the chain's VM id in avalanchego's plugin dir (`--plugin-dir`, default `~/.avalanchego/plugins`). For a subnet-evm chain the VM id is `srEXiWaHuhNyGwPUi444Tu47ZEDwxTWrbQiuD7FmgSAQ6X7Dy`, so:

```
cp rs/target/x86_64-unknown-linux-musl/release/epochdb-rs ~/.avalanchego/plugins/srEXiWaHuhNyGwPUi444Tu47ZEDwxTWrbQiuD7FmgSAQ6X7Dy
```

and avalanchego launches it for every chain of that VM it tracks (`--track-subnets`). The chain config is `~/.avalanchego/configs/chains/<blockchainID>/config.json`; its bytes are what the plugin receives as `config_bytes`. The plugin's data goes under avalanchego's chain data dir (`<data-dir>/chainData/<blockchainID>/`): `vmstate/` (run + trie + MANIFEST of the state engine) and `store/` (the epochdb store). The plugin is a follower: BuildBlock is refused, state sync answers "not implemented", `/rpc` and `/ws` are mounted at `/ext/bc/<blockchainID>/`. Not yet done under a real avalanchego (only the harness, which is the same client code); the difference under a real node is consensus calling Reject and verifying siblings, which `plugin::tree`'s test covers.

### Configuration

Config bytes (the chain config JSON: `~/.avalanchego/configs/chains/<blockchainID>/config.json` under a stock avalanchego, `--config` under the hosts). One table, defined once in `rs/plugin/src/config.rs`:

| key | environment fallback | meaning |
|---|---|---|
| `state-sync-enabled` | | ignored by the plugin (it never state-syncs); the harness passes `false` so stock subnet-evm executes every block under the same config |
| `roll-budget-mb` | | overlay + Dirty bytes before a roll while bootstrapping (default 2048 = `SyncRoll`); the tip budget after SetState(NormalOp) is `min(roll-budget-mb, 128)` |
| `block-size-target-kib` (default 1800: the miner's cut on tx bytes per built block; a 16k-transfer block is ~1.1 MB zstd on the wire against avalanchego's 2 MiB message limit, BUT avalanchego checks the RAW container against the 2 MiB message limit before compressing and it is a constant, not a flag, so the engine clamps the key to 1900) and `roll-every-blocks`, `roll-every-secs` | | the second roll trigger, in both states: a roll is also due once this many blocks (default 500,000) or seconds (default 3600) passed since the last one, whichever first; `0` turns one off. Bounds the crash replay: on beam the byte budget never fired in 9.5M blocks and every restart replayed the whole store (90 s) |
| `s3-endpoint`, `s3-bucket`, `s3-access-key`, `s3-secret-key` | `EPOCHDB_S3_ENDPOINT`, `EPOCHDB_S3_BUCKET`, `EPOCHDB_S3_ACCESS_KEY`, `EPOCHDB_S3_SECRET_KEY` | the casfs remote (SigV4 path style); all four required together, static keys only, no default credential chain. No endpoint = local only |
| `s3-prefix`, `s3-region` | `EPOCHDB_S3_PREFIX`, `EPOCHDB_S3_REGION` | key prefix (default none) and region (default `auto`) |
| `cache-dir` | `EPOCHDB_CACHE_DIR` | chunk cache root (default `<store>/cache`) |
| `cache-min-free` | `EPOCHDB_CACHE_MIN_FREE` | admission floor in bytes of free space on the cache filesystem (default 5 percent of it); the eviction target is twice it |
| `cache-max-age` | `EPOCHDB_CACHE_MAX_AGE` | cache window age limit, seconds or `Nh`/`Nm`/`Ns` (default 30 days) |
| `terminal-txs` | `EPOCHDB_TERMINAL_TXS` | TxNum slots per terminal run (default 8,000,000); lowered for the merge oracle |
| `window-max-bytes` | `EPOCHDB_WINDOW_MAX_BYTES` | the third window flush trigger beside 500,000 slots and 50,000 blocks: the window is cut into an L0 run once its log holds this many raw bytes (default 128 MiB; was 1 GiB), checked at block boundaries. Bounds the seal a shutdown may abandon, the re-seal at the next open (the contract-heavy Step tail is 13 GB per 50,000 blocks) and the memtable's resident memory: the state index costs ~1.3x the log (it was 3.6x with a HashMap of Vecs; a 410 MB window put a validator at 2.0-2.6 GB RSS) |
| `shutdown-grace-secs` | | how long Shutdown waits for a seal or terminal merge in flight (default 10). The window log is fsynced first; past the grace the seal / merge is abandoned and the next open re-seals the frozen log / re-merges (the same recovery as a crash). `0` = never wait |
| `new-chain` | `EPOCHDB_NEW_CHAIN=1` | let `join` start a chain that has no `latest-<chainroot>` pointer on the remote |

The rule is mechanical: strip `EPOCHDB_`, lower case, `_` -> `-`, so `EPOCHDB_ROLL_EVERY_BLOCKS=200000` on a `cmd/epochdb-host` container reaches the plugin as `roll-every-blocks`. Values may be JSON strings, numbers or booleans (`true` = `1`); the numeric keys (`roll-*`) accept a number or a string holding one (the host merges variables in as strings). Unknown keys are ignored, so a stock subnet-evm config works as is. The plugin sets the listed variables into its own environment from the config bytes before it opens the store, so `rs/store` keeps one env-reading code path; a key in the JSON wins over an inherited variable, an absent key leaves the variable alone. The bytes are never logged whole: the startup line prints them with `s3-access-key` / `s3-secret-key` redacted.

Why: avalanchego's subprocess runtime (and therefore both hosts, which use it) forwards only `GRPC_*` and `GODEBUG*` variables to the plugin, plus `AVALANCHE_VM_RUNTIME_ENGINE_ADDR`. The environment column is what `storecheck`, `epochdb-rpc-serve` and the bench read directly.

`cmd/epochdb-host` closes the loop for a container: `--config` (inline JSON or `@file`, default `{"state-sync-enabled":false}`) plus every `EPOCHDB_*` variable of its own environment merged in under the same rule, so a compose file keeps `EPOCHDB_*` on the container and the host hands them to the plugin (`ops/compose.rust.example.yml`). The Go plugin under the same host is unaffected: it ignores the extra keys.

Not configurable yet (no knob in rs): block cache size (the store has no decoded-block cache, see Open items), trace mode (traces are always rendered and stored).

Other environment variables (tools and tests only): `EPOCHDB_RPC_NOW=<unix s>` pins the RPC's wall clock (eth_gasPrice / maxPriorityFeePerGas) for a deterministic differential; `EPOCHDB_V1_CONFIGS` is the dir of fleet v1 chain configs for `exec`'s `fleet_configs_parse` test (skipped when unset).

### The image

The repo `Dockerfile` has a `rust` stage (`rust:1.97-trixie`, `musl-tools` + `protobuf-compiler`, target `x86_64-unknown-linux-musl`, cargo registry and target dir as BuildKit cache mounts like the Go stage) that builds `epochdb-rs` static, builds the glibc `libepochdb_engine.a` and localizes it (`rs/ffi/localize.sh`) for the Go stage, which links it into `/usr/local/bin/epochdb-validator` beside the other Go binaries (glibc, cgo: the runtime image is `distroless/cc-debian13`), and copies `epochdb-rs` into the runtime image twice: `/usr/local/bin/epochdb-rs` beside the Go binaries (what `epochdb-host --vm` points at) and `/plugins/srEXiWaHuhNyGwPUi444Tu47ZEDwxTWrbQiuD7FmgSAQ6X7Dy` (the stock subnet-evm VM id, for `docker cp` into a stock avalanchego's plugin dir or a bind mount of `/plugins`; a chain created under a vanity VM id wants the same file under that id, `platform.getBlockchains` says which). The entrypoint is still the Go `epochdb`. `.github/workflows/build.yml` builds the same Dockerfile on every push to `main` (unchanged: the new stage rides in the same `docker/build-push-action` step; its GHA layer cache does not hold BuildKit cache mounts, so the Rust stage is cold there unless `rs/` is untouched: 2m43s cold on a 16-thread i7-10700K, so expect 6-10 min of the docker job on a 4 vCPU runner, in parallel with the Go stage); `rs/target` is in `.dockerignore` so a local build tree never enters the context. Image 774 MB (612 before `epochdb-validator` and its glibc engine archive; 4m25s locally with warm cache mounts), of which `epochdb-rs` is one 78 MB layer (both paths are hard links of one file; debug info kept for backtrace line numbers).

```
docker build -t epochdb:rust-ops .
docker run --rm --entrypoint epochdb-rs epochdb:rust-ops --version    # epochdb-rs/0.1.0 [rpcchainvm=45]
docker run --rm --entrypoint epochdb-validator epochdb:rust-ops --version    # epochdb-validator/0.1 [rpcchainvm=45]
```

`ops/compose.rust.example.yml` is one chain as `epochdb-host` + `epochdb-rs` with `EPOCHDB_*` on the container, and the equivalent stock-avalanchego layout (plugin dir + chain config file) in its comments.

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

## Memory and rolls (branch `rust-harden`, after the beam e2e in `E2E.md`)

The live beam sync left three findings; all three are measured on Step 1M under the harness (`--config '{"state-sync-enabled":false,"roll-every-blocks":200000,"terminal-txs":400000}'`, so five rolls and ten terminal merges in one run; RssAnon of the plugin sampled from `/proc` every 10 s).

1. The roll never fired on beam (9.5M blocks under the 2 GB budget, every restart replayed the whole store, 35M rows in 90 s). `roll-every-blocks` / `roll-every-secs` (above) is the second trigger: rolls at exactly 200,000 / 400,000 / 600,000 / 800,000 / 1,000,000, every rolled root equal to the verified one; `kill -9` at 472,362 and a restart on the same data: `recovered: rolled at 400000 (gen 2), head 472361 ..., rows replayed 1012248, ..., root ok, in 2203 ms`.
2. The anon heap stepped up at every L0 seal: `write_sections` read the whole window's chain rows (containers, receipts, traces) into one Vec before writing the sstable, so a seal's transient memory was the window's raw size (13 GB for Step 950,001..1,000,000, whose blocks are contract heavy: the plugin hit 12 GB RssAnon at the final seal). The seal now streams every section row by row (`RunWriter::section_with`); nothing is buffered beyond the sstable's open block.
3. What glibc's malloc kept of those transients: the plugin's global allocator is jemalloc (`tikv-jemallocator`, builds static-musl), and it prints `epochdb-rs: heap allocated=.. active=.. resident=.. retained=.. rss-anon=..` every 60 s so live memory (allocated) and the allocator's holdings (resident) can be told apart.

| Step 1M under the harness | RssAnon at 1M | peak sampled RssAnon | at shutdown |
|---|---|---|---|
| before (whole-window Vec, glibc) | 6,500 MB | 7,393 MB | 12,033 MB (the final 13 GB window's seal) |
| streaming seal, glibc | 2,540 MB | 2,670 MB | 2,530 MB |
| streaming seal + jemalloc | 1,300 to 1,600 MB | 1,597 MB | 1,300 MB |

jemalloc's own line at the end of the run: `allocated=1706MB resident=1885MB rss-anon=1595MB`, so what remains is live: the store's memtable of the current window (its state rows and indexes; the contract-heavy tail's are large), the executor and the code table, the 8k parsed blocks, the overlay and Dirty between rolls (180 MB with a roll every 200k blocks). Throughput: 874.7 blk/s before, 833.3 with the streaming seal (one run each, the tail runs at 85 blk/s and 750 mgas/s on the executor thread); the jemalloc run was split by the crash test, so no whole-dump figure.

Shutdown (branch `rust-shutdown`): the harness's Shutdown deadline is 30 s (a stock avalanchego has one too) and `DB::close` used to wait for the seal and the merge in flight; on the 13 GB tail window the seal alone took ~3 min, so the stopper killed the plugin at exit (`subprocess was killed`, every 1M run). Two changes: `window-max-bytes` (above, default 1 GiB) cuts the window by raw bytes, so no seal is ever more than ~1 GiB of log, and `shutdown-grace-secs` (default 10) bounds `DB::close`: the window log is fsynced, a seal or merge still running past the grace is abandoned (`store: close: abandoning the seal still running after 10s`) and the next open re-seals the frozen log, the same path a crash takes. Proof on Step 50k with `shutdown-grace-secs: 0` (the seal of blocks 1..50000 is always in flight at exit): Shutdown took 269 ms, a `run-l0-...tmp` and `window.frozen.log` were left, the restart re-sealed it at open into the same run `4b688741...` in 2.1 s, `root ok`, `check ... match=true`. Step 1M under the harness with the defaults (same `--config` as the table above): 41 L0 seals instead of 19, the tail cut by bytes every 2,500 to 17,000 blocks (`[blocks 993547..996399] in 0.8s`; the last window, 3,601 blocks, was 885 MB of log at exit), the longest seal 2.3 s, the largest terminal merge 11 L0 runs in 30.9 s, `Shutdown took 4.867s` (the final roll, 4.7 s; no seal was in flight and nothing was killed), `root-checked=1000000`, both `check ... match=true`; 1118 s and 894.3 blk/s for the whole dump against 1143 s / 874.7 (before) and 1200 s / 833.3 (streaming seal).

## Firewood state engine (branch `rust-firewood`)

An alternative state engine behind the executor: ava-labs Firewood v0.8.0 in `ethhash` mode (`rs/node/src/firewood.rs`; a git dependency pinned to the `v0.8.0` tag, crates.io stops at 0.3.1). Firewood holds the whole state as one trie in one file (`vmstate/firewood/firewood.db`), keyed the way subnet-evm's `triedb/firewood` keys it: account = keccak(addr) (32 B) with the value RLP[nonce, balance, storageRoot, codeHash] (Firewood splices the real storage root into field 2 when it hashes, so the empty root is written), slot = keccak(addr) ++ keccak(slot) (64 B) with the value RLP(left-trimmed word), a zero slot is a key delete and an account delete is a prefix delete of the 32 B key (storage goes with it). `propose` of a block's batch IS the state root (Firewood hashes then); `commit` makes the proposal a revision. Firewood stores no code: the code table is ours, as with `Backend`.

Shape: the executor writes a block into a `Layer` (a map in our contract key form, so `take_ws` hands out the same ordered write set as `Backend`, and the store, the checker history and recovery are unchanged); reads go cur -> the pending chain (verified, not accepted) -> the accepted layers (accepted, not yet proposed) -> the newest proposal's view (Firewood answers reads from a proposal, so the executor never waits for a commit). The checker turns the layer into Firewood ops (wipes first, then the final value per key), proposes on top of the previous proposal (a proposal chain), compares the proposal root with the header (a mismatch exits), and commits. An optional key-value read cache (`--fw-kv-cache-mb`, `firewood-kv-cache-mb`) in front of the trie walk keeps the latest accepted value per key; a wiped account bumps an epoch that orphans its cached slots.

- Bench: `epochdb-rs --dump ... --state firewood [--fw-cache-mb 192] [--fw-revisions 128] [--fw-deferred 1] [--fw-parallel auto|never|always] [--fw-kv-cache-mb 0] [--root-inline]`. Pipelined = the executor runs block N+1 while the checker proposes and commits N; `--root-inline` = the executor waits for the proposal root before the next block. `--roll-budget` is ignored (no roll). The checker split reads `propose=` / `commit=`; the bench line carries `anon=` (RssAnon).
- Plugin: config `state-engine: "firewood"` (default `"native"`), `firewood-cache-mb` (node cache, default 192), `firewood-kv-cache-mb` (default 0). Verify executes on the pending chain as before (a rejected block is a dropped layer, never proposed); accept hands the layer to the checker, which proposes and root-checks it and commits the chain every 32 blocks right after the store's fsync, so Firewood's persisted revision never runs ahead of the store. Recovery: the persisted root's height is found among the store's headers (walking back from the head, bounded to 100,000), the write sets since are replayed through proposals, the rebuilt root must equal the head's. No `run.N` / `trie.N` / MANIFEST; Firewood's node store, free list and revisions replace them.
- Firewood settings used: `NodeHashAlgorithm::Ethereum`, `max_revisions` 128 (in memory), `node_cache_memory_limit` 192 MB default (4 GB in the A/B), `deferred_persistence_commit_count` 1, `use_parallel` `BatchSize(8)` (Firewood's default), `root_store` off.

Numbers, the oracle commands and the profile are in `FIREWOOD.md`.

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
- Throughput under rpcchainvm is round-trip bound (~1,100-1,280 blk/s on Step: Verify + Accept per block); the executor is busy 10-15 percent of the time.
- Executor: the contract-heavy tail (600-750 mgas/s single-threaded) is unprofiled; trace JSON rendering could move to the checker; Dirty root at 100k-400k is 60 percent of the checker.
- Store: zero-copy reads (decoded-block LRU, restart-point cursor), the read-only cohabitation lock, index artifacts beside runs, `MergeIter` heap for a fan-in above 16, RAM ring of 16 chunks in front of the chunk cache.
- RPC: flatCallTracer and JS tracers; eth_createAccessList from the prestate (equal on the probes, untested on calls touching other contracts); ERC-721 / ERC-1155 paths in `tokens.rs` need a corpus with 4-topic Transfer / TransferSingle logs; a single-tx re-executing trace re-executes the whole block; descending postings are the ascending scan reversed; the Go node's `edb_getTopicGroups` / `edb_getTokenContracts` answer `null` for empty (reported, not changed).
- `/ws`: reorgs (`removed: true`) unimplemented (the follower never reorgs); subscription ids seeded from pid + counter + clock, not `crypto/rand`; `epochdb-rpc-serve` handles an upgrade only on a connection's first request.
- exec: FeeManager Durango events and `setManager`, Granite / P256Verify, beacon root are hard errors (Step and beam 1..1M never reach them); `WarpSet::flatten` costs about a second per 1,000 validators, cached per (height, subnet).
- block: no low-s check in recovery; no per-record checksum in the dump.

# epochdb-ffi

The C ABI of the epochdb engine: `libepochdb_engine.a` + `epochdb_engine.h`, for the Go validator shell
(cgo). `ABI.md` is the contract; the header is generated from `src/lib.rs`.

## Build

```
cd rs
cargo build --release -p epochdb-ffi                                   # target/release/libepochdb_engine.a (glibc)
cargo build --release --target x86_64-unknown-linux-musl -p epochdb-ffi  # target/x86_64-unknown-linux-musl/release/libepochdb_engine.a (static)
cbindgen --config ffi/cbindgen.toml --crate epochdb-ffi --output ffi/epochdb_engine.h ffi   # after an ABI change (cargo install cbindgen)
```

The archive is ~340 MB with the workspace profile (debug info, fat LTO); `strip --strip-debug` if size
matters. `--no-default-features` drops jemalloc (the system allocator; valgrind can then see the engine's
heap).

## Linking

Run `rs/ffi/localize.sh <archive>` after every build of the archive: it merges the archive into one
relocatable object and makes every defined symbol but `epochdb_*` local (blst, secp256k1, zstd, ring,
jemalloc's `_rjem_*`, compiler-builtins and the Rust runtime included), so a host that carries its own
copies (avalanchego's `bls` package = another blst; libevm) links without `--allow-multiple-definition`.
`rs/ffi/golink` is the proof: `go build -tags epochdb_ffi_link ./rs/ffi/golink` links avalanchego's BLS
signer and the engine into one binary.

A Go binary that links this archive cannot also link `subnet-evm/core` (it pulls in Firewood's Rust
staticlib; two Rust runtimes collide on `rust_eh_personality` and the allocator shims, which the host
cannot localize). The Go shell uses libevm's `core/txpool` for that reason.

cgo, glibc:

```go
// #cgo CFLAGS: -I${SRCDIR}/../../rs/ffi
// #cgo LDFLAGS: ${SRCDIR}/../../rs/target/release/libepochdb_engine.a -lpthread -ldl -lm
// #include "epochdb_engine.h"
import "C"
```

Static (musl archive, static Go binary): `-extldflags '-static'` with the musl `.a`, or link the glibc
archive and ship the glibc binary. A plain C consumer: `cc smoke.c libepochdb_engine.a -lpthread -ldl -lm`.

## Threading

Every function may be called from any thread. Verify, accept, reject and build serialize on the engine's
execution mutex (one block at a time, as avalanchego's ctx.Lock does anyway). `epochdb_account_state`,
`epochdb_head_header`, `epochdb_get_block`, `epochdb_rpc` and `epochdb_health` take their own snapshots
and may run concurrently with a verify. `epochdb_open` and `epochdb_close` are not concurrent with anything
on the same engine. No callbacks into the caller; the engine's own threads (checker, flusher, rolls, the
store's seal and merge) are internal.

## Ownership

- Every input pointer is borrowed for the call only; the engine copies what it keeps.
- Every output is an `epochdb_buf` (Rust heap): free each exactly once with `epochdb_buf_free`, including
  `epochdb_build_out.block_bytes` and `.skipped`, and `epochdb_open`'s error. A `{NULL, 0}` buffer is a
  valid empty result and a valid argument to `epochdb_buf_free`.
- Fixed-size outputs (`epochdb_block_meta`, `epochdb_verify_out`, `id[32]`, `height`) are caller-owned
  memory the engine writes into.
- Panics never unwind into C: each entry point is wrapped, the call returns `EPOCHDB_EPANIC` and
  `epochdb_last_error` carries the message; close the engine after one.

## Smoke test

`smoke/smoke.c` + `smoke/run.sh <dump> <chain.json> <upgrade.json> <workdir> [n] [valgrind]`: opens on the
Step genesis, parses + verifies + accepts the first n dump blocks, builds block n+1 from its own txs and
requires the bytes and hash to equal the real block, reads the alloc account, calls eth_blockNumber,
closes, reopens, checks last_accepted. `valgrind` runs it under `--leak-check=full` (use the
`--no-default-features` archive for a meaningful heap view).

## Benchmarks and the build oracle

`rs/chain`'s `vbench` drives the engine the way the shell does (NormalOp, root inside verify):

```
vbench --dump step-containers-1-50000.bin --chain chain.json --upgrade upgrade.json --data D --build --quiet
vbench --synthetic 1000 --data D
vbench --window --dump beam-durango-1901030-1906029.bin --chain chain.json --upgrade upgrade.json --from 1901030 --rpc URL --rpc-cache F
```

`--build` rebuilds every block from its own tx list on its real parent and requires byte-identical bytes
and hash; `--window` does the same on a later dump window over an archive node's state with the root
copied from the real header (no trie without local state).

## Results (2026-09-09 JST, i7-10700K, other agents' runs in parallel, load 2-5)

Latency per block, the archive fully on (traces rendered and stored on the checker thread after accept):

| run | blocks | verify (execute + inline root) mean / p50 / p99 / max ms | build mean / p50 / p99 / max ms | build oracle |
|---|---|---|---|---|
| Step 1..50,000 (`vbench --build`) | 50,000, 343,731 txs | 0.135 / 0.091 / 2.462 / 12.4 | 0.174 / 0.126 / 2.577 / 15.7 | 50,000 byte-identical |
| beam 1..50,000 (`vbench --build`) | 50,000, 51,683 txs | 0.057 / 0.035 / 0.227 / 3.5 | 0.080 / 0.054 / 0.272 / 3.7 | 50,000 byte-identical |
| synthetic 1,000 transfers, Granite header | 1 | 5.88 | 8.17 | included 1,000 |
| beam Durango window 1901031..1902030 (`--window`, RPC state) | 1,000 | (execution only) | 0.147 p50 (cache hits; network misses up to 9.6 s) | 1,000 identical, root copied |
| beam Granite window 6970225..6970324 (`--window`) | 100 | | 0.010 p50 | 100 identical, root copied |
| beam Etna window 4029217..4029330 (`--window`) | 114 | | 0.009 p50 | 112 identical, root copied; 4029316 and 4029320 carry a warp predicate (popped without a validator state, executed as is) |

Step's inline root is 30 percent of verify (4.59 of 15.39 s over 50,000 blocks); the callTracer render
left on the execution thread was 0.15 s (1 percent), so nothing more to move. Verifying the same 50,000
Step blocks under `cmd/epochdb-host-bench` with the unchanged plugin: root-checked 50,000, both `check`
lines `match=true`.

Smoke (`smoke/run.sh`, 1,000 Step blocks then block 1,001 built): verify mean 0.27 ms, the built block
byte-identical, reopen at 1,001. `valgrind --leak-check=full` on the 200-block run: 0 errors, 0 bytes
definitely lost with both allocators (jemalloc: 2.4 KB possibly lost in 8 thread-local blocks; the
system allocator build: 3.8 KB possibly lost, 130 KB still reachable in statics).

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

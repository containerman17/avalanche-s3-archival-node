# THE RUST NODE. epochdb-rs (rs/plugin) is one static musl binary: it is the
# rpcchainvm plugin epochdb-host drives, and it is what a stock avalanchego
# launches from its plugin dir, so it must not depend on the image's libc.
# musl-tools gives cc-rs the musl-gcc the C crates (zstd-sys, ring) build
# with; protoc compiles rs/plugin/proto at build time (plugin/build.rs) and
# libprotobuf-dev holds the google/protobuf/*.proto those import.
# The same stage builds the engine's C ABI (rs/ffi, glibc target: the Go
# validator links it with cgo in the Go stage below, and the runtime image is
# glibc) and localizes it (rs/ffi/localize.sh: every symbol but epochdb_*
# hidden, so avalanchego's blst and libevm link beside it); the archive goes
# to /engine outside the target cache mount so the Go stage can COPY it.
# The registry and target caches are BuildKit cache mounts like the Go
# stage's: a local rebuild recompiles the changed crates only; CI is cold.
FROM rust:1.97-trixie AS rust
ARG SUBNET_EVM_VMID=srEXiWaHuhNyGwPUi444Tu47ZEDwxTWrbQiuD7FmgSAQ6X7Dy
RUN apt-get update && apt-get install -y --no-install-recommends musl-tools protobuf-compiler libprotobuf-dev && \
    rm -rf /var/lib/apt/lists/* && rustup target add x86_64-unknown-linux-musl
WORKDIR /src/rs
COPY rs/ .
RUN --mount=type=cache,target=/usr/local/cargo/registry --mount=type=cache,target=/src/rs/target \
    cargo build --release --target x86_64-unknown-linux-musl -p epochdb-plugin && \
    cargo build --release -p epochdb-ffi && \
    mkdir -p /out/usr/local/bin /out/plugins /engine && cp target/x86_64-unknown-linux-musl/release/epochdb-rs /out/usr/local/bin/ && \
    ln /out/usr/local/bin/epochdb-rs /out/plugins/${SUBNET_EVM_VMID} && \
    cp target/release/libepochdb_engine.a /engine/ && ffi/localize.sh /engine/libepochdb_engine.a

FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
# BuildKit cache mounts keep the module and compile caches on the BUILDER
# across image builds (local iteration drops to the changed packages'
# compile time). CI runners start cold and just repopulate them; the mounts
# never reach the image layers either way.
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
# The localized engine archive where validator/engine.go's cgo LDFLAGS look
# for it (rs/target is in .dockerignore, so the context never carries one).
COPY --from=rust /engine/libepochdb_engine.a rs/target/release/
# GOFLAGS is a build knob for memory-tight machines (e.g. -p=4); empty in CI.
ARG GOFLAGS=
# cgo is mandatory: firewood ships a prebuilt libfirewood_ffi.a linked against
# glibc, so the runtime image must be glibc-based too, not distroless/static.
# distroless/cc-debian13 matches the builder's Debian 13 and carries the
# libgcc_s.so.1 the binary needs; distroless/base does not ship it.
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=1 go build -o /epochdb ./cmd/epochdb && \
    CGO_ENABLED=1 go build -o /out/ ./cmd/epochdb-vm ./cmd/epochdb-host ./cmd/epochdb-vm-plugin ./cmd/epochdb-archive-serve ./cmd/epochdb-validator

# THE PINNED zstd CLI LAYER IS GONE, as DESIGN said it would be: it existed for
# the sealer's per-epoch dictionary training, and storage v0 deleted both the
# dictionaries and the `state` package the version was read out of. Nothing in
# this binary shells out to anything any more; block compression is the pebble
# library's, pinned by module version.

FROM gcr.io/distroless/cc-debian13
LABEL org.opencontainers.image.source=https://github.com/containerman17/avalanche-s3-archival-node
COPY --from=build /epochdb /usr/local/bin/epochdb
# The rework's binaries ride beside the old one; `epochdb` stays the entrypoint.
# epochdb-validator is the Go validator shell over the Rust engine (cgo, glibc):
# install it in a stock avalanchego's plugin dir under the chain's VM id.
COPY --from=build /out/ /usr/local/bin/
# The plugin-dir copy for a stock avalanchego (`--plugin-dir /plugins`, or mount
# /plugins over its ~/.avalanchego/plugins). A chain's plugin is the file named
# by its VM id; this is the stock subnet-evm id, which is what almost every L1
# runs under. A chain created with a vanity VM id (FIFA does this with a stock
# binary) needs the same file under that id: copy or hard-link
# /usr/local/bin/epochdb-rs to /plugins/<that id> in the operator's setup.
# One COPY of both paths keeps the hard link, so the 78 MB binary is one layer.
COPY --from=rust /out/ /
# GO RETURNS FREED HEAP PAGES LAZILY BY DEFAULT (MADV_FREE): they stay RESIDENT
# until the kernel reclaims them, so the arena ratchets to its high-water mark
# and never gives the ground back. This node's speed comes from the page cache,
# so that ground is exactly what it cannot spare. Measured on mainnet C: the
# arena held 18.4GB RSS against a live heap oscillating 12-13GB, and switching
# to MADV_DONTNEED moved container anon 35.34 -> 21.83GB, page cache
# 20.24 -> 24.45GB, and mgas 229.6 -> 302.7.
#
# It has to be an env var: `//go:debug madvdontneed=1` is rejected (not a
# versioned setting) and the runtime reads GODEBUG before main() runs.
#
# WARNING: docker/compose `environment: GODEBUG=...` REPLACES this wholesale
# rather than merging, so any operator value must repeat madvdontneed=1.
# epochdb logs a loud line at startup when it is missing.
ENV GODEBUG=madvdontneed=1
ENTRYPOINT ["/usr/local/bin/epochdb"]

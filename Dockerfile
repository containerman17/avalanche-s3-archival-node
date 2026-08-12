FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# GOFLAGS is a build knob for memory-tight machines (e.g. -p=4); empty in CI.
ARG GOFLAGS=
# cgo is mandatory: firewood ships a prebuilt libfirewood_ffi.a linked against
# glibc, so the runtime image must be glibc-based too, not distroless/static.
# distroless/cc-debian13 matches the builder's Debian 13 and carries the
# libgcc_s.so.1 the binary needs; distroless/base does not ship it.
RUN CGO_ENABLED=1 go build -o /epochdb ./cmd/epochdb

# THE PINNED zstd CLI LAYER IS GONE, as DESIGN said it would be: it existed for
# the sealer's per-epoch dictionary training, and storage v0 deleted both the
# dictionaries and the `state` package the version was read out of. Nothing in
# this binary shells out to anything any more; block compression is the pebble
# library's, pinned by module version.

FROM gcr.io/distroless/cc-debian13
LABEL org.opencontainers.image.source=https://github.com/containerman17/epochdb
COPY --from=build /epochdb /usr/local/bin/epochdb
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

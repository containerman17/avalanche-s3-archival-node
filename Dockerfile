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

# THE SEALER SHELLS OUT TO THE zstd CLI to train each epoch's compression
# dictionary, and the version is part of the artifact's identity: a seal on a
# different zstd (or none at all) writes different bytes for the same blocks,
# which byte-identity across independent builders forbids. The runtime layer is
# distroless and has no package manager, so the pinned trainer is built here,
# statically, and copied in. KEEP ZSTD_VERSION IN STEP WITH state/epoch.go's
# zstdPinnedVersion. Optional codecs are off: the sealer only ever calls
# --train, and every dropped dependency is one less shared object to carry.
ARG ZSTD_VERSION=1.5.7
ARG ZSTD_SHA256=eb33e51f49a15e023950cd7825ca74a4a2b43db8354825ac24fc1b7ee09e6fa3
RUN curl -fsSL -o /tmp/zstd.tar.gz \
      "https://github.com/facebook/zstd/releases/download/v${ZSTD_VERSION}/zstd-${ZSTD_VERSION}.tar.gz" \
 && echo "${ZSTD_SHA256}  /tmp/zstd.tar.gz" | sha256sum -c - \
 && tar -xzf /tmp/zstd.tar.gz -C /tmp \
 && make -C "/tmp/zstd-${ZSTD_VERSION}/programs" -j"$(nproc)" zstd \
      HAVE_ZLIB=0 HAVE_LZMA=0 HAVE_LZ4=0 LDFLAGS=-static \
 && strip "/tmp/zstd-${ZSTD_VERSION}/programs/zstd" \
 && cp "/tmp/zstd-${ZSTD_VERSION}/programs/zstd" /zstd \
 && /zstd --version

FROM gcr.io/distroless/cc-debian13
LABEL org.opencontainers.image.source=https://github.com/containerman17/epochdb
COPY --from=build /epochdb /usr/local/bin/epochdb
COPY --from=build /zstd /usr/local/bin/zstd
# exec.Command("zstd", ...) resolves through PATH; distroless sets one, but the
# trainer is load-bearing enough to state rather than inherit.
ENV PATH=/usr/local/bin:/usr/bin:/bin
ENTRYPOINT ["/usr/local/bin/epochdb"]

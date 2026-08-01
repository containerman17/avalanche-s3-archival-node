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

FROM gcr.io/distroless/cc-debian13
LABEL org.opencontainers.image.source=https://github.com/containerman17/epochdb
COPY --from=build /epochdb /usr/local/bin/epochdb
ENTRYPOINT ["/usr/local/bin/epochdb"]

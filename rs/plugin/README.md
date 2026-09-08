# rs/plugin: epochdb-rs, the rpcchainvm plugin

`proto/` is copied verbatim from avalanchego `v1.14.3-0.20260804141953-6dc4c3b395b6`
(`proto/` of that module; `io/prometheus/client/metrics.proto` from
`github.com/prometheus/client_model v0.6.2`, the version that module pins).
RPCChainVM protocol version 45 (`version/constants.go`). Regenerate nothing by hand:
`build.rs` runs protoc (`apt-get install protobuf-compiler`) through tonic-prost-build.

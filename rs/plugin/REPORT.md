# rs/plugin: epochdb-rs as an avalanchego rpcchainvm plugin (protocol 45)

Branch `rs-plugin` (main + rs-state + rs-block + rs-exec + rs-node merged), 2026-09-08 JST. Crate `rs/plugin` (workspace member, lib `plugin`, bin `epochdb-rs`), Go harness `cmd/epochdb-host-bench` (new command, no existing Go file edited). Nothing ran on the Tokyo box.

## Architecture

```
avalanchego / epochdb-host / epochdb-host-bench            epochdb-rs (this crate)
  rpcchainvm.VMClient  --- gRPC vm.VM -------------------->  vm::VmService<E: Engine>   (src/vm.rs)
  runtime engine       <-- Runtime.Initialize(45, addr) ---  vm::serve (the handshake)
  ghttp.Client         --- gRPC http.HTTP ---------------->  ghttp::HttpService per handler (src/ghttp.rs)
  rpcdb / validatorstate / ... servers  <-- clients ---------  vm::Host (rpcdb for Health, validatorstate kept for warp)
                                                              tree::Tree<E>            (src/tree.rs, the block tree)
                                                              engine::TrivialEngine    (src/engine.rs) | rs/node's executor later
```

- `src/vm.rs`: every method of `vm.proto`, written against `vms/rpcchainvm/vm_server.go` for a follower: Initialize dials the host's db and service addresses, builds the engine through a factory and answers the last accepted block; SetState records the state; ParseBlock / BatchedParseBlock decode; BlockVerify parses again and verifies through the tree (with the optional P-chain height); BlockAccept / BlockReject by id; GetBlock (verified blocks first, then accepted), GetBlockIDAtHeight, GetAncestors (block.GetAncestors' local walk with the num / size / time limits), Health (`{"database": <rpcdb HealthCheck>, "health": <engine>}` like the Go server), Version, CreateHandlers (one gRPC HTTP server per prefix on 127.0.0.1:0), NewHTTPHandler (none), WaitForEvent (blocks until Shutdown, then `Canceled`), Shutdown, the App* / Connected / Disconnected no-ops, BuildBlock refused, the five state-sync methods answering `ERROR_STATE_SYNC_NOT_IMPLEMENTED` exactly as `errorToErrEnum` does for a VM without `StateSyncableVM`. Blocking work (parse, verify, accept) runs on tokio's blocking pool.
- `vm::serve`: rpcchainvm.Serve: TCP listener on 127.0.0.1:0, `Runtime.Initialize{protocol_version: 45, addr}` to `$AVALANCHE_VM_RUNTIME_ENGINE_ADDR` within 5 s, serve; SIGINT / SIGTERM ignored until Shutdown was called, then SIGTERM stops the server (what avalanchego's subprocess runtime sends after Shutdown). Message sizes unbounded on every server and client like `grpcutils.DefaultServerOptions`; TCP_NODELAY on (grpc-go sets it; without it Nagle plus the client's delayed ACK made every round trip 40 ms, 20 blk/s).
- `src/ghttp.rs`: `HandleSimple` (what the host uses for every non-upgrade request) runs the handler on the request body and answers 200 + `Content-Type: application/json`. `Handle` (upgrades / websockets over the reader and writer streams) returns Unimplemented.
- `src/tree.rs`: `Tree<E>` keeps verified-not-accepted blocks with their pending state behind one mutex (avalanchego makes one VM call at a time). Verify is idempotent for a block already verified or accepted; otherwise the parent must be the accepted head (pending = None) or a verified block (pending = the parent's, so siblings and grandchildren each see their own branch). Accept requires the parent to be the accepted head, applies the pending state through the engine and drops the entry; Reject drops the entry (consensus rejects descendants one by one).
- `src/engine.rs`: `TrivialEngine`: parse = rs/block's proposervm unwrap + subnet-evm block decode (the inner block bytes a plugin receives, or a whole container), id = keccak(header RLP); verify = nothing; accept = append to an in-memory id / height index (bytes kept); JSON-RPC `eth_chainId`, `eth_blockNumber`, `eth_getBlockByNumber`, `eth_getBlockByHash` (batches too). The genesis id comes from the config bytes (`{"genesis-id": "0x..."}`, see the harness); everything is in memory (a harness engine: 50k blocks = 150 MB).
- `src/main.rs`: `epochdb-rs` with no arguments = plugin mode; `--version` prints `epochdb-rs/0.1.0 [rpcchainvm=45]`.

## Proto set and avalanchego version

`rs/plugin/proto/` is copied verbatim from avalanchego `v1.14.3-0.20260804141953-6dc4c3b395b6` (`version/constants.go`: `RPCChainVMProtocol = 45`): `vm/vm.proto`, `vm/runtime/runtime.proto`, `http/http.proto`, `validatorstate/validator_state.proto`, `appsender/appsender.proto`, `sharedmemory/sharedmemory.proto`, `rpcdb/rpcdb.proto`, `warp/message.proto`, `aliasreader/aliasreader.proto`, plus `io/prometheus/client/metrics.proto` from `github.com/prometheus/client_model v0.6.2` (vm.proto's Gather imports it). `build.rs` runs protoc through `tonic-prost-build 0.14` (tonic 0.14.6, prost 0.14.4); servers and clients are generated for all of them (`plugin::pb::*`). Not copied: `http/responsewriter`, `io/reader`, `io/writer`, `net/conn` (the websocket path), `signer`, `sync`, `p2p`, `platformvm`, `sdk`.

## The Engine trait (src/tree.rs)

```rust
pub trait Engine: Send + Sync + 'static {
    type Block: Send + Sync + 'static;    // the parsed block (cheap to clone, shares the bytes)
    type Pending: Send + Sync + 'static;  // what verify computed and accept applies; dropped on reject
    fn parse(&self, bytes: Bytes) -> Result<Self::Block, Error>;
    fn meta(&self, b: &Self::Block) -> Meta;                    // id, parent, height, timestamp (unix s)
    fn bytes(&self, b: &Self::Block) -> Bytes;
    fn verify(&self, b: &Self::Block, parent: Option<&Arc<Self::Pending>>, pchain_height: Option<u64>) -> Result<Self::Pending, Error>;
    fn accept(&self, b: &Self::Block, p: &Self::Pending) -> Result<(), Error>;
    fn last_accepted(&self) -> Self::Block;
    fn get_block(&self, id: &Id) -> Option<Self::Block>;      // accepted blocks only, the tree answers for verified ones
    fn block_id_at_height(&self, height: u64) -> Option<Id>;
    fn rpc(&self, body: &[u8]) -> Vec<u8>;                     // one JSON-RPC request body in, one response body out
    fn health(&self) -> Result<serde_json::Value, Error> { ... }
    fn shutdown(&self) {}
}
```

`verify` executes `b` on top of its parent's state: `parent` is the parent's `Pending` when the parent is verified but not accepted (the engine reads through that chain of pending write sets, then the accepted state), None when the parent is the accepted head. The sibling test (`cd rs && cargo test -p epochdb-plugin`, `tree::tests::siblings_accept_one_reject_other`) drives a key-value engine through two children of one parent and a grandchild on each, accepts one branch, rejects the other, and checks that the accepted state and what the next block sees are the accepted branch's only, that the rejected branch cannot be extended, that accept-before-verify is refused and that re-verifying an accepted block is a no-op.

## What rs/node must implement to plug in

rs/node today (`rs/node/src/main.rs`): `Executor<Backend>::execute_block` commits each tx straight into the fresh overlay and the block's ordered write set (`Backend::take_ws`), checks gasUsed / receiptsRoot / logsBloom against the header, then hands `CheckItem {ws, header_rlp, receipts, traces, code}` to the checker thread (Dirty root one block behind, history file), with `maybe_roll` / `swap_roll` around it. Mapping onto the trait:

- `Block` = rs/block's `Block` with senders recovered (parse submits to the recovery pool the way vmchain/recover.go does; BatchedParseBlock hands 256 at once).
- `Pending` = `{ parent: Option<Arc<Pending>>, ws: Vec<(key, value)>, code, receipts, traces, gas_used }`. `verify` runs `Executor<Layered>` where `Layered` is a `StateDb` whose reads go pending chain -> `Backend` (fresh overlay -> frozen -> run) and whose commits land in the new `Pending`'s map instead of the overlay; rs/exec's `Executor<D: StateDb>` is already generic, so this is one wrapper type. The receiptsRoot / logsBloom / gasUsed checks stay in verify (a mismatch fails Verify, which is what consensus expects).
- `accept` = put `ws` into the fresh overlay in order (what `DatabaseCommit` does today), set the block hash, send the `CheckItem` to the checker (root check and history / store write one block behind, as now; a root mismatch still exits the process, the Go follower's `log.Fatal`), `maybe_roll`.
- `last_accepted` / `get_block` / `block_id_at_height` / `rpc`: from the store (P2 rs/store, P4 rs/rpc); until then an in-memory index like the trivial engine's plus MANIFEST. The genesis block: the engine must build the genesis header (the alloc root is already computed in rs/node, `genesis state ok`) so `last_accepted` at height 0 carries the real hash; the trivial engine takes it from the config bytes instead.
- Bootstrapping is Verify + Accept serially per block with no siblings, so the pending chain is one deep; NormalOp under a real validator set holds a few.

Workspace: `rs/Cargo.toml` members now include `plugin`. rs/node's bin is also named `epochdb-rs` (the benchmark node); the two collide in `rs/target/release` only when both are built together (`cargo build --release -p epochdb-plugin` and `-p epochdb-node` each work). P5 merges them into one binary (plugin mode with no arguments, the bench mode with `--dump`).

## Harness: cmd/epochdb-host-bench

`cmd/epochdb-host/main.go` with the fetcher replaced by the go-bench dump source (`dump.go` copied from `go-bench:cmd/epochdb-vm-bench/dump.go`), everything else as the host does it: `rpcchainvm.NewFactory(...).New(logger)` launches the plugin, `Initialize` with a pebble-backed rpcdb, shared memory, aliaser, warp signer, the RPC validator state (only called if the plugin asks; `--node`), `SetState(Bootstrapping)`, `CreateHandlers` mounted at `/ext/bc/<chainID>/<prefix>` on `--http`, then per block `BatchedParseBlock` (`--batch 256`; `--batch 1` = ParseBlock one at a time, what avalanchego's bootstrapper does) ahead of `Verify` / `VerifyWithContext` (proposervm's P-chain height rule) + `Accept` under one VM mutex, `SetPreference` every 1000 blocks and per block in NormalOp, `SetState(NormalOp)` at the dump's last block, the host's bench line every 10 s plus `bench exit ... blk/s=`, then a check line (`eth_blockNumber` and `eth_getBlockByNumber(head).hash` through the mounted handler against keccak(header) of the dump's container, and `LastAccepted`), `Shutdown`. `--serve` keeps HTTP up after the dump. `fetch.RegisterExtras(chain.SubnetEVM)` so the harness decodes subnet-evm headers (tx and gas counts, the check).

The dump dir must hold `chain.json` (the chain package's cache file: `genesisData` base64 = the Initialize genesis bytes, `blockchainID`, `subnetID`, `networkID`) and `upgrade.json` (the upgrade bytes). Config bytes: `--config` (default `{"state-sync-enabled":false}`) plus `"genesis-id"` = block 1's parent hash, added by the harness for epochdb-rs's trivial engine (stock subnet-evm ignores unknown keys; avalanchego forwards only `GRPC_*` / `GODEBUG` environment variables to plugins, so the config bytes are the channel).

```
S=/tmp/claude-1000/-home-ilia-epochdb/222c563c-789d-46b6-8726-b8af4b2a6f62/scratchpad
go run ./cmd/epochdb-host-bench --dump $S/rs/step/step-containers-1-50000.bin \
  --vm $S/plugins/srEXiWaHuhNyGwPUi444Tu47ZEDwxTWrbQiuD7FmgSAQ6X7Dy --data $S/hb-stock --http 127.0.0.1:19901
go run ./cmd/epochdb-host-bench --dump $S/rs/step/step-containers-1-50000.bin \
  --vm rs/target/release/epochdb-rs --data $S/hb-rs-256 --http 127.0.0.1:19902 --batch 256
```
The stock plugin is `subnet-evm-linux-amd64-v1.14.2.tar.gz` from the avalanchego v1.14.2 release (`Subnet-EVM/v1.14.2@6e5acf90 [rpcchainvm=45]`), named by the VM id `srEXiWaHuhNyGwPUi444Tu47ZEDwxTWrbQiuD7FmgSAQ6X7Dy` under `$S/plugins/`.

## Results

All runs: the Step 50k dump (heights 1..50,000, 343,731 txs, 21.6 Ggas), fresh data dir, local i7-10700K, 2026-09-08 22:21-22:26 JST, sequential. `match=true` on the check line means eth_blockNumber == 50000 and eth_getBlockByNumber(0xc350).hash == keccak(header RLP) of container 50,000 in the dump == `0xa9a6c28a4081e99a5a91b0864c18d97f6a08505d3c0f0b92410d45ede5a7b3f6`, and the plugin's LastAccepted is that block (`2HiX1mLTKyGyDbJnrbojpKUppTSDH2dSBfsvbk6AXzTMvokTxg`, the same id as base58).

| plugin | batch | wall | blk/s | vm rss | check |
|---|---|---|---|---|---|
| stock subnet-evm v1.14.2 (executes every block, pruning default) | 256 | 101 s | 492 | 515 MB | match=true |
| epochdb-rs, TrivialEngine | 256 (BatchedParseBlock) | 28 s | 1,769 | 141 MB | match=true |
| epochdb-rs, TrivialEngine | 1 (ParseBlock per block) | 42 s | 1,167 | 141 MB | match=true |

1. Harness + stock subnet-evm: every one of the 50,000 blocks parsed, verified and accepted (`bench exit ... h=50000 blk=50000`), `SetState(NormalOp)` at 50,000, eth_blockNumber and eth_getBlockByNumber through the mounted `/rpc` handler answer, hash of block 50,000 equals keccak(header) from the dump, clean Shutdown (251 ms). The stock plugin also mounted `/validators` and `/ws`. (An earlier run of the same binary before the harness registered the libevm extras: 526 blk/s; the two numbers bracket the stock plugin's rate.)
2. Harness + epochdb-rs (TrivialEngine): the same protocol path end to end: Initialize with the db, shared memory, aliaser, app sender, validator state and warp signer servers up (the plugin dials db and server addresses lazily; Health uses the db one), SetState(Bootstrapping), BatchedParseBlock or ParseBlock, BlockVerify (`ShouldVerifyWithContext` false, so plain Verify), BlockAccept, GetBlock and LastAccepted at start, GetBlockIDAtHeight (the tree's is_accepted path on every verify), SetPreference (every 1000 blocks and at the tip), CreateHandlers answering `eth_blockNumber` / `eth_getBlockByNumber` / `eth_chainId`, SetState(NormalOp), Shutdown clean (44 ms; the plugin exits on the SIGTERM the runtime sends, `vm server: graceful termination success`).
   Round-trip cost: with parse batched 256 per call the per-block work is Verify + Accept = 2 gRPC round trips through avalanchego's client: 28 s / 50,000 = 0.57 ms per block, 0.28 ms per round trip. With ParseBlock per block (3 round trips): 0.84 ms per block, again 0.28 ms per round trip. So BatchedParseBlock is worth 1.5x here (1,167 -> 1,769 blk/s) and the tree's ceiling with no execution is about 1,770 blk/s, versus the ~880 blk/s the Go plugin path measured (1 ms per block of round trips). `full=` is at the window's length and `wait=0` in both runs: the VM round trips are the limiter, not the dump. The remaining cost is on both sides of the wire (the Go client's `chain.State` bookkeeping and gRPC, the plugin's tokio + spawn_blocking hop); a Verify+Accept fused into one call would halve it but is not in the protocol.
   The first attempt ran at 18 blk/s: 40 ms per round trip from Nagle on the plugin's sockets against grpc-go's delayed ACKs; tonic's `serve_with_incoming` ignores the builder's `tcp_nodelay`, `TcpIncoming::from(listener).with_nodelay(Some(true))` fixed it. Worth remembering for every tonic server that talks to a Go client.
3. Sibling test: `cd rs && cargo test -p epochdb-plugin` passes (`tree::tests::siblings_accept_one_reject_other`, see the trait section).
4. `epochdb-rs --version` prints `epochdb-rs/0.1.0 [rpcchainvm=45]` (the Go plugin's line shape, `epochdb-vm/0.1 [rpcchainvm=45]`). `cargo build --release --target x86_64-unknown-linux-musl -p epochdb-plugin` produces a static-pie binary (21.9 MB, debug info kept by the workspace profile) that prints the same version line and, with no runtime address in the environment, exits 1 with `required env var missing: "AVALANCHE_VM_RUNTIME_ENGINE_ADDR"` like the Go plugin. Deviations: the section below.

Not done in the time box: wiring rs/node's executor behind the trait (the mapping is above), the genesis header from the genesis JSON (the trivial engine takes the id from the config bytes), the `/ws` handler and websocket upgrades, metrics in Gather, a run under a stock avalanchego binary (the harness is avalanchego's own rpcchainvm client, factory and runtime manager, so the handshake, plugin naming and Initialize are the ones avalanchego uses; the difference under a real node is consensus calling Reject and verifying siblings, which the tree test covers).

## Deviations from vm_server.go

- No `grpc.health.v1` service on the plugin's server (the Go plugin registers one; nothing in avalanchego's client, runtime or the host calls it).
- `Gather` returns no metric families (the Go server exposes process, Go runtime, gRPC client and VM metrics).
- `CreateHandlers` serves `HandleSimple` only; `Handle` (upgrade / websocket requests, the `/ws` handler) is Unimplemented, and the only prefix is `/rpc` (the Go follower also mounts `/ws`).
- `NewHTTPHandler` answers no handler (the Go follower does the same).
- Initialize does not validate the BLS public key, the node id length or the network upgrades (the Go server parses them into `snow.Context`); they are handed to the engine in `vm::Init` as bytes.
- Health calls the host's rpcdb `HealthCheck` like the Go server, but the VM keeps no data in that database.
- Plain errors are `Unknown` with the message (what a Go handler returning `error` produces); the enum-coded errors (`NOT_FOUND`, `STATE_SYNC_NOT_IMPLEMENTED`) follow `errorToErrEnum` with a nil gRPC error.
- BlockVerify re-parses the bytes (as the Go server does); the parsed-block cache of `chain.State` lives on the host side, not here.
- Logging is `eprintln!` lines, not the zap logger the Go server builds; avalanchego relays the plugin's stderr.

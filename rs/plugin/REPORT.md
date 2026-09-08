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

# PHASE 2: the real executor behind the plugin (branch rs-vm)

Branch `rs-vm` on top of rs-plugin (edafa2c), 2026-09-09 00:00-01:00 JST, local i7-10700K, nothing on the Tokyo box. One binary `epochdb-rs` (crate `rs/plugin`): no arguments = rpcchainvm plugin over rs/node's executor; `--dump ...` = rs/node's in-process bench (unchanged flags, rs/node is a library now); `--version`. No Go file edited except the new harness `cmd/epochdb-host-bench` (it is rs-plugin's own code). The TrivialEngine is gone (its numbers are above; git has it).

## Engine mapping (rs/plugin/src/node_engine.rs, layered.rs)

| trait | NodeEngine |
|---|---|
| `Block` | `Arc<block::Block>` with senders recovered. `parse` = `block::decode_container` (inner bytes or a whole container) + libsecp256k1 recovery per tx; the parsed blocks are kept by id (8,192 max, swept below the accepted head every 256 accepts) so the host's re-parse in BlockVerify is a lookup. `parse_batch` (new trait default = map parse) runs the batch on a rayon pool of NumCPU-2 threads: BatchedParseBlock is where the recovery happens, like vmchain/recover.go's pool but synchronous inside the call (the host parses the next batch while it verifies the current one, so the executor never recovers). |
| `Pending` | `{ number, hash, time, parent: Option<Arc<Pending>>, map (reads, slot tombstones included), owners, code, payload: Mutex<Option<{ws, code, receipts, traces, gas_used, txs}>> }`. The payload is taken once, at accept. |
| `verify` | `Layered::begin(parent)`, `Executor<Layered>::execute_block(b, parent_time)`, gasUsed / receiptsRoot / logsBloom against the header (a mismatch is the Verify error), `Layered::finish` -> Pending. `Layered: StateDb` reads cur -> pending chain -> `Backend` (fresh overlay -> frozen -> run), commits into cur's map + ordered write set (Backend::commit's rows, tombstones in the read map only, as engine.tombstoneSlots regenerates them at overlay apply). Code by hash: cur -> chain -> Backend's table. BLOCKHASH: the chain's hashes, then the accepted ones. |
| `accept` | swap a finished roll first (checker parked, store fsync, MANIFEST, Dirty rebased: rs/node's `Roller::finish_roll`), `Backend::apply_ws(ws)` (Go's applyOverlay: puts + slot tombstones + owners), code table, block hash, head = b, `Roller::maybe_roll(budget, h, root)`, then the CheckItem to the checker thread (depth 4). |
| checker thread | Dirty apply + root one block behind, compared with the header root: a mismatch prints `block N: state root mismatch: computed X, header Y` and exits 1 (the Go follower's log.Fatal); then the interim store append (blocks.log + code.log), fsync every 256 blocks on a flusher thread through its own file handles, `root-checked` counter. |
| `last_accepted` / `get_block` / `block_id_at_height` | the head (in memory), the genesis, the accepted-not-yet-logged window (at most 4), then the store's id index. |
| `set_state` (new trait default) | Bootstrapping = catch-up profile, NormalOp = tip profile (below). |
| `shutdown` | a roll in flight is waited for and swapped in (so a restart replays from the newest roll), the checker drains, the store is synced, the counters and time split go to stderr. |

The engine's executor sits behind one mutex (`Inner { Executor<Layered>, Roller, roll_budget }`); verify and accept are serialized by the tree's lock anyway, RPC state reads and eth_call take the same mutex.

Genesis (rs/plugin/src/genesis.rs): the header as subnet-evm's `core.Genesis.toBlock` builds it (fields with their defaults, baseFee = genesis baseFeePerGas or feeConfig.minBaseFee under SubnetEVM, blockGasCost 0 under Etna, the EIP-4844/4788 zeros under Cancun, Granite refused) over the root of the alloc plus the precompiles active at 0 (rs/exec's seeded state, alloy-trie recompute); `block::eth::encode_header` (new, the inverse of the decoder). Step: `0x628a4aba...5cb1b0c1` = block 1's parentHash (unit test `genesis::tests::step_genesis_hash` and the harness's `check genesis` line). The `genesis-id` config hack is gone from the harness.

Budgets (vmexec/budget.go): `SyncRoll` 2 GiB while Bootstrapping, `TipRoll` 128 MiB on SetState(NormalOp) plus one roll of whatever the overlay holds at the switch (tickBudget); no GOGC equivalent. Config bytes `{"roll-budget-mb": N}` override the catch-up budget (tip = min(N, 128)), the harness's way to force rolls.

Call mode (rs/exec): `Executor::call(head_header, CallMsg)` = subnet-evm DoCall (base fee, balance, nonce and EIP-3607 checks off, block gas limit off, journal cleared, nothing committed) and `Executor::open(cfg, db)` (no genesis seeding) for a db that already holds a state. revm's `optional_*` features enabled in rs/exec.

## The interim store (rs/plugin/src/log.rs)

```rust
pub trait BlockStore: Send {
    fn head(&self) -> u64;                                   // last logged height, 0 when empty
    fn height_of(&self, id: &Id) -> Option<u64>;
    fn id_at(&self, height: u64) -> Option<Id>;
    fn container(&self, height: u64) -> io::Result<Option<Bytes>>;  // the bytes the plugin was handed
    fn read(&self, height: u64) -> io::Result<Option<Record>>;      // + receipts, traces, write set, code (recovery)
    fn append(&mut self, r: &Record) -> io::Result<()>;             // height == head + 1
    fn sync(&self) -> io::Result<()>;
}
```
`BlockLog` = `<chainData>/blocks.log`, records `[u32 len][u64 height][32 id][u32 crc32][payload]`, payload = container, receipts RLP, callTracer JSONs, the block's ordered write set (contract keys), deployed code, each u32-length-prefixed; one write per record; the height/id index is rebuilt on open from the record heads (one pread each: 50k blocks 3.7 s cold, 30-40 ms warm; 1M would be around a minute cold, rs/store's job). `CodeLog` = `code.log` (`[32 hash][u32][code]`), read whole on open into the Backend's code table (the run holds no code). Torn tail rule (the wiki note): a record whose bytes are not all there, or whose crc fails as the LAST record, is truncated away; anything short or bad elsewhere, and any read error, is an error. Test `log::tests::roundtrip_and_torn_tail`. 50k Step: blocks.log 319 MB (the traces are most of it, uncompressed as rs/node's history file was), code.log 168 KB, vmstate 21 MB.

## Recovery (NodeEngine::open, from vmexec/recover.go)

1. Config from the genesis + upgrade bytes, the genesis block, config bytes.
2. `vmstate/MANIFEST` present: `open_rolled` opens `run.<gen>` / `trie.<gen>`, checks their user data and the trie root against the manifest, sweeps every other file in vmstate (a torn roll is anything unnamed). Absent: vmstate is wiped, the genesis state goes through the executor into `run.0` / `trie.0`, root-checked, `MANIFEST {0, 0, root}`.
3. Open blocks.log and code.log (torn tails dropped, logged), code into the Backend. `head < manifest.height` is an error. The rolled root must equal the chain's root at the manifest height (genesis root at 0, else the stored header's). Block 1's parentHash must be the genesis hash.
4. Replay heights manifest.height+1..=head: `Backend::apply_ws` + `Dirty::apply` per row (code came from code.log). If anything was replayed, `Dirty::root` must equal the head header's root, else exit 1 with both roots. The last 256 block hashes are loaded for BLOCKHASH. Head = the stored block (or the genesis).
5. Log line `recovered: rolled at H (gen G), head N <hash>, rows replayed R, root ok, in T ms`; the executor opens over the Backend; the checker thread starts.

MANIFEST is written by `Roller::finish_roll` after the rolled root matched the verified root at the roll height and after the store was fsynced (Go's syncStore), temp + fsync + rename + dir fsync, then the old pair is unlinked.

## Results

Harness runs: `go run ./cmd/epochdb-host-bench --dump $S/rs/step/step-containers-1-50000.bin --vm rs/target/release/epochdb-rs --data $S/vm-50k/data --http 127.0.0.1:19902 --batch 256 --config '{"state-sync-enabled":false,"roll-budget-mb":8}'` (`$S` = the session scratchpad; logs under `$S/vm-50k`, `$S/vm-crash`, `$S/vm-1m`).

1. Step 50k, 8 MB roll budget, fresh dir (`$S/vm-50k/run3.log`): every block parsed, verified, accepted and root-checked (`exit: blocks=50000 txs=343731 gas=21828575876 root-checked=50000 rolls=4 head=50000`), rolls at 17,546 / 31,462 / 45,339 by budget and one at 50,000 on SetState(NormalOp), every rolled root equal to the verified root (a mismatch exits), `check eth_blockNumber=50000 ... match=true` and `check genesis eth_getBlockByNumber(0x0).hash=0x628a4aba... block1.parentHash=0x628a4aba... match=true`. 46 s, 1,078 blk/s, vm_rss 133 MB. Time split from the exit line: `evm=4.40s trace=0.37s commit=0.55s | parse-batch=2.72s parse=0.55s verify=6.21s accept=3.60s checker=6.48s`: the engine spends 12.5 s inside VM calls (0.25 ms per block: 0.05 parse batch on 14 threads, 0.12 verify, 0.07 accept, the checker off the critical path) and the other 33 s are the gRPC round trips plus the host's bookkeeping, the same ~28 s the trivial engine measured for 50k blocks (Verify + Accept = 2 round trips of 0.28 ms plus the batched parse). So under rpcchainvm the plugin is round-trip bound at about 1,100 blk/s on these blocks, the executor is busy 10 percent of the time, and BatchedParseBlock + parse-time recovery keeps it fed: `wait=0.0s` on the host side and no recovery inside verify.
   RPC oracle: the public door answered 502 for the whole session (`error code: 502`), so the oracle is stock subnet-evm v1.14.2 under the same harness on its 50k data dir (`--serve`, the RPC ORACLE RULE's tiebreaker), both plugins at head 50,000 (`$S/vm-50k/rpccmp.py`, output `rpccmp.out`): 30 of 30 answers byte-equal: eth_chainId, eth_blockNumber, eth_getBalance of the alloc address `0x7212Ac7f...` at `latest` and at `0xc350` (`0x10263f6f007f8154972fb800`), of `0x337858b9...` (`0xe851ff19c196e00`) and of the contract, eth_getTransactionCount (`0x7c25`, `0x6`), eth_getCode of the Step contract `0x48f7f068...` (607 bytes) and of an EOA, eth_getStorageAt slots 0..2, eth_call of the contract's `spam(uint256)` (`0x5858d161`, the only function on Step 1..50k; it writes n hashed slots) with n=3 from `0x337858b9...` and with n=1 from the zero address (`0x` on both, the state is not committed), the four reverting calls (`-32000 execution reverted` with no data, geth's shape for an empty revert), the intrinsic-gas error text, eth_estimateGas of a plain transfer (`0x5208`, the 21000 shortcut), of spam(3) / spam(40) / spam(1) at a gas price (`0x866f`, `0xdec3c`, `0x65fd`: the gasestimator port with `lo = gasUsed - 1`, the optimistic 64/63 probe, the 1.5 percent error ratio and the `mid <= 2 lo` skew, exactly), eth_getBlockByHash of block 50,000 and eth_getBlockByNumber(0x1, full) with the tx objects (senders recovered on read).
2. Crash test (`$S/vm-crash/run2.sh`, kill -9 on the plugin pid when a log pattern appears, `--from 0` resumes from the plugin's LastAccepted as the host does):
   - outside a roll: killed at height 31,700 right after roll 2 had swapped in (`kill1.log`; the host saw `Verify: rpc error: code = Unavailable`). MANIFEST `{gen 2, 31462}`. Restart (`resume1.log`): `recovered: rolled at 31462 (gen 2), head 31700, rows replayed 3549, root ok, in 29 ms`, `plugin last accepted height=31700`, the run continued from 31,701 (roll 3 at 45,339 equal to the verified root).
   - inside a roll: killed on `roll 4 start` at 50,000 (`resume2.log`), leaving `run.4` without `trie.4` and MANIFEST at gen 3 / 45,339. Restart (`resume3.log`): `swept run.4: not named by the manifest`, `recovered: rolled at 45339 (gen 3), head 50000 0xa9a6c28a..., rows replayed 64431, root ok, in 134 ms`, both check lines `match=true`, head hash equal to the fresh run's (`0xa9a6c28a4081e99a5a91b0864c18d97f6a08505d3c0f0b92410d45ede5a7b3f6`, the same root at 50,000 the checker verified in the fresh run).
   (Kills on `roll N start` with the default `tail -F` landed after the 170-270 ms rolls had finished; `-s 0.005` lands inside.)
3. 1M dump, 180 s, 256 MB roll budget: see the table below.
4. `cd rs && cargo test --workspace`: 20 tests pass (state 14, exec 2, block 1, plugin 3: the sibling tree test, the Step genesis hash, the log round trip + torn tail). `cargo build --release --target x86_64-unknown-linux-musl -p epochdb-plugin`: static-pie 40.9 MB, `epochdb-rs/0.1.0 [rpcchainvm=45]`.

1M dump under the harness (`$S/vm-1m/run.log`, `roll-budget-mb: 256`, `--batch 256`, fresh dir; the harness was SIGINTed 10 s after its t=17x line, so the exit line is at t=190 s; the t=179 line is the 180 s figure):

| | epochdb-rs under rpcchainvm (this) | rs/node in-process (rs-node report) | TrivialEngine ceiling (above) |
|---|---|---|---|
| blocks at 180 s | 220,450 (t=179), 234,307 at t=190 | 895,592 | (50k in 28 s) |
| blk/s | 1,225 (window), 1,232 cum at exit | 4,976 | 1,769 |
| tx/s | 5,240 (window), 5,665 cum | 19,182 | |
| mgas/s cum | 305 (302.9 at exit) | 1,348 | |
| plugin RSS | 395 MB at t=179, 424 MB at exit (host 661 MB) | 1,338 MB peak | 141 MB |
| rolls | 0 (overlay under 256 MB at 234k) | 1 | |
| root-checked | 234,307 of 234,307 | 896,445 | none |

Where the time goes (the plugin's exit line, 191 s): `evm=12.35s trace=1.43s commit=1.96s` (the executor busy 16 s, 3,660 mgas/s while it runs), `parse-batch=8.36s parse=2.00s verify=18.85s accept=10.21s`: 37 s inside VM calls, `checker=24.20s` off the critical path. The other ~150 s are gRPC round trips and the host's per-block bookkeeping: 2 round trips per block (Verify, Accept) at the 0.28 ms the trivial engine measured plus the host's chain.State work, about 0.65 ms per block, against 0.16 ms of engine work per block. `wait=0.0s full=184.6s` on the host: the dump was always ahead, the VM path was the wall the whole run. BatchedParseBlock + parse-time recovery keeps the executor fed (no recovery inside verify; parse-batch is 36 us per block on the pool, serialized with verify by the host's VM mutex). So under rpcchainvm this engine runs at 1.2k blk/s, a quarter of its in-process 5k: the protocol, not the executor, is the ceiling, and the way past it is fewer or cheaper round trips (a fused Verify+Accept is not in the protocol; the Go plugin path measured ~880 blk/s on the same blocks). The 50k window here (t=49: h=56,592) is close to the 50k run's 1,078 blk/s.

## Deviations

- Recovery replays the store's write sets block by block in order (rs/node's contract-key rows) instead of Go's address-form rows with per-key latest-wins; same result, simpler.
- The parse cache replaces Go's sender cache on the tx object; a block parsed but never verified is swept at 8,192 entries.
- Empty write sets are still root-compared (current root vs header), as rs/node does.
- `totalDifficulty` in block JSON is the height (difficulty 1 per block over a genesis of 0 on every subnet-evm chain we run).
- eth_getBlockByNumber / ByHash for a stored block decodes the container on every call, no cache; state RPCs only at `latest` (the interim store has no history).
- Shutdown waits for a roll in flight instead of abandoning it (Go abandons; the sweep covers both).
- The `Inner` (revm Evm) is `unsafe impl Send` behind the mutex: revm's LocalContext holds an Rc the Evm alone touches.

## What rs/store and rs/rpc must provide to replace the interim pieces

rs/store: an implementation of `BlockStore` (head, height by id, id by height, container by height, the per-block record for recovery replay: write set + code; append; sync), the code table by hash for the Backend (today code.log read whole), and the recovery contract: rows since the manifest height must be readable after a crash through the store's own head (the WAL semantics of Go's runs). The engine calls `sync()` before writing MANIFEST and every 256 blocks. Receipts and traces are handed over in the same record; the store decides their format. rs/rpc: takes `NodeEngine`'s `head`, `store`, `inner` (executor + backend for state reads and `Executor::call`) and the `rpc::handle` envelope; historical state and the debug_/ots_/edb_ namespaces are its own.

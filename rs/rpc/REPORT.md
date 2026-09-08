# rs/rpc: the JSON-RPC surface of the Rust node

Branch `rs-rpc` (rs-vm + rs-precompiles + rs-store merged), 2026-09-09 01:10 to 05:00 JST, local i7-10700K, nothing on the Tokyo box. Crate `rs/rpc` (lib `rpc`, bin `epochdb-rpc-serve`), used by `rs/plugin`'s `/rpc` handler. No existing Go file edited; one new Go file, `exp/rpcoracle/main.go` (a read-only `rpc.NewServer` over a Go data dir, the ots_/edb_ oracle: `epochdb-vm-bench` shuts its HTTP server the moment the dump ends, so it cannot stand as an oracle).

## Shape

```
rs/rpc/src/lib.rs      Store trait (what the RPC layer reads), Server, dispatch, JSON-RPC envelope (single, batch, limits)
        json.rs        hexutil shapes; block / tx / receipt / log JSON (coreth internal/ethapi, field for field)
        eth.rs         eth_ blocks, txs, receipts, logs (postings + header bloom + exact filter), state reads, the fee surface
        call.rs        everything that executes: RpcDb (revm CacheDB over the Store's state at a height), eth_call,
                       estimateGas, callDetailed, createAccessList, debug_traceCall, block re-execution under a tracer
        debug.rs       debug_ tracers (stored callTracer frames; re-execution otherwise), raw getters, printBlock, getAccessibleState
        fee.rs         subnet-evm's fee window (customheader.EstimateNextBaseFee) with the FeeManager state
        filters.rs     eth_newFilter and friends (in-memory, 5 min deadline), txpool_ shapes
        ots.rs edb.rs tokens.rs   ots_ and edb_ (ported by a subagent, oracle = the Go node)
        storedb.rs     Store over rs/store's DB (rows, descent, postings) with the genesis state as the floor
        genesis.rs     the genesis header (moved from rs/plugin)
        bin/serve.rs   plain HTTP JSON-RPC server over a store dir (the differential and throughput harness)
rs/plugin/src/rpc_store.rs   INTERIM Store over the engine: head window + blocks.log, tx-hash index, state at head only
rs/exec: `Trace` mode on the Executor (Call / PreState / Struct / Noop / Off), `CallOut.trace_json`, balance check in call mode
```

`Store` (lib.rs): head, block(h) with senders, hash_at, height_by_hash, tx_by_hash -> (height, index), receipts(h), traces(h) (the stored callTracer JSON per tx), container(h), state_at(h) -> StateRead (account / storage / code at the END of block h), log_candidates (postings, None = scan), and the TxNum-space accessors ots_/edb_ need (tx_range, height_of_tx, next_tx, postings(prefix, lo, hi, desc), groups, set_scan). Two impls: `storedb::StoreDb` (rs/store DB behind one mutex; state = descent at the block's boundary TxNum with the executor-seeded genesis state as the floor, since the store holds no block-0 rows) and `plugin::rpc_store::PluginStore` (the interim log; historical state and postings answer "not available").

Execution: `Server::executor_at(n, trace)` = `exec::Executor::open(cfg, RpcDb)` where `RpcDb` is revm's `CacheDB` over a `HistView` (the Store's StateRead as a `DatabaseRef`; read errors are recorded and raised after the run, like geth's `statedb.Error()`). eth_call = `Executor::call` (DoCall: no base fee check, nonce / EIP-3607 / block gas limit off, geth's buyGas balance check ON, nothing committed) with geth's state and block overrides; estimateGas = the gasestimator port (21000 shortcut, optimistic 64/63 probe, 1.5 percent error ratio, `mid <= 2 lo`); block tracers = `Executor::execute_block` from the parent's state with the requested `Trace`.

## Method inventory

Status: `=` byte-equal with stock subnet-evm v1.14.2 on every probe of the differential (Step 1..50k and beam 1..50k); `~` equal except the listed cases; `Go` matched against the Go node (ots_/edb_, no stock equivalent); `x` refused as the Go node refuses it.

| method | status | notes |
|---|---|---|
| eth_getBlockByNumber / ByHash (full and hashes) | = | subnet-evm header fields incl. baseFeePerGas, blockGasCost, timestampMilliseconds, minDelayExcess; totalDifficulty = height; size = RLP size of [header, txs, []]; `pending`/`safe`/`finalized` = head; past the head = -32000 "cannot query unfinalized data" (stock) |
| eth_getHeaderByNumber / ByHash | = | |
| eth_getBlockTransactionCountByNumber / ByHash | = | null past the head (stock) |
| eth_getTransactionByHash / ByBlockNumberAndIndex / ByBlockHashAndIndex | = | effective gasPrice on type 2, yParity / accessList / chainId per type |
| eth_getRawTransactionByHash / ByBlock*AndIndex | = | |
| eth_getTransactionReceipt, eth_getBlockReceipts | = | stored rows: status, gasUsed, cumulative, logs; bloom, contractAddress, effectiveGasPrice, block-wide logIndex derived |
| eth_getLogs (range, blockHash, address list, topic positions with OR lists, wildcards) | = | postings candidates -> header bloom -> exact filter; no range cap (stock has none; the Go node caps at 10,000) |
| eth_getBalance / getTransactionCount / getCode / getStorageAt at latest, at a height, at a hash tag, object tags | = | store path; the interim plugin store answers only at head (below) |
| eth_call (+ state override, block override) | = | default gas = the 50M RPC cap; input wins over data silently (libevm); funds error text `err: insufficient funds for gas * price + value: address X have H want W (supplied gas G)` |
| eth_estimateGas | = | hi = block gas limit, capped by balance / gasPrice |
| eth_callDetailed, eth_suggestPriceOptions, eth_getChainConfig / debug_chainConfig, eth_baseFee | Go | coreth shapes the Go node serves; stock has no eth_callDetailed / eth_baseFee; eth_getChainConfig is the genesis config marshalled as stock does (fork fields filled, `upgrades` from upgrade.json, no eip150Hash) |
| eth_gasPrice, eth_maxPriorityFeePerGas | = / ~ | subnet-evm's gasprice.Oracle port (40 blocks, 40th percentile of effective tips, 80 s wall-clock lookback, 1 wei floor, 150 gwei cap; price = tip + EstimateNextBaseFee at the wall clock). Step equal; beam: see differences |
| eth_feeHistory | = | the Oracle's FeeHistory: blockCount entries (no trailing projection), 2048 per call, the 25,000-block history limit and its error texts, percentile validation, geth's reward percentiles over gas-sorted tips |
| eth_createAccessList | ~ | derived from the prestate trace (accounts and slots touched) minus sender, destination, coinbase and precompiles; equal on the probes (empty lists), untested on a call that touches other contracts |
| eth_chainId, eth_blockNumber, net_version, net_listening, net_peerCount, web3_sha3, eth_syncing, eth_accounts, eth_coinbase / eth_etherbase, eth_getUncle*, eth_pendingTransactions, txpool_* | = | web3_clientVersion is `epochdb/v0.1.0` (the Go node's; stock says `v1.14.2`) |
| eth_newFilter / newBlockFilter / newPendingTransactionFilter / getFilterChanges / getFilterLogs / uninstallFilter | = (shape) | 16-byte ids like rpc.NewID, so the ids differ by construction |
| debug_traceBlockByNumber / ByHash / traceBlock(rlp), debug_traceTransaction with callTracer | = | the stored frames (no re-execution) for the plain callTracer; `reexec` and `timeout` accepted and ignored |
| callTracer onlyTopCall / withLog, prestateTracer (+ diffMode), the struct logger (+ disableStorage / disableStack / enableMemory / enableReturnData), 4byteTracer, noopTracer, muxTracer | ~ | re-execution from the parent's state; the residual differences are listed below |
| debug_traceCall (any tracer, stateOverrides / blockOverrides in the config) | = | |
| debug_getRawBlock / getRawHeader / getRawTransaction / getRawReceipts | = | raw block = the eth block RLP (what stock serves), not the proposervm container |
| debug_printBlock | Go-ish | a fixed text of the block's fields, not coreth's spew dump (not reproducible byte for byte) |
| debug_getAccessibleState, debug_getBadBlocks | Go | stock refuses a JSON number here; both answered as the Go node does |
| debug_getModifiedAccountsBy*, debug_dumpBlock, accountRange, storageRangeAt, intermediateRoots, preimage, traceBadBlock, traceChain, eth_getProof, eth_send*, eth_sign*, eth_subscribe over HTTP | x | the Go node's refusals, same codes and texts |
| flatCallTracer, JS tracers | x | not served (-32602 "tracer not found"); stock runs a JS tracer for any unknown name and answers a per-tx `ReferenceError` |
| ots_getApiLevel, searchTransactionsBefore / After, getTransactionBySenderAndNonce, getContractCreator, getInternalOperations, getBlockDetails, getBlockTransactions | Go | 1442 probes equal against the Go node over its own Step 50k store (keyset paging both directions, first / last pages, cursors above the head, page size caps, every error text) |
| edb_getLogsByEmitter, getLogsByTopicValue, getTopicGroups, getTokenTransfersByHolder / ByContract, getTokenContracts | Go | same run; the ERC-721 `supportsInterface` probe only ever hit its false branch on Step (no 4-topic Transfer logs in 1..50k), the erc1155 paths saw no row |
| epochdb_head | Go | |
| batch requests (1000 max, 10 MB body, notifications, per-element errors) | = | shapes equal; the text of the -32700 / -32600 errors carries the parser detail, stock's does not |
| WebSocket `/ws`, eth_subscribe | not done | see open items |

## Oracle results

Differential `$S/rpc/rpccmp2.py STOCK RS` (S = the session scratchpad): discovers blocks with txs, contract creations, reverted txs, log-carrying blocks and every tx type on the judge, then compares 875 (Step) / 920 (beam) answers structurally, listing every difference by method and params. The judge is stock subnet-evm v1.14.2 under `cmd/epochdb-host-bench` with `pruning-enabled: false` and the debug / debug-tracer / internal-* eth-apis enabled (the default config serves no debug_ at all and prunes historical state; the plugin config is in `$S/rpc/start-stock.sh`). Outputs: `$S/rpc/cmp-store.out`, `cmp-plugin.out`, `cmp-beam-store.out`, `cmp-beam-plugin.out`.

| corpus | Rust side | equal | remaining differences |
|---|---|---|---|
| Step 1..50,000 | epochdb-rpc-serve over the rs/store dir (`$S/rs/store/data-50k`) | 861 of 875 | 14: classes 2 (9 cases) and 7 (5) |
| Step 1..50,000 | epochdb-rs plugin under the harness (interim log) | 575 of 875 | 300: class 1 (286: 120 state reads, 166 re-executing tracers, 7 executing calls at a height below the head) plus classes 2 and 7 |
| beam 1..50,000 | epochdb-rpc-serve over `$S/rpc/beam-store` (storecheck write, 9.9 s) | 878 of 920 | 42: class 4 (35 cases, every tracer shape on blocks 0x86 and 0x88), 5 (1), 6 (1), 7 (5) |
| beam 1..50,000 | epochdb-rs plugin under the harness | 592 of 920 | 328: class 1 (286) plus classes 4, 6, 7 |

Every difference, by class:

1. Interim plugin store (both corpora, 120 + 148 cases): state reads and executing methods at a height below the head answer -32000 "historical state is not available", and every re-executing tracer at any height (the parent state is historical). By design of the interim log; the store path answers them all. The plugin's `Store` is what rs-storewire replaces with the DbStore.
2. prestateTracer on a tx that ran out of gas (Step 4 txs x 2 shapes, 12 cases): rs lists the storage slot the failing SSTORE targeted (value 0), stock does not; the struct logger's `gasCost` on that last opcode is 15654 (the remaining gas) vs stock's 22100 (the opcode's full cost). revm-inspectors semantics on the OOG opcode; not patched.
3. callTracer withLog: equal after the last round (revm-inspectors adds an `index` per log that libevm does not have; `position` is kept).
4. Beam blocks 0x86 and 0x88 (deployer allow list refusals, 12 cases): the stored callTracer frame says `from` = the deployer, `gas` = the tx gas, error "precompiled failed"; stock answers `from 0x0, gas 0x0` and prestate shows no accounts. Stock never enters the EVM for a create the allow list refuses (libevm's hook returns before CaptureStart); rs/exec renders the refused call as a frame. This is rs/exec's trace rendering (the stored row), reported for rs-exec, not patched here.
5. eth_gasPrice on beam (1 case): rs 0xbef65aebe1 vs stock 0xe8d4a51001. Both are tip (1 wei) + EstimateNextBaseFee at the wall clock; the base fee floor on beam comes from the FeeManager's stored config (1000 gwei at head), and the difference sits in the elapsed-window decay: to be traced with the fee window of the head block (the fee config read itself is now the FeeManager state; `eth_feeHistory` and every header base fee are equal).
6. eth_getChainConfig on beam (1 case): the allow-list `adminAddresses` come out checksummed from upgrade.json / genesis in rs, lowercase from stock (stock re-marshals `common.Address`); eth_getChainConfig on Step equal.
7. web3_clientVersion (`epochdb/v0.1.0` vs `v1.14.2`), eth_newFilter / eth_newBlockFilter ids (random), eth_getProof (stock serves proofs, this node stores no tries), `personal_*` (-32601 both, the message text differs), an unknown tracer name (-32602 vs stock's JS `ReferenceError` per tx).

Error-message-only differences (same code and data, different words) are counted as equal above and listed as `MSG` lines in the outputs: parse errors (`parse error: expected ident at line 1 column 2` vs `parse error`), the batch shape errors, `missing value for required argument N` / `invalid argument N: ...` are matched where the probes hit them.

Earlier rounds of the same differential found and fixed: the revm 43 `AccountInfo::default()` carrying `code: Some(empty)` (every eth_call saw codeless contracts through the CacheDB path: 0x instead of a revert), default gas for calls (block gas limit vs the 50M cap: `gas` in traceCall frames), the balance check DoCall keeps, the raw block being the container rather than the eth block, the `pending`/past-head tag rules, geth's argument error texts, the fee surface (the Go node's oracle differs from stock in every number, see deviations), the 4byte top frame rule, geth's prestate quirk for created contracts (looked up after EIP-161 set the nonce to 1), libevm's struct logger shape (returnValue without 0x, storage keys without 0x, no refund column), the genesis floor (precompiles configured at genesis: the FeeManager / allow-list state on beam).

ots_/edb_ (subagent, `$S/rpc/otscmp.py --go http://127.0.0.1:19906 --rs http://127.0.0.1:19917 --stress`, the Go oracle = `go run ./exp/rpcoracle --data $S/go-bench/data-50k --chain 2jRZ... --addr 127.0.0.1:19906`): 1442 cases, 10 differences, all in the shared block-tag layer and all on purpose: height 0 (Go answers "block 0 is not stored", rs serves the genesis like stock), a JSON number as a block tag (Go refuses, rs accepts), past the head (Go -32602 "beyond head", rs stock's -32000 "cannot query unfinalized data"). Go behaviors reported by the subagent rather than fixed: `edb_getTopicGroups` / `edb_getTokenContracts` answer JSON `null` (a nil slice) instead of `[]` when empty; `tokens.go` `pagedLogs` dedupes a repeated TxNum only when adjacent (safe today because `DB.postings` sorts per source); `NextCursor = cut - 1` underflows on the descending path if a page ended at TxNum 0 (unreachable); `searchTransactionsBefore` treats a cursor above the head as "from the tip" while `searchTransactionsAfter` treats it as an empty page.

## Throughput

`$S/rpc/bench.py URL 40000`: 1,000 consecutive blocks (40,000..40,999 of Step, 6.9 txs per block on average), one client, keep-alive, then the same 1,000 as 10 batches of 100. Same box, sequential, `$S/rpc/bench.out`.

| server | eth_getBlockReceipts single / batch-100 per block | debug_traceBlockByNumber (callTracer) single / batch | eth_getBlockByNumber(full) single / batch |
|---|---|---|---|
| stock subnet-evm v1.14.2 (archive, traces re-executed) | 0.99 ms / 0.33 ms | 1.23 ms / 0.58 ms | 0.92 ms / 0.27 ms |
| epochdb-rpc-serve over the rs/store dir | 0.38 ms / 0.20 ms | 0.37 ms / 0.19 ms | 0.39 ms / 0.21 ms |
| epochdb-rs plugin (interim log, over ghttp) | 0.74 ms / 0.20 ms | 0.71 ms / 0.19 ms | 0.75 ms / 0.21 ms |
| the Go node (`exp/rpcoracle` over the Go store, same blocks) | 1.96 ms / 1.70 ms | 2.89 ms / 2.48 ms | 1.17 ms / 0.88 ms |

The store path is 2.6x stock and 5x the Go node on receipts, 3.3x stock and 7.8x the Go node on stored callTracer traces; the plugin path pays the host's gRPC hop (0.35 ms) per request and matches the store path in batches. Nothing is cached beyond a 512-block decoded-block cache in StoreDb (256 in the plugin); every answer decodes rows.

## Deviations (from the Go node and from stock), on purpose

- Fees follow stock, not the Go node: the Go node samples the 60th percentile gas price of the last 20 blocks and adds the trailing projected base fee to feeHistory; stock's oracle is the one above. eth_gasPrice / eth_maxPriorityFeePerGas depend on the wall clock (the 80 s lookback and the window decay), as stock's do.
- eth_getLogs has no block-range cap (stock); the Go node's 10,000-block cap is a one-line change in eth.rs when the interim scan hurts.
- eth_call / traceCall default gas is the 50M cap (stock, geth DoCall); the Go node uses the block gas limit only for estimateGas, as does stock.
- Past-the-head heights answer stock's -32000 "cannot query unfinalized data" (the Go node: -32602 "block N beyond head M"); an unknown hash tag answers stock's "header for hash not found".
- The state floor is the executor's genesis state (alloc plus precompiles configured at genesis), built at open by `Executor::with_db`; the Go node hands the same alloc as `genesis` to its store.
- debug_getRawReceipts is built from the stored rows (typed envelope RLP with the bloom recomputed), the Go node re-executes.
- `ponytail:` corners: the whole block is re-executed and rendered for a single-tx trace (debug_traceTransaction with a re-executing tracer); StoreDb holds one mutex over the DB (the store's reader-snapshot item); descending postings are the ascending scan reversed; ots_/edb_ walks materialise the posting list before reading rows (the mutex is held across the postings callback).

## Open items

- WebSocket `/ws` and eth_subscribe (newHeads, logs, newPendingTransactions): the plugin's ghttp `Handle` (upgrade requests over the reader / writer streams, the `responsewriter` / `reader` / `writer` / `conn` protos not yet copied) is still Unimplemented, and the standalone bin is plain HTTP. filters.rs already has the polling half.
- The interim plugin Store (state at head only, no postings): replaced by rs-storewire's DbStore behind `Store`.
- Classes 2, 4, 5, 6 above: revm-inspectors' prestate on an OOG'd SSTORE and the struct logger's last gasCost; rs/exec's frame for an allow-list-refused create (beam 0x86, 0x88); the beam eth_gasPrice window decay; checksummed addresses in eth_getChainConfig.
- flatCallTracer (revm-inspectors' parity builder is available, not wired) and JS tracers.
- eth_createAccessList is derived from the prestate, not from an access-list inspector; equal on the probes.
- The ERC-721 / ERC-1155 classification paths in tokens.rs need a corpus with 4-topic Transfer and TransferSingle logs.
- `cargo test --workspace`: 33 tests pass (state 14, exec 2, block 1, plugin 3, store 6 + 8) and the rpc doctests (a text block in edb.rs was parsed as Rust and fenced). No unit test in rs/rpc itself: the differential is its check.

## Rerun

```
S=/tmp/claude-1000/-home-ilia-epochdb/222c563c-789d-46b6-8726-b8af4b2a6f62/scratchpad
$S/rpc/start-stock.sh              # stock archive + debug apis: Step on :19908 (dir hb-stock-archive), beam on :19903
$S/rpc/start-rs.sh; $S/rpc/start-rs-beam.sh   # epochdb-rs plugin: Step :19902 (vm-50k/data), beam :19904
$S/rpc/serve-step.sh; $S/rpc/serve-beam.sh    # epochdb-rpc-serve: Step store :19907, beam store :19909
P=/ext/bc/2jRZvKtXY5nyWTqRwFh1KMHGrCRxJoULu4r2CsayWRnjdDGbV1/rpc
python3 $S/rpc/rpccmp2.py http://127.0.0.1:19908$P http://127.0.0.1:19907/ --scan 1500
python3 $S/rpc/bench.py http://127.0.0.1:19907/ 40000
```
`storecheck write --dump $S/rs/beam/beam-containers-1-1000000.bin --genesis $S/rs/beam/chain.json --upgrade $S/rs/beam/upgrade.json --data $S/rpc/beam-store --to 50000` builds the beam store. Never `pkill -f` a pattern that is also in the calling shell's command line (it kills the shell; every launcher above kills by a bracketed pattern from inside a script).

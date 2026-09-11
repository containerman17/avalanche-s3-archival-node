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

`Store` (lib.rs): head, block(h) with senders, hash_at, height_by_hash, tx_by_hash -> (height, index), receipts(h), traces(h) (the stored callTracer JSON per tx), container(h), state_at(h) -> StateRead (account / storage / code at the END of block h), log_candidates (postings, None = scan), and the TxNum-space accessors ots_/edb_ need (tx_range, height_of_tx, next_tx, postings(prefix, lo, hi, desc), groups, set_scan). Two impls: `storedb::StoreDb` (rs/store DB, lock-free reads on the DB's own version snapshots since rs-storewire; state = descent at the block's boundary TxNum with the executor-seeded genesis state as the floor, since the store holds no block-0 rows) and `plugin::rpc_store::PluginStore` (since rs-storewire: StoreDb on the engine's `Arc<DB>` plus the accepted head, the unappended tail and the head state from the executor's backend; before it the interim log, where historical state and postings answered "not available").

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
| WebSocket `/ws`, eth_subscribe / eth_unsubscribe (newHeads, logs, newPendingTransactions, newAcceptedTransactions) | = | rs-ws below: notifications byte-equal with stock on the live beam feed |

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
- `ponytail:` corners: the whole block is re-executed and rendered for a single-tx trace (debug_traceTransaction with a re-executing tracer); descending postings are the ascending scan reversed; ots_/edb_ walks materialise the posting list before reading rows (the mutex is held across the postings callback).

## Open items

- The interim plugin Store (state at head only, no postings): replaced by rs-storewire's DbStore behind `Store`.
- flatCallTracer (revm-inspectors' parity builder is available, not wired) and JS tracers.
- eth_createAccessList is derived from the prestate, not from an access-list inspector; equal on the probes.
- The ERC-721 / ERC-1155 classification paths in tokens.rs need a corpus with 4-topic Transfer and TransferSingle logs.
- `cargo test --workspace --release` on branch `rust` (everything merged): 44 tests pass (exec 15, plugin 1, rpc 5, state 2 + 6 + 12, store 3). rs/rpc's own unit tests are the fee window against Go vectors and the three ws.rs tests; the differential is the rest of its check.

## rs-tracefix: the four residual classes (Round 3)

Fixes to classes 2, 4, 5 and 6 of the Oracle results above, plus the module error texts the beam corpus surfaced. The spec is stock v1.14.2 under the same harness config; the Go source lines are libevm `core/vm/evm.go create`, `eth/tracers/native/call.go`, `eth/tracers/logger/logger.go`, `eth/tracers/native/prestate.go`, subnet-evm `plugin/evm/customheader/dynamic_fee_windower.go`, `eth/gasprice/gasprice.go`.

What changed:

- Class 4 (persistent data, `rs/exec`): libevm's `evm.create` refuses a create the deployer allow list denies (and a `CreateCollision`) before `CaptureStart` / `CaptureEnter`, so the tracer never learns of it. At depth 0 the callTracer keeps its untouched `callstack[0]` with `CaptureTxEnd`'s gasUsed: `{"from":"0x0000000000000000000000000000000000000000","gas":"0x0","gasUsed":"0x61a800","input":"0x","type":"STOP"}`; the struct logger answers `{"gas":6400000,"failed":false,"returnValue":"","structLogs":[]}` (no CaptureEnd, so no error); prestate `{}` (diffMode `{"post":{},"pre":{}}`); 4byte `{}`. At depth > 0 the refused CREATE leaves no child frame and the parent goes on (`SevmInspector::create` skips the tracer and swallows `create_end`). Frames libevm's `Call` / `Create` never enter (depth, balance, collision, nonce overflow) are pruned from the tree. The stored rows for beam 0x86 and 0x88 are now byte-equal to stock's `debug_traceBlockByNumber` output (checked from `epochdb-exec --traces-out` after the re-execution). Tx allow list refusal is a `preCheck` error in Go (`core/state_transition.go:255`): the block is invalid, no receipt and no frame on either side (rs bails per tx in `execute_block`). Unit test `refused_create_and_module_error_frames` in `rs/exec/src/tests.rs` covers the three tracers at depth 0, the nested refusal, the admin's normal create and a module error text, through `Executor::call` (same inspector and render path as the stored row).
- Module error texts (`rs/exec`, beam 0x7f27, 5 probes): libevm's frame `error` for a failed stateful precompile is `err.Error()` (`cannot modify allow list: modify address: 0x2772…, from role: NoRole, to role: EnabledRole`); revm-inspectors only knows "precompiled failed". The provider records each module's `Halt::Err` text per tx (`SevmPrecompiles::errors`) and the frame tree takes them in post-order (call_end order) on module addresses only.
- Class 2 (`rs/exec`, 9 Step probes): geth's interpreter logs the errored SLOAD / SSTORE through the deferred `CaptureState` with the full static + dynamic cost (`gasSStoreEIP2929`: cold 2100 + 20000 / 2900 / 100, 0 when the 2300 reentrancy sentry fails) while revm spends what is left; the prestate tracer's `CaptureState` returns on `err` before `lookupStorage`. `SevmInspector::step` computes the cost from the journal before the op runs and `step_end` records it when the op halted; the struct logger's `gasCost` is patched from that list and the first-loaded slots of errored ops are dropped from the prestate. Only on for the prestate / struct tracers (`oog_hook`), never for the stored row.
- Class 5 (`rs/rpc`): the root cause was the fee config read from the FeeManager's storage keys 0..7 instead of 1..8 (`feemanager/contract.go`: `Hash{byte(i)}` for i = 1..8), so `minBaseFee` was garbage and the wall-clock estimate decayed below the real floor. `eth_feeConfig` (with `lastChangedAt`) added; `fee_config_at` follows `GetFeeConfigAt` (DefaultFeeConfig before SubnetEVM, chain config without the FeeManager, else its state). The window arithmetic is now the free function `fee::next_base_fee` and its unit test `next_base_fee_matches_go` holds 47 vectors printed by `go run ./exp/feecheck synth` (customheader.EstimateNextBaseFee, the very call `eth_gasPrice` makes with `clock.Time().UnixMilli()`): over / under target, shifts of 0..1000 s, the `windowsElapsed > 1` multiplier, the exact-target early return, the floor. Live: `eth_gasPrice` equal to stock at three wall-clock seconds each on beam (`0xe8d4a51001`) and Step (`0x3b9aca01`); both heads are years old, so the live value is always the min base fee plus the 1 wei tip, and no block in either corpus 1..50000 carries a base fee above its floor (scanned), hence the Go vectors for the decay branches. `EPOCHDB_RPC_NOW=<unix s>` pins rs's clock for a deterministic comparison; `exp/feecheck URL BLOCK off...` prints stock's estimate for a live head at offsets.
- Class 6 (`rs/rpc`): stock re-marshals the parsed config: `common.Address` lowercase (allow-list roles, initialMint keys, rewardAddress) and the fields without `omitempty` present at their zero value (warp `quorumNumerator` 0 and `requirePrimaryNetworkSigners` false, also on a `disable` entry; `initialRewardConfig.allowFeeRecipients`). `stock_chain_config` applies both.

Probe counts after the fixes (`$S/tracefix/cmp-step.out`, `cmp-beam.out`; stores rebuilt with the new exec by `$S/tracefix/stores.sh`):

| corpus | before | after | remaining differences |
|---|---|---|---|
| Step 1..50,000, epochdb-rpc-serve over the rs/store dir | 861 of 875 | 869 of 875 | 6, all by construction (below) |
| beam 1..50,000, epochdb-rpc-serve over the rs/store dir | 878 of 920 | 914 of 920 | 6, all by construction (below) |

Remaining differences, each corpus: `web3_clientVersion` (`epochdb/v0.1.0` vs `v1.14.2`); `debug_traceBlockByNumber` with an unknown tracer name (stock evaluates it as a JS tracer and answers a per-tx `ReferenceError`, rs -32602); `eth_getProof` (stock serves Merkle proofs, this node stores no tries); `personal_listAccounts` (stock `[]` from its keystore under `internal-personal`, rs -32601: no accounts); `eth_newFilter` / `eth_newBlockFilter` (random ids). Message-only differences (same code and data): 4 on Step, 6 on beam (parse and not-found texts, `eth_sendRawTransaction` on a read server, `eth_estimateGas` fee-cap wording).

Re-execution: beam 1..1,000,000 with the fixed exec (`epochdb-exec --checkpoint 10000`): receipts, gasUsed and bloom equal on every block (asserted per block), roots ok at the 108 checkpoints and activations, 41 precompile txs, 101.6 s wall (`$S/tracefix/exec-1m.log`).

Rerun:
```
S=/tmp/claude-1000/-home-ilia-epochdb/222c563c-789d-46b6-8726-b8af4b2a6f62/scratchpad
# stock: Step :19920 (data hb-stock-archive), beam :19941 (data beam-stock), same CFG as start-stock.sh, from this worktree
bash $S/tracefix/stores.sh                      # rebuild $S/tracefix/{step,beam}-store with rs/target/release/storecheck
./rs/target/release/epochdb-rpc-serve --data $S/tracefix/step-store --genesis $S/rs/step/chain.json --upgrade $S/rs/step/upgrade.json --http 127.0.0.1:19942
./rs/target/release/epochdb-rpc-serve --data $S/tracefix/beam-store --genesis $S/rs/beam/chain.json --upgrade $S/rs/beam/upgrade.json --http 127.0.0.1:19943
python3 $S/rpc/rpccmp2.py http://127.0.0.1:19920/ext/bc/2jRZ.../rpc http://127.0.0.1:19942/ --scan 1500
python3 $S/rpc/rpccmp2.py http://127.0.0.1:19941/ext/bc/2tmrrBo1.../rpc http://127.0.0.1:19943/ --scan 3000
go run ./exp/feecheck synth                     # the Go vectors behind fee::tests::next_base_fee_matches_go
```
Kill servers by the pid `ss -ltnp` reports for the port; a `pgrep -f` pattern that also appears later in the same command line (a nohup launch) kills the calling shell.

## rs-ws: WebSocket `/ws` and eth_subscribe

Branch `rs-ws` on top of rs-rpc, 2026-09-09 02:00 to 04:00 JST, local box. Spec: stock subnet-evm v1.14.2's `/ws` (libevm `rpc/websocket.go`, `rpc/subscription.go`, `rpc/handler.go`; subnet-evm `eth/filters/api.go`), measured against the stock plugin under `cmd/epochdb-host-bench`.

### Design

- `rs/rpc/src/ws.rs` (new): one transport-agnostic `serve(&Server, stream)` over any `AsyncRead + AsyncWrite` stream, framing by `tokio-tungstenite` (server role, `from_raw_socket`; 32 MiB read limit = `wsDefaultReadLimit`). Requests go through the same `Server::handle_with` as HTTP (single and batch, same envelope, same errors) with a per-connection hook that intercepts `eth_subscribe` / `eth_unsubscribe`; everything else on the socket is the HTTP dispatch. Accepted heads reach every connection through a `tokio::sync::broadcast` channel on `Server` (`Server::publish(block, receipts_rlp)`, queue 20,000 = `maxClientSubscriptionBuffer`; a client that lags is disconnected). Idle ping every 30 s with a 30 s pong deadline, 10 s per write, `writeJSON`'s ping reset on every write. `handshake()` is gorilla's `Upgrader.Upgrade` check order (Connection, Upgrade, method, version, key) with its status codes and `Sec-Websocket-Version: 13` on refusal, so both transports (the plugin's ghttp and the plain TCP server) answer the same.
- Subscriptions: `newHeads`, `logs` (FilterCriteria: address list up to 1000, topics, from/to as `rpc.BlockNumber` with `SubscribeAcceptedLogs`' accepted combinations, `blockHash` exclusive with the range), `newPendingTransactions` (accepted, never fires: a follower has no mempool, and stock under the harness has an empty pool so it never fires there either), `newAcceptedTransactions` (hashes or full txs). A subscription records the head at creation and delivers heights above it only. Per head the notifications go out in subscription-id order, logs in receipt / log order with the block-wide `logIndex`. Ids come from `filters::new_id` (rpc.NewID's shape: 16 bytes hex, leading zeros trimmed), shared with the polling filters.
- Accept hook: `NodeEngine::accept` calls `rpc.publish(block, receipts)` right after the block is executed and its stats counted, before the record is handed to the checker / store writer; `publish` is a no-op with no subscribers. The store-path oracle (`epochdb-rpc-serve`) has no accept path, so its subscriptions answer but never fire.
- Plugin (`rs/plugin/src/ghttp.rs`, `vm.rs`): `Handle` implements avalanchego's hijack path (see rs/plugin/REPORT.md); `CreateHandlers` mounts `/rpc` and `/ws`. `Engine::ws_server()` (default None) is how an engine offers the `rpc::Server` for `/ws`.
- `epochdb-rpc-serve`: an `Upgrade` request on any path is handed to `ws::serve` on a tokio runtime; the HTTP/1.1 loop is unchanged otherwise.
- Harness: `--feed-delay <dur>` paces the feed (a sleep before every block) and logs `accepted height=N start_ns end_ns` per block, so a client can subscribe first and time its notifications against Accept.
- Tests: `ws::tests` (3): gorilla handshake (RFC 6455 key -> accept, the refusals), subscription bookkeeping (fan-out, since-gating, logs filter hit and miss, pending silent, unsubscribe and its errors), and `serve` end to end over a `tokio::io::duplex` with a tungstenite client (call, batch, subscribe, a published head arrives, unsubscribe stops it, clean close). `rs/rpc/scripts/ws_e2e.py` + `wsc.py` (stdlib only, no `websockets` module or `websocat` on this box): `capture URL N OUT` subscribes newHeads + logs + newPendingTransactions and collects N heads, then eth_getBlockByNumber, a batch (eth_chainId, eth_blockNumber, eth_getLogs) with notifications interleaving, eth_unsubscribe x3 plus the two error cases, and a clean close (expects the server's 1000 close frame); `diff A B` compares height by height with subscription ids masked; `latency CAP LOG` joins receive times with the harness lines; `static URL` is the no-live-heads set (subscribe answers and error texts, call, batch with an unknown method, notification without id, parse error).

### Results

Runs: beam chain, both plugins under the harness from the same 50k data dirs, `--feed-delay 500ms`, 50,401..51,200 (stock `$S/ws/beam-stock-data`, epochdb-rs `$S/rpc/beam-rs`; `$S/ws/beam-*.log`), and Step 50,081..50,500 at 1 s. Captures and outputs in `$S/ws/`.

- Through the host mount (`/ext/bc/<chain>/ws` -> ghttp `Handle` -> hijack -> `Conn` streams): 150 heads captured from each node in the same window, 149 common heights, **201 of 201 notifications byte-equal** with stock (149 `newHeads`, 52 `logs`) after masking the subscription id: same field set and order (`HeaderSerializable`: parentHash ... nonce, baseFeePerGas, blockGasCost, blobGasUsed, excessBlobGas, parentBeaconBlockRoot, timestampMilliseconds, minDelayExcess, hash; nulls present), same `eth_subscription` envelope, and the trailing newline `json.Encoder` writes. The Step run before the field-order fix had the same field set (0 byte-equal, order only).
- `static` passes on stock, on epochdb-rs through the host mount and on `epochdb-rpc-serve /ws`: identical subscription-error texts (`no "bogus" subscription in eth namespace`, `invalid argument 1: json: cannot unmarshal number into Go value of type filters.input`, `invalid from and to block combination: from > to`, `subscription not found`, `invalid argument 0: ... rpc.ID`, `missing value for required argument 1`, `too many arguments, want at most 1`); plain call, batch, unsubscribe, clean close with a 1000 close frame from the server. A non-upgrade GET on `/ws` through the host answers gorilla's 400 with `Sec-Websocket-Version: 13`, as stock.
- Latency, harness Accept to client receive (`newHeads`, 150 each, same box, 500 ms pace): epochdb-rs median 0.55 ms from Accept start / 0.16 ms from Accept return (p90 0.77 / 0.25, max 3.0 / 2.5); stock 1.81 / 1.25 ms (p90 3.38 / 2.41, max 11.4 / 7.0). Ours fires inside Accept (before the store write), stock's fires from the chain event after Accept.
- `cargo test --workspace` green (36 tests, rs/rpc 4 of them); `go vet ./cmd/epochdb-host-bench/` clean.

### Deviations from stock

- After a message that is not JSON, stock writes the -32700 error and drops the TCP connection (the client sees a reset); we write the same error and then a proper close frame.
- `newPendingTransactions` never fires (no mempool); stock's subscription exists and is fed by its txpool. Same observable behaviour under the harness.
- `eth_subscribe` over plain HTTP keeps rs-rpc's refusal (the Go node's code and text), unchanged.
- Origins are not checked (stock's `WebsocketHandler` is built with `[]string{"*"}` for the VM's handlers), compression is not negotiated (stock's `wsDefaultReadLimit` server does not enable it either), and there is no per-connection request concurrency: replies leave in request order, which is also how one client observes stock over a single connection.
- The 30 s ping / pong deadline is enforced from our side only; stock's client-side pings are answered by tungstenite automatically.

### Open items

- `newHeads` under reorgs: the follower never reorgs, so removed heads and `removed: true` logs never arise; nothing is implemented for them.
- The subscription id generator seeds from pid, a counter and the clock (rs-rpc's filters), not from `crypto/rand`; the shape is stock's.
- `epochdb-rpc-serve` runs one tokio runtime for ws sessions next to its thread-per-connection HTTP loop; a plain-HTTP session that upgrades late is not handled (the harness and clients upgrade on the first request).

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

## SAE mode (P3 rs-sae-rpc, branch `sae`)

Under Streaming Asynchronous Execution (ACP-194, rs/SAE.md) acceptance no
longer implies executed state: the ACCEPTED head (the chain height) runs `k`
blocks ahead of the SETTLED head (the last executed height, whose state,
receipts and traces are stored). The RPC never serves a wrong value for the
unsettled gap; it answers what is settled, and says "not settled yet" for the
rest.

### Two heads on the Store trait

- `Store::head()` = the SETTLED head (unchanged meaning: the last executed
  height). It is `latest` state and the ceiling for state / receipt / trace
  reads.
- `Store::accepted_head()` = the ACCEPTED head (the chain height). NEW, with a
  default of `head()`, so the synchronous model (accept implies execution, and
  every existing `Store` impl, `StoreDb` included) is unchanged: accepted ==
  settled, lag 0. A SAE store overrides it to report the two heads; the RPC
  code is identical for both, only the numbers differ.

`Server::head()` (settled), `Server::accepted_head()`, and
`Server::require_settled(n)` (the gate) sit on top.

### Resolution and gating

- `block_number(tag)` resolves a block/tx read to a height in `[0, accepted]`:
  a numeric or hash tag reaches an accepted-but-unsettled height (its block
  exists); the tags `latest` / `pending` / `safe` / `finalized` resolve to the
  SETTLED head, so a state read at a tag serves settled state and
  `eth_call("latest")` never lands in the unsettled band. Above the accepted
  head is the existing `ErrUnfinalizedData` ("cannot query unfinalized data",
  -32000).
- `require_settled(n)` refuses a height in `(settled, accepted]` with the
  defined error `not_settled` (code **-32011**, message
  "block N is accepted but not settled yet (settled head S, lag L)"). Never a
  value, never a silent `latest`.

### Per-method contract

| Method(s) | `latest` | at/below settled | accepted-but-unsettled | above accepted |
|---|---|---|---|---|
| eth_call, estimateGas, callDetailed, createAccessList, debug_traceCall | settled state | that height | -32011 not settled | -32000 unfinalized |
| eth_getBalance, getTransactionCount, getCode, getStorageAt | settled state | that height | -32011 not settled | -32000 unfinalized |
| eth_feeConfig | settled state | that height | -32011 not settled | -32000 unfinalized |
| eth_getBlockByNumber / ByHash, getHeaderBy*, getBlockTransactionCount*, getTransactionByBlock* , debug_getRawBlock / RawHeader | full block header + txs | full | **header + txs returned** (execution results are separate calls) | -32000 unfinalized |
| eth_getBlockReceipts, getTransactionReceipt, debug_getRawReceipts | settled | full | -32011 not settled | -32000 unfinalized |
| debug_traceBlockByNumber / ByHash, debug_traceTransaction | settled | full | -32011 not settled | -32000 unfinalized |
| eth_getLogs, eth_newFilter range | up to settled | full | range capped at the settled head (logs live only in settled receipts) | n/a |
| eth_getTransactionCount `pending` | settled nonce + pool pending | | | |

The block header at an unsettled height is returned verbatim, including
whatever `stateRoot` (settled root of `h-k`), `gasUsed` (worst-case) and
`receiptsRoot` the SAE header carries; only the block's OWN execution results
(its receipts, traces, own post-state) are gated, since those exist only once
`h` itself settles. So `eth_getBlockByNumber` at the accepted head returns the
block, and `eth_getBlockReceipts` / the tracers at that height answer -32011.

### Head / lag surface

- `eth_blockNumber` = the ACCEPTED head (the chain height).
- `edb_settledNumber` (alias `epochdb_settledNumber`) = `{settled, accepted,
  lag}` (lag = accepted - settled).
- `epochdb_head` carries `number` (= accepted), `accepted`, `settled`, `lag`,
  plus the accepted block's hash / timestamp and `txs`.
- WebSocket `eth_subscribe` records the ACCEPTED head as its baseline (heads
  fan out on accept).

Note: a block read at the `latest` tag resolves to the SETTLED head (the latest
fully-available block), while `eth_blockNumber` returns the accepted head. This
keeps `latest` state and `latest` block consistent and safe (never an unsettled
answer); a caller that wants the accepted tip reads `eth_blockNumber` (or
`epochdb_head`) and asks for that numeric height, which returns the block with
its execution results gated as above.

### Oracle

`rs/rpc/tests/sae.rs` drives the real dispatch (`Server::handle`) over a
synthetic `Store` with settled head 3 and accepted head 6: the settled-state
answers equal the store's recorded post-state at each settled height (the
stand-in for a re-execution's post-state), every height in `(3, 6]` returns the
-32011 not-settled error for state / receipts / traces and never a value, the
block itself is still returned at those heights, and the settled-head / lag
surface is correct. Proven against a TEST HARNESS, not a real SAE store: the
strong oracle (settled answers byte-equal a full synchronous EVM re-execution
at the settled height) needs the SAE store rs-sae-plugin builds, whose
`PluginStore` must override `accepted_head()` to the accepted head and keep
`head()` at the settled (executed) height; today's `PluginStore` reports one
head (accepted == settled), so it already serves correctly via the default and
gains the SAE behavior the moment those two heads diverge.

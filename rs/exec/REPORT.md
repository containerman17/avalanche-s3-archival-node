# rs/exec: subnet-evm block execution on revm (Round 1, rs-exec)

2026-09-08, branch `rs-exec`, crate `rs/exec` (standalone; depends on rs-block's
`rs/block` by path `../block`, which on this branch is NOT committed but checked
out from branch `rs-block` into the worktree: `git checkout rs-block -- rs/block
&& git restore --staged rs/block`; the integration workspace resolves it).

## Crate API

- `Config::from_genesis(genesis_json, upgrade_json, network_id) -> Config` (`src/config.rs`):
  chain id, fee config, allowFeeRecipients, timestamp forks (genesis values, network
  defaults for nil/0, `networkUpgradeOverrides` from upgrade.json), genesis precompile
  configs, `precompileUpgrades`, alloc. `spec(time) -> SpecId`, `activating(parent_time,
  time)`, `precompile_enabled(addr, time)`.
- `Executor::new(cfg)` (`src/exec.rs`): in-memory state (`revm::database::CacheDB<EmptyDB>`)
  holding what genesis materialises (alloc + precompiles enabled at genesis, vmexec
  `trieAlloc`). `execute_block(&block::Block, parent_time) -> BlockResult { gas_used,
  receipts_root, bloom, txs: Vec<TxResult { hash, status, gas_used, cumulative_gas_used,
  receipt: ReceiptEnvelope, trace_json, rows: Vec<StateRow> }>, tail (block-level rows:
  precompile activations), code (deployed bytecode by hash) }`. `StateRow` is the store's
  contract form: account = RLP[nonce, balance, codeHash], slot = left-trimmed word, empty =
  delete, plus the code-use row. `set_block_hash(number, hash)` feeds BLOCKHASH;
  `db()`/`db_mut()` expose the state; `t_evm/t_trace/t_commit` are the time split.
- `oracle::state_root(&CacheDB) -> B256` (`src/oracle.rs`): full secure-trie recompute
  with alloy-trie (`state_root_unhashed` over `storage_root_unhashed`).
- `feemanager` (`src/feemanager.rs`): the FeeManager stateful precompile over the revm
  journal, and `configure()` for activation.
- Binary: `epochdb-exec --dump FILE --genesis chain.json --upgrade upgrade.json [--to N]
  [--checkpoint 1000] [--workers 4] [--traces-out f.jsonl --traces-from A --traces-to B]`.
  Replays a container dump, checks every block's gasUsed / receiptsRoot / logsBloom against
  the header and the state root at every checkpoint, prints a bench line. `--genesis` takes
  the epochdb chain descriptor (base64 `genesisData` + `networkID`) or the bare genesis.
- `tools/trace_diff.py OUR.jsonl CACHE_DIR [--max-heights N] [--raw]`: the trace oracle
  against the door's `debug_traceBlockByNumber` callTracer (normalized and byte compare).
- `cargo test`: config parse + FeeManager slot layout self-check.

Design: revm 43 `Evm` with a custom `Handler` (`SevmHandler`), a custom
`PrecompileProvider` (`SevmPrecompiles`) and `revm_inspectors::TracingInspector`.
The Executor owns the Evm and therefore the DB across blocks (revm's Context owns
the Database); the plan's `execute_block(cfg, block, parent, &mut state)` shape would
need a Context rebuild per block, so the state is owned instead.

## subnet-evm rules implemented (the Go source is the spec)

| rule | Go | here |
|---|---|---|
| no gas refunds at all | `core/state_transition.go refundGas(subnetEVM)`, `params/hooks_libevm.go ShouldRefundGas = !IsSubnetEVM` | `SevmHandler::refund` zeroes the refund counter |
| coinbase gets gasUsed * effectiveGasPrice, base fee not burned | `state_transition.go TransitionDb` | `SevmHandler::reward_beneficiary` |
| coinbase = header.Coinbase; allowFeeRecipients is only a header rule | `consensus/dummy Author`, `verifyCoinbase` | `BlockEnv.beneficiary = header.coinbase` |
| DIFFICULTY = header (1) pre-Durango, PREVRANDAO = difficulty bytes after | libevm `NewEVM` (`IsMerge = Random != nil`), subnet-evm `core/evm.go OverrideNewEVMArgs` | spec LONDON / SHANGHAI (Durango = ShanghaiTime) / CANCUN (Etna = CancunTime), `prevrandao = B256(difficulty)` |
| stateful precompile runs only while its config is active and is never warm at tx start | `params/hooks_libevm.go PrecompileOverride`, `ActivePrecompiles` (only P256Verify under Granite) | `SevmPrecompiles`: `warm_addresses` = eth set, `run` dispatches 0x0200..03 |
| ApplyUpgrades at activation (parent < ts <= block): nonce 1, code 0x01, Configure; disable = SelfDestruct | `core/state_processor_ext.go ApplyPrecompileActivations` | `Executor::activate`, DB commit before the first tx, rows in `tail` |
| FeeManager storage layout, gas (20k/write, 5k/read slot), allow-list roles and CanModify, strict input lengths pre-Durango, any error reverts the frame with all gas | `precompile/contracts/feemanager/contract.go`, `precompile/allowlist/*.go`, `precompile/contract/utils.go`, libevm `evm.call` | `feemanager.rs` |
| mainnet/fuji upgrade schedule, `SetDefaults`, `Override` | avalanchego `upgrade/upgrade.go`, `params/extras/network_upgrades.go` | `config.rs` (Durango 1709740800, Etna 1734368400, Granite 1763568000) |
| genesis = alloc + precompiles enabled at genesis | `vmexec/genesis.go trieAlloc` | `Executor::new` |
| Durango init code size | `TransitionDb` MaxInitCodeSize | EIP-3860 under SHANGHAI |
| receipts: typed encoding, bloom, root | libevm `types.Receipt`, `DeriveSha` | alloy-consensus `ReceiptEnvelope::encode_2718` + `ordered_trie_root_with_encoder` |
| callTracer JSON | libevm `eth/tracers/native/call.go` | `TracingInspector` + `set_transaction_gas_limit(tx gas limit)` + `geth_call_traces` |
| block end | `dummy.Finalize` only verifies the block gas cost; vmexec skips it | nothing runs; EIP-158 deletion is the journal's, CacheDB fixed up in `Executor::commit` |

Not implemented (hard errors at parse or exec, never silently wrong): the other
precompile modules (deployer allow list, tx allow list, native minter, reward manager,
warp), `stateUpgrades`, the Durango precompile events (`RoleSet`, `FeeConfigChanged`)
and `setManager`, Granite (P256Verify at 0x100, precompile delegatecall revert), the
`InvalidateDelegateUnix` (1754107200) delegatecall invalidation, EIP-4788 beacon root
processing post-Etna, block-number forks other than 0. None occur on Step 1..1,000,000
(block 1M is 2022-10-20, all pre-Durango; FeeManager activates at 1675252800, later).

## Oracle results (all pass)

Local (i7-10700K), `step-containers-1-50000.bin` and `step-containers-1-1000000.bin`:
- gasUsed, receiptsRoot, logsBloom equal to the header on EVERY block 1..1,000,000
  (4,087,552 txs, 451.7 Ggas).
- stateRoot equal to the header at every checkpoint: 1000, 2000, ..., 50000 on the 50k
  file (50 checkpoints) and 50000, 100000, ..., 1000000 on the 1M file (20 checkpoints;
  state at 1M: 9,284 accounts, 3,283,266 slots, 3.3 s per full recompute).
- callTracer: `tools/trace_diff.py` over 224 heights covering all 13 call shapes seen in
  1..50000 (CALL/CREATE roots, nested CALL/STATICCALL/DELEGATECALL/CREATE2 to depth 3,
  the `out of gas` failures) plus the 10 busiest blocks: 3,187 txs, 3,187 identical after
  normalization and 3,187 byte-identical to the door's stored frames (same key order,
  lowercase hex, same omitted empties). The one difference found on the way: the root
  frame's `gas` is the tx gas limit (`CaptureTxStart`), fixed with
  `set_transaction_gas_limit`. No semantic difference remains (gas, gasUsed, output,
  error, revertReason, calls, value, type).

Box (Tokyo, Xeon 8559C 8 vCPU, the live fleet on it, nice 10, static musl binary at
`/data/epochdb-v0/tmp/rs/epochdb-exec`, log `exec-run.log`): the same oracles pass on
both files (checkpoints every 10000 / 50000), root ok at h=1000000.

## Throughput (one execution thread, in-memory HashMap state)

"exec-thread" divides by the executor's own time (EVM + receipts + trace JSON + rows +
commit), "wall" by elapsed time including waiting for rs-block's recovery pool
(`--workers`) and the checkpoint roots.

| where | blocks | workers | exec-thread | wall |
|---|---|---|---|---|
| local | 1..50000 | 14 | 10.7k blk/s, 73k tx/s, 4663 mgas/s | 9.5k blk/s, 65k tx/s, 4130 mgas/s (5.3 s) |
| local | 1..50000 | 1 | 12.0k blk/s, 82k tx/s, 5229 mgas/s | 3.6k blk/s, 25k tx/s, 1585 mgas/s (9.1 of 13.8 s waiting on recovery) |
| local, `--traces-out` (110 MB JSON) | 1..50000 | 14 | 9.8k blk/s, 67k tx/s, 4286 mgas/s | |
| local | 1..1000000 | 14 | 2.2k blk/s, 9.1k tx/s, 1005 mgas/s (449 s: evm 392 s, trace JSON 42 s, commit 5 s) | 2.0k blk/s, 8.4k tx/s, 924 mgas/s (489 s, roots 37 s) |
| box, nice 10 | 1..50000 | 6 | 6.7k blk/s, 46k tx/s, 2941 mgas/s | 5.9k blk/s, 40k tx/s, 2567 mgas/s (8.5 s) |
| box, nice 10 | 1..50000 | 1 | 8.2k blk/s, 56k tx/s, 3573 mgas/s | 3.1k blk/s, 21k tx/s, 1361 mgas/s |
| box, nice 10 | 1..1000000 | 6 | 1.5k blk/s, 6.2k tx/s, 684 mgas/s (660 s) | 1.4k blk/s, 5.7k tx/s, 628 mgas/s (720 s, roots 51 s) |

The workload changes character: blocks 1..600k are transfer-heavy (63 kgas per tx, half
plain transfers; 4 Ggas/s is mostly per-tx fixed cost), from ~700k the blocks are a few
txs of 0.8 to 1.4 Mgas each with 64 nested STATICCALLs per tx (door traces at 900k,
980k, 999k), where the executor does 600 to 750 mgas/s (local) and 400 to 500 (box).
Per 50k-block window, local: 4192 mgas/s at 50k, 2270 at 500k, 1004 at 700k, 747 at 1M.
The Go reference (676 mgas/s cumulative at 2.66 cores, early-window, with root checks and
store writes) is not like for like: this has no keccak on state keys except at checkpoints,
no store, no per-block root; rs-node's box comparison in sequential windows is the real one.
Sender recovery is the wall-time bottleneck with one worker (same finding as the Go profile).

## Deviations and open items

- State ownership: `Executor` owns the revm `Evm` and the `CacheDB`. rs-node can hand in
  its own `Database + DatabaseCommit` by making `Executor` generic over the DB (only the
  root oracle needs `CacheDB`).
- CacheDB semantics: an EIP-158-deleted account stays as an empty `Touched` entry with its
  storage; `Executor::commit` replaces it with `NotExisting` so a later CREATE2 at that
  address cannot inherit stale slots. The root oracle also skips empty accounts.
- BLOCKHASH: every executed block's hash is kept in `CacheDB.cache.block_hashes`
  (unbounded; the 256-window prune is rs-node's).
- The per-tx state rows come from revm's finalized journal state (touched accounts,
  changed slots, created code), not from a trie interceptor; row order differs from the Go
  capture (map-ordered there too). Not oracle-checked here.
- Durango and later: spec mapping and EIP-3860 wired; FeeManager Durango events and
  `setManager`, Granite/P256Verify, beacon root: not (hard errors). Step never reaches them
  in 1..1M; Beam and other chains will.
- Trace `error` strings for stateful precompile failures come from revm
  (`PrecompileError`) rather than the Go error text; no such tx exists in the range checked.
- Throughput on the contract-heavy tail (600 to 750 mgas/s local) is unprofiled: the
  candidates are the TracingInspector hooks (always on), CacheDB HashMap misses at 3.3M
  slots, and revm's per-frame cost on 64 STATICCALLs per tx. perf is not installed here or
  on the box.

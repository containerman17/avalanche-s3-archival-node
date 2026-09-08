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

# PHASE 2: the subnet-evm precompile set, stateUpgrades and the later forks (rs-precompiles)

2026-09-09, branch `rs-precompiles` (rs-exec + rs-block + rs-state + rs-node merged, so the
executor is the generic `Executor<D: StateDb>`; nothing pushed). The Go module
`github.com/ava-labs/avalanchego/graft/subnet-evm@v1.14.3-0.20260804141953-6dc4c3b395b6` is
the spec. Nothing ran on the Tokyo box; the v1-configs were copied out read-only. No Go
file was edited; one new Go command.

## Block source: `cmd/epochdb-dump-fetch`

The fetch package exactly as `cmd/epochdb-vm` wires it (`chain.Resolve` with the cached
chain.json + upgrade.json in `--data`, `fetch.New`, `StartForward` from `--from` anchored on
the genesis hash or `--anchor` (the eth hash of `--from` minus one), `SetCeiling(--to)`,
`Follow` for the tip), writing `[u64 LE height][u32 LE len][container]` plus chain.json and
upgrade.json beside the dump. Beam (2tmrrBo1Lgt1mzzvPSFt73kkQKFas5d1AP88tv9cicwoFp8BSn,
233 validators, 54 archival) from this machine: heights 1..1,000,000 in 3 m 47 s at
4,400 blk/s, 2,453 MB (`$SCRATCH/rs/beam/beam-containers-1-1000000.bin`), plus five
5,000-block windows anchored on hashes from the public RPC (`fork_heights.json`): Durango
1901030, warp activation 3227132, warp re-enable with requirePrimaryNetworkSigners 4029216,
InvalidateDelegateUnix 5597742, Granite 6970224 (11 s each).

## Modules (crate `rs/exec`, the Go file is the spec)

| module | Go | here |
|---|---|---|
| shared plumbing: addresses 0x0200..05, gas constants, geth ABI unpack rules (empty input, short head, `uint32` range, dynamic `bytes` bounds), journal sload/sstore/log, error to `PrecompileError` + all gas | `precompile/contract`, `params/hooks_libevm.go makePrecompile`, libevm `evm.call` | `src/precompile.rs` |
| allow list: role slot `BytesToHash(addr)`, roles 0/1/2/3, `CanModify`, setAdmin/setEnabled/setNone, setManager only from Durango (`invalid non-activated function selector`), readAllowList, strict 32-byte input pre-Durango, `RoleSet(role,account,sender,oldRole)` event (2131 gas) post-Durango, `Configure` order enabled, admins, managers | `precompile/allowlist/*.go` | `src/allowlist.rs` |
| deployer allow list: `CanCreateContract(tx.origin)` on every CREATE/CREATE2/create-tx while active: caller nonce bumped, created address warmed, all gas consumed, `PrecompileError` | `params/hooks_libevm.go`, libevm `evm.libevm.go canCreateContract` (after the collision check) | `SevmInspector::create` in `src/exec.rs`, role read without warming (`read_state_no_warm`) |
| tx allow list: sender role checked in `preCheck` (tx invalid = block invalid) | `core/state_transition.go` | asserted per tx in `execute_block` (bail), the precompile itself is the shared allow list |
| native minter: `mintNativeCoin(address,uint256)` 30,000 gas, enabled/admin/manager, `NativeCoinMinted` (1756 gas) post-Durango, strict 64-byte input pre-Durango, `initialMint` at activation | `precompile/contracts/nativeminter` | `src/nativeminter.rs` |
| reward manager: slot `rask` = `afrav` / `BytesToHash(BlackholeAddr)` / reward address; allowFeeRecipients, setRewardAddress (pre-Durango only `len % 32`), disableRewards, currentRewardAddress, areFeeRecipientsAllowed; the three Durango events; `Configure` = initialRewardConfig, else the chain's allowFeeRecipients, else disabled | `precompile/contracts/rewardmanager` | `src/rewardmanager.rs`; execution still pays `header.Coinbase` (the coinbase rule is `verifyCoinbase`, a header check) |
| fee manager: as PHASE 1 plus `FeeConfigChanged(sender, old, new)` (45,221 gas: GetFeeConfig + log + 512 data bytes) post-Durango, setManager | `precompile/contracts/feemanager` | `src/feemanager.rs` over the shared allow list |
| warp: `getBlockchainID`, `sendWarpMessage(bytes)` (41,500 + 8 per input byte; `SendWarpMessage(sender, messageID, message)` log with the unsigned message bytes; id = sha256), `getVerifiedWarpMessage/BlockHash(uint32)` over the tx's predicates and the header's failed bitset (`(bytes32,address,bytes),bool` and `(bytes32,bytes32),bool` outputs), avalanchego linearcodec for UnsignedMessage / Message / BitSetSignature / AddressedCall / Hash, `predicate.New/Bytes` (0xff delimiter), `BlockResults` codec of `header.Extra[80..]` | `precompile/contracts/warp`, `vms/evm/predicate`, `vms/platformvm/warp` | `src/warp.rs` |
| predicate check per tx: warp access-list entries are predicates; intrinsic gas replaces `2400 + 1900 per key` with `PredicateGas` (base 200,000 / 125,000 Granite + per chunk 3,200 / 512 + per signer 500 / 250; an unparseable message invalidates the tx: bail) | `core/predicate_check.go`, `graft/evm/precompileconfig.AccessListGasWithPredicates` | `SevmHandler::validate_initial_tx_gas` delta, `execute_block` |
| predicate verification: `ValidatorState` trait (`subnet_id(chain)`, `validator_set(pchain_height, subnet)` = the canonical `WarpSet`: keys merged, sorted by uncompressed key, weight summed, total incl. keyless), primary-network source rule with `requirePrimaryNetworkSigners`, quorum `sigWeight*100 >= total*num`, BLS via `blst` (uncompress + validate the signature, aggregate the signer keys, verify with DST `BLS_SIG_BLS12381G2_XMD:SHA-256_SSWU_RO_POP_`); results compared bit for bit with the header's | `warp/config.go VerifyPredicate`, `platformvm/warp/signature.go`, `snow/validators/warp.go FlattenValidatorSet` | `warp::verify_predicate`; `Executor.validator_state: Option<Box<dyn ValidatorState>>`; without one the header's results are used as they are (`predicates_trusted` counts them) |
| the context height the predicates are verified at | proposervm `block.go`: parent's P-chain height pre-Etna, the block's own from Etna, the epoch's under Granite | `rs/block` now parses `Epoch.PChainHeight` of a Granite block; the executor keeps `prev_pchain_height` |
| stateUpgrades: per account create if absent, `AddBalance`, `SetCode` (nonce 1 when 0), `SetState`; after the precompile activations of the same block | `core/state_processor_ext.go ApplyUpgrades`, `stateupgrade/state_upgrade.go` | `Config.state_upgrades`, `Executor::apply_state_upgrade` |
| activation: disable = SelfDestruct + Finalise (a re-enable in the same block starts clean), enable = nonce 1, code 0x01, `Configure` writes possibly to other accounts (initialMint) | `ApplyPrecompileActivations` | `Executor::activate` over a small op list, one commit per account |
| genesis: alloc first, then the precompiles enabled at genesis (an initialMint adds to an alloc balance) | `core/genesis.go` | `Executor::with_db` |

Forks (`params/extras/network_upgrades.go`, mainnet 1709740800 / 1734368400 / 1746057600 /
1763568000, fuji 1707840000 / 1732550400 / 1744648800 / 1761750000, `networkUpgradeOverrides`
applied):
- Durango: SHANGHAI spec (PHASE 1), strict input lengths off, the events above, setManager,
  predicates allowed.
- Etna: CANCUN spec (PHASE 1); the P-chain context height becomes the block's own. Fortuna:
  no effect on subnet-evm (parsed, `is_fortuna`).
- InvalidateDelegateUnix 1754107200: a DELEGATECALL/CALLCODE into a stateful precompile
  invalidates the tx (so the block): hard error here.
- Granite: P256Verify at 0x100 (6900 gas, revm's `p256_verify_osaka`), warm at tx start
  (`ActivePrecompiles`); DELEGATECALL/CALLCODE into a stateful precompile reverts with all its
  gas; the warp gas config switches; the header's TimeMilliseconds / MinDelayExcess are
  header-only (`plugin/evm/customheader`), nothing else changes execution.
- EIP-4788: `ProcessBeaconBlockRoot` is a call into an address with no code on every
  subnet-evm chain (no alloc puts code there), so it is a no-op and is not run.

## Oracles

Beam 1..1,000,000 (`exec-1m.log`, local i7, `--checkpoint 10000 --workers 12`): gasUsed,
receiptsRoot and logsBloom equal to the header on EVERY block (1,358,991 txs, 69.6 Ggas,
41 txs to precompile addresses); stateRoot equal at 108 heights: every 10,000th block
(100) and every activation height and the block after it: 2203/2204 (native minter
1692702000), 44322/44323 (txAllowList 1698760800), 44349/44350 (txAllowList disable
1698771600), 44383/44384 (deployer allow list disable 1698775200). 92 s wall, 37 s executor
(26.7k blk/s, 1859 mgas/s exec-thread), 53 s in the 108 root recomputes. The trap: without
`--upgrade` the same run fails at block 2204 (`gasUsed 21632 != header 51632`, the mint
falls through to a plain call), so the config path is exercised.

Later windows (5,000 blocks each, fetched): a window needs the state before it, which only
the archive RPC has; `--from N --rpc URL` replays over `CacheDB<RpcDb>` (eth_getBalance /
getTransactionCount / getCode / getStorageAt at N minus 1, cached to disk) with the receipts
oracle only. Both public beam RPCs answer at 1 to 3 calls per second, so only a few blocks
per window fit the time box: see the window lines appended below (or "not reached").
`platform.getValidatorsAt` on api.avax.network refuses numeric heights ("Unsupported height
parameter value"); publicnode's P-chain answers them, so `--pchain
https://avalanche-p-chain-rpc.publicnode.com/ext/bc/P` is the `ValidatorState` feed
(`RpcValidatorState`, cached).

Unit tests (`cargo test -p epochdb-exec`, 14 tests): the allow-list role transition matrix
of `allowlisttest/test_allowlist.go` (admin / manager / enabled / none callers against every
target role, pre and post Durango, events, read-only, out of gas, padded input), the
nativeminter, rewardmanager and feemanager `contract_test.go` tables (same input, gas,
output, error class, storage and logs after), warp `contract_test.go` (send with log and id,
getVerified success / invalid / non-zero index / failed bitset / out of gas / bad packing /
bad message / bad payload / index errors, block hash variant), the `predicate_test.go` and
`results_test.go` byte vectors, PredicateGas, and BLS verification with generated keys
(canonical ordering, keyless weight in the total, quorum 33 vs 67, wrong signer set, index
past the set, `verify_predicate` through a fake `ValidatorState`). Plus the config test:
overrides, stateUpgrades, initialMint / initialRewardConfig / warp fields, unknown keys,
cb58 round trip.

## Fleet inventory (v1-configs from the box, 52 dirs, `EPOCHDB_V1_CONFIGS=... cargo test`)

All 51 chains with a chain.json parse (`Config::from_genesis`) except pandasea; lamina1 has
only an upgrade.json (rewardManagerConfig + contractNativeMinterConfig, no genesis to parse).
Genesis keys `contractFeeManagerConfig` (nftchain, smartchain, vyochain) and
`eUpgradeTimestamp` (space, straitsx) are unknown to subnet-evm and ignored by it, so ignored
here. `gasPriceManagerConfig` (0x0200..06) is registered in this subnet-evm but used by no
chain: a parse error here.

| chain | genesis precompiles | upgrade.json | stateUpgrades | forks in genesis |
|---|---|---|---|---|
| andromeda, blaze, even, fifa, tixchain, kite, kula, mj0714ms1, thegrotto, ttchain, turing, rwachain, ivorygopher, technicalivorygopher, doschain, apertum, datagram (feeManager+reward+warp), watr | nativeMinter (most), feeManager, rewardManager, warp | none | 0 | durango/etna explicit on some |
| athera, frqtal, hanchain, hanchain16888 | warp only | none | 0 | |
| vlxl1 | feeManager, warp | none | 0 | |
| vyochain | rewardManager, warp (+ ignored contractFeeManagerConfig) | none | 0 | |
| nftchain, smartchain, omnicoin, depchain | nativeMinter, rewardManager, warp | none | 0 | |
| blockticity, dinari, northinv, northinvestments | deployerAllowList, nativeMinter, txAllowList, feeManager, rewardManager, warp | none | 0 | |
| cxchain, gunz, henesys | deployerAllowList, feeManager, rewardManager, warp | none | 0 | |
| hashfire | deployerAllowList, feeManager, rewardManager, warp | deployerAllowList disable | 0 | |
| loyal, orange, numine | deployerAllowList, nativeMinter, feeManager, rewardManager, warp | none (numine: etna override 1763398800 over genesis 253399622400) | 0 | |
| titan | deployerAllowList, nativeMinter, feeManager, rewardManager, warp | none | 1 | |
| beam | deployerAllowList, feeManager, rewardManager | nativeMinter, txAllowList on/off, deployerAllowList off, warp on/off/on(requirePrimaryNetworkSigners) | 0 | |
| dexalot | deployerAllowList, nativeMinter, feeManager | rewardManager, warp on/off/on | 0 | |
| dfk | deployerAllowList, nativeMinter | feeManager, warp | 1 | |
| numbers | deployerAllowList, nativeMinter, feeManager | deployerAllowList off, warp | 0 | |
| playa3ull | deployerAllowList, nativeMinter, feeManager | warp on/off/on | 4 | |
| uptn | deployerAllowList, nativeMinter, txAllowList, feeManager, rewardManager | warp | 4 | |
| innovo | deployerAllowList, txAllowList, feeManager, warp | warp off/on | 4 | durango |
| straitsx | nativeMinter, feeManager, warp | warp off/on | 4 | durango |
| space | nativeMinter, feeManager, rewardManager, warp | warp off/on | 0 | durango |
| step | none | feeManager | 0 | |
| pandasea | deployerAllowList, feeManager, rewardManager, warp | txAllowList on/off, then `txBlockListConfig` | 0 | |

pandasea: its upgrade.json's third entry `txBlockListConfig` is not a registered module in
this subnet-evm version either (`PrecompileUpgrade.UnmarshalJSON` returns "unknown precompile
config"), so the Go node on this module version would refuse the file too; the Rust parser
refuses it the same way. Every module any chain references (the six) exists in Rust.

## Deviations and open items

- Deployer allow list refusal: Go checks after the depth and balance checks; the inspector
  hook here runs before revm's own, so a refused create at depth 1024 or with an
  insufficient value consumes all gas instead of returning it (never seen; needs a
  frame_init override to match).
- Trace `error` strings for refused creates and precompile failures are revm's, not Go's
  (trace JSON only).
- The predicate results of a block are taken from `header.Extra` when no `ValidatorState`
  is given (counted as `predicates_trusted`); with one, ours must equal the header's or the
  block fails. The pre-Etna context height needs the previous block's proposervm height,
  unknown for a window's first block (trusted then).
- `Executor::resume` + `CacheDB<RpcDb>` is an oracle tool; the node's `Backend` path is
  unchanged (`with_db`).
- `WarpSet::flatten` uncompresses each key (blst); a 1,000-validator primary set costs about
  a second, cached per (height, subnet).

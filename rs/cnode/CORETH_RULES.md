# coreth rules for a revm follower of mainnet C-chain (post-Granite)

Sources read: coreth at `/home/ilia/avalanchego/graft/coreth` (commit c4e4e55eff),
libevm `v1.13.15-0.20260622141141-097921408ecf` (the version coreth's go.mod pins, in
`/home/ilia/go/pkg/mod/github.com/ava-labs/libevm@v1.13.15-0.20260622141141-097921408ecf`),
`/home/ilia/avalanchego/upgrade/upgrade.go`, and a live query of `https://api.avax.network/ext/bc/C/rpc`
on 2026-09-12 (head 0x5aa131a = 95,097,626).

Scope: everything that changes accounts, balances, nonces, code, or storage differently from
a plain Ethereum revm run, plus the encoding facts a follower needs to hash the same state root.
Rules that only decide block validity (never state) are marked "validity only" and can be skipped
because we only replay accepted blocks.

---

## 1. Fork schedule and Ethereum spec mapping

`upgrade/upgrade.go` `Mainnet`:

| Avalanche fork | mainnet time (UTC) | unix |
|---|---|---|
| Durango | 2024-03-06 16:00 | 1709740800 |
| Etna | 2024-12-16 17:00 | 1734368400 |
| Fortuna | 2025-04-08 15:00 | 1744124400 |
| Granite | 2025-11-19 16:00 | 1763568000 |
| Helicon | `UnscheduledActivationTime` (9999-12-01) | not active |

All of Durango, Etna, Fortuna, Granite are active on every block we replay. Helicon is not
scheduled (`upgrade.go` line 41, `HeliconTime: UnscheduledActivationTime`).

How coreth maps them to geth forks: `params/config_extra.go` `SetEthUpgrades`:
- Homestead through MuirGlacier at block 0, Berlin at block 1640340, London at block 3308552 (mainnet).
- `ShanghaiTime = DurangoBlockTimestamp`
- `CancunTime = EtnaTimestamp`
- Prague/Verkle: never set. This libevm has `PragueTime` in its `ChainConfig` (libevm `params/config.go`
  line 354) but coreth never sets it and libevm has no Prague EVM logic at all (no EIP-7702, 7623,
  2935, 7002, 7251, 2537 code exists in this libevm; `core/vm/jump_table.go` ends at
  `newCancunInstructionSet`).

So the EVM spec for every block after Etna is geth Cancun, with the deviations below.

Per-EIP status on mainnet C after Granite:

| EIP | status | where |
|---|---|---|
| 1153 transient storage, 5656 MCOPY, 6780 SELFDESTRUCT-same-tx, 4844 BLOBHASH opcode, 7516 BLOBBASEFEE opcode | on (Cancun) | libevm `core/vm/jump_table.go` `newCancunInstructionSet` |
| 3860 initcode limit and per-word gas | on. Intrinsic gas: libevm `core/state_transition.go` `IntrinsicGas` under `rules.IsShanghai`. Size limit: coreth `core/state_transition.go` `TransitionDb` checks `rulesExtra.IsDurango && len(msg.Data) > MaxInitCodeSize` (same time as Shanghai) | |
| 3651 warm coinbase, 3855 PUSH0 | on (Shanghai) | libevm |
| 4844 blob transactions | off. Blocks may not contain blob txs; header must have `BlobGasUsed == 0`, `ExcessBlobGas == 0` | `plugin/evm/wrapped_block.go` `syntacticVerify` (errBlobsNotEnabled). Live header confirms `blobGasUsed: 0x0, excessBlobGas: 0x0` |
| 4788 beacon roots | header `ParentBeaconRoot` is REQUIRED and must be the zero hash (`wrapped_block.go` `syntacticVerify`: errParentBeaconRootNonEmpty). Because it is non-nil, `core/state_processor.go` `Process` DOES call `ProcessBeaconBlockRoot(0x00..00, ...)` every block. It is a zero-value call from `SystemAddress` to `0x000F3df6D732807Ef1319fB7B8bB8522d0Beac02`. That address has no code and no account on mainnet (live `eth_getCode` = `0x`), so libevm `core/vm/evm.go` `Call` returns at the `!Exist(addr) && IsEIP158 && value.IsZero()` check before any state touch. Net effect today: no state change. See section 7 for the caveat | |
| 2935 history storage | off (no code in libevm) | |
| 7702 set code, 7623 calldata floor, 7002/7251 requests, 2537 BLS precompiles | off (no code in libevm; `PrecompiledContractsBLS` exists in `core/vm/contracts.go` but is not in any active set) | |
| 7212 P256VERIFY at `0x0000000000000000000000000000000000000100` | on since Granite | `params/hooks_libevm.go` `PrecompiledContractsGranite` |
| 3529 refund cap | irrelevant: refunds are zero, see section 5 | |
| 1559 base fee burn | NOT burned, see section 5 | |

revm `SpecId`: use `SpecId::CANCUN` for every block after Etna (all blocks we replay). Do not use PRAGUE.
Overrides needed on top of CANCUN: no refunds, full gas price to coinbase, extra precompiles, storage
key normalization, account RLP extra field, atomic txs at block end, prevrandao = 1 (sections 2, 4, 5, 6).

## 2. Block environment

All from `core/evm.go` `NewEVMBlockContext` and `hooks.OverrideNewEVMArgs`, plus `consensus/dummy/consensus.go`:

- Coinbase: `header.Coinbase` (`DummyEngine.Author`). Validity rule forces it to
  `constants.BlackholeAddr` = `0x0100000000000000000000000000000000000000`
  (`graft/evm/constants/constants.go`, checked in `wrapped_block.go` `syntacticVerify`). Live header
  `miner` confirms. All tx fees are credited to this address (section 5). Its live balance is
  `0x42fa160d68a9ebff7ba95` wei (about 5.06M AVAX), so it is a normal non-empty account.
  Note: that same address is also a precompile override (`DeprecatedContract`, section 4), and it
  has genesis bytecode (live `eth_getCode` returns code starting `0x7300...`). The bytecode is never
  executed because the precompile override wins.
- Base fee: `header.BaseFee`, verified against ACP-176 fee state in the header extra prefix
  (`customheader.BaseFee`, validity only). Just read it from the header.
- Gas limit: `header.GasLimit` (live: 0x2625a00 = 40,000,000). Validity only; the EVM GASLIMIT opcode
  returns the header value.
- Difficulty / PREVRANDAO: header difficulty is always 1 (`syntacticVerify`). `OverrideNewEVMArgs`
  under `rules.IsShanghai` sets `BlockContext.Random = Difficulty bytes` (= 0x00..01) and
  `Difficulty = 0`. libevm `NewEVM` derives `IsMerge = Random != nil`, so opcode 0x44 is PREVRANDAO
  and pushes 1. revm: `BlockEnv.prevrandao = Some(B256::from(U256::from(1)))`, `difficulty = 0`.
- BLOCKHASH: `GetHashFn` walks parent hashes from the header; standard 256-block window
  semantics. Use the real canonical hashes.
- Timestamp: `header.Time` in seconds. Granite adds `TimeMilliseconds` in the header extras
  (`customtypes.HeaderExtra`) but `BlockContext.Time = header.Time`, so TIMESTAMP returns seconds.
- Blob base fee: `ExcessBlobGas` is always 0 after Etna, so `BlobBaseFee = eip4844.CalcBlobFee(0) = 1`.
  revm CANCUN with `blob_excess_gas_and_price = Some(new(0, false))` gives the same. BLOBHASH always
  pushes 0 (no blob txs).
- Burned base fee: nothing is burned. The whole `gasUsed * effectiveGasPrice` goes to the coinbase
  (section 5). No other account is credited.
- Chain id: 43114.

## 3. Transaction order and atomic transactions

Order (`core/state_processor.go` `Process`):
1. `ApplyUpgrades` (precompile activations; nothing activates after Durango, so a no-op for us).
2. `ProcessBeaconBlockRoot(zero hash)` (no-op today, section 1).
3. Every regular tx in block order.
4. `engine.Finalize` -> `DummyEngine.Finalize` (`consensus/dummy/consensus.go`) ->
   `cb.OnExtraStateChange` = `plugin/evm/atomic/vm/vm.go` `(*VM).onExtraStateChange`.
   Atomic txs are applied here, AFTER all regular txs, in the order they appear in the batch.
5. State root = `statedb.IntermediateRoot(IsEIP158)` after all of the above
   (`core/block_validator.go` `ValidateState`).

`onExtraStateChange`:
- `txs := atomic.ExtractAtomicTxs(customtypes.BlockExtData(block), rulesExtra.IsApricotPhase5, atomic.Codec)`
  (`plugin/evm/atomic/codec.go`). After AP5 the `batch` flag is true, so the bytes are the avalanchego
  linearcodec encoding of `[]*atomic.Tx` (codec version u16 = 0, then a u32 slice length, then each Tx).
- `EVMStateTransfer` per tx, wrapped in `extstate.New(statedb)` (multicoin-aware).
- `verifyTxs`, `BlockFeeContribution`, `RemainingAtomicGasCapacity`: validity only.

State effects (`plugin/evm/atomic/import_tx.go` and `export_tx.go` `EVMStateTransfer`):
- ImportTx: for each `Outs[i]` (`EVMOutput{Address, Amount uint64, AssetID}`): if `AssetID == AVAX`,
  `AddBalance(Address, Amount * X2CRate)` with `X2CRate = 1_000_000_000` (`tx.go` line 33; nAVAX to wei).
  Non-AVAX asset: `AddBalanceMultiCoin` (writes a storage slot and sets the account's multicoin flag).
  Banff (2022) restricts import/export to AVAX only (`network_upgrades.go` comment on
  `BanffBlockTimestamp`), so the multicoin branch does not fire on blocks we replay.
- ExportTx: for each `Ins[i]` (`EVMInput{Address, Amount, AssetID, Nonce}`): `SubBalance(Address,
  Amount * X2CRate)`, requires `GetNonce(Address) == Nonce` (validity only, checked after regular txs),
  then `SetNonce(addr, nonce+1)` once per distinct address (a map, so one increment even if an
  address appears in several inputs).
- The atomic tx fee is the difference between UTXO inputs and outputs and lives on the X/P side.
  No EVM account is credited with it. Nothing goes to the coinbase for atomic txs.
- ImportTx `ImportedInputs` and ExportTx `ExportedOutputs` touch shared memory only, not EVM state.

Decoding the ExtData:
- RPC: `eth_getBlockByHash` returns it as the `blockExtraData` hex field
  (`plugin/evm/customtypes/block_ext.go` `PostRPCMarshal`). Empty means no atomic txs.
- Block RLP: the block body is `[header, txs, uncles, version uint32, extData []byte]`
  (`block_ext.go` `BlockRLPFieldsForEncoding`). `version` must be 0 (`syntacticVerify`).
- `ExtDataHash` in the header = `rlpHash(extData)` when non-empty, else `EmptyExtDataHash`
  (`block_ext.go` `CalcExtDataHash`). Header only, not part of the state root.
- Codec (`plugin/evm/atomic/codec.go` `init`): linearcodec, version 0, type ids in registration order:
  0 `UnsignedImportTx`, 1 `UnsignedExportTx`, 2..4 skipped, 5 `secp256k1fx.TransferInput`, 6 skipped,
  7 `secp256k1fx.TransferOutput`, 8 skipped, 9 `secp256k1fx.Credential`, 10 `secp256k1fx.Input`,
  11 `secp256k1fx.OutputOwners`.
  `Tx = {UnsignedAtomicTx (interface: u32 type id then struct), Creds []verify.Verifiable}`.
  `UnsignedImportTx = {NetworkID u32, BlockchainID [32], SourceChain [32], ImportedInputs
  []*avax.TransferableInput, Outs []EVMOutput}`; `UnsignedExportTx = {NetworkID, BlockchainID,
  DestinationChain, Ins []EVMInput, ExportedOutputs []*avax.TransferableOutput}`.
  `EVMOutput = {Address [20], Amount u64, AssetID [32]}`; `EVMInput = {Address, Amount, AssetID, Nonce u64}`.
  Only `Outs` / `Ins` matter for EVM state; the credentials and UTXO fields can be skipped over
  (the follower must still parse past them: TransferableInput = UTXOID{TxID [32], OutputIndex u32}
  + AssetID [32] + Input interface; see avalanchego `vms/components/avax` for exact layouts).
  The existing rs/exec `warp.rs` header comment documents the same linearcodec primitives.
- Bonus blocks (`atomic/vm/bonus_blocks.go`, `mainnet_ext_data_hashes.json`) are 2021-era heights;
  ignore.

## 4. Precompiles

`params/hooks_libevm.go`:

`currentPrecompiles()` under `IsGranite` returns `PrecompiledContractsGranite`:
- `0x0100000000000000000000000000000000000000` (GenesisContractAddr = BlackholeAddr = coinbase),
  `0x0100000000000000000000000000000000000001`, `0x0100000000000000000000000000000000000002`:
  `nativeasset.DeprecatedContract`. Its `Run` (`nativeasset/contract.go` line 169) returns
  `(nil, suppliedGas, ErrExecutionReverted)`. Through `legacy.PrecompiledStatefulContract.Upgrade`
  (libevm `libevm/legacy/legacy.go`) that is `UseGas(0)` plus a revert, and libevm `evm.go` `Call`
  keeps the returned gas on `ErrExecutionReverted`. So any CALL/STATICCALL/tx to these three
  addresses reverts with ALL gas returned to the caller frame and the value transfer undone.
  A top-level tx to the blackhole therefore has status 0, uses only intrinsic gas, still bumps the
  sender nonce and still pays the fee to the coinbase.
- `0x0000000000000000000000000000000000000100`: `vm.P256Verify` (EIP-7212), gas 3450, standard.
- The standard Cancun set 0x01..0x0a (libevm `PrecompiledContractsCancun`, including
  `kzgPointEvaluation` at 0x0a).

`PrecompileOverride(addr)` (same file) also maps module precompiles whose config is active:
- Warp at `0x0200000000000000000000000000000000000005` (`precompile/contracts/warp/module.go`
  `ContractAddress`), enabled at Durango (`plugin/evm/vm.go` `parseGenesis` appends
  `warpcontract.NewDefaultConfig(DurangoBlockTimestamp)`). It is the only registered module
  (`precompile/registry/registry.go`). The account has nonce 1 and code `0x01` (set by
  `core/state_processor_ext.go` `ApplyPrecompileActivations` at activation; live RPC confirms
  nonce 1, code 0x01). The revm precompile provider MUST intercept this address, otherwise revm
  would execute byte 0x01 (ADD on an empty stack).

Warm-at-tx-start set (`RulesExtra.ActivePrecompiles`): the geth Cancun list plus the four
`PrecompiledContractsGranite` addresses. The warp address is NOT added (only
`currentPrecompiles()` keys are appended), so the first access to `0x0200..05` in a tx is cold
(2600) unless it is in the access list. This changes gas used and therefore the coinbase credit.

Warp precompile state effects (`precompile/contracts/warp/contract.go`, `contract_warp_handler.go`):
- `getBlockchainID`, `getVerifiedWarpMessage`, `getVerifiedWarpBlockHash`: read only.
- `sendWarpMessage`: the only write is `AddLog` (one log with 3 topics). Logs affect the receipts
  root and bloom, never the state root. It errors with `ErrWriteProtection` under STATICCALL.
- No storage slot is ever written by the warp precompile.
- Gas: `graniteGasConfig` (getBlockchainID 200, getVerified base 750, per signer 250, per 32-byte
  chunk 512, verifyPredicate base 125_000; sendWarpMessage base = LogGas + 3*LogTopicGas + 20_000 +
  20_000, plus LogDataGas per input byte).
- Granite rule in `makePrecompile`: a DELEGATECALL or CALLCODE to any stateful precompile
  returns `(nil, 0, ErrExecutionReverted)`: revert with all gas of that frame consumed.
  Before Granite (after `InvalidateDelegateUnix`) it invalidated the tx; that path is dead now.

Predicates (`core/predicate_check.go`, `wrapped_block.go` `verifyPredicates`):
- A tx that carries warp predicates (access-list tuples whose address is the warp address; each
  tuple's storage keys are one predicate, `predicate.FromAccessList`) is NOT rejected when a
  predicate fails. The block builder/verifier computes a per-tx bitset of FAILED predicate indexes
  and stores it in the header: `header.Extra[24:]` after Fortuna (`customheader.PredicateBytesFromExtra`,
  offset `acp176.StateSize = 24`), encoded with avalanchego linearcodec as
  `map[txHash][32] -> map[address][20] -> bitset bytes` (`vms/evm/predicate/results.go`
  `ParseBlockResults`; version u16 0, then u32 map length, keys sorted). The live header's extra is
  30 bytes: 24 bytes fee state + `0x0000 00000000` (empty results map).
- The precompile's `handleWarpMessage` returns `valid=false` when `predicateResults.Contains(index)`
  or the index has no predicate; it never reverts for an invalid message.
- So the follower does NOT need BLS verification or a validator set: parse the bitset from the
  header extra and hand `(txHash, warpAddress) -> failed indexes` to the precompile. It DOES need
  the predicate bytes from the tx access list (`predicate.Predicate` = the storage keys; `Bytes()`
  strips right zero padding and the trailing 0xff delimiter) to return the message contents.
- Intrinsic gas: `RulesExtra.AccessListGas` (hooks_libevm.go) replaces the default access-list gas
  whenever predicaters exist (always, post-Durango). Non-warp tuples cost the normal
  2400 + 1900/key; warp tuples cost `Config.PredicateGas` (`warp/config.go`: 125_000 + 512 per
  32-byte chunk + 250 per signer in the BitSetSignature) instead. This changes gas used, hence the
  coinbase credit. It requires parsing the warp message's signer bitset (no BLS verify).

`CanCreateContract` and `CanExecuteTransaction` hooks are no-ops (hooks_libevm.go).

## 5. Gas accounting that changes state

coreth uses its OWN `core/state_transition.go` (not libevm's) for block processing:

- No refunds at all: `refundGas(apricotPhase1 bool)` skips the SSTORE/SELFDESTRUCT refund counter when
  `IsApricotPhase1` (always true). `gasUsed = gasLimit - gasRemaining` with zero refund. revm: override
  `Handler::refund` to `set_refund(0)` (rs/exec `SevmHandler::refund` already does this).
- Coinbase credit: `fee = gasUsed * msg.GasPrice` where `msg.GasPrice = min(tip + baseFee, feeCap)`
  (`TransactionToMessage`), then `AddBalance(Coinbase, fee)`. That is the FULL effective gas price,
  base fee included, credited to `0x0100..00`. Nothing is burned. revm: override
  `Handler::reward_beneficiary` (rs/exec `SevmHandler::reward_beneficiary` is the shape).
- Sender is charged `gasLimit * GasPrice` up front and refunded `gasRemaining * GasPrice` (`buyGas`,
  `refundGas`), standard.
- Intrinsic gas: libevm `IntrinsicGas` (21000/53000, 4/16 per data byte, 3860 words) with the
  access-list override above.
- `MinimumGasConsumption` (ACP-194, ceil(limit/2)) is Helicon-only (`RulesExtra.MinimumGasConsumption`)
  and coreth's own `TransitionDb` does not call it anyway. Ignore until Helicon.
- `BlockGasCost` and `ExtDataGasUsed` (`customtypes.HeaderExtra`, `customheader.BlockGasCost`,
  `customheader.VerifyBlockFee`): validity only. `VerifyBlockFee` sums tips and compares; it moves no
  balance. Live header shows both 0.
- Minimum gas price / ACP-176 fee state: validity only.
- `ExecutionInvalidated` (libevm `evm.libevm.go`): a precompile can void a tx; after Granite no
  active path calls `InvalidateExecution`, and a voided tx makes the block invalid, so never seen.

## 6. Header extras and the state root

- `header.Root` is the plain account trie root: `statedb.IntermediateRoot(IsEIP158)` in
  `ValidateState`, secure trie keyed by `keccak(address)`, storage tries keyed by `keccak(slot)`.
  Nothing coreth-side (atomic trie, shared memory, warp DB, ExtDataHash, BlockGasCost, fee state,
  predicate results) is inside it; those live in the header or in separate DBs.
- Account leaf RLP has FIVE fields on coreth: `[Nonce, Balance, Root, CodeHash, IsMultiCoin bool]`.
  libevm `core/types/state_account.go` `StateAccount.Extra *StateAccountExtra` is always encoded
  when extras are registered (`rlp_payload.libevm.go` `StateAccountExtra.EncodeRLP`), and coreth
  registers `isMultiCoin bool` (`plugin/evm/customtypes/state_account_ext.go`, `customtypes/libevm.go`).
  RLP of `false` is `0x80`, of `true` is `0x01`. A follower that re-encodes account leaves MUST emit
  the fifth field and MUST preserve the existing flag for accounts it rewrites. Nothing after Banff
  sets the flag to true on new accounts (multicoin import/export is gone), so: read it from the
  snapshot, carry it through, always write it.
- Empty-account rule (EIP-158/161): libevm `core/state/state_object.go` `empty()` is
  `Nonce == 0 && Balance == 0 && CodeHash == EmptyCodeHash && Extra.IsZero()`. An account with
  `IsMultiCoin == true` is never deleted as empty even with zero nonce/balance/code. Otherwise standard:
  touched empty accounts are removed at `Finalise(true)` after every tx and at the final root.
- Storage key normalization (root-affecting): `core/extstate/statedb.go` registers
  `normalizeStateKeysHook.TransformStateKey`, applied by libevm `core/state/statedb.go` in
  `GetState`, `GetCommittedState`, `SetState` (lines 349, 359, 422) unless
  `SkipStateKeyTransformation` is passed. `normalizeStateKey`: `key[0] &^= 0x01`, i.e. clear the
  lowest bit of the most-significant byte (bit 248 of the 256-bit slot). Every SLOAD/SSTORE of slot K
  reads/writes trie key `keccak(K & !(1 << 248))`. Multicoin balances live at the odd keys
  (`normalizeCoinID` sets that bit) and are only reached via `SkipStateKeyTransformation`.
  revm: apply the mask in the database adapter for `storage(addr, slot)` and when committing
  storage changes. Transient storage (`SetTransientState`) and access-list slots
  (`AddSlotToAccessList`) are NOT transformed, so two raw slots differing only in bit 248 share
  one persistent slot but have separate warm/cold tracking (a gas edge, not a root edge).
- Header RLP field order (`customtypes/header_ext.go` `HeaderSerializable`): the 15 geth fields,
  then `ExtDataHash`, then optional `BaseFee, ExtDataGasUsed, BlockGasCost, BlobGasUsed,
  ExcessBlobGas, ParentBeaconRoot, TimeMilliseconds, MinDelayExcess, TargetExponent,
  MinPriceExponent, SettledHeight, SettledGasUnix, SettledGasNumerator, SettledExcess`.
  Needed only if the follower hashes headers itself. `ExtDataHash` sits BEFORE `BaseFee`, unlike
  any geth header.

## 7. Other state mutations outside the plain EVM path

Sweep of `core/blockchain*.go`, `core/state_processor*.go`, `core/state_transition*.go`,
`plugin/evm/vm*.go`, `plugin/evm/wrapped_block.go`, `plugin/evm/atomic/vm/*.go` for
`AddBalance/SubBalance/SetBalance/SetNonce/SetCode/SetState/SelfDestruct/CreateAccount`:
- `core/genesis.go` (genesis alloc only).
- `core/state_processor_ext.go` `ApplyPrecompileActivations`: on a precompile activation sets nonce 1
  and code `0x01` at the module address and calls `Configure` (warp's is a no-op); on a disable
  `SelfDestruct` + `Finalise`. Only fires on the block whose parent is before and whose time is at or
  after an upgrade timestamp; the last one was Durango. Not reachable for us unless a new precompile
  upgrade is shipped in a future coreth release.
- `plugin/evm/atomic/*` `EVMStateTransfer` (section 3).
- `core/state_transition.go` coinbase credit (section 5).
- `ProcessBeaconBlockRoot` (section 1). Caveat: the EIP-4788 deployer tx is a pre-EIP-155 tx and can
  be replayed on any chain. If someone ever deploys the beacon-roots contract at
  `0x000F3df6D732807Ef1319fB7B8bB8522d0Beac02` on C-chain, coreth will start executing it every
  block with a zero root (writing ring-buffer slots) and the follower must do the same. Check the
  address for code at startup and, if code appears, run the standard 4788 system call with root 0.
- `plugin/evm/vm.go` / `wrapped_block.go` `Accept`: atomic trie and shared memory only, no EVM state.
- `core/evm.go` `OverrideNewEVMArgs` wraps the StateDB in `extstate.StateDB` (predicates, multicoin)
  and, pre-AP1 only, in `StateDBAP0`. No extra writes.
- `core/blockchain.go` `commitWithSnap`: `statedb.Commit(number, IsEIP158)`, standard.
- Nothing runs at block end other than `Finalize` (atomic txs plus validity checks). No block reward,
  no withdrawals (`Durango` comment in `network_upgrades.go`: EIP-4895 excluded), no uncles.

Found nothing else.

---

## Checklist: revm configuration for mainnet C after Granite

1. `CfgEnv`: `chain_id = 43114`, `spec = SpecId::CANCUN`. Never PRAGUE. Keep EIP-3607 (senders with
   code are rejected at validity; revm's check matches coreth `preCheck` ErrSenderNoEOA).
2. `BlockEnv`: `number = header.number`, `beneficiary = 0x0100000000000000000000000000000000000000`,
   `timestamp = header.timestamp` (seconds, not `timestampMilliseconds`), `gas_limit = header.gasLimit`,
   `basefee = header.baseFeePerGas`, `difficulty = 0`, `prevrandao = Some(0x00..01)`,
   `blob_excess_gas_and_price = Some(BlobExcessGasAndPrice::new(0, false))` (blob base fee 1).
3. BLOCKHASH: serve real canonical hashes for the previous 256 blocks.
4. Pre-block: skip the EIP-4788 system call as long as `0x000F3df6D732807Ef1319fB7B8bB8522d0Beac02`
   has no code; assert that at startup (or run the call with root 0, which is a no-op while empty).
   No EIP-2935 call.
5. Transactions: reject nothing; replay legacy, 2930, 1559 txs in block order. There are no blob or
   7702 txs.
6. Handler overrides (rs/exec `SevmHandler` is the template):
   a. `refund`: `set_refund(0)` (no SSTORE/SELFDESTRUCT refunds).
   b. `reward_beneficiary`: `coinbase += gas_used * effective_gas_price` (base fee included).
   c. `validate_initial_tx_gas`: replace the access-list gas of tuples whose address is
      `0x0200000000000000000000000000000000000005` with warp `PredicateGas`
      (125_000 + 512 per 32-byte key + 250 per signer bit in the BitSetSignature).
7. Precompile provider = revm CANCUN set (0x01..0x0a) plus:
   a. `0x0100..00`, `0x0100..01`, `0x0100..02`: revert, return all gas, no state change.
   b. `0x0000..0100`: P256VERIFY (EIP-7212), 3450 gas.
   c. `0x0200..05`: warp (rs/exec `warp.rs` implements the ABI, gas config and sendWarpMessage log).
      Under Granite use `graniteGasConfig`. DELEGATECALL/CALLCODE to any of these stateful
      precompiles reverts with the frame's gas consumed.
   d. Warm set at tx start: 0x01..0x0a plus the three `0x0100..` addresses plus `0x0000..0100`.
      NOT the warp address.
8. Warp predicates: parse `header.extraData[24..]` with the predicate results codec into
   `(txHash, warpAddr) -> failed index bitset`. For each tx, predicates = access-list tuples at the
   warp address (storage keys, in order). `getVerifiedWarpMessage(i)` returns `valid=false` when
   `i` is in the failed set or out of range; else decode the message from the predicate bytes. No
   BLS verification, no validator set.
9. Storage key mask: every persistent storage read/write uses `slot & !(U256::ONE << 248)`. Do not
   mask transient storage or access-list membership.
10. Account encoding for the state root: leaf = `rlp([nonce, balance, storage_root, code_hash,
    is_multicoin])`; carry `is_multicoin` from the snapshot unchanged (it is `false` for every
    account revm creates). An account with `is_multicoin = true` is never deleted as empty.
11. After each tx: EIP-158 delete touched empty accounts (revm's default under SPURIOUS_DRAGON+).
12. After the last regular tx, decode `blockExtraData` (linearcodec `[]*atomic.Tx`, version 0) and
    apply in slice order: ImportTx `Outs`: `balance += amount * 1e9`; ExportTx `Ins`:
    `balance -= amount * 1e9`, then `nonce += 1` once per distinct address. AVAX asset only.
    Nothing to the coinbase.
13. Compute the root and compare with `header.stateRoot`.
14. Ignore for state: `blockGasCost`, `extDataGasUsed`, `extDataHash`, `minDelayExcess`,
    `timestampMilliseconds`, the 24-byte ACP-176 fee state prefix of `extraData`, `MinimumGasConsumption`
    (Helicon), the block fee check, the atomic gas capacity check.
15. Watch for Helicon: when `HeliconTime` is scheduled in `upgrade/upgrade.go`, revisit
    `MinimumGasConsumption` (ACP-194), the `TargetExponent`/`MinPriceExponent`/`Settled*` header
    fields, and any new precompile in `precompile/registry/registry.go`.

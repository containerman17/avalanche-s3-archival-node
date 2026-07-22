# Helicon (v1.15.0-fuji) impact report: ACP-194 SAE + ACP-283

Research date: 2026-07-22 (JST). Source studied at the tag `avalanchego@v1.15.0-fuji` (commit e8f9781). All file:line citations below are repo-relative paths at that tag.

## Headline architecture fact

SAE is not a coreth change. It is a **whole new VM**, `vms/saevm` ("strevm", the ACP-194 reference implementation, `vms/saevm/README.md`). The C-chain now registers a `transitionvm.Factory{PreFactory: coreth, PostFactory: saevm/cchain}` that runs coreth until `HeliconTime - 10s`, then shuts coreth down, writes a `transitioned` marker, and boots the SAE VM on the same database (`node/node.go:1230-1248`, `vms/transitionvm/vm.go:224-244`). Coreth itself actively rejects any block carrying SAE settled markers (`graft/coreth/plugin/evm/customheader/settled.go:23-32`), so coreth-as-library can never process post-Helicon blocks. Everything your two followers do with coreth must re-point to `vms/saevm` for post-Helicon heights.

## a. What header.Root means post-SAE, and the follower invariant

`header.Root` of block N is the **post-execution state root of the last block settled by N**, not N's own state:

- Set at build: `hdr.Root = lastSettled.PostExecutionStateRoot()` (`vms/saevm/sae/block_builder.go:227`); accessor renamed to `Block.SettledStateRoot()` because `types.Block.Root` is "ambiguous under SAE" (`vms/saevm/blocks/export.go:28-33`).
- Invariant checked on recovery: `b.SettledStateRoot() != b.LastSettled().PostExecutionStateRoot()` is a broken invariant (`vms/saevm/blocks/invariants.go:202`).
- Which block settles: deterministic function `LastToSettleAt(hooks, blockTime - τ, parent)` with **τ = 5 seconds** and λ = 2 (`vms/saevm/sae/block_builder.go:354-374`, `vms/saevm/params/params.go`): the last ancestor whose *execution gas-time* is ≤ blockTime − τ (`vms/saevm/blocks/settlement.go:179-256`). Gas time is a proxy clock that advances by wall gaps and by gas/R (`vms/saevm/gastime`).
- The header additionally carries an explicit settled marker in 4 new extra fields: `SettledHeight`, `SettledGasUnix`, `SettledGasNumerator`, `SettledExcess` (`graft/coreth/plugin/evm/customtypes/header_ext.go:335-338`, set in `vms/saevm/cchain/hooks.go:496-501`).
- Validators verify by full rebuild + hash equality (`vms/saevm/sae/blocks.go:134-147`); during bootstrap they check the root and height explicitly: `lastSettled.PostExecutionStateRoot() == b.SettledStateRoot()` and `lastSettled.NumberU64() == SettledBy(header).Height` (`vms/saevm/sae/blocks.go:166-187`).

**Follower invariant**: for every accepted block N, execute the chain in order, record `root(h)` for each height h; then assert `N.header.Root == root(N.SettledHeight)` and that `SettledHeight` is monotonic. Per-block verification is still possible for every incoming header, but the root being checked belongs to an older height. Only heights that are some block's settlement target get their root directly committed; blocks in the middle of a multi-block settle range are verified transitively (deterministic chained execution). Lag: ≥ τ = 5s of gas time, in practice roughly 5-10s / a handful of blocks; bounded by the queue cap (see Scale notes, ~20-30 gas-seconds worst case).

## b. Receipts/logs finality and commitment

- Receipts for block N are computed when N reaches the head of the execution stream, and are **written to disk under N's own hash at execution time**: `rawdb.WriteReceipts(batch, b.Hash(), b.NumberU64(), receipts)` (`vms/saevm/blocks/execution.go:126`). They are consensus-deterministic the moment N is accepted (execution is a pure function of the accepted chain), materialized moments later.
- They are **committed to a header only at settlement**: the settling block's `receiptsRoot`/`logsBloom` are computed over the **concatenation of receipts of ALL blocks it settles** (`vms/saevm/sae/block_builder.go:307-311` collects `blocks.Range(parent.LastSettled(), lastSettled)` receipts; `vms/saevm/cchain/hooks.go:503-511` passes them into `NewBlockWithExtData` → `ethtypes.NewBlock`, which derives `ReceiptHash = DeriveSha(receipts)` and `Bloom = CreateBloom(receipts)`; `graft/coreth/plugin/evm/customtypes/block_ext.go:181-197`). The merged trie re-indexes receipts 0..n−1 across the range, per the ACP-194 spec.
- `rawdb` "finalized" pointer = last settled block (`vms/saevm/sae/consensus.go:73-78`); RPC labels: `pending` = last accepted, `latest` = last executed, `safe`/`finalized` = last settled (`vms/saevm/docs/invariants.md`).
- Two content changes inside receipts: per-tx gas charged now floors at `ceil(gasLimit/λ)` = limit/2 via the `MinimumGasConsumption` rules hook (`vms/saevm/hook/hook.go:205-208`), and `BlockHash`/`EffectiveGasPrice` are fixed up post-execution because the header used for EVM execution is a modified copy (`vms/saevm/saexec/execution.go:254-266`).

## c. Wire format, GetAncestors, handshake

- **Block encoding**: still one RLP coreth-style block-with-extData. The header gains RLP-optional tail fields: `TargetExponent`, `MinPriceExponent` (Helicon), plus the 4 `Settled*` fields (`graft/coreth/plugin/evm/customtypes/header_ext.go:327-338`). Pre-Helicon blocks are byte-identical to before. Consequences: your **pre-SAE history walk is unaffected**, but a v1.14.2 coreth library **cannot RLP-decode any post-Helicon header** (extra list elements = decode error), so fetch crash-loops at the boundary unless updated or stopped.
- **proposervm / p2p messages**: zero Helicon changes in `vms/proposervm/` or `message/` (grep confirms). GetAncestors request/response format is unchanged. Server side: `transitionvm` does not implement `BatchedChainVM`, so v1.15 peers serve GetAncestors via the generic per-block fallback loop (`snow/engine/snowman/block/batched_vm.go:37-110`), still batched at the message level, possibly a bit slower per request.
- **Handshake gating**: `network.NewNetwork` is given `HeliconTime` as the compatibility switch time (`node/node.go:632-636`); `MinimumCompatibleVersion` after the upgrade is **v1.15.0**, before it v1.14.0 (`version/constants.go:26-42`). So on **Fuji at activation, v1.15.0 peers disconnect anything below v1.15.0**, including epochdb's naked-p2p fetcher if it announces a v1.14.2 version. On **mainnet nothing changes** until a mainnet release schedules HeliconTime (currently `UnscheduledActivationTime`, `upgrade/upgrade.go:41`); v1.14.2 remains fully compatible there.

## d. What must change in a replay-verifier (details in the per-codebase section below)

- Root check re-points from "my root for N == N.Root" to "my root for N.SettledHeight == N.Root". Keep a small window of computed roots (queue depth ~2-3 max blocks) awaiting their settling header. Per-block execution is unchanged in cadence; per-block *root confirmation* arrives with ~5-10s lag, and some heights are only transitively confirmed.
- Execution itself must switch engines for post-Helicon heights: base fee comes from the deterministic gas clock, **not** `header.BaseFee` (the header carries the consensus worst-case upper bound; execution recomputes the real one, `vms/saevm/saexec/execution.go:200-218`); gas charging floors at limit/2; atomic (cross-chain) txs become end-of-block `hook.Op` debits/mints instead of coreth's atomic-tx path (`vms/saevm/cchain/hooks.go` `EndOfBlockOps`/`AfterExecutingBlock`). The good news: `saexec.Execute` is exported and takes hooks + chain config + a StateDB opener (`vms/saevm/saexec/execution.go:184`), and the whole C-chain hook set is in `vms/saevm/cchain`. Replays need no extra data beyond the blocks: `Execute(..., baseFee=nil, ...)` re-derives the base fee from the clock.
- Receipts validation re-points from per-block `receiptsRoot` to per-settlement merged root; `header.GasUsed` is no longer executed gas, it is worst-case charged gas of the block's own body: Σ tx gas limits + op gas (`vms/saevm/sae/block_builder.go:297`, `vms/saevm/worstcase/state.go:270-280,324-327`), checkable statically from the body without execution; `header.Bloom` covers the settled range's logs, not the block's own.
- Bonus for epochdb: SAE ships first-class Firewood support as a state scheme (`vms/saevm/saedb/tracker.go:59,87-121`, `vms/saevm/firewood/`), hash scheme remains the default; roots are the same Ethereum MPT roots either way.

## e. ACP-283 dynamic minimum gas price

- Lives in the **C-chain header**, not on the P-chain: new extra field `MinPriceExponent` (`header_ext.go:331-333`). Floor M = 1 wei × e^(q/415828534307635077), initial q = 0, per-block movement capped so the floor can double/halve no faster than ~3600 blocks (`vms/saevm/cchain/dynamic/price.go`). Each producer nudges q toward its configured vote, new C-chain config `min-price-target` (`vms/saevm/cchain/config.go:32`), stake-weighted-median dynamics like ACP-176/226.
- It is a **hard floor inside the base fee itself**: `price = max(config.MinPrice, computed)` (`vms/saevm/gastime/gastime.go:128-134`), wired from the parent header's exponent (`vms/saevm/cchain/hooks.go:186-201`). So it affects execution results (fees burned, balances, therefore settled state roots), not just gas-price suggestion RPCs. `eth_gasPrice`/`eth_feeHistory`/`eth_maxPriorityFeePerGas` must respect it.
- Replay impact for you: none beyond adopting the SAE gas-clock code, which already consumes `MinPriceExponent`; a replayer that hardcodes a 1-wei floor stays correct only until validators actually vote the floor up. Day one M = 1 wei, zero behavioral change.
- Also new and analogous: `TargetExponent` (ACP-176 target moves from the coreth `Extra`-bytes encoding to a first-class header extra field; the transition parses the old encoding from the last coreth header, `vms/saevm/cchain/hooks.go:163-175`).

## f. Timeline mechanics and deadline shape

- **Fuji activation: 2026-07-28 15:00 UTC = 2026-07-29 00:00 JST** (`upgrade/upgrade.go:66`; release notes say "all Fuji nodes must upgrade before 11 AM ET Jul 28").
- v1.15.0-fuji **refuses to run mainnet** ("mainnet is not supported", `config/config.go:1384-1386`); mainnet `HeliconTime` is unscheduled in the tag (`upgrade/upgrade.go:41`).
- **Your mainnet-pinned v1.14.2 nodes are safe on mainnet until the mainnet Helicon activation**: handshake min-compatible stays v1.14.0 until that (currently non-existent) time, block format unchanged, transitionvm never fires. The hard deadline is the mainnet activation timestamp that ships in the future mainnet release (v1.15.x). Precedent: Granite went Fuji 2025-10-29 → mainnet 2025-11-19 (`upgrade/upgrade.go:39,64`), a 3-week gap; expect Helicon mainnet roughly mid-to-late August 2026, confirmed only when the release lands. The deadline is sharp: at that instant, (1) upgraded peers drop v1.14 connections, (2) the first SAE block is unparseable and unexecutable by v1.14.2 libraries.
- Boundary detail: the coreth→saevm handover happens at `HeliconTime - 10s` (last coreth block), the first SAE block has timestamp ≥ HeliconTime; the last coreth block becomes the SAE genesis-like "synchronous" block whose header still self-commits root and receipts (`vms/saevm/blocks/execution.go:289-307`).

## Scale notes (day-one params, epochdb sizing)

- Gas target carries over from the live ACP-176 state (parsed from the last coreth header), min 1M gas/s; R = 2 × target (`vms/saevm/gastime/gastime.go:90-91`).
- **Max block gas = R·τ·λ = 20 × target** (`vms/saevm/worstcase/state.go:89-91,144-159`), double the pre-Helicon ACP-176 capacity cap of 10 × target (`vms/evm/acp176/acp176.go:30-37`). At the 1M floor: 20M-gas blocks, ~950 plain transfers per block ceiling; scale linearly with target.
- Consensus stops waiting for execution: up to 2 max-size blocks may sit unexecuted before block building backpressures (`MaxFullBlocksInOpenQueue = 2`, `vms/saevm/params/params.go`; `worstcase/state.go:120-125`), i.e. bursts of ~40 × target gas in flight, worst-case queue wall time 30s (`MaxQueueWallTime`). epochdb's EPOCH_TXS and per-epoch gas assumptions should be sized on charged gas (min charge per tx = max(used, limit/2)), with sustained rate still R = 2 × target; SAE day one does not raise throughput by itself, it removes the reason validators kept the target low, so expect target-raise votes on mainnet after Helicon.
- Day-one Fuji observables worth scraping: `avalanche_evm_sae_last_executed_height`, `_last_settled_height`, `_execution_queue_gas_limit`, `_executed_base_fee` vs `_worst_case_base_fee` (RELEASES.md metrics section).

# What changes in our two codebases (ranked by effort)

**epochdb** (p2p GetAncestors fetch → coreth-library exec → Firewood root check per block):
1. **[Small, urgent] Don't crash at the boundary.** v1.14.2 RLP decode fails on the first post-Helicon Fuji header, and v1.15 peers drop v1.14-version handshakes at activation. Minimum viable for Jul 28: stop/park fetch at HeliconTime on Fuji; all pre-SAE history fetching stays valid indefinitely.
2. **[Small] Dep bump** to `avalanchego@v1.15.0-fuji` (+ graft/coreth v1.15.0-fuji) so parsing and handshake work; fetch pipeline itself is otherwise untouched (message format unchanged, GetAncestors served via fallback loop).
3. **[Medium] Verifier re-point**: keep a ring of computed roots (a few blocks deep); on each new header with `SettledHeight = h`, assert `header.Root == root(h)` and marker monotonicity; treat non-target heights as transitively verified. Also assert the in-body statics: `header.GasUsed == Σ tx.Gas() + Σ op.Gas`, `header.ReceiptHash == DeriveSha(merged receipts of (prevSettled, h])`.
4. **[Large] Execution engine swap for post-Helicon heights**: import `vms/saevm/saexec.Execute` + `vms/saevm/cchain` hooks (gas clock base fee, limit/2 charge floor, ops-based atomic txs, receipt fixups) in place of the coreth processor. Firewood stays; SAE even has native Firewood scheme support to crib from (`vms/saevm/firewood`).

**flatstate** (embedded avalanchego, receiptsRoot/gasUsed/bloom checks, tip capture):
1. **[Small, urgent] Same boundary protection** for the Fuji instance; mainnet instance untouched until the mainnet release.
2. **[Medium] Validation re-point**: receiptsRoot/bloom become per-settlement merged checks against the settling header; `header.GasUsed` becomes a static body property (or drop the check); executed gas per block is only committed via cumulative gas inside the merged receipts trie.
3. **[Large] Capture hook re-port**: the embedded node becomes transitionvm→saevm at Helicon; the fork-patch capture point (docs/node-integration.md) moves from coreth's accept path into the SAE execution stream (`saexec.Executor.afterExecution`, `vms/saevm/saexec/execution.go:320-346`). Semantics shift: state for block N materializes asynchronously after acceptance ("latest" = last executed), so tip simulation must key off the executed head, and verified-but-unexecuted blocks have no state to publish. Replay must derive base fee from the gas clock, never `header.BaseFee`.

# Recommended 7-day plan (JST)

- **Jul 22-23 (now)**: decide day-one posture per instance. Recommended: both Fuji followers get a hard stop gate at 2026-07-29 00:00 JST (block timestamp ≥ HeliconTime → pause fetch/execution cleanly, keep serving history). Mainnet instances: no action, confirm they are pinned v1.14.2 and mainnet-only.
- **Jul 23-25**: epochdb dep bump to v1.15.0-fuji behind a branch; get fetch parsing post-Helicon headers in a unit test (craft a header with the 6 new extra fields); land the settlement-lag verifier skeleton (root ring + marker check) even if execution still stops at the boundary.
- **Jul 25-27**: prototype the saexec-based executor path (epochdb first, flatstate learns from it); target "execute the first N SAE blocks on Fuji after activation and match settled roots".
- **Jul 29 (activation 00:00 JST)**: watch the boundary live on Fuji: transition logs, first SAE block shape, real settlement lag (settled-height vs accepted-height metrics), GetAncestors behavior across the boundary from v1.15 peers. This is free real-world test data you cannot get any other way.
- **Jul 29 onward**: from observations, finalize the mainnet-readiness checklist; subscribe to avalanchego releases for the mainnet v1.15.x cut, that announcement starts your real (~3-week, based on Granite precedent) countdown for the mainnet followers.

One risk to flag: the SAE Go APIs are explicitly "no stability guarantees" (`vms/saevm/README.md`), so pin exactly to release tags and expect churn between the Fuji and mainnet releases.

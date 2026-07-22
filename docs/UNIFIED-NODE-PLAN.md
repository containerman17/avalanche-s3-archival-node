# Merge plan: flatstate + epochdb → one node

Spec of record read: `~/epochdb/DESIGN.md` "Unified node direction" (line 37) plus the tiering/flock/torrent/verification sections. Both codebases read in full at the relevant seams. No files touched.

## 1. Inventory

**flatstate** (`~/flatstate`, 10,883 LOC total incl. tests):

| Package | LOC (test) | Role | Fate in merge |
|---|---|---|---|
| `follower/consensus` | 1,266 (319) | snowman shell over `Topological` | **retire** (superseded by epochdb port) |
| `follower/net` | 781 (166) | p2p join recipe, validator fetch | **retire** (epochdb `fetch` covers it) |
| `follower/sync` | 1,205 (363) | hash-keyed baseline state-sync loader | survives, unique |
| `follower/exec` | 873 (290) | trie-less coreth-as-library exec, capture.Batch producer | survives for **unfinalized preferred** layers only |
| `store` | 1,208 (379) | LMDB (D6 layout) | survives = the tip sink |
| `mem` | 778 (334) | pinned base map (the `--pin-state` contract) | survives |
| `tipbus` | 465 (147) | ephemeral tip env, ~100µs reader wakeup | survives (the sub-5ms tail mechanism) |
| `node` | 651 (325) | Tracker + composite Sink (D7 ordering) | survives, becomes tip-sink glue |
| `capture` | 274 (68) | Batch encoding + `Sink` interface | survives = the merge seam |
| `sim` / `engine` / `replay` / `suitecache` | 1,082/347/254/392 | bot-side simulation over LMDB | survive unchanged (separate process, see risk 3) |
| `schema` / `chaincfg` / `cmd/*` | 233/84/990 | | schema survives; cmds rewired |

**epochdb** (`~/epochdb`, 17,610 LOC total):

| Package | LOC (test) | Role | Fate |
|---|---|---|---|
| `fetch` | 2,652 (270) | p2p dial, archival peer pool, GetAncestors walk, staging store, Follow glue + **multi-source validator cross-check** | survives = the unified net layer |
| `fetch/consensus` | 984 (204) | the ported flatstate engine (provenance in header comment) | survives = the unified engine |
| `exec` | 2,198 (582) | Firewood executor, trie-interceptor post-image capture, root check per block | survives = authoritative accepted-stream producer |
| `state` | 5,447 (1,102) | bucketlog, epoch format v2, seal, SSTs, tx index, blooms, History overlay | survives untouched |
| `rpc` | 3,693 (899) | JSON-RPC/WS, stored logs/receipts, DeriveStored | survives |
| `dist` | 460 | torrent seed/leech | survives |
| `cmd/epochdb` | 2,176 | | grows the unified `serve --follow` |

**Shared-by-copy today** (the duplication the merge kills):
- Snowman engine shell: `flatstate/follower/consensus/consensus.go` (925) → `epochdb/fetch/consensus/engine.go` (780), ported with flatstate's live-measured constants (10s poll floor, 16 concurrent repolls, 64 max Gets) baked in.
- p2p join recipe: `flatstate/follower/net` (~615 non-test) vs `epochdb/fetch/fetcher.go` dial/handler/parse (~700); `reconcileValidators` ported verbatim (comment says so).
- Net effect: ~1.7k LOC of consensus/net duplication, two followers to babysit.

## 2. The capture.Sink seam

**flatstate side** (`capture/capture.go`): a real interface, preference-aware:
```go
type Sink interface {
    Block(b *Batch) error                              // unfinalized preferred
    Finalize(block uint64, hash schema.Hash) error
    PreferenceReset(preferred []*Batch) error
}
// Batch{Block, Hash, Parent, Time, Ops []Op}
// Op kinds: Account / Slot / DeleteSlot / Destruct / Code(hash,blob)
```

**epochdb side** (`exec/capture.go` + `exec/executor.go`): no interface. The trie-interceptor (`wrappingTrie`) fills a `blockFrame` (`'a'` addr+accountRLP|nil, `'s'` addr+slot+valueRLP|nil, `'c'` addr+codeHash), and `executeBlock` writes directly: `Store.AppendWrites / AppendHeader / AppendLogs(posting frame) / PutCode / FlushAndSetExecHead`. Accepted-only, sequential. Full logs + receipt fields for the v2 stored sections come from a **seal-time re-execution pass** (`rpc.NewDeriveStored`).

**Unifying interface** (extends flatstate's Batch; epoch sink needs more than state ops):
```go
type Block struct {
    Height       uint64
    Hash, Parent common.Hash   // eth hash
    Time         uint64
    Container    []byte        // verbatim proposervm-wrapped container (epoch bodies guarantee)
    Header       *types.Header
    Ops          []capture.Op  // post-image, preimage-keyed (flatstate's op set, richest)
    Logs         []*types.Log  // full logs
    TxMeta       []TxMeta      // {GasUsed uint64; Status uint8} per tx
    Code         map[common.Hash][]byte
}
type Sink interface {
    Block(b *Block) error                                // preferred delivery; history sinks no-op/buffer
    Finalize(height uint64, hash common.Hash) error
    PreferenceReset(preferred []*Block) error
}
```
Plus two decorators: `AcceptedOnly` (buffers Block, acts on Finalize — the epoch sink) and `Async` (bounded channel, fail-loud on overflow — the chain-pace guard).

Changes per side:
- **flatstate**: add Container/Header/Logs/TxMeta/Code to Batch. Free: `follower/exec` already computes receipts+logs for its receiptsRoot check and holds the raw container. LMDB store keeps consuming Ops only; 0x04 diff row encoding version-bumps or stores the old subset.
- **epochdb**: extract the executor's Store-append tail into an `EpochSink` (Finalize = AppendWrites + AppendHeader + AppendLogs + PutCode + staging container append + flushEvery cadence). Op mapping: unified `OpAccount` must carry the **full `types.StateAccount`** (epochdb's SST rows and diff-apply verification need Root/CodeHash RLP; flatstate derives its fixed nonce/balance/codehash encoding from it). Destruct: flatstate's explicit `OpDestruct` maps down to epochdb's `recordAccount(addr, nil)`.
- **Bonus**: live Logs+TxMeta capture makes `DeriveStored` re-execution unnecessary for newly sealed epochs (it stays only for upgrading old epochs) — but see risk 5 on bit-identity.

## 3. Repo strategy: **(a) epochdb absorbs flatstate**

| Option | Code moved | Code deleted | Churn |
|---|---|---|---|
| (a) epochdb absorbs | ~7.4k LOC of leaf packages (store/mem/tipbus/node/capture/schema/sim/engine/replay/suitecache/sync/exec) | flatstate follower/consensus+net (~2k) | low: moved packages are self-contained, mostly zero avalanchego deps |
| (b) new repo | all 28k | same | max churn, breaks the committed manifest/docs paths, zero benefit (user owns both) |
| (c) flatstate absorbs | ~17.6k incl. epoch format, torrents, firewood | same | moves the side that must NOT churn (frozen epoch format, manifest paths, firewood FFI) |

epochdb has ~2.4x the surviving code, the newer follower generation, the heavier dependency set already integrated (firewood FFI, anacrolix torrent), and the format whose paths (`docs/epochs.manifest`, torrent infohashes) must stay stable. go.mod merge is clean: both on avalanchego/coreth/evm v1.14.2 + libevm 1.13.14-0.4.0.rc.2; add `PowerDNS/lmdb-go`, bump go 1.25.8→1.26.4, uint256 v1.2.4→v1.3.2. History via `git subtree add` if wanted; flatstate is 3 weeks old, not sacred.

## 4. The follower: **epochdb's port survives** as the engine core

It is the second-generation copy: flatstate's live-measured tuning already baked in, decoupled from libevm via injected `Parse`, has `Stats`, idle polling, and sits beside the archival-pool/GetAncestors machinery the unified node needs for deep history. Decisively: the **multi-source validator cross-check** (`fetch/follow.go: crossCheckedWeights` + hourly refresh with last-good fallback) is a trust property flatstate's single-RPC `fetchWeights` lacks — tip choice must not trust one RPC.

Deltas to reconcile (port back from flatstate into `fetch/consensus`):
1. **Preferred-tip surface**: `Sink.Verified/Head` + `emitHead` preference walking (epochdb engine only has `OnAnchor/OnAccept`). This is the tier-1 in-process API's food; the riskiest piece (see risk 1).
2. **Executor hook at issue time** (execute preferred blocks → unfinalized capture stack) + `SeedHeaders` for the BLOCKHASH 256 window.
3. **Store-resume anchor** (`Anchor{Height, EthHash, HashSet}` from the finalized watermark) alongside epochdb's poll-the-network anchor; resume from store when a watermark exists.
4. **Backfill placement**: keep epochdb's outside-the-engine `walkSpan`/archival-pool backfill (cleaner than flatstate's in-engine backfill); the executor already consumes staging.
5. Cosmetics: slog vs log; `net.Callbacks` wiring dies with flatstate's net package (epochdb's `inboundHandler.setConsensusCallbacks` covers it).

## 5. Process topology: **single process by default**

- The per-block history-side work is append-only writes (bucketlog/headers/logs/code), fsync batched every 256 blocks — µs-ms against a 1 blk/s tip; the executor replays at 5-6k blk/s. No structural backpressure.
- **Sealing is not on the capture path.** It reads staging read-only; live cohabitation of writer + cook/bench/verify via `OpenReadOnly` is production-proven. An in-process background sealer goroutine over the same files keeps that isolation.
- The real single-process risk is CPU/page-cache contention during a ~10-day-cadence zstd-max seal burst, not stalls. Mitigate: cap seal workers below GOMAXPROCS, and gate the default on a measured tip-commit-latency-during-seal test in stage 3.
- The two-process fallback is nearly free to keep: flock HA already assumes N identical processes on one corpus, so "writer process + sealer process" is a config, not a rewrite. Wrap history sinks in the `Async` decorator with fail-loud overflow either way — block application must never wait.
- **Hard constraint regardless**: sim/bots stay separate processes. `corethcore.RegisterExtras` type-asserts the concrete `*state.StateDB` and panics on sim's custom statedb (flatstate D16, load-bearing). The unified node's in-process Go API serves state/history/tip reads; *simulation* in-process API lives in the bot process over files-as-API (LMDB + tipbus), exactly tier 2 of the DESIGN tiering.

## 6. Migration order

| Stage | Work | Parallel with production |
|---|---|---|
| 0. Import | flatstate packages into epochdb, one go.mod, both old binaries still build | prod untouched |
| 1. One follower | Port Verified/Head + executor hook + store-resume into `fetch/consensus`; rewire flatstate's follower cmd onto epochdb's fetch net layer | new follower **dry-run 24h+ against live flatstate follower**, diff accepted heights/hashes |
| 2. Unified sink | `capture.Block`/`Sink` as §2; `node.Sink` (LMDB+mem+tipbus) and `EpochSink` as siblings; Firewood executor becomes the authoritative accepted-stream producer; live Logs/TxMeta capture | A/B: LMDB rows from old exec vs new stream over a replay range; byte-diff EpochSink output vs current executor output |
| 3. **First integrated milestone** | One binary: `epochdb serve --follow --history --pin-state` follows mainnet tip; LMDB sink + staging/epoch sink both live; historical RPC in-process; in-process sealer; flatstate's sim API served by the unchanged bot process against the same LMDB | runs ~1 week beside current flatstate follower before switchover |
| 4. Contracts | flock HA + generation counter, inotify/tipbus tail wakeup for epoch-tail readers, pinned-map OOM full-drop valve, in-process Go API packaged as a library door | additive |
| 5. Retire | delete flatstate follower/consensus+net, migrate prod, archive flatstate repo | switchover |

## 7. Effort and risks

| Stage | 0 | 1 | 2 | 3 | 4 | 5 | Total |
|---|---|---|---|---|---|---|---|
| Agent-days | 1 | 3-4 | 4-5 | 3-4 | 3 | 1 | **15-18** |

Top 5 risks:
1. **Re-adding preferred-tip emission to the ported engine** — consensus-shell regressions could stall acceptance or track a wrong tip. Mitigation: `Topological` stays untouched; long parallel dry-run diff vs the live flatstate follower is the gate.
2. **Post-image divergence between the two executors** (Firewood-authoritative vs trie-less: EIP-6780 assumption, multicoin slot normalization `key[0]&^=0x01` — flatstate normalizes via extstate, epochdb's interceptor captures raw statedb keys; both import extstate but this must be A/B-proven before the LMDB sink switches source).
3. **RegisterExtras process isolation** caps the "in-process Go API first-class" ambition: simulation can never live inside the unified node process. The tier-1 door serves reads/tip; sim stays a files-as-API reader. Surface this expectation explicitly.
4. **Seal bursts vs chain-pace/tail-latency** in single-process mode — measured gate in stage 3, two-process config preserved as the fallback the DESIGN already names.
5. **Bit-identity of epochs during the capture switch**: live-captured Logs/TxMeta must produce byte-identical stored sections to `DeriveStored` output, or infohashes/manifest break for independently built epochs; plus one binary now carries lmdb-go + firewood FFI + torrent cgo and a go-version bump — pin and CI-verify determinism before any newly-sealed epoch ships.

Key files behind the analysis: `~/epochdb/DESIGN.md:37-68`, `~/flatstate/capture/capture.go:192-206`, `~/flatstate/node/sink.go`, `~/flatstate/node/tracker.go`, `~/epochdb/exec/capture.go`, `~/epochdb/exec/executor.go:594-680`, `~/epochdb/fetch/consensus/engine.go:1-135`, `~/epochdb/fetch/follow.go:188-320`, `~/flatstate/follower/consensus/consensus.go:1-100`, `~/flatstate/cmd/follower/main.go`, `~/epochdb/cmd/epochdb/main.go:310-432`, `~/epochdb/state/seal.go`, `~/flatstate/DESIGN.md` (D7/D10/D16).

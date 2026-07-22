# epochdb HANDOVER — complete project state

Written 2026-07-22 ~11:00 JST at full handover. Everything a cold session needs to adopt this project. The design of record is `DESIGN.md` (read it first, every decision is dated there); this file is the operational state snapshot.

## What this is

Super-compact Avalanche C-chain historical node. Full standard RPC surface from immutable sealed epoch files at ~20-40x less disk than a stock archive node (measured ~460-630 B/tx sealed; full mainnet 1.2B txs projects to ~0.59-0.69TB including stored logs/receipts vs ~20TB stock). No databases: append-only files + RAM maps + mmap. Re-execution was the universal primitive; after the 2026-07-20 ruling, logs/receipts are STORED (epoch format v2) and re-execution remains only for traces/createAccessList/verification. Real snowman consensus tip-following (ported from flatstate). Torrent distribution of deterministic bit-identical epochs.

Repo: `~/epochdb` (github.com/containerman17/epochdb name, LOCAL ONLY, never pushed anywhere). Everything committed on `main`. Tag v0.1.0 pending release finale.

## Story in one paragraph

Designed from scratch in conversation 2026-07-17 (session f2b65d30's fork; second brain `projects/epochdb.md`). Fuji dress-rehearsal build proved the pipeline (fetch/replay/overlay/A-B), then was deleted as unrepresentative. Mainnet corpora: 100k -> 1M -> 10M blocks, each fully gated (thousands of A/B probes vs api.avax.network, zero unexplained mismatches, ever). Format v2 with unconditional stored logs+receipts. From-scratch clean rebuild (2026-07-21/22) produced the production corpus.

## Current state (2026-07-22 ~11:00 JST)

### Data on disk
- `~/epochdb/data-fresh/` — THE production corpus: mainnet blocks 0-10,000,000, from-scratch fetch + root-verified replay (ZERO mismatches over all 10M), all captures from block 0, cooked, sealed into 7 production epochs (EPOCH_TXS=10,000,000; epoch_1 alone = 4.9M blocks/6.38GB; ~3.1x compression) + raw tail above ~block 9.99M. Contains firewood/ frontier (~20GB) from the replay.
- `~/epochdb/data/` — the OLD 10M corpus (previous sync). Kept ONLY until the determinism comparison completes; its raw captures feed the comparison seal. 25k-tx epochs inside are mixed v1/v2 (a v2 re-seal was deliberately killed at 1,871/2,847 as doomed work). DELETE after verdict (user clean-slate mandate).
- `~/epochdb/data-prod-old/` — comparison seal output (old corpus raws sealed at 10M-tx). Two epochs only, then verdict, then deletable.
- Manifests: `data-fresh/epochs.manifest` native; `docs/epochs-prod.manifest` committed copy (pending final epoch + driver commit). The old `docs/epochs.manifest` (25k epochs) is being removed in the same commit.

### Processes / agents in flight (will complete autonomously; adopt their outputs)
1. RELEASE DRIVER agent: owns the finale: old-corpus comparison seal (2 epochs, ETA ~10:45 + ~12:45 JST) -> hash-for-hash determinism verdict vs fresh epochs 1-2 -> FULL-SURFACE gates on data-fresh (every method in docs/rpc-coverage.md: ab-bench, ab-bench-tx, ab-bench-rpc, ab-bench-logs, rpc-bench table) -> clean-slate deletion of old world -> commit docs/epochs-prod.manifest + docs/RUNBOOK.md (drafted, uncommitted) -> start persistent seeder (`epochdb seed --data ./data-fresh`, setsid, ~/epochdb/seed.log) -> `git tag v0.1.0` -> final scoreboard report.
2. FFI v0.7 BUMP agent: attempting firewood-go-ethhash ffi v0.3.1 -> v0.7.0 for `eth_getProof` at tip (v0.7 added EthGetProof, EIP-1186). Guardrails: stop if it drags graft/avalanchego; on-disk 0.3->0.7 compatibility must be proven or frontier rebuilt from SSTs; gate = 200+ proofs verified vs block-10M header stateRoot. May land or report a blocker.
3. VERIFY BUILDER agent: `bootstrap --verify` + standalone `epochdb verify`: pipelined no-execution verification (diff-apply state roots via throwaway Firewood + txRoot + reconstructed receiptsRoot + header chain; DESIGN.md "Verification without re-execution" + "Verification DX"). Corruption unit tests written first; full-corpus gate deferred until the box is quiet.

### Release checklist status
- [x] Full method parity (docs/rpc-coverage.md is truthful; last additions 82b7d1e: eth_etherbase, debug_traceCall, debug_getModifiedAccountsBy* AT ANY HEIGHT from writelog diffs (differentiator), debug_getBadBlocks)
- [x] From-scratch corpus, zero mismatches
- [x] Production seal at EPOCH_TXS=10M
- [ ] Determinism verdict (in flight, ~13:00 JST)
- [ ] Full-surface gates on data-fresh (driver, after verdict)
- [ ] Clean-slate deletion + manifest + seeder + runbook + v0.1.0 tag (driver)
- [ ] getProof-at-tip (bump agent; optional for release)
- [ ] bootstrap --verify (builder; post-release acceptable)

## Key measured numbers (scoreboard of record)

- Replay (full pipeline on this 16-core/25GB box): fetch ~1,650 blk/s multi-anchor (nighttime pool degrades to ~450-640); replay 0-10M ~13.5h (zero root mismatches, twice: two independent syncs); seal ~2h per 10M-tx epoch (tx-bound); cooks: minutes.
- Sizes: sealed 409-592 B/tx by era (dense eras cheaper/tx); stored logs +18.5% (dedicated 128KB logs dict beats container dict by ~2.5pt), receipt fields +1.2%; whole-epoch compression ~3.1-4.2x; 1.2B-tx mainnet ~0.59-0.69TB.
- State-vs-trie split at 10M: live state = 1.79M accounts + 41.5M slots = 0.63GB values / 2.82GB keys+values; Firewood = 20.7GB => 86.4% merkle overhead. Light-verify (no Firewood) saves ~140GB at full mainnet; frontier rebuilds from SSTs in minutes.
- Serving (1M corpus, HTTP): state reads 200-320µs p50, 23-31k rps @c8 (transport ceiling ~31k); eth_call 3.8k rps; stored-logs killed re-execution: receipts/getLogs now direct reads. Lock-free server 16k-20k rps mixed.
- Tx index: EF fp48, 288ns lookups, 0 FPs; 47-51 bits/tx.
- Snowman follower: 31-min live mainnet gate, 1,743 blocks accepted via real snowball (K=20/a=15/b=20), median lag 4 blocks; validator set from 2+ cross-checked P-chain RPCs (>=95% stake agreement).
- Torrent: manifest gen 23s for 2,847 epochs; two-node bootstrap 51 epochs/65s localhost; `bootstrap` exits 0 exactly on manifest completion (gate-verified both directions).

## Decisions of record (all in DESIGN.md with dates; headline list)

EPOCH_TXS=10M production (25k was corpus-stress only). Stored logs+receipts UNCONDITIONAL, flag killed, re-exec deleted from reads. Per-epoch container dict + dedicated logs dict (per-TYPE dicts otherwise rejected by measurement; per-EPOCH essential: old dicts decay on new data). Verbatim proposervm containers guaranteed. Signing manifest-level only, never in-file. eth_getProof: impossible historically (no per-block tries) AND at tip on ffi v0.3 (no EIP-1186 support; v0.7 added it: bump in flight). Snowman must-fix DONE via flatstate port. Files-as-API entry-point tiering; flock single-machine HA + dev/prod cohabitation; sub-5ms tail via visibility/durability split + inotify; pinned-map contract with OOM full-drop valve (unified node). S3-as-truth (disk = LRU of byte ranges) recorded as v3 deployment mode. Unified node: epochdb absorbs flatstate, docs/UNIFIED-NODE-PLAN.md is the 15-18 agent-day blueprint (committed ae56251).

## Operations quick reference

- Build/run: `go run ./cmd/epochdb <cmd>` — fetch (--network mainnet --tip-override N | --follow --vdr-sources ...), exec (--stop N --state-cache 3 --commit-every 1000; set GOMEMLIMIT=10GiB on 25GB boxes), cook-index, cook-txindex, seal (--out DIR --epoch-txs N), manifest, seed, bootstrap, serve (--port), verify (pending), ab-bench* / rpc-bench gates.
- New machine bootstrap flow: copy manifest -> `epochdb bootstrap --data <dir>` (exits 0 when complete) -> `epochdb serve`. Live follow: `fetch --follow` (needs vdr-sources).
- Long stages MUST be setsid-detached (harness bg tasks get reaped; learned twice).
- Memory: default 6GiB state-cache thrashes 25GB boxes in dense eras: use --state-cache 3 + GOMEMLIMIT=10GiB (incident 2026-07-19, documented).
- Multi-process: writers truncate torn tails on open: NEVER open a live writer's dir with the writing opener; state.OpenReadOnly / fetch.Reader exist for cohabitation (production-proven).
- Two cook/seal processes must not share a dir (tmp+rename races).
- du lies on WSL2/page-cache-heavy dirs; footers/manifests are truth.

## Gotchas archive
Wiki: `~/dotfiles/wiki/epochdb/` (replay wiring + anchored backfill; overlay read contract + sorted index format incl. SELFDESTRUCT delete-map lesson: never per-read scans). Devlogs: `~/dotfiles/devlogs/epochdb/2026-07-17..22.md` (complete daily record: every incident, number, and decision). deforestationdb's "block 100k" anchor constant is mislabeled. klauspost dict training is nondeterministic: dicts come from pinned zstd CLI v1.5.7 only. Public archive (api.avax.network) refuses the whole debug namespace + createAccessList + getProof: gates there are self-consistency/cryptographic.

## What's next (in priority order, when someone resumes)

1. Adopt the in-flight agents' results: verdict, gates, release artifacts, possible getProof, verify unit. If a verdict FAILED: the section-level diff localizes fetch- vs capture-nondeterminism: treat as the top finding.
2. Deploy the live node on the OTHER machine (user decision pending): bootstrap from this box's seeder, then `--follow`; this box stays the 10M fixed corpus + seeder (disk ceiling).
3. Unified node merge: execute docs/UNIFIED-NODE-PLAN.md (user pre-approved direction 2026-07-20, staged, 15-18 agent-days).
4. Deferred: ffi v0.7 EthGetProof if the bump agent reported a blocker; slab-parallel verify (rejected for v1); atomic-tx index section (future in-place format bump); S3 range-read deployment mode; SAE-era re-tuning.

## Second brain / PM

`~/dotfiles/agents/second-brain/projects/epochdb.md` (state) + `projects/epochdb.log.md` (dated history) + INDEX line. PM loop convention: 30-min cron "keep pushing" during active builds (was stopped 07-19, re-armed ad hoc). Attention markers: 🚨 needs-user / 🟢 FYI.

## HELICON ALERT (added at handover, 2026-07-22)
AvalancheGo v1.15.0-fuji "Helicon" activates on Fuji 2026-07-28 11:00 ET. ACP-194 Streaming Asynchronous Execution DECOUPLES CONSENSUS FROM EXECUTION: this likely changes header/state-root semantics and will affect epochdb's replay verification, the follower's acceptance model, and flatstate equally. Mainnet-compatible release comes after Fuji validation. Our v1.14.2 pin is fine for the mainnet production node UNTIL the mainnet Helicon release; the dep-bump-on-release-schedule policy now has a real fuse. ACP-283 dynamic minimum gas price also touches fee logic. The user pivoted to tackling Helicon at this handover; epochdb resumption must reconcile with whatever Helicon work produced.

# Limited-history mode: state-synced floor + epochs above it

Status: design agreed 2026-07-28 (JST), unimplemented. Owner decisions in this
file are user-confirmed in conversation on 2026-07-28 unless noted.

## Business case

Validator host: two real validator nodes share a 2TB disk, ~600-700GB free, and
the same box must run the fast current-state node for a trading bot. Needed:
current state and fast tip. Not needed: deep historical lookups. Full-mainnet
epochdb is ~590-690GB of sealed epochs plus ~100-150GB of Firewood, which does
not fit. Target ~200GB.

## The one structural change

State reads descend epochs newest-to-oldest and fall through to a floor
(`state/overlay.go:298-328`), and that floor is the genesis alloc
(`state/overlay.go:359-383`). That is the only reason full coverage from genesis
is mandatory. Replace the floor with a flat live-state snapshot at block B and
every state read at N >= B is answered by (raw tail, epochs above B, base file)
with no history below B existing at all.

Deleting epochs WITHOUT swapping the floor is silently wrong, not an error: the
descent misses, hits the genesis alloc, and reports "account does not exist".
The floor check is the safety property of this whole feature.

## Decisions (2026-07-28)

1. **Bottom comes from network state sync**, not from torrenting and folding old
   epochs. Reason: the full-mainnet sealed corpus does not exist yet (this box
   has blocks 1..9.93M only), so the fold path is blocked behind a 9-13 day
   replay. Sync gets a root-verified bottom in hours.
2. **Torrents are out of scope here.** They remain the day-one distribution
   story for a full archival node, nothing more. No manifest alignment work, no
   cumulative-tx-count anchor, no bit-identical local epochs.
3. **B is wherever sync lands.** You cannot pick the block, you serve what the
   network gives. The epoch containing B is unsealable (no post-images below B),
   so capture and sealing start above B and epoch boundaries are local
   (EPOCH_TXS counted from our own start).
4. **Hash-keyed bottom, accepted forever.** State sync yields keccak keys, not
   preimages. Reads hash the address/slot on the way down. Consequence: the base
   layer can never be iterated or folded together with preimage-keyed epoch
   rows. Accepted.
5. **Firewood stays.** Per-block state-root verification is not negotiable, so
   the ~100-150GB frontier is a fixed cost, not a lever.
6. **Port, do not copy.** flatstate's `follower/sync` moves into epochdb per
   docs/UNIFIED-NODE-PLAN.md stage 0/1. "Code is cheap, ideas and testing are
   expensive."
7. **Target: Fuji, upgraded to Helicon, on this box.** Fuji activates SAE
   2026-07-29 00:00 JST, and v1.14.2 cannot handshake with v1.15 peers, so this
   track carries the Helicon dep bump with it (docs/HELICON-IMPACT.md).
8. **Gate: A/B against the public network RPC at tip**, same discipline as every
   previous milestone.

## Base file format (implementer's call, 2026-07-28)

`base_<block>` is a new file kind, not an epoch: hashed keys do not fit the
53-byte preimage row layout. It reuses the epoch machinery (zstd-framed sorted
blocks, sparse index, keybloom, mmap, footer with magic + version) with:

- key layout `kind(1) | keccak(addr)[32] | keccak(slot)[32]` = 65 bytes
- rows are values only, no block numbers: the whole file is "state as of B"
- a code section: every code blob referenced by a live account, by hash
  (see "code.log gap" below, this fixes it for the floor)
- the 256 headers below B, so `eth_call` BLOCKHASH works in [B, B+256)
- footer records B and the state root, verifiable by rebuilding into a throwaway
  Firewood and comparing to `header(B).Root`

## Code.log gap (pre-existing, found during this design)

Contract code is not in the epoch format and not in the manifest: it lives only
in the standalone `code.log` (`state/codestore.go`, read at
`state/overlay.go:433-445`). A node bootstrapped purely from torrented epochs
therefore cannot serve `eth_getCode` or any `eth_call` touching a contract. The
base file's code section fixes this for limited-history nodes; the torrent
bootstrap path still needs its own answer.

## What changes in the code

- `state/overlay.go`: floor check at the top of `History.search`; genesis-alloc
  fallthrough only below a floor of 0; base-file lookup as the last descent step
  (hash the key here).
- `state/epochread.go:522-556`: coverage anchors at the floor instead of
  genesis; `RequireCovered` reports "pruned below block B".
- `state/overlay.go:95-121`: the delete map only needs epochs above B (the base
  file is a post-image, no pre-B deletions or resurrected slots can exist).
- `state/overlay.go:92`: `OpenHistory` must open with a base file and no buckets.
- `cmd/epochdb/main.go` serveMain: block-hash index scan starts at the floor,
  not 0.
- `rpc/tx.go:106`, `rpc/block.go:221`, `rpc/logs.go:195`: coverage errors become
  pruned errors. Below-floor tx-by-hash returns a typed pruned error, not null
  (a bot silently seeing null for a real tx is the bad failure mode).
- Sealing above B: unchanged machinery, boundaries counted from our own start.

## RPC contract

Full archive semantics from block B forward, nothing below B, and B is printed
at startup. Specifically: state getters, `eth_call`, traces, receipts, logs,
bodies and tx lookups all serve for N >= B and return a typed pruned error for
N < B. `debug_getModifiedAccountsBy*` remains raw-tail-only (already true after
`--delete-raw`). `eth_getProof` unchanged (tip only, and only with ffi v0.7).

## Disk budget (full mainnet, tip-only)

base ~20GB + code ~5GB + 0-3 epochs (4GB each) + raw tail ~15GB + Firewood
~125GB = ~165-180GB. The raw tail is the churn item: at EPOCH_TXS=10M it holds
~10 days of captures at 3-4x sealed size. Epochs above B accumulate at roughly
4GB per 10 days (~150GB/year), so a fold-down step is eventually needed but is
explicitly not in this design (blocked by decision 4: hashed bottom cannot fold).

## Work order

1. Floor plumbing against the existing mainnet 10M corpus with a synthetic
   floor. No Helicon, no sync, no network. Gate: a node pruned to floor F answers
   byte-identically to the full node for every block >= F.
2. Helicon bump (deps to v1.15.0-fuji, saevm execution path, v1.15 handshake).
   Long pole, bigger than the limited-history feature itself.
3. Port flatstate's `follower/sync`, writing its output into the base file plus
   code.log.
4. Follow and seal above B on Fuji, A/B against the public Fuji RPC.

## Open questions

- Does SAE change the state-sync protocol? Unverified, and it gates step 3 on
  the target network.
- Raw-tail size: worth cutting EPOCH_TXS locally on a limited node now that
  local boundaries no longer have to match anything?

## Rejected alternatives

- **Lazy-attach old epochs on query**: works for epoch-local reads (bodies, tx,
  receipts, logs) and is useless for state, since a tip read can hit a key last
  written years ago. Would need the three coverage gates relaxed to per-epoch
  presence. Not now.
- **Torrent old epochs, fold last-write-per-key into a flat bottom, delete**:
  strictly better if a full sealed corpus ever exists (preimage-keyed, single
  keyspace, verified by construction). Blocked today, revisit later.
- **Published snapshots every M epochs** (so anyone can start state history at
  an arbitrary depth): the scalable public form of this design, out of scope.

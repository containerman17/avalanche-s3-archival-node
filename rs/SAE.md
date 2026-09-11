# SAE (ACP-194) header, root and admission spec

Branch `sae`, off `rust` a8ab2c0. This is the pinned contract P1 (engine, this
crate), P2 (plugin) and P3 (rpc) build against. It changes WHEN execution
happens, not WHAT is checked. Acceptance no longer implies executed state;
everything else stays fully verified (signatures, projected nonces, worst-case
funds, size and gas capacity), and no consensus parameter changes.

## Two streams

- `accept(block)` records the block in the accepted chain and advances the
  projection, then returns. No execution, no state root on the vote path.
- A continuous executor runs in acceptance order, `k` blocks behind the
  accepted head (`k` = `sae-settlement-blocks`, default 8). Per block it
  produces receipts, logs, the state diff and the SETTLED state root.
- `settle(h)` advances the settled root to height `h`, exposes `h`'s
  receipts / logsBloom / stateRoot, and retires `h`'s txs from the projection.

The accepted head races ahead of the settled head; the gap is the settlement
LAG (in blocks and, via block timestamps, in seconds). Under a feed faster than
execution the lag is bounded by the consensus in-flight cap (the number of
processing blocks), not by execution falling behind; if execution cannot keep
up the lag grows without bound and the settled tx/s number is meaningless.

## Header / root semantics

Compatibility break (accepted): a SAE header commits to the settled root of
`h-k`, not `h`, so a SAE chain is NOT block-compatible with stock subnet-evm.
Fields, matching the C-Chain SAE (`vms/saevm`) layout:

- `header.Root` (stateRoot) at height `h` = the SETTLED post-execution root of
  the block at height `h-k`, not of `h`. For `h <= k` it is the genesis root.
- `header.ReceiptsRoot` / `header.LogsBloom` at `h` cover the block settled at
  `h`, i.e. the block at `h-k`, not `h`'s own body. (`h`'s own receipts and
  bloom become available only when `h` itself settles, at the header of `h+k`.)
- `header.GasUsed` at `h` = the WORST-CASE charged gas of `h`'s OWN body:
  `sum(tx.gasLimit)` (plus atomic-op gas on a real C-Chain). This is the only
  per-block quantity still checkable in isolation at accept time.
- `header.GasLimit` / `header.BaseFee` are the builder's worst-case capacity
  and base fee, not the execution values; never compared against execution.

So `settle(h)` must produce exactly the values the header of `h+k` carries:
the settled root at `h`, and `h`'s receiptsRoot / logsBloom.

`k = 0` collapses SAE to the synchronous model (the `rust` behaviour): the
header at `h` commits to `h`'s own settled root, accept implies execution.

## Admission: verify_light (no execution, no unsettled state read)

`verify_light(block)` returns valid/invalid only, reading the projection, never
executing and never reading unsettled state:

1. Every tx's signature recovers (sender present).
2. Each tx's nonce == its sender's PROJECTED nonce at the tx's position:
   `projected_nonce(sender) = settled_nonce(sender) + (count of that sender's
   accepted-but-unsettled txs) + (count of that sender's earlier txs in this
   block)`.
3. Worst-case funds, cumulative over the sender's unsettled sequence:
   `settled_balance(sender) >= sum over the sender's unsettled txs (already
   accepted plus this block, up to this position) of gasLimit*feeCap + value`.
   `feeCap` = the tx's max fee per gas (`gas_price` for legacy / 2930,
   `maxFeePerGas` for 1559). Cumulative, so a sender's second unsettled tx is
   checked against the balance already committed to the first.
4. Block gas capacity: `sum(tx.gasLimit) <= capacity`, capacity = `20s x target`
   (the ACP-194 block-gas capacity, charged on tx gas LIMIT for unsettled
   blocks). Block byte size <= the size cap.

The per-tx charged-gas floor for the gas clock is `ceil(gasLimit/2)`
(`MinimumGasConsumption`): a tx is charged at least half its limit even if it
uses less. It does not change admission (worst case is the full limit) but the
settled gas clock uses it. `charged_gas_floor(limit) = (limit + 1) / 2`.

## Projection

Per sender: the SETTLED nonce and balance, plus a running count and worst-case
cost of that sender's accepted-but-unsettled txs.

- `accept_block(b)`: for each tx, `unsettled_count += 1`,
  `unsettled_cost += gasLimit*feeCap + value`; advance the accepted head.
- `settle_block(h)`: for each tx of `h`, `unsettled_count -= 1`,
  `unsettled_cost -= its cost`, and `settled_nonce += 1` for its sender (the
  executed nonce advanced by exactly the settled txs). `settled_balance` is set
  from the executor's post-`h` account state where available. Advance the
  settled head; the invariant `projected_nonce = settled_nonce +
  unsettled_count` is preserved across accept and settle.

A sender first seen at accept with no settled baseline is seeded to
`settled_nonce = the tx's nonce` (its correct chain nonce at that height) and a
placeholder balance; the first real settle of that sender overwrites the
baseline. Genesis-alloc accounts are seeded from the alloc at open.

## Recovery (settlement-window contract)

Durable state = the accepted chain (block store) and the settled root (the
rolled trie / Firewood revision). On open:

1. Find the last settled height `S` (the durable root's height among the
   store's headers, the existing `rust` replay path).
2. Re-execute the accepted-but-unsettled backlog `S+1 .. accepted_head` forward
   from the settled root, root-checking each against its header, to resume the
   executor and rebuild the projection (`accept_block` over the backlog rebuilds
   the projected nonces and costs; `settle_block` advances as re-execution
   settles).
3. Resume the two streams. The settled root and projection resume, and the
   final settled root matches an uninterrupted run.

The backlog is bounded by `k` (plus the consensus in-flight cap), so recovery
is bounded work.

## Oracle (the correctness gate)

The settled state root at every height `h` equals a full synchronous
re-execution of blocks `1..h` with the existing executor, byte-for-byte. On the
Step and beam dumps (synchronous subnet-evm blocks whose `header.Root` is the
post-execution root of `h`), this means the settled root at `h` == `header[h].Root`
for every `h`. The engine checks this on the settle path (the checker thread);
a mismatch exits the process. A synchronous dump can trip the stricter SAE
worst-case funds check at admission (the synchronous builder never applied it);
the bench counts such admission rejections rather than aborting, because the
dump is the ground-truth chain and must stream in full. The nonce projection is
exact on a valid dump and any mismatch is a projection bug.

## What P2 / P3 implement against this

- P2 (plugin): `Verify = verify_light` (no execution, independent of the parent
  EXECUTED state so heights pipeline to OptimalProcessing depth); `BuildBlock`
  with no execution, the header committing the settled `h-k` root; `Accept` /
  `Reject` on the projection; the SAE header encode / decode for the layout
  above.
- P3 (rpc): state reads serve the SETTLED state by default (`latest` = settled
  head); a block / receipt / trace for an accepted-but-unsettled height answers
  a clear "not settled yet"; a new surface exposes the settled head and the lag;
  `eth_blockNumber` = the accepted head.

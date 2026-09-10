# epochdb validator: Go shell (mempool, gossip, BuildBlock) + Rust engine (execution, state, RPC)

User ruling 2026-09-09: build the validator as a hybrid. As much as possible in Rust, as few boundary
crossings as possible, minimal Go heap (the Go GC was up to 30% of runtime in benchmarks). Reuse
subnet-evm's building blocks (txpool, gossip, tx selection) on the Go side. No Go execution, no Go
state, no Go trie: the Go side never holds a StateDB or an interpreter.

## Shape

- One process per chain: the rpcchainvm plugin is Go (`cmd/epochdb-validator`, built from our
  `vmchain` follower shell), linking the Rust engine as a static library over cgo.
- Crossings per block: Verify 1, Accept 1, BuildBlock 1 (2 if the gas limit is not filled), pool reset
  1 batched read of pending senders' nonces/balances, plus 1 per incoming tx for admission (or 1 per
  gossip message, batched). RPC: 1 per request forwarded to Rust; Go answers only pool methods.
- Verify computes the state root INLINE and returns it (validator shape). Bootstrapping keeps the
  pipelined checker (blocks the network already accepted), NormalOp = root in Verify.
- Go heap discipline: tx bytes live in the pool once; every payload to Rust is passed as a pointer +
  length, results come back in Rust-owned buffers freed by `epochdb_buf_free`. No JSON across cgo.

## C ABI (rs/ffi, crate `epochdb-ffi`, staticlib `libepochdb_engine.a`, header `epochdb_engine.h` via cbindgen)

All functions return 0 on success or a negative code (`EPOCHDB_ERR` -1 failed, `EPOCHDB_EINVAL` -2 null
pointer / bad length / unknown enum, `EPOCHDB_ENOTFOUND` -3 unknown block id or height, `EPOCHDB_EPANIC` -4
a Rust panic caught at the boundary: close the engine); the last error text is readable with
`epochdb_last_error(engine, epochdb_buf*)`. `epochdb_buf { uint8_t* ptr; size_t len; }` is Rust-owned,
freed with `epochdb_buf_free`. Hashes are 32-byte big-endian arrays. Block bytes are the INNER
subnet-evm block RLP (what avalanchego hands a plugin's ParseBlock; proposervm wraps outside).

```
epochdb_engine* epochdb_open(const uint8_t* data_dir, size_t,
                             const uint8_t* genesis_json, size_t,
                             const uint8_t* upgrade_json, size_t,     // may be empty
                             const uint8_t* config_json, size_t,      // same keys as the plugin's Initialize config bytes
                             const uint8_t chain_id[32], const uint8_t subnet_id[32], uint32_t network_id,
                             epochdb_buf* err);
void  epochdb_close(epochdb_engine*);
int   epochdb_set_state(epochdb_engine*, uint32_t state);            // 1 bootstrapping, 2 normal op (avalanchego snow.State values)

int   epochdb_parse(epochdb_engine*, const uint8_t* block, size_t, epochdb_block_meta* out);
      // out: id[32] (keccak(header)), parent[32], height, timestamp; caches the parsed block by id
int   epochdb_verify(epochdb_engine*, const uint8_t id[32], uint64_t pchain_height /* 0 = none */,
                     epochdb_verify_out* out);
      // executes on top of the parent's pending state (or the accepted head), checks gasUsed /
      // receiptsRoot / logsBloom / stateRoot against the header (state root INLINE in NormalOp),
      // keeps the pending state; out: state_root[32], gas_used, tx_count. Non-zero return = invalid.
int   epochdb_accept(epochdb_engine*, const uint8_t id[32]);         // apply pending, store, roll bookkeeping
int   epochdb_reject(epochdb_engine*, const uint8_t id[32]);         // drop pending (descendants rejected by consensus one by one)
int   epochdb_last_accepted(epochdb_engine*, uint8_t id[32], uint64_t* height);
int   epochdb_block_id_at_height(epochdb_engine*, uint64_t height, uint8_t id[32]);
int   epochdb_get_block(epochdb_engine*, const uint8_t id[32], epochdb_buf* block_bytes);

int   epochdb_build(epochdb_engine*, const uint8_t parent_id[32], uint64_t timestamp_ms,
                    const uint8_t coinbase[20], uint64_t pchain_height,
                    const uint8_t* txs, size_t /* RLP list of tx envelopes, in the miner's order */,
                    const uint8_t* senders, size_t /* 20 bytes per candidate (the pool's recovered senders), or NULL, 0 */,
                    epochdb_build_out* out);
      // executes candidates on top of parent's state with subnet-evm's miner semantics (skip nonce-low,
      // pop sender on nonce-high/out-of-gas/underpriced, stop at gas limit), builds header (baseFee from
      // parent per fee config, blockGasCost, extra with fee window, timestamp rules), computes stateRoot,
      // receipts, bloom; keeps the result as a verified pending block so the following Verify of the same
      // id is a lookup. out: block_bytes (epochdb_buf), id[32], gas_used, included_count,
      // skipped (epochdb_buf: for each candidate index a u8 reason code), needs_more (1 if gas limit
      // unfilled and candidates exhausted).
      // txs NULL / 0: the engine takes the candidates from ITS OWN POOL (effective tip at the block's base fee
      // desc, arrival, per-sender nonce order; 1.5x the gas limit and the miner's size target plus 1/8 as the
      // cut; senders past what the block's unaccepted ancestors already hold), see "Mempool" below.
int   epochdb_pool_add(epochdb_engine*, const uint8_t* txs, size_t /* RLP list of byte strings, one tx envelope
                       (MarshalBinary form) each */, uint8_t local, epochdb_buf* out);
      // out: 33 bytes per input: a code, then the tx hash (keccak of the bytes handed in). Codes: 0 ok, 1 already
      // known, 2 replaced an older tx of the same nonce, 3 underpriced (tip under tx-pool-price-limit, fee cap under
      // the fee config's min base fee, a replacement under the price bump, or not better than the cheapest tx of
      // a full pool), 4 nonce too low, 5 insufficient funds (alone, or with the sender's executable sequence),
      // 6 gas limit over the block's, 7 intrinsic gas, 8 invalid signature / chain id / unprotected legacy tx,
      // 9 pool full (per-account queue, per-account slots under a full pending set, or no evictable tx), 10 other
      // (does not decode, type > 2, over 128 KiB, tip over fee cap, initcode over 49152). `local` is honoured only
      // with `local-txs-enabled` (exempt from the price limit and from eviction).
int   epochdb_pool_status(epochdb_engine*, uint64_t* pending, uint64_t* queued);
int   epochdb_pool_has(epochdb_engine*, const uint8_t hash[32], uint8_t* out);      // 1 when the pool holds it
int   epochdb_pool_gaps(epochdb_engine*, epochdb_buf* out);   // UTF-8: up to 3 senders with queued txs and no executable one
                                                                // (pool nonce, state nonce, lowest queued, what became of the gap nonces)
int   epochdb_pool_content(epochdb_engine*, const uint8_t* addr /* 20 bytes, or NULL = every address */,
                           size_t limit /* per half, 0 = all */, epochdb_buf* out);
      // out: RLP list of envelopes, pending (address order, then nonce) then queued
int   epochdb_pool_nonce(epochdb_engine*, const uint8_t addr[20], uint64_t* out);  // state nonce + executable txs;
      // EPOCHDB_ENOTFOUND when the pool holds nothing of the address (the caller uses the state's nonce)
int   epochdb_pool_wait(epochdb_engine*, const uint8_t* parent_id, uint64_t timeout_ms, uint64_t* out); // blocks until a FREE executable tx (not held by the unaccepted chain under parent_id; null/zero/head = any)
      // is pending (out = how many free ones the pool holds) or the timeout passes (out 0); returns at once when one already is
int   epochdb_pool_drain_gossip(epochdb_engine*, epochdb_buf* out);                  // RLP [[envelopes...], [hashes...]]:
      // every tx admitted since the previous call (local and remote), oldest first, and every tx hash that left the
      // pool since then (mined, replaced, dropped), both under one pool lock; empty buffer = neither
int   epochdb_account_state(epochdb_engine*, const uint8_t* addrs /* 20*n */, size_t n,
                            const uint8_t block_id[32] /* zero = accepted head */, epochdb_buf* out);
      // out: n x { uint64 nonce LE, uint8 balance[32] BE } at the given block's state (pending allowed)
int   epochdb_head_header(epochdb_engine*, epochdb_buf* header_rlp);   // accepted head header (base fee etc. for the pool)
int   epochdb_rpc(epochdb_engine*, const uint8_t* body, size_t, epochdb_buf* response);   // JSON-RPC, single or batch
int   epochdb_health(epochdb_engine*, epochdb_buf* json);   // {"height","root-checked","normal-op","pool-dup" (txs answered Known by hash, no recovery),"pool-recovered","pool-lock-ms","pool-add-ms"}
void  epochdb_buf_free(epochdb_buf*);
```

Thread safety: every function may be called from any goroutine; the engine serializes verify/accept/
reject/build behind its existing mutex, reads and rpc take snapshots. The pool functions take the pool's
own lock (microseconds per tx); `epochdb_pool_add` reads the unseen senders' state under the execution
mutex once per batch, before the pool lock, and `epochdb_pool_wait` blocks the calling thread on the
pool's condition variable.

## Mempool (rs/chain/src/pool.rs, 2026-09-10)

The transaction pool lives in the engine. Admission (`epochdb_pool_add`, and `eth_sendRawTransaction`
through `epochdb_rpc`, which admits every send of a JSON-RPC batch in one pool call) decodes, checks
libevm's stateless rules and recovers the sender in parallel on rayon, reads the unseen senders' nonce
and balance at the accepted head in one batch, then inserts under the pool lock: known hash, nonce >=
state nonce, balance >= cost and >= the sender's executable sequence + cost, replacement by fee cap AND
tip both over `tx-pool-price-bump` percent, the caps (`tx-pool-account-slots`, `tx-pool-global-slots`,
`tx-pool-account-queue`, `tx-pool-global-queue`; a full pool evicts its cheapest remote tx for a dearer
newcomer), `tx-pool-lifetime` for idle senders with no executable tx. `epochdb_accept` moves the pool in
the same call: the block's txs leave, its senders and (when they hold txs) its recipients are re-read at
the new head and re-settled, the head rules (block gas limit, fee config min base fee) move; nothing is
walked per head. `epochdb_build` with no candidates takes them from the pool. Only the Go shell's gossip
and BuildBlock timing remain in Go; the RPC pool methods are answered by the engine's JSON-RPC
(`txpool_status/content/contentFrom/inspect`, `eth_pendingTransactions`, `eth_getTransactionCount(addr,
"pending")` = state nonce + executable txs). Rules, deviations and numbers: cmd/epochdb-validator/E2E.md,
"Mempool in the engine". No callbacks into Go (heads for
/ws are served by the Rust rpc's own ws when mounted; the Go shell mounts `/ws` by proxying the upgrade
to Rust's ws server over a local socket, or leaves /ws to a later step).

## Go shell (cmd/epochdb-validator)

- rpcchainvm plugin (our vmchain shell): Initialize -> epochdb_open with the Initialize bytes; ParseBlock
  -> epochdb_parse; Verify/Accept/Reject -> the engine; SetState; GetBlock etc. Health.
- Mempool: subnet-evm/libevm `core/txpool` (legacy pool) compiled in, fed by a `BlockChain` adapter:
  CurrentBlock/head from epochdb_head_header, chain head events from Accept, `StateAt` returns a state
  reader whose GetNonce/GetBalance go through epochdb_account_state (cache per head; the pool's reset
  batches every pending sender in one call). No trie, no StateDB.
- Gossip: subnet-evm's tx gossip (`plugin/evm/gossip*.go`, avalanchego p2p gossip SDK) over AppGossip /
  AppRequest / AppResponse; the pool signals `common.PendingTxs` to consensus through the messenger.
- BuildBlock: subnet-evm miner's ordering (price-and-nonce heap by effective tip), candidates capped by
  gas limit estimate, one epochdb_build call; on needs_more, one more round. The returned bytes are the
  block; the wrapper is proposervm's.
- RPC: mount `/rpc` (and `/ws` if cheap) -> epochdb_rpc, except eth_sendRawTransaction,
  eth_sendTransaction (refuse), txpool_*, eth_pendingTransactions, which the Go pool answers; pending-tag
  reads (`eth_getTransactionCount(addr, "pending")`) = engine nonce + pool pending count.
- Nothing else in Go. Fetch/bootstrap stays avalanchego's (this plugin runs under avalanchego).

## Oracle (the only one that matters)

A local tmpnet L1 (avalanchego at ~/avalanchego, tmpnet tooling used for the warpauth and evmwallet
demos) with 5 validators: 3 running our plugin, 2 running stock subnet-evm v1.14.2 with the same genesis.
Submit transactions (transfers, a contract deploy, contract calls, a failing tx, a nonce gap) through
both kinds of node; the chain must advance, every block built by ours must be accepted by the stock
validators and vice versa, roots and receipts identical on every node (eth_getBlockByNumber /
eth_getBlockReceipts byte-equal across all 5 at the same height), and the stock nodes' logs show no
invalid-block rejections. Then a load run: a tx generator at a few hundred tx/s for 10 minutes, blocks
full, no divergence, memory flat on ours.

## Go side notes for rs-ffi (go-validator, 2026-09-09)

The Go shell (branch `go-validator`, `validator/engine.go`, header copy at
`cmd/epochdb-validator/stub/epochdb_engine.h`) compiles against the ABI above with these readings;
follow them or tell go-validator what changed:

1. `epochdb_build`'s timestamp is MILLISECONDS (`timestamp_ms`). Local networks (tmpnet, id 12345)
   activate Granite at genesis, so the engine derives `Time = ms/1000` and `TimeMilliseconds = ms`
   and applies the parent rules (non-decreasing, min delay excess). Go already waits for the ACP-226
   min delay before calling and passes `customheader.GetNextTimestamp(parent, now)`.
2. `epochdb_build` with candidates that all get skipped returns 0 with `included_count = 0` (Go then
   refuses to propose). Candidates are an RLP list of tx envelopes (typed txs as byte strings, as in a
   block body), in the miner's order (effective tip desc, nonce asc per sender), up to 1.5x gas limit.
3. `epochdb_head_header` must answer the genesis header before any block is accepted (Initialize decodes
   it for the pool: Number, Root, Time, BaseFee, GasLimit, the subnet-evm extra).
4. `epochdb_get_block(genesis id)` may fail: Go answers the genesis block itself. For every other stored
   id it must return the exact inner bytes; Go re-`parse`s them for the metadata (expect a cache hit).
5. `epochdb_account_state` with a zero block id reads the accepted head; Go calls it once per Accept
   with every cached address (the pool's reset then never crosses), and once per unseen sender on
   admission. A non-zero id of a rolled-past head may fail; Go then retries with the zero id.
6. `epochdb_rpc` receives the raw HTTP body (single or batch) or one batch element; Go never re-encodes.
7. `pchain_height` is 0 when avalanchego calls plain Verify/BuildBlock (no proposervm context).
8. Go frees every `epochdb_buf` it receives exactly once, including `skipped` and the open error.

## What the implementation fixed (rs-ffi, 2026-09-09 JST; the header `rs/ffi/epochdb_engine.h` is generated from `rs/ffi/src/lib.rs`)

Signature changes against the block above, both already assumed by go-validator's notes:

1. **`epochdb_build` takes `uint64_t timestamp_ms` (Unix milliseconds)**, not seconds. `Time = ms / 1000`;
   under Granite `TimeMilliseconds = ms` and `MinDelayExcess` are set. Pre-Granite the ms part is dropped.
2. **`epochdb_open(..., epochdb_buf* err)` returns NULL on failure** with the message in `err` (free it);
   every other function reports through `epochdb_last_error`. There is no `epochdb_engine*` to read an
   error from when open fails, hence the out buffer.

No other change. Return type is C `int`; lengths are `size_t`; structs are `epochdb_buf`, `epochdb_block_meta`
`{id[32], parent[32], height, timestamp}`, `epochdb_verify_out` `{state_root[32], gas_used, tx_count}`,
`epochdb_build_out` `{block_bytes, id[32], gas_used, included_count, skipped, needs_more, phase_ns[10]}` (`phase_ns`:
nanoseconds per build phase, for the Go side's per-build log line; see the header).

- `epochdb_build` `senders`: the Go pool recovered every candidate's sender at admission, so Go hands them over
  (20 bytes each, in candidate order) and the engine recovers only a candidate whose 20 bytes are zero (or every
  candidate when `senders` is NULL). Recovery was 83% of the engine's build time (36 us per candidate, sequential;
  BuildBlock profile in cmd/epochdb-validator/E2E.md).

Semantics pinned down:

- `epochdb_build` skip codes (one byte per candidate in `skipped`): 0 included, 1 nonce too low (Shift:
  skipped, the sender's later candidates still considered), 2 the tx failed to apply (nonce too high,
  funds, intrinsic gas, fee cap under base fee, tx allow list, an unverifiable warp predicate: the sender
  is popped), 3 an earlier tx of the sender was popped, 4 the gas left in the pool is below the tx's gas
  limit (popped), 5 the block's tx bytes would pass the miner's 1800 KiB target (popped), 6 not reached
  (the loop stopped with under 21,000 gas left). Candidates all skipped: rc 0 with `included_count = 0`
  and an empty (still valid, still built) block.
- `epochdb_build` fails (rc -1) when the built block does not pay its block gas cost
  (`customheader.VerifyBlockFee`, the miner's `FinalizeAndAssemble` error), when `timestamp_ms` is below
  the parent's, when the coinbase is zero, or when the parent is neither the accepted head nor a verified
  block (-3). It never checks the wall clock (`VerifyTime`'s "too far in the future" and the ACP-226 minimum
  delay are the caller's, via `customheader.GetNextTimestamp` on the Go side).
- Coinbase rule: `GetCoinbaseAt(parent)`; when fee recipients are not allowed the configured address (the
  RewardManager's stored one, or `BlackholeAddr`) replaces the caller's.
- ACP-226: the built block's `MinDelayExcess` moves from the parent's (or `InitialDelayExcess` when the
  parent is pre-Granite) toward the config key `min-delay-target` (ms, the subnet-evm config key) by at most
  200; without the key it stays the parent's. Genesis under Granite carries `TimeMilliseconds = time * 1000`
  and `InitialDelayExcess` (or `DesiredDelayExcess(initialMinDelayMS)`), as `core.Genesis.toBlock` does.
- Warp predicates: the engine has no validator state, so a candidate carrying a warp predicate is popped
  (code 2) instead of built; verify trusts the header's predicate results as today. Wiring avalanchego's
  validator state through the shell is a later step.
- `pchain_height` is passed as both the proposervm height and the Granite epoch height (one value in the ABI).
- `epochdb_verify` of an id never parsed / built returns -3. Verifying a verified or accepted id is a lookup.
  In bootstrapping (`epochdb_set_state(1)`) `state_root` is the header's (checked by the checker thread one
  block behind); in NormalOp it is the computed root, and a mismatch fails the verify. `epochdb_set_state(2)`
  drains the checker first so the accepted trie state is the head's.
- `epochdb_account_state`'s `block_id` may be zero or the head's id (the accepted head) or a verified block's
  id (its pending state); anything else is -3.
- `epochdb_get_block` answers the genesis as `[header, [], []]`; verified and accepted ids return the bytes
  handed in (or built), and so does a parsed block still in the parsed cache (a competing block at the accepted
  height stays there one block: avalanchego looks it up before Reject, and ENOTFOUND there is fatal to the chain).
- `epochdb_head_header` is the genesis header before the first accept.
- Build and verify hold the engine's execution mutex; `epochdb_account_state`, `epochdb_rpc`,
  `epochdb_head_header`, `epochdb_get_block` and `epochdb_health` are safe from any thread at any time.
- Nothing archive-related runs inside verify or build: the callTracer JSON, the store rows, postings and
  the state-history rows are produced on the checker thread after accept.

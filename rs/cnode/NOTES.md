# cnode notes

Running log of decisions, measured numbers, open questions. Design of record: `~/dotfiles/projects/archival-node/assets/defi_node_handover_task.md`.

## 2026-09-12

- Hot state layout: nested maps keyed by keccak(addr) and keccak(slot), inline keys and values, take-8-bytes xor-fold hasher, `papaya` for lock-free reads. Measured on bot384 (`rs/state/src/bin/bench2.rs`, 10M slots in 10k contracts, one chiplet): nested 56 ns cold / 11 ns hot vs flat inline map 229 / 109, boxed-key map 441 / 239, rs/state run file over mmap 896 / 253.
- Generation = seqlock: `seq` odd while the applier writes, readers check before and after a lookup, mismatch = `Stale`. Applier never waits for readers; readers never block it.
- Bootstrap: temporary stock avalanchego v1.14.2 (`~/cnode-sync/compose.yml` on bot384, ports 9660/9661, data `~/cnode-sync/data`) started 2026-09-12 01:39 JST. The export tool is `cmd/cnode-export` (Go, reads the stopped node's pebble db).
- Keys are hashed because coreth's snapshot has no preimages (state-synced node). The read API takes an address and hashes it; a preimage cache comes when measured.
- Executor: rs/exec's revm executor is reused under a coreth Config (`exec.rs: coreth_config`). Its handler already matches coreth (zero refunds, gas * effective price to the coinbase, warp predicate gas); added on top: the three deprecated native-asset precompiles (revert, all gas back, warm), storage key mask (bit 248), multicoin flag (sticky, blocks EIP-158 deletion), atomic txs decoded from `blockExtraData` (linearcodec, `decode_atomic`) and applied after the txs. Rules: `CORETH_RULES.md` (coreth c4e4e55eff).
- Live oracle (`bin/oracle`, runs on bot384 against the validator RPC): pre-state at h-1, compare every touched account and slot at h, gas used and receipts root against the header. The pruned validator serves state for the last ~24 to 31 blocks only, so rounds of 4 blocks. Matching so far, no atomic tx seen yet.
- History: redb, three tables by height (blocks as the RPC JSON with received/applied ms, diffs as contract rows, mempool as JSON lines). Restart snapshot = the checker's newest rolled run (`import::load_run`), plus code.bin from the export; diffs after it replayed from the history.
- simulate: the txs as the next block (real tx semantics: nonce, balance, intrinsic gas) on a `GenBase` overlay; a generation change during the run marks the base stale and the result is dropped. Whole batch fails on one bad tx (ponytail).
- Mempool: `newPendingTransactions` with bodies from each validator WS; 30 s on bot384: 226 distinct txs, first seen 212 on avago and 14 on avago2; dedup by hash; flushed to the history per block interval.
- Corruption switch for acceptance 3: `CNODE_CORRUPT_AT=<height>`.

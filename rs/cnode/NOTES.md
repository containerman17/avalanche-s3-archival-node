# cnode notes

Running log of decisions, measured numbers, open questions. Design of record: `~/dotfiles/projects/archival-node/assets/defi_node_handover_task.md`.

## 2026-09-12

- Hot state layout: nested maps keyed by keccak(addr) and keccak(slot), inline keys and values, take-8-bytes xor-fold hasher, `papaya` for lock-free reads. Measured on bot384 (`rs/state/src/bin/bench2.rs`, 10M slots in 10k contracts, one chiplet): nested 56 ns cold / 11 ns hot vs flat inline map 229 / 109, boxed-key map 441 / 239, rs/state run file over mmap 896 / 253.
- Generation = seqlock: `seq` odd while the applier writes, readers check before and after a lookup, mismatch = `Stale`. Applier never waits for readers; readers never block it.
- Bootstrap: temporary stock avalanchego v1.14.2 (`~/cnode-sync/compose.yml` on bot384, ports 9660/9661, data `~/cnode-sync/data`) started 2026-09-12 01:39 JST. The export tool is `cmd/cnode-export` (Go, reads the stopped node's pebble db).
- Keys are hashed because coreth's snapshot has no preimages (state-synced node). The read API takes an address and hashes it; a preimage cache comes when measured.

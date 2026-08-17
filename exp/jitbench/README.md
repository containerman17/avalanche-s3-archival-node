# jitbench

EXPERIMENT, not production. Nothing in `exp/` is imported by the node.

It answers one question: how much of libevm's EVM interpreter time is per-opcode
bookkeeping (gas charging, stack bounds checking) rather than real work? That is
the ceiling on evmone-style basic-block gas accounting, and it is the number
that decides whether an AOT/JIT compiler is worth its integration cost.

## Running

	./exp/jitbench/regen.sh                      # build the vmx fork
	go test ./exp/jitbench/                      # exactness
	go test ./exp/jitbench/ -run XXX -bench . -benchtime 400ms -cpu 1 -count 5

`regen.sh` copies libevm's `core/vm` out of the module cache, renames it to
package `vmx`, and drops in `patch/blockgas.go.txt` and
`patch/run_variants.go.txt`. The copy is gitignored: epochdb is MIT and
go-ethereum's `core/vm` is LGPL-3.0.

## What is in the fork

Three run loops, selected by `vmx.Mode`:

- `ModeStock`: libevm's loop, byte for byte.
- `ModeBlockGas`: evmone's "advanced" basic-block accounting. A straight-line
  block's constant gas is charged once at block entry and the block's stack
  requirement is checked once at block entry. Opcodes that READ the remaining
  gas (GAS, the CALL family, the CREATE family, SSTORE) get a correction added
  back so they see the per-opcode value exactly. Consensus-exact, tracer-visible.
- `ModeNoMeter`: no gas, no stack bounds. DELIBERATELY WRONG. It exists only to
  measure the ceiling of removing per-opcode bookkeeping.

`TestBlockGasIsExact` sweeps ~630 gas limits per contract across the whole gas
range of the call, which walks the out-of-gas boundary through every basic
block, and asserts the block loop burns identical gas and returns identical
bytes to the stock loop.

## Findings

Written up in `~/dotfiles/wiki/epochdb/`.

## testdata

Runtime bytecode of the most-called contracts in 60 sampled mainnet C-chain
blocks, pulled from the public archive RPC on 2026-08-17. The local `data-*`
corpora are all pre-v1 storage format and cannot be opened by HEAD.

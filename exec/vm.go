package exec

// THE EXECUTION BACKEND SEAM (DESIGN "SUBNET-EVM SCOPING", M3).
//
// coreth and subnet-evm expose a NEAR-IDENTICAL execution API (ApplyUpgrades,
// NewBlockContext, NewEVMBlockContext, ApplyTransaction, GasPool,
// Genesis.Commit, extstate.NewDatabaseWithNodeDB, dummy.NewFullFaker,
// params.GetExtra / SetEthUpgrades), but as two DISTINCT sets of Go types: a
// coreth ChainContext wants a coreth consensus.Engine, a subnet-evm one wants
// subnet-evm's. So this is not a second execution backend, it is the same
// backend named twice, plus the two places the chains genuinely differ:
//
//   - coreth carries ATOMIC TXS in the block's ExtData; subnet-evm has no
//     ExtData at all (it registers NOOPBlockBodyHooks), so that block is not
//     "disabled" on the subnet-evm side, it does not exist.
//   - coreth has SAE (ACP-194, Helicon): a settlement-lagged root and four
//     header markers. subnet-evm at v1.15.0-fuji has neither the markers in
//     its header extras nor an SAE VM, so transitionTimestamp reports "never"
//     and hasSettledMarkers reports false, which makes the whole SAE branch
//     (saeExecuted, executeSAEBlock, saehooks.go, saering.go) unreachable on
//     that path WITHOUT a single runtime `if kind ==` inside it (M4).
//
// Everything else in exec is chain-agnostic and stays exactly where it is.

import (
	"github.com/ava-labs/avalanchego/snow"
	"github.com/ava-labs/libevm/common"
	ethstate "github.com/ava-labs/libevm/core/state"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/ethdb"
	"github.com/ava-labs/libevm/params"
	"github.com/ava-labs/libevm/triedb"

	"github.com/containerman17/epochdb/chain"
	"github.com/containerman17/epochdb/fetch"
)

// Genesis is the VM-NEUTRAL view of a chain's genesis: what the executor and
// every read-side consumer actually keep from it. Config and TrieAlloc are
// libevm types shared by both VM kinds (the VM-specific part of Config lives in
// its libevm extras payload, which the process-global registration pins), so a
// caller holding this never has to know which VM produced it.
type Genesis struct {
	Config *params.ChainConfig

	// TrieAlloc is what the genesis actually MATERIALISES IN THE TRIE, which is
	// not the same thing as the genesis `alloc` and is deliberately not named
	// after it: on subnet-evm a precompile enabled at genesis has an account and
	// its configured storage in the trie while the alloc lists only EOAs (see
	// trieAlloc in vm_sevm.go). Every read-side floor is this map, so anything
	// missing from it reads back as "account does not exist".
	TrieAlloc types.GenesisAlloc

	Timestamp uint64
	Hash      common.Hash // genesis block hash
	Root      common.Hash // genesis state root

	// commit materialises the alloc into triedb. It is the VM's own
	// Genesis.Commit, closed over the parsed genesis.
	commit func(db ethdb.Database, tdb *triedb.Database) error
}

// Commit materialises the genesis alloc into tdb (the VM's own
// Genesis.Commit). db takes the genesis code: Firewood delegates code storage
// to rawdb entirely, so it must be the same ethdb the caller reads from.
func (g *Genesis) Commit(db ethdb.Database, tdb *triedb.Database) error {
	return g.commit(db, tdb)
}

// vmBackend is the seam: exactly the symbols where the two VMs differ.
// TxHook is called after each transaction is applied, on the executor's own
// goroutine. It is where per-tx state capture drains and where the tx's lookup
// rows are built from execution outputs.
type TxHook func(txIndex int, tx *types.Transaction, receipt *types.Receipt) error

type vmBackend interface {
	// genesis parses the descriptor's genesis bytes and wires the chain's
	// upgrades EXACTLY as that VM's own plugin/evm parseGenesis does.
	// Nothing is committed here.
	genesis(c *chain.Chain, snowCtx *snow.Context) (*Genesis, error)

	// newStateDatabase is the VM's extstate.NewDatabaseWithNodeDB.
	newStateDatabase(db ethdb.Database, tdb *triedb.Database) ethstate.Database

	// runEVM applies the block's upgrades and every transaction onto statedb
	// and returns the receipts. parentTime is the PARENT's timestamp, which is
	// what makes a precompile activate exactly once.
	runEVM(cc chainContext, cfg *params.ChainConfig, snowCtx *snow.Context,
		blk *types.Block, parentTime uint64, statedb *ethstate.StateDB, onTx TxHook) (types.Receipts, error)

	// transitionTimestamp is the transitionvm switch time and whether the SAE
	// transition is scheduled at all.
	transitionTimestamp(cfg *params.ChainConfig) (uint64, bool)

	// hasSettledMarkers reports whether a header carries the ACP-194
	// settlement markers, i.e. whether the SAE VM built it.
	hasSettledMarkers(h *types.Header) bool
}

// vmFor picks the backend for a VM kind. The zero kind is coreth, which is
// what every caller predating the descriptor meant.
func vmFor(kind chain.VMKind) vmBackend {
	if kind == chain.SubnetEVM {
		return sevmVM{}
	}
	return corethVM{}
}

// registeredVM is the backend for the kind THIS PROCESS registered with
// libevm. Package-level read-side helpers (HasSettledMarkers) have no
// descriptor in hand, and the libevm extras registry is process-global and
// one-shot, so the registered kind is the only answer there is.
func registeredVM() vmBackend { return vmFor(fetch.RegisteredKind()) }

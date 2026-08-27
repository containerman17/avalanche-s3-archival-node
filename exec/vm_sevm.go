package exec

// The subnet-evm side of the execution seam (see vm.go). Structurally the
// coreth file minus the two things an L1 does not have: atomic txs and SAE.

import (
	"encoding/json"
	"fmt"
	"maps"
	"math/big"

	sevmcommontype "github.com/ava-labs/avalanchego/graft/subnet-evm/commontype"
	sevmconsensus "github.com/ava-labs/avalanchego/graft/subnet-evm/consensus"
	sevmdummy "github.com/ava-labs/avalanchego/graft/subnet-evm/consensus/dummy"
	sevmcore "github.com/ava-labs/avalanchego/graft/subnet-evm/core"
	sevmextstate "github.com/ava-labs/avalanchego/graft/subnet-evm/core/extstate"
	sevmparams "github.com/ava-labs/avalanchego/graft/subnet-evm/params"
	sevmextras "github.com/ava-labs/avalanchego/graft/subnet-evm/params/extras"
	sevmmodules "github.com/ava-labs/avalanchego/graft/subnet-evm/precompile/modules"

	// GENESIS PRECOMPILES DO NOT PARSE WITHOUT THIS, AND THEY FAIL SILENTLY.
	// extras.Precompiles.UnmarshalJSON walks modules.RegisteredModules() and
	// keeps only the keys it finds there; with an empty registry a genesis
	// carrying "nativeMinterConfig" (which FIFA does, at time 0) unmarshals to
	// ZERO precompiles, no error, and every state root from genesis onward is
	// wrong. The registry is init-registration only, so importing it is the
	// whole fix.
	_ "github.com/ava-labs/avalanchego/graft/subnet-evm/precompile/registry"
	"github.com/ava-labs/avalanchego/snow"
	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/rawdb"
	ethstate "github.com/ava-labs/libevm/core/state"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/core/vm"
	"github.com/ava-labs/libevm/ethdb"
	"github.com/ava-labs/libevm/params"
	"github.com/ava-labs/libevm/triedb"

	"github.com/containerman17/avalanche-s3-archival-node/chain"
)

// sevmVM is any Avalanche L1 running subnet-evm.
type sevmVM struct{}

// sevmChainContext is the executor's headers-log context wearing subnet-evm's
// ChainContext instead of coreth's. The outer Engine shadows the promoted
// coreth one; GetHeader (the part that actually does work, so BLOCKHASH
// resolves real hashes) is the same method underneath.
type sevmChainContext struct{ chainContext }

func (sevmChainContext) Engine() sevmconsensus.Engine { return sevmdummy.NewFullFaker() }

// genesis mirrors subnet-evm/plugin/evm/vm.go parseGenesis. The differences
// from coreth's are the chain's own fee config, its genesis precompiles, and
// upgrade bytes: an L1's upgrade.json unmarshals into UpgradeConfig, which is
// what puts BOTH its precompile upgrades AND its state upgrades in front of
// ApplyUpgrades on every block. They are NOT in the chain root (amended
// 2026-08-05: ApplyUpgrades takes a parent timestamp, so upgrades apply inside
// a block and never to genesis), and they are still mandatory to REPLAY a chain
// that ships one: a missing or wrong file diverges at its activation height and
// the state-root check hard-stops there.
func (sevmVM) genesis(c *chain.Chain, snowCtx *snow.Context) (*Genesis, error) {
	g := new(sevmcore.Genesis)
	if err := json.Unmarshal(c.GenesisJSON, g); err != nil {
		return nil, fmt.Errorf("unmarshal subnet-evm genesis: %w", err)
	}
	if g.Config == nil {
		g.Config = sevmparams.SubnetEVMDefaultChainConfig
	}

	configExtra := sevmparams.GetExtra(g.Config)
	configExtra.AvalancheContext = sevmextras.AvalancheContext{SnowCtx: snowCtx}
	if configExtra.FeeConfig == sevmcommontype.EmptyFeeConfig {
		configExtra.FeeConfig = sevmparams.DefaultFeeConfig
	}
	// The network's upgrade schedule fills every timestamp the genesis left
	// unset. snowCtx.NetworkUpgrades is where the VM reads it from too.
	configExtra.SetDefaults(snowCtx.NetworkUpgrades)

	if len(c.UpgradeJSON) > 0 {
		var upgradeConfig sevmextras.UpgradeConfig
		if err := json.Unmarshal(c.UpgradeJSON, &upgradeConfig); err != nil {
			return nil, fmt.Errorf("parse upgrade bytes: %w", err)
		}
		configExtra.UpgradeConfig = upgradeConfig
	}
	if overrides := configExtra.UpgradeConfig.NetworkUpgradeOverrides; overrides != nil {
		configExtra.Override(overrides)
	}

	if err := g.Verify(); err != nil {
		return nil, fmt.Errorf("invalid subnet-evm genesis: %w", err)
	}
	if err := sevmparams.SetEthUpgrades(g.Config); err != nil {
		return nil, fmt.Errorf("set eth upgrades: %w", err)
	}

	alloc, err := trieAlloc(g)
	if err != nil {
		return nil, err
	}
	blk := g.ToBlock()
	return &Genesis{
		Config:    g.Config,
		TrieAlloc: alloc,
		Timestamp: g.Timestamp,
		Hash:      blk.Hash(),
		Root:      blk.Root(),
		commit: func(db ethdb.Database, tdb *triedb.Database) error {
			_, err := g.Commit(db, tdb)
			return err
		},
	}, nil
}

// trieAlloc returns what this genesis actually MATERIALISES, which on
// subnet-evm is more than g.Alloc: Genesis.toBlock runs
// ApplyPrecompileActivations BEFORE it writes the alloc, so every precompile
// enabled at time 0 gets an account (nonce 1, code 0x01) plus whatever its
// module's Configure stores, and a nativeMinter initialMint credits addresses
// that need not be in the alloc either. None of that is in `alloc`, and the
// historical read overlay uses this map as its below-first-capture floor
// (state/overlay.go), so anything missing here answers an RPC read of a
// genesis-configured precompile with "account does not exist". That is the
// read-path half of the blind spot that merged a wrong frontier root on the
// first Beam join (see BuildFrontier, DESIGN 2026-08-05).
//
// It runs the VM'S OWN activation code against a throwaway in-memory statedb
// holding NOTHING but the activations, so no rule here is a second
// implementation of one in subnet-evm and the cost is the precompile count
// (six addresses at most), not the alloc size. Once, at open.
//
// RESIDUAL GAP, honest: a genesis AirdropData block would also be in the trie
// and is not modelled here. It is a coreth-era C-chain field that subnet-evm
// still parses, no L1 sets it, and one would only cost balances (never an
// account's existence) on a chain that did.
func trieAlloc(g *sevmcore.Genesis) (types.GenesisAlloc, error) {
	db := rawdb.NewMemoryDatabase()
	// Preimages: the dump below is how the raw addresses and slot keys come
	// back out of the hashed trie.
	tdb := triedb.NewDatabase(db, &triedb.Config{Preimages: true})
	sdb, err := ethstate.New(types.EmptyRootHash, sevmextstate.NewDatabaseWithNodeDB(db, tdb), nil)
	if err != nil {
		return nil, err
	}
	blockCtx := sevmcore.NewBlockContext(new(big.Int).SetUint64(g.Number), g.Timestamp)
	if err := sevmcore.ApplyPrecompileActivations(g.Config, nil, blockCtx, sdb); err != nil {
		return nil, fmt.Errorf("configure genesis precompiles: %w", err)
	}
	root, err := sdb.Commit(0, false) // deleteEmptyObjects=false, as toBlock commits
	if err != nil {
		return nil, err
	}
	if root == types.EmptyRootHash {
		return g.Alloc, nil // no precompile at time 0: the trie IS the alloc
	}
	if sdb, err = ethstate.New(root, sevmextstate.NewDatabaseWithNodeDB(db, tdb), nil); err != nil {
		return nil, err
	}
	out := make(types.GenesisAlloc, len(g.Alloc)+len(sevmmodules.RegisteredModules()))
	for _, da := range sdb.RawDump(&ethstate.DumpConfig{}).Accounts {
		if da.Address == nil {
			return nil, fmt.Errorf("genesis precompile account %x has no preimage", da.AddressHash)
		}
		bal, ok := new(big.Int).SetString(da.Balance, 10)
		if !ok {
			return nil, fmt.Errorf("genesis precompile %s: undecodable balance %q", da.Address, da.Balance)
		}
		acct := types.Account{Nonce: da.Nonce, Balance: bal, Code: da.Code}
		if len(da.Storage) > 0 {
			acct.Storage = make(map[common.Hash]common.Hash, len(da.Storage))
			for slot, v := range da.Storage {
				acct.Storage[slot] = common.HexToHash(v)
			}
		}
		out[*da.Address] = acct
	}
	for addr, a := range g.Alloc {
		// The alloc is applied AFTER the activations, field by field: SetBalance,
		// SetCode and SetNonce OVERWRITE, SetState per slot MERGES. An alloc entry
		// at a module address is nonsense, but this is what the trie would hold.
		if pre, ok := out[addr]; ok && len(pre.Storage) > 0 {
			st := maps.Clone(pre.Storage)
			maps.Copy(st, a.Storage)
			a.Storage = st
		}
		out[addr] = a
	}
	return out, nil
}

func (sevmVM) newStateDatabase(db ethdb.Database, tdb *triedb.Database) ethstate.Database {
	return sevmextstate.NewDatabaseWithNodeDB(db, tdb)
}

// runEVM mirrors subnet-evm core.StateProcessor.Process, minus engine.Finalize
// (which takes no StateDB in subnet-evm: it only re-verifies BlockGasCost and
// the block fee, so it cannot move a state root) and minus coreth's atomic-tx
// block, which has no subnet-evm counterpart at all.
func (sevmVM) runEVM(cc chainContext, chainCfg *params.ChainConfig, _ *snow.Context,
	blk *types.Block, parentTime uint64, statedb *ethstate.StateDB, onTx TxHook,
) (types.Receipts, error) {
	header := blk.Header()
	sc := sevmChainContext{cc}

	// Parent timestamp, same one-shot activation rule as coreth: ApplyUpgrades
	// here covers the chain's precompile upgrades AND its state upgrades.
	upgradeBlockCtx := sevmcore.NewBlockContext(header.Number, header.Time)
	if err := sevmcore.ApplyUpgrades(chainCfg, &parentTime, upgradeBlockCtx, statedb); err != nil {
		return nil, fmt.Errorf("apply upgrades: %w", err)
	}

	// Warp predicate results ride in header.Extra, which NewEVMBlockContext
	// parses; nothing here has to verify them (DESIGN: a historical replay
	// needs no validator set and no P-chain height).
	blockCtx := sevmcore.NewEVMBlockContext(header, sc, nil)
	gp := new(sevmcore.GasPool).AddGas(header.GasLimit)
	var (
		usedGas  uint64
		receipts types.Receipts
	)
	for txIndex, tx := range blk.Transactions() {
		statedb.SetTxContext(tx.Hash(), txIndex)
		receipt, err := sevmcore.ApplyTransaction(
			chainCfg, sc, blockCtx, gp, statedb,
			header, tx, &usedGas, vm.Config{Tracer: frameTracer()},
		)
		if err != nil {
			return nil, fmt.Errorf("tx %d: %w", txIndex, err)
		}
		receipts = append(receipts, receipt)
		if onTx != nil {
			if err := onTx(txIndex, tx, receipt); err != nil {
				return nil, fmt.Errorf("tx %d capture: %w", txIndex, err)
			}
		}
	}
	return receipts, nil
}

// transitionTimestamp: SAE (ACP-194) is a C-chain thing. subnet-evm at
// v1.15.0-fuji has no settlement markers in its header extras and there is no
// SAE VM for an L1, so the transition is never scheduled and every SAE branch
// in exec is dead code on this path.
func (sevmVM) transitionTimestamp(*params.ChainConfig) (uint64, bool) { return 0, false }

func (sevmVM) hasSettledMarkers(*types.Header) bool { return false }

// warmEVM: subnet-evm chains are small; the warm stage is a coreth (mainnet C) lever.
func (sevmVM) warmEVM(chainContext, *params.ChainConfig, *types.Block, *ethstate.StateDB) {}

package exec

// The subnet-evm side of the execution seam (see vm.go). Structurally the
// coreth file minus the two things an L1 does not have: atomic txs and SAE.

import (
	"encoding/json"
	"fmt"

	sevmcommontype "github.com/ava-labs/avalanchego/graft/subnet-evm/commontype"
	sevmconsensus "github.com/ava-labs/avalanchego/graft/subnet-evm/consensus"
	sevmdummy "github.com/ava-labs/avalanchego/graft/subnet-evm/consensus/dummy"
	sevmcore "github.com/ava-labs/avalanchego/graft/subnet-evm/core"
	sevmextstate "github.com/ava-labs/avalanchego/graft/subnet-evm/core/extstate"
	sevmparams "github.com/ava-labs/avalanchego/graft/subnet-evm/params"
	sevmextras "github.com/ava-labs/avalanchego/graft/subnet-evm/params/extras"
	"github.com/ava-labs/avalanchego/snow"
	ethstate "github.com/ava-labs/libevm/core/state"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/core/vm"
	"github.com/ava-labs/libevm/ethdb"
	"github.com/ava-labs/libevm/params"
	"github.com/ava-labs/libevm/triedb"

	"github.com/containerman17/epochdb/chain"
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
// ApplyUpgrades on every block. Those bytes are inside the chain root (DESIGN
// ruling 6), so a chain that ships one cannot be replayed without it.
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

	blk := g.ToBlock()
	return &Genesis{
		Config:    g.Config,
		Alloc:     g.Alloc,
		Timestamp: g.Timestamp,
		Hash:      blk.Hash(),
		Root:      blk.Root(),
		commit: func(db ethdb.Database, tdb *triedb.Database) error {
			_, err := g.Commit(db, tdb)
			return err
		},
	}, nil
}

func (sevmVM) newStateDatabase(db ethdb.Database, tdb *triedb.Database) ethstate.Database {
	return sevmextstate.NewDatabaseWithNodeDB(db, tdb)
}

// runEVM mirrors subnet-evm core.StateProcessor.Process, minus engine.Finalize
// (which takes no StateDB in subnet-evm: it only re-verifies BlockGasCost and
// the block fee, so it cannot move a state root) and minus coreth's atomic-tx
// block, which has no subnet-evm counterpart at all.
func (sevmVM) runEVM(cc chainContext, chainCfg *params.ChainConfig, _ *snow.Context,
	blk *types.Block, parentTime uint64, statedb *ethstate.StateDB,
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
			header, tx, &usedGas, vm.Config{},
		)
		if err != nil {
			return nil, fmt.Errorf("tx %d: %w", txIndex, err)
		}
		receipts = append(receipts, receipt)
	}
	return receipts, nil
}

// transitionTimestamp: SAE (ACP-194) is a C-chain thing. subnet-evm at
// v1.15.0-fuji has no settlement markers in its header extras and there is no
// SAE VM for an L1, so the transition is never scheduled and every SAE branch
// in exec is dead code on this path.
func (sevmVM) transitionTimestamp(*params.ChainConfig) (uint64, bool) { return 0, false }

func (sevmVM) hasSettledMarkers(*types.Header) bool { return false }

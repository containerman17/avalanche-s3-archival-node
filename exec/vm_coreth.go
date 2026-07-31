package exec

// The coreth side of the execution seam (see vm.go). Every function here was
// MOVED, not rewritten: loadCorethGenesis came from genesis.go, runEVM and
// chainContext.Engine from executor.go, transitionTimestamp and
// hasSettledMarkers from sae.go. The C-chain path is byte-for-byte the code it
// was before the seam existed.

import (
	"encoding/json"
	"fmt"

	"github.com/ava-labs/avalanchego/graft/coreth/consensus"
	"github.com/ava-labs/avalanchego/graft/coreth/consensus/dummy"
	corethcore "github.com/ava-labs/avalanchego/graft/coreth/core"
	"github.com/ava-labs/avalanchego/graft/coreth/core/extstate"
	cparams "github.com/ava-labs/avalanchego/graft/coreth/params"
	"github.com/ava-labs/avalanchego/graft/coreth/params/extras"
	"github.com/ava-labs/avalanchego/graft/coreth/plugin/evm/atomic"
	ccustomtypes "github.com/ava-labs/avalanchego/graft/coreth/plugin/evm/customtypes"
	warpcontract "github.com/ava-labs/avalanchego/graft/coreth/precompile/contracts/warp"
	"github.com/ava-labs/avalanchego/snow"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/core/vm"
	"github.com/ava-labs/libevm/ethdb"
	"github.com/ava-labs/libevm/params"
	"github.com/ava-labs/libevm/triedb"

	ethstate "github.com/ava-labs/libevm/core/state"

	"github.com/containerman17/epochdb/chain"
)

// corethVM is the primary network's C-chain.
type corethVM struct{}

// Engine is the coreth ChainContext half of the executor's headers-log
// context. It lives here rather than beside chainContext itself because the
// engine type is exactly one of the things the two VMs do not share.
func (c chainContext) Engine() consensus.Engine { return dummy.NewFullFaker() }

func (corethVM) genesis(c *chain.Chain, snowCtx *snow.Context) (*Genesis, error) {
	g, err := loadCorethGenesis(c, snowCtx)
	if err != nil {
		return nil, err
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

// loadCorethGenesis parses the C-Chain genesis out of the descriptor and
// wires the Avalanche network upgrades BEFORE SetEthUpgrades: without
// configExtra.NetworkUpgrades the Avalanche phases never activate, state
// roots diverge at the first AP1 block, and SetEthUpgrades cannot place
// Berlin or London (Fuji 184985/805078, mainnet 1640340/3308552). Mirrors
// coreth/plugin/evm/vm.go parseGenesis.
func loadCorethGenesis(c *chain.Chain, snowCtx *snow.Context) (*corethcore.Genesis, error) {
	var g corethcore.Genesis
	if err := json.Unmarshal(c.GenesisJSON, &g); err != nil {
		return nil, fmt.Errorf("unmarshal C-Chain genesis: %w", err)
	}

	configExtra := cparams.GetExtra(g.Config)
	configExtra.AvalancheContext = extras.AvalancheContext{SnowCtx: snowCtx}
	configExtra.NetworkUpgrades = extras.GetNetworkUpgrades(snowCtx.NetworkUpgrades)

	// If Durango is scheduled, schedule the Warp precompile at the same
	// time (its activation writes to state, so roots would diverge at the
	// Durango boundary without this). Mirrors vm.go.
	if configExtra.DurangoBlockTimestamp != nil {
		configExtra.PrecompileUpgrades = append(configExtra.PrecompileUpgrades, extras.PrecompileUpgrade{
			Config: warpcontract.NewDefaultConfig(configExtra.DurangoBlockTimestamp),
		})
	}
	if err := configExtra.Verify(); err != nil {
		return nil, fmt.Errorf("invalid chain config: %w", err)
	}

	if err := cparams.SetEthUpgrades(g.Config); err != nil {
		return nil, fmt.Errorf("set eth upgrades: %w", err)
	}
	return &g, nil
}

func (corethVM) newStateDatabase(db ethdb.Database, tdb *triedb.Database) ethstate.Database {
	return extstate.NewDatabaseWithNodeDB(db, tdb)
}

// runEVM applies upgrades, all transactions, and the atomic ExtData
// transfers of blk onto statedb, returning every receipt produced (they
// already exist in memory: capture is free). Shared by the per-block and
// batched paths.
func (corethVM) runEVM(cc chainContext, chainCfg *params.ChainConfig, snowCtx *snow.Context,
	blk *types.Block, parentTime uint64, statedb *ethstate.StateDB,
) (types.Receipts, error) {
	header := blk.Header()

	// The PARENT timestamp is what makes a precompile activate exactly once:
	// IsForkTransition treats a nil parent as "never forked", so nil
	// re-activates every already-active precompile on EVERY block
	// (SetNonce 1 + SetCode {0x01} + Configure). Idempotent for Warp, but it
	// writes two junk rows per post-Durango block into the write frame,
	// which breaks epoch byte-determinism against a correct builder, and it
	// is a live correctness bug for any precompile whose Configure is not
	// idempotent. Mirrors coreth core/state_processor.go Process.
	//
	// parentTime IS the parent's timestamp here: executeRaw advances
	// e.headTime only after the block returns, and bisect rewinds it with
	// batchStartTime.
	upgradeBlockCtx := corethcore.NewBlockContext(header.Number, header.Time)
	if err := corethcore.ApplyUpgrades(chainCfg, &parentTime, upgradeBlockCtx, statedb); err != nil {
		return nil, fmt.Errorf("apply upgrades: %w", err)
	}

	blockCtx := corethcore.NewEVMBlockContext(header, cc, nil)
	gp := new(corethcore.GasPool).AddGas(header.GasLimit)
	var (
		usedGas  uint64
		receipts types.Receipts
	)

	for txIndex, tx := range blk.Transactions() {
		statedb.SetTxContext(tx.Hash(), txIndex)
		receipt, err := corethcore.ApplyTransaction(
			chainCfg, cc, blockCtx, gp, statedb,
			header, tx, &usedGas, vm.Config{},
		)
		if err != nil {
			return nil, fmt.Errorf("tx %d: %w", txIndex, err)
		}
		receipts = append(receipts, receipt)
	}

	// Atomic txs ride in the block's ExtData; Fuji has real imports and
	// exports, so this path is mandatory for root correctness.
	if extData := ccustomtypes.BlockExtData(blk); len(extData) > 0 {
		rules := chainCfg.Rules(header.Number, cparams.IsMergeTODO, header.Time)
		isAP5 := false
		if rulesExtra := cparams.GetRulesExtra(rules); rulesExtra != nil {
			isAP5 = rulesExtra.AvalancheRules.IsApricotPhase5
		}
		atomicTxs, err := atomic.ExtractAtomicTxs(extData, isAP5, atomic.Codec)
		if err != nil {
			return nil, fmt.Errorf("extract atomic txs: %w", err)
		}
		wrapped := extstate.New(statedb)
		for i, atx := range atomicTxs {
			if err := atx.UnsignedAtomicTx.EVMStateTransfer(snowCtx, wrapped); err != nil {
				return nil, fmt.Errorf("atomic tx %d: %w", i, err)
			}
		}
	}
	return receipts, nil
}

// heliconTransitionLead is transitionvm's switch offset: the C-Chain
// registers TransitionTime = HeliconTime - 10s.
const heliconTransitionLead = 10

// transitionTimestamp returns the transitionvm switch time for cfg and
// whether Helicon is scheduled at all (mainnet is not, in this dep set).
func transitionTimestamp(cfg *params.ChainConfig) (uint64, bool) {
	ts := cparams.GetExtra(cfg).HeliconTimestamp
	if ts == nil {
		return 0, false
	}
	if *ts < heliconTransitionLead {
		return 0, true
	}
	return *ts - heliconTransitionLead, true
}

func (corethVM) transitionTimestamp(cfg *params.ChainConfig) (uint64, bool) {
	return transitionTimestamp(cfg)
}

// hasSettledMarkers reports whether a header carries the four ACP-194
// settlement markers, which is exactly the set of blocks the SAE VM built:
// coreth's own customheader.VerifySettled REJECTS a coreth block carrying
// any of them, so their presence is a second, independent read on which
// side of the boundary a block sits.
func (corethVM) hasSettledMarkers(h *types.Header) bool {
	he := ccustomtypes.GetHeaderExtra(h)
	return he.SettledHeight != nil && he.SettledGasUnix != nil &&
		he.SettledGasNumerator != nil && he.SettledExcess != nil
}

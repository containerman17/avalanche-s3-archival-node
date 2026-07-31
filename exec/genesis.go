package exec

import (
	"encoding/json"
	"fmt"

	corethcore "github.com/ava-labs/avalanchego/graft/coreth/core"
	cparams "github.com/ava-labs/avalanchego/graft/coreth/params"
	"github.com/ava-labs/avalanchego/graft/coreth/params/extras"
	warpcontract "github.com/ava-labs/avalanchego/graft/coreth/precompile/contracts/warp"
	"github.com/ava-labs/avalanchego/snow"
	"github.com/ava-labs/avalanchego/upgrade"
	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/rawdb"
	"github.com/ava-labs/libevm/ethdb"
	"github.com/ava-labs/libevm/triedb"

	"github.com/containerman17/epochdb/chain"
)

// snowContextFor builds the snow.Context the EVM sees. EVERY field is an
// EXECUTION INPUT, not bookkeeping, so none of them may be left zero:
//
//   - ChainID: the chain's own blockchain ID, which the Warp precompile's
//     getBlockchainID() returns straight into EVM output. Leaving it zero
//     diverged the state root at Fuji 29,955,803, the first block after
//     Durango whose transaction called Warp: a TeleporterRegistry deploy
//     baked the returned value into an immutable, so the deployed code (and
//     the account's code hash) differed from the chain's.
//   - SubnetID: the primary network for the C-chain, the L1's own subnet
//     otherwise. Warp signature verification reads it.
//   - AVAXAssetID: the atomic-tx state transfer credits imported AVAX by it.
//     C-CHAIN ONLY: atomic txs do not exist on subnet-evm, and an L1 on a
//     network with no embedded config has no way to learn it.
//   - CChainID: the primary network's C-chain, filled for any chain on a
//     network whose config is embedded.
func snowContextFor(c *chain.Chain) (*snow.Context, error) {
	ctx := &snow.Context{
		NetworkID: c.NetworkID,
		SubnetID:  c.SubnetID,
		ChainID:   c.BlockchainID,
	}
	cChainID, avaxAssetID, err := chain.PrimaryC(c.NetworkID)
	if err != nil {
		if c.VMKind == chain.Coreth {
			return nil, err
		}
		// A subnet-evm chain on a network avalanchego does not embed (a local
		// network): neither field is reachable and neither is an input for it.
		return ctx, nil
	}
	ctx.CChainID = cChainID
	if c.VMKind == chain.Coreth {
		ctx.AVAXAssetID = avaxAssetID
	}
	return ctx, nil
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
	configExtra.NetworkUpgrades = extras.GetNetworkUpgrades(upgrade.GetConfig(c.NetworkID))

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

// commitGenesisIfNeeded materialises the genesis state inside the
// Firewood-backed triedb. No-op if the genesis root is already present.
//
// ethdbKV must be the same persistent ethdb the executor uses at runtime:
// genesis Alloc contains pre-deployed contract code whose hash/body get
// routed there via rawdb.WriteCode, and Firewood delegates code storage to
// rawdb entirely.
func commitGenesisIfNeeded(tdb *triedb.Database, g *corethcore.Genesis, ethdbKV ethdb.KeyValueStore) (common.Hash, error) {
	expectedRoot := g.ToBlock().Root()
	if tdb.Initialized(expectedRoot) {
		return expectedRoot, nil
	}
	memdb := rawdb.NewDatabase(ethdbKV)
	if _, err := g.Commit(memdb, tdb); err != nil {
		return common.Hash{}, fmt.Errorf("commit genesis: %w", err)
	}
	if !tdb.Initialized(expectedRoot) {
		return common.Hash{}, fmt.Errorf("genesis committed but root %x not initialized", expectedRoot)
	}
	return expectedRoot, nil
}

package exec

import (
	"encoding/json"
	"fmt"

	"github.com/ava-labs/avalanchego/genesis"
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
)

// loadCChainGenesis parses the C-Chain genesis for networkID from
// avalanchego's embedded config and wires the Avalanche network upgrades
// BEFORE SetEthUpgrades — without configExtra.NetworkUpgrades the
// Avalanche phases never activate, state roots diverge at the first AP1
// block, and SetEthUpgrades cannot place Berlin or London (Fuji
// 184985/805078, mainnet 1640340/3308552). Mirrors
// coreth/plugin/evm/vm.go parseGenesis.
func loadCChainGenesis(networkID uint32, snowCtx *snow.Context) (*corethcore.Genesis, error) {
	cfg := genesis.GetConfig(networkID)
	if cfg == nil {
		return nil, fmt.Errorf("no embedded genesis config for network %d", networkID)
	}
	var g corethcore.Genesis
	if err := json.Unmarshal([]byte(cfg.CChainGenesis), &g); err != nil {
		return nil, fmt.Errorf("unmarshal C-Chain genesis: %w", err)
	}

	configExtra := cparams.GetExtra(g.Config)
	configExtra.AvalancheContext = extras.AvalancheContext{SnowCtx: snowCtx}
	configExtra.NetworkUpgrades = extras.GetNetworkUpgrades(upgrade.GetConfig(networkID))

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

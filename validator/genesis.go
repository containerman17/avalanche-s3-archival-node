package validator

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/ava-labs/avalanchego/graft/subnet-evm/commontype"
	sevmparams "github.com/ava-labs/avalanchego/graft/subnet-evm/params"
	"github.com/ava-labs/avalanchego/graft/subnet-evm/params/extras"
	"github.com/ava-labs/avalanchego/graft/subnet-evm/plugin/evm/customtypes"
	"github.com/ava-labs/avalanchego/upgrade"
	"github.com/ava-labs/libevm/params"
)

// registerExtras installs subnet-evm's header and chain-config payloads in
// libevm, once per process. Not subnet-evm/core's hooks: those are for
// execution, and subnet-evm/core links firewood's Rust runtime, which
// cannot share a binary with the engine's.
var registerExtras = sync.OnceFunc(func() {
	customtypes.Register()
	sevmparams.RegisterExtras()
})

// parseChainConfig mirrors subnet-evm's parseGenesis for the config alone:
// the pool, the fee rules and the coinbase rule need it; the engine owns the
// alloc and the genesis block.
func parseChainConfig(genesisJSON, upgradeJSON []byte, upgrades upgrade.Config) (*params.ChainConfig, error) {
	registerExtras()
	var g struct {
		Config *params.ChainConfig `json:"config"`
	}
	if err := json.Unmarshal(genesisJSON, &g); err != nil {
		return nil, fmt.Errorf("validator: genesis: %w", err)
	}
	if g.Config == nil {
		g.Config = sevmparams.SubnetEVMDefaultChainConfig
	}
	extra := sevmparams.GetExtra(g.Config)
	if extra.FeeConfig == commontype.EmptyFeeConfig {
		extra.FeeConfig = sevmparams.DefaultFeeConfig
	}
	extra.SetDefaults(upgrades)
	if len(upgradeJSON) > 0 {
		var uc extras.UpgradeConfig
		if err := json.Unmarshal(upgradeJSON, &uc); err != nil {
			return nil, fmt.Errorf("validator: upgrade bytes: %w", err)
		}
		extra.UpgradeConfig = uc
	}
	if o := extra.UpgradeConfig.NetworkUpgradeOverrides; o != nil {
		extra.Override(o)
	}
	if err := sevmparams.SetEthUpgrades(g.Config); err != nil {
		return nil, fmt.Errorf("validator: eth upgrades: %w", err)
	}
	return g.Config, nil
}

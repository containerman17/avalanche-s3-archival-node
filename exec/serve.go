package exec

import (
	corethcore "github.com/ava-labs/avalanchego/graft/coreth/core"
	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/snow"
	avaconstants "github.com/ava-labs/avalanchego/utils/constants"

	"github.com/containerman17/epochdb/fetch"
	"github.com/containerman17/epochdb/state"
)

// FujiGenesis returns the fully wired Fuji C-chain genesis (chain config
// with Avalanche upgrades + alloc) for read-side consumers: the historical
// overlay needs the alloc as its below-first-capture floor, the RPC server
// needs the chain config for eth_call.
func FujiGenesis() (*corethcore.Genesis, error) {
	fetch.RegisterExtras()
	avaxAssetID, err := ids.FromString(FujiAVAXAssetID)
	if err != nil {
		return nil, err
	}
	snowCtx := &snow.Context{
		NetworkID:   avaconstants.FujiID,
		AVAXAssetID: avaxAssetID,
	}
	return loadFujiCChainGenesis(snowCtx)
}

// ParseEthBlock decodes a raw staging container (ProposerVM-wrapped or
// pre-fork) into an eth block, for the read-side tx APIs.
var ParseEthBlock = parseEthBlock

// NewChainContext exposes the executor's headers-log-backed ChainContext so
// BLOCKHASH inside historical eth_call resolves real hashes. Not
// goroutine-safe (bucketLog LRU state): callers serialize.
func NewChainContext(store *state.Store) corethcore.ChainContext {
	return chainContext{store: store}
}

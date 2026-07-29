package exec

import (
	corethcore "github.com/ava-labs/avalanchego/graft/coreth/core"
	avaconstants "github.com/ava-labs/avalanchego/utils/constants"

	"github.com/containerman17/epochdb/fetch"
	"github.com/containerman17/epochdb/state"
)

// NetworkGenesis returns the fully wired C-chain genesis (chain config
// with Avalanche upgrades + alloc) for networkID, for read-side
// consumers: the historical overlay needs the alloc as its
// below-first-capture floor, the RPC server needs the chain config for
// eth_call. networkID 0 defaults to Fuji.
func NetworkGenesis(networkID uint32) (*corethcore.Genesis, error) {
	fetch.RegisterExtras()
	if networkID == 0 {
		networkID = avaconstants.FujiID
	}
	snowCtx, err := snowContextFor(networkID)
	if err != nil {
		return nil, err
	}
	return loadCChainGenesis(networkID, snowCtx)
}

// ParseEthBlock decodes a raw staging container (ProposerVM-wrapped or
// pre-fork) into an eth block, for the read-side tx APIs.
var ParseEthBlock = parseEthBlock

// EncodeLogsFrame exposes the live capture's event-log record encoder so
// the log backfill produces byte-identical records.
var EncodeLogsFrame = encodeLogsFrame

// HasSettledMarkers reports whether a header carries the ACP-194
// settlement markers, i.e. whether it was built by the SAE VM. Read-side
// consumers need it because every per-block header invariant changes above
// the boundary (header.Root belongs to the settled block, receiptsRoot
// covers the whole settled range).
var HasSettledMarkers = hasSettledMarkers

// NewChainContext exposes the executor's headers-log-backed ChainContext so
// BLOCKHASH inside historical eth_call resolves real hashes. Not
// goroutine-safe (bucketLog LRU state): callers serialize.
func NewChainContext(store *state.Store) corethcore.ChainContext {
	return chainContext{store: store}
}

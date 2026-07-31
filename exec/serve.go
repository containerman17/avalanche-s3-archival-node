package exec

import (
	"fmt"

	corethcore "github.com/ava-labs/avalanchego/graft/coreth/core"
	avaconstants "github.com/ava-labs/avalanchego/utils/constants"

	"github.com/containerman17/epochdb/chain"
	"github.com/containerman17/epochdb/fetch"
	"github.com/containerman17/epochdb/state"
)

// ChainGenesis returns the fully wired genesis (chain config with Avalanche
// upgrades + alloc) for a chain descriptor, for read-side consumers: the
// historical overlay needs the alloc as its below-first-capture floor, the RPC
// server needs the chain config for eth_call. nil means the Fuji C-chain.
//
// It also performs this process's one-and-only extras registration, for the
// descriptor's VM kind.
func ChainGenesis(c *chain.Chain) (*corethcore.Genesis, error) {
	if c == nil {
		var err error
		if c, err = chain.CChain(avaconstants.FujiID); err != nil {
			return nil, err
		}
	}
	fetch.RegisterExtras(c.VMKind)
	if c.VMKind != chain.Coreth {
		return nil, fmt.Errorf("exec: %s genesis is not wired yet (subnet-evm execution is M3)", c.VMKind)
	}
	snowCtx, err := snowContextFor(c)
	if err != nil {
		return nil, err
	}
	return loadCorethGenesis(c, snowCtx)
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

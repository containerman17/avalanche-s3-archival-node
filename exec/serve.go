package exec

import (
	corethcore "github.com/ava-labs/avalanchego/graft/coreth/core"
	avaconstants "github.com/ava-labs/avalanchego/utils/constants"
	ethstate "github.com/ava-labs/libevm/core/state"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/ethdb"
	"github.com/ava-labs/libevm/triedb"

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
func ChainGenesis(c *chain.Chain) (*Genesis, error) {
	if c == nil {
		var err error
		if c, err = chain.CChain(avaconstants.FujiID); err != nil {
			return nil, err
		}
	}
	fetch.RegisterExtras(c.VMKind)
	snowCtx, err := snowContextFor(c)
	if err != nil {
		return nil, err
	}
	return vmFor(c.VMKind).genesis(c, snowCtx)
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
//
// These callers hold no descriptor, so the answer comes from the kind THIS
// PROCESS registered with libevm, which is the only kind whose header extras
// can be read at all (see registeredVM). On subnet-evm it is always false.
var HasSettledMarkers = func(h *types.Header) bool { return registeredVM().hasSettledMarkers(h) }

// NewStateDatabase is the registered VM's extstate database over a triedb, for
// the no-execution verifier's throwaway Firewood. Like HasSettledMarkers it
// holds no descriptor, so the kind is the one THIS PROCESS registered.
//
// HONEST NOTE: at v1.15.0-fuji the two VMs' NewDatabaseWithNodeDB are the SAME
// three lines in two packages (libevm's state.NewDatabaseWithNodeDB wrapped by
// graft/evm/firewood.NewStateAccessor), so today this routes to identical code
// and no test can tell them apart. It goes through the seam anyway because
// naming coreth's package from a subnet-evm path is precisely the class of bug
// M8 found in rpc, and it is where a divergence would land.
func NewStateDatabase(db ethdb.Database, tdb *triedb.Database) ethstate.Database {
	return registeredVM().newStateDatabase(db, tdb)
}

// NewChainContext exposes the executor's headers-log-backed ChainContext so
// BLOCKHASH inside historical eth_call resolves real hashes. Not
// goroutine-safe (bucketLog LRU state): callers serialize.
func NewChainContext(store *state.Store) corethcore.ChainContext {
	return chainContext{store: store}
}

package fetch

import (
	"fmt"
	"sync"

	corethcore "github.com/ava-labs/avalanchego/graft/coreth/core"
	"github.com/ava-labs/avalanchego/graft/coreth/core/extstate"
	cparams "github.com/ava-labs/avalanchego/graft/coreth/params"
	ccustomtypes "github.com/ava-labs/avalanchego/graft/coreth/plugin/evm/customtypes"
	"github.com/ava-labs/avalanchego/ids"
	proposerblock "github.com/ava-labs/avalanchego/vms/proposervm/block"
	ethtypes "github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/rlp"

	"github.com/containerman17/epochdb/chain"
	"github.com/containerman17/epochdb/fetch/sevm"
)

// C-Chain blocks and state use four libevm extras:
//   - corethcore registers the EVM hooks (pre-AP1 StateDBAP0 wrap, Shanghai
//     random handling).
//   - ccustomtypes installs the Avalanche-specific header, body, and account
//     extras (ExtDataHash, BlockGasCost, isMultiCoin).
//   - cparams installs the Avalanche rules extras on *params.ChainConfig.
//   - extstate installs the state-key normalization hook so storage reads
//     and writes match coreth's multi-coin slot layout. This one matters for
//     state root verification even if we don't use coreth's VM directly.
//
// A subnet-evm chain uses its own set instead (see fetch/sevm). The two are
// MUTUALLY EXCLUSIVE: libevm's extras registry is process-global and panics on
// re-registration, and the header encodings disagree anyway (coreth has a
// mandatory ExtDataHash field where subnet-evm goes straight to the optional
// BaseFee). So a process registers exactly one kind, at start, and every later
// call must name the same kind.
var (
	regMu   sync.Mutex
	regKind chain.VMKind
)

// RegisterExtras installs the libevm extras for one VM kind. Safe to call from
// packages that don't open a Fetcher (e.g. debug/bench commands); idempotent
// for the kind already registered, and a hard panic for the other one, since
// mixing them would silently misdecode every header in the process.
func RegisterExtras(kind chain.VMKind) {
	regMu.Lock()
	defer regMu.Unlock()
	if regKind != "" {
		if regKind != kind {
			panic(fmt.Sprintf("fetch: libevm extras already registered as %q, cannot also register %q in this process", regKind, kind))
		}
		return
	}
	switch kind {
	case chain.Coreth:
		corethcore.RegisterExtras()
		ccustomtypes.Register()
		cparams.RegisterExtras()
		extstate.RegisterExtras()
	case chain.SubnetEVM:
		sevm.Register()
	default:
		panic(fmt.Sprintf("fetch: unknown VM kind %q", kind))
	}
	regKind = kind
}

// RegisteredKind reports the kind registered in this process, "" if none.
func RegisteredKind() chain.VMKind {
	regMu.Lock()
	defer regMu.Unlock()
	return regKind
}

// parsedContainer holds everything we need to store and continue walking,
// plus what the consensus follower needs (parent hash, timestamp).
type parsedContainer struct {
	containerID ids.ID
	blockNumber uint64
	blockHash   ids.ID
	parentID    ids.ID
	parentHash  ids.ID // inner eth block's parent hash
	blockTime   uint64 // inner eth block timestamp (unix seconds)
}

// parseContainer decodes a raw container (ProposerVM-wrapped or pre-fork eth)
// and returns its container ID, block number, eth block hash, and parent
// container ID.
func parseContainer(raw []byte) (parsedContainer, error) {
	if len(raw) == 0 {
		return parsedContainer{}, fmt.Errorf("empty container")
	}

	proposerBlk, perr := proposerblock.ParseWithoutVerification(raw)
	if perr == nil {
		inner := new(ethtypes.Block)
		if err := rlp.DecodeBytes(proposerBlk.Block(), inner); err != nil {
			return parsedContainer{}, fmt.Errorf("decode inner eth block: %w", err)
		}
		return parsedContainer{
			containerID: proposerBlk.ID(),
			blockNumber: inner.NumberU64(),
			blockHash:   ids.ID(inner.Hash()),
			parentID:    proposerBlk.ParentID(),
			parentHash:  ids.ID(inner.ParentHash()),
			blockTime:   inner.Time(),
		}, nil
	}

	// Pre-ProposerVM: the raw bytes are the RLP-encoded eth block and the
	// container ID equals the eth block hash. Both failures are reported
	// together from here on: a corrupt ProposerVM block reaches this path too,
	// and "decode pre-fork eth block" alone names the wrong decoder.
	_, _, rest, err := rlp.Split(raw)
	if err != nil {
		return parsedContainer{}, fmt.Errorf("rlp split: %w (not a ProposerVM block either: %v)", err, perr)
	}
	blockBytes := raw[:len(raw)-len(rest)]
	inner := new(ethtypes.Block)
	if err := rlp.DecodeBytes(blockBytes, inner); err != nil {
		return parsedContainer{}, fmt.Errorf("decode pre-fork eth block: %w (not a ProposerVM block either: %v)", err, perr)
	}
	return parsedContainer{
		containerID: ids.ID(inner.Hash()),
		blockNumber: inner.NumberU64(),
		blockHash:   ids.ID(inner.Hash()),
		parentID:    ids.ID(inner.ParentHash()),
		parentHash:  ids.ID(inner.ParentHash()),
		blockTime:   inner.Time(),
	}, nil
}

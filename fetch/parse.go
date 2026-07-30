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
// RegisterExtras installs the avalanchego graft submodule extras that
// all C-Chain state-touching code paths depend on. Safe to call from
// packages that don't open a Fetcher (e.g. debug/bench commands);
// idempotent after the first call.
var RegisterExtras = registerExtras

var registerExtras = sync.OnceFunc(func() {
	corethcore.RegisterExtras()
	ccustomtypes.Register()
	cparams.RegisterExtras()
	extstate.RegisterExtras()
})

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

	if proposerBlk, err := proposerblock.ParseWithoutVerification(raw); err == nil {
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
	// container ID equals the eth block hash.
	_, _, rest, err := rlp.Split(raw)
	if err != nil {
		return parsedContainer{}, fmt.Errorf("rlp split: %w", err)
	}
	blockBytes := raw[:len(raw)-len(rest)]
	inner := new(ethtypes.Block)
	if err := rlp.DecodeBytes(blockBytes, inner); err != nil {
		return parsedContainer{}, fmt.Errorf("decode pre-fork eth block: %w", err)
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

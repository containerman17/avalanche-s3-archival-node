package exec

import (
	"fmt"

	proposerblock "github.com/ava-labs/avalanchego/vms/proposervm/block"
	ethtypes "github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/rlp"
)

// parseEthBlock decodes a raw container into an eth block, transparently
// handling both ProposerVM-wrapped containers and the pre-fork RLP-only
// containers of the early history. The libevm extras must already be
// registered (fetch.RegisterExtras).
func parseEthBlock(raw []byte) (*ethtypes.Block, error) {
	if len(raw) == 0 {
		return nil, fmt.Errorf("empty container")
	}

	if proposerBlk, err := proposerblock.ParseWithoutVerification(raw); err == nil {
		inner := new(ethtypes.Block)
		if err := rlp.DecodeBytes(proposerBlk.Block(), inner); err != nil {
			return nil, fmt.Errorf("decode inner eth block: %w", err)
		}
		return inner, nil
	}

	// Pre-ProposerVM: the raw bytes are the RLP-encoded eth block, possibly
	// with trailing bytes to strip.
	_, _, rest, err := rlp.Split(raw)
	if err != nil {
		return nil, fmt.Errorf("rlp split: %w", err)
	}
	blockBytes := raw[:len(raw)-len(rest)]
	inner := new(ethtypes.Block)
	if err := rlp.DecodeBytes(blockBytes, inner); err != nil {
		return nil, fmt.Errorf("decode pre-fork eth block: %w", err)
	}
	return inner, nil
}

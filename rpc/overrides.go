package rpc

// eth_call's trailing override objects. They were PARSED NOWHERE before this
// file: a client that sent `stateOverride` got an answer computed without it,
// which is worse than an error because it looks like it worked. coreth's
// internal/ethapi is not importable (internal), so the two structs are
// reproduced field-for-field, including geth's `**hexutil.Big` balance (an
// explicit JSON null is distinguishable from an absent key) and the untagged
// BlockOverrides field names, which encoding/json matches case-insensitively.

import (
	"encoding/json"
	"fmt"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/common/hexutil"
	ethstate "github.com/ava-labs/libevm/core/state"
	"github.com/ava-labs/libevm/core/vm"
	"github.com/holiman/uint256"
)

type overrideAccount struct {
	Nonce     *hexutil.Uint64              `json:"nonce"`
	Code      *hexutil.Bytes               `json:"code"`
	Balance   **hexutil.Big                `json:"balance"`
	State     *map[common.Hash]common.Hash `json:"state"`
	StateDiff *map[common.Hash]common.Hash `json:"stateDiff"`
}

// stateOverride is the collection of overridden accounts.
type stateOverride map[common.Address]overrideAccount

// apply writes the overrides into st, then finalises, so the whole set
// behaves as if a transaction just before this one had produced it (geth's
// StateOverride.Apply, same order, same Finalise(false)).
func (diff *stateOverride) apply(st *ethstate.StateDB) error {
	if diff == nil {
		return nil
	}
	for addr, account := range *diff {
		if account.Nonce != nil {
			st.SetNonce(addr, uint64(*account.Nonce))
		}
		if account.Code != nil {
			st.SetCode(addr, *account.Code)
		}
		if account.Balance != nil && *account.Balance != nil {
			bal, overflow := uint256.FromBig((**account.Balance).ToInt())
			if overflow {
				return fmt.Errorf("account %s balance override overflows uint256", addr.Hex())
			}
			st.SetBalance(addr, bal)
		}
		if account.State != nil && account.StateDiff != nil {
			return fmt.Errorf("account %s has both 'state' and 'stateDiff'", addr.Hex())
		}
		if account.State != nil {
			st.SetStorage(addr, *account.State)
		}
		if account.StateDiff != nil {
			for key, value := range *account.StateDiff {
				st.SetState(addr, key, value)
			}
		}
	}
	st.Finalise(false)
	return nil
}

// blockOverrides is the set of header fields a call may pretend into the
// block context. BlobBaseFee is accepted and applied like the rest; neither
// Avalanche VM produces blob txs today, so it only ever changes what the
// BLOBBASEFEE opcode reads inside an overridden call.
type blockOverrides struct {
	Number      *hexutil.Big    `json:"number"`
	Difficulty  *hexutil.Big    `json:"difficulty"`
	Time        *hexutil.Uint64 `json:"time"`
	GasLimit    *hexutil.Uint64 `json:"gasLimit"`
	Coinbase    *common.Address `json:"coinbase"`
	BaseFee     *hexutil.Big    `json:"baseFee"`
	BlobBaseFee *hexutil.Big    `json:"blobBaseFee"`
}

func (diff *blockOverrides) apply(blockCtx *vm.BlockContext) {
	if diff == nil {
		return
	}
	if diff.Number != nil {
		blockCtx.BlockNumber = diff.Number.ToInt()
	}
	if diff.Difficulty != nil {
		blockCtx.Difficulty = diff.Difficulty.ToInt()
	}
	if diff.Time != nil {
		blockCtx.Time = uint64(*diff.Time)
	}
	if diff.GasLimit != nil {
		blockCtx.GasLimit = uint64(*diff.GasLimit)
	}
	if diff.Coinbase != nil {
		blockCtx.Coinbase = *diff.Coinbase
	}
	if diff.BaseFee != nil {
		blockCtx.BaseFee = diff.BaseFee.ToInt()
	}
	if diff.BlobBaseFee != nil {
		blockCtx.BlobBaseFee = diff.BlobBaseFee.ToInt()
	}
}

// overrides is the pair as one optional argument, so the executing helpers
// take one nil-able parameter instead of two.
type overrides struct {
	state *stateOverride
	block *blockOverrides
}

func (o *overrides) stateDiff() *stateOverride {
	if o == nil {
		return nil
	}
	return o.state
}

func (o *overrides) blockDiff() *blockOverrides {
	if o == nil {
		return nil
	}
	return o.block
}

// parseOverrides reads the optional state-override param at index sIdx and
// the optional block-override param at index bIdx (bIdx < 0 = the method has
// no block-override param). Returns nil when neither is present.
func parseOverrides(params []json.RawMessage, sIdx, bIdx int) (*overrides, *rpcError) {
	out := &overrides{}
	if raw := optParam(params, sIdx); raw != nil {
		if err := json.Unmarshal(raw, &out.state); err != nil {
			return nil, errInvalid("bad state override: %v", err)
		}
	}
	if bIdx >= 0 {
		if raw := optParam(params, bIdx); raw != nil {
			if err := json.Unmarshal(raw, &out.block); err != nil {
				return nil, errInvalid("bad block override: %v", err)
			}
		}
	}
	if out.state == nil && out.block == nil {
		return nil, nil
	}
	return out, nil
}

// optParam is params[i] when it is present and not JSON null.
func optParam(params []json.RawMessage, i int) json.RawMessage {
	if i < 0 || len(params) <= i || len(params[i]) == 0 || string(params[i]) == "null" {
		return nil
	}
	return params[i]
}

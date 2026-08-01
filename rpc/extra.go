package rpc

// The coreth-only and geth-only methods the parity audit found missing.
// Everything here is a thin shape over machinery that already exists in this
// package; nothing here executes anything the rest of the server does not
// already execute.

import (
	"encoding/json"
	"fmt"
	"math/big"

	"github.com/ava-labs/libevm/common/hexutil"
	"github.com/ava-labs/libevm/common/math"
	"github.com/davecgh/go-spew/spew"
)

// --- eth_callDetailed (coreth internal/ethapi/api_extra.go) -------------------

// detailedExecutionResult is coreth's DetailedExecutionResult verbatim: the
// same call as eth_call, but a failed execution is a RESULT, not a JSON-RPC
// error, so a caller gets gas and return data out of a revert too.
type detailedExecutionResult struct {
	UsedGas    uint64        `json:"gas"`
	ErrCode    int           `json:"errCode"`
	Err        string        `json:"err"`
	ReturnData hexutil.Bytes `json:"returnData"`
}

func (s *Server) callDetailed(params []json.RawMessage) (any, *rpcError) {
	if len(params) < 1 {
		return nil, errInvalid("need [callArgs, blockTag, stateOverride]")
	}
	var args callArgs
	if err := json.Unmarshal(params[0], &args); err != nil {
		return nil, errInvalid("bad call args: %v", err)
	}
	n, rerr := s.blockNumber(optParam(params, 1))
	if rerr != nil {
		return nil, rerr
	}
	if n == 0 {
		return nil, errInvalid("eth_callDetailed needs an executed block (>=1)")
	}
	ov, rerr := parseOverrides(params, 2, -1)
	if rerr != nil {
		return nil, rerr
	}
	gas := uint64(GasCap)
	if args.Gas != nil && uint64(*args.Gas) < gas {
		gas = uint64(*args.Gas)
	}
	res, rerr := s.runCall(&args, n, gas, nil, ov)
	if rerr != nil {
		return nil, rerr
	}
	out := &detailedExecutionResult{UsedGas: res.UsedGas, ReturnData: res.ReturnData}
	if res.Err != nil {
		out.Err = res.Err.Error()
	}
	// A revert carries geth's error code 3 and its "execution reverted"
	// message, exactly as revertError shapes it on the eth_call path.
	if len(res.Revert) > 0 {
		out.ErrCode = 3
		out.Err = "execution reverted"
	}
	return out, nil
}

// --- eth_suggestPriceOptions (coreth internal/ethapi/api.coreth.go) -----------

type priceOption struct {
	GasTip *hexutil.Big `json:"maxPriorityFeePerGas"`
	GasFee *hexutil.Big `json:"maxFeePerGas"`
}

type priceOptions struct {
	Slow   *priceOption `json:"slow"`
	Normal *priceOption `json:"normal"`
	Fast   *priceOption `json:"fast"`
}

// coreth's defaults from plugin/evm/config: slow 95%, fast 105%, tip capped
// at 20 gwei, floor 1 wei. They are flags on a real node; nothing on a read
// server configures them, so the defaults are the whole answer.
var (
	priceOptSlowPct = big.NewInt(95)
	priceOptFastPct = big.NewInt(105)
	priceOptDenom   = big.NewInt(100)
	priceOptMaxTip  = new(big.Int).Mul(big.NewInt(20), big.NewInt(1e9))
	priceOptMinTip  = big.NewInt(1)
)

// suggestPriceOptions is coreth's wallet-facing fee suggestion.
//
// ONE DOCUMENTED DEVIATION: coreth doubles its estimate of the NEXT block's
// base fee; this doubles the CURRENT head's base fee, because epochdb projects
// no forward base fee on either VM kind (the same gap that leaves
// eth_feeHistory's trailing entry at 0, DESIGN "subnet-evm scoping"). The
// doubling is the margin that exists for exactly this drift.
func (s *Server) suggestPriceOptions() (any, *rpcError) {
	header, rerr := s.headerAt(s.hist.Head())
	if rerr != nil {
		return nil, rerr
	}
	if header.BaseFee == nil {
		return nil, nil // pre-dynamic-fee head: coreth returns null too
	}
	estimate := s.gasOracle()
	capped := math.BigMin(estimate, priceOptMaxTip)

	slow := new(big.Int).Div(new(big.Int).Mul(capped, priceOptSlowPct), priceOptDenom)
	slow = math.BigMax(slow, priceOptMinTip)
	normal := new(big.Int).Set(capped)
	fast := new(big.Int).Div(new(big.Int).Mul(estimate, priceOptFastPct), priceOptDenom)

	base := new(big.Int).Lsh(header.BaseFee, 1)
	price := func(tip *big.Int) *priceOption {
		return &priceOption{
			GasTip: (*hexutil.Big)(tip),
			GasFee: (*hexutil.Big)(new(big.Int).Add(base, tip)),
		}
	}
	return &priceOptions{Slow: price(slow), Normal: price(normal), Fast: price(fast)}, nil
}

// --- eth_getChainConfig / debug_chainConfig -----------------------------------

// chainConfig returns the config the server executes with. It marshals
// through libevm, so the VM-specific extras payload (coreth's or
// subnet-evm's) is emitted by the VM'S OWN marshaller rather than by a
// hand-rolled copy here.
func (s *Server) chainConfig() (any, *rpcError) { return s.chainCfg, nil }

// --- debug_printBlock ---------------------------------------------------------

// printBlock is coreth's spew dump of the parsed block. The parameter is a
// plain JSON number in geth's signature, so a tag is accepted too.
func (s *Server) printBlock(params []json.RawMessage) (any, *rpcError) {
	if len(params) < 1 {
		return nil, errInvalid("need [blockNumber]")
	}
	n, rerr := s.modifiedAccountsBlock(params[0], false)
	if rerr != nil {
		return nil, rerr
	}
	blk, rerr := s.blockAt(n)
	if rerr != nil {
		return nil, rerr
	}
	return spew.Sdump(blk), nil
}

// --- debug_getAccessibleState -------------------------------------------------

// getAccessibleState answers geth's "which block in this range has state I
// can open". On a real node that is a pruning question; here every height up
// to the executed head has state by construction (the historical descent),
// so the answer is the first height of the range that is in range at all.
// The scan direction follows the arguments, as geth's does.
func (s *Server) getAccessibleState(params []json.RawMessage) (any, *rpcError) {
	if len(params) < 2 {
		return nil, errInvalid("need [fromBlock, toBlock]")
	}
	from, rerr := s.modifiedAccountsBlock(params[0], false)
	if rerr != nil {
		return nil, rerr
	}
	to, rerr := s.modifiedAccountsBlock(params[1], false)
	if rerr != nil {
		return nil, rerr
	}
	// stateAt refuses anything above the EXECUTED head, which is the only
	// bound that can make a height inaccessible on this node.
	lo, hi := min(from, to), max(from, to)
	first := lo
	if from > to {
		first = hi
	}
	if _, rerr := s.stateAt(first); rerr != nil {
		return nil, &rpcError{Code: -32000, Message: fmt.Sprintf(
			"no state available in range %d..%d: %s", from, to, rerr.Message)}
	}
	return hexutil.Uint64(first), nil
}

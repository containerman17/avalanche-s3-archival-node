// Package rpc is a minimal JSON-RPC HTTP server over the historical
// overlay: eth_chainId, eth_blockNumber, eth_getBalance,
// eth_getTransactionCount, eth_getCode, eth_getStorageAt, eth_call. No
// websockets, no subscriptions, single requests only (a JSON array gets a
// polite error).
package rpc

import (
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"sync"

	"github.com/ava-labs/avalanchego/graft/coreth/consensus"
	"github.com/ava-labs/avalanchego/graft/coreth/consensus/dummy"
	corethcore "github.com/ava-labs/avalanchego/graft/coreth/core"
	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/common/hexutil"
	ethstate "github.com/ava-labs/libevm/core/state"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/core/vm"
	"github.com/ava-labs/libevm/params"
	"github.com/ava-labs/libevm/rlp"

	"github.com/containerman17/epochdb/state"
)

// GasCap bounds eth_call execution.
const GasCap = 50_000_000

// Server serves historical reads at heights [0, hist.Head()].
type Server struct {
	// mu serializes request handling: the headers bucketLog (Hash(),
	// BLOCKHASH ChainContext) is not goroutine-safe.
	// ponytail: global lock; move header reads behind their own lock if
	// concurrent throughput ever matters.
	mu       sync.Mutex
	hist     *state.History
	chainCtx corethcore.ChainContext
	chainCfg *params.ChainConfig

	// tx APIs, wired by EnableTxAPIs (nil = methods unavailable).
	txidx  TxCandidateSource
	blocks BlockSource
	parse  ContainerParser
}

// HistoryChainContext serves BLOCKHASH headers through the epochs-aware
// History (raw store first, sealed epochs after --delete-raw).
func HistoryChainContext(hist *state.History) corethcore.ChainContext {
	return histChainCtx{hist}
}

type histChainCtx struct{ hist *state.History }

func (c histChainCtx) Engine() consensus.Engine { return dummy.NewFullFaker() }

func (c histChainCtx) GetHeader(_ common.Hash, n uint64) *types.Header {
	raw, ok, err := c.hist.HeaderRLP(n)
	if err != nil || !ok {
		return nil
	}
	var h types.Header
	if err := rlp.DecodeBytes(raw, &h); err != nil {
		return nil
	}
	return &h
}

func NewServer(hist *state.History, chainCtx corethcore.ChainContext, chainCfg *params.ChainConfig) *Server {
	return &Server{hist: hist, chainCtx: chainCtx, chainCfg: chainCfg}
}

type rpcRequest struct {
	ID     json.RawMessage   `json:"id"`
	Method string            `json:"method"`
	Params []json.RawMessage `json:"params"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    any    `json:"data,omitempty"`
}

func (e *rpcError) Error() string { return e.Message }

func errInvalid(format string, args ...any) *rpcError {
	return &rpcError{Code: -32602, Message: fmt.Sprintf(format, args...)}
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var req rpcRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeReply(w, nil, nil, &rpcError{Code: -32700, Message: "parse error (no batching)"})
		return
	}
	s.mu.Lock()
	result, rerr := s.dispatch(&req)
	s.mu.Unlock()
	writeReply(w, req.ID, result, rerr)
}

func writeReply(w http.ResponseWriter, id json.RawMessage, result any, rerr *rpcError) {
	w.Header().Set("Content-Type", "application/json")
	if id == nil {
		id = json.RawMessage("null")
	}
	reply := map[string]any{"jsonrpc": "2.0", "id": id}
	if rerr != nil {
		reply["error"] = rerr
	} else {
		reply["result"] = result
	}
	json.NewEncoder(w).Encode(reply)
}

func (s *Server) dispatch(req *rpcRequest) (any, *rpcError) {
	switch req.Method {
	case "eth_chainId":
		return hexutil.EncodeBig(s.chainCfg.ChainID), nil
	case "eth_blockNumber":
		return hexutil.EncodeUint64(s.hist.Head()), nil
	case "eth_getBalance":
		return s.accountField(req.Params, func(st *ethstate.StateDB, a common.Address) any {
			return hexutil.EncodeBig(st.GetBalance(a).ToBig())
		})
	case "eth_getTransactionCount":
		return s.accountField(req.Params, func(st *ethstate.StateDB, a common.Address) any {
			return hexutil.EncodeUint64(st.GetNonce(a))
		})
	case "eth_getCode":
		return s.accountField(req.Params, func(st *ethstate.StateDB, a common.Address) any {
			return hexutil.Encode(st.GetCode(a))
		})
	case "eth_getStorageAt":
		return s.getStorageAt(req.Params)
	case "eth_call":
		return s.ethCall(req.Params)
	case "eth_getTransactionByHash", "eth_getTransactionReceipt":
		if s.txidx == nil {
			return nil, &rpcError{Code: -32000, Message: "tx index not available (run epochdb cook-txindex)"}
		}
		if req.Method == "eth_getTransactionByHash" {
			return s.getTransactionByHash(req.Params)
		}
		return s.getTransactionReceipt(req.Params)
	default:
		return nil, &rpcError{Code: -32601, Message: "method not found: " + req.Method}
	}
}

// blockNumber resolves a block tag parameter to a height within coverage.
func (s *Server) blockNumber(raw json.RawMessage) (uint64, *rpcError) {
	head := s.hist.Head()
	if raw == nil {
		return head, nil
	}
	var tag string
	if err := json.Unmarshal(raw, &tag); err != nil {
		return 0, errInvalid("bad block tag: %v", err)
	}
	switch tag {
	case "latest", "pending", "safe", "finalized", "":
		return head, nil
	case "earliest":
		return 0, nil
	}
	n, err := hexutil.DecodeUint64(tag)
	if err != nil {
		return 0, errInvalid("bad block number %q: %v", tag, err)
	}
	if n > head {
		return 0, errInvalid("block %d beyond head %d", n, head)
	}
	return n, nil
}

func (s *Server) stateAt(n uint64) (*ethstate.StateDB, *rpcError) {
	st, err := ethstate.New(common.Hash{}, s.hist.StateAt(n), nil)
	if err != nil {
		return nil, &rpcError{Code: -32000, Message: err.Error()}
	}
	return st, nil
}

func (s *Server) accountField(params []json.RawMessage, f func(*ethstate.StateDB, common.Address) any) (any, *rpcError) {
	if len(params) < 1 {
		return nil, errInvalid("need [address, blockTag]")
	}
	var addr common.Address
	if err := json.Unmarshal(params[0], &addr); err != nil {
		return nil, errInvalid("bad address: %v", err)
	}
	var tag json.RawMessage
	if len(params) > 1 {
		tag = params[1]
	}
	n, rerr := s.blockNumber(tag)
	if rerr != nil {
		return nil, rerr
	}
	st, rerr := s.stateAt(n)
	if rerr != nil {
		return nil, rerr
	}
	res := f(st, addr)
	if err := st.Error(); err != nil {
		return nil, &rpcError{Code: -32000, Message: err.Error()}
	}
	return res, nil
}

func (s *Server) getStorageAt(params []json.RawMessage) (any, *rpcError) {
	if len(params) < 2 {
		return nil, errInvalid("need [address, slot, blockTag]")
	}
	var addr common.Address
	if err := json.Unmarshal(params[0], &addr); err != nil {
		return nil, errInvalid("bad address: %v", err)
	}
	var slotHex string
	if err := json.Unmarshal(params[1], &slotHex); err != nil {
		return nil, errInvalid("bad slot: %v", err)
	}
	slot := common.HexToHash(slotHex)
	var tag json.RawMessage
	if len(params) > 2 {
		tag = params[2]
	}
	n, rerr := s.blockNumber(tag)
	if rerr != nil {
		return nil, rerr
	}
	st, rerr := s.stateAt(n)
	if rerr != nil {
		return nil, rerr
	}
	val := st.GetState(addr, slot)
	if err := st.Error(); err != nil {
		return nil, &rpcError{Code: -32000, Message: err.Error()}
	}
	return val.Hex(), nil
}

type callArgs struct {
	From     *common.Address `json:"from"`
	To       *common.Address `json:"to"`
	Gas      *hexutil.Uint64 `json:"gas"`
	GasPrice *hexutil.Big    `json:"gasPrice"`
	Value    *hexutil.Big    `json:"value"`
	Data     *hexutil.Bytes  `json:"data"`
	Input    *hexutil.Bytes  `json:"input"`
}

func (s *Server) ethCall(params []json.RawMessage) (any, *rpcError) {
	if len(params) < 1 {
		return nil, errInvalid("need [callArgs, blockTag]")
	}
	var args callArgs
	if err := json.Unmarshal(params[0], &args); err != nil {
		return nil, errInvalid("bad call args: %v", err)
	}
	var tag json.RawMessage
	if len(params) > 1 {
		tag = params[1]
	}
	n, rerr := s.blockNumber(tag)
	if rerr != nil {
		return nil, rerr
	}
	if n == 0 {
		return nil, errInvalid("eth_call needs an executed block (>=1)")
	}
	raw, ok, err := s.hist.HeaderRLP(n)
	if err != nil || !ok {
		return nil, &rpcError{Code: -32000, Message: fmt.Sprintf("header %d unavailable: ok=%v err=%v", n, ok, err)}
	}
	var header types.Header
	if err := rlp.DecodeBytes(raw, &header); err != nil {
		return nil, &rpcError{Code: -32000, Message: err.Error()}
	}

	st, rerr := s.stateAt(n)
	if rerr != nil {
		return nil, rerr
	}

	gas := uint64(GasCap)
	if args.Gas != nil && uint64(*args.Gas) < gas {
		gas = uint64(*args.Gas)
	}
	msg := &corethcore.Message{
		From:              common.Address{},
		To:                args.To,
		Value:             new(big.Int),
		GasLimit:          gas,
		GasPrice:          new(big.Int),
		GasFeeCap:         new(big.Int),
		GasTipCap:         new(big.Int),
		SkipAccountChecks: true,
	}
	if args.From != nil {
		msg.From = *args.From
	}
	if args.Value != nil {
		msg.Value = (*big.Int)(args.Value)
	}
	if args.GasPrice != nil {
		msg.GasPrice = (*big.Int)(args.GasPrice)
	}
	if args.Input != nil {
		msg.Data = *args.Input
	} else if args.Data != nil {
		msg.Data = *args.Data
	}

	blockCtx := corethcore.NewEVMBlockContext(&header, s.chainCtx, nil)
	txCtx := corethcore.NewEVMTxContext(msg)
	evm := vm.NewEVM(blockCtx, txCtx, st, s.chainCfg, vm.Config{NoBaseFee: true})
	gp := new(corethcore.GasPool).AddGas(gas)
	res, err := corethcore.ApplyMessage(evm, msg, gp)
	if err != nil {
		return nil, &rpcError{Code: -32000, Message: err.Error()}
	}
	if err := st.Error(); err != nil {
		return nil, &rpcError{Code: -32000, Message: err.Error()}
	}
	if res.Err != nil {
		rerr := &rpcError{Code: 3, Message: "execution reverted"}
		if len(res.Revert()) > 0 {
			rerr.Data = hexutil.Encode(res.Revert())
		} else {
			rerr.Message = res.Err.Error()
		}
		return nil, rerr
	}
	return hexutil.Encode(res.ReturnData), nil
}

// ListenAndServe runs the server on addr until the listener fails.
func (s *Server) ListenAndServe(addr string) error {
	return http.ListenAndServe(addr, s)
}

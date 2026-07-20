package rpc

import (
	"encoding/json"
	"fmt"
	"math/big"

	corethcore "github.com/ava-labs/avalanchego/graft/coreth/core"
	cparams "github.com/ava-labs/avalanchego/graft/coreth/params"
	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/common/hexutil"
	ethstate "github.com/ava-labs/libevm/core/state"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/core/vm"
	"github.com/ava-labs/libevm/crypto"
	"github.com/ava-labs/libevm/eth/tracers"
	"github.com/ava-labs/libevm/eth/tracers/logger"
	"github.com/ava-labs/libevm/rlp"

	// Register the native tracers (callTracer, prestateTracer, ...).
	_ "github.com/ava-labs/libevm/eth/tracers/native"
)

// traceConfig mirrors geth's TraceConfig: struct-logger options inline,
// or a named tracer from the default directory.
type traceConfig struct {
	logger.Config
	Tracer       *string         `json:"tracer"`
	TracerConfig json.RawMessage `json:"tracerConfig"`
}

func newTracer(cfg *traceConfig, tctx *tracers.Context) (tracers.Tracer, error) {
	if cfg != nil && cfg.Tracer != nil && *cfg.Tracer != "" {
		return tracers.DefaultDirectory.New(*cfg.Tracer, tctx, cfg.TracerConfig)
	}
	var lcfg logger.Config
	if cfg != nil {
		lcfg = cfg.Config
	}
	return logger.NewStructLogger(&lcfg), nil
}

func parseTraceConfig(params []json.RawMessage, idx int) (*traceConfig, *rpcError) {
	if len(params) <= idx || string(params[idx]) == "null" {
		return nil, nil
	}
	var cfg traceConfig
	if err := json.Unmarshal(params[idx], &cfg); err != nil {
		return nil, errInvalid("bad trace config: %v", err)
	}
	return &cfg, nil
}

// traceTxsInBlock replays blk from its parent state. target < 0 traces
// every tx; otherwise only txs[target] is traced (the preceding txs replay
// untraced to build the intra-block state, the following ones are skipped).
func (s *Server) traceTxsInBlock(blk *types.Block, target int, cfg *traceConfig) ([]json.RawMessage, *rpcError) {
	header := blk.Header()
	n := blk.NumberU64()
	if n == 0 || n > s.hist.Head() {
		return nil, errInvalid("block %d not traceable (head %d)", n, s.hist.Head())
	}
	statedb, err := ethstate.New(common.Hash{}, s.hist.StateAt(n-1), nil)
	if err != nil {
		return nil, &rpcError{Code: -32000, Message: err.Error()}
	}
	if err := corethcore.ApplyUpgrades(s.chainCfg, nil, corethcore.NewBlockContext(header.Number, header.Time), statedb); err != nil {
		return nil, &rpcError{Code: -32000, Message: fmt.Sprintf("apply upgrades: %v", err)}
	}
	blockCtx := corethcore.NewEVMBlockContext(header, s.chainCtx, nil)
	gp := new(corethcore.GasPool).AddGas(header.GasLimit)
	var (
		usedGas uint64
		out     []json.RawMessage
	)
	for i, tx := range blk.Transactions() {
		if target >= 0 && i > target {
			break
		}
		statedb.SetTxContext(tx.Hash(), i)
		vmCfg := vm.Config{}
		var tracer tracers.Tracer
		if target < 0 || i == target {
			tracer, err = newTracer(cfg, &tracers.Context{
				BlockHash:   blk.Hash(),
				BlockNumber: header.Number,
				TxIndex:     i,
				TxHash:      tx.Hash(),
			})
			if err != nil {
				return nil, errInvalid("tracer: %v", err)
			}
			vmCfg.Tracer = tracer
		}
		if _, err := corethcore.ApplyTransaction(
			s.chainCfg, s.chainCtx, blockCtx, gp, statedb,
			header, tx, &usedGas, vmCfg,
		); err != nil {
			return nil, &rpcError{Code: -32000, Message: fmt.Sprintf("tx %d: %v", i, err)}
		}
		if tracer != nil {
			res, err := tracer.GetResult()
			if err != nil {
				return nil, &rpcError{Code: -32000, Message: fmt.Sprintf("tracer result tx %d: %v", i, err)}
			}
			out = append(out, res)
		}
	}
	return out, nil
}

func (s *Server) debugTraceTransaction(params []json.RawMessage) (any, *rpcError) {
	if s.txidx == nil {
		return nil, &rpcError{Code: -32000, Message: "tx index not available (run epochdb cook-txindex)"}
	}
	hash, rerr := txHashParam(params)
	if rerr != nil {
		return nil, rerr
	}
	cfg, rerr := parseTraceConfig(params, 1)
	if rerr != nil {
		return nil, rerr
	}
	blk, i, found, err := s.findTx(hash)
	if err != nil {
		return nil, &rpcError{Code: -32000, Message: err.Error()}
	}
	if !found {
		return nil, &rpcError{Code: -32000, Message: fmt.Sprintf("transaction %s not found", hash)}
	}
	res, rerr := s.traceTxsInBlock(blk, i, cfg)
	if rerr != nil {
		return nil, rerr
	}
	return res[len(res)-1], nil
}

// txTraceResult is geth's per-tx entry in block traces.
type txTraceResult struct {
	TxHash common.Hash     `json:"txHash"`
	Result json.RawMessage `json:"result,omitempty"`
	Error  string          `json:"error,omitempty"`
}

func (s *Server) debugTraceBlock(params []json.RawMessage, byHash bool) (any, *rpcError) {
	blk, ok, rerr := s.blockParam(params, byHash)
	if rerr != nil {
		return nil, rerr
	}
	if !ok {
		return nil, &rpcError{Code: -32000, Message: "block not found"}
	}
	cfg, rerr := parseTraceConfig(params, 1)
	if rerr != nil {
		return nil, rerr
	}
	results, rerr := s.traceTxsInBlock(blk, -1, cfg)
	if rerr != nil {
		return nil, rerr
	}
	out := make([]txTraceResult, len(results))
	for i, r := range results {
		out[i] = txTraceResult{TxHash: blk.Transactions()[i].Hash(), Result: r}
	}
	return out, nil
}

// --- raw getters -------------------------------------------------------------

func (s *Server) debugGetRawBlock(params []json.RawMessage) (any, *rpcError) {
	blk, ok, rerr := s.blockParam(params, false)
	if rerr != nil || !ok {
		return nil, rerr
	}
	raw, err := rlp.EncodeToBytes(blk)
	if err != nil {
		return nil, &rpcError{Code: -32000, Message: err.Error()}
	}
	return hexutil.Bytes(raw), nil
}

func (s *Server) debugGetRawHeader(params []json.RawMessage) (any, *rpcError) {
	blk, ok, rerr := s.blockParam(params, false)
	if rerr != nil || !ok {
		return nil, rerr
	}
	raw, err := rlp.EncodeToBytes(blk.Header())
	if err != nil {
		return nil, &rpcError{Code: -32000, Message: err.Error()}
	}
	return hexutil.Bytes(raw), nil
}

func (s *Server) debugGetRawTransaction(params []json.RawMessage) (any, *rpcError) {
	if s.txidx == nil {
		return nil, &rpcError{Code: -32000, Message: "tx index not available (run epochdb cook-txindex)"}
	}
	hash, rerr := txHashParam(params)
	if rerr != nil {
		return nil, rerr
	}
	blk, i, found, err := s.findTx(hash)
	if err != nil {
		return nil, &rpcError{Code: -32000, Message: err.Error()}
	}
	if !found {
		return hexutil.Bytes{}, nil // geth returns empty for unknown
	}
	raw, err := blk.Transactions()[i].MarshalBinary()
	if err != nil {
		return nil, &rpcError{Code: -32000, Message: err.Error()}
	}
	return hexutil.Bytes(raw), nil
}

func (s *Server) debugGetRawReceipts(params []json.RawMessage) (any, *rpcError) {
	blk, ok, rerr := s.blockParam(params, false)
	if rerr != nil || !ok {
		return nil, rerr
	}
	if blk.NumberU64() == 0 || len(blk.Transactions()) == 0 {
		return []hexutil.Bytes{}, nil
	}
	// debug namespace: raw consensus receipts come from re-execution
	// (allowed alongside traces; the eth receipt/log methods never do).
	receipts, err := ReExecuteBlock(s.hist, s.chainCtx, s.chainCfg, blk)
	if err != nil {
		return nil, &rpcError{Code: -32000, Message: err.Error()}
	}
	out := make([]hexutil.Bytes, len(receipts))
	for i, r := range receipts {
		raw, err := r.MarshalBinary()
		if err != nil {
			return nil, &rpcError{Code: -32000, Message: err.Error()}
		}
		out[i] = raw
	}
	return out, nil
}

// --- eth_createAccessList -----------------------------------------------------

type accessListArgs struct {
	callArgs
	AccessList *types.AccessList `json:"accessList"`
	Nonce      *hexutil.Uint64   `json:"nonce"`
}

// createAccessList runs the call with geth's AccessListTracer to a fixed
// point (usually two iterations).
func (s *Server) createAccessList(params []json.RawMessage) (any, *rpcError) {
	if len(params) < 1 {
		return nil, errInvalid("need [callArgs, blockTag]")
	}
	var args accessListArgs
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
		return nil, errInvalid("eth_createAccessList needs an executed block (>=1)")
	}
	header, rerr := s.headerAt(n)
	if rerr != nil {
		return nil, rerr
	}

	from := common.Address{}
	if args.From != nil {
		from = *args.From
	}
	// Destination: explicit, or the address the creation would deploy to.
	var to common.Address
	if args.To != nil {
		to = *args.To
	} else {
		st, rerr := s.stateAt(n)
		if rerr != nil {
			return nil, rerr
		}
		nonce := st.GetNonce(from)
		if args.Nonce != nil {
			nonce = uint64(*args.Nonce)
		}
		to = crypto.CreateAddress(from, nonce)
	}
	rules := s.chainCfg.Rules(header.Number, cparams.IsMergeTODO, header.Time)
	precompiles := vm.ActivePrecompiles(rules)

	gas := uint64(GasCap)
	if args.Gas != nil && uint64(*args.Gas) < gas {
		gas = uint64(*args.Gas)
	}
	var data []byte
	if args.Input != nil {
		data = *args.Input
	} else if args.Data != nil {
		data = *args.Data
	}

	// Geth's fixed-point loop: run with the previous round's list until
	// the traced list stops changing (AccessListTracer.Equal).
	prevTracer := logger.NewAccessListTracer(nil, from, to, precompiles)
	if args.AccessList != nil {
		prevTracer = logger.NewAccessListTracer(*args.AccessList, from, to, precompiles)
	}
	for i := 0; ; i++ {
		al := prevTracer.AccessList()
		st, rerr := s.stateAt(n)
		if rerr != nil {
			return nil, rerr
		}
		tracer := logger.NewAccessListTracer(al, from, to, precompiles)
		msg := &corethcore.Message{
			From:              from,
			To:                args.To,
			Value:             new(big.Int),
			GasLimit:          gas,
			GasPrice:          new(big.Int),
			GasFeeCap:         new(big.Int),
			GasTipCap:         new(big.Int),
			Data:              data,
			AccessList:        al,
			SkipAccountChecks: true,
		}
		if args.Value != nil {
			msg.Value = (*big.Int)(args.Value)
		}
		blockCtx := corethcore.NewEVMBlockContext(header, s.chainCtx, nil)
		evm := vm.NewEVM(blockCtx, corethcore.NewEVMTxContext(msg), st, s.chainCfg, vm.Config{Tracer: tracer, NoBaseFee: true})
		gp := new(corethcore.GasPool).AddGas(gas)
		res, err := corethcore.ApplyMessage(evm, msg, gp)
		if err != nil {
			return nil, &rpcError{Code: -32000, Message: err.Error()}
		}
		if tracer.Equal(prevTracer) {
			out := map[string]any{
				"accessList": al,
				"gasUsed":    hexutil.Uint64(res.UsedGas),
			}
			if res.Err != nil {
				out["error"] = res.Err.Error()
			}
			return out, nil
		}
		prevTracer = tracer
		if i >= 8 {
			return nil, &rpcError{Code: -32000, Message: "access list did not converge"}
		}
	}
}

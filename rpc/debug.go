package rpc

import (
	"encoding/json"
	"fmt"
	"math/big"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/common/hexutil"
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
// or a named tracer from the default directory. StateOverrides/BlockOverrides
// are geth's TraceCallConfig extension, only read by debug_traceCall.
type traceConfig struct {
	logger.Config
	Tracer         *string         `json:"tracer"`
	TracerConfig   json.RawMessage `json:"tracerConfig"`
	StateOverrides *stateOverride  `json:"stateOverrides"`
	BlockOverrides *blockOverrides `json:"blockOverrides"`
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
	if rerr := s.setExecBaseFee(header); rerr != nil {
		return nil, rerr
	}
	// The parent state through the SAME band selection eth_call uses: the
	// descent alone (hist.StateAt) answers zeroed accounts above the cooked
	// watermark, so a trace on a following node used to come back as a
	// complete, plausible, entirely fictional trace over the whole cook
	// window. stateAt refuses instead, and says cook lag.
	statedb, rerr := s.stateAt(n - 1)
	if rerr != nil {
		return nil, rerr
	}
	// Parent timestamp, not nil: nil re-activates every already-active
	// precompile here, so every post-Durango trace carried a phantom
	// SetNonce/SetCode the executor never performed (see exec runEVM).
	cctx := captureHeaders(s.chainCtx)
	parent := cctx.GetHeader(header.ParentHash, n-1)
	if cctx.err != nil {
		return nil, &rpcError{Code: -32000, Message: fmt.Sprintf("parent header %d: %v", n-1, cctx.err)}
	}
	if parent == nil {
		return nil, errInvalid("parent header %d missing", n-1)
	}
	backend := registeredVM()
	if err := backend.applyUpgrades(s.chainCfg, parent.Time, header, statedb); err != nil {
		return nil, &rpcError{Code: -32000, Message: fmt.Sprintf("apply upgrades: %v", err)}
	}
	blockCtx := backend.blockContext(header, cctx)
	gasLeft := header.GasLimit
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
			t, err := newTracer(cfg, &tracers.Context{
				BlockHash:   blk.Hash(),
				BlockNumber: header.Number,
				TxIndex:     i,
				TxHash:      tx.Hash(),
			})
			if err != nil {
				return nil, errInvalid("tracer: %v", err)
			}
			tracer, vmCfg.Tracer = t, t
		}
		_, execErr := backend.applyTx(
			s.chainCfg, cctx, blockCtx, &gasLeft, statedb,
			header, tx, &usedGas, vmCfg,
		)
		// The read failure FIRST, exactly as ReExecuteBlock does it: geth's
		// getDeleteStateObject records the error and hands back an empty
		// account, so a failed read surfaces as a nonsense "nonce too high"
		// or, worse, as a complete trace over zeroed state.
		if dbErr := statedb.Error(); dbErr != nil {
			return nil, &rpcError{Code: -32000, Message: dbErr.Error()}
		}
		if cctx.err != nil {
			return nil, &rpcError{Code: -32000, Message: cctx.err.Error()}
		}
		if execErr != nil {
			return nil, &rpcError{Code: -32000, Message: fmt.Sprintf("tx %d: %v", i, execErr)}
		}
		if tracer != nil {
			res, err := tracer.GetResult()
			if err != nil {
				return nil, &rpcError{Code: -32000, Message: fmt.Sprintf("tracer result tx %d: %v", i, err)}
			}
			out = append(out, res)
		}
	}
	if dbErr := statedb.Error(); dbErr != nil {
		return nil, &rpcError{Code: -32000, Message: dbErr.Error()}
	}
	if cctx.err != nil {
		return nil, &rpcError{Code: -32000, Message: cctx.err.Error()}
	}
	return out, nil
}

func (s *Server) debugTraceTransaction(params []json.RawMessage) (any, *rpcError) {
	if s.txidx == nil {
		return nil, &rpcError{Code: -32000, Message: "tx index not available yet (the node cooks it on its own cadence)"}
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
		return nil, &rpcError{Code: -32000, Message: "tx index not available yet (the node cooks it on its own cadence)"}
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
	base, rerr := s.headerBaseFee(blk.Header())
	if rerr != nil {
		return nil, rerr
	}
	receipts, err := ReExecuteBlock(s.hist, s.chainCtx, s.chainCfg, blk, base)
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

// debugTraceCall executes an eth_call-shaped message on the post-state of
// the given block under a tracer (geth's debug_traceCall; same state base
// as eth_call at that tag).
func (s *Server) debugTraceCall(params []json.RawMessage) (any, *rpcError) {
	if len(params) < 1 {
		return nil, errInvalid("need [callArgs, blockTag, traceConfig]")
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
		return nil, errInvalid("debug_traceCall needs an executed block (>=1)")
	}
	cfg, rerr := parseTraceConfig(params, 2)
	if rerr != nil {
		return nil, rerr
	}
	tracer, err := newTracer(cfg, &tracers.Context{BlockNumber: new(big.Int).SetUint64(n)})
	if err != nil {
		return nil, errInvalid("tracer: %v", err)
	}
	gas := uint64(GasCap)
	if args.Gas != nil && uint64(*args.Gas) < gas {
		gas = uint64(*args.Gas)
	}
	var ov *overrides
	if cfg != nil && (cfg.StateOverrides != nil || cfg.BlockOverrides != nil) {
		ov = &overrides{state: cfg.StateOverrides, block: cfg.BlockOverrides}
	}
	if _, rerr := s.runCall(&args, n, gas, tracer, ov); rerr != nil {
		return nil, rerr
	}
	res, err := tracer.GetResult()
	if err != nil {
		return nil, &rpcError{Code: -32000, Message: err.Error()}
	}
	return res, nil
}

// --- debug_getModifiedAccountsByNumber / ByHash --------------------------------

// modifiedAccountsBlock resolves one parameter of the method: a plain JSON
// number (geth's signature), a hex/named tag, or a block hash.
func (s *Server) modifiedAccountsBlock(raw json.RawMessage, byHash bool) (uint64, *rpcError) {
	if byHash {
		n, ok, rerr := s.heightByHash(raw)
		if rerr != nil {
			return 0, rerr
		}
		if !ok {
			return 0, &rpcError{Code: -32000, Message: "block not found"}
		}
		return n, nil
	}
	var num uint64
	if err := json.Unmarshal(raw, &num); err == nil {
		if num > s.hist.Head() {
			return 0, errInvalid("block %d beyond head %d", num, s.hist.Head())
		}
		return num, nil
	}
	return s.blockNumber(raw)
}

// debugGetModifiedAccounts serves debug_getModifiedAccountsByNumber/ByHash
// from the per-block write capture, at ANY height. One param: accounts
// modified in that block. Two params: union over (start, end] (geth diffs
// the two tries instead, so a value rewritten to its original across
// blocks still counts here; addresses come in capture order, not hash
// order).
func (s *Server) debugGetModifiedAccounts(params []json.RawMessage, byHash bool) (any, *rpcError) {
	if len(params) < 1 {
		return nil, errInvalid("need [startBlock, endBlock?]")
	}
	start, rerr := s.modifiedAccountsBlock(params[0], byHash)
	if rerr != nil {
		return nil, rerr
	}
	lo, hi := start, start
	if len(params) > 1 && string(params[1]) != "null" {
		end, rerr := s.modifiedAccountsBlock(params[1], byHash)
		if rerr != nil {
			return nil, rerr
		}
		if end <= start {
			return nil, errInvalid("end block %d must be after start block %d", end, start)
		}
		lo, hi = start+1, end
	}
	if hi-lo+1 > GetLogsMaxRange {
		return nil, errInvalid("block range %d exceeds %d", hi-lo+1, GetLogsMaxRange)
	}
	seen := map[common.Address]bool{}
	out := []common.Address{}
	for n := lo; n <= hi; n++ {
		addrs, ok, err := s.hist.ModifiedAccounts(n)
		if err != nil {
			return nil, &rpcError{Code: -32000, Message: err.Error()}
		}
		if !ok {
			// No frame: fine for an empty block, an error if txs ran
			// (write capture absent, e.g. an epoch-only node).
			blk, rerr := s.blockAt(n)
			if rerr != nil {
				return nil, rerr
			}
			if len(blk.Transactions()) > 0 {
				return nil, &rpcError{Code: -32000, Message: fmt.Sprintf("write capture missing for block %d (raw writelog absent on this node)", n)}
			}
			continue
		}
		for _, a := range addrs {
			if !seen[a] {
				seen[a] = true
				out = append(out, a)
			}
		}
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
	header, rerr := s.execHeader(n)
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
		if err := st.Error(); err != nil {
			return nil, &rpcError{Code: -32000, Message: err.Error()}
		}
		if args.Nonce != nil {
			nonce = uint64(*args.Nonce)
		}
		to = crypto.CreateAddress(from, nonce)
	}
	// The active precompile set is what the tracer must NOT list as accessed.
	// Rules is libevm's, and its VM-specific half comes out of the config's
	// registered extras payload, so this is one call on both kinds.
	rules := s.chainCfg.Rules(header.Number, isMergeTODO, header.Time)
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
	backend := registeredVM()
	cctx := captureHeaders(s.chainCtx)
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
		msg := &callMsg{
			From:       from,
			To:         args.To,
			Value:      new(big.Int),
			GasLimit:   gas,
			GasPrice:   new(big.Int),
			GasFeeCap:  new(big.Int),
			GasTipCap:  new(big.Int),
			Data:       data,
			AccessList: al,
		}
		if args.Value != nil {
			msg.Value = (*big.Int)(args.Value)
		}
		res, err := backend.applyMsg(s.chainCfg, backend.blockContext(header, cctx), st,
			msg, vm.Config{Tracer: tracer, NoBaseFee: true})
		if err != nil {
			return nil, &rpcError{Code: -32000, Message: err.Error()}
		}
		// An unreadable BLOCKHASH header is 0x0 in the trace and absent from
		// st.Error(), so the list would be a fiction of a different kind.
		if cctx.err != nil {
			return nil, &rpcError{Code: -32000, Message: cctx.err.Error()}
		}
		// A state read that failed (a coverage hole) makes the whole traced
		// list and gasUsed fiction, exactly as in ethCall.
		if err := st.Error(); err != nil {
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

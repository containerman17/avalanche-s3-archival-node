package rpc

import (
	"encoding/json"
	"fmt"
	"math/big"
	"sort"

	corethcore "github.com/ava-labs/avalanchego/graft/coreth/core"
	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/common/hexutil"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/core/vm"
	"github.com/ava-labs/libevm/eth/tracers"
	ethparams "github.com/ava-labs/libevm/params"
	"github.com/ava-labs/libevm/rlp"
)

// ClientVersion is returned by web3_clientVersion.
const ClientVersion = "epochdb/v0.1.0"

// fallbackGasPrice is the C-chain phase-0 minimum gas price (470 gwei),
// returned when the recent corpus blocks carry no transactions to sample.
var fallbackGasPrice = new(big.Int).Mul(big.NewInt(470), big.NewInt(ethparams.GWei))

func (s *Server) headerAt(n uint64) (*types.Header, *rpcError) {
	raw, ok, err := s.hist.HeaderRLP(n)
	if err != nil || !ok {
		return nil, &rpcError{Code: -32000, Message: fmt.Sprintf("header %d unavailable: ok=%v err=%v", n, ok, err)}
	}
	var h types.Header
	if err := rlp.DecodeBytes(raw, &h); err != nil {
		return nil, &rpcError{Code: -32000, Message: err.Error()}
	}
	return &h, nil
}

// runCall executes args as a message at height n with the given gas limit,
// optionally under a tracer (debug_traceCall). Kept separate from
// server.go's ethCall (file discipline); same semantics.
func (s *Server) runCall(args *callArgs, n, gas uint64, tracer tracers.Tracer) (*corethcore.ExecutionResult, *rpcError) {
	header, rerr := s.headerAt(n)
	if rerr != nil {
		return nil, rerr
	}
	st, rerr := s.stateAt(n)
	if rerr != nil {
		return nil, rerr
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
	if args.Input != nil {
		msg.Data = *args.Input
	} else if args.Data != nil {
		msg.Data = *args.Data
	}
	blockCtx := corethcore.NewEVMBlockContext(header, s.chainCtx, nil)
	vmCfg := vm.Config{NoBaseFee: true}
	if tracer != nil {
		vmCfg.Tracer = tracer
	}
	evm := vm.NewEVM(blockCtx, corethcore.NewEVMTxContext(msg), st, s.chainCfg, vmCfg)
	gp := new(corethcore.GasPool).AddGas(gas)
	res, err := corethcore.ApplyMessage(evm, msg, gp)
	if err != nil {
		return nil, &rpcError{Code: -32000, Message: err.Error()}
	}
	if err := st.Error(); err != nil {
		return nil, &rpcError{Code: -32000, Message: err.Error()}
	}
	return res, nil
}

// estimateGas: geth-style binary search over the executable gas limit.
func (s *Server) estimateGas(reqParams []json.RawMessage) (any, *rpcError) {
	if len(reqParams) < 1 {
		return nil, errInvalid("need [callArgs, blockTag]")
	}
	var args callArgs
	if err := json.Unmarshal(reqParams[0], &args); err != nil {
		return nil, errInvalid("bad call args: %v", err)
	}
	var tag json.RawMessage
	if len(reqParams) > 1 {
		tag = reqParams[1]
	}
	n, rerr := s.blockNumber(tag)
	if rerr != nil {
		return nil, rerr
	}
	if n == 0 {
		return nil, errInvalid("eth_estimateGas needs an executed block (>=1)")
	}

	hi := uint64(GasCap)
	if args.Gas != nil && uint64(*args.Gas) >= ethparams.TxGas {
		hi = min(hi, uint64(*args.Gas))
	} else if header, rerr := s.headerAt(n); rerr == nil && header.GasLimit > 0 {
		hi = min(hi, header.GasLimit)
	}

	// The ceiling must execute at all, or the call fails regardless of gas.
	res, rerr := s.runCall(&args, n, hi, nil)
	if rerr != nil {
		return nil, rerr
	}
	if res.Err != nil {
		out := &rpcError{Code: 3, Message: "execution reverted"}
		if len(res.Revert()) > 0 {
			out.Data = hexutil.Encode(res.Revert())
		} else {
			out.Message = res.Err.Error()
		}
		return nil, out
	}

	return hexutil.EncodeUint64(searchGas(ethparams.TxGas-1, hi, func(gas uint64) bool {
		res, rerr := s.runCall(&args, n, gas, nil)
		return rerr == nil && res.Err == nil
	})), nil
}

// searchGas returns the smallest gas in (lo, hi] for which executable
// holds, assuming executable is monotonic and executable(hi) is true.
func searchGas(lo, hi uint64, executable func(uint64) bool) uint64 {
	for lo+1 < hi {
		mid := (lo + hi) / 2
		if executable(mid) {
			hi = mid
		} else {
			lo = mid // intrinsic-gas and execution errors both mean "too low"
		}
	}
	return hi
}

// gasOracle samples the gas prices of transactions in recent corpus blocks
// (pre-London: legacy gasPrice semantics) and returns the 60th percentile;
// fallbackGasPrice when nothing is sampled.
func (s *Server) gasOracle() *big.Int {
	var prices []*big.Int
	n := s.hist.Head()
	for scanned := 0; n > 0 && scanned < 20 && len(prices) < 100; n-- {
		blk, rerr := s.blockAt(n)
		if rerr != nil {
			break
		}
		if len(blk.Transactions()) == 0 {
			continue
		}
		scanned++
		for _, tx := range blk.Transactions() {
			prices = append(prices, tx.GasPrice())
		}
	}
	if len(prices) == 0 {
		return new(big.Int).Set(fallbackGasPrice)
	}
	sort.Slice(prices, func(i, j int) bool { return prices[i].Cmp(prices[j]) < 0 })
	return prices[len(prices)*60/100]
}

// feeHistory serves the geth-shaped response from headers (and, when
// reward percentiles are requested, per-block re-execution for tx weights).
// ponytail: percentile rewards cap the range at 128 blocks; raise when a
// receipts store exists.
func (s *Server) feeHistory(params []json.RawMessage) (any, *rpcError) {
	if len(params) < 2 {
		return nil, errInvalid("need [blockCount, newestBlock, rewardPercentiles]")
	}
	var countHex string
	if err := json.Unmarshal(params[0], &countHex); err != nil {
		return nil, errInvalid("bad blockCount: %v", err)
	}
	count, err := hexutil.DecodeUint64(countHex)
	if err != nil || count == 0 {
		return nil, errInvalid("bad blockCount %q", countHex)
	}
	newest, rerr := s.blockNumber(params[1])
	if rerr != nil {
		return nil, rerr
	}
	var percentiles []float64
	if len(params) > 2 {
		if err := json.Unmarshal(params[2], &percentiles); err != nil {
			return nil, errInvalid("bad rewardPercentiles: %v", err)
		}
	}
	if count > 1024 {
		count = 1024
	}
	if len(percentiles) > 0 && count > 128 {
		return nil, errInvalid("reward percentiles capped at 128 blocks")
	}
	oldest := uint64(0)
	if newest+1 > count {
		oldest = newest + 1 - count
	}

	zero := (*hexutil.Big)(new(big.Int))
	var (
		baseFees []any
		ratios   []float64
		rewards  []any
	)
	for n := oldest; n <= newest; n++ {
		header, rerr := s.headerAt(n)
		if rerr != nil {
			return nil, rerr
		}
		bf := zero
		if header.BaseFee != nil {
			bf = (*hexutil.Big)(header.BaseFee)
		}
		baseFees = append(baseFees, bf)
		ratio := 0.0
		if header.GasLimit > 0 {
			ratio = float64(header.GasUsed) / float64(header.GasLimit)
		}
		ratios = append(ratios, ratio)
		if len(percentiles) > 0 {
			row, rerr := s.rewardRow(n, header, percentiles)
			if rerr != nil {
				return nil, rerr
			}
			rewards = append(rewards, row)
		}
	}
	baseFees = append(baseFees, zero) // next block's base fee: pre-London zero
	out := map[string]any{
		"oldestBlock":   hexutil.EncodeUint64(oldest),
		"baseFeePerGas": baseFees,
		"gasUsedRatio":  ratios,
	}
	if len(percentiles) > 0 {
		out["reward"] = rewards
	}
	return out, nil
}

// rewardRow computes the effective-tip percentiles of one block, weighted
// by per-tx gas used (geth's algorithm; gas weights come from one
// re-execution).
func (s *Server) rewardRow(n uint64, header *types.Header, percentiles []float64) ([]any, *rpcError) {
	row := make([]any, len(percentiles))
	zero := (*hexutil.Big)(new(big.Int))
	blk, rerr := s.blockAt(n)
	if rerr != nil {
		return nil, rerr
	}
	txs := blk.Transactions()
	if n == 0 || len(txs) == 0 {
		for i := range row {
			row[i] = zero
		}
		return row, nil
	}
	// ponytail: gas weights come from one re-execution; feeHistory must
	// cover the unsealed raw tail (incl. "latest"), which has no stored
	// receipt-fields yet. Switch to the stored sections when the unified
	// follower stores them for the tail too.
	receipts, err := ReExecuteBlock(s.hist, s.chainCtx, s.chainCfg, blk)
	if err != nil {
		return nil, &rpcError{Code: -32000, Message: fmt.Sprintf("re-execute block %d: %v", n, err)}
	}
	type tg struct {
		tip *big.Int
		gas uint64
	}
	items := make([]tg, len(txs))
	baseFee := header.BaseFee
	if baseFee == nil {
		baseFee = new(big.Int)
	}
	for i, tx := range txs {
		tip, err := tx.EffectiveGasTip(baseFee)
		if err != nil {
			tip = new(big.Int)
		}
		items[i] = tg{tip: tip, gas: receipts[i].GasUsed}
	}
	sort.Slice(items, func(i, j int) bool { return items[i].tip.Cmp(items[j].tip) < 0 })
	var cum uint64
	idx := 0
	for i, p := range percentiles {
		threshold := uint64(float64(header.GasUsed) * p / 100)
		for idx < len(items)-1 && cum+items[idx].gas < threshold {
			cum += items[idx].gas
			idx++
		}
		row[i] = (*hexutil.Big)(items[idx].tip)
	}
	return row, nil
}

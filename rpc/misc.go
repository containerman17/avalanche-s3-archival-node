package rpc

import (
	"encoding/json"
	"fmt"
	"math/big"
	"sort"

	"github.com/ava-labs/libevm/common/hexutil"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/core/vm"
	"github.com/ava-labs/libevm/eth/tracers"
	ethparams "github.com/ava-labs/libevm/params"
	"github.com/ava-labs/libevm/rlp"

	"github.com/containerman17/epochdb/state"
)

// ClientVersion is returned by web3_clientVersion.
const ClientVersion = "epochdb/v0.1.0"

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

// headerFor is headerAt with the plain-error signature gasChain wants.
func (s *Server) headerFor(n uint64) (*types.Header, error) {
	h, rerr := s.headerAt(n)
	if rerr != nil {
		return nil, fmt.Errorf("%s", rerr.Message)
	}
	return h, nil
}

// gasConsumed is the gas block n's execution ACTUALLY consumed, which is what
// the ACP-194 gas clock ticks by and what no header records: above the Helicon
// boundary `header.GasUsed` is the WORST CASE, the sum of transaction gas
// LIMITS plus end-of-block (atomic) op gas.
//
// Both halves are in the corpus. The real per-transaction gas is the stored
// receipt-fields section, the same rows eth_getTransactionReceipt answers from
// and never a re-execution. The op gas is recovered by SUBTRACTION, from the
// worst-case identity the executor verifies on every single SAE block
// (exec.checkSAEGasUsed): GasUsed == sum(tx limits) + op gas. That is why this
// needs no atomic-transaction parse, and therefore no AVAX asset ID, which the
// read server does not have.
func (s *Server) gasConsumed(n uint64) (uint64, error) {
	blk, rerr := s.blockAt(n)
	if rerr != nil {
		return 0, fmt.Errorf("%s", rerr.Message)
	}
	txs := blk.Transactions()
	var limits uint64
	for _, tx := range txs {
		limits += tx.Gas()
	}
	if blk.GasUsed() < limits {
		return 0, fmt.Errorf("block %d: header gas used %d is below the %d of transaction gas limits it must cover",
			n, blk.GasUsed(), limits)
	}
	used := blk.GasUsed() - limits // end-of-block op gas
	if len(txs) == 0 {
		return used, nil
	}
	rcptRec, _, rerr := s.storedSections(n)
	if rerr != nil {
		return 0, fmt.Errorf("%s", rerr.Message)
	}
	entries, err := state.DecodeStoredReceipts(rcptRec)
	if err != nil || len(entries) != len(txs) {
		return 0, fmt.Errorf("stored receipts decode for block %d: %d entries for %d txs: %v", n, len(entries), len(txs), err)
	}
	return used + entries[len(entries)-1].CumulativeGas, nil
}

// baseFees is the base fee of every block in [from, to], through the per-VM
// seam. A nil entry means the chain had no base fee at that height (pre-AP3,
// pre-SubnetEVM), which is an answer; anything the seam cannot establish is an
// ERROR, never a zero, because a zero base fee is a number a wallet acts on.
func (s *Server) baseFees(from, to uint64) ([]*big.Int, *rpcError) {
	fees, err := registeredVM().baseFees(s.chainCfg, from, to, s)
	if err != nil {
		return nil, &rpcError{Code: -32000, Message: fmt.Sprintf("base fee of blocks %d..%d: %v", from, to, err)}
	}
	return fees, nil
}

// baseFeeAt is baseFees for one stored height.
func (s *Server) baseFeeAt(n uint64) (*big.Int, *rpcError) {
	h, rerr := s.headerAt(n)
	if rerr != nil {
		return nil, rerr
	}
	return s.headerBaseFee(h)
}

// headerBaseFee is baseFeeAt for a block already in hand. Every site that
// holds one uses THIS, not baseFeeAt: it saves a header read, and it is the
// only form that can answer for the accepted-but-unexecuted tail, whose header
// is not in the store.
func (s *Server) headerBaseFee(h *types.Header) (*big.Int, *rpcError) {
	fee, err := registeredVM().baseFeeOf(s.chainCfg, h, s)
	if err != nil {
		return nil, &rpcError{Code: -32000, Message: fmt.Sprintf("base fee of block %d: %v", h.Number, err)}
	}
	return fee, nil
}

// setExecBaseFee stamps onto h the base fee EXECUTION saw, which above the
// Helicon boundary is not the one h carries (it carries none). h MUST be a
// header the caller owns: headerAt decodes a fresh one and blk.Header() is a
// copy, so no stored or hashed header is touched.
//
// EVERY EXECUTING READ PATH DOES THIS BEFORE BUILDING A BLOCK CONTEXT.
// `vm.Config{NoBaseFee: true}` disables the fee CHECKS, not the value: the
// BASEFEE opcode still returns blockCtx.BaseFee, so a contract that reads
// `block.basefee` answered zero to every eth_call, eth_estimateGas,
// eth_createAccessList and tracer run above the boundary, and a caller could
// not tell. saexec.Execute does the same substitution on its own header copy
// before it builds the context, which is what makes this the executed value
// rather than a plausible one.
//
// Below the boundary and on subnet-evm it writes back what was already there.
func (s *Server) setExecBaseFee(h *types.Header) *rpcError {
	base, rerr := s.headerBaseFee(h)
	if rerr != nil {
		return rerr
	}
	h.BaseFee = base
	return nil
}

// execHeader is headerAt(n) ready to execute against: see setExecBaseFee.
func (s *Server) execHeader(n uint64) (*types.Header, *rpcError) {
	h, rerr := s.headerAt(n)
	if rerr != nil {
		return nil, rerr
	}
	if rerr := s.setExecBaseFee(h); rerr != nil {
		return nil, rerr
	}
	return h, nil
}

// runCall executes args as a message at height n with the given gas limit,
// optionally under a tracer (debug_traceCall) and optionally with eth_call's
// state/block overrides. Kept separate from server.go's ethCall (file
// discipline); same semantics.
func (s *Server) runCall(args *callArgs, n, gas uint64, tracer tracers.Tracer, ov *overrides) (*callResult, *rpcError) {
	header, rerr := s.execHeader(n)
	if rerr != nil {
		return nil, rerr
	}
	st, rerr := s.stateAt(n)
	if rerr != nil {
		return nil, rerr
	}
	if err := ov.stateDiff().apply(st); err != nil {
		return nil, errInvalid("%v", err)
	}
	msg := &callMsg{
		To:        args.To,
		Value:     new(big.Int),
		GasLimit:  gas,
		GasPrice:  new(big.Int),
		GasFeeCap: new(big.Int),
		GasTipCap: new(big.Int),
	}
	if args.From != nil {
		msg.From = *args.From
	}
	if args.Value != nil {
		msg.Value = (*big.Int)(args.Value)
	}
	if args.GasPrice != nil {
		// Dropped before, which ethCall never did: with NoBaseFee set, a ZERO
		// gas price is what makes the EVM zero blockCtx.BaseFee (geth's
		// basefee <= feecap invariant, vm.NewEVM), so ignoring the caller's
		// price left GASPRICE and BASEFEE at 0 in every eth_estimateGas and
		// debug_traceCall no matter what the caller asked for.
		msg.GasPrice = (*big.Int)(args.GasPrice)
	}
	if args.Input != nil {
		msg.Data = *args.Input
	} else if args.Data != nil {
		msg.Data = *args.Data
	}
	vmCfg := vm.Config{NoBaseFee: true}
	if tracer != nil {
		vmCfg.Tracer = tracer
	}
	backend := registeredVM()
	blockCtx := backend.blockContext(header, s.chainCtx)
	ov.blockDiff().apply(&blockCtx)
	res, err := backend.applyMsg(s.chainCfg, blockCtx, st, msg, vmCfg)
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
	ov, rerr := parseOverrides(reqParams, 2, -1)
	if rerr != nil {
		return nil, rerr
	}

	hi := uint64(GasCap)
	if args.Gas != nil && uint64(*args.Gas) >= ethparams.TxGas {
		hi = min(hi, uint64(*args.Gas))
	} else if header, rerr := s.headerAt(n); rerr == nil && header.GasLimit > 0 {
		hi = min(hi, header.GasLimit)
	}

	// The ceiling must execute at all, or the call fails regardless of gas.
	res, rerr := s.runCall(&args, n, hi, nil, ov)
	if rerr != nil {
		return nil, rerr
	}
	if res.Err != nil {
		return nil, revertError(res)
	}

	// A TRANSPORT OR STATE failure is not "not executable". Reading it as one
	// pushed lo up exactly as an out-of-gas would and returned the cap as a
	// confident estimate, which a wallet then builds a transaction around.
	gas, rerr := searchGas(ethparams.TxGas-1, hi, func(gas uint64) (bool, *rpcError) {
		res, rerr := s.runCall(&args, n, gas, nil, ov)
		if rerr != nil {
			return false, rerr
		}
		return res.Err == nil, nil
	})
	if rerr != nil {
		return nil, rerr
	}
	return hexutil.EncodeUint64(gas), nil
}

// searchGas returns the smallest gas in (lo, hi] for which executable
// holds, assuming executable is monotonic and executable(hi) is true. An
// executable that ERRORS could not answer at all, and aborts the search:
// there is no estimate to report, only a failure.
func searchGas(lo, hi uint64, executable func(uint64) (bool, *rpcError)) (uint64, *rpcError) {
	for lo+1 < hi {
		mid := (lo + hi) / 2
		ok, rerr := executable(mid)
		if rerr != nil {
			return 0, rerr
		}
		if ok {
			hi = mid
		} else {
			lo = mid // intrinsic-gas and execution errors both mean "too low"
		}
	}
	return hi, nil
}

// gasOracle samples the gas prices of transactions in recent corpus blocks
// (pre-London: legacy gasPrice semantics) and returns the 60th percentile.
// With nothing to sample it falls back to the VM's own floor: a protocol
// constant on the C-chain, the chain's configured minBaseFee on an L1 (see
// rpcVM.minGasPrice).
//
// A BLOCK IT CANNOT READ IS AN ERROR, not an empty sample. Falling through to
// the floor answered 470 gwei on the C-chain and minBaseFee on an L1, the
// latter far below the real price, so the caller's transaction never mines,
// and eth_gasPrice / eth_maxPriorityFeePerGas / eth_suggestPriceOptions all
// reported it as a confident number.
func (s *Server) gasOracle() (*big.Int, *rpcError) {
	var prices []*big.Int
	n := s.hist.Head()
	for scanned := 0; n > 0 && scanned < 20 && len(prices) < 100; n-- {
		blk, rerr := s.blockAt(n)
		if rerr != nil {
			return nil, rerr
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
		return registeredVM().minGasPrice(s.chainCfg), nil // no txs to sample
	}
	sort.Slice(prices, func(i, j int) bool { return prices[i].Cmp(prices[j]) < 0 })
	return prices[len(prices)*60/100], nil
}

// suggestTip is eth_maxPriorityFeePerGas (and the tip eth_suggestPriceOptions
// builds its three speeds from): the part of the sampled gas price that sits
// ABOVE the current base fee, floored at zero.
//
// IT IS THE TIP, NOT THE PRICE. Reporting the whole sampled price, which this
// did, is not a rounding difference: a wallet sets maxPriorityFeePerGas to
// what this returns and maxFeePerGas to base + tip, so the whole price coming
// back as a "tip" makes it bid roughly twice the base fee as a priority fee
// and pay it to the coinbase. On a pre-dynamic-fee head there is no base fee
// to subtract and the whole price genuinely IS the tip.
func (s *Server) suggestTip() (*big.Int, *rpcError) {
	price, rerr := s.gasOracle()
	if rerr != nil {
		return nil, rerr
	}
	base, rerr := s.baseFeeAt(s.hist.Head())
	if rerr != nil {
		return nil, rerr
	}
	if base == nil {
		return price, nil
	}
	tip := new(big.Int).Sub(price, base)
	if tip.Sign() < 0 {
		tip.SetInt64(0) // the sample sits below the current base fee
	}
	return tip, nil
}

// nextBaseFee projects the base fee of the block that would be built on the
// header at n, through the per-VM seam (rpcVM.nextBaseFee). A nil result is
// "this chain has no base fee at that height", which is an answer; anything
// the seam cannot project is an ERROR, never a plausible stand-in, because
// every caller hands the number straight to a wallet.
func (s *Server) nextBaseFee(n uint64) (*big.Int, *rpcError) {
	header, rerr := s.headerAt(n)
	if rerr != nil {
		return nil, rerr
	}
	bf, err := registeredVM().nextBaseFee(s.chainCfg, header, s)
	if err != nil {
		return nil, &rpcError{Code: -32000, Message: fmt.Sprintf("project the base fee above block %d: %v", n, err)}
	}
	return bf, nil
}

// trailingBaseFee is eth_feeHistory's blockCount+1'th entry: the base fee of
// the block after n. Below the head that block is READ, not estimated; at the
// head it is projected.
func (s *Server) trailingBaseFee(n uint64) (*hexutil.Big, *rpcError) {
	var next *big.Int
	if n < s.hist.Head() {
		var rerr *rpcError
		if next, rerr = s.baseFeeAt(n + 1); rerr != nil {
			return nil, rerr
		}
	} else {
		var rerr *rpcError
		if next, rerr = s.nextBaseFee(n); rerr != nil {
			return nil, rerr
		}
	}
	if next == nil {
		next = new(big.Int) // pre-AP3 / pre-SubnetEVM: there is no base fee
	}
	return (*hexutil.Big)(next), nil
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
	// One walk for the whole range: above the Helicon boundary each entry is
	// derived from the gas clock, and deriving them one at a time would repeat
	// the settlement-lag walk per block.
	fees, rerr := s.baseFees(oldest, newest)
	if rerr != nil {
		return nil, rerr
	}
	for n := oldest; n <= newest; n++ {
		header, rerr := s.headerAt(n)
		if rerr != nil {
			return nil, rerr
		}
		bf := zero
		if f := fees[n-oldest]; f != nil {
			bf = (*hexutil.Big)(f)
		}
		baseFees = append(baseFees, bf)
		ratio := 0.0
		if header.GasLimit > 0 {
			ratio = float64(header.GasUsed) / float64(header.GasLimit)
		}
		ratios = append(ratios, ratio)
		if len(percentiles) > 0 {
			row, rerr := s.rewardRow(n, header, (*big.Int)(bf), percentiles)
			if rerr != nil {
				return nil, rerr
			}
			rewards = append(rewards, row)
		}
	}
	// geth's shape wants blockCount+1 entries, the last describing the block
	// AFTER the range. A wallet reads exactly that entry to set maxFeePerGas,
	// so the 0 this used to append was the answer that makes a transaction
	// never mine.
	//
	// ON AN ARCHIVE MOST OF THOSE BLOCKS ARE NOT UNBUILT. Only a range ending
	// at the head needs an estimate at all; anywhere below it the next block
	// is a block this node HOLDS, and its header's own base fee is a fact
	// rather than a projection. That is also exactly what a real node does
	// (avalanchego's saevm gasprice estimator takes the trailing entry off
	// block last+1 whenever last is not the accepted head).
	next, rerr := s.trailingBaseFee(newest)
	if rerr != nil {
		return nil, rerr
	}
	baseFees = append(baseFees, next)
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
func (s *Server) rewardRow(n uint64, header *types.Header, baseFee *big.Int, percentiles []float64) ([]any, *rpcError) {
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
	receipts, err := ReExecuteBlock(s.hist, s.chainCtx, s.chainCfg, blk, baseFee)
	if err != nil {
		return nil, &rpcError{Code: -32000, Message: fmt.Sprintf("re-execute block %d: %v", n, err)}
	}
	type tg struct {
		tip *big.Int
		gas uint64
	}
	items := make([]tg, len(txs))
	if baseFee == nil {
		baseFee = new(big.Int)
	}
	for i, tx := range txs {
		// A tx whose fee cap is under the block's base fee cannot have been
		// mined in it. Reporting a 0 tip would put a made-up number in the
		// percentile array as if it were sampled.
		tip, err := tx.EffectiveGasTip(baseFee)
		if err != nil {
			return nil, &rpcError{Code: -32000, Message: fmt.Sprintf(
				"block %d tx %x: effective tip against base fee %v: %v", n, tx.Hash(), baseFee, err)}
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

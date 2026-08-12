package rpc

import (
	"encoding/json"
	"fmt"
	"sort"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/types"

	"github.com/containerman17/epochdb/store"
)

// GetLogsMaxRange caps eth_getLogs to this many blocks per query.
const GetLogsMaxRange = 10_000

type logsFilter struct {
	FromBlock json.RawMessage   `json:"fromBlock"`
	ToBlock   json.RawMessage   `json:"toBlock"`
	Address   json.RawMessage   `json:"address"`
	Topics    []json.RawMessage `json:"topics"`
	BlockHash *common.Hash      `json:"blockHash"`
}

// getLogs: posting-list candidates -> the blocks' stored per-tx receipt rows
// -> exact positional filter. The posting lists are position-agnostic by
// design, so they only ever produce a candidate superset; the receipt rows
// provide ground truth. Never re-executes.
func (s *Server) getLogs(params []json.RawMessage) (any, *rpcError) {
	if len(params) < 1 {
		return nil, errInvalid("need [filter]")
	}
	var f logsFilter
	if err := json.Unmarshal(params[0], &f); err != nil {
		return nil, errInvalid("bad filter: %v", err)
	}
	if f.BlockHash != nil {
		// The blockHash form needs a hash index, which storage v0 has none of.
		// An empty array here would say "that block has no matching logs",
		// which is a different answer and a wrong one.
		return nil, notInPhase1("eth_getLogs with blockHash")
	}
	from, to, addrs, topics, rerr := s.parseFilterCriteria(&f)
	if rerr != nil {
		return nil, rerr
	}
	return s.runGetLogs(from, to, addrs, topics)
}

// parseFilterCriteria resolves and validates a logs filter object, shared
// between eth_getLogs and the filter API.
func (s *Server) parseFilterCriteria(f *logsFilter) (from, to uint64, addrs []common.Address, topics [][]common.Hash, _ *rpcError) {
	// A STORED filter is a moving range, so a single fixed block has no
	// meaning here.
	if f.BlockHash != nil {
		return 0, 0, nil, nil, errInvalid("blockHash filter is for eth_getLogs, not for a stored filter over a range")
	}
	from, rerr := s.blockNumber(f.FromBlock)
	if rerr != nil {
		return 0, 0, nil, nil, rerr
	}
	to, rerr = s.blockNumber(f.ToBlock)
	if rerr != nil {
		return 0, 0, nil, nil, rerr
	}
	if from == 0 {
		from = 1 // genesis has no logs
	}
	if to < from {
		return 0, 0, nil, nil, errInvalid("toBlock %d below fromBlock %d", to, from)
	}
	if to-from+1 > GetLogsMaxRange {
		return 0, 0, nil, nil, errInvalid("block range %d exceeds %d", to-from+1, GetLogsMaxRange)
	}
	addrs, topics, rerr = parseLogMatchers(f)
	if rerr != nil {
		return 0, 0, nil, nil, rerr
	}
	return from, to, addrs, topics, nil
}

// parseLogMatchers is the address/topic half of a logs filter, without any
// block range. A logs SUBSCRIPTION has no range to validate (it streams
// forward from now), so it needs this half alone.
func parseLogMatchers(f *logsFilter) (addrs []common.Address, topics [][]common.Hash, _ *rpcError) {
	if len(f.Address) > 0 {
		var one common.Address
		if err := json.Unmarshal(f.Address, &one); err == nil {
			addrs = []common.Address{one}
		} else if err := json.Unmarshal(f.Address, &addrs); err != nil {
			return nil, nil, errInvalid("bad address: %v", err)
		}
	}
	// topics[i] = nil (wildcard) | hash | [hash...]
	for _, raw := range f.Topics {
		if string(raw) == "null" || len(raw) == 0 {
			topics = append(topics, nil)
			continue
		}
		var one common.Hash
		if err := json.Unmarshal(raw, &one); err == nil {
			topics = append(topics, []common.Hash{one})
			continue
		}
		var many []common.Hash
		if err := json.Unmarshal(raw, &many); err != nil {
			return nil, nil, errInvalid("bad topics entry: %v", err)
		}
		topics = append(topics, many)
	}
	for len(topics) > 0 && topics[len(topics)-1] == nil {
		topics = topics[:len(topics)-1] // trailing wildcards are no-ops
	}
	return addrs, topics, nil
}

// runGetLogs is the parsed-filter execution path, shared with the filter
// API (eth_getFilterLogs / eth_getFilterChanges).
func (s *Server) runGetLogs(from, to uint64, addrs []common.Address, topics [][]common.Hash) ([]*types.Log, *rpcError) {
	candidates, rerr := s.logCandidates(from, to, addrs, topics)
	if rerr != nil {
		return nil, rerr
	}

	out := []*types.Log{}
	for _, n := range candidates {
		logs, rerr := s.logsOfBlock(n)
		if rerr != nil {
			return nil, rerr
		}
		for _, l := range logs {
			if logMatches(l, addrs, topics) {
				out = append(out, l)
			}
		}
	}
	return out, nil
}

// logsOfBlock is every stored log of block n, fully addressed (tx hash, block
// hash, positional indexes), with no filter applied. The addressing (block
// hash, tx hash, block-wide log index) is exactly what storedBlockReceipts
// derives, so there is ONE place that turns rcpt/ rows into types.Log.
func (s *Server) logsOfBlock(n uint64) ([]*types.Log, *rpcError) {
	blk, rerr := s.blockAt(n)
	if rerr != nil {
		return nil, rerr
	}
	if len(blk.Transactions()) == 0 {
		return nil, nil
	}
	receipts, rerr := s.storedBlockReceipts(blk)
	if rerr != nil {
		return nil, rerr
	}
	var out []*types.Log
	for _, r := range receipts {
		out = append(out, r.Logs...)
	}
	return out, nil
}

// logMatches is the exact positional filter (standard eth_getLogs
// semantics: addresses OR'd, topic positions AND'd, values within a
// position OR'd, missing positions never match).
func logMatches(l *types.Log, addrs []common.Address, topics [][]common.Hash) bool {
	if len(addrs) > 0 {
		ok := false
		for _, a := range addrs {
			ok = ok || a == l.Address
		}
		if !ok {
			return false
		}
	}
	for i, want := range topics {
		if want == nil {
			continue
		}
		if i >= len(l.Topics) {
			return false
		}
		ok := false
		for _, t := range want {
			ok = ok || t == l.Topics[i]
		}
		if !ok {
			return false
		}
	}
	return true
}

// logCandidates returns the ascending candidate block set for [from, to],
// driven by the lookup postings: union over the addresses, union per topic
// position, then INTERSECT, all in TxNum space, and the surviving TxNums map
// back to their blocks. The postings are position-agnostic, so this is a
// superset and runGetLogs applies the exact filter.
//
// A filter with NO address and NO topics has no posting list to drive it, so
// every block of the range is a candidate. That scan is bounded by
// GetLogsMaxRange.
func (s *Server) logCandidates(from, to uint64, addrs []common.Address, topics [][]common.Hash) ([]uint64, *rpcError) {
	loTx, hiTx, rerr := s.txRangeOf(from, to)
	if rerr != nil {
		return nil, rerr
	}

	var sets []map[uint64]bool
	if len(addrs) > 0 {
		set := map[uint64]bool{}
		for _, a := range addrs {
			if rerr := s.collectPostings(store.LogAddrPrefix(a[:]), loTx, hiTx, set); rerr != nil {
				return nil, rerr
			}
		}
		sets = append(sets, set)
	}
	for _, want := range topics {
		if want == nil {
			continue
		}
		set := map[uint64]bool{}
		for _, t := range want {
			if rerr := s.collectPostings(store.TopicPrefix(t[:]), loTx, hiTx, set); rerr != nil {
				return nil, rerr
			}
		}
		sets = append(sets, set)
	}

	if len(sets) == 0 {
		out := make([]uint64, 0, to-from+1)
		for n := from; n <= to; n++ {
			out = append(out, n)
		}
		return out, nil
	}

	// Intersect, smallest set drives.
	smallest := sets[0]
	for _, set := range sets[1:] {
		if len(set) < len(smallest) {
			smallest = set
		}
	}
	inSet := map[uint64]bool{}
	for txnum := range smallest {
		all := true
		for _, set := range sets {
			all = all && set[txnum]
		}
		if !all {
			continue
		}
		n, ok, err := s.db.HeightOfTx(txnum)
		if err != nil {
			return nil, &rpcError{Code: -32000, Message: err.Error()}
		}
		if ok && n >= from && n <= to {
			inSet[n] = true
		}
	}
	out := make([]uint64, 0, len(inSet))
	for b := range inSet {
		out = append(out, b)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out, nil
}

// collectPostings adds every TxNum under prefix in [loTx, hiTx] to set.
func (s *Server) collectPostings(prefix []byte, loTx, hiTx uint64, set map[uint64]bool) *rpcError {
	if err := s.db.Postings(prefix, loTx, hiTx, func(txnum uint64, _ byte) bool {
		set[txnum] = true
		return true
	}); err != nil {
		return &rpcError{Code: -32000, Message: err.Error()}
	}
	return nil
}

// txRangeOf converts a block range into the TxNum range the postings live in.
// A block with no transactions reports the TxNum it WOULD have started at, so
// an empty range comes back as hiTx < loTx and matches nothing.
func (s *Server) txRangeOf(from, to uint64) (loTx, hiTx uint64, _ *rpcError) {
	loTx, _, ok, err := s.db.BlockTxRange(from)
	if err != nil || !ok {
		return 0, 0, &rpcError{Code: -32000, Message: fmt.Sprintf("tx range of block %d: ok=%v err=%v", from, ok, err)}
	}
	first, count, ok, err := s.db.BlockTxRange(to)
	if err != nil || !ok {
		return 0, 0, &rpcError{Code: -32000, Message: fmt.Sprintf("tx range of block %d: ok=%v err=%v", to, ok, err)}
	}
	if count == 0 {
		if first == 0 {
			return 1, 0, nil // nothing at all below the range
		}
		return loTx, first - 1, nil
	}
	return loTx, first + uint64(count) - 1, nil
}

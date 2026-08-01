package rpc

import (
	"encoding/json"
	"fmt"
	"sort"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/types"

	"github.com/containerman17/epochdb/state"
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

// getLogs: posting-list candidates (sealed epochs) -> stored-logs sections
// -> exact positional filter. The posting lists are position-agnostic by
// design, so they only ever produce a candidate superset; the stored
// sections provide ground truth. Never re-executes.
func (s *Server) getLogs(params []json.RawMessage) (any, *rpcError) {
	if len(params) < 1 {
		return nil, errInvalid("need [filter]")
	}
	var f logsFilter
	if err := json.Unmarshal(params[0], &f); err != nil {
		return nil, errInvalid("bad filter: %v", err)
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
	if f.BlockHash != nil {
		return 0, 0, nil, nil, errInvalid("blockHash filter unsupported")
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
		// Sealed epoch below, live capture above: one encoding, so the tail
		// answers with the same full log payloads as a sealed epoch does.
		var rec []byte
		if e, inEpoch := s.hist.Epochs().At(n); inEpoch {
			var err error
			rec, _, err = e.StoredLogsRecord(n)
			if err != nil {
				return nil, &rpcError{Code: -32000, Message: err.Error()}
			}
		} else {
			logsRec, _, ok, err := s.hist.StoredTail(n)
			if err != nil {
				return nil, &rpcError{Code: -32000, Message: err.Error()}
			}
			if !ok {
				return nil, &rpcError{Code: -32000, Message: fmt.Sprintf(
					"no stored logs for block %d: it is not in a sealed epoch and this node captured none (it never executed that block)", n)}
			}
			rec = logsRec
		}
		if len(rec) == 0 {
			continue
		}
		stored, err := state.DecodeStoredLogs(rec)
		if err != nil {
			return nil, &rpcError{Code: -32000, Message: err.Error()}
		}
		// txHash/blockHash are derived from the block body at read time.
		raw, ok, err := s.blocks.GetByHeight(n)
		if err != nil || !ok {
			return nil, &rpcError{Code: -32000, Message: fmt.Sprintf("container %d: ok=%v err=%v", n, ok, err)}
		}
		blk, err := s.parse(raw)
		if err != nil {
			return nil, &rpcError{Code: -32000, Message: err.Error()}
		}
		blockHash := blk.Hash()
		txs := blk.Transactions()
		for pos, sl := range stored {
			l := &types.Log{
				Address:     sl.Address,
				Topics:      sl.Topics,
				Data:        sl.Data,
				BlockNumber: n,
				TxHash:      txs[sl.TxIndex].Hash(),
				TxIndex:     sl.TxIndex,
				BlockHash:   blockHash,
				Index:       uint(pos),
			}
			if logMatches(l, addrs, topics) {
				out = append(out, l)
			}
		}
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

// logCandidates returns the ascending candidate block set: epoch posting
// lists for the sealed range, raw logs-record scan for the tail. With no
// filters at all, every block in the sealed range is a candidate (the
// posting lists cannot enumerate "any log"; re-execution skips empty
// blocks fast). ponytail: wildcard queries re-execute the whole range,
// bounded by GetLogsMaxRange; add an any-log block list section if that
// ever matters.
func (s *Server) logCandidates(from, to uint64, addrs []common.Address, topics [][]common.Hash) ([]uint64, *rpcError) {
	inSet := map[uint64]bool{}
	epochs := s.hist.Epochs()
	if err := epochs.RequireCovered(from); err != nil {
		return nil, coverageError(err)
	}
	if err := epochs.RequireCovered(to); err != nil {
		return nil, coverageError(err)
	}
	sealedEnd := uint64(0)
	if end, ok := epochs.SealedEnd(); ok {
		sealedEnd = end
	}

	// sealed portion via posting lists
	for _, e := range epochs.All() {
		lo, hi := max(from, e.Start), min(to, e.End())
		if lo > hi {
			continue
		}
		blocks, err := epochLogCandidates(e, lo, hi, addrs, topics)
		if err != nil {
			return nil, &rpcError{Code: -32000, Message: err.Error()}
		}
		for _, b := range blocks {
			inSet[b] = true
		}
	}

	// raw tail via the captured per-block records
	for n := max(from, sealedEnd+1); n <= to; n++ {
		rec, ok, err := s.hist.LogTuples(n)
		if err != nil {
			return nil, &rpcError{Code: -32000, Message: err.Error()}
		}
		if !ok {
			continue // no logs in this block
		}
		if recMatches(rec, addrs, topics) {
			inSet[n] = true
		}
	}

	out := make([]uint64, 0, len(inSet))
	for b := range inSet {
		out = append(out, b)
	}
	sortUint64(out)
	return out, nil
}

// epochLogCandidates intersects the epoch's position-agnostic posting
// lists: union over addresses AND union per topic position.
func epochLogCandidates(e *state.Epoch, lo, hi uint64, addrs []common.Address, topics [][]common.Hash) ([]uint64, error) {
	var sets []map[uint64]bool
	if len(addrs) > 0 {
		set := map[uint64]bool{}
		for _, a := range addrs {
			blocks, err := e.LogAddrBlocks([20]byte(a))
			if err != nil {
				return nil, err
			}
			for _, b := range blocks {
				set[b] = true
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
			blocks, err := e.LogTopicBlocks([32]byte(t))
			if err != nil {
				return nil, err
			}
			for _, b := range blocks {
				set[b] = true
			}
		}
		sets = append(sets, set)
	}
	var out []uint64
	if len(sets) == 0 {
		for b := lo; b <= hi; b++ { // wildcard: whole clipped range
			out = append(out, b)
		}
		return out, nil
	}
	// intersect, smallest set drives
	smallest := sets[0]
	for _, s := range sets[1:] {
		if len(s) < len(smallest) {
			smallest = s
		}
	}
	for b := range smallest {
		if b < lo || b > hi {
			continue
		}
		all := true
		for _, s := range sets {
			all = all && s[b]
		}
		if all {
			out = append(out, b)
		}
	}
	return out, nil
}

// recMatches applies the position-agnostic superset test to a raw logs
// record (any requested address present AND, per topic position, any of
// its values present anywhere).
func recMatches(rec state.LogRec, addrs []common.Address, topics [][]common.Hash) bool {
	if len(addrs) > 0 {
		ok := false
		for _, a := range addrs {
			for _, ra := range rec.Addrs {
				ok = ok || ra == [20]byte(a)
			}
		}
		if !ok {
			return false
		}
	}
	for _, want := range topics {
		if want == nil {
			continue
		}
		ok := false
		for _, t := range want {
			for _, rt := range rec.Topics {
				ok = ok || rt == [32]byte(t)
			}
		}
		if !ok {
			return false
		}
	}
	return true
}

func sortUint64(v []uint64) {
	sort.Slice(v, func(i, j int) bool { return v[i] < v[j] })
}

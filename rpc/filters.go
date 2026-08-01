package rpc

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/common/hexutil"
	"github.com/ava-labs/libevm/crypto"
)

// Filter API: an in-memory registry with real incremental semantics
// (changes = everything since the last poll). On a static corpus the head
// never moves, so every filter legitimately returns nothing after
// installation; the same code goes live in phase 3 when the tip follower
// advances the head. ponytail: no 5-minute deadline eviction like geth,
// add when the server is long-lived with untrusted clients.
type filterEntry struct {
	kind     byte // 'l' logs, 'b' blocks, 'p' pending txs
	from, to uint64
	addrs    []common.Address
	topics   [][]common.Hash
	lastHead uint64 // consumed up to and including this block
}

type filterReg struct {
	mu sync.Mutex
	m  map[string]*filterEntry
}

func newFilterID() string {
	var b [16]byte
	rand.Read(b[:])
	return "0x" + hex.EncodeToString(b[:])
}

func (s *Server) filtersReg() *filterReg {
	s.filtersOnce.Do(func() { s.filters = &filterReg{m: map[string]*filterEntry{}} })
	return s.filters
}

func (s *Server) newFilter(params []json.RawMessage) (any, *rpcError) {
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
	reg := s.filtersReg()
	id := newFilterID()
	reg.mu.Lock()
	reg.m[id] = &filterEntry{
		kind: 'l', from: from, to: to, addrs: addrs, topics: topics,
		lastHead: s.hist.Head(),
	}
	reg.mu.Unlock()
	return id, nil
}

func (s *Server) newBlockFilter() (any, *rpcError) {
	reg := s.filtersReg()
	id := newFilterID()
	reg.mu.Lock()
	reg.m[id] = &filterEntry{kind: 'b', lastHead: s.hist.Head()}
	reg.mu.Unlock()
	return id, nil
}

func (s *Server) newPendingTxFilter() (any, *rpcError) {
	reg := s.filtersReg()
	id := newFilterID()
	reg.mu.Lock()
	reg.m[id] = &filterEntry{kind: 'p'}
	reg.mu.Unlock()
	return id, nil
}

func filterIDParam(params []json.RawMessage) (string, *rpcError) {
	if len(params) < 1 {
		return "", errInvalid("need [filterID]")
	}
	var id string
	if err := json.Unmarshal(params[0], &id); err != nil {
		return "", errInvalid("bad filter id: %v", err)
	}
	return id, nil
}

func (s *Server) getFilterChanges(params []json.RawMessage) (any, *rpcError) {
	id, rerr := filterIDParam(params)
	if rerr != nil {
		return nil, rerr
	}
	reg := s.filtersReg()
	reg.mu.Lock()
	f, ok := reg.m[id]
	if !ok {
		reg.mu.Unlock()
		return nil, &rpcError{Code: -32000, Message: "filter not found"}
	}
	head := s.hist.Head()
	last := f.lastHead
	f.lastHead = head
	kind, from, to, addrs, topics := f.kind, f.from, f.to, f.addrs, f.topics
	reg.mu.Unlock()

	switch kind {
	case 'p':
		return []string{}, nil // no mempool: never any pending txs
	case 'b':
		hashes := []string{}
		for n := last + 1; n <= head; n++ {
			blk, rerr := s.blockAt(n)
			if rerr != nil {
				return nil, rerr
			}
			hashes = append(hashes, blk.Hash().Hex())
		}
		return hashes, nil
	default: // logs: new matches in (last, head], clipped to the criteria
		lo := max(from, last+1)
		hi := min(to, head)
		if lo > hi {
			return []any{}, nil
		}
		return s.runGetLogs(lo, hi, addrs, topics)
	}
}

func (s *Server) getFilterLogs(params []json.RawMessage) (any, *rpcError) {
	id, rerr := filterIDParam(params)
	if rerr != nil {
		return nil, rerr
	}
	reg := s.filtersReg()
	reg.mu.Lock()
	f, ok := reg.m[id]
	reg.mu.Unlock()
	if !ok || f.kind != 'l' {
		return nil, &rpcError{Code: -32000, Message: "filter not found"}
	}
	return s.runGetLogs(f.from, min(f.to, s.hist.Head()), f.addrs, f.topics)
}

func (s *Server) uninstallFilter(params []json.RawMessage) (any, *rpcError) {
	id, rerr := filterIDParam(params)
	if rerr != nil {
		return nil, rerr
	}
	reg := s.filtersReg()
	reg.mu.Lock()
	_, ok := reg.m[id]
	delete(reg.m, id)
	reg.mu.Unlock()
	return ok, nil
}

// ---------- trivia the audit surfaced ----------

func web3Sha3(params []json.RawMessage) (any, *rpcError) {
	if len(params) < 1 {
		return nil, errInvalid("need [data]")
	}
	var data hexutil.Bytes
	if err := json.Unmarshal(params[0], &data); err != nil {
		return nil, errInvalid("bad data: %v", err)
	}
	return hexutil.Encode(crypto.Keccak256(data)), nil
}

// emptyTxPool mirrors coreth's TxPoolAPI shapes with no pool.
func emptyTxPool(method string) any {
	switch method {
	case "txpool_status":
		return map[string]string{"pending": "0x0", "queued": "0x0"}
	default: // content, contentFrom, inspect
		return map[string]any{"pending": map[string]any{}, "queued": map[string]any{}}
	}
}

// baseFee mirrors coreth's eth_baseFee: the current base fee. Pre-AP3
// heads have none; report zero rather than an error.
func (s *Server) baseFee() (any, *rpcError) {
	header, rerr := s.headerAt(s.hist.Head())
	if rerr != nil {
		return nil, rerr
	}
	if header.BaseFee == nil {
		return "0x0", nil
	}
	return hexutil.EncodeBig(header.BaseFee), nil
}

func (s *Server) getHeaderBy(params []json.RawMessage, byHash bool) (any, *rpcError) {
	blk, ok, rerr := s.blockParam(params, byHash)
	if rerr != nil {
		return nil, rerr
	}
	if !ok {
		return nil, nil
	}
	return s.marshalHeader(blk), nil
}

func (s *Server) rawTxByHash(params []json.RawMessage) (any, *rpcError) {
	hash, rerr := txHashParam(params)
	if rerr != nil {
		return nil, rerr
	}
	blk, i, found, err := s.findTx(hash)
	if err != nil {
		return nil, &rpcError{Code: -32000, Message: err.Error()}
	}
	if !found {
		return "0x", nil // coreth returns empty bytes for unknown txs
	}
	raw, err := blk.Transactions()[i].MarshalBinary()
	if err != nil {
		return nil, &rpcError{Code: -32000, Message: err.Error()}
	}
	return hexutil.Encode(raw), nil
}

func (s *Server) rawTxByBlockAndIndex(params []json.RawMessage, byHash bool) (any, *rpcError) {
	blk, ok, rerr := s.blockParam(params, byHash)
	if rerr != nil {
		return nil, rerr
	}
	if !ok {
		return nil, nil
	}
	if len(params) < 2 {
		return nil, errInvalid("need [block, index]")
	}
	var idxHex string
	if err := json.Unmarshal(params[1], &idxHex); err != nil {
		return nil, errInvalid("bad index: %v", err)
	}
	idx, err := hexutil.DecodeUint64(idxHex)
	if err != nil {
		return nil, errInvalid("bad index: %v", err)
	}
	if idx >= uint64(len(blk.Transactions())) {
		return nil, nil
	}
	raw, err := blk.Transactions()[idx].MarshalBinary()
	if err != nil {
		return nil, &rpcError{Code: -32000, Message: err.Error()}
	}
	return hexutil.Encode(raw), nil
}

// errTxSubmission is the documented phase-3 gap.
func errTxSubmission(method string) *rpcError {
	return &rpcError{Code: -32000, Message: fmt.Sprintf(
		"%s: transaction submission not supported: read-only archive node (mempool relay planned)", method)}
}

// errNoKeystore refuses the signing methods. They are not a gap to close
// later: epochdb holds no keys and never will, so a request to sign has no
// answer that is not a lie.
func errNoKeystore(method string) *rpcError {
	return &rpcError{Code: -32000, Message: fmt.Sprintf(
		"%s: no keystore on a read-only archive node: sign the transaction client-side", method)}
}

// errNotArchivable names the whole class of methods that need a component an
// archival read server does not have (a keystore, a miner, a p2p/node-ops
// surface). -32601 keeps geth's code for "this node does not serve that",
// with a message that says WHY instead of leaving the caller to guess.
func errNotArchivable(method string) *rpcError {
	return &rpcError{Code: -32601, Message: fmt.Sprintf(
		"%s: unsupported namespace on a read-only archive node (no keystore, no mempool, no miner, no node-ops surface)", method)}
}

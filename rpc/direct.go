package rpc

import (
	"errors"

	"github.com/ava-labs/libevm/common"
	ethstate "github.com/ava-labs/libevm/core/state"
	"github.com/ava-labs/libevm/core/types"

	"github.com/containerman17/epochdb/state"
)

// Direct Go reads for in-process consumers (the read-only SDK). Every one of
// these is the SAME call the JSON-RPC handler makes, minus the JSON: there is
// no second read path, and no HTTP hop for a consumer in the same process.

func (e *rpcError) error() error {
	if e == nil {
		return nil
	}
	return errors.New(e.Message)
}

// StateAt opens the state at height n with the three serving bands of
// stateAt: above the executed head is an error, at or above the serving head
// is the uncooked-tail overlay over the descent, below it is the descent
// alone. The returned StateDB is single-use and NOT goroutine-safe.
func (s *Server) StateAt(n uint64) (*ethstate.StateDB, error) {
	st, rerr := s.stateAt(n)
	return st, rerr.error()
}

// BlockAt returns the parsed block at height n.
func (s *Server) BlockAt(n uint64) (*types.Block, error) {
	blk, rerr := s.blockAt(n)
	return blk, rerr.error()
}

// HeightByHash resolves an eth block hash to its height: the bounded
// live-tail map first, then the fingerprint index, which since epoch format
// v6 carries every block hash mapped to that block's own height. There is no
// whole-history hash map anywhere.
//
// A candidate is VERIFIED against the real header, so a fingerprint that
// belongs to a tx rather than to a block (or to a different block entirely)
// is rejected at the cost of one header read, exactly like the tx path's
// container read.
func (s *Server) HeightByHash(hash common.Hash) (uint64, bool, error) {
	if n, ok := s.recent.get(hash); ok {
		return n, true, nil
	}
	if s.txidx == nil {
		return 0, false, errors.New("block hash index not available yet (the node cooks it on its own cadence)")
	}
	var (
		height uint64
		found  bool
	)
	err := s.txidx.WalkCandidates(hash, func(n uint64) (bool, error) {
		raw, ok, err := s.hist.HeaderRLP(n)
		if err != nil {
			return false, err
		}
		if !ok || state.BlockHashFromHeaderRLP(raw) != hash {
			return false, nil
		}
		height, found = n, true
		return true, nil
	})
	if err != nil {
		return 0, false, err
	}
	return height, found, nil
}

// FindTx resolves a tx hash to its block and index. found=false is a clean
// "unknown tx".
func (s *Server) FindTx(hash common.Hash) (blk *types.Block, txIndex int, found bool, err error) {
	if s.txidx == nil {
		return nil, 0, false, errors.New("tx index not available yet (the node cooks it on its own cadence)")
	}
	return s.findTx(hash)
}

// BlockReceipts reconstructs every receipt of blk from the stored sections
// (sealed epoch or live tail capture, one encoding, no re-execution).
func (s *Server) BlockReceipts(blk *types.Block) (types.Receipts, error) {
	rs, rerr := s.storedBlockReceipts(blk)
	return rs, rerr.error()
}

// GetLogs runs a log query over [from, to] through the posting lists and the
// stored log sections. Range bounds are the caller's to keep sane; the JSON
// path caps them at GetLogsMaxRange.
func (s *Server) GetLogs(from, to uint64, addrs []common.Address, topics [][]common.Hash) ([]*types.Log, error) {
	if s.blocks == nil {
		return nil, errors.New("block source not available")
	}
	logs, rerr := s.runGetLogs(from, to, addrs, topics)
	return logs, rerr.error()
}

package rpc

import (
	"errors"

	"github.com/ava-labs/libevm/common"
	ethstate "github.com/ava-labs/libevm/core/state"
	"github.com/ava-labs/libevm/core/types"
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
// stateAt: above the executed head is an error, at or above the stored head is
// the latest view (re-read per access), below it is the fixed-height view. The
// returned StateDB is single-use and NOT goroutine-safe.
func (s *Server) StateAt(n uint64) (*ethstate.StateDB, error) {
	st, rerr := s.stateAt(n)
	return st, rerr.error()
}

// BlockAt returns the parsed block at height n.
func (s *Server) BlockAt(n uint64) (*types.Block, error) {
	blk, rerr := s.blockAt(n)
	return blk, rerr.error()
}

// HeightByHash resolves a block hash through the blkh/ lookup rows. found=false
// is a clean "not on this chain"; a read failure is the error.
func (s *Server) HeightByHash(h common.Hash) (uint64, bool, error) {
	n, ok, rerr := s.heightByHash(h)
	return n, ok, rerr.error()
}

// FindTx resolves a tx hash to its block and index. found=false is a clean
// "unknown tx".
func (s *Server) FindTx(hash common.Hash) (blk *types.Block, txIndex int, found bool, err error) {
	return s.findTx(hash)
}

// BlockReceipts reconstructs every receipt of blk from its stored per-tx
// receipt rows (no re-execution).
func (s *Server) BlockReceipts(blk *types.Block) (types.Receipts, error) {
	rs, rerr := s.storedBlockReceipts(blk)
	return rs, rerr.error()
}

// GetLogs runs a log query over [from, to] through the posting lists and the
// stored log sections. Range bounds are the caller's to keep sane; the JSON
// path caps them at GetLogsMaxRange.
func (s *Server) GetLogs(from, to uint64, addrs []common.Address, topics [][]common.Hash) ([]*types.Log, error) {
	logs, rerr := s.runGetLogs(from, to, addrs, topics)
	return logs, rerr.error()
}

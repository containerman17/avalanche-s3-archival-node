package rpc

import (
	"errors"

	"github.com/ava-labs/libevm/common"
	ethstate "github.com/ava-labs/libevm/core/state"
	"github.com/ava-labs/libevm/core/types"

	"github.com/containerman17/epochdb/store"
)

// THE CORE QUERY LAYER in Go (DESIGN "Entry points and adapters"): the reads
// every wire format is a thin PEER adapter over. Every one of these is the SAME
// call the JSON-RPC handler makes, minus the JSON, so there is no second read
// path, no HTTP hop for a consumer in the same process, and no adapter stacked
// on another.

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

// Head is the serving frontier, the three labels at once.
type Head struct {
	Number    uint64 // last block this node serves: the `latest` label
	Hash      common.Hash
	Timestamp uint64
	Accepted  uint64 // the follower's accepted head: the `pending` label
	Settled   uint64 // SAE-settled: `safe`/`finalized`. == Number below Helicon
}

// Head reads it. An empty store answers Number 0 with a zero hash.
func (s *Server) Head() (Head, error) {
	n := s.head()
	h := Head{Number: n, Accepted: s.acceptedHead(), Settled: n}
	if s.live != nil {
		h.Settled = min(s.live.SettledHeight(), n)
	}
	header, rerr := s.headerAt(n)
	if rerr != nil {
		return Head{}, rerr.error()
	}
	h.Hash, h.Timestamp = header.Hash(), header.Time
	return h, nil
}

// CallMsg is the message an in-process call executes: the VM-neutral
// core.Message the two backends share, with no JSON shape anywhere near it.
type CallMsg = callMsg

// Call runs msg at height n and returns its return data. THE FAST PATH: this
// is eth_call with the JSON removed on both sides, and it is the same
// s.call the JSON handler runs. A reverted call is an error whose return data
// is still handed back.
func (s *Server) Call(msg *CallMsg, n uint64) ([]byte, error) {
	res, rerr := s.call(msg, n, nil, nil)
	if rerr != nil {
		return nil, rerr.error()
	}
	if res.Err != nil {
		return res.ReturnData, revertError(res).error()
	}
	return res.ReturnData, nil
}

// Frames returns a transaction's STORED call frames (DESIGN: traces are
// stored, so this is a read and never a re-execution). found=false is a clean
// "unknown transaction"; a known transaction with no nested call returns no
// frames and found=true.
func (s *Server) Frames(hash common.Hash) (frames []store.Frame, found bool, err error) {
	num, ok, err := s.db.TxNumByHash(hash[:])
	if err != nil || !ok {
		return nil, false, err
	}
	rec, ok, err := s.db.Frames(num)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, errors.New("transaction " + hash.Hex() + " has no stored call frames: this node never executed it")
	}
	frames, err = store.DecodeFrames(rec)
	return frames, err == nil, err
}

// AddrHit is one `addr/` posting row: a transaction the address took part in,
// and the roles it played (store.Role* bits).
type AddrHit struct {
	TxNum  uint64
	Height uint64
	Hash   common.Hash
	Roles  byte
}

// SearchByAddress walks an address's transaction history, KEYSET-paged over
// the posting rows exactly as the Otterscan methods are: cursor is the TxNum to
// continue from (inclusive), 0 meaning "from the end this walk starts at", and
// more reports whether a further page exists, read one row past the page so it
// is exact rather than guessed.
func (s *Server) SearchByAddress(addr common.Address, cursor uint64, limit int, desc bool) (hits []AddrHit, more bool, err error) {
	head, ok := s.otsHeadTx()
	if !ok || limit <= 0 {
		return nil, false, nil
	}
	lo, hi := uint64(0), head
	if desc {
		if cursor > 0 && cursor < hi {
			hi = cursor
		}
	} else {
		lo = cursor
	}
	nums, more, rerr := s.otsWalk(addr, lo, hi, limit, desc)
	if rerr != nil {
		return nil, false, rerr.error()
	}
	// The role bits are the posting VALUE, so they come from a second pass over
	// the same rows rather than from otsWalk, whose shape the ots_ methods own.
	roles := make(map[uint64]byte, len(nums))
	if len(nums) > 0 {
		first, last := nums[0], nums[len(nums)-1]
		if first > last {
			first, last = last, first
		}
		if err := s.db.Postings(store.AddrPrefix(addr[:]), first, last, func(n uint64, v byte) bool {
			roles[n] = v
			return true
		}); err != nil {
			return nil, false, err
		}
	}
	for _, num := range nums {
		height, ok, err := s.db.HeightOfTx(num)
		if err != nil {
			return nil, false, err
		}
		if !ok {
			return nil, false, errors.New("TxNum is in no block")
		}
		tx, rerr := s.txAt(num)
		if rerr != nil {
			return nil, false, rerr.error()
		}
		hits = append(hits, AddrHit{TxNum: num, Height: height, Hash: tx.Hash(), Roles: roles[num]})
	}
	return hits, more, nil
}

// HeaderAt returns the header at height n, decoded from its stored bytes.
func (s *Server) HeaderAt(n uint64) (*types.Header, error) {
	h, rerr := s.headerAt(n)
	return h, rerr.error()
}

// HeadPollInterval is how often an adapter should look for a new head. The
// WebSocket subscriptions poll on it too, so every push surface here shares
// one number (see wsPollInterval for why polling).
const HeadPollInterval = wsPollInterval

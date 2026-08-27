package rpc

import (
	"fmt"
	"sort"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/crypto"

	"github.com/containerman17/avalanche-s3-archival-node/store"
)

// THE POSTING-LIST LOG READS: the questions eth_getLogs cannot answer in one
// bounded call (a wallet's token transfers over all of history, a contract's
// events, the tokens a wallet ever touched). They are keyset-paged over TxNum
// exactly like SearchByAddress: cursor is the TxNum to continue from
// (inclusive), 0 meaning the end the walk starts at, and the page is cut on
// TRANSACTIONS (every log of a tx is returned, so a page is never torn inside
// one). The store stays generic; ERC-20/721/1155 are the read-time filters
// below over (topic0, position).

// PagedLogsMaxPage caps one page in transactions, as otsMaxPageSize does.
const PagedLogsMaxPage = 1000

// PagedLogs is one page of logs: More is exact (the walk read one tx past the
// page), NextCursor continues the walk in its direction.
type PagedLogs struct {
	Logs       []*types.Log
	More       bool
	NextCursor uint64
}

// TopicGroup is one (topic0, emitter) a topic value has ever stood in, with
// its TxNum span: the "tokens a wallet ever touched" answer.
type TopicGroup struct {
	Topic0  common.Hash
	Emitter common.Address
	First   uint64
	Last    uint64
}

// The three token standards' event signatures.
var (
	SigTransfer       = crypto.Keccak256Hash([]byte("Transfer(address,address,uint256)"))
	SigTransferSingle = crypto.Keccak256Hash([]byte("TransferSingle(address,address,address,uint256,uint256)"))
	SigTransferBatch  = crypto.Keccak256Hash([]byte("TransferBatch(address,address,address,uint256[],uint256[])"))
)

// LogsByEmitter is the logs emitter emitted, optionally only with topic0.
func (s *Server) LogsByEmitter(emitter common.Address, topic0 *common.Hash, cursor uint64, limit int, desc bool) (*PagedLogs, error) {
	prefix := store.ELogPrefix(emitter[:])
	if topic0 != nil {
		prefix = store.ELogGroup(emitter[:], topic0[:])
	}
	return s.pagedLogs(prefix, 0, cursor, limit, desc, func(l *types.Log) bool {
		return l.Address == emitter && (topic0 == nil || (len(l.Topics) > 0 && l.Topics[0] == *topic0))
	})
}

// LogsByTopicValue is the logs where value stood at one of the indexed topic
// positions in positions (store.Pos1|Pos2|Pos3; 0 means any), optionally only
// under topic0.
func (s *Server) LogsByTopicValue(value common.Hash, topic0 *common.Hash, positions byte, cursor uint64, limit int, desc bool) (*PagedLogs, error) {
	prefix := store.TValPrefix(value[:])
	if topic0 != nil {
		prefix = store.TValGroup(value[:], topic0[:])
	}
	return s.pagedLogs(prefix, positions, cursor, limit, desc, func(l *types.Log) bool {
		return (topic0 == nil || (len(l.Topics) > 0 && l.Topics[0] == *topic0)) && topicAt(l, value, positions)
	})
}

// TopicGroups lists every (topic0, emitter) value has stood under at topics
// 1..3, optionally only under topic0. The emitter is not in the tval/ key,
// so each run's matching logs are read once; the cost is the value's own
// posting count, the same as paging its whole history.
func (s *Server) TopicGroups(value common.Hash, topic0 *common.Hash) ([]TopicGroup, error) {
	prefix := store.TValPrefix(value[:])
	if topic0 != nil {
		prefix = store.TValGroup(value[:], topic0[:])
	}
	type key struct {
		t0 common.Hash
		em common.Address
	}
	groups := map[key]*TopicGroup{}
	var fail error
	err := s.db.Postings(prefix, 0, ^uint64(0), func(txnum uint64, _ byte) bool {
		logs, err := s.txLogsBare(txnum)
		if err != nil {
			fail = err
			return false
		}
		for _, l := range logs {
			if len(l.Topics) == 0 || !topicAt(l, value, 0) {
				continue
			}
			k := key{l.Topics[0], l.Address}
			g := groups[k]
			if g == nil {
				g = &TopicGroup{Topic0: l.Topics[0], Emitter: l.Address, First: txnum}
				groups[k] = g
			}
			g.Last = txnum
		}
		return true
	})
	if err == nil {
		err = fail
	}
	if err != nil {
		return nil, err
	}
	out := make([]TopicGroup, 0, len(groups))
	for _, g := range groups {
		out = append(out, *g)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].First < out[j].First })
	return out, nil
}

// --- the ERC shortcuts: fixed (topic0, positions) over the reads above -----

// tokenStandard is what a standard name means in the generic reads: its
// signatures, where the holder stands, and how many topics its event carries
// (Transfer with 3 topics is ERC-20, with 4 it is ERC-721).
type tokenStandard struct {
	sigs      []common.Hash
	positions byte
	topics    int
}

var standards = map[string]tokenStandard{
	"erc20":   {[]common.Hash{SigTransfer}, store.Pos1 | store.Pos2, 3},
	"erc721":  {[]common.Hash{SigTransfer}, store.Pos1 | store.Pos2, 4},
	"erc1155": {[]common.Hash{SigTransferSingle, SigTransferBatch}, store.Pos2 | store.Pos3, 4},
}

func standardOf(name string) (tokenStandard, error) {
	st, ok := standards[name]
	if !ok {
		return st, fmt.Errorf("unknown token standard %q (erc20, erc721, erc1155)", name)
	}
	return st, nil
}

func (st tokenStandard) matches(l *types.Log) bool {
	if len(l.Topics) != st.topics {
		return false
	}
	for _, sig := range st.sigs {
		if l.Topics[0] == sig {
			return true
		}
	}
	return false
}

// TokenTransfersByHolder is the standard's transfer events where holder is
// the sender or the receiver: = LogsByTopicValue(holder, sig, positions).
func (s *Server) TokenTransfersByHolder(holder common.Address, standard string, cursor uint64, limit int, desc bool) (*PagedLogs, error) {
	st, err := standardOf(standard)
	if err != nil {
		return nil, err
	}
	value := common.BytesToHash(holder[:])
	prefix := store.TValGroup(value[:], st.sigs[0][:])
	if len(st.sigs) > 1 {
		prefix = store.TValPrefix(value[:]) // both 1155 signatures; the filter picks
	}
	return s.pagedLogs(prefix, st.positions, cursor, limit, desc, func(l *types.Log) bool {
		return st.matches(l) && topicAt(l, value, st.positions)
	})
}

// TokenTransfersByContract is every transfer event token emitted:
// = LogsByEmitter(token, sig).
func (s *Server) TokenTransfersByContract(token common.Address, standard string, cursor uint64, limit int, desc bool) (*PagedLogs, error) {
	st, err := standardOf(standard)
	if err != nil {
		return nil, err
	}
	prefix := store.ELogGroup(token[:], st.sigs[0][:])
	if len(st.sigs) > 1 {
		prefix = store.ELogPrefix(token[:])
	}
	return s.pagedLogs(prefix, 0, cursor, limit, desc, func(l *types.Log) bool {
		return l.Address == token && st.matches(l)
	})
}

// TokenContract is one token a holder ever moved, with its TxNum span.
type TokenContract struct {
	Standard string
	Token    common.Address
	First    uint64
	Last     uint64
}

// TokenContracts is every token contract holder ever sent or received under
// the three standards: = TopicGroups(holder) filtered to the transfer
// signatures. Whether anything is still held is a balanceOf/ownerOf call.
func (s *Server) TokenContracts(holder common.Address) ([]TokenContract, error) {
	value := common.BytesToHash(holder[:])
	var out []TokenContract
	seen := map[TokenContract]int{}
	for _, name := range []string{"erc20", "erc721", "erc1155"} {
		st := standards[name]
		for _, sig := range st.sigs {
			sig := sig
			prefix := store.TValGroup(value[:], sig[:])
			var fail error
			err := s.db.Postings(prefix, 0, ^uint64(0), func(txnum uint64, pos byte) bool {
				if pos&st.positions == 0 {
					return true
				}
				logs, err := s.txLogsBare(txnum)
				if err != nil {
					fail = err
					return false
				}
				for _, l := range logs {
					if !st.matches(l) || l.Topics[0] != sig || !topicAt(l, value, st.positions) {
						continue
					}
					k := TokenContract{Standard: name, Token: l.Address}
					if i, ok := seen[k]; ok {
						out[i].Last = txnum
						continue
					}
					seen[k] = len(out)
					k.First, k.Last = txnum, txnum
					out = append(out, k)
				}
				return true
			})
			if err == nil {
				err = fail
			}
			if err != nil {
				return nil, err
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].First < out[j].First })
	return out, nil
}

// txLogsBare is one transaction's logs as (emitter, topics, data) only: ONE
// point read of its rcpt/ row, no block resolution and no addressing. It is
// what the full-history reads (TopicGroups, TokenContracts) walk with; a
// wallet with a million postings costs a million sequential row reads, not
// a million block reconstructions.
func (s *Server) txLogsBare(txnum uint64) ([]*types.Log, error) {
	rec, ok, err := s.db.Receipt(txnum)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("no receipt at TxNum %d", txnum)
	}
	_, _, _, stored, err := store.DecodeTxReceipt(rec)
	if err != nil {
		return nil, err
	}
	out := make([]*types.Log, len(stored))
	for i, l := range stored {
		out[i] = &types.Log{Address: l.Address, Topics: l.Topics, Data: l.Data}
	}
	return out, nil
}

// --- the walk --------------------------------------------------------------

// pagedLogs walks the posting entries under prefix (payload & mask != 0 when
// mask is set), reads each tx's logs, keeps those match accepts, and cuts the
// page after limit transactions.
func (s *Server) pagedLogs(prefix []byte, mask byte, cursor uint64, limit int, desc bool, match func(*types.Log) bool) (*PagedLogs, error) {
	head, ok := s.otsHeadTx()
	if !ok {
		return &PagedLogs{Logs: []*types.Log{}}, nil
	}
	if limit <= 0 || limit > PagedLogsMaxPage {
		limit = PagedLogsMaxPage
	}
	lo, hi := uint64(0), head
	if desc {
		if cursor > 0 && cursor < hi {
			hi = cursor
		}
	} else {
		lo = cursor
	}
	out := &PagedLogs{Logs: []*types.Log{}}
	w := &txLogs{s: s}
	var last, cut uint64 // last: the last candidate seen; cut: the last tx on the page
	txs, seen := 0, false
	visit := func(txnum uint64, payload byte) bool {
		if mask != 0 && payload&mask == 0 {
			return true
		}
		if seen && txnum == last {
			return true // a second group of the same tx
		}
		seen, last = true, txnum
		logs, rerr := w.at(txnum)
		if rerr != nil {
			w.fail = rerr
			return false
		}
		var hit []*types.Log
		for _, l := range logs {
			if match(l) {
				hit = append(hit, l)
			}
		}
		if len(hit) == 0 {
			return true // a candidate the exact filter rejects is not a page row
		}
		if txs == limit {
			out.More = true // exact: this tx would be the next page's first
			return false
		}
		txs++
		cut = txnum
		out.Logs = append(out.Logs, hit...)
		return true
	}
	var err error
	if desc {
		err = s.db.PostingsDesc(prefix, lo, hi, visit)
	} else {
		err = s.db.Postings(prefix, lo, hi, visit)
	}
	if err == nil && w.fail != nil {
		err = w.fail.error()
	}
	if err != nil {
		return nil, err
	}
	if out.More {
		if desc {
			out.NextCursor = cut - 1
		} else {
			out.NextCursor = cut + 1
		}
	}
	return out, nil
}

// txLogs reads one transaction's fully addressed logs, keeping the last block
// it decoded: a walk visits a block's transactions in a row.
type txLogs struct {
	s      *Server
	first  uint64
	count  uint64
	logs   []*types.Log
	loaded bool
	fail   *rpcError
}

func (w *txLogs) at(txnum uint64) ([]*types.Log, *rpcError) {
	if !w.loaded || txnum < w.first || txnum >= w.first+w.count {
		h, ok, err := w.s.db.HeightOfTx(txnum)
		if err != nil {
			return nil, &rpcError{Code: -32000, Message: err.Error()}
		}
		if !ok {
			return nil, &rpcError{Code: -32000, Message: fmt.Sprintf("TxNum %d is in no block", txnum)}
		}
		first, count, ok, err := w.s.db.BlockTxRange(h)
		if err != nil || !ok {
			return nil, &rpcError{Code: -32000, Message: fmt.Sprintf("tx range of block %d: ok=%v err=%v", h, ok, err)}
		}
		logs, rerr := w.s.logsOfBlock(h)
		if rerr != nil {
			return nil, rerr
		}
		w.first, w.count, w.logs, w.loaded = first, uint64(count), logs, true
	}
	idx := uint(txnum - w.first)
	var out []*types.Log
	for _, l := range w.logs {
		if l.TxIndex == idx {
			out = append(out, l)
		}
	}
	return out, nil
}

// topicAt reports whether value stands at one of the topic positions in
// positions (1..3 as store.Pos bits; 0 means any of the three).
func topicAt(l *types.Log, value common.Hash, positions byte) bool {
	for i := 1; i < len(l.Topics) && i <= 3; i++ {
		if l.Topics[i] == value && (positions == 0 || positions&(1<<(i-1)) != 0) {
			return true
		}
	}
	return false
}

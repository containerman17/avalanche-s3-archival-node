package rpc

import (
	"bytes"
	"fmt"
	"math/big"
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

// TopicGroup is one (position, emitter) a topic value has ever stood in
// under a signature: the "tokens a wallet ever touched" answer.
type TopicGroup struct {
	Position byte
	Emitter  common.Address
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

// TopicGroups lists every (position, emitter) value has stood under topic0
// at topics 1..3: three set/ prefix scans, no receipt read. The signature is
// required by design (set/ is signature-first, so a value-only listing is
// not a prefix scan).
func (s *Server) TopicGroups(value, topic0 common.Hash) ([]TopicGroup, error) {
	var out []TopicGroup
	for pos := byte(1); pos <= 3; pos++ {
		emitters, err := s.setEmitters(topic0, pos, value)
		if err != nil {
			return nil, err
		}
		for _, e := range emitters {
			out = append(out, TopicGroup{Position: pos, Emitter: e})
		}
	}
	return out, nil
}

// setEmitters is the emitters under one set/ (topic0, position, value)
// prefix, in key order.
func (s *Server) setEmitters(topic0 common.Hash, pos byte, value common.Hash) ([]common.Address, error) {
	prefix := store.SetPrefix(topic0[:], pos, value[:])
	var out []common.Address
	err := s.db.SetScan(prefix, func(k []byte) bool {
		out = append(out, common.BytesToAddress(k[len(prefix):]))
		return true
	})
	return out, err
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

// TokenContract is one token a holder ever moved.
type TokenContract struct {
	Standard string
	Token    common.Address
}

// TokenContracts is every token contract holder ever sent or received under
// the three standards: set/ prefix scans over (Transfer, positions 1 and 2),
// (TransferSingle, 2 and 3) and (TransferBatch, 2 and 3), zero receipt reads.
// ERC-20 and ERC-721 share the Transfer signature and are told apart by
// supportsInterface(0x80ac58cd) at latest. Whether anything is still held is
// a balanceOf/ownerOf call. Sorted by standard, then by address.
func (s *Server) TokenContracts(holder common.Address) ([]TokenContract, error) {
	value := common.BytesToHash(holder[:])
	seen := map[TokenContract]bool{}
	var out []TokenContract
	scan := func(sig common.Hash, positions []byte, standard func(common.Address) string) error {
		for _, pos := range positions {
			emitters, err := s.setEmitters(sig, pos, value)
			if err != nil {
				return err
			}
			for _, e := range emitters {
				k := TokenContract{Standard: standard(e), Token: e}
				if !seen[k] {
					seen[k] = true
					out = append(out, k)
				}
			}
		}
		return nil
	}
	erc1155 := func(common.Address) string { return "erc1155" }
	if err := scan(SigTransfer, []byte{1, 2}, s.transferStandard); err != nil {
		return nil, err
	}
	if err := scan(SigTransferSingle, []byte{2, 3}, erc1155); err != nil {
		return nil, err
	}
	if err := scan(SigTransferBatch, []byte{2, 3}, erc1155); err != nil {
		return nil, err
	}
	rank := map[string]int{"erc20": 0, "erc721": 1, "erc1155": 2}
	sort.Slice(out, func(i, j int) bool {
		if a, b := rank[out[i].Standard], rank[out[j].Standard]; a != b {
			return a < b
		}
		return bytes.Compare(out[i].Token[:], out[j].Token[:]) < 0
	})
	return out, nil
}

// erc165SupportsERC721 is supportsInterface(0x80ac58cd): the ERC-165 selector
// 0x01ffc9a7 and the ERC-721 interface id, ABI-padded.
var erc165SupportsERC721 = append([]byte{0x01, 0xff, 0xc9, 0xa7, 0x80, 0xac, 0x58, 0xcd}, make([]byte, 28)...)

// transferStandard tells an ERC-721 emitter from an ERC-20 one by an
// eth_call of supportsInterface(0x80ac58cd) at latest, cached per address
// for the life of the process (code does not change under a contract, and a
// proxy that flips is not worth a per-call read). A failed or reverting call
// reports erc20.
func (s *Server) transferStandard(token common.Address) string {
	s.ifaceMu.Lock()
	std, ok := s.iface721[token]
	s.ifaceMu.Unlock()
	if ok {
		return std
	}
	std = "erc20"
	if n := s.head(); n > 0 {
		ret, err := s.Call(&CallMsg{To: &token, Value: new(big.Int), GasLimit: 100_000, GasPrice: new(big.Int), GasFeeCap: new(big.Int), GasTipCap: new(big.Int), Data: erc165SupportsERC721}, n)
		if err == nil && len(ret) == 32 && ret[31] == 1 && bytes.Equal(ret[:31], make([]byte, 31)) {
			std = "erc721"
		}
	}
	s.ifaceMu.Lock()
	if s.iface721 == nil {
		s.iface721 = map[common.Address]string{}
	}
	s.iface721[token] = std
	s.ifaceMu.Unlock()
	return std
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

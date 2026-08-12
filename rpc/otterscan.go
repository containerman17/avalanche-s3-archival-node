package rpc

// THE OTTERSCAN NAMESPACE, day one (DESIGN, entry points: "it forces required
// indexes", user 2026-08-12). Every method here is a read over the lookup
// families that already exist:
//
//	addr/<address>/<txnum> -> role bits   address history, sender-and-nonce,
//	                                      contract creator
//	itx/<txnum>                           internal operations
//	blk/, hdr/, tx/, rcpt/                block details and block transactions
//
// PAGINATION IS KEYSET, NEWEST-FIRST, and it is the posting order itself: a
// page is the first pageSize rows of the descending walk from the caller's
// cursor, and the next cursor is the block of the last row returned. No offset,
// no count, nothing that has to scan what it will not return.

import (
	"encoding/json"
	"fmt"
	"math/big"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/common/hexutil"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/core/vm"
	"github.com/ava-labs/libevm/rlp"

	"github.com/containerman17/epochdb/store"
)

// OtsAPILevel is the Otterscan API version this server implements.
const OtsAPILevel = 8

// otsMaxPageSize caps a page for the same reason MaxBatchSize exists: one
// request must not be able to ask for unbounded work.
const otsMaxPageSize = 100

// otsDispatch is the ots_ half of dispatch, kept beside the implementations.
// ok=false means "not an ots_ method", which lets dispatch fall through to its
// own default.
func (s *Server) otsDispatch(method string, params []json.RawMessage) (any, *rpcError, bool) {
	switch method {
	case "ots_getApiLevel":
		return OtsAPILevel, nil, true
	case "ots_searchTransactionsBefore":
		res, rerr := s.otsSearchBefore(params)
		return res, rerr, true
	case "ots_searchTransactionsAfter":
		res, rerr := s.otsSearchAfter(params)
		return res, rerr, true
	case "ots_getTransactionBySenderAndNonce":
		res, rerr := s.otsTxBySenderAndNonce(params)
		return res, rerr, true
	case "ots_getContractCreator":
		res, rerr := s.otsContractCreator(params)
		return res, rerr, true
	case "ots_getInternalOperations":
		res, rerr := s.otsInternalOperations(params)
		return res, rerr, true
	case "ots_getBlockDetails":
		res, rerr := s.otsBlockDetails(params)
		return res, rerr, true
	case "ots_getBlockTransactions":
		res, rerr := s.otsBlockTransactions(params)
		return res, rerr, true
	}
	return nil, nil, false
}

// --- parameters ---------------------------------------------------------------

func otsAddrParam(params []json.RawMessage, i int) (common.Address, *rpcError) {
	if len(params) <= i {
		return common.Address{}, errInvalid("need an address at position %d", i)
	}
	var a common.Address
	if err := json.Unmarshal(params[i], &a); err != nil {
		return common.Address{}, errInvalid("bad address: %v", err)
	}
	return a, nil
}

// otsUintParam reads a parameter Otterscan spells as a plain JSON number, and
// accepts the hex string form too.
func otsUintParam(params []json.RawMessage, i int, name string) (uint64, *rpcError) {
	if len(params) <= i || len(params[i]) == 0 || string(params[i]) == "null" {
		return 0, nil
	}
	var n uint64
	if err := json.Unmarshal(params[i], &n); err == nil {
		return n, nil
	}
	var hex string
	if err := json.Unmarshal(params[i], &hex); err != nil {
		return 0, errInvalid("bad %s: %v", name, err)
	}
	v, err := hexutil.DecodeUint64(hex)
	if err != nil {
		return 0, errInvalid("bad %s %q: %v", name, hex, err)
	}
	return v, nil
}

func otsPageSize(params []json.RawMessage, i int) (int, *rpcError) {
	n, rerr := otsUintParam(params, i, "pageSize")
	if rerr != nil {
		return 0, rerr
	}
	if n == 0 {
		return 25, nil // Otterscan's own default
	}
	if n > otsMaxPageSize {
		return 0, errInvalid("pageSize %d exceeds the limit of %d", n, otsMaxPageSize)
	}
	return int(n), nil
}

// --- address history ----------------------------------------------------------

// otsSearchResult is Otterscan's TransactionsWithReceipts.
type otsSearchResult struct {
	Txs       []*rpcTransaction `json:"txs"`
	Receipts  []map[string]any  `json:"receipts"`
	FirstPage bool              `json:"firstPage"`
	LastPage  bool              `json:"lastPage"`
}

// blockCache assembles each block at most once per request: a page of 25
// transactions usually touches far fewer blocks, and receipts are block-wide
// (logIndex runs across the block) so the whole block is the unit anyway.
type blockCache struct {
	s     *Server
	blk   map[uint64]*types.Block
	rcpts map[uint64]types.Receipts
}

func (s *Server) newBlockCache() *blockCache {
	return &blockCache{s: s, blk: map[uint64]*types.Block{}, rcpts: map[uint64]types.Receipts{}}
}

func (c *blockCache) get(n uint64) (*types.Block, types.Receipts, *rpcError) {
	if b, ok := c.blk[n]; ok {
		return b, c.rcpts[n], nil
	}
	b, rerr := c.s.blockAt(n)
	if rerr != nil {
		return nil, nil, rerr
	}
	rs, rerr := c.s.storedBlockReceipts(b)
	if rerr != nil {
		return nil, nil, rerr
	}
	c.blk[n], c.rcpts[n] = b, rs
	return b, rs, nil
}

// otsHeadTx is the highest TxNum this node can answer for.
func (s *Server) otsHeadTx() (uint64, bool) {
	next := s.db.NextTx()
	if next == 0 {
		return 0, false
	}
	return next - 1, true
}

// otsFirstTxOf is the TxNum a block starts at.
func (s *Server) otsFirstTxOf(n uint64) (uint64, *rpcError) {
	first, _, ok, err := s.db.BlockTxRange(n)
	if err != nil {
		return 0, &rpcError{Code: -32000, Message: err.Error()}
	}
	if !ok {
		return 0, errInvalid("block %d is not stored", n)
	}
	return first, nil
}

// otsSearchBefore: the address's transactions in blocks STRICTLY BELOW
// blockNum, newest first. blockNum 0 means "from the tip".
func (s *Server) otsSearchBefore(params []json.RawMessage) (any, *rpcError) {
	addr, rerr := otsAddrParam(params, 0)
	if rerr != nil {
		return nil, rerr
	}
	blockNum, rerr := otsUintParam(params, 1, "blockNumber")
	if rerr != nil {
		return nil, rerr
	}
	pageSize, rerr := otsPageSize(params, 2)
	if rerr != nil {
		return nil, rerr
	}
	head, ok := s.otsHeadTx()
	if !ok {
		return &otsSearchResult{Txs: []*rpcTransaction{}, Receipts: []map[string]any{}, FirstPage: true, LastPage: true}, nil
	}
	hiTx := head
	if blockNum > s.head() {
		blockNum = 0 // a cursor above the head means "from the tip", not "none"
	}
	if blockNum > 0 {
		first, rerr := s.otsFirstTxOf(blockNum)
		if rerr != nil {
			return nil, rerr
		}
		if first == 0 {
			return &otsSearchResult{Txs: []*rpcTransaction{}, Receipts: []map[string]any{}, LastPage: true}, nil
		}
		hiTx = first - 1
	}

	nums, more, rerr := s.otsWalk(addr, 0, hiTx, pageSize, true)
	if rerr != nil {
		return nil, rerr
	}
	res, rerr := s.otsPage(nums)
	if rerr != nil {
		return nil, rerr
	}
	res.FirstPage = blockNum == 0
	res.LastPage = !more
	return res, nil
}

// otsSearchAfter: the address's transactions in blocks STRICTLY ABOVE blockNum,
// still returned newest-first (Otterscan reverses the ascending walk).
func (s *Server) otsSearchAfter(params []json.RawMessage) (any, *rpcError) {
	addr, rerr := otsAddrParam(params, 0)
	if rerr != nil {
		return nil, rerr
	}
	blockNum, rerr := otsUintParam(params, 1, "blockNumber")
	if rerr != nil {
		return nil, rerr
	}
	pageSize, rerr := otsPageSize(params, 2)
	if rerr != nil {
		return nil, rerr
	}
	head, ok := s.otsHeadTx()
	if !ok {
		return &otsSearchResult{Txs: []*rpcTransaction{}, Receipts: []map[string]any{}, FirstPage: true, LastPage: true}, nil
	}
	var loTx uint64
	if blockNum > 0 {
		if blockNum >= s.head() {
			return &otsSearchResult{Txs: []*rpcTransaction{}, Receipts: []map[string]any{}, FirstPage: true}, nil
		}
		if loTx, rerr = s.otsFirstTxOf(blockNum + 1); rerr != nil {
			return nil, rerr
		}
	}
	nums, more, rerr := s.otsWalk(addr, loTx, head, pageSize, false)
	if rerr != nil {
		return nil, rerr
	}
	// The walk was ascending, the answer is newest-first.
	for i, j := 0, len(nums)-1; i < j; i, j = i+1, j-1 {
		nums[i], nums[j] = nums[j], nums[i]
	}
	res, rerr := s.otsPage(nums)
	if rerr != nil {
		return nil, rerr
	}
	res.FirstPage = !more
	res.LastPage = blockNum == 0
	return res, nil
}

// otsWalk collects up to pageSize posting TxNums for addr in [lo, hi], and
// reports whether the walk stopped early because the page filled (more=true) or
// because the address has nothing further (more=false). It reads ONE row past
// the page to tell those apart, which is what makes lastPage exact instead of
// a guess.
func (s *Server) otsWalk(addr common.Address, lo, hi uint64, pageSize int, desc bool) (nums []uint64, more bool, _ *rpcError) {
	if hi < lo {
		return nil, false, nil
	}
	prefix := store.AddrPrefix(addr[:])
	collect := func(n uint64, _ byte) bool {
		if len(nums) == pageSize {
			more = true
			return false
		}
		nums = append(nums, n)
		return true
	}
	var err error
	if desc {
		err = s.db.PostingsDesc(prefix, lo, hi, collect)
	} else {
		err = s.db.Postings(prefix, lo, hi, collect)
	}
	if err != nil {
		return nil, false, &rpcError{Code: -32000, Message: err.Error()}
	}
	return nums, more, nil
}

// otsPage turns a list of TxNums into Otterscan's txs+receipts pair, in the
// order given.
func (s *Server) otsPage(nums []uint64) (*otsSearchResult, *rpcError) {
	out := &otsSearchResult{Txs: []*rpcTransaction{}, Receipts: []map[string]any{}}
	cache := s.newBlockCache()
	for _, num := range nums {
		height, ok, err := s.db.HeightOfTx(num)
		if err != nil {
			return nil, &rpcError{Code: -32000, Message: err.Error()}
		}
		if !ok {
			return nil, &rpcError{Code: -32000, Message: fmt.Sprintf("TxNum %d is in no block", num)}
		}
		blk, receipts, rerr := cache.get(height)
		if rerr != nil {
			return nil, rerr
		}
		first, rerr := s.otsFirstTxOf(height)
		if rerr != nil {
			return nil, rerr
		}
		i := int(num - first)
		if i < 0 || i >= len(blk.Transactions()) {
			return nil, &rpcError{Code: -32000, Message: fmt.Sprintf(
				"TxNum %d is outside block %d's transactions", num, height)}
		}
		tx := blk.Transactions()[i]
		base, rerr := s.headerBaseFee(blk.Header())
		if rerr != nil {
			return nil, rerr
		}
		rt := newRPCTransaction(tx, blk.Hash(), height, blk.Time(), uint64(i), base, s.chainCfg)
		if rt == nil {
			return nil, errBadSender(tx, height)
		}
		signer := types.MakeSigner(s.chainCfg, blk.Header().Number, blk.Header().Time)
		m := marshalReceipt(receipts[i], blk.Hash(), height, signer, tx, i)
		if m == nil {
			return nil, errBadSender(tx, height)
		}
		// Otterscan's search receipts carry the block timestamp: the UI shows a
		// time per row and has no block object to read it from.
		m["timestamp"] = hexutil.Uint64(blk.Time())
		out.Txs = append(out.Txs, rt)
		out.Receipts = append(out.Receipts, m)
	}
	return out, nil
}

// --- sender and nonce ---------------------------------------------------------

// otsTxBySenderAndNonce answers "which transaction did this account send with
// this nonce". The sender's postings are already in TxNum order and an
// account's nonce only ever increases, so the walk stops at the first row whose
// nonce reaches the target.
//
// ponytail: linear over the sender's history; the fix if it ever matters is a
// sender/nonce lookup family (a new key prefix plus a reindex), not a cache.
func (s *Server) otsTxBySenderAndNonce(params []json.RawMessage) (any, *rpcError) {
	sender, rerr := otsAddrParam(params, 0)
	if rerr != nil {
		return nil, rerr
	}
	nonce, rerr := otsUintParam(params, 1, "nonce")
	if rerr != nil {
		return nil, rerr
	}
	head, ok := s.otsHeadTx()
	if !ok {
		return nil, nil
	}
	var (
		found *common.Hash
		fail  *rpcError
	)
	err := s.db.Postings(store.AddrPrefix(sender[:]), 0, head, func(num uint64, role byte) bool {
		if role&store.RoleSender == 0 {
			return true
		}
		tx, rerr := s.txAt(num)
		if rerr != nil {
			fail = rerr
			return false
		}
		switch {
		case tx.Nonce() < nonce:
			return true
		case tx.Nonce() == nonce:
			h := tx.Hash()
			found = &h
		}
		return false // at or past the target: nonces never come back down
	})
	if err != nil {
		return nil, &rpcError{Code: -32000, Message: err.Error()}
	}
	if fail != nil {
		return nil, fail
	}
	if found == nil {
		return nil, nil
	}
	return found, nil
}

// txAt decodes one transaction by TxNum without assembling its block.
func (s *Server) txAt(num uint64) (*types.Transaction, *rpcError) {
	raw, ok, err := s.db.TxRLP(num)
	if err != nil {
		return nil, &rpcError{Code: -32000, Message: err.Error()}
	}
	if !ok {
		return nil, &rpcError{Code: -32000, Message: fmt.Sprintf("tx/%d is not stored", num)}
	}
	// The row is the container's own form (an RLP string around the typed
	// bytes); rlp.DecodeBytes reads that and the bare shape both.
	tx := new(types.Transaction)
	if err := rlp.DecodeBytes(raw, tx); err != nil {
		return nil, &rpcError{Code: -32000, Message: fmt.Sprintf("decode tx/%d: %v", num, err)}
	}
	return tx, nil
}

// --- contract creator ---------------------------------------------------------

type otsCreator struct {
	Hash    common.Hash    `json:"hash"`
	Creator common.Address `json:"creator"`
}

// otsContractCreator finds the transaction that deployed a contract, and the
// account that sent it. Two shapes, both answered from rows already stored:
//
//   - a TOP-LEVEL creation carries RoleCreated on the address's first posting;
//   - a FACTORY deployment appears as a CREATE/CREATE2 frame in the itx/ row of
//     the transaction, and the address's first posting is that transaction
//     (a contract cannot be touched before it exists).
//
// A plain account answers null, and that check runs FIRST so an EOA costs one
// state read rather than a walk.
func (s *Server) otsContractCreator(params []json.RawMessage) (any, *rpcError) {
	addr, rerr := otsAddrParam(params, 0)
	if rerr != nil {
		return nil, rerr
	}
	head, ok := s.otsHeadTx()
	if !ok {
		return nil, nil
	}
	st, rerr := s.stateAt(s.head())
	if rerr != nil {
		return nil, rerr
	}
	code := st.GetCode(addr)
	if err := st.Error(); err != nil {
		return nil, &rpcError{Code: -32000, Message: err.Error()}
	}
	if len(code) == 0 {
		return nil, nil // not a contract at the head: nothing to attribute
	}

	var (
		out  *otsCreator
		fail *rpcError
	)
	err := s.db.Postings(store.AddrPrefix(addr[:]), 0, head, func(num uint64, role byte) bool {
		created := role&store.RoleCreated != 0
		if !created {
			// A factory deployment: the CREATE frame names the address.
			rec, ok, err := s.db.Frames(num)
			if err != nil {
				fail = &rpcError{Code: -32000, Message: err.Error()}
				return false
			}
			if !ok {
				return true
			}
			frames, err := store.DecodeFrames(rec)
			if err != nil {
				fail = &rpcError{Code: -32000, Message: fmt.Sprintf("decode itx/%d: %v", num, err)}
				return false
			}
			for _, f := range frames {
				if (f.Kind == byte(vm.CREATE) || f.Kind == byte(vm.CREATE2)) && f.To == addr && !f.Failed {
					created = true
					break
				}
			}
		}
		if !created {
			return true
		}
		tx, rerr := s.txAt(num)
		if rerr != nil {
			fail = rerr
			return false
		}
		height, _, err := s.db.HeightOfTx(num)
		if err != nil {
			fail = &rpcError{Code: -32000, Message: err.Error()}
			return false
		}
		hdr, rerr := s.execHeader(height)
		if rerr != nil {
			fail = rerr
			return false
		}
		from, err := types.Sender(types.MakeSigner(s.chainCfg, hdr.Number, hdr.Time), tx)
		if err != nil {
			fail = errBadSender(tx, height)
			return false
		}
		out = &otsCreator{Hash: tx.Hash(), Creator: from}
		return false
	})
	if err != nil {
		return nil, &rpcError{Code: -32000, Message: err.Error()}
	}
	if fail != nil {
		return nil, fail
	}
	if out == nil {
		return nil, nil
	}
	return out, nil
}

// --- internal operations ------------------------------------------------------

// Otterscan's operation kinds.
const (
	otsOpTransfer     = 0
	otsOpSelfDestruct = 1
	otsOpCreate       = 2
	otsOpCreate2      = 3
)

type otsInternalOp struct {
	Type  int            `json:"type"`
	From  common.Address `json:"from"`
	To    common.Address `json:"to"`
	Value *hexutil.Big   `json:"value"`
}

// otsInternalOperations reads the transaction's STORED call frames: no
// re-execution, because frames are the one thing that cannot be rebuilt by IO
// and are therefore captured at execution.
//
// THE TRANSFER FILTER IS AT READ TIME, exactly as DESIGN says: a DELEGATECALL
// frame carries the PARENT's value by capture, so only CALL frames report a
// transfer. SELF_DESTRUCT (kind 1) never appears: the tracer pin here has no
// SELFDESTRUCT hook, which is a known gap of the capture and not of this read.
func (s *Server) otsInternalOperations(params []json.RawMessage) (any, *rpcError) {
	hash, rerr := txHashParam(params)
	if rerr != nil {
		return nil, rerr
	}
	num, ok, err := s.db.TxNumByHash(hash[:])
	if err != nil {
		return nil, &rpcError{Code: -32000, Message: err.Error()}
	}
	if !ok {
		return nil, nil
	}
	rec, ok, err := s.db.Frames(num)
	if err != nil {
		return nil, &rpcError{Code: -32000, Message: err.Error()}
	}
	if !ok {
		return nil, &rpcError{Code: -32000, Message: fmt.Sprintf(
			"transaction %s has no stored call frames: this node never executed it", hash)}
	}
	frames, err := store.DecodeFrames(rec)
	if err != nil {
		return nil, &rpcError{Code: -32000, Message: fmt.Sprintf("decode itx/%d: %v", num, err)}
	}
	out := []otsInternalOp{}
	for _, f := range frames {
		op := otsInternalOp{From: f.From, To: f.To, Value: (*hexutil.Big)(f.Value)}
		switch {
		case f.Kind == byte(vm.CREATE):
			op.Type = otsOpCreate
		case f.Kind == byte(vm.CREATE2):
			op.Type = otsOpCreate2
		case f.Kind == byte(vm.CALL) && f.Value != nil && f.Value.Sign() > 0:
			op.Type = otsOpTransfer
		default:
			continue
		}
		if op.Value == nil {
			op.Value = (*hexutil.Big)(common.Big0)
		}
		out = append(out, op)
	}
	return out, nil
}

// --- block details ------------------------------------------------------------

type otsIssuance struct {
	BlockReward *hexutil.Big `json:"blockReward"`
	UncleReward *hexutil.Big `json:"uncleReward"`
	Issuance    *hexutil.Big `json:"issuance"`
}

// otsBlockDetails is the block WITHOUT its transaction list, plus the two
// numbers a block page shows: issuance and total fees.
//
// ISSUANCE IS ZERO ON THESE CHAINS AND THAT IS A FACT, NOT A STUB: Avalanche
// pays no block reward and BURNS the base fee, so nothing is issued at a block.
func (s *Server) otsBlockDetails(params []json.RawMessage) (any, *rpcError) {
	blk, rerr := s.blockParam(params)
	if rerr != nil {
		return nil, rerr
	}
	fields, rerr := s.marshalBlock(blk, false)
	if rerr != nil {
		return nil, rerr
	}
	delete(fields, "transactions")
	fields["transactionCount"] = len(blk.Transactions())

	total := new(big.Int)
	if len(blk.Transactions()) > 0 {
		receipts, rerr := s.storedBlockReceipts(blk)
		if rerr != nil {
			return nil, rerr
		}
		for _, r := range receipts {
			total.Add(total, new(big.Int).Mul(r.EffectiveGasPrice, new(big.Int).SetUint64(r.GasUsed)))
		}
	}
	zero := (*hexutil.Big)(common.Big0)
	return map[string]any{
		"block":     fields,
		"issuance":  otsIssuance{BlockReward: zero, UncleReward: zero, Issuance: zero},
		"totalFees": (*hexutil.Big)(total),
	}, nil
}

// otsBlockTransactions is one page of a block's transactions, in block order,
// with their receipts.
func (s *Server) otsBlockTransactions(params []json.RawMessage) (any, *rpcError) {
	blk, rerr := s.blockParam(params)
	if rerr != nil {
		return nil, rerr
	}
	pageNumber, rerr := otsUintParam(params, 1, "pageNumber")
	if rerr != nil {
		return nil, rerr
	}
	pageSize, rerr := otsPageSize(params, 2)
	if rerr != nil {
		return nil, rerr
	}
	txs := blk.Transactions()
	n := blk.NumberU64()
	lo := min(pageNumber*uint64(pageSize), uint64(len(txs)))
	hi := min(lo+uint64(pageSize), uint64(len(txs)))

	fields, rerr := s.marshalBlock(blk, false)
	if rerr != nil {
		return nil, rerr
	}
	fields["transactionCount"] = len(txs)
	base, rerr := s.headerBaseFee(blk.Header())
	if rerr != nil {
		return nil, rerr
	}
	page := make([]*rpcTransaction, 0, hi-lo)
	out := make([]map[string]any, 0, hi-lo)
	if hi > lo {
		receipts, rerr := s.storedBlockReceipts(blk)
		if rerr != nil {
			return nil, rerr
		}
		signer := types.MakeSigner(s.chainCfg, blk.Header().Number, blk.Header().Time)
		for i := lo; i < hi; i++ {
			tx := txs[i]
			rt := newRPCTransaction(tx, blk.Hash(), n, blk.Time(), i, base, s.chainCfg)
			if rt == nil {
				return nil, errBadSender(tx, n)
			}
			m := marshalReceipt(receipts[i], blk.Hash(), n, signer, tx, int(i))
			if m == nil {
				return nil, errBadSender(tx, n)
			}
			page = append(page, rt)
			out = append(out, m)
		}
	}
	fields["transactions"] = page
	return map[string]any{"fullblock": fields, "receipts": out}, nil
}

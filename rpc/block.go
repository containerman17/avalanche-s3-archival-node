package rpc

import (
	"encoding/json"
	"fmt"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/common/hexutil"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/rlp"
)

// blockByNumber assembles the block at height n out of storage v0: the header
// row plus the block's own tx rows. Uncles are always empty on these chains,
// which is what makes this an assembly and not a container decode. Nothing is
// cached: the rows are the cache.
func (s *Server) blockByNumber(n uint64) (*types.Block, error) {
	headerRLP, ok, err := s.db.HeaderRLP(n)
	if err != nil {
		return nil, fmt.Errorf("read header %d: %w", n, err)
	}
	if !ok {
		return nil, fmt.Errorf("block %d is not stored", n)
	}
	var header types.Header
	if err := rlp.DecodeBytes(headerRLP, &header); err != nil {
		return nil, fmt.Errorf("decode header %d: %w", n, err)
	}
	txs, err := s.blockTxs(n)
	if err != nil {
		return nil, err
	}
	return types.NewBlockWithHeader(&header).WithBody(types.Body{Transactions: txs}), nil
}

// blockTxs decodes a block's transactions from their tx/ rows, in block order.
func (s *Server) blockTxs(n uint64) (types.Transactions, error) {
	first, count, ok, err := s.db.BlockTxRange(n)
	if err != nil {
		return nil, fmt.Errorf("read tx range of block %d: %w", n, err)
	}
	if !ok {
		return nil, fmt.Errorf("block %d is not stored", n)
	}
	txs := make(types.Transactions, count)
	for i := range txs {
		raw, ok, err := s.db.TxRLP(first + uint64(i))
		if err != nil {
			return nil, fmt.Errorf("read tx %d: %w", first+uint64(i), err)
		}
		if !ok {
			return nil, fmt.Errorf("tx %d of block %d is not stored", i, n)
		}
		tx := new(types.Transaction)
		if err := tx.UnmarshalBinary(raw); err != nil {
			return nil, fmt.Errorf("decode tx %d of block %d: %w", i, n, err)
		}
		txs[i] = tx
	}
	return txs, nil
}

// blockAt is blockByNumber with the JSON-RPC error shape.
func (s *Server) blockAt(n uint64) (*types.Block, *rpcError) {
	blk, err := s.blockByNumber(n)
	if err != nil {
		return nil, &rpcError{Code: -32000, Message: err.Error()}
	}
	return blk, nil
}

// marshalBlock duplicates coreth internal/ethapi RPCMarshalHeader/Block
// (that package is not importable) so responses byte-match the public API.
//
// It ERRORS when fullTx is set and a transaction's sender does not recover.
// Marshalling the nil newRPCTransaction hands back would put `null` in the
// transactions array, which a client cannot tell from a protocol quirk, and
// the container is genuinely unusable: the same failure is an error on every
// single-transaction path already.
func (s *Server) marshalBlock(blk *types.Block, fullTx bool) (map[string]any, *rpcError) {
	head := blk.Header()
	fields := map[string]any{
		"number":           (*hexutil.Big)(head.Number),
		"hash":             head.Hash(),
		"parentHash":       head.ParentHash,
		"nonce":            head.Nonce,
		"mixHash":          head.MixDigest,
		"sha3Uncles":       head.UncleHash,
		"logsBloom":        head.Bloom,
		"stateRoot":        head.Root,
		"miner":            head.Coinbase,
		"difficulty":       (*hexutil.Big)(head.Difficulty),
		"extraData":        hexutil.Bytes(head.Extra),
		"gasLimit":         hexutil.Uint64(head.GasLimit),
		"gasUsed":          hexutil.Uint64(head.GasUsed),
		"timestamp":        hexutil.Uint64(head.Time),
		"transactionsRoot": head.TxHash,
		"receiptsRoot":     head.ReceiptHash,
		"size":             hexutil.Uint64(blk.Size()),
		// totalDifficulty is VM-kind dependent and set in
		// addHeaderExtraFields: subnet-evm reports the height (difficulty is
		// always 1), coreth reports 0.
		"uncles": []common.Hash{},
	}
	// NOT head.BaseFee: above the Helicon boundary the stored header carries
	// none and the value comes from the ACP-194 gas clock (saefee.go).
	base, rerr := s.headerBaseFee(head)
	if rerr != nil {
		return nil, rerr
	}
	if base != nil {
		fields["baseFeePerGas"] = (*hexutil.Big)(base)
	}
	// The optional Cancun header fields, emitted exactly as libevm's
	// RPCMarshalHeader does (present iff non-nil). Both Avalanche VMs set them
	// on modern headers; omitting them made every eth_getBlockBy* answer differ
	// from the reference RPC above the activation height.
	if head.BlobGasUsed != nil {
		fields["blobGasUsed"] = hexutil.Uint64(*head.BlobGasUsed)
	}
	if head.ExcessBlobGas != nil {
		fields["excessBlobGas"] = hexutil.Uint64(*head.ExcessBlobGas)
	}
	if head.ParentBeaconRoot != nil {
		fields["parentBeaconBlockRoot"] = head.ParentBeaconRoot
	}
	addHeaderExtraFields(fields, blk)
	txs := blk.Transactions()
	transactions := make([]any, len(txs))
	for i, tx := range txs {
		if fullTx {
			rt := newRPCTransaction(tx, blk.Hash(), blk.NumberU64(), blk.Time(),
				uint64(i), base, s.chainCfg)
			if rt == nil {
				return nil, errBadSender(tx, blk.NumberU64())
			}
			transactions[i] = rt
		} else {
			transactions[i] = tx.Hash()
		}
	}
	fields["transactions"] = transactions
	return fields, nil
}

// marshalHeader is marshalBlock's header-only shape, shared by
// eth_getHeaderBy* and the newHeads subscription. A header has no sender in
// it, so the sender branch below cannot fire; the base fee still can, because
// above the Helicon boundary it is derived and the derivation reads the
// corpus.
func (s *Server) marshalHeader(blk *types.Block) (map[string]any, *rpcError) {
	m, rerr := s.marshalBlock(blk, false)
	if rerr != nil {
		return nil, rerr
	}
	delete(m, "transactions")
	delete(m, "uncles")
	delete(m, "size")
	return m, nil
}

// fullTxFlag reads the optional fullTx parameter. A MALFORMED one is an
// error: swallowing it meant false, so a caller that asked for full
// transactions silently got hashes.
func fullTxFlag(params []json.RawMessage, idx int) (bool, *rpcError) {
	if len(params) <= idx {
		return false, nil
	}
	var b bool
	if err := json.Unmarshal(params[idx], &b); err != nil {
		return false, errInvalid("bad fullTx flag: %v", err)
	}
	return b, nil
}

func (s *Server) getBlockByNumber(params []json.RawMessage) (any, *rpcError) {
	if len(params) < 1 {
		return nil, errInvalid("need [blockTag, fullTx]")
	}
	n, rerr := s.blockNumber(params[0])
	if rerr != nil {
		return nil, rerr
	}
	// SAE label: `pending` is the last ACCEPTED container, which the tail
	// serves even though the executor has not reached it yet. Every other
	// caller of blockNumber wants pending == latest (state semantics).
	if s.live != nil && isTag(params[0], "pending") {
		n = s.acceptedHead()
	}
	full, rerr := fullTxFlag(params, 1)
	if rerr != nil {
		return nil, rerr
	}
	blk, rerr := s.blockAt(n)
	if rerr != nil {
		return nil, rerr
	}
	return s.marshalBlock(blk, full)
}

// isTag reports whether a raw block-tag param is exactly the given tag.
func isTag(raw json.RawMessage, tag string) bool {
	var s string
	return json.Unmarshal(raw, &s) == nil && s == tag
}

func (s *Server) blockTxCount(params []json.RawMessage) (any, *rpcError) {
	blk, rerr := s.blockParam(params)
	if rerr != nil {
		return nil, rerr
	}
	return hexutil.Uint(len(blk.Transactions())), nil
}

func (s *Server) txByBlockAndIndex(params []json.RawMessage) (any, *rpcError) {
	blk, rerr := s.blockParam(params)
	if rerr != nil {
		return nil, rerr
	}
	if len(params) < 2 {
		return nil, errInvalid("need [block, txIndex]")
	}
	var idxHex string
	if err := json.Unmarshal(params[1], &idxHex); err != nil {
		return nil, errInvalid("bad tx index: %v", err)
	}
	i, err := hexutil.DecodeUint64(idxHex)
	if err != nil {
		return nil, errInvalid("bad tx index %q: %v", idxHex, err)
	}
	txs := blk.Transactions()
	if i >= uint64(len(txs)) {
		return nil, nil
	}
	base, rerr := s.headerBaseFee(blk.Header())
	if rerr != nil {
		return nil, rerr
	}
	out := newRPCTransaction(txs[i], blk.Hash(), blk.NumberU64(), blk.Time(), i, base, s.chainCfg)
	if out == nil {
		return nil, errBadSender(txs[i], blk.NumberU64())
	}
	return out, nil
}

// blockParam resolves params[0] as a block tag and reads that block.
func (s *Server) blockParam(params []json.RawMessage) (*types.Block, *rpcError) {
	if len(params) < 1 {
		return nil, errInvalid("need [block...]")
	}
	n, rerr := s.blockNumber(params[0])
	if rerr != nil {
		return nil, rerr
	}
	return s.blockAt(n)
}

// getBlockReceipts returns every receipt of the block from its stored per-tx
// receipt rows (coreth's public API does not expose this method; shape matches
// geth's, entries match coreth's per-tx eth_getTransactionReceipt).
func (s *Server) getBlockReceipts(params []json.RawMessage) (any, *rpcError) {
	blk, rerr := s.blockParam(params)
	if rerr != nil {
		return nil, rerr
	}
	n := blk.NumberU64()
	if n == 0 || len(blk.Transactions()) == 0 {
		return []any{}, nil
	}
	receipts, rerr := s.storedBlockReceipts(blk)
	if rerr != nil {
		return nil, rerr
	}
	header := blk.Header()
	signer := types.MakeSigner(s.chainCfg, header.Number, header.Time)
	out := make([]any, len(receipts))
	for i, r := range receipts {
		m := marshalReceipt(r, blk.Hash(), n, signer, blk.Transactions()[i], i)
		if m == nil {
			return nil, errBadSender(blk.Transactions()[i], n)
		}
		out[i] = m
	}
	return out, nil
}

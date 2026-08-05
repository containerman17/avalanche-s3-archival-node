package rpc

import (
	"encoding/json"
	"fmt"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/common/hexutil"
	"github.com/ava-labs/libevm/core/types"
)

// AddBlockHash records an accepted block in the bounded live-tail map
// (serve, sdk). Blocks below it resolve through the tx index, whose
// fingerprint space carries every block hash, so this is the ONLY block-hash
// state the process keeps and it never grows past recentBlockHashes.
func (s *Server) AddBlockHash(h common.Hash, n uint64) { s.recent.add(h, n) }

// blockAt fetches and parses the container at height n (must be <= head).
func (s *Server) blockAt(n uint64) (*types.Block, *rpcError) {
	if s.blocks == nil || s.parse == nil {
		return nil, &rpcError{Code: -32000, Message: "block source not available"}
	}
	if n > s.acceptedHead() {
		return nil, errInvalid("block %d beyond head %d", n, s.hist.Head())
	}
	raw, ok, err := s.blocks.GetByHeight(n)
	if err != nil {
		return nil, &rpcError{Code: -32000, Message: err.Error()}
	}
	if !ok {
		return nil, &rpcError{Code: -32000, Message: fmt.Sprintf("container %d missing", n)}
	}
	blk, err := s.parse(raw)
	if err != nil {
		return nil, &rpcError{Code: -32000, Message: err.Error()}
	}
	return blk, nil
}

// heightByHash resolves an eth block hash. ok=false = unknown hash (null
// result, geth-style).
func (s *Server) heightByHash(raw json.RawMessage) (uint64, bool, *rpcError) {
	var h common.Hash
	if err := json.Unmarshal(raw, &h); err != nil {
		return 0, false, errInvalid("bad block hash: %v", err)
	}
	n, ok, err := s.HeightByHash(h)
	if err != nil {
		return 0, false, &rpcError{Code: -32000, Message: err.Error()}
	}
	return n, ok, nil
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
	if head.BaseFee != nil {
		fields["baseFeePerGas"] = (*hexutil.Big)(head.BaseFee)
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
				uint64(i), blk.BaseFee(), s.chainCfg)
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
// it, so the fullTx=false call below is marshalBlock's one branch that cannot
// fail and this stays error-free.
func (s *Server) marshalHeader(blk *types.Block) map[string]any {
	m, _ := s.marshalBlock(blk, false)
	delete(m, "transactions")
	delete(m, "uncles")
	delete(m, "size")
	return m
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

func (s *Server) getBlockByHash(params []json.RawMessage) (any, *rpcError) {
	if len(params) < 1 {
		return nil, errInvalid("need [blockHash, fullTx]")
	}
	n, ok, rerr := s.heightByHash(params[0])
	if rerr != nil {
		return nil, rerr
	}
	if !ok {
		return nil, nil
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

func (s *Server) blockTxCount(params []json.RawMessage, byHash bool) (any, *rpcError) {
	blk, ok, rerr := s.blockParam(params, byHash)
	if rerr != nil || !ok {
		return nil, rerr
	}
	return hexutil.Uint(len(blk.Transactions())), nil
}

func (s *Server) txByBlockAndIndex(params []json.RawMessage, byHash bool) (any, *rpcError) {
	blk, ok, rerr := s.blockParam(params, byHash)
	if rerr != nil || !ok {
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
	out := newRPCTransaction(txs[i], blk.Hash(), blk.NumberU64(), blk.Time(), i, blk.BaseFee(), s.chainCfg)
	if out == nil {
		return nil, errBadSender(txs[i], blk.NumberU64())
	}
	return out, nil
}

// blockParam resolves params[0] as either a block tag or a block hash.
// ok=false with nil error means unknown hash (null result).
func (s *Server) blockParam(params []json.RawMessage, byHash bool) (*types.Block, bool, *rpcError) {
	if len(params) < 1 {
		return nil, false, errInvalid("need [block...]")
	}
	var n uint64
	if byHash {
		var ok bool
		var rerr *rpcError
		n, ok, rerr = s.heightByHash(params[0])
		if rerr != nil || !ok {
			return nil, false, rerr
		}
	} else {
		var rerr *rpcError
		n, rerr = s.blockNumber(params[0])
		if rerr != nil {
			return nil, false, rerr
		}
	}
	blk, rerr := s.blockAt(n)
	if rerr != nil {
		return nil, false, rerr
	}
	return blk, true, nil
}

// getBlockReceipts returns every receipt of the block from the stored
// epoch sections (coreth's public API does not expose this method; shape
// matches geth's, entries match coreth's per-tx eth_getTransactionReceipt).
func (s *Server) getBlockReceipts(params []json.RawMessage) (any, *rpcError) {
	blk, ok, rerr := s.blockParam(params, false)
	if rerr != nil || !ok {
		return nil, rerr
	}
	n := blk.NumberU64()
	if n == 0 || len(blk.Transactions()) == 0 {
		return []any{}, nil
	}
	if err := s.hist.Epochs().RequireCovered(n); err != nil {
		return nil, coverageError(err)
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

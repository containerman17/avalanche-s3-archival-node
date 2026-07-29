package rpc

import (
	"encoding/json"
	"fmt"

	ccustomtypes "github.com/ava-labs/avalanchego/graft/coreth/plugin/evm/customtypes"
	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/common/hexutil"
	"github.com/ava-labs/libevm/core/types"
)

// EnableBlockAPIs wires the eth block hash -> height map (built once from
// the staging sidecars) enabling the *ByHash methods. The block-body
// methods themselves ride on the EnableTxAPIs container source.
func (s *Server) EnableBlockAPIs(hashToHeight map[common.Hash]uint64) {
	s.hashIdx = &hashIndex{m: hashToHeight}
}

// AddBlockHash extends the block-hash index as the follower accepts blocks
// (serve --follow). No-op before EnableBlockAPIs.
func (s *Server) AddBlockHash(h common.Hash, n uint64) {
	if s.hashIdx != nil {
		s.hashIdx.add(h, n)
	}
}

// BlockHashCount reports the index size (startup/status logging).
func (s *Server) BlockHashCount() int {
	if s.hashIdx == nil {
		return 0
	}
	return s.hashIdx.len()
}

// blockAt fetches and parses the container at height n (must be <= head).
func (s *Server) blockAt(n uint64) (*types.Block, *rpcError) {
	if s.blocks == nil || s.parse == nil {
		return nil, &rpcError{Code: -32000, Message: "block source not available"}
	}
	if n > s.acceptedHead() {
		return nil, errInvalid("block %d beyond head %d", n, s.hist.Head())
	}
	// Below the floor the container is gone, not missing: name the floor like
	// block-by-hash, tx-by-hash and getLogs do.
	if floor := s.hist.Floor(); n < floor {
		return nil, &rpcError{Code: errCodePruned, Message: fmt.Sprintf(
			"block %d is pruned below block %d: this node serves history from block %d up (limited-history mode)", n, floor, floor)}
	}
	raw, ok, err := s.blocks.GetByHeight(n)
	if err != nil {
		return nil, &rpcError{Code: -32000, Message: err.Error()}
	}
	if !ok {
		// Block F itself is the asymmetric case: the base file is state AT F,
		// but F's body lived in the epoch that pruning dropped, so state reads
		// at F answer while the block does not exist here.
		if floor := s.hist.Floor(); floor > 0 && n <= floor {
			return nil, &rpcError{Code: errCodePruned, Message: fmt.Sprintf(
				"block %d is pruned: this node holds state from block %d but block bodies only above it (limited-history mode)", n, floor)}
		}
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
	if s.hashIdx == nil {
		return 0, false, &rpcError{Code: -32000, Message: "block hash index not available"}
	}
	n, ok := s.hashIdx.get(h)
	if !ok {
		// Same failure mode as tx-by-hash: the index starts at the floor, so
		// an unknown hash on a pruned node may well be a real block below it.
		if floor := s.hist.Floor(); floor > 0 {
			return 0, false, &rpcError{Code: errCodePruned, Message: fmt.Sprintf(
				"block hash not found at or above block %d; this node is pruned below %d and cannot tell an unknown hash from a pruned one (limited-history mode)",
				floor, floor)}
		}
	}
	return n, ok, nil
}

// marshalBlock duplicates coreth internal/ethapi RPCMarshalHeader/Block
// (that package is not importable) so responses byte-match the public API.
func (s *Server) marshalBlock(blk *types.Block, fullTx bool) map[string]any {
	head := blk.Header()
	headExtra := ccustomtypes.GetHeaderExtra(head)
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
		"extDataHash":      headExtra.ExtDataHash,
		"size":             hexutil.Uint64(blk.Size()),
		"blockExtraData":   hexutil.Bytes(ccustomtypes.BlockExtData(blk)),
		// Coreth difficulty is always 1: totalDifficulty == height.
		"totalDifficulty": (*hexutil.Big)(head.Number),
		"uncles":          []common.Hash{},
	}
	if head.BaseFee != nil {
		fields["baseFeePerGas"] = (*hexutil.Big)(head.BaseFee)
	}
	if headExtra.ExtDataGasUsed != nil {
		fields["extDataGasUsed"] = (*hexutil.Big)(headExtra.ExtDataGasUsed)
	}
	if headExtra.BlockGasCost != nil {
		fields["blockGasCost"] = (*hexutil.Big)(headExtra.BlockGasCost)
	}
	txs := blk.Transactions()
	transactions := make([]any, len(txs))
	for i, tx := range txs {
		if fullTx {
			transactions[i] = newRPCTransaction(tx, blk.Hash(), blk.NumberU64(), blk.Time(),
				uint64(i), blk.BaseFee(), s.chainCfg)
		} else {
			transactions[i] = tx.Hash()
		}
	}
	fields["transactions"] = transactions
	return fields
}

func fullTxFlag(params []json.RawMessage, idx int) bool {
	if len(params) <= idx {
		return false
	}
	var b bool
	json.Unmarshal(params[idx], &b)
	return b
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
	blk, rerr := s.blockAt(n)
	if rerr != nil {
		return nil, rerr
	}
	return s.marshalBlock(blk, fullTxFlag(params, 1)), nil
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
	blk, rerr := s.blockAt(n)
	if rerr != nil {
		return nil, rerr
	}
	return s.marshalBlock(blk, fullTxFlag(params, 1)), nil
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
	return newRPCTransaction(txs[i], blk.Hash(), blk.NumberU64(), blk.Time(), i, blk.BaseFee(), s.chainCfg), nil
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
		out[i] = marshalReceipt(r, blk.Hash(), n, signer, blk.Transactions()[i], i)
	}
	return out, nil
}

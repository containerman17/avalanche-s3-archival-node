package rpc

import (
	"encoding/json"
	"fmt"
	"math/big"

	corethcore "github.com/ava-labs/avalanchego/graft/coreth/core"
	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/common/hexutil"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/core/vm"
	"github.com/ava-labs/libevm/params"

	"github.com/containerman17/epochdb/state"
)

// BlockSource yields raw staging containers by height (a fetch.Reader).
type BlockSource interface {
	GetByHeight(n uint64) ([]byte, bool, error)
}

// ContainerParser decodes a raw container into an eth block (exec.ParseEthBlock).
type ContainerParser func([]byte) (*types.Block, error)

// EnableTxAPIs wires the tx-hash index, the staging reader, and the
// container parser into the server, enabling eth_getTransactionByHash and
// eth_getTransactionReceipt.
func (s *Server) EnableTxAPIs(idx *state.TxIndex, blocks BlockSource, parse ContainerParser) {
	s.txidx, s.blocks, s.parse = idx, blocks, parse
}

// findTx resolves a tx hash through the fingerprint index and verifies the
// candidates against the actual blocks. found=false is a clean "unknown tx".
func (s *Server) findTx(hash common.Hash) (blk *types.Block, txIndex int, found bool, err error) {
	for _, h := range s.txidx.Candidates(hash) {
		raw, ok, err := s.blocks.GetByHeight(h)
		if err != nil {
			return nil, 0, false, err
		}
		if !ok {
			continue // candidate block not (visibly) staged
		}
		b, err := s.parse(raw)
		if err != nil {
			return nil, 0, false, fmt.Errorf("parse container %d: %w", h, err)
		}
		for i, tx := range b.Transactions() {
			if tx.Hash() == hash {
				return b, i, true, nil
			}
		}
	}
	return nil, 0, false, nil
}

func txHashParam(params []json.RawMessage) (common.Hash, *rpcError) {
	if len(params) < 1 {
		return common.Hash{}, errInvalid("need [txHash]")
	}
	var h common.Hash
	if err := json.Unmarshal(params[0], &h); err != nil {
		return common.Hash{}, errInvalid("bad tx hash: %v", err)
	}
	return h, nil
}

func (s *Server) getTransactionByHash(params []json.RawMessage) (any, *rpcError) {
	hash, rerr := txHashParam(params)
	if rerr != nil {
		return nil, rerr
	}
	blk, i, found, err := s.findTx(hash)
	if err != nil {
		return nil, &rpcError{Code: -32000, Message: err.Error()}
	}
	if !found {
		return nil, nil // unknown tx: result null, like coreth
	}
	return newRPCTransaction(blk.Transactions()[i], blk.Hash(), blk.NumberU64(), blk.Time(),
		uint64(i), blk.BaseFee(), s.chainCfg), nil
}

func (s *Server) getTransactionReceipt(params []json.RawMessage) (any, *rpcError) {
	hash, rerr := txHashParam(params)
	if rerr != nil {
		return nil, rerr
	}
	blk, i, found, err := s.findTx(hash)
	if err != nil {
		return nil, &rpcError{Code: -32000, Message: err.Error()}
	}
	if !found {
		return nil, nil
	}
	n := blk.NumberU64()
	if n > s.hist.Head() {
		return nil, &rpcError{Code: -32000, Message: fmt.Sprintf("receipt needs state: block %d beyond replayed head %d", n, s.hist.Head())}
	}
	receipts, err := s.executeForReceipts(blk)
	if err != nil {
		return nil, &rpcError{Code: -32000, Message: fmt.Sprintf("re-execute block %d: %v", n, err)}
	}
	header := blk.Header()
	signer := types.MakeSigner(s.chainCfg, header.Number, header.Time)
	return marshalReceipt(receipts[i], blk.Hash(), n, signer, blk.Transactions()[i], i), nil
}

// executeForReceipts replays every regular tx of blk against the overlay
// state at the parent height and derives the full receipts. Atomic txs
// (extdata) execute after regular txs and cannot affect these receipts, so
// they are skipped entirely.
func (s *Server) executeForReceipts(blk *types.Block) (types.Receipts, error) {
	header := blk.Header()
	n := blk.NumberU64()
	statedb, rerr := s.stateAt(n - 1)
	if rerr != nil {
		return nil, rerr
	}
	// Precompile activations at this block boundary write state before txs
	// run (same call the verified executor makes).
	if err := corethcore.ApplyUpgrades(s.chainCfg, nil, corethcore.NewBlockContext(header.Number, header.Time), statedb); err != nil {
		return nil, fmt.Errorf("apply upgrades: %w", err)
	}
	blockCtx := corethcore.NewEVMBlockContext(header, s.chainCtx, nil)
	gp := new(corethcore.GasPool).AddGas(header.GasLimit)
	var (
		usedGas  uint64
		receipts types.Receipts
	)
	for i, tx := range blk.Transactions() {
		statedb.SetTxContext(tx.Hash(), i)
		receipt, err := corethcore.ApplyTransaction(
			s.chainCfg, s.chainCtx, blockCtx, gp, statedb,
			header, tx, &usedGas, vm.Config{},
		)
		if err != nil {
			return nil, fmt.Errorf("tx %d: %w", i, err)
		}
		receipts = append(receipts, receipt)
	}
	if err := receipts.DeriveFields(s.chainCfg, blk.Hash(), n, header.Time, header.BaseFee, nil, blk.Transactions()); err != nil {
		return nil, fmt.Errorf("derive receipt fields: %w", err)
	}
	return receipts, nil
}

// --- coreth internal/ethapi JSON shapes, duplicated verbatim so responses
// --- byte-match the public API (that package is not importable).

type rpcTransaction struct {
	BlockHash        *common.Hash      `json:"blockHash"`
	BlockNumber      *hexutil.Big      `json:"blockNumber"`
	From             common.Address    `json:"from"`
	Gas              hexutil.Uint64    `json:"gas"`
	GasPrice         *hexutil.Big      `json:"gasPrice"`
	GasFeeCap        *hexutil.Big      `json:"maxFeePerGas,omitempty"`
	GasTipCap        *hexutil.Big      `json:"maxPriorityFeePerGas,omitempty"`
	Hash             common.Hash       `json:"hash"`
	Input            hexutil.Bytes     `json:"input"`
	Nonce            hexutil.Uint64    `json:"nonce"`
	To               *common.Address   `json:"to"`
	TransactionIndex *hexutil.Uint64   `json:"transactionIndex"`
	Value            *hexutil.Big      `json:"value"`
	Type             hexutil.Uint64    `json:"type"`
	Accesses         *types.AccessList `json:"accessList,omitempty"`
	ChainID          *hexutil.Big      `json:"chainId,omitempty"`
	V                *hexutil.Big      `json:"v"`
	R                *hexutil.Big      `json:"r"`
	S                *hexutil.Big      `json:"s"`
	YParity          *hexutil.Uint64   `json:"yParity,omitempty"`
}

func effectiveGasPrice(tx *types.Transaction, baseFee *big.Int) *big.Int {
	fee := tx.GasTipCap()
	fee = fee.Add(fee, baseFee)
	if tx.GasFeeCapIntCmp(fee) < 0 {
		return tx.GasFeeCap()
	}
	return fee
}

func newRPCTransaction(tx *types.Transaction, blockHash common.Hash, blockNumber, blockTime, index uint64, baseFee *big.Int, config *params.ChainConfig) *rpcTransaction {
	signer := types.MakeSigner(config, new(big.Int).SetUint64(blockNumber), blockTime)
	from, _ := types.Sender(signer, tx)
	v, r, sv := tx.RawSignatureValues()
	result := &rpcTransaction{
		Type:     hexutil.Uint64(tx.Type()),
		From:     from,
		Gas:      hexutil.Uint64(tx.Gas()),
		GasPrice: (*hexutil.Big)(tx.GasPrice()),
		Hash:     tx.Hash(),
		Input:    hexutil.Bytes(tx.Data()),
		Nonce:    hexutil.Uint64(tx.Nonce()),
		To:       tx.To(),
		Value:    (*hexutil.Big)(tx.Value()),
		V:        (*hexutil.Big)(v),
		R:        (*hexutil.Big)(r),
		S:        (*hexutil.Big)(sv),
	}
	if blockHash != (common.Hash{}) {
		result.BlockHash = &blockHash
		result.BlockNumber = (*hexutil.Big)(new(big.Int).SetUint64(blockNumber))
		result.TransactionIndex = (*hexutil.Uint64)(&index)
	}
	switch tx.Type() {
	case types.LegacyTxType:
		// if a legacy transaction has an EIP-155 chain id, include it explicitly
		if id := tx.ChainId(); id.Sign() != 0 {
			result.ChainID = (*hexutil.Big)(id)
		}
	case types.AccessListTxType:
		al := tx.AccessList()
		yparity := hexutil.Uint64(v.Sign())
		result.Accesses = &al
		result.ChainID = (*hexutil.Big)(tx.ChainId())
		result.YParity = &yparity
	case types.DynamicFeeTxType:
		al := tx.AccessList()
		yparity := hexutil.Uint64(v.Sign())
		result.Accesses = &al
		result.ChainID = (*hexutil.Big)(tx.ChainId())
		result.YParity = &yparity
		result.GasFeeCap = (*hexutil.Big)(tx.GasFeeCap())
		result.GasTipCap = (*hexutil.Big)(tx.GasTipCap())
		// mined dynamic-fee tx: gasPrice = effective gas price
		if baseFee != nil && blockHash != (common.Hash{}) {
			result.GasPrice = (*hexutil.Big)(effectiveGasPrice(tx, baseFee))
		} else {
			result.GasPrice = (*hexutil.Big)(tx.GasFeeCap())
		}
	}
	return result
}

func marshalReceipt(receipt *types.Receipt, blockHash common.Hash, blockNumber uint64, signer types.Signer, tx *types.Transaction, txIndex int) map[string]any {
	from, _ := types.Sender(signer, tx)
	fields := map[string]any{
		"blockHash":         blockHash,
		"blockNumber":       hexutil.Uint64(blockNumber),
		"transactionHash":   tx.Hash(),
		"transactionIndex":  hexutil.Uint64(txIndex),
		"from":              from,
		"to":                tx.To(),
		"gasUsed":           hexutil.Uint64(receipt.GasUsed),
		"cumulativeGasUsed": hexutil.Uint64(receipt.CumulativeGasUsed),
		"contractAddress":   nil,
		"logs":              receipt.Logs,
		"logsBloom":         receipt.Bloom,
		"type":              hexutil.Uint(tx.Type()),
		"effectiveGasPrice": (*hexutil.Big)(receipt.EffectiveGasPrice),
	}
	if len(receipt.PostState) > 0 {
		fields["root"] = hexutil.Bytes(receipt.PostState)
	} else {
		fields["status"] = hexutil.Uint(receipt.Status)
	}
	if receipt.Logs == nil {
		fields["logs"] = []*types.Log{}
	}
	if receipt.ContractAddress != (common.Address{}) {
		fields["contractAddress"] = receipt.ContractAddress
	}
	return fields
}

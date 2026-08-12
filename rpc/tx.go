package rpc

import (
	"encoding/json"
	"fmt"
	"math/big"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/common/hexutil"
	ethstate "github.com/ava-labs/libevm/core/state"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/core/vm"
	"github.com/ava-labs/libevm/crypto"
	"github.com/ava-labs/libevm/params"

	"github.com/containerman17/epochdb/store"
)

// findTx resolves a tx hash to its block and its index in that block. It is
// ONE lookup row (txh/ -> TxNum) plus the block the TxNum falls in: there is
// no candidate walk and no bloom holder any more. found=false is a clean
// "unknown tx".
func (s *Server) findTx(hash common.Hash) (blk *types.Block, txIndex int, found bool, err error) {
	txnum, ok, err := s.db.TxNumByHash(hash[:])
	if err != nil || !ok {
		return nil, 0, false, err
	}
	n, ok, err := s.db.HeightOfTx(txnum)
	if err != nil {
		return nil, 0, false, err
	}
	if !ok {
		return nil, 0, false, fmt.Errorf("tx %s is stored at TxNum %d, which is in no block", hash, txnum)
	}
	first, count, ok, err := s.db.BlockTxRange(n)
	if err != nil {
		return nil, 0, false, err
	}
	if !ok || txnum < first || txnum >= first+uint64(count) {
		return nil, 0, false, fmt.Errorf("tx %s at TxNum %d is outside block %d's range", hash, txnum, n)
	}
	if blk, err = s.blockByNumber(n); err != nil {
		return nil, 0, false, err
	}
	return blk, int(txnum - first), true, nil
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
	base, rerr := s.headerBaseFee(blk.Header())
	if rerr != nil {
		return nil, rerr
	}
	out := newRPCTransaction(blk.Transactions()[i], blk.Hash(), blk.NumberU64(), blk.Time(),
		uint64(i), base, s.chainCfg)
	if out == nil {
		return nil, errBadSender(blk.Transactions()[i], blk.NumberU64())
	}
	return out, nil
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
	header := blk.Header()
	signer := types.MakeSigner(s.chainCfg, header.Number, header.Time)
	receipts, rerr := s.storedBlockReceipts(blk)
	if rerr != nil {
		return nil, rerr
	}
	out := marshalReceipt(receipts[i], blk.Hash(), n, signer, blk.Transactions()[i], i)
	if out == nil {
		return nil, errBadSender(blk.Transactions()[i], n)
	}
	return out, nil
}

// storedReceipt decodes the rcpt/<txnum> row of one transaction. THE ROW IS
// PER TRANSACTION in storage v0, so there is no block-level record to split.
func (s *Server) storedReceipt(txnum uint64) (status, gasUsed, cumulative uint64, logs []store.StoredLog, _ *rpcError) {
	rec, ok, err := s.db.Receipt(txnum)
	if err != nil {
		return 0, 0, 0, nil, &rpcError{Code: -32000, Message: fmt.Sprintf("read receipt of tx %d: %v", txnum, err)}
	}
	if !ok {
		return 0, 0, 0, nil, &rpcError{Code: -32000, Message: fmt.Sprintf(
			"tx %d has no stored receipt: this node never executed it", txnum)}
	}
	status, gasUsed, cumulative, logs, err = store.DecodeTxReceipt(rec)
	if err != nil {
		return 0, 0, 0, nil, &rpcError{Code: -32000, Message: fmt.Sprintf("decode receipt of tx %d: %v", txnum, err)}
	}
	return status, gasUsed, cumulative, logs, nil
}

// storedBlockReceipts reconstructs every receipt of blk from its per-tx rcpt/
// rows (the ONLY receipt source, no re-execution): status / gasUsed /
// cumulative and the full logs come out of the row, everything else is derived
// from the txs themselves.
func (s *Server) storedBlockReceipts(blk *types.Block) (types.Receipts, *rpcError) {
	n := blk.NumberU64()
	txs := blk.Transactions()
	first, count, ok, err := s.db.BlockTxRange(n)
	if err != nil {
		return nil, &rpcError{Code: -32000, Message: err.Error()}
	}
	if !ok || int(count) != len(txs) {
		return nil, &rpcError{Code: -32000, Message: fmt.Sprintf(
			"block %d holds %d transactions but its tx range covers %d (stored=%v)", n, len(txs), count, ok)}
	}
	header := blk.Header()
	base, rerr := s.headerBaseFee(header)
	if rerr != nil {
		return nil, rerr
	}
	signer := types.MakeSigner(s.chainCfg, header.Number, header.Time)
	blockHash := blk.Hash()
	receipts := make(types.Receipts, len(txs))
	logIndex := uint(0) // logIndex is block-wide, so it runs across the txs
	for i, tx := range txs {
		status, gasUsed, cumulative, logs, rerr := s.storedReceipt(first + uint64(i))
		if rerr != nil {
			return nil, rerr
		}
		r := &types.Receipt{
			Type:              tx.Type(),
			Status:            status,
			CumulativeGasUsed: cumulative,
			GasUsed:           gasUsed,
			TxHash:            tx.Hash(),
			EffectiveGasPrice: effectiveGasPrice(tx, base),
			Logs:              []*types.Log{},
		}
		if tx.To() == nil {
			// A sender that will not recover makes CreateAddress return a
			// well-formed, entirely wrong contract address.
			from, err := types.Sender(signer, tx)
			if err != nil {
				return nil, &rpcError{Code: -32000, Message: fmt.Sprintf("recover sender of tx %s in block %d: %v", tx.Hash(), n, err)}
			}
			r.ContractAddress = crypto.CreateAddress(from, tx.Nonce())
		}
		for _, sl := range logs {
			r.Logs = append(r.Logs, &types.Log{
				Address:     sl.Address,
				Topics:      sl.Topics,
				Data:        sl.Data,
				BlockNumber: n,
				TxHash:      r.TxHash,
				TxIndex:     uint(i),
				BlockHash:   blockHash,
				Index:       logIndex,
			})
			logIndex++
		}
		r.Bloom = types.CreateBloom(types.Receipts{r})
		receipts[i] = r
	}
	return receipts, nil
}

// ReExecuteBlock replays every regular tx of blk against the overlay state
// at the parent height and derives the full receipts. Atomic txs (extdata)
// execute after regular txs, emit no EVM logs, and cannot affect these
// receipts, so they are skipped entirely (matching the live capture's log
// collection). chainCtx must be safe for the caller's concurrency.
//
// baseFee, when non-nil, REPLACES the header's own for the replay, and above
// the Helicon boundary that is the only way to price it correctly: a stored
// post-Helicon header carries no base fee, and replaying against the nil it
// does carry charges every dynamic-fee sender its whole fee cap instead of
// base+tip, so the balances (and therefore any state the txs read back) drift
// from what really executed. It is the same substitution saexec.Execute makes
// on its own header copy; blk.Header() is a copy, so the block is untouched.
// nil means "the header's own", which is right everywhere below the boundary.
func ReExecuteBlock(db *store.DB, genesis types.GenesisAlloc, chainCtx ChainContext, chainCfg *params.ChainConfig, blk *types.Block, baseFee *big.Int) (types.Receipts, error) {
	header := blk.Header()
	if baseFee != nil {
		header.BaseFee = baseFee
	}
	n := blk.NumberU64()
	statedb, err := ethstate.New(common.Hash{}, db.StateAt(genesis, n-1), nil)
	if err != nil {
		return nil, err
	}
	// Precompile activations at this block boundary write state before txs
	// run (same call the verified executor makes, parent timestamp included:
	// nil would re-activate every active precompile, see exec runEVM).
	cctx := captureHeaders(chainCtx)
	parent := cctx.GetHeader(header.ParentHash, n-1)
	if cctx.err != nil {
		return nil, fmt.Errorf("parent header %d: %w", n-1, cctx.err)
	}
	if parent == nil {
		return nil, fmt.Errorf("parent header %d missing", n-1)
	}
	backend := registeredVM()
	if err := backend.applyUpgrades(chainCfg, parent.Time, header, statedb); err != nil {
		return nil, fmt.Errorf("apply upgrades: %w", err)
	}
	blockCtx := backend.blockContext(header, cctx)
	gasLeft := header.GasLimit
	var (
		usedGas  uint64
		receipts types.Receipts
	)
	for i, tx := range blk.Transactions() {
		statedb.SetTxContext(tx.Hash(), i)
		receipt, err := backend.applyTx(
			chainCfg, cctx, blockCtx, &gasLeft, statedb,
			header, tx, &usedGas, vm.Config{},
		)
		// A statedb read failure is RECORDED, not returned: geth's
		// getDeleteStateObject calls setError and hands back an empty account,
		// so a re-execution over a height whose rows are not there reads every
		// account as nonce 0 and either fails with a nonsense
		// "nonce too high ... state: 0" or, worse, succeeds against zeroed
		// state and returns wrong receipts. Surface the read error instead, so
		// this path gives the same clean refusal eth_call does.
		if dbErr := statedb.Error(); dbErr != nil {
			return nil, dbErr
		}
		if err != nil {
			return nil, fmt.Errorf("tx %d: %w", i, err)
		}
		receipts = append(receipts, receipt)
	}
	if dbErr := statedb.Error(); dbErr != nil {
		return nil, dbErr
	}
	// Same class as a failed state read, different channel: an unreadable
	// BLOCKHASH header is 0x0 in the EVM and nothing in statedb.Error().
	if cctx.err != nil {
		return nil, cctx.err
	}
	if err := receipts.DeriveFields(chainCfg, blk.Hash(), n, header.Time, header.BaseFee, nil, blk.Transactions()); err != nil {
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
	if baseFee == nil {
		return tx.GasPrice() // pre-AP3: the gas price is the whole fee
	}
	fee := tx.GasTipCap()
	fee = fee.Add(fee, baseFee)
	if tx.GasFeeCapIntCmp(fee) < 0 {
		return tx.GasFeeCap()
	}
	return fee
}

// newRPCTransaction marshals a mined tx, or NIL if the signature does not
// recover: `"from": "0x000...0"` is an authoritative-looking lie a client
// cannot detect, and a container whose signature will not recover is a real
// failure (a corrupt body, or the wrong network's chain id). Every caller
// must treat nil as an error, not as an empty transaction.
func newRPCTransaction(tx *types.Transaction, blockHash common.Hash, blockNumber, blockTime, index uint64, baseFee *big.Int, config *params.ChainConfig) *rpcTransaction {
	signer := types.MakeSigner(config, new(big.Int).SetUint64(blockNumber), blockTime)
	from, err := types.Sender(signer, tx)
	if err != nil {
		return nil
	}
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

// marshalReceipt returns NIL on an unrecoverable sender, for the reason
// newRPCTransaction does: `"from": "0x000...0"` reads as authoritative.
func marshalReceipt(receipt *types.Receipt, blockHash common.Hash, blockNumber uint64, signer types.Signer, tx *types.Transaction, txIndex int) map[string]any {
	from, err := types.Sender(signer, tx)
	if err != nil {
		return nil
	}
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

// errBadSender is what a nil from newRPCTransaction / marshalReceipt means.
func errBadSender(tx *types.Transaction, n uint64) *rpcError {
	return &rpcError{Code: -32000, Message: fmt.Sprintf(
		"sender of tx %s in block %d does not recover (corrupt container, or this node's chain id is not the one it was signed for)", tx.Hash(), n)}
}

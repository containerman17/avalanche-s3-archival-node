package vmexec

import (
	"math/big"

	sevmcore "github.com/ava-labs/avalanchego/graft/subnet-evm/core"
	"github.com/ava-labs/libevm/common"
	ethstate "github.com/ava-labs/libevm/core/state"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/core/vm"
	"github.com/ava-labs/libevm/crypto"
	"github.com/ava-labs/libevm/params"
)

// applyTx is subnet-evm's core.applyTransaction (unexported there), copied
// verbatim so that runEVM can drive it the way core.StateProcessor.Process
// does and core.ApplyTransaction does not: ONE EVM PER BLOCK (Reset per tx;
// NewEVM per tx costs two ChainConfig.Rules and a fresh interpreter) and the
// block hash from the block's cached Hash (ApplyTransaction RLP-encodes and
// keccaks the header again for every tx). On the Step profile those two
// were 2.5 s of the executor thread's 26 s.
func applyTx(msg *sevmcore.Message, config *params.ChainConfig, gp *sevmcore.GasPool, statedb *ethstate.StateDB,
	blockNumber *big.Int, blockHash common.Hash, tx *types.Transaction, usedGas *uint64, evm *vm.EVM,
) (*types.Receipt, error) {
	txContext := sevmcore.NewEVMTxContext(msg)
	evm.Reset(txContext, statedb)

	result, err := sevmcore.ApplyMessage(evm, msg, gp)
	if err != nil {
		return nil, err
	}

	var root []byte
	if config.IsByzantium(blockNumber) {
		statedb.Finalise(true)
	} else {
		root = statedb.IntermediateRoot(config.IsEIP158(blockNumber)).Bytes()
	}
	*usedGas += result.UsedGas

	receipt := &types.Receipt{Type: tx.Type(), PostState: root, CumulativeGasUsed: *usedGas}
	if result.Failed() {
		receipt.Status = types.ReceiptStatusFailed
	} else {
		receipt.Status = types.ReceiptStatusSuccessful
	}
	receipt.TxHash = tx.Hash()
	receipt.GasUsed = result.UsedGas

	if tx.Type() == types.BlobTxType {
		receipt.BlobGasUsed = uint64(len(tx.BlobHashes()) * params.BlobTxBlobGasPerBlob)
		receipt.BlobGasPrice = evm.Context.BlobBaseFee
	}

	if msg.To == nil {
		receipt.ContractAddress = crypto.CreateAddress(evm.TxContext.Origin, tx.Nonce())
	}

	receipt.Logs = statedb.GetLogs(tx.Hash(), blockNumber.Uint64(), blockHash)
	receipt.Bloom = types.CreateBloom(types.Receipts{receipt})
	receipt.BlockHash = blockHash
	receipt.BlockNumber = blockNumber
	receipt.TransactionIndex = uint(statedb.TxIndex())
	return receipt, err
}

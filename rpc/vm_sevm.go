package rpc

// The subnet-evm side of the rpc execution seam (see vm.go). Structurally the
// coreth file with subnet-evm's core package substituted, because the two
// expose the same four entry points under distinct Go types.
//
// The one place they genuinely differ is the fee floor: the C-chain's minimum
// gas price is a protocol constant, an L1's lives in ITS OWN genesis
// feeConfig. See minGasPrice.

import (
	"math/big"

	sevmconsensus "github.com/ava-labs/avalanchego/graft/subnet-evm/consensus"
	sevmdummy "github.com/ava-labs/avalanchego/graft/subnet-evm/consensus/dummy"
	sevmcore "github.com/ava-labs/avalanchego/graft/subnet-evm/core"
	sevmparams "github.com/ava-labs/avalanchego/graft/subnet-evm/params"
	ethstate "github.com/ava-labs/libevm/core/state"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/core/vm"
	"github.com/ava-labs/libevm/params"
)

// sevmRPC is any Avalanche L1 running subnet-evm.
type sevmRPC struct{}

type sevmChainCtx struct{ ChainContext }

func (sevmChainCtx) Engine() sevmconsensus.Engine { return sevmdummy.NewFullFaker() }

func (sevmRPC) applyUpgrades(cfg *params.ChainConfig, parentTime uint64, header *types.Header, statedb *ethstate.StateDB) error {
	// Covers the chain's PrecompileUpgrades AND its StateUpgrades, both of
	// which are operator-supplied upgrade.json content on an L1 (DESIGN ruling
	// 6). Same parent-timestamp rule as coreth: see the interface comment.
	return sevmcore.ApplyUpgrades(cfg, &parentTime,
		sevmcore.NewBlockContext(header.Number, header.Time), statedb)
}

func (sevmRPC) blockContext(header *types.Header, cc ChainContext) vm.BlockContext {
	// Reads the Warp predicate results straight out of header.Extra; a
	// historical replay never verifies them, so no validator set and no
	// P-chain height are needed here (DESIGN, subnet-evm scoping).
	return sevmcore.NewEVMBlockContext(header, sevmChainCtx{cc}, nil)
}

func (sevmRPC) applyTx(cfg *params.ChainConfig, cc ChainContext, blockCtx vm.BlockContext, gasLeft *uint64,
	statedb *ethstate.StateDB, header *types.Header, tx *types.Transaction,
	usedGas *uint64, vmCfg vm.Config,
) (*types.Receipt, error) {
	return sevmcore.ApplyTransaction(cfg, sevmChainCtx{cc}, blockCtx,
		(*sevmcore.GasPool)(gasLeft), statedb, header, tx, usedGas, vmCfg)
}

func (sevmRPC) applyMsg(cfg *params.ChainConfig, blockCtx vm.BlockContext, statedb *ethstate.StateDB,
	m *callMsg, vmCfg vm.Config,
) (*callResult, error) {
	msg := &sevmcore.Message{
		From:              m.From,
		To:                m.To,
		Value:             m.Value,
		GasLimit:          m.GasLimit,
		GasPrice:          m.GasPrice,
		GasFeeCap:         m.GasFeeCap,
		GasTipCap:         m.GasTipCap,
		Data:              m.Data,
		AccessList:        m.AccessList,
		SkipAccountChecks: true,
	}
	evm := vm.NewEVM(blockCtx, sevmcore.NewEVMTxContext(msg), statedb, cfg, vmCfg)
	gp := new(sevmcore.GasPool).AddGas(m.GasLimit)
	res, err := sevmcore.ApplyMessage(evm, msg, gp)
	if err != nil {
		return nil, err
	}
	return &callResult{UsedGas: res.UsedGas, ReturnData: res.ReturnData, Revert: res.Revert(), Err: res.Err}, nil
}

// minGasPrice is the chain's OWN configured minimum base fee, which is the
// honest floor on an L1: subnet-evm has no protocol-wide minimum gas price,
// every chain sets its own in genesis feeConfig, and DefaultFeeConfig is what
// exec's genesis parse falls back to when a genesis omits it. The live base
// fee itself is never derived here: it is already in header.BaseFee (that is
// what eth_baseFee and eth_feeHistory read), and the fee WINDOW in
// header.Extra, which is only needed to project the NEXT block's base fee, is
// deliberately never parsed.
func (sevmRPC) minGasPrice(cfg *params.ChainConfig) *big.Int {
	fc := sevmparams.GetExtra(cfg).FeeConfig
	if fc.MinBaseFee == nil {
		fc = sevmparams.DefaultFeeConfig
	}
	return new(big.Int).Set(fc.MinBaseFee)
}

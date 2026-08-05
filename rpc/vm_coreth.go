package rpc

// The coreth side of the rpc execution seam (see vm.go). Every call here was
// MOVED, not rewritten: the bodies are the exact corethcore calls that used to
// sit inline in server.go (ethCall), misc.go (runCall), debug.go
// (traceTxsInBlock, createAccessList) and tx.go (ReExecuteBlock), so the
// C-chain path is byte-for-byte the code it was before the seam existed.

import (
	"fmt"
	"math/big"

	"github.com/ava-labs/avalanchego/graft/coreth/consensus"
	"github.com/ava-labs/avalanchego/graft/coreth/consensus/dummy"
	corethcore "github.com/ava-labs/avalanchego/graft/coreth/core"
	cparams "github.com/ava-labs/avalanchego/graft/coreth/params"
	"github.com/ava-labs/avalanchego/graft/coreth/plugin/evm/customheader"
	ccustomtypes "github.com/ava-labs/avalanchego/graft/coreth/plugin/evm/customtypes"
	ethstate "github.com/ava-labs/libevm/core/state"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/core/vm"
	"github.com/ava-labs/libevm/params"
)

// corethRPC is the primary network's C-chain.
type corethRPC struct{}

// corethChainCtx is the server's header source wearing coreth's ChainContext:
// the only thing it adds is the consensus engine, which is exactly the method
// the two VMs cannot share.
type corethChainCtx struct{ ChainContext }

func (corethChainCtx) Engine() consensus.Engine { return dummy.NewFullFaker() }

func (corethRPC) applyUpgrades(cfg *params.ChainConfig, parentTime uint64, header *types.Header, statedb *ethstate.StateDB) error {
	return corethcore.ApplyUpgrades(cfg, &parentTime,
		corethcore.NewBlockContext(header.Number, header.Time), statedb)
}

func (corethRPC) blockContext(header *types.Header, cc ChainContext) vm.BlockContext {
	return corethcore.NewEVMBlockContext(header, corethChainCtx{cc}, nil)
}

func (corethRPC) applyTx(cfg *params.ChainConfig, cc ChainContext, blockCtx vm.BlockContext, gasLeft *uint64,
	statedb *ethstate.StateDB, header *types.Header, tx *types.Transaction,
	usedGas *uint64, vmCfg vm.Config,
) (*types.Receipt, error) {
	return corethcore.ApplyTransaction(cfg, corethChainCtx{cc}, blockCtx,
		(*corethcore.GasPool)(gasLeft), statedb, header, tx, usedGas, vmCfg)
}

func (corethRPC) applyMsg(cfg *params.ChainConfig, blockCtx vm.BlockContext, statedb *ethstate.StateDB,
	m *callMsg, vmCfg vm.Config,
) (*callResult, error) {
	msg := &corethcore.Message{
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
	evm := vm.NewEVM(blockCtx, corethcore.NewEVMTxContext(msg), statedb, cfg, vmCfg)
	gp := new(corethcore.GasPool).AddGas(m.GasLimit)
	res, err := corethcore.ApplyMessage(evm, msg, gp)
	if err != nil {
		return nil, err
	}
	return &callResult{UsedGas: res.UsedGas, ReturnData: res.ReturnData, Revert: res.Revert(), Err: res.Err}, nil
}

// corethMinGasPrice is the C-chain phase-0 minimum gas price (470 gwei),
// reported when the recent corpus blocks carry no transactions to sample.
var corethMinGasPrice = new(big.Int).Mul(big.NewInt(470), big.NewInt(params.GWei))

func (corethRPC) minGasPrice(*params.ChainConfig) *big.Int {
	return new(big.Int).Set(corethMinGasPrice)
}

// heliconTransitionLead is transitionvm's switch offset: the C-Chain registers
// TransitionTime = HeliconTime - 10s. The twin of exec/vm_coreth.go's constant
// and rule, repeated here because the two packages read it for different
// reasons and exec's copy is not exported.
const heliconTransitionLead = 10

// nextBaseFee is coreth's OWN estimator, at the parent's own timestamp (see
// the interface comment for why not wall clock). EstimateNextBaseFee clamps a
// timestamp below the parent's up to the parent's, so handing it the parent's
// is the identity that names itself.
//
// ABOVE THE HELICON BOUNDARY IT REFUSES, and the refusal is the honest answer
// rather than a conservative fallback. Post-Helicon the base fee comes from
// ACP-194's deterministic GAS CLOCK, never from a header: the block's own
// wire header carries no executed base fee at all (SAE builds the header
// before it executes), and consensus republishes the clock only at SETTLEMENT
// points, which skip most heights. So the clock lives in the EXECUTOR's ring
// (exec/saering.go) and nowhere this read server can reach, and coreth's
// estimator would silently take its Fortuna branch and read parent.Extra as
// ACP-176 state that a post-Helicon header does not carry. The rule is exec's
// verbatim: the child of a parent at or past the transition time is SAE.
func (corethRPC) nextBaseFee(cfg *params.ChainConfig, parent *types.Header) (*big.Int, error) {
	extra := cparams.GetExtra(cfg)
	if ts := extra.HeliconTimestamp; ts != nil && parent.Time+heliconTransitionLead >= *ts {
		return nil, fmt.Errorf(
			"the block after %d is post-Helicon (SAE): its base fee comes from the ACP-194 gas clock, which consensus publishes only at settlement points and which this read server does not hold",
			parent.Number)
	}
	return customheader.EstimateNextBaseFee(extra, parent, ccustomtypes.HeaderTimeMilliseconds(parent))
}

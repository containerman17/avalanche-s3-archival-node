package rpc

// THE RPC-SIDE EXECUTION SEAM (DESIGN "SUBNET-EVM SCOPING", M8).
//
// The read server executes too: eth_call, eth_estimateGas, eth_createAccessList,
// the debug tracers and the receipt/fee re-execution all run the EVM. Those
// paths called coreth's core package directly, which PANICS on a subnet-evm
// chain (the libevm extras payload on *params.ChainConfig is the other VM's
// type), so `serve --chain` could fetch and execute but not answer.
//
// This is the same seam shape exec/vm.go already uses, one layer up and
// smaller, because rpc needs a strict subset of what the executor does:
// ApplyUpgrades, NewEVMBlockContext, ApplyTransaction, ApplyMessage. It is a
// SECOND interface rather than an export of exec's because the two want
// different things (exec runs whole blocks off a descriptor; rpc runs single
// messages under a tracer with no descriptor in hand) and because a shared one
// would have to carry both call shapes.
//
// WHERE THE KIND COMES FROM: the process-global libevm registration, exactly
// like rpc/blockextra.go. Exactly one kind can be registered per process (the
// header encodings are mutually exclusive), so there is no second answer for a
// server to hold and no constructor argument to thread.
//
// WHAT IS NOT IN HERE, because it is genuinely shared: vm.BlockContext,
// vm.TxContext, vm.Config, vm.NewEVM, types.Receipt, *params.ChainConfig and
// *ethstate.StateDB are all libevm types both VMs build on. Only the four
// entry points above, plus the ChainContext they take, are per-VM.

import (
	"math/big"

	"github.com/ava-labs/libevm/common"
	ethstate "github.com/ava-labs/libevm/core/state"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/core/vm"
	"github.com/ava-labs/libevm/params"

	"github.com/containerman17/epochdb/chain"
	"github.com/containerman17/epochdb/fetch"
)

// ChainContext is the header source the EVM needs for BLOCKHASH. It is
// GetHeader alone: both VMs' core.ChainContext also want an Engine(), and that
// is the one method whose type they do not share, so each backend adds its own
// (see exec's sevmChainContext for the same trick).
type ChainContext interface {
	GetHeader(common.Hash, uint64) *types.Header
}

// callMsg is the VM-neutral core.Message. coreth's and subnet-evm's structs
// are field-for-field identical but are DISTINCT Go types, so a caller has to
// hand over the values and let the backend build its own. SkipAccountChecks is
// implied: every caller here is an eth_call-shaped read.
type callMsg struct {
	From       common.Address
	To         *common.Address
	Value      *big.Int
	GasLimit   uint64
	GasPrice   *big.Int
	GasFeeCap  *big.Int
	GasTipCap  *big.Int
	Data       []byte
	AccessList types.AccessList
}

// callResult is the VM-neutral core.ExecutionResult. Revert is the backend's
// own ExecutionResult.Revert() (nil unless the EVM reverted with data), so the
// error-shaping in the callers stays one branch.
type callResult struct {
	UsedGas    uint64
	ReturnData []byte
	Revert     []byte
	Err        error
}

// rpcVM is the seam: exactly the execution entry points that differ.
type rpcVM interface {
	// applyUpgrades is core.ApplyUpgrades at header's boundary. parentTime is
	// the PARENT's timestamp and is a VALUE, not a pointer, because that is the
	// whole bug this signature exists to prevent: a nil parent timestamp reads
	// as "never forked" and re-activates every already-active precompile on
	// every block (SetNonce 1 + SetCode + Configure), which makes a trace and a
	// re-executed receipt disagree with what the executor actually did.
	applyUpgrades(cfg *params.ChainConfig, parentTime uint64, header *types.Header, statedb *ethstate.StateDB) error

	// blockContext is core.NewEVMBlockContext. On subnet-evm it also parses the
	// Warp predicate results out of header.Extra, which is why the header, not
	// a synthesized context, is the input.
	blockContext(header *types.Header, cc ChainContext) vm.BlockContext

	// applyTx is core.ApplyTransaction. gasLeft is the block's gas pool:
	// core.GasPool is `type GasPool uint64` in both VMs, so a *uint64 converts
	// in place and the pool survives the whole block's loop without this
	// interface having to name a per-VM type.
	applyTx(cfg *params.ChainConfig, cc ChainContext, blockCtx vm.BlockContext, gasLeft *uint64,
		statedb *ethstate.StateDB, header *types.Header, tx *types.Transaction,
		usedGas *uint64, vmCfg vm.Config) (*types.Receipt, error)

	// applyMsg builds the EVM and runs msg under a fresh gas pool of
	// msg.GasLimit (what all three call sites did inline).
	applyMsg(cfg *params.ChainConfig, blockCtx vm.BlockContext, statedb *ethstate.StateDB,
		msg *callMsg, vmCfg vm.Config) (*callResult, error)

	// minGasPrice is the floor eth_gasPrice reports when no recent block
	// carries a transaction to sample. It is the one chain-config-shaped
	// answer in the fee surface: the C-chain's phase-0 minimum is a constant,
	// an L1's is whatever its own genesis fee config says.
	minGasPrice(cfg *params.ChainConfig) *big.Int
}

// isMergeTODO is coreth's and subnet-evm's params.IsMergeTODO, which is `true`
// in both (Avalanche has no merge). It only ever feeds
// libevm's *params.ChainConfig.Rules, so it needs no seam of its own.
const isMergeTODO = true

// registeredVM is the backend for the kind THIS PROCESS registered with
// libevm. The zero kind is coreth, which is what every caller predating the
// descriptor meant.
func registeredVM() rpcVM {
	if fetch.RegisteredKind() == chain.SubnetEVM {
		return sevmRPC{}
	}
	return corethRPC{}
}

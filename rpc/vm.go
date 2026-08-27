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

	"github.com/containerman17/avalanche-s3-archival-node/chain"
	"github.com/containerman17/avalanche-s3-archival-node/fetch"
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

// gasChain is the per-height corpus content the ACP-194 base fee derivation
// reads (see saefee.go): a header, for its settlement markers, block time and
// gas exponents, and the gas a block's execution ACTUALLY consumed, which is
// the one input no header records. Server implements it; keeping it an
// interface is what lets the derivation live behind the VM seam without the
// backends knowing what a Server is.
type gasChain interface {
	headerFor(n uint64) (*types.Header, error)
	gasConsumed(n uint64) (uint64, error)
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

	// nextBaseFee projects the base fee of the block that would be built on
	// parent. It is the one FORWARD-LOOKING fee answer this server gives
	// (eth_feeHistory's trailing baseFeePerGas entry and
	// eth_suggestPriceOptions' maxFeePerGas), and it lives behind the seam
	// because the fee algorithm itself is per-VM: coreth advances the
	// ACP-176/AP3 fee state it keeps in parent.Extra, subnet-evm slides the
	// fee window it keeps in parent.Extra against the chain's own feeConfig.
	// Both VMs export the estimator, so neither side reimplements arithmetic
	// a node already agrees on.
	//
	// THE TIMESTAMP IS THE PARENT'S OWN, not wall clock, and that is a
	// deliberate difference from a validator: a validator projects for a
	// block it is about to build NOW, while this server answers for a corpus
	// whose head may be years old, where "now" decays the fee state to the
	// floor and produces a confident number about a chain that stopped. At
	// the tip the two differ by one block interval, and in the direction that
	// is safe for the only thing a caller does with it: elapsed time only
	// ever LOWERS the next base fee, so projecting at the parent's timestamp
	// is an upper bound on what the next block will charge.
	//
	// A nil result with a nil error means the chain has NO base fee at that
	// height (pre-AP3, pre-SubnetEVM); that is an answer, not a failure. An
	// error means the projection could not be made and the caller must not
	// invent one.
	nextBaseFee(cfg *params.ChainConfig, parent *types.Header, c gasChain) (*big.Int, error)

	// baseFees returns the base fee OF each block in [from, to], one entry per
	// height, in order. It is the backward-looking twin of nextBaseFee and it
	// exists because on ONE path the answer is not in the header: above the
	// Helicon boundary a stored header carries no base fee at all, and the
	// value has to be derived from the ACP-194 gas clock (saefee.go). Below the
	// boundary, and on every subnet-evm chain, this is header.BaseFee verbatim
	// and a nil entry means the chain genuinely had no base fee at that height.
	//
	// It takes a RANGE rather than a height because the derivation walks the
	// clock forward: answering a whole range in one walk costs the range plus
	// the settlement lag, where answering each height separately would cost the
	// range TIMES the lag.
	baseFees(cfg *params.ChainConfig, from, to uint64, c gasChain) ([]*big.Int, error)

	// baseFeeOf is baseFees for ONE block whose header the caller already
	// holds. It takes the header rather than a height so that the accepted tail
	// answers too: `pending` is a container the executor has not reached, so no
	// header of it is stored, while everything the derivation walks sits below
	// it and is.
	baseFeeOf(cfg *params.ChainConfig, h *types.Header, c gasChain) (*big.Int, error)
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

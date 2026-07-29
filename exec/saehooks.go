package exec

import (
	"fmt"
	"time"

	"github.com/ava-labs/avalanchego/graft/coreth/core/extstate"
	"github.com/ava-labs/avalanchego/graft/coreth/params/extras"
	ccustomtypes "github.com/ava-labs/avalanchego/graft/coreth/plugin/evm/customtypes"
	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/vms/components/gas"
	"github.com/ava-labs/avalanchego/vms/evm/acp176"
	"github.com/ava-labs/avalanchego/vms/saevm/cchain/dynamic"
	saetx "github.com/ava-labs/avalanchego/vms/saevm/cchain/tx"
	"github.com/ava-labs/avalanchego/vms/saevm/gastime"
	"github.com/ava-labs/avalanchego/vms/saevm/hook"
	"github.com/ava-labs/libevm/common"
	ethstate "github.com/ava-labs/libevm/core/state"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/libevm"
	"github.com/ava-labs/libevm/params"

	cparams "github.com/ava-labs/avalanchego/graft/coreth/params"
	corethwarp "github.com/ava-labs/avalanchego/graft/coreth/precompile/contracts/warp"
	saetypes "github.com/ava-labs/avalanchego/vms/saevm/types"
)

// saeHooks is the C-Chain hook.Points implementation that saexec.Execute
// needs, FORKED from vms/saevm/cchain/hooks.go at avalanchego
// v1.15.0-fuji. The upstream `cchain.hooks` type is unexported and has no
// factory, so a node-external executor cannot obtain one; every helper it
// leans on IS exported (cchain/tx, cchain/dynamic, coreth customtypes,
// coreth params), so this file is glue, not re-derived semantics.
//
// SAE Go APIs are explicitly unstable upstream. Everything here is pinned
// to v1.15.0-fuji and MUST be re-read against cchain/hooks.go on every dep
// bump. Deliberate divergences from upstream, all outside EVM state:
//
//   - no metrics;
//   - AfterExecutingBlock keeps the non-AVAX transfers (real EVM state)
//     and drops the cross-chain shared-memory Apply and the warp message
//     store (separate databases a replay node does not own);
//   - block building (hook.PointsG: BuildHeader/BuildBlock/
//     PotentialEndOfBlockOps/BlockRebuilderFrom) is absent: we replay
//     blocks, we never build them, and saexec.Execute only takes the
//     non-generic hook.Points;
//   - ExecutionResultsDB is never called (that is the VM's own store).
//
// Not goroutine-safe: one executor goroutine, like the rest of exec.
type saeHooks struct {
	chainCfg    *params.ChainConfig
	avaxAssetID ids.ID
	// err latches the first silent-default upstream would have taken. The
	// hook.Points signatures cannot return an error where a bad header
	// would otherwise be papered over with an initial exponent, and a
	// wrong gas target means a wrong base fee means wrong execution, so
	// the executor checks this after every block instead.
	err error
}

var _ hook.Points = (*saeHooks)(nil)

func (h *saeHooks) fail(err error) {
	if h.err == nil {
		h.err = err
	}
}

// takeErr returns and clears any latched hook error.
func (h *saeHooks) takeErr() error {
	err := h.err
	h.err = nil
	return err
}

// ExecutionResultsDB is part of hook.Points but only the VM calls it (the
// executor persists its own results, see saering.go).
func (*saeHooks) ExecutionResultsDB(string) (saetypes.ExecutionResults, error) {
	return saetypes.ExecutionResults{}, fmt.Errorf("exec: SAE execution-results DB is not used by the replay executor")
}

// priceExponent returns h's ACP-283 price exponent, defaulting to
// dynamic.InitialPriceExponent when the header does not carry one.
func priceExponent(h *types.Header) dynamic.PriceExponent {
	if pe := ccustomtypes.GetHeaderExtra(h).MinPriceExponent; pe != nil {
		return *pe
	}
	return dynamic.InitialPriceExponent
}

// targetExponent returns h's ACP-176 target exponent. Post-Helicon headers
// carry it; the last synchronous block still encodes the ACP-176 fee state
// in header.Extra, which is exactly how the gas clock is seeded across the
// transition.
func targetExponent(config *extras.ChainConfig, h *types.Header) (dynamic.TargetExponent, error) {
	if te := ccustomtypes.GetHeaderExtra(h).TargetExponent; te != nil {
		return *te, nil
	}
	if !config.IsFortuna(h.Time) || h.Number.Sign() == 0 {
		return dynamic.InitialTargetExponent, nil
	}
	state, err := acp176.ParseState(h.Extra)
	if err != nil {
		return 0, fmt.Errorf("parsing fee state: %w", err)
	}
	return dynamic.TargetExponent(state.TargetExcess), nil
}

func (h *saeHooks) GasConfigAfter(header *types.Header) (gas.Gas, gastime.GasPriceConfig) {
	te, err := targetExponent(cparams.GetExtra(h.chainCfg), header)
	if err != nil {
		// Upstream logs and falls back to the initial exponent here. A
		// replay node cannot: the fallback changes the base fee and thus
		// the state root, so latch it and let the executor hard-stop.
		h.fail(fmt.Errorf("gas target of block %d: %w", header.Number, err))
		te = dynamic.InitialTargetExponent
	}
	return te.Target(), gastime.GasPriceConfig{
		TargetToExcessScaling: 87, // 87 ~= 60 / ln(2)
		MinPrice:              priceExponent(header).Price(),
	}
}

func (*saeHooks) SettledBy(h *types.Header) hook.Settled {
	return settledBy(h)
}

// settledBy reads the four ACP-194 settlement markers off a header. A
// header missing any of them settles nothing (pre-SAE headers, and the
// genesis block, which is self-settling).
func settledBy(h *types.Header) hook.Settled {
	he := ccustomtypes.GetHeaderExtra(h)
	if he.SettledHeight == nil ||
		he.SettledGasUnix == nil ||
		he.SettledGasNumerator == nil ||
		he.SettledExcess == nil {
		return hook.Settled{}
	}
	return hook.Settled{
		Height:       *he.SettledHeight,
		GasUnix:      *he.SettledGasUnix,
		GasNumerator: gas.Gas(*he.SettledGasNumerator),
		Excess:       gas.Gas(*he.SettledExcess),
	}
}

// BlockTime returns the canonical wall-clock time of a block. The
// whole-second value is authoritative and the millisecond field only
// refines it below the second.
func (*saeHooks) BlockTime(h *types.Header) time.Time {
	return saeBlockTime(h)
}

func saeBlockTime(h *types.Header) time.Time {
	ms := ccustomtypes.HeaderTimeMilliseconds(h)
	subSecondNanos := int64(ms%1000) * int64(time.Millisecond)
	return time.Unix(int64(h.Time), subSecondNanos)
}

// EndOfBlockOps turns the block's atomic (cross-chain) transactions into
// the operations SAE applies after the EVM transactions. Under SAE these
// carry their own gas, which is why header.GasUsed covers them too.
func (h *saeHooks) EndOfBlockOps(b *types.Block) ([]hook.Op, error) {
	txs, err := saetx.ParseSlice(ccustomtypes.BlockExtData(b))
	if err != nil {
		return nil, fmt.Errorf("parsing atomic txs: %w", err)
	}
	ops := make([]hook.Op, len(txs))
	for i, t := range txs {
		op, err := t.AsOp(h.avaxAssetID)
		if err != nil {
			return nil, fmt.Errorf("converting atomic tx %s (%d): %w", t.ID(), i, err)
		}
		ops[i] = op
	}
	return ops, nil
}

func (*saeHooks) CanExecuteTransaction(common.Address, *common.Address, libevm.StateReader) error {
	return nil
}

func (h *saeHooks) BeforeExecutingBlock(rules params.Rules, statedb *ethstate.StateDB, parent *types.Header, _ *types.Block) error {
	config := cparams.GetExtra(h.chainCfg)
	if isFirstDurangoBlock := cparams.GetRulesExtra(rules).IsDurango && !config.IsDurango(parent.Time); isFirstDurangoBlock {
		// Dead on both networks (Durango long precedes Helicon), kept so
		// the fork stays a mirror of upstream.
		statedb.SetNonce(corethwarp.ContractAddress, 1)
		statedb.SetCode(corethwarp.ContractAddress, []byte{0x01})
	}
	return nil
}

func (h *saeHooks) AfterExecutingBlock(statedb *ethstate.StateDB, b *types.Block, _ types.Receipts) error {
	txs, err := saetx.ParseSlice(ccustomtypes.BlockExtData(b))
	if err != nil {
		return fmt.Errorf("parsing atomic txs: %w", err)
	}
	extstatedb := extstate.New(statedb)
	for i, t := range txs {
		if err := t.TransferNonAVAX(h.avaxAssetID, extstatedb); err != nil {
			return fmt.Errorf("transferring non-AVAX assets of tx %s (%d): %w", t.ID(), i, err)
		}
	}
	return nil
}

package rpc

// THE POST-HELICON BASE FEE, DERIVED FROM EPOCH CONTENT (DESIGN, SAE).
//
// A STORED POST-HELICON HEADER CARRIES NO BASE FEE. The SAE builder constructs
// its header BEFORE execution and saexec.Execute writes the executed base fee
// onto a COPY it takes for the run, so the header that reaches our store, our
// epochs and every consumer has `BaseFee` untouched. Read straight off the
// header, every fee this server reports above the boundary is zero.
//
// It does not have to be stored, because the ACP-194 fee is a DETERMINISTIC
// GAS CLOCK and everything that clock consumes is already in the corpus:
//
//   - consensus republishes the clock at SETTLEMENT POINTS, in the four
//     markers every post-Helicon header carries (SettledHeight,
//     SettledGasUnix, SettledGasNumerator, SettledExcess), so header N alone
//     yields the clock as of the post-execution state of the height N settles,
//     1-5 blocks back;
//   - between settlement points the clock advances by BLOCK TIME and by the
//     gas each block's execution actually consumed, both of which we hold.
//
// So the walk is: take the settlement point named by the header at the bottom
// of the range, then replay gastime's own BeforeBlock/AfterBlock per block up
// to the height asked about. The arithmetic is upstream's throughout
// (gastime, proxytime, dynamic); this file only reads header fields and
// counts gas.
//
// THE ONE INPUT THAT IS NOT A HEADER FIELD is the gas a block actually
// consumed: under SAE `header.GasUsed` is the WORST CASE (the sum of
// transaction gas LIMITS plus end-of-block op gas), while the clock ticks by
// what execution really used. See Server.gasConsumed.
//
// The header reads below are TWINS of exec/saehooks.go's, repeated rather than
// shared for the same reason heliconTransitionLead is (see vm_coreth.go): the
// two packages read the same rule for different purposes and exec's copies are
// unexported. They MUST be re-read against vms/saevm/cchain/hooks.go on every
// dep bump.

import (
	"fmt"
	"math/big"
	"time"

	"github.com/ava-labs/avalanchego/graft/coreth/params/extras"
	ccustomtypes "github.com/ava-labs/avalanchego/graft/coreth/plugin/evm/customtypes"
	"github.com/ava-labs/avalanchego/vms/components/gas"
	"github.com/ava-labs/avalanchego/vms/evm/acp176"
	"github.com/ava-labs/avalanchego/vms/saevm/cchain/dynamic"
	"github.com/ava-labs/avalanchego/vms/saevm/gastime"
	"github.com/ava-labs/avalanchego/vms/saevm/hook"
	"github.com/ava-labs/avalanchego/vms/saevm/proxytime"
	"github.com/ava-labs/libevm/core/types"

	cparams "github.com/ava-labs/avalanchego/graft/coreth/params"
	"github.com/ava-labs/libevm/params"
)

// saeSettledBy reads the four ACP-194 settlement markers off a header. A
// header missing any of them settles nothing, which is what identifies a
// pre-SAE header (and genesis, which is self-settling).
func saeSettledBy(h *types.Header) (hook.Settled, bool) {
	he := ccustomtypes.GetHeaderExtra(h)
	if he.SettledHeight == nil ||
		he.SettledGasUnix == nil ||
		he.SettledGasNumerator == nil ||
		he.SettledExcess == nil {
		return hook.Settled{}, false
	}
	return hook.Settled{
		Height:       *he.SettledHeight,
		GasUnix:      *he.SettledGasUnix,
		GasNumerator: gas.Gas(*he.SettledGasNumerator),
		Excess:       gas.Gas(*he.SettledExcess),
	}, true
}

// saeBlockTime is the canonical block time the clock advances to: the
// whole-second header value, refined below the second by the milliseconds
// field.
func saeBlockTime(h *types.Header) time.Time {
	ms := ccustomtypes.HeaderTimeMilliseconds(h)
	return time.Unix(int64(h.Time), int64(ms%1000)*int64(time.Millisecond)) //#nosec G115
}

// saeTargetExponent is the ACP-176 target exponent in effect after h.
// Post-Helicon headers carry it outright; the last SYNCHRONOUS block still
// encodes the fee state in header.Extra, and that is exactly how the gas clock
// is seeded across the transition, so the range walk hits this branch once, on
// the settled header just below the boundary.
func saeTargetExponent(cfg *extras.ChainConfig, h *types.Header) (dynamic.TargetExponent, error) {
	if te := ccustomtypes.GetHeaderExtra(h).TargetExponent; te != nil {
		return *te, nil
	}
	if !cfg.IsFortuna(h.Time) || h.Number.Sign() == 0 {
		return dynamic.InitialTargetExponent, nil
	}
	st, err := acp176.ParseState(h.Extra)
	if err != nil {
		return 0, fmt.Errorf("parsing fee state: %w", err)
	}
	return dynamic.TargetExponent(st.TargetExcess), nil
}

// saeGasConfigAfter is cchain hooks' GasConfigAfter. Upstream logs and falls
// back to the initial exponent when the target cannot be read; a reader
// answering fees cannot, because the fallback silently produces a different,
// confident base fee.
func saeGasConfigAfter(chainCfg *params.ChainConfig, h *types.Header) (gas.Gas, gastime.GasPriceConfig, error) {
	te, err := saeTargetExponent(cparams.GetExtra(chainCfg), h)
	if err != nil {
		return 0, gastime.GasPriceConfig{}, fmt.Errorf("gas target of block %d: %w", h.Number, err)
	}
	pe := dynamic.InitialPriceExponent
	if p := ccustomtypes.GetHeaderExtra(h).MinPriceExponent; p != nil {
		pe = *p
	}
	return te.Target(), gastime.GasPriceConfig{
		TargetToExcessScaling: 87, // 87 ~= 60 / ln(2)
		MinPrice:              pe.Price(),
	}, nil
}

// saeSettledGasTime is hook.SettledGasTime: the gas clock as of the
// post-execution state of the height `settler` settles. Inlined (it is three
// lines) because the upstream helper takes a whole hook.Points, which is a
// node-side object this read server has no business constructing.
func saeSettledGasTime(chainCfg *params.ChainConfig, settled, settler *types.Header) (*gastime.Time, error) {
	target, cfg, err := saeGasConfigAfter(chainCfg, settled)
	if err != nil {
		return nil, err
	}
	s, ok := saeSettledBy(settler)
	if !ok {
		return nil, fmt.Errorf("block %d carries no settlement markers", settler.Number)
	}
	pt := proxytime.New(s.GasUnix, s.GasNumerator, gastime.SafeRateOfTarget(target))
	return gastime.FromProxyTime(pt, s.Excess, cfg)
}

// saeWalk derives the executed base fee of every block in [start, to], where
// `start` is the header of the lowest height wanted and every height in the
// range is post-Helicon. It also returns the clock left exactly where
// saexec.Execute has it when it reads `to`'s base fee: advanced to to's block
// time, NOT yet ticked by to's own gas.
//
// The loop is Execute's own order: advance the clock to the block's time, read
// the base fee THERE (that is the value Execute stamps onto its header copy),
// then tick the clock by the block's consumed gas under the gas config that
// block leaves behind.
//
// `start` is passed in rather than read, so a caller that already holds the
// header does not re-read it AND so the top of the range may be a height whose
// header is not in the store yet: the "pending" block is an accepted container
// the executor has not reached, and its own markers name a height well below
// it, which is.
//
// ponytail: one pass per call, so a range costs (to-from) + the settlement lag
// block reads rather than that times the lag. No clock is cached between
// calls; add one if a post-SAE eth_feeHistory over hundreds of blocks ever
// shows up in a profile.
func saeWalk(chainCfg *params.ChainConfig, start *types.Header, to uint64, c gasChain) ([]*big.Int, *gastime.Time, error) {
	from := start.Number.Uint64()
	s, ok := saeSettledBy(start)
	if !ok {
		return nil, nil, fmt.Errorf("block %d carries no settlement markers: it is not post-Helicon", from)
	}
	if s.Height >= from {
		return nil, nil, fmt.Errorf("block %d settles height %d: a block cannot settle itself or the future", from, s.Height)
	}
	settled, err := c.headerFor(s.Height)
	if err != nil {
		return nil, nil, err
	}
	clock, err := saeSettledGasTime(chainCfg, settled, start)
	if err != nil {
		return nil, nil, err
	}

	fees := make([]*big.Int, 0, to-from+1)
	for n := s.Height + 1; n <= to; n++ {
		h := start
		if n != from {
			if h, err = c.headerFor(n); err != nil {
				return nil, nil, err
			}
		}
		clock.BeforeBlock(saeBlockTime(h))
		if n >= from {
			fees = append(fees, clock.BaseFee().ToBig())
		}
		if n == to {
			break // to's own gas belongs to the block AFTER it
		}
		if err := saeAfterBlock(chainCfg, h, c, clock); err != nil {
			return nil, nil, err
		}
	}
	return fees, clock, nil
}

// saeAfterBlock ticks the clock past the block h describes, by the gas that
// block's execution actually consumed.
func saeAfterBlock(chainCfg *params.ChainConfig, h *types.Header, c gasChain, clock *gastime.Time) error {
	n := h.Number.Uint64()
	used, err := c.gasConsumed(n)
	if err != nil {
		return err
	}
	target, cfg, err := saeGasConfigAfter(chainCfg, h)
	if err != nil {
		return err
	}
	if err := clock.AfterBlock(gas.Gas(used), target, cfg); err != nil {
		return fmt.Errorf("after block %d: %w", n, err)
	}
	return nil
}

package validator

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ava-labs/avalanchego/graft/evm/constants"
	sevmparams "github.com/ava-labs/avalanchego/graft/subnet-evm/params"
	"github.com/ava-labs/avalanchego/graft/subnet-evm/plugin/evm/customheader"
	"github.com/ava-labs/avalanchego/graft/subnet-evm/plugin/evm/customtypes"
	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/snow/consensus/snowman"
	"github.com/ava-labs/avalanchego/snow/engine/common"
	"github.com/ava-labs/avalanchego/utils/lock"
	"github.com/ava-labs/avalanchego/utils/logging"
	ethcommon "github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/rlp"
	"go.uber.org/zap"
)

// retryDelay: subnet-evm's minimum gap between two build attempts on the
// same parent.
const retryDelay = 100 * time.Millisecond

// poolWaitSlice: how long one epochdb_pool_wait blocks before the caller's
// context is checked again (a preference change cancels the context; the
// wait then restarts against the new preferred chain).
const poolWaitSlice = 50 * time.Millisecond

// buildStats: every WaitForEvent wake and what became of it. A wake that
// yields no block is logged as `validator: build-skip {reason}`:
//   - proposervm-window: PendingTxs was returned but no BuildBlock reached
//     the VM before the next WaitForEvent (proposervm dropped it: not this
//     node's slot for the preferred parent, avalanchego's "build block
//     dropped" debug line, snowman's blks_built_failed counter);
//   - engine-empty: epochdb_build included no tx (every candidate held by
//     the parent chain or skipped);
//   - engine-error: epochdb_build failed;
//   - retry-gap-wait / min-delay-wait: the wait was cut short by a
//     preference change (the wake never fired; `waited` is the time lost).
type buildStats struct {
	wakes, builds, built                                           atomic.Uint64
	proposervmWindow, engineEmpty, engineError, gapWait, delayWait atomic.Uint64
}

func (s *buildStats) fields() []zap.Field {
	return []zap.Field{zap.Uint64("wakes", s.wakes.Load()), zap.Uint64("builds", s.builds.Load()), zap.Uint64("blocks", s.built.Load()),
		zap.Uint64("skipProposervmWindow", s.proposervmWindow.Load()), zap.Uint64("skipEngineEmpty", s.engineEmpty.Load()),
		zap.Uint64("skipEngineError", s.engineError.Load()), zap.Uint64("skipRetryGapWait", s.gapWait.Load()), zap.Uint64("skipMinDelayWait", s.delayWait.Load())}
}

func (s *buildStats) health() map[string]uint64 {
	return map[string]uint64{"wakes": s.wakes.Load(), "builds": s.builds.Load(), "blocks": s.built.Load(),
		"skip-proposervm-window": s.proposervmWindow.Load(), "skip-engine-empty": s.engineEmpty.Load(), "skip-engine-error": s.engineError.Load(),
		"skip-retry-gap-wait": s.gapWait.Load(), "skip-min-delay-wait": s.delayWait.Load()}
}

// builder decides WHEN to build (subnet-evm's blockBuilder rules): the
// engine's pool has executable txs (epochdb_pool_wait blocks on it), and the
// timing rules (retry delay, Granite minimum block delay) allow it.
type builder struct {
	eng  *engine
	log  logging.Logger
	mu   sync.Mutex
	cond *lock.Cond

	normalOp        bool
	lastBuildTime   time.Time
	lastBuildParent ethcommon.Hash
	// wakeUnbuilt: the last wake's PendingTxs has not reached BuildBlock yet
	// (the snowman engine turns one wake into at most one BuildBlock, and
	// proposervm may drop that call above the VM).
	wakeUnbuilt bool
	stats       buildStats
	// pref is closed (and replaced) by every SetPreference: a wait on the
	// retry gap or the Granite delay re-evaluates against the new parent at
	// once, without relying on the engine's WaitForEvent cancellation alone.
	pref chan struct{}
}

func newBuilder(eng *engine, log logging.Logger) *builder {
	b := &builder{eng: eng, log: log, pref: make(chan struct{})}
	b.cond = lock.NewCond(&b.mu)
	return b
}

// preferenceChanged wakes a waitForEvent sitting in a timing wait.
func (b *builder) preferenceChanged() {
	b.mu.Lock()
	close(b.pref)
	b.pref = make(chan struct{})
	b.mu.Unlock()
}

func (b *builder) setNormalOp() {
	b.mu.Lock()
	b.normalOp = true
	b.cond.Broadcast()
	b.mu.Unlock()
}

func (b *builder) built(parent ethcommon.Hash) {
	b.mu.Lock()
	b.lastBuildTime, b.lastBuildParent = time.Now(), parent
	b.mu.Unlock()
}

// buildCalled: BuildBlock reached the VM (pairs the last wake).
func (b *builder) buildCalled() {
	b.stats.builds.Add(1)
	b.mu.Lock()
	b.wakeUnbuilt = false
	b.mu.Unlock()
}

// waitForEvent: NormalOp, then the pool holds an executable tx (the wait
// lives in the engine; the pool's head moves inside Accept, so a mined tx
// never counts as pending here), then the timing rules. `state` is the
// accepted head's header and the preferred block's id.
func (b *builder) waitForEvent(ctx context.Context, state func() (*types.Header, ids.ID)) (common.Message, error) {
	b.mu.Lock()
	for !b.normalOp {
		if err := b.cond.Wait(ctx); err != nil {
			b.mu.Unlock()
			return 0, err
		}
	}
	unbuilt := b.wakeUnbuilt
	b.wakeUnbuilt = false
	b.mu.Unlock()
	h, preferred := state()
	if unbuilt {
		// A new subscription with the previous wake never built: proposervm
		// dropped the BuildBlock (the ChangeNotifier re-subscribes after
		// every BuildBlock attempt, dropped or not).
		b.stats.proposervmWindow.Add(1)
		b.log.Info("validator: build-skip", zap.String("reason", "proposervm-window"), zap.Uint64("head", h.Number.Uint64()), zap.Stringer("preferred", preferred))
	}
	t0 := time.Now()
	var gap time.Duration
	for {
		// Only FREE executable txs count: the preferred chain's txs stay in
		// the pool until accept, and a build on it skips them.
		for !b.eng.poolWait(preferred, poolWaitSlice) {
			if err := ctx.Err(); err != nil {
				return 0, err
			}
			h, preferred = state()
		}
		b.mu.Lock()
		lastTime, lastParent, pref := b.lastBuildTime, b.lastBuildParent, b.pref
		b.mu.Unlock()

		// The retry gap applies to a REPEATED build on the same preferred
		// parent (our block is out and consensus has not moved yet). A new
		// preferred block, ours or a peer's, builds at once: the engine's
		// candidates skip the txs of the unaccepted ancestors, so nothing is
		// re-offered. Both waits end when the preference moves (SetPreference
		// closes pref; the ChangeNotifier also cancels this WaitForEvent) and
		// the loop re-evaluates against the new parent.
		var next time.Time
		reason, counter := "", (*atomic.Uint64)(nil)
		if lastParent == ethcommon.Hash(preferred) {
			next, reason, counter = lastTime.Add(retryDelay), "retry-gap-wait", &b.stats.gapWait
		} else if preferred == ids.ID(h.Hash()) {
			next, reason, counter = minNextBlockTime(h), "min-delay-wait", &b.stats.delayWait // Granite: the wait lives here, not in BuildBlock
		}
		gap = time.Until(next)
		if gap <= 0 {
			break
		}
		cut := func(by string) {
			counter.Add(1)
			b.log.Info("validator: build-skip", zap.String("reason", reason), zap.String("by", by), zap.Uint64("head", h.Number.Uint64()),
				zap.Stringer("preferred", preferred), zap.Duration("gap", gap), zap.Duration("waited", gap-time.Until(next)))
		}
		select {
		case <-ctx.Done():
			cut("cancel")
			return 0, ctx.Err()
		case <-pref:
			cut("preference")
			h, preferred = state()
			continue
		case <-time.After(gap):
		}
		break
	}
	b.stats.wakes.Add(1)
	b.mu.Lock()
	b.wakeUnbuilt = true
	b.mu.Unlock()
	b.log.Info("validator: wake", zap.Uint64("head", h.Number.Uint64()), zap.Stringer("preferred", preferred), zap.Duration("poolWait", time.Since(t0)-max(gap, 0)), zap.Duration("gap", max(gap, 0)))
	return common.PendingTxs, nil
}

// minNextBlockTime: Granite's minimum delay after the parent (ACP-226).
func minNextBlockTime(parent *types.Header) time.Time {
	extra := customtypes.GetHeaderExtra(parent)
	if extra.MinDelayExcess == nil {
		return time.Time{}
	}
	return customtypes.BlockTime(parent).Add(time.Duration(extra.MinDelayExcess.Delay()) * time.Millisecond)
}

var errNoTxs = errors.New("validator: no transactions to build with")

// buildBlock: one engine crossing. The engine takes the candidates from its
// pool (effective tip order, per-sender nonce order, 1.5x the gas limit and
// the miner's size target as the cut), executes them with the miner's skip
// semantics, builds the header (base fee, gas cost, fee window, Granite
// times), computes the state root and keeps the result as a verified
// pending block.
func (vm *VM) buildBlock(pchainHeight uint64) (snowman.Block, error) {
	start := time.Now()
	vm.b.buildCalled()
	head, headID := vm.current()
	vm.mu.Lock()
	parentID := vm.preferred
	vm.mu.Unlock()
	parent := head
	if parentID != headID && parentID != ids.Empty {
		h, err := vm.headerOf(parentID)
		if err != nil {
			return nil, err
		}
		parent = h
	} else {
		parentID = headID
	}

	if d := time.Until(minNextBlockTime(parent)); d > 0 {
		time.Sleep(d)
	}
	now := customheader.GetNextTimestamp(parent, time.Now())
	tsMS := uint64(now.UnixMilli())
	var ph phases
	ph.lap("head") // header lookups, the Granite wait
	out, err := vm.buildOnce(parentID, tsMS, pchainHeight, &ph)
	if err != nil {
		vm.b.stats.engineError.Add(1)
		vm.ctx.Log.Warn("validator: build-skip", zap.String("reason", "engine-error"), zap.Uint64("height", parent.Number.Uint64()+1), zap.Error(err))
		return nil, err
	}
	// built() before the empty check: an all-skipped build (every candidate
	// already mined, or invalid) gets the same 100 ms retry gap as a block
	// that consensus has not accepted yet. Without it WaitForEvent fired
	// again at once while the pool still held the skipped txs (K7 on the
	// settle run: 11595 engine builds for 175 blocks).
	vm.b.built(parent.Hash())
	if out.included == 0 {
		vm.m.buildEmpty.Inc()
		vm.b.stats.engineEmpty.Add(1)
		vm.ctx.Log.Info("validator: build-skip", zap.String("reason", "engine-empty"), zap.Uint64("height", parent.Number.Uint64()+1),
			zap.Int("candidates", len(out.skipped)), zap.Duration("took", time.Since(start)))
		return nil, errNoTxs
	}
	vm.b.stats.built.Add(1)
	vm.m.buildTxs.Observe(float64(out.included))
	vm.mu.Lock()
	vm.built[out.id] = time.Now()
	vm.mu.Unlock()
	var skips [7]int
	for _, c := range out.skipped {
		if int(c) < len(skips) {
			skips[c]++
		}
	}
	pending, queued := vm.eng.poolStatus() // what the pool holds right after the build (its txs stay until accept)
	fields := []zap.Field{zap.Uint64("height", parent.Number.Uint64()+1), zap.Uint64("included", out.included),
		zap.Int("candidates", len(out.skipped)), zap.Uint64("pending", pending), zap.Uint64("queued", queued), zap.Uint64("gasUsed", out.gasUsed),
		zap.Int("skipNonceLow", skips[1]), zap.Int("skipFailed", skips[2]), zap.Int("skipPopped", skips[3]),
		zap.Int("skipNoGas", skips[4]), zap.Int("skipSize", skips[5]), zap.Int("skipNotReached", skips[6]),
		zap.Uint64("poolDup", vm.eng.poolDup()), zap.String("ingest", vm.ingest.summary()),
		zap.Duration("took", time.Since(start))}
	fields = append(fields, vm.b.stats.fields()...)
	fields = append(fields, ph.fields()...)
	vm.ctx.Log.Info("validator: built", fields...)
	return &Block{vm: vm, raw: out.block, id: out.id, parent: parentID, height: parent.Number.Uint64() + 1, time: tsMS / 1000}, nil
}

// phases is the per-build phase split: monotonic laps, logged as fields.
type phases struct {
	last  time.Time
	names []string
	durs  []time.Duration
}

func (p *phases) lap(name string) {
	now := time.Now()
	if !p.last.IsZero() {
		p.names = append(p.names, name)
		p.durs = append(p.durs, now.Sub(p.last))
	}
	p.last = now
}

// add records a phase measured elsewhere (the engine's own split).
func (p *phases) add(name string, d time.Duration) {
	p.names = append(p.names, name)
	p.durs = append(p.durs, d)
	p.last = time.Now()
}

func (p *phases) fields() []zap.Field {
	out := make([]zap.Field, 0, len(p.names))
	for i, n := range p.names {
		out = append(out, zap.Duration("t_"+n, p.durs[i]))
	}
	return out
}

// buildOnce: one engine crossing with no candidates (the engine's pool
// supplies them); epochdb_build_seconds is this call alone (BuildBlock's
// wall time also holds the Granite min-delay wait).
func (vm *VM) buildOnce(parent ids.ID, tsMS, pchainHeight uint64, ph *phases) (buildOut, error) {
	start := time.Now()
	out, err := vm.eng.build(parent, tsMS, vm.coinbase(), pchainHeight, nil, nil)
	vm.m.build.Observe(time.Since(start).Seconds())
	ph.lap("cgo")
	if err == nil {
		var sum time.Duration
		for i, d := range out.phaseNS {
			ph.add("eng_"+enginePhaseNames[i], d)
			sum += d
		}
		ph.add("eng_other", ph.durs[len(ph.durs)-len(out.phaseNS)-1]-sum) // the cgo lap minus the engine's own split
	}
	return out, err
}

// coinbase: subnet-evm's rule without a RewardManager. ponytail: a chain with
// the RewardManager precompile needs the engine to answer the reward address.
func (vm *VM) coinbase() ethcommon.Address {
	if !sevmparams.GetExtra(vm.config).AllowFeeRecipients {
		return constants.BlackholeAddr
	}
	if ethcommon.IsHexAddress(vm.cfg.FeeRecipient) {
		return ethcommon.HexToAddress(vm.cfg.FeeRecipient)
	}
	return constants.BlackholeAddr
}

// headerOf decodes only the header of a stored block (the preferred block
// when it is not yet accepted; rare).
func (vm *VM) headerOf(id ids.ID) (*types.Header, error) {
	raw, err := vm.eng.getBlock(id)
	if err != nil {
		return nil, fmt.Errorf("validator: preferred block %s: %w", id, err)
	}
	s := rlp.NewStream(bytesReader(raw), uint64(len(raw)))
	if _, err := s.List(); err != nil {
		return nil, err
	}
	h := new(types.Header)
	if err := s.Decode(h); err != nil {
		return nil, err
	}
	return h, nil
}

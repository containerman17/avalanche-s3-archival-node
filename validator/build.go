package validator

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ava-labs/avalanchego/graft/evm/constants"
	sevmparams "github.com/ava-labs/avalanchego/graft/subnet-evm/params"
	"github.com/ava-labs/avalanchego/graft/subnet-evm/plugin/evm/customheader"
	"github.com/ava-labs/avalanchego/graft/subnet-evm/plugin/evm/customtypes"
	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/snow/consensus/snowman"
	"github.com/ava-labs/avalanchego/snow/engine/common"
	"github.com/ava-labs/avalanchego/utils/lock"
	ethcommon "github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/rlp"
	"go.uber.org/zap"
)

// retryDelay: subnet-evm's minimum gap between two build attempts on the
// same parent.
const retryDelay = 100 * time.Millisecond

// poolWaitSlice: how long one epochdb_pool_wait blocks before the caller's
// context is checked again.
const poolWaitSlice = 200 * time.Millisecond

// builder decides WHEN to build (subnet-evm's blockBuilder rules): the
// engine's pool has executable txs (epochdb_pool_wait blocks on it), and the
// timing rules (retry delay, Granite minimum block delay) allow it.
type builder struct {
	eng  *engine
	mu   sync.Mutex
	cond *lock.Cond

	normalOp        bool
	lastBuildTime   time.Time
	lastBuildParent ethcommon.Hash
}

func newBuilder(eng *engine) *builder {
	b := &builder{eng: eng}
	b.cond = lock.NewCond(&b.mu)
	return b
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

// waitForEvent: NormalOp, then the pool holds an executable tx (the wait
// lives in the engine; the pool's head moves inside Accept, so a mined tx
// never counts as pending here), then the timing rules.
func (b *builder) waitForEvent(ctx context.Context, head func() *types.Header) (common.Message, error) {
	b.mu.Lock()
	for !b.normalOp {
		if err := b.cond.Wait(ctx); err != nil {
			b.mu.Unlock()
			return 0, err
		}
	}
	b.mu.Unlock()
	for !b.eng.poolWait(poolWaitSlice) {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
	}
	b.mu.Lock()
	lastTime, lastParent := b.lastBuildTime, b.lastBuildParent
	b.mu.Unlock()

	h := head()
	// A build within the retry gap of the previous one waits it out
	// whatever the parent: a build on the preferred (unaccepted) block right
	// after the build that made it found the same candidates minus that
	// block's and burned an engine build every 50 ms (58 per height seen).
	next := lastTime.Add(retryDelay)
	if lastParent != h.Hash() {
		next = maxTime(next, minNextBlockTime(h)) // Granite: the wait lives here, not in BuildBlock
	}
	if d := time.Until(next); d > 0 {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(d):
		}
	}
	return common.PendingTxs, nil
}

func maxTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
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
		return nil, errNoTxs
	}
	vm.m.buildTxs.Observe(float64(out.included))
	vm.mu.Lock()
	vm.lastBuilt, vm.lastBuiltAt = out.id, time.Now()
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
		zap.Duration("took", time.Since(start))}
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

package validator

import (
	"bytes"
	"container/heap"
	"context"
	"errors"
	"fmt"
	"math/big"
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
	ethcommon "github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core"
	"github.com/ava-labs/libevm/core/txpool"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/rlp"
	"github.com/holiman/uint256"
	"go.uber.org/zap"
)

// retryDelay: subnet-evm's minimum gap between two build attempts on the
// same parent.
const retryDelay = 100 * time.Millisecond

// builder decides WHEN to build (subnet-evm's blockBuilder rules): the pool
// has executable txs, and the timing rules (retry delay, Granite minimum
// block delay) allow it. The pool's reset after a head change is async; a
// build that races it hands the engine a few already-mined txs, which it
// skips (code 1).
type builder struct {
	pool  *txpool.TxPool
	chain *poolChain
	mu    sync.Mutex
	cond  *lock.Cond

	normalOp        bool
	lastBuildTime   time.Time
	lastBuildParent ethcommon.Hash
	admitted        atomic.Uint64
}

func newBuilder(pool *txpool.TxPool, chain *poolChain) *builder {
	b := &builder{pool: pool, chain: chain}
	b.cond = lock.NewCond(&b.mu)
	return b
}

// run wakes waitForEvent on every promotion (new txs, or a reset after a
// head change) and counts admitted txs.
func (b *builder) run(ctx context.Context) {
	txs := make(chan core.NewTxsEvent, 16)
	sub := b.pool.SubscribeTransactions(txs, true)
	defer sub.Unsubscribe()
	for {
		select {
		case <-ctx.Done():
			return
		case ev := <-txs:
			b.admitted.Add(uint64(len(ev.Txs)))
			b.mu.Lock()
			b.cond.Broadcast()
			b.mu.Unlock()
		}
	}
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

func (b *builder) waitForEvent(ctx context.Context, head *types.Header) (common.Message, error) {
	b.mu.Lock()
	for !b.normalOp || !b.hasPending() {
		if err := b.cond.Wait(ctx); err != nil {
			b.mu.Unlock()
			return 0, err
		}
	}
	lastTime, lastParent := b.lastBuildTime, b.lastBuildParent
	b.mu.Unlock()

	var next time.Time
	if lastParent == head.Hash() && !lastTime.IsZero() {
		next = lastTime.Add(retryDelay) // a retry on the same parent: the block was not accepted
	} else {
		next = minNextBlockTime(head) // Granite: the wait lives here, not in BuildBlock
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

// hasPending: the pool is on the accepted head and holds an executable tx.
// While the head moves the count still holds the mined txs (chain.onMoved
// wakes us when the reset lands). Stats is O(accounts); Pending would copy
// every pending tx, and pool.Sync FORCES a full reset (it is the
// simulator's hook), which under a 100k-tx pool starved every other pool
// user and stalled the chain (E2E.md run 3).
func (b *builder) hasPending() bool {
	if !b.chain.isSettled() {
		return false
	}
	pending, _ := b.pool.Stats()
	return pending > 0
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

// The miner's block size target (subnet-evm: 1800 KiB of tx bytes), with
// slack: candidates past it can only be popped by the engine (skip code 5).
const (
	maxCandidateBytes = 1800*1024 + 1800*1024/8
	skipSize          = 5
)

// buildBlock selects candidates from the pool by effective tip and nonce
// (the miner's order), capped at 1.5x the gas limit, and hands them to the
// engine in one crossing (two when the limit is not filled and the pool has
// more). The engine executes, builds the header (base fee, gas cost, fee
// window, Granite times) and keeps the result as a verified pending block.
func (vm *VM) buildBlock(pchainHeight uint64) (snowman.Block, error) {
	start := time.Now()
	head, headID := vm.chain.current()
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
	extra := sevmparams.GetExtra(vm.config)
	feeConfig := extra.FeeConfig
	baseFee, err := customheader.BaseFee(extra, feeConfig, parent, tsMS)
	if err != nil {
		return nil, err
	}
	gasLimit, err := customheader.GasLimit(extra, feeConfig, parent, tsMS)
	if err != nil {
		return nil, err
	}

	filter := txpool.PendingFilter{OnlyPlainTxs: true}
	if baseFee != nil {
		filter.BaseFee = uint256.MustFromBig(baseFee)
	}
	var ph phases
	ph.lap("head") // header lookups, fee math, the Granite wait
	pending := vm.pool.Pending(filter)
	ph.lap("pending")
	order := newByPriceAndNonce(pending, baseFee)
	ph.lap("order")
	budget := gasLimit + gasLimit/2
	txs, gas, size := order.take(nil, budget, maxCandidateBytes)
	ph.lap("take")
	if len(txs) == 0 {
		return nil, errNoTxs
	}
	out, err := vm.buildOnce(parentID, tsMS, pchainHeight, txs, &ph)
	if err != nil {
		return nil, err
	}
	rounds := 1
	// A second round only when the engine ran out of candidates for gas, not
	// for size: a size-popped candidate (code 5) means the block is full.
	if out.needsMore && !order.empty() && size < maxCandidateBytes && !bytes.Contains(out.skipped, []byte{skipSize}) {
		txs, _, _ = order.take(txs, gas+budget, maxCandidateBytes)
		ph.lap("take2")
		if out, err = vm.buildOnce(parentID, tsMS, pchainHeight, txs, &ph); err != nil {
			return nil, err
		}
		rounds++
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
	fields := []zap.Field{zap.Uint64("height", parent.Number.Uint64()+1), zap.Uint64("included", out.included),
		zap.Int("candidates", len(txs)), zap.Int("pendingAccounts", len(pending)), zap.Int("rounds", rounds), zap.Uint64("gasUsed", out.gasUsed),
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

// buildOnce: one engine crossing; epochdb_build_seconds is this call alone
// (BuildBlock's wall time also holds the Granite min-delay wait).
func (vm *VM) buildOnce(parent ids.ID, tsMS, pchainHeight uint64, txs types.Transactions, ph *phases) (buildOut, error) {
	raw, err := rlp.EncodeToBytes(txs)
	if err != nil {
		return buildOut{}, err
	}
	// The pool recovered every sender at admission (cached in the tx), so the
	// engine does not recover again: that was 83% of its build time.
	senders := make([]byte, 0, 20*len(txs))
	for _, tx := range txs {
		from, _ := types.Sender(vm.signer, tx) // an error leaves zeros: the engine recovers that one
		senders = append(senders, from[:]...)
	}
	ph.lap("rlp")
	start := time.Now()
	out, err := vm.eng.build(parent, tsMS, vm.coinbase(), pchainHeight, raw, senders)
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

// byPriceAndNonce is the miner's transactionsByPriceAndNonce: a heap of each
// sender's next tx keyed by effective tip (min(tipCap, feeCap - baseFee)),
// ties by arrival time; per-sender lists stay in nonce order.
type byPriceAndNonce struct {
	txs     map[ethcommon.Address][]*txpool.LazyTransaction
	heads   tipHeap
	baseFee *uint256.Int
}

type tipHead struct {
	tx   *txpool.LazyTransaction
	from ethcommon.Address
	fee  *uint256.Int
}

type tipHeap []*tipHead

func (h tipHeap) Len() int { return len(h) }
func (h tipHeap) Less(i, j int) bool {
	if c := h[i].fee.Cmp(h[j].fee); c != 0 {
		return c > 0
	}
	return h[i].tx.Time.Before(h[j].tx.Time)
}
func (h tipHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *tipHeap) Push(x interface{}) { *h = append(*h, x.(*tipHead)) }
func (h *tipHeap) Pop() interface{} {
	old := *h
	x := old[len(old)-1]
	*h = old[:len(old)-1]
	return x
}

func newByPriceAndNonce(txs map[ethcommon.Address][]*txpool.LazyTransaction, baseFee *big.Int) *byPriceAndNonce {
	o := &byPriceAndNonce{txs: txs, heads: make(tipHeap, 0, len(txs))}
	if baseFee != nil {
		o.baseFee = uint256.MustFromBig(baseFee)
	}
	for from, list := range txs {
		if h := o.head(from, list[0]); h != nil {
			o.heads = append(o.heads, h)
			o.txs[from] = list[1:]
		}
	}
	heap.Init(&o.heads)
	return o
}

func (o *byPriceAndNonce) head(from ethcommon.Address, tx *txpool.LazyTransaction) *tipHead {
	fee := new(uint256.Int).Set(tx.GasTipCap)
	if o.baseFee != nil {
		if tx.GasFeeCap.Cmp(o.baseFee) < 0 {
			return nil // cannot pay the base fee
		}
		if cap := new(uint256.Int).Sub(tx.GasFeeCap, o.baseFee); cap.Cmp(fee) < 0 {
			fee = cap
		}
	}
	return &tipHead{tx: tx, from: from, fee: fee}
}

func (o *byPriceAndNonce) empty() bool { return len(o.heads) == 0 }

// take appends resolved txs in order to dst until the summed gas limits
// reach budget or the summed sizes reach maxBytes; a sender whose next tx
// cannot pay the base fee is dropped.
func (o *byPriceAndNonce) take(dst types.Transactions, budget uint64, maxBytes uint64) (types.Transactions, uint64, uint64) {
	var gas, size uint64
	for _, tx := range dst {
		size += tx.Size()
	}
	for len(o.heads) > 0 && gas < budget && size < maxBytes {
		h := o.heads[0]
		if tx := h.tx.Resolve(); tx != nil {
			dst = append(dst, tx)
			gas += h.tx.Gas
			size += tx.Size()
		}
		if rest := o.txs[h.from]; len(rest) > 0 {
			if nh := o.head(h.from, rest[0]); nh != nil {
				o.txs[h.from] = rest[1:]
				o.heads[0] = nh
				heap.Fix(&o.heads, 0)
				continue
			}
		}
		heap.Pop(&o.heads)
	}
	return dst, gas, size
}

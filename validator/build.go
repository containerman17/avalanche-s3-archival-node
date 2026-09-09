package validator

import (
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
// has executable txs at the current head (pool.Sync waits for its reset),
// and the timing rules (retry delay, Granite minimum block delay) allow it.
type builder struct {
	pool *txpool.TxPool
	mu   sync.Mutex
	cond *lock.Cond

	normalOp        bool
	lastBuildTime   time.Time
	lastBuildParent ethcommon.Hash
	admitted        atomic.Uint64
}

func newBuilder(pool *txpool.TxPool) *builder {
	b := &builder{pool: pool}
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

// hasPending: the pool, reset to the latest head, holds an executable tx.
func (b *builder) hasPending() bool {
	b.pool.Sync()
	return len(b.pool.Pending(txpool.PendingFilter{OnlyPlainTxs: true})) > 0
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

	vm.pool.Sync() // the pool's view of nonces is the accepted head's
	filter := txpool.PendingFilter{OnlyPlainTxs: true}
	if baseFee != nil {
		filter.BaseFee = uint256.MustFromBig(baseFee)
	}
	order := newByPriceAndNonce(vm.pool.Pending(filter), baseFee)
	budget := gasLimit + gasLimit/2
	txs, gas := order.take(nil, budget)
	if len(txs) == 0 {
		return nil, errNoTxs
	}
	out, err := vm.buildOnce(parentID, tsMS, pchainHeight, txs)
	if err != nil {
		return nil, err
	}
	if out.needsMore && !order.empty() {
		txs, _ = order.take(txs, gas+budget)
		if out, err = vm.buildOnce(parentID, tsMS, pchainHeight, txs); err != nil {
			return nil, err
		}
	}
	if out.included == 0 {
		return nil, errNoTxs
	}
	vm.b.built(parent.Hash())
	vm.m.buildTxs.Observe(float64(out.included))
	vm.ctx.Log.Debug("validator: built", zap.Uint64("height", parent.Number.Uint64()+1), zap.Uint64("included", out.included),
		zap.Int("candidates", len(txs)), zap.Uint64("gasUsed", out.gasUsed), zap.Duration("took", time.Since(start)))
	return &Block{vm: vm, raw: out.block, id: out.id, parent: parentID, height: parent.Number.Uint64() + 1, time: tsMS / 1000}, nil
}

// buildOnce: one engine crossing; epochdb_build_seconds is this call alone
// (BuildBlock's wall time also holds the Granite min-delay wait).
func (vm *VM) buildOnce(parent ids.ID, tsMS, pchainHeight uint64, txs types.Transactions) (buildOut, error) {
	raw, err := rlp.EncodeToBytes(txs)
	if err != nil {
		return buildOut{}, err
	}
	start := time.Now()
	out, err := vm.eng.build(parent, tsMS, vm.coinbase(), pchainHeight, raw)
	vm.m.build.Observe(time.Since(start).Seconds())
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
// reach budget; a sender whose next tx cannot pay the base fee is dropped.
func (o *byPriceAndNonce) take(dst types.Transactions, budget uint64) (types.Transactions, uint64) {
	var gas uint64
	for len(o.heads) > 0 && gas < budget {
		h := o.heads[0]
		if tx := h.tx.Resolve(); tx != nil {
			dst = append(dst, tx)
			gas += h.tx.Gas
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
	return dst, gas
}

package vmchain

import (
	"container/heap"
	"runtime"
	"sync"

	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/params"
)

// Lookahead budget: blocks and transactions queued for recovery at once.
// Past it a submission is dropped (the executor recovers what it finds
// cold), so a host that parses far ahead cannot pin unbounded memory here.
const (
	maxQueuedBlocks = 4000
	maxQueuedTxs    = 200_000
)

// recoverer runs types.Sender over parsed blocks' transactions on
// NumCPU-2 goroutines, lowest height first. types.Sender caches the address
// on the tx object, so a later Sender on the same object is a map hit.
//
// ponytail: with vmexec driven through a []byte BlockSource, the executor
// re-decodes the block and recovers on its own objects; this pool pays off
// once vmexec takes decoded blocks from the VM (a small change in Run's
// prefetcher). Until then it is the parse-time hook the boundary asks for,
// verified by the test, and it keeps the same queue a decoded source needs.
type recoverer struct {
	cfg  *params.ChainConfig
	live func() uint64 // the executed head: blocks at or below it are dropped unrecovered

	mu      sync.Mutex
	cond    *sync.Cond
	queue   blockHeap
	txs     int // transactions in queue
	busy    int // blocks being recovered right now
	done    uint64
	dropped uint64
	closed  bool
}

func newRecoverer(cfg *params.ChainConfig, live func() uint64) *recoverer {
	r := &recoverer{cfg: cfg, live: live}
	r.cond = sync.NewCond(&r.mu)
	for i := 0; i < max(runtime.NumCPU()-2, 1); i++ {
		go r.work()
	}
	return r
}

// submit queues blocks for recovery; blocks the executor is past and empty
// blocks are skipped, and the budget drops the rest.
func (r *recoverer) submit(blks ...*types.Block) {
	r.mu.Lock()
	defer r.mu.Unlock()
	live := r.live()
	for r.queue.Len() > 0 && r.queue[0].NumberU64() <= live {
		r.txs -= len(heap.Pop(&r.queue).(*types.Block).Transactions())
	}
	for _, b := range blks {
		n := len(b.Transactions())
		if n == 0 || b.NumberU64() <= live {
			continue
		}
		if r.queue.Len() >= maxQueuedBlocks || r.txs+n > maxQueuedTxs {
			r.dropped++
			continue
		}
		heap.Push(&r.queue, b)
		r.txs += n
	}
	r.cond.Broadcast()
}

func (r *recoverer) work() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for {
		for r.queue.Len() == 0 && !r.closed {
			r.cond.Wait()
		}
		if r.closed {
			return
		}
		b := heap.Pop(&r.queue).(*types.Block)
		r.txs -= len(b.Transactions())
		r.busy++
		r.mu.Unlock()
		if b.NumberU64() > r.live() {
			signer := types.MakeSigner(r.cfg, b.Number(), b.Time())
			for _, tx := range b.Transactions() {
				types.Sender(signer, tx)
			}
		}
		r.mu.Lock()
		r.busy--
		r.done++
		r.cond.Broadcast()
	}
}

// idle reports whether nothing is queued or in flight.
func (r *recoverer) idle() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.queue.Len() == 0 && r.busy == 0
}

func (r *recoverer) stats() (done, dropped uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.done, r.dropped
}

func (r *recoverer) close() {
	r.mu.Lock()
	r.closed = true
	r.cond.Broadcast()
	r.mu.Unlock()
}

// blockHeap is a min-heap by block number.
type blockHeap []*types.Block

func (h blockHeap) Len() int           { return len(h) }
func (h blockHeap) Less(i, j int) bool { return h[i].NumberU64() < h[j].NumberU64() }
func (h blockHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *blockHeap) Push(x any)        { *h = append(*h, x.(*types.Block)) }
func (h *blockHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return x
}

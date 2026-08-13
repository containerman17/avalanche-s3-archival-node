package fetch

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/ava-labs/avalanchego/ids"
	avacommon "github.com/ava-labs/avalanchego/snow/engine/common"
	"github.com/ava-labs/avalanchego/subnets"
	"github.com/ava-labs/avalanchego/utils/set"
)

// FETCH ASCENDS AND STAGING DOES NOT EXIST (DESIGN, rulings 2026-08-13).
//
// The forward-walk primitive is in the consensus protocol itself: a PullQuery
// carries a RequestedHeight and the peer's Chits answer with
// GetBlockIDAtHeight(requestedHeight) for any height below its last accepted
// (avalanchego v1.15.0-fuji, snow/engine/snowman/engine.go sendChits). That is
// the one thing a backward walk had and a forward one did not: an entry point
// at an arbitrary height. Everything else here is flow control:
//
//   - THE WINDOW RULE, the whole of it: fetch target = min(real tip, executed +
//     WindowBlocks), refill when the gap ahead falls under RefillBelow.
//   - FORWARD VERIFICATION: a block joins the queue only if its parent hash is
//     the hash of the block before it, anchored at the genesis block (a fresh
//     dir) or at the last block the runs hold (a resume). A lying peer is
//     caught by the very next link, its span is discarded and re-asked
//     elsewhere.
//   - Fetched-not-executed containers live HERE, in RAM, and nowhere else.
const (
	// WindowBlocks is how far ahead of the executor the fetch may run.
	// BUDGET: at numine's ~1KB containers that is ~100MB and at mainnet C's
	// ~4-10KB ~0.4-1GB, and at the executor's measured 2,900 blk/s it is ~34s
	// of runway. queueMaxBytes is the hard stop for a chain whose blocks are
	// bigger than either guess.
	WindowBlocks = 100_000
	// RefillBelow is the other half of the window rule: a new burst of spans
	// is scheduled once the gap ahead of the executor falls under it.
	RefillBelow = 50_000
	// queueMaxBytes bounds the queue in bytes whatever the block size, because
	// the block count alone is a guess about a chain we may never have seen.
	// Scheduling pauses above it; the executor drains it in seconds.
	queueMaxBytes = 1 << 30

	// KeepBehind is how many executed containers the queue holds BELOW the
	// executor's cursor. exec re-reads a batch per block when its root
	// mismatches (bisect), and its prefetcher runs up to 512 blocks ahead of
	// the block actually executed, so the retained tail must cover both.
	// exec.New refuses a CommitEvery that would need more.
	KeepBehind = 8_192

	// spanBlocks is how much history ONE PullQuery seeds. A GetAncestors
	// answer carries 660-944 containers, so a span is a handful of round
	// trips per poll: the poll is what makes the fetch forward, the ancestors
	// batch is what makes it fast (one message per ~800 blocks, against one
	// per block if every height were resolved on its own).
	spanBlocks = 4_096
	// fetchSpans is how many spans are in flight. Measured ceiling on Fuji:
	// 16 concurrent fetches = 2,300 blk/s, 24 = 500, i.e. more is slower.
	fetchSpans = 16
	// pollFanout is how many peers one height-resolution poll asks.
	pollFanout = 4
)

// Queue is the bounded RAM handoff between the fetcher and the executor, and
// the only place a fetched-not-executed container ever lives: the durable copy
// of an unexecuted block is the network (DESIGN). It holds one contiguous,
// hash-verified run of containers: [low..head], consumed up to `consumed`.
type Queue struct {
	mu   sync.Mutex
	wake *sync.Cond
	blk  map[uint64][]byte

	head     uint64 // highest verified height held
	headHash ids.ID // its eth block hash, the anchor of the next link
	low      uint64 // lowest height still held
	consumed uint64 // highest height handed to the executor
	bytes    uint64
	peak     uint64
	closed   bool
}

// newQueue starts a queue whose first block is `from`, verified against
// anchorHash (the eth hash of block from-1).
func newQueue(ctx context.Context, from uint64, anchorHash ids.ID) *Queue {
	q := &Queue{
		blk:      make(map[uint64][]byte),
		head:     from - 1,
		headHash: anchorHash,
		low:      from,
		consumed: from - 1,
	}
	q.wake = sync.NewCond(&q.mu)
	go func() {
		<-ctx.Done()
		q.mu.Lock()
		q.closed = true
		q.mu.Unlock()
		q.wake.Broadcast()
	}()
	return q
}

// Append takes one container if it is the next one AND its parent hash is the
// hash of the block already at the head. That single line is the forward
// verification: nothing else in this package decides whether a peer told the
// truth.
func (q *Queue) Append(p parsedContainer, raw []byte) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if p.blockNumber != q.head+1 || p.parentHash != q.headHash {
		return false
	}
	q.blk[p.blockNumber] = raw
	q.head = p.blockNumber
	q.headHash = p.blockHash
	q.bytes += uint64(len(raw))
	if q.bytes > q.peak {
		q.peak = q.bytes
	}
	q.wake.Broadcast()
	return true
}

// GetByHeight is exec.BlockSource. It BLOCKS until the height lands, because
// with no staging on disk "not here yet" and "not here at all" are the same
// answer and only the fetcher knows which: it returns an error for a height
// the queue has already released, and for a closed queue (the node is going
// down), and otherwise waits.
func (q *Queue) GetByHeight(n uint64) ([]byte, bool, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for {
		if raw, ok := q.blk[n]; ok {
			if n > q.consumed {
				q.consumed = n
				q.release(n)
			}
			return raw, true, nil
		}
		if n < q.low {
			return nil, false, fmt.Errorf("fetch: container %d was already released from the RAM queue (it holds %d..%d)", n, q.low, q.head)
		}
		if q.closed {
			return nil, false, context.Canceled
		}
		q.wake.Wait()
	}
}

// release drops everything more than KeepBehind blocks below the executor.
func (q *Queue) release(upTo uint64) {
	if upTo < KeepBehind {
		return
	}
	for ; q.low+KeepBehind <= upTo; q.low++ {
		if raw, ok := q.blk[q.low]; ok {
			q.bytes -= uint64(len(raw))
			delete(q.blk, q.low)
		}
	}
}

func (q *Queue) Head() uint64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.head
}

func (q *Queue) Consumed() uint64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.consumed
}

// Bytes is the queue's live size; Peak is the high-water mark of the run.
func (q *Queue) Bytes() (live, peak uint64) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.bytes, q.peak
}

// StartForward opens the queue at `from` (anchored on the eth hash of block
// from-1) and starts the ascending fetch. The returned Queue is the executor's
// block source.
func (f *Fetcher) StartForward(ctx context.Context, from uint64, anchorHash ids.ID) *Queue {
	if from == 0 {
		from = 1
	}
	f.q = newQueue(ctx, from, anchorHash)
	go f.probeLoop(ctx)
	go f.forward(ctx)
	return f.q
}

// Queue is the fetcher's RAM queue, nil until StartForward.
func (f *Fetcher) Queue() *Queue { return f.q }

// tipTarget is the real tip, or the --tip-override ceiling when one is set.
// Zero means nothing has told us where the chain ends yet.
func (f *Fetcher) tipTarget() uint64 {
	tip := f.tip.Load()
	if c := f.ceiling.Load(); c > 0 && (tip == 0 || c < tip) {
		return c
	}
	return tip
}

// noteTip raises the known tip. Every Chits answer carries the peer's accepted
// height, so the window's ceiling comes free with the height resolution and
// does not wait for the consensus follower to go live.
func (f *Fetcher) noteTip(h uint64) {
	for {
		cur := f.tip.Load()
		if h <= cur || f.tip.CompareAndSwap(cur, h) {
			return
		}
	}
}

type span struct{ lo, hi uint64 }

type rawBlock struct {
	p   parsedContainer
	raw []byte
}

type spanResult struct {
	span
	blocks []rawBlock // ascending
}

// forward is the window rule and nothing else: hand spans to the workers while
// the gap ahead of the executor is under the window, join the answers in
// ascending order, re-ask on a broken link.
func (f *Fetcher) forward(ctx context.Context) {
	jobs := make(chan span)
	results := make(chan spanResult, fetchSpans)
	var wg sync.WaitGroup
	for i := 0; i < fetchSpans; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for s := range jobs {
				r, ok := f.fetchSpan(ctx, s)
				if !ok {
					return
				}
				select {
				case results <- r:
				case <-ctx.Done():
					return
				}
			}
		}()
	}
	defer func() {
		close(jobs)
		wg.Wait()
	}()

	var (
		next     = f.q.Head() + 1 // next height not yet scheduled
		redo     []span           // spans a broken link sent back
		pending  = map[uint64]spanResult{}
		inflight int
		filling  = true
		job      span
		out      chan span // nil unless a job is ready to hand out
		lastTipQ time.Time
	)
	tick := time.NewTicker(200 * time.Millisecond)
	defer tick.Stop()

	for {
		// Join the span that covers the next height. A span whose blocks the
		// follower already delivered is dropped; a span whose first block does
		// not link is discarded from the offending height up and re-asked.
		for {
			want := f.q.Head() + 1
			r, ok := spanned(pending, want)
			if !ok {
				break
			}
			delete(pending, r.lo)
			if bad, ok := f.joinSpan(r); !ok {
				redo = append(redo, span{lo: bad, hi: r.hi})
			}
		}

		// The window rule, one line each way.
		head, consumed := f.q.Head(), f.q.Consumed()
		if next <= head {
			// The follower delivered the tip zone under us.
			next = head + 1
		}
		if head-consumed < RefillBelow {
			filling = true
		}
		if next > consumed+WindowBlocks {
			filling = false
		}
		target := f.tipTarget()
		if c := consumed + WindowBlocks; c < target {
			target = c
		}
		live, _ := f.q.Bytes()
		if out == nil && filling && live < queueMaxBytes && inflight < fetchSpans {
			switch {
			case len(redo) > 0:
				job, redo = redo[0], redo[1:]
				out = jobs
			case next <= target:
				job = span{lo: next, hi: min(next+spanBlocks-1, target)}
				next = job.hi + 1
				out = jobs
			}
		}
		if target == 0 && time.Since(lastTipQ) > 5*time.Second {
			lastTipQ = time.Now()
			go f.discoverTip(ctx)
		}

		select {
		case out <- job:
			out, inflight = nil, inflight+1
		case r := <-results:
			inflight--
			pending[r.lo] = r
		case <-tick.C:
		case <-ctx.Done():
			return
		}
	}
}

// spanned picks the arrived span that covers height `want` and forgets the
// ones the follower has already carried past. Spans are disjoint, so at most
// one can cover it; the map is never larger than the number of workers.
func spanned(pending map[uint64]spanResult, want uint64) (spanResult, bool) {
	var hit spanResult
	found := false
	for lo, r := range pending {
		switch {
		case r.hi < want:
			delete(pending, lo)
		case lo <= want:
			hit, found = r, true
		}
	}
	return hit, found
}

// joinSpan appends a span's blocks in ascending order. ok=false names the
// height whose parent hash did not match, which is where the span is cut and
// re-asked from another peer.
func (f *Fetcher) joinSpan(r spanResult) (uint64, bool) {
	for _, b := range r.blocks {
		if b.p.blockNumber <= f.q.Head() {
			continue // the follower got there first
		}
		if !f.q.Append(b.p, b.raw) {
			f.badLinks.Add(1)
			log.Printf("fetch: container %d does not link to the block below it; discarding [%d..%d] and asking another peer",
				b.p.blockNumber, b.p.blockNumber, r.hi)
			return b.p.blockNumber, false
		}
	}
	return 0, true
}

// fetchSpan resolves the span's top height to a container and walks the
// ancestors batches down to its floor. ok=false means the context ended.
//
// The batch is the protocol's own bulk primitive and it arrives newest-first;
// nothing about that reaches the executor, which is fed strictly ascending out
// of the queue.
func (f *Fetcher) fetchSpan(ctx context.Context, s span) (spanResult, bool) {
	resp, cur, ok := f.seedSpan(ctx, s.hi)
	if !ok {
		return spanResult{}, false
	}
	blocks := make([]rawBlock, 0, s.hi-s.lo+1)
	for {
		done := false
		n := 0
		for _, raw := range resp.blocks {
			p, err := parseContainer(raw)
			if err != nil {
				log.Printf("fetch: undecodable container in an ancestors batch: %v", err)
				break
			}
			// The batch is a parent chain: every container must be the one the
			// previous one named, so a peer cannot splice history into it.
			if p.containerID != cur {
				break
			}
			blocks = append(blocks, rawBlock{p: p, raw: raw})
			cur = p.parentID
			n++
			if p.blockNumber <= s.lo {
				done = true
				break
			}
		}
		if done {
			break
		}
		if ctx.Err() != nil {
			return spanResult{}, false
		}
		if n == 0 {
			// The peer answered with something that does not continue this
			// walk; fetchAncestors has already demoted it, so ask again.
			log.Printf("fetch: no usable container for %s, re-asking", cur)
		}
		if resp, _, ok = f.fetchAncestors(ctx, cur); !ok {
			return spanResult{}, false
		}
	}
	// Ascending, and trimmed to the span: the last batch overshoots the floor
	// by whatever the peer had room for.
	out := make([]rawBlock, 0, len(blocks))
	for i := len(blocks) - 1; i >= 0; i-- {
		if blocks[i].p.blockNumber >= s.lo {
			out = append(out, blocks[i])
		}
	}
	return spanResult{span: s, blocks: out}, true
}

// seedSpan turns a HEIGHT into a container: poll peers for the block ID at
// that height, then fetch it. A peer that has pruned the height answers with
// its LAST ACCEPTED id instead (the documented fallback in sendChits), which
// is detectable for free because the container says what height it is; that
// candidate is dropped and the next one tried.
func (f *Fetcher) seedSpan(ctx context.Context, h uint64) (ancestorsResponse, ids.ID, bool) {
	for {
		if ctx.Err() != nil {
			return ancestorsResponse{}, ids.Empty, false
		}
		for _, id := range f.pollIDAt(ctx, h) {
			resp, _, ok := f.fetchAncestors(ctx, id)
			if !ok {
				return ancestorsResponse{}, ids.Empty, false
			}
			p, err := parseContainer(resp.blocks[0])
			if err != nil {
				continue
			}
			if p.blockNumber != h {
				// The pruned-peer fallback, in the wild.
				f.prunedSeeds.Add(1)
				continue
			}
			return resp, id, true
		}
	}
}

// pollIDAt asks pollFanout peers for the container ID at height h and returns
// the candidates, most-voted first. Agreement is a hint, never a proof: the
// hash chain is what accepts or rejects the containers this seeds.
func (f *Fetcher) pollIDAt(ctx context.Context, h uint64) []ids.ID {
	peers := f.pool.peersForFrontier(pollFanout)
	if len(peers) == 0 {
		select {
		case <-ctx.Done():
		case <-time.After(200 * time.Millisecond):
		}
		return nil
	}
	reqID := f.reqIDCounter.Add(1)
	ch := make(chan chitsAnswer, len(peers))
	f.handler.registerChitsRoute(reqID, ch)
	defer f.handler.unregisterChitsRoute(reqID)

	msg, err := f.creator.PullQuery(f.chainID, reqID, defaultRequestTimeout, ids.Empty, h)
	if err != nil {
		logLocalBug("PullQuery", err)
		return nil
	}
	to := set.Of(peers...)
	f.pollsSent.Add(1)
	noteSendGap("PullQuery", to, f.net.Send(msg, avacommon.SendConfig{NodeIDs: to}, f.subnetID, subnets.NoOpAllower))

	timeout := time.NewTimer(defaultRequestTimeout)
	defer timeout.Stop()
	votes := make(map[ids.ID]int, len(peers))
	order := make([]ids.ID, 0, len(peers))
collect:
	for range peers {
		select {
		case a := <-ch:
			f.noteTip(a.acceptedHeight)
			if a.atHeight == ids.Empty {
				continue
			}
			if votes[a.atHeight] == 0 {
				order = append(order, a.atHeight)
			}
			votes[a.atHeight]++
		case <-timeout.C:
			break collect
		case <-ctx.Done():
			return nil
		}
	}
	// Most-voted first, ties in arrival order: a stable order costs nothing
	// and keeps a run reproducible when peers disagree.
	for i := 1; i < len(order); i++ {
		for j := i; j > 0 && votes[order[j]] > votes[order[j-1]]; j-- {
			order[j], order[j-1] = order[j-1], order[j]
		}
	}
	return order
}

// discoverTip learns where the chain ends without waiting for the consensus
// follower: the accepted frontier is a consensus message every peer answers,
// and the container it names says its own height.
func (f *Fetcher) discoverTip(ctx context.Context) {
	id, err := f.acceptedFrontier(ctx)
	if err != nil {
		log.Printf("fetch: accepted frontier: %v", err)
		return
	}
	resp, _, ok := f.fetchAncestors(ctx, id)
	if !ok {
		return
	}
	p, err := parseContainer(resp.blocks[0])
	if err != nil {
		return
	}
	log.Printf("fetch: chain tip is height %d (container %s)", p.blockNumber, id)
	f.noteTip(p.blockNumber)
}

// ResolveAnchor fetches one container and returns it with its parsed height.
// That parse is the ONLY height source for a --tip-override container ID: the
// CLI names a physical block, and the chain itself says where it sits.
func (f *Fetcher) ResolveAnchor(ctx context.Context, id ids.ID) (Anchor, error) {
	resp, _, ok := f.fetchAncestors(ctx, id)
	if !ok {
		return Anchor{}, fmt.Errorf("resolve container %s: %w", id, ctx.Err())
	}
	p, err := parseContainer(resp.blocks[0])
	if err != nil {
		return Anchor{}, fmt.Errorf("resolve container %s: %w", id, err)
	}
	if p.containerID != id {
		return Anchor{}, fmt.Errorf("peer served container %s for %s", p.containerID, id)
	}
	return Anchor{ID: id, Height: p.blockNumber}, nil
}

// Anchor is a container ID with the height its own bytes report.
type Anchor struct {
	ID     ids.ID
	Height uint64
}

// SetCeiling is --tip-override: the forward fetch runs to this height and
// STOPS THERE, which is where it stops trusting the network. No consensus
// follower runs beside it, so the corpus a run builds is fixed and
// reproducible.
func (f *Fetcher) SetCeiling(h uint64) {
	f.ceiling.Store(h)
	f.syncTarget.Store(h)
}

// SyncTarget is the ceiling of a bounded (--tip-override) run, 0 while
// following. It is what a backfill measures progress against, because there is
// no accepted head to measure against.
func (f *Fetcher) SyncTarget() uint64 { return f.syncTarget.Load() }

// AcceptedHead is the highest height the network has told us it accepted.
func (f *Fetcher) AcceptedHead() uint64 { return f.tip.Load() }

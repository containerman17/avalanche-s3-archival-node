package fetch

import (
	"context"
	"encoding/binary"
	"testing"
	"time"

	"github.com/ava-labs/avalanchego/ids"
)

// chainOf builds a fake ascending chain: block h's hash is h, its parent hash
// h-1, which is exactly what forward verification checks.
func chainOf(h uint64) (parsedContainer, []byte) {
	var hash, parent ids.ID
	binary.BigEndian.PutUint64(hash[:8], h)
	binary.BigEndian.PutUint64(parent[:8], h-1)
	return parsedContainer{
		containerID: hash,
		blockNumber: h,
		blockHash:   hash,
		parentHash:  parent,
	}, []byte{byte(h)}
}

// TestQueueVerifiesForwardAndBounds is the whole contract of the RAM queue:
// strictly ascending, parent-hash linked, blocking on a height that has not
// landed, and releasing what the executor is done with.
func TestQueueVerifiesForwardAndBounds(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	anchor, _ := chainOf(10)
	q := newQueue(ctx, 11, anchor.blockHash)

	p, raw := chainOf(11)
	if !q.Append(p, raw) {
		t.Fatal("the first block after the anchor was refused")
	}
	// Out of order: a block that is not next.
	if p, raw := chainOf(13); q.Append(p, raw) {
		t.Fatal("a block two heights ahead was accepted")
	}
	// A liar: right height, wrong parent.
	bad, badRaw := chainOf(12)
	bad.parentHash = ids.GenerateTestID()
	if q.Append(bad, badRaw) {
		t.Fatal("a block whose parent hash does not link was accepted")
	}
	// A duplicate of what is already at the head.
	if p, raw := chainOf(11); q.Append(p, raw) {
		t.Fatal("a duplicate was accepted")
	}
	if got := q.Head(); got != 11 {
		t.Fatalf("head=%d after the refusals, want 11", got)
	}

	// A read of a height that has not landed blocks until it does.
	landed := make(chan struct{})
	go func() {
		defer close(landed)
		if _, ok, err := q.GetByHeight(12); !ok || err != nil {
			t.Errorf("GetByHeight(12) = %v, %v", ok, err)
		}
	}()
	select {
	case <-landed:
		t.Fatal("GetByHeight returned before the block landed")
	case <-time.After(50 * time.Millisecond):
	}
	if p, raw := chainOf(12); !q.Append(p, raw) {
		t.Fatal("block 12 refused")
	}
	select {
	case <-landed:
	case <-time.After(2 * time.Second):
		t.Fatal("GetByHeight did not wake when the block landed")
	}

	// Released heights are an error, never a silent miss: with no staging on
	// disk there is nowhere else the executor could look.
	for h := uint64(13); h <= 13+KeepBehind; h++ {
		p, raw := chainOf(h)
		if !q.Append(p, raw) {
			t.Fatalf("block %d refused", h)
		}
	}
	if _, _, err := q.GetByHeight(13 + KeepBehind); err != nil {
		t.Fatal(err)
	}
	if live, peak := q.Bytes(); live > peak || peak == 0 {
		t.Fatalf("queue bytes live=%d peak=%d", live, peak)
	}
	if _, ok, err := q.GetByHeight(11); ok || err == nil {
		t.Fatalf("a released height answered ok=%v err=%v, want a loud error", ok, err)
	}

	// A closed queue wakes its waiters instead of hanging the executor.
	cancel()
	if _, _, err := q.GetByHeight(1 << 40); err == nil {
		t.Fatal("a closed queue did not report the shutdown")
	}
}

// TestSpannedPicksTheCoveringSpan pins the join rule: exactly the span that
// covers the next height, and the ones the follower already carried past are
// forgotten rather than waited for (which is how the tip handover used to
// wedge).
func TestSpannedPicksTheCoveringSpan(t *testing.T) {
	pending := map[uint64]spanResult{
		100: {span: span{lo: 100, hi: 200}},
		201: {span: span{lo: 201, hi: 300}},
		301: {span: span{lo: 301, hi: 400}},
	}
	r, ok := spanned(pending, 251)
	if !ok || r.lo != 201 {
		t.Fatalf("span for height 251 = %v, %v", r.span, ok)
	}
	if _, stale := pending[100]; stale {
		t.Fatal("a span entirely below the next height was kept")
	}
	if _, ok := spanned(pending, 500); ok {
		t.Fatal("a height no span covers was answered")
	}
	if len(pending) != 0 {
		t.Fatalf("spans left after everything was overtaken: %v", pending)
	}
}

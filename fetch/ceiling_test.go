package fetch

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/containerman17/epochdb/fetch/consensus"
)

// shortStagingPoll makes a paused walk re-check in milliseconds instead of the
// production seconds, so these tests measure the mechanism and not the timer.
func shortStagingPoll(t *testing.T) {
	t.Helper()
	old := stagingPollInterval
	stagingPollInterval = 20 * time.Millisecond
	t.Cleanup(func() { stagingPollInterval = old })
}

// stagedStore is a store holding one contiguous run in bucket 0, which is what
// a walk short-circuits over: with no ceiling in the way walkSpan returns from
// it immediately, so any delay in these tests is the ceiling and nothing else.
func stagedStore(t *testing.T) (*Store, parsedContainer) {
	t.Helper()
	s, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	var tip parsedContainer
	for h := uint64(1); h <= 10; h++ {
		p, raw := fakeContainer(h, 0xc0, 256)
		if err := s.Append(p, raw); err != nil {
			t.Fatal(err)
		}
		tip = p
	}
	if s.StagedBytes() == 0 {
		t.Fatal("nothing staged; the ceiling tests would prove nothing")
	}
	return s, tip
}

// TestWalkPausesOverTheCeilingAndResumesOnDrain is the safety valve itself: the
// walk stalls while retained staging is over the ceiling and continues by
// itself once sealing has retired the staging behind it. Retire IS the drain,
// and it runs here while the walk is paused, which is the other half of the
// contract: a paused fetcher must not be in the way of the seal that frees the
// space it is waiting for.
func TestWalkPausesOverTheCeilingAndResumesOnDrain(t *testing.T) {
	shortStagingPoll(t)
	s, tip := stagedStore(t)
	f := &Fetcher{store: s, dispatchErrCh: make(chan error, 1)}
	f.SetCeiling(s.StagedBytes() / 2) // already exceeded

	done := make(chan error, 1)
	go func() { done <- f.walkSpan(context.Background(), tip.containerID, 0) }()

	select {
	case err := <-done:
		t.Fatalf("walk ran over the ceiling (err=%v)", err)
	case <-time.After(200 * time.Millisecond):
	}
	if !f.Paused() {
		t.Fatal("walk is stalled but does not report itself paused")
	}

	// The seal, as serve calls it: floor raised FIRST, then raw unlinked and
	// staging accounted for again (cmd/epochdb/follow.go, adjacent lines). The
	// order is the invariant Retire relies on, so the test has to keep it: once
	// a bucket is retired its by-height index is gone, and only the floor stops
	// a walk from asking for a height that no longer has raw behind it.
	f.SetFloor(SegmentBlocks - 1)
	if err := s.Retire(SegmentBlocks - 1); err != nil {
		t.Fatal(err)
	}
	if got := s.StagedBytes(); got != 0 {
		t.Fatalf("staged=%d after retiring the only bucket, want 0", got)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("resumed walk: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("walk never resumed after the drain")
	}
	if f.Paused() {
		t.Fatal("walk resumed but still reports itself paused")
	}
}

// TestTipFollowerIsNeverThrottled: consensus acceptance is not a walk and must
// never wait on the ceiling. A node at the tip stages a handful of blocks a
// second, so the bound is about catch-up only; here the ceiling is as exceeded
// as it can be and every accepted container still lands at once.
func TestTipFollowerIsNeverThrottled(t *testing.T) {
	shortStagingPoll(t)
	s, _ := stagedStore(t)
	f := &Fetcher{store: s, dispatchErrCh: make(chan error, 1)}
	f.SetCeiling(1)

	// The bound really is engaged: a walk here would block until ctx died.
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := f.awaitStagingRoom(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("awaitStagingRoom over the ceiling returned %v, want the walk to be held", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for h := uint64(11); h <= 15; h++ {
		p, raw := fakeContainer(h, 0xd0, 256)
		if err := f.appendContainer(&consensus.Container{
			ID: p.containerID, Height: h, EthHash: p.blockHash, Bytes: raw,
		}); err != nil {
			t.Fatalf("accepted container at height %d: %v", h, err)
		}
	}
	if time.Now().After(deadline) {
		t.Fatal("accepting tip containers was throttled by the staging ceiling")
	}
	for h := uint64(11); h <= 15; h++ {
		if _, ok, err := s.GetByHeight(h); err != nil || !ok {
			t.Fatalf("accepted height %d is not staged (ok=%v err=%v)", h, ok, err)
		}
	}
}

// TestPausedWalkExitsOnContext is the stopped-executor case: nothing will ever
// drain the staging, so the pause is permanent, and it must still cost one idle
// goroutine and end the moment the process is asked to stop. The loop is a
// select on ctx and a timer, so there is no spin to measure; what is asserted
// is that it neither gives up on its own nor holds up the shutdown.
func TestPausedWalkExitsOnContext(t *testing.T) {
	shortStagingPoll(t)
	s, tip := stagedStore(t)
	f := &Fetcher{store: s, dispatchErrCh: make(chan error, 1)}
	f.SetCeiling(1)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- f.walkSpan(ctx, tip.containerID, 0) }()

	select {
	case err := <-done:
		t.Fatalf("paused walk returned on its own with a stopped executor: %v", err)
	case <-time.After(200 * time.Millisecond): // many poll intervals
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("walk exited with %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("paused walk did not exit on SIGINT; it would block shutdown")
	}
}

// TestStagingCeilingDefaultAndOverride: the default is a fraction of the free
// space of the dir (never zero on a real filesystem), the operator override is
// read from the environment, and an unparseable one is refused rather than
// silently ignored.
func TestStagingCeilingDefaultAndOverride(t *testing.T) {
	dir := t.TempDir()
	n, err := stagingCeiling(dir, 0)
	if err != nil || n == 0 {
		t.Fatalf("default ceiling for %s: %d, %v", dir, n, err)
	}
	t.Setenv("EPOCHDB_MAX_STAGING", "12345")
	if n, err := stagingCeiling(dir, 0); err != nil || n != 12345 {
		t.Fatalf("override: %d, %v", n, err)
	}
	t.Setenv("EPOCHDB_MAX_STAGING", "0") // the explicit opt-out
	if n, err := stagingCeiling(dir, 0); err != nil || n != 0 {
		t.Fatalf("opt-out: %d, %v", n, err)
	}
	t.Setenv("EPOCHDB_MAX_STAGING", "400GB")
	if _, err := stagingCeiling(dir, 0); err == nil {
		t.Fatal("a bad EPOCHDB_MAX_STAGING was accepted")
	}
}

// TestStagingCeilingDoesNotRatchetDown: restarting with staging already on disk
// must not shrink the budget. Free space at that point is the disk minus our own
// staging, so counting only free space makes every restart tighten the bound by
// whatever we were holding, until the walk pauses forever. Retained bytes are
// space sealing gives back, so they belong in the pool the share divides.
func TestStagingCeilingDoesNotRatchetDown(t *testing.T) {
	dir := t.TempDir()
	cold, err := stagingCeiling(dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	// A restart holding `retained` sees free space lower by exactly that much.
	const retained = 200 << 30
	warm, err := stagingCeiling(dir, retained)
	if err != nil {
		t.Fatal(err)
	}
	if warm <= cold {
		t.Fatalf("restart with %d B staged gave ceiling %d, not above the cold %d: the bound ratchets down",
			uint64(retained), warm, cold)
	}
	if got := warm - cold; got != retained/stagingFreeShare {
		t.Fatalf("retained %d B moved the ceiling by %d, want %d", uint64(retained), got, retained/stagingFreeShare)
	}
}

package validator

import (
	"context"
	"math/big"
	"testing"
	"time"

	ethcommon "github.com/ava-labs/libevm/common"
)

// armEmptyRetry marks the preferred parent (genesis) as the target of a
// just-finished EMPTY build, so the next wake takes the retry gap. lastBuildTime
// sets when that empty build "happened": now for the short-gap test, the future
// for the cancel test (a gap long enough to observe an interrupt).
func armEmptyRetry(h *harness, when time.Time) {
	_, headID := h.vm.current()
	h.vm.b.mu.Lock()
	h.vm.b.lastBuildParent = ethcommon.Hash(headID)
	h.vm.b.lastBuildEmpty = true
	h.vm.b.lastBuildTime = when
	h.vm.b.mu.Unlock()
}

// TestRetryGapEmptySameParent: a repeated build on the same preferred parent
// whose last build was empty still waits, but only the short retry gap
// (retryDelay), not the old 100 ms floor.
func TestRetryGapEmptySameParent(t *testing.T) {
	if !realEngine {
		t.Skip("stub engine")
	}
	h := newHarness(t)
	to := ethcommon.HexToAddress("0x1000000000000000000000000000000000000007")
	armEmptyRetry(h, time.Now())
	h.transfer(to, big.NewInt(1))
	t0 := time.Now()
	if _, err := h.vm.WaitForEvent(context.Background()); err != nil {
		t.Fatal(err)
	}
	d := time.Since(t0)
	if d < 3*time.Millisecond {
		t.Fatalf("empty same-parent retry did not wait (%v); the short gap must still throttle the empty-build spin", d)
	}
	if d > 70*time.Millisecond {
		t.Fatalf("empty same-parent retry waited %v, want ~%v (the short gap, not the 100 ms floor)", d, retryDelay)
	}
}

// TestRetryGapPreferenceCancels: a preference change during the retry sleep
// cancels it at once and the next evaluation sees sincePreferred ~ 0 (the new
// parent, not the same one, so no gap). The armed gap is ~500 ms; a cancel must
// return far sooner.
func TestRetryGapPreferenceCancels(t *testing.T) {
	if !realEngine {
		t.Skip("stub engine")
	}
	h := newHarness(t)
	to := ethcommon.HexToAddress("0x1000000000000000000000000000000000000008")
	armEmptyRetry(h, time.Now().Add(500*time.Millisecond)) // gap ~ 500ms + retryDelay
	h.transfer(to, big.NewInt(1))
	done := make(chan time.Duration, 1)
	go func() {
		t0 := time.Now()
		h.vm.WaitForEvent(context.Background())
		done <- time.Since(t0)
	}()
	time.Sleep(15 * time.Millisecond) // let the wake reach the retry select
	// Move to a different parent, then signal: the re-evaluation no longer
	// matches the same-parent retry, so it returns without the gap.
	h.vm.b.mu.Lock()
	h.vm.b.lastBuildParent = ethcommon.Hash{0xff}
	h.vm.b.mu.Unlock()
	h.vm.b.preferenceChanged() // closes pref, resets prefAt (the sincePreferred clock)
	d := <-done
	if d > 200*time.Millisecond {
		t.Fatalf("preference change did not cancel the retry sleep: woke after %v, armed gap was ~500ms", d)
	}
	h.vm.b.mu.Lock()
	sincePreferred := time.Since(h.vm.b.prefAt)
	h.vm.b.mu.Unlock()
	if sincePreferred > 200*time.Millisecond {
		t.Fatalf("sincePreferred after the cancel was %v, want ~0", sincePreferred)
	}
}

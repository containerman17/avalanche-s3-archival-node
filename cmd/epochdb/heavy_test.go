package main

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/containerman17/epochdb/state"
)

// TestHeavyGateSerializes is the whole contract: with one slot, a second build
// does not start until the first one releases. flock is per open file
// description, so two opens in one process contend exactly as two containers
// on one box do, and the gate can be tested without spawning any.
func TestHeavyGateSerializes(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("EPOCHDB_HEAVY_SLOTS", "1")
	heavySlotPoll = time.Millisecond
	t.Cleanup(func() { heavySlotPoll = 5 * time.Second })

	release, err := acquireHeavySlot(context.Background(), dir)
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}

	// The slot is taken, so this one must wait until its deadline instead of
	// running a second frontier build beside the first.
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := acquireHeavySlot(ctx, dir); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("second acquire while the slot is held = %v, want a wait that times out", err)
	}

	release()
	release2, err := acquireHeavySlot(context.Background(), dir)
	if err != nil {
		t.Fatalf("acquire after release: %v", err)
	}
	release2()
}

// TestHeavyGateSlotsAndBadValue: two slots admit two builds, and a value an
// operator fat-fingered is a startup error rather than a silently ungated box.
func TestHeavyGateSlotsAndBadValue(t *testing.T) {
	dir := t.TempDir()
	heavySlotPoll = time.Millisecond
	t.Cleanup(func() { heavySlotPoll = 5 * time.Second })

	t.Setenv("EPOCHDB_HEAVY_SLOTS", "2")
	a, err := acquireHeavySlot(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	b, err := acquireHeavySlot(context.Background(), dir)
	if err != nil {
		t.Fatalf("second of two slots: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := acquireHeavySlot(ctx, dir); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("a third build got in with two slots")
	}
	a()
	b()

	t.Setenv("EPOCHDB_HEAVY_SLOTS", "0")
	if _, err := acquireHeavySlot(context.Background(), dir); err == nil {
		t.Fatal("EPOCHDB_HEAVY_SLOTS=0 must be an error, not an open gate")
	}
}

// TestDataDirLockRefusesASecondWriter: the second process fails IMMEDIATELY
// with the dir in the message, and never waits or proceeds. flock is per open
// file description, so a second open in one process contends exactly as a
// second container would.
func TestDataDirLockRefusesASecondWriter(t *testing.T) {
	dir := t.TempDir()
	release, err := lockDataDir(dir)
	if err != nil {
		t.Fatalf("first lock: %v", err)
	}
	_, err = lockDataDir(dir)
	if err == nil {
		t.Fatal("a second writer got the same data dir")
	}
	if !strings.Contains(err.Error(), dir) || !strings.Contains(err.Error(), "another epochdb process") {
		t.Fatalf("error must name the dir and the holder, got: %v", err)
	}
	release()
	release2, err := lockDataDir(dir)
	if err != nil {
		t.Fatalf("lock after release: %v", err)
	}
	release2()
}

// TestDataDirLockLeavesReadersAlone: reading a live chain's dir beside its
// writer is production-proven (the SDK, `dev probe`), so the lock must not
// reach it.
func TestDataDirLockReadOnlyOpenerStillWorks(t *testing.T) {
	dir := t.TempDir()
	release, err := lockDataDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	ro, err := state.OpenReadOnly(dir)
	if err != nil {
		t.Fatalf("read-only open beside the writer: %v", err)
	}
	ro.Close()
}

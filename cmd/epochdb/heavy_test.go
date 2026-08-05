package main

import (
	"context"
	"errors"
	"testing"
	"time"
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

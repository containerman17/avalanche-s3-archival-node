package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

// newTestChain is a fleetChain with fakes in place of a real node: the
// supervisor's isolation rule is about the context and the stop hook, and
// neither needs a chain to exist.
func newTestChain(ctx context.Context, id string) (*fleetChain, context.Context, *atomic.Int32) {
	cctx, cancel := context.WithCancel(ctx)
	var stops atomic.Int32
	return &fleetChain{id: id, cancel: cancel, stop: func() { stops.Add(1) }}, cctx, &stops
}

// TestFleetChainStopsAlone pins the fleet's whole reason to exist: a chain
// whose executor hard-stops takes down that chain and NOTHING else. The
// process lives, the sibling's goroutines are not cancelled, and /status says
// which one died.
func TestFleetChainStopsAlone(t *testing.T) {
	ctx := context.Background()
	dead, deadCtx, deadStops := newTestChain(ctx, "dead")
	alive, aliveCtx, aliveStops := newTestChain(ctx, "alive")
	f := &fleet{chains: []*fleetChain{dead, alive}}

	// A finished component and a cancellation are not failures.
	alive.report("backfill", nil)
	alive.report("follower", context.Canceled)
	if err := alive.stopped(); err != nil {
		t.Fatalf("a clean exit stopped the chain: %v", err)
	}
	if aliveStops.Load() != 0 {
		t.Fatalf("a clean exit flushed the chain")
	}

	dead.report("executor", errors.New("root mismatch at 42"))
	if err := dead.stopped(); err == nil || !strings.Contains(err.Error(), "root mismatch at 42") {
		t.Fatalf("dead chain not marked stopped: %v", err)
	}
	select {
	case <-deadCtx.Done():
	default:
		t.Fatal("dead chain's goroutines were not cancelled")
	}
	if deadStops.Load() != 1 {
		t.Fatalf("dead chain flushed %d times, want 1", deadStops.Load())
	}

	// The second failure of the same chain must not re-flush a released
	// Firewood handle.
	dead.report("follower", errors.New("and its follower too"))
	if deadStops.Load() != 1 {
		t.Fatalf("dead chain flushed %d times after a second failure, want 1", deadStops.Load())
	}

	// The sibling is untouched by all of it.
	if err := alive.stopped(); err != nil {
		t.Fatalf("sibling stopped: %v", err)
	}
	if aliveStops.Load() != 0 {
		t.Fatalf("sibling flushed")
	}
	select {
	case <-aliveCtx.Done():
		t.Fatal("sibling's goroutines were cancelled")
	default:
	}

	rec := httptest.NewRecorder()
	f.statusHandler(rec, httptest.NewRequest(http.MethodGet, "/status", nil))
	var got []struct {
		Chain   string `json:"chain"`
		Stopped bool   `json:"stopped"`
		Error   string `json:"error"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
		t.Fatalf("status: %v", err)
	}
	if len(got) != 2 || got[0].Chain != "dead" || !got[0].Stopped || got[1].Chain != "alive" || got[1].Stopped {
		t.Fatalf("status does not name the dead chain alone: %+v", got)
	}
	if !strings.Contains(got[0].Error, "root mismatch at 42") {
		t.Fatalf("status hides the cause: %+v", got[0])
	}
}

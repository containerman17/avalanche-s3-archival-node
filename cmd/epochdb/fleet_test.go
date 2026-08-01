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

// One container ID in both spellings --tip-override accepts: a 0x-hex eth
// block hash (pre-ProposerVM, where it IS the container ID) and cb58.
const hexTip = "0x0f2ce4f0e7b6e1b0e2e0b3ca7f9a0ad2a9f6f1a4b8c3d2e1f0a9b8c7d6e5f4a3"

var cb58Tip = func() string {
	id, err := parseContainerID(hexTip)
	if err != nil {
		panic(err)
	}
	return id.String()
}()

// TestParseTipOverrideRefusesAHeight pins the ruling: --tip-override is a
// PHYSICAL container ID on every command, and a height is refused with the
// place to find one.
func TestParseTipOverrideRefusesAHeight(t *testing.T) {
	for _, v := range []string{"3000000", "0"} {
		_, err := parseTipOverride(v)
		if err == nil {
			t.Fatalf("--tip-override %s accepted a height", v)
		}
		if !strings.Contains(err.Error(), "block height") || !strings.Contains(err.Error(), "consensus: accepted height=N container=") {
			t.Fatalf("%s: error does not say how to get an ID: %v", v, err)
		}
	}
	if _, err := parseTipOverride("not-an-id"); err == nil || !strings.Contains(err.Error(), "consensus: accepted height=N container=") {
		t.Fatalf("garbage value: %v", err)
	}
	id, err := parseTipOverride(hexTip)
	if err != nil {
		t.Fatalf("0x-hex eth block hash refused: %v", err)
	}
	if got, err := parseTipOverride(id.String()); err != nil || got != id {
		t.Fatalf("cb58 round trip: %v %v", got, err)
	}
}

// TestParseFleetTipOverrides pins the per-chain form: a fleet runs N chains,
// so the override names one of --chains and a bare value is refused.
func TestParseFleetTipOverrides(t *testing.T) {
	specs := []string{"C", cb58Tip}

	got, err := parseFleetTipOverrides("C="+hexTip+", "+cb58Tip+"="+cb58Tip, specs)
	if err != nil {
		t.Fatalf("valid override refused: %v", err)
	}
	if len(got) != 2 || got["C"].String() != cb58Tip || got[cb58Tip].String() != cb58Tip {
		t.Fatalf("parsed wrong: %v", got)
	}
	// A chain with no entry keeps following: absent, not zero.
	if got, err := parseFleetTipOverrides("C="+cb58Tip, specs); err != nil || len(got) != 1 {
		t.Fatalf("one entry: %v %v", got, err)
	}
	if got, err := parseFleetTipOverrides("", specs); err != nil || len(got) != 0 {
		t.Fatalf("empty flag: %v %v", got, err)
	}

	for name, v := range map[string]string{
		"bare value":      cb58Tip,
		"bare height":     "3000000",
		"unknown chain":   "D=" + cb58Tip,
		"duplicate key":   "C=" + cb58Tip + ",C=" + hexTip,
		"height per key":  "C=3000000",
		"garbage per key": "C=nope",
	} {
		if _, err := parseFleetTipOverrides(v, specs); err == nil {
			t.Fatalf("%s accepted: %q", name, v)
		}
	}
}

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

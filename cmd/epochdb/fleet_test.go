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

// TestParseTipOverrides pins the per-chain form: with N chains in the process
// the override names one of --chains and a bare value is refused, and with ONE
// chain the bare value single-chain serve always took still works.
func TestParseTipOverrides(t *testing.T) {
	specs := []string{"C", cb58Tip}

	got, err := parseTipOverrides("C="+hexTip+", "+cb58Tip+"="+cb58Tip, specs)
	if err != nil {
		t.Fatalf("valid override refused: %v", err)
	}
	if len(got) != 2 || got["C"].String() != cb58Tip || got[cb58Tip].String() != cb58Tip {
		t.Fatalf("parsed wrong: %v", got)
	}
	// A chain with no entry keeps following: absent, not zero.
	if got, err := parseTipOverrides("C="+cb58Tip, specs); err != nil || len(got) != 1 {
		t.Fatalf("one entry: %v %v", got, err)
	}
	if got, err := parseTipOverrides("", specs); err != nil || len(got) != 0 {
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
		if _, err := parseTipOverrides(v, specs); err == nil {
			t.Fatalf("%s accepted: %q", name, v)
		}
	}

	// ONE chain: the bare form is the whole point of `serve --tip-override <id>`
	// and it must keep meaning that chain. The keyed form still parses.
	for _, one := range [][]string{{"C"}, {cb58Tip}} {
		got, err := parseTipOverrides(hexTip, one)
		if err != nil {
			t.Fatalf("%v: bare value refused: %v", one, err)
		}
		if len(got) != 1 || got[one[0]].String() != cb58Tip {
			t.Fatalf("%v: bare value parsed wrong: %v", one, got)
		}
		if got, err := parseTipOverrides(one[0]+"="+hexTip, one); err != nil || got[one[0]].String() != cb58Tip {
			t.Fatalf("%v: keyed value: %v %v", one, got, err)
		}
		if _, err := parseTipOverrides("3000000", one); err == nil {
			t.Fatalf("%v: a height was accepted", one)
		}
	}
}

// TestServeSpecs pins the flag fold: --chains and --chain are ONE knob, a single
// --chains entry is exactly --chain, and only two or more chains make this a
// fleet at all.
func TestServeSpecs(t *testing.T) {
	for name, tc := range map[string]struct {
		one, many string
		want      []string
	}{
		"default":          {"C", "", []string{"C"}},
		"chain":            {cb58Tip, "", []string{cb58Tip}},
		"chains, one":      {"C", cb58Tip, []string{cb58Tip}},
		"chains, several":  {"C", "C," + cb58Tip, []string{"C", cb58Tip}},
		"chains, spaced":   {"C", " C , " + cb58Tip + " ", []string{"C", cb58Tip}},
		"chains, trailing": {"C", "C,", []string{"C"}},
	} {
		got, err := serveSpecs(tc.one, tc.many)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if len(got) != len(tc.want) {
			t.Fatalf("%s: got %v want %v", name, got, tc.want)
		}
		for i := range got {
			if got[i] != tc.want[i] {
				t.Fatalf("%s: got %v want %v", name, got, tc.want)
			}
		}
	}
	for name, tc := range map[string][2]string{
		"empty --chain":  {"", ""},
		"empty --chains": {"C", " , "},
		"duplicate":      {"C", "C,c"},
	} {
		if got, err := serveSpecs(tc[0], tc[1]); err == nil {
			t.Fatalf("%s accepted: %v", name, got)
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

	got := statusOf(t, f)
	if len(got) != 2 || got[0].Chain != "dead" || !got[0].Stopped || got[1].Chain != "alive" || got[1].Stopped {
		t.Fatalf("status does not name the dead chain alone: %+v", got)
	}
	if !strings.Contains(got[0].Error, "root mismatch at 42") {
		t.Fatalf("status hides the cause: %+v", got[0])
	}
}

// TestChainThatNeverStartedIsAStatusEntry pins the boot half of the resilience
// contract (RULING 2026-08-03): a chain that REFUSES TO START is a per-chain
// failure state, not a dead process. The process is up, the healthy sibling is
// untouched, and /status carries the operator-facing reason for the broken one.
func TestChainThatNeverStartedIsAStatusEntry(t *testing.T) {
	f := &fleet{}
	f.fail("broken", errors.New("the `latest` pointer names history this node cannot assemble"))
	alive, aliveCtx, aliveStops := newTestChain(context.Background(), "alive")
	f.mu.Lock()
	f.chains = append(f.chains, alive)
	f.mu.Unlock()

	// Nothing about the refusal reached the sibling: not its context, not its
	// writers, and it is not marked stopped.
	if err := alive.stopped(); err != nil {
		t.Fatalf("sibling stopped by another chain's refusal: %v", err)
	}
	if aliveStops.Load() != 0 {
		t.Fatalf("sibling flushed by another chain's refusal")
	}
	select {
	case <-aliveCtx.Done():
		t.Fatal("sibling's goroutines were cancelled by another chain's refusal")
	default:
	}

	got := statusOf(t, f)
	if len(got) != 2 || got[0].Chain != "broken" || !got[0].Stopped || got[1].Chain != "alive" || got[1].Stopped {
		t.Fatalf("status does not name the refusing chain alone: %+v", got)
	}
	if !strings.Contains(got[0].Error, "cannot assemble") {
		t.Fatalf("status hides why the chain refused: %+v", got[0])
	}

	// With a single chain the process has nothing left to serve, and only then
	// does a refusal reach the process.
	var died string
	solo := &fleet{onFail: func(id string) { died = id }}
	solo.fail("only", errors.New("boom"))
	if died != "only" {
		t.Fatalf("a solo chain's refusal did not reach the process: %q", died)
	}
}

type statusRow struct {
	Chain   string `json:"chain"`
	Stopped bool   `json:"stopped"`
	Error   string `json:"error"`
}

func statusOf(t *testing.T, f *fleet) []statusRow {
	t.Helper()
	rec := httptest.NewRecorder()
	f.statusHandler(rec, httptest.NewRequest(http.MethodGet, "/status", nil))
	var got []statusRow
	if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
		t.Fatalf("status: %v", err)
	}
	return got
}

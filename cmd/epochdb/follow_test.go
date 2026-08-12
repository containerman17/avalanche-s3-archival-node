package main

import (
	"encoding/json"
	"github.com/containerman17/epochdb"
	"net"
	"strings"
	"testing"
	"time"
)

// TestStatusIsOneChain pins the shape /status answers with since 2026-08-04: ONE
// OBJECT, not the fleet's array, and it answers from the bind rather than from
// the moment the chain is up, which on a cold dir is hours later. The nil node
// IS that window, and `serving` is what tells an operator which side of it they
// are looking at.
func TestStatusIsOneChain(t *testing.T) {
	b, err := json.Marshal(statusOf("C", nil))
	if err != nil {
		t.Fatal(err)
	}
	got := string(b)
	if !strings.HasPrefix(got, "{") {
		t.Fatalf("/status is not a single object: %s", got)
	}
	for _, want := range []string{`"chain":"C"`, `"serving":false`, `"accepted":0`} {
		if !strings.Contains(got, want) {
			t.Fatalf("/status before the chain is up: %s, want %s", got, want)
		}
	}
}

// TestBackfillReportsAgainstTheOverrideCeiling pins the 2026-08-05 fix. A
// --tip-override run has no follower, so the staging store's head is not its
// accepted Head: on the mainnet stage-1 node it was 66,854,601 (sidecars left
// by an earlier follow run) against a 10,129,485 ceiling, and exec_lag was
// 59.3M of noise. Backfill reports the ceiling it was given and how much is
// staged; follow mode reports exactly what it always did.
func TestBackfillReportsAgainstTheOverrideCeiling(t *testing.T) {
	bf := epochdb.Status{
		Backfill: true, Head: 10_129_485, Stored: 7_600_000,
		Executed: 7_548_834, Served: 7_548_834, Flushed: 7_548_834, Settled: 7_548_834,
	}
	line := statusLine(bf)
	for _, want := range []string{"target=10129485", "stored=7600000", "exec_lag=2580651"} {
		if !strings.Contains(line, want) {
			t.Fatalf("backfill status line %q, want %s", line, want)
		}
	}
	if strings.Contains(line, "accepted=") {
		t.Fatalf("backfill status line reports an accepted head it does not have: %q", line)
	}
	b, err := json.Marshal(serveStatusOf(bf, "C"))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`"accepted":0`, `"target":10129485`, `"stored":7600000`} {
		if !strings.Contains(string(b), want) {
			t.Fatalf("backfill /status %s, want %s", b, want)
		}
	}

	// The ceiling is unknown until the anchors resolve, which is the one window
	// where a lag would be an invented number.
	if line := statusLine(epochdb.Status{Backfill: true, Stored: 12, Executed: 5}); !strings.Contains(line, "target=? ") || !strings.Contains(line, "exec_lag=?") {
		t.Fatalf("unresolved backfill status line %q", line)
	}

	// Follow mode, byte for byte what it was before the fix.
	follow := epochdb.Status{Head: 66_854_601, Executed: 7_548_834, Served: 7_548_834, Flushed: 7_548_834, Settled: 7_548_834}
	const want = "accepted=66854601 executed=7548834 served=7548834 cooked=7548834 settled=7548834 exec_lag=59305767 tail=0/0.0MB"
	if got := statusLine(follow); got != want {
		t.Fatalf("follow status line changed:\n got %s\nwant %s", got, want)
	}
	b, err = json.Marshal(serveStatusOf(follow, "C"))
	if err != nil {
		t.Fatal(err)
	}
	if got := string(b); got != `{"chain":"C","serving":true,"accepted":66854601,"executed":7548834,"cooked":7548834}` {
		t.Fatalf("follow /status changed: %s", got)
	}
}

// TestServeBindsBeforeStartupWork pins the ordering serveOn exists for: the RPC
// port is taken BEFORE any startup work, so a collision is an error in
// milliseconds and nothing in the data dir has been opened. Before this, serve
// bound last and a taken port surfaced as FATAL 68 minutes into a 57M-block
// Fuji start, twice in one night (2026-08-01).
func TestServeBindsBeforeStartupWork(t *testing.T) {
	busy, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer busy.Close()

	ran := false
	start := time.Now()
	err = serveOn(busy.Addr().(*net.TCPAddr).Port, func(net.Listener) { ran = true })
	if err == nil {
		t.Fatal("a taken port started serving")
	}
	// The one assertion that catches a reordering: everything after the bind
	// (chain resolution, joinChain's walk, the frontier build, the exec open)
	// lives in this callback, and none of it may run.
	if ran {
		t.Fatal("the startup work ran before the bind succeeded")
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Fatalf("the port collision took %s to surface", d)
	}
}

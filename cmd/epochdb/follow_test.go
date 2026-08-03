package main

import (
	"encoding/json"
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

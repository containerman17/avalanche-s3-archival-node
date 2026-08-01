package main

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestServeBindsBeforeStartupWork pins the ordering serveOn exists for: the RPC
// port is taken BEFORE the startup work, so a collision is an error in
// milliseconds and nothing in the data dir has been opened. Before this, serve
// bound last and a taken port surfaced as FATAL 68 minutes into a 57M-block
// Fuji start, twice in one night (2026-08-01).
func TestServeBindsBeforeStartupWork(t *testing.T) {
	busy, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer busy.Close()

	dir := filepath.Join(t.TempDir(), "data")
	port := busy.Addr().(*net.TCPAddr).Port
	start := time.Now()
	n, ln, err := serveOn(context.Background(), nodeConfig{DataDir: dir}, port, func(string, error) {})
	if err == nil {
		ln.Close()
		n.closeAll()
		t.Fatal("a taken port started a node")
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Fatalf("the port collision took %s to surface", d)
	}
	// The one assertion that catches a reordering: startNode would have created
	// the data dir on its way to the epoch-chain walk.
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("startNode ran before the bind: %s exists (%v)", dir, err)
	}
}

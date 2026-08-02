package main

import (
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/containerman17/epochdb/dist"
	"github.com/containerman17/epochdb/state"
)

// buildLinkedEpochs publishes two chained epochs and returns their hashes.
func buildLinkedEpochs(t *testing.T, st *dist.Store, root [32]byte) (string, string) {
	t.Helper()
	build := func(start uint64, prev [32]byte) string {
		in := &state.EpochInput{
			Start:    start,
			Prev:     prev,
			TxHashes: map[uint64][][32]byte{},
		}
		for i := 0; i < 4; i++ {
			in.Containers = append(in.Containers, []byte(strings.Repeat("container", 20)))
			in.Headers = append(in.Headers, []byte(strings.Repeat("header", 20)))
		}
		hash, err := state.BuildEpoch(st, in)
		if err != nil {
			t.Fatal(err)
		}
		return hash
	}
	h1 := build(1, root)
	raw, err := hex.DecodeString(h1)
	if err != nil {
		t.Fatal(err)
	}
	return h1, build(5, [32]byte(raw))
}

// TestBootstrapChainWalk: with the local index thrown away, the `latest`
// pointer plus the footers alone rebuild it, and a wrong chain is refused.
func TestBootstrapChainWalk(t *testing.T) {
	dir := t.TempDir()
	st, err := dist.Local(dir)
	if err != nil {
		t.Fatal(err)
	}
	root := [32]byte{7, 7, 7}
	h1, h2 := buildLinkedEpochs(t, st, root)
	if err := st.SetLatest(root, dist.Latest{Epoch: h2}); err != nil {
		t.Fatal(err)
	}
	for _, n := range []string{state.EpochMarkerName(1, 4), state.EpochMarkerName(5, 4)} {
		if err := os.Remove(filepath.Join(dir, n)); err != nil {
			t.Fatal(err)
		}
	}

	epochs, err := bootstrapChain(st, root)
	if err != nil || epochs != 2 {
		t.Fatalf("walk: %d epochs, err %v", epochs, err)
	}
	if got, err := state.ReadMarker(dir, state.EpochMarkerName(1, 4)); err != nil || got != h1 {
		t.Fatalf("epoch 1 marker: %s %v, want %s", got, err, h1)
	}
	set, err := state.OpenEpochSet(st)
	if err != nil {
		t.Fatal(err)
	}
	defer set.Close()
	if end, ok := set.SealedEnd(); !ok || end != 8 {
		t.Fatalf("rebuilt set covers through %d (ok=%v), want 8", end, ok)
	}

	// The chain root is the trust anchor, and the pointer NAME carrying it is
	// not the check: publish the same tip under another root's pointer name and
	// the footers still refuse it.
	bad := [32]byte{9}
	if err := st.SetLatest(bad, dist.Latest{Epoch: h2}); err != nil {
		t.Fatal(err)
	}
	if _, err := bootstrapChain(st, bad); err == nil || !strings.Contains(err.Error(), "wrong chain") {
		t.Fatalf("walk with the wrong chain root: %v", err)
	}
}

// TestValidateChainAtStartup: the startup walk passes on an intact chain, is a
// no-op on a dir that never sealed, and refuses to start once a linked
// artifact is gone.
func TestValidateChainAtStartup(t *testing.T) {
	dir := t.TempDir()
	st, err := dist.Local(dir)
	if err != nil {
		t.Fatal(err)
	}
	root := [32]byte{7, 7, 7}

	// Nothing sealed yet: no `latest`, nothing to walk, and that is fine.
	if n, err := validateChain(st, root); n != 0 || err != nil {
		t.Fatalf("empty dir: %d epochs, err %v", n, err)
	}

	h1, h2 := buildLinkedEpochs(t, st, root)
	if err := st.SetLatest(root, dist.Latest{Epoch: h2}); err != nil {
		t.Fatal(err)
	}
	if n, err := validateChain(st, root); n != 2 || err != nil {
		t.Fatalf("intact chain: %d epochs, err %v", n, err)
	}

	// Existence is half the check: lose the artifact the chain links to and
	// the node must refuse to serve.
	if err := os.Remove(st.SpoolPath(h1)); err != nil {
		t.Fatal(err)
	}
	if _, err := validateChain(st, root); err == nil {
		t.Fatalf("missing epoch %s accepted", h1)
	}
}

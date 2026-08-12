package store

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/containerman17/epochdb/dist"
)

// joinable builds a published corpus in dir and returns its chain root. The
// spool doubles as the bucket here: without credentials dist keeps everything
// local, and Join reads exactly the same pointer, manifest and footers.
func joinable(t *testing.T, dir string, runs int) [32]byte {
	t.Helper()
	root := [32]byte{1}
	db := buildCorpus(t, dir, runs)
	if err := db.Publish(); err != nil {
		t.Fatal(err)
	}
	return root
}

// consumerOf makes a fresh dir that can see dir's artifacts and pointer.
func consumerOf(t *testing.T, producer string) (*dist.Store, string) {
	t.Helper()
	cons := t.TempDir()
	if err := os.MkdirAll(filepath.Join(cons, "cas"), 0o755); err != nil {
		t.Fatal(err)
	}
	ents, err := os.ReadDir(filepath.Join(producer, "cas"))
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range ents {
		b, err := os.ReadFile(filepath.Join(producer, "cas", e.Name()))
		if err != nil {
			continue // .pointers is a directory
		}
		if err := os.WriteFile(filepath.Join(cons, "cas", e.Name()), b, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	// The pointer the producer wrote beside its data dir.
	top, err := os.ReadDir(producer)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range top {
		if strings.HasPrefix(e.Name(), "latest-") {
			b, err := os.ReadFile(filepath.Join(producer, e.Name()))
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(cons, e.Name()), b, 0o644); err != nil {
				t.Fatal(err)
			}
		}
	}
	cas, err := dist.Local(cons)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cas.Close() })
	return cas, cons
}

// TestJoinWalksToTheChainRoot: a fresh dir resolves the pointer, walks the runs
// backward by prev-name, and comes up serving the published history.
func TestJoinWalksToTheChainRoot(t *testing.T) {
	prod := t.TempDir()
	root := joinable(t, prod, 3)
	cas, cons := consumerOf(t, prod)

	if err := Join(cas, cons, root); err != nil {
		t.Fatal(err)
	}
	db, err := Open(cons, cas, root)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if got := len(db.Manifest().Runs); got != 3 {
		t.Fatalf("joined %d runs, want 3", got)
	}
	if h, ok := db.Head(); !ok || h != 8 {
		t.Fatalf("joined head is %d (%v), want 8", h, ok)
	}
	for h := uint64(0); h < 9; h++ {
		if _, ok, err := db.HeaderRLP(h); err != nil || !ok {
			t.Fatalf("joined dir cannot serve header %d: %v %v", h, ok, err)
		}
	}
	// A SECOND join is a warm start: it must not even look at the pointer.
	if err := os.Remove(filepath.Join(cons, dist.LatestPointer(root))); err != nil {
		t.Fatal(err)
	}
	if err := Join(cas, cons, root); err != nil {
		t.Fatalf("a warm dir went looking for the pointer: %v", err)
	}
}

// TestJoinRefusesBrokenChains: every way a corpus can be wrong is a HARD ERROR
// that names the artifact, never a silent fall back to re-syncing from p2p.
func TestJoinRefusesBrokenChains(t *testing.T) {
	prod := t.TempDir()
	root := joinable(t, prod, 3)

	t.Run("missing run", func(t *testing.T) {
		cas, cons := consumerOf(t, prod)
		man := readPublished(t, cas, cons, root)
		if err := os.Remove(cas.SpoolPath(man.Runs[1].Name)); err != nil {
			t.Fatal(err)
		}
		err := Join(cas, cons, root)
		if err == nil || !strings.Contains(err.Error(), man.Runs[1].Name) {
			t.Fatalf("a missing run must be refused by name: %v", err)
		}
	})

	t.Run("broken prev link", func(t *testing.T) {
		cas, cons := consumerOf(t, prod)
		man := readPublished(t, cas, cons, root)
		// Drop the oldest run from the list: the second run's prev then names
		// something no longer in the chain, and the new oldest does not link to
		// the chain root.
		man.Runs = man.Runs[1:]
		republish(t, cas, cons, root, man)
		err := Join(cas, cons, root)
		if err == nil || !strings.Contains(err.Error(), "chain's root is") {
			t.Fatalf("a corpus whose oldest run does not reach the chain root must be refused: %v", err)
		}
	})

	t.Run("hole in the middle", func(t *testing.T) {
		cas, cons := consumerOf(t, prod)
		man := readPublished(t, cas, cons, root)
		man.Runs = append(man.Runs[:1], man.Runs[2:]...)
		republish(t, cas, cons, root, man)
		err := Join(cas, cons, root)
		if err == nil || !strings.Contains(err.Error(), "hash chain is broken") {
			t.Fatalf("a corpus missing a middle run must be refused: %v", err)
		}
	})

	t.Run("another chain's manifest", func(t *testing.T) {
		// Pointers are chain-root-qualified, so this is the shape that can
		// actually reach a node: the right pointer naming a manifest that
		// belongs to somebody else's chain.
		cas, cons := consumerOf(t, prod)
		man := readPublished(t, cas, cons, root)
		man.ChainRoot = hexName([32]byte{9})
		republish(t, cas, cons, root, man)
		err := Join(cas, cons, root)
		if err == nil || !strings.Contains(err.Error(), "belongs to chain root") {
			t.Fatalf("a manifest from another chain must be refused: %v", err)
		}
	})
}

func readPublished(t *testing.T, cas *dist.Store, dir string, root [32]byte) *Manifest {
	t.Helper()
	l, err := cas.Latest(root)
	if err != nil {
		t.Fatal(err)
	}
	blob, err := cas.Open(l.Manifest)
	if err != nil {
		t.Fatal(err)
	}
	defer blob.Close()
	raw, err := blob.Read(0, blob.Size())
	if err != nil {
		t.Fatal(err)
	}
	m := new(Manifest)
	if err := json.Unmarshal(raw, m); err != nil {
		t.Fatal(err)
	}
	return m
}

func republish(t *testing.T, cas *dist.Store, dir string, root [32]byte, m *Manifest) {
	t.Helper()
	raw, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	name, err := cas.Put(raw)
	if err != nil {
		t.Fatal(err)
	}
	if err := cas.SetLatest(root, dist.Latest{Manifest: name}); err != nil {
		t.Fatal(err)
	}
}

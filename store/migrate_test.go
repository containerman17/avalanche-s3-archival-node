package store

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/containerman17/epochdb/dist"
)

// TestMigrateV1: the committed v1 fixture, migrated, answers every posting
// and hash lookup exactly as a v2 store built from the same blocks does, its
// chain and state sections are the same bytes, and the run chain walks back to
// the root. A second Migrate is a no-op.
func TestMigrateV1(t *testing.T) {
	t.Run("with window", func(t *testing.T) { testMigrateV1(t, true) })
	t.Run("no window", func(t *testing.T) { testMigrateV1(t, false) })
}

func testMigrateV1(t *testing.T, window bool) {
	dir := t.TempDir()
	if out, err := exec.Command("cp", "-r", filepath.Join("testdata", "v1")+"/.", dir).CombinedOutput(); err != nil {
		t.Fatalf("cp: %v %s", err, out)
	}
	if !window {
		os.Remove(filepath.Join(dir, "window", "window.log"))
	}
	cas, err := dist.Local(dir)
	if err != nil {
		t.Fatal(err)
	}
	logf := func(f string, a ...any) { t.Logf(f, a...) }
	if err := Migrate(dir, cas, logf); err != nil {
		t.Fatal(err)
	}
	if err := Migrate(dir, cas, logf); err != nil { // resumable, idempotent
		t.Fatal(err)
	}
	cas.Close()
	cas, err = dist.Local(dir)
	if err != nil {
		t.Fatal(err)
	}
	got, err := Open(dir, cas, [32]byte{1})
	if err != nil {
		t.Fatal(err)
	}
	defer got.Close()
	want := writeFixture(t, t.TempDir())
	defer want.Close()
	// Without the window the migrated dir holds the sealed runs only, so the
	// comparison stops at their end.
	hiTx, blocks := uint64(1<<62), uint64(56)
	if !window {
		hiTx, blocks = 161, 54
	}

	gm, wm := got.Manifest(), want.Manifest()
	if gm.StorageVersion != StorageVersion || len(gm.Runs) != len(wm.Runs) {
		t.Fatalf("manifest: %+v", gm)
	}
	if err := WalkRuns(cas, gm.Runs, [32]byte{1}); err != nil {
		t.Fatal(err)
	}
	for i := range gm.Runs {
		g, err := OpenRun(cas, gm.Runs[i].Name)
		if err != nil {
			t.Fatal(err)
		}
		w, err := OpenRun(want.cas, wm.Runs[i].Name)
		if err != nil {
			t.Fatal(err)
		}
		for s := SecChain; s <= SecState; s++ {
			gb, _ := g.blob.Read(g.Footer.Off[s], g.Footer.Len[s])
			wb, _ := w.blob.Read(w.Footer.Off[s], w.Footer.Len[s])
			if !bytes.Equal(gb, wb) {
				t.Fatalf("run %d section %d differs", i, s)
			}
		}
		g.Close()
		w.Close()
	}
	type entry struct {
		g string
		n uint64
		p byte
	}
	dump := func(db *DB) (out []entry) {
		for _, fam := range []string{PrefixAddr, PrefixELog, PrefixTVal, PrefixSig} {
			seen := map[string]bool{}
			var groups []string
			if err := db.Groups([]byte(fam), func(g []byte) bool {
				if !seen[string(g)] {
					seen[string(g)] = true
					groups = append(groups, string(g))
				}
				return true
			}); err != nil {
				t.Fatal(err)
			}
			for _, g := range groups {
				if err := db.Postings([]byte(g), 0, hiTx, func(n uint64, p byte) bool {
					out = append(out, entry{g, n, p})
					return true
				}); err != nil {
					t.Fatal(err)
				}
			}
		}
		return out
	}
	ge, we := dump(got), dump(want)
	if len(ge) == 0 || len(ge) != len(we) {
		t.Fatalf("%d entries, want %d", len(ge), len(we))
	}
	for i := range ge {
		if ge[i] != we[i] {
			t.Fatalf("entry %d: %q/%d/%d want %q/%d/%d", i, ge[i].g[:5], ge[i].n, ge[i].p, we[i].g[:5], we[i].n, we[i].p)
		}
	}
	for h := uint64(0); h < blocks; h++ {
		first, n, ok, err := got.BlockTxRange(h)
		if err != nil || !ok {
			t.Fatalf("block %d: %v %v", h, ok, err)
		}
		for i := uint64(0); i < uint64(n); i++ {
			num, ok, err := got.TxNumByHash(hash32(byte(h*10 + i)))
			if err != nil || !ok || num != first+i {
				t.Fatalf("txh %d/%d: %d %v %v", h, i, num, ok, err)
			}
		}
	}
	// The old runs are still there.
	if _, err := os.Stat(filepath.Join(dir, "cas", "b9287504fd678672abb90b3bcb1b7755a07b429bf0e6ca2256d8778d9aa353fd")); err != nil {
		t.Fatal("v1 terminal run was deleted")
	}
}

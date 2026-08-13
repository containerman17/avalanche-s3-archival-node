package store

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/containerman17/epochdb/dist"
)

// testTerminalTxs is the TEST-ONLY terminal boundary (export_test.go): exactly
// what MergeFanIn of buildCorpus's runs hold, so a test cuts a terminal every
// sixteen flushes the way mainnet does every 8M transactions.
const testTerminalTxs = 3 * 2 * MergeFanIn

// buildCorpus fills dir with n L0 runs of 3 blocks each and returns the store.
// MergeFanIn runs later it merges itself, which is the point.
func buildCorpus(t *testing.T, dir string, runs int) *DB {
	t.Helper()
	cas, err := dist.Local(dir)
	if err != nil {
		t.Fatal(err)
	}
	db, err := Open(dir, cas, [32]byte{1})
	if err != nil {
		t.Fatal(err)
	}
	db.scaleTriggers(FlushTxs, FlushBlocks, testTerminalTxs)
	t.Cleanup(func() { db.Close(); cas.Close() })
	h := uint64(0)
	for r := 0; r < runs; r++ {
		for i := 0; i < 3; i++ {
			if err := db.WriteBlock(block(h, 2)); err != nil {
				t.Fatal(err)
			}
			h++
		}
		if err := db.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	return db
}

// TestMergeIsDeterministic: two independent builders that saw the same blocks
// produce the same TERMINAL run, byte for byte and name for name, under the
// same name on disk (DESIGN's core promise). It also pins the trigger: the L0
// tail reaching a terminal's worth of transactions, which at these scaled
// triggers is MergeFanIn runs, becomes one terminal run.
func TestMergeIsDeterministic(t *testing.T) {
	dirA, dirB := t.TempDir(), t.TempDir()
	a := buildCorpus(t, dirA, MergeFanIn)
	b := buildCorpus(t, dirB, MergeFanIn)

	manA, manB := a.Manifest(), b.Manifest()
	if len(manA.Runs) != 1 || !manA.Runs[0].Terminal() {
		t.Fatalf("%d L0 runs did not merge into one terminal run: %+v", MergeFanIn, manA.Runs)
	}
	if manA.Runs[0].Name != manB.Runs[0].Name {
		t.Fatalf("two independent merges of the same blocks disagree:\n  %s\n  %s", manA.Runs[0].Name, manB.Runs[0].Name)
	}
	if got, want := RunLabel(manA.Runs[0].Level, manA.Runs[0].FromTx, manA.Runs[0].ToTx), "t-0000000000000000-0000000000000096"; got != want {
		t.Fatalf("terminal run label is %q, want %q", got, want)
	}
	ba, err := os.ReadFile(filepath.Join(dirA, "cas", manA.Runs[0].Name))
	if err != nil {
		t.Fatal(err)
	}
	bb, err := os.ReadFile(filepath.Join(dirB, "cas", manB.Runs[0].Name))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(ba, bb) {
		t.Fatalf("the merged run files differ: %d vs %d bytes", len(ba), len(bb))
	}

	// The inputs' LOCAL copies retired; nothing else in the spool did.
	ents, err := os.ReadDir(filepath.Join(dirA, "cas"))
	if err != nil {
		t.Fatal(err)
	}
	var live int
	for _, e := range ents {
		if dist.ValidHash(e.Name()) {
			live++
		}
	}
	if live != 2 {
		t.Fatalf("%d hash-named files left in the spool, want 2 (the merged run and the live manifest)", live)
	}
	if ents, err := os.ReadDir(filepath.Join(dirA, "runs")); err != nil || len(ents) != 0 {
		t.Fatalf("the local run directory is not empty after the merge: %v %v", ents, err)
	}

	// EVERY ROW SURVIVED THE MERGE: the terminal run answers what the L0 runs
	// answered, through the same descent.
	for h := uint64(0); h < 3*MergeFanIn; h++ {
		hdr, ok, err := a.HeaderRLP(h)
		if err != nil || !ok || string(hdr) != fmt.Sprintf("header-%d", h) {
			t.Fatalf("after the merge, header %d: %q %v %v", h, hdr, ok, err)
		}
		first, n, ok, err := a.BlockTxRange(h)
		if err != nil || !ok || n != 2 {
			t.Fatalf("after the merge, blk %d: %d %v %v", h, n, ok, err)
		}
		for i := uint64(0); i < 2; i++ {
			if got, ok, err := a.TxRLP(first + i); err != nil || !ok || string(got) != fmt.Sprintf("tx-%d-%d", h, i) {
				t.Fatalf("after the merge, tx %d: %q %v %v", first+i, got, ok, err)
			}
			if num, ok, err := a.TxNumByHash(hash32(byte(h*10 + i))); err != nil || !ok || num != first+i {
				t.Fatalf("after the merge, txh %d/%d: %d %v %v", h, i, num, ok, err)
			}
			v, ok, err := a.StorageAt(addr(2), hash32(7), first+i)
			if err != nil || !ok || !bytes.Equal(v, []byte{byte(h), byte(i)}) {
				t.Fatalf("after the merge, slot at %d: %x %v %v", first+i, v, ok, err)
			}
		}
	}
}

// TestRecomputeMergeFromInputs is DESIGN's "compaction without re-download":
// a consumer holding the small runs recomputes the merged run locally instead
// of downloading it, and the name it lands on is a pure function of the inputs.
func TestRecomputeMergeFromInputs(t *testing.T) {
	// Build the inputs WITHOUT letting the trigger fire, by stopping one run
	// short of a terminal's worth of transactions, then merging by hand.
	src := t.TempDir()
	scas, err := dist.Local(src)
	if err != nil {
		t.Fatal(err)
	}
	defer scas.Close()
	db, err := Open(src, scas, [32]byte{1})
	if err != nil {
		t.Fatal(err)
	}
	db.scaleTriggers(FlushTxs, FlushBlocks, testTerminalTxs)
	defer db.Close()
	h := uint64(0)
	for r := 0; r < MergeFanIn-1; r++ {
		for i := 0; i < 3; i++ {
			if err := db.WriteBlock(block(h, 2)); err != nil {
				t.Fatal(err)
			}
			h++
		}
		if err := db.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	names := make([]RunName, 0, MergeFanIn)
	for _, r := range db.Manifest().Runs {
		names = append(names, r.Name)
	}
	name, err := RecomputeMerge(scas, scas.SpoolDir(), [32]byte{1}, names)
	if err != nil {
		t.Fatal(err)
	}
	again, err := RecomputeMerge(scas, scas.SpoolDir(), [32]byte{1}, names)
	if err != nil {
		t.Fatal(err)
	}
	if name != again {
		t.Fatalf("two recomputes of the same inputs disagree: %s vs %s", name, again)
	}
	// It is a real run: it opens, and it covers the inputs' whole range.
	r, err := OpenRun(scas, name)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if r.Footer.FromTx != 0 || r.Footer.ToTx != db.Manifest().Runs[MergeFanIn-2].ToTx {
		t.Fatalf("recomputed run covers tx [%d,%d)", r.Footer.FromTx, r.Footer.ToTx)
	}
}

// ---------------------------------------------------------------------------
// CRASH POINTS AROUND THE SWAP
// ---------------------------------------------------------------------------

// TestMergeCrashPoints kills a real process at each stage of a merge (after the
// run is written and fsynced, after it is verified, after the manifest swap,
// after the local inputs are unlinked) and reopens the dir. DATA IS ALWAYS SAFE:
// every kill leaves a dir that opens and answers every query, and the merge
// completes on the next attempt.
func TestMergeCrashPoints(t *testing.T) {
	if os.Getenv("EPOCHDB_CRASH_DIR") != "" {
		crashChild(t)
		return
	}
	for _, stage := range []string{"written", "verified", "swapped", "deleted"} {
		t.Run(stage, func(t *testing.T) {
			dir := t.TempDir()
			// The child builds the corpus and dies at `stage` inside the merge.
			cmd := exec.Command(os.Args[0], "-test.run=TestMergeCrashPoints", "-test.v")
			cmd.Env = append(os.Environ(), "EPOCHDB_CRASH_DIR="+dir, "EPOCHDB_MERGE_CRASH="+stage)
			out, err := cmd.CombinedOutput()
			if err == nil {
				t.Fatalf("the child was supposed to die at %q but exited cleanly:\n%s", stage, out)
			}
			if !strings.Contains(string(out), "dying here on purpose") {
				t.Fatalf("the child died somewhere else:\n%s", out)
			}

			// REOPEN. Whatever the kill left behind, the dir serves.
			cas, err := dist.Local(dir)
			if err != nil {
				t.Fatal(err)
			}
			defer cas.Close()
			db, err := Open(dir, cas, [32]byte{1})
			if err != nil {
				t.Fatalf("after a kill at %q the dir does not open: %v", stage, err)
			}
			db.scaleTriggers(FlushTxs, FlushBlocks, testTerminalTxs)
			defer db.Close()
			man := db.Manifest()
			switch stage {
			case "written", "verified":
				if len(man.Runs) != MergeFanIn {
					t.Fatalf("a kill before the swap must leave the %d inputs live, got %d", MergeFanIn, len(man.Runs))
				}
			case "swapped", "deleted":
				if len(man.Runs) != 1 || !man.Runs[0].Terminal() {
					t.Fatalf("a kill at or after the swap must leave the merged run live, got %+v", man.Runs)
				}
			}
			for h := uint64(0); h < 3*MergeFanIn; h++ {
				hdr, ok, err := db.HeaderRLP(h)
				if err != nil || !ok || string(hdr) != fmt.Sprintf("header-%d", h) {
					t.Fatalf("after a kill at %q, header %d: %q %v %v", stage, h, hdr, ok, err)
				}
			}
			// And the merge finishes on the next attempt, landing on the name
			// a clean run would have produced.
			if err := db.MaybeMerge(); err != nil {
				t.Fatalf("after a kill at %q the retry failed: %v", stage, err)
			}
			clean := t.TempDir()
			ref := buildCorpus(t, clean, MergeFanIn)
			if got, want := db.Manifest().Runs[0].Name, ref.Manifest().Runs[0].Name; got != want {
				t.Fatalf("after a kill at %q the retried merge is %s, a clean merge is %s", stage, got, want)
			}
		})
	}
}

// crashChild is the subprocess half of TestMergeCrashPoints: it builds the
// corpus, which trips the merge, which dies at EPOCHDB_MERGE_CRASH.
func crashChild(t *testing.T) {
	dir := os.Getenv("EPOCHDB_CRASH_DIR")
	cas, err := dist.Local(dir)
	if err != nil {
		t.Fatal(err)
	}
	db, err := Open(dir, cas, [32]byte{1})
	if err != nil {
		t.Fatal(err)
	}
	db.scaleTriggers(FlushTxs, FlushBlocks, testTerminalTxs)
	h := uint64(0)
	for r := 0; r < MergeFanIn; r++ {
		for i := 0; i < 3; i++ {
			if err := db.WriteBlock(block(h, 2)); err != nil {
				t.Fatal(err)
			}
			h++
		}
		if err := db.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	t.Fatal("the merge did not die where it was told to")
}

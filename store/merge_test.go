package store

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/containerman17/epochdb/dist"
)

// testTerminalTxs is the TEST-ONLY terminal boundary (export_test.go): exactly
// what MergeFanIn of buildCorpus's runs hold, so a test cuts a terminal every
// sixteen flushes the way mainnet does every 8M transactions. A run of three
// blocks holds 3 x (2 txs + ONE BOUNDARY SLOT) = 9 slots, because the boundary
// is a TxNum like any other and the trigger counts TxNums.
const testTerminalTxs = 3 * (2 + 1) * MergeFanIn

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
	// The merge runs beside execution now, so a test that wants to LOOK at the
	// terminal has to wait for it. Nothing in production does: the executor
	// flushes and moves on.
	if err := db.WaitMerge(); err != nil {
		t.Fatal(err)
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
	if got, want := RunLabel(manA.Runs[0].Level, manA.Runs[0].FromTx, manA.Runs[0].ToTx), "t-0000000000000000-0000000000000144"; got != want {
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

// crashExtraRuns is how many L0 runs the crash child cuts WHILE THE MERGE IS
// STOPPED at its kill point. It is the whole new hazard in one number: the
// merge no longer owns the executor, so every kill point now happens with runs
// the merge never saw already appended to the manifest, and the retry must
// still merge the sixteen the boundary names and no more.
const crashExtraRuns = 3

// TestMergeCrashPoints kills a real process at each stage of a merge (mid
// merge, after the run is written and fsynced, after it is verified, after the
// manifest swap, after the local inputs are unlinked) and reopens the dir.
// EVERY KILL HAPPENS WHILE EXECUTION IS RUNNING PAST THE MERGE: the child holds
// the merge at the stage, cuts three more L0 runs, and only then dies.
//
// DATA IS ALWAYS SAFE: every kill leaves a dir that opens with no repair step,
// answers every query, and finishes the merge on the next flush, landing on the
// same terminal a clean serial build lands on.
func TestMergeCrashPoints(t *testing.T) {
	if os.Getenv("EPOCHDB_CRASH_DIR") != "" {
		crashChild(t)
		return
	}
	total := MergeFanIn + crashExtraRuns
	for _, stage := range []string{"merging", "written", "verified", "swapped", "deleted"} {
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

			// REOPEN. Whatever the kill left behind, the dir serves, and nothing
			// below this line is a repair routine: it is a normal open.
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
			case "merging", "written", "verified":
				if len(man.Runs) != total {
					t.Fatalf("a kill before the swap must leave all %d L0 runs live, got %d", total, len(man.Runs))
				}
			case "swapped", "deleted":
				if len(man.Runs) != 1+crashExtraRuns || !man.Runs[0].Terminal() {
					t.Fatalf("a kill at or after the swap must leave the merged run plus the %d newer L0 runs live, got %+v", crashExtraRuns, man.Runs)
				}
			}
			for h := uint64(0); h < 3*uint64(total); h++ {
				hdr, ok, err := db.HeaderRLP(h)
				if err != nil || !ok || string(hdr) != fmt.Sprintf("header-%d", h) {
					t.Fatalf("after a kill at %q, header %d: %q %v %v", stage, h, hdr, ok, err)
				}
			}
			// AND EXECUTION CARRIES ON, which is where the retry really has to
			// happen: a resumed node does not call MaybeMerge, it executes and
			// flushes. THE SPAN IS FIXED BY CONTENT, so the retry merges the
			// sixteen runs the boundary names even though nineteen are sitting
			// there. Swallowing the extra three would move every later terminal
			// boundary and leave this builder's artifacts unequal to everyone
			// else's forever.
			h := 3 * uint64(total)
			for i := 0; i < 3; i++ {
				if err := db.WriteBlock(block(h, 2)); err != nil {
					t.Fatal(err)
				}
				h++
			}
			if err := db.Flush(); err != nil {
				t.Fatalf("after a kill at %q the next flush failed: %v", stage, err)
			}
			if err := db.WaitMerge(); err != nil {
				t.Fatalf("after a kill at %q the retried merge failed: %v", stage, err)
			}
			clean := t.TempDir()
			ref := buildCorpus(t, clean, MergeFanIn)
			if got, want := db.Manifest().Runs[0].Name, ref.Manifest().Runs[0].Name; got != want {
				t.Fatalf("after a kill at %q the retried merge is %s, a clean merge is %s", stage, got, want)
			}
			if got := db.Manifest().Runs[0].ToTx; got != testTerminalTxs {
				t.Fatalf("after a kill at %q the terminal ends at tx %d, the boundary is %d", stage, got, testTerminalTxs)
			}
		})
	}
}

// crashChild is the subprocess half of TestMergeCrashPoints. It builds the
// corpus, which STARTS the merge; the merge stops dead at EPOCHDB_MERGE_CRASH;
// the executor then cuts three more L0 runs beside it and kills the process.
func crashChild(t *testing.T) {
	dir, stage := os.Getenv("EPOCHDB_CRASH_DIR"), os.Getenv("EPOCHDB_MERGE_CRASH")
	reached := make(chan struct{})
	mergeCrash = func(s string) {
		if s != stage {
			return
		}
		close(reached)
		select {} // park here: the executor kills the whole process from below
	}
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
	write := func(runs int) {
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
	}
	write(MergeFanIn) // the boundary flush STARTS the merge
	select {
	case <-reached:
	case <-time.After(30 * time.Second):
		t.Fatalf("the merge never reached stage %q", stage)
	}
	write(crashExtraRuns) // EXECUTION CARRIES ON while the merge sits at its stage
	log.Printf("store: EPOCHDB_MERGE_CRASH=%s, dying here on purpose", stage)
	os.Exit(9)
}

// ---------------------------------------------------------------------------
// THE TWO PROPERTIES THE CONCURRENT MERGE EXISTS FOR
// ---------------------------------------------------------------------------

// blocksPerRun is sized so a terminal is tens of megabytes and its merge is
// seconds of zstd-9, which is the smallest scale at which "did execution keep
// going" and "what did this do to the page cache" mean anything.
const blocksPerRun = 500

// fatTerminalTxs is one terminal per MergeFanIn of those runs: three slots per
// block (2 txs plus the boundary slot).
const fatTerminalTxs = uint64(3 * blocksPerRun * MergeFanIn)

// fatCorpus builds a corpus whose runs are big enough that a merge is SECONDS
// of work rather than milliseconds, which is the only scale at which "does
// execution keep going" and "what did this do to the page cache" are
// measurable at all. Each tx carries padBytes of incompressible payload, so the
// zstd-9 pass the merge spends most of its time in is real work.
func fatCorpus(t *testing.T, dir string, runs int, terminalTxs uint64) (*DB, *dist.Store) {
	t.Helper()
	cas, err := dist.Local(dir)
	if err != nil {
		t.Fatal(err)
	}
	db, err := Open(dir, cas, [32]byte{1})
	if err != nil {
		t.Fatal(err)
	}
	db.scaleTriggers(FlushTxs, FlushBlocks, terminalTxs)
	t.Cleanup(func() { db.Close(); cas.Close() })
	h := uint64(0)
	for r := 0; r < runs; r++ {
		for i := 0; i < blocksPerRun; i++ {
			if err := db.WriteBlock(fatBlock(h, 2)); err != nil {
				t.Fatal(err)
			}
			h++
		}
		if err := db.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	return db, cas
}

// TestExecutionRunsThroughAMerge is the defect this design exists to fix. The
// merge used to run on the executor's own goroutine and froze execution for six
// to nine minutes per terminal. Here the boundary flush starts the merge and
// blocks keep landing while it runs.
//
// THE MEASURE IS A WHOLE RUN, writes plus the flush that seals them, on both
// sides of the comparison: timing bare writes against a window that also pays
// for flushes would flatter the result. THE ASSERTION IS THAT EXECUTION NEVER
// STOPS; the rate is logged for the record, because how big the dip is depends
// on how many cores the machine has.
func TestExecutionRunsThroughAMerge(t *testing.T) {
	if testing.Short() {
		t.Skip("builds a multi-megabyte corpus")
	}
	dir := t.TempDir()
	db, _ := fatCorpus(t, dir, MergeFanIn-2, fatTerminalTxs)
	h := db.NextHeight()
	run := func() time.Duration {
		t.Helper()
		t0 := time.Now()
		for i := 0; i < blocksPerRun; i++ {
			if err := db.WriteBlock(fatBlock(h, 2)); err != nil {
				t.Fatal(err)
			}
			h++
		}
		if err := db.Flush(); err != nil {
			t.Fatal(err)
		}
		return time.Since(t0)
	}

	base := run() // BASELINE: one run's worth of blocks with nothing else running.

	// THE BOUNDARY RUN. Its flush starts the merge instead of becoming it.
	boundary := run()

	// EXECUTION CARRIES ON: keep cutting runs until the merge is done.
	t1 := time.Now()
	blocks := 0
	for {
		db.mu.RLock()
		merging := db.mergeDone != nil
		db.mu.RUnlock()
		if !merging {
			break
		}
		run()
		blocks += blocksPerRun
	}
	during := time.Since(t1)
	if err := db.WaitMerge(); err != nil {
		t.Fatal(err)
	}
	if len(db.Manifest().Runs) == 0 || !db.Manifest().Runs[0].Terminal() {
		t.Fatalf("no terminal run came out of the merge: %+v", db.Manifest().Runs)
	}
	if blocks == 0 {
		t.Fatal("not one block was executed while the merge ran: the merge is still blocking the executor")
	}
	baseRate := float64(blocksPerRun) / base.Seconds()
	mergeRate := float64(blocks) / during.Seconds()
	t.Logf("execution: %.0f blocks/s idle, %.0f blocks/s during a %s merge (%d blocks), the boundary run took %s against %s idle",
		baseRate, mergeRate, during.Round(time.Millisecond), blocks, boundary.Round(time.Millisecond), base.Round(time.Millisecond))
	// A merge burns a core, so a dip is expected; a HALT is the defect. The bound
	// is deliberately loose because this runs on whatever machine CI has.
	if mergeRate < baseRate/4 {
		t.Fatalf("execution ran at %.0f blocks/s during the merge against %.0f idle: that is a stall, not a dip", mergeRate, baseRate)
	}
}

// TestMergeKeepsItsIOOutOfThePageCache is the page-cache rule, MEASURED. A
// merge streams gigabytes exactly once; the executor's state reads live on the
// page cache, at an 87-94% hit rate that is worth ~80% of throughput. So the
// merge must not evict them, and both halves of that are checked here against a
// CONTROL that does not use the mitigation: RecomputeMerge is the same k-way
// merge with no page-cache handling at all.
//
// The control corpus is also the byte-identity check: the same blocks under a
// trigger that never fires, merged by hand, must land on the same name the
// concurrent merge lands on.
func TestMergeKeepsItsIOOutOfThePageCache(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("page-cache residency is a Linux measurement")
	}
	if testing.Short() {
		t.Skip("builds a multi-megabyte corpus")
	}
	// THE CONTROL: MergeFanIn runs under a boundary that never fires, so the
	// inputs are all still there and nothing has merged them.
	ctlDir := t.TempDir()
	ctl, ctlCas := fatCorpus(t, ctlDir, MergeFanIn, 1<<40)
	names := make([]RunName, 0, MergeFanIn)
	for _, r := range ctl.Manifest().Runs {
		names = append(names, r.Name)
	}
	if len(names) != MergeFanIn {
		t.Fatalf("the control corpus holds %d runs, want %d", len(names), MergeFanIn)
	}

	// AN INPUT RUN, WARMED THEN DROPPED. This is what the merge does to every
	// run it consumes, on the exact mapping the read path uses.
	in, err := OpenRun(ctlCas, names[0])
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()
	for s := Section(0); s < numSections; s++ {
		if err := in.ScanRange(s, nil, nil, func(k, v []byte) bool { return true }); err != nil {
			t.Fatal(err)
		}
	}
	warm := residency(t, ctlCas.LocalPath(names[0]))
	if err := in.Cold(); err != nil {
		t.Fatal(err)
	}
	cold := residency(t, ctlCas.LocalPath(names[0]))
	t.Logf("merge input: %.0f%% of its pages resident after a full scan, %.0f%% after Cold", warm*100, cold*100)
	if warm < 0.9 {
		t.Fatalf("a fully scanned run is only %.0f%% resident: the measurement is not measuring anything", warm*100)
	}
	if cold > 0.05 {
		t.Fatalf("a merge input is still %.0f%% resident after Cold: the page-cache mitigation does not work", cold*100)
	}

	// THE OUTPUT, UNMITIGATED: the same rows through the same writer, by hand.
	ctlName, err := RecomputeMerge(ctlCas, ctlCas.SpoolDir(), [32]byte{1}, names)
	if err != nil {
		t.Fatal(err)
	}
	ctlRes := residency(t, ctlCas.SpoolPath(ctlName))

	// THE OUTPUT, THROUGH THE REAL MERGE: same blocks, trigger armed.
	dir := t.TempDir()
	db, cas := fatCorpus(t, dir, MergeFanIn, fatTerminalTxs)
	if err := db.WaitMerge(); err != nil {
		t.Fatal(err)
	}
	term := db.Manifest().Runs[0]
	if !term.Terminal() {
		t.Fatalf("no terminal run: %+v", db.Manifest().Runs)
	}
	if term.Name != ctlName {
		t.Fatalf("the concurrent merge produced %s, the serial recompute of the same inputs produced %s", term.Name, ctlName)
	}
	got := residency(t, cas.SpoolPath(term.Name))
	t.Logf("merge output: %.0f%% resident unmitigated (RecomputeMerge), %.0f%% resident through the real merge", ctlRes*100, got*100)
	if ctlRes < 0.5 {
		t.Fatalf("the unmitigated control left only %.0f%% of its output resident: the control is not a control", ctlRes*100)
	}
	if got > 0.1 {
		t.Fatalf("the merged run is %.0f%% resident: the merge is still filling the page cache with its own output", got*100)
	}
}

// residency is the fraction of a file's pages the page cache holds.
func residency(t *testing.T, path string) float64 {
	t.Helper()
	if path == "" {
		t.Fatal("no path for the artifact")
	}
	res, total, err := dist.ResidentPages(path)
	if err != nil {
		t.Fatal(err)
	}
	if total == 0 {
		t.Fatalf("%s is empty", path)
	}
	return float64(res) / float64(total)
}

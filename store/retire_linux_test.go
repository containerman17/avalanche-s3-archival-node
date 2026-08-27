package store

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/containerman17/avalanche-s3-archival-node/dist"
)

// A RETIRED RUN'S BLOCKS MUST GO BACK TO THE FILESYSTEM WHILE THE PROCESS RUNS
// (DESIGN, "local SSD = merkleized state plus 50%"). Unlinking a file this
// process still has open or mapped frees the NAME and not the BLOCKS: `du`
// stops counting it, `df` keeps counting it, and the space comes back at exit.
// Measured on mainnet C: ~8.5GB per published terminal, ~100GB a day, which
// alone puts the 200GB target out of reach.
//
// These tests measure the thing itself. ghostBytes is the same evidence
// `lsof -p <pid> | grep deleted` gives an operator, read from this process's
// own /proc entries, so a leak is a number here and not an assertion about
// intent.

// ghostBytes is what this process holds under dir that no longer has a name:
// deleted files it still has open (the credentialed road, where casfs keeps
// the spool descriptor) and deleted files it still has mapped (the local road,
// where a run is one mmap). A path counted both ways counts once.
func ghostBytes(t *testing.T, dir string) (uint64, []string) {
	t.Helper()
	abs, err := filepath.Abs(dir)
	if err != nil {
		t.Fatal(err)
	}
	held := map[string]uint64{}
	mine := func(p string) bool { return strings.HasPrefix(p, abs+string(filepath.Separator)) }

	fds, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range fds {
		target, err := os.Readlink(filepath.Join("/proc/self/fd", e.Name()))
		if err != nil {
			continue // the descriptor closed under the walk
		}
		p, ok := strings.CutSuffix(target, " (deleted)")
		if !ok || !mine(p) {
			continue
		}
		// The magic symlink still reaches the inode, so this is the real size
		// of a file with no name left.
		fi, err := os.Stat(filepath.Join("/proc/self/fd", e.Name()))
		if err != nil {
			continue
		}
		if n := uint64(fi.Size()); n > held[p] {
			held[p] = n
		}
	}

	maps, err := os.ReadFile("/proc/self/maps")
	if err != nil {
		t.Fatal(err)
	}
	mapped := map[string]uint64{}
	for _, line := range strings.Split(string(maps), "\n") {
		f := strings.SplitN(strings.TrimSpace(line), " ", 6)
		if len(f) < 6 {
			continue
		}
		p, ok := strings.CutSuffix(strings.TrimSpace(f[5]), " (deleted)")
		if !ok || !mine(p) {
			continue
		}
		lo, hi, ok := strings.Cut(f[0], "-")
		if !ok {
			continue
		}
		a, err1 := strconv.ParseUint(lo, 16, 64)
		b, err2 := strconv.ParseUint(hi, 16, 64)
		if err1 != nil || err2 != nil {
			continue
		}
		mapped[p] += b - a
	}
	for p, n := range mapped {
		if n > held[p] {
			held[p] = n
		}
	}

	var total uint64
	var names []string
	for p, n := range held {
		total += n
		names = append(names, fmt.Sprintf("%s (%.1f MB)", filepath.Base(p), float64(n)/(1<<20)))
	}
	return total, names
}

// freeBytes is the filesystem's own answer, for the record beside ghostBytes.
func freeBytes(t *testing.T, dir string) uint64 {
	t.Helper()
	var st syscall.Statfs_t
	if err := syscall.Statfs(dir, &st); err != nil {
		t.Fatal(err)
	}
	return uint64(st.Bavail) * uint64(st.Bsize)
}

// TestARetiredRunOutlivesItsReaderAndNotOneInstantLonger is BOTH FACES OF THE
// DEFECT IN ONE TEST, on the merge road, with no credentials involved.
//
// A reader takes a snapshot, a merge then retires every run in it (the manifest
// swaps and the local files are unlinked), and the reader reads on:
//
//   - IT MUST STILL GET ITS BYTES. Closing those runs at the swap, which is
//     what the merge used to do, is a use-after-munmap: the reader is between
//     the snapshot and the Get.
//   - THE BLOCKS MUST STILL BE HELD, because a mapping is exactly what "still
//     readable" means. That is the ghost this process is allowed to hold.
//   - THEY MUST GO THE MOMENT THE READER LETS GO, with no process exit, no
//     restart and nothing to wait for.
func TestARetiredRunOutlivesItsReaderAndNotOneInstantLonger(t *testing.T) {
	dir := t.TempDir()
	db := buildCorpus(t, dir, MergeFanIn-1) // one flush short of the terminal

	// The reader is now INSIDE the run set, holding it exactly the way an RPC
	// descent does between d.snapshot() and runs[i].Get().
	runs, done := db.snapshot()
	if len(runs) != MergeFanIn-1 {
		t.Fatalf("the corpus holds %d runs, want %d", len(runs), MergeFanIn-1)
	}
	want := make([][]byte, 0, len(runs))
	for _, r := range runs {
		v, ok, err := r.Get(SecChain, numKey(famPrefix[famHdr], r.Footer.FromHeight))
		if err != nil || !ok {
			t.Fatalf("run %s cannot serve its own first header: %v %v", r.Name, ok, err)
		}
		want = append(want, v)
	}

	// THE MERGE RETIRES EVERY ONE OF THEM while the reader holds them.
	for h := db.NextHeight(); h < 3*MergeFanIn; h++ {
		if err := db.WriteBlock(block(h, 2)); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := db.WaitMerge(); err != nil {
		t.Fatal(err)
	}
	if man := db.Manifest(); len(man.Runs) != 1 || !man.Runs[0].Terminal() {
		t.Fatalf("the merge did not retire the L0 runs: %+v", man.Runs)
	}
	if ents, err := os.ReadDir(filepath.Join(dir, "runs")); err != nil || len(ents) != 0 {
		t.Fatalf("the retired L0 files are still named: %v %v", ents, err)
	}

	// FACE ONE: the reader is still inside runs that no longer exist by name,
	// and it still gets the right bytes. Every read here was a segfault or a
	// closed-reader error before the refcount.
	for i, r := range runs {
		v, ok, err := r.Get(SecChain, numKey(famPrefix[famHdr], r.Footer.FromHeight))
		if err != nil || !ok || string(v) != string(want[i]) {
			t.Fatalf("retired run %s stopped answering its reader: %q %v %v", r.Name, v, ok, err)
		}
	}

	// FACE TWO: those bytes are on disk with no name, which is the ghost. It is
	// legitimate for exactly as long as the reader holds it.
	ghost, held := ghostBytes(t, dir)
	if ghost == 0 {
		t.Fatalf("no unnamed bytes are held while a reader is inside %d retired runs, so this test is not measuring anything", len(runs))
	}
	t.Logf("while the reader holds them: %d retired runs, %.2f MB unnamed and still allocated: %v", len(runs), float64(ghost)/(1<<20), held)

	freeBefore := freeBytes(t, dir)
	done() // the reader leaves: last reference, so munmap, close, blocks back

	after, still := ghostBytes(t, dir)
	if after != 0 {
		t.Fatalf("the reader let go and %.2f MB are still unnamed and allocated: %v", float64(after)/(1<<20), still)
	}
	t.Logf("the reader let go: 0 bytes unnamed, %.2f MB back to the filesystem (statfs free %.2f -> %.2f MB)",
		float64(ghost)/(1<<20), float64(freeBefore)/(1<<20), float64(freeBytes(t, dir))/(1<<20))
}

// TestPublishedRunReleasesItsBlocks is THE LEAK THAT MADE THIS A RELEASE
// BLOCKER, over a real S3 wire (MinIO): dist.Sync uploads a terminal run and
// unlinks the local copy, and the handle this process still holds on that file
// keeps every block allocated. On mainnet C that is ~8.5GB per merge and ~100GB
// a day, invisible to `du`, freed only by a restart.
//
// After the fix the run moves onto the chunk cache the instant its local copy
// goes, which is where a joiner reads it from anyway, and it keeps answering.
// Skipped unless EPOCHDB_MINIO_ENDPOINT is set; never point it at a real
// bucket.
func TestPublishedRunReleasesItsBlocks(t *testing.T) {
	endpoint := requireMinio(t)
	// Through the counting proxy: the reopen must not re-read what it already
	// has in RAM, and a GET count is the only honest way to say so.
	c := newCounter(t, endpoint)
	minioEnv(t, c.srv.URL, fmt.Sprintf("retire/%d/", time.Now().UnixNano()))
	dir := t.TempDir()
	root := [32]byte{7}

	cas, err := dist.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer cas.Close()
	db, err := Open(dir, cas, root)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	// A corpus several 4MB chunks wide, so both numbers below mean something:
	// the bytes released, and the GETs the reopen costs.
	const blocks, txs = 40, 8
	db.scaleTriggers(FlushTxs, FlushBlocks, uint64(blocks*(txs+1)*MergeFanIn))
	for r := 0; r < MergeFanIn; r++ {
		for i := 0; i < blocks; i++ {
			if err := db.WriteBlock(fatBlock(db.NextHeight(), txs)); err != nil {
				t.Fatal(err)
			}
		}
		if err := db.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.WaitMerge(); err != nil {
		t.Fatal(err)
	}
	man := db.Manifest()
	if len(man.Runs) != 1 || !man.Runs[0].Terminal() {
		t.Fatalf("no terminal run to publish: %+v", man.Runs)
	}
	terminal := man.Runs[0]
	spool := cas.SpoolPath(terminal.Name)
	fi, err := os.Stat(spool)
	if err != nil {
		t.Fatal(err)
	}
	size := uint64(fi.Size())

	head, _ := db.Head()
	hdrBefore, ok, err := db.HeaderRLP(head - 1)
	if err != nil || !ok {
		t.Fatalf("the corpus cannot serve block %d before publishing: %v %v", head-1, ok, err)
	}

	free0 := freeBytes(t, dir)
	// THE UPLOAD, then the release the bucket has confirmed, then the reopen.
	since := c.reset()
	if err := db.SyncArtifacts(); err != nil {
		t.Fatal(err)
	}
	syncGets, syncChunks := since()
	if _, err := os.Stat(spool); !os.IsNotExist(err) {
		t.Fatalf("the local copy of the published run is still named: %v", err)
	}
	ghost, held := ghostBytes(t, dir)
	if ghost != 0 {
		t.Fatalf("the published run was unlinked but this process still holds %.2f MB of it (%v): those blocks stay allocated until exit, which is the ~8.5GB per merge leak",
			float64(ghost)/(1<<20), held)
	}
	// statfs is reported and not asserted on: MinIO writes the uploaded object
	// to its own directory, which on a one-filesystem test box is the same
	// filesystem, so the free-space delta here is the release MINUS the upload.
	// ghostBytes above is the measurement that isolates this process's hold.
	free1 := freeBytes(t, dir)
	t.Logf("published terminal run %s: %.2f MB unlinked, 0 bytes still held by this process, statfs free %.1f -> %.1f MB (delta %.2f MB, net of MinIO's own copy)",
		terminal.Name[:12], float64(size)/(1<<20), float64(free0)/(1<<20), float64(free1)/(1<<20), float64(int64(free1)-int64(free0))/(1<<20))

	t.Logf("the reopen cost %d ranged GETs / %d chunks: the run's blooms and index blocks are the same bytes for the same name, so they are carried across rather than fetched again",
		syncGets, syncChunks)

	// AND IT STILL SERVES, now through the chunk cache.
	hdrAfter, ok, err := db.HeaderRLP(head - 1)
	if err != nil || !ok || string(hdrAfter) != string(hdrBefore) {
		t.Fatalf("the published run stopped answering after its local copy went: %q %v %v", hdrAfter, ok, err)
	}
	for h := uint64(0); h <= head; h += 7 {
		if _, ok, err := db.HeaderRLP(h); err != nil || !ok {
			t.Fatalf("block %d is unreadable after the release: %v %v", h, ok, err)
		}
	}
}

// TestReadersRaceARetiringMerge is the same defect under load and under -race:
// readers hammer the descent from every direction while a merge retires the
// runs they are walking. It must not race, must not deadlock and must not lose
// a row.
func TestReadersRaceARetiringMerge(t *testing.T) {
	dir := t.TempDir()
	db := buildCorpus(t, dir, MergeFanIn-1)
	head, _ := db.Head()

	stop := make(chan struct{})
	var wg sync.WaitGroup
	fail := make(chan error, 16)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(seed uint64) {
			defer wg.Done()
			for n := seed; ; n++ {
				select {
				case <-stop:
					return
				default:
				}
				h := n % (head + 1)
				v, ok, err := db.HeaderRLP(h)
				if err != nil || !ok || string(v) != fmt.Sprintf("header-%d", h) {
					select {
					case fail <- fmt.Errorf("header %d came back %q ok=%v err=%v", h, v, ok, err):
					default:
					}
					return
				}
				if _, _, _, err := db.BlockTxRange(h); err != nil {
					select {
					case fail <- fmt.Errorf("blk %d: %w", h, err):
					default:
					}
					return
				}
			}
		}(uint64(i) * 977)
	}

	// The merge runs under all of that.
	for h := db.NextHeight(); h < 3*MergeFanIn; h++ {
		if err := db.WriteBlock(block(h, 2)); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := db.WaitMerge(); err != nil {
		t.Fatal(err)
	}
	// Keep reading past the retirement, then stop.
	time.Sleep(50 * time.Millisecond)
	close(stop)
	wg.Wait()
	select {
	case err := <-fail:
		t.Fatal(err)
	default:
	}
	if man := db.Manifest(); len(man.Runs) != 1 || !man.Runs[0].Terminal() {
		t.Fatalf("the merge did not happen under the readers: %+v", man.Runs)
	}
	// Every retired file is gone AND its blocks with it: the readers all
	// returned, so nothing holds them.
	if ghost, held := ghostBytes(t, dir); ghost != 0 {
		t.Fatalf("%.2f MB are still unnamed and allocated after every reader returned: %v", float64(ghost)/(1<<20), held)
	}
}

package store

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/containerman17/epochdb/dist"
)

// THE BUCKET IS WRITE-ONCE AND FOREVER (DESIGN, storage v0). These are the
// tests of the upload gate and the pointer cadence: only terminal runs upload,
// the pointer advances once per terminal, everything the pointer names is
// resident, and a chain that has not earned a terminal publishes NOTHING.

// spoolRuns is what this data dir would put in a bucket: the hash-named files
// in the spool. Without credentials the spool IS the bucket, so a local test
// reads the same truth a credentialed one does.
func spoolRuns(t *testing.T, dir string) []string {
	t.Helper()
	ents, err := os.ReadDir(filepath.Join(dir, "cas"))
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, e := range ents {
		if dist.ValidHash(e.Name()) {
			out = append(out, e.Name())
		}
	}
	return out
}

// TestGateNeverUploadsAnL0 walks a corpus up to its first terminal one flush at
// a time. Until the terminal exists the spool is EMPTY, no pointer exists and
// Publish does nothing; the terminal is the first artifact of the whole engine
// that becomes publishable.
func TestGateNeverUploadsAnL0(t *testing.T) {
	dir := t.TempDir()
	root := [32]byte{1}
	db := buildCorpus(t, dir, MergeFanIn-1)

	man := db.Manifest()
	if len(man.Runs) != MergeFanIn-1 {
		t.Fatalf("want %d L0 runs, got %d", MergeFanIn-1, len(man.Runs))
	}
	for _, r := range man.Runs {
		if r.Terminal() {
			t.Fatalf("run %+v is terminal before the boundary", r)
		}
	}
	// The L0 runs are on this machine and nowhere else, under names that read.
	ents, err := os.ReadDir(filepath.Join(dir, "runs"))
	if err != nil {
		t.Fatal(err)
	}
	if len(ents) != MergeFanIn-1 {
		t.Fatalf("the local run directory holds %d files, want %d", len(ents), MergeFanIn-1)
	}
	for _, e := range ents {
		if !strings.HasPrefix(e.Name(), "l0-") {
			t.Fatalf("local run file %q does not carry its level and tx range", e.Name())
		}
	}
	if got := spoolRuns(t, dir); len(got) != 0 {
		t.Fatalf("the gate let %d artifact(s) into the spool before any terminal existed: %v", len(got), got)
	}
	// PUBLISH IS A NO-OP AND LEAVES NO POINTER: this chain publishes nothing.
	if err := db.Publish(); err != nil {
		t.Fatal(err)
	}
	if _, err := db.cas.Latest(root); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("a chain with no terminal run published a pointer: %v", err)
	}

	// THE FLUSH THAT EARNS THE TERMINAL.
	for h := uint64(3 * (MergeFanIn - 1)); h < 3*MergeFanIn; h++ {
		if err := db.WriteBlock(block(h, 2)); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	man = db.Manifest()
	if len(man.Runs) != 1 || !man.Runs[0].Terminal() {
		t.Fatalf("the boundary did not cut a terminal run: %+v", man.Runs)
	}
	if got := spoolRuns(t, dir); len(got) != 2 {
		t.Fatalf("the spool holds %v, want the terminal run and the published manifest", got)
	}

	// THE PUBLISHED MANIFEST NAMES ONLY WHAT THE BUCKET HOLDS.
	pub := readPublished(t, db.cas, dir, root)
	if len(pub.Runs) != 1 || pub.Runs[0].Name != man.Runs[0].Name {
		t.Fatalf("published manifest lists %+v, want the one terminal run", pub.Runs)
	}
	for _, r := range pub.Runs {
		if _, err := os.Stat(db.cas.SpoolPath(r.Name)); err != nil {
			t.Fatalf("the published manifest names %s, which is not in the bucket: %v", r.Name, err)
		}
	}

	// THE LOCAL LIVE MANIFEST RUNS AHEAD, AND THE POINTER DOES NOT MOVE FOR IT.
	before, err := db.cas.Latest(root)
	if err != nil {
		t.Fatal(err)
	}
	for h := uint64(3 * MergeFanIn); h < 3*MergeFanIn+9; h++ {
		if err := db.WriteBlock(block(h, 2)); err != nil {
			t.Fatal(err)
		}
		if h%3 == 2 {
			if err := db.Flush(); err != nil {
				t.Fatal(err)
			}
		}
	}
	if got := len(db.Manifest().Runs); got != 4 {
		t.Fatalf("the live manifest holds %d runs, want the terminal plus three L0s", got)
	}
	after, err := db.cas.Latest(root)
	if err != nil {
		t.Fatal(err)
	}
	if after != before {
		t.Fatalf("the pointer moved for an L0 flush: %v -> %v", before, after)
	}
	if got := spoolRuns(t, dir); len(got) != 2 {
		t.Fatalf("three L0 flushes changed the bucket's contents: %v", got)
	}
}

// TestAChainUnderTheBoundaryPublishesNothing is numine, at the REAL constants.
// It has 1,066,587 transactions over 1,036,335 blocks, so it flushes on the
// 50,000-BLOCK trigger into twenty-one L0 runs and never comes near the eight
// million transactions a terminal run is. It therefore publishes nothing at all
// and is joined over p2p, which is twenty minutes for a chain that size.
func TestAChainUnderTheBoundaryPublishesNothing(t *testing.T) {
	const numineTxs, numineBlocks = 1_066_587, 1_036_335
	if numineTxs >= TerminalTxs {
		t.Fatalf("numine's %d txs reach the terminal boundary of %d", numineTxs, TerminalTxs)
	}
	numineRuns := (numineBlocks + FlushBlocks - 1) / FlushBlocks
	if numineRuns < MergeFanIn {
		t.Fatalf("numine flushes %d runs, which no longer exercises the run count", numineRuns)
	}

	dir := t.TempDir()
	root := [32]byte{1}
	cas, err := dist.Local(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer cas.Close()
	db, err := Open(dir, cas, root) // REAL triggers: no scaling here, that is the point
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	// numine's run count, not its block count: the assertion is that MORE THAN
	// MergeFanIn L0 runs still earn nothing, because the boundary is TxNum.
	h := uint64(0)
	for r := 0; r < numineRuns; r++ {
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
	man := db.Manifest()
	if len(man.Runs) != numineRuns {
		t.Fatalf("%d L0 runs merged into %d: a chain under the boundary must not merge", numineRuns, len(man.Runs))
	}
	for _, r := range man.Runs {
		if r.Terminal() {
			t.Fatalf("a chain under the boundary produced a terminal run: %+v", r)
		}
	}
	if got := spoolRuns(t, dir); len(got) != 0 {
		t.Fatalf("a chain that published nothing put %v in the bucket", got)
	}
	if _, err := cas.Latest(root); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("a chain that published nothing has a pointer: %v", err)
	}
}

// TestConsumerJoinsWhileTheProducerIsAhead is the whole shape over a REAL S3
// wire (MinIO): the producer publishes its terminal and keeps going, the bucket
// holds the terminal and NOTHING ELSE, and a consumer that joins mid-flight
// gets a complete corpus that ends at the terminal boundary. Skipped unless
// EPOCHDB_MINIO_ENDPOINT is set; never point it at a real bucket.
func TestConsumerJoinsWhileTheProducerIsAhead(t *testing.T) {
	endpoint := requireMinio(t)
	prefix := fmt.Sprintf("publish/%d/", time.Now().UnixNano())
	minioEnv(t, endpoint, prefix)
	root := [32]byte{5}

	prod := t.TempDir()
	cas, err := dist.Open(prod)
	if err != nil {
		t.Fatal(err)
	}
	defer cas.Close()
	db, err := Open(prod, cas, root)
	if err != nil {
		t.Fatal(err)
	}
	db.scaleTriggers(FlushTxs, FlushBlocks, testTerminalTxs)
	write := func(runs int) {
		t.Helper()
		for r := 0; r < runs; r++ {
			for i := 0; i < 3; i++ {
				h := db.NextHeight()
				if err := db.WriteBlock(block(h, 2)); err != nil {
					t.Fatal(err)
				}
			}
			if err := db.Flush(); err != nil {
				t.Fatal(err)
			}
		}
	}
	write(MergeFanIn) // earns the terminal, which publishes the pointer
	if err := cas.Sync(); err != nil {
		t.Fatal(err)
	}
	// THE PRODUCER RUNS AHEAD. Five more flushes, no new terminal, and a Sync
	// on top of them: the bucket must not change.
	write(5)
	if err := cas.Sync(); err != nil {
		t.Fatal(err)
	}
	live := db.Manifest()
	if len(live.Runs) != 6 || !live.Runs[0].Terminal() {
		t.Fatalf("producer's live set is %+v, want one terminal plus five L0s", live.Runs)
	}
	db.Close()

	// THE CONSUMER: a fresh dir with nothing but credentials.
	cons := t.TempDir()
	ccas, err := dist.Open(cons)
	if err != nil {
		t.Fatal(err)
	}
	defer ccas.Close()
	if err := Join(ccas, cons, root); err != nil {
		t.Fatal(err)
	}
	cdb, err := Open(cons, ccas, root)
	if err != nil {
		t.Fatal(err)
	}
	defer cdb.Close()

	joined := cdb.Manifest()
	if len(joined.Runs) != 1 || joined.Runs[0].Name != live.Runs[0].Name {
		t.Fatalf("the consumer joined %+v, want the one terminal run", joined.Runs)
	}
	// A COMPLETE FINAL SET: every block up to the terminal boundary serves out
	// of the bucket, and nothing above it is even claimed.
	head := live.Runs[0].ToHeight
	for h := uint64(0); h <= head; h++ {
		hdr, ok, err := cdb.HeaderRLP(h)
		if err != nil || !ok || string(hdr) != fmt.Sprintf("header-%d", h) {
			t.Fatalf("the joined consumer cannot serve header %d: %q %v %v", h, hdr, ok, err)
		}
	}
	if got, ok := cdb.Head(); !ok || got != head {
		t.Fatalf("the consumer's head is %d (%v), want the terminal boundary %d", got, ok, head)
	}
	if _, ok, err := cdb.HeaderRLP(head + 1); ok || err != nil {
		t.Fatalf("the consumer claims a block above the published boundary: %v %v", ok, err)
	}

	// THE GATE, OVER THE WIRE: not one L0 run reached the bucket. The consumer
	// has no local copy of any of them, so an Open that succeeds could only mean
	// the bucket served it.
	for _, r := range live.Runs[1:] {
		if blob, err := ccas.Open(r.Name); err == nil {
			blob.Close()
			t.Fatalf("L0 run %s is in the bucket", r.Name)
		}
	}
}

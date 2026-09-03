package store

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/containerman17/avalanche-s3-archival-node/dist"
)

// TestSidecarRoundTrip pins the sidecar lifecycle: the first open of a run
// writes one and re-points its overlay at the mapping, a second open loads it
// without touching the artifact's tail, and a corrupt sidecar is rebuilt
// rather than trusted.
func TestSidecarRoundTrip(t *testing.T) {
	db, dir := testDB(t)
	for h := uint64(0); h < 3; h++ {
		if err := db.WriteBlock(block(h, 4)); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	cas, err := dist.Local(dir)
	if err != nil {
		t.Fatal(err)
	}
	name := db.Manifest().Runs[0].Name

	probe := Suffixed(SlotPrefix(addr(2), hash32(7)), 5)
	check := func(r *Run, when string) {
		t.Helper()
		if !r.MayHave(SecState, probe) {
			t.Fatalf("%s: bloom missed a present slot", when)
		}
		if r.sec[SecState].mp == nil {
			t.Fatalf("%s: state overlay is heap, not the sidecar mapping", when)
		}
	}

	r1, err := OpenRun(cas, name)
	if err != nil {
		t.Fatal(err)
	}
	side := residentPath(cas, name)
	if _, err := os.Stat(side); err != nil {
		t.Fatalf("first open left no sidecar: %v", err)
	}
	check(r1, "build open")
	r1.Close()

	r2, err := OpenRun(cas, name)
	if err != nil {
		t.Fatal(err)
	}
	check(r2, "sidecar open")
	r2.Close()

	// A torn sidecar (here: truncated mid-payload) must rebuild, not fail.
	raw, err := os.ReadFile(side)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(side, raw[:len(raw)-7], 0o644); err != nil {
		t.Fatal(err)
	}
	r3, err := OpenRun(cas, name)
	if err != nil {
		t.Fatalf("open with a torn sidecar: %v", err)
	}
	check(r3, "rebuilt open")
	r3.Close()
	if st, err := os.Stat(side); err != nil || st.Size() != int64(len(raw)) {
		t.Fatalf("torn sidecar was not rewritten whole: %v size=%d want %d", err, st.Size(), len(raw))
	}

	// A stale-but-well-formed sidecar (bytes from another run's walk shape:
	// simulate by an empty valid file) must be detected by the handle check
	// and rebuilt.
	if err := os.WriteFile(side, append([]byte(resMagic), 0, 0, 0, 0), 0o644); err != nil {
		t.Fatal(err)
	}
	r4, err := OpenRun(cas, name)
	if err != nil {
		t.Fatalf("open with a stale sidecar: %v", err)
	}
	check(r4, "stale-rebuilt open")
	r4.Close()

	// The sweep removes sidecars for runs the manifest no longer names.
	stray := filepath.Join(filepath.Dir(side), "deadbeef")
	if err := os.WriteFile(stray, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	m := db.Manifest()
	sweepSidecars(cas, &m)
	if _, err := os.Stat(stray); !os.IsNotExist(err) {
		t.Fatal("sweep kept a sidecar for a run not in the manifest")
	}
	if _, err := os.Stat(side); err != nil {
		t.Fatal("sweep removed a live run's sidecar")
	}
}

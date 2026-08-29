package store

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ava-labs/libevm/common"
)

// TestScanRunsIsConcurrent pins the whole point of scanRuns: the per-run scans
// are in flight AT THE SAME TIME. Every work blocks until scanFanout of them
// are running, so a sequential loop cannot get past the first one.
func TestScanRunsIsConcurrent(t *testing.T) {
	var inflight, peak atomic.Int32
	full := make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(full) }) }
	work := func(i int) (int, error) {
		n := inflight.Add(1)
		for p := peak.Load(); n > p && !peak.CompareAndSwap(p, n); p = peak.Load() {
		}
		if n >= int32(scanFanout) {
			release()
		}
		select {
		case <-full:
		case <-time.After(5 * time.Second):
			// Serialized: nothing else will ever join, so let the rest through
			// once instead of paying the timeout per run.
			t.Errorf("run %d waited alone: the scans are serialized", i)
			release()
		}
		inflight.Add(-1)
		return i, nil
	}
	var got []int
	if err := scanRuns(2*scanFanout, work, func(v int) bool { got = append(got, v); return true }); err != nil {
		t.Fatal(err)
	}
	if peak.Load() < int32(scanFanout) {
		t.Fatalf("peak concurrency %d, want %d", peak.Load(), scanFanout)
	}
	for i, v := range got {
		if v != i {
			t.Fatalf("emitted out of order at %d: %v", i, got)
		}
	}
	if len(got) != 2*scanFanout {
		t.Fatalf("emitted %d of %d", len(got), 2*scanFanout)
	}
}

// TestScanRunsOrder: results arrive in run order however the scans complete.
func TestScanRunsOrder(t *testing.T) {
	const n = 40
	work := func(i int) (int, error) {
		time.Sleep(time.Duration(n-i) * time.Millisecond) // completions reversed
		return i, nil
	}
	var got []int
	if err := scanRuns(n, work, func(v int) bool { got = append(got, v); return true }); err != nil {
		t.Fatal(err)
	}
	for i, v := range got {
		if v != i {
			t.Fatalf("emitted out of order at %d: %v", i, got)
		}
	}
}

// TestScanRunsStop: emit returning false keeps what it accepted, emits nothing
// after it, and stops STARTING work (a 400-run walk stopped at 0 must not have
// touched 400 runs).
func TestScanRunsStop(t *testing.T) {
	const n = 400
	var started atomic.Int32
	work := func(i int) (int, error) { started.Add(1); return i, nil }
	var got []int
	if err := scanRuns(n, work, func(v int) bool {
		got = append(got, v)
		return v < 2
	}); err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || got[2] != 2 {
		t.Fatalf("got %v", got)
	}
	// scanRuns has returned, so every goroutine it started is done.
	if s := started.Load(); s >= n {
		t.Fatalf("started %d of %d runs after a stop at run 2", s, n)
	}
}

// TestScanRunsFirstError: the LOWEST failing index wins, as the sequential
// loop's first error did, whatever order the failures complete in.
func TestScanRunsFirstError(t *testing.T) {
	early, late := errors.New("run 1"), errors.New("run 3")
	work := func(i int) (int, error) {
		switch i {
		case 1:
			time.Sleep(50 * time.Millisecond) // completes after 3
			return 0, early
		case 3:
			return 0, late
		}
		return i, nil
	}
	var got []int
	err := scanRuns(10, work, func(v int) bool { got = append(got, v); return true })
	if !errors.Is(err, early) {
		t.Fatalf("err = %v, want %v", err, early)
	}
	if len(got) != 1 || got[0] != 0 {
		t.Fatalf("emitted %v past the failing run", got)
	}
}

// TestSetScanFanoutMatchesSequential: over a real multi-run corpus, the
// concurrent SetScan yields exactly what a sequential run-by-run walk does,
// deduped and in the same order, and an early stop cuts that same sequence.
func TestSetScanFanoutMatchesSequential(t *testing.T) {
	db := writeFixture(t, t.TempDir())
	defer db.Close()
	prefix := SetPrefix(hash32(0xA0), 1, common.BytesToHash(addr(21)).Bytes())

	runs, done := db.snapshot()
	var want []string
	seen := map[string]bool{}
	for _, r := range runs {
		if err := r.ScanSet(prefix, func(k []byte) bool {
			if !seen[string(k)] {
				seen[string(k)] = true
				want = append(want, string(k))
			}
			return true
		}); err != nil {
			t.Fatal(err)
		}
	}
	done()
	for _, k := range db.windowSets(prefix) {
		if !seen[string(k)] {
			seen[string(k)] = true
			want = append(want, string(k))
		}
	}
	if len(want) < 2 {
		t.Fatalf("corpus is too thin to test: %d keys", len(want))
	}

	var got []string
	if err := db.SetScan(prefix, func(k []byte) bool { got = append(got, string(k)); return true }); err != nil {
		t.Fatal(err)
	}
	if len(got) != len(want) {
		t.Fatalf("SetScan gave %d keys, sequential gave %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("key %d differs from the sequential walk", i)
		}
	}

	var early []string
	if err := db.SetScan(prefix, func(k []byte) bool {
		early = append(early, string(k))
		return len(early) < 2
	}); err != nil {
		t.Fatal(err)
	}
	if len(early) != 2 || early[0] != want[0] || early[1] != want[1] {
		t.Fatalf("early stop gave %d keys, not the first two of the sequential walk", len(early))
	}
}

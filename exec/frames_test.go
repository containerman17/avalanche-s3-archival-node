package exec

import (
	"math/big"
	"testing"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/vm"

	"github.com/containerman17/epochdb/store"
)

// THE FAIL-STOP IS THE DESIGN (DESIGN, principles: "traces are stored, and
// capture failure is death"), so what it fires on is worth pinning. take()
// reporting ok on a trace that is not what execution did is the one way a
// block reaches the store with garbage frames, and both holes are silent by
// nature: an unarmed capture yields NO frames and an unbalanced one yields
// frames whose gasUsed is 0, which reads back as a real zero-gas success.
func TestFrameCaptureRefusesAHoleAndAccountsForAWholeTx(t *testing.T) {
	c := newFrameCapture()
	l := frameLogger{c}

	// 1. The tracer never ran: the saexec seam at this pin.
	if _, _, why := c.take(); why == "" {
		t.Error("take() reported a good trace for a transaction the tracer never saw")
	}

	// 2. A balanced transaction round trips through the stored record.
	l.CaptureTxStart(21000)
	l.CaptureStart(nil, common.Address{1}, common.Address{2}, false, nil, 0, nil)
	l.CaptureEnter(vm.CALL, common.Address{2}, common.Address{3}, []byte("in"), 500, big.NewInt(7))
	l.CaptureExit([]byte("out"), 400, nil)
	rec, addrs, why := c.take()
	if why != "" {
		t.Fatalf("balanced trace refused: %s", why)
	}
	frames, err := store.DecodeFrames(rec)
	if err != nil {
		t.Fatal(err)
	}
	if len(frames) != 1 {
		t.Fatalf("got %d frames, want 1 (the top-level frame is excluded)", len(frames))
	}
	f := frames[0]
	if f.From != (common.Address{2}) || f.To != (common.Address{3}) ||
		f.Gas != 500 || f.GasUsed != 400 || f.Value.Int64() != 7 ||
		string(f.Input) != "in" || string(f.Output) != "out" || f.Failed {
		t.Errorf("frame round trip lost data: %+v", f)
	}
	// from, to and the callee are all participants of the addr/ postings.
	if len(addrs) != 3 {
		t.Errorf("got %d participants, want 3", len(addrs))
	}

	// 3. A transaction that made no nested call is a REAL answer, not a hole:
	// an empty record, and take() must not confuse it with a missing tracer.
	l.CaptureTxStart(21000)
	l.CaptureStart(nil, common.Address{1}, common.Address{2}, false, nil, 0, nil)
	rec, _, why = c.take()
	if why != "" {
		t.Errorf("a plain value transfer was treated as an uncaptured trace: %s", why)
	}
	if len(rec) != 0 {
		t.Errorf("a transfer with no nested call stored %d bytes of frames", len(rec))
	}

	// 4. THE REGRESSION: an enter with no exit. Before this guard take()
	// reported ok and stored the open frame with gasUsed 0, no output and no
	// error flag, which is indistinguishable from a real zero-gas success once
	// it is in a run. libevm pairs enter with a deferred exit at this pin, so
	// the guard is here to make a dep bump loud rather than quiet.
	l.CaptureTxStart(21000)
	l.CaptureStart(nil, common.Address{1}, common.Address{2}, false, nil, 0, nil)
	l.CaptureEnter(vm.CALL, common.Address{2}, common.Address{3}, []byte("in"), 500, big.NewInt(7))
	l.CaptureEnter(vm.CALL, common.Address{3}, common.Address{4}, []byte("in2"), 400, nil)
	l.CaptureExit([]byte("out2"), 300, nil) // only the inner frame closes
	if _, _, why := c.take(); why == "" {
		t.Error("PARTIAL FRAMES ACCEPTED: the call stack never closed and take() reported a good trace")
	}

	// 5. THE COLLAPSE MUST NOT EAT A REAL CALL. reannounced() folds two enters
	// into one only when they name the SAME call, and the forwarded gas is what
	// separates them: a frame's first nested call is always given strictly less
	// gas than the frame itself. Same participants, same input, one gas apart
	// is two frames, and the sole real difference from case 4 is the exit.
	l.CaptureTxStart(21000)
	l.CaptureStart(nil, common.Address{1}, common.Address{2}, false, nil, 0, nil)
	l.CaptureEnter(vm.CALL, common.Address{2}, common.Address{3}, []byte("in"), 500, nil)
	l.CaptureEnter(vm.CALL, common.Address{2}, common.Address{3}, []byte("in"), 499, nil)
	l.CaptureExit([]byte("inner"), 100, nil)
	l.CaptureExit([]byte("outer"), 200, nil)
	rec, _, why = c.take()
	if why != "" {
		t.Fatalf("two real calls one gas apart were refused: %s", why)
	}
	frames, err = store.DecodeFrames(rec)
	if err != nil {
		t.Fatal(err)
	}
	if len(frames) != 2 || frames[0].Gas != 500 || frames[1].Gas != 499 || frames[1].Depth != 1 {
		t.Errorf("a real nested call was collapsed into its parent: %+v", frames)
	}
}

// A REFUSED TRANSACTION MUST NOT POISON THE NEXT ONE: take() resets on the way
// out of every path, so the transaction after a hole starts clean rather than
// inheriting the open frames that caused the hole.
func TestFrameCaptureResetsAfterAHole(t *testing.T) {
	c := newFrameCapture()
	l := frameLogger{c}

	l.CaptureTxStart(21000)
	l.CaptureEnter(vm.CALL, common.Address{2}, common.Address{3}, []byte("in"), 500, nil)
	if _, _, why := c.take(); why == "" {
		t.Fatal("unbalanced take() reported a good trace")
	}

	l.CaptureTxStart(21000)
	l.CaptureEnter(vm.CALL, common.Address{5}, common.Address{6}, []byte("x"), 100, nil)
	l.CaptureExit(nil, 50, nil)
	rec, _, why := c.take()
	if why != "" {
		t.Fatalf("the transaction after a hole was refused: %s", why)
	}
	frames, err := store.DecodeFrames(rec)
	if err != nil {
		t.Fatal(err)
	}
	if len(frames) != 1 || frames[0].To != (common.Address{6}) {
		t.Errorf("the transaction after a hole inherited frames: %+v", frames)
	}
}

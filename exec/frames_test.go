package exec

import (
	"encoding/binary"
	"math/big"
	"testing"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/vm"

	"github.com/containerman17/epochdb/state"
)

func a(b byte) common.Address { return common.Address{b} }

// TestFrameCaptureRecord drives the EVMLogger hooks exactly as the EVM does
// (CaptureTxStart, CaptureStart, nested Enter/Exit, CaptureTxEnd) and checks
// the two records that come out: frames in ENTER order with their depth, and
// one participants group per transaction whatever that transaction did.
func TestFrameCaptureRecord(t *testing.T) {
	l := frameLogger{newFrameCapture()}
	c := l.c
	c.begin()

	// tx 0: eoa -> contract, which calls a token, which delegatecalls a lib.
	l.CaptureTxStart(100000)
	l.CaptureStart(nil, a(1), a(2), false, []byte("top"), 100000, big.NewInt(5))
	l.CaptureEnter(vm.CALL, a(2), a(3), []byte("inner"), 90000, big.NewInt(7))
	l.CaptureEnter(vm.DELEGATECALL, a(3), a(4), []byte("lib"), 80000, nil)
	l.CaptureExit([]byte("libout"), 1000, nil)
	l.CaptureExit(nil, 5000, vm.ErrExecutionReverted)
	l.CaptureTxEnd(0)

	// tx 1: a plain transfer, no nested frame at all.
	l.CaptureTxStart(21000)
	l.CaptureStart(nil, a(1), a(9), false, nil, 21000, big.NewInt(1))
	l.CaptureTxEnd(0)

	rec := c.record()
	if rec == nil {
		t.Fatal("a block with transactions must produce a record")
	}
	framesRec, partsRec, err := state.DecodeTailItx(rec)
	if err != nil {
		t.Fatal(err)
	}

	// Participants: one group per tx, positional, deduped, first-appearance.
	var groups [][][20]byte
	if err := state.DecodeParticipants(partsRec, func(addrs [][20]byte) {
		groups = append(groups, append([][20]byte(nil), addrs...))
	}); err != nil {
		t.Fatal(err)
	}
	if len(groups) != 2 {
		t.Fatalf("%d participant groups, want one per transaction", len(groups))
	}
	want0 := [][20]byte{{1}, {2}, {3}, {4}}
	for i, w := range want0 {
		if groups[0][i] != w {
			t.Fatalf("tx 0 participants %v, want %v (top-level pair then every frame's, deduped)", groups[0], want0)
		}
	}
	if len(groups[0]) != len(want0) {
		t.Fatalf("tx 0 participants %v, want %v", groups[0], want0)
	}
	if len(groups[1]) != 2 || groups[1][0] != [20]byte{1} || groups[1][1] != [20]byte{9} {
		t.Fatalf("tx 1 participants %v, want the sender and the recipient", groups[1])
	}

	// Frames: only tx 0 has any, in ENTER order, the outer one first.
	txIdx, n := binary.Uvarint(framesRec)
	if txIdx != 0 || n <= 0 {
		t.Fatalf("first frame group is for tx %d", txIdx)
	}
	nFrames, k := binary.Uvarint(framesRec[n:])
	pos := n + k
	if nFrames != 2 {
		t.Fatalf("%d frames, want 2", nFrames)
	}
	type got struct {
		kind, depth   byte
		from, to      common.Address
		value         []byte
		gas, gasUsed  uint64
		failed        bool
		input, output []byte
	}
	var frames []got
	for i := uint64(0); i < nFrames; i++ {
		var f got
		f.kind, f.depth = framesRec[pos], framesRec[pos+1]
		copy(f.from[:], framesRec[pos+2:pos+22])
		copy(f.to[:], framesRec[pos+22:pos+42])
		pos += 42
		rd := func() []byte {
			ln, k := binary.Uvarint(framesRec[pos:])
			pos += k
			b := framesRec[pos : pos+int(ln)]
			pos += int(ln)
			return b
		}
		f.value = rd()
		var k int
		f.gas, k = binary.Uvarint(framesRec[pos:])
		pos += k
		f.gasUsed, k = binary.Uvarint(framesRec[pos:])
		pos += k
		f.failed = framesRec[pos] == 1
		pos++
		f.input, f.output = rd(), rd()
		frames = append(frames, f)
	}
	if pos != len(framesRec) {
		t.Fatalf("decoded %d of %d bytes", pos, len(framesRec))
	}
	if frames[0].kind != byte(vm.CALL) || frames[0].depth != 0 || frames[0].to != a(3) {
		t.Fatalf("frame 0 %+v: want the OUTER call first (enter order)", frames[0])
	}
	if string(frames[0].value) != "\x07" || frames[0].gas != 90000 || frames[0].gasUsed != 5000 || !frames[0].failed {
		t.Fatalf("frame 0 %+v: exit half must be patched onto the enter half", frames[0])
	}
	if string(frames[0].input) != "inner" || len(frames[0].output) != 0 {
		t.Fatalf("frame 0 payloads: %q %q", frames[0].input, frames[0].output)
	}
	if frames[1].kind != byte(vm.DELEGATECALL) || frames[1].depth != 1 || frames[1].to != a(4) {
		t.Fatalf("frame 1 %+v: want the nested delegatecall at depth 1", frames[1])
	}
	if string(frames[1].output) != "libout" || frames[1].failed {
		t.Fatalf("frame 1 %+v", frames[1])
	}

	// A new block starts clean, and a block with no transactions has no
	// record: that is what the seal's no-records rule keys on.
	c.begin()
	if rec := c.record(); rec != nil {
		t.Fatalf("empty block produced %d bytes", len(rec))
	}
}

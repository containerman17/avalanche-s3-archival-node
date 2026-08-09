package state

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/containerman17/epochdb/dist"
)

// synthRcpt is a stand-in for the executor's live capture: nTxs receipt
// entries (uvarint gasUsed + status byte) and no logs, wrapped in the tail
// framing. Seal REFUSES a tx-bearing block without one, so every synthetic
// corpus has to write them exactly as exec does.
func synthRcpt(nTxs int, n uint64) []byte {
	var rr []byte
	for i := 0; i < nTxs; i++ {
		rr = binary.AppendUvarint(rr, 21000+n)
		rr = append(rr, 1)
	}
	return EncodeTailRcpt(nil, rr)
}

// synthItx is the frames-family twin of synthRcpt: one participants group per
// transaction (one address, derived from the block) and one call frame for the
// first transaction, which is enough to exercise both V2 sections.
func synthItx(nTxs int, n uint64) []byte {
	if nTxs == 0 {
		return nil
	}
	var parts []byte
	for i := 0; i < nTxs; i++ {
		a := synthParticipant(n, i)
		parts = binary.AppendUvarint(parts, 1)
		parts = append(parts, a[:]...)
	}
	from, to := synthParticipant(n, 0), synthParticipant(n, 1)
	frame := []byte{0xf1, 0} // CALL at depth 0
	frame = append(frame, from[:]...)
	frame = append(frame, to[:]...)
	frame = append(frame, 0)                   // no value
	frame = binary.AppendUvarint(frame, 21000) // gas
	frame = binary.AppendUvarint(frame, 2100)  // gasUsed
	frame = append(frame, 0, 0, 0)             // no error, no input, no output
	rec := binary.AppendUvarint(nil, 0)        // tx index 0
	rec = binary.AppendUvarint(rec, 1)         // one frame
	return EncodeTailItx(append(rec, frame...), parts)
}

// synthParticipant is a deterministic address for block n's tx i.
func synthParticipant(n uint64, i int) (a [20]byte) {
	binary.BigEndian.PutUint64(a[:8], n)
	a[19] = byte(i)
	return
}

// fixedEpochTxs pins the seal schedule to a flat n for the duration of a
// test. The real schedule starts at 250,000 txs, so a synthetic corpus of a
// few dozen txs would seal nothing at all; this is the only override that
// exists and it is test-only (see epochTxsAt).
func fixedEpochTxs(t *testing.T, n uint64) {
	prev := epochTxsAt
	t.Cleanup(func() { epochTxsAt = prev })
	epochTxsAt = func(int) uint64 { return n }
}

// TestEpochTxsSchedule pins the ruled boundary schedule: doubling from 250k,
// flat at 8M from epoch 5 on, and never wrapping at a large index.
func TestEpochTxsSchedule(t *testing.T) {
	want := []uint64{250_000, 500_000, 1_000_000, 2_000_000, 4_000_000, 8_000_000, 8_000_000, 8_000_000}
	for i, w := range want {
		if got := EpochTxsAt(i); got != w {
			t.Fatalf("EpochTxsAt(%d) = %d, want %d", i, got, w)
		}
	}
	if got := EpochTxsAt(1000); got != 8_000_000 {
		t.Fatalf("EpochTxsAt(1000) = %d, want 8000000", got)
	}
}

// TestSealCutAndResume drives the sealer end-to-end on synthetic staging +
// capture: fixed 3 txs/block, boundary at 10 txs => epochs of 4 blocks;
// blocks beyond the exec head stay raw; re-running seals nothing new.
func TestSealCutAndResume(t *testing.T) {
	dir := t.TempDir()

	// staging containers, blocks 1..10, 3 txs each (writeStagingBlock is
	// the txindex test helper).
	txHashes := map[uint64][]struct{ h [32]byte }{}
	for n := uint64(1); n <= 10; n++ {
		hs, _ := writeStagingBlock(t, dir, 0, n, 3)
		for _, h := range hs {
			txHashes[n] = append(txHashes[n], struct{ h [32]byte }{[32]byte(h)})
		}
	}

	st, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	addr := [20]byte{0xaa}
	slotKey := synthKey('s', 42)
	_ = addr
	for n := uint64(1); n <= 10; n++ {
		if err := st.AppendHeader(n, []byte{0x99, byte(n), 0x77, 0x66}); err != nil {
			t.Fatal(err)
		}
		// one storage write per block for the SST
		var frame []byte
		frame = append(frame, recKindStorage)
		frame = append(frame, slotKey[1:53]...)
		frame = binary.AppendUvarint(frame, 1)
		frame = append(frame, byte(n))
		if err := st.AppendWrites(n, frame); err != nil {
			t.Fatal(err)
		}
		if err := st.AppendRcpt(n, synthRcpt(3, n)); err != nil {
			t.Fatal(err)
		}
		if err := st.AppendItx(n, synthItx(3, n)); err != nil {
			t.Fatal(err)
		}
		// logs record on even blocks: 1 addr, 1 topic
		if n%2 == 0 {
			var rec []byte
			rec = binary.AppendUvarint(rec, 1)
			rec = append(rec, bytes.Repeat([]byte{byte(n)}, 20)...)
			rec = binary.AppendUvarint(rec, 1)
			rec = append(rec, bytes.Repeat([]byte{byte(n + 100)}, 32)...)
			if err := st.AppendLogs(n, rec); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := st.SetLogsStart(1); err != nil {
		t.Fatal(err)
	}
	if err := st.FlushAndSetExecHead(8); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	// 3 txs/block, boundary 10 => blocks 1-4 (12 txs), 5-8 (12 txs); 9,10
	// beyond the exec head.
	fixedEpochTxs(t, 10)
	cas := testStore(t, dir)
	if err := SealEpochs(dir, cas, [32]byte{}); err != nil {
		t.Fatal(err)
	}
	set, err := OpenEpochSet(cas)
	if err != nil {
		t.Fatal(err)
	}
	defer set.Close()
	if len(set.All()) != 2 {
		t.Fatalf("epochs: %d", len(set.All()))
	}
	e1, e2 := set.All()[0], set.All()[1]
	if e1.Start != 1 || e1.Count != 4 || e1.TxCount != 12 {
		t.Fatalf("epoch1: %+v", e1)
	}
	if e2.Start != 5 || e2.Count != 4 || e2.TxCount != 12 {
		t.Fatalf("epoch2: %+v", e2)
	}

	// header + tx + state roundtrip through the sealed file
	h3, err := e1.HeaderRLP(3)
	if err != nil || !bytes.Equal(h3, []byte{0x99, 3, 0x77, 0x66}) {
		t.Fatalf("header 3: %x %v", h3, err)
	}
	for _, th := range txHashes[6] {
		fp := binary.BigEndian.Uint64(th.h[:8]) >> 16
		found := false
		cands, err := e2.TxCandidates(fp)
		if err != nil {
			t.Fatal(err)
		}
		for _, c := range cands {
			found = found || c == 6
		}
		if !found {
			t.Fatalf("tx of block 6 not in epoch2 txidx")
		}
	}
	if v, blk, found, _ := e1.StateSearch(slotKey[:], 3); !found || blk != 3 || !bytes.Equal(v, []byte{3}) {
		t.Fatalf("state at 3: v=%x blk=%d found=%v", v, blk, found)
	}
	if v, _, found, _ := e2.StateSearch(slotKey[:], 8); !found || !bytes.Equal(v, []byte{8}) {
		t.Fatalf("state at 8 in epoch2: %x %v", v, found)
	}

	// the hash chain: epoch 1 links to the chain root, epoch 2 to epoch 1
	if e1.Prev != ([32]byte{}) {
		t.Fatalf("epoch1 prev %x, want the chain root passed in", e1.Prev)
	}
	if want, _ := hashBytes(e1.Hash); e2.Prev != want {
		t.Fatalf("epoch2 prev %x, want epoch1 %x", e2.Prev, want)
	}
	if l, err := cas.Latest([32]byte{}); err != nil || l.Epoch != e2.Hash {
		t.Fatalf("latest pointer: %+v %v, want epoch %s", l, err, e2.Hash)
	}

	// resume: nothing new to seal
	before, _ := os.ReadDir(dir)
	if err := SealEpochs(dir, cas, [32]byte{}); err != nil {
		t.Fatal(err)
	}
	after, _ := os.ReadDir(dir)
	if len(before) != len(after) {
		t.Fatalf("re-seal created files: %d -> %d", len(before), len(after))
	}
	if _, err := os.Stat(filepath.Join(dir, EpochMarkerName(9, 2))); err == nil {
		t.Fatal("blocks beyond exec head must not seal")
	}
}

// TestTailRcptFraming: the tail record must split back into the exact epoch
// section bytes, including the two asymmetric cases (a tx-bearing block that
// emitted no logs, and a block with neither). The whole no-re-execution seal
// rests on these halves being the epoch encodings verbatim.
func TestTailRcptFraming(t *testing.T) {
	logs := []byte{1, 2, 3, 4, 5}
	rcpt := []byte{0xaa, 0, 0xbb, 1}
	for _, c := range []struct{ l, r []byte }{
		{logs, rcpt},
		{nil, rcpt}, // txs but no logs: the common case
		{logs, nil}, // logs without txs is impossible, but must not corrupt
	} {
		rec := EncodeTailRcpt(c.l, c.r)
		if rec == nil {
			t.Fatalf("no record for %x/%x", c.l, c.r)
		}
		gotL, gotR, err := DecodeTailRcpt(rec)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(gotL, c.l) || !bytes.Equal(gotR, c.r) {
			t.Fatalf("round trip: %x/%x -> %x/%x", c.l, c.r, gotL, gotR)
		}
	}
	if EncodeTailRcpt(nil, nil) != nil {
		t.Fatal("a block with no txs and no logs must produce no record")
	}
	if _, _, err := DecodeTailRcpt([]byte{9}); err == nil {
		t.Fatal("a record claiming 9 log bytes it does not have was accepted")
	}
}

// TestSweepSealScratchAtStartup is the unconditional sweep: an abandoned
// artifact goes whatever its age (a killed build's file is minutes old at a
// restart, and the 24h guard it replaced left 19.9 GB behind), while the file
// the LIVE build in this process owns is the one thing it must not touch.
func TestSweepSealScratchAtStartup(t *testing.T) {
	st, err := dist.Local(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	dead, err := os.CreateTemp(st.SpoolDir(), "epoch-*.tmp")
	if err != nil {
		t.Fatal(err)
	}
	dead.Close()
	scratch := filepath.Join(st.Dir(), sealTmpPrefix+"12345")
	if err := os.MkdirAll(scratch, 0o755); err != nil {
		t.Fatal(err)
	}
	live, err := newEpochOut(st)
	if err != nil {
		t.Fatal(err)
	}
	defer live.discard()

	SweepSealScratch(st)

	if _, err := os.Stat(dead.Name()); !os.IsNotExist(err) {
		t.Fatalf("abandoned %s survived the sweep (err=%v)", dead.Name(), err)
	}
	if _, err := os.Stat(scratch); !os.IsNotExist(err) {
		t.Fatalf("stale scratch dir survived the sweep (err=%v)", err)
	}
	mine := live.path
	if _, err := os.Stat(mine); err != nil {
		t.Fatalf("the live build's own file was swept: %v", err)
	}

	// Once the build is over the file is nobody's, so the next sweep takes it.
	live.discard()
	if _, still := liveEpochBuilds.Load(mine); still {
		t.Fatal("a finished build is still registered as live")
	}
}

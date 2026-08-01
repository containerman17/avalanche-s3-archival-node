package state

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/ava-labs/libevm/common"
)

// fixedBucketBlocks pins the raw-retirement granularity for the duration of a
// test, so a corpus of a few blocks can retire a whole bucket instead of
// needing 100,000 (see bucketBlocks).
func fixedBucketBlocks(t *testing.T, n uint64) {
	prev := bucketBlocks
	t.Cleanup(func() { bucketBlocks = prev })
	bucketBlocks = n
}

// TestSealTailInProcess is the in-process seal sequence end to end, on the
// pieces a serving node actually holds: a LIVE state.Store (still open, still
// the writer) with a History over its cooked buckets. It pins the ordering
// that makes sealing beside a live reader safe:
//
//	before  a historical read is answered by the raw cooked bucket,
//	after   the same read is answered by the sealed epoch, because the raw
//	        files, this process's handles on them and the mapped bucket are
//	        all gone by the time SealTail returns.
func TestSealTailInProcess(t *testing.T) {
	dir := t.TempDir()

	// blocks 1..8, 3 txs each, one storage write per block, all in staging
	// bucket 0. Sealing them all means the whole bucket retires, which is what
	// the deletion step needs to have anything to do.
	txHashes := map[uint64][]common.Hash{}
	for n := uint64(1); n <= 8; n++ {
		hs, _ := writeStagingBlock(t, dir, 0, n, 3)
		txHashes[n] = hs
	}

	st, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	slotKey := synthKey('s', 42)
	for n := uint64(1); n <= 8; n++ {
		if err := st.AppendHeader(n, []byte{0x99, byte(n), 0x77, 0x66}); err != nil {
			t.Fatal(err)
		}
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
	}
	if err := st.SetLogsStart(1); err != nil {
		t.Fatal(err)
	}
	if err := st.FlushAndSetExecHead(8); err != nil {
		t.Fatal(err)
	}

	// The serving node's read path: cooked raw buckets, no epochs yet.
	if err := CookIndex(dir); err != nil {
		t.Fatal(err)
	}
	if err := CookTxIndex(dir); err != nil {
		t.Fatal(err)
	}
	hist, err := OpenHistory(dir, st, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer hist.Close()
	if n := len(hist.Epochs().All()); n != 0 {
		t.Fatalf("%d epochs before sealing", n)
	}
	if v, _, found, err := hist.search(slotKey[:], 3); err != nil || !found || !bytes.Equal(v, []byte{3}) {
		t.Fatalf("raw descent at 3: v=%x found=%v err=%v", v, found, err)
	}

	// 3 txs/block, boundary 10 => epochs of 4 blocks: 1-4 and 5-8. Retiring at
	// 8 blocks per bucket then makes bucket 0 (blocks 0..7) fully sealed.
	fixedEpochTxs(t, 10)
	fixedBucketBlocks(t, 8)

	epochs, sealedEnd, err := hist.SealTail([32]byte{})
	if err != nil {
		t.Fatal(err)
	}
	if epochs != 2 || sealedEnd != 8 {
		t.Fatalf("sealed %d epochs through %d, want 2 through 8", epochs, sealedEnd)
	}

	// Published: the SAME *EpochSet every reader captured before the seal.
	if n := len(hist.Epochs().All()); n != 2 {
		t.Fatalf("%d epochs published", n)
	}
	if end, ok := hist.Epochs().SealedEnd(); !ok || end != 8 {
		t.Fatalf("sealed end %d (ok=%v)", end, ok)
	}

	// Deleted: every raw file of the retired bucket, and this process's own
	// mapped bucket with them.
	for _, name := range []string{
		"arrival_00000.log", "index_00000.log",
		"writelog_00000.log", "writelog_idx_00000.log",
		"headers_00000.log", "headers_idx_00000.log",
		"rcpt_00000.log", "rcpt_idx_00000.log",
		"sorted_00000.idx", "txidx_00000.idx",
	} {
		if _, err := os.Stat(filepath.Join(dir, name)); !os.IsNotExist(err) {
			t.Fatalf("%s survived the seal: %v", name, err)
		}
	}
	hist.mu.RLock()
	nBuckets := len(hist.buckets)
	hist.mu.RUnlock()
	if nBuckets != 0 {
		t.Fatalf("%d sorted buckets still mapped after retirement", nBuckets)
	}

	// The raw is gone, so every answer below now comes from the epochs.
	if v, _, found, err := hist.search(slotKey[:], 3); err != nil || !found || !bytes.Equal(v, []byte{3}) {
		t.Fatalf("sealed descent at 3: v=%x found=%v err=%v", v, found, err)
	}
	if v, _, found, err := hist.search(slotKey[:], 8); err != nil || !found || !bytes.Equal(v, []byte{8}) {
		t.Fatalf("sealed descent at 8: v=%x found=%v err=%v", v, found, err)
	}
	if hdr, ok, err := hist.HeaderRLP(3); err != nil || !ok || !bytes.Equal(hdr, []byte{0x99, 3, 0x77, 0x66}) {
		t.Fatalf("header 3 after seal: %x ok=%v err=%v", hdr, ok, err)
	}
	if _, _, ok, err := hist.StoredTail(3); err != nil || ok {
		t.Fatalf("raw receipts for a sealed block: ok=%v err=%v", ok, err)
	}

	// tx-by-hash resolves through the same combined index the RPC holds.
	idx := CombinedTxIndex{Epochs: hist.Epochs()}
	for _, h := range txHashes[6] {
		found := false
		if err := idx.WalkCandidates(h, func(blk uint64) (bool, error) {
			found = found || blk == 6
			return found, nil
		}); err != nil {
			t.Fatal(err)
		}
		if !found {
			t.Fatalf("tx %s of block 6 not found after sealing", h)
		}
	}

	// Nothing left to cut, and the gate means the second pass does not even
	// open the corpus.
	epochs, _, err = hist.SealTail([32]byte{})
	if err != nil || epochs != 0 {
		t.Fatalf("re-seal cut %d epochs: %v", epochs, err)
	}
}

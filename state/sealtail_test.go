package state

import (
	"bytes"
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/ava-labs/libevm/common"
)

// fixedBucketBlocks pins the raw-bucket width for the duration of a test, so a
// corpus of a few blocks can fill and retire a whole bucket instead of needing
// 100,000. Set it BEFORE writing any block: it is also where a raw record
// physically lands (see bucketBlocks).
func fixedBucketBlocks(t *testing.T, n uint64) {
	prev := bucketBlocks
	t.Cleanup(func() { bucketBlocks = prev })
	bucketBlocks = n
}

// sealCorpus builds the smallest sealable dir and hands back the LIVE state
// store, still open and still the writer, the way a serving node holds it:
// blocks 1..n of 3 txs each in staging, a header, a storage write of the block
// number into sealSlotKey and a captured receipts record each, exec head at n,
// cooked. The caller closes the store.
func sealCorpus(t *testing.T, dir string, n uint64) (*Store, map[uint64][]common.Hash) {
	t.Helper()
	txHashes := map[uint64][]common.Hash{}
	for h := uint64(1); h <= n; h++ {
		hs, _ := writeStagingBlock(t, dir, 0, h, 3)
		txHashes[h] = hs
	}
	st, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	slotKey := sealSlotKey()
	for h := uint64(1); h <= n; h++ {
		if err := st.AppendHeader(h, []byte{0x99, byte(h), 0x77, 0x66}); err != nil {
			t.Fatal(err)
		}
		var frame []byte
		frame = append(frame, recKindStorage)
		frame = append(frame, slotKey[1:53]...)
		frame = binary.AppendUvarint(frame, 1)
		frame = append(frame, byte(h))
		if err := st.AppendWrites(h, frame); err != nil {
			t.Fatal(err)
		}
		if err := st.AppendRcpt(h, synthRcpt(3, h)); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.SetLogsStart(1); err != nil {
		t.Fatal(err)
	}
	if err := st.FlushAndSetExecHead(n); err != nil {
		t.Fatal(err)
	}
	if err := CookIndex(dir); err != nil {
		t.Fatal(err)
	}
	if err := CookTxIndex(dir); err != nil {
		t.Fatal(err)
	}
	return st, txHashes
}

// sealSlotKey is the one storage slot sealCorpus writes, once per block.
func sealSlotKey() [sortedKeySize]byte { return synthKey('s', 42) }

// epochHashes is the sealed hash chain of a set, in order: the artifact names
// ARE content hashes and each carries its predecessor's, so two runs with the
// same list produced byte-identical epochs from the same corpus.
func epochHashes(set *EpochSet) []string {
	var out []string
	for _, e := range set.All() {
		out = append(out, e.Hash)
	}
	return out
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
	// 3 txs/block, boundary 10 => epochs of 4 blocks: 1-4 and 5-8. A bucket of
	// 8 blocks then makes bucket 0 (blocks 0..7) fully sealed at the end.
	fixedEpochTxs(t, 10)
	fixedBucketBlocks(t, 8)

	dir := t.TempDir()
	st, txHashes := sealCorpus(t, dir, 8)
	defer st.Close()
	slotKey := sealSlotKey()

	// The serving node's read path: cooked raw buckets, no epochs yet.
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

	epochs, sealedEnd, err := hist.SealTail(context.Background(), [32]byte{}, nil)
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
	epochs, _, err = hist.SealTail(context.Background(), [32]byte{}, nil)
	if err != nil || epochs != 0 {
		t.Fatalf("re-seal cut %d epochs: %v", epochs, err)
	}
}

// TestSealTailCancelBetweenEpochs pins the shutdown contract (Fuji, 2026-08-01:
// a SIGINT mid-crunch left the node sealing 14 epochs for hours). A canceled
// pass stops at an epoch boundary with what it cut already durable and
// published, and the next pass finishes the backlog: the interrupted run's
// hash chain is identical to an uninterrupted one over the same corpus, so
// resuming cuts where an uninterrupted seal would, byte for byte.
func TestSealTailCancelBetweenEpochs(t *testing.T) {
	fixedEpochTxs(t, 10)
	fixedBucketBlocks(t, 8) // no whole bucket retires before the last epoch

	// The reference: the same corpus, one uninterrupted pass.
	refDir := t.TempDir()
	refSt, _ := sealCorpus(t, refDir, 8)
	defer refSt.Close()
	refHist, err := OpenHistory(refDir, refSt, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer refHist.Close()
	if cut, end, err := refHist.SealTail(context.Background(), [32]byte{}, nil); err != nil || cut != 2 || end != 8 {
		t.Fatalf("reference seal cut %d through %d: %v", cut, end, err)
	}
	want := epochHashes(refHist.Epochs())

	dir := t.TempDir()
	st, _ := sealCorpus(t, dir, 8)
	defer st.Close()
	hist, err := OpenHistory(dir, st, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer hist.Close()
	slotKey := sealSlotKey()

	// Cancel from the per-epoch callback: the pass is then canceled exactly
	// between two epochs, which is the granularity being pinned.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cut, end, err := hist.SealTail(ctx, [32]byte{}, func(uint64) { cancel() })
	if err != nil {
		t.Fatalf("canceled seal: %v", err)
	}
	if cut != 1 || end != 4 {
		t.Fatalf("canceled seal cut %d epochs through %d, want 1 through 4", cut, end)
	}
	if n := len(hist.Epochs().All()); n != 1 {
		t.Fatalf("%d epochs published by the canceled pass", n)
	}
	// What it did cut is durable and published; what it did not stays raw and
	// still answers.
	if v, _, found, err := hist.search(slotKey[:], 4); err != nil || !found || !bytes.Equal(v, []byte{4}) {
		t.Fatalf("sealed descent at 4: v=%x found=%v err=%v", v, found, err)
	}
	if v, _, found, err := hist.search(slotKey[:], 8); err != nil || !found || !bytes.Equal(v, []byte{8}) {
		t.Fatalf("raw descent at 8 after the canceled pass: v=%x found=%v err=%v", v, found, err)
	}

	// A live ctx picks the backlog up where the cancellation left it.
	cut, end, err = hist.SealTail(context.Background(), [32]byte{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if cut != 1 || end != 8 {
		t.Fatalf("resumed seal cut %d epochs through %d, want 1 through 8", cut, end)
	}
	if got := epochHashes(hist.Epochs()); !slices.Equal(got, want) {
		t.Fatalf("interrupted seal produced %v, uninterrupted %v", got, want)
	}
}

// TestSealTailRetiresRawPerEpoch pins the disk contract (Fuji, 2026-08-01: a
// 14-epoch crunch held ~100G of sealed output on top of all 411G of raw until
// the last epoch was cut). Raw whose whole bucket is behind epoch 1 is unlinked
// BEFORE epoch 2 exists, and the pass keeps cutting from what is left.
//
// At this scale a 4-block bucket shares its FILES with blocks 5..8 (where a
// record lives is still BucketBlocks, see bucketBlocks), so epoch 2 gathers
// them through the handles the pass already holds on the unlinked files. That
// is a property of the shrunk bucket, not of production, where a fully sealed
// bucket's files hold nothing above the sealed end; it is also why this test
// does not resume across the retirement (the cancel test does, at a bucket
// width no epoch retires).
func TestSealTailRetiresRawPerEpoch(t *testing.T) {
	fixedEpochTxs(t, 10)    // epochs of 4 blocks: 1-4 and 5-8
	fixedBucketBlocks(t, 4) // bucket 0 = blocks 0..3, wholly inside epoch 1

	dir := t.TempDir()
	st, _ := sealCorpus(t, dir, 8)
	defer st.Close()
	hist, err := OpenHistory(dir, st, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer hist.Close()
	slotKey := sealSlotKey()

	checked := false
	cut, end, err := hist.SealTail(context.Background(), [32]byte{}, func(sealedEnd uint64) {
		if sealedEnd != 4 {
			return
		}
		checked = true
		if n := len(hist.Epochs().All()); n != 1 {
			t.Errorf("%d epochs at the epoch-1 callback, want 1", n)
		}
		for _, name := range []string{
			"writelog_00000.log", "writelog_idx_00000.log",
			"headers_00000.log", "headers_idx_00000.log",
			"rcpt_00000.log", "rcpt_idx_00000.log",
			"arrival_00000.log", "index_00000.log",
		} {
			if _, err := os.Stat(filepath.Join(dir, name)); !os.IsNotExist(err) {
				t.Errorf("%s survived epoch 1 (it should go before epoch 2 is cut): %v", name, err)
			}
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	if !checked {
		t.Fatal("the per-epoch callback never fired for epoch 1")
	}
	if cut != 2 || end != 8 {
		t.Fatalf("sealed %d epochs through %d, want 2 through 8", cut, end)
	}
	// Retiring epoch 1's raw mid-pass cost epoch 2 nothing: both answer.
	for _, n := range []uint64{3, 8} {
		if v, _, found, err := hist.search(slotKey[:], n); err != nil || !found || !bytes.Equal(v, []byte{byte(n)}) {
			t.Fatalf("sealed descent at %d: v=%x found=%v err=%v", n, v, found, err)
		}
	}
}

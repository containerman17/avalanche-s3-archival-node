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

// TestSealTailCatchUpCeiling is the wasted-gather failure of 2026-08-05, in
// miniature: on a node CATCHING UP, `seal: header N: ok=false err=<nil>` threw
// away a whole epoch gather (minutes of CPU, gigabytes of scanned rows) on
// every cook tick for hours, on every VM kind including the C-chain.
//
// The shape is this one: the fetcher is far ahead (containers for 11..12 are in
// staging), the executor's raw appends reach 10, and exechead names 12. The
// seal used to take exechead as its ceiling, walk into 11 and die on a header
// that was never its to read. It must instead bound itself to what it can
// serve: cut the epochs it can, decline the rest through the normal
// tail-stays-raw path, and stay byte-identical to a seal of the same blocks
// with no skew.
func TestSealTailCatchUpCeiling(t *testing.T) {
	fixedEpochTxs(t, 10)     // 3 txs/block: epochs of 4 blocks, 1-4 and 5-8
	fixedBucketBlocks(t, 16) // no whole bucket retires, so the raw tail survives

	// The reference: the same eight sealable blocks, no skew at all.
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
	st, _ := sealCorpus(t, dir, 10)
	defer st.Close()
	for h := uint64(11); h <= 12; h++ {
		writeStagingBlock(t, dir, 0, h, 3)
	}
	// The executor ran ahead of its own visible raw tail: exechead 12, headers
	// (and every other family) 10.
	if err := st.FlushAndSetExecHead(12); err != nil {
		t.Fatal(err)
	}
	hist, err := OpenHistory(dir, st, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer hist.Close()

	cut, end, err := hist.SealTail(context.Background(), [32]byte{}, nil)
	if err != nil {
		t.Fatalf("seal on a catching-up corpus: %v", err)
	}
	if cut != 2 || end != 8 {
		t.Fatalf("catch-up seal cut %d epochs through %d, want 2 through 8", cut, end)
	}
	if got := epochHashes(hist.Epochs()); !slices.Equal(got, want) {
		t.Fatalf("catch-up seal produced %v, unskewed %v", got, want)
	}
	// It declined the rest rather than failing, so the pass dated the retry
	// gate (a failed pass leaves it at zero, 2026-08-02) and the next tick is
	// cheap instead of another full gather.
	if hist.sealRetryAt == 0 {
		t.Fatal("the declining pass did not date the retry gate")
	}
	if cut, _, err := hist.SealTail(context.Background(), [32]byte{}, nil); err != nil || cut != 0 {
		t.Fatalf("second pass cut %d epochs: %v", cut, err)
	}
}

// TestSealTailNeedsNoBucket is the ruling of 2026-08-02: SEALING DOES NOT KNOW
// S3 EXISTS. The store here is fully credentialed and its endpoint is a closed
// port, so any byte the seal sends is a connection refused; if the seal path
// still reached for the bucket anywhere (the `latest` pointer, the chunk
// lists, opening the sealed head to chain onto it) this could not pass.
//
// That is also the offline/expired-credentials equivalence: a node with no
// EPOCHDB_S3_* at all runs the same code with cas nil, and this one cannot
// reach its bucket. Both seal, repeatedly, and only uploads stall.
func TestSealTailNeedsNoBucket(t *testing.T) {
	fixedEpochTxs(t, 10) // epochs of 4 blocks: 1-4 and 5-8
	fixedBucketBlocks(t, 8)
	s3 := newFakeS3(t)
	t.Setenv("EPOCHDB_S3_ENDPOINT", s3.URL)
	t.Setenv("EPOCHDB_S3_BUCKET", "epochs")
	t.Setenv("EPOCHDB_S3_ACCESS_KEY", "ak")
	t.Setenv("EPOCHDB_S3_SECRET_KEY", "sk")

	dir := t.TempDir()
	st, _ := sealCorpus(t, dir, 8)
	defer st.Close()
	if !st.Cas().Remote() {
		t.Fatal("the test store has no bucket configured, it would prove nothing")
	}
	hist, err := OpenHistory(dir, st, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer hist.Close()

	// Epoch 1, uploaded and RELEASED: its bytes now exist only in the bucket,
	// which is the state a producer box is in for everything but its newest
	// epoch.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if cut, _, err := hist.SealTail(ctx, [32]byte{}, func(uint64) { cancel() }); err != nil || cut != 1 {
		t.Fatalf("first seal cut %d epochs: %v", cut, err)
	}
	if err := st.Cas().Sync(); err != nil {
		t.Fatal(err)
	}
	hash, err := ReadMarker(dir, EpochMarkerName(1, 4))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(st.Cas().SpoolPath(hash)); !os.IsNotExist(err) {
		t.Fatalf("epoch 1 is still spooled, so this proves nothing: %v", err)
	}

	// The token dies. Sealing must not notice.
	s3.Server.Close()
	cut, end, err := hist.SealTail(context.Background(), [32]byte{}, nil)
	if err != nil {
		t.Fatalf("seal with an unreachable bucket: %v", err)
	}
	if cut != 1 || end != 8 {
		t.Fatalf("seal with an unreachable bucket cut %d through %d, want 1 through 8", cut, end)
	}
	if l, err := st.Cas().Latest([32]byte{}); err != nil || l.Epoch == "" {
		t.Fatalf("local latest after sealing without a bucket: %+v %v", l, err)
	}
	// Only the upload stalls, and it says so.
	if err := st.Cas().Sync(); err == nil {
		t.Fatal("Sync to an unreachable bucket reported success")
	}
}

// TestSealTailRetriesAfterFailure is the overnight stall of 2026-08-01, in
// miniature: three seals failed on an expired SSO token and the node then
// never tried again, because a failed pass pushed the retry gate a whole
// bucket of blocks ahead and a node at the tip needs a day and a half to make
// that many. A FAILED PASS MUST LEAVE THE GATE OPEN.
//
// The failure is injected where the seal actually writes, by taking write
// permission off the artifact spool for one pass: BuildEpoch cannot land its
// file, exactly as a store that errors once.
func TestSealTailRetriesAfterFailure(t *testing.T) {
	fixedEpochTxs(t, 10)    // epochs of 4 blocks: 1-4 and 5-8
	fixedBucketBlocks(t, 8) // exec head 8, so a bucket-wide gate would be 100008

	dir := t.TempDir()
	st, _ := sealCorpus(t, dir, 8)
	defer st.Close()
	hist, err := OpenHistory(dir, st, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer hist.Close()

	spool := filepath.Join(dir, "cas")
	if err := os.Chmod(spool, 0o500); err != nil {
		t.Fatal(err)
	}
	cut, _, err := hist.SealTail(context.Background(), [32]byte{}, nil)
	if err == nil {
		t.Fatal("seal onto a read-only spool reported success")
	}
	if cut != 0 {
		t.Fatalf("failed pass cut %d epochs", cut)
	}
	if err := os.Chmod(spool, 0o755); err != nil {
		t.Fatal(err)
	}

	// The very next tick, with no new blocks and no new exec head.
	cut, end, err := hist.SealTail(context.Background(), [32]byte{}, nil)
	if err != nil {
		t.Fatalf("seal after a failed pass: %v", err)
	}
	if cut != 2 || end != 8 {
		t.Fatalf("retry cut %d epochs through %d, want 2 through 8", cut, end)
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

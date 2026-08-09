package state

import (
	"bytes"
	"context"
	"encoding/binary"
	"slices"
	"testing"
)

// logRecOf builds one capture-format logs record (exec's encodeLogsFrame
// layout) from the addresses and topics a block emitted.
func logRecOf(addrs [][20]byte, topics [][32]byte) []byte {
	rec := binary.AppendUvarint(nil, uint64(len(addrs)))
	for _, a := range addrs {
		rec = append(rec, a[:]...)
	}
	rec = binary.AppendUvarint(rec, uint64(len(topics)))
	for _, t := range topics {
		rec = append(rec, t[:]...)
	}
	return rec
}

// TestTailLogIndexAcrossSeal is the one thing the hot-tail log index can get
// wrong in a way no other test sees: the seal boundary. The index is built over
// blocks 1..8, then epoch 1 (blocks 1..4) is sealed under it. After that the
// index must name blocks 5..8 and NOTHING below, because the epoch's own
// posting lists answer there and the raw records the index was built from are
// retired: naming 1..4 again would double-count every candidate a query
// intersects, and dropping 5..8 would lose the events that just happened.
func TestTailLogIndexAcrossSeal(t *testing.T) {
	fixedEpochTxs(t, 10)     // 3 txs/block => epochs of 4 blocks: 1-4, 5-8
	fixedBucketBlocks(t, 16) // no bucket retires, so the raw survives the pass

	hot := [20]byte{0xaa}
	cold := [20]byte{0xbb}
	topic := [32]byte{0xcc}

	dir := t.TempDir()
	st, _ := sealCorpus(t, dir, 8)
	defer st.Close()
	// hot logs in every block, cold only in 3 and 7, the topic only in 6..8.
	for n := uint64(1); n <= 8; n++ {
		addrs := [][20]byte{hot}
		if n == 3 || n == 7 {
			addrs = append(addrs, cold)
		}
		var topics [][32]byte
		if n >= 6 {
			topics = append(topics, topic)
		}
		if err := st.AppendLogs(n, logRecOf(addrs, topics)); err != nil {
			t.Fatal(err)
		}
	}

	hist, err := OpenHistory(dir, st, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer hist.Close()
	hist.SetHead(8)

	// Before any seal the whole corpus is tail, so the index is the only thing
	// that can answer at all.
	blocks, covered, matched, err := hist.TailLogCandidates(1, 8, [][20]byte{hot}, nil)
	if err != nil || !matched {
		t.Fatalf("pre-seal candidates: matched=%v err=%v", matched, err)
	}
	if covered != 8 || !slices.Equal(blocks, []uint64{1, 2, 3, 4, 5, 6, 7, 8}) {
		t.Fatalf("pre-seal hot blocks %v covered %d", blocks, covered)
	}
	if b, _, _, _ := hist.TailLogCandidates(1, 8, [][20]byte{cold}, nil); !slices.Equal(b, []uint64{3, 7}) {
		t.Fatalf("pre-seal cold blocks %v, want [3 7]", b)
	}
	if b, _, _, _ := hist.TailLogCandidates(1, 8, nil, [][][32]byte{{topic}}); !slices.Equal(b, []uint64{6, 7, 8}) {
		t.Fatalf("pre-seal topic blocks %v, want [6 7 8]", b)
	}
	// Nothing to narrow on says so, rather than claiming every block.
	if b, _, matched, _ := hist.TailLogCandidates(1, 8, nil, nil); matched || b != nil {
		t.Fatalf("an unnarrowed query claimed %v (matched=%v)", b, matched)
	}

	// Seal epoch 1 only (cancel from the per-epoch callback), so blocks 1..4
	// move under the sealed end while 5..8 stay tail.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if cut, end, err := hist.SealTail(ctx, [32]byte{}, func(uint64) { cancel() }); err != nil || cut != 1 || end != 4 {
		t.Fatalf("seal cut %d through %d: %v", cut, end, err)
	}

	// The retirement floor is respected and the sealed half is gone from the
	// index, exactly once: repeating the call must not resurrect or duplicate.
	for pass := range 2 {
		blocks, covered, matched, err = hist.TailLogCandidates(1, 8, [][20]byte{hot}, nil)
		if err != nil || !matched {
			t.Fatalf("pass %d: matched=%v err=%v", pass, matched, err)
		}
		if covered != 8 || !slices.Equal(blocks, []uint64{5, 6, 7, 8}) {
			t.Fatalf("pass %d: post-seal hot blocks %v covered %d, want [5 6 7 8] covered 8", pass, blocks, covered)
		}
	}
	if b, _, _, _ := hist.TailLogCandidates(1, 8, [][20]byte{cold}, nil); !slices.Equal(b, []uint64{7}) {
		t.Fatalf("post-seal cold blocks %v, want [7]", b)
	}
	if hist.taillog.floor != 4 {
		t.Fatalf("index floor %d after sealing through 4", hist.taillog.floor)
	}

	// A process that opens the same dir AFTER the seal builds the index from
	// the floor up: same answer, and it never reads a retired height.
	fresh, err := OpenHistory(dir, st, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Close()
	fresh.SetHead(8)
	b, _, _, err := fresh.TailLogCandidates(1, 8, [][20]byte{hot}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(b, []uint64{5, 6, 7, 8}) {
		t.Fatalf("fresh index hot blocks %v, want [5 6 7 8]", b)
	}
	if fresh.taillog.floor != 4 {
		t.Fatalf("fresh index floor %d, want 4", fresh.taillog.floor)
	}

	// And the index never claims coverage it does not have: a block appended
	// above the logs family's max is answered by the caller's per-block walk,
	// not silently dropped.
	if err := st.AppendLogs(9, logRecOf([][20]byte{hot}, nil)); err != nil {
		t.Fatal(err)
	}
	hist.SetHead(9)
	b, covered, _, err = hist.TailLogCandidates(1, 9, [][20]byte{hot}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if covered != 9 || !slices.Equal(b, []uint64{5, 6, 7, 8, 9}) {
		t.Fatalf("after one more block: %v covered %d", b, covered)
	}
}

// TestTailLogIndexRefusesCorruptRecord: derived state never papers over on-disk
// damage. A logs record that will not decode is a loud refusal, not a tail
// answered without the block it could not read.
func TestTailLogIndexRefusesCorruptRecord(t *testing.T) {
	fixedEpochTxs(t, 1_000_000) // nothing seals
	fixedBucketBlocks(t, 16)

	dir := t.TempDir()
	st, _ := sealCorpus(t, dir, 4)
	defer st.Close()
	if err := st.AppendLogs(1, logRecOf([][20]byte{{0xaa}}, nil)); err != nil {
		t.Fatal(err)
	}
	// A record claiming two addresses and carrying none.
	if err := st.AppendLogs(2, bytes.Repeat([]byte{0x02}, 1)); err != nil {
		t.Fatal(err)
	}
	hist, err := OpenHistory(dir, st, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer hist.Close()
	hist.SetHead(4)
	if _, _, _, err := hist.TailLogCandidates(1, 4, [][20]byte{{0xaa}}, nil); err == nil {
		t.Fatal("a corrupt logs record was indexed instead of refused")
	}
}

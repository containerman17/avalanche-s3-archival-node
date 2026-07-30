package state

import (
	"encoding/binary"
	"testing"

	"github.com/ava-labs/libevm/common"
	"github.com/containerman17/epochdb/dist"
)

// synthSet builds n epochs of 100 blocks each and opens them as a set.
func synthSet(t *testing.T, n int) (*dist.Store, *EpochSet, []*EpochInput) {
	t.Helper()
	st := testStore(t, t.TempDir())
	var ins []*EpochInput
	for i := 0; i < n; i++ {
		in, _ := synthEpoch(t, st, 1000+uint64(i)*100)
		ins = append(ins, in)
	}
	s, err := OpenEpochSet(st)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	if len(s.Epochs) != n {
		t.Fatalf("opened %d epochs, want %d", len(s.Epochs), n)
	}
	return st, s, ins
}

func txLoadCounts(s *EpochSet) []uint64 {
	out := make([]uint64, len(s.Epochs))
	for i, e := range s.Epochs {
		out[i] = e.txLoads.Load()
	}
	return out
}

// TestUnknownTxHashLoadsNoIndex is the point of the whole tx bloom: a hash
// this node never saw (the pending tx every wallet polls for) must be
// rejected by the per-epoch blooms alone, with no Elias-Fano index decoded
// anywhere and no candidate produced.
func TestUnknownTxHashLoadsNoIndex(t *testing.T) {
	_, s, _ := synthSet(t, 3)
	c := CombinedTxIndex{Epochs: s}

	// Fixed hash, fixed epoch content: deterministic, no flake window.
	unknown := common.HexToHash("0xfeedfacecafebeef0123456789abcdef0123456789abcdef0123456789abcdef")
	visits := 0
	if err := c.WalkCandidates(unknown, func(uint64) (bool, error) {
		visits++
		return false, nil
	}); err != nil {
		t.Fatal(err)
	}
	if visits != 0 {
		t.Fatalf("unknown hash produced %d candidates", visits)
	}
	for i, n := range txLoadCounts(s) {
		if n != 0 {
			t.Fatalf("epoch %d loaded its tx index for an unknown hash (%d loads)", i, n)
		}
	}
	for _, e := range s.Epochs {
		if e.txIdx != nil {
			t.Fatalf("epoch %d holds a decoded tx index after a bloom reject", e.Start)
		}
	}

	// And the filter is actually selective, not merely lucky on one hash.
	const probes = 2000
	fps := 0
	for i := 0; i < probes; i++ {
		var h common.Hash
		binary.BigEndian.PutUint64(h[:8], 0x9e3779b97f4a7c15*uint64(i+1))
		for _, e := range s.Epochs {
			if e.MayContainTx(txFingerprint(h)) {
				fps++
			}
		}
	}
	if fps > probes*len(s.Epochs)/100 { // 16 bits/tx targets 0.045%
		t.Fatalf("tx bloom false-positive rate too high: %d of %d probes", fps, probes*len(s.Epochs))
	}
}

// TestKnownTxWalkStopsAtFirstHit: a real tx is still found, only the epochs
// the walk actually reaches decode their index, and the duplicate-fingerprint
// case still yields both blocks.
func TestKnownTxWalkStopsAtFirstHit(t *testing.T) {
	_, s, ins := synthSet(t, 3)
	c := CombinedTxIndex{Epochs: s}
	newest := ins[len(ins)-1]

	// A tx in the NEWEST epoch: the walk starts there, so no older epoch is
	// probed and no older index is decoded.
	var (
		wantBlock uint64
		wantHash  common.Hash
	)
	for blk, hashes := range newest.TxHashes {
		if blk > wantBlock {
			wantBlock, wantHash = blk, common.Hash(hashes[0])
		}
	}
	var got []uint64
	if err := c.WalkCandidates(wantHash, func(b uint64) (bool, error) {
		got = append(got, b)
		return b == wantBlock, nil // stand-in for the container verification
	}); err != nil {
		t.Fatal(err)
	}
	if len(got) == 0 || got[len(got)-1] != wantBlock {
		t.Fatalf("tx %x: walked %v, want to end at %d", wantHash[:6], got, wantBlock)
	}
	loads := txLoadCounts(s)
	if loads[2] != 1 || loads[0] != 0 || loads[1] != 0 {
		t.Fatalf("loads %v: only the newest epoch should have decoded its index", loads)
	}

	// Every tx of every epoch resolves to its own block.
	for ei, in := range ins {
		for blk, hashes := range in.TxHashes {
			for _, raw := range hashes {
				h := common.Hash(raw)
				hit := false
				if err := c.WalkCandidates(h, func(b uint64) (bool, error) {
					hit = hit || b == blk
					return b == blk, nil
				}); err != nil {
					t.Fatal(err)
				}
				if !hit {
					t.Fatalf("epoch %d tx %x: block %d never walked", ei, h[:6], blk)
				}
			}
		}
	}

	// The duplicate fingerprint (same fp48 at start+10 and start+90 of every
	// synthetic epoch): a walk that never says "found" must see both blocks
	// of every epoch, newest first.
	var dup common.Hash
	copy(dup[:], []byte("duplicated-fingerprint-hash!!"))
	var all []uint64
	if err := c.WalkCandidates(dup, func(b uint64) (bool, error) {
		all = append(all, b)
		return false, nil
	}); err != nil {
		t.Fatal(err)
	}
	want := []uint64{1290, 1210, 1190, 1110, 1090, 1010}
	if len(all) != len(want) {
		t.Fatalf("duplicate fp: walked %v, want %v", all, want)
	}
	for i := range want {
		if all[i] != want[i] {
			t.Fatalf("duplicate fp: walked %v, want %v", all, want)
		}
	}
}

// TestTxIndexLRUEvicts: past txHotEpochs decoded indexes the oldest one is
// dropped, which is what bounds the heap at a few epochs instead of all of
// history.
func TestTxIndexLRUEvicts(t *testing.T) {
	_, s, ins := synthSet(t, txHotEpochs+1)
	c := CombinedTxIndex{Epochs: s}
	for _, in := range ins {
		for _, hashes := range in.TxHashes {
			h := common.Hash(hashes[0])
			if err := c.WalkCandidates(h, func(uint64) (bool, error) { return false, nil }); err != nil {
				t.Fatal(err)
			}
			break
		}
	}
	hot := 0
	for _, e := range s.Epochs {
		if e.txIdx != nil {
			hot++
		}
	}
	if hot != txHotEpochs {
		t.Fatalf("%d epochs hold a decoded tx index, want %d", hot, txHotEpochs)
	}
	if len(s.txHot) != txHotEpochs {
		t.Fatalf("LRU holds %d entries, want %d", len(s.txHot), txHotEpochs)
	}
}

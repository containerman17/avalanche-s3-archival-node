package state

import (
	"bytes"
	"fmt"
	"testing"
)

// TestSpillDiffs checks the (key, block) -> (block, key) re-sort: the
// cursor must yield exactly the SST's rows, blocks strictly ascending,
// rows key-sorted within each block.
func TestSpillDiffs(t *testing.T) {
	st := testStore(t, t.TempDir())
	_, hash := synthEpoch(t, st, 1000)
	e, err := OpenEpoch(st, hash)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	want := map[string]bool{}
	if err := e.WalkStateRows(func(r StateRow) {
		want[fmt.Sprintf("%d|%x|%x", r.Block, r.Key, r.Value)] = true
	}); err != nil {
		t.Fatal(err)
	}

	cur, err := e.SpillDiffs(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer cur.Close()
	got := 0
	lastBlk := uint64(0)
	for {
		blk, rows, ok, err := cur.Next()
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		if blk <= lastBlk && lastBlk != 0 {
			t.Fatalf("blocks not ascending: %d after %d", blk, lastBlk)
		}
		lastBlk = blk
		for i, r := range rows {
			if r.Block != blk {
				t.Fatalf("row block %d in group %d", r.Block, blk)
			}
			if i > 0 && bytes.Compare(rows[i-1].Key[:], r.Key[:]) >= 0 {
				t.Fatalf("block %d: rows not key-sorted", blk)
			}
			k := fmt.Sprintf("%d|%x|%x", r.Block, r.Key, r.Value)
			if !want[k] {
				t.Fatalf("unexpected row %s", k)
			}
			got++
		}
	}
	if got != len(want) {
		t.Fatalf("cursor yielded %d rows, SST has %d", got, len(want))
	}
}

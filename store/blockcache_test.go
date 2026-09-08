package store

import "testing"

// A point read of a sealed run must come out of the block cache the second
// time, not out of another zstd decompression of the same data block.
func TestRunPointReadsHitTheBlockCache(t *testing.T) {
	if blockCache == nil {
		t.Skip("EPOCHDB_BLOCK_CACHE=0")
	}
	db, _ := testDB(t)
	if err := db.WriteBlock(block(1, 2)); err != nil {
		t.Fatal(err)
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	runs, done := db.snapshot()
	defer done()
	if len(runs) == 0 {
		t.Fatal("Flush sealed no run")
	}
	before := blockCache.Metrics()
	for i := 0; i < 2; i++ {
		if _, ok, err := runs[len(runs)-1].Get(SecChain, numKey(famPrefix[famRcpt], 0)); err != nil || !ok {
			t.Fatalf("read %d: ok=%v err=%v", i, ok, err)
		}
	}
	after := blockCache.Metrics()
	if after.Hits <= before.Hits {
		t.Fatalf("second read missed the block cache: hits %d -> %d, misses %d -> %d", before.Hits, after.Hits, before.Misses, after.Misses)
	}
}

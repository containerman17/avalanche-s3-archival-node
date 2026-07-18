package state

import (
	"bytes"
	"testing"
)

func TestLogsBackfillWriterAndResume(t *testing.T) {
	dir := t.TempDir()
	bf, err := OpenLogsBackfill(dir)
	if err != nil {
		t.Fatal(err)
	}
	rec5 := []byte{1, 0xaa, 0xbb, 0} // opaque capture-format bytes
	rec150k := []byte{2, 1, 2, 3, 4}
	if err := bf.Append(5, rec5); err != nil {
		t.Fatal(err)
	}
	if err := bf.Append(150_000, rec150k); err != nil {
		t.Fatal(err)
	}
	// Idempotent re-append must not duplicate.
	if err := bf.Append(5, []byte{9, 9}); err != nil {
		t.Fatal(err)
	}
	if err := bf.MarkBucketDone(0); err != nil {
		t.Fatal(err)
	}
	if err := bf.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen: records survive, done marker drives resume skip.
	bf2, err := OpenLogsBackfill(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer bf2.Close()
	got, ok, err := bf2.Get(5)
	if err != nil || !ok || !bytes.Equal(got, rec5) {
		t.Fatalf("block 5: got %x ok=%v err=%v", got, ok, err)
	}
	if got, ok, _ := bf2.Get(150_000); !ok || !bytes.Equal(got, rec150k) {
		t.Fatalf("block 150000: got %x ok=%v", got, ok)
	}
	if _, ok, _ := bf2.Get(6); ok {
		t.Fatal("phantom record for block 6")
	}
	if !bf2.BucketDone(0) {
		t.Fatal("bucket 0 should be done after MarkBucketDone")
	}
	if bf2.BucketDone(1) {
		t.Fatal("bucket 1 must not be done")
	}
}

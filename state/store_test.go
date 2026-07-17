package state

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/ava-labs/libevm/common"
)

func TestBucketLogTornTailRecovery(t *testing.T) {
	dir := t.TempDir()
	l, err := openBucketLog(dir, "writelog")
	if err != nil {
		t.Fatal(err)
	}
	payloads := [][]byte{[]byte("frame-one"), []byte("frame-two-longer"), []byte("frame-three")}
	for i, p := range payloads {
		if err := l.Append(uint64(i+1), p); err != nil {
			t.Fatal(err)
		}
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	// Tear the tail: chop the data file mid-way through block 3's payload.
	// The index record for block 3 now points past EOF.
	dataPath := filepath.Join(dir, "writelog_00000.log")
	st, _ := os.Stat(dataPath)
	if err := os.Truncate(dataPath, st.Size()-5); err != nil {
		t.Fatal(err)
	}

	l, err = openBucketLog(dir, "writelog")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	if l.Has(3) {
		t.Fatal("torn block 3 must be dropped")
	}
	for i := 1; i <= 2; i++ {
		got, ok, err := l.Get(uint64(i))
		if err != nil || !ok || !bytes.Equal(got, payloads[i-1]) {
			t.Fatalf("block %d: got %q ok=%v err=%v", i, got, ok, err)
		}
	}
	if max, ok := l.Max(); !ok || max != 2 {
		t.Fatalf("max: got %d ok=%v, want 2", max, ok)
	}
	// Idempotent re-append of the torn block, then of an existing one.
	if err := l.Append(3, payloads[2]); err != nil {
		t.Fatal(err)
	}
	if err := l.Append(2, []byte("different")); err != nil {
		t.Fatal(err)
	}
	got, ok, _ := l.Get(2)
	if !ok || !bytes.Equal(got, payloads[1]) {
		t.Fatal("re-append must not overwrite an indexed block")
	}
	got, ok, _ = l.Get(3)
	if !ok || !bytes.Equal(got, payloads[2]) {
		t.Fatal("block 3 must be readable after re-append")
	}
}

func TestCodeStoreRebuildAndTornTail(t *testing.T) {
	dir := t.TempDir()
	c, err := openCodeStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	h1 := common.HexToHash("0x01")
	h2 := common.HexToHash("0x02")
	blob1 := bytes.Repeat([]byte{0xaa}, 100)
	blob2 := bytes.Repeat([]byte{0xbb}, 3000)
	for _, put := range []struct {
		h common.Hash
		b []byte
	}{{h1, blob1}, {h2, blob2}, {h1, []byte("dup ignored")}} {
		if err := c.Put(put.h, put.b); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}

	// Rebuild by scan.
	c, err = openCodeStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if c.Count() != 2 {
		t.Fatalf("count: got %d want 2", c.Count())
	}
	got, ok, err := c.Get(h1)
	if err != nil || !ok || !bytes.Equal(got, blob1) {
		t.Fatalf("h1 after rebuild: ok=%v err=%v", ok, err)
	}
	c.Close()

	// Torn tail: chop mid-blob of the last record.
	path := filepath.Join(dir, "code.log")
	st, _ := os.Stat(path)
	if err := os.Truncate(path, st.Size()-10); err != nil {
		t.Fatal(err)
	}
	c, err = openCodeStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if c.Has(h2) {
		t.Fatal("torn blob must be dropped")
	}
	if !c.Has(h1) {
		t.Fatal("intact blob must survive")
	}
	// Re-put after the tear (replay path).
	if err := c.Put(h2, blob2); err != nil {
		t.Fatal(err)
	}
	got, ok, err = c.Get(h2)
	if err != nil || !ok || !bytes.Equal(got, blob2) {
		t.Fatalf("h2 after re-put: ok=%v err=%v", ok, err)
	}
}

func TestMiscStoreReplayAndEthDBRouting(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	kv := s.EthDB()

	codeKey := append([]byte{'c'}, common.HexToHash("0xbeef").Bytes()...)
	if err := kv.Put(codeKey, []byte("codeblob")); err != nil {
		t.Fatal(err)
	}
	if err := kv.Put([]byte("chain-config"), []byte("v1")); err != nil {
		t.Fatal(err)
	}
	if err := kv.Put([]byte("gone"), []byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := kv.Delete([]byte("gone")); err != nil {
		t.Fatal(err)
	}
	if s.CodeCount() != 1 {
		t.Fatalf("code key must route to code.log, count=%d", s.CodeCount())
	}
	if err := s.FlushAndSetExecHead(42); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	kv = s.EthDB()
	if v, err := kv.Get(codeKey); err != nil || !bytes.Equal(v, []byte("codeblob")) {
		t.Fatalf("code roundtrip: %v %q", err, v)
	}
	if v, err := kv.Get([]byte("chain-config")); err != nil || !bytes.Equal(v, []byte("v1")) {
		t.Fatalf("misc roundtrip: %v %q", err, v)
	}
	if _, err := kv.Get([]byte("gone")); err == nil {
		t.Fatal("deleted key must stay deleted after replay")
	}
	if h, ok := s.ExecHead(); !ok || h != 42 {
		t.Fatalf("exechead: got %d ok=%v", h, ok)
	}
}

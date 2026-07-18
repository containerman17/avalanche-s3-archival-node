package fetch

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
)

func TestBlockHashes(t *testing.T) {
	dir := t.TempDir()
	var buf []byte
	for i := uint64(1); i <= 3; i++ {
		rec := make([]byte, indexRecSize)
		binary.BigEndian.PutUint64(rec[0:8], i)
		rec[40] = byte(0xA0 + i) // distinct eth block hash first byte
		buf = append(buf, rec...)
	}
	if err := os.WriteFile(filepath.Join(dir, "index_00000.log"), buf, 0o644); err != nil {
		t.Fatal(err)
	}
	m, err := BlockHashes(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(m) != 3 {
		t.Fatalf("got %d entries", len(m))
	}
	var h [32]byte
	h[0] = 0xA2
	if m[h] != 2 {
		t.Fatalf("hash->height: got %d want 2", m[h])
	}
}

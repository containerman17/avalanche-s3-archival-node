package main

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
)

func TestDumpSource(t *testing.T) {
	var buf bytes.Buffer
	for h := uint64(1); h <= 3; h++ {
		body := bytes.Repeat([]byte{byte(h)}, int(h)*5)
		binary.Write(&buf, binary.LittleEndian, h)
		binary.Write(&buf, binary.LittleEndian, uint32(len(body)))
		buf.Write(body)
	}
	path := filepath.Join(t.TempDir(), "d.bin")
	os.WriteFile(path, buf.Bytes(), 0o644)

	d, err := openDump(path, 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer d.close()
	if d.Last() != 3 {
		t.Fatalf("last=%d", d.Last())
	}
	for h := uint64(1); h <= 3; h++ {
		raw, ok, err := d.GetByHeight(h)
		if err != nil || !ok || !bytes.Equal(raw, bytes.Repeat([]byte{byte(h)}, int(h)*5)) {
			t.Fatalf("height %d: ok=%v err=%v raw=%x", h, ok, err, raw)
		}
	}
	if _, ok, err := d.GetByHeight(4); ok || err != nil {
		t.Fatalf("past the end: ok=%v err=%v", ok, err)
	}

	d2, err := openDump(path, 2, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer d2.close()
	if _, _, err := d2.GetByHeight(1); err == nil {
		t.Fatal("below --from must error")
	}
	if _, ok, _ := d2.GetByHeight(3); ok {
		t.Fatal("past --to must be ok=false")
	}

	// A gap in the heights is refused.
	bad := append([]byte{}, buf.Bytes()...)
	binary.LittleEndian.PutUint64(bad[12+5:], 7)
	os.WriteFile(path, bad, 0o644)
	if _, err := openDump(path, 1, 0); err == nil {
		t.Fatal("gap must be refused")
	}
}

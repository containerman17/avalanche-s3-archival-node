package dist

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io/fs"
	"math/rand"
	"os"
	"testing"
)

// TestDigestChunks pins the two things the whole artifact layer rests on: the
// name is the sha256 of the bytes, and the chunk list is the sha256 of each
// aligned ChunkSize range, tail included.
func TestDigestChunks(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	body := make([]byte, 2*ChunkSize+1234)
	rng.Read(body)

	d := NewDigest()
	// written in awkward pieces on purpose: chunk boundaries must not depend
	// on how the producer happened to call Write.
	for off := 0; off < len(body); off += 7919 {
		end := min(off+7919, len(body))
		d.Write(body[off:end])
	}
	hash, list := d.Sum()

	if want := sha256.Sum256(body); hash != hex.EncodeToString(want[:]) {
		t.Fatalf("whole-file hash %s, want %x", hash, want)
	}
	if len(list) != 3*sha256.Size {
		t.Fatalf("chunk list is %d bytes, want 3 chunks", len(list)/sha256.Size)
	}
	for i := 0; i < 3; i++ {
		end := min((i+1)*ChunkSize, len(body))
		if err := VerifyChunk(list, i, body[i*ChunkSize:end]); err != nil {
			t.Fatal(err)
		}
	}
	bad := append([]byte(nil), body[:ChunkSize]...)
	bad[0] ^= 0xff
	if err := VerifyChunk(list, 0, bad); err == nil {
		t.Fatal("a corrupt chunk must not verify")
	}
}

// TestStoreLocalRoundtrip is the no-credentials path end to end: publish,
// read back through the one read call, and find the chunk list by pointer.
func TestStoreLocalRoundtrip(t *testing.T) {
	s, err := Local(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if s.Remote() {
		t.Fatal("Local must never be remote")
	}
	body := bytes.Repeat([]byte("epoch bytes "), 1000)
	hash, err := s.Put(body)
	if err != nil {
		t.Fatal(err)
	}
	if !ValidHash(hash) {
		t.Fatalf("hash %q", hash)
	}
	b, err := s.Open(hash)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if b.Size() != uint64(len(body)) {
		t.Fatalf("size %d, want %d", b.Size(), len(body))
	}
	got, err := b.Slice(10, 40)
	if err != nil || !bytes.Equal(got, body[10:50]) {
		t.Fatalf("slice: %q %v", got, err)
	}
	if _, err := b.Slice(b.Size()-1, 2); err == nil {
		t.Fatal("a range past the end must error")
	}
	list, err := s.Chunks(hash)
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifyChunk(list, 0, body); err != nil {
		t.Fatal(err)
	}
	// Sync is a no-op without credentials: the spool IS the durable copy.
	if err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(s.SpoolPath(hash)); err != nil {
		t.Fatalf("spool file must survive Sync without credentials: %v", err)
	}
}

// TestReaderBlobMatchesMmap covers the path a node with S3 credentials takes:
// the same bytes arrive through ReadAt copies instead of the mapping.
func TestReaderBlobMatchesMmap(t *testing.T) {
	dir := t.TempDir()
	body := bytes.Repeat([]byte("0123456789"), 5000)
	p := dir + "/blob"
	if err := os.WriteFile(p, body, 0o644); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(p)
	if err != nil {
		t.Fatal(err)
	}
	ra := ReaderBlob(f, uint64(len(body)), f)
	defer ra.Close()
	mm, err := MmapBlob(p)
	if err != nil {
		t.Fatal(err)
	}
	defer mm.Close()
	for _, r := range [][2]uint64{{0, 1}, {17, 4096}, {uint64(len(body)) - 3, 3}} {
		a, err1 := ra.Slice(r[0], r[1])
		b, err2 := mm.Slice(r[0], r[1])
		if err1 != nil || err2 != nil || !bytes.Equal(a, b) {
			t.Fatalf("range %v: %v %v", r, err1, err2)
		}
	}
}

func TestLatestPointer(t *testing.T) {
	s, err := Local(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Latest(); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("missing pointer: %v, want fs.ErrNotExist", err)
	}
	want := Latest{Epoch: hex.EncodeToString(bytes.Repeat([]byte{1}, 32))}
	if err := s.SetLatest(want); err != nil {
		t.Fatal(err)
	}
	got, err := s.Latest()
	if err != nil || got != want {
		t.Fatalf("latest: %+v %v", got, err)
	}
	want.Snapshot = hex.EncodeToString(bytes.Repeat([]byte{2}, 32))
	if err := s.SetLatest(want); err != nil {
		t.Fatal(err)
	}
	if got, err := s.Latest(); err != nil || got != want {
		t.Fatalf("latest with snapshot: %+v %v", got, err)
	}
}

package dist

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io/fs"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
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

// TestPointersOutsideChunkCache pins the one thing that made the credentialed
// path eat its own metadata: casfs deletes everything in its cache directory
// that is not one of its sharded artifact files, so no pointer may live there.
func TestPointersOutsideChunkCache(t *testing.T) {
	dir := t.TempDir()
	s, err := Local(dir)
	if err != nil {
		t.Fatal(err)
	}
	cache := filepath.Join(rootFor(dir), cacheName)
	for _, name := range []string{LatestPointer([32]byte{5}), ChunkPointer(hex.EncodeToString(bytes.Repeat([]byte{3}, 32)))} {
		p := s.pointerPath(name)
		if p == cache || strings.HasPrefix(p, cache+string(os.PathSeparator)) {
			t.Fatalf("pointer %q lands at %s, inside the casfs chunk cache %s", name, p, cache)
		}
	}
}

// TestOpenWithoutStaticKeys pins the credential contract: an endpoint with no
// EPOCHDB_S3_ACCESS_KEY/SECRET_KEY is the AWS default chain, not an error. The
// stub is env credentials, which is the first link of that chain, so the test
// resolves nothing over the network and never touches a real account.
func TestOpenWithoutStaticKeys(t *testing.T) {
	t.Setenv("EPOCHDB_S3_ENDPOINT", "https://s3.example.invalid")
	t.Setenv("EPOCHDB_S3_BUCKET", "epochs")
	t.Setenv("EPOCHDB_S3_ACCESS_KEY", "")
	t.Setenv("EPOCHDB_S3_SECRET_KEY", "")
	t.Setenv("AWS_ACCESS_KEY_ID", "AKIDSTUB")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "stub")
	t.Setenv("AWS_SESSION_TOKEN", "stubtoken")
	t.Setenv("AWS_REGION", "ap-northeast-1")

	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("open with no static keys: %v", err)
	}
	defer s.Close()
	if !s.Remote() {
		t.Fatal("store is local, want a credentialed store from the default chain")
	}
}

func TestLatestPointer(t *testing.T) {
	s, err := Local(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	root := [32]byte{7}
	if _, err := s.Latest(root); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("missing pointer: %v, want fs.ErrNotExist", err)
	}
	want := Latest{Epoch: hex.EncodeToString(bytes.Repeat([]byte{1}, 32))}
	if err := s.SetLatest(root, want); err != nil {
		t.Fatal(err)
	}
	got, err := s.Latest(root)
	if err != nil || got != want {
		t.Fatalf("latest: %+v %v", got, err)
	}
	want.Epoch = hex.EncodeToString(bytes.Repeat([]byte{2}, 32))
	if err := s.SetLatest(root, want); err != nil {
		t.Fatal(err)
	}
	if got, err := s.Latest(root); err != nil || got != want {
		t.Fatalf("latest after advance: %+v %v", got, err)
	}
	// Another chain's tip is another pointer, in the same dir and the same
	// bucket: nothing about one chain's `latest` is visible to a sibling.
	if _, err := s.Latest([32]byte{8}); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("a sibling chain's pointer: %v, want fs.ErrNotExist", err)
	}
}

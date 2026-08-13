package dist

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io/fs"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestArtifactNameIsTheChunkList pins what the whole artifact layer rests on:
// an artifact's NAME is sha256 of the per-chunk hash list over its content,
// the last chunk short, and the file on disk carries that list past the
// content where no consumer sees it.
func TestArtifactNameIsTheChunkList(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	body := make([]byte, 2*ChunkSize+1234)
	rng.Read(body)

	h := NewHasher()
	// written in awkward pieces on purpose: chunk boundaries must not depend
	// on how the producer happened to call Write.
	for off := 0; off < len(body); off += 7919 {
		h.Write(body[off:min(off+7919, len(body))])
	}
	name, tail, err := h.Finish()
	if err != nil {
		t.Fatal(err)
	}

	var want []byte
	for off := 0; off < len(body); off += ChunkSize {
		sum := sha256.Sum256(body[off:min(off+ChunkSize, len(body))])
		want = append(want, sum[:]...)
	}
	if len(want) != 3*sha256.Size {
		t.Fatalf("built a %d chunk list, want 3", len(want)/sha256.Size)
	}
	if sum := sha256.Sum256(want); name != hex.EncodeToString(sum[:]) {
		t.Fatalf("name %s, want sha256 of the chunk list %x", name, sum)
	}
	if !bytes.HasPrefix(tail, want) {
		t.Fatal("the tail does not open with the chunk list")
	}

	// And a store round-trips it with the tail invisible: L bytes, not L+tail.
	s, err := Local(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	hash, err := s.Put(body)
	if err != nil {
		t.Fatal(err)
	}
	if hash != name {
		t.Fatalf("Put named it %s, the list names it %s", hash, name)
	}
	b, err := s.Open(hash)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if b.Size() != uint64(len(body)) {
		t.Fatalf("Size() = %d, want the logical %d with no tail in it", b.Size(), len(body))
	}
	got, err := b.Read(0, b.Size())
	if err != nil || !bytes.Equal(got, body) {
		t.Fatalf("read back differed: %v", err)
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
	got, err := b.Read(10, 40)
	if err != nil || !bytes.Equal(got, body[10:50]) {
		t.Fatalf("read: %q %v", got, err)
	}
	if _, err := b.Read(b.Size()-1, 2); err == nil {
		t.Fatal("a range past the end must error")
	}
	v, err := b.View(10, 40)
	if err != nil || !bytes.Equal(v.Slice(0, 40), body[10:50]) {
		t.Fatalf("view: %v", err)
	}
	v.Close()
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
// that is not one of its own window directories, so no pointer may live there.
func TestPointersOutsideChunkCache(t *testing.T) {
	dir := t.TempDir()
	s, err := Local(dir)
	if err != nil {
		t.Fatal(err)
	}
	cache := CacheRoot(dir)
	for _, name := range []string{LatestPointer([32]byte{5})} {
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
	want := Latest{Manifest: hex.EncodeToString(bytes.Repeat([]byte{1}, 32))}
	if err := s.SetLatest(root, want); err != nil {
		t.Fatal(err)
	}
	got, err := s.Latest(root)
	if err != nil || got != want {
		t.Fatalf("latest: %+v %v", got, err)
	}
	want.Manifest = hex.EncodeToString(bytes.Repeat([]byte{2}, 32))
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

	// A TORN POINTER IS NOT AN ANSWER. An empty value used to decode to a
	// valid Latest{Manifest: ""}, which reads as "this chain has no runs".
	if err := os.WriteFile(filepath.Join(s.dir, LatestPointer(root)), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Latest(root); err == nil {
		t.Fatal("an empty pointer value decoded as a chain with no runs")
	}
}

// TestNothingCanDeleteAnObject is a GREP, and it is the right shape for this
// rule: THE BUCKET IS WRITE-ONCE AND FOREVER (DESIGN, storage v0). There is no
// delete, recycle or garbage-collection concept anywhere, so the check is not
// "does the delete path behave" but "does a delete path exist at all". It reads
// every non-test Go file in this repository AND in casfs, which is the only
// code that ever speaks to a bucket.
//
// THE ONE ALLOWED DELETE IS AN ABORT OF A MULTIPART UPLOAD (`uploadId`), which
// cancels bytes that never became an object. Anything else fails here.
func TestNothingCanDeleteAnObject(t *testing.T) {
	roots := []string{".."}
	out, err := exec.Command("go", "list", "-m", "-f", "{{.Dir}}", "github.com/containerman17/casfs").Output()
	if err != nil {
		t.Fatalf("locate casfs: %v", err)
	}
	roots = append(roots, strings.TrimSpace(string(out)))

	for _, root := range roots {
		err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
			if err != nil || d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return err
			}
			b, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			for i, line := range strings.Split(string(b), "\n") {
				if !strings.Contains(line, "MethodDelete") && !strings.Contains(line, `"DELETE"`) {
					continue
				}
				if strings.Contains(line, "uploadId") {
					continue // aborting a multipart upload deletes no object
				}
				t.Errorf("%s:%d speaks DELETE to a bucket: %s", path, i+1, strings.TrimSpace(line))
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}

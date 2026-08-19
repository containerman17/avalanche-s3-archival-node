package dist

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestMinioRoundTrip is the ONLY test that touches a real S3 wire. Everything
// else in this repo runs against an in-process fake, which proves the shapes
// but not that a real server accepts casfs's hand-rolled SigV4, its unsigned
// Range header, or its literal "auto" region.
//
// It is skipped unless EPOCHDB_MINIO_ENDPOINT is set, so `go test ./...` never
// depends on docker. See the wiki note
// how_to_run_the_minio_s3_roundtrip_test.md for standing MinIO up; NEVER point
// it at a real cloud bucket, and never mint credentials for it.
func TestMinioRoundTrip(t *testing.T) {
	endpoint := os.Getenv("EPOCHDB_MINIO_ENDPOINT")
	if endpoint == "" {
		t.Skip("set EPOCHDB_MINIO_ENDPOINT (and _BUCKET/_ACCESS_KEY/_SECRET_KEY) to run")
	}
	dir := t.TempDir()
	t.Setenv("EPOCHDB_S3_ENDPOINT", endpoint)
	t.Setenv("EPOCHDB_S3_BUCKET", os.Getenv("EPOCHDB_MINIO_BUCKET"))
	t.Setenv("EPOCHDB_S3_ACCESS_KEY", os.Getenv("EPOCHDB_MINIO_ACCESS_KEY"))
	t.Setenv("EPOCHDB_S3_SECRET_KEY", os.Getenv("EPOCHDB_MINIO_SECRET_KEY"))
	// A prefix per run, so repeated runs against one bucket never collide.
	t.Setenv("EPOCHDB_S3_PREFIX", fmt.Sprintf("casroundtrip/%d/", time.Now().UnixNano()))

	st, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if !st.Remote() {
		t.Fatal("credentials were set but the store came up local")
	}

	// Several chunks wide, incompressible, so the read below is genuinely a
	// series of ranged GETs and not one lucky whole-object fetch.
	body := make([]byte, 3*ChunkSize+7919)
	rand.New(rand.NewSource(1)).Read(body)
	hash, err := st.Put(body)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Sync(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(st.SpoolPath(hash)); err == nil {
		t.Fatal("Sync must upload and release: the local copy is still there, so the read below would not touch S3")
	}

	// The whole point: every byte now comes back over the wire, through the
	// window chunk cache, and has to be identical.
	b, err := st.Open(hash)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	got, err := b.Read(0, b.Size())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, body) {
		t.Fatal("the bytes that came back over the wire differ")
	}
	// A view across a chunk boundary, i.e. the resident-section path.
	v, err := b.View(ChunkSize-16, 32)
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	if !bytes.Equal(v.Slice(0, 32), body[ChunkSize-16:ChunkSize+16]) {
		t.Fatal("a view straddling a chunk boundary differs")
	}

	// And the cache really was written: <cache>/<window>/<chain>/ with chunk
	// files in it, not an empty tree that happened to serve from memory.
	wins, err := os.ReadDir(filepath.Join(dir, cacheName))
	if err != nil || len(wins) == 0 {
		t.Fatalf("no window directories after a full read: %v", err)
	}
	var files int
	for _, w := range wins {
		ents, err := os.ReadDir(filepath.Join(dir, cacheName, w.Name(), chainKey(dir)))
		if err != nil {
			t.Fatal(err)
		}
		files += len(ents)
	}
	if want := 4; files != want {
		t.Fatalf("%d chunk files cached, want %d", files, want)
	}

	// A second process over the same cache directory reads it all with the
	// bucket untouched, which is the startup walk doing its job.
	st2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()
	b2, err := st2.Open(hash)
	if err != nil {
		t.Fatal(err)
	}
	defer b2.Close()
	got2, err := b2.Read(ChunkSize, ChunkSize)
	if err != nil || !bytes.Equal(got2, body[ChunkSize:2*ChunkSize]) {
		t.Fatalf("a restarted store read the cache wrong: %v", err)
	}
}

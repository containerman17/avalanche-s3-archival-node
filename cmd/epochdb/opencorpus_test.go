package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/containerman17/epochdb/dist"
	"github.com/containerman17/epochdb/store"
)

// A PUBLISHED CHAIN HAS NO LOCAL RUN FILE. dist.Sync uploads the terminal run
// and then RELEASES the producer's copy, so `verify` and `rpcdiff` opening with
// dist.Local died with "dist: open <name>: no such file or directory" on
// exactly the chains that publish (found in production by the ladder driver,
// 2026-08-13, first hit uptn). openCorpus opens read-through, like serve.

// writeCorpus fills dataDir with a flushed corpus and returns its head height.
func writeCorpus(t *testing.T, dataDir string, root [32]byte) uint64 {
	t.Helper()
	cas, err := dist.Open(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	db, err := store.Open(dataDir, cas, root)
	if err != nil {
		t.Fatal(err)
	}
	const blocks = 8
	for h := uint64(0); h < blocks; h++ {
		b := &store.BlockWrite{
			Height:    h,
			HeaderRLP: []byte(fmt.Sprintf("header-%d", h)),
			Pvm:       []byte(fmt.Sprintf("pvm-%d", h)),
			Code:      map[string][]byte{},
		}
		if err := db.WriteBlock(b); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := cas.Close(); err != nil {
		t.Fatal(err)
	}
	return blocks - 1
}

// publish puts the corpus's runs in the bucket and NOWHERE ELSE, which is the
// state a chain is left in once it has published (dist.Sync releases every
// spool file the bucket confirms).
//
// The runs are moved from the local directory into the spool by hand because
// only a TERMINAL run uploads, and a terminal run is eight million
// transactions: nothing a test can write. The artifact is sealed identically
// either way (run.go's Adopt and AdoptLocal differ only in which directory the
// sealed file is renamed into), so the bytes here are the bytes a real publish
// uploads.
func publish(t *testing.T, dataDir string) {
	t.Helper()
	cas, err := dist.Open(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	defer cas.Close()
	ents, err := os.ReadDir(cas.LocalDir())
	if err != nil {
		t.Fatal(err)
	}
	if len(ents) == 0 {
		t.Fatal("the corpus has no runs at all, so nothing below would be read from the bucket")
	}
	for _, e := range ents {
		hash := e.Name()[strings.LastIndex(e.Name(), "-")+1:]
		if !dist.ValidHash(hash) {
			t.Fatalf("%s does not end in a hash", e.Name())
		}
		if err := os.Rename(filepath.Join(cas.LocalDir(), e.Name()), cas.SpoolPath(hash)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := cas.Sync(); err != nil {
		t.Fatal(err)
	}
	// Nothing local is left: without this, the read below would pass over
	// dist.Local too and prove nothing.
	for _, dir := range []string{cas.LocalDir(), cas.SpoolDir()} {
		ents, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range ents {
			if dist.ValidHash(e.Name()[strings.LastIndex(e.Name(), "-")+1:]) {
				t.Fatalf("%s survived the publish in %s", e.Name(), dir)
			}
		}
	}
}

// readsBack is what both commands do first: open the dir and touch a stored row.
func readsBack(t *testing.T, dataDir string, root [32]byte, head uint64) {
	t.Helper()
	cas, db, err := openCorpus(dataDir, root)
	if err != nil {
		t.Fatalf("openCorpus: %v", err)
	}
	defer cas.Close()
	defer db.Close()
	if got, ok := db.Head(); !ok || got != head {
		t.Fatalf("head %d/%v, want %d", got, ok, head)
	}
	raw, ok, err := db.HeaderRLP(head)
	if err != nil || !ok {
		t.Fatalf("hdr/%d: %v %v", head, ok, err)
	}
	if want := fmt.Sprintf("header-%d", head); string(raw) != want {
		t.Fatalf("hdr/%d is %q, want %q", head, raw, want)
	}
}

// TestOpenCorpusReadsAPublishedRun is the regression: the run file exists ONLY
// in the bucket, and the command path still reads it.
func TestOpenCorpusReadsAPublishedRun(t *testing.T) {
	endpoint := os.Getenv("EPOCHDB_MINIO_ENDPOINT")
	if endpoint == "" {
		t.Skip("set EPOCHDB_MINIO_ENDPOINT (and _BUCKET/_ACCESS_KEY/_SECRET_KEY) to run")
	}
	t.Setenv("EPOCHDB_S3_ENDPOINT", endpoint)
	t.Setenv("EPOCHDB_S3_BUCKET", os.Getenv("EPOCHDB_MINIO_BUCKET"))
	t.Setenv("EPOCHDB_S3_ACCESS_KEY", os.Getenv("EPOCHDB_MINIO_ACCESS_KEY"))
	t.Setenv("EPOCHDB_S3_SECRET_KEY", os.Getenv("EPOCHDB_MINIO_SECRET_KEY"))
	// A prefix per run, so repeated runs against one bucket never collide.
	t.Setenv("EPOCHDB_S3_PREFIX", fmt.Sprintf("opencorpus/%d/", time.Now().UnixNano()))

	dir := t.TempDir()
	root := [32]byte{7}
	head := writeCorpus(t, dir, root)
	publish(t, dir)
	readsBack(t, dir, root, head)
}

// TestOpenCorpusOffline pins the other half: no S3 environment at all still
// opens local-only, exactly as dist.Local did.
func TestOpenCorpusOffline(t *testing.T) {
	t.Setenv("EPOCHDB_S3_ENDPOINT", "")
	dir := t.TempDir()
	root := [32]byte{7}
	head := writeCorpus(t, dir, root)
	readsBack(t, dir, root, head)
}

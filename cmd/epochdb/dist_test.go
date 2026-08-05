package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/containerman17/epochdb/chain"
	"github.com/containerman17/epochdb/dist"
	"github.com/containerman17/epochdb/state"
)

// buildLinkedEpochs publishes two chained epochs and returns their hashes.
func buildLinkedEpochs(t *testing.T, st *dist.Store, root [32]byte) (string, string) {
	t.Helper()
	build := func(start uint64, prev [32]byte) string {
		in := &state.EpochInput{
			Start:    start,
			Prev:     prev,
			TxHashes: map[uint64][][32]byte{},
		}
		for i := 0; i < 4; i++ {
			in.Containers = append(in.Containers, []byte(strings.Repeat("container", 20)))
			in.Headers = append(in.Headers, []byte(strings.Repeat("header", 20)))
		}
		hash, err := state.BuildEpoch(st, in)
		if err != nil {
			t.Fatal(err)
		}
		return hash
	}
	h1 := build(1, root)
	raw, err := hex.DecodeString(h1)
	if err != nil {
		t.Fatal(err)
	}
	return h1, build(5, [32]byte(raw))
}

// TestBootstrapChainWalk: with the local index thrown away, the `latest`
// pointer plus the footers alone rebuild it, and a wrong chain is refused.
func TestBootstrapChainWalk(t *testing.T) {
	dir := t.TempDir()
	st, err := dist.Local(dir)
	if err != nil {
		t.Fatal(err)
	}
	root := [32]byte{7, 7, 7}
	h1, h2 := buildLinkedEpochs(t, st, root)
	if err := st.SetLatest(root, dist.Latest{Epoch: h2}); err != nil {
		t.Fatal(err)
	}
	for _, n := range []string{state.EpochMarkerName(1, 4), state.EpochMarkerName(5, 4)} {
		if err := os.Remove(filepath.Join(dir, n)); err != nil {
			t.Fatal(err)
		}
	}

	epochs, err := bootstrapChain(st, root)
	if err != nil || epochs != 2 {
		t.Fatalf("walk: %d epochs, err %v", epochs, err)
	}
	if got, err := state.ReadMarker(dir, state.EpochMarkerName(1, 4)); err != nil || got != h1 {
		t.Fatalf("epoch 1 marker: %s %v, want %s", got, err, h1)
	}
	set, err := state.OpenEpochSet(st)
	if err != nil {
		t.Fatal(err)
	}
	defer set.Close()
	if end, ok := set.SealedEnd(); !ok || end != 8 {
		t.Fatalf("rebuilt set covers through %d (ok=%v), want 8", end, ok)
	}

	// The chain root is the trust anchor, and the pointer NAME carrying it is
	// not the check: publish the same tip under another root's pointer name and
	// the footers still refuse it.
	bad := [32]byte{9}
	if err := st.SetLatest(bad, dist.Latest{Epoch: h2}); err != nil {
		t.Fatal(err)
	}
	if _, err := bootstrapChain(st, bad); err == nil || !strings.Contains(err.Error(), "wrong chain") {
		t.Fatalf("walk with the wrong chain root: %v", err)
	}
}

// ---------- a bucket, in process ----------

// fakeS3 is a single-bucket S3 good enough for HEAD, PUT, GET and ranged GET,
// which is all casfs speaks. Same shape as state/pin_test.go's: Go test
// scaffolding does not cross package boundaries, and lifting ~50 lines into a
// non-test package to share them would put net/http/httptest in the build
// graph of the binary.
type fakeS3 struct {
	*httptest.Server
	reqs    atomic.Int64 // every request, so a test can prove one was NOT made
	mu      sync.Mutex
	objects map[string][]byte
}

func newFakeS3(t *testing.T) *fakeS3 {
	t.Helper()
	f := &fakeS3{objects: map[string][]byte{}}
	f.Server = httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(f.Server.Close)
	return f
}

func (f *fakeS3) serve(w http.ResponseWriter, r *http.Request) {
	f.reqs.Add(1)
	key := strings.TrimPrefix(r.URL.Path, "/epochs/")
	switch r.Method {
	case http.MethodPut:
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "IncompleteBody", http.StatusBadRequest)
			return
		}
		f.mu.Lock()
		f.objects[key] = body
		f.mu.Unlock()
	case http.MethodHead, http.MethodGet:
		f.mu.Lock()
		obj, ok := f.objects[key]
		f.mu.Unlock()
		if !ok {
			http.Error(w, "NoSuchKey", http.StatusNotFound)
			return
		}
		if r.Method == http.MethodHead {
			w.Header().Set("Content-Length", strconv.Itoa(len(obj)))
			return
		}
		start, end, ranged := parseTestRange(r.Header.Get("Range"), int64(len(obj)))
		if !ranged {
			w.Header().Set("Content-Length", strconv.Itoa(len(obj)))
			w.Write(obj)
			return
		}
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(obj)))
		w.Header().Set("Content-Length", strconv.FormatInt(end-start+1, 10))
		w.WriteHeader(http.StatusPartialContent)
		w.Write(obj[start : end+1])
	default:
		http.Error(w, "MethodNotAllowed", http.StatusMethodNotAllowed)
	}
}

// take removes one object (an operator's typo, a lifecycle rule, a botched
// copy) and hands it back so a subtest can put it right again.
func (f *fakeS3) take(key string) []byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	obj := f.objects[key]
	delete(f.objects, key)
	return obj
}

func (f *fakeS3) restore(key string, obj []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.objects[key] = obj
}

func parseTestRange(h string, size int64) (start, end int64, ok bool) {
	a, b, found := strings.Cut(strings.TrimPrefix(h, "bytes="), "-")
	if !strings.HasPrefix(h, "bytes=") || !found {
		return 0, 0, false
	}
	start, err := strconv.ParseInt(a, 10, 64)
	if err != nil {
		return 0, 0, false
	}
	if end, err = strconv.ParseInt(b, 10, 64); err != nil {
		return 0, 0, false
	}
	if end >= size {
		end = size - 1
	}
	return start, end, true
}

// testChain is a descriptor that exists only for its chain root, which is all
// the join sequence asks of one.
func testChain(genesis string) *chain.Chain {
	return &chain.Chain{GenesisJSON: []byte(genesis)}
}

// TestJoinChain is the start sequence of RULING 2026-08-03, all four branches,
// against a real bucket in process: a node with credentials and an EMPTY data
// dir joins a published chain by itself, and there is no bootstrap step to run
// first. The frontier build is stubbed (it is exec's, tested in exec and
// state); what is pinned here is WHICH branch runs.
func TestJoinChain(t *testing.T) {
	s3 := newFakeS3(t)
	t.Setenv("EPOCHDB_S3_ENDPOINT", s3.URL)
	t.Setenv("EPOCHDB_S3_BUCKET", "epochs")
	t.Setenv("EPOCHDB_S3_ACCESS_KEY", "ak")
	t.Setenv("EPOCHDB_S3_SECRET_KEY", "sk")

	// The producer: its own box, its own dir. It publishes two linked epochs
	// and a `latest`, then Sync releases every local copy, so from here on the
	// BUCKET is the only source there is.
	c := testChain("joined-chain-genesis")
	root := c.Root()
	prod, err := dist.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	h1, h2 := buildLinkedEpochs(t, prod, root)
	if err := prod.SetLatest(root, dist.Latest{Epoch: h2}); err != nil {
		t.Fatal(err)
	}
	if err := prod.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := prod.Close(); err != nil {
		t.Fatal(err)
	}

	// join runs the sequence on a fresh consumer dir and records whether the
	// frontier build was reached.
	join := func(t *testing.T, dir string, c *chain.Chain) (int, error) {
		t.Helper()
		built := 0
		err := joinChain(context.Background(), nodeConfig{DataDir: dir, Chain: c}, func(_ context.Context, gotDir string, st *dist.Store, gotC *chain.Chain) error {
			if gotDir != dir || gotC != c || st == nil {
				t.Errorf("frontier build got (%s, %v, %v)", gotDir, st, gotC)
			}
			built++
			return nil
		})
		return built, err
	}

	// (a) EMPTY dir, credentials, a published chain: the whole join happens by
	// itself, and it ends in the frontier build.
	t.Run("empty dir joins the published chain", func(t *testing.T) {
		dir := t.TempDir()
		built, err := join(t, dir, c)
		if err != nil {
			t.Fatalf("a fresh dir could not join a published chain: %v", err)
		}
		if built != 1 {
			t.Fatalf("frontier built %d times, want 1", built)
		}
		// The walk wrote the local index out of the bucket alone.
		if got, err := state.ReadMarker(dir, state.EpochMarkerName(1, 4)); err != nil || got != h1 {
			t.Fatalf("epoch 1 marker: %s %v, want %s", got, err, h1)
		}
		if got, err := state.ReadMarker(dir, state.EpochMarkerName(5, 4)); err != nil || got != h2 {
			t.Fatalf("epoch 5 marker: %s %v, want %s", got, err, h2)
		}
	})

	// (b) The pointer resolves but an artifact under it is gone: HARD ERROR,
	// naming the hash. The one thing that must never happen is a silent
	// from-scratch fallback, which would re-replay the whole chain.
	t.Run("a missing artifact refuses to start", func(t *testing.T) {
		obj := s3.take(h1)
		defer s3.restore(h1, obj)

		dir := t.TempDir()
		built, err := join(t, dir, c)
		if err == nil {
			t.Fatalf("a chain missing epoch %s started anyway", h1)
		}
		if !strings.Contains(err.Error(), h1) {
			t.Fatalf("the error does not name the missing artifact: %v", err)
		}
		if !strings.Contains(err.Error(), "REFUSING TO START") {
			t.Fatalf("the error does not say the node refuses to start: %v", err)
		}
		if built != 0 {
			t.Fatalf("a broken chain still reached the frontier build")
		}
		if _, err := state.ReadMarker(dir, state.EpochMarkerName(5, 4)); err != nil {
			t.Fatalf("the epochs it DID walk were not indexed: %v", err)
		}
	})

	// (c) No pointer anywhere for this chain root: a genuinely fresh chain,
	// which is the from-scratch path and not an error.
	t.Run("no pointer starts from scratch", func(t *testing.T) {
		dir := t.TempDir()
		built, err := join(t, dir, testChain("a-chain-nobody-ever-published"))
		if err != nil {
			t.Fatalf("an unpublished chain refused to start from scratch: %v", err)
		}
		if built != 0 {
			t.Fatalf("a chain with no history tried to build a frontier")
		}
		if _, err := os.Stat(filepath.Join(dir, state.EpochMarkerName(1, 4))); !os.IsNotExist(err) {
			t.Fatalf("a fresh chain wrote an epoch marker: %v", err)
		}
	})

	// (d) A dir that has already joined and executed: the warm start, and it
	// must be CHEAP (RULING 2026-08-03). Nothing is rebuilt, and the validation
	// is the local index alone: ZERO requests, whatever the bucket holds.
	t.Run("a warm dir keeps the fast path", func(t *testing.T) {
		dir := t.TempDir()
		if _, err := join(t, dir, c); err != nil { // the cold join that wrote the index
			t.Fatal(err)
		}
		store, err := state.Open(dir)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.FlushAndSetExecHead(6); err != nil {
			t.Fatal(err)
		}
		if err := store.Close(); err != nil {
			t.Fatal(err)
		}

		before := s3.reqs.Load()
		built, err := join(t, dir, c)
		if err != nil {
			t.Fatalf("warm start: %v", err)
		}
		if built != 0 {
			t.Fatalf("a dir with execution state was frontier-built again")
		}
		if n := s3.reqs.Load() - before; n != 0 {
			t.Fatalf("a warm start made %d bucket requests; it must be a filesystem check", n)
		}
	})

	// A warm dir whose local index was damaged does NOT refuse and does NOT
	// rebuild the frontier: the index is derivable from the chain, so it heals
	// by walking again.
	t.Run("a warm dir with a lost marker heals", func(t *testing.T) {
		dir := t.TempDir()
		if _, err := join(t, dir, c); err != nil {
			t.Fatal(err)
		}
		store, err := state.Open(dir)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.FlushAndSetExecHead(6); err != nil {
			t.Fatal(err)
		}
		if err := store.Close(); err != nil {
			t.Fatal(err)
		}
		if err := os.Remove(filepath.Join(dir, state.EpochMarkerName(1, 4))); err != nil {
			t.Fatal(err)
		}
		built, err := join(t, dir, c)
		if err != nil {
			t.Fatalf("a warm dir with a lost marker refused to start: %v", err)
		}
		if built != 0 {
			t.Fatalf("healing the index rebuilt the frontier over live state")
		}
		if got, err := state.ReadMarker(dir, state.EpochMarkerName(1, 4)); err != nil || got != h1 {
			t.Fatalf("the marker did not heal: %s %v", got, err)
		}
	})
}

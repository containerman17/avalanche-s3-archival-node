package store

import (
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/containerman17/epochdb/dist"
)

// READ-THROUGH FROM S3 (DESIGN, "The read path"). A node holds its blooms, its
// SST index blocks and its manifest locally and NOTHING ELSE; every data block
// comes down as a 4MB chunk on demand, and a cold point read costs local bloom
// probes plus exactly ONE remote GET.
//
// This test is the proof, over a REAL S3 wire (MinIO) with a counting proxy in
// front of it, so the numbers are the ones a node would really pay. It is
// skipped unless EPOCHDB_MINIO_ENDPOINT is set; see the wiki note
// how_to_run_the_minio_s3_roundtrip_test.md. NEVER point it at a real bucket.

// counter is a reverse proxy in front of the bucket that records what the node
// actually asks for. The unit that matters is the CHUNK: casfs fetches one 4MB
// chunk as parallel 1MB sub-ranges, so four HTTP GETs of one chunk are one
// remote read in DESIGN's sense.
type counter struct {
	mu     sync.Mutex
	gets   int
	chunks map[string]bool
	srv    *httptest.Server
}

func newCounter(t *testing.T, endpoint string) *counter {
	t.Helper()
	u, err := url.Parse(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	c := &counter{chunks: map[string]bool{}}
	rp := &httputil.ReverseProxy{Director: func(r *http.Request) {
		r.URL.Scheme, r.URL.Host = u.Scheme, u.Host // Host stays the signed one: SigV4 covers it
	}}
	c.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			c.note(r.URL.Path, r.Header.Get("Range"))
		}
		rp.ServeHTTP(w, r)
	}))
	t.Cleanup(c.srv.Close)
	return c
}

func (c *counter) note(path, rng string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.gets++
	// "bytes=<start>-<end>" -> the 4MB chunk the start falls in.
	if s, ok := strings.CutPrefix(rng, "bytes="); ok {
		if start, _, ok := strings.Cut(s, "-"); ok {
			if n, err := strconv.ParseInt(start, 10, 64); err == nil {
				c.chunks[fmt.Sprintf("%s#%d", path, n/dist.ChunkSize)] = true
			}
		}
	}
}

// reset zeroes the counters and returns a func reporting what happened since.
func (c *counter) reset() func() (gets, chunks int) {
	c.mu.Lock()
	c.gets, c.chunks = 0, map[string]bool{}
	c.mu.Unlock()
	return func() (int, int) {
		c.mu.Lock()
		defer c.mu.Unlock()
		return c.gets, len(c.chunks)
	}
}

// minioEnv points a data dir at the bucket through the counting proxy.
func minioEnv(t *testing.T, endpoint, prefix string) {
	t.Helper()
	t.Setenv("EPOCHDB_S3_ENDPOINT", endpoint)
	t.Setenv("EPOCHDB_S3_BUCKET", os.Getenv("EPOCHDB_MINIO_BUCKET"))
	t.Setenv("EPOCHDB_S3_ACCESS_KEY", os.Getenv("EPOCHDB_MINIO_ACCESS_KEY"))
	t.Setenv("EPOCHDB_S3_SECRET_KEY", os.Getenv("EPOCHDB_MINIO_SECRET_KEY"))
	t.Setenv("EPOCHDB_S3_PREFIX", prefix)
}

func requireMinio(t *testing.T) string {
	t.Helper()
	endpoint := os.Getenv("EPOCHDB_MINIO_ENDPOINT")
	if endpoint == "" {
		t.Skip("set EPOCHDB_MINIO_ENDPOINT (and _BUCKET/_ACCESS_KEY/_SECRET_KEY) to run")
	}
	return endpoint
}

// fatBlock is a block whose rows are big enough that a run spans several 4MB
// chunks, which is the only way a per-read chunk count means anything.
func fatBlock(height uint64, n int) *BlockWrite {
	b := block(height, n)
	// INCOMPRESSIBLE, and deterministic: snappy would otherwise fold a
	// repeated pad down to nothing and the corpus would never span a chunk.
	rng := rand.New(rand.NewSource(int64(height)))
	pad := func() []byte {
		p := make([]byte, 5<<10)
		rng.Read(p)
		return p
	}
	for i := range b.Txs {
		b.Txs[i].RLP = append(b.Txs[i].RLP, pad()...)
		b.Txs[i].Receipt = append(b.Txs[i].Receipt, pad()...)
		b.Txs[i].State = append(b.Txs[i].State, StateRow{
			Kind: 's', Addr: addr(byte(height)), Slot: hash32(byte(i)),
			Val: []byte(fmt.Sprintf("v-%d-%d", height, i)),
		})
	}
	return b
}

// publishRuns builds a corpus of `runs` L0 flushes in a producer dir, merges
// them into the ONE TERMINAL RUN THAT UPLOADS, and syncs, releasing every local
// copy so a consumer cannot read a single byte without the bucket.
func publishRuns(t *testing.T, endpoint, prefix string, runs int) (dir string, root [32]byte) {
	t.Helper()
	dir = t.TempDir()
	minioEnv(t, endpoint, prefix)
	cas, err := dist.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer cas.Close()
	root = [32]byte{7}
	db, err := Open(dir, cas, root)
	if err != nil {
		t.Fatal(err)
	}
	const blocks, txs = 40, 8
	// EVERY BLOCK OWNS A BOUNDARY SLOT, so a run is blocks*(txs+1) slots wide
	// and the boundary has to be the whole span: the merge span ends at the
	// FIRST run that carries it past the boundary, so a lower number here cuts
	// the terminal one run early and leaves an L0 behind.
	db.scaleTriggers(FlushTxs, FlushBlocks, uint64(blocks*(txs+1)*runs))
	h := uint64(0)
	for r := 0; r < runs; r++ {
		for i := 0; i < blocks; i++ {
			if err := db.WriteBlock(fatBlock(h, txs)); err != nil {
				t.Fatal(err)
			}
			h++
		}
		if err := db.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	// The merge runs beside execution, so a test that wants to look at the
	// terminal waits for it.
	if err := db.WaitMerge(); err != nil {
		t.Fatal(err)
	}
	if got := db.Manifest().Runs; len(got) != 1 || !got[0].Terminal() {
		t.Fatalf("the corpus did not merge into one terminal run: %+v", got)
	}
	db.Close()
	if _, err := cas.Sync(); err != nil {
		t.Fatal(err)
	}
	ents, err := os.ReadDir(filepath.Join(dir, "cas"))
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range ents {
		if dist.ValidHash(e.Name()) {
			t.Fatalf("Sync left %s in the spool: the consumer below would read it locally", e.Name())
		}
	}
	return dir, root
}

// TestReadThroughFromS3 opens a FRESH data dir against a bucket that holds
// everything and nothing local, and measures what serving costs.
func TestReadThroughFromS3(t *testing.T) {
	endpoint := requireMinio(t)
	c := newCounter(t, endpoint)
	prefix := fmt.Sprintf("readthrough/%d/", time.Now().UnixNano())

	// SIXTEEN L0 FLUSHES, ONE TERMINAL RUN: the terminal is the only artifact
	// in the bucket, so this measures read-through against exactly what a
	// consumer of a real chain would find there.
	prod, root := publishRuns(t, c.srv.URL, prefix, MergeFanIn)
	man, err := os.ReadFile(filepath.Join(prod, manifestFile))
	if err != nil {
		t.Fatal(err)
	}

	// A fresh dir: no spool, no cache, only the manifest (which is ALWAYS
	// resident by DESIGN, and which the join path rebuilds from the pointer).
	cold := t.TempDir()
	if err := os.WriteFile(filepath.Join(cold, manifestFile), man, 0o644); err != nil {
		t.Fatal(err)
	}
	minioEnv(t, c.srv.URL, prefix)
	cas, err := dist.Open(cold)
	if err != nil {
		t.Fatal(err)
	}
	defer cas.Close()

	since := c.reset()
	db, err := Open(cold, cas, root)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	openGets, openChunks := since()

	var resident uint64
	runs, done := db.snapshot()
	defer done()
	for _, r := range runs {
		resident += r.ResidentBytes()
	}
	if resident == 0 {
		t.Fatal("nothing is resident: the blooms and index blocks did not come down at open")
	}
	sizes := db.SectionSizes()
	total := sizes[SecChain] + sizes[SecState] + sizes[SecLookup]
	t.Logf("cold open of %d runs (%.1f MB of runs): %d GETs / %d chunks, resident %d bytes (%.2f%% of the corpus)",
		len(runs), float64(total)/(1<<20), openGets, openChunks, resident, 100*float64(resident)/float64(total))

	// Residency is only a claim worth making if every run spans several
	// chunks: a corpus that fits in one chunk per run is read whole either way.
	for _, r := range runs {
		if got := r.Footer.Off[numSections-1] + r.Footer.Len[numSections-1]; got < 2*dist.ChunkSize {
			t.Fatalf("run %s is %d bytes, under two chunks: too small to prove read-through", r.Name, got)
		}
	}

	// A COLD POINT READ. Every query shape below is answered out of runs that
	// have never been read, and each must cost ONE chunk.
	type probe struct {
		name string
		run  func() error
	}
	probes := []probe{
		{"state slot", func() error {
			v, ok, err := db.StorageAt(addr(2), hash32(7), 4)
			if err != nil || !ok || len(v) == 0 {
				return fmt.Errorf("%x %v %v", v, ok, err)
			}
			return nil
		}},
		{"account", func() error {
			v, ok, err := db.AccountAt(addr(2), 12)
			if err != nil || !ok || len(v) == 0 {
				return fmt.Errorf("%x %v %v", v, ok, err)
			}
			return nil
		}},
		{"tx by hash", func() error {
			n, ok, err := db.TxNumByHash(hash32(byte(20)))
			if err != nil || !ok {
				return fmt.Errorf("%d %v %v", n, ok, err)
			}
			return nil
		}},
		{"tx body", func() error {
			v, ok, err := db.TxRLP(100)
			if err != nil || !ok || len(v) == 0 {
				return fmt.Errorf("%x %v %v", v, ok, err)
			}
			return nil
		}},
		{"header", func() error {
			v, ok, err := db.HeaderRLP(30)
			if err != nil || !ok || len(v) == 0 {
				return fmt.Errorf("%x %v %v", v, ok, err)
			}
			return nil
		}},
	}
	for _, p := range probes {
		since := c.reset()
		if err := p.run(); err != nil {
			t.Fatalf("cold %s: %v", p.name, err)
		}
		gets, chunks := since()
		t.Logf("cold %-11s: %d chunks (%d ranged GETs)", p.name, chunks, gets)
		// AT MOST one: a read whose block happens to live in a chunk the open
		// already pulled is free, which is the same rule from the good side.
		if chunks > 1 {
			t.Fatalf("cold %s cost %d chunks, DESIGN says at most one", p.name, chunks)
		}
		// The second touch is free.
		since = c.reset()
		if err := p.run(); err != nil {
			t.Fatalf("warm %s: %v", p.name, err)
		}
		if gets, chunks := since(); gets != 0 || chunks != 0 {
			t.Fatalf("warm %s went back to the bucket: %d GETs / %d chunks", p.name, gets, chunks)
		}
	}

	// READ-THROUGH CORRECTNESS: what the cold bucket serves is what was written.
	for h := uint64(0); h < 640; h += 37 {
		want, ok, err := db.HeaderRLP(h)
		if err != nil || !ok {
			t.Fatalf("header %d: %v %v", h, ok, err)
		}
		if string(want) != fmt.Sprintf("header-%d", h) {
			t.Fatalf("header %d served %q from cold storage", h, want)
		}
	}
}

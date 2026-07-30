package state

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"math/bits"
	"math/rand"
	"os"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/containerman17/epochdb/dist"
)

// ---------- a bucket, in process ----------

// fakeS3 is a single-bucket S3 good enough for HEAD, PUT, GET and ranged GET,
// which is all casfs speaks. It counts ranged GETs, so a test can prove that
// eviction really happened rather than assuming it.
type fakeS3 struct {
	*httptest.Server
	rangedGets atomic.Int64

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
	key := strings.TrimPrefix(r.URL.Path, "/epochs/")
	switch r.Method {
	case http.MethodPut:
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "IncompleteBody", http.StatusBadRequest)
			return
		}
		sum := sha256.Sum256(body)
		if hex.EncodeToString(sum[:]) != r.Header.Get("X-Amz-Content-Sha256") {
			http.Error(w, "XAmzContentSHA256Mismatch", http.StatusBadRequest)
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
		f.rangedGets.Add(1)
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(obj)))
		w.Header().Set("Content-Length", strconv.FormatInt(end-start+1, 10))
		w.WriteHeader(http.StatusPartialContent)
		w.Write(obj[start : end+1])
	default:
		http.Error(w, "MethodNotAllowed", http.StatusMethodNotAllowed)
	}
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

// credStore is the CREDENTIALED path: a real casfs store over the fake bucket,
// with a cache cap of cacheBytes. Nothing above dist knows the difference.
func credStore(t *testing.T, dir string, cacheBytes int64) (*dist.Store, *fakeS3) {
	t.Helper()
	s3 := newFakeS3(t)
	t.Setenv("EPOCHDB_S3_ENDPOINT", s3.URL)
	t.Setenv("EPOCHDB_S3_BUCKET", "epochs")
	t.Setenv("EPOCHDB_S3_ACCESS_KEY", "ak")
	t.Setenv("EPOCHDB_S3_SECRET_KEY", "sk")
	t.Setenv("EPOCHDB_CACHE_BYTES", strconv.FormatInt(cacheBytes, 10))
	st, err := dist.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	return st, s3
}

// chunkyEpoch is a synthetic epoch several casfs chunks wide, so that a cache
// cap of one chunk forces real hole punching between reads: incompressible
// container bodies, plus the usual state rows, txs and logs.
func chunkyEpoch(t *testing.T, st *dist.Store, start uint64) (*EpochInput, string) {
	t.Helper()
	rng := rand.New(rand.NewSource(11))
	const nBlocks = 64
	in := &EpochInput{Start: start, TxHashes: map[uint64][][32]byte{}}
	for i := 0; i < nBlocks; i++ {
		body := make([]byte, 200<<10) // 64 x 200KB = 12.8MB of bodies
		rng.Read(body)
		in.Containers = append(in.Containers, body)
		hdr := make([]byte, 120)
		copy(hdr, fmt.Sprintf("header-%d-", start+uint64(i)))
		in.Headers = append(in.Headers, hdr)
		var h [32]byte
		rng.Read(h[:])
		in.TxHashes[start+uint64(i)] = [][32]byte{h}
		in.TxCount++
	}
	for i := 0; i < 20000; i++ {
		v := make([]byte, 40)
		rng.Read(v)
		in.StateRows = append(in.StateRows, StateRow{
			Key: synthKey('s', i), Block: start + uint64(rng.Intn(nBlocks)), Value: v,
		})
	}
	in.StateRows = append(in.StateRows, StateRow{Key: synthKey('a', 2), Block: start + 60, Value: nil})
	in.Logs = []LogRec{{Block: start + 7, Addrs: [][20]byte{addr20(1)}, Topics: [][32]byte{topic32(9)}}}
	in.FullLogs = map[uint64][]byte{start + 7: {7, 7, 7}}
	in.RcptRecs = map[uint64][]byte{start + 7: {0x21, 1}}
	hash, err := BuildEpoch(st, in)
	if err != nil {
		t.Fatal(err)
	}
	return in, hash
}

// TestCredentialedEpochSurvivesEviction is the pin contract end to end, on the
// path a node with S3 credentials actually takes.
//
// The epoch is published, uploaded, and its local copy released, so every byte
// now comes from the bucket through the casfs chunk cache. The cache is then
// capped at ONE chunk while the artifact is several, so every read punches
// holes in whatever it is not reading. The resident sections are pinned at
// open, so they must survive that; everything else must be refilled and
// re-verified by the read itself. A punched page reads back as zeros with no
// error, so any mistake here shows up as WRONG ANSWERS, not as failures.
func TestCredentialedEpochSurvivesEviction(t *testing.T) {
	dir := t.TempDir()
	st, s3 := credStore(t, dir, dist.ChunkSize) // one chunk of cache
	in, hash := chunkyEpoch(t, st, 1000)

	// Answers taken while the artifact is still spool-resident: the reference.
	want, err := OpenEpoch(st, hash)
	if err != nil {
		t.Fatal(err)
	}
	defer want.Close()

	if err := st.Sync(); err != nil { // upload, then unlink the local copy
		t.Fatal(err)
	}
	if _, err := os.Stat(st.SpoolPath(hash)); err == nil {
		t.Fatal("Sync must release the spool copy, or the test reads the local file")
	}

	e, err := OpenEpoch(st, hash)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	for i := range in.Containers {
		n := in.Start + uint64(i)
		got, err := e.Container(n)
		if err != nil {
			t.Fatalf("container %d: %v", n, err)
		}
		if !bytes.Equal(got, in.Containers[i]) {
			t.Fatalf("container %d differs under eviction pressure", n)
		}
		gh, err := e.HeaderRLP(n)
		if err != nil || !bytes.Equal(gh, in.Headers[i]) {
			t.Fatalf("header %d: %v", n, err)
		}
	}
	// The blooms and the sparse index are resident: they are the ones a hole
	// punch would turn into false negatives.
	for i := 0; i < 20000; i += 977 {
		k := synthKey('s', i)
		wv, wb, wf, err1 := want.StateSearch(k[:], in.Start+63)
		gv, gb, gf, err2 := e.StateSearch(k[:], in.Start+63)
		if err1 != nil || err2 != nil {
			t.Fatalf("state search %d: %v %v", i, err1, err2)
		}
		if !gf || gf != wf || gb != wb || !bytes.Equal(gv, wv) {
			t.Fatalf("state search %d: %x@%d(%v) want %x@%d(%v)", i, gv, gb, gf, wv, wb, wf)
		}
		if !e.MayContainKey(k[:]) {
			t.Fatalf("key bloom says no for key %d: a zeroed bloom page is a wrong answer", i)
		}
	}
	for blk, hashes := range in.TxHashes {
		fp := txFingerprint(hashes[0])
		got, err := e.TxCandidates(fp)
		if err != nil || len(got) == 0 || got[0] != blk {
			t.Fatalf("tx candidates for block %d: %v %v", blk, got, err)
		}
	}
	if b, err := e.LogAddrBlocks(addr20(1)); err != nil || len(b) != 1 || b[0] != in.Start+7 {
		t.Fatalf("log addr blocks: %v %v", b, err)
	}
	if rec, ok, err := e.StoredLogsRecord(in.Start + 7); err != nil || !ok || !bytes.Equal(rec, []byte{7, 7, 7}) {
		t.Fatalf("stored logs: %x %v %v", rec, ok, err)
	}
	if rec, ok, err := e.StoredRcptRecord(in.Start + 7); err != nil || !ok || !bytes.Equal(rec, []byte{0x21, 1}) {
		t.Fatalf("stored receipts: %x %v %v", rec, ok, err)
	}
	rows, wantRows := 0, 0
	if err := e.WalkStateRows(func(StateRow) { rows++ }); err != nil {
		t.Fatal(err)
	}
	if err := want.WalkStateRows(func(StateRow) { wantRows++ }); err != nil {
		t.Fatal(err)
	}
	if rows != wantRows || rows == 0 {
		t.Fatalf("walked %d rows, want %d", rows, wantRows)
	}

	// Eviction has to have actually bitten, or none of the above proved
	// anything: with a one-chunk cap over a multi-chunk artifact, the reads
	// above refetch ranges they already had.
	if fetched, chunks := s3.rangedGets.Load(), (want.blob.Size()+dist.ChunkSize-1)/dist.ChunkSize; fetched <= int64(chunks) {
		t.Fatalf("%d ranged GETs over a %d-chunk artifact: nothing was ever evicted, the pins proved nothing", fetched, chunks)
	}
}

// TestConcurrentTxWalkUnderEviction: the tx index is a byte view now, so a
// candidate walk reads the mapping directly. Many goroutines walking the same
// unpinned-by-default section at once, under eviction pressure, must each pin
// their own traversal and get the right answer (run with -race).
func TestConcurrentTxWalkUnderEviction(t *testing.T) {
	dir := t.TempDir()
	st, _ := credStore(t, dir, dist.ChunkSize)
	in, hash := chunkyEpoch(t, st, 2000)
	if err := st.Sync(); err != nil {
		t.Fatal(err)
	}
	e, err := OpenEpoch(st, hash)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	type probe struct {
		fp  uint64
		blk uint64
	}
	var probes []probe
	for blk, hashes := range in.TxHashes {
		probes = append(probes, probe{txFingerprint(hashes[0]), blk})
	}
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i, p := range probes {
				if (i+w)%3 == 0 { // stagger, and mix in reads that punch holes
					if _, err := e.Container(in.Start + uint64(i%len(in.Containers))); err != nil {
						errs <- err
						return
					}
				}
				got, err := e.TxCandidates(p.fp)
				if err != nil {
					errs <- err
					return
				}
				if len(got) == 0 || got[0] != p.blk {
					errs <- fmt.Errorf("worker %d: fp %x walked %v, want %d", w, p.fp, got, p.blk)
					return
				}
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	if err := <-errs; err != nil {
		t.Fatal(err)
	}
}

// TestEFByteViewMatchesHeapDecode: the Elias-Fano accessors now index the
// section bytes in place. Same bytes through the OLD heap decoder (kept here,
// and only here, as the reference) must answer identically, entry for entry.
func TestEFByteViewMatchesHeapDecode(t *testing.T) {
	st := testStore(t, t.TempDir())
	in, hash := synthEpoch(t, st, 5000)
	e, err := OpenEpoch(st, hash)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	tx, release, err := e.read(secTxidx, 0, e.off[secTxidx][1])
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	view, viewBlk, err := e.txIndex(tx)
	if err != nil {
		t.Fatal(err)
	}
	ref, refBlk := heapDecodeTxIdx(t, tx)

	if view.n != ref.n || view.l != ref.l {
		t.Fatalf("header: n=%d l=%d, reference n=%d l=%d", view.n, view.l, ref.n, ref.l)
	}
	for i := 0; i < ref.n; i++ {
		if view.get(i) != ref.get(i) || viewBlk.get(i) != refBlk.get(i) {
			t.Fatalf("entry %d: fp %x blk %d, reference fp %x blk %d",
				i, view.get(i), viewBlk.get(i), ref.get(i), refBlk.get(i))
		}
	}
	// And through the API: every tx of the epoch, plus a hash it never saw.
	for blk, hashes := range in.TxHashes {
		for _, h := range hashes {
			fp := binary.BigEndian.Uint64(h[:8]) >> 16
			lo, hi := ref.lookup(fp)
			var refBlocks []uint64
			for i := hi - 1; i >= lo; i-- {
				refBlocks = append(refBlocks, in.Start+refBlk.get(i))
			}
			got, err := e.TxCandidates(fp)
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != len(refBlocks) {
				t.Fatalf("tx %x: %v, reference %v", h[:6], got, refBlocks)
			}
			hit := false
			for i := range got {
				if got[i] != refBlocks[i] {
					t.Fatalf("tx %x: %v, reference %v", h[:6], got, refBlocks)
				}
				hit = hit || got[i] == blk
			}
			if !hit {
				t.Fatalf("tx %x: block %d not among %v", h[:6], blk, got)
			}
		}
	}
	if lo, hi := view.lookup(1<<fpBits - 1); lo != hi {
		t.Fatalf("a fingerprint nothing wrote matched [%d,%d)", lo, hi)
	}
}

// heapDecodeTxIdx is the pre-2026-07-31 reader: every word section copied into
// []uint64 on the Go heap. It exists ONLY as this test's oracle.
func heapDecodeTxIdx(t *testing.T, b []byte) (*efHeap, *packedHeap) {
	t.Helper()
	nTx := binary.LittleEndian.Uint64(b[0:8])
	l := uint(binary.LittleEndian.Uint32(b[8:12]))
	blkBits := uint(binary.LittleEndian.Uint32(b[12:16]))
	pos := 16
	var secs [5][]uint64
	for i := range secs {
		n := int(binary.LittleEndian.Uint64(b[pos:]))
		pos += 8
		ws := make([]uint64, n)
		for j := range ws {
			ws[j] = binary.LittleEndian.Uint64(b[pos+8*j:])
		}
		secs[i], pos = ws, pos+8*n
	}
	return &efHeap{
			n:    int(nTx),
			l:    l,
			lows: &packedHeap{w: secs[0], bits: l},
			high: secs[1],
			sel0: secs[2],
			sel1: secs[3],
		}, &packedHeap{w: secs[4], bits: blkBits}
}

type packedHeap struct {
	w    []uint64
	bits uint
}

func (p *packedHeap) get(i int) uint64 {
	bit := uint64(i) * uint64(p.bits)
	w, off := bit/64, bit%64
	v := p.w[w] >> off
	if off+uint64(p.bits) > 64 {
		v |= p.w[w+1] << (64 - off)
	}
	return v & (1<<p.bits - 1)
}

type efHeap struct {
	n    int
	l    uint
	lows *packedHeap
	high []uint64
	sel0 []uint64
	sel1 []uint64
}

func (e *efHeap) select0(k uint64) uint64 {
	pos := e.sel0[k/sel0Sample]
	need := k%sel0Sample + 1
	w := pos / 64
	word := ^e.high[w] >> (pos % 64) << (pos % 64)
	for {
		c := uint64(bits.OnesCount64(word))
		if c >= need {
			for i := uint64(1); i < need; i++ {
				word &= word - 1
			}
			return w*64 + uint64(bits.TrailingZeros64(word))
		}
		need -= c
		w++
		word = ^e.high[w]
	}
}

func (e *efHeap) select1(k uint64) uint64 {
	pos := e.sel1[k/sel1Sample]
	need := k%sel1Sample + 1
	w := pos / 64
	word := e.high[w] >> (pos % 64) << (pos % 64)
	for {
		c := uint64(bits.OnesCount64(word))
		if c >= need {
			for i := uint64(1); i < need; i++ {
				word &= word - 1
			}
			return w*64 + uint64(bits.TrailingZeros64(word))
		}
		need -= c
		w++
		word = e.high[w]
	}
}

func (e *efHeap) get(i int) uint64 {
	hi := e.select1(uint64(i)) - uint64(i)
	if e.l == 0 {
		return hi
	}
	return hi<<e.l | e.lows.get(i)
}

func (e *efHeap) lookup(v uint64) (int, int) {
	if e.n == 0 {
		return 0, 0
	}
	b := v >> e.l
	var lo uint64
	if b > 0 {
		lo = e.select0(b-1) - (b - 1)
	}
	hi := e.select0(b) - b
	if lo >= hi {
		return 0, 0
	}
	lv := v & (1<<e.l - 1)
	s, eIdx := int(lo), int(lo)
	for i := int(lo); i < int(hi); i++ {
		x := e.lows.get(i)
		if x < lv {
			s, eIdx = i+1, i+1
		} else if x == lv {
			eIdx = i + 1
		} else {
			break
		}
	}
	return s, eIdx
}

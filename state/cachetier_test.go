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
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
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

// credStore is the CREDENTIALED path: a real casfs store over the fake bucket.
// Nothing above dist knows the difference.
//
// minFree is the admission watermark. A watermark larger than the disk means
// the cache REFUSES EVERY FILL, which is the harshest thing the window cache
// can do to a reader and the replacement for the old one-chunk byte cap: every
// read refetches, and every answer still has to be right.
func credStore(t *testing.T, dir string, minFree int64) (*dist.Store, *fakeS3) {
	t.Helper()
	s3 := newFakeS3(t)
	t.Setenv("EPOCHDB_S3_ENDPOINT", s3.URL)
	t.Setenv("EPOCHDB_S3_BUCKET", "epochs")
	t.Setenv("EPOCHDB_S3_ACCESS_KEY", "ak")
	t.Setenv("EPOCHDB_S3_SECRET_KEY", "sk")
	if minFree > 0 {
		t.Setenv("EPOCHDB_CACHE_MIN_FREE", strconv.FormatInt(minFree, 10))
	}
	st, err := dist.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	return st, s3
}

// refuseEverything is a watermark no filesystem can be under.
const refuseEverything = int64(1) << 62

// wipeCache deletes every window directory under a data dir, i.e. what a
// sibling process's eviction worker does to this one's cache.
func wipeCache(t *testing.T, dir string) {
	t.Helper()
	ents, err := os.ReadDir(filepath.Join(dir, "chunkcache"))
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range ents {
		if err := os.RemoveAll(filepath.Join(dir, "chunkcache", e.Name())); err != nil {
			t.Fatal(err)
		}
	}
}

// TestSharedSpoolKeepsOneLatestPerChain is the fleet's pointer namespace: N
// chains through ONE casfs (dist.SetRoot, so one spool and one bucket prefix)
// each publish a tip, and neither the shared spool nor the bucket may let one
// overwrite the other's. A consumer that has only the bucket then resolves each
// chain's tip from its chain root alone, which is the single-chain bootstrap
// path (`epochdb bootstrap`) reading a fleet's output.
func TestSharedSpoolKeepsOneLatestPerChain(t *testing.T) {
	s3 := newFakeS3(t)
	t.Setenv("EPOCHDB_S3_ENDPOINT", s3.URL)
	t.Setenv("EPOCHDB_S3_BUCKET", "epochs")
	t.Setenv("EPOCHDB_S3_ACCESS_KEY", "ak")
	t.Setenv("EPOCHDB_S3_SECRET_KEY", "sk")

	fleetRoot := t.TempDir()
	dist.SetRoot(fleetRoot)
	t.Cleanup(func() { dist.SetRoot("") })

	rootA, rootB := [32]byte{0xaa, 1}, [32]byte{0xbb, 2}
	open := func(name string) *dist.Store {
		dir := filepath.Join(fleetRoot, name)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		st, err := dist.Open(dir)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { st.Close() })
		return st
	}
	a, b := open("chain-a"), open("chain-b")

	_, ha := synthEpoch(t, a, 1)
	_, hb := synthEpoch(t, b, 1000)
	if ha == hb {
		t.Fatal("the two chains published the same artifact; the test cannot tell them apart")
	}
	if err := a.SetLatest(rootA, dist.Latest{Epoch: ha}); err != nil {
		t.Fatal(err)
	}
	if err := b.SetLatest(rootB, dist.Latest{Epoch: hb}); err != nil {
		t.Fatal(err)
	}
	// One Sync for the whole fleet: the stores share the casfs, so a sibling's
	// would upload the same spool again (see fleetMain's single syncLoop).
	if err := a.Sync(); err != nil {
		t.Fatal(err)
	}

	// The consumer is a fresh box: its own spool and cache, nothing local, the
	// same bucket.
	dist.SetRoot("")
	c, err := dist.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	for _, tc := range []struct {
		name  string
		root  [32]byte
		epoch string
		start uint64
	}{{"chain-a", rootA, ha, 1}, {"chain-b", rootB, hb, 1000}} {
		l, err := c.Latest(tc.root)
		if err != nil {
			t.Fatalf("%s: latest from the bucket: %v", tc.name, err)
		}
		if l.Epoch != tc.epoch {
			t.Fatalf("%s: latest names epoch %s, want %s (a sibling chain overwrote the pointer)", tc.name, l.Epoch, tc.epoch)
		}
		e, err := OpenEpoch(c, l.Epoch)
		if err != nil {
			t.Fatalf("%s: open the epoch the pointer names: %v", tc.name, err)
		}
		if e.Start != tc.start {
			t.Fatalf("%s: pointer resolves to an epoch at %d, want %d", tc.name, e.Start, tc.start)
		}
		e.Close()
	}
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

// TestCredentialedEpochUnderAHostileCache is the correctness contract of the
// window cache, on the path a node with S3 credentials actually takes.
//
// The epoch is published, uploaded, and its local copy released, so every byte
// now comes from the bucket. The cache is then told the disk is full, so it
// REFUSES EVERY FILL: nothing is ever written, every read refetches, and the
// resident views hold heap bytes instead of mappings. Then the window tree is
// wiped underneath the open epoch, which is what a sibling process's eviction
// worker does. Every answer must still be identical to the one taken while the
// artifact was local, because a cache is not allowed to change an answer, only
// its cost.
func TestCredentialedEpochUnderAHostileCache(t *testing.T) {
	dir := t.TempDir()
	st, s3 := credStore(t, dir, refuseEverything)
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
	wipeCache(t, dir) // a sibling's worker, mid-flight

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
	// The blooms are on the heap and the sparse index is a held view: neither
	// may be affected by anything the cache does underneath.
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

	// The refusal has to have actually bitten, or none of the above proved
	// anything: with nothing allowed to land, the reads above refetch ranges
	// they already had, many times over.
	if fetched, chunks := s3.rangedGets.Load(), (want.blob.Size()+dist.ChunkSize-1)/dist.ChunkSize; fetched <= int64(chunks) {
		t.Fatalf("%d ranged GETs over a %d-chunk artifact: the cache was not refusing, so this proved nothing", fetched, chunks)
	}
	if n, err := os.ReadDir(filepath.Join(dir, "chunkcache")); err != nil || len(n) != 0 {
		t.Fatalf("the cache wrote %d window dirs while over the watermark: %v", len(n), err)
	}
}

// TestReadEpochLinkReadsOnlyTheFooter pins RULING 2026-08-03's cheapness
// constraint where it can actually be measured: the startup chain walk must
// never DOWNLOAD an epoch to prove it exists. Over a bucket-only artifact,
// reading one epoch's link costs the footer's chunk and nothing else, while
// OpenEpoch (the SERVING path, where paying for the blooms is the point) costs
// strictly more.
func TestReadEpochLinkReadsOnlyTheFooter(t *testing.T) {
	dir := t.TempDir()
	st, s3 := credStore(t, dir, 0) // the real watermark: everything is cached
	in, hash := chunkyEpoch(t, st, 3000)
	if err := st.Sync(); err != nil { // upload, then unlink the local copy
		t.Fatal(err)
	}

	before := s3.rangedGets.Load()
	link, err := ReadEpochLink(st, hash)
	if err != nil {
		t.Fatal(err)
	}
	footer := s3.rangedGets.Load() - before
	if link.Start != in.Start || link.Count != uint64(len(in.Containers)) || link.Prev != in.Prev {
		t.Fatalf("link %+v, want start %d count %d prev %x", link, in.Start, len(in.Containers), in.Prev)
	}
	if link.End() != in.Start+uint64(len(in.Containers))-1 {
		t.Fatalf("link ends at %d", link.End())
	}
	// Exactly one: 0 would mean casfs pulled the whole object instead, which is
	// the download this test exists to forbid.
	if footer != 1 {
		t.Fatalf("reading one epoch's link cost %d ranged GETs, want the footer's chunk alone", footer)
	}

	e, err := OpenEpoch(st, hash)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if full := s3.rangedGets.Load() - before - footer; full == 0 {
		t.Fatal("OpenEpoch read nothing beyond the footer's chunk, so this comparison proves nothing about the walk")
	}
}

// TestConcurrentTxWalkSharesOneMappedIndex: the tx index is mapped once, on
// the first query that gets past the bloom, and shared from then on. Many
// goroutines racing that first mapping, over a cache that refuses to keep
// anything, must all get the right answer (run with -race).
func TestConcurrentTxWalkSharesOneMappedIndex(t *testing.T) {
	dir := t.TempDir()
	st, _ := credStore(t, dir, refuseEverything)
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

	raw, err := e.read(secTxidx, 0, e.off[secTxidx][1])
	if err != nil {
		t.Fatal(err)
	}
	txv, err := e.txidx()
	if err != nil {
		t.Fatal(err)
	}
	view, viewBlk, err := e.txIndex(txv)
	if err != nil {
		t.Fatal(err)
	}
	ref, refBlk := heapDecodeTxIdx(t, raw)

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

// TestBloomsAreLoadedOnceOntoTheHeap is the ruling that took the two filters
// out of the cache entirely (2026-08-03): an epoch is immutable, so its blooms
// are copied at open and never read again. The proof is brutal on purpose: the
// entire chunk cache is deleted and the bucket is shut down, and both filters
// must still answer, for keys that are there and for keys that are not.
//
// A filter is exactly the structure where a byte that changed underneath is a
// WRONG ANSWER rather than a slow one: a zeroed bloom page turns "maybe" into
// "definitely not", i.e. history the node holds and denies.
func TestBloomsAreLoadedOnceOntoTheHeap(t *testing.T) {
	dir := t.TempDir()
	st, s3 := credStore(t, dir, 0)
	in, hash := chunkyEpoch(t, st, 4000)
	if err := st.Sync(); err != nil {
		t.Fatal(err)
	}
	e, err := OpenEpoch(st, hash)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	wipeCache(t, dir)
	s3.Server.Close() // nothing can be re-read from anywhere, ever again

	for i := 0; i < 20000; i += 331 {
		k := synthKey('s', i)
		if !e.MayContainKey(k[:]) {
			t.Fatalf("key bloom denies key %d with the cache gone: the filter was not on the heap", i)
		}
	}
	if k := synthKey('s', 1<<20); e.MayContainKey(k[:]) {
		t.Fatal("key bloom claims a key nothing ever wrote (the probe itself is broken)")
	}
	for _, hashes := range in.TxHashes {
		if !e.MayContainTx(txFingerprint(hashes[0])) {
			t.Fatal("tx bloom denies a tx it holds with the cache gone")
		}
	}
	if e.MayContainTx(1<<fpBits - 1) {
		t.Fatal("tx bloom claims a fingerprint nothing wrote")
	}
}

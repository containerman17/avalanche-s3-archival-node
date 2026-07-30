package state

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"testing"

	"github.com/containerman17/epochdb/dist"
)

// synthEpoch builds a deterministic synthetic epoch: 100 blocks from start,
// containers/headers with repetitive structure (so the dict has something
// to learn), state rows incl. exact-boundary and delete cases, txs, logs.
func synthEpoch(t *testing.T, st *dist.Store, start uint64) (*EpochInput, string) {
	t.Helper()
	rng := rand.New(rand.NewSource(int64(start) + 7))
	const nBlocks = 100
	in := &EpochInput{Start: start, TxHashes: map[uint64][][32]byte{}}
	for i := 0; i < nBlocks; i++ {
		blk := start + uint64(i)
		body := make([]byte, 300+rng.Intn(200))
		copy(body, []byte(fmt.Sprintf("container-%d-", blk)))
		for j := 20; j < len(body); j++ {
			body[j] = byte(j % 37) // repetitive: compressible
		}
		in.Containers = append(in.Containers, body)
		hdr := make([]byte, 120)
		copy(hdr, []byte(fmt.Sprintf("header-%d-", blk)))
		in.Headers = append(in.Headers, hdr)

		nTx := rng.Intn(4)
		for j := 0; j < nTx; j++ {
			var h [32]byte
			rng.Read(h[:])
			in.TxHashes[blk] = append(in.TxHashes[blk], h)
			in.TxCount++
		}
	}
	// duplicate tx fingerprint across two blocks
	var dup [32]byte
	copy(dup[:], []byte("duplicated-fingerprint-hash!!"))
	in.TxHashes[start+10] = append(in.TxHashes[start+10], dup)
	in.TxHashes[start+90] = append(in.TxHashes[start+90], dup)
	in.TxCount += 2

	key := synthKey
	// state rows: k1 written at start+5 and start+50; k2 deleted at start+60;
	// same (key,block) double write (seq dedupe, last wins)
	k1, k2 := key('s', 1), key('a', 2)
	in.StateRows = []StateRow{
		{Key: k1, Block: start + 5, Value: []byte{0x11}, Seq: 0},
		{Key: k1, Block: start + 5, Value: []byte{0x12}, Seq: 1}, // last wins
		{Key: k1, Block: start + 50, Value: []byte{0x22}},
		{Key: k2, Block: start + 20, Value: []byte{0xaa, 0xbb}},
		{Key: k2, Block: start + 60, Value: nil}, // account delete
	}
	// filler rows to force multiple SST blocks
	for i := 0; i < 5000; i++ {
		v := make([]byte, 40)
		rng.Read(v)
		in.StateRows = append(in.StateRows, StateRow{
			Key: key('s', 100+i), Block: start + uint64(rng.Intn(nBlocks)), Value: v,
		})
	}
	in.Logs = []LogRec{
		{Block: start + 7, Addrs: [][20]byte{addr20(1)}, Topics: [][32]byte{topic32(9)}},
		{Block: start + 77, Addrs: [][20]byte{addr20(1), addr20(2)}, Topics: [][32]byte{topic32(9)}},
	}

	hash, err := BuildEpoch(st, in)
	if err != nil {
		t.Fatal(err)
	}
	return in, hash
}

// testStore is a store with no S3 credentials whatever the environment says:
// the spool directory is the whole of it.
func testStore(t *testing.T, dir string) *dist.Store {
	t.Helper()
	st, err := dist.Local(dir)
	if err != nil {
		t.Fatal(err)
	}
	return st
}

func addr20(seed byte) (a [20]byte)  { a[0], a[19] = seed, seed; return }
func topic32(seed byte) (h [32]byte) { h[0], h[31] = seed, seed; return }

func synthKey(kind byte, seed int) (k [sortedKeySize]byte) {
	k[0] = kind
	copy(k[1:], []byte(fmt.Sprintf("key-%c-%08d-padpadpadpadpadpadpadpadpadpadpadpad", kind, seed)))
	return
}

func TestEpochRoundtrip(t *testing.T) {
	st := testStore(t, t.TempDir())
	in, hash := synthEpoch(t, st, 1000)
	e, err := OpenEpoch(st, hash)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if e.Start != 1000 || e.Count != 100 || e.TxCount != in.TxCount {
		t.Fatalf("footer: %+v", e)
	}

	// bodies + headers, every block incl. frame boundaries (15,16,17, last)
	for i := range in.Containers {
		n := in.Start + uint64(i)
		got, err := e.Container(n)
		if err != nil || !bytes.Equal(got, in.Containers[i]) {
			t.Fatalf("container %d mismatch (err=%v)", n, err)
		}
		gh, err := e.HeaderRLP(n)
		if err != nil || !bytes.Equal(gh, in.Headers[i]) {
			t.Fatalf("header %d mismatch (err=%v)", n, err)
		}
	}
	if _, err := e.Container(in.Start - 1); err == nil {
		t.Fatal("container below epoch must error")
	}
	if _, err := e.Container(e.End() + 1); err == nil {
		t.Fatal("container above epoch must error")
	}

	// state: exact boundary, in-between, delete row, bloom
	// (BuildEpoch sorts StateRows in place: use the generator keys)
	k1 := synthKey('s', 1)
	if _, _, found, _ := e.StateSearch(k1[:], 1004); found {
		t.Fatal("write must be invisible below its block")
	}
	if v, blk, found, _ := e.StateSearch(k1[:], 1005); !found || blk != 1005 || !bytes.Equal(v, []byte{0x12}) {
		t.Fatalf("at 1005: v=%x blk=%d found=%v (seq dedupe: last write wins)", v, blk, found)
	}
	if v, _, found, _ := e.StateSearch(k1[:], 1049); !found || !bytes.Equal(v, []byte{0x12}) {
		t.Fatalf("at 1049: %x %v", v, found)
	}
	if v, blk, found, _ := e.StateSearch(k1[:], 1099); !found || blk != 1050 || !bytes.Equal(v, []byte{0x22}) {
		t.Fatalf("at 1099: v=%x blk=%d", v, blk)
	}
	k2 := synthKey('a', 2)
	if v, blk, found, _ := e.StateSearch(k2[:], 1099); !found || blk != 1060 || v != nil {
		t.Fatalf("delete row: v=%x blk=%d found=%v", v, blk, found)
	}
	if !e.MayContainKey(k1[:]) || !e.MayContainKey(k2[:]) {
		t.Fatal("bloom must contain written keys")
	}
	miss := 0
	for i := 0; i < 2000; i++ {
		k := [sortedKeySize]byte{}
		binary.BigEndian.PutUint64(k[1:], uint64(1e12)+uint64(i))
		if !e.MayContainKey(k[:]) {
			miss++
		}
	}
	if miss < 1900 { // ~10 bits/key => <1% FP
		t.Fatalf("bloom too dense: only %d/2000 misses", miss)
	}
	dels := 0
	e.AccountDeletes(func(key []byte, blk uint64) {
		if !bytes.Equal(key, k2[:]) || blk != 1060 {
			t.Fatalf("unexpected delete row %x@%d", key, blk)
		}
		dels++
	})
	if dels != 1 {
		t.Fatalf("deletes: %d", dels)
	}

	// txidx: every hash resolves to its block; duplicate fp yields both
	for blk, hashes := range in.TxHashes {
		for _, h := range hashes {
			fp := binary.BigEndian.Uint64(h[:8]) >> 16
			cands := e.TxCandidates(fp)
			ok := false
			for _, c := range cands {
				ok = ok || c == blk
			}
			if !ok {
				t.Fatalf("tx %x: candidates %v missing block %d", h[:6], cands, blk)
			}
		}
	}
	var dup [32]byte
	copy(dup[:], []byte("duplicated-fingerprint-hash!!"))
	dupC := e.TxCandidates(binary.BigEndian.Uint64(dup[:8]) >> 16)
	if len(dupC) != 2 {
		t.Fatalf("duplicate fp: %v", dupC)
	}

	// logidx
	b1, err := e.LogAddrBlocks(addr20(1))
	if err != nil || len(b1) != 2 || b1[0] != 1007 || b1[1] != 1077 {
		t.Fatalf("addr1 blocks: %v %v", b1, err)
	}
	b2, _ := e.LogAddrBlocks(addr20(2))
	if len(b2) != 1 || b2[0] != 1077 {
		t.Fatalf("addr2 blocks: %v", b2)
	}
	bt, _ := e.LogTopicBlocks(topic32(9))
	if len(bt) != 2 {
		t.Fatalf("topic blocks: %v", bt)
	}
	if bn, _ := e.LogAddrBlocks(addr20(99)); bn != nil {
		t.Fatalf("absent addr: %v", bn)
	}
}

func TestEpochSetContiguity(t *testing.T) {
	st := testStore(t, t.TempDir())
	synthEpoch(t, st, 0)
	synthEpoch(t, st, 100)
	s, err := OpenEpochSet(st)
	if err != nil {
		t.Fatal(err)
	}
	if end, ok := s.SealedEnd(); !ok || end != 199 {
		t.Fatalf("sealed end: %d %v", end, ok)
	}
	if ep, ok := s.At(150); !ok || ep.Start != 100 {
		t.Fatalf("At(150): %+v %v", ep, ok)
	}
	if _, ok := s.At(200); ok {
		t.Fatal("At(200) must miss")
	}
	s.Close()

	// A gap opens as a contiguous prefix: epoch-local reads above the gap
	// stay available, full-descent reads must refuse via RequireCovered.
	st2 := testStore(t, t.TempDir())
	synthEpoch(t, st2, 0)
	synthEpoch(t, st2, 300)
	s2, err := OpenEpochSet(st2)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if got := s2.CoveredEnd(); got != 99 {
		t.Fatalf("covered end: %d, want 99", got)
	}
	if err := s2.RequireCovered(99); err != nil {
		t.Fatalf("RequireCovered(99): %v", err)
	}
	err = s2.RequireCovered(150)
	if err == nil || !strings.Contains(err.Error(), "missing epoch epoch_100") {
		t.Fatalf("RequireCovered(150): %v, want missing epoch epoch_100", err)
	}
	if _, ok := s2.At(350); !ok {
		t.Fatal("epoch-local At(350) above the gap must still resolve")
	}
	if end, ok := s2.SealedEnd(); !ok || end != 399 {
		t.Fatalf("sealed end: %d %v", end, ok)
	}
}

// TestEpochSetFloorCoverage: in limited-history mode coverage anchors at
// the base file's block, not genesis.
func TestEpochSetFloorCoverage(t *testing.T) {
	st := testStore(t, t.TempDir())
	synthEpoch(t, st, 500) // [500,599]
	synthEpoch(t, st, 600) // [600,699]
	synthEpoch(t, st, 900) // hole at 700
	s, err := OpenEpochSet(st)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// Without a floor a set that skips genesis is entirely gapped.
	if got := s.CoveredEnd(); got != 0 {
		t.Fatalf("covered end without floor: %d, want 0", got)
	}

	s.SetFloor(499)
	if got := s.CoveredEnd(); got != 699 {
		t.Fatalf("covered end with floor 499: %d, want 699", got)
	}
	if err := s.RequireCovered(500); err != nil {
		t.Fatalf("RequireCovered(500): %v", err)
	}
	// Below the floor is pruned, and typed.
	var pe *PrunedError
	err = s.RequireCovered(498)
	if !errors.As(err, &pe) || pe.Floor != 499 || pe.Block != 498 {
		t.Fatalf("RequireCovered(498): %v, want *PrunedError{498,499}", err)
	}
	// A hole above the floor is still a hole, not a pruned read.
	err = s.RequireCovered(750)
	if err == nil || errors.As(err, &pe) || !strings.Contains(err.Error(), "missing epoch epoch_700") {
		t.Fatalf("RequireCovered(750): %v, want missing epoch epoch_700", err)
	}
}

func TestEpochDeterminism(t *testing.T) {
	s1, s2 := testStore(t, t.TempDir()), testStore(t, t.TempDir())
	_, h1 := synthEpoch(t, s1, 500)
	_, h2 := synthEpoch(t, s2, 500)
	if h1 != h2 {
		t.Fatalf("same input must produce bit-identical epochs: %s vs %s", h1, h2)
	}
	b1, _ := readFileT(t, s1.SpoolPath(h1))
	b2, _ := readFileT(t, s2.SpoolPath(h2))
	if !bytes.Equal(b1, b2) {
		t.Fatal("same input must produce bit-identical epoch files")
	}
}

// TestOpenEpochRefusesOldFormat: there is no upgrade path, so an older file
// must be named as such at open rather than half-read.
func TestOpenEpochRefusesOldFormat(t *testing.T) {
	st := testStore(t, t.TempDir())
	_, hash := synthEpoch(t, st, 700)
	raw, _ := readFileT(t, st.SpoolPath(hash))
	binary.LittleEndian.PutUint32(raw[len(raw)-epochFooterSize+4:], 2)
	if err := os.WriteFile(st.SpoolPath(hash), raw, 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := OpenEpoch(st, hash)
	if err == nil || !strings.Contains(err.Error(), "format v2, unsupported") {
		t.Fatalf("OpenEpoch on a v2 file: %v, want a format refusal", err)
	}
}

func readFileT(t *testing.T, p string) ([]byte, error) {
	t.Helper()
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	return b, nil
}

func TestEpochStoredLogsSections(t *testing.T) {
	st := testStore(t, t.TempDir())
	in, hash := synthEpoch(t, st, 3000) // sealed without the stored sections
	e0, err := OpenEpoch(st, hash)
	if err != nil {
		t.Fatal(err)
	}
	if e0.HasStoredLogs() {
		t.Fatal("sections must be absent when inputs were nil")
	}
	e0.Close()

	// reseal-style: same input plus stored records for two blocks
	in.FullLogs = map[uint64][]byte{
		3007: {9, 9, 9},
		3077: {1, 2, 3, 4},
	}
	in.RcptRecs = map[uint64][]byte{
		3007: {0x21, 1},
		3042: {0x33, 0},
	}
	hash2, err := BuildEpoch(st, in)
	if err != nil {
		t.Fatal(err)
	}
	e, err := OpenEpoch(st, hash2)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if !e.HasStoredLogs() {
		t.Fatal("sections must be present")
	}
	if rec, ok, err := e.StoredLogsRecord(3077); err != nil || !ok || !bytes.Equal(rec, []byte{1, 2, 3, 4}) {
		t.Fatalf("stored logs 3077: %x %v %v", rec, ok, err)
	}
	if _, ok, _ := e.StoredLogsRecord(3042); ok {
		t.Fatal("3042 has no logs record")
	}
	if rec, ok, _ := e.StoredRcptRecord(3042); !ok || !bytes.Equal(rec, []byte{0x33, 0}) {
		t.Fatalf("stored rcpt 3042: %x %v", rec, ok)
	}
	// old blocks still readable (v2 full roundtrip sanity)
	if _, err := e.Container(3010); err != nil {
		t.Fatal(err)
	}
	// state rows survive a walk (reseal input source)
	rows := 0
	if err := e.WalkStateRows(func(StateRow) { rows++ }); err != nil {
		t.Fatal(err)
	}
	if rows == 0 {
		t.Fatal("no SST rows walked")
	}
}

// TestEpochThroughRangedReads is the S3 path in miniature: the same epoch read
// through ReadAt copies instead of the local mapping must answer identically,
// section by section. Without it the whole credentials-configured read path
// would be untested until a bucket exists.
func TestEpochThroughRangedReads(t *testing.T) {
	st := testStore(t, t.TempDir())
	in, hash := synthEpoch(t, st, 1000)
	in.FullLogs = map[uint64][]byte{1007: {7, 7, 7}}
	in.RcptRecs = map[uint64][]byte{1007: {0x21, 1}}
	hash, err := BuildEpoch(st, in)
	if err != nil {
		t.Fatal(err)
	}

	local, err := OpenEpoch(st, hash)
	if err != nil {
		t.Fatal(err)
	}
	defer local.Close()

	f, err := os.Open(st.SpoolPath(hash))
	if err != nil {
		t.Fatal(err)
	}
	fi, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	ranged, err := openEpochBlob(dist.ReaderBlob(f, uint64(fi.Size()), f), hash)
	if err != nil {
		t.Fatal(err)
	}
	defer ranged.Close()

	for n := local.Start; n <= local.End(); n++ {
		a, err1 := local.Container(n)
		b, err2 := ranged.Container(n)
		if err1 != nil || err2 != nil || !bytes.Equal(a, b) {
			t.Fatalf("container %d: %v %v", n, err1, err2)
		}
		ha, _ := local.HeaderRLP(n)
		hb, _ := ranged.HeaderRLP(n)
		if !bytes.Equal(ha, hb) {
			t.Fatalf("header %d differs", n)
		}
	}
	k1 := synthKey('s', 1)
	va, ba, fa, _ := local.StateSearch(k1[:], 1099)
	vb, bb, fb, _ := ranged.StateSearch(k1[:], 1099)
	if !bytes.Equal(va, vb) || ba != bb || fa != fb {
		t.Fatalf("state search: %x@%d(%v) vs %x@%d(%v)", va, ba, fa, vb, bb, fb)
	}
	la, err1 := local.LogAddrBlocks(addr20(1))
	lb, err2 := ranged.LogAddrBlocks(addr20(1))
	if err1 != nil || err2 != nil || len(la) != len(lb) || la[0] != lb[0] {
		t.Fatalf("log addr blocks: %v %v %v %v", la, err1, lb, err2)
	}
	ta, _ := local.LogTopicBlocks(topic32(9))
	tb, _ := ranged.LogTopicBlocks(topic32(9))
	if len(ta) != len(tb) {
		t.Fatalf("log topic blocks: %v vs %v", ta, tb)
	}
	ra, oka, _ := ranged.StoredLogsRecord(1007)
	if !oka || !bytes.Equal(ra, []byte{7, 7, 7}) {
		t.Fatalf("stored logs through ranged reads: %x %v", ra, oka)
	}
	rows := 0
	if err := ranged.WalkStateRows(func(StateRow) { rows++ }); err != nil {
		t.Fatal(err)
	}
	localRows := 0
	if err := local.WalkStateRows(func(StateRow) { localRows++ }); err != nil {
		t.Fatal(err)
	}
	if rows != localRows || rows == 0 {
		t.Fatalf("walked %d rows, local walked %d", rows, localRows)
	}
}

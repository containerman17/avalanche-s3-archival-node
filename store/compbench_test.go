package store

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/rlp"
	"github.com/cockroachdb/pebble/v2/sstable"
	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/containerman17/epochdb/dist"
)

// THE COMPRESSION AND SECTION-TUNING MATRIX (DESIGN, "Storage v0": compression
// and block size are per-section format constants, measured then pinned).
//
// This is a MEASUREMENT HARNESS, not a format: it reads a real corpus's runs
// and REWRITES the same rows through the same sstable writer the store uses,
// varying only the codec and the block size. It is what produced the pinned
// constants in format.go and it is what a re-measurement runs.
//
//	EPOCHDB_CORPUS_DIR=$PWD/data-numine3 go test -run TestCompressionMatrix -v -timeout 6h ./store/
//
// Optional: EPOCHDB_BENCH_RUNS=n limits the run count, EPOCHDB_BENCH_DIR moves
// the scratch space off the corpus filesystem.

type codec struct {
	name string
	prof *sstable.CompressionProfile
}

func codecs() []codec {
	return []codec{
		{"snappy", sstable.SnappyCompression},
		{"zstd1", zstdProfile(1)},
		{"zstd3", zstdProfile(3)},
		{"zstd9", zstdProfile(9)},
		{"zstd19", zstdProfile(19)},
	}
}

// blockSizesFor is the matrix's block-size axis. Chain takes the three sizes
// under review; state and lookup also take the two SHIPPED sizes (16K and 8K),
// because a point-read section's answer is worthless without the size it is
// being compared against.
func blockSizesFor(s Section) []int {
	if s == SecChain {
		return []int{32 << 10, 128 << 10, 512 << 10}
	}
	return []int{8 << 10, 16 << 10, 32 << 10, 128 << 10, 512 << 10}
}

// benchOptions is the SHIPPED per-section profile with the codec and block size
// under test swapped in. IndexBlockSize follows the block size by the same rule
// production uses, because an index block that cannot hold the section's
// separators turns into a two-level index and that is a property of the block
// size, not a free choice.
func benchOptions(s Section, prof *sstable.CompressionProfile, blockSize int) sstable.WriterOptions {
	o := writerOptions(s)
	o.Compression = prof
	o.BlockSize = blockSize
	o.IndexBlockSize = max(64<<10, 2*blockSize)
	return o
}

// fileWritable is pebble's objstorage.Writable over a plain file.
type fileWritable struct {
	f  *os.File
	bw *bufio.Writer
	n  uint64
}

func newFileWritable(path string) (*fileWritable, error) {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	return &fileWritable{f: f, bw: bufio.NewWriterSize(f, 1<<20)}, nil
}

func (w *fileWritable) Write(p []byte) error {
	w.n += uint64(len(p))
	_, err := w.bw.Write(p)
	return err
}
func (w *fileWritable) Finish() error {
	if err := w.bw.Flush(); err != nil {
		return err
	}
	return w.f.Close()
}
func (w *fileWritable) Abort() { w.f.Close() }

// rowset is one section's rows in key order, held in two arenas so a whole
// section costs two allocations rather than millions.
type rowset struct {
	keys, vals []([]byte)
	rawBytes   uint64
}

func (r *rowset) add(k, v []byte) {
	r.keys = append(r.keys, append([]byte(nil), k...))
	r.vals = append(r.vals, append([]byte(nil), v...))
	r.rawBytes += uint64(len(k) + len(v))
}

// loadSection reads every row of one section out of a v1 run.
func loadSection(r *Run, s Section) (*rowset, error) {
	rs := &rowset{}
	err := r.ScanRange(s, nil, nil, func(k, v []byte) bool {
		rs.add(k, v)
		return true
	})
	return rs, err
}

// writeSST rewrites rows through the sstable writer and reports the file size
// and the wall time the writer took (Close included, fsync excluded: the codec
// is what is under test, not the disk).
func writeSST(path string, rs *rowset, o sstable.WriterOptions, from, to int) (uint64, time.Duration, error) {
	fw, err := newFileWritable(path)
	if err != nil {
		return 0, 0, err
	}
	start := time.Now()
	w := sstable.NewWriter(fw, o)
	for i := from; i < to; i++ {
		if err := w.Set(rs.keys[i], rs.vals[i]); err != nil {
			w.Close()
			return 0, 0, err
		}
	}
	if err := w.Close(); err != nil {
		return 0, 0, err
	}
	return fw.n, time.Since(start), nil
}

// writeSubset rewrites an arbitrary key/value list (a family carved out of a
// section) through the sstable writer.
func writeSubset(path string, keys, vals [][]byte, o sstable.WriterOptions) (uint64, time.Duration, error) {
	rs := &rowset{keys: keys, vals: vals}
	return writeSST(path, rs, o, 0, len(keys))
}

// openSST opens a rewritten section for point reads.
func openSST(path string) (*sstable.Reader, error) {
	f, err := vfs.Default.Open(path)
	if err != nil {
		return nil, err
	}
	readable, err := sstable.NewSimpleReadable(f)
	if err != nil {
		f.Close()
		return nil, err
	}
	return sstable.NewReader(context.Background(), readable, readerOptions())
}

// pointReadMedian is the measured cost of serving ONE row: seek, read the data
// block, decompress it, hand back the value. No block cache is configured, so
// every probe decompresses, which is exactly the cold-ish shape a read-through
// node sees once the chunk is local.
func pointReadMedian(r *sstable.Reader, keys [][]byte) (time.Duration, error) {
	d := make([]time.Duration, 0, len(keys))
	for _, k := range keys {
		start := time.Now()
		it, err := r.NewIter(sstable.NoTransforms, nil, nil, sstable.AssertNoBlobHandles)
		if err != nil {
			return 0, err
		}
		kv := it.SeekGE(k, 0)
		if kv == nil || !bytes.Equal(kv.K.UserKey, k) {
			it.Close()
			return 0, fmt.Errorf("probe key %x not found", k)
		}
		v, _, err := kv.Value(nil)
		if err != nil {
			it.Close()
			return 0, err
		}
		_ = v
		it.Close()
		d = append(d, time.Since(start))
	}
	sort.Slice(d, func(i, j int) bool { return d[i] < d[j] })
	return d[len(d)/2], nil
}

// sampleKeys picks n keys carrying prefix, spread over the whole section.
func sampleKeys(rs *rowset, prefix string, n int) [][]byte {
	var idx []int
	for i, k := range rs.keys {
		if strings.HasPrefix(string(k), prefix) {
			idx = append(idx, i)
		}
	}
	if len(idx) == 0 {
		return nil
	}
	rng := rand.New(rand.NewSource(1))
	out := make([][]byte, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, rs.keys[idx[rng.Intn(len(idx))]])
	}
	return out
}

// slotKeys is sampleKeys for state slot rows, which are not identified by a
// prefix but by the shape byte at 27.
func slotKeys(rs *rowset, n int) [][]byte {
	var idx []int
	for i, k := range rs.keys {
		if len(k) > 27 && k[27] == 's' {
			idx = append(idx, i)
		}
	}
	if len(idx) == 0 {
		return nil
	}
	rng := rand.New(rand.NewSource(2))
	out := make([][]byte, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, rs.keys[idx[rng.Intn(len(idx))]])
	}
	return out
}

// benchWorkers is how many runs are compressed at once. Sizes are order
// independent, so the parallelism is pure wall-clock relief; the write clock is
// taken separately, single-threaded.
func benchWorkers() int {
	if s := os.Getenv("EPOCHDB_BENCH_WORKERS"); s != "" {
		if n, err := strconv.Atoi(s); err == nil && n > 0 {
			return n
		}
	}
	return 6
}

func benchDir(t *testing.T) string {
	d := os.Getenv("EPOCHDB_BENCH_DIR")
	if d == "" {
		d = t.TempDir()
	} else {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	return d
}

// openCorpus opens a data dir's runs read-only, oldest first.
func openCorpus(t *testing.T) ([]*Run, string) {
	t.Helper()
	corpus := os.Getenv("EPOCHDB_CORPUS_DIR")
	if corpus == "" {
		t.Skip("set EPOCHDB_CORPUS_DIR to a data dir holding storage v0 runs")
	}
	man, err := loadManifest(corpus)
	if err != nil {
		t.Fatal(err)
	}
	if len(man.Runs) == 0 {
		t.Fatalf("%s holds no runs", corpus)
	}
	local, err := dist.Local(corpus)
	if err != nil {
		t.Fatal(err)
	}
	limit := len(man.Runs)
	if s := os.Getenv("EPOCHDB_BENCH_RUNS"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil {
			t.Fatal(err)
		}
		if n < limit {
			limit = n
		}
	}
	runs := make([]*Run, 0, limit)
	for _, rm := range man.Runs[:limit] {
		r, err := OpenRun(local, rm.Name)
		if err != nil {
			t.Fatal(err)
		}
		runs = append(runs, r)
	}
	t.Cleanup(func() {
		for _, r := range runs {
			r.Close()
		}
		local.Close()
	})
	return runs, corpus
}

// ---------------------------------------------------------------------------
// THE MATRIX
// ---------------------------------------------------------------------------

func TestCompressionMatrix(t *testing.T) {
	runs, corpus := openCorpus(t)
	dir := benchDir(t)
	t.Logf("corpus %s, %d runs, scratch %s", corpus, len(runs), dir)

	type cell struct {
		bytes uint64
		dur   time.Duration
	}
	// results[section][codec][blockSize]
	results := map[Section]map[string]map[int]*cell{}
	for s := SecChain; s < numSections; s++ {
		results[s] = map[string]map[int]*cell{}
		for _, c := range codecs() {
			results[s][c.name] = map[int]*cell{}
			for _, bs := range blockSizesFor(s) {
				results[s][c.name][bs] = &cell{}
			}
		}
	}
	var v1Bytes [numSections]uint64
	var rawBytes [numSections]uint64
	var rowCount [numSections]uint64

	// The probe run is the newest one: its keys are the ones a point read is
	// asked for, and its rewritten sections stay on disk for the read pass.
	probe := len(runs) - 1
	probeDir := filepath.Join(dir, "probe")
	if err := os.MkdirAll(probeDir, 0o755); err != nil {
		t.Fatal(err)
	}
	var probeRows [numSections]*rowset

	// SIZES ARE ACCUMULATED IN PARALLEL, ONE WORKER PER RUN; WRITE TIME IS NOT.
	// zstd at level 19 over a whole corpus is hours of single-core work, and a
	// byte count does not care which core produced it. The write clock is taken
	// separately below, alone on the machine, over the probe run.
	var mu sync.Mutex
	var wg sync.WaitGroup
	sem := make(chan struct{}, benchWorkers())
	for ri, r := range runs {
		wg.Add(1)
		go func(ri int, r *Run) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			for s := SecChain; s < numSections; s++ {
				rs, err := loadSection(r, s)
				if err != nil {
					t.Error(err)
					return
				}
				sizes := map[string]map[int]uint64{}
				for _, c := range codecs() {
					sizes[c.name] = map[int]uint64{}
					for _, bs := range blockSizesFor(s) {
						path := filepath.Join(dir, fmt.Sprintf("cur-%d.sst", ri))
						n, _, err := writeSST(path, rs, benchOptions(s, c.prof, bs), 0, len(rs.keys))
						if err != nil {
							t.Errorf("run %d %v %s %dKB: %v", ri, s, c.name, bs>>10, err)
							return
						}
						os.Remove(path)
						sizes[c.name][bs] = n
					}
				}
				mu.Lock()
				v1Bytes[s] += r.SectionSize(s)
				rawBytes[s] += rs.rawBytes
				rowCount[s] += uint64(len(rs.keys))
				for _, c := range codecs() {
					for _, bs := range blockSizesFor(s) {
						results[s][c.name][bs].bytes += sizes[c.name][bs]
					}
				}
				mu.Unlock()
			}
			t.Logf("run %d/%d sized", ri+1, len(runs))
		}(ri, r)
	}
	wg.Wait()
	if t.Failed() {
		t.FailNow()
	}

	// ---- write clock and point-read files, single-threaded on the probe run --
	for s := SecChain; s < numSections; s++ {
		rs, err := loadSection(runs[probe], s)
		if err != nil {
			t.Fatal(err)
		}
		probeRows[s] = rs
		for _, c := range codecs() {
			for _, bs := range blockSizesFor(s) {
				path := filepath.Join(probeDir, fmt.Sprintf("%v-%s-%d.sst", s, c.name, bs))
				_, d, err := writeSST(path, rs, benchOptions(s, c.prof, bs), 0, len(rs.keys))
				if err != nil {
					t.Fatal(err)
				}
				results[s][c.name][bs].dur = d
			}
		}
	}

	// ---- point reads on the probe run -------------------------------------
	const probes = 1000
	shapes := map[Section][][]byte{
		SecChain:  sampleKeys(probeRows[SecChain], PrefixTx, probes),
		SecState:  slotKeys(probeRows[SecState], probes),
		SecLookup: sampleKeys(probeRows[SecLookup], PrefixTxHash, probes),
	}
	// read[section][codec][blockSize]
	read := map[Section]map[string]map[int]time.Duration{}
	for s := SecChain; s < numSections; s++ {
		read[s] = map[string]map[int]time.Duration{}
		for _, c := range codecs() {
			read[s][c.name] = map[int]time.Duration{}
			for _, bs := range blockSizesFor(s) {
				path := filepath.Join(probeDir, fmt.Sprintf("%v-%s-%d.sst", s, c.name, bs))
				rd, err := openSST(path)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := pointReadMedian(rd, shapes[s][:50]); err != nil {
					t.Fatal(err) // warm the page cache so the clock times decode, not the first touch
				}
				med, err := pointReadMedian(rd, shapes[s])
				if err != nil {
					t.Fatalf("%v %s %dKB: %v", s, c.name, bs>>10, err)
				}
				rd.Close()
				read[s][c.name][bs] = med
			}
		}
	}

	// ---- report ------------------------------------------------------------
	t.Logf("\n=== MATRIX over %d runs of %s ===", len(runs), corpus)
	for s := SecChain; s < numSections; s++ {
		t.Logf("\n-- section %v: %d rows, %.1f MB raw, %.1f MB as shipped (v1 snappy, prod block size)",
			s, rowCount[s], mb(rawBytes[s]), mb(v1Bytes[s]))
		t.Logf("%-8s %8s %12s %14s %12s", "codec", "block", "MB", "write s/run", "read us")
		for _, c := range codecs() {
			for _, bs := range blockSizesFor(s) {
				cl := results[s][c.name][bs]
				t.Logf("%-8s %7dK %12.1f %14.1f %12.1f",
					c.name, bs>>10, mb(cl.bytes), cl.dur.Seconds(),
					float64(read[s][c.name][bs].Nanoseconds())/1000)
			}
		}
	}
	// The corpus total is quoted at the sizes every section shares, so it is a
	// like-for-like column and not a mix of shapes.
	t.Logf("\n-- whole corpus, one codec/block size for all three sections")
	for _, c := range codecs() {
		for _, bs := range blockSizesFor(SecChain) {
			var n uint64
			for s := SecChain; s < numSections; s++ {
				n += results[s][c.name][bs].bytes
			}
			t.Logf("%-8s %7dK %12.1f MB", c.name, bs>>10, mb(n))
		}
	}
}

// ---------------------------------------------------------------------------
// SECTION REDISTRIBUTION
// ---------------------------------------------------------------------------

// TestSectionRedistribution measures what each candidate family costs TODAY
// (as a delta against the same section with the family removed) and what it
// would cost in a section of its own under its own tuning. Speculation is not
// admissible here: every number below is a rewrite of real corpus rows.
type redistTotals struct {
	chainWhole, chainNoItx, chainNoRcpt, chainNoInput uint64
	itxOwn, itxOwnHigh                                uint64
	rcptOwn, rcptOwnHigh                              uint64
	inputOwn, inputOwnHigh                            uint64
	inputRaw, inputRows                               uint64
	famRows, famBytes                                 map[string]uint64
}

func (a *redistTotals) add(b *redistTotals) {
	a.chainWhole += b.chainWhole
	a.chainNoItx += b.chainNoItx
	a.chainNoRcpt += b.chainNoRcpt
	a.chainNoInput += b.chainNoInput
	a.itxOwn += b.itxOwn
	a.itxOwnHigh += b.itxOwnHigh
	a.rcptOwn += b.rcptOwn
	a.rcptOwnHigh += b.rcptOwnHigh
	a.inputOwn += b.inputOwn
	a.inputOwnHigh += b.inputOwnHigh
	a.inputRaw += b.inputRaw
	a.inputRows += b.inputRows
	for k, v := range b.famRows {
		a.famRows[k] += v
	}
	for k, v := range b.famBytes {
		a.famBytes[k] += v
	}
}

// redistOne carves ONE run's chain section into the candidate placements and
// compresses each of them. Everything here is a rewrite of real rows: the
// question "would this family pay for its own section" is answered by writing
// the section both ways and subtracting.
func redistOne(path string, rs *rowset) (*redistTotals, error) {
	tt := &redistTotals{famRows: map[string]uint64{}, famBytes: map[string]uint64{}}
	fam := func(k []byte) string {
		if i := bytes.IndexByte(k, '/'); i >= 0 {
			return string(k[:i+1])
		}
		return "?"
	}
	var (
		kAll, vAll       = rs.keys, rs.vals
		kNoItx, vNoItx   [][]byte
		kItx, vItx       [][]byte
		kNoRcpt, vNoRcpt [][]byte
		kRcpt, vRcpt     [][]byte
		kStrip, vStrip   [][]byte
		kInput, vInput   [][]byte
	)
	for i, k := range kAll {
		v := vAll[i]
		tt.famRows[fam(k)]++
		tt.famBytes[fam(k)] += uint64(len(k) + len(v))
		if bytes.HasPrefix(k, []byte(PrefixItx)) {
			kItx, vItx = append(kItx, k), append(vItx, v)
		} else {
			kNoItx, vNoItx = append(kNoItx, k), append(vNoItx, v)
		}
		if bytes.HasPrefix(k, []byte(PrefixRcpt)) {
			kRcpt, vRcpt = append(kRcpt, k), append(vRcpt, v)
		} else {
			kNoRcpt, vNoRcpt = append(kNoRcpt, k), append(vNoRcpt, v)
		}
		// TX INPUT DATA: the calldata is a contiguous byte range inside the tx
		// RLP, so the split is measured by SPLICING it out rather than
		// re-encoding the transaction. The stripped envelope keeps the data
		// field's length prefix, which overstates the stripped side by a couple
		// of bytes per tx and understates nothing.
		if bytes.HasPrefix(k, []byte(PrefixTx)) {
			if in := txInput(v); len(in) >= 32 {
				if at := bytes.Index(v, in); at >= 0 {
					stripped := append(append([]byte(nil), v[:at]...), v[at+len(in):]...)
					kStrip, vStrip = append(kStrip, k), append(vStrip, stripped)
					kInput, vInput = append(kInput, k), append(vInput, in)
					tt.inputRaw += uint64(len(in))
					tt.inputRows++
					continue
				}
			}
		}
		kStrip, vStrip = append(kStrip, k), append(vStrip, v)
	}

	baseProf, highProf := zstdProfile(3), zstdProfile(19)
	const (
		bs128 = 128 << 10
		bs512 = 512 << 10
	)
	var err error
	size := func(keys, vals [][]byte, bs int, prof *sstable.CompressionProfile) uint64 {
		if err != nil {
			return 0
		}
		var n uint64
		n, _, err = writeSubset(path, keys, vals, benchOptions(SecChain, prof, bs))
		os.Remove(path)
		return n
	}
	tt.chainWhole = size(kAll, vAll, bs128, baseProf)
	tt.chainNoItx = size(kNoItx, vNoItx, bs128, baseProf)
	tt.chainNoRcpt = size(kNoRcpt, vNoRcpt, bs128, baseProf)
	tt.chainNoInput = size(kStrip, vStrip, bs128, baseProf)
	tt.itxOwn = size(kItx, vItx, bs128, baseProf)
	tt.itxOwnHigh = size(kItx, vItx, bs512, highProf)
	tt.rcptOwn = size(kRcpt, vRcpt, bs128, baseProf)
	tt.rcptOwnHigh = size(kRcpt, vRcpt, bs512, highProf)
	tt.inputOwn = size(kInput, vInput, bs128, baseProf)
	tt.inputOwnHigh = size(kInput, vInput, bs512, highProf)
	return tt, err
}

// TestSectionRedistribution measures what each candidate family costs TODAY
// (as a delta against the same section with the family removed) and what it
// would cost in a section of its own under its own tuning. Speculation is not
// admissible here: every number below is a rewrite of real corpus rows.
func TestSectionRedistribution(t *testing.T) {
	runs, corpus := openCorpus(t)
	dir := benchDir(t)
	t.Logf("corpus %s, %d runs", corpus, len(runs))

	total := &redistTotals{famRows: map[string]uint64{}, famBytes: map[string]uint64{}}
	var mu sync.Mutex
	var wg sync.WaitGroup
	sem := make(chan struct{}, benchWorkers())
	for ri, r := range runs {
		wg.Add(1)
		go func(ri int, r *Run) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			rs, err := loadSection(r, SecChain)
			if err != nil {
				t.Error(err)
				return
			}
			tt, err := redistOne(filepath.Join(dir, fmt.Sprintf("redist-%d.sst", ri)), rs)
			if err != nil {
				t.Error(err)
				return
			}
			mu.Lock()
			total.add(tt)
			mu.Unlock()
			t.Logf("run %d/%d done", ri+1, len(runs))
		}(ri, r)
	}
	wg.Wait()
	if t.Failed() {
		t.FailNow()
	}

	names := make([]string, 0, len(total.famRows))
	for n := range total.famRows {
		names = append(names, n)
	}
	sort.Strings(names)
	t.Logf("\n=== chain-section families, RAW bytes (key+value) ===")
	for _, n := range names {
		t.Logf("%-8s %12d rows %12.1f MB raw", n, total.famRows[n], mb(total.famBytes[n]))
	}
	t.Logf("%-8s %12d rows %12.1f MB raw (inside the tx rows above)", "input", total.inputRows, mb(total.inputRaw))

	t.Logf("\n=== redistribution, all sizes zstd3/128K unless noted ===")
	t.Logf("chain whole                 %10.1f MB", mb(total.chainWhole))
	t.Logf("chain minus itx             %10.1f MB  (itx costs %.1f MB in place)", mb(total.chainNoItx), mb(total.chainWhole-total.chainNoItx))
	t.Logf("itx own section             %10.1f MB  (zstd19/512K: %.1f MB)", mb(total.itxOwn), mb(total.itxOwnHigh))
	t.Logf("chain minus rcpt            %10.1f MB  (rcpt costs %.1f MB in place)", mb(total.chainNoRcpt), mb(total.chainWhole-total.chainNoRcpt))
	t.Logf("rcpt own section            %10.1f MB  (zstd19/512K: %.1f MB)", mb(total.rcptOwn), mb(total.rcptOwnHigh))
	t.Logf("chain minus tx input        %10.1f MB  (input costs %.1f MB in place)", mb(total.chainNoInput), mb(total.chainWhole-total.chainNoInput))
	t.Logf("tx input own section        %10.1f MB  (zstd19/512K: %.1f MB)", mb(total.inputOwn), mb(total.inputOwnHigh))
	t.Logf("\nsplit deltas (negative = the split saves bytes)")
	t.Logf("itx:      %+.1f MB   (%+.1f MB at zstd19/512K)", mb(total.chainNoItx+total.itxOwn)-mb(total.chainWhole), mb(total.chainNoItx+total.itxOwnHigh)-mb(total.chainWhole))
	t.Logf("rcpt:     %+.1f MB   (%+.1f MB at zstd19/512K)", mb(total.chainNoRcpt+total.rcptOwn)-mb(total.chainWhole), mb(total.chainNoRcpt+total.rcptOwnHigh)-mb(total.chainWhole))
	t.Logf("tx input: %+.1f MB   (%+.1f MB at zstd19/512K)", mb(total.chainNoInput+total.inputOwn)-mb(total.chainWhole), mb(total.chainNoInput+total.inputOwnHigh)-mb(total.chainWhole))
}

// txInput returns a stored tx row's calldata, or nil if the row does not
// decode (which no row of a verified corpus should).
func txInput(raw []byte) []byte {
	tx := new(types.Transaction)
	if err := rlp.DecodeBytes(raw, tx); err != nil {
		return nil
	}
	return tx.Data()
}

// ---------------------------------------------------------------------------
// DETERMINISM
// ---------------------------------------------------------------------------

// TestZstdDeterminism is the gate the whole zstd question hangs on (DESIGN:
// byte-identity across independent builders is the core promise). It writes the
// same rows twice at each zstd level and requires identical bytes, and it
// PRINTS the hashes so a second process (and a run with -tags pebblegozstd,
// which swaps DataDog's cgo zstd for klauspost's pure Go one) can be diffed
// against it.
func TestZstdDeterminism(t *testing.T) {
	dir := t.TempDir()
	rng := rand.New(rand.NewSource(7))
	var keys, vals [][]byte
	for i := 0; i < 20000; i++ {
		keys = append(keys, TxKey(uint64(i)))
		v := make([]byte, 64+rng.Intn(512))
		// half structure, half noise: compressible like a real corpus row
		for j := range v {
			if j%4 == 0 {
				v[j] = byte(rng.Intn(256))
			} else {
				v[j] = byte(j % 7)
			}
		}
		vals = append(vals, v)
	}
	for _, c := range codecs() {
		var first string
		for pass := 0; pass < 2; pass++ {
			p := filepath.Join(dir, fmt.Sprintf("%s-%d.sst", c.name, pass))
			if _, _, err := writeSubset(p, keys, vals, benchOptions(SecChain, c.prof, 128<<10)); err != nil {
				t.Fatal(err)
			}
			b, err := os.ReadFile(p)
			if err != nil {
				t.Fatal(err)
			}
			sum := sha256.Sum256(b)
			h := hex.EncodeToString(sum[:])
			if pass == 0 {
				first = h
				t.Logf("%-8s %d bytes sha256 %s", c.name, len(b), h)
			} else if h != first {
				t.Fatalf("%s: two passes produced different bytes (%s vs %s)", c.name, first, h)
			}
			// and it must read back through the same binary that wrote it
			rd, err := openSST(p)
			if err != nil {
				t.Fatalf("%s: reopen: %v", c.name, err)
			}
			if _, err := pointReadMedian(rd, keys[:100]); err != nil {
				t.Fatalf("%s: read back: %v", c.name, err)
			}
			rd.Close()
		}
	}
}

// TestResidentBudget reports what each shape costs the node's ALWAYS-RESIDENT
// local budget (DESIGN, read-through: blooms, SST index blocks, properties and
// the metaindex never leave RAM). Block size trades directly against it: half
// the block size is roughly twice the index. It reads the sections
// TestCompressionMatrix left in EPOCHDB_BENCH_DIR/probe, so run that first.
func TestResidentBudget(t *testing.T) {
	dir := os.Getenv("EPOCHDB_BENCH_DIR")
	if dir == "" {
		t.Skip("set EPOCHDB_BENCH_DIR to the dir TestCompressionMatrix wrote")
	}
	probeDir := filepath.Join(dir, "probe")
	t.Logf("%-8s %-8s %8s %10s %10s %10s", "section", "codec", "block", "index KB", "filter KB", "total KB")
	for s := SecChain; s < numSections; s++ {
		for _, c := range codecs() {
			for _, bs := range blockSizesFor(s) {
				path := filepath.Join(probeDir, fmt.Sprintf("%v-%s-%d.sst", s, c.name, bs))
				rd, err := openSST(path)
				if err != nil {
					t.Skipf("%s: %v (run TestCompressionMatrix first)", path, err)
				}
				lay, err := rd.Layout()
				if err != nil {
					t.Fatal(err)
				}
				var idx, filt uint64
				for _, bh := range lay.Index {
					idx += bh.Length
				}
				idx += lay.TopIndex.Length
				for _, bh := range lay.Filter {
					filt += bh.Length
				}
				other := lay.Properties.Length + lay.MetaIndex.Length
				t.Logf("%-8v %-8s %7dK %10.1f %10.1f %10.1f", s, c.name, bs>>10,
					float64(idx)/1024, float64(filt)/1024,
					float64(idx+filt+other)/1024)
				rd.Close()
			}
		}
	}
}

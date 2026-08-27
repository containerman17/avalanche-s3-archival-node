package store

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble/v2/sstable"
	sstblock "github.com/cockroachdb/pebble/v2/sstable/block"
	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/containerman17/epochdb/dist"
)

// A RUN is one immutable file:
//
//	[chain SST][state SST][lookup SST][footer]
//
// The footer is fixed width and sits at the end, so opening a run is one pread
// of footerSize bytes plus three sstable.Reader opens over ranges of the same
// artifact. Its prev field is the PREVIOUS run's name, and the first run's prev
// is the CHAIN ROOT: one head hash still authenticates all of history.
const (
	runMagic   = "EPOCHRUN"
	footerSize = 3*16 + 4*8 + 32 + 4 + len(runMagic) // 124
)

// Footer is a run's trailer.
type Footer struct {
	Off                  [numSections]uint64
	Len                  [numSections]uint64
	FromTx, ToTx         uint64 // [FromTx, ToTx): the run's TxNum range and its name
	FromHeight, ToHeight uint64 // [FromHeight, ToHeight]: blocks, inclusive
	Prev                 [32]byte
	Version              uint32
}

func (f *Footer) encode() []byte {
	b := make([]byte, 0, footerSize)
	for i := range f.Off {
		b = binary.BigEndian.AppendUint64(b, f.Off[i])
		b = binary.BigEndian.AppendUint64(b, f.Len[i])
	}
	b = binary.BigEndian.AppendUint64(b, f.FromTx)
	b = binary.BigEndian.AppendUint64(b, f.ToTx)
	b = binary.BigEndian.AppendUint64(b, f.FromHeight)
	b = binary.BigEndian.AppendUint64(b, f.ToHeight)
	b = append(b, f.Prev[:]...)
	b = binary.BigEndian.AppendUint32(b, f.Version)
	return append(b, runMagic...)
}

func decodeFooter(b []byte) (*Footer, error) { return decodeFooterVersion(b, StorageVersion) }

func decodeFooterVersion(b []byte, version uint32) (*Footer, error) {
	if len(b) != footerSize {
		return nil, fmt.Errorf("store: footer is %d bytes, want %d", len(b), footerSize)
	}
	if string(b[len(b)-len(runMagic):]) != runMagic {
		return nil, fmt.Errorf("store: bad run magic")
	}
	var f Footer
	p := b
	u := func() uint64 { v := binary.BigEndian.Uint64(p); p = p[8:]; return v }
	for i := range f.Off {
		f.Off[i], f.Len[i] = u(), u()
	}
	f.FromTx, f.ToTx, f.FromHeight, f.ToHeight = u(), u(), u(), u()
	copy(f.Prev[:], p)
	p = p[32:]
	f.Version = binary.BigEndian.Uint32(p)
	if f.Version != version {
		return nil, fmt.Errorf("store: run is storage version %d, want %d", f.Version, version)
	}
	return &f, nil
}

// RunName is a run's identity: the casfs name (sha256 of its chunk-hash list).
type RunName = string

// ---------------------------------------------------------------------------
// writing
// ---------------------------------------------------------------------------

// RunWriter builds one run. Sections are written in order and each takes a
// stream of ALREADY SORTED rows; the caller owns the sorting because it is the
// caller who knows whether the rows arrive sorted (chain) or have to be sorted
// at flush (state, lookup).
type RunWriter struct {
	path  string
	level int
	f     *os.File
	bw    *bufio.Writer
	h     *dist.Hasher
	off   uint64

	footer Footer
	cur    Section
	sst    *sstable.Writer
	sec    *sectionWritable
	// counts is per-section rows and bytes, for the size report.
	Rows  [numSections]uint64
	Bytes [numSections]uint64
}

// NewRunWriter starts a run of level at path, which must sit in the directory
// the finished run lands in (the rename that adopts it cannot cross a
// filesystem): the LOCAL directory for an L0 run, the spool for a terminal.
func NewRunWriter(path string, prev [32]byte, level int) (*RunWriter, error) {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	w := &RunWriter{path: path, level: level, f: f, bw: bufio.NewWriterSize(f, 1<<20), h: dist.NewHasher(), cur: -1}
	w.footer.Prev = prev
	w.footer.Version = StorageVersion
	return w, nil
}

// sectionWritable is pebble's objstorage.Writable over the run file: every byte
// also goes through the casfs hasher, so the run's NAME accumulates as it is
// written and nothing is held in memory.
type sectionWritable struct {
	w *RunWriter
	n uint64
}

func (s *sectionWritable) Write(p []byte) error {
	if _, err := s.w.h.Write(p); err != nil {
		return err
	}
	if _, err := s.w.bw.Write(p); err != nil {
		return err
	}
	s.n += uint64(len(p))
	return nil
}
func (s *sectionWritable) Finish() error { return nil }
func (s *sectionWritable) Abort()        {}

// Begin opens section s. Sections must be written in order.
func (w *RunWriter) Begin(s Section) error {
	if s != w.cur+1 {
		return fmt.Errorf("store: section %v out of order", s)
	}
	w.cur = s
	w.sec = &sectionWritable{w: w}
	w.sst = sstable.NewWriter(w.sec, writerOptions(s, w.level))
	w.footer.Off[s] = w.off
	return nil
}

// Set adds one row. Keys must arrive in Comparer order within the section.
func (w *RunWriter) Set(key, value []byte) error {
	w.Rows[w.cur]++
	w.Bytes[w.cur] += uint64(len(key) + len(value))
	return w.sst.Set(key, value)
}

// End closes the current section.
func (w *RunWriter) End() error {
	if err := w.sst.Close(); err != nil {
		return err
	}
	w.footer.Len[w.cur] = w.sec.n
	w.off += w.sec.n
	w.sst, w.sec = nil, nil
	return nil
}

// CopySection writes section s as the verbatim bytes [off, off+n) of blob: an
// SST section that is byte-identical between two storage versions goes through
// the hasher and onto disk without being decoded (the migrator's chain and
// state sections).
func (w *RunWriter) CopySection(s Section, blob *dist.Blob, off, n uint64) error {
	if s != w.cur+1 {
		return fmt.Errorf("store: section %v out of order", s)
	}
	w.cur = s
	w.footer.Off[s], w.footer.Len[s] = w.off, n
	const piece = 4 << 20
	for done := uint64(0); done < n; {
		k := min(uint64(piece), n-done)
		b, err := blob.Read(off+done, k)
		if err != nil {
			return err
		}
		if _, err := w.h.Write(b); err != nil {
			return err
		}
		if _, err := w.bw.Write(b); err != nil {
			return err
		}
		done += k
	}
	w.off += n
	return nil
}

// Finish writes the footer, fsyncs, and hands the file to the artifact store
// under the name its hash list commits to.
//
// THIS IS THE UPLOAD GATE, and it is one branch: a TERMINAL run is adopted into
// the spool, which Sync uploads; an L0 run is adopted into the local directory,
// which Sync never looks at. L0 runs and the window never leave the machine, so
// nothing in the bucket is ever superseded.
func (w *RunWriter) Finish(cas *dist.Store, fromTx, toTx, fromHeight, toHeight uint64) (RunName, *Footer, error) {
	w.footer.FromTx, w.footer.ToTx = fromTx, toTx
	w.footer.FromHeight, w.footer.ToHeight = fromHeight, toHeight
	fb := w.footer.encode()
	if _, err := w.h.Write(fb); err != nil {
		return "", nil, err
	}
	if _, err := w.bw.Write(fb); err != nil {
		return "", nil, err
	}
	if err := w.bw.Flush(); err != nil {
		return "", nil, err
	}
	if err := w.f.Sync(); err != nil {
		return "", nil, err
	}
	if err := w.f.Close(); err != nil {
		return "", nil, err
	}
	w.f = nil
	var name string
	var err error
	if w.level >= TerminalLevel {
		name, err = cas.Adopt(w.path, w.h)
	} else {
		name, err = cas.AdoptLocal(w.path, RunLabel(w.level, fromTx, toTx), w.h)
	}
	if err != nil {
		return "", nil, err
	}
	return name, &w.footer, nil
}

// Abort drops a half-written run.
func (w *RunWriter) Abort() {
	if w.f != nil {
		w.f.Close()
	}
	os.Remove(w.path)
}

// ---------------------------------------------------------------------------
// reading
// ---------------------------------------------------------------------------

// Run is an open run: three sstable.Readers over one artifact, plus the two
// resident blooms. Goroutine-safe (everything is immutable after open).
//
// A RUN IS CLOSED BY ITS LAST REFERENCE, NEVER BY THE CODE THAT RETIRES IT
// (db.go's version). refs counts the VERSIONS of the run list that hold it, and
// a version outlives every reader inside it, so the munmap and the close happen
// at the one instant nobody can be reading: not before, which was a
// use-after-munmap, and not at process exit, which was 8.5GB of allocated
// blocks per merge that `du` could not even see.
type Run struct {
	Name   RunName
	Footer *Footer

	refs   atomic.Int64
	blob   *dist.Blob
	rd     [numSections]*sstable.Reader
	sec    [numSections]*sectionReadable
	filter [numSections][]byte
}

// hold and drop are the run's lifetime, and only a version calls them.
func (r *Run) hold() { r.refs.Add(1) }

func (r *Run) drop() {
	if r.refs.Add(-1) == 0 {
		r.Close()
	}
}

// OpenRun opens the named run out of the artifact store.
func OpenRun(cas *dist.Store, name RunName) (*Run, error) {
	return openRunVersion(cas, name, nil, StorageVersion)
}

// OpenRunVersion opens a run written by an OLDER storage version, for the
// migrator only: the SST sections read the same, and the caller knows which
// keys and values it is looking at. Blooms of an old lookup section were built
// under the old split and must not be probed.
func OpenRunVersion(cas *dist.Store, name RunName, version uint32) (*Run, error) {
	return openRunVersion(cas, name, nil, version)
}

// ReopenRun opens the run again over whatever the artifact store hands out NOW,
// carrying the old handle's resident bytes across so the reopen reads nothing:
// its footer, its blooms and its SST index blocks are the same bytes for the
// same name, forever, because a run is immutable and content-addressed.
//
// It exists for ONE event: `dist.Sync` uploaded a terminal run and unlinked the
// local copy the old handle still had open. That handle keeps the file's blocks
// allocated until it closes, so the run has to move onto the chunk cache, which
// is exactly where every other node reads it from.
func ReopenRun(cas *dist.Store, old *Run) (*Run, error) {
	return openRunVersion(cas, old.Name, old, StorageVersion)
}

func openRunVersion(cas *dist.Store, name RunName, seed *Run, version uint32) (*Run, error) {
	blob, err := cas.Open(name)
	if err != nil {
		return nil, err
	}
	r := &Run{Name: name, blob: blob}
	if blob.Size() < uint64(footerSize) {
		blob.Close()
		return nil, fmt.Errorf("store: run %s is %d bytes, too small for a footer", name, blob.Size())
	}
	if seed != nil {
		r.Footer, r.filter = seed.Footer, seed.filter
	} else {
		fb, err := blob.Read(blob.Size()-uint64(footerSize), uint64(footerSize))
		if err != nil {
			blob.Close()
			return nil, err
		}
		if r.Footer, err = decodeFooterVersion(fb, version); err != nil {
			blob.Close()
			return nil, fmt.Errorf("store: run %s: %w", name, err)
		}
	}
	for s := Section(0); s < numSections; s++ {
		sec := &sectionReadable{b: blob, off: r.Footer.Off[s], n: r.Footer.Len[s]}
		if seed != nil {
			// The resident ranges are a copy, never the seed's own slice: the
			// seed is still serving reads out of it until its last reference
			// goes, and reside appends.
			sec.res = append([]residentRange(nil), seed.sec[s].res...)
			sec.resident = seed.sec[s].resident
		}
		r.sec[s] = sec
		readable, err := sstable.NewSimpleReadable(sec)
		if err != nil {
			r.Close()
			return nil, err
		}
		rd, err := sstable.NewReader(context.Background(), readable, readerOptions())
		if err != nil {
			r.Close()
			return nil, fmt.Errorf("store: run %s section %v: %w", name, s, err)
		}
		r.rd[s] = rd
		lay, err := rd.Layout()
		if err != nil {
			r.Close()
			return nil, err
		}
		// BLOOMS AND SST INDEX BLOCKS ARE ALWAYS RESIDENT LOCALLY (DESIGN,
		// read-through): a cold point read must cost local bloom probes plus
		// EXACTLY ONE remote GET, and an index block re-read from the artifact
		// would be a second one. They are read once here into the section's
		// resident overlay, which serves those byte ranges from RAM forever
		// after; only data blocks ever reach the blob again.
		resident := append([]sstblock.Handle{lay.TopIndex, lay.Properties, lay.MetaIndex}, lay.Index...)
		for _, nbh := range lay.Filter {
			resident = append(resident, nbh.Handle)
		}
		for _, bh := range resident {
			if err := sec.reside(bh); err != nil {
				r.Close()
				return nil, fmt.Errorf("store: run %s section %v: %w", name, s, err)
			}
		}
		// pebble's sstable.Reader has no point-get, so the bloom gate is ours
		// to apply: keep the filter bytes in hand for MayHave.
		if seed == nil && len(lay.Filter) > 0 && lay.Filter[0].Length > 0 {
			f, err := blob.Read(r.Footer.Off[s]+lay.Filter[0].Offset, lay.Filter[0].Length)
			if err != nil {
				r.Close()
				return nil, err
			}
			r.filter[s] = f
		}
	}
	return r, nil
}

// ResidentBytes is what this run costs the node's local budget: its blooms and
// its SST index blocks, which never leave RAM. Everything else is fetched
// through the chunk cache on demand.
func (r *Run) ResidentBytes() uint64 {
	var n uint64
	for _, s := range r.sec {
		if s != nil {
			n += s.resident
		}
	}
	return n
}

// Cold hands this run's pages back to the machine (dist.Blob.Cold). It is what
// the merge calls on the runs it is streaming through, so a one-pass reader
// cannot evict the state pages serving depends on. Advisory: an evicted page
// faults straight back in.
func (r *Run) Cold() error {
	if r.blob == nil {
		return nil
	}
	return r.blob.Cold()
}

func (r *Run) Close() error {
	for i, rd := range r.rd {
		if rd != nil {
			rd.Close()
			r.rd[i] = nil
		}
	}
	if r.blob != nil {
		err := r.blob.Close()
		r.blob = nil
		return err
	}
	return nil
}

// FromTx, ToTx are the run's TxNum range, [from, to).
func (r *Run) FromTx() uint64 { return r.Footer.FromTx }
func (r *Run) ToTx() uint64   { return r.Footer.ToTx }

// SectionSize is the on-disk byte size of one section.
func (r *Run) SectionSize(s Section) uint64 { return r.Footer.Len[s] }

// MayHave is the bloom gate. Chain has no filter and answers true; its
// existence question is answered by the run's tx range before any file opens.
func (r *Run) MayHave(s Section, key []byte) bool {
	if r.filter[s] == nil {
		return true
	}
	return mayContain(r.filter[s], key)
}

// newIter is the ONE place a section iterator is constructed. Runs carry no
// suffix rewriting and no blob files, so the transform set is empty and the
// blob context asserts that nothing here ever produces a blob handle.
func (r *Run) newIter(s Section) (sstable.Iterator, error) {
	return r.rd[s].NewIter(sstable.NoTransforms, nil, nil, sstable.AssertNoBlobHandles)
}

// Get is an exact-key point read.
func (r *Run) Get(s Section, key []byte) ([]byte, bool, error) {
	if !r.MayHave(s, key) {
		return nil, false, nil
	}
	it, err := r.newIter(s)
	if err != nil {
		return nil, false, err
	}
	defer it.Close()
	kv := it.SeekGE(key, 0)
	if kv == nil || !Comparer.Equal(kv.K.UserKey, key) {
		return nil, false, nil
	}
	val, _, err := kv.Value(nil)
	if err != nil {
		return nil, false, err
	}
	return append([]byte(nil), val...), true, nil
}

// Latest returns the value of the largest key under prefix whose TxNum suffix
// is <= at. This is THE state descent step: state at block N is the last write
// at or below N's end.
func (r *Run) Latest(s Section, prefix []byte, at uint64) (val []byte, txnum uint64, ok bool, err error) {
	if !r.MayHave(s, Suffixed(prefix, 0)) {
		return nil, 0, false, nil
	}
	it, err := r.newIter(s)
	if err != nil {
		return nil, 0, false, err
	}
	defer it.Close()
	// SeekLT past the sought TxNum, then check we are still under the prefix.
	kv := it.SeekLT(Suffixed(prefix, at+1), 0)
	if kv == nil || len(kv.K.UserKey) != len(prefix)+8 || string(kv.K.UserKey[:len(prefix)]) != string(prefix) {
		return nil, 0, false, nil
	}
	raw, _, err := kv.Value(nil)
	if err != nil {
		return nil, 0, false, err
	}
	return append([]byte(nil), raw...), TxNumOf(kv.K.UserKey), true, nil
}

// ScanRange calls fn for every chain-section row in [loKey, hiKey), ascending.
func (r *Run) ScanRange(s Section, loKey, hiKey []byte, fn func(key, val []byte) bool) error {
	it, err := r.rd[s].NewIter(sstable.NoTransforms, nil, hiKey, sstable.AssertNoBlobHandles)
	if err != nil {
		return err
	}
	defer it.Close()
	for kv := it.SeekGE(loKey, 0); kv != nil; kv = it.Next() {
		raw, _, err := kv.Value(nil)
		if err != nil {
			return err
		}
		if !fn(kv.K.UserKey, raw) {
			return nil
		}
	}
	return nil
}

// sectionReadable presents [off, off+n) of an artifact as a whole file to
// pebble's sstable reader, with a RESIDENT OVERLAY in front of it: the ranges
// registered by reside are served from RAM and never reach the artifact again.
type sectionReadable struct {
	b        *dist.Blob
	off, n   uint64
	res      []residentRange // sorted by offset, one entry per index/filter block
	resident uint64
}

// residentRange is one section-relative byte range held in RAM for good.
type residentRange struct {
	off  uint64
	data []byte
}

// blockTrailer is pebble's per-block trailer (compression byte + checksum),
// which readBlock always asks for on top of the block handle's length. A
// resident range that stopped at the handle's length would be a partial hit
// and would send the read to the artifact anyway.
const blockTrailer = sstblock.TrailerLen

// reside pins one block handle's bytes in RAM.
func (s *sectionReadable) reside(bh sstblock.Handle) error {
	if bh.Length == 0 {
		return nil
	}
	n := bh.Length + blockTrailer
	if bh.Offset+n > s.n {
		return fmt.Errorf("block [%d,%d) is outside a %d byte section", bh.Offset, bh.Offset+n, s.n)
	}
	b, err := s.b.Read(s.off+bh.Offset, n)
	if err != nil {
		return err
	}
	i := sort.Search(len(s.res), func(i int) bool { return s.res[i].off >= bh.Offset })
	if i < len(s.res) && s.res[i].off == bh.Offset {
		return nil
	}
	s.res = append(s.res, residentRange{})
	copy(s.res[i+1:], s.res[i:])
	s.res[i] = residentRange{off: bh.Offset, data: b}
	s.resident += n
	return nil
}

func (s *sectionReadable) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 || uint64(off) > s.n {
		return 0, io.EOF
	}
	n := uint64(len(p))
	if n > s.n-uint64(off) {
		return 0, io.EOF
	}
	if i := sort.Search(len(s.res), func(i int) bool { return s.res[i].off > uint64(off) }) - 1; i >= 0 {
		if r := s.res[i]; uint64(off)+n <= r.off+uint64(len(r.data)) {
			return copy(p, r.data[uint64(off)-r.off:]), nil
		}
	}
	b, err := s.b.Read(s.off+uint64(off), n)
	if err != nil {
		return 0, err
	}
	return copy(p, b), nil
}

func (s *sectionReadable) Close() error { return nil }

func (s *sectionReadable) Stat() (vfs.FileInfo, error) { return sectionStat{n: int64(s.n)}, nil }

// sectionStat is the only thing pebble asks a ReadableFile about: the size. The
// rest of the FileInfo surface is filled with zeroes ON PURPOSE, because a run
// carries no wall clock anywhere (DESIGN's determinism pins).
type sectionStat struct{ n int64 }

func (s sectionStat) Name() string           { return "section" }
func (s sectionStat) Size() int64            { return s.n }
func (s sectionStat) Mode() os.FileMode      { return 0 }
func (s sectionStat) ModTime() time.Time     { return time.Time{} }
func (s sectionStat) IsDir() bool            { return false }
func (s sectionStat) Sys() any               { return nil }
func (s sectionStat) DeviceID() vfs.DeviceID { return vfs.DeviceID{} }

var _ sstable.ReadableFile = (*sectionReadable)(nil)

// RunLabel is what a run is called beside its casfs hash: its LEVEL and its
// TxNum range, which is everything an operator or an agent needs to read a
// directory listing. `l0-...` is a flush, `t-...` is a terminal.
func RunLabel(level int, fromTx, toTx uint64) string {
	kind := fmt.Sprintf("l%d", level)
	if level >= TerminalLevel {
		kind = "t"
	}
	return fmt.Sprintf("%s-%016d-%016d", kind, fromTx, toTx)
}

// RunFileName is the staging name of a run under construction, in the directory
// the finished run lands in.
func RunFileName(dir string, level int, fromTx, toTx uint64) string {
	return filepath.Join(dir, "run-"+RunLabel(level, fromTx, toTx)+".tmp")
}

func hexName(b [32]byte) string { return hex.EncodeToString(b[:]) }

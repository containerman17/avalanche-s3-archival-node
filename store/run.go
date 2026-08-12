package store

import (
	"bufio"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/cockroachdb/pebble/sstable"
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
	Off  [numSections]uint64
	Len  [numSections]uint64
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

func decodeFooter(b []byte) (*Footer, error) {
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
	if f.Version != StorageVersion {
		return nil, fmt.Errorf("store: run is storage version %d, this binary is %d", f.Version, StorageVersion)
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
	path string
	f    *os.File
	bw   *bufio.Writer
	h    *dist.Hasher
	off  uint64

	footer Footer
	cur    Section
	sst    *sstable.Writer
	sec    *sectionWritable
	// counts is per-section rows and bytes, for the size report.
	Rows  [numSections]uint64
	Bytes [numSections]uint64
}

// NewRunWriter starts a run at path (which must be on the artifact store's
// filesystem, because Adopt renames it into the spool).
func NewRunWriter(path string, prev [32]byte) (*RunWriter, error) {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	w := &RunWriter{path: path, f: f, bw: bufio.NewWriterSize(f, 1<<20), h: dist.NewHasher(), cur: -1}
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
	w.sst = sstable.NewWriter(w.sec, writerOptions(s))
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

// Finish writes the footer, fsyncs, and hands the file to the artifact store,
// which renames it into the spool under the name its hash list commits to.
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
	name, err := cas.Adopt(w.path, w.h)
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
type Run struct {
	Name   RunName
	Footer *Footer

	blob   *dist.Blob
	rd     [numSections]*sstable.Reader
	filter [numSections][]byte
}

// OpenRun opens the named run out of the artifact store.
func OpenRun(cas *dist.Store, name RunName) (*Run, error) {
	blob, err := cas.Open(name)
	if err != nil {
		return nil, err
	}
	r := &Run{Name: name, blob: blob}
	if blob.Size() < uint64(footerSize) {
		blob.Close()
		return nil, fmt.Errorf("store: run %s is %d bytes, too small for a footer", name, blob.Size())
	}
	fb, err := blob.Read(blob.Size()-uint64(footerSize), uint64(footerSize))
	if err != nil {
		blob.Close()
		return nil, err
	}
	if r.Footer, err = decodeFooter(fb); err != nil {
		blob.Close()
		return nil, fmt.Errorf("store: run %s: %w", name, err)
	}
	for s := Section(0); s < numSections; s++ {
		readable, err := sstable.NewSimpleReadable(&sectionReadable{b: blob, off: r.Footer.Off[s], n: r.Footer.Len[s]})
		if err != nil {
			r.Close()
			return nil, err
		}
		rd, err := sstable.NewReader(readable, readerOptions())
		if err != nil {
			r.Close()
			return nil, fmt.Errorf("store: run %s section %v: %w", name, s, err)
		}
		r.rd[s] = rd
		// THE BLOOM IS ALWAYS RESIDENT (DESIGN, read-through): a cold state
		// read must cost local bloom probes plus exactly one remote GET, so the
		// filter block is read once here and never again. pebble's
		// sstable.Reader has no point-get, so the gate is ours to apply.
		lay, err := rd.Layout()
		if err != nil {
			r.Close()
			return nil, err
		}
		if lay.Filter.Length > 0 {
			f, err := blob.Read(r.Footer.Off[s]+lay.Filter.Offset, lay.Filter.Length)
			if err != nil {
				r.Close()
				return nil, err
			}
			r.filter[s] = f
		}
	}
	return r, nil
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

// Get is an exact-key point read.
func (r *Run) Get(s Section, key []byte) ([]byte, bool, error) {
	if !r.MayHave(s, key) {
		return nil, false, nil
	}
	it, err := r.rd[s].NewIter(nil, nil)
	if err != nil {
		return nil, false, err
	}
	defer it.Close()
	k, v := it.SeekGE(key, 0)
	if k == nil || !Comparer.Equal(k.UserKey, key) {
		return nil, false, nil
	}
	val, _, err := v.Value(nil)
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
	it, err := r.rd[s].NewIter(nil, nil)
	if err != nil {
		return nil, 0, false, err
	}
	defer it.Close()
	// SeekLT past the sought TxNum, then check we are still under the prefix.
	k, v := it.SeekLT(Suffixed(prefix, at+1), 0)
	if k == nil || len(k.UserKey) != len(prefix)+8 || string(k.UserKey[:len(prefix)]) != string(prefix) {
		return nil, 0, false, nil
	}
	raw, _, err := v.Value(nil)
	if err != nil {
		return nil, 0, false, err
	}
	return append([]byte(nil), raw...), TxNumOf(k.UserKey), true, nil
}

// Scan calls fn for every row under prefix with a TxNum suffix in [lo, hi],
// ascending. fn returning false stops the scan.
func (r *Run) Scan(s Section, prefix []byte, lo, hi uint64, fn func(txnum uint64, val []byte) bool) error {
	if !r.MayHave(s, Suffixed(prefix, 0)) {
		return nil
	}
	it, err := r.rd[s].NewIter(nil, nil)
	if err != nil {
		return err
	}
	defer it.Close()
	for k, v := it.SeekGE(Suffixed(prefix, lo), 0); k != nil; k, v = it.Next() {
		if len(k.UserKey) != len(prefix)+8 || string(k.UserKey[:len(prefix)]) != string(prefix) {
			return nil
		}
		n := TxNumOf(k.UserKey)
		if n > hi {
			return nil
		}
		raw, _, err := v.Value(nil)
		if err != nil {
			return err
		}
		if !fn(n, raw) {
			return nil
		}
	}
	return nil
}

// ScanRange calls fn for every chain-section row in [loKey, hiKey), ascending.
func (r *Run) ScanRange(s Section, loKey, hiKey []byte, fn func(key, val []byte) bool) error {
	it, err := r.rd[s].NewIter(nil, hiKey)
	if err != nil {
		return err
	}
	defer it.Close()
	for k, v := it.SeekGE(loKey, 0); k != nil; k, v = it.Next() {
		raw, _, err := v.Value(nil)
		if err != nil {
			return err
		}
		if !fn(k.UserKey, raw) {
			return nil
		}
	}
	return nil
}

// sectionReadable presents [off, off+n) of an artifact as a whole file to
// pebble's sstable reader.
type sectionReadable struct {
	b        *dist.Blob
	off, n   uint64
}

func (s *sectionReadable) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 || uint64(off) > s.n {
		return 0, io.EOF
	}
	n := uint64(len(p))
	if n > s.n-uint64(off) {
		return 0, io.EOF
	}
	b, err := s.b.Read(s.off+uint64(off), n)
	if err != nil {
		return 0, err
	}
	return copy(p, b), nil
}

func (s *sectionReadable) Close() error { return nil }

func (s *sectionReadable) Stat() (os.FileInfo, error) { return sectionStat{n: int64(s.n)}, nil }

type sectionStat struct{ n int64 }

func (s sectionStat) Name() string       { return "section" }
func (s sectionStat) Size() int64        { return s.n }
func (s sectionStat) Mode() os.FileMode  { return 0 }
func (s sectionStat) ModTime() time.Time { return time.Time{} }
func (s sectionStat) IsDir() bool        { return false }
func (s sectionStat) Sys() any           { return nil }

var _ sstable.ReadableFile = (*sectionReadable)(nil)

// RunFileName is the human-readable staging name of a run under construction:
// runs ARE named by their TxNum range, and the casfs name is what the manifest
// records once the bytes exist.
func RunFileName(dir string, fromTx, toTx uint64) string {
	return filepath.Join(dir, fmt.Sprintf("run-%016d-%016d.tmp", fromTx, toTx))
}

func hexName(b [32]byte) string { return hex.EncodeToString(b[:]) }

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
	"github.com/containerman17/avalanche-s3-archival-node/dist"
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

// CopySection writes section s as the verbatim bytes [off, off+n) of src: an
// SST section that is byte-identical between two storage versions goes through
// the hasher and onto disk without being decoded (the migrator's chain and
// state sections).
//
// src is an io.ReaderAt, not the artifact, so the caller can hand it a source
// that already holds the bytes: the migrator resides the chain section before
// it copies, and the copy then reads that RAM instead of pulling the section
// out of the bucket a second time.
func (w *RunWriter) CopySection(s Section, src io.ReaderAt, off, n uint64) error {
	if s != w.cur+1 {
		return fmt.Errorf("store: section %v out of order", s)
	}
	w.cur = s
	w.footer.Off[s], w.footer.Len[s] = w.off, n
	const piece = 4 << 20
	buf := make([]byte, min(uint64(piece), n))
	for done := uint64(0); done < n; {
		b := buf[:min(uint64(piece), n-done)]
		if _, err := src.ReadAt(b, int64(off+done)); err != nil {
			return err
		}
		if _, err := w.h.Write(b); err != nil {
			return err
		}
		if _, err := w.bw.Write(b); err != nil {
			return err
		}
		done += uint64(len(b))
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
	r, stale, err := openRunAttempt(cas, name, seed, version)
	if err != nil && seed == nil {
		// ANY failure with a sidecar present is answered by a rebuild, not
		// just the coverage check: a torn or bit-flipped block trips pebble's
		// own trailer checksum deep inside NewReader or Layout, and the
		// sidecar is derived state, so the artifact is the authority either
		// way. If the artifact itself is bad the retry fails the same way and
		// that error is the one reported.
		if _, statErr := os.Stat(residentPath(cas, name)); statErr == nil {
			stale = true
		}
	}
	if stale {
		os.Remove(residentPath(cas, name))
		r, _, err = openRunAttempt(cas, name, seed, version)
	}
	return r, err
}

func openRunAttempt(cas *dist.Store, name RunName, seed *Run, version uint32) (_ *Run, stale bool, _ error) {
	blob, err := cas.Open(name)
	if err != nil {
		return nil, false, err
	}
	r := &Run{Name: name, blob: blob}
	if blob.Size() < uint64(footerSize) {
		blob.Close()
		return nil, false, fmt.Errorf("store: run %s is %d bytes, too small for a footer", name, blob.Size())
	}
	if seed != nil {
		r.Footer, r.filter = seed.Footer, seed.filter
	} else {
		fb, err := blob.Read(blob.Size()-uint64(footerSize), uint64(footerSize))
		if err != nil {
			blob.Close()
			return nil, false, err
		}
		if r.Footer, err = decodeFooterVersion(fb, version); err != nil {
			blob.Close()
			return nil, false, fmt.Errorf("store: run %s: %w", name, err)
		}
	}
	// A sidecar, when present, IS the open (resident.go): every byte NewReader,
	// Layout and the bloom gate are about to want is already in the mapping, so
	// a warm open reads nothing from the artifact at all, and the pinned bytes
	// are file-backed: the kernel pages a cold chain's blooms out and a
	// historical query pages back exactly what it probes.
	var side [numSections][]residentRange
	var mp *resMap
	if seed == nil {
		if m, sres, err := loadSidecar(residentPath(cas, name)); err == nil {
			mp, side = m, sres
		}
	}
	fromSidecar := mp != nil
	if mp != nil {
		defer mp.drop() // the loader's reference; sections take their own
	}
	var filterBH [numSections]sstblock.Handle
	for s := Section(0); s < numSections; s++ {
		sec := &sectionReadable{b: blob, off: r.Footer.Off[s], n: r.Footer.Len[s]}
		switch {
		case seed != nil:
			// The resident ranges are a copy, never the seed's own slice: the
			// seed is still serving reads out of it until its last reference
			// goes, and reside appends.
			sec.res = append([]residentRange(nil), seed.sec[s].res...)
			sec.resident = seed.sec[s].resident
			if sec.mp = seed.sec[s].mp; sec.mp != nil {
				sec.mp.hold()
			}
		case fromSidecar:
			sec.res = side[s]
			for _, rr := range side[s] {
				sec.resident += uint64(len(rr.data))
			}
			sec.mp = mp
			mp.hold()
		}
		r.sec[s] = sec
		readable, err := sstable.NewSimpleReadable(sec)
		if err != nil {
			r.Close()
			return nil, false, err
		}
		rd, err := sstable.NewReader(context.Background(), readable, readerOptions())
		if err != nil {
			r.Close()
			return nil, false, fmt.Errorf("store: run %s section %v: %w", name, s, err)
		}
		r.rd[s] = rd
		// THE TAIL COMES DOWN BEFORE Layout, NOT AFTER IT. Layout walks the
		// top-level index and reads EVERY sub-index block one at a time, so on
		// a cold chunk cache a 12GB chain section pays a 4MB chunk per index
		// block, times three sections times every run in the manifest. The
		// resident overlay cannot help: it is still empty at this point.
		// resideTail puts the whole tail in RAM in ONE sequential read first,
		// and every read Layout and reside then make is served out of it.
		//
		// A reopen skips it: the seed already carries those exact blocks
		// resident, so its Layout reads nothing and a tail read would be pure
		// loss. A sidecar open skips it for the same reason.
		if seed == nil && !fromSidecar {
			if err := sec.resideTail(rd); err != nil {
				r.Close()
				return nil, false, fmt.Errorf("store: run %s section %v: %w", name, s, err)
			}
		}
		lay, err := rd.Layout()
		if err != nil {
			r.Close()
			return nil, false, err
		}
		// BLOOMS AND SST INDEX BLOCKS ARE ALWAYS RESIDENT LOCALLY (DESIGN,
		// read-through): a cold point read must cost local bloom probes plus
		// EXACTLY ONE remote GET, and an index block re-read from the artifact
		// would be a second one. They are read once here into the section's
		// resident overlay, which serves those byte ranges from RAM (the
		// sidecar mapping, once one exists) forever after; only data blocks
		// ever reach the blob again.
		resident := append([]sstblock.Handle{lay.TopIndex, lay.Properties, lay.MetaIndex}, lay.Index...)
		for _, nbh := range lay.Filter {
			resident = append(resident, nbh.Handle)
		}
		if fromSidecar {
			// A handle the mapping does not cover means the sidecar was some
			// other binary's layout walk: stale, rebuild (openRunVersion).
			for _, bh := range resident {
				if bh.Length > 0 && !sec.isResident(bh.Offset) {
					r.Close()
					return nil, true, fmt.Errorf("store: run %s section %v: sidecar does not cover block at %d", name, s, bh.Offset)
				}
			}
		} else {
			if err := sec.reside(resident); err != nil {
				r.Close()
				return nil, false, fmt.Errorf("store: run %s section %v: %w", name, s, err)
			}
			// THE FOOTER GAP RIDES ALONG. On the next open NewReader reads the
			// pebble footer before any of the handles above are known, so the
			// bytes after the last pinned handle go into the overlay too, or a
			// sidecar open would pay one artifact read per section right
			// there. Served out of the tail here, so it costs nothing now.
			if seed == nil && sec.n > 0 {
				var end uint64
				for _, bh := range resident {
					if bh.Length > 0 {
						end = max(end, bh.Offset+bh.Length+blockTrailer)
					}
				}
				if end < sec.n {
					gap, err := sec.read(end, sec.n-end)
					if err != nil {
						r.Close()
						return nil, false, err
					}
					sec.insert(end, append([]byte(nil), gap...))
				}
			}
		}
		if seed == nil && len(lay.Filter) > 0 && lay.Filter[0].Length > 0 {
			filterBH[s] = lay.Filter[0].Handle
		}
		// The tail was scaffolding for the steps above and nothing else.
		// It is per section and per run, and a node opens hundreds of runs, so
		// holding it would be the resident budget several times over. The exact
		// handles keep their own copies; only the bridged bytes go.
		sec.dropTail()
	}
	// The build path persists what it just pinned and re-points the overlay at
	// the mapping, so the heap copies are garbage the moment this returns. A
	// failure anywhere here keeps the heap overlay, which is exactly the
	// pre-sidecar behavior: correct and merely anonymous.
	if seed == nil && !fromSidecar {
		path := residentPath(cas, name)
		if err := writeSidecar(path, r.sec); err == nil {
			if m, sres, err := loadSidecar(path); err == nil {
				for s := Section(0); s < numSections; s++ {
					sec := r.sec[s]
					sec.res, sec.resident = sres[s], 0
					for _, rr := range sres[s] {
						sec.resident += uint64(len(rr.data))
					}
					sec.mp = m
					m.hold()
				}
				m.drop()
			}
		}
	}
	// pebble's sstable.Reader has no point-get, so the bloom gate is ours to
	// apply: keep the filter bytes in hand for MayHave. It ALIASES the overlay
	// (a copy held every bloom twice, 11.4GB on mainnet C), and it aliases
	// AFTER the swap above so it points into the mapping, not at heap the swap
	// just orphaned.
	if seed == nil {
		for s := Section(0); s < numSections; s++ {
			if bh := filterBH[s]; bh.Length > 0 {
				f := r.sec[s].residentData(bh.Offset)
				if f == nil {
					r.Close()
					return nil, false, fmt.Errorf("store: run %s section %v: filter block not resident", name, s)
				}
				// THE BLOOM IS THE ONE BLOCK NOBODY ELSE CHECKS: index and
				// meta blocks go through pebble's reader, which validates
				// every trailer, but the filter is probed raw. A zeroed or
				// bit-flipped bloom answers "absent" for present keys, which
				// is the one silent wrong answer this file can produce, so
				// its trailer is validated here once. From a sidecar that is
				// a torn file and openRunVersion rebuilds; from the artifact
				// it is corruption and the error stands.
				if err := sstblock.ValidateChecksum(sstblock.ChecksumTypeCRC32c, f, bh); err != nil {
					r.Close()
					return nil, false, fmt.Errorf("store: run %s section %v: filter block: %w", name, s, err)
				}
				r.filter[s] = f[:bh.Length]
			}
		}
	}
	return r, false, nil
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
	for _, sc := range r.sec {
		if sc != nil {
			sc.mp.drop()
			sc.mp = nil
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
	// tail is the TRANSIENT window resideTail holds while the section is being
	// opened, and it is deliberately NOT part of res: it overlaps the ranges
	// reside pins inside it, and res is a "last range at or before the offset"
	// lookup that overlap would break. It costs no resident budget because it
	// is gone before openRunVersion returns, and it needs no lock because open
	// is the only thing holding the section then.
	tail residentRange
	// mp is the sidecar mapping res aliases when the overlay is file-backed
	// (see resident.go). nil means the overlay is heap bytes. Refcounted
	// because a reopen's sections share the seed's mapping.
	mp *resMap
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

// maxResideGap is how far reside will read ACROSS a hole to fold two blocks
// into one artifact read. One chunk: bridging a gap that small costs at most
// the chunk the next block already sits in, so the read is never bigger than
// the fetch a separate read would have done anyway.
const maxResideGap = uint64(dist.ChunkSize)

// reside pins a batch of block handles in RAM, with as few artifact reads as
// the layout allows.
//
// ONE READ PER HANDLE IS THE OPEN PATH'S AMPLIFICATION: a miss in the chunk
// cache pulls a whole dist.ChunkSize chunk, so a section whose index and
// filter blocks are a few KB each cost a 4MB fetch EACH, times three sections
// times every run in the manifest. Measured on mainnet C: `store.Open` over a
// 197-run corpus sat for half an hour and pulled 131GB.
//
// The blocks are clustered by construction: pebble writes the filter, the
// index, the top-level index, the properties and the metaindex one after the
// other at the tail of the section, so a real corpus has ONE span with gaps of
// a block trailer between the ranges. Sorting them and reading each coalesced
// span once turns those fetches into one sequential pass over the tail.
//
// Only the ranges themselves are kept, never the bytes bridged across a gap,
// so the resident budget is exactly what it was when this read one block at a
// time.
func (s *sectionReadable) reside(bhs []sstblock.Handle) error {
	want := make([]blockRange, 0, len(bhs))
	for _, bh := range bhs {
		if bh.Length == 0 {
			continue
		}
		n := bh.Length + blockTrailer
		if bh.Offset+n > s.n {
			return fmt.Errorf("block [%d,%d) is outside a %d byte section", bh.Offset, bh.Offset+n, s.n)
		}
		if s.isResident(bh.Offset) { // a seed carried it across: the reopen reads nothing
			continue
		}
		want = append(want, blockRange{bh.Offset, n})
	}
	sort.Slice(want, func(i, j int) bool { return want[i].off < want[j].off })
	i := 0
	for _, j := range resideSpans(want) {
		lo, hi := want[i].off, uint64(0)
		for _, r := range want[i:j] {
			hi = max(hi, r.off+r.n)
		}
		buf, err := s.read(lo, hi-lo)
		if err != nil {
			return err
		}
		for _, r := range want[i:j] {
			// The slice is copied, not aliased: buf holds the bytes bridged
			// across the gaps too, and nothing should keep them alive.
			s.insert(r.off, append([]byte(nil), buf[r.off-lo:r.off-lo+r.n]...))
		}
		i = j
	}
	return nil
}

// blockRange is one block's [off, off+n) inside a section, trailer included.
type blockRange struct{ off, n uint64 }

// resideSpans splits ranges SORTED BY OFFSET into the spans reside reads, one
// artifact read each, and returns the exclusive end index of every span. A new
// span starts where the hole in front of a range is wider than maxResideGap,
// which is what keeps two blocks at opposite ends of a 12GB section from
// turning into a 12GB read.
func resideSpans(want []blockRange) []int {
	var ends []int
	var end uint64
	for i, r := range want {
		if i > 0 && r.off > end+maxResideGap {
			ends = append(ends, i)
		}
		end = max(end, r.off+r.n)
	}
	if len(want) > 0 {
		ends = append(ends, len(want))
	}
	return ends
}

func (s *sectionReadable) isResident(off uint64) bool {
	return s.residentData(off) != nil
}

// residentData returns the pinned bytes of the range registered at exactly
// off (trailer included), or nil when there is none.
func (s *sectionReadable) residentData(off uint64) []byte {
	i := sort.Search(len(s.res), func(i int) bool { return s.res[i].off >= off })
	if i < len(s.res) && s.res[i].off == off {
		return s.res[i].data
	}
	return nil
}

// insert adds one resident range, keeping s.res sorted by offset. A range
// already registered at that offset stays: a run is immutable, so it is the
// same bytes.
func (s *sectionReadable) insert(off uint64, data []byte) {
	i := sort.Search(len(s.res), func(i int) bool { return s.res[i].off >= off })
	if i < len(s.res) && s.res[i].off == off {
		return
	}
	s.res = append(s.res, residentRange{})
	copy(s.res[i+1:], s.res[i:])
	s.res[i] = residentRange{off: off, data: data}
	s.resident += uint64(len(data))
}

// resideAll pulls the WHOLE section into RAM with CopySection's access pattern
// (sequential 4MB pieces, chunk-aligned) and serves every later read from it.
//
// It is for a caller that ITERATES a section row by row while the chunk cache
// is evicting under it. A walk of the lookup section reads 8KB blocks, one
// dist.ChunkSize (4MB) fetch each on a miss, so on a box whose cache sits at
// its min-free floor a 920MB section costs hundreds of GB of S3 traffic: 250x
// to 500x amplification, measured on a v2 -> v3 migration.
//
// The single range covers the section, so it REPLACES the index and filter
// ranges instead of joining them: ReadAt takes the last range starting at or
// before the read, and a short range sitting inside this one would shadow it.
func (s *sectionReadable) resideAll() error {
	if s.n == 0 {
		return nil
	}
	buf := make([]byte, s.n)
	const piece = 4 << 20
	for done := uint64(0); done < s.n; {
		k := min(uint64(piece), s.n-done)
		b, err := s.b.Read(s.off+done, k)
		if err != nil {
			return err
		}
		copy(buf[done:], b)
		done += k
	}
	s.res = []residentRange{{off: 0, data: buf}}
	s.resident = s.n
	// The single heap range replaced whatever aliased the sidecar mapping.
	s.mp.drop()
	s.mp = nil
	return nil
}

// resideTail pulls the section's TAIL into RAM in one sequential read, for the
// length of the open and not one instant longer (dropTail).
//
// EVERY BLOCK THAT IS NOT A DATA BLOCK LIVES THERE. The writer emits the data
// blocks, then the filter, then the index partitions, then the top-level index,
// then the properties, the metaindex and the footer, so the tail is exactly
// what `sstable.Reader` reads at open: Layout's walk of every sub-index block,
// and the exact handles reside pins afterwards. One read covers all of it.
//
// The window is not a guess: `rocksdb.data.size` is the offset the writer
// stamped when the last data block was done, which IS the first tail byte. It
// costs one small read of the properties block, which sits in the chunk the
// reader's footer read has already brought down.
//
// A section whose properties do not say (a foreign or empty SST) simply gets no
// window: Layout still reads correctly, one block at a time, as it did before.
func (s *sectionReadable) resideTail(rd *sstable.Reader) error {
	props, err := rd.ReadPropertiesBlock(context.Background(), nil)
	if err != nil {
		return err
	}
	off := props.DataSize
	if off == 0 || off >= s.n {
		return nil
	}
	buf, err := s.b.Read(s.off+off, s.n-off)
	if err != nil {
		return err
	}
	s.tail = residentRange{off: off, data: buf}
	return nil
}

// dropTail frees the open-time window. What reside pinned out of it stays.
func (s *sectionReadable) dropTail() { s.tail = residentRange{} }

// read is the one place a section goes for bytes it was asked for: the
// open-time tail if that covers them, the artifact otherwise. reside goes
// through it too, so the handles it pins are copied out of the tail instead of
// fetched a second time.
func (s *sectionReadable) read(off, n uint64) ([]byte, error) {
	if t := s.tail; t.data != nil && off >= t.off && off+n <= t.off+uint64(len(t.data)) {
		return t.data[off-t.off : off-t.off+n], nil
	}
	return s.b.Read(s.off+off, n)
}

// release drops the resident overlay. Reads fall back to the artifact, which is
// correct and only slower: it is for a caller that walked the section once and
// is done with it.
func (s *sectionReadable) release() {
	s.res, s.resident = nil, 0
	s.mp.drop()
	s.mp = nil
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
	b, err := s.read(uint64(off), n)
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

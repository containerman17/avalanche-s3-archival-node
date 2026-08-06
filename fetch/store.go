package fetch

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ava-labs/avalanchego/ids"
	"github.com/klauspost/compress/zstd"
)

// Store is an append-only flat-file container store. No database.
//
// History is height-bucketed into segments of SegmentBlocks blocks each. The
// container for height H lives in arrival_<H/SegmentBlocks, %05d>.log with a
// matching sidecar index_<bucket>.log. The destination file is a pure
// function of height, independent of arrival order. The filenames ARE the
// catalogue: deleting a segment is rm of both files of the bucket.
//
//	arrival_NNNNN.log: [uvarint compressedLen][zstd frame]... in arrival
//	                   order; each frame is one container compressed
//	                   standalone (no dict, default level).
//	index_NNNNN.log:   fixed 84-byte records, one per container, appended as
//	                   containers land: height(8 BE) containerID(32)
//	                   blockHash(32) offset(8 BE, into the segment's arrival
//	                   file at the zstd frame) len(4 BE, compressed).
//
// Startup rebuilds the RAM maps from a sequential scan of every index
// sidecar. If a sidecar's tail points past the end of its arrival file
// (torn write), that segment pair is truncated back to the last consistent
// record.
type Store struct {
	// ponytail: one mutex for maps, file handles, and I/O; appends are
	// network-bound and reads are a map hit + one ReadAt. Split locks if a
	// profiler ever blames this.
	mu   sync.Mutex
	dir  string
	segs map[uint64]*segment // bucket -> open segment pair

	byHeight map[uint64]heightRec
	byID     map[ids.ID]uint64 // containerID -> height
	// idxUse is the LRU clock for byHeight, one entry per bucket whose records
	// are currently cached. byHeight is bounded through it; byID is NOT and is
	// deliberately complete (see dropIndex).
	idxUse  map[uint64]uint64 // bucket -> last use
	idxTick uint64
	head    uint64
	haveAny bool

	enc *zstd.Encoder
	dec *zstd.Decoder

	// staged is StagedBytes: the on-disk size of every bucket the seal has not
	// retired. Mutated under mu, read lock-free, because the walk's ceiling
	// check reads it per block.
	staged      atomic.Int64
	bucketBytes map[uint64]int64 // bucket -> its share of staged

	useCounter      uint64
	sessionBytes    atomic.Uint64 // compressed + index bytes written
	sessionRawBytes atomic.Uint64 // container bytes before compression
}

type segment struct {
	arrival    *os.File
	index      *os.File
	arrivalOff uint64
	dirty      bool
	lastUse    uint64
}

type heightRec struct {
	id  ids.ID
	off uint64 // into the segment's arrival file, at the container bytes
	ln  uint32
}

const (
	// SegmentBlocks is the height width of one segment pair. It is also the
	// future epoch size: segments and epochs are 1:1 by design.
	SegmentBlocks = 100_000

	indexRecSize = 8 + 32 + 32 + 8 + 4 // 84

	// maxOpenSegments caps open file handles; the backward walk touches 1-2
	// buckets at a time, least-recently-used segments are flushed and closed.
	maxOpenSegments = 8
)

func arrivalName(bucket uint64) string { return fmt.Sprintf("arrival_%05d.log", bucket) }
func indexName(bucket uint64) string   { return fmt.Sprintf("index_%05d.log", bucket) }

// BlockEvent is a single "block is present locally" notification emitted by
// Subscribe. Raw is a fresh copy safe to retain.
//
// Err is the LAST event of a stream that ended badly, and it is the only one
// with no block in it: the channel closes on a read failure exactly as it
// closes on cancellation, so without this a corrupt container ends the block
// stream and reads as completion. A consumer must check it.
type BlockEvent struct {
	BlockNumber uint64
	ContainerID ids.ID
	Raw         []byte
	Err         error
}

// OpenStore opens the segment files in dir and rebuilds the RAM index from
// every index_*.log sidecar, applying the torn-tail truncation rule per
// segment pair.
func OpenStore(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	enc, err := zstd.NewWriter(nil)
	if err != nil {
		return nil, err
	}
	dec, err := zstd.NewReader(nil)
	if err != nil {
		return nil, err
	}
	s := &Store{
		dir:         dir,
		segs:        make(map[uint64]*segment),
		byHeight:    make(map[uint64]heightRec),
		byID:        make(map[ids.ID]uint64),
		bucketBytes: make(map[uint64]int64),
		enc:         enc,
		dec:         dec,
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		var bucket uint64
		if _, err := fmt.Sscanf(e.Name(), "index_%d.log", &bucket); err != nil {
			continue
		}
		if err := s.rebuildSegment(bucket); err != nil {
			s.Close()
			return nil, fmt.Errorf("rebuild segment %04d: %w", bucket, err)
		}
	}
	return s, nil
}

// rebuildSegment scans one index sidecar, populates the RAM maps, and
// truncates the pair back to the last record whose container bytes fully
// fit in the segment's arrival file.
func (s *Store) rebuildSegment(bucket uint64) error {
	index, err := os.OpenFile(filepath.Join(s.dir, indexName(bucket)), os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	defer index.Close()
	// NOT O_CREATE. seg() writes the arrival file before the index and Retire
	// deletes both, so an index sidecar with no arrival file is damage, not a
	// state this store can be in. Creating one here would hand the scan an
	// arrivalSize of 0 and truncate a whole segment's index away on the spot.
	arrival, err := os.OpenFile(filepath.Join(s.dir, arrivalName(bucket)), os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	defer arrival.Close()
	ast, err := arrival.Stat()
	if err != nil {
		return err
	}
	arrivalSize := uint64(ast.Size())

	var (
		rec     [indexRecSize]byte
		good    int64
		lastEnd uint64
	)
	for {
		if _, err := io.ReadFull(index, rec[:]); err != nil {
			// io.EOF is a clean end and ErrUnexpectedEOF is a torn tail
			// record, which the truncation below drops. ANYTHING ELSE IS A
			// READ FAILURE: an EIO taking this branch used to truncate both
			// files to the last record read, i.e. permanently delete the rest
			// of a 100k-block segment because a sector was unreadable once.
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}
			return fmt.Errorf("read index record %d: %w", good, err)
		}
		height := binary.BigEndian.Uint64(rec[0:8])
		var id ids.ID
		copy(id[:], rec[8:40])
		// rec[40:72] is the eth block hash: stored for future consumers,
		// not needed in RAM.
		off := binary.BigEndian.Uint64(rec[72:80])
		ln := binary.BigEndian.Uint32(rec[80:84])
		if off+uint64(ln) > arrivalSize {
			break // torn: container bytes never made it to the arrival file
		}
		s.byHeight[height] = heightRec{id: id, off: off, ln: ln}
		s.byID[id] = height
		s.touchIndexBucket(height / SegmentBlocks)
		if !s.haveAny || height > s.head {
			s.head = height
			s.haveAny = true
		}
		good++
		lastEnd = off + uint64(ln)
	}
	if err := index.Truncate(good * indexRecSize); err != nil {
		return err
	}
	if err := arrival.Truncate(int64(lastEnd)); err != nil {
		return err
	}
	// The retained size of this bucket AFTER the torn-tail truncation, which is
	// what is actually on disk. A restart therefore starts with the true staging
	// figure instead of counting only what this session appends.
	s.addBucketBytes(bucket, int64(lastEnd)+good*indexRecSize)
	return nil
}

// addBucketBytes folds a bucket's on-disk delta into the staging total. Caller
// holds s.mu (or is still building the store).
func (s *Store) addBucketBytes(bucket uint64, delta int64) {
	s.bucketBytes[bucket] += delta
	s.staged.Add(delta)
}

// seg returns the open segment pair for bucket, opening it (and LRU-closing
// the least recently used one over the cap) if needed. Caller holds s.mu.
//
// create=false never brings a segment BACK: a segment the seal retired is
// gone, and
// recreating it here would put an empty arrival file where history used to be
// and turn every read of that range into an EOF instead of an epoch fallback
// (the bug state/bucketlog.go's pair() already carries this rule for).
// nil, nil means the segment is not here.
func (s *Store) seg(bucket uint64, create bool) (*segment, error) {
	s.useCounter++
	if sg, ok := s.segs[bucket]; ok {
		sg.lastUse = s.useCounter
		return sg, nil
	}
	for len(s.segs) >= maxOpenSegments {
		var (
			oldest    uint64
			oldestUse = ^uint64(0)
		)
		for b, sg := range s.segs {
			if sg.lastUse < oldestUse {
				oldest, oldestUse = b, sg.lastUse
			}
		}
		if err := s.closeSegment(oldest); err != nil {
			return nil, err
		}
	}
	flags := os.O_RDWR
	idxFlags := os.O_WRONLY | os.O_APPEND
	if create {
		flags |= os.O_CREATE
		idxFlags |= os.O_CREATE
	}
	arrival, err := os.OpenFile(filepath.Join(s.dir, arrivalName(bucket)), flags, 0o644)
	if os.IsNotExist(err) {
		return nil, nil // retired segment: not an error, just not here
	}
	if err != nil {
		return nil, err
	}
	index, err := os.OpenFile(filepath.Join(s.dir, indexName(bucket)), idxFlags, 0o644)
	if os.IsNotExist(err) {
		arrival.Close()
		return nil, nil
	}
	if err != nil {
		arrival.Close()
		return nil, err
	}
	off, err := arrival.Seek(0, io.SeekEnd)
	if err != nil {
		arrival.Close()
		index.Close()
		return nil, err
	}
	sg := &segment{arrival: arrival, index: index, arrivalOff: uint64(off), lastUse: s.useCounter}
	s.segs[bucket] = sg
	return sg, nil
}

// closeSegment flushes (if dirty) and closes one open segment. Caller holds s.mu.
func (s *Store) closeSegment(bucket uint64) error {
	sg := s.segs[bucket]
	if sg.dirty {
		if err := sg.flush(); err != nil {
			return err
		}
	}
	delete(s.segs, bucket)
	if err := sg.arrival.Close(); err != nil {
		sg.index.Close()
		return err
	}
	return sg.index.Close()
}

// resync repairs a segment's write state after a failed write, and returns the
// error that caused it.
//
// NEITHER FILE IS SAFE TO LEAVE ALONE. The arrival file is opened without
// O_APPEND, so Write moves the descriptor by whatever it managed to put down
// while the cached arrivalOff does not move at all: every later record in that
// segment would then be indexed short by the failed record's length and decode
// as garbage or into a neighbouring frame. A torn index record is the mirror
// image: the next record is appended after it and misaligns the whole sidecar,
// so it is truncated back to a record boundary, which is the same rule the
// startup scan applies.
func (sg *segment) resync(cause error) error {
	off, err := sg.arrival.Seek(0, io.SeekCurrent)
	if err != nil {
		return errors.Join(cause, fmt.Errorf("arrival offset unreadable: %w", err))
	}
	sg.arrivalOff = uint64(off)
	fi, err := sg.index.Stat()
	if err != nil {
		return errors.Join(cause, fmt.Errorf("index size unreadable: %w", err))
	}
	if n := fi.Size() % indexRecSize; n != 0 {
		if err := sg.index.Truncate(fi.Size() - n); err != nil {
			return errors.Join(cause, fmt.Errorf("index realign: %w", err))
		}
	}
	return cause
}

// flush fsyncs the arrival file before the index so a crash can only leave
// the index behind the data, which the torn-tail rule already handles.
func (sg *segment) flush() error {
	if err := sg.arrival.Sync(); err != nil {
		return err
	}
	if err := sg.index.Sync(); err != nil {
		return err
	}
	sg.dirty = false
	return nil
}

// Append stores one container in its height bucket. Duplicate container IDs
// are ignored. Writes are unbuffered but not fsynced; call Flush after a batch.
func (s *Store) Append(p parsedContainer, raw []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, dup := s.byID[p.containerID]; dup {
		return nil
	}
	sg, err := s.seg(p.blockNumber/SegmentBlocks, true)
	if err != nil {
		return err
	}

	frame := s.enc.EncodeAll(raw, nil)
	var lenBuf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(lenBuf[:], uint64(len(frame)))
	off := sg.arrivalOff + uint64(n) // offset of the zstd frame

	buf := make([]byte, 0, n+len(frame))
	buf = append(buf, lenBuf[:n]...)
	buf = append(buf, frame...)
	if _, err := sg.arrival.Write(buf); err != nil {
		return sg.resync(fmt.Errorf("arrival write: %w", err))
	}

	var rec [indexRecSize]byte
	binary.BigEndian.PutUint64(rec[0:8], p.blockNumber)
	copy(rec[8:40], p.containerID[:])
	copy(rec[40:72], p.blockHash[:])
	binary.BigEndian.PutUint64(rec[72:80], off)
	binary.BigEndian.PutUint32(rec[80:84], uint32(len(frame)))
	if _, err := sg.index.Write(rec[:]); err != nil {
		return sg.resync(fmt.Errorf("index write: %w", err))
	}

	sg.arrivalOff = off + uint64(len(frame))
	sg.dirty = true
	if old, ok := s.byHeight[p.blockNumber]; ok && old.id != p.containerID {
		// Two containers claiming one height: a reorg the walk has not caught
		// up with, or a peer fabricating. Last writer still wins (the index
		// sidecar is append-only and the newer record is the one a rebuild
		// keeps), but it is no longer silent.
		log.Printf("fetch: height %d reassigned from container %s to %s", p.blockNumber, old.id, p.containerID)
	}
	s.byHeight[p.blockNumber] = heightRec{id: p.containerID, off: off, ln: uint32(len(frame))}
	s.byID[p.containerID] = p.blockNumber
	s.touchIndexBucket(p.blockNumber / SegmentBlocks)
	if !s.haveAny || p.blockNumber > s.head {
		s.head = p.blockNumber
		s.haveAny = true
	}
	s.sessionBytes.Add(uint64(len(buf) + indexRecSize))
	s.sessionRawBytes.Add(uint64(len(raw)))
	s.addBucketBytes(p.blockNumber/SegmentBlocks, int64(len(buf)+indexRecSize))
	return nil
}

// Flush fsyncs every dirty open segment.
func (s *Store) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, sg := range s.segs {
		if !sg.dirty {
			continue
		}
		if err := sg.flush(); err != nil {
			return err
		}
	}
	return nil
}

// Close flushes and closes every open segment.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var firstErr error
	for bucket := range s.segs {
		if err := s.closeSegment(bucket); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// Has reports whether a container with this ID is stored.
func (s *Store) Has(id ids.ID) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.byID[id]
	return ok
}

// HeightOf returns the block height of a stored container ID.
func (s *Store) HeightOf(id ids.ID) (uint64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	h, ok := s.byID[id]
	return h, ok
}

// GetByHeight returns a copy of the raw container stored at height n.
// ok=false means nothing is stored at that height.
func (s *Store) GetByHeight(n uint64) ([]byte, bool, error) {
	raw, _, ok, err := s.readAt(n)
	return raw, ok, err
}

func (s *Store) readAt(n uint64) ([]byte, ids.ID, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.byHeight[n]
	if !ok {
		// A MISS IS A CACHE MISS, NOT AN ANSWER. byHeight is no longer the only
		// record of what is staged: dropIndex retires whole buckets' worth of it
		// to keep the map from growing with the fetch head, and the index
		// sidecar on disk still holds every record. Re-read that bucket and
		// retry once. A RETIRED bucket has no sidecar to read, so this still
		// reports "not here" for sealed history, which is the answer that
		// matters. Without this fallback, freeing memory would silently turn
		// stored blocks into missing ones.
		if err := s.loadBucketIndex(n / SegmentBlocks); err != nil {
			return nil, ids.Empty, false, err
		}
		if rec, ok = s.byHeight[n]; !ok {
			return nil, ids.Empty, false, nil
		}
	}
	sg, err := s.seg(n/SegmentBlocks, false)
	if err != nil {
		return nil, ids.Empty, false, err
	}
	if sg == nil {
		// Segment retired under us by the in-process seal: the sealed epoch
		// answers this height now, so drop the stale index entry and say
		// "not here" rather than resurrecting an empty file.
		delete(s.byHeight, n)
		return nil, ids.Empty, false, nil
	}
	frame := make([]byte, rec.ln)
	if _, err := sg.arrival.ReadAt(frame, int64(rec.off)); err != nil {
		return nil, ids.Empty, false, fmt.Errorf("read container at height %d: %w", n, err)
	}
	raw, err := s.dec.DecodeAll(frame, nil)
	if err != nil {
		return nil, ids.Empty, false, fmt.Errorf("decompress container at height %d: %w", n, err)
	}
	return raw, rec.id, true, nil
}

// loadBucketIndex repopulates byHeight for one bucket from its index sidecar,
// which is the durable copy of exactly what the map caches. Called only on a
// miss, so a warm bucket costs nothing.
//
// A MISSING SIDECAR IS NOT AN ERROR: that is a bucket the seal retired and
// unlinked, and "no entries" is the correct answer for it. Only a real I/O
// failure is reported, because silently treating an unreadable disk as an empty
// bucket is how a walk decides history it already has is missing and re-fetches
// it (recovery_scans_must_stop_only_on_a_clean_eof).
//
// Records are fixed width and validated against the arrival file's length the
// same way rebuild does it, so a torn tail is skipped rather than trusted.
// Caller holds s.mu.
// maxIndexBuckets caps how many buckets' worth of by-height records stay in
// RAM. The walk descends one bucket at a time and the executor does not use
// this map at all (it reads staging through fetch.Reader, which has its own
// LRU), so a handful is plenty; it mirrors maxOpenSegments for the same reason.
//
// THIS IS THE BOUND THE STAGING CEILING NEVER WAS. That ceiling limits BYTES ON
// DISK and says nothing about RAM, so a walk 15:1 ahead of the executor grew
// byHeight with the FETCH head: measured 6.7-8.2GB on mainnet C at 58.5M staged
// blocks, on a box where the Firewood node cache it competes with is 8GB and
// cache misses are what bound throughput.
//
// Safe only because of three things that had to land first: byHeight is a cache
// over the index sidecar so a miss re-reads rather than lying (c8ad0f3),
// LowestContiguous scans at most one bucket so an eviction cannot make it
// stream history off disk (19977bf), and nothing iterates byHeight.
const maxIndexBuckets = 8

// touchIndexBucket marks a bucket used and evicts the least recently used one
// once the cache is over its bound. Caller holds s.mu.
func (s *Store) touchIndexBucket(bucket uint64) {
	if s.idxUse == nil {
		s.idxUse = make(map[uint64]uint64)
	}
	s.idxTick++
	s.idxUse[bucket] = s.idxTick
	for len(s.idxUse) > maxIndexBuckets {
		var (
			oldest    uint64
			oldestUse = ^uint64(0)
		)
		for b, use := range s.idxUse {
			if use < oldestUse {
				oldest, oldestUse = b, use
			}
		}
		if oldest == bucket {
			return // never evict what we just populated
		}
		s.dropIndex(oldest)
	}
}

func (s *Store) loadBucketIndex(bucket uint64) error {
	index, err := os.Open(filepath.Join(s.dir, indexName(bucket)))
	if err != nil {
		if os.IsNotExist(err) {
			return nil // retired, or never written
		}
		return fmt.Errorf("open index for bucket %d: %w", bucket, err)
	}
	defer index.Close()
	ast, err := os.Stat(filepath.Join(s.dir, arrivalName(bucket)))
	if err != nil {
		if os.IsNotExist(err) {
			return nil // index without arrival: same as retired
		}
		return fmt.Errorf("stat arrival for bucket %d: %w", bucket, err)
	}
	arrivalSize := uint64(ast.Size())

	buf := make([]byte, indexRecSize)
	for off := int64(0); ; off += indexRecSize {
		if _, err := index.ReadAt(buf, off); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return nil
			}
			return fmt.Errorf("read index record for bucket %d: %w", bucket, err)
		}
		height := binary.BigEndian.Uint64(buf[0:8])
		var id ids.ID
		copy(id[:], buf[8:40])
		recOff := binary.BigEndian.Uint64(buf[72:80])
		ln := binary.BigEndian.Uint32(buf[80:84])
		if recOff+uint64(ln) > arrivalSize {
			return nil // torn tail: the container bytes never landed
		}
		s.byHeight[height] = heightRec{id: id, off: recOff, ln: ln}
		s.touchIndexBucket(bucket)
	}
}

// Retire drops this store's handles on the staging segments a seal has just
// deleted. Same reason as state.Store.RetireBuckets: the arrival log is the
// biggest raw family there is, and an unlinked file this process still holds
// open frees no disk at all.
//
// THE CALLER MUST RAISE THE FETCH FLOOR TO sealedEnd FIRST. Retire now also
// drops the by-height index of every retired bucket (see dropIndex), so after
// it returns this store can no longer answer for a height at or below
// sealedEnd, while byID still reports that the container exists. Only the floor
// keeps a walk from taking the "I have this stored" branch and then finding no
// raw behind it. serve does the two on adjacent lines, floor first.
func (s *Store) Retire(sealedEnd uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var firstErr error
	for b := range s.segs {
		if (b+1)*SegmentBlocks-1 > sealedEnd {
			continue
		}
		if err := s.closeSegment(b); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	// And the accounting, over every known bucket rather than only the open
	// ones: the seal has just unlinked these files, so their bytes are the
	// space that came back and StagedBytes must stop counting them.
	for b, n := range s.bucketBytes {
		if (b+1)*SegmentBlocks-1 > sealedEnd {
			continue
		}
		s.staged.Add(-n)
		delete(s.bucketBytes, b)
		s.dropIndex(b)
	}
	return firstErr
}

// dropIndex forgets the BY-HEIGHT index of one retired bucket.
//
// byHeight grew with the FETCH head and nothing ever shrank it: the only
// eviction was readAt's one-entry lazy delete, which fires solely if a read
// happens to land on a retired segment, and for a backfill walking DOWN that
// read never comes. Measured on mainnet C at 58.5M staged blocks byHeight held
// ~6.7-8.2GB of live heap, which is the whole Firewood node cache again, and
// Firewood misses are what actually bound throughput. A height below the sealed
// end answers nothing here: its segment is unlinked, so readAt already returns
// "not here" for it. This just stops waiting for a read that never comes.
//
// byID IS DELIBERATELY KEPT. It is the smaller map and it is the ONLY local
// record that a container ID sits inside sealed history: ResolveCheckpoints
// asks HeightOf so it can skip a sealed checkpoint WITHOUT a network fetch,
// which is the 2026-08-04 mainnet fix (TestResolveCheckpointsSkipsSealedHistory).
// Dropping it too would trade that fix for the smaller half of the memory.
//
// This bounds GROWTH, it does not hand memory back today: Go never returns a
// map's table to the allocator, so the win is that the freed slots absorb the
// next buckets instead of the map expanding again. Reclaiming the peak needs
// the index to be bucket-scoped and LRU-bounded, the way fetch/reader.go
// already does it for readers.
//
// Scoped to the retired bucket's own height range, NOT a scan of byHeight: at
// 58.5M entries a full scan per seal would cost seconds under s.mu, which the
// walk takes for every block.
func (s *Store) dropIndex(bucket uint64) {
	lo := bucket * SegmentBlocks
	for h := lo; h < lo+SegmentBlocks; h++ {
		delete(s.byHeight, h)
	}
	delete(s.idxUse, bucket)
}

// StagedBytes is the raw staging RETAINED ON DISK right now: the arrival and
// index files of every bucket the seal has not yet retired.
//
// RETAINED, NOT FETCHED-MINUS-EXECUTED. Sealing is what actually returns the
// space, and it lags execution by up to a whole epoch, so the executed point
// says nothing about how full the disk is. This counter only falls when files
// are unlinked (Retire), which is the same event.
//
// ARRIVAL IS THE ONLY RAW FAMILY THAT CAN RUN AWAY, which is why bounding it
// bounds the disk: writelog/headers/logs/rcpt are the EXECUTOR's output, so
// they only ever span exec head minus sealed end, which the seal holds inside
// an epoch, while the arrival log spans FETCH head minus sealed end, which is
// the 15:1 gap between the two.
//
// Kept incrementally because the walk reads it before every block; a stat over
// hundreds of segment files per block would not survive that.
func (s *Store) StagedBytes() uint64 {
	n := s.staged.Load()
	if n < 0 {
		return 0
	}
	return uint64(n)
}

// Head returns the highest stored block height, ok=false if empty.
func (s *Store) Head() (uint64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.head, s.haveAny
}

// LowestContiguous walks down from height `from` (which must be stored) and
// returns the lowest height of the contiguous stored run containing it,
// never scanning below floor (callers that only care whether the run
// reaches their floor pass it to avoid an O(history) scan).
//
// THE RESULT IS A SHORTCUT, NOT AN ANSWER, so it is allowed to stop early and
// the scan is bounded to one bucket. walkSpan uses it to skip past stored
// history: it reads the block at the returned height, takes that block's
// parent, and short-circuits again on the next turn of the loop. Returning a
// height ABOVE the true lowest therefore costs one extra block read per bucket
// and nothing else.
//
// Unbounded it was O(stored history) UNDER s.mu: on mainnet C, ~46M map lookups
// in one call, seconds during which every Append and every read blocked. It is
// also what made bounding byHeight impossible, since a bounded index would have
// had to stream every bucket off disk to answer one call.
//
// Stopping early is also the right behaviour now that byHeight is a cache with
// holes (dropIndex): a miss that is really a retired bucket and a miss that is
// really a gap both mean "do not claim this run goes further", which is the
// conservative direction.
const maxContiguousScan = SegmentBlocks

func (s *Store) LowestContiguous(from, floor uint64) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	h := from
	for budget := maxContiguousScan; h > floor && budget > 0; budget-- {
		if _, ok := s.byHeight[h-1]; !ok {
			break
		}
		h--
	}
	return h
}

// Count returns the number of stored containers.
func (s *Store) Count() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return uint64(len(s.byID))
}

// SessionBytes returns bytes written (compressed arrival + index) since open.
func (s *Store) SessionBytes() uint64 { return s.sessionBytes.Load() }

// SessionRawBytes returns pre-compression container bytes appended since open.
func (s *Store) SessionRawBytes() uint64 { return s.sessionRawBytes.Load() }

// Subscribe emits stored blocks in ascending height order starting at
// fromBlock. The channel stays open across gaps: heights not yet stored are
// polled until they land. Closed when ctx is canceled, and closed after one
// BlockEvent carrying Err when a read fails.
//
// ponytail: single poll loop instead of deforestationdb's two-phase
// drain-then-poll; the RAM index makes each probe a map lookup + one ReadAt,
// so the drain phase is just the poll loop running at full speed.
func (s *Store) Subscribe(ctx context.Context, fromBlock uint64) <-chan BlockEvent {
	out := make(chan BlockEvent, 64)
	go func() {
		defer close(out)
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		next := fromBlock
		for {
			raw, id, ok, err := s.readAt(next)
			if err != nil {
				// The channel closes either way, so the consumer gets the
				// failure as a final event: a closed channel alone reads as a
				// clean shutdown, which would make a corrupt container look
				// like the end of the chain.
				log.Printf("fetch: subscribe stopped at height %d: %v", next, err)
				select {
				case out <- BlockEvent{BlockNumber: next, Err: err}:
				case <-ctx.Done():
				}
				return
			}
			if ok {
				select {
				case <-ctx.Done():
					return
				case out <- BlockEvent{BlockNumber: next, ContainerID: id, Raw: raw}:
				}
				next++
				continue
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	return out
}

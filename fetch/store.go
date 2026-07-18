package fetch

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
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
	head     uint64
	haveAny  bool

	enc *zstd.Encoder
	dec *zstd.Decoder

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
type BlockEvent struct {
	BlockNumber uint64
	ContainerID ids.ID
	Raw         []byte
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
		dir:      dir,
		segs:     make(map[uint64]*segment),
		byHeight: make(map[uint64]heightRec),
		byID:     make(map[ids.ID]uint64),
		enc:      enc,
		dec:      dec,
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
	arrival, err := os.OpenFile(filepath.Join(s.dir, arrivalName(bucket)), os.O_RDWR|os.O_CREATE, 0o644)
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
	r := io.Reader(index)
	for {
		if _, err := io.ReadFull(r, rec[:]); err != nil {
			// io.EOF: clean end. ErrUnexpectedEOF: torn index record, drop it.
			break
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
	return arrival.Truncate(int64(lastEnd))
}

// seg returns the open segment pair for bucket, opening it (and LRU-closing
// the least recently used one over the cap) if needed. Caller holds s.mu.
func (s *Store) seg(bucket uint64) (*segment, error) {
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
	arrival, err := os.OpenFile(filepath.Join(s.dir, arrivalName(bucket)), os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	index, err := os.OpenFile(filepath.Join(s.dir, indexName(bucket)), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
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
	sg, err := s.seg(p.blockNumber / SegmentBlocks)
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
		return fmt.Errorf("arrival write: %w", err)
	}

	var rec [indexRecSize]byte
	binary.BigEndian.PutUint64(rec[0:8], p.blockNumber)
	copy(rec[8:40], p.containerID[:])
	copy(rec[40:72], p.blockHash[:])
	binary.BigEndian.PutUint64(rec[72:80], off)
	binary.BigEndian.PutUint32(rec[80:84], uint32(len(frame)))
	if _, err := sg.index.Write(rec[:]); err != nil {
		return fmt.Errorf("index write: %w", err)
	}

	sg.arrivalOff = off + uint64(len(frame))
	sg.dirty = true
	s.byHeight[p.blockNumber] = heightRec{id: p.containerID, off: off, ln: uint32(len(frame))}
	s.byID[p.containerID] = p.blockNumber
	if !s.haveAny || p.blockNumber > s.head {
		s.head = p.blockNumber
		s.haveAny = true
	}
	s.sessionBytes.Add(uint64(len(buf) + indexRecSize))
	s.sessionRawBytes.Add(uint64(len(raw)))
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
		return nil, ids.Empty, false, nil
	}
	sg, err := s.seg(n / SegmentBlocks)
	if err != nil {
		return nil, ids.Empty, false, err
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
func (s *Store) LowestContiguous(from, floor uint64) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	h := from
	for h > floor {
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
// polled until they land. Closed when ctx is canceled.
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

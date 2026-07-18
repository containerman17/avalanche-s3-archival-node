package state

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/containerman17/epochdb/fetch"
)

// BucketBlocks is the height width of one bucket pair, shared with the
// fetch staging layout so writelog/header buckets line up with arrival
// segments (and future epochs).
const BucketBlocks = fetch.SegmentBlocks

// bucketLog is an append-only, height-bucketed record log with a fixed
// per-bucket sidecar index (block -> offset,len). One payload per block,
// appended in any order but expected ascending; re-appending a block that
// is already indexed is a no-op (replayed blocks produce identical state).
//
//	<prefix>_NNNNN.log:     raw payload bytes back to back.
//	<prefix>_idx_NNNNN.log: fixed 20-byte records: block(8 BE) offset(8 BE)
//	                        len(4 BE), appended as payloads land.
//
// Startup rebuilds the RAM index from every sidecar and truncates a pair
// back to the last record whose payload fully fits in the data file
// (torn-tail rule, same as fetch staging).
type bucketLog struct {
	dir, prefix string
	idx         map[uint64]recLoc // block -> location within its bucket's data file
	pairs       map[uint64]*blPair
	maxBlock    uint64
	any         bool
	useCounter  uint64
	bytes       uint64 // total payload bytes on disk (for progress logging)

	// mu guards the pair map, its LRU state, and reads through pair file
	// handles (eviction closes them, so ReadAt must stay under the lock).
	// Concurrent RPC readers contend only for the microsecond-scale pread;
	// ponytail: per-pair refcounts if header reads ever top a profile.
	mu sync.Mutex
}

type recLoc struct {
	off uint64
	ln  uint32
}

type blPair struct {
	data    *os.File
	index   *os.File
	dataOff uint64
	dirty   bool
	lastUse uint64
}

const (
	blIdxRecSize = 8 + 8 + 4
	maxOpenPairs = 4
)

func (l *bucketLog) dataName(bucket uint64) string {
	return fmt.Sprintf("%s_%05d.log", l.prefix, bucket)
}

func (l *bucketLog) idxName(bucket uint64) string {
	return fmt.Sprintf("%s_idx_%05d.log", l.prefix, bucket)
}

// openBucketLog scans every sidecar of prefix in dir, rebuilds the RAM
// index, and applies the torn-tail truncation rule per pair.
func openBucketLog(dir, prefix string) (*bucketLog, error) {
	l := &bucketLog{
		dir:    dir,
		prefix: prefix,
		idx:    make(map[uint64]recLoc),
		pairs:  make(map[uint64]*blPair),
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	pattern := prefix + "_idx_%d.log"
	for _, e := range entries {
		var bucket uint64
		if _, err := fmt.Sscanf(e.Name(), pattern, &bucket); err != nil {
			continue
		}
		if err := l.rebuild(bucket); err != nil {
			l.Close()
			return nil, fmt.Errorf("%s: rebuild bucket %05d: %w", prefix, bucket, err)
		}
	}
	return l, nil
}

func (l *bucketLog) rebuild(bucket uint64) error {
	index, err := os.OpenFile(filepath.Join(l.dir, l.idxName(bucket)), os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	defer index.Close()
	data, err := os.OpenFile(filepath.Join(l.dir, l.dataName(bucket)), os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return err
	}
	defer data.Close()
	dst, err := data.Stat()
	if err != nil {
		return err
	}
	dataSize := uint64(dst.Size())

	var (
		rec     [blIdxRecSize]byte
		good    int64
		lastEnd uint64
	)
	r := io.Reader(index)
	for {
		if _, err := io.ReadFull(r, rec[:]); err != nil {
			break // EOF: clean end; ErrUnexpectedEOF: torn index record, drop
		}
		block := binary.BigEndian.Uint64(rec[0:8])
		off := binary.BigEndian.Uint64(rec[8:16])
		ln := binary.BigEndian.Uint32(rec[16:20])
		if off+uint64(ln) > dataSize {
			break // torn: payload never fully reached the data file
		}
		l.idx[block] = recLoc{off: off, ln: ln}
		l.bytes += uint64(ln)
		if !l.any || block > l.maxBlock {
			l.maxBlock = block
			l.any = true
		}
		good++
		if end := off + uint64(ln); end > lastEnd {
			lastEnd = end
		}
	}
	if err := index.Truncate(good * blIdxRecSize); err != nil {
		return err
	}
	return data.Truncate(int64(lastEnd))
}

// pair returns the open file pair for bucket, LRU-closing over the cap.
func (l *bucketLog) pair(bucket uint64) (*blPair, error) {
	l.useCounter++
	if p, ok := l.pairs[bucket]; ok {
		p.lastUse = l.useCounter
		return p, nil
	}
	for len(l.pairs) >= maxOpenPairs {
		var (
			oldest    uint64
			oldestUse = ^uint64(0)
		)
		for b, p := range l.pairs {
			if p.lastUse < oldestUse {
				oldest, oldestUse = b, p.lastUse
			}
		}
		if err := l.closePair(oldest); err != nil {
			return nil, err
		}
	}
	data, err := os.OpenFile(filepath.Join(l.dir, l.dataName(bucket)), os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	index, err := os.OpenFile(filepath.Join(l.dir, l.idxName(bucket)), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		data.Close()
		return nil, err
	}
	off, err := data.Seek(0, io.SeekEnd)
	if err != nil {
		data.Close()
		index.Close()
		return nil, err
	}
	p := &blPair{data: data, index: index, dataOff: uint64(off), lastUse: l.useCounter}
	l.pairs[bucket] = p
	return p, nil
}

func (l *bucketLog) closePair(bucket uint64) error {
	p := l.pairs[bucket]
	if p.dirty {
		if err := p.flush(); err != nil {
			return err
		}
	}
	delete(l.pairs, bucket)
	if err := p.data.Close(); err != nil {
		p.index.Close()
		return err
	}
	return p.index.Close()
}

// flush fsyncs data before index so a crash can only leave the index
// behind the data, which the torn-tail rule already handles.
func (p *blPair) flush() error {
	if err := p.data.Sync(); err != nil {
		return err
	}
	if err := p.index.Sync(); err != nil {
		return err
	}
	p.dirty = false
	return nil
}

// Has reports whether block already has an indexed payload.
func (l *bucketLog) Has(block uint64) bool {
	_, ok := l.idx[block]
	return ok
}

// Append stores payload for block. No-op if the block is already indexed
// (replay idempotency). Writes are unbuffered but not fsynced; call Sync
// after a batch. Single writer by design; the lock exists for concurrent
// readers.
func (l *bucketLog) Append(block uint64, payload []byte) error {
	if _, ok := l.idx[block]; ok {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	p, err := l.pair(block / BucketBlocks)
	if err != nil {
		return err
	}
	if _, err := p.data.Write(payload); err != nil {
		return fmt.Errorf("%s data write: %w", l.prefix, err)
	}
	var rec [blIdxRecSize]byte
	binary.BigEndian.PutUint64(rec[0:8], block)
	binary.BigEndian.PutUint64(rec[8:16], p.dataOff)
	binary.BigEndian.PutUint32(rec[16:20], uint32(len(payload)))
	if _, err := p.index.Write(rec[:]); err != nil {
		return fmt.Errorf("%s index write: %w", l.prefix, err)
	}
	l.idx[block] = recLoc{off: p.dataOff, ln: uint32(len(payload))}
	p.dataOff += uint64(len(payload))
	p.dirty = true
	l.bytes += uint64(len(payload))
	if !l.any || block > l.maxBlock {
		l.maxBlock = block
		l.any = true
	}
	return nil
}

// Get returns a copy of block's payload. ok=false if not stored.
// Goroutine-safe: the pair lock covers handle lookup and the pread.
func (l *bucketLog) Get(block uint64) ([]byte, bool, error) {
	loc, ok := l.idx[block]
	if !ok {
		return nil, false, nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	p, err := l.pair(block / BucketBlocks)
	if err != nil {
		return nil, false, err
	}
	buf := make([]byte, loc.ln)
	if _, err := p.data.ReadAt(buf, int64(loc.off)); err != nil {
		return nil, false, fmt.Errorf("%s read block %d: %w", l.prefix, block, err)
	}
	return buf, true, nil
}

// Max returns the highest indexed block, ok=false if empty.
func (l *bucketLog) Max() (uint64, bool) { return l.maxBlock, l.any }

// Bytes returns total payload bytes on disk.
func (l *bucketLog) Bytes() uint64 { return l.bytes }

// Sync fsyncs every dirty open pair.
func (l *bucketLog) Sync() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, p := range l.pairs {
		if !p.dirty {
			continue
		}
		if err := p.flush(); err != nil {
			return err
		}
	}
	return nil
}

// Close flushes and closes every open pair.
func (l *bucketLog) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	var firstErr error
	for bucket := range l.pairs {
		if err := l.closePair(bucket); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

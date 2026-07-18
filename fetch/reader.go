package fetch

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/klauspost/compress/zstd"
)

// Reader is a read-only follower over a Store directory owned by another
// (possibly running) process. It never writes or truncates: index records
// whose container bytes have not yet reached the arrival file are simply
// not surfaced until they land. Buckets are opened lazily and re-scanned
// incrementally on miss, so a Reader sees blocks as the writer appends them.
type Reader struct {
	mu      sync.Mutex
	dir     string
	dec     *zstd.Decoder
	buckets map[uint64]*readerBucket
}

type readerBucket struct {
	arrival  *os.File
	index    *os.File
	indexOff int64 // consumed bytes of the index sidecar
	byHeight map[uint64]heightRec
	lastUse  uint64
}

// maxReaderBuckets caps open bucket pairs; the executor consumes heights
// ascending so old buckets go cold immediately.
const maxReaderBuckets = 4

// BlockHashes scans every index sidecar in dir once and returns the
// eth block hash -> height map (the sidecar stores the hash at rec[40:72]
// exactly for consumers like eth_getBlockByHash). ~40 bytes RAM per block.
func BlockHashes(dir string) (map[[32]byte]uint64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	out := make(map[[32]byte]uint64)
	for _, e := range entries {
		var bucket uint64
		if _, err := fmt.Sscanf(e.Name(), "index_%d.log", &bucket); err != nil {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			return nil, err
		}
		for off := 0; off+indexRecSize <= len(raw); off += indexRecSize {
			height := binary.BigEndian.Uint64(raw[off : off+8])
			var h [32]byte
			copy(h[:], raw[off+40:off+72])
			out[h] = height
		}
	}
	return out, nil
}

// OpenReader opens a read-only follower over dir.
func OpenReader(dir string) (*Reader, error) {
	dec, err := zstd.NewReader(nil)
	if err != nil {
		return nil, err
	}
	return &Reader{dir: dir, dec: dec, buckets: make(map[uint64]*readerBucket)}, nil
}

// Close releases all open bucket handles.
func (r *Reader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for b, rb := range r.buckets {
		rb.arrival.Close()
		rb.index.Close()
		delete(r.buckets, b)
	}
	r.dec.Close()
	return nil
}

var readerUse uint64

// GetByHeight returns a copy of the raw container stored at height n.
// ok=false means the writer has not (visibly) stored that height yet.
func (r *Reader) GetByHeight(n uint64) ([]byte, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	rb, err := r.bucket(n / SegmentBlocks)
	if err != nil || rb == nil {
		return nil, false, err
	}
	rec, ok := rb.byHeight[n]
	if !ok {
		if err := r.refresh(rb); err != nil {
			return nil, false, err
		}
		if rec, ok = rb.byHeight[n]; !ok {
			return nil, false, nil
		}
	}
	frame := make([]byte, rec.ln)
	if _, err := rb.arrival.ReadAt(frame, int64(rec.off)); err != nil {
		return nil, false, fmt.Errorf("reader: read container at height %d: %w", n, err)
	}
	raw, err := r.dec.DecodeAll(frame, nil)
	if err != nil {
		return nil, false, fmt.Errorf("reader: decompress container at height %d: %w", n, err)
	}
	return raw, true, nil
}

// bucket returns the follower state for one bucket, opening it if the
// writer has created its files. nil, nil means the bucket does not exist yet.
func (r *Reader) bucket(b uint64) (*readerBucket, error) {
	readerUse++
	if rb, ok := r.buckets[b]; ok {
		rb.lastUse = readerUse
		return rb, nil
	}
	index, err := os.Open(filepath.Join(r.dir, indexName(b)))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	arrival, err := os.Open(filepath.Join(r.dir, arrivalName(b)))
	if os.IsNotExist(err) {
		index.Close()
		return nil, nil
	}
	if err != nil {
		index.Close()
		return nil, err
	}
	for len(r.buckets) >= maxReaderBuckets {
		var (
			oldest    uint64
			oldestUse = ^uint64(0)
		)
		for ob, orb := range r.buckets {
			if orb.lastUse < oldestUse {
				oldest, oldestUse = ob, orb.lastUse
			}
		}
		r.buckets[oldest].arrival.Close()
		r.buckets[oldest].index.Close()
		delete(r.buckets, oldest)
	}
	rb := &readerBucket{arrival: arrival, index: index, byHeight: make(map[uint64]heightRec), lastUse: readerUse}
	r.buckets[b] = rb
	return rb, r.refresh(rb)
}

// refresh consumes new complete index records. A record is complete when
// its container bytes fully fit in the current arrival file; incomplete
// tails are left unconsumed and retried on the next refresh.
func (r *Reader) refresh(rb *readerBucket) error {
	ast, err := rb.arrival.Stat()
	if err != nil {
		return err
	}
	arrivalSize := uint64(ast.Size())
	var rec [indexRecSize]byte
	for {
		if _, err := rb.index.ReadAt(rec[:], rb.indexOff); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return nil
			}
			return err
		}
		height := binary.BigEndian.Uint64(rec[0:8])
		off := binary.BigEndian.Uint64(rec[72:80])
		ln := binary.BigEndian.Uint32(rec[80:84])
		if off+uint64(ln) > arrivalSize {
			return nil // container bytes not visible yet
		}
		rb.byHeight[height] = heightRec{off: off, ln: ln}
		rb.indexOff += indexRecSize
	}
}

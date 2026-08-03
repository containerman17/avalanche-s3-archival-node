package state

// Verification-side epoch readers: sequential framed-section walkers (one
// frame decode per framedGroup blocks instead of one per block) and the
// SST re-sort spill that turns (key, block)-ordered post-image rows into
// (block, key)-ordered per-block diff groups. The Firewood application
// engine lives in the verify package (state cannot import the EVM stack).

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/containerman17/epochdb/dist"
)

// WalkHeadersRange calls fn with the RLP header of every block in
// [from, to), decoding each zstd frame once. The byte slice is a view into
// a per-frame decode buffer: copy to retain beyond the callback.
func (e *Epoch) WalkHeadersRange(from, to uint64, fn func(n uint64, hdrRLP []byte) error) error {
	return e.walkFramedRange(secHeaders, e.sec[secHeadersIdx], from, to, fn)
}

// WalkContainersRange is WalkHeadersRange over the raw containers.
func (e *Epoch) WalkContainersRange(from, to uint64, fn func(n uint64, raw []byte) error) error {
	return e.walkFramedRange(secBodies, e.sec[secBodiesIdx], from, to, fn)
}

func (e *Epoch) walkFramedRange(dataSec int, index *dist.View, from, to uint64, fn func(uint64, []byte) error) error {
	if from < e.Start || to > e.End()+1 || from >= to {
		return fmt.Errorf("walk range [%d,%d) outside epoch [%d,%d]", from, to, e.Start, e.End())
	}
	for f := (from - e.Start) / framedGroup; f*framedGroup < to-e.Start; f++ {
		lo := vu64(index, f*8)
		hi := vu64(index, (f+1)*8)
		frame, err := e.read(dataSec, lo, hi-lo)
		if err != nil {
			return fmt.Errorf("frame %d: %w", f, err)
		}
		raw, err := e.decodeAll(frame)
		if err != nil {
			return fmt.Errorf("frame %d: %w", f, err)
		}
		pos := 0
		for j := uint64(0); j < framedGroup && pos < len(raw); j++ {
			ln, k := binary.Uvarint(raw[pos:])
			if k <= 0 {
				return fmt.Errorf("frame %d: bad payload", f)
			}
			pos += k
			n := e.Start + f*framedGroup + j
			if n >= to {
				break
			}
			if n >= from {
				if err := fn(n, raw[pos:pos+int(ln)]); err != nil {
					return err
				}
			}
			pos += int(ln)
		}
	}
	return nil
}

// SpillDiffs re-sorts the epoch's SST rows from (key, block) to
// (block, key) order for the per-block diff replay. Production epochs
// decompress to far more than RAM, so rows are spilled into block-range
// bucket files under tmpDir and each bucket is sorted in memory.
// Spill record: relBlock u32 | vlen u32 | key53 | value.
func (e *Epoch) SpillDiffs(tmpDir string) (*DiffCursor, error) {
	const bucketTarget = 512 << 20
	// ponytail: ~3x zstd expansion estimate sizes the bucket count; a
	// skewed epoch just gets bigger in-memory buckets.
	nB := int(e.off[secSST][1])*3/bucketTarget + 1
	if nB > 64 {
		nB = 64
	}
	blocksPer := (e.Count + uint64(nB) - 1) / uint64(nB)

	dir, err := os.MkdirTemp(tmpDir, fmt.Sprintf("spill-%d-", e.Start))
	if err != nil {
		return nil, err
	}
	c := &DiffCursor{start: e.Start, dir: dir}
	bufs := make([][]byte, nB)
	var spillErr error
	if err := e.WalkStateRows(func(r StateRow) {
		if spillErr != nil {
			return
		}
		if r.Key[0] == recKindCodeUse {
			return // v3 code blob row, not a state diff
		}
		if r.Block < e.Start || r.Block > e.End() {
			spillErr = fmt.Errorf("SST row for block %d outside epoch [%d,%d]", r.Block, e.Start, e.End())
			return
		}
		bi := (r.Block - e.Start) / blocksPer
		b := bufs[bi]
		b = binary.LittleEndian.AppendUint32(b, uint32(r.Block-e.Start))
		b = binary.LittleEndian.AppendUint32(b, uint32(len(r.Value)))
		b = append(b, r.Key[:]...)
		b = append(b, r.Value...)
		bufs[bi] = b
		if len(b) >= 8<<20 {
			spillErr = c.flush(int(bi), b)
			bufs[bi] = b[:0]
		}
	}); err != nil {
		c.Close()
		return nil, err
	}
	for bi, b := range bufs {
		if spillErr != nil {
			break
		}
		if len(b) > 0 {
			spillErr = c.flush(bi, b)
		}
	}
	if spillErr != nil {
		c.Close()
		return nil, spillErr
	}
	c.nBuckets = nB
	return c, nil
}

// DiffCursor yields the epoch's diff-bearing blocks in ascending block
// order, each with its rows sorted by key (account rows before storage
// rows, matching commit-time trie-op order). Rows reference an internal
// buffer valid until the next Next call.
type DiffCursor struct {
	start    uint64
	dir      string
	nBuckets int
	bi       int // next bucket to load
	buf      []byte
	rows     []StateRow
	i        int
}

func (c *DiffCursor) flush(bi int, b []byte) error {
	f, err := os.OpenFile(c.path(bi), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

func (c *DiffCursor) path(bi int) string {
	return filepath.Join(c.dir, fmt.Sprintf("b%03d", bi))
}

// Next returns the next diff-bearing block and its rows. ok=false when
// the epoch is exhausted.
func (c *DiffCursor) Next() (blk uint64, rows []StateRow, ok bool, err error) {
	for c.i >= len(c.rows) {
		if c.bi >= c.nBuckets {
			return 0, nil, false, nil
		}
		if err := c.loadBucket(c.bi); err != nil {
			return 0, nil, false, err
		}
		c.bi++
	}
	j := c.i
	blk = c.rows[j].Block
	for c.i < len(c.rows) && c.rows[c.i].Block == blk {
		c.i++
	}
	return blk, c.rows[j:c.i], true, nil
}

func (c *DiffCursor) loadBucket(bi int) error {
	c.rows, c.i = c.rows[:0], 0
	b, err := os.ReadFile(c.path(bi))
	if os.IsNotExist(err) {
		return nil // bucket had no rows
	}
	if err != nil {
		return err
	}
	c.buf = b
	for pos := 0; pos < len(b); {
		if pos+8+sortedKeySize > len(b) {
			return fmt.Errorf("spill bucket %d: truncated record", bi)
		}
		rel := binary.LittleEndian.Uint32(b[pos:])
		vlen := int(binary.LittleEndian.Uint32(b[pos+4:]))
		var r StateRow
		copy(r.Key[:], b[pos+8:pos+8+sortedKeySize])
		r.Block = c.start + uint64(rel)
		pos += 8 + sortedKeySize
		if vlen > 0 {
			r.Value = b[pos : pos+vlen]
			pos += vlen
		}
		c.rows = append(c.rows, r)
	}
	sort.Slice(c.rows, func(i, j int) bool {
		if c.rows[i].Block != c.rows[j].Block {
			return c.rows[i].Block < c.rows[j].Block
		}
		return bytes.Compare(c.rows[i].Key[:], c.rows[j].Key[:]) < 0
	})
	return nil
}

// Close removes the spill files.
func (c *DiffCursor) Close() {
	os.RemoveAll(c.dir)
	c.rows, c.buf = nil, nil
}

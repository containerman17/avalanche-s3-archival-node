package state

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// Cooked per-bucket sorted index over the writelog: sorted_NNNNN.idx.
//
//	header (24B): magic "EDBS" | recSize uint32 BE | cookedThrough uint64 BE |
//	              wlSize uint64 BE (writelog data file size at cook time)
//	records (73B fixed, sorted ascending by (key, block)):
//	  key[53]  = kind(1: 'a' account | 's' storage) + addr(20) + slot(32,
//	             zero for accounts)
//	  block[8] BE
//	  voff[8]  BE  absolute offset of the post-image value bytes inside
//	               writelog_NNNNN.log (values stay in the writelog; the
//	               sorted file only points at them)
//	  vlen[4]  BE  0 = explicit delete / zero write
//
// cookedThrough is the highest block the cook covered (min of bucket end
// and exechead at cook time). A bucket whose cookedThrough already reaches
// that target AND whose writelog size still matches wlSize is skipped; the
// tip bucket is re-cooked as exechead advances, and a regenerated writelog
// (wiped data dir re-executed with different capture behavior) invalidates
// the stored value offsets, which the wlSize mismatch catches.
const (
	sortedHdrSize = 24
	sortedKeySize = 53
	sortedRecSize = sortedKeySize + 8 + 8 + 4
)

var sortedMagic = [4]byte{'E', 'D', 'B', 'S'}

// Write-capture record kinds, mirroring exec/capture.go (exec imports
// state, so the constants live there and are duplicated here).
const (
	recKindAccount byte = 'a'
	recKindStorage byte = 's'
	recKindCodeUse byte = 'c'
)

func sortedName(bucket uint64) string { return fmt.Sprintf("sorted_%05d.idx", bucket) }

// readSortedHeader returns the cookedThrough and cook-time writelog size of
// an existing sorted file, ok=false if missing or malformed (= re-cook).
func readSortedHeader(path string) (cookedThrough, wlSize uint64, ok bool) {
	f, err := os.Open(path)
	if err != nil {
		return 0, 0, false
	}
	defer f.Close()
	var hdr [sortedHdrSize]byte
	if _, err := f.ReadAt(hdr[:], 0); err != nil {
		return 0, 0, false
	}
	if !bytes.Equal(hdr[0:4], sortedMagic[:]) || binary.BigEndian.Uint32(hdr[4:8]) != sortedRecSize {
		return 0, 0, false
	}
	return binary.BigEndian.Uint64(hdr[8:16]), binary.BigEndian.Uint64(hdr[16:24]), true
}

type cookRec struct {
	key  [sortedKeySize]byte
	blk  uint64
	voff uint64
	vlen uint32
	seq  int // frame order within a block, dedupe tiebreaker
}

// CookIndex external-sorts every writelog bucket that has blocks at or
// below the durable exechead into sorted_NNNNN.idx. Idempotent and
// re-runnable: fully cooked buckets are skipped, the partial tip bucket is
// rewritten (tmp+rename). Read-only on the writelog, safe next to a
// running exec (everything at or below exechead is durable and immutable).
func CookIndex(dir string) error {
	raw, err := os.ReadFile(filepath.Join(dir, execHeadFile))
	if err != nil {
		return fmt.Errorf("cook: read exechead: %w", err)
	}
	if len(raw) != 8 {
		return fmt.Errorf("cook: exechead is %d bytes, want 8", len(raw))
	}
	execHead := binary.BigEndian.Uint64(raw)

	// Read-only scan: the writelog may be owned by a live exec; the regular
	// open would truncate torn tails out from under the writer.
	wl, err := openBucketLogRO(dir, "writelog")
	if err != nil {
		return fmt.Errorf("cook: open writelog: %w", err)
	}
	defer wl.Close()

	// Bucket set with any block <= exechead.
	inBucket := make(map[uint64][]uint64) // bucket -> blocks
	for block := range wl.idx {
		if block > execHead {
			continue // writelog tail can be ahead of exechead after a crash
		}
		b := block / BucketBlocks
		inBucket[b] = append(inBucket[b], block)
	}
	buckets := make([]uint64, 0, len(inBucket))
	for b := range inBucket {
		buckets = append(buckets, b)
	}
	sort.Slice(buckets, func(i, j int) bool { return buckets[i] < buckets[j] })

	for _, b := range buckets {
		target := (b+1)*BucketBlocks - 1
		if execHead < target {
			target = execHead
		}
		wlStat, err := os.Stat(filepath.Join(dir, wl.dataName(b)))
		if err != nil {
			return fmt.Errorf("cook: bucket %05d: %w", b, err)
		}
		wlSize := uint64(wlStat.Size())
		path := filepath.Join(dir, sortedName(b))
		if got, gotSize, ok := readSortedHeader(path); ok && got >= target && gotSize == wlSize {
			fmt.Printf("cook: bucket %05d already cooked through %d, skip\n", b, got)
			continue
		}
		start := time.Now()
		n, err := cookBucket(dir, wl, b, inBucket[b], target, wlSize)
		if err != nil {
			return fmt.Errorf("cook: bucket %05d: %w", b, err)
		}
		st, _ := os.Stat(path)
		fmt.Printf("cook: bucket %05d records=%d size=%.1fMB through=%d in %s\n",
			b, n, float64(st.Size())/1e6, target, time.Since(start).Round(time.Millisecond))
	}
	return nil
}

// cookBucket parses every frame of one bucket, sorts the records by
// (key, block) and writes the sorted file atomically. Returns record count.
//
// ponytail: per-bucket in-RAM sort; a bucket is 100k blocks (~100MB of
// writelog on Fuji), switch to a real external merge sort when a mainnet
// bucket outgrows RAM.
func cookBucket(dir string, wl *bucketLog, bucket uint64, blocks []uint64, cookedThrough, wlSize uint64) (int, error) {
	var recs []cookRec
	for _, block := range blocks {
		loc := wl.idx[block]
		frame, ok, err := wl.Get(block)
		if err != nil || !ok {
			return 0, fmt.Errorf("read frame %d: ok=%v err=%w", block, ok, err)
		}
		if err := parseFrame(frame, func(kind byte, key [sortedKeySize]byte, valOff int, vlen uint32) {
			if kind == recKindCodeUse {
				return
			}
			recs = append(recs, cookRec{
				key:  key,
				blk:  block,
				voff: loc.off + uint64(valOff),
				vlen: vlen,
				seq:  len(recs),
			})
		}); err != nil {
			return 0, fmt.Errorf("frame %d: %w", block, err)
		}
	}

	sort.Slice(recs, func(i, j int) bool {
		if c := bytes.Compare(recs[i].key[:], recs[j].key[:]); c != 0 {
			return c < 0
		}
		if recs[i].blk != recs[j].blk {
			return recs[i].blk < recs[j].blk
		}
		return recs[i].seq < recs[j].seq
	})

	tmp := filepath.Join(dir, sortedName(bucket)+".tmp")
	f, err := os.Create(tmp)
	if err != nil {
		return 0, err
	}
	w := bufio.NewWriterSize(f, 1<<20)
	var hdr [sortedHdrSize]byte
	copy(hdr[0:4], sortedMagic[:])
	binary.BigEndian.PutUint32(hdr[4:8], sortedRecSize)
	binary.BigEndian.PutUint64(hdr[8:16], cookedThrough)
	binary.BigEndian.PutUint64(hdr[16:24], wlSize)
	if _, err := w.Write(hdr[:]); err != nil {
		f.Close()
		return 0, err
	}
	written := 0
	var rec [sortedRecSize]byte
	for i, r := range recs {
		// Same (key, block) written more than once in a frame: post-image
		// semantics, the last write wins, drop the earlier ones.
		if i+1 < len(recs) && recs[i+1].blk == r.blk && recs[i+1].key == r.key {
			continue
		}
		copy(rec[:sortedKeySize], r.key[:])
		binary.BigEndian.PutUint64(rec[53:61], r.blk)
		binary.BigEndian.PutUint64(rec[61:69], r.voff)
		binary.BigEndian.PutUint32(rec[69:73], r.vlen)
		if _, err := w.Write(rec[:]); err != nil {
			f.Close()
			return 0, err
		}
		written++
	}
	if err := w.Flush(); err != nil {
		f.Close()
		return 0, err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return 0, err
	}
	if err := f.Close(); err != nil {
		return 0, err
	}
	return written, os.Rename(tmp, filepath.Join(dir, sortedName(bucket)))
}

// parseFrame walks one write-capture frame (exec/capture.go layout) and
// calls fn per record with the 53-byte sorted key, the value offset within
// the frame, and the value length.
func parseFrame(frame []byte, fn func(kind byte, key [sortedKeySize]byte, valOff int, vlen uint32)) error {
	pos := 0
	for pos < len(frame) {
		kind := frame[pos]
		var key [sortedKeySize]byte
		key[0] = kind
		switch kind {
		case recKindAccount:
			if pos+21 > len(frame) {
				return fmt.Errorf("truncated account record at %d", pos)
			}
			copy(key[1:21], frame[pos+1:pos+21])
			pos += 21
		case recKindStorage, recKindCodeUse:
			if pos+53 > len(frame) {
				return fmt.Errorf("truncated storage record at %d", pos)
			}
			copy(key[1:53], frame[pos+1:pos+53])
			pos += 53
		default:
			return fmt.Errorf("unknown record kind %q at %d", kind, pos)
		}
		vlen, n := binary.Uvarint(frame[pos:])
		if n <= 0 {
			return fmt.Errorf("bad varint at %d", pos)
		}
		pos += n
		if pos+int(vlen) > len(frame) {
			return fmt.Errorf("truncated value at %d", pos)
		}
		fn(kind, key, pos, uint32(vlen))
		pos += int(vlen)
	}
	return nil
}

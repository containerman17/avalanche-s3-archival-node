// Package latest is the latest-state store: an in-memory Overlay of writes
// since the last checkpoint over one or more immutable, mmapped, front-coded
// sorted Runs. Keys are compared bytewise; the package does not care what
// they mean. Keys are 1..255 bytes, values 0..255 bytes; an empty value
// means deleted (a tombstone in the overlay, dropped by Merge).
package latest

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"syscall"
)

// File layout: N blocks of blockSize bytes, then the index (N packed
// first-key prefixes of prefixLen bytes, zero padded), then the footer.
// A block is [used u16] then entries [shared u8][unshared u8][vlen u8]
// [key suffix][value], the first entry of a block with shared = 0, and zero
// padding to blockSize. An entry never straddles blocks.
const (
	blockSize = 4096
	prefixLen = 40
	footerLen = 76
	maxKey    = 255
	maxValue  = 255
	magic     = "epochrun"
	version   = 1
)

// Iterator walks entries in ascending key order. Key and Value are valid
// until the next call to Next.
type Iterator interface {
	Next() bool
	Key() []byte
	Value() []byte
	Err() error
}

// Writer builds a run. Add must be called in strictly ascending key order.
type Writer struct {
	f     *os.File
	w     *bufio.Writer
	blk   []byte // current block: header then entries
	prev  []byte // last key added
	index []byte // packed first-key prefixes
	n     uint64
	user  [32]byte
}

func NewWriter(path string) (*Writer, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	return &Writer{f: f, w: bufio.NewWriterSize(f, 1<<20), blk: make([]byte, 2, blockSize)}, nil
}

// SetUserData sets the 32-byte field stored in the footer (a height, a root).
func (w *Writer) SetUserData(u [32]byte) { w.user = u }

func (w *Writer) Add(key, value []byte) error {
	if len(key) == 0 || len(key) > maxKey || len(value) > maxValue {
		return fmt.Errorf("latest: key %d bytes, value %d bytes (limits 1..%d and 0..%d)", len(key), len(value), maxKey, maxValue)
	}
	if w.n > 0 && bytes.Compare(key, w.prev) <= 0 {
		return fmt.Errorf("latest: key %x not after %x", key, w.prev)
	}
	shared := 0
	if len(w.blk) > 2 {
		for shared < len(w.prev) && shared < len(key) && w.prev[shared] == key[shared] {
			shared++
		}
		if len(w.blk)+3+len(key)-shared+len(value) > blockSize {
			if err := w.flush(); err != nil {
				return err
			}
			shared = 0
		}
	}
	if len(w.blk) == 2 {
		var p [prefixLen]byte
		copy(p[:], key)
		w.index = append(w.index, p[:]...)
	}
	w.blk = append(w.blk, byte(shared), byte(len(key)-shared), byte(len(value)))
	w.blk = append(w.blk, key[shared:]...)
	w.blk = append(w.blk, value...)
	w.prev = append(w.prev[:0], key...)
	w.n++
	return nil
}

func (w *Writer) flush() error {
	used := len(w.blk)
	if used == 2 {
		return nil
	}
	binary.LittleEndian.PutUint16(w.blk, uint16(used))
	w.blk = w.blk[:blockSize]
	clear(w.blk[used:])
	_, err := w.w.Write(w.blk)
	w.blk = w.blk[:2]
	return err
}

// Close writes the last block, the index and the footer, and syncs.
func (w *Writer) Close() error {
	err := w.flush()
	nblk := uint64(len(w.index) / prefixLen)
	var ft [footerLen]byte
	copy(ft[0:8], magic)
	binary.LittleEndian.PutUint32(ft[8:], version)
	binary.LittleEndian.PutUint32(ft[12:], blockSize)
	binary.LittleEndian.PutUint64(ft[16:], w.n)
	binary.LittleEndian.PutUint64(ft[24:], nblk)
	binary.LittleEndian.PutUint64(ft[32:], nblk*blockSize)
	copy(ft[40:72], w.user[:])
	binary.LittleEndian.PutUint32(ft[72:], crc32.Update(crc32.ChecksumIEEE(w.index), crc32.IEEETable, ft[:72]))
	if err == nil {
		_, err = w.w.Write(w.index)
	}
	if err == nil {
		_, err = w.w.Write(ft[:])
	}
	if err == nil {
		err = w.w.Flush()
	}
	if err == nil {
		err = w.f.Sync()
	}
	return errors.Join(err, w.f.Close())
}

// Run is an open, immutable, mmapped run. Safe for concurrent readers.
type Run struct {
	mm    []byte
	index []byte // the packed prefixes, aliasing mm
	nblk  int
	n     uint64
	user  [32]byte
}

// Open maps the file read-only and validates the footer and the index checksum.
func Open(path string) (*Run, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	size := st.Size()
	if size < footerLen {
		return nil, fmt.Errorf("latest: %s: %d bytes, no footer", path, size)
	}
	mm, err := syscall.Mmap(int(f.Fd()), 0, int(size), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, err
	}
	ft := mm[size-footerLen:]
	nblk := binary.LittleEndian.Uint64(ft[24:])
	ioff := binary.LittleEndian.Uint64(ft[32:])
	if string(ft[:8]) != magic || binary.LittleEndian.Uint32(ft[8:]) != version || binary.LittleEndian.Uint32(ft[12:]) != blockSize ||
		nblk > uint64(size)/blockSize || ioff != nblk*blockSize || ioff+nblk*prefixLen+footerLen != uint64(size) {
		syscall.Munmap(mm)
		return nil, fmt.Errorf("latest: %s: bad footer", path)
	}
	index := mm[ioff : ioff+nblk*prefixLen]
	if crc32.Update(crc32.ChecksumIEEE(index), crc32.IEEETable, ft[:72]) != binary.LittleEndian.Uint32(ft[72:]) {
		syscall.Munmap(mm)
		return nil, fmt.Errorf("latest: %s: index checksum mismatch", path)
	}
	r := &Run{mm: mm, index: index, nblk: int(nblk), n: binary.LittleEndian.Uint64(ft[16:])}
	copy(r.user[:], ft[40:72])
	return r, nil
}

func (r *Run) Len() int            { return int(r.n) }
func (r *Run) Bytes() int          { return len(r.mm) }
func (r *Run) UserData() [32]byte  { return r.user }
func (r *Run) block(b int) []byte  { return r.mm[b*blockSize : (b+1)*blockSize] }
func (r *Run) prefix(b int) []byte { return r.index[b*prefixLen : b*prefixLen+prefixLen] }

// firstKey is block b's first key, stored whole at the block start.
func (r *Run) firstKey(b int) []byte {
	blk := r.block(b)
	return blk[5 : 5+int(blk[3])]
}

// Close unmaps the file. Every value returned by Get or an Iterator aliases
// the mapping and is invalid after Close.
func (r *Run) Close() { syscall.Munmap(r.mm) }

// Get finds key; val aliases the mapping and is valid until Close.
// No allocation.
func (r *Run) Get(key []byte) (val []byte, ok bool) {
	b := r.seek(key)
	if b < 0 {
		return nil, false
	}
	return scanBlock(r.block(b), key)
}

// seek returns the last block whose first key is <= key, or -1.
func (r *Run) seek(key []byte) int {
	var kp [prefixLen]byte
	copy(kp[:], key)
	lo, hi := 0, r.nblk
	for lo < hi {
		m := int(uint(lo+hi) >> 1)
		if bytes.Compare(r.prefix(m), kp[:]) <= 0 {
			lo = m + 1
		} else {
			hi = m
		}
	}
	b := lo - 1
	if b < 0 || !bytes.Equal(r.prefix(b), kp[:]) || bytes.Compare(r.firstKey(b), key) <= 0 {
		return b
	}
	// The prefix ties and block b starts after key: find the first tied
	// block, then binary search the tied range on the full first keys.
	lo, hi = 0, b
	for lo < hi {
		m := int(uint(lo+hi) >> 1)
		if bytes.Compare(r.prefix(m), kp[:]) < 0 {
			lo = m + 1
		} else {
			hi = m
		}
	}
	hi = b
	for lo < hi {
		m := int(uint(lo+hi) >> 1)
		if bytes.Compare(r.firstKey(m), key) <= 0 {
			lo = m + 1
		} else {
			hi = m
		}
	}
	return lo - 1
}

// scanBlock walks the block's entries in order without materializing a
// key. eq is how many leading bytes the previous key shares with key. The
// writer stores the maximal shared prefix, so an entry that reuses more
// than eq bytes of the previous key sorts before key, one that reuses fewer
// sorts after it, and only an entry with shared == eq needs its suffix
// compared against key[eq:].
func scanBlock(b, key []byte) ([]byte, bool) {
	used := int(binary.LittleEndian.Uint16(b))
	eq := 0
	for i := 2; i < used; {
		sh, un, vl := int(b[i]), int(b[i+1]), int(b[i+2])
		i += 3
		if sh < eq {
			return nil, false
		}
		if sh == eq {
			suf, rest := b[i:i+un], key[eq:]
			n := min(len(suf), len(rest))
			j := 0
			for j < n && suf[j] == rest[j] {
				j++
			}
			switch {
			case j < n && suf[j] > rest[j], j == n && len(suf) > len(rest):
				return nil, false
			case j == n && len(suf) == len(rest):
				return b[i+un : i+un+vl], true
			}
			eq += j
		}
		i += un + vl
	}
	return nil, false
}

// Iter walks [lo, hi); nil means unbounded. Values alias the mapping.
func (r *Run) Iter(lo, hi []byte) Iterator {
	it := &runIter{r: r, lo: lo, hi: hi, b: -1, key: make([]byte, 0, maxKey)}
	if lo != nil {
		if b := r.seek(lo); b > 0 {
			it.b = b - 1
		}
	}
	return it
}

type runIter struct {
	r        *Run
	lo, hi   []byte
	b, i     int // current block, offset within it
	used     int
	blk      []byte
	key, val []byte
}

func (it *runIter) Next() bool {
	for it.step() {
		if it.lo != nil {
			if bytes.Compare(it.key, it.lo) < 0 {
				continue
			}
			it.lo = nil
		}
		if it.hi != nil && bytes.Compare(it.key, it.hi) >= 0 {
			it.b, it.i, it.used = it.r.nblk, 0, 0
			return false
		}
		return true
	}
	return false
}

func (it *runIter) step() bool {
	for it.i >= it.used {
		it.b++
		if it.b >= it.r.nblk {
			return false
		}
		it.blk = it.r.block(it.b)
		it.used = int(binary.LittleEndian.Uint16(it.blk))
		it.i = 2
	}
	b := it.blk
	sh, un, vl := int(b[it.i]), int(b[it.i+1]), int(b[it.i+2])
	it.i += 3
	it.key = append(it.key[:sh], b[it.i:it.i+un]...)
	it.i += un
	it.val = b[it.i : it.i+vl]
	it.i += vl
	return true
}

func (it *runIter) Key() []byte   { return it.key }
func (it *runIter) Value() []byte { return it.val }
func (it *runIter) Err() error    { return nil }

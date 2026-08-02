package state

import (
	"bufio"
	"bytes"
	"container/heap"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
)

// The seal's bounded sort. Records go in in any order and come back out in
// byte order over their first keyLen bytes; RAM never exceeds extSortBudget
// plus one read buffer per spilled chunk, whatever the epoch holds. That is
// the whole point: an 8M-tx epoch of a sparse L1 carries tens of millions of
// post-image rows, and sorting those on the heap is what OOM-killed the
// sealer three times in 24h (2026-08-02).
//
// Chunk files land in a scratch DIRECTORY the caller owns (inside the data
// dir, never /tmp: same filesystem, and a killed seal's leftovers are swept
// on the next pass). Wire format is uvarint length + bytes, which is all a
// sequential writer and a merging reader need.
//
// TIES KEEP INSERTION ORDER (stable chunk sort, chunk index breaks merge
// ties), so a caller whose key is a prefix of the record still gets a
// deterministic order over the rest of it.
const extSortBudget = 256 << 20

type extSort struct {
	dir    string
	name   string
	keyLen int // leading bytes that order a record; 0 = the whole record

	buf   []byte  // records back to back
	offs  []int32 // record starts in buf (+ end sentinel while sorting)
	files []string
}

func newExtSort(dir, name string, keyLen int) *extSort {
	return &extSort{dir: dir, name: name, keyLen: keyLen}
}

// add copies one record in; the caller's slice is not retained.
func (s *extSort) add(rec []byte) error {
	s.offs = append(s.offs, int32(len(s.buf)))
	s.buf = append(s.buf, rec...)
	if len(s.buf) >= extSortBudget {
		return s.spill()
	}
	return nil
}

func (s *extSort) rec(i int32) []byte { return s.buf[s.offs[i]:s.offs[i+1]] }

func (s *extSort) key(r []byte) []byte {
	if s.keyLen > 0 && len(r) > s.keyLen {
		return r[:s.keyLen]
	}
	return r
}

// order sorts what is buffered and returns the record indexes in order,
// appending the end sentinel that makes rec(i) well defined.
func (s *extSort) order() []int32 {
	s.offs = append(s.offs, int32(len(s.buf)))
	idx := make([]int32, len(s.offs)-1)
	for i := range idx {
		idx[i] = int32(i)
	}
	sort.SliceStable(idx, func(a, b int) bool {
		return bytes.Compare(s.key(s.rec(idx[a])), s.key(s.rec(idx[b]))) < 0
	})
	return idx
}

// spill writes the buffered records to a sorted chunk file and empties the
// buffer, keeping its capacity for the next chunk.
func (s *extSort) spill() error {
	if len(s.offs) == 0 {
		return nil
	}
	path := filepath.Join(s.dir, fmt.Sprintf("%s-%04d.chunk", s.name, len(s.files)))
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	w := bufio.NewWriterSize(f, 1<<20)
	var hdr [binary.MaxVarintLen64]byte
	write := func(b []byte) {
		if err == nil {
			_, err = w.Write(b)
		}
	}
	for _, i := range s.order() {
		r := s.rec(i)
		write(hdr[:binary.PutUvarint(hdr[:], uint64(len(r)))])
		write(r)
	}
	if err == nil {
		err = w.Flush()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return err
	}
	s.files = append(s.files, path)
	s.buf, s.offs = s.buf[:0], s.offs[:0]
	return nil
}

// sorted streams every record in order. A record is valid only until the
// next call, and calling sorted twice is not supported.
func (s *extSort) sorted(fn func(rec []byte) error) error {
	if len(s.files) == 0 { // it all fit: no file was ever written
		for _, i := range s.order() {
			if err := fn(s.rec(i)); err != nil {
				return err
			}
		}
		s.buf, s.offs = nil, nil
		return nil
	}
	if err := s.spill(); err != nil {
		return err
	}
	s.buf, s.offs = nil, nil // the merge holds one read buffer per chunk, no more

	var h chunkHeap
	defer func() {
		for _, c := range h {
			c.close()
		}
	}()
	for i, p := range s.files {
		c, err := openChunk(p, i, s.keyLen)
		if err != nil {
			return err
		}
		if c.cur == nil {
			c.close()
			continue
		}
		h = append(h, c)
	}
	heap.Init(&h)
	for len(h) > 0 {
		c := h[0]
		if err := fn(c.cur); err != nil {
			return err
		}
		if err := c.next(); err != nil {
			return err
		}
		if c.cur == nil {
			heap.Pop(&h)
			c.close()
			continue
		}
		heap.Fix(&h, 0)
	}
	return nil
}

// chunkReader is one spilled chunk, positioned on its current record.
type chunkReader struct {
	f      *os.File
	r      *bufio.Reader
	idx    int
	keyLen int
	cur    []byte
	buf    []byte
}

func openChunk(path string, idx, keyLen int) (*chunkReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	c := &chunkReader{f: f, r: bufio.NewReaderSize(f, 1<<20), idx: idx, keyLen: keyLen}
	if err := c.next(); err != nil {
		c.close()
		return nil, err
	}
	return c, nil
}

func (c *chunkReader) next() error {
	n, err := binary.ReadUvarint(c.r)
	if err == io.EOF {
		c.cur = nil
		return nil
	}
	if err != nil {
		return err
	}
	if uint64(cap(c.buf)) < n {
		c.buf = make([]byte, n)
	}
	c.buf = c.buf[:n]
	if _, err := io.ReadFull(c.r, c.buf); err != nil {
		return err
	}
	c.cur = c.buf
	return nil
}

func (c *chunkReader) key() []byte {
	if c.keyLen > 0 && len(c.cur) > c.keyLen {
		return c.cur[:c.keyLen]
	}
	return c.cur
}

func (c *chunkReader) close() {
	if c.f != nil {
		c.f.Close()
		c.f = nil
	}
}

type chunkHeap []*chunkReader

func (h chunkHeap) Len() int { return len(h) }
func (h chunkHeap) Less(i, j int) bool {
	if c := bytes.Compare(h[i].key(), h[j].key()); c != 0 {
		return c < 0
	}
	return h[i].idx < h[j].idx // ties keep insertion order
}
func (h chunkHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *chunkHeap) Push(x any)   { *h = append(*h, x.(*chunkReader)) }
func (h *chunkHeap) Pop() (x any) { old := *h; x = old[len(old)-1]; *h = old[:len(old)-1]; return }

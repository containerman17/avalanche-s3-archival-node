package state

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/ava-labs/libevm/common"
)

// codeStore is the append-only contract-code container: every unique code
// blob ever written, keyed by its hash.
//
//	code.log: [hash 32B][uvarint len][blob]... in first-seen order.
//
// The RAM map hash -> (offset,len of blob) is rebuilt by a sequential scan
// at startup; a torn tail (partial record) is truncated away. Put dedups
// by hash, which also makes replayed blocks free.
type codeStore struct {
	f     *os.File
	off   uint64 // append offset
	idx   map[common.Hash]recLoc
	dirty bool
}

func openCodeStore(dir string) (*codeStore, error) {
	f, err := os.OpenFile(filepath.Join(dir, "code.log"), os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	c := &codeStore{f: f, idx: make(map[common.Hash]recLoc)}
	r := bufio.NewReaderSize(f, 1<<20)
	var (
		pos  uint64
		hash common.Hash
	)
	for {
		if _, err := io.ReadFull(r, hash[:]); err != nil {
			break // clean EOF or torn hash: truncate at pos
		}
		ln, err := binary.ReadUvarint(r)
		if err != nil {
			break
		}
		blobOff := pos + 32 + uint64(uvarintLen(ln))
		if _, err := r.Discard(int(ln)); err != nil {
			break
		}
		c.idx[hash] = recLoc{off: blobOff, ln: uint32(ln)}
		pos = blobOff + ln
	}
	if err := f.Truncate(int64(pos)); err != nil {
		f.Close()
		return nil, err
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		f.Close()
		return nil, err
	}
	c.off = pos
	return c, nil
}

func uvarintLen(v uint64) int {
	var buf [binary.MaxVarintLen64]byte
	return binary.PutUvarint(buf[:], v)
}

// Put appends a blob unless its hash is already stored.
func (c *codeStore) Put(hash common.Hash, blob []byte) error {
	if _, ok := c.idx[hash]; ok {
		return nil
	}
	var lenBuf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(lenBuf[:], uint64(len(blob)))
	buf := make([]byte, 0, 32+n+len(blob))
	buf = append(buf, hash[:]...)
	buf = append(buf, lenBuf[:n]...)
	buf = append(buf, blob...)
	if _, err := c.f.Write(buf); err != nil {
		return fmt.Errorf("code.log write: %w", err)
	}
	c.idx[hash] = recLoc{off: c.off + 32 + uint64(n), ln: uint32(len(blob))}
	c.off += uint64(len(buf))
	c.dirty = true
	return nil
}

// Get returns a copy of the blob for hash, ok=false if unknown.
func (c *codeStore) Get(hash common.Hash) ([]byte, bool, error) {
	loc, ok := c.idx[hash]
	if !ok {
		return nil, false, nil
	}
	buf := make([]byte, loc.ln)
	if _, err := c.f.ReadAt(buf, int64(loc.off)); err != nil {
		return nil, false, fmt.Errorf("code.log read %x: %w", hash, err)
	}
	return buf, true, nil
}

func (c *codeStore) Has(hash common.Hash) bool {
	_, ok := c.idx[hash]
	return ok
}

func (c *codeStore) Count() int { return len(c.idx) }

func (c *codeStore) Sync() error {
	if !c.dirty {
		return nil
	}
	if err := c.f.Sync(); err != nil {
		return err
	}
	c.dirty = false
	return nil
}

func (c *codeStore) Close() error {
	if err := c.Sync(); err != nil {
		c.f.Close()
		return err
	}
	return c.f.Close()
}

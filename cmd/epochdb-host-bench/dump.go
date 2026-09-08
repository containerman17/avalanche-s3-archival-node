package main

import (
	"encoding/binary"
	"fmt"
	"os"
	"syscall"
)

// dumpSource is the block source over a container dump file written by
// cmd/epochdb-dump-containers: records [u64 LE height][u32 LE len][container],
// heights ascending and contiguous from 1. The file is mmapped and indexed
// on open; a served container is copied out (the executor keeps it until the
// checker wrote the block, exactly like the fetcher's heap bytes) and the
// consumed pages are given back with MADV_DONTNEED so the mapping does not
// inflate the bench's rss.
type dumpSource struct {
	mm       []byte
	off      []int64 // off[h-1] is the record start of height h
	from, to uint64
	released int64 // pages below this offset were dropped from the mapping
}

const hdrLen = 12

// releaseEvery is how many consumed bytes accumulate before MADV_DONTNEED.
const releaseEvery = 64 << 20

func openDump(path string, from, to uint64) (*dumpSource, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	mm, err := syscall.Mmap(int(f.Fd()), 0, int(st.Size()), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, fmt.Errorf("mmap %s: %w", path, err)
	}
	d := &dumpSource{mm: mm, from: from}
	var pos int64
	for pos < int64(len(mm)) {
		if pos+hdrLen > int64(len(mm)) {
			d.close()
			return nil, fmt.Errorf("%s: truncated header at %d", path, pos)
		}
		h := binary.LittleEndian.Uint64(mm[pos:])
		n := int64(binary.LittleEndian.Uint32(mm[pos+8:]))
		if want := uint64(len(d.off) + 1); h != want {
			d.close()
			return nil, fmt.Errorf("%s: record %d holds height %d, want %d", path, len(d.off), h, want)
		}
		if pos+hdrLen+n > int64(len(mm)) {
			d.close()
			return nil, fmt.Errorf("%s: height %d: %d bytes past the end", path, h, pos+hdrLen+n-int64(len(mm)))
		}
		d.off = append(d.off, pos)
		pos += hdrLen + n
		d.release(pos)
	}
	d.release(int64(len(mm)))
	d.released = 0
	last := uint64(len(d.off))
	if to == 0 || to > last {
		to = last
	}
	d.to = to
	if from < 1 || from > to {
		d.close()
		return nil, fmt.Errorf("%s: --from %d is outside 1..%d", path, from, to)
	}
	return d, nil
}

// release drops the mapping's pages below pos once releaseEvery of them
// accumulated; the page cache keeps them, a later touch refaults.
func (d *dumpSource) release(pos int64) {
	if pos-d.released < releaseEvery {
		return
	}
	lo := d.released &^ int64(os.Getpagesize()-1)
	syscall.Madvise(d.mm[lo:pos], syscall.MADV_DONTNEED)
	d.released = pos
}

// Last is the last height served (--to or the file's end).
func (d *dumpSource) Last() uint64 { return d.to }

// GetByHeight serves one container: ok=false past --to, an error below
// --from (the executor's start height comes from the store, not from here).
func (d *dumpSource) GetByHeight(n uint64) ([]byte, bool, error) {
	if n > d.to {
		return nil, false, nil
	}
	if n < d.from {
		return nil, false, fmt.Errorf("dump: height %d is below --from %d", n, d.from)
	}
	pos := d.off[n-1]
	sz := int64(binary.LittleEndian.Uint32(d.mm[pos+8:]))
	raw := make([]byte, sz)
	copy(raw, d.mm[pos+hdrLen:pos+hdrLen+sz])
	d.release(pos + hdrLen + sz)
	return raw, true, nil
}

func (d *dumpSource) close() { syscall.Munmap(d.mm) }

package state

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/ava-labs/libevm/common"
	"github.com/containerman17/epochdb/dist"
)

// OpenReadOnly opens the state layer without EVER writing or truncating:
// safe on a data dir owned by a live fetch/exec process. The regular Open
// applies the torn-tail truncation rule with O_RDWR handles, which would
// corrupt a log another process is appending to; here torn tails are just
// skipped in memory. The view is a snapshot of what was durable at open
// time. Appends/flushes on a read-only store are not supported.
func OpenReadOnly(dir string) (*Store, error) {
	wl, err := openBucketLogRO(dir, "writelog")
	if err != nil {
		return nil, fmt.Errorf("open writelog ro: %w", err)
	}
	hd, err := openBucketLogRO(dir, "headers")
	if err != nil {
		wl.Close()
		return nil, fmt.Errorf("open headers ro: %w", err)
	}
	lg, err := openBucketLogRO(dir, "logs")
	if err != nil {
		wl.Close()
		hd.Close()
		return nil, fmt.Errorf("open logs ro: %w", err)
	}
	// A missing rcpt family is an EMPTY store, not an error: openBucketLogRO
	// finds no sidecars and stops there, which is exactly the download-
	// bootstrapped node that never executed a block.
	rc, err := openBucketLogRO(dir, rcptPrefix)
	if err != nil {
		wl.Close()
		hd.Close()
		lg.Close()
		return nil, fmt.Errorf("open receipts ro: %w", err)
	}
	code, err := openCodeStoreRO(dir)
	if err != nil {
		wl.Close()
		hd.Close()
		lg.Close()
		rc.Close()
		return nil, fmt.Errorf("open code store ro: %w", err)
	}
	misc, err := openMiscStoreRO(dir)
	if err != nil {
		wl.Close()
		hd.Close()
		lg.Close()
		rc.Close()
		code.Close()
		return nil, fmt.Errorf("open misc store ro: %w", err)
	}
	cas, err := dist.Open(dir)
	if err != nil {
		wl.Close()
		hd.Close()
		lg.Close()
		rc.Close()
		code.Close()
		misc.Close()
		return nil, err
	}
	s := &Store{dir: dir, cas: cas, wl: wl, hd: hd, lg: lg, rc: rc, code: code, misc: misc}
	raw, err := os.ReadFile(filepath.Join(dir, execHeadFile))
	if err == nil && len(raw) == 8 {
		s.execHead = binary.BigEndian.Uint64(raw)
		s.execHeadOK = true
	} else if err != nil && !os.IsNotExist(err) {
		s.Close()
		return nil, fmt.Errorf("read exechead: %w", err)
	}
	raw, err = os.ReadFile(filepath.Join(dir, logsStartFile))
	if err == nil && len(raw) == 8 {
		s.logsStart = binary.BigEndian.Uint64(raw)
		s.logsStartOK = true
	} else if err != nil && !os.IsNotExist(err) {
		s.Close()
		return nil, fmt.Errorf("read logs.start: %w", err)
	}
	return s, nil
}

// RescanRO consumes everything the WRITING process appended since this
// read-only store was opened or last rescanned: write frames, headers, log
// records, receipt records and code blobs. Calling it in a loop is what makes
// a read-only opener follow a live `epochdb serve` at its head instead of
// serving an open-time snapshot (the SDK's tailing loop).
//
// Ordering, which is what makes the head signal safe: the writer appends a
// block's payload before its index record and its write frame before its
// header, so a visible header record implies every earlier append of that
// block is visible too. HeadersMax() after this call is therefore the highest
// FULLY appended block.
//
// misc.log is deliberately not rescanned: it holds the handful of static
// rawdb keys written at genesis, which no read path re-reads.
func (s *Store) RescanRO() error {
	for _, l := range []*bucketLog{s.wl, s.hd, s.lg, s.rc} {
		if err := l.rescanRO(); err != nil {
			return err
		}
	}
	return s.code.rescanRO(s.dir)
}

// openBucketLogRO mirrors openBucketLog but never truncates: torn index or
// data tails are skipped in memory. Reads still go through pair(), which
// opens existing files only (an indexed block implies its data file exists).
func openBucketLogRO(dir, prefix string) (*bucketLog, error) {
	l := &bucketLog{
		dir:    dir,
		prefix: prefix,
		idx:    make(map[uint64]recLoc),
		pairs:  make(map[uint64]*blPair),
		roOff:  make(map[uint64]int64),
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
		if err := l.scanRO(bucket); err != nil {
			l.Close()
			return nil, fmt.Errorf("%s: scan bucket %05d: %w", prefix, bucket, err)
		}
	}
	return l, nil
}

// scanRO is rebuild() minus the truncation, and RESUMABLE: it consumes only
// the index records past roOff[bucket] and leaves an incomplete tail record
// unconsumed, so calling it again picks up whatever the writer appended
// meanwhile. Caller holds l.mu (rescanRO does; open has no other goroutine).
func (l *bucketLog) scanRO(bucket uint64) error {
	index, err := os.Open(filepath.Join(l.dir, l.idxName(bucket)))
	if err != nil {
		return err
	}
	defer index.Close()
	off := l.roOff[bucket]
	l.roOff[bucket] = off // register the bucket even if nothing is readable yet
	dst, err := os.Stat(filepath.Join(l.dir, l.dataName(bucket)))
	if err != nil {
		if os.IsNotExist(err) {
			return nil // sidecar without data yet: nothing visible
		}
		return err
	}
	dataSize := uint64(dst.Size())
	if _, err := index.Seek(off, io.SeekStart); err != nil {
		return err
	}

	var rec [blIdxRecSize]byte
	r := bufio.NewReaderSize(index, 1<<20)
	for {
		if _, err := io.ReadFull(r, rec[:]); err != nil {
			return nil // EOF or torn index record: stop consuming
		}
		block := binary.BigEndian.Uint64(rec[0:8])
		recOff := binary.BigEndian.Uint64(rec[8:16])
		ln := binary.BigEndian.Uint32(rec[16:20])
		if recOff+uint64(ln) > dataSize {
			return nil // payload not fully on disk yet
		}
		l.idx[block] = recLoc{off: recOff, ln: ln}
		l.bytes += uint64(ln)
		if !l.any || block > l.maxBlock {
			l.maxBlock = block
			l.any = true
		}
		off += blIdxRecSize
		l.roOff[bucket] = off
	}
}

// rescanRO consumes whatever the writer appended since the last scan. It is
// what turns a read-only opener from an open-time snapshot into a FOLLOWER
// of a live writer (the SDK's tailing loop).
//
// No ReadDir: blocks arrive ascending, so the only bucket that can appear is
// the one after the highest block seen, and one speculative open costs a
// syscall instead of a directory listing per poll.
func (l *bucketLog) rescanRO() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.roOff == nil {
		return nil // writer log: it already knows everything it wrote
	}
	for bucket := range l.roOff {
		if err := l.scanRO(bucket); err != nil {
			if os.IsNotExist(err) {
				delete(l.roOff, bucket) // retired by a sibling seal or fold
				continue
			}
			return fmt.Errorf("%s: rescan bucket %05d: %w", l.prefix, bucket, err)
		}
	}
	next := uint64(0)
	if l.any {
		next = l.maxBlock/BucketBlocks + 1
	}
	if _, seen := l.roOff[next]; !seen {
		if err := l.scanRO(next); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("%s: rescan bucket %05d: %w", l.prefix, next, err)
		}
	}
	return nil
}

// openCodeStoreRO scans code.log read-only without truncating the tail. A
// missing code.log is an empty hot tail, not an error: a download-bootstrapped
// node never ran an executor, and format v3 epochs carry the code (as does
// the base file in limited-history mode).
func openCodeStoreRO(dir string) (*codeStore, error) {
	f, err := os.Open(filepath.Join(dir, "code.log"))
	if os.IsNotExist(err) {
		return &codeStore{idx: make(map[common.Hash]recLoc)}, nil
	}
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
			break
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
	c.off = pos
	return c, nil
}

// rescanRO reads the code blobs appended since the last scan, so a reader
// process serves eth_getCode for a contract the writer deployed seconds ago.
// code.log is one flat append-only file, so resuming is a seek to c.off.
func (c *codeStore) rescanRO(dir string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.f == nil {
		// Absent at open (a node that never executed) and created since.
		f, err := os.Open(filepath.Join(dir, "code.log"))
		if os.IsNotExist(err) {
			return nil
		}
		if err != nil {
			return err
		}
		c.f = f
	}
	st, err := c.f.Stat()
	if err != nil {
		return err
	}
	if uint64(st.Size()) <= c.off {
		return nil
	}
	if _, err := c.f.Seek(int64(c.off), io.SeekStart); err != nil {
		return err
	}
	r := bufio.NewReaderSize(c.f, 1<<20)
	var (
		pos  = c.off
		hash common.Hash
	)
	for {
		if _, err := io.ReadFull(r, hash[:]); err != nil {
			break
		}
		ln, err := binary.ReadUvarint(r)
		if err != nil {
			break
		}
		blobOff := pos + 32 + uint64(uvarintLen(ln))
		if _, err := r.Discard(int(ln)); err != nil {
			break // blob bytes not fully visible yet: retry from pos next time
		}
		c.idx[hash] = recLoc{off: blobOff, ln: uint32(ln)}
		pos = blobOff + ln
	}
	c.off = pos
	return nil
}

// openMiscStoreRO replays misc.log read-only without truncating the tail.
// Absent on a download-bootstrapped node (nothing ever executed there), which
// is an empty map, not an error.
func openMiscStoreRO(dir string) (*miscStore, error) {
	f, err := os.Open(filepath.Join(dir, "misc.log"))
	if os.IsNotExist(err) {
		return &miscStore{m: make(map[string][]byte)}, nil
	}
	if err != nil {
		return nil, err
	}
	s := &miscStore{f: f, m: make(map[string][]byte)}
	r := bufio.NewReaderSize(f, 1<<20)
	var pos uint64
	for {
		end, op, k, v, err := readMiscRecord(r, pos)
		if err != nil {
			break
		}
		if op == 0 {
			s.m[string(k)] = v
		} else {
			delete(s.m, string(k))
		}
		pos = end
	}
	return s, nil
}

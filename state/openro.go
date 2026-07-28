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
	code, err := openCodeStoreRO(dir)
	if err != nil {
		wl.Close()
		hd.Close()
		lg.Close()
		return nil, fmt.Errorf("open code store ro: %w", err)
	}
	misc, err := openMiscStoreRO(dir)
	if err != nil {
		wl.Close()
		hd.Close()
		lg.Close()
		code.Close()
		return nil, fmt.Errorf("open misc store ro: %w", err)
	}
	s := &Store{dir: dir, wl: wl, hd: hd, lg: lg, code: code, misc: misc}
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

// openBucketLogRO mirrors openBucketLog but never truncates: torn index or
// data tails are skipped in memory. Reads still go through pair(), which
// opens existing files only (an indexed block implies its data file exists).
func openBucketLogRO(dir, prefix string) (*bucketLog, error) {
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
		if err := l.scanRO(bucket); err != nil {
			l.Close()
			return nil, fmt.Errorf("%s: scan bucket %05d: %w", prefix, bucket, err)
		}
	}
	return l, nil
}

// scanRO is rebuild() minus the truncation.
func (l *bucketLog) scanRO(bucket uint64) error {
	index, err := os.Open(filepath.Join(l.dir, l.idxName(bucket)))
	if err != nil {
		return err
	}
	defer index.Close()
	dst, err := os.Stat(filepath.Join(l.dir, l.dataName(bucket)))
	if err != nil {
		if os.IsNotExist(err) {
			return nil // sidecar without data yet: nothing visible
		}
		return err
	}
	dataSize := uint64(dst.Size())

	var rec [blIdxRecSize]byte
	r := bufio.NewReaderSize(index, 1<<20)
	for {
		if _, err := io.ReadFull(r, rec[:]); err != nil {
			return nil // EOF or torn index record: stop consuming
		}
		block := binary.BigEndian.Uint64(rec[0:8])
		off := binary.BigEndian.Uint64(rec[8:16])
		ln := binary.BigEndian.Uint32(rec[16:20])
		if off+uint64(ln) > dataSize {
			return nil // payload not fully on disk yet
		}
		l.idx[block] = recLoc{off: off, ln: ln}
		l.bytes += uint64(ln)
		if !l.any || block > l.maxBlock {
			l.maxBlock = block
			l.any = true
		}
	}
}

// openCodeStoreRO scans code.log read-only without truncating the tail. A
// missing code.log is an empty hot tail, not an error: a torrent-bootstrapped
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

// openMiscStoreRO replays misc.log read-only without truncating the tail.
// Absent on a torrent-bootstrapped node (nothing ever executed there), which
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

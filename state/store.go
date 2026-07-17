package state

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"

	"github.com/ava-labs/libevm/common"
)

// Store is the executor's flat-file state layer inside the data directory:
//
//	writelog_NNNNN.log (+_idx_) post-image write capture, one frame per block
//	headers_NNNNN.log  (+_idx_) RLP headers, one per executed block
//	code.log                    every contract code blob, by hash
//	misc.log                    the few non-code rawdb keys
//	exechead                    executorHead: last group-fsynced height
//
// No database. Appends are unfsynced per block; FlushAndSetExecHead fsyncs
// the whole group and then advances the durable executorHead, so exechead
// never claims more than what is on disk.
type Store struct {
	dir  string
	wl   *bucketLog
	hd   *bucketLog
	code *codeStore
	misc *miscStore

	execHead   uint64
	execHeadOK bool
}

const execHeadFile = "exechead"

// Open opens (or creates) the state layer inside dir.
func Open(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	wl, err := openBucketLog(dir, "writelog")
	if err != nil {
		return nil, fmt.Errorf("open writelog: %w", err)
	}
	hd, err := openBucketLog(dir, "headers")
	if err != nil {
		wl.Close()
		return nil, fmt.Errorf("open headers: %w", err)
	}
	code, err := openCodeStore(dir)
	if err != nil {
		wl.Close()
		hd.Close()
		return nil, fmt.Errorf("open code store: %w", err)
	}
	misc, err := openMiscStore(dir)
	if err != nil {
		wl.Close()
		hd.Close()
		code.Close()
		return nil, fmt.Errorf("open misc store: %w", err)
	}
	s := &Store{dir: dir, wl: wl, hd: hd, code: code, misc: misc}
	raw, err := os.ReadFile(filepath.Join(dir, execHeadFile))
	if err == nil && len(raw) == 8 {
		s.execHead = binary.BigEndian.Uint64(raw)
		s.execHeadOK = true
	} else if err != nil && !os.IsNotExist(err) {
		s.Close()
		return nil, fmt.Errorf("read exechead: %w", err)
	}
	return s, nil
}

// Close flushes everything and releases file handles. It does NOT advance
// exechead: only FlushAndSetExecHead does that.
func (s *Store) Close() error {
	var firstErr error
	for _, c := range []func() error{s.wl.Close, s.hd.Close, s.code.Close, s.misc.Close} {
		if err := c(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// ExecHead returns the durable executor head. ok=false on a fresh store.
func (s *Store) ExecHead() (uint64, bool) { return s.execHead, s.execHeadOK }

// FlushAndSetExecHead fsyncs every dirty file (writelog, headers, code,
// misc) and only then persists executorHead = n via tmp+rename.
func (s *Store) FlushAndSetExecHead(n uint64) error {
	for _, f := range []func() error{s.wl.Sync, s.hd.Sync, s.code.Sync, s.misc.Sync} {
		if err := f(); err != nil {
			return err
		}
	}
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], n)
	tmp := filepath.Join(s.dir, execHeadFile+".tmp")
	if err := os.WriteFile(tmp, buf[:], 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, filepath.Join(s.dir, execHeadFile)); err != nil {
		return err
	}
	s.execHead = n
	s.execHeadOK = true
	return nil
}

// AppendWrites stores block's post-image write frame. Idempotent per block.
func (s *Store) AppendWrites(block uint64, frame []byte) error {
	return s.wl.Append(block, frame)
}

// HasWrites reports whether block already has a writelog frame.
func (s *Store) HasWrites(block uint64) bool { return s.wl.Has(block) }

// AppendHeader stores block's RLP header. Idempotent per block.
func (s *Store) AppendHeader(block uint64, headerRLP []byte) error {
	return s.hd.Append(block, headerRLP)
}

// HeaderRLP returns the stored RLP header for block.
func (s *Store) HeaderRLP(block uint64) ([]byte, bool, error) { return s.hd.Get(block) }

// HeadersMax returns the highest stored header height, ok=false if none.
func (s *Store) HeadersMax() (uint64, bool) { return s.hd.Max() }

// PutCode stores a contract code blob by hash (dedup by hash).
func (s *Store) PutCode(hash common.Hash, blob []byte) error { return s.code.Put(hash, blob) }

// CodeCount returns the number of unique code blobs stored.
func (s *Store) CodeCount() int { return s.code.Count() }

// WritelogBytes returns total writelog payload bytes on disk.
func (s *Store) WritelogBytes() uint64 { return s.wl.Bytes() }

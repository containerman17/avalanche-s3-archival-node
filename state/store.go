package state

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"

	"github.com/ava-labs/libevm/common"
	"github.com/containerman17/epochdb/dist"
)

// Store is the executor's flat-file state layer inside the data directory:
//
//	writelog_NNNNN.log (+_idx_) post-image write capture, one frame per block
//	headers_NNNNN.log  (+_idx_) RLP headers, one per executed block
//	logs_NNNNN.log     (+_idx_) per-block unique event-log addrs+topics
//	rcpt_NNNNN.log     (+_idx_) per-block receipts + full logs, epoch encoding
//	code.log                    every contract code blob, by hash
//	misc.log                    the few non-code rawdb keys
//	exechead                    executorHead: last group-fsynced height
//	logs.start                  first height with event-log capture
//
// No database. Appends are unfsynced per block; FlushAndSetExecHead fsyncs
// the whole group and then advances the durable executorHead, so exechead
// never claims more than what is on disk.
type Store struct {
	dir  string
	cas  *dist.Store
	wl   *bucketLog
	hd   *bucketLog
	lg   *bucketLog
	rc   *bucketLog
	code *codeStore
	misc *miscStore

	// tail is the uncooked-write overlay, non-nil only when a History
	// enabled it (serve). Set before the executor goroutine starts.
	tail *tail

	execHead   uint64
	execHeadOK bool

	logsStart   uint64
	logsStartOK bool
}

const (
	execHeadFile  = "exechead"
	logsStartFile = "logs.start"

	// rcptPrefix names the live receipts+full-logs tail family. Its records
	// are the epoch stored sections verbatim (see state/storedlogs.go), so
	// serving the unsealed tail and sealing an epoch read the same bytes.
	rcptPrefix = "rcpt"
)

// Open opens (or creates) the state layer inside dir.
func Open(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	cas, err := dist.Open(dir)
	if err != nil {
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
	lg, err := openBucketLog(dir, "logs")
	if err != nil {
		wl.Close()
		hd.Close()
		return nil, fmt.Errorf("open logs: %w", err)
	}
	rc, err := openBucketLog(dir, rcptPrefix)
	if err != nil {
		wl.Close()
		hd.Close()
		lg.Close()
		return nil, fmt.Errorf("open receipts: %w", err)
	}
	code, err := openCodeStore(dir)
	if err != nil {
		wl.Close()
		hd.Close()
		lg.Close()
		rc.Close()
		return nil, fmt.Errorf("open code store: %w", err)
	}
	misc, err := openMiscStore(dir)
	if err != nil {
		wl.Close()
		hd.Close()
		lg.Close()
		rc.Close()
		code.Close()
		return nil, fmt.Errorf("open misc store: %w", err)
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

// Close flushes everything and releases file handles. It does NOT advance
// exechead: only FlushAndSetExecHead does that.
//
// It also closes the artifact store, which is what marks the casfs chunk cache
// CLEAN: a process that dies without this starts with a cold cache (casfs
// cannot tell a torn chunk from a whole one, so it wipes rather than journals).
// Costs the cache and nothing durable.
func (s *Store) Close() error {
	var firstErr error
	for _, c := range []func() error{s.wl.Close, s.hd.Close, s.lg.Close, s.rc.Close, s.code.Close, s.misc.Close, s.cas.Close} {
		if err := c(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// ExecHead returns the durable executor head. ok=false on a fresh store.
func (s *Store) ExecHead() (uint64, bool) { return s.execHead, s.execHeadOK }

// RetireBuckets drops this store's handles and index entries for the raw
// buckets a seal has just deleted (History.SealTail's last step). Without it
// the delete frees nothing: the writer keeps the unlinked files open, so their
// blocks stay allocated for the life of the process.
func (s *Store) RetireBuckets(sealedEnd uint64) error {
	var firstErr error
	for _, l := range []*bucketLog{s.wl, s.hd, s.lg, s.rc} {
		if err := l.retire(sealedEnd); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// FlushAndSetExecHead fsyncs every dirty file (writelog, headers, code,
// misc) and only then persists executorHead = n via tmp+rename.
func (s *Store) FlushAndSetExecHead(n uint64) error {
	for _, f := range []func() error{s.wl.Sync, s.hd.Sync, s.lg.Sync, s.rc.Sync, s.code.Sync, s.misc.Sync} {
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
//
// This is also where the uncooked-tail overlay is fed (serve, via
// History.EnableTail): every execution path (per-block, batched, SAE, crash
// re-execution) already funnels its frames through here, so the overlay sees
// the same stream the writelog does with nothing re-derived. The overlay is
// updated BEFORE the append so a reader can never see a write that is in the
// writelog (which the descent ignores until it is cooked) but not yet in the
// overlay; the block is already root-verified at this point and its head is
// not published until later, so the other order is the only unsafe one.
func (s *Store) AppendWrites(block uint64, frame []byte) error {
	if s.tail != nil {
		if err := s.tail.apply(block, frame); err != nil {
			return fmt.Errorf("tail overlay block %d: %w", block, err)
		}
	}
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

// AppendLogs stores block's event-log index record (unique addrs+topics).
// Blocks without logs get no record. Idempotent per block.
func (s *Store) AppendLogs(block uint64, rec []byte) error {
	return s.lg.Append(block, rec)
}

// LogsRecord returns the stored event-log record for block. ok=false means
// no logs in that block (or the block predates capture, see LogsStart).
func (s *Store) LogsRecord(block uint64) ([]byte, bool, error) { return s.lg.Get(block) }

// AppendRcpt stores block's receipts+full-logs record (EncodeTailRcpt).
// Blocks with no transactions get no record. Idempotent per block.
func (s *Store) AppendRcpt(block uint64, rec []byte) error { return s.rc.Append(block, rec) }

// RcptRecord returns block's captured receipts+full-logs record. ok=false
// means the block had no transactions, or predates capture entirely (an old
// corpus, or a node that never executed): the two are indistinguishable here
// on purpose, callers that need the difference check the tx count.
func (s *Store) RcptRecord(block uint64) ([]byte, bool, error) { return s.rc.Get(block) }

// RcptBytes returns total captured receipt+logs payload bytes on disk.
func (s *Store) RcptBytes() uint64 { return s.rc.Bytes() }

// LogsBytes returns total logs payload bytes on disk.
func (s *Store) LogsBytes() uint64 { return s.lg.Bytes() }

// LogsStart returns the first height with event-log capture, ok=false if
// capture never started.
func (s *Store) LogsStart() (uint64, bool) { return s.logsStart, s.logsStartOK }

// SetLogsStart persists the capture start height (write-once marker; the
// backfill job covers blocks 0..start-1).
func (s *Store) SetLogsStart(h uint64) error {
	if s.logsStartOK {
		return nil
	}
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], h)
	tmp := filepath.Join(s.dir, logsStartFile+".tmp")
	if err := os.WriteFile(tmp, buf[:], 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, filepath.Join(s.dir, logsStartFile)); err != nil {
		return err
	}
	s.logsStart = h
	s.logsStartOK = true
	return nil
}

// vmKindKey stamps the data dir with the VM kind that built it, inside misc.
// Namespaced so it cannot collide with a rawdb key.
const vmKindKey = "epochdb.vmkind"

// BindVMKind claims the data dir for one VM kind, or refuses it for the other.
// coreth and subnet-evm header RLP are mutually exclusive and libevm's extras
// registry is process-global, so a dir built by one is unreadable by the other
// and there is NO migration: delete the corpus and resync.
func (s *Store) BindVMKind(kind string) error {
	if cur, ok := s.misc.Get([]byte(vmKindKey)); ok {
		if string(cur) != kind {
			return fmt.Errorf("state: data dir was built by %s, refusing to open it as %s: delete the corpus and resync", cur, kind)
		}
		return nil
	}
	return s.misc.Put([]byte(vmKindKey), []byte(kind))
}

// PutCode stores a contract code blob by hash (dedup by hash).
func (s *Store) PutCode(hash common.Hash, blob []byte) error { return s.code.Put(hash, blob) }

// CodeCount returns the number of unique code blobs stored.
func (s *Store) CodeCount() int { return s.code.Count() }

// WritelogBytes returns total writelog payload bytes on disk.
func (s *Store) WritelogBytes() uint64 { return s.wl.Bytes() }

// Cas is the process's artifact store for this data directory: sealed epochs,
// artifacts and the `latest` pointer (dist). One per state layer, so one chunk
// cache per process.
func (s *Store) Cas() *dist.Store { return s.cas }

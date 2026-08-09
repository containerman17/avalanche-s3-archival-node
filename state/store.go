package state

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/ava-labs/libevm/common"
	"github.com/containerman17/epochdb/dist"
)

// Store is the executor's flat-file state layer inside the data directory:
//
//	writelog_NNNNN.log (+_idx_) post-image write capture, one frame per block
//	headers_NNNNN.log  (+_idx_) RLP headers, one per executed block
//	logs_NNNNN.log     (+_idx_) per-block unique event-log addrs+topics
//	rcpt_NNNNN.log     (+_idx_) per-block receipts + full logs, epoch encoding
//	itx_NNNNN.log      (+_idx_) per-block call frames + address participants
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
	ix   *bucketLog
	code *codeStore
	misc *miscStore

	// epochs is THE sealed epoch set of this data directory: the RPC descent
	// (History), the executor's rawdb (ethdb.go) and the sealer (seal.go) all
	// share it, so an in-process seal's Reload reaches all three at once.
	// Opened on FIRST USE, because opening it reads every epoch's footer and
	// those may live in the bucket: a store that only writes (fetch, cook, the
	// sealer's read-only opener) must keep working with no credentials at all
	// (TestSealTailNeedsNoBucket).
	epochsOnce sync.Once
	epochs     *EpochSet
	epochsErr  error

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

	// itxPrefix names the live call-frames tail family (V2). Its frames half
	// is the epoch stored-frames encoding verbatim; its participants half is
	// the address index's input and never reaches an epoch.
	itxPrefix = "itx"
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
	ix, err := openBucketLog(dir, itxPrefix)
	if err != nil {
		wl.Close()
		hd.Close()
		lg.Close()
		rc.Close()
		return nil, fmt.Errorf("open frames: %w", err)
	}
	code, err := openCodeStore(dir)
	if err != nil {
		wl.Close()
		hd.Close()
		lg.Close()
		rc.Close()
		ix.Close()
		return nil, fmt.Errorf("open code store: %w", err)
	}
	misc, err := openMiscStore(dir)
	if err != nil {
		wl.Close()
		hd.Close()
		lg.Close()
		rc.Close()
		ix.Close()
		code.Close()
		return nil, fmt.Errorf("open misc store: %w", err)
	}
	s := &Store{dir: dir, cas: cas, wl: wl, hd: hd, lg: lg, rc: rc, ix: ix, code: code, misc: misc}
	head, ok, err := ExecHead(dir)
	if err != nil {
		s.Close()
		return nil, err
	}
	s.execHead, s.execHeadOK = head, ok
	if err := s.loadLogsStart(); err != nil {
		s.Close()
		return nil, err
	}
	return s, nil
}

// loadLogsStart reads logs.start, the first height with event-log capture.
// Absent means capture never started. PRESENT BUT NOT 8 BYTES IS AN ERROR, not
// a third silent synonym for absent: falling through both branches made exec
// re-stamp the file at its current head and silently declare every block below
// it log-free.
func (s *Store) loadLogsStart() error {
	raw, err := os.ReadFile(filepath.Join(s.dir, logsStartFile))
	switch {
	case err == nil && len(raw) == 8:
		s.logsStart = binary.BigEndian.Uint64(raw)
		s.logsStartOK = true
	case err == nil:
		return fmt.Errorf("logs.start is %d bytes, want 8: the capture floor is unreadable, so no read can tell a log-free block from an uncaptured one", len(raw))
	case !os.IsNotExist(err):
		return fmt.Errorf("read logs.start: %w", err)
	}
	return nil
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
	if s.epochs != nil {
		s.epochs.Close()
		s.epochs = nil
	}
	for _, c := range []func() error{s.wl.Close, s.hd.Close, s.lg.Close, s.rc.Close, s.ix.Close, s.code.Close, s.misc.Close, s.cas.Close} {
		if err := c(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// ExecHead returns the durable executor head. ok=false on a fresh store.
func (s *Store) ExecHead() (uint64, bool) { return s.execHead, s.execHeadOK }

// ExecHead reads a data dir's durable executor head WITHOUT opening the state
// layer: it is one 8-byte file, while Open builds a RAM index over every
// unsealed raw bucket (182 B/block, see DESIGN.md's seal-memory section). The
// startup decision tree only needs to know whether this dir has ever executed
// anything (cmd/epochdb joinChain), so it must not pay that twice.
func ExecHead(dir string) (uint64, bool, error) {
	raw, err := os.ReadFile(filepath.Join(dir, execHeadFile))
	switch {
	case err == nil && len(raw) == 8:
		return binary.BigEndian.Uint64(raw), true, nil
	case err != nil && !os.IsNotExist(err):
		return 0, false, fmt.Errorf("read exechead: %w", err)
	}
	return 0, false, nil
}

// RetireBuckets drops this store's handles and index entries for the raw
// buckets a seal has just deleted (History.SealTail's last step). Without it
// the delete frees nothing: the writer keeps the unlinked files open, so their
// blocks stay allocated for the life of the process.
func (s *Store) RetireBuckets(sealedEnd uint64) error {
	var firstErr error
	for _, l := range []*bucketLog{s.wl, s.hd, s.lg, s.rc, s.ix} {
		if err := l.retire(sealedEnd); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// FlushAndSetExecHead fsyncs every dirty file (writelog, headers, code,
// misc) and only then persists executorHead = n via tmp+rename.
func (s *Store) FlushAndSetExecHead(n uint64) error {
	for _, f := range []func() error{s.wl.Sync, s.hd.Sync, s.lg.Sync, s.rc.Sync, s.ix.Sync, s.code.Sync, s.misc.Sync} {
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

// Writes returns block's raw write-capture frame, for a caller that wants the
// executor's post-images back out (FrameRows decodes one).
func (s *Store) Writes(block uint64) ([]byte, bool, error) { return s.wl.Get(block) }

// AppendHeader stores block's RLP header. Idempotent per block.
func (s *Store) AppendHeader(block uint64, headerRLP []byte) error {
	return s.hd.Append(block, headerRLP)
}

// RawHeaderRLP returns the RLP header from the RAW headers family only, with
// NO epoch fallback: not-found is the honest answer for "is this height still
// in the unsealed tail", which is all the sealer wants. Every other caller
// wants HeaderRLP, because seal RETIRES the raw buckets it sealed, and below
// the sealed end this answers "absent" for every header the node still has.
func (s *Store) RawHeaderRLP(block uint64) ([]byte, bool, error) { return s.hd.Get(block) }

// HeaderRLP resolves a block header: the raw headers family first (the hot
// tail), then the sealed epochs. Same rule and same reason as Code: on a node
// whose raw buckets were retired by seal, or one that joined from a bucket and
// never executed, the epochs are the ONLY place the header exists.
//
// It lives on the Store and not on History because the EXECUTOR needs it too:
// chainContext.GetHeader serves BLOCKHASH from here, and a raw-only read there
// returns nil, which libevm reads as "no such header" and turns into the ZERO
// HASH inside a running contract: wrong logs, written to disk, and no longer
// distinguishable from real ones.
func (s *Store) HeaderRLP(block uint64) ([]byte, bool, error) {
	raw, ok, err := s.hd.Get(block)
	if err != nil || ok {
		return raw, ok, err
	}
	set, err := s.Epochs()
	if err != nil {
		return nil, false, err
	}
	e, ok := set.At(block)
	if !ok {
		return nil, false, nil
	}
	hdr, err := e.HeaderRLP(block)
	if err != nil {
		return nil, false, err
	}
	return hdr, true, nil
}

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

// LogsMax returns the highest block that has a stored logs record, ok=false if
// none. It is the hot-tail log index's catch-up ceiling: a header is appended
// before its logs record, so the header height is not proof the record landed.
func (s *Store) LogsMax() (uint64, bool) { return s.lg.Max() }

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

// AppendItx stores block's call-frames + participants record (EncodeTailItx).
// Blocks with no transactions get no record. Idempotent per block.
func (s *Store) AppendItx(block uint64, rec []byte) error { return s.ix.Append(block, rec) }

// ItxRecord returns block's captured frames+participants record. ok=false
// means the block had no transactions, or predates frame capture entirely
// (a V1 corpus); seal tells the two apart by the block's tx count.
func (s *Store) ItxRecord(block uint64) ([]byte, bool, error) { return s.ix.Get(block) }

// ItxBytes returns total captured frame payload bytes on disk.
func (s *Store) ItxBytes() uint64 { return s.ix.Bytes() }

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

// frontierFloorKey records the height a bootstrap frontier merge parked this
// data dir at, inside misc. Namespaced so it cannot collide with a rawdb key.
const frontierFloorKey = "epochdb.frontierfloor"

// FrontierFloor is the height `bootstrap --frontier` merged and verified this
// dir's state at, ok=false for a dir that executed its way up from genesis.
// NOTHING BELOW IT WAS EVER EXECUTED HERE, which is what the ACP-194 settled
// -root check needs to know: a header settling a height below the floor names
// a block this node has no post-execution root for (the floor itself was
// verified, against the header that settles it).
func (s *Store) FrontierFloor() (uint64, bool) {
	v, ok := s.misc.Get([]byte(frontierFloorKey))
	if !ok || len(v) != 8 {
		return 0, false
	}
	return binary.BigEndian.Uint64(v), true
}

// SetFrontierFloor stamps the height a frontier merge parked at.
func (s *Store) SetFrontierFloor(h uint64) error {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], h)
	return s.misc.Put([]byte(frontierFloorKey), buf[:])
}

// PutCode stores a contract code blob by hash (dedup by hash).
func (s *Store) PutCode(hash common.Hash, blob []byte) error { return s.code.Put(hash, blob) }

// CodeCount returns the number of unique code blobs stored.
func (s *Store) CodeCount() int { return s.code.Count() }

// Code resolves a contract code blob: code.log first (the hot tail, every blob
// this node's own executor ever wrote), then the sealed epochs
// newest-to-oldest, bloom-gated. Not found is (nil, false, nil).
//
// THE EPOCH DESCENT IS NOT A SERVING NICETY, IT IS THE EXECUTION PATH. A node
// that joined from a bucket replayed nothing, so its code.log is EMPTY, and
// the format v3 'c' rows are the only code it has: the placement rule puts an
// account's code in every epoch that writes that account. This is why it lives
// on the Store and not on History: the executor's rawdb (state/ethdb.go), the
// RPC descent (History.CodeByHash) and the sealer (seal.go) all resolve code
// through this one function. Fuji, 2026-08-04: the executor had only code.log
// and died on the first block past a merged frontier.
//
// ponytail: no memo of an epoch hit back into code.log. libevm's cachingDB
// holds a 64MB code LRU in front of every ContractCode call, so the descent is
// paid once per blob per process. Revisit only if a profile shows it.
func (s *Store) Code(hash common.Hash) ([]byte, bool, error) {
	blob, ok, err := s.code.Get(hash)
	if err != nil || ok {
		return blob, ok, err
	}
	set, err := s.Epochs()
	if err != nil {
		return nil, false, err
	}
	eps := set.All()
	for i := len(eps) - 1; i >= 0; i-- {
		if blob, ok, err = eps[i].Code(hash); err != nil || ok {
			return blob, ok, err
		}
	}
	return nil, false, nil
}

// Epochs is the sealed epoch set of this data directory, opened on first use
// (see the field).
func (s *Store) Epochs() (*EpochSet, error) {
	s.epochsOnce.Do(func() { s.epochs, s.epochsErr = OpenEpochSet(s.cas) })
	return s.epochs, s.epochsErr
}

// WritelogBytes returns total writelog payload bytes on disk.
func (s *Store) WritelogBytes() uint64 { return s.wl.Bytes() }

// Cas is the process's artifact store for this data directory: sealed epochs,
// artifacts and the `latest` pointer (dist). One per state layer, so one chunk
// cache per process.
func (s *Store) Cas() *dist.Store { return s.cas }

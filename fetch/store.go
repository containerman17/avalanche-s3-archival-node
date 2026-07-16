package fetch

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ava-labs/avalanchego/ids"
)

// Store is an append-only flat-file container store. No database.
//
//	arrival.log: [uvarint len][container bytes]... in arrival order.
//	index.log:   fixed 84-byte records, one per container, appended as
//	             containers land: height(8 BE) containerID(32) blockHash(32)
//	             offset(8 BE, into arrival.log at the container bytes)
//	             len(4 BE).
//
// Startup rebuilds the RAM maps from a sequential scan of index.log. If the
// tail of index.log points past the end of arrival.log (torn write), both
// files are truncated back to the last consistent record.
type Store struct {
	mu      sync.RWMutex
	arrival *os.File
	index   *os.File

	arrivalOff uint64 // append position in arrival.log
	byHeight   map[uint64]heightRec
	byID       map[ids.ID]uint64 // containerID -> height
	head       uint64
	haveAny    bool

	sessionBytes atomic.Uint64
}

type heightRec struct {
	id  ids.ID
	off uint64
	ln  uint32
}

const indexRecSize = 8 + 32 + 32 + 8 + 4 // 84

// BlockEvent is a single "block is present locally" notification emitted by
// Subscribe. Raw is a fresh copy safe to retain.
type BlockEvent struct {
	BlockNumber uint64
	ContainerID ids.ID
	Raw         []byte
}

// OpenStore opens (creating if needed) the flat files in dir and rebuilds
// the RAM index from index.log, applying the torn-tail truncation rule.
func OpenStore(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	arrival, err := os.OpenFile(filepath.Join(dir, "arrival.log"), os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	index, err := os.OpenFile(filepath.Join(dir, "index.log"), os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		arrival.Close()
		return nil, err
	}
	s := &Store{
		arrival:  arrival,
		index:    index,
		byHeight: make(map[uint64]heightRec),
		byID:     make(map[ids.ID]uint64),
	}
	if err := s.rebuild(); err != nil {
		arrival.Close()
		index.Close()
		return nil, err
	}
	return s, nil
}

// rebuild scans index.log, populates the RAM maps, and truncates both files
// back to the last record whose container bytes fully fit in arrival.log.
func (s *Store) rebuild() error {
	ast, err := s.arrival.Stat()
	if err != nil {
		return err
	}
	arrivalSize := uint64(ast.Size())

	if _, err := s.index.Seek(0, io.SeekStart); err != nil {
		return err
	}
	var (
		rec     [indexRecSize]byte
		good    int64
		lastEnd uint64
	)
	r := io.Reader(s.index)
	for {
		if _, err := io.ReadFull(r, rec[:]); err != nil {
			// io.EOF: clean end. ErrUnexpectedEOF: torn index record, drop it.
			break
		}
		height := binary.BigEndian.Uint64(rec[0:8])
		var id ids.ID
		copy(id[:], rec[8:40])
		// rec[40:72] is the eth block hash: stored for future consumers,
		// not needed in RAM.
		off := binary.BigEndian.Uint64(rec[72:80])
		ln := binary.BigEndian.Uint32(rec[80:84])
		if off+uint64(ln) > arrivalSize {
			break // torn: container bytes never made it to arrival.log
		}
		s.byHeight[height] = heightRec{id: id, off: off, ln: ln}
		s.byID[id] = height
		if !s.haveAny || height > s.head {
			s.head = height
			s.haveAny = true
		}
		good++
		lastEnd = off + uint64(ln)
	}
	if err := s.index.Truncate(good * indexRecSize); err != nil {
		return err
	}
	if err := s.arrival.Truncate(int64(lastEnd)); err != nil {
		return err
	}
	if _, err := s.index.Seek(0, io.SeekEnd); err != nil {
		return err
	}
	if _, err := s.arrival.Seek(0, io.SeekEnd); err != nil {
		return err
	}
	s.arrivalOff = lastEnd
	return nil
}

// Append stores one container. Duplicate container IDs are ignored.
// Writes are unbuffered but not fsynced; call Flush after a batch.
func (s *Store) Append(p parsedContainer, raw []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, dup := s.byID[p.containerID]; dup {
		return nil
	}

	var lenBuf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(lenBuf[:], uint64(len(raw)))
	off := s.arrivalOff + uint64(n) // offset of the container bytes

	buf := make([]byte, 0, n+len(raw))
	buf = append(buf, lenBuf[:n]...)
	buf = append(buf, raw...)
	if _, err := s.arrival.Write(buf); err != nil {
		return fmt.Errorf("arrival write: %w", err)
	}

	var rec [indexRecSize]byte
	binary.BigEndian.PutUint64(rec[0:8], p.blockNumber)
	copy(rec[8:40], p.containerID[:])
	copy(rec[40:72], p.blockHash[:])
	binary.BigEndian.PutUint64(rec[72:80], off)
	binary.BigEndian.PutUint32(rec[80:84], uint32(len(raw)))
	if _, err := s.index.Write(rec[:]); err != nil {
		return fmt.Errorf("index write: %w", err)
	}

	s.arrivalOff = off + uint64(len(raw))
	s.byHeight[p.blockNumber] = heightRec{id: p.containerID, off: off, ln: uint32(len(raw))}
	s.byID[p.containerID] = p.blockNumber
	if !s.haveAny || p.blockNumber > s.head {
		s.head = p.blockNumber
		s.haveAny = true
	}
	s.sessionBytes.Add(uint64(len(buf) + indexRecSize))
	return nil
}

// Flush fsyncs arrival.log before index.log so a crash can only leave the
// index behind the data, which rebuild's torn-tail rule already handles.
func (s *Store) Flush() error {
	if err := s.arrival.Sync(); err != nil {
		return err
	}
	return s.index.Sync()
}

// Close flushes and closes both files.
func (s *Store) Close() error {
	if err := s.Flush(); err != nil {
		return err
	}
	if err := s.arrival.Close(); err != nil {
		s.index.Close()
		return err
	}
	return s.index.Close()
}

// Has reports whether a container with this ID is stored.
func (s *Store) Has(id ids.ID) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.byID[id]
	return ok
}

// HeightOf returns the block height of a stored container ID.
func (s *Store) HeightOf(id ids.ID) (uint64, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	h, ok := s.byID[id]
	return h, ok
}

// GetByHeight returns a copy of the raw container stored at height n.
// ok=false means nothing is stored at that height.
func (s *Store) GetByHeight(n uint64) ([]byte, bool, error) {
	s.mu.RLock()
	rec, ok := s.byHeight[n]
	s.mu.RUnlock()
	if !ok {
		return nil, false, nil
	}
	raw := make([]byte, rec.ln)
	if _, err := s.arrival.ReadAt(raw, int64(rec.off)); err != nil {
		return nil, false, fmt.Errorf("read container at height %d: %w", n, err)
	}
	return raw, true, nil
}

// Head returns the highest stored block height, ok=false if empty.
func (s *Store) Head() (uint64, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.head, s.haveAny
}

// LowestContiguous walks down from height `from` (which must be stored) and
// returns the lowest height of the contiguous stored run containing it.
func (s *Store) LowestContiguous(from uint64) uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	h := from
	for h > 0 {
		if _, ok := s.byHeight[h-1]; !ok {
			break
		}
		h--
	}
	return h
}

// Count returns the number of stored containers.
func (s *Store) Count() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return uint64(len(s.byID))
}

// SessionBytes returns bytes written (arrival + index) since open.
func (s *Store) SessionBytes() uint64 { return s.sessionBytes.Load() }

// Subscribe emits stored blocks in ascending height order starting at
// fromBlock. The channel stays open across gaps: heights not yet stored are
// polled until they land. Closed when ctx is canceled.
//
// ponytail: single poll loop instead of deforestationdb's two-phase
// drain-then-poll; the RAM index makes each probe a map lookup + one ReadAt,
// so the drain phase is just the poll loop running at full speed.
func (s *Store) Subscribe(ctx context.Context, fromBlock uint64) <-chan BlockEvent {
	out := make(chan BlockEvent, 64)
	go func() {
		defer close(out)
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		next := fromBlock
		for {
			s.mu.RLock()
			rec, ok := s.byHeight[next]
			s.mu.RUnlock()
			if ok {
				raw := make([]byte, rec.ln)
				if _, err := s.arrival.ReadAt(raw, int64(rec.off)); err != nil {
					return
				}
				select {
				case <-ctx.Done():
					return
				case out <- BlockEvent{BlockNumber: next, ContainerID: rec.id, Raw: raw}:
				}
				next++
				continue
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	return out
}

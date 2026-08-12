package store

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

// THE MEMTABLE covers the unflushed window and serves it (DESIGN rule 2).
//
// MEMORY IS STRICTLY BOUNDED REGARDLESS OF CHAIN LENGTH. State and lookup rows
// are held in RAM because they have to be sorted at flush anyway; CHAIN ROWS
// ARE NOT, because they are the bulk (tx and receipt bytes) and they already
// arrive sorted. They stream to per-family spill files during execution and the
// only thing kept in RAM is one offset per row, so the window costs ~16 bytes
// per tx, not ~500.
type memtable struct {
	mu sync.RWMutex

	dir string

	// window bounds
	baseTx     uint64 // first TxNum of the window
	nextTx     uint64 // TxNum the next tx will take
	baseHeight uint64
	nextHeight uint64
	started    bool

	// chain spills, one per family, in Comparer order.
	chain [5]*spill

	// state rows: prefix (state/... without the TxNum suffix) -> ascending
	// (txnum, value) history within the window.
	state map[string][]memVal
	// code blobs by hash, deduplicated for the run.
	code map[string][]byte

	// lookup rows.
	txh  map[string]uint64 // tx hash -> TxNum
	post []posting         // addr/, logaddr/, topic/ rows

	bytes uint64
}

type memVal struct {
	txnum uint64
	val   []byte
}

type posting struct {
	key   []byte // full key including the TxNum suffix
	val   byte
	num   uint64 // txh rows only: the TxNum the hash resolves to
	isTxh bool
}

// chain spill families, in the order their key prefixes sort.
const (
	famBlk = iota
	famHdr
	famPvm
	famRcpt
	famTx
)

var famPrefix = [5]string{PrefixBlk, PrefixHdr, PrefixPvm, PrefixRcpt, PrefixTx}
var famName = [5]string{"blk", "hdr", "pvm", "rcpt", "tx"}

func newMemtable(dir string) (*memtable, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	m := &memtable{
		dir:   dir,
		state: map[string][]memVal{},
		code:  map[string][]byte{},
		txh:   map[string]uint64{},
	}
	for i := range m.chain {
		s, err := newSpill(filepath.Join(dir, "spill-"+famName[i]+".tmp"))
		if err != nil {
			m.discard()
			return nil, err
		}
		m.chain[i] = s
	}
	return m, nil
}

// reset starts a fresh window at the given TxNum/height.
func (m *memtable) reset(baseTx, baseHeight uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, s := range m.chain {
		if err := s.reset(); err != nil {
			return err
		}
	}
	m.baseTx, m.nextTx = baseTx, baseTx
	m.baseHeight, m.nextHeight = baseHeight, baseHeight
	m.started = false
	m.state = map[string][]memVal{}
	m.code = map[string][]byte{}
	m.txh = map[string]uint64{}
	m.post = nil
	m.bytes = 0
	return nil
}

func (m *memtable) discard() {
	for _, s := range m.chain {
		if s != nil {
			s.close()
			os.Remove(s.path)
		}
	}
}

// ---------------------------------------------------------------------------
// writes
// ---------------------------------------------------------------------------

// StateRow is one post-tx state write. Kind is 'a' (account RLP), 'c' (code
// hash) or 's' (storage slot). A cleared slot or a deleted account is an empty
// Val: the EVM defines cleared as zero, so no tombstone mechanism exists.
type StateRow struct {
	Kind byte
	Addr []byte
	Slot []byte
	Val  []byte
}

func stateKeyPrefix(r StateRow) []byte {
	switch r.Kind {
	case 'a':
		return AccountPrefix(r.Addr)
	case 'c':
		return CodeRefPrefix(r.Addr)
	default:
		return SlotPrefix(r.Addr, r.Slot)
	}
}

// TxWrite is everything one transaction contributes.
type TxWrite struct {
	Hash     []byte
	RLP      []byte
	Receipt  []byte
	State    []StateRow
	Sender   []byte
	To       []byte // nil for a creation
	Created  []byte // contract address for a creation
	LogAddrs [][]byte
	Topics   [][]byte
}

// BlockWrite is everything one block contributes, handed over in one call so
// the ordering rules live in one place.
type BlockWrite struct {
	Height    uint64
	HeaderRLP []byte
	Pvm       []byte
	Txs       []TxWrite
	// Code blobs first seen in this block, by hash.
	Code map[string][]byte
}

// add folds one block into the window. Blocks must arrive in height order.
func (m *memtable) add(b *BlockWrite) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.started {
		m.baseHeight, m.nextHeight = b.Height, b.Height
		m.started = true
	}
	if b.Height != m.nextHeight {
		return fmt.Errorf("store: block %d out of order, window expects %d", b.Height, m.nextHeight)
	}
	first := m.nextTx

	var blkVal [12]byte
	binary.BigEndian.PutUint64(blkVal[:8], first)
	binary.BigEndian.PutUint32(blkVal[8:], uint32(len(b.Txs)))
	if err := m.chain[famBlk].append(b.Height, blkVal[:]); err != nil {
		return err
	}
	if err := m.chain[famHdr].append(b.Height, b.HeaderRLP); err != nil {
		return err
	}
	if err := m.chain[famPvm].append(b.Height, b.Pvm); err != nil {
		return err
	}
	for i := range b.Txs {
		t := &b.Txs[i]
		n := first + uint64(i)
		if err := m.chain[famRcpt].append(n, t.Receipt); err != nil {
			return err
		}
		if err := m.chain[famTx].append(n, t.RLP); err != nil {
			return err
		}
		m.txh[string(t.Hash)] = n
		roles := map[string]byte{}
		if len(t.Sender) > 0 {
			roles[string(t.Sender)] |= RoleSender
		}
		if len(t.To) > 0 {
			roles[string(t.To)] |= RoleRecipient
		}
		if len(t.Created) > 0 {
			roles[string(t.Created)] |= RoleCreated
		}
		for _, a := range t.LogAddrs {
			roles[string(a)] |= RoleEmitter
			m.post = append(m.post, posting{key: Suffixed(LogAddrPrefix(a), n)})
		}
		for _, tp := range t.Topics {
			m.post = append(m.post, posting{key: Suffixed(TopicPrefix(tp), n)})
		}
		for a, bits := range roles {
			m.post = append(m.post, posting{key: Suffixed(AddrPrefix([]byte(a)), n), val: bits})
		}
		for _, r := range t.State {
			k := string(stateKeyPrefix(r))
			m.state[k] = append(m.state[k], memVal{txnum: n, val: r.Val})
			m.bytes += uint64(len(k) + len(r.Val) + 32)
		}
	}
	for h, blob := range b.Code {
		if _, ok := m.code[h]; !ok {
			m.code[h] = blob
			m.bytes += uint64(len(blob) + 64)
		}
	}
	m.nextTx = first + uint64(len(b.Txs))
	m.nextHeight = b.Height + 1
	return nil
}

// ---------------------------------------------------------------------------
// reads
// ---------------------------------------------------------------------------

func (m *memtable) chainGet(fam int, n uint64) ([]byte, bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.chain[fam].get(n)
}

// latestState returns the newest value under prefix at or below txnum.
func (m *memtable) latestState(prefix []byte, at uint64) ([]byte, uint64, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	h := m.state[string(prefix)]
	i := sort.Search(len(h), func(i int) bool { return h[i].txnum > at })
	if i == 0 {
		return nil, 0, false
	}
	return h[i-1].val, h[i-1].txnum, true
}

func (m *memtable) codeBlob(hash []byte) ([]byte, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	b, ok := m.code[string(hash)]
	return b, ok
}

func (m *memtable) txNum(hash []byte) (uint64, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	n, ok := m.txh[string(hash)]
	return n, ok
}

// window reports the window's TxNum and height bounds.
func (m *memtable) window() (baseTx, nextTx, baseHeight, nextHeight uint64, started bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.baseTx, m.nextTx, m.baseHeight, m.nextHeight, m.started
}

// ---------------------------------------------------------------------------
// spill: an append-only file plus one offset per row
// ---------------------------------------------------------------------------

// spill holds one chain family's rows of the current window. Row numbers are
// contiguous from base, so the index is a plain offset slice, ~16 bytes per row
// and nothing proportional to the chain.
type spill struct {
	path string
	f    *os.File
	base uint64
	offs []uint64 // offs[i] is the end offset of row base+i
	pos  uint64
	buf  []byte
	set  bool
}

func newSpill(path string) (*spill, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	return &spill{path: path, f: f}, nil
}

func (s *spill) reset() error {
	if err := s.f.Truncate(0); err != nil {
		return err
	}
	s.base, s.offs, s.pos, s.buf, s.set = 0, s.offs[:0], 0, s.buf[:0], false
	return nil
}

func (s *spill) append(n uint64, val []byte) error {
	if !s.set {
		s.base, s.set = n, true
	}
	if n != s.base+uint64(len(s.offs)) {
		return fmt.Errorf("store: spill %s: row %d is not contiguous after %d", s.path, n, s.base+uint64(len(s.offs)))
	}
	s.buf = append(s.buf, val...)
	s.pos += uint64(len(val))
	s.offs = append(s.offs, s.pos)
	if len(s.buf) >= 1<<20 {
		return s.flushBuf()
	}
	return nil
}

func (s *spill) flushBuf() error {
	if len(s.buf) == 0 {
		return nil
	}
	if _, err := s.f.Write(s.buf); err != nil {
		return err
	}
	s.buf = s.buf[:0]
	return nil
}

func (s *spill) count() uint64 { return uint64(len(s.offs)) }

func (s *spill) get(n uint64) ([]byte, bool, error) {
	if !s.set || n < s.base || n >= s.base+uint64(len(s.offs)) {
		return nil, false, nil
	}
	i := n - s.base
	end := s.offs[i]
	var start uint64
	if i > 0 {
		start = s.offs[i-1]
	}
	if end == start {
		return nil, true, nil
	}
	// The tail may still be in the write buffer.
	written := s.pos - uint64(len(s.buf))
	if start >= written {
		return append([]byte(nil), s.buf[start-written:end-written]...), true, nil
	}
	if err := s.flushBuf(); err != nil {
		return nil, false, err
	}
	p := make([]byte, end-start)
	if _, err := s.f.ReadAt(p, int64(start)); err != nil {
		return nil, false, err
	}
	return p, true, nil
}

// each streams every row of the spill in number order.
func (s *spill) each(fn func(n uint64, val []byte) error) error {
	if err := s.flushBuf(); err != nil {
		return err
	}
	var start uint64
	for i, end := range s.offs {
		p := make([]byte, end-start)
		if end > start {
			if _, err := s.f.ReadAt(p, int64(start)); err != nil {
				return err
			}
		}
		if err := fn(s.base+uint64(i), p); err != nil {
			return err
		}
		start = end
	}
	return nil
}

func (s *spill) close() error { return s.f.Close() }

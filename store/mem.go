package store

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/ava-labs/libevm/crypto"
)

// THE MEMTABLE covers the unflushed window and serves it (DESIGN rule 2).
//
// MEMORY IS STRICTLY BOUNDED REGARDLESS OF CHAIN LENGTH, and the window is
// DURABLE, for a reason that is not about crash-safety of the rows themselves:
// Firewood cannot be rolled back. It keeps two revisions, so a restart that
// found the state layer 50,000 blocks behind Firewood could neither rewind
// Firewood to the flush boundary nor re-execute forward from it. The window
// therefore streams into ONE append-only log beside the runs, the same
// first-class on-disk staging pattern the fetcher uses, fsynced on the
// executor's own cadence. On restart it is replayed back into RAM and the
// executor resumes exactly where the reconcile walk-back expects it.
//
// What stays in RAM is what has to be sorted at flush anyway: the state and
// lookup rows. The chain families are the bulk (tx and receipt bytes) and keep
// only ONE OFFSET PER ROW, so the window costs ~16 bytes per tx, not ~500.
type memtable struct {
	mu sync.RWMutex

	path string
	f    *os.File
	bw   *bufio.Writer
	pos  uint64 // bytes written (including the buffer)

	// window bounds
	baseTx     uint64
	nextTx     uint64
	baseHeight uint64
	nextHeight uint64
	started    bool

	// chain rows: one (offset, length) per row, contiguous from base.
	chain [6]chainIndex

	// state rows: key prefix -> ascending (txnum, value) inside the window.
	state map[string][]memVal
	code  map[string][]byte

	// lookup rows. nums holds the two suffix-free families (txh/ -> TxNum and
	// blkh/ -> height) keyed by their FULL key, so one map and one log record
	// kind serve both and a tx hash can never answer a block-hash probe.
	nums map[string]uint64
	post []posting
}

type memVal struct {
	txnum uint64
	val   []byte
}

type posting struct {
	key   []byte // full key including the TxNum suffix
	val   byte
	num   uint64 // txh/ and blkh/ rows only
	isNum bool   // the value is num as 8 big-endian bytes, not the role byte
}

type chainIndex struct {
	base uint64
	set  bool
	off  []uint64
	n    []uint32
}

func (c *chainIndex) add(num, off uint64, n int) error {
	if !c.set {
		c.base, c.set = num, true
	}
	if num != c.base+uint64(len(c.off)) {
		return fmt.Errorf("store: chain row %d is not contiguous after %d", num, c.base+uint64(len(c.off)))
	}
	c.off = append(c.off, off)
	c.n = append(c.n, uint32(n))
	return nil
}

func (c *chainIndex) find(num uint64) (off uint64, n uint32, ok bool) {
	if !c.set || num < c.base || num >= c.base+uint64(len(c.off)) {
		return 0, 0, false
	}
	i := num - c.base
	return c.off[i], c.n[i], true
}

func (c *chainIndex) reset() { c.base, c.set, c.off, c.n = 0, false, c.off[:0], c.n[:0] }

// chain spill families, in the order their key prefixes sort.
const (
	famBlk = iota
	famHdr
	famItx
	famPvm
	famRcpt
	famTx
)

var famPrefix = [6]string{PrefixBlk, PrefixHdr, PrefixItx, PrefixPvm, PrefixRcpt, PrefixTx}

// window-log record kinds.
const (
	recBlk   = 'B'
	recHdr   = 'H'
	recItx   = 'I'
	recPvm   = 'P'
	recRcpt  = 'R'
	recTx    = 'T'
	recState = 'S'
	recPost  = 'L'
	recNum   = 'X' // a suffix-free lookup row: full key in the payload, value in num
	recCode  = 'C'
	recEnd   = 'E' // end of a block: the only point a torn tail may be cut at
)

var famRec = [6]byte{recBlk, recHdr, recItx, recPvm, recRcpt, recTx}

const recHeader = 1 + 8 + 4 // kind, num, payload length

func newMemtable(dir string) (*memtable, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	m := &memtable{path: filepath.Join(dir, "window.log")}
	m.clear()
	f, err := os.OpenFile(m.path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	m.f = f
	m.bw = bufio.NewWriterSize(f, 1<<20)
	return m, nil
}

func (m *memtable) clear() {
	m.state = map[string][]memVal{}
	m.code = map[string][]byte{}
	m.nums = map[string]uint64{}
	m.post = nil
	for i := range m.chain {
		m.chain[i].reset()
	}
	m.pos = 0
	m.started = false
}

// recover replays the window log, rebuilding the in-memory indexes and cutting
// a torn tail back to the last complete block. baseTx/baseHeight are where the
// last flush left off, which is where an empty window starts.
func (m *memtable) recover(baseTx, baseHeight uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.clear()
	m.baseTx, m.nextTx = baseTx, baseTx
	m.baseHeight, m.nextHeight = baseHeight, baseHeight

	if err := m.bw.Flush(); err != nil {
		return err
	}
	if _, err := m.f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	r := bufio.NewReaderSize(m.f, 1<<20)
	var (
		off    uint64
		good   uint64 // end offset of the last complete block
		hdrBuf [recHeader]byte
		pend   []func()
	)
	commit := func() {
		for _, f := range pend {
			f()
		}
		pend = nil
	}
	// A TORN TAIL IS CUT AT A BLOCK BOUNDARY, never inside one: mutations
	// accumulate as closures and are applied only when the block's end record
	// lands. Anything after the last end record is unreadable by definition
	// (its block is half-recorded), so it is truncated away and the executor
	// re-executes it.
replay:
	for {
		if _, err := io.ReadFull(r, hdrBuf[:]); err != nil {
			break
		}
		kind := hdrBuf[0]
		num := binary.BigEndian.Uint64(hdrBuf[1:9])
		n := binary.BigEndian.Uint32(hdrBuf[9:])
		if n > 1<<30 {
			break
		}
		payload := make([]byte, n)
		if _, err := io.ReadFull(r, payload); err != nil {
			break
		}
		body := off + recHeader
		off = body + uint64(n)

		switch kind {
		case recBlk, recHdr, recItx, recPvm, recRcpt, recTx:
			fam := 0
			for i, k := range famRec {
				if k == kind {
					fam = i
				}
			}
			f, b, l := fam, body, int(n)
			pend = append(pend, func() { m.chain[f].add(num, b, l) })
		case recState:
			if len(payload) < 2 {
				break replay
			}
			kl := int(binary.BigEndian.Uint16(payload))
			if len(payload) < 2+kl {
				break replay
			}
			key, val := string(payload[2:2+kl]), append([]byte(nil), payload[2+kl:]...)
			pend = append(pend, func() { m.putState(key, num, val) })
		case recPost:
			if len(payload) < 1 {
				break replay
			}
			key, v := append([]byte(nil), payload[1:]...), payload[0]
			pend = append(pend, func() { m.post = append(m.post, posting{key: key, val: v}) })
		case recNum:
			k := string(payload)
			pend = append(pend, func() { m.nums[k] = num })
		case recCode:
			if len(payload) < 32 {
				break replay
			}
			h, blob := string(payload[:32]), append([]byte(nil), payload[32:]...)
			pend = append(pend, func() { m.code[h] = blob })
		case recEnd:
			if n != 8 {
				break replay
			}
			if !m.started {
				m.baseHeight, m.started = num, true
			}
			commit()
			m.nextHeight = num + 1
			m.nextTx = binary.BigEndian.Uint64(payload)
			good = off
		default:
			break replay
		}
	}
	if err := m.f.Truncate(int64(good)); err != nil {
		return err
	}
	if _, err := m.f.Seek(int64(good), io.SeekStart); err != nil {
		return err
	}
	m.bw.Reset(m.f)
	m.pos = good
	return nil
}

// reset starts a fresh, empty window at the given TxNum/height.
func (m *memtable) reset(baseTx, baseHeight uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.clear()
	m.baseTx, m.nextTx = baseTx, baseTx
	m.baseHeight, m.nextHeight = baseHeight, baseHeight
	if err := m.f.Truncate(0); err != nil {
		return err
	}
	if _, err := m.f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	m.bw.Reset(m.f)
	return nil
}

func (m *memtable) close() error {
	if m.f == nil {
		return nil
	}
	err := m.bw.Flush()
	if cerr := m.f.Close(); err == nil {
		err = cerr
	}
	m.f = nil
	return err
}

// Sync makes the window durable. It is called on the executor's own cadence,
// which is what keeps the state layer at or ahead of Firewood.
func (m *memtable) sync() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.bw.Flush(); err != nil {
		return err
	}
	return m.f.Sync()
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
	Hash    []byte
	RLP     []byte
	Receipt []byte
	// Frames is the tx's call frames in enter order with depth, the `itx/`
	// row. Empty for a transaction that made no nested call, which is a real
	// answer and not a missing one: FRAMES CAN NEVER BE BACKFILLED BY IO, so
	// the executor dies rather than store a block it could not trace.
	Frames     []byte
	FrameAddrs [][]byte
	State      []StateRow
	Sender     []byte
	To         []byte // nil for a creation
	Created    []byte // contract address for a creation
	LogAddrs   [][]byte
	Topics     [][]byte
}

// BlockWrite is everything one block contributes, handed over in one call so
// the ordering rules live in one place.
type BlockWrite struct {
	Height    uint64
	HeaderRLP []byte
	Pvm       []byte
	Txs       []TxWrite
	Code      map[string][]byte // code blobs first seen in this block, by hash
	// Tail is state written outside any transaction: a fork upgrade's
	// activation, coreth's atomic transfers, block finalisation. It lands at
	// the block's LAST TxNum, or at the TxNum the block would have started at
	// when it has no transactions of its own.
	Tail []StateRow
}

func (m *memtable) write(kind byte, num uint64, payload ...[]byte) (body uint64, n int, err error) {
	for _, p := range payload {
		n += len(p)
	}
	var h [recHeader]byte
	h[0] = kind
	binary.BigEndian.PutUint64(h[1:9], num)
	binary.BigEndian.PutUint32(h[9:], uint32(n))
	if _, err = m.bw.Write(h[:]); err != nil {
		return 0, 0, err
	}
	body = m.pos + recHeader
	for _, p := range payload {
		if _, err = m.bw.Write(p); err != nil {
			return 0, 0, err
		}
	}
	m.pos = body + uint64(n)
	return body, n, nil
}

// add folds one block into the window. Blocks arrive in height order; a block
// the window already holds is skipped, because re-execution after a walk-back
// reconcile replays blocks that are already here and the rows are identical.
func (m *memtable) add(b *BlockWrite) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.started && b.Height < m.nextHeight {
		return nil
	}
	if !m.started {
		m.baseHeight, m.nextHeight, m.started = b.Height, b.Height, true
	}
	if b.Height != m.nextHeight {
		return fmt.Errorf("store: block %d out of order, window expects %d", b.Height, m.nextHeight)
	}
	first := m.nextTx

	var blkVal [12]byte
	binary.BigEndian.PutUint64(blkVal[:8], first)
	binary.BigEndian.PutUint32(blkVal[8:], uint32(len(b.Txs)))
	if err := m.chainRow(famBlk, b.Height, blkVal[:]); err != nil {
		return err
	}
	if err := m.chainRow(famHdr, b.Height, b.HeaderRLP); err != nil {
		return err
	}
	if err := m.chainRow(famPvm, b.Height, b.Pvm); err != nil {
		return err
	}
	// THE BLOCK-HASH INDEX ROW, derived here rather than carried in: a block
	// hash is keccak256 of the header RLP, and the header RLP is right there.
	if err := m.numRow(BlkHashKey(crypto.Keccak256(b.HeaderRLP)), b.Height); err != nil {
		return err
	}
	for h, blob := range b.Code {
		if _, ok := m.code[h]; ok {
			continue
		}
		if _, _, err := m.write(recCode, 0, []byte(h), blob); err != nil {
			return err
		}
		m.code[h] = blob
	}
	for i := range b.Txs {
		t := &b.Txs[i]
		n := first + uint64(i)
		if err := m.chainRow(famItx, n, t.Frames); err != nil {
			return err
		}
		if err := m.chainRow(famRcpt, n, t.Receipt); err != nil {
			return err
		}
		if err := m.chainRow(famTx, n, t.RLP); err != nil {
			return err
		}
		if err := m.numRow(TxHashKey(t.Hash), n); err != nil {
			return err
		}

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
		for _, a := range t.FrameAddrs {
			roles[string(a)] |= RoleFrame
		}
		for _, a := range t.LogAddrs {
			roles[string(a)] |= RoleEmitter
			if err := m.postRow(Suffixed(LogAddrPrefix(a), n), 0); err != nil {
				return err
			}
		}
		for _, tp := range t.Topics {
			if err := m.postRow(Suffixed(TopicPrefix(tp), n), 0); err != nil {
				return err
			}
		}
		addrs := make([]string, 0, len(roles))
		for a := range roles {
			addrs = append(addrs, a)
		}
		sort.Strings(addrs)
		for _, a := range addrs {
			if err := m.postRow(Suffixed(AddrPrefix([]byte(a)), n), roles[a]); err != nil {
				return err
			}
		}
		for _, r := range t.State {
			k := stateKeyPrefix(r)
			var kl [2]byte
			binary.BigEndian.PutUint16(kl[:], uint16(len(k)))
			if _, _, err := m.write(recState, n, kl[:], k, r.Val); err != nil {
				return err
			}
			m.putState(string(k), n, r.Val)
		}
	}
	tailAt := first
	if len(b.Txs) > 0 {
		tailAt = first + uint64(len(b.Txs)) - 1
	}
	for _, r := range b.Tail {
		k := stateKeyPrefix(r)
		var kl [2]byte
		binary.BigEndian.PutUint16(kl[:], uint16(len(k)))
		if _, _, err := m.write(recState, tailAt, kl[:], k, r.Val); err != nil {
			return err
		}
		m.putState(string(k), tailAt, r.Val)
	}
	m.nextTx = first + uint64(len(b.Txs))
	m.nextHeight = b.Height + 1
	var endVal [8]byte
	binary.BigEndian.PutUint64(endVal[:], m.nextTx)
	_, _, err := m.write(recEnd, b.Height, endVal[:])
	return err
}

func (m *memtable) chainRow(fam int, num uint64, val []byte) error {
	off, n, err := m.write(famRec[fam], num, val)
	if err != nil {
		return err
	}
	return m.chain[fam].add(num, off, n)
}

// numRow records one suffix-free lookup row (txh/ or blkh/).
func (m *memtable) numRow(key []byte, num uint64) error {
	if _, _, err := m.write(recNum, num, key); err != nil {
		return err
	}
	m.nums[string(key)] = num
	return nil
}

func (m *memtable) postRow(key []byte, val byte) error {
	if _, _, err := m.write(recPost, TxNumOf(key), []byte{val}, key); err != nil {
		return err
	}
	m.post = append(m.post, posting{key: key, val: val})
	return nil
}

// ---------------------------------------------------------------------------
// reads
// ---------------------------------------------------------------------------

// chainGet takes the WRITE lock, not the read lock: serving a row that is still
// in the write buffer has to flush it, and two readers flushing a bufio.Writer
// at once is a data race on the executor's own output.
func (m *memtable) chainGet(fam int, num uint64) ([]byte, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	off, n, ok := m.chain[fam].find(num)
	if !ok {
		return nil, false, nil
	}
	if n == 0 {
		return nil, true, nil
	}
	if err := m.bw.Flush(); err != nil {
		return nil, false, err
	}
	p := make([]byte, n)
	if _, err := m.f.ReadAt(p, int64(off)); err != nil && !errors.Is(err, io.EOF) {
		return nil, false, err
	}
	return p, true, nil
}

// putState appends one post-tx row, or REPLACES the row already at that TxNum.
// A block with no transactions of its own still writes state (an upgrade
// activation, a coreth atomic tx), and its rows land at the TxNum the block
// would have started at, which the next such block shares. The post-image at a
// TxNum is by definition the last write at it, so later wins. The alternative
// is Erigon's block-boundary pseudo-txnums, which would buy the distinction at
// the cost of making `tx/<txnum>` non-dense; not worth it until something needs
// to tell two empty blocks' writes apart.
func (m *memtable) putState(key string, txnum uint64, val []byte) {
	h := m.state[key]
	if n := len(h); n > 0 && h[n-1].txnum == txnum {
		h[n-1].val = val
		return
	}
	m.state[key] = append(h, memVal{txnum: txnum, val: val})
}

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

// lookupNum serves a suffix-free lookup row (txh/ or blkh/) out of the window.
func (m *memtable) lookupNum(key []byte) (uint64, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	n, ok := m.nums[string(key)]
	return n, ok
}

func (m *memtable) window() (baseTx, nextTx, baseHeight, nextHeight uint64, started bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.baseTx, m.nextTx, m.baseHeight, m.nextHeight, m.started
}

// eachChain streams one chain family's rows in number order, for the flush.
func (m *memtable) eachChain(fam int, fn func(num uint64, val []byte) error) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.bw.Flush(); err != nil {
		return err
	}
	c := &m.chain[fam]
	for i := range c.off {
		p := make([]byte, c.n[i])
		if c.n[i] > 0 {
			if _, err := m.f.ReadAt(p, int64(c.off[i])); err != nil {
				return err
			}
		}
		if err := fn(c.base+uint64(i), p); err != nil {
			return err
		}
	}
	return nil
}

package store

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/crypto"
	"github.com/ava-labs/libevm/ethdb"
)

// EthDB returns a libevm ethdb.KeyValueStore over a storage v0 DB: contract
// code (rawdb key 'c'+hash) READS route to DB.Code, i.e. the memtable and then
// the live runs newest-to-oldest, bloom-gated. Every other key routes to the
// MiscStore.
//
// Code WRITES are a no-op. The executor captures every code blob it sees into
// the BlockWrite, so by the time rawdb's WriteCode runs the blob is already on
// its way into a run; writing it a second time would only duplicate it.
//
// This is the durable backing rawdb needs because Firewood explicitly delegates
// code storage to rawdb. Lifetime is tied to the DB and the MiscStore, neither
// of which this closes.
// EthDB routes contract code to the descent and everything else to the small
// durable misc store beside the runs.
//
// GENESIS CODE IS NOT STORED, IT IS RESOLVED. The genesis commit routes its
// pre-deployed contracts through rawdb.WriteCode, and those writes are no-ops
// here because a run only ever carries what execution produced. What genesis
// MATERIALISED is a pure function of the chain root, so the alloc answers for
// it directly: one map built once at open, the same floor the read descent
// bottoms at. Without it the first transaction that calls a genesis contract
// dies with "can't load code hash" (caught live at numine block 1).
func EthDB(d *DB, m *MiscStore, genesis types.GenesisAlloc) ethdb.KeyValueStore {
	code := make(map[common.Hash][]byte)
	for _, a := range genesis {
		if len(a.Code) > 0 {
			code[crypto.Keccak256Hash(a.Code)] = a.Code
		}
	}
	return &ethKV{d: d, misc: m, genesis: code}
}

type ethKV struct {
	mu      sync.Mutex
	d       *DB
	misc    *MiscStore
	genesis map[common.Hash][]byte
}

// isCodeKey matches libevm rawdb's code key layout: 'c' + 32-byte hash.
func isCodeKey(k []byte) (common.Hash, bool) {
	if len(k) == 33 && k[0] == 'c' {
		return common.BytesToHash(k[1:]), true
	}
	return common.Hash{}, false
}

func (a *ethKV) Has(key []byte) (bool, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if h, ok := isCodeKey(key); ok {
		if _, ok := a.genesis[h]; ok {
			return true, nil
		}
		_, found, err := a.d.Code(h[:])
		return found, err
	}
	_, ok := a.misc.Get(key)
	return ok, nil
}

func (a *ethKV) Get(key []byte) ([]byte, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if h, ok := isCodeKey(key); ok {
		blob, ok, err := a.d.Code(h[:])
		if err != nil {
			return nil, err
		}
		if ok {
			return blob, nil
		}
		if blob, ok := a.genesis[h]; ok {
			return blob, nil
		}
		return nil, errNotFound
	}
	if v, ok := a.misc.Get(key); ok {
		return v, nil
	}
	return nil, errNotFound
}

func (a *ethKV) Put(key, value []byte) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, ok := isCodeKey(key); ok {
		// NO-OP: the executor already captured this blob into the BlockWrite.
		return nil
	}
	return a.misc.Put(key, value)
}

func (a *ethKV) Delete(key []byte) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, ok := isCodeKey(key); ok {
		// Code is content-addressed and never garbage-collected here.
		return nil
	}
	return a.misc.Delete(key)
}

func (a *ethKV) Stat(string) (string, error) {
	return "", errors.New("store ethdb: stat not supported")
}

func (a *ethKV) Compact([]byte, []byte) error { return nil }

func (a *ethKV) NewSnapshot() (ethdb.Snapshot, error) {
	return nil, errors.New("store ethdb: snapshot not supported")
}

func (a *ethKV) Close() error { return nil } // owned by the DB and the MiscStore

// --- batches ---------------------------------------------------------------

type ethKVBatch struct {
	a    *ethKV
	ops  []kvOp
	size int
}

type kvOp struct {
	key, value []byte
	del        bool
}

func (a *ethKV) NewBatch() ethdb.Batch { return &ethKVBatch{a: a} }

func (a *ethKV) NewBatchWithSize(int) ethdb.Batch { return &ethKVBatch{a: a} }

func (b *ethKVBatch) Put(key, value []byte) error {
	k := make([]byte, len(key))
	copy(k, key)
	v := make([]byte, len(value))
	copy(v, value)
	b.ops = append(b.ops, kvOp{key: k, value: v})
	b.size += len(key) + len(value) + 1
	return nil
}

func (b *ethKVBatch) Delete(key []byte) error {
	k := make([]byte, len(key))
	copy(k, key)
	b.ops = append(b.ops, kvOp{key: k, del: true})
	b.size += len(key) + 1
	return nil
}

func (b *ethKVBatch) ValueSize() int { return b.size }

func (b *ethKVBatch) Write() error {
	for _, op := range b.ops {
		if op.del {
			if err := b.a.Delete(op.key); err != nil {
				return err
			}
			continue
		}
		if err := b.a.Put(op.key, op.value); err != nil {
			return err
		}
	}
	return nil
}

func (b *ethKVBatch) Reset() {
	b.ops = b.ops[:0]
	b.size = 0
}

func (b *ethKVBatch) Replay(w ethdb.KeyValueWriter) error {
	for _, op := range b.ops {
		if op.del {
			if err := w.Delete(op.key); err != nil {
				return err
			}
			continue
		}
		if err := w.Put(op.key, op.value); err != nil {
			return err
		}
	}
	return nil
}

// --- iterator ---------------------------------------------------------------

// NewIterator iterates a snapshot of matching MISC keys sorted ascending.
// Rarely used in our flow (nothing in the executor path iterates); correctness
// over speed.
//
// Code keys are NOT enumerated: in storage v0 code lives in the run files, and
// listing every code hash would mean opening and scanning every run. Nothing
// asks for it, so nothing pays for it.
func (a *ethKV) NewIterator(prefix, start []byte) ethdb.Iterator {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.misc.mu.Lock()
	defer a.misc.mu.Unlock()
	lower := string(prefix) + string(start)
	var keys []string
	for k := range a.misc.m {
		if strings.HasPrefix(k, string(prefix)) && k >= lower {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	return &ethKVIter{a: a, keys: keys, pos: -1}
}

type ethKVIter struct {
	a    *ethKV
	keys []string
	pos  int
	err  error
}

func (i *ethKVIter) Next() bool {
	i.pos++
	return i.pos < len(i.keys)
}

func (i *ethKVIter) Error() error { return i.err }

func (i *ethKVIter) Key() []byte {
	if i.pos < 0 || i.pos >= len(i.keys) {
		return nil
	}
	return []byte(i.keys[i.pos])
}

func (i *ethKVIter) Value() []byte {
	k := i.Key()
	if k == nil {
		return nil
	}
	v, err := i.a.Get(k)
	if err != nil {
		i.err = err
		return nil
	}
	return v
}

func (i *ethKVIter) Release() {}

// ---------------------------------------------------------------------------
// THE MISC STORE
// ---------------------------------------------------------------------------

// MiscStore holds the handful of non-code rawdb keys (genesis block rows,
// chain config, head markers) as a full RAM map backed by an append-only
// change log, replayed at startup.
//
//	misc.log: records [op 1B: 0=put 1=del][uvarint klen][key]
//	          [uvarint vlen][value]   (del records carry no value fields)
//
// Puts of an identical value are skipped, so replayed blocks and repeated
// genesis writes don't grow the file. Torn tails are truncated at startup.
type MiscStore struct {
	// mu guards m/dirty: serve --follow reads rawdb keys through this map
	// from RPC goroutines while the executor writes them.
	mu    sync.Mutex
	f     *os.File
	m     map[string][]byte
	dirty bool
}

// OpenMisc opens (or creates) misc.log inside dir, which is the DB's own
// directory.
func OpenMisc(dir string) (*MiscStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(dir, "misc.log"), os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	s := &MiscStore{f: f, m: make(map[string][]byte)}
	r := bufio.NewReaderSize(f, 1<<20)
	var pos uint64
	for {
		end, op, k, v, err := readMiscRecord(r, pos)
		if err != nil {
			if !cleanEOF(err) {
				f.Close()
				return nil, fmt.Errorf("misc.log: read record at %d: %w", pos, err)
			}
			break // clean EOF or torn record: truncate at pos
		}
		if op == 0 {
			s.m[string(k)] = v
		} else {
			delete(s.m, string(k))
		}
		pos = end
	}
	if err := f.Truncate(int64(pos)); err != nil {
		f.Close()
		return nil, err
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		f.Close()
		return nil, err
	}
	return s, nil
}

func cleanEOF(err error) bool {
	return errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)
}

func uvarintLen(v uint64) int {
	var buf [binary.MaxVarintLen64]byte
	return binary.PutUvarint(buf[:], v)
}

func readMiscRecord(r *bufio.Reader, pos uint64) (end uint64, op byte, k, v []byte, err error) {
	op, err = r.ReadByte()
	if err != nil {
		return 0, 0, nil, nil, err
	}
	klen, err := binary.ReadUvarint(r)
	if err != nil {
		return 0, 0, nil, nil, err
	}
	k = make([]byte, klen)
	if _, err = io.ReadFull(r, k); err != nil {
		return 0, 0, nil, nil, err
	}
	end = pos + 1 + uint64(uvarintLen(klen)) + klen
	if op == 1 {
		return end, op, k, nil, nil
	}
	vlen, err := binary.ReadUvarint(r)
	if err != nil {
		return 0, 0, nil, nil, err
	}
	v = make([]byte, vlen)
	if _, err = io.ReadFull(r, v); err != nil {
		return 0, 0, nil, nil, err
	}
	end += uint64(uvarintLen(vlen)) + vlen
	return end, op, k, v, nil
}

func (s *MiscStore) Get(key []byte) ([]byte, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.m[string(key)]
	if !ok {
		return nil, false
	}
	out := make([]byte, len(v))
	copy(out, v)
	return out, true
}

func (s *MiscStore) Put(key, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.f == nil { // read-only open with no misc.log present
		return fmt.Errorf("misc.log: read-only store")
	}
	if cur, ok := s.m[string(key)]; ok && bytes.Equal(cur, value) {
		return nil
	}
	var buf []byte
	buf = append(buf, 0)
	buf = binary.AppendUvarint(buf, uint64(len(key)))
	buf = append(buf, key...)
	buf = binary.AppendUvarint(buf, uint64(len(value)))
	buf = append(buf, value...)
	if _, err := s.f.Write(buf); err != nil {
		return fmt.Errorf("misc.log write: %w", err)
	}
	v := make([]byte, len(value))
	copy(v, value)
	s.m[string(key)] = v
	s.dirty = true
	return nil
}

func (s *MiscStore) Delete(key []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.m[string(key)]; !ok {
		return nil
	}
	var buf []byte
	buf = append(buf, 1)
	buf = binary.AppendUvarint(buf, uint64(len(key)))
	buf = append(buf, key...)
	if _, err := s.f.Write(buf); err != nil {
		return fmt.Errorf("misc.log write: %w", err)
	}
	delete(s.m, string(key))
	s.dirty = true
	return nil
}

func (s *MiscStore) Sync() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.dirty || s.f == nil { // nil file = absent misc.log (read-only, empty)
		return nil
	}
	if err := s.f.Sync(); err != nil {
		return err
	}
	s.dirty = false
	return nil
}

func (s *MiscStore) Close() error {
	if s.f == nil {
		return nil
	}
	if err := s.Sync(); err != nil {
		s.f.Close()
		return err
	}
	return s.f.Close()
}

// vmKindKey stamps the data dir with the VM kind that built it, inside misc.
// Namespaced so it cannot collide with a rawdb key.
const vmKindKey = "epochdb.vmkind"

// BindVMKind claims the data dir for one VM kind, or refuses it for the other.
// coreth and subnet-evm header RLP are mutually exclusive and libevm's extras
// registry is process-global, so a dir built by one is unreadable by the other
// and there is NO migration: delete the corpus and resync.
func (s *MiscStore) BindVMKind(kind string) error {
	if cur, ok := s.Get([]byte(vmKindKey)); ok {
		if string(cur) != kind {
			return fmt.Errorf("store: data dir was built by %s, refusing to open it as %s: delete the corpus and resync", cur, kind)
		}
		return nil
	}
	return s.Put([]byte(vmKindKey), []byte(kind))
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
func (s *MiscStore) FrontierFloor() (uint64, bool) {
	v, ok := s.Get([]byte(frontierFloorKey))
	if !ok || len(v) != 8 {
		return 0, false
	}
	return binary.BigEndian.Uint64(v), true
}

// SetFrontierFloor stamps the height a frontier merge parked at.
func (s *MiscStore) SetFrontierFloor(h uint64) error {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], h)
	return s.Put([]byte(frontierFloorKey), buf[:])
}

package exec

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/state"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/ethdb"
	"github.com/ava-labs/libevm/rlp"
	"github.com/ava-labs/libevm/trie"
	"github.com/ava-labs/libevm/trie/trienode"
	"github.com/ava-labs/libevm/triedb"
)

// Write-capture record kinds. Unlike the reference (old-value changesets),
// we record post-images: the NEW value being written. A deleted account or
// slot is an explicit zero-length value record.
const (
	kindAccount byte = 'a' // [kind][addr 20][uvarint vlen][account RLP]
	kindStorage byte = 's' // [kind][addr 20][slot key 32][uvarint vlen][value RLP]
	kindCodeUse byte = 'c' // [kind][addr 20][code hash 32][uvarint 0]
)

// blockFrame accumulates one block's write records.
type blockFrame struct {
	buf []byte
	n   int // record count
}

func (f *blockFrame) recordAccount(addr common.Address, valRLP []byte) {
	f.buf = append(f.buf, kindAccount)
	f.buf = append(f.buf, addr[:]...)
	f.buf = binary.AppendUvarint(f.buf, uint64(len(valRLP)))
	f.buf = append(f.buf, valRLP...)
	f.n++
}

func (f *blockFrame) recordStorage(addr common.Address, key []byte, valRLP []byte) {
	f.buf = append(f.buf, kindStorage)
	f.buf = append(f.buf, addr[:]...)
	var slot common.Hash
	copy(slot[:], key)
	f.buf = append(f.buf, slot[:]...)
	f.buf = binary.AppendUvarint(f.buf, uint64(len(valRLP)))
	f.buf = append(f.buf, valRLP...)
	f.n++
}

func (f *blockFrame) recordCodeUse(addr common.Address, codeHash common.Hash) {
	f.buf = append(f.buf, kindCodeUse)
	f.buf = append(f.buf, addr[:]...)
	f.buf = append(f.buf, codeHash[:]...)
	f.buf = binary.AppendUvarint(f.buf, 0)
	f.n++
}

// encodeLogsFrame builds one block's event-log index record: the block's
// UNIQUE log addresses and UNIQUE topics, first-appearance order. Posting
// lists are position-agnostic by design, so "appears in this block" is all
// the future getLogs index needs. nil for blocks without logs.
//
//	[uvarint nAddr][addr 20B ...][uvarint nTopic][topic 32B ...]
//
// ponytail: no zstd; unique hashes barely compress and dedup already did
// the real work (~tens of bytes per log-bearing block). Revisit if
// bytes/block ever rivals the writelog.
func encodeLogsFrame(logs []*types.Log) []byte {
	if len(logs) == 0 {
		return nil
	}
	var (
		addrs     []common.Address
		topics    []common.Hash
		addrSeen  = make(map[common.Address]struct{})
		topicSeen = make(map[common.Hash]struct{})
	)
	for _, l := range logs {
		if _, ok := addrSeen[l.Address]; !ok {
			addrSeen[l.Address] = struct{}{}
			addrs = append(addrs, l.Address)
		}
		for _, tp := range l.Topics {
			if _, ok := topicSeen[tp]; !ok {
				topicSeen[tp] = struct{}{}
				topics = append(topics, tp)
			}
		}
	}
	buf := binary.AppendUvarint(nil, uint64(len(addrs)))
	for _, a := range addrs {
		buf = append(buf, a[:]...)
	}
	buf = binary.AppendUvarint(buf, uint64(len(topics)))
	for _, tp := range topics {
		buf = append(buf, tp[:]...)
	}
	return buf
}

// decodeLogsFrame is the inverse of encodeLogsFrame.
func decodeLogsFrame(rec []byte) (addrs []common.Address, topics []common.Hash, err error) {
	n, off := binary.Uvarint(rec)
	if off <= 0 {
		return nil, nil, fmt.Errorf("logs frame: bad addr count")
	}
	for i := uint64(0); i < n; i++ {
		if off+20 > len(rec) {
			return nil, nil, fmt.Errorf("logs frame: truncated addr")
		}
		addrs = append(addrs, common.BytesToAddress(rec[off:off+20]))
		off += 20
	}
	m, k := binary.Uvarint(rec[off:])
	if k <= 0 {
		return nil, nil, fmt.Errorf("logs frame: bad topic count")
	}
	off += k
	for i := uint64(0); i < m; i++ {
		if off+32 > len(rec) {
			return nil, nil, fmt.Errorf("logs frame: truncated topic")
		}
		topics = append(topics, common.BytesToHash(rec[off:off+32]))
		off += 32
	}
	return addrs, topics, nil
}

// codeSink receives every contract code blob at write time.
type codeSink interface {
	PutCode(hash common.Hash, blob []byte) error
}

// wrapDatabase wraps an inner state.Database so that every account,
// storage, and contract-code write passes through a trie interceptor that
// records post-images into the current blockFrame. A nil frame disables
// recording (genesis commit).
//
// cacheBytes > 0 additionally enables a Go-side read-through cache of
// frontier state so EVM reads skip the per-key Firewood FFI crossing
// (~50% of replay CPU at height ~3.7M). The cache feeds ONLY reads:
// every write still flows to Firewood unchanged, and the state root is
// still computed by Firewood's own tree, so the executor's
// newRoot != header.Root hard stop keeps detecting any divergence.
// verify makes every cache hit also do the FFI read and panic on any
// difference (validation harness, not steady state).
func wrapDatabase(inner state.Database, code codeSink, cacheBytes uint64, verify bool) *wrappedDatabase {
	d := &wrappedDatabase{inner: inner, code: code, cacheCap: cacheBytes, verify: verify}
	if cacheBytes > 0 {
		d.cache = make(map[common.Address]*acctEntry)
	}
	return d
}

type wrappedDatabase struct {
	inner state.Database
	code  codeSink
	frame *blockFrame

	// ponytail: no locks, the executor is single-goroutine by design;
	// add a mutex if a prefetcher or parallel EVM ever appears.
	cache        map[common.Address]*acctEntry
	cacheCap     uint64
	cacheSize    uint64
	verify       bool
	hits, misses uint64

	// Batch mode (--commit-every > 1): one shared inner account trie per
	// batch. Firewood's own baseTrie.dirtyKeys IS the pending-batch
	// overlay: reads check it before the revision at batchRoot, with the
	// exact deleted-account tombstone semantics we need, and its
	// accumulated updateOps become the single boundary proposal. The
	// wrapper's Hash/Commit on the account trie return batchRoot so a
	// mid-batch statedb.Commit drains writes through the capture path
	// without ever proposing (root == origin skips TrieDB().Update).
	batchRoot common.Hash
	batchTrie state.Trie
	// batchDeleted tracks accounts destructed within the open batch and
	// the slots rewritten for them afterwards. Firewood's dirtyKeys
	// overlay loses the destruct boundary once the account key is
	// rewritten (recreation), so storage reads of a destructed-then-
	// recreated account would fall through to the STALE batch-start
	// revision. Caught live by the boundary bisect at Fuji block
	// ~7220268. Mirrors the reference overlay's lastAccountDelete rule.
	batchDeleted map[common.Address]map[common.Hash][]byte
}

// beginBatch enters batch mode with the given committed start root.
func (d *wrappedDatabase) beginBatch(root common.Hash) {
	d.batchRoot = root
	d.batchTrie = nil
	d.batchDeleted = make(map[common.Address]map[common.Hash][]byte)
}

// endBatch leaves batch mode, discarding the shared trie.
func (d *wrappedDatabase) endBatch() {
	d.batchRoot = common.Hash{}
	d.batchTrie = nil
	d.batchDeleted = nil
}

// batchProposalRoot proposes every accumulated batch write to Firewood in
// one shot and returns the resulting root. Only valid mid-batch with at
// least one write drained into the shared trie.
func (d *wrappedDatabase) batchProposalRoot() common.Hash {
	return d.batchTrie.Hash()
}

// resetCache drops every cached entry (bisect: entries may hold values
// produced by a bad batch execution).
func (d *wrappedDatabase) resetCache() {
	if d.cache != nil {
		d.cache = make(map[common.Address]*acctEntry)
		d.cacheSize = 0
	}
}

// acctEntry caches one account's frontier state. Per-account nesting is
// what makes SELFDESTRUCT safe: Firewood's DeleteAccount prefix-deletes
// the whole storage subtree, and dropping the entry mirrors that
// atomically.
type acctEntry struct {
	acct      *types.StateAccount // nil with acctKnown = known-nonexistent
	acctKnown bool
	storage   map[common.Hash][]byte // present nil/empty value = known zero/deleted
	cost      uint64
}

// Coarse per-item cost estimates (map+entry overhead, keys, pointers).
const (
	entryCost = 256
	slotCost  = 104
)

func (d *wrappedDatabase) setFrame(f *blockFrame) { d.frame = f }

// entry returns the cache entry for addr, creating it (and evicting over
// the cap) as needed. Caller must have checked d.cache != nil.
func (d *wrappedDatabase) entry(addr common.Address) *acctEntry {
	if e, ok := d.cache[addr]; ok {
		return e
	}
	if d.cacheSize > d.cacheCap {
		// Random-victim, whole accounts, down to 7/8 of the cap so
		// eviction runs in bursts. Always safe: a miss falls through to
		// authoritative Firewood.
		// ponytail: map-order random victim; make it LRU if hit rate
		// ever proves too low.
		target := d.cacheCap - d.cacheCap/8
		for a, e := range d.cache {
			if a == addr {
				continue
			}
			d.cacheSize -= e.cost
			delete(d.cache, a)
			if d.cacheSize <= target {
				break
			}
		}
	}
	e := &acctEntry{storage: make(map[common.Hash][]byte), cost: entryCost}
	d.cache[addr] = e
	d.cacheSize += entryCost
	return e
}

func (e *acctEntry) setSlot(d *wrappedDatabase, slot common.Hash, val []byte) {
	add := slotCost + uint64(len(val))
	if old, existed := e.storage[slot]; existed {
		sub := slotCost + uint64(len(old))
		e.cost -= sub
		d.cacheSize -= sub
	}
	e.cost += add
	d.cacheSize += add
	e.storage[slot] = val
}

// CacheStats returns hits, misses since open.
func (d *wrappedDatabase) CacheStats() (uint64, uint64) { return d.hits, d.misses }

func (d *wrappedDatabase) OpenTrie(root common.Hash) (state.Trie, error) {
	if d.batchRoot != (common.Hash{}) && root == d.batchRoot {
		if d.batchTrie == nil {
			t, err := d.inner.OpenTrie(root)
			if err != nil {
				return nil, err
			}
			d.batchTrie = t
		}
		return &wrappingTrie{inner: d.batchTrie, db: d, batchAccount: true}, nil
	}
	t, err := d.inner.OpenTrie(root)
	if err != nil {
		return nil, err
	}
	return &wrappingTrie{inner: t, db: d}, nil
}

func (d *wrappedDatabase) OpenStorageTrie(stateRoot common.Hash, addr common.Address, root common.Hash, parent state.Trie) (state.Trie, error) {
	innerParent := parent
	if w, ok := parent.(*wrappingTrie); ok {
		innerParent = w.inner
	}
	t, err := d.inner.OpenStorageTrie(stateRoot, addr, root, innerParent)
	if err != nil {
		return nil, err
	}
	return &wrappingTrie{inner: t, db: d}, nil
}

func (d *wrappedDatabase) CopyTrie(t state.Trie) state.Trie {
	inner := t
	if w, ok := t.(*wrappingTrie); ok {
		inner = w.inner
	}
	return &wrappingTrie{inner: d.inner.CopyTrie(inner), db: d}
}

func (d *wrappedDatabase) ContractCode(addr common.Address, codeHash common.Hash) ([]byte, error) {
	return d.inner.ContractCode(addr, codeHash)
}

func (d *wrappedDatabase) ContractCodeSize(addr common.Address, codeHash common.Hash) (int, error) {
	return d.inner.ContractCodeSize(addr, codeHash)
}

func (d *wrappedDatabase) DiskDB() ethdb.KeyValueStore { return d.inner.DiskDB() }
func (d *wrappedDatabase) TrieDB() *triedb.Database    { return d.inner.TrieDB() }

// wrappingTrie intercepts the write methods to capture post-images; all
// other methods pass through.
type wrappingTrie struct {
	inner state.Trie
	db    *wrappedDatabase
	// batchAccount marks the batch-shared ACCOUNT trie: its Hash/Commit
	// return batchRoot so mid-batch statedb.Commit calls never propose
	// (and root == origin skips the firewood Update). Storage tries stay
	// pass-through: firewood's storage Hash is always the zero hash and
	// that zero is embedded in captured account RLP, which must not
	// change between batch and per-block modes.
	batchAccount bool
}

func (t *wrappingTrie) UpdateAccount(addr common.Address, acc *types.StateAccount) error {
	if f := t.db.frame; f != nil {
		valRLP, err := rlp.EncodeToBytes(acc)
		if err != nil {
			return err
		}
		f.recordAccount(addr, valRLP)
	}
	if d := t.db; d.cache != nil {
		e := d.entry(addr)
		e.acct = acc.Copy()
		e.acctKnown = true
	}
	return t.inner.UpdateAccount(addr, acc)
}

func (t *wrappingTrie) DeleteAccount(addr common.Address) error {
	if f := t.db.frame; f != nil {
		f.recordAccount(addr, nil)
	}
	// THE selfdestruct hazard: Firewood prefix-deletes the whole storage
	// subtree here, so the cached account AND all its slots must go
	// atomically. Recreation repopulates via normal write-through.
	if d := t.db; d.cache != nil {
		if e, ok := d.cache[addr]; ok {
			d.cacheSize -= e.cost
			delete(d.cache, addr)
		}
	}
	if d := t.db; d.batchDeleted != nil {
		// Tombstone (or re-tombstone) the account for the rest of the
		// batch: only slots written after this point may be read.
		d.batchDeleted[addr] = make(map[common.Hash][]byte)
	}
	return t.inner.DeleteAccount(addr)
}

func (t *wrappingTrie) UpdateStorage(addr common.Address, key, value []byte) error {
	if f := t.db.frame; f != nil {
		f.recordStorage(addr, key, value)
	}
	if d := t.db; d.cache != nil || d.batchDeleted != nil {
		var slot common.Hash
		copy(slot[:], key)
		v := make([]byte, len(value))
		copy(v, value)
		if d.cache != nil {
			d.entry(addr).setSlot(d, slot, v)
		}
		if rw, ok := d.batchDeleted[addr]; ok {
			rw[slot] = v
		}
	}
	return t.inner.UpdateStorage(addr, key, value)
}

func (t *wrappingTrie) DeleteStorage(addr common.Address, key []byte) error {
	if f := t.db.frame; f != nil {
		f.recordStorage(addr, key, nil)
	}
	if d := t.db; d.cache != nil || d.batchDeleted != nil {
		var slot common.Hash
		copy(slot[:], key)
		if d.cache != nil {
			d.entry(addr).setSlot(d, slot, nil)
		}
		if rw, ok := d.batchDeleted[addr]; ok {
			rw[slot] = nil
		}
	}
	return t.inner.DeleteStorage(addr, key)
}

func (t *wrappingTrie) UpdateContractCode(addr common.Address, codeHash common.Hash, code []byte) error {
	if f := t.db.frame; f != nil {
		f.recordCodeUse(addr, codeHash)
	}
	// Capture the blob at write time. The commit-phase rawdb.WriteCode
	// lands in the same code store via the ethdb adapter; both paths
	// dedup by hash.
	if err := t.db.code.PutCode(codeHash, code); err != nil {
		return err
	}
	return t.inner.UpdateContractCode(addr, codeHash, code)
}

// --- reads (cache read-through) ---------------------------------------------

func (t *wrappingTrie) GetAccount(addr common.Address) (*types.StateAccount, error) {
	d := t.db
	if d.cache != nil {
		if e, ok := d.cache[addr]; ok && e.acctKnown {
			d.hits++
			if d.verify {
				t.verifyAccountHit(addr, e)
			}
			if e.acct == nil {
				return nil, nil
			}
			// Copy: statedb takes the struct by value but shares the
			// Balance pointer.
			return e.acct.Copy(), nil
		}
	}
	acct, err := t.inner.GetAccount(addr)
	if err != nil || d.cache == nil {
		return acct, err
	}
	d.misses++
	e := d.entry(addr)
	e.acctKnown = true
	if acct != nil {
		e.acct = acct.Copy()
	} else {
		e.acct = nil // negative cache; overwritten by UpdateAccount on creation
	}
	return acct, nil
}

func (t *wrappingTrie) GetStorage(addr common.Address, key []byte) ([]byte, error) {
	d := t.db
	var slot common.Hash
	if d.cache != nil {
		copy(slot[:], key)
		if e, ok := d.cache[addr]; ok {
			if v, ok := e.storage[slot]; ok {
				d.hits++
				if d.verify {
					t.verifyStorageHit(addr, key, v)
				}
				// Returned slice is treated read-only by statedb
				// (SetBytes copies immediately); no defensive copy on
				// the hot path.
				return v, nil
			}
		}
	}
	v, err := t.storageFallthrough(addr, key)
	if err != nil || d.cache == nil {
		return v, err
	}
	d.misses++
	cv := make([]byte, len(v))
	copy(cv, v)
	d.entry(addr).setSlot(d, slot, cv)
	return v, nil
}

// storageFallthrough is the authoritative miss path. For an account
// destructed within the open batch, only slots rewritten since count; the
// inner trie must NOT be consulted (its dirtyKeys lose the destruct
// boundary on recreation and would serve the stale batch-start value).
func (t *wrappingTrie) storageFallthrough(addr common.Address, key []byte) ([]byte, error) {
	if rw, ok := t.db.batchDeleted[addr]; ok {
		var slot common.Hash
		copy(slot[:], key)
		return rw[slot], nil
	}
	return t.inner.GetStorage(addr, key)
}

// verifyAccountHit re-reads addr through Firewood and panics if the
// cached view differs. --verify-cache harness only.
func (t *wrappingTrie) verifyAccountHit(addr common.Address, e *acctEntry) {
	got, err := t.inner.GetAccount(addr)
	if err != nil {
		panic(fmt.Sprintf("verify-cache: account %x: firewood read error: %v", addr, err))
	}
	switch {
	case (got == nil) != (e.acct == nil):
		panic(fmt.Sprintf("verify-cache: account %x: cache=%+v firewood=%+v", addr, e.acct, got))
	case got != nil &&
		(got.Nonce != e.acct.Nonce || got.Balance.Cmp(e.acct.Balance) != 0 ||
			got.Root != e.acct.Root || !bytes.Equal(got.CodeHash, e.acct.CodeHash)):
		panic(fmt.Sprintf("verify-cache: account %x mismatch: cache=%+v firewood=%+v", addr, e.acct, got))
	}
}

// verifyStorageHit re-reads a slot through Firewood and panics if the
// cached view differs. --verify-cache harness only.
func (t *wrappingTrie) verifyStorageHit(addr common.Address, key, cached []byte) {
	got, err := t.storageFallthrough(addr, key)
	if err != nil {
		panic(fmt.Sprintf("verify-cache: storage %x/%x: firewood read error: %v", addr, key, err))
	}
	if !bytes.Equal(got, cached) {
		panic(fmt.Sprintf("verify-cache: storage %x/%x mismatch: cache=%x firewood=%x", addr, key, cached, got))
	}
}

// --- pure delegators --------------------------------------------------------

func (t *wrappingTrie) GetKey(k []byte) []byte { return t.inner.GetKey(k) }

func (t *wrappingTrie) Hash() common.Hash {
	if t.batchAccount {
		return t.db.batchRoot
	}
	return t.inner.Hash()
}

func (t *wrappingTrie) Commit(collectLeaf bool) (common.Hash, *trienode.NodeSet, error) {
	if t.batchAccount {
		return t.db.batchRoot, trienode.NewNodeSet(common.Hash{}), nil
	}
	return t.inner.Commit(collectLeaf)
}

func (t *wrappingTrie) NodeIterator(startKey []byte) (trie.NodeIterator, error) {
	return t.inner.NodeIterator(startKey)
}

func (t *wrappingTrie) Prove(key []byte, proofDb ethdb.KeyValueWriter) error {
	return t.inner.Prove(key, proofDb)
}

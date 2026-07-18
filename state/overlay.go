package state

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"syscall"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/state"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/crypto"
	"github.com/ava-labs/libevm/ethdb"
	"github.com/ava-labs/libevm/rlp"
	"github.com/ava-labs/libevm/trie"
	"github.com/ava-labs/libevm/trie/trienode"
	"github.com/ava-labs/libevm/triedb"
	"github.com/holiman/uint256"
)

// SentinelStorageRoot is the storage root every historical account carries.
// The real per-account root is unknowable by design (no per-block tries);
// this is non-empty so geth never short-circuits storage reads on
// EmptyRootHash, and it is deliberately not a root anything downstream can
// resolve.
var SentinelStorageRoot = crypto.Keccak256Hash([]byte("epochdb: sentinel storage root"))

var errOverlayReadOnly = errors.New("epochdb overlay: read-only historical view")

// History serves historical account/storage reads from the cooked
// sorted_NNNNN.idx buckets (mmap binary search), falling through to the
// genesis alloc for keys never written on-chain. It also owns header-root
// lookups for Hash() (bucketLog is not goroutine-safe, so those go through
// a mutex).
type History struct {
	store   *Store
	genesis types.GenesisAlloc
	buckets []*sortedBucket // ascending bucket number
	head    uint64          // highest cooked block

	// deletes: account key -> ascending blocks of explicit account-delete
	// records (SELFDESTRUCT). Built by one sequential scan at open; rare
	// enough to keep resident, and it turns the per-read delete check into
	// a map lookup (a linear run scan here was 96% of backfill CPU).
	deletes map[string][]uint64

	hdrMu sync.Mutex // guards store header reads (bucketLog LRU state)
}

type sortedBucket struct {
	bucket uint64
	mm     []byte
	n      int
	wl     *os.File // writelog_NNNNN.log, values live here
}

// OpenHistory mmaps every sorted bucket in dir. alloc is the genesis alloc
// used as the below-first-capture floor.
func OpenHistory(dir string, store *Store, alloc types.GenesisAlloc) (*History, error) {
	h := &History{store: store, genesis: alloc}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		var bucket uint64
		if _, err := fmt.Sscanf(e.Name(), "sorted_%d.idx", &bucket); err != nil {
			continue
		}
		sb, cookedThrough, err := openSortedBucket(dir, bucket)
		if err != nil {
			h.Close()
			return nil, fmt.Errorf("open sorted bucket %05d: %w", bucket, err)
		}
		h.buckets = append(h.buckets, sb)
		if cookedThrough > h.head {
			h.head = cookedThrough
		}
	}
	sort.Slice(h.buckets, func(i, j int) bool { return h.buckets[i].bucket < h.buckets[j].bucket })
	if len(h.buckets) == 0 {
		return nil, fmt.Errorf("no sorted_NNNNN.idx buckets in %s (run epochdb cook-index)", dir)
	}
	// Ascending buckets + per-bucket (key, block) order = ascending blocks
	// per key without sorting.
	h.deletes = make(map[string][]uint64)
	for _, b := range h.buckets {
		for i := 0; i < b.n; i++ {
			r := b.rec(i)
			if r[0] == recKindAccount && binary.BigEndian.Uint32(r[69:73]) == 0 {
				k := string(r[:sortedKeySize])
				h.deletes[k] = append(h.deletes[k], binary.BigEndian.Uint64(r[53:61]))
			}
		}
	}
	return h, nil
}

func openSortedBucket(dir string, bucket uint64) (*sortedBucket, uint64, error) {
	f, err := os.Open(filepath.Join(dir, sortedName(bucket)))
	if err != nil {
		return nil, 0, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return nil, 0, err
	}
	size := int(st.Size())
	if size < sortedHdrSize || (size-sortedHdrSize)%sortedRecSize != 0 {
		return nil, 0, fmt.Errorf("bad size %d", size)
	}
	mm, err := syscall.Mmap(int(f.Fd()), 0, size, syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, 0, err
	}
	if !bytes.Equal(mm[0:4], sortedMagic[:]) || binary.BigEndian.Uint32(mm[4:8]) != sortedRecSize {
		syscall.Munmap(mm)
		return nil, 0, fmt.Errorf("bad header")
	}
	cookedThrough := binary.BigEndian.Uint64(mm[8:16])
	wlSize := binary.BigEndian.Uint64(mm[16:24])
	wl, err := os.Open(filepath.Join(dir, fmt.Sprintf("writelog_%05d.log", bucket)))
	if err != nil {
		syscall.Munmap(mm)
		return nil, 0, err
	}
	wst, err := wl.Stat()
	if err == nil && uint64(wst.Size()) < wlSize {
		err = fmt.Errorf("stale sorted index: writelog is %d bytes, cooked against %d (re-run epochdb cook-index)", wst.Size(), wlSize)
	}
	if err != nil {
		syscall.Munmap(mm)
		wl.Close()
		return nil, 0, err
	}
	return &sortedBucket{
		bucket: bucket,
		mm:     mm,
		n:      (size - sortedHdrSize) / sortedRecSize,
		wl:     wl,
	}, cookedThrough, nil
}

// Close unmaps and closes every bucket.
func (h *History) Close() {
	for _, b := range h.buckets {
		syscall.Munmap(b.mm)
		b.wl.Close()
	}
	h.buckets = nil
}

// Head returns the highest block the cooked index covers.
func (h *History) Head() uint64 { return h.head }

// HeaderRLP returns the stored header RLP for block n (mutex-guarded: the
// underlying bucketLog mutates LRU state on every read).
func (h *History) HeaderRLP(n uint64) ([]byte, bool, error) {
	h.hdrMu.Lock()
	defer h.hdrMu.Unlock()
	return h.store.HeaderRLP(n)
}

func (b *sortedBucket) rec(i int) []byte {
	off := sortedHdrSize + i*sortedRecSize
	return b.mm[off : off+sortedRecSize]
}

// upperBound returns the index of the first record > (key, n).
func (b *sortedBucket) upperBound(key []byte, n uint64) int {
	return sort.Search(b.n, func(i int) bool {
		r := b.rec(i)
		if c := bytes.Compare(r[:sortedKeySize], key); c != 0 {
			return c > 0
		}
		return binary.BigEndian.Uint64(r[53:61]) > n
	})
}

// lookup finds the largest write <= n of key inside this bucket.
// val is nil for an explicit delete/zero record.
func (b *sortedBucket) lookup(key []byte, n uint64) (val []byte, blk uint64, found bool, err error) {
	i := b.upperBound(key, n) - 1
	if i < 0 {
		return nil, 0, false, nil
	}
	r := b.rec(i)
	if !bytes.Equal(r[:sortedKeySize], key) {
		return nil, 0, false, nil
	}
	blk = binary.BigEndian.Uint64(r[53:61])
	vlen := binary.BigEndian.Uint32(r[69:73])
	if vlen == 0 {
		return nil, blk, true, nil
	}
	voff := binary.BigEndian.Uint64(r[61:69])
	val = make([]byte, vlen)
	if _, err := b.wl.ReadAt(val, int64(voff)); err != nil {
		return nil, 0, false, fmt.Errorf("writelog bucket %05d read at %d: %w", b.bucket, voff, err)
	}
	return val, blk, true, nil
}

// search descends buckets from bucket(n) downward for the largest write <=
// n of key. A key missing from a bucket falls through to the next lower
// bucket; nothing anywhere = (found=false), i.e. genesis/zero territory.
func (h *History) search(key []byte, n uint64) (val []byte, blk uint64, found bool, err error) {
	for i := len(h.buckets) - 1; i >= 0; i-- {
		b := h.buckets[i]
		if b.bucket*BucketBlocks > n {
			continue
		}
		val, blk, found, err = b.lookup(key, n)
		if err != nil || found {
			return val, blk, found, err
		}
	}
	return nil, 0, false, nil
}

// lastAccountDelete finds the largest block <= n where addr's account was
// deleted (map lookup over the open-time delete index).
func (h *History) lastAccountDelete(key []byte, n uint64) (uint64, bool) {
	ds := h.deletes[string(key)]
	i := sort.Search(len(ds), func(i int) bool { return ds[i] > n }) - 1
	if i < 0 {
		return 0, false
	}
	return ds[i], true
}

func accountKey(addr common.Address) []byte {
	k := make([]byte, sortedKeySize)
	k[0] = recKindAccount
	copy(k[1:21], addr[:])
	return k
}

func storageKey(addr common.Address, slot []byte) []byte {
	k := make([]byte, sortedKeySize)
	k[0] = recKindStorage
	copy(k[1:21], addr[:])
	copy(k[21:53], slot) // left-aligned, exactly as capture recordStorage stores it
	return k
}

// AccountAt returns the account state of addr as of the end of block n.
// nil, nil = account does not exist at n. The returned Root is always
// SentinelStorageRoot.
func (h *History) AccountAt(addr common.Address, n uint64) (*types.StateAccount, error) {
	val, _, found, err := h.search(accountKey(addr), n)
	if err != nil {
		return nil, err
	}
	if !found {
		// Below the first capture / never written: genesis alloc floor.
		ga, ok := h.genesis[addr]
		if !ok {
			return nil, nil
		}
		acc := &types.StateAccount{
			Nonce:    ga.Nonce,
			Balance:  new(uint256.Int),
			Root:     SentinelStorageRoot,
			CodeHash: types.EmptyCodeHash.Bytes(),
		}
		if ga.Balance != nil {
			acc.Balance = uint256.MustFromBig(ga.Balance)
		}
		if len(ga.Code) > 0 {
			acc.CodeHash = crypto.Keccak256(ga.Code)
		}
		return acc, nil
	}
	if len(val) == 0 {
		return nil, nil // explicit delete: account gone at n
	}
	var acc types.StateAccount
	if err := rlp.DecodeBytes(val, &acc); err != nil {
		return nil, fmt.Errorf("decode account %x at %d: %w", addr, n, err)
	}
	acc.Root = SentinelStorageRoot
	return &acc, nil
}

// StorageAt returns the trie-encoded (RLP of trimmed bytes) value of
// (addr, slot) as of the end of block n; nil = zero. A storage write that
// predates the account's most recent deletion at or before n is dead and
// reads as zero (SELFDESTRUCT drops the whole storage trie).
func (h *History) StorageAt(addr common.Address, slot []byte, n uint64) ([]byte, error) {
	aKey := accountKey(addr)
	val, wblk, found, err := h.search(storageKey(addr, slot), n)
	if err != nil {
		return nil, err
	}
	if found {
		if d, ok := h.lastAccountDelete(aKey, n); ok && d > wblk {
			return nil, nil
		}
		return val, nil
	}
	if _, ok := h.lastAccountDelete(aKey, n); ok {
		return nil, nil // account deleted at some point <= n, genesis storage is dead
	}
	ga, ok := h.genesis[addr]
	if !ok || ga.Storage == nil {
		return nil, nil
	}
	var sl common.Hash
	copy(sl[:], slot)
	v, ok := ga.Storage[sl]
	if !ok || v == (common.Hash{}) {
		return nil, nil
	}
	enc, err := rlp.EncodeToBytes(common.TrimLeftZeroes(v[:]))
	if err != nil {
		return nil, err
	}
	return enc, nil
}

// CodeByHash serves contract code from the code.log RAM map only, never any
// frontier. EmptyCodeHash resolves to nil.
func (h *History) CodeByHash(codeHash common.Hash) ([]byte, error) {
	if codeHash == types.EmptyCodeHash || codeHash == (common.Hash{}) {
		return nil, nil
	}
	blob, ok, err := h.store.code.Get(codeHash)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("code %x not in code.log", codeHash)
	}
	return blob, nil
}

// CodeAt returns addr's contract code as of the end of block n (nil for
// EOAs and nonexistent accounts).
func (h *History) CodeAt(addr common.Address, n uint64) ([]byte, error) {
	acc, err := h.AccountAt(addr, n)
	if err != nil || acc == nil {
		return nil, err
	}
	return h.CodeByHash(common.BytesToHash(acc.CodeHash))
}

// SampleRecord returns a random cooked record for bench probing:
// kind 'a' with a zero slot, or kind 's' with the slot key.
func (h *History) SampleRecord(r *rand.Rand) (kind byte, addr common.Address, slot common.Hash, block uint64, ok bool) {
	total := 0
	for _, b := range h.buckets {
		total += b.n
	}
	if total == 0 {
		return 0, addr, slot, 0, false
	}
	i := r.Intn(total)
	for _, b := range h.buckets {
		if i >= b.n {
			i -= b.n
			continue
		}
		rec := b.rec(i)
		copy(addr[:], rec[1:21])
		copy(slot[:], rec[21:53])
		return rec[0], addr, slot, binary.BigEndian.Uint64(rec[53:61]), true
	}
	return 0, addr, slot, 0, false
}

// StateAt returns a read-only libevm state.Database serving the historical
// read contract as of the end of block n.
func (h *History) StateAt(n uint64) state.Database {
	return &Overlay{hist: h, target: n}
}

// Overlay is the read-only historical state.Database at a fixed target
// block. Only value reads work; every trie-structure or write method fails
// loudly.
type Overlay struct {
	hist   *History
	target uint64
}

func (o *Overlay) OpenTrie(common.Hash) (state.Trie, error) {
	// The root argument is ignored: the overlay is pinned to its target
	// block, there is no Merkle trie to open.
	return &overlayTrie{o: o}, nil
}

func (o *Overlay) OpenStorageTrie(_ common.Hash, _ common.Address, _ common.Hash, _ state.Trie) (state.Trie, error) {
	return &overlayTrie{o: o}, nil
}

func (o *Overlay) CopyTrie(t state.Trie) state.Trie {
	if w, ok := t.(*overlayTrie); ok {
		return &overlayTrie{o: w.o}
	}
	panic(fmt.Sprintf("epochdb overlay: CopyTrie of foreign trie %T", t))
}

func (o *Overlay) ContractCode(_ common.Address, codeHash common.Hash) ([]byte, error) {
	return o.hist.CodeByHash(codeHash)
}

func (o *Overlay) ContractCodeSize(addr common.Address, codeHash common.Hash) (int, error) {
	blob, err := o.ContractCode(addr, codeHash)
	return len(blob), err
}

func (o *Overlay) DiskDB() ethdb.KeyValueStore { return o.hist.store.EthDB() }

// TrieDB is not on any served read path (eth_call and the simple getters
// never touch it); a nil return fails loudly at the caller if that ever
// changes.
func (o *Overlay) TrieDB() *triedb.Database { return nil }

// overlayTrie implements the value-read subset of state.Trie.
type overlayTrie struct {
	o *Overlay
}

func (t *overlayTrie) GetKey(k []byte) []byte { return k }

func (t *overlayTrie) GetAccount(addr common.Address) (*types.StateAccount, error) {
	return t.o.hist.AccountAt(addr, t.o.target)
}

func (t *overlayTrie) GetStorage(addr common.Address, key []byte) ([]byte, error) {
	return t.o.hist.StorageAt(addr, key, t.o.target)
}

// Hash returns header(target).Root: the one true root for this height.
func (t *overlayTrie) Hash() common.Hash {
	raw, ok, err := t.o.hist.HeaderRLP(t.o.target)
	if err != nil || !ok {
		panic(fmt.Sprintf("epochdb overlay: header %d unavailable: ok=%v err=%v", t.o.target, ok, err))
	}
	var h types.Header
	if err := rlp.DecodeBytes(raw, &h); err != nil {
		panic(fmt.Sprintf("epochdb overlay: decode header %d: %v", t.o.target, err))
	}
	return h.Root
}

// --- fail-loud: writes and trie-structure methods ---------------------------

func (t *overlayTrie) UpdateAccount(common.Address, *types.StateAccount) error {
	return errOverlayReadOnly
}
func (t *overlayTrie) UpdateStorage(common.Address, []byte, []byte) error {
	return errOverlayReadOnly
}
func (t *overlayTrie) DeleteAccount(common.Address) error         { return errOverlayReadOnly }
func (t *overlayTrie) DeleteStorage(common.Address, []byte) error { return errOverlayReadOnly }
func (t *overlayTrie) UpdateContractCode(common.Address, common.Hash, []byte) error {
	return errOverlayReadOnly
}
func (t *overlayTrie) Commit(bool) (common.Hash, *trienode.NodeSet, error) {
	return common.Hash{}, nil, errOverlayReadOnly
}
func (t *overlayTrie) NodeIterator([]byte) (trie.NodeIterator, error) {
	return nil, errors.New("epochdb overlay: NodeIterator unsupported over history")
}
func (t *overlayTrie) Prove([]byte, ethdb.KeyValueWriter) error {
	return errors.New("epochdb overlay: Prove unsupported over history (no Merkle nodes)")
}

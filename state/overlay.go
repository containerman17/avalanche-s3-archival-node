package state

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
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
// sorted_NNNNN.idx buckets (mmap binary search) and sealed epochs, falling
// through to the genesis alloc for keys never written. Fully goroutine-safe:
// everything here is immutable after open and the store's bucketLogs are
// internally locked.
//
// There is no floor and no pruned mode (DESIGN.md ruling 1 of 2026-07-31):
// every node's descent ends at the genesis alloc, and a bootstrapped node
// gets its Firewood frontier from the epochs themselves (exec.BuildFrontier).
type History struct {
	dir     string
	store   *Store
	genesis types.GenesisAlloc
	epochs  *EpochSet // sealed epochs (may be empty)

	// head is the highest block the SERVER answers for. On a static node it
	// equals stateHead; serve --follow advances it to the executor's head
	// (blocks, headers, receipts and logs all come from live sources) and
	// leaves stateHead trailing by at most one cook cadence.
	head atomic.Uint64
	// stateHead is the highest block the historical DESCENT can answer:
	// the cooked buckets plus sealed epochs. search refuses above it rather
	// than silently descending past uncooked writes, which would return a
	// stale value for a key written in the gap.
	stateHead atomic.Uint64

	// tail is the uncooked-write overlay (state/tail.go), non-nil only after
	// EnableTail: the executor's writes since the last cook. It is the FIRST
	// half of the one read path; `latest` state is this then the descent.
	// nil everywhere else (cook, seal, read-only openers), where every
	// read is historical by definition.
	tail *tail
	// tailApplied is the highest block folded into the overlay, owned by the
	// single goroutine that calls EnableTail/AdvanceTail. It only moves up, so
	// a prune (which drops cooked entries) never makes work reappear.
	tailApplied uint64

	// sealRetryAt is the exec head below which SealTail cannot possibly cut an
	// epoch, so it does not even open the corpus. Owned by the same single
	// goroutine that calls SealTail (the cook loop).
	sealRetryAt uint64

	// mu guards buckets and deletes, which Refresh swaps when cook lands new
	// sorted files. Everything else here is immutable after open.
	mu      sync.RWMutex
	buckets []*sortedBucket // ascending bucket number

	// deletes: account key -> ascending blocks of explicit account-delete
	// records (SELFDESTRUCT, and EIP-158 empty-account deletion). Built by one
	// sequential scan at open; rare enough to keep resident, and it turns the
	// per-read delete check into a map lookup (a linear run scan here was 96%
	// of backfill CPU).
	//
	// MEASURED 2026-07-31 before deciding whether this had to become a mapped
	// binary search like the tx index (DESIGN.md ruling 3): over mainnet
	// blocks 1..200,000 (data-v3's cooked buckets, 1.71M rows) there are
	// 13,747 delete rows and exactly TWO distinct deleted accounts, 0x..01 and
	// 0x..02, the ecrecover and sha256 precompiles emptied by touch-and-delete.
	// So this map is a couple of dense []uint64 runs, 8 B per row, against the
	// 61 B per row the epochs' deletes sections spend on the same facts, and
	// the row count is bounded by BLOCK count (one post-image row per key per
	// block), not by tx count: 0.069 rows/block measured, i.e. ~6.2M rows and
	// ~50MB at mainnet's 90M blocks, a few hundred MB even if every block
	// starts emptying a precompile. It stays a heap map. REVISIT only if a
	// real corpus shows delete rows approaching one per block AND many
	// distinct accounts, which is when the per-epoch bloom-gated binary search
	// (over sections that are already mapped and pinned) starts paying.
	deletes map[string][]uint64
}

type sortedBucket struct {
	bucket uint64
	mm     []byte
	n      int
	wl     *os.File // writelog_NNNNN.log, values live here
	cooked uint64   // cookedThrough from the file header (Refresh's change test)
}

// OpenHistory mmaps every sorted bucket in dir. alloc is the genesis alloc,
// the floor every descent ends at.
func OpenHistory(dir string, store *Store, alloc types.GenesisAlloc) (*History, error) {
	h := &History{dir: dir, store: store, genesis: alloc}
	var (
		stateHead uint64
		err       error
	)
	if h.buckets, stateHead, err = openSortedBuckets(dir, stateHead); err != nil {
		h.Close()
		return nil, err
	}
	// The epoch set belongs to the Store, so the executor's rawdb reads and
	// this descent answer from ONE set (state/store.go Code).
	if h.epochs, err = store.Epochs(); err != nil {
		h.Close()
		return nil, err
	}
	if end, ok := h.epochs.SealedEnd(); ok && end > stateHead {
		stateHead = end
	}
	if len(h.buckets) == 0 && len(h.epochs.All()) == 0 {
		// A never-cooked dir is a valid empty window, not an error: the node
		// cooks at startup and Refresh picks the buckets up. Refusing here
		// would make first start on a raw corpus impossible.
		log.Printf("state: nothing cooked or sealed in %s yet, historical window empty until the first cook", dir)
	}
	h.deletes = make(map[string][]uint64)
	addEpochDeletes(h.deletes, h.epochs.All())
	addBucketDeletes(h.deletes, h.buckets)
	h.stateHead.Store(stateHead)
	h.head.Store(stateHead)
	return h, nil
}

// openSortedBuckets mmaps every sorted_NNNNN.idx in dir, ascending, and
// returns the highest cooked height seen (never below floorHead).
func openSortedBuckets(dir string, floorHead uint64) ([]*sortedBucket, uint64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, 0, err
	}
	var out []*sortedBucket
	for _, e := range entries {
		var bucket uint64
		if _, err := fmt.Sscanf(e.Name(), "sorted_%d.idx", &bucket); err != nil {
			continue
		}
		sb, cookedThrough, err := openSortedBucket(dir, bucket)
		if err != nil {
			for _, b := range out {
				b.close()
			}
			return nil, 0, fmt.Errorf("open sorted bucket %05d: %w", bucket, err)
		}
		out = append(out, sb)
		if cookedThrough > floorHead {
			floorHead = cookedThrough
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].bucket < out[j].bucket })
	return out, floorHead, nil
}

// deleteAdder folds account-delete rows into dst, keeping each key's block
// list sorted and deduped. Raw buckets STILL overlap sealed epochs even though
// seal always deletes fully-sealed raws (buckets are 100k blocks, epochs cut
// on tx count), a re-cooked tip bucket is a superset of its previous cook, and
// an epoch published by the in-process seal repeats rows the cooked buckets
// already contributed, so dedupe is load-bearing at open, on Refresh and on
// RefreshEpochs alike.
type deleteAdder struct {
	dst     map[string][]uint64
	touched map[string]bool
}

func newDeleteAdder(dst map[string][]uint64) *deleteAdder {
	return &deleteAdder{dst: dst, touched: make(map[string]bool)}
}

func (a *deleteAdder) add(key []byte, blk uint64) {
	k := string(key)
	if !a.touched[k] {
		// Refresh works on a shallow copy of the map, so the first touch must
		// clone: append + the dedupe in done rewrite the backing array, which
		// a reader may still be holding from the previous map.
		a.dst[k] = append(append([]uint64(nil), a.dst[k]...), blk)
		a.touched[k] = true
		return
	}
	a.dst[k] = append(a.dst[k], blk)
}

func (a *deleteAdder) done() {
	for k := range a.touched {
		ds := a.dst[k]
		sort.Slice(ds, func(i, j int) bool { return ds[i] < ds[j] })
		out := ds[:0]
		for i, d := range ds {
			if i == 0 || d != ds[i-1] {
				out = append(out, d)
			}
		}
		a.dst[k] = out
	}
}

func addBucketDeletes(dst map[string][]uint64, buckets []*sortedBucket) {
	a := newDeleteAdder(dst)
	for _, b := range buckets {
		for i := 0; i < b.n; i++ {
			r := b.rec(i)
			if r[0] == recKindAccount && binary.BigEndian.Uint32(r[69:73]) == 0 {
				a.add(r[:sortedKeySize], binary.BigEndian.Uint64(r[53:61]))
			}
		}
	}
	a.done()
}

func addEpochDeletes(dst map[string][]uint64, epochs []*Epoch) {
	a := newDeleteAdder(dst)
	for _, e := range epochs {
		e.AccountDeletes(a.add)
	}
	a.done()
}

// Refresh re-opens the sorted buckets whose cook advanced (and any new one),
// then republishes stateHead. serve --follow calls it after each in-process
// cook so the historical descent chases the executor; a static serve never
// calls it. Safe beside live readers: the swap happens under the write lock
// and a replaced mmap is unmapped only after no reader can hold it.
//
// ponytail: re-scans a changed bucket's rows whole to merge its deletes. A
// bucket is 100k blocks, so the cost is bounded by the tip bucket, not by the
// corpus; make it incremental only if a profile ever blames it.
func (h *History) Refresh() error {
	h.mu.RLock()
	cur := make(map[uint64]uint64, len(h.buckets))
	for _, b := range h.buckets {
		cur[b.bucket] = b.cooked
	}
	h.mu.RUnlock()

	entries, err := os.ReadDir(h.dir)
	if err != nil {
		return err
	}
	var changed []uint64
	for _, e := range entries {
		var bucket uint64
		if _, err := fmt.Sscanf(e.Name(), "sorted_%d.idx", &bucket); err != nil {
			continue
		}
		cooked, _, ok := readSortedHeader(filepath.Join(h.dir, sortedName(bucket)))
		if !ok {
			continue
		}
		if old, have := cur[bucket]; !have || cooked > old {
			changed = append(changed, bucket)
		}
	}
	if len(changed) == 0 {
		return nil
	}

	fresh := make([]*sortedBucket, 0, len(changed))
	for _, bucket := range changed {
		sb, _, err := openSortedBucket(h.dir, bucket)
		if err != nil {
			for _, b := range fresh {
				b.close()
			}
			return fmt.Errorf("refresh sorted bucket %05d: %w", bucket, err)
		}
		fresh = append(fresh, sb)
	}

	h.mu.Lock()
	replaced := make([]*sortedBucket, 0, len(fresh))
	byBucket := make(map[uint64]*sortedBucket, len(h.buckets)+len(fresh))
	for _, b := range h.buckets {
		byBucket[b.bucket] = b
	}
	for _, b := range fresh {
		if old, ok := byBucket[b.bucket]; ok {
			replaced = append(replaced, old)
		}
		byBucket[b.bucket] = b
	}
	list := make([]*sortedBucket, 0, len(byBucket))
	var stateHead uint64
	for _, b := range byBucket {
		list = append(list, b)
		if b.cooked > stateHead {
			stateHead = b.cooked
		}
	}
	sort.Slice(list, func(i, j int) bool { return list[i].bucket < list[j].bucket })
	deletes := make(map[string][]uint64, len(h.deletes))
	for k, v := range h.deletes {
		deletes[k] = v
	}
	addBucketDeletes(deletes, fresh)
	h.buckets, h.deletes = list, deletes
	h.mu.Unlock()

	// Readers hold the read lock for the whole descent, so the write lock
	// above already drained them: nothing can still be inside a replaced mmap.
	for _, b := range replaced {
		b.close()
	}
	if end, ok := h.epochs.SealedEnd(); ok && end > stateHead {
		stateHead = end
	}
	h.stateHead.Store(stateHead)
	if h.head.Load() < stateHead {
		h.head.Store(stateHead)
	}
	return nil
}

// Epochs exposes the sealed epoch set (shared with the serve wiring for
// bodies and tx lookups).
func (h *History) Epochs() *EpochSet { return h.epochs }

// refreshEpochs publishes epochs sealed since open to the live read path: the
// set is reloaded IN PLACE, so every reader that captured it (the RPC body
// source, the combined tx index) starts answering from them without being
// rewired, and their account deletes join the descent's delete index. Returns
// how many epochs it added.
//
// This is step two of the in-process seal, and it must complete before the raw
// buckets those epochs replace are deleted: after it, every read of a sealed
// height has a sealed answer, so losing the raw one costs nothing.
func (h *History) refreshEpochs() (int, error) {
	fresh, err := h.epochs.Reload(h.store.Cas())
	if err != nil || len(fresh) == 0 {
		return 0, err
	}
	h.mu.Lock()
	deletes := make(map[string][]uint64, len(h.deletes))
	for k, v := range h.deletes {
		deletes[k] = v
	}
	addEpochDeletes(deletes, fresh)
	h.deletes = deletes
	h.mu.Unlock()
	if end, ok := h.epochs.SealedEnd(); ok && end > h.stateHead.Load() {
		h.stateHead.Store(end)
		if h.head.Load() < end {
			h.head.Store(end)
		}
	}
	return len(fresh), nil
}

// dropSealedBuckets releases the raw buckets whose whole range is at or below
// sealedEnd, LAST step of the in-process seal. It is what makes the deletion
// actually free disk: an unlinked file that this process still mmaps (the
// sorted index) or still holds open (its writelog) keeps its blocks allocated
// until the handle goes. Every dropped block is inside a published epoch, so
// the descent answers it from there.
func (h *History) dropSealedBuckets(sealedEnd uint64) int {
	h.mu.Lock()
	var keep, drop []*sortedBucket
	for _, b := range h.buckets {
		if (b.bucket+1)*bucketBlocks-1 <= sealedEnd {
			drop = append(drop, b)
			continue
		}
		keep = append(keep, b)
	}
	if len(drop) > 0 {
		h.buckets = keep
	}
	h.mu.Unlock()
	// Same drain rule as Refresh: readers hold the read lock for the whole
	// descent, so the write lock above already got them out of these mmaps.
	for _, b := range drop {
		b.close()
	}
	return len(drop)
}

// LogTuples returns block n's captured log tuple record from the raw logs
// bucketLog (unsealed-tail getLogs candidates). ok=false = no logs.
// Goroutine-safe (bucketLog is internally locked).
func (h *History) LogTuples(n uint64) (LogRec, bool, error) {
	rec, ok, err := h.store.LogsRecord(n)
	if err != nil || !ok {
		return LogRec{}, false, err
	}
	lr, err := decodeLogRec(n, rec)
	if err != nil {
		return LogRec{}, false, err
	}
	return lr, true, nil
}

// StoredTail returns block n's captured receipts and full-logs records from
// the live tail family, in the SAME encoding the sealed epoch sections hold.
// ok=false = no record: the block had no transactions, or it predates capture
// (an old corpus, or a node that only ever downloaded epochs).
// Goroutine-safe (bucketLog is internally locked).
func (h *History) StoredTail(n uint64) (logsRec, rcptRec []byte, ok bool, err error) {
	rec, ok, err := h.store.RcptRecord(n)
	if err != nil || !ok {
		return nil, nil, false, err
	}
	logsRec, rcptRec, err = DecodeTailRcpt(rec)
	if err != nil {
		return nil, nil, false, fmt.Errorf("tail receipts record %d: %w", n, err)
	}
	return logsRec, rcptRec, true, nil
}

// ModifiedAccounts returns the unique addresses whose account record or
// storage changed in block n, in capture order (code-use records are reads,
// not modifications). ok=false = no write frame captured for n: an empty
// block, or the raw writelog is absent (epoch-only node: seal deletes fully
// sealed raw buckets, so this answers for the tail only).
// Goroutine-safe (bucketLog is internally locked).
func (h *History) ModifiedAccounts(n uint64) ([]common.Address, bool, error) {
	frame, ok, err := h.store.wl.Get(n)
	if err != nil || !ok {
		return nil, false, err
	}
	var out []common.Address
	seen := map[common.Address]bool{}
	if err := parseFrame(frame, func(kind byte, key [sortedKeySize]byte, _ int, _ uint32) {
		if kind == recKindCodeUse {
			return
		}
		var a common.Address
		copy(a[:], key[1:21])
		if !seen[a] {
			seen[a] = true
			out = append(out, a)
		}
	}); err != nil {
		return nil, false, err
	}
	return out, true, nil
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
		err = fmt.Errorf("stale sorted index: writelog is %d bytes, cooked against %d (the node cooks on its own cadence; `epochdb dev cook-index` fixes an offline dir)", wst.Size(), wlSize)
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
		cooked: cookedThrough,
	}, cookedThrough, nil
}

func (b *sortedBucket) close() {
	syscall.Munmap(b.mm)
	b.wl.Close()
}

// Close unmaps and closes every bucket. The epoch set is the Store's, so
// Store.Close is what closes it.
func (h *History) Close() {
	for _, b := range h.buckets {
		b.close()
	}
	h.buckets = nil
}

// Head returns the highest block this node serves. Static serve: the cooked
// index. serve --follow: the executor's head, which the tail sources (headers,
// receipts, logs, containers) already answer for.
func (h *History) Head() uint64 { return h.head.Load() }

// SetHead publishes a new serving head (serve --follow, after the executor
// advanced and the block-hash index caught up). Never moves below stateHead.
func (h *History) SetHead(n uint64) {
	if n < h.stateHead.Load() {
		return
	}
	h.head.Store(n)
}

// StateHead returns the highest block the historical descent can answer:
// heads above it need the executor's live frontier (serve --follow) and are
// refused by the descent.
func (h *History) StateHead() uint64 { return h.stateHead.Load() }

// HeaderRLP returns the stored header RLP for block n: raw store first
// (bucketLog is internally locked), sealed epochs as the fallback once raw
// buckets are deleted. Goroutine-safe.
func (h *History) HeaderRLP(n uint64) ([]byte, bool, error) {
	raw, ok, err := h.store.HeaderRLP(n)
	if err != nil || ok {
		return raw, ok, err
	}
	if e, ok := h.epochs.At(n); ok {
		hdr, err := e.HeaderRLP(n)
		if err != nil {
			return nil, false, err
		}
		return hdr, true, nil
	}
	return nil, false, nil
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
// n of key, then the sealed epochs newest-to-oldest (bloom-gated). A key
// missing from a bucket/epoch falls through to the next lower one; nothing
// anywhere = (found=false), i.e. genesis/zero territory. The straddling raw
// bucket overlaps the newest epoch (bucket and epoch boundaries never align);
// the data is identical, so whichever side hits first wins.
func (h *History) search(key []byte, n uint64) (val []byte, blk uint64, found bool, err error) {
	// State correctness requires full descent to genesis: refuse above a
	// coverage hole instead of silently skipping history. This is the choke
	// point, hence the only coverage check.
	if err := h.epochs.RequireCovered(n); err != nil {
		return nil, 0, false, err
	}
	// Above the cooked watermark the descent would skip straight past the
	// uncooked writes and answer with an older value. serve --follow answers
	// the frontier heights from the executor instead; everything in between is
	// cook lag and says so.
	if sh := h.stateHead.Load(); n > sh {
		return nil, 0, false, fmt.Errorf("state at block %d is not indexed yet (cooked through %d, cook lag): retry, or read at latest", n, sh)
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	for i := len(h.buckets) - 1; i >= 0; i-- {
		b := h.buckets[i]
		if b.bucket*BucketBlocks > n {
			continue
		}
		if val, blk, found, err = b.lookup(key, n); err != nil || found {
			return val, blk, found, err
		}
	}
	eps := h.epochs.All()
	for i := len(eps) - 1; i >= 0; i-- {
		e := eps[i]
		if e.Start > n || !e.MayContainKey(key) {
			continue
		}
		if val, blk, found, err = e.StateSearch(key, n); err != nil || found {
			return val, blk, found, err
		}
	}
	return nil, 0, false, nil
}

// lastAccountDelete finds the largest block <= n where addr's account was
// deleted (map lookup over the open-time delete index).
func (h *History) lastAccountDelete(key []byte, n uint64) (uint64, bool) {
	h.mu.RLock()
	ds := h.deletes[string(key)]
	h.mu.RUnlock()
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
	return decodeAccount(val, addr) // empty value = explicit delete, account gone
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

// CodeByHash serves contract code from code.log (the hot tail: everything
// deployed above the sealed end), then the sealed epochs newest-to-oldest,
// then the genesis alloc. Never any frontier. EmptyCodeHash resolves to nil.
//
// The epoch descent is what makes a download-bootstrapped node (no code.log
// at all) serve eth_getCode: a v3 epoch carries the code of every account it
// wrote, so whichever epoch answered the account read carries the blob too.
func (h *History) CodeByHash(codeHash common.Hash) ([]byte, error) {
	if codeHash == types.EmptyCodeHash || codeHash == (common.Hash{}) {
		return nil, nil
	}
	blob, ok, err := h.store.Code(codeHash)
	if err != nil {
		return nil, err
	}
	if !ok {
		// Genesis alloc code was never deployed by a block, so no epoch
		// carries it; the alloc ships with the binary.
		for _, ga := range h.genesis {
			if len(ga.Code) > 0 && crypto.Keccak256Hash(ga.Code) == codeHash {
				return ga.Code, nil
			}
		}
		return nil, fmt.Errorf("code %x not in code.log or the sealed epochs", codeHash)
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

// SampleRecord returns a random state record for bench probing: kind 'a'
// with a zero slot, or kind 's' with the slot key. Samples raw cooked
// buckets when present, sealed epochs otherwise (raw-free node).
func (h *History) SampleRecord(r *rand.Rand) (kind byte, addr common.Address, slot common.Hash, block uint64, ok bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	total := 0
	for _, b := range h.buckets {
		total += b.n
	}
	if total == 0 {
		return h.sampleEpochRecord(r)
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

// sampleEpochRecord picks a random sparse-index entry from a random epoch
// SST and a random row inside its block.
func (h *History) sampleEpochRecord(r *rand.Rand) (kind byte, addr common.Address, slot common.Hash, block uint64, ok bool) {
	eps := h.epochs.All()
	if len(eps) == 0 {
		return 0, addr, slot, 0, false
	}
	// An SST block can hold nothing but code rows, which are not state:
	// resample instead of reporting an empty corpus.
	var (
		key [sortedKeySize]byte
		blk uint64
	)
	for try := 0; try < 10 && !ok; try++ {
		key, blk, ok = eps[r.Intn(len(eps))].sampleSSTRow(r)
	}
	if !ok {
		return 0, addr, slot, 0, false
	}
	copy(addr[:], key[1:21])
	copy(slot[:], key[21:53])
	return key[0], addr, slot, blk, true
}

// StateAt returns a read-only libevm state.Database serving the historical
// read contract as of the end of block n.
func (h *History) StateAt(n uint64) state.Database {
	return &Overlay{hist: h, target: n}
}

// --- the executed head: uncooked-tail overlay, then the descent -------------

// EnableTail turns on the uncooked-write overlay and backfills it from the
// raw writelog for (cooked watermark, executedHead]: those blocks are
// executed but not indexed, so nothing else on this node can answer for them.
// After this, `latest` reads never need Firewood (ruling 2026-07-29).
//
// serve calls it after exec.New's reconcile and BEFORE starting the executor
// and the RPC server; that ordering is what makes the two plain field
// assignments below safe, since no other goroutine touches either yet.
func (h *History) EnableTail(executedHead uint64) (blocks int, entries int, size uint64, err error) {
	from := h.stateHead.Load() + 1
	if executedHead >= from && executedHead-from+1 > 2*BucketBlocks {
		return 0, 0, 0, fmt.Errorf("cooked through %d but executed to %d: %d uncooked blocks would not fit in the tail overlay (run `epochdb dev cook-index` on this dir)",
			from-1, executedHead, executedHead-from+1)
	}
	t := newTail()
	for n := from; n <= executedHead; n++ {
		frame, ok, err := h.store.wl.Get(n)
		if err != nil {
			return 0, 0, 0, fmt.Errorf("tail backfill block %d: %w", n, err)
		}
		if !ok {
			continue // empty block, or a bucket seal already retired (all cooked)
		}
		if err := t.apply(n, frame); err != nil {
			return 0, 0, 0, fmt.Errorf("tail backfill block %d: %w", n, err)
		}
		blocks++
	}
	h.tail = t
	h.store.tail = t
	h.tailApplied = max(from-1, executedHead)
	entries, size = t.stats()
	return blocks, entries, size, nil
}

// AdvanceTail folds the write frames of (lastApplied, head] into the overlay.
// It is the TAILING counterpart of the executor's Store.AppendWrites: a reader
// process that follows a live writer sees new frames through Store.RescanRO
// and folds them in here, so its `latest` reads answer at the writer's head
// with no cook, no RPC hop and no second read path. EnableTail sets the
// starting point; head comes from Store.HeadersMax (see RescanRO for why that
// is the highest fully appended block).
//
// Single-caller, exactly like EnableTail: one tailing goroutine owns it. A
// block with no frame at all is an empty block and is simply skipped.
func (h *History) AdvanceTail(head uint64) (blocks int, err error) {
	if h.tail == nil {
		return 0, errors.New("tail overlay not enabled (call EnableTail first)")
	}
	for n := h.tailApplied + 1; n <= head; n++ {
		frame, ok, err := h.store.wl.Get(n)
		if err != nil {
			return blocks, fmt.Errorf("tail advance block %d: %w", n, err)
		}
		if ok {
			if err := h.tail.apply(n, frame); err != nil {
				return blocks, fmt.Errorf("tail advance block %d: %w", n, err)
			}
			blocks++
		}
		h.tailApplied = n
	}
	return blocks, nil
}

// PruneTail drops overlay entries the cook has taken over. MUST be called
// after Refresh, never before: see the race argument on tail.prune.
func (h *History) PruneTail() (entries int, size uint64) {
	if h.tail == nil {
		return 0, 0
	}
	return h.tail.prune(h.stateHead.Load())
}

// TailStats reports the overlay's entry count and key+value bytes.
func (h *History) TailStats() (entries int, size uint64) {
	if h.tail == nil {
		return 0, 0
	}
	return h.tail.stats()
}

// AccountLatest returns addr's account at the EXECUTED head: the uncooked
// overlay, else the descent at the cooked watermark.
//
// The watermark is loaded AFTER the overlay miss, deliberately: an entry
// pruned between the two steps is guaranteed cooked by the watermark we then
// read (full argument on tail.prune).
func (h *History) AccountLatest(addr common.Address) (*types.StateAccount, error) {
	if h.tail != nil {
		if val, hit := h.tail.account(addr); hit {
			return decodeAccount(val, addr)
		}
	}
	return h.AccountAt(addr, h.StateHead())
}

// StorageLatest returns (addr, slot) at the EXECUTED head. Same two layers,
// same ordering rule as AccountLatest.
func (h *History) StorageLatest(addr common.Address, slot []byte) ([]byte, error) {
	if h.tail != nil {
		val, hit, dead := h.tail.storage(addr, slot)
		if hit {
			if len(val) == 0 {
				return nil, nil
			}
			return val, nil
		}
		if dead {
			// Destructed up here and not rewritten since: zero, and the
			// descent must not be consulted (it holds the pre-destruct value
			// and its own delete index knows nothing about an uncooked
			// SELFDESTRUCT).
			return nil, nil
		}
	}
	return h.StorageAt(addr, slot, h.StateHead())
}

// StateLatest returns the read surface for the executed head. With no
// overlay enabled it is exactly the descent at the cooked watermark.
func (h *History) StateLatest() state.Database {
	return &Overlay{hist: h, latest: true}
}

// decodeAccount turns a captured account post-image into a StateAccount.
// An empty value is an explicit delete: the account does not exist.
func decodeAccount(val []byte, addr common.Address) (*types.StateAccount, error) {
	if len(val) == 0 {
		return nil, nil
	}
	var acc types.StateAccount
	if err := rlp.DecodeBytes(val, &acc); err != nil {
		return nil, fmt.Errorf("decode account %x: %w", addr, err)
	}
	acc.Root = SentinelStorageRoot
	return &acc, nil
}

// Overlay is the read-only historical state.Database at a fixed target
// block. Only value reads work; every trie-structure or write method fails
// loudly.
//
// latest replaces the fixed target with the executed head: reads go through
// the uncooked-tail overlay and fall back to the descent at whatever the
// cooked watermark is at miss time.
type Overlay struct {
	hist   *History
	target uint64
	latest bool
}

// head is the block this view answers for: the fixed target, or the executed
// head in latest mode (used only for the BLOCKHASH-ish header lookups, never
// to bound a state read).
func (o *Overlay) head() uint64 {
	if o.latest {
		return o.hist.Head()
	}
	return o.target
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
	if t.o.latest {
		return t.o.hist.AccountLatest(addr)
	}
	return t.o.hist.AccountAt(addr, t.o.target)
}

func (t *overlayTrie) GetStorage(addr common.Address, key []byte) ([]byte, error) {
	if t.o.latest {
		return t.o.hist.StorageLatest(addr, key)
	}
	return t.o.hist.StorageAt(addr, key, t.o.target)
}

// Hash returns header(target).Root: the one true root for this height.
func (t *overlayTrie) Hash() common.Hash {
	n := t.o.head()
	raw, ok, err := t.o.hist.HeaderRLP(n)
	if err != nil || !ok {
		panic(fmt.Sprintf("epochdb overlay: header %d unavailable: ok=%v err=%v", n, ok, err))
	}
	var h types.Header
	if err := rlp.DecodeBytes(raw, &h); err != nil {
		panic(fmt.Sprintf("epochdb overlay: decode header %d: %v", n, err))
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

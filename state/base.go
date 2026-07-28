package state

// The limited-history floor (DESIGN.md "Limited-history mode"): base_<block>
// is a flat live-state snapshot at block B that REPLACES the genesis-alloc
// floor of History.search. Everything at or below B collapses into it, so a
// node carrying only (base, epochs above B, raw tail) answers every read at
// N >= B and nothing below B exists.
//
// It is NOT an epoch file: keys are HASH-keyed (kind|keccak(addr)|keccak(slot),
// 65B), which is what a network state sync yields and what makes the file
// un-foldable with the preimage-keyed epoch rows, forever. Rows carry no
// block number: the file IS state at B.
//
// Sections in write order, fixed-size footer last (reader seeks from EOF):
//
//	sst      rows sorted by key65, values inline, zstd blocks of
//	         ~sstBlockTarget raw bytes
//	sstIdx   sparse index: 73B entries key65|off u64
//	keybloom bloom over every key in the file (bloomBitsPerKey)
//	headers  one zstd frame of the RLP headers in [B-256, B] (uvarint len +
//	         bytes each), decoded once at open: ~150KB resident, so
//	         BLOCKHASH inside eth_call costs nothing at read time
//
// No zstd dictionary: the bulk of an SST block is 65B random hashes plus
// short values, a dict buys little there, and training one would need a
// second pass over the whole corpus. Epoch files train theirs on containers,
// which is a different (highly self-similar) corpus.

import (
	"bytes"
	"container/heap"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"syscall"
	"time"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/crypto"
	"github.com/ava-labs/libevm/rlp"
	"github.com/holiman/uint256"
	"github.com/klauspost/compress/zstd"
)

const (
	baseKeySize      = 1 + 32 + 32 // kind | keccak(addr) | keccak(slot)
	baseIdxEntrySize = baseKeySize + 8
	baseVersion      = 1
	baseNumSections  = 4
	baseFooterSize   = 4 + 4 + 8 + 8 + 32 + baseNumSections*16 + 4
	baseHeaderWindow = 256 // headers carried are [B-256, B]: BLOCKHASH reach of eth_call in [B, B+256]

	// baseKindCode keys a code blob by its hash. Code lives in the same
	// sorted keyspace as state ('a' < 'c' < 's'), which costs one extra
	// row kind instead of a whole section with its own index and bloom.
	baseKindCode byte = 'c'
)

var baseMagic = [4]byte{'B', 'A', 'S', 'E'}

// Section indexes into the footer table.
const (
	secBaseSST = iota
	secBaseSSTIdx
	secBaseKeybloom
	secBaseHeaders
)

func BaseFileName(block uint64) string { return fmt.Sprintf("base_%d", block) }

// ParseBaseFileName returns (block, ok).
func ParseBaseFileName(name string) (uint64, bool) {
	var block uint64
	if n, err := fmt.Sscanf(name, "base_%d", &block); n != 1 || err != nil {
		return 0, false
	}
	return block, BaseFileName(block) == name
}

// ---------- reader ----------

// Base is one open (mmap'd) base file: the read floor at block B.
// Goroutine-safe for reads (everything is immutable after open and
// zstd DecodeAll is concurrent-safe).
type Base struct {
	block uint64
	root  common.Hash

	mm  []byte
	sec [baseNumSections][]byte
	dec *zstd.Decoder

	bloomM    uint64
	bloomK    uint32
	bloomBits []byte

	hdrFrom uint64
	headers [][]byte // hdrFrom..hdrFrom+len-1, nil entry = absent
}

// OpenBase opens the single base_<block> file in dir. ok=false means the
// directory has no base file (a full-history node); a present but corrupt
// file is an error, never a silent miss.
func OpenBase(dir string) (*Base, bool, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, false, err
	}
	var names []string
	for _, en := range entries {
		if _, ok := ParseBaseFileName(en.Name()); ok {
			names = append(names, en.Name())
		}
	}
	if len(names) == 0 {
		return nil, false, nil
	}
	if len(names) > 1 {
		sort.Strings(names)
		return nil, false, fmt.Errorf("ambiguous floor: %d base files in %s (%v)", len(names), dir, names)
	}
	b, err := openBaseFile(filepath.Join(dir, names[0]))
	if err != nil {
		return nil, false, err
	}
	return b, true, nil
}

func openBaseFile(path string) (*Base, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	size := int(st.Size())
	if size < baseFooterSize {
		return nil, fmt.Errorf("base %s: too small (%d bytes)", path, size)
	}
	mm, err := syscall.Mmap(int(f.Fd()), 0, size, syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, err
	}
	ft := mm[size-baseFooterSize:]
	if !bytes.Equal(ft[0:4], baseMagic[:]) || !bytes.Equal(ft[baseFooterSize-4:], baseMagic[:]) {
		syscall.Munmap(mm)
		return nil, fmt.Errorf("base %s: bad footer magic (truncated or corrupt)", path)
	}
	if v := binary.LittleEndian.Uint32(ft[4:8]); v != baseVersion {
		syscall.Munmap(mm)
		return nil, fmt.Errorf("base %s: format version %d, want %d", path, v, baseVersion)
	}
	b := &Base{
		block:   binary.LittleEndian.Uint64(ft[8:16]),
		hdrFrom: binary.LittleEndian.Uint64(ft[16:24]),
		mm:      mm,
	}
	copy(b.root[:], ft[24:56])
	if want, ok := ParseBaseFileName(filepath.Base(path)); ok && want != b.block {
		syscall.Munmap(mm)
		return nil, fmt.Errorf("base %s: footer says block %d", path, b.block)
	}
	for i := 0; i < baseNumSections; i++ {
		off := binary.LittleEndian.Uint64(ft[56+i*16:])
		ln := binary.LittleEndian.Uint64(ft[64+i*16:])
		body := uint64(size - baseFooterSize)
		if off > body || ln > body-off { // overflow-safe bounds check

			syscall.Munmap(mm)
			return nil, fmt.Errorf("base %s: section %d out of bounds", path, i)
		}
		b.sec[i] = mm[off : off+ln]
	}
	if b.dec, err = zstd.NewReader(nil); err != nil {
		syscall.Munmap(mm)
		return nil, err
	}
	bl := b.sec[secBaseKeybloom]
	if len(bl) < 16 {
		b.Close()
		return nil, fmt.Errorf("base %s: truncated bloom", path)
	}
	b.bloomM = binary.LittleEndian.Uint64(bl[0:8])
	b.bloomK = binary.LittleEndian.Uint32(bl[8:12])
	b.bloomBits = bl[16:]
	if b.bloomM == 0 || uint64(len(b.bloomBits))*8 < b.bloomM {
		b.Close()
		return nil, fmt.Errorf("base %s: bloom claims %d bits over %d bytes", path, b.bloomM, len(b.bloomBits))
	}
	if len(b.sec[secBaseHeaders]) > 0 {
		raw, err := b.dec.DecodeAll(b.sec[secBaseHeaders], nil)
		if err != nil {
			b.Close()
			return nil, fmt.Errorf("base %s: headers: %w", path, err)
		}
		for pos := 0; pos < len(raw); {
			ln, n := binary.Uvarint(raw[pos:])
			if n <= 0 || pos+n+int(ln) > len(raw) {
				b.Close()
				return nil, fmt.Errorf("base %s: bad header payload", path)
			}
			pos += n
			var hdr []byte
			if ln > 0 {
				hdr = raw[pos : pos+int(ln)]
			}
			b.headers = append(b.headers, hdr)
			pos += int(ln)
		}
	}
	return b, nil
}

// Block returns the floor block B: this file is the state at the end of B.
func (b *Base) Block() uint64 { return b.block }

// StateRoot returns header(B).Root, the root the file's rows rebuild to.
func (b *Base) StateRoot() common.Hash { return b.root }

func (b *Base) Close() {
	if b.dec != nil {
		b.dec.Close()
		b.dec = nil
	}
	if b.mm != nil {
		syscall.Munmap(b.mm)
		b.mm = nil
	}
	b.headers = nil
}

func baseAccountKey(addrHash common.Hash) []byte {
	k := make([]byte, baseKeySize)
	k[0] = recKindAccount
	copy(k[1:33], addrHash[:])
	return k
}

func baseStorageKey(addrHash, slotHash common.Hash) []byte {
	k := baseAccountKey(addrHash)
	k[0] = recKindStorage
	copy(k[33:65], slotHash[:])
	return k
}

func baseCodeKey(hash common.Hash) []byte {
	k := baseAccountKey(hash)
	k[0] = baseKindCode
	return k
}

// Account returns the account RLP of addr at B (the stored StorageRoot is
// the real one captured on chain; substituting the sentinel is the caller's
// job, exactly as for epoch rows). found=false = the account does not exist.
func (b *Base) Account(addr common.Address) ([]byte, bool, error) {
	return b.lookup(baseAccountKey(crypto.Keccak256Hash(addr[:])))
}

// Storage returns the trie-encoded value of (addr, slot) at B; found=false
// = zero. slot is the preimage (left-aligned, as capture stores it).
func (b *Base) Storage(addr common.Address, slot []byte) ([]byte, bool, error) {
	var s common.Hash
	copy(s[:], slot)
	return b.lookup(baseStorageKey(crypto.Keccak256Hash(addr[:]), crypto.Keccak256Hash(s[:])))
}

// Code returns the code blob for hash. Empty code is never stored, so
// EmptyCodeHash reports found=false: callers short-circuit it first.
func (b *Base) Code(hash common.Hash) ([]byte, bool, error) {
	return b.lookup(baseCodeKey(hash))
}

// HeaderRLP returns the stored header for block n. Only [B-256, B] is
// carried: header(B) is included so a base-only node is self-contained at
// its own floor.
func (b *Base) HeaderRLP(n uint64) ([]byte, bool, error) {
	if n < b.hdrFrom || n-b.hdrFrom >= uint64(len(b.headers)) {
		return nil, false, nil
	}
	raw := b.headers[n-b.hdrFrom]
	return raw, raw != nil, nil
}

func (b *Base) mayContain(key []byte) bool {
	h1, h2 := bloomHash(key)
	for i := uint64(0); i < uint64(b.bloomK); i++ {
		bit := (h1 + i*h2) % b.bloomM
		if binary.LittleEndian.Uint64(b.bloomBits[bit/64*8:])&(1<<(bit%64)) == 0 {
			return false
		}
	}
	return true
}

// lookup binary-searches the sparse index for key's block, decodes it and
// scans. Keys are unique, so this is an exact-match probe.
func (b *Base) lookup(key []byte) ([]byte, bool, error) {
	if !b.mayContain(key) {
		return nil, false, nil
	}
	idx := b.sec[secBaseSSTIdx]
	n := len(idx) / baseIdxEntrySize
	entry := func(i int) []byte { return idx[i*baseIdxEntrySize : (i+1)*baseIdxEntrySize] }
	bi := sort.Search(n, func(i int) bool {
		return bytes.Compare(entry(i)[:baseKeySize], key) > 0
	}) - 1
	if bi < 0 {
		return nil, false, nil
	}
	lo := binary.LittleEndian.Uint64(entry(bi)[baseKeySize:])
	hi := uint64(len(b.sec[secBaseSST]))
	if bi+1 < n {
		hi = binary.LittleEndian.Uint64(entry(bi + 1)[baseKeySize:])
	}
	raw, err := b.dec.DecodeAll(b.sec[secBaseSST][lo:hi], nil)
	if err != nil {
		return nil, false, fmt.Errorf("base block %d: decode sst block %d: %w", b.block, bi, err)
	}
	for pos := 0; pos+baseKeySize < len(raw); {
		rk := raw[pos : pos+baseKeySize]
		vlen, vn := binary.Uvarint(raw[pos+baseKeySize:])
		if vn <= 0 {
			return nil, false, fmt.Errorf("base block %d: bad row at %d", b.block, pos)
		}
		vstart := pos + baseKeySize + vn
		if vstart+int(vlen) > len(raw) {
			return nil, false, fmt.Errorf("base block %d: truncated row at %d", b.block, pos)
		}
		switch bytes.Compare(rk, key) {
		case 0:
			return raw[vstart : vstart+int(vlen)], true, nil
		case 1:
			return nil, false, nil
		}
		pos = vstart + int(vlen)
	}
	return nil, false, nil
}

// ---------- builder ----------

// BaseSink observes every live state row (preimage-keyed, in key53 order:
// accounts before storage) as the base is built. It is the hook for the
// build-time root check: the file itself is hash-keyed, so nothing can
// rebuild a trie from it afterwards. nil = no check.
type BaseSink func(key [sortedKeySize]byte, val []byte) error

// BuildBase folds h's corpus into a base file at block `at` under outDir and
// returns the path. Memory is bounded: the merge is streaming and the hashed
// rows go through on-disk prefix buckets (state/verify.go's spill pattern).
func BuildBase(h *History, outDir string, at uint64, sink BaseSink) (string, error) {
	if at == 0 || at > h.head {
		return "", fmt.Errorf("base at %d: corpus covers blocks up to %d", at, h.head)
	}
	// An existing base file is NOT a merge source here (openBaseCursors walks
	// buckets and epochs only), so re-flooring would drop every key whose
	// last write was at or below the old floor. The fold-down step DESIGN
	// defers is what would make this work; until it exists, refuse.
	if h.Floor() > 0 {
		return "", fmt.Errorf("base at %d: source is already pruned below block %d; re-flooring an already-pruned dir is unsupported (the existing base cannot be a merge source)", at, h.Floor())
	}
	if err := h.epochs.RequireCovered(at); err != nil {
		return "", err
	}
	hdrRLP, ok, err := h.HeaderRLP(at)
	if err != nil || !ok {
		return "", fmt.Errorf("base at %d: header unavailable: ok=%v err=%v", at, ok, err)
	}
	var hdr types.Header
	if err := rlp.DecodeBytes(hdrRLP, &hdr); err != nil {
		return "", fmt.Errorf("base at %d: decode header: %w", at, err)
	}

	spillDir, err := os.MkdirTemp(outDir, fmt.Sprintf(".base-%d-", at))
	if err != nil {
		return "", err
	}
	defer os.RemoveAll(spillDir)
	sp := &baseSpill{dir: spillDir}

	if err := foldLiveRows(h, at, sink, sp); err != nil {
		return "", err
	}
	if err := sp.finish(); err != nil {
		return "", err
	}
	log.Printf("base: %d live rows spilled, writing base_%d", sp.rows, at)

	headers := make([][]byte, 0, baseHeaderWindow+1)
	from := uint64(0)
	if at > baseHeaderWindow {
		from = at - baseHeaderWindow
	}
	for n := from; n <= at; n++ {
		raw, ok, err := h.HeaderRLP(n)
		if err != nil {
			return "", fmt.Errorf("base at %d: header %d: %w", at, n, err)
		}
		if !ok {
			raw = nil
		}
		headers = append(headers, raw)
	}
	return writeBaseFile(outDir, at, hdr.Root, from, headers, sp)
}

// foldLiveRows merges every source ordered by key53, keeps each key's last
// write at or before `at`, and emits the ones that are still live.
func foldLiveRows(h *History, at uint64, sink BaseSink, sp *baseSpill) error {
	cursors, err := openBaseCursors(h, at)
	if err != nil {
		return err
	}
	hp := &cursorHeap{}
	for _, c := range cursors {
		if c.key() != nil {
			*hp = append(*hp, c)
		}
	}
	heap.Init(hp)

	var (
		emitted   = &baseEmitter{h: h, at: at, sink: sink, sp: sp}
		curKey    []byte
		bestVal   []byte
		bestBlk   uint64
		have      bool
		rows      uint64
		lastLog   = time.Now()
		startTime = time.Now()
	)
	for hp.Len() > 0 {
		curKey = append(curKey[:0], (*hp)[0].key()...)
		bestVal, bestBlk, have = bestVal[:0], 0, false
		for hp.Len() > 0 && bytes.Equal((*hp)[0].key(), curKey) {
			c := (*hp)[0]
			if blk := c.block(); blk <= at && (!have || blk > bestBlk) {
				v, err := c.value()
				if err != nil {
					return err
				}
				bestVal, bestBlk, have = append(bestVal[:0], v...), blk, true
			}
			if err := c.next(); err != nil {
				return err
			}
			if c.key() == nil {
				heap.Pop(hp)
			} else {
				heap.Fix(hp, 0)
			}
			rows++
		}
		if have {
			if err := emitted.emit(curKey, bestBlk, bestVal); err != nil {
				return err
			}
		}
		if time.Since(lastLog) >= 30*time.Second {
			log.Printf("base: folding at key %x: %d rows scanned, %d live rows, %.0f rows/s",
				curKey[:9], rows, sp.rows, float64(rows)/time.Since(startTime).Seconds())
			lastLog = time.Now()
		}
	}
	return emitted.emitGenesis()
}

// baseEmitter turns surviving (key53, value) pairs into hashed base rows,
// applying the liveness rules: an empty value is a delete (the key is absent
// from the base), and a storage slot orphaned by an account delete at or
// before `at` is dead (SELFDESTRUCT drops the whole storage trie), mirroring
// History.StorageAt.
type baseEmitter struct {
	h    *History
	at   uint64
	sink BaseSink
	sp   *baseSpill

	addr     common.Address // current address run
	addrHash common.Hash
	delBlk   uint64 // last account delete <= at for addr
	delOK    bool
	haveAddr bool

	seenGenesis map[common.Address]bool // genesis accounts written at or below at
	codeSeen    map[common.Hash]bool
}

// setAddr caches the per-address facts (key hash, last account delete) for
// the contiguous run of rows sharing an address.
func (b *baseEmitter) setAddr(key []byte) {
	var a common.Address
	copy(a[:], key[1:21])
	if b.haveAddr && a == b.addr {
		return
	}
	b.addr, b.haveAddr = a, true
	b.addrHash = crypto.Keccak256Hash(a[:])
	ak := accountKey(a)
	b.delBlk, b.delOK = b.h.lastAccountDelete(ak, b.at)
}

func (b *baseEmitter) emit(key []byte, blk uint64, val []byte) error {
	if key[0] == baseKindCode {
		// A v3 epoch's own code row: the base rebuilds its code section from
		// the live accounts it emits (emitCode), so these are not merge input.
		return nil
	}
	b.setAddr(key)
	switch key[0] {
	case recKindAccount:
		if b.seenGenesis == nil {
			b.seenGenesis = map[common.Address]bool{}
		}
		if _, ok := b.h.genesis[b.addr]; ok {
			b.seenGenesis[b.addr] = true
		}
		if len(val) == 0 {
			return nil // account deleted at or before at
		}
		var acc types.StateAccount
		if err := rlp.DecodeBytes(val, &acc); err != nil {
			return fmt.Errorf("base: decode account %x: %w", b.addr, err)
		}
		if err := b.emitCode(common.BytesToHash(acc.CodeHash)); err != nil {
			return err
		}
		return b.row(baseAccountKey(b.addrHash), key, val)
	case recKindStorage:
		if len(val) == 0 {
			return nil // zero write
		}
		if b.delOK && b.delBlk > blk {
			return nil // orphaned by a later SELFDESTRUCT, exactly History.StorageAt
		}
		return b.row(baseStorageKey(b.addrHash, crypto.Keccak256Hash(key[21:53])), key, val)
	default:
		return fmt.Errorf("base: unknown row kind %q", key[0])
	}
}

func (b *baseEmitter) emitCode(hash common.Hash) error {
	if hash == types.EmptyCodeHash || hash == (common.Hash{}) {
		return nil
	}
	if b.codeSeen == nil {
		b.codeSeen = map[common.Hash]bool{}
	}
	if b.codeSeen[hash] {
		return nil
	}
	b.codeSeen[hash] = true
	// Full descent (code.log, v3 epochs, genesis alloc): a folded corpus can
	// have had its code.log dropped once the epochs carry the blobs.
	blob, err := b.h.CodeByHash(hash)
	if err != nil {
		return fmt.Errorf("base: code %x referenced by a live account: %w", hash, err)
	}
	return b.sp.add(baseCodeKey(hash), blob)
}

// row spills the hashed row and feeds the preimage row to the sink.
func (b *baseEmitter) row(hashed, key53, val []byte) error {
	if err := b.sp.add(hashed, val); err != nil {
		return err
	}
	if b.sink == nil {
		return nil
	}
	var k [sortedKeySize]byte
	copy(k[:], key53)
	return b.sink(k, val)
}

// emitGenesis adds the alloc keys that were never written at or below `at`:
// the base replaces the alloc as the read floor, so they must be in the file.
func (b *baseEmitter) emitGenesis() error {
	addrs := make([]common.Address, 0, len(b.h.genesis))
	for addr := range b.h.genesis {
		addrs = append(addrs, addr)
	}
	sort.Slice(addrs, func(i, j int) bool { return bytes.Compare(addrs[i][:], addrs[j][:]) < 0 })
	for _, addr := range addrs {
		if b.seenGenesis[addr] {
			continue
		}
		ga := b.h.genesis[addr]
		if len(ga.Storage) > 0 {
			// Neither mainnet nor fuji has alloc storage. Supporting it
			// needs the alloc storage trie root for the account RLP, which
			// nothing here computes: fail instead of writing a wrong root.
			return fmt.Errorf("base: genesis account %x has %d alloc storage slots, unsupported", addr, len(ga.Storage))
		}
		acc := types.StateAccount{
			Nonce:    ga.Nonce,
			Balance:  new(uint256.Int),
			Root:     types.EmptyRootHash,
			CodeHash: types.EmptyCodeHash.Bytes(),
		}
		if ga.Balance != nil {
			acc.Balance = uint256.MustFromBig(ga.Balance)
		}
		if len(ga.Code) > 0 {
			acc.CodeHash = crypto.Keccak256(ga.Code)
		}
		val, err := rlp.EncodeToBytes(&acc)
		if err != nil {
			return err
		}
		b.addr, b.haveAddr = addr, true
		b.addrHash = crypto.Keccak256Hash(addr[:])
		if err := b.emitCode(common.BytesToHash(acc.CodeHash)); err != nil {
			return err
		}
		if err := b.row(baseAccountKey(b.addrHash), accountKey(addr), val); err != nil {
			return err
		}
	}
	return nil
}

// ---------- merge cursors ----------

// baseCursor is one row source in (key53, block) order. Both the cooked raw
// buckets and the sealed epoch SSTs are already stored in exactly that
// order, so folding the corpus is a k-way merge with no re-sort: only the
// surviving row of each key is ever materialised.
type baseCursor interface {
	key() []byte // nil = exhausted
	block() uint64
	value() ([]byte, error)
	next() error
}

// openBaseCursors returns one positioned cursor per source that can hold a
// row at or below `at` (History.search's descent set, minus the ordering).
func openBaseCursors(h *History, at uint64) ([]baseCursor, error) {
	var out []baseCursor
	for _, b := range h.buckets {
		if b.bucket*BucketBlocks > at {
			continue
		}
		out = append(out, &bucketCursor{b: b})
	}
	for _, e := range h.epochs.Epochs {
		if e.Start > at {
			continue
		}
		c := &epochCursor{e: e}
		if err := c.load(); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, nil
}

// bucketCursor walks one cooked sorted_NNNNN.idx. Values stay in the
// writelog and are read only for rows that survive.
type bucketCursor struct {
	b *sortedBucket
	i int
}

func (c *bucketCursor) key() []byte {
	if c.i >= c.b.n {
		return nil
	}
	return c.b.rec(c.i)[:sortedKeySize]
}

func (c *bucketCursor) block() uint64 {
	return binary.BigEndian.Uint64(c.b.rec(c.i)[53:61])
}

func (c *bucketCursor) value() ([]byte, error) {
	r := c.b.rec(c.i)
	vlen := binary.BigEndian.Uint32(r[69:73])
	if vlen == 0 {
		return nil, nil
	}
	buf := make([]byte, vlen)
	off := binary.BigEndian.Uint64(r[61:69])
	if _, err := c.b.wl.ReadAt(buf, int64(off)); err != nil {
		return nil, fmt.Errorf("writelog bucket %05d read at %d: %w", c.b.bucket, off, err)
	}
	return buf, nil
}

func (c *bucketCursor) next() error { c.i++; return nil }

// epochCursor walks one epoch's SST, one compressed block at a time.
type epochCursor struct {
	e   *Epoch
	bi  int // next sst block to decode
	raw []byte
	pos int
	k   []byte
	blk uint64
	val []byte
}

func (c *epochCursor) key() []byte            { return c.k }
func (c *epochCursor) block() uint64          { return c.blk }
func (c *epochCursor) value() ([]byte, error) { return c.val, nil }

// load positions on the row at pos, decoding further blocks as needed.
func (c *epochCursor) load() error {
	idx := c.e.sec[secSSTIdx]
	n := len(idx) / sstIdxEntrySize
	for c.pos >= len(c.raw) {
		if c.bi >= n {
			c.k, c.val = nil, nil
			return nil
		}
		lo := binary.LittleEndian.Uint64(idx[c.bi*sstIdxEntrySize+sortedKeySize+8:])
		hi := uint64(len(c.e.sec[secSST]))
		if c.bi+1 < n {
			hi = binary.LittleEndian.Uint64(idx[(c.bi+1)*sstIdxEntrySize+sortedKeySize+8:])
		}
		raw, err := c.e.decodeAll(c.e.sec[secSST][lo:hi])
		if err != nil {
			return fmt.Errorf("epoch %d: decode sst block %d: %w", c.e.Start, c.bi, err)
		}
		c.raw, c.pos, c.bi = raw, 0, c.bi+1
	}
	if c.pos+sortedKeySize+8 > len(c.raw) {
		return fmt.Errorf("epoch %d: truncated sst row", c.e.Start)
	}
	c.k = c.raw[c.pos : c.pos+sortedKeySize]
	c.blk = binary.BigEndian.Uint64(c.raw[c.pos+sortedKeySize:])
	vlen, vn := binary.Uvarint(c.raw[c.pos+sortedKeySize+8:])
	if vn <= 0 {
		return fmt.Errorf("epoch %d: bad sst row length", c.e.Start)
	}
	vstart := c.pos + sortedKeySize + 8 + vn
	if vstart+int(vlen) > len(c.raw) {
		return fmt.Errorf("epoch %d: truncated sst value", c.e.Start)
	}
	c.val = c.raw[vstart : vstart+int(vlen)]
	c.pos = vstart + int(vlen)
	return nil
}

func (c *epochCursor) next() error { return c.load() }

// cursorHeap orders the live cursors by current key (container/heap).
type cursorHeap []baseCursor

func (h cursorHeap) Len() int           { return len(h) }
func (h cursorHeap) Less(i, j int) bool { return bytes.Compare(h[i].key(), h[j].key()) < 0 }
func (h cursorHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *cursorHeap) Push(x any)        { *h = append(*h, x.(baseCursor)) }
func (h *cursorHeap) Pop() any {
	old := *h
	n := len(old)
	c := old[n-1]
	*h = old[:n-1]
	return c
}

// ---------- spill ----------

// baseSpill scatters hashed rows into prefix buckets on disk (one per
// kind + first hash byte), each small enough to sort in memory during the
// write pass. Record: key65 | uvarint vlen | value.
type baseSpill struct {
	dir  string
	bufs [3 * 256][]byte
	rows uint64
}

func baseBucketIdx(key []byte) int {
	kind := 0
	switch key[0] {
	case baseKindCode:
		kind = 1
	case recKindStorage:
		kind = 2
	}
	return kind*256 + int(key[1])
}

func (s *baseSpill) path(bi int) string { return filepath.Join(s.dir, fmt.Sprintf("p%03d", bi)) }

func (s *baseSpill) add(key, val []byte) error {
	bi := baseBucketIdx(key)
	b := s.bufs[bi]
	b = append(b, key...)
	b = binary.AppendUvarint(b, uint64(len(val)))
	b = append(b, val...)
	s.bufs[bi] = b
	s.rows++
	// 1MB per bucket bounds the scatter phase at ~768MB resident; the write
	// pass then loads one bucket (tens of MB at full mainnet) at a time.
	if len(b) >= 1<<20 {
		return s.flush(bi)
	}
	return nil
}

func (s *baseSpill) flush(bi int) error {
	if len(s.bufs[bi]) == 0 {
		return nil
	}
	f, err := os.OpenFile(s.path(bi), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(s.bufs[bi]); err != nil {
		f.Close()
		return err
	}
	s.bufs[bi] = s.bufs[bi][:0]
	return f.Close()
}

func (s *baseSpill) finish() error {
	for bi := range s.bufs {
		if err := s.flush(bi); err != nil {
			return err
		}
		s.bufs[bi] = nil
	}
	return nil
}

type baseRow struct {
	key [baseKeySize]byte
	val []byte
}

// load reads one spill bucket back, sorted by key.
func (s *baseSpill) load(bi int) ([]baseRow, error) {
	raw, err := os.ReadFile(s.path(bi))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var rows []baseRow
	for pos := 0; pos < len(raw); {
		if pos+baseKeySize >= len(raw) {
			return nil, fmt.Errorf("base spill %d: truncated record", bi)
		}
		var r baseRow
		copy(r.key[:], raw[pos:pos+baseKeySize])
		vlen, n := binary.Uvarint(raw[pos+baseKeySize:])
		if n <= 0 {
			return nil, fmt.Errorf("base spill %d: bad length", bi)
		}
		pos += baseKeySize + n
		if pos+int(vlen) > len(raw) {
			return nil, fmt.Errorf("base spill %d: truncated value", bi)
		}
		r.val = raw[pos : pos+int(vlen)]
		pos += int(vlen)
		rows = append(rows, r)
	}
	sort.Slice(rows, func(i, j int) bool { return bytes.Compare(rows[i].key[:], rows[j].key[:]) < 0 })
	return rows, nil
}

// ---------- file writer ----------

func writeBaseFile(outDir string, at uint64, root common.Hash, hdrFrom uint64, headers [][]byte, sp *baseSpill) (string, error) {
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedBestCompression))
	if err != nil {
		return "", err
	}
	defer enc.Close()

	// Bloom sized from the exact row count (known after the spill), bits
	// set inline: the key set is far too large to hold for buildBloom.
	m := sp.rows * bloomBitsPerKey
	if m < 64 {
		m = 64
	}
	m = (m + 63) / 64 * 64
	words := make([]uint64, m/64)

	var (
		sst      []byte
		idx      []byte
		raw      []byte
		firstKey []byte
	)
	flush := func() {
		if len(raw) == 0 {
			return
		}
		idx = append(idx, firstKey...)
		idx = binary.LittleEndian.AppendUint64(idx, uint64(len(sst)))
		sst = enc.EncodeAll(raw, sst)
		raw, firstKey = raw[:0], nil
	}
	for bi := range sp.bufs {
		rows, err := sp.load(bi)
		if err != nil {
			return "", err
		}
		for i := range rows {
			r := &rows[i]
			h1, h2 := bloomHash(r.key[:])
			for j := uint64(0); j < bloomHashes; j++ {
				bit := (h1 + j*h2) % m
				words[bit/64] |= 1 << (bit % 64)
			}
			if firstKey == nil {
				firstKey = append([]byte(nil), r.key[:]...)
			}
			raw = append(raw, r.key[:]...)
			raw = binary.AppendUvarint(raw, uint64(len(r.val)))
			raw = append(raw, r.val...)
			if len(raw) >= sstBlockTarget {
				flush()
			}
		}
	}
	flush()

	bloom := binary.LittleEndian.AppendUint64(nil, m)
	bloom = binary.LittleEndian.AppendUint32(bloom, bloomHashes)
	bloom = binary.LittleEndian.AppendUint32(bloom, 0)
	for _, w := range words {
		bloom = binary.LittleEndian.AppendUint64(bloom, w)
	}

	var hdrPayload []byte
	for _, h := range headers {
		hdrPayload = binary.AppendUvarint(hdrPayload, uint64(len(h)))
		hdrPayload = append(hdrPayload, h...)
	}
	var hdrSec []byte
	if len(hdrPayload) > 0 {
		hdrSec = enc.EncodeAll(hdrPayload, nil)
	}

	var (
		buf     bytes.Buffer
		offsets [baseNumSections][2]uint64
	)
	section := func(id int, b []byte) {
		offsets[id][0] = uint64(buf.Len())
		offsets[id][1] = uint64(len(b))
		buf.Write(b)
	}
	section(secBaseSST, sst)
	section(secBaseSSTIdx, idx)
	section(secBaseKeybloom, bloom)
	section(secBaseHeaders, hdrSec)

	var ft [baseFooterSize]byte
	copy(ft[0:4], baseMagic[:])
	binary.LittleEndian.PutUint32(ft[4:8], baseVersion)
	binary.LittleEndian.PutUint64(ft[8:16], at)
	binary.LittleEndian.PutUint64(ft[16:24], hdrFrom)
	copy(ft[24:56], root[:])
	for i := 0; i < baseNumSections; i++ {
		binary.LittleEndian.PutUint64(ft[56+i*16:], offsets[i][0])
		binary.LittleEndian.PutUint64(ft[64+i*16:], offsets[i][1])
	}
	copy(ft[baseFooterSize-4:], baseMagic[:])
	buf.Write(ft[:])

	path := filepath.Join(outDir, BaseFileName(at))
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, buf.Bytes(), 0o644); err != nil {
		return "", err
	}
	return path, os.Rename(tmp, path)
}

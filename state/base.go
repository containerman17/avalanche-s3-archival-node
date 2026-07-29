package state

// The limited-history floor / snapshot file (DESIGN.md "Our own state sync"
// and "Limited-history mode"): base_<block> is a flat live-state snapshot at
// block B that REPLACES the genesis-alloc floor of History.search. Everything
// at or below B collapses into it, so a node carrying only (base, epochs
// above B, raw tail) answers every read at N >= B and nothing below B exists.
//
// Keys are PREIMAGE-KEYED, exactly the 53-byte epoch/bucket keyspace
// ('a' | addr | zeros, 'c' | codehash | zeros, 's' | addr | slot, and
// 'a' < 'c' < 's' so one sorted keyspace carries state and code). Format v2,
// user design 2026-07-29: the hash-keyed v1 layout existed only for network
// leaf-sync, which is deleted; the producer is now a pruning node's own
// capture, which has the preimages. Reads therefore cost NO keccak, and the
// Firewood load hashes on the way out (WalkRows). Rows carry no block number:
// the file IS state at B.
//
// Sections in write order, fixed-size footer last (reader seeks from EOF):
//
//	sst      rows sorted by key53, values inline, zstd blocks of
//	         ~sstBlockTarget raw bytes
//	sstIdx   sparse index: 61B entries key53|off u64
//	keybloom bloom over every key in the file (bloomBitsPerKey)
//	headers  one zstd frame of the RLP headers in [B-256, B] (uvarint len +
//	         bytes each), decoded once at open: ~150KB resident, so
//	         BLOCKHASH inside eth_call costs nothing at read time
//
// The footer carries B, the state root, and CUMULATIVE TX COUNT AT B (the
// byte-identity requirement: every producer cuts epochs at the same canonical
// tx-count boundaries, and a snapshot-synced node cannot recompute that count
// from history it does not have).
//
// NO UPGRADE PATH, same policy as epochs: a v1 file is refused by name.
//
// No zstd dictionary: short values plus 53B keys are not worth training one,
// and it would need a second pass over the whole corpus.

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"syscall"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/crypto"
	"github.com/klauspost/compress/zstd"
)

const (
	baseKeySize      = sortedKeySize // kind | addr | slot, the epoch keyspace
	baseIdxEntrySize = baseKeySize + 8
	baseVersion      = 2
	baseNumSections  = 4
	baseFooterSize   = 4 + 4 + 8 + 8 + 8 + 32 + baseNumSections*16 + 4
	baseHeaderWindow = 256 // headers carried are [B-256, B]: BLOCKHASH reach of eth_call in [B, B+256]

	// baseHashedKeySize is what WalkRows emits: kind | keccak(addr) |
	// keccak(slot), the shape Firewood's own keyspace uses.
	baseHashedKeySize = 1 + 32 + 32
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
	cumTx uint64
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

// OpenBase opens the newest base_<block> file in dir. ok=false means the
// directory has no base file (a full-history node); a present but corrupt
// file is an error, never a silent miss.
//
// NEWEST WINS, deliberately (fold's crash table): the fold commits a new
// snapshot by renaming base_<B>.tmp into place and only then unlinks the
// older one, so two base files is a normal transient state after a kill -9
// in that window. The rename is ordered strictly after the new file's fsync,
// so the highest B on disk is always the complete one. Refusing to guess
// here (the pre-fold rule) left exec and serve unable to start at all until
// the next fold ran; the fold's own sweep is what removes the loser.
func OpenBase(dir string) (*Base, bool, error) {
	name, _, ok, err := newestBase(dir)
	if err != nil || !ok {
		return nil, false, err
	}
	b, err := openBaseFile(filepath.Join(dir, name))
	if err != nil {
		return nil, false, err
	}
	return b, true, nil
}

// newestBase returns the highest-numbered base file in dir, plus every base
// file name found (ascending by block) so the fold can unlink the losers.
func newestBase(dir string) (name string, all []string, ok bool, err error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return "", nil, false, err
	}
	type bf struct {
		blk  uint64
		name string
	}
	var found []bf
	for _, en := range entries {
		if blk, ok := ParseBaseFileName(en.Name()); ok {
			found = append(found, bf{blk, en.Name()})
		}
	}
	if len(found) == 0 {
		return "", nil, false, nil
	}
	sort.Slice(found, func(i, j int) bool { return found[i].blk < found[j].blk })
	for _, f := range found {
		all = append(all, f.name)
	}
	if len(found) > 1 {
		log.Printf("state: %d base files in %s (%v): a fold was killed between its rename and its cleanup; using the newest, %s",
			len(all), dir, all, all[len(all)-1])
	}
	return all[len(all)-1], all, true, nil
}

// OpenBaseFile opens one base file by path. Unlike OpenBase it does not scan
// a directory, which is what the fold's pre-rename gate needs: it verifies
// base_<B>.tmp before that name ever becomes visible.
func OpenBaseFile(path string) (*Base, error) { return openBaseFile(path) }

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
		// No migration, ever (user ruling 2026-07-28): v1 was hash-keyed and
		// nothing can re-key it without the preimages it never stored.
		return nil, fmt.Errorf("base %s: format v%d, unsupported: delete it and re-fetch the snapshot (only v%d is readable)", path, v, baseVersion)
	}
	b := &Base{
		block:   binary.LittleEndian.Uint64(ft[8:16]),
		hdrFrom: binary.LittleEndian.Uint64(ft[16:24]),
		cumTx:   binary.LittleEndian.Uint64(ft[24:32]),
		mm:      mm,
	}
	copy(b.root[:], ft[32:64])
	if want, ok := ParseBaseFileName(filepath.Base(path)); ok && want != b.block {
		syscall.Munmap(mm)
		return nil, fmt.Errorf("base %s: footer says block %d", path, b.block)
	}
	for i := 0; i < baseNumSections; i++ {
		off := binary.LittleEndian.Uint64(ft[64+i*16:])
		ln := binary.LittleEndian.Uint64(ft[72+i*16:])
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

// CumTx returns the cumulative tx count of the chain at the end of B. It is
// the canonical epoch-boundary anchor: a node that starts here has no history
// to count from, and every producer must agree on where the next epoch cuts.
func (b *Base) CumTx() uint64 { return b.cumTx }

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

// Account returns the account RLP of addr at B, verbatim as the write
// capture recorded it, which means its StorageRoot field is the ZERO hash:
// firewood-ethhash manages storage roots internally (its storage tries hash
// to zero) and the RPC read path substitutes SentinelStorageRoot, exactly as
// for epoch rows. (The v1 text here claimed a real captured storage root;
// that was leaf-sync's hash-keyed format, which is deleted.) found=false =
// the account does not exist. One direct probe, no hashing: the key IS the
// address.
func (b *Base) Account(addr common.Address) ([]byte, bool, error) {
	return b.lookup(accountKey(addr))
}

// Storage returns the trie-encoded value of (addr, slot) at B; found=false
// = zero. slot is the preimage (left-aligned, as capture stores it).
func (b *Base) Storage(addr common.Address, slot []byte) ([]byte, bool, error) {
	return b.lookup(storageKey(addr, slot))
}

// Code returns the code blob for hash. Empty code is never stored, so
// EmptyCodeHash reports found=false: callers short-circuit it first.
func (b *Base) Code(hash common.Hash) ([]byte, bool, error) {
	k := epochCodeKey(hash)
	return b.lookup(k[:])
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

// WalkRows streams every row of the file with HASHED keys: kind | keccak(addr)
// | keccak(slot), 65 bytes, which is how a node loads its Firewood frontier at
// B (exec.startFromBase). The disk holds preimages because the producer has
// them and reads want them; Firewood wants hashed keys, so keccak happens here,
// once, at load time. Code rows ('c') pass their content-addressed hash
// through unchanged.
//
// ORDER is preimage order, not hash order, so the load is a random-order bulk
// insert. key and val alias internal buffers: copy to retain.
func (b *Base) WalkRows(fn func(key, val []byte) error) error {
	var (
		hk       [baseHashedKeySize]byte
		lastAddr common.Address
		lastHash common.Hash
		haveAddr bool
	)
	addrHash := func(a common.Address) common.Hash {
		if !haveAddr || a != lastAddr {
			lastAddr, lastHash, haveAddr = a, crypto.Keccak256Hash(a[:]), true
		}
		return lastHash
	}
	return b.walk(func(key, val []byte) error {
		hk = [baseHashedKeySize]byte{}
		hk[0] = key[0]
		switch key[0] {
		case recKindAccount:
			copy(hk[1:33], addrHash(common.Address(key[1:21])).Bytes())
		case recKindStorage:
			copy(hk[1:33], addrHash(common.Address(key[1:21])).Bytes())
			copy(hk[33:65], crypto.Keccak256(key[21:53]))
		case recKindCodeUse: // 'c': content-addressed already
			copy(hk[1:33], key[1:33])
		default:
			return fmt.Errorf("base block %d: unknown row kind %q", b.block, key[0])
		}
		return fn(hk[:], val)
	})
}

// walk streams the raw preimage-keyed rows in key order.
func (b *Base) walk(fn func(key, val []byte) error) error {
	it := b.iter()
	for {
		key, val, ok, err := it.next()
		if err != nil || !ok {
			return err
		}
		if err := fn(key, val); err != nil {
			return err
		}
	}
}

// baseIter is walk() turned inside out: the fold merges the base against a
// delta stream, and a merge needs a pull cursor, not a callback. One decoded
// 64KB SST block is resident; key and val alias it, so copy to retain.
type baseIter struct {
	b   *Base
	bi  int    // next SST block to decode
	raw []byte // current decoded block
	pos int
}

func (b *Base) iter() *baseIter { return &baseIter{b: b} }

func (it *baseIter) next() (key, val []byte, ok bool, err error) {
	for {
		if it.pos+baseKeySize < len(it.raw) {
			vlen, vn := binary.Uvarint(it.raw[it.pos+baseKeySize:])
			if vn <= 0 {
				return nil, nil, false, fmt.Errorf("base block %d: bad row at %d", it.b.block, it.pos)
			}
			vstart := it.pos + baseKeySize + vn
			if vstart+int(vlen) > len(it.raw) {
				return nil, nil, false, fmt.Errorf("base block %d: truncated row at %d", it.b.block, it.pos)
			}
			key, val = it.raw[it.pos:it.pos+baseKeySize], it.raw[vstart:vstart+int(vlen)]
			it.pos = vstart + int(vlen)
			return key, val, true, nil
		}
		idx := it.b.sec[secBaseSSTIdx]
		n := len(idx) / baseIdxEntrySize
		if it.bi >= n {
			return nil, nil, false, nil
		}
		lo := binary.LittleEndian.Uint64(idx[it.bi*baseIdxEntrySize+baseKeySize:])
		hi := uint64(len(it.b.sec[secBaseSST]))
		if it.bi+1 < n {
			hi = binary.LittleEndian.Uint64(idx[(it.bi+1)*baseIdxEntrySize+baseKeySize:])
		}
		raw, err := it.b.dec.DecodeAll(it.b.sec[secBaseSST][lo:hi], it.raw[:0])
		if err != nil {
			return nil, nil, false, fmt.Errorf("base block %d: decode sst block %d: %w", it.b.block, it.bi, err)
		}
		it.raw, it.pos, it.bi = raw, 0, it.bi+1
	}
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

// PeekBase reports the floor block of dir's newest base file, without
// mapping it. Newest-wins for the same reason OpenBase is.
func PeekBase(dir string) (uint64, bool, error) {
	name, _, ok, err := newestBase(dir)
	if err != nil || !ok {
		return 0, false, err
	}
	blk, _ := ParseBaseFileName(name)
	return blk, true, nil
}

// ---------- writer ----------
//
// baseWriter is the streaming writer both producers share: the fold
// (state/fold.go, the real one) and WriteBase (a sort-then-loop wrapper the
// tests use). Sharing it is what makes "identical rows produce identical
// bytes" true by construction rather than by review.
//
// The bloom is the only thing that ever wanted the whole key set in RAM
// (buildBloom sizes m from len(keys)), which would have been an OOM at full
// mainnet, so the row count is passed IN: the fold's pass 1 counts, pass 2
// writes. Bits are OR-accumulated as rows arrive, which is order-independent,
// so the section bytes are identical to buildBloom's over the same keys.

// BaseRow is one preimage-keyed row: the 53-byte epoch keyspace.
type BaseRow struct {
	Key [baseKeySize]byte
	Val []byte
}

type baseWriter struct {
	m    BaseMeta
	enc  *zstd.Encoder
	f    *os.File
	out  *bufio.Writer
	dir  string
	tmp  string
	path string
	done bool // Commit succeeded: Abort is a no-op

	written  uint64 // SST bytes emitted so far, which is also the section offset
	idx      []byte
	raw      []byte
	firstKey []byte
	block    []byte

	bloomM uint64
	words  []uint64

	rows, want uint64
	last       [baseKeySize]byte
	haveLast   bool
}

// newBaseWriter creates dir/base_<B>.tmp. rowCount must be the exact number
// of Add calls that follow (Finish refuses otherwise): it sizes the bloom.
func newBaseWriter(dir string, m BaseMeta, rowCount uint64) (*baseWriter, error) {
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedBestCompression))
	if err != nil {
		return nil, err
	}
	path := filepath.Join(dir, BaseFileName(m.Block))
	f, err := os.Create(path + ".tmp")
	if err != nil {
		enc.Close()
		return nil, err
	}
	bits := bloomBits(rowCount)
	return &baseWriter{
		m: m, enc: enc, f: f, out: bufio.NewWriterSize(f, 4<<20),
		dir: dir, tmp: path + ".tmp", path: path,
		bloomM: bits, words: make([]uint64, bits/64),
		want: rowCount,
	}, nil
}

// Add appends one row. Keys must arrive strictly ascending: the sparse index,
// the lookup binary search and the merge all assume it, and a duplicate key
// would make the file answer two different values for one key.
func (w *baseWriter) Add(key, val []byte) error {
	if len(key) != baseKeySize {
		return fmt.Errorf("base writer: key is %d bytes, want %d", len(key), baseKeySize)
	}
	if w.haveLast && bytes.Compare(key, w.last[:]) <= 0 {
		return fmt.Errorf("base writer: key %x is not above the previous key %x", key, w.last[:])
	}
	copy(w.last[:], key)
	w.haveLast = true
	w.rows++
	if w.firstKey == nil {
		w.firstKey = append([]byte(nil), key...)
	}
	w.raw = append(w.raw, key...)
	w.raw = binary.AppendUvarint(w.raw, uint64(len(val)))
	w.raw = append(w.raw, val...)
	h1, h2 := bloomHash(key)
	for i := uint64(0); i < bloomHashes; i++ {
		bit := (h1 + i*h2) % w.bloomM
		w.words[bit/64] |= 1 << (bit % 64)
	}
	if len(w.raw) >= sstBlockTarget {
		return w.flushBlock()
	}
	return nil
}

func (w *baseWriter) flushBlock() error {
	if len(w.raw) == 0 {
		return nil
	}
	w.idx = append(w.idx, w.firstKey...)
	w.idx = binary.LittleEndian.AppendUint64(w.idx, w.written)
	w.block = w.enc.EncodeAll(w.raw, w.block[:0])
	if _, err := w.out.Write(w.block); err != nil {
		return err
	}
	w.written += uint64(len(w.block))
	w.raw, w.firstKey = w.raw[:0], nil
	return nil
}

// Finish writes the trailing sections and the footer and fsyncs the temp
// file, returning its path. The file is NOT visible under its real name
// until Commit: the fold verifies the temp file first (fold.go crash table).
func (w *baseWriter) Finish() (string, error) {
	if w.rows != w.want {
		return "", fmt.Errorf("base writer: %d rows added, %d announced (the bloom is sized from the announced count)", w.rows, w.want)
	}
	if err := w.flushBlock(); err != nil {
		return "", err
	}

	var hdrPayload []byte
	for _, h := range w.m.Headers {
		hdrPayload = binary.AppendUvarint(hdrPayload, uint64(len(h)))
		hdrPayload = append(hdrPayload, h...)
	}
	var hdrSec []byte
	if len(hdrPayload) > 0 {
		hdrSec = w.enc.EncodeAll(hdrPayload, nil)
	}

	var offsets [baseNumSections][2]uint64
	offsets[secBaseSST] = [2]uint64{0, w.written}
	pos := w.written
	section := func(id int, b []byte) error {
		offsets[id][0] = pos
		offsets[id][1] = uint64(len(b))
		pos += uint64(len(b))
		_, err := w.out.Write(b)
		return err
	}
	if err := section(secBaseSSTIdx, w.idx); err != nil {
		return "", err
	}
	if err := section(secBaseKeybloom, encodeBloom(w.bloomM, w.words)); err != nil {
		return "", err
	}
	if err := section(secBaseHeaders, hdrSec); err != nil {
		return "", err
	}

	var ft [baseFooterSize]byte
	copy(ft[0:4], baseMagic[:])
	binary.LittleEndian.PutUint32(ft[4:8], baseVersion)
	binary.LittleEndian.PutUint64(ft[8:16], w.m.Block)
	binary.LittleEndian.PutUint64(ft[16:24], w.m.HdrFrom)
	binary.LittleEndian.PutUint64(ft[24:32], w.m.CumTx)
	copy(ft[32:64], w.m.Root[:])
	for i := 0; i < baseNumSections; i++ {
		binary.LittleEndian.PutUint64(ft[64+i*16:], offsets[i][0])
		binary.LittleEndian.PutUint64(ft[72+i*16:], offsets[i][1])
	}
	copy(ft[baseFooterSize-4:], baseMagic[:])
	if _, err := w.out.Write(ft[:]); err != nil {
		return "", err
	}
	if err := w.out.Flush(); err != nil {
		return "", err
	}
	if err := w.f.Sync(); err != nil {
		return "", err
	}
	if err := w.f.Close(); err != nil {
		return "", err
	}
	w.f = nil
	w.enc.Close()
	w.enc = nil
	return w.tmp, nil
}

// Commit makes the file visible. THE COMMIT POINT: the rename is ordered
// after the file's own fsync, and the directory fsync after the rename, so a
// crash either leaves the old base untouched or the new one complete.
func (w *baseWriter) Commit() error {
	if err := os.Rename(w.tmp, w.path); err != nil {
		return err
	}
	if err := syncDir(w.dir); err != nil {
		return err
	}
	w.done = true
	return nil
}

// Abort drops the temp file. Safe to defer: a no-op after Commit.
func (w *baseWriter) Abort() {
	if w.done {
		return
	}
	if w.f != nil {
		w.f.Close()
		w.f = nil
	}
	if w.enc != nil {
		w.enc.Close()
		w.enc = nil
	}
	os.Remove(w.tmp)
}

// syncDir fsyncs a directory so a rename inside it is durable.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// BaseMeta is everything the footer and the header window carry.
type BaseMeta struct {
	Block   uint64      // B
	CumTx   uint64      // cumulative tx count at the end of B
	Root    common.Hash // header(B).Root
	HdrFrom uint64      // first block of the header window (max(0, B-256))
	Headers [][]byte    // RLP for HdrFrom..B, nil entry = absent
}

// WriteBase writes base_<B> into dir (tmp + rename) and returns its path.
// Rows are sorted here; the streaming writer refuses duplicates. The fold
// producer (state/fold.go) drives the same baseWriter directly, so identical
// row sets produce identical files whichever path built them.
func WriteBase(dir string, m BaseMeta, rows []BaseRow) (string, error) {
	sort.Slice(rows, func(i, j int) bool { return bytes.Compare(rows[i].Key[:], rows[j].Key[:]) < 0 })
	w, err := newBaseWriter(dir, m, uint64(len(rows)))
	if err != nil {
		return "", err
	}
	defer w.Abort()
	for i := range rows {
		if err := w.Add(rows[i].Key[:], rows[i].Val); err != nil {
			return "", err
		}
	}
	if _, err := w.Finish(); err != nil {
		return "", err
	}
	if err := w.Commit(); err != nil {
		return "", err
	}
	return w.path, nil
}

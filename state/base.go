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

// Account returns the account RLP of addr at B (the stored StorageRoot is
// the real one captured on chain; substituting the sentinel is the caller's
// job, exactly as for epoch rows). found=false = the account does not exist.
// One direct probe, no hashing: the key IS the address.
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
	idx := b.sec[secBaseSSTIdx]
	n := len(idx) / baseIdxEntrySize
	var buf []byte
	for bi := 0; bi < n; bi++ {
		lo := binary.LittleEndian.Uint64(idx[bi*baseIdxEntrySize+baseKeySize:])
		hi := uint64(len(b.sec[secBaseSST]))
		if bi+1 < n {
			hi = binary.LittleEndian.Uint64(idx[(bi+1)*baseIdxEntrySize+baseKeySize:])
		}
		raw, err := b.dec.DecodeAll(b.sec[secBaseSST][lo:hi], buf[:0])
		if err != nil {
			return fmt.Errorf("base block %d: decode sst block %d: %w", b.block, bi, err)
		}
		buf = raw
		for pos := 0; pos+baseKeySize < len(raw); {
			vlen, vn := binary.Uvarint(raw[pos+baseKeySize:])
			if vn <= 0 {
				return fmt.Errorf("base block %d: bad row at %d", b.block, pos)
			}
			vstart := pos + baseKeySize + vn
			if vstart+int(vlen) > len(raw) {
				return fmt.Errorf("base block %d: truncated row at %d", b.block, pos)
			}
			if err := fn(raw[pos:pos+baseKeySize], raw[vstart:vstart+int(vlen)]); err != nil {
				return err
			}
			pos = vstart + int(vlen)
		}
	}
	return nil
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

// PeekBase reports whether dir carries a base file, without mapping it.
func PeekBase(dir string) (uint64, bool, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0, false, err
	}
	for _, e := range entries {
		if b, ok := ParseBaseFileName(e.Name()); ok {
			return b, true, nil
		}
	}
	return 0, false, nil
}

// ---------- writer ----------
//
// THE ONLY CALLERS TODAY ARE TESTS. The real producer is a pruning node
// folding snapshot(K-1) with the period's own captured writes (DESIGN.md
// "Our own state sync"), which does not exist yet; this exists so the format
// is exercised end to end rather than only read. When that producer lands it
// takes over this function.
// ponytail: rows is one in-RAM slice, fine for tests and hopeless at full
// mainnet; the fold producer streams a merge instead.

// BaseRow is one preimage-keyed row: the 53-byte epoch keyspace.
type BaseRow struct {
	Key [baseKeySize]byte
	Val []byte
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
// Rows are sorted here; duplicates are not checked (the producer owns that).
func WriteBase(dir string, m BaseMeta, rows []BaseRow) (string, error) {
	sort.Slice(rows, func(i, j int) bool { return bytes.Compare(rows[i].Key[:], rows[j].Key[:]) < 0 })

	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedBestCompression))
	if err != nil {
		return "", err
	}
	defer enc.Close()

	path := filepath.Join(dir, BaseFileName(m.Block))
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return "", err
	}
	defer func() {
		f.Close()
		os.Remove(tmp) // no-op once the rename succeeded
	}()
	out := bufio.NewWriterSize(f, 4<<20)

	var (
		written  uint64 // bytes of SST emitted so far, which is also the section offset
		idx      []byte
		raw      []byte
		firstKey []byte
		block    []byte
		keys     [][]byte
	)
	flush := func() error {
		if len(raw) == 0 {
			return nil
		}
		idx = append(idx, firstKey...)
		idx = binary.LittleEndian.AppendUint64(idx, written)
		block = enc.EncodeAll(raw, block[:0])
		if _, err := out.Write(block); err != nil {
			return err
		}
		written += uint64(len(block))
		raw, firstKey = raw[:0], nil
		return nil
	}
	for i := range rows {
		r := &rows[i]
		keys = append(keys, r.Key[:])
		if firstKey == nil {
			firstKey = append([]byte(nil), r.Key[:]...)
		}
		raw = append(raw, r.Key[:]...)
		raw = binary.AppendUvarint(raw, uint64(len(r.Val)))
		raw = append(raw, r.Val...)
		if len(raw) >= sstBlockTarget {
			if err := flush(); err != nil {
				return "", err
			}
		}
	}
	if err := flush(); err != nil {
		return "", err
	}

	var hdrPayload []byte
	for _, h := range m.Headers {
		hdrPayload = binary.AppendUvarint(hdrPayload, uint64(len(h)))
		hdrPayload = append(hdrPayload, h...)
	}
	var hdrSec []byte
	if len(hdrPayload) > 0 {
		hdrSec = enc.EncodeAll(hdrPayload, nil)
	}

	var offsets [baseNumSections][2]uint64
	offsets[secBaseSST] = [2]uint64{0, written}
	pos := written
	section := func(id int, b []byte) error {
		offsets[id][0] = pos
		offsets[id][1] = uint64(len(b))
		pos += uint64(len(b))
		_, err := out.Write(b)
		return err
	}
	if err := section(secBaseSSTIdx, idx); err != nil {
		return "", err
	}
	if err := section(secBaseKeybloom, buildBloom(keys)); err != nil {
		return "", err
	}
	if err := section(secBaseHeaders, hdrSec); err != nil {
		return "", err
	}

	var ft [baseFooterSize]byte
	copy(ft[0:4], baseMagic[:])
	binary.LittleEndian.PutUint32(ft[4:8], baseVersion)
	binary.LittleEndian.PutUint64(ft[8:16], m.Block)
	binary.LittleEndian.PutUint64(ft[16:24], m.HdrFrom)
	binary.LittleEndian.PutUint64(ft[24:32], m.CumTx)
	copy(ft[32:64], m.Root[:])
	for i := 0; i < baseNumSections; i++ {
		binary.LittleEndian.PutUint64(ft[64+i*16:], offsets[i][0])
		binary.LittleEndian.PutUint64(ft[72+i*16:], offsets[i][1])
	}
	copy(ft[baseFooterSize-4:], baseMagic[:])
	if _, err := out.Write(ft[:]); err != nil {
		return "", err
	}
	if err := out.Flush(); err != nil {
		return "", err
	}
	if err := f.Sync(); err != nil {
		return "", err
	}
	if err := f.Close(); err != nil {
		return "", err
	}
	return path, os.Rename(tmp, path)
}

package state

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/ava-labs/libevm/common"
	"github.com/containerman17/epochdb/dist"
	"github.com/klauspost/compress/zstd"
)

// Epoch is one open sealed epoch artifact. Bytes come from dist: an mmap of
// the spool file on a node with no S3 credentials, casfs (window chunk cache,
// ranged GETs) on one with them. The SMALL sections are read once at open and
// kept; the big ones (bodies, headers, sst, logidx, stored logs and receipts)
// are read by range as queries need them, which is what makes an epoch
// servable without holding it locally at all.
//
// TWO KINDS OF RESIDENT SECTION since the window cache (DESIGN.md ruling of
// 2026-08-03). The two BLOOMS are copied onto the Go heap once and never read
// again: an epoch is immutable, so there is nothing to re-read, and a filter
// is exactly the structure where a byte that turns into something else is a
// wrong answer rather than a slow one. Everything else keeps a dist.View, one
// mapping per 4MB chunk, held for the life of the epoch.
type Epoch struct {
	Start   uint64
	Count   uint64 // blocks
	TxCount uint64

	// Hash is the artifact's hex sha256, i.e. its name everywhere.
	Hash string
	// Prev is sha256 of epoch K-1, or the chain root (dist.ChainRoot) for the
	// first epoch of a chain: the hash chain of DESIGN.md "Distribution".
	Prev [32]byte

	blob *dist.Blob
	off  [epochNumSections][2]uint64  // offset, length per section
	sec  [epochNumSections]*dist.View // resident views (nil = read on demand)
	dec  *zstd.Decoder                // registered with this epoch's dicts

	keyBloom bloom
	txBloom  bloom

	// txidx is the one section that is resident ON DEMAND: the tx bloom
	// rejects an unknown hash without touching it, so an epoch nobody asks a
	// tx question about never pays for it, and one that is asked keeps the
	// view rather than re-mapping per query.
	txidxOnce sync.Once
	txidxView *dist.View
	txidxErr  error

	// txLoads counts tx-index traversals, i.e. how often the tx bloom failed
	// to reject (tests).
	txLoads atomic.Uint64
}

// bloom is a filter on the Go heap. It was a view of the mapping until the
// window cache landed; it is a copy now because it is read on every descent
// forever, so one copy at open beats a mapping to defend for the life of the
// process. The probe reads words with binary.LittleEndian.Uint64, which needs
// no alignment.
type bloom struct {
	m    uint64
	k    uint32
	bits []byte
}

func parseBloom(sec []byte) (bloom, error) {
	if len(sec) < 16 {
		return bloom{}, fmt.Errorf("truncated bloom (%d bytes)", len(sec))
	}
	b := bloom{
		m:    binary.LittleEndian.Uint64(sec[0:8]),
		k:    binary.LittleEndian.Uint32(sec[8:12]),
		bits: sec[16:],
	}
	if b.m == 0 || uint64(len(b.bits))*8 < b.m {
		return bloom{}, fmt.Errorf("bloom claims %d bits over %d bytes", b.m, len(b.bits))
	}
	return b, nil
}

func (b bloom) mayContain(key []byte) bool {
	h1, h2 := bloomHash(key)
	for i := uint64(0); i < uint64(b.k); i++ {
		bit := (h1 + i*h2) % b.m
		w := binary.LittleEndian.Uint64(b.bits[bit/64*8:])
		if w&(1<<(bit%64)) == 0 {
			return false
		}
	}
	return true
}

// residentSections are MAPPED at open and kept for the life of the epoch: the
// indexes every query starts from. Everything else is ranged. The two blooms
// are resident too but go on the heap instead (see Epoch), and secTxidx is
// mapped lazily (see Epoch.TxCandidates).
var residentSections = []int{
	secDict, secBodiesIdx, secHeadersIdx, secSSTIdx, secDeletes,
	secLogsDict, secFullLogsIdx, secRcptIdx,
}

// vu64 and vu32 read one little-endian word out of a view. A word CAN straddle
// a 4MB chunk boundary, because a section starts at an arbitrary artifact
// offset and none of the strides here divide 4MB; View.Slice copies in exactly
// that case, so nothing above it has to know where the chunks are.
func vu64(v *dist.View, off uint64) uint64 {
	return binary.LittleEndian.Uint64(v.Slice(int64(off), 8))
}

func vu32(v *dist.View, off uint64) uint32 {
	return binary.LittleEndian.Uint32(v.Slice(int64(off), 4))
}

// vslice is View.Slice in the uint64 offsets the epoch format uses.
func vslice(v *dist.View, off, n uint64) []byte { return v.Slice(int64(off), int64(n)) }

func vlen(v *dist.View) uint64 { return uint64(v.Len()) }

// End returns the last block in the epoch (inclusive).
func (e *Epoch) End() uint64 { return e.Start + e.Count - 1 }

// Dict returns the epoch's trained zstd dictionary (empty for dict-less
// epochs).
func (e *Epoch) Dict() []byte { return vslice(e.sec[secDict], 0, vlen(e.sec[secDict])) }

// SectionSizes returns section byte sizes by name (compression scoreboard).
func (e *Epoch) SectionSizes() map[string]uint64 {
	names := []string{"dict", "bodies", "bodiesIdx", "headers", "headersIdx",
		"sst", "sstIdx", "deletes", "txidx", "logidx", "keybloom",
		"logsDict", "fullLogs", "fullLogsIdx", "rcpt", "rcptIdx", "txbloom"}
	out := make(map[string]uint64, epochNumSections)
	for i, n := range names {
		out[n] = e.off[i][1]
	}
	return out
}

// OpenEpoch opens the epoch artifact named by hash.
func OpenEpoch(st *dist.Store, hash string) (*Epoch, error) {
	blob, err := st.Open(hash)
	if err != nil {
		return nil, err
	}
	e, err := openEpochBlob(blob, hash)
	if err != nil {
		blob.Close()
		return nil, err
	}
	return e, nil
}

// parseFooter fills e from the artifact's fixed-size footer: format check,
// coverage, prev-hash and the section table. It reads the tail of the blob and
// nothing else.
func (e *Epoch) parseFooter(blob *dist.Blob, hash string) error {
	size := blob.Size()
	if size < epochFooterSize {
		return fmt.Errorf("epoch %s: too small", hash)
	}
	ft, err := blob.Read(size-epochFooterSize, epochFooterSize)
	if err != nil {
		return err
	}
	// v5 is the only supported format. Older files are recognized far enough
	// to say so and no further: there is no upgrade path (user ruling
	// 2026-07-28), the corpus is disposable and gets resynced.
	if !bytes.Equal(ft[0:4], epochMagic[:]) || !bytes.Equal(ft[epochFooterSize-4:], epochMagic[:]) {
		return fmt.Errorf("epoch %s: unrecognized footer, not format v%d (older formats are unsupported: delete the corpus and resync)", hash, epochVersion)
	}
	if v := binary.LittleEndian.Uint32(ft[4:8]); v != epochVersion {
		return fmt.Errorf("epoch %s is format v%d, unsupported: delete the corpus and resync", hash, v)
	}
	e.Start = binary.LittleEndian.Uint64(ft[8:16])
	e.Count = binary.LittleEndian.Uint64(ft[16:24])
	e.TxCount = binary.LittleEndian.Uint64(ft[24:32])
	e.Hash = hash
	copy(e.Prev[:], ft[32:64])
	body := size - epochFooterSize
	for i := 0; i < epochNumSections; i++ {
		off := binary.LittleEndian.Uint64(ft[epochTableOff+i*16:])
		ln := binary.LittleEndian.Uint64(ft[epochTableOff+8+i*16:])
		if off > body || ln > body-off { // overflow-safe bounds check
			return fmt.Errorf("epoch %s: section %d out of bounds", hash, i)
		}
		e.off[i] = [2]uint64{off, ln}
	}
	return nil
}

// EpochLink is what a chain WALK needs from an epoch and nothing else: the
// range it covers and the artifact it chains back to. See ReadEpochLink.
type EpochLink struct {
	Start, Count uint64
	Prev         [32]byte
}

// End is the epoch's last block.
func (l EpochLink) End() uint64 { return l.Start + l.Count - 1 }

// ReadEpochLink reads ONE epoch's FOOTER and stops there: existence (the blob
// open is a size probe, a HEAD under casfs) plus the ~1KB tail that carries the
// coverage and the prev-hash. NOTHING ELSE IS TOUCHED, which is the whole
// point: OpenEpoch additionally materializes every resident section (the key
// bloom alone is 20 bits per key, gigabytes across a mainnet corpus), and a
// startup chain walk that paid that would download the corpus to prove it
// exists.
// Content is never rehashed here either; that is `serve --verify`.
func ReadEpochLink(st *dist.Store, hash string) (EpochLink, error) {
	blob, err := st.Open(hash)
	if err != nil {
		return EpochLink{}, err
	}
	defer blob.Close()
	e := &Epoch{}
	if err := e.parseFooter(blob, hash); err != nil {
		return EpochLink{}, err
	}
	return EpochLink{Start: e.Start, Count: e.Count, Prev: e.Prev}, nil
}

func openEpochBlob(blob *dist.Blob, hash string) (*Epoch, error) {
	e := &Epoch{blob: blob}
	if err := e.parseFooter(blob, hash); err != nil {
		return nil, err
	}
	var err error
	for _, id := range residentSections {
		if e.sec[id], err = e.view(id); err != nil {
			e.Close()
			return nil, fmt.Errorf("epoch %s: section %d: %w", hash, id, err)
		}
	}
	var dicts [][]byte
	if d := e.Dict(); len(d) > 0 {
		dicts = append(dicts, d)
	}
	if d := vslice(e.sec[secLogsDict], 0, vlen(e.sec[secLogsDict])); len(d) > 0 {
		dicts = append(dicts, d) // DecodeAll picks by frame dictID
	}
	var decOpts []zstd.DOption
	if len(dicts) > 0 {
		decOpts = append(decOpts, zstd.WithDecoderDicts(dicts...))
	}
	if e.dec, err = zstd.NewReader(nil, decOpts...); err != nil {
		e.Close()
		return nil, err
	}

	// The blooms are COPIED, once, and never read from the artifact again.
	for _, b := range []struct {
		id   int
		name string
		dst  *bloom
	}{{secKeybloom, "keybloom", &e.keyBloom}, {secTxBloom, "txbloom", &e.txBloom}} {
		raw, err := e.read(b.id, 0, e.off[b.id][1])
		if err != nil {
			e.Close()
			return nil, fmt.Errorf("epoch %s: %s: %w", hash, b.name, err)
		}
		if *b.dst, err = parseBloom(raw); err != nil {
			e.Close()
			return nil, fmt.Errorf("epoch %s: %s: %w", hash, b.name, err)
		}
	}
	return e, nil
}

func (e *Epoch) Close() {
	if e.dec != nil {
		e.dec.Close()
		e.dec = nil
	}
	for i, v := range e.sec {
		if v != nil {
			v.Close()
			e.sec[i] = nil
		}
	}
	if e.txidxView != nil {
		e.txidxView.Close()
		e.txidxView = nil
	}
	if e.blob != nil {
		e.blob.Close()
		e.blob = nil
	}
	e.keyBloom, e.txBloom = bloom{}, bloom{}
}

// view maps a whole section and holds the mapping until Close. A resident
// section is read all the time, so it is worth one mapping per chunk rather
// than a pread per access; the price is that its chunk files keep their blocks
// on disk while the epoch is open, even if the eviction worker unlinks them.
func (e *Epoch) view(id int) (*dist.View, error) {
	return e.blob.View(e.off[id][0], e.off[id][1])
}

// read COPIES bytes [off, off+n) of one section onto the heap. This is the
// query path: the bytes are the caller's from then on, so there is nothing to
// hold, nothing to release, and no way for the cache underneath to change
// them.
func (e *Epoch) read(id int, off, n uint64) ([]byte, error) {
	if off > e.off[id][1] || n > e.off[id][1]-off {
		return nil, fmt.Errorf("epoch %d: section %d range [%d,%d) outside %d bytes", e.Start, id, off, off+n, e.off[id][1])
	}
	return e.blob.Read(e.off[id][0]+off, n)
}

// decodeAll is goroutine-safe: zstd Decoder.DecodeAll is documented
// concurrent-safe ("DecodeAll can be used concurrently").
func (e *Epoch) decodeAll(frame []byte) ([]byte, error) {
	return e.dec.DecodeAll(frame, nil)
}

// framedBlob returns entry (block - Start) from a framed section pair.
func (e *Epoch) framedBlob(dataSec int, index *dist.View, rel uint64) ([]byte, error) {
	frame := rel / framedGroup
	if (frame+2)*8 > vlen(index) {
		return nil, fmt.Errorf("frame %d beyond index", frame)
	}
	lo := vu64(index, frame*8)
	hi := vu64(index, (frame+1)*8)
	frameBytes, err := e.read(dataSec, lo, hi-lo)
	if err != nil {
		return nil, err
	}
	raw, err := e.decodeAll(frameBytes)
	if err != nil {
		return nil, err
	}
	pos := 0
	for i := uint64(0); ; i++ {
		ln, n := binary.Uvarint(raw[pos:])
		if n <= 0 {
			return nil, fmt.Errorf("bad frame payload")
		}
		pos += n
		if i == rel%framedGroup {
			return raw[pos : pos+int(ln)], nil
		}
		pos += int(ln)
	}
}

// Container returns the raw container for block n.
func (e *Epoch) Container(n uint64) ([]byte, error) {
	if n < e.Start || n > e.End() {
		return nil, fmt.Errorf("block %d outside epoch [%d,%d]", n, e.Start, e.End())
	}
	return e.framedBlob(secBodies, e.sec[secBodiesIdx], n-e.Start)
}

// HeaderRLP returns the RLP header for block n.
func (e *Epoch) HeaderRLP(n uint64) ([]byte, error) {
	if n < e.Start || n > e.End() {
		return nil, fmt.Errorf("block %d outside epoch [%d,%d]", n, e.Start, e.End())
	}
	return e.framedBlob(secHeaders, e.sec[secHeadersIdx], n-e.Start)
}

// MayContainKey is the bloom prefilter for the cross-epoch descent.
func (e *Epoch) MayContainKey(key []byte) bool { return e.keyBloom.mayContain(key) }

// MayContainTx is the bloom prefilter in front of the tx index: false means
// this epoch definitely holds neither a tx nor a block with that fingerprint,
// which is the whole answer for an unknown hash and costs no index load.
func (e *Epoch) MayContainTx(fp uint64) bool {
	k := txBloomKey(fp)
	return e.txBloom.mayContain(k[:])
}

// sstBlock decodes the SST block the sparse-index entry bi points at.
func (e *Epoch) sstBlock(idx *dist.View, bi, nEntries int) ([]byte, error) {
	lo := vu64(idx, uint64(bi*sstIdxEntrySize+sortedKeySize+8))
	hi := e.off[secSST][1]
	if bi+1 < nEntries {
		hi = vu64(idx, uint64((bi+1)*sstIdxEntrySize+sortedKeySize+8))
	}
	block, err := e.read(secSST, lo, hi-lo)
	if err != nil {
		return nil, err
	}
	return e.decodeAll(block)
}

// StateSearch finds the largest write <= n of key inside this epoch.
// val nil with found=true is an explicit delete/zero record.
func (e *Epoch) StateSearch(key []byte, n uint64) (val []byte, blk uint64, found bool, err error) {
	idx := e.sec[secSSTIdx]
	nEntries := int(vlen(idx)) / sstIdxEntrySize
	if nEntries == 0 {
		return nil, 0, false, nil
	}
	entry := func(i int) []byte { return vslice(idx, uint64(i*sstIdxEntrySize), sstIdxEntrySize) }
	// first sparse entry > (key, n)
	ub := sort.Search(nEntries, func(i int) bool {
		en := entry(i)
		if c := bytes.Compare(en[:sortedKeySize], key); c != 0 {
			return c > 0
		}
		return binary.BigEndian.Uint64(en[sortedKeySize:sortedKeySize+8]) > n
	})
	bi := ub - 1
	if bi < 0 {
		return nil, 0, false, nil
	}
	raw, err := e.sstBlock(idx, bi, nEntries)
	if err != nil {
		return nil, 0, false, err
	}
	// scan rows for the largest <= (key, n); rows are sorted.
	pos := 0
	for pos < len(raw) {
		rk := raw[pos : pos+sortedKeySize]
		rb := binary.BigEndian.Uint64(raw[pos+sortedKeySize:])
		vlen, vn := binary.Uvarint(raw[pos+sortedKeySize+8:])
		vstart := pos + sortedKeySize + 8 + vn
		c := bytes.Compare(rk, key)
		if c > 0 || (c == 0 && rb > n) {
			break
		}
		if c == 0 {
			blk = rb
			found = true
			if vlen == 0 {
				val = nil
			} else {
				val = raw[vstart : vstart+int(vlen)]
			}
		}
		pos = vstart + int(vlen)
	}
	return val, blk, found, nil
}

// Code returns the contract code blob for hash from this epoch's 'c' rows.
// found=false = this epoch wrote no account carrying that code.
func (e *Epoch) Code(hash common.Hash) ([]byte, bool, error) {
	k := epochCodeKey(hash)
	if !e.MayContainKey(k[:]) {
		return nil, false, nil
	}
	val, _, found, err := e.StateSearch(k[:], ^uint64(0))
	return val, found, err
}

// WalkStateRows streams every SST row (verification, diff spill).
// Values are views into a transient decode buffer: copy to retain.
func (e *Epoch) WalkStateRows(fn func(StateRow)) error {
	idx := e.sec[secSSTIdx]
	nEntries := int(vlen(idx)) / sstIdxEntrySize
	for bi := 0; bi < nEntries; bi++ {
		raw, err := e.sstBlock(idx, bi, nEntries)
		if err != nil {
			return err
		}
		pos := 0
		for pos < len(raw) {
			var r StateRow
			copy(r.Key[:], raw[pos:pos+sortedKeySize])
			r.Block = binary.BigEndian.Uint64(raw[pos+sortedKeySize:])
			vlen, vn := binary.Uvarint(raw[pos+sortedKeySize+8:])
			vstart := pos + sortedKeySize + 8 + vn
			if vlen > 0 {
				r.Value = raw[vstart : vstart+int(vlen)]
			}
			fn(r)
			pos = vstart + int(vlen)
		}
	}
	return nil
}

// AccountDeletes calls fn for every account-delete row in this epoch.
func (e *Epoch) AccountDeletes(fn func(key []byte, block uint64)) {
	d := e.sec[secDeletes]
	n := vlen(d)
	for pos := uint64(0); pos+deleteEntrySize <= n; pos += deleteEntrySize {
		ent := vslice(d, pos, deleteEntrySize)
		fn(ent[:sortedKeySize], binary.BigEndian.Uint64(ent[sortedKeySize:]))
	}
}

// txIndex is a READER over the txidx section: a 16-byte header and five word
// slices, nothing decoded and nothing copied (DESIGN.md ruling 3). Every
// accessor reads the view, so the view must outlive the result, which is why
// the epoch owns it.
func (e *Epoch) txIndex(tx *dist.View) (efIdx *ef, blk packed, err error) {
	if tx.Len() < 16 {
		return nil, packed{}, fmt.Errorf("epoch %d: truncated txidx", e.Start)
	}
	nTx := vu64(tx, 0)
	efL := uint(vu32(tx, 8))
	blkBits := uint(vu32(tx, 12))
	pos := int64(16)
	var secs [5]words
	for i := range secs {
		if secs[i], pos, err = sliceWords(tx, pos); err != nil {
			return nil, packed{}, fmt.Errorf("epoch %d: txidx: %w", e.Start, err)
		}
	}
	return &ef{
		n:    int(nTx),
		l:    efL,
		lows: packed{w: secs[0], bits: efL},
		high: secs[1],
		sel0: secs[2],
		sel1: secs[3],
	}, packed{w: secs[4], bits: blkBits}, nil
}

// txidx maps the tx index ONCE, on the first query that gets past the bloom,
// and keeps the mapping for the life of the epoch. Lazily, not at open: the
// bloom rejects an unknown hash outright, so an epoch nobody asks a tx
// question about never fetches a single Elias-Fano chunk, which is what keeps
// eth_getTransactionByHash for a pending tx off the whole of history.
func (e *Epoch) txidx() (*dist.View, error) {
	e.txidxOnce.Do(func() {
		e.txidxView, e.txidxErr = e.view(secTxidx)
	})
	return e.txidxView, e.txidxErr
}

// TxCandidates returns absolute candidate blocks for a fingerprint (a tx
// hash, or since v6 a block hash mapping to its own height), descending.
func (e *Epoch) TxCandidates(fp uint64) ([]uint64, error) {
	if !e.MayContainTx(fp) {
		return nil, nil
	}
	tx, err := e.txidx()
	if err != nil {
		return nil, err
	}
	e.txLoads.Add(1)
	idx, blk, err := e.txIndex(tx)
	if err != nil {
		return nil, err
	}
	lo, hi := idx.lookup(fp)
	var out []uint64
	for i := hi - 1; i >= lo; i-- {
		out = append(out, e.Start+blk.get(i))
	}
	return out, nil
}

// logidxLookup finds a key's posting list. keyLen distinguishes the addr
// (20) and topic (32) tables.
//
// The logidx section is the one big section with a searchable table in it
// (10-13% of an epoch, so it is never resident), and the search runs on ranged
// reads: one small read per binary-search probe, then the list itself. The
// list's END comes from the NEXT table entry's offset, which is exact because
// buildLogidx appends lists in table order (addrs, then topics).
func (e *Epoch) logidxLookup(key []byte) ([]uint64, error) {
	total := e.off[secLogidx][1]
	if total < 4 {
		return nil, nil
	}
	// Each probe preads the few bytes it needs. The section is far too big to
	// map (mapping it would fill every chunk of it), and a copy of four bytes
	// is not worth avoiding.
	u32 := func(off uint64) (uint32, error) {
		b, err := e.read(secLogidx, off, 4)
		if err != nil {
			return 0, err
		}
		return binary.LittleEndian.Uint32(b), nil
	}
	nAddr64, err := u32(0)
	if err != nil {
		return nil, err
	}
	nAddr := uint64(nAddr64)
	topicHdr := 4 + nAddr*(20+8)
	if topicHdr+4 > total {
		return nil, fmt.Errorf("epoch %d: logidx truncated", e.Start)
	}
	nTopic64, err := u32(topicHdr)
	if err != nil {
		return nil, err
	}
	nTopic := uint64(nTopic64)
	listsOff := topicHdr + 4 + nTopic*(32+8)
	if listsOff > total {
		return nil, fmt.Errorf("epoch %d: logidx truncated", e.Start)
	}

	var base, stride, count uint64
	switch len(key) {
	case 20:
		base, stride, count = 4, 20+8, nAddr
	case 32:
		base, stride, count = topicHdr+4, 32+8, nTopic
	default:
		return nil, fmt.Errorf("logidx: bad key length %d", len(key))
	}
	// entryOff walks the two tables as one sequence, so "the next entry" at
	// the end of the addr table is the first topic entry.
	entryOff := func(i uint64) (entOff, width uint64, ok bool) {
		if len(key) == 20 && i == nAddr {
			if nTopic == 0 {
				return 0, 0, false
			}
			return topicHdr + 4, 32 + 8, true
		}
		if i >= count {
			return 0, 0, false
		}
		return base + i*stride, stride, true
	}
	listAt := func(i uint64) (uint64, error) {
		entOff, width, _ := entryOff(i)
		ent, err := e.read(secLogidx, entOff, width)
		if err != nil {
			return 0, err
		}
		return binary.LittleEndian.Uint64(ent[width-8:]), nil
	}

	var searchErr error
	i := uint64(sort.Search(int(count), func(i int) bool {
		ent, err := e.read(secLogidx, base+uint64(i)*stride, uint64(len(key)))
		if err != nil {
			searchErr = err
			return true
		}
		return bytes.Compare(ent, key) >= 0
	}))
	if searchErr != nil {
		return nil, searchErr
	}
	if i >= count {
		return nil, nil
	}
	ent, err := e.read(secLogidx, base+i*stride, stride)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(ent[:len(key)], key) {
		return nil, nil
	}
	off := binary.LittleEndian.Uint64(ent[len(key):])
	end := total - listsOff
	if _, _, ok := entryOff(i + 1); ok {
		if end, err = listAt(i + 1); err != nil {
			return nil, err
		}
	}
	if end < off {
		return nil, fmt.Errorf("epoch %d: logidx list [%d,%d) is inverted", e.Start, off, end)
	}
	raw, err := e.read(secLogidx, listsOff+off, end-off)
	if err != nil {
		return nil, err
	}
	ef, _, err := efUnmarshal(dist.ViewOf(raw))
	if err != nil {
		return nil, err
	}
	rel := ef.values()
	out := make([]uint64, len(rel))
	for j, r := range rel {
		out[j] = e.Start + r
	}
	return out, nil
}

// LogAddrBlocks returns the absolute blocks where addr emitted a log.
func (e *Epoch) LogAddrBlocks(addr [20]byte) ([]uint64, error) { return e.logidxLookup(addr[:]) }

// LogTopicBlocks returns the absolute blocks where topic appeared.
func (e *Epoch) LogTopicBlocks(topic [32]byte) ([]uint64, error) { return e.logidxLookup(topic[:]) }

// HasStoredLogs reports whether this epoch carries the stored-logs
// sections (index headers present; an epoch with zero logs still counts).
func (e *Epoch) HasStoredLogs() bool {
	return vlen(e.sec[secFullLogsIdx]) >= 4 && vlen(e.sec[secRcptIdx]) >= 4
}

// storedRecord fetches block n's record from a stored-frames section pair.
func (e *Epoch) storedRecord(dataSec int, index *dist.View, n uint64) ([]byte, bool, error) {
	if vlen(index) < 4 {
		return nil, false, fmt.Errorf("epoch %d: stored section absent", e.Start)
	}
	nMembers := int(vu32(index, 0))
	member := func(i int) uint64 { return uint64(4 + i*12) }
	offs := uint64(4 + nMembers*12)
	rel := uint32(n - e.Start)
	i := sort.Search(nMembers, func(i int) bool {
		return vu32(index, member(i)) >= rel
	})
	if i == nMembers || vu32(index, member(i)) != rel {
		return nil, false, nil
	}
	frame := uint64(vu32(index, member(i)+4))
	slot := vu32(index, member(i)+8)
	lo := vu64(index, offs+frame*8)
	hi := vu64(index, offs+(frame+1)*8)
	frameBytes, err := e.read(dataSec, lo, hi-lo)
	if err != nil {
		return nil, false, err
	}
	raw, err := e.decodeAll(frameBytes)
	if err != nil {
		return nil, false, err
	}
	pos := 0
	for s := uint32(0); ; s++ {
		ln, k := binary.Uvarint(raw[pos:])
		if k <= 0 {
			return nil, false, fmt.Errorf("epoch %d: bad stored frame", e.Start)
		}
		pos += k
		if s == slot {
			return raw[pos : pos+int(ln)], true, nil
		}
		pos += int(ln)
	}
}

// StoredLogsRecord returns block n's full-logs record (rpc.DecodeStoredLogs
// layout). ok=false = block has no logs.
func (e *Epoch) StoredLogsRecord(n uint64) ([]byte, bool, error) {
	return e.storedRecord(secFullLogs, e.sec[secFullLogsIdx], n)
}

// StoredRcptRecord returns block n's receipt-fields record (per tx:
// uvarint gasUsed + status byte). ok=false = block has no txs.
func (e *Epoch) StoredRcptRecord(n uint64) ([]byte, bool, error) {
	return e.storedRecord(secRcpt, e.sec[secRcptIdx], n)
}

// sampleSSTRow returns a random (key, block) row for bench probing.
func (e *Epoch) sampleSSTRow(r *rand.Rand) (key [sortedKeySize]byte, blk uint64, ok bool) {
	idx := e.sec[secSSTIdx]
	nEntries := int(vlen(idx)) / sstIdxEntrySize
	if nEntries == 0 {
		return key, 0, false
	}
	raw, err := e.sstBlock(idx, r.Intn(nEntries), nEntries)
	if err != nil {
		return key, 0, false
	}
	// reservoir-pick a row while walking the block (code rows are not state:
	// a block can be all code, hence the caller's retry)
	pos, seen := 0, 0
	for pos < len(raw) {
		rb := binary.BigEndian.Uint64(raw[pos+sortedKeySize:])
		vlen, vn := binary.Uvarint(raw[pos+sortedKeySize+8:])
		if raw[pos] != recKindCodeUse {
			seen++
			if r.Intn(seen) == 0 {
				copy(key[:], raw[pos:pos+sortedKeySize])
				blk = rb
				ok = true
			}
		}
		pos += sortedKeySize + 8 + vn + int(vlen)
	}
	return key, blk, ok
}

// ---------- local index ----------
//
// Artifacts are named by hash, so the data directory carries one MARKER per
// locally known epoch: `epoch_<start>_<count>.cas`, holding that epoch's hex
// sha256. The marker is the local index, nothing more: it says which artifact
// covers which heights, the hash inside it is self-verifying, and it is
// written by whoever learned about the epoch (seal, or bootstrap walking the
// hash chain). There is no manifest and no global authority anywhere.

const markerSuffix = ".cas"

// EpochMarkerName is the local index entry for the epoch starting at start.
func EpochMarkerName(start, count uint64) string {
	return fmt.Sprintf("epoch_%d_%d%s", start, count, markerSuffix)
}

// ParseEpochMarkerName returns (start, count, ok).
func ParseEpochMarkerName(name string) (uint64, uint64, bool) {
	var start, count uint64
	if n, err := fmt.Sscanf(name, "epoch_%d_%d"+markerSuffix, &start, &count); n != 2 || err != nil {
		return 0, 0, false
	}
	return start, count, EpochMarkerName(start, count) == name
}

// WriteMarker writes one local index entry (tmp+rename).
func WriteMarker(dir, name, hash string) error {
	if !dist.ValidHash(hash) {
		return fmt.Errorf("marker %s: %q is not a hex sha256", name, hash)
	}
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p+".tmp", []byte(hash+"\n"), 0o644); err != nil {
		return err
	}
	return os.Rename(p+".tmp", p)
}

// ReadMarker reads one local index entry.
func ReadMarker(dir, name string) (string, error) {
	raw, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		return "", err
	}
	hash := strings.TrimSpace(string(raw))
	if !dist.ValidHash(hash) {
		return "", fmt.Errorf("marker %s: %q is not a hex sha256", name, hash)
	}
	return hash, nil
}

// ---------- epoch set ----------

// EpochSet is every sealed epoch the local index knows, ascending. Sealing is
// strictly sequential, but a downloaded set may have holes: coverage is the
// contiguous epoch range from genesis. Epochs above the first gap stay open
// (their data is epoch-local and valid for body/tx-by-hash reads), but
// full-descent reads (state, receipts, logs) must call RequireCovered.
//
// THE SET IS LIVE (2026-08-01, when sealing moved into the serve process):
// Reload publishes epochs cut since open into the SAME *EpochSet every reader
// already holds, so nothing is rewired when an epoch is cut and no reader ever
// sees a half-swapped set. Readers get an immutable snapshot: All() returns a
// slice that is replaced, never mutated, and the *Epoch values in it are
// shared with the next snapshot (Reload opens only the markers it does not
// already have, so nothing an in-flight read holds is ever closed).
type EpochSet struct {
	cur atomic.Pointer[epochSnapshot]
}

// epochSnapshot is one immutable published view of the set.
type epochSnapshot struct {
	epochs []*Epoch // ascending by Start

	covered  uint64 // last block of the contiguous prefix from genesis
	gapStart uint64 // expected Start of the first missing epoch (covered+1)
	gapped   bool   // true when at least one epoch above the prefix exists
}

func (s *EpochSet) snap() *epochSnapshot {
	if v := s.cur.Load(); v != nil {
		return v
	}
	return &epochSnapshot{}
}

// All returns the current epochs, ascending. The slice is immutable: callers
// read it, never write it.
func (s *EpochSet) All() []*Epoch { return s.snap().epochs }

// OpenEpochSet opens every epoch the store's data directory indexes. An empty
// set is valid.
func OpenEpochSet(st *dist.Store) (*EpochSet, error) {
	s := &EpochSet{}
	eps, err := openEpochMarkers(st, nil)
	if err != nil {
		return nil, err
	}
	s.publish(eps)
	return s, nil
}

// Reload opens every epoch marker in the store's directory this set does not
// hold yet and publishes the enlarged set in one atomic swap. Returns the
// epochs it added. This is the in-process seal's publish step: it must run
// BEFORE the raw buckets those epochs replace are deleted, so no read of a
// sealed height can fall between the two sources.
func (s *EpochSet) Reload(st *dist.Store) ([]*Epoch, error) {
	cur := s.snap()
	have := make(map[string]bool, len(cur.epochs))
	for _, e := range cur.epochs {
		have[EpochMarkerName(e.Start, e.Count)] = true
	}
	fresh, err := openEpochMarkers(st, have)
	if err != nil || len(fresh) == 0 {
		return nil, err
	}
	s.publish(append(append(make([]*Epoch, 0, len(cur.epochs)+len(fresh)), cur.epochs...), fresh...))
	return fresh, nil
}

// openEpochMarkers opens the epochs named by the local index, skipping the
// marker names in have. On any error every epoch it opened is closed.
func openEpochMarkers(st *dist.Store, have map[string]bool) ([]*Epoch, error) {
	dir := st.Dir()
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var out []*Epoch
	fail := func(format string, args ...any) ([]*Epoch, error) {
		for _, e := range out {
			e.Close()
		}
		return nil, fmt.Errorf(format, args...)
	}
	for _, en := range entries {
		// A pre-casfs corpus (whole epoch files sitting in the data dir) is
		// refused by name: there is no migration, only delete and resync.
		if strings.HasPrefix(en.Name(), "epoch_") && strings.HasSuffix(en.Name(), ".epoch") {
			return fail("%s: %s is a pre-casfs epoch file; epochs are now content-addressed artifacts in %s/cas (no migration: delete the corpus and resync)", dir, en.Name(), dir)
		}
		start, count, ok := ParseEpochMarkerName(en.Name())
		if !ok || have[en.Name()] {
			continue
		}
		hash, err := ReadMarker(dir, en.Name())
		if err != nil {
			return fail("%w", err)
		}
		e, err := OpenEpoch(st, hash)
		if err != nil {
			return fail("%s: %w", en.Name(), err)
		}
		if e.Start != start || e.Count != count {
			e.Close()
			return fail("%s names blocks %d..%d but %s covers %d..%d", en.Name(), start, start+count-1, hash, e.Start, e.End())
		}
		out = append(out, e)
	}
	return out, nil
}

// publish sorts, walks coverage upward from genesis, and swaps the snapshot in.
// Anything after the first gap stays open for epoch-local reads but is outside
// coverage.
func (s *EpochSet) publish(eps []*Epoch) {
	sort.Slice(eps, func(i, j int) bool { return eps[i].Start < eps[j].Start })
	snap := &epochSnapshot{epochs: eps}
	for _, e := range eps {
		if e.End() <= snap.covered {
			continue
		}
		want := snap.covered + 1
		if snap.covered == 0 && e.Start <= 1 { // block 0 is genesis (no container); sealing starts at 1
			want = e.Start
		}
		if e.Start > want {
			snap.gapped, snap.gapStart = true, want
			break
		}
		snap.covered = e.End()
	}
	if !snap.gapped {
		snap.gapStart = snap.covered + 1
	}
	s.cur.Store(snap)
}

// CoveredEnd returns the last block of the contiguous sealed prefix from
// genesis (0 when nothing is covered).
func (s *EpochSet) CoveredEnd() uint64 { return s.snap().covered }

// RequireCovered errors when block n is beyond the contiguous prefix while
// later epochs exist (a hole): state, receipt, and log reads at n would
// silently skip missing history. Bodies/tx-by-hash are epoch-local and may
// still be served above the gap without this check.
func (s *EpochSet) RequireCovered(n uint64) error {
	snap := s.snap()
	if snap.gapped && n > snap.covered {
		return fmt.Errorf("missing epoch epoch_%d: sealed coverage is contiguous only through block %d", snap.gapStart, snap.covered)
	}
	return nil
}

func (s *EpochSet) Close() {
	for _, e := range s.All() {
		e.Close()
	}
	s.cur.Store(&epochSnapshot{})
}

// SealedEnd returns the last sealed block, ok=false for an empty set.
func (s *EpochSet) SealedEnd() (uint64, bool) {
	eps := s.All()
	if len(eps) == 0 {
		return 0, false
	}
	return eps[len(eps)-1].End(), true
}

// Head returns the newest epoch, ok=false for an empty set. Seal chains the
// next epoch's footer onto its hash.
func (s *EpochSet) Head() (*Epoch, bool) {
	eps := s.All()
	if len(eps) == 0 {
		return nil, false
	}
	return eps[len(eps)-1], true
}

// At returns the epoch containing block n.
func (s *EpochSet) At(n uint64) (*Epoch, bool) {
	eps := s.All()
	i := sort.Search(len(eps), func(i int) bool { return eps[i].End() >= n })
	if i == len(eps) || eps[i].Start > n {
		return nil, false
	}
	return eps[i], true
}

// GetByHeight serves raw containers from sealed epochs (rpc.BlockSource
// shape). ok=false when the block is not sealed.
func (s *EpochSet) GetByHeight(n uint64) ([]byte, bool, error) {
	e, ok := s.At(n)
	if !ok {
		return nil, false, nil
	}
	c, err := e.Container(n)
	if err != nil {
		return nil, false, err
	}
	return c, true, nil
}

// CombinedTxIndex answers tx-hash candidate queries over sealed epochs
// plus the raw per-bucket index (the unsealed tail; the bucket straddling
// the sealed end always overlaps the newest epoch, since buckets are 100k
// blocks and epochs cut on tx count, so candidates are deduped).
type CombinedTxIndex struct {
	Raw    *TxIndex
	Epochs *EpochSet
}

// WalkCandidates calls fn for every candidate block of hash, NEWEST FIRST
// (the raw tail, then epochs newest to oldest), and stops as soon as fn
// returns true. The caller verifies each candidate against the container, so
// stopping at the first verified hit is what keeps an
// eth_getTransactionByHash off the whole of history: a hash this node does
// not know is rejected by each epoch's tx bloom without loading anything, and
// a known one usually stops in the first epoch that answers.
//
// Duplicate heights are suppressed (the staging bucket straddling the sealed
// end overlaps the newest epoch), and candidate counts are tiny, so the seen
// list is a slice.
func (c CombinedTxIndex) WalkCandidates(hash common.Hash, fn func(blk uint64) (bool, error)) error {
	fp := txFingerprint(hash)
	var seen []uint64
	visit := func(blk uint64) (bool, error) {
		for _, s := range seen {
			if s == blk {
				return false, nil
			}
		}
		seen = append(seen, blk)
		return fn(blk)
	}
	if c.Raw != nil {
		if stop, err := c.Raw.walkCandidatesFP(fp, visit); stop || err != nil {
			return err
		}
	}
	if c.Epochs == nil {
		return nil
	}
	eps := c.Epochs.All()
	for i := len(eps) - 1; i >= 0; i-- {
		e := eps[i]
		if !e.MayContainTx(fp) {
			continue
		}
		blocks, err := e.TxCandidates(fp)
		if err != nil {
			return err
		}
		for _, blk := range blocks {
			stop, err := visit(blk)
			if stop || err != nil {
				return err
			}
		}
	}
	return nil
}

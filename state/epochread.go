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
// the spool file on a node with no S3 credentials, casfs (chunk cache, ranged
// GETs) on one with them. The SMALL sections are read once at open and kept;
// the big ones (bodies, headers, sst, logidx, stored logs and receipts) are
// read by range as queries need them, which is what makes an epoch servable
// without holding it locally at all.
type Epoch struct {
	Start   uint64
	Count   uint64 // blocks
	TxCount uint64

	// Hash is the artifact's hex sha256, i.e. its name everywhere.
	Hash string
	// Prev is sha256 of epoch K-1, or the chain root (sha256 of the genesis
	// config) for the first epoch of a chain: the hash chain of DESIGN.md
	// "Distribution".
	Prev [32]byte

	blob *dist.Blob
	off  [epochNumSections][2]uint64 // offset, length per section
	sec  [epochNumSections][]byte    // resident sections (nil = read on demand)
	dec  *zstd.Decoder               // registered with this epoch's dicts

	keyBloom bloomView
	txBloom  bloomView

	// The tx index is loaded lazily (only when txBloom says maybe) and
	// dropped by the EpochSet's LRU, so it is heap for a handful of epochs
	// instead of for all of history. txLoads counts real loads (tests).
	txMu    sync.Mutex
	txIdx   *epochTxIdx
	txLoads atomic.Uint64
}

// bloomView is a filter read straight out of the section bytes: on a
// credentials-free node that is a zero-copy view of the mmap, and the probe
// reads words with binary.LittleEndian.Uint64, which needs no alignment. So a
// bloom costs page cache, not Go heap.
type bloomView struct {
	m    uint64
	k    uint32
	bits []byte
}

func parseBloom(sec []byte) (bloomView, error) {
	if len(sec) < 16 {
		return bloomView{}, fmt.Errorf("truncated bloom (%d bytes)", len(sec))
	}
	b := bloomView{
		m:    binary.LittleEndian.Uint64(sec[0:8]),
		k:    binary.LittleEndian.Uint32(sec[8:12]),
		bits: sec[16:],
	}
	if b.m == 0 || uint64(len(b.bits))*8 < b.m {
		return bloomView{}, fmt.Errorf("bloom claims %d bits over %d bytes", b.m, len(b.bits))
	}
	return b, nil
}

func (b bloomView) mayContain(key []byte) bool {
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

// epochTxIdx is the decoded Elias-Fano tx index of one epoch: fp48 -> the
// epoch-relative blocks that may hold the tx.
type epochTxIdx struct {
	ef  *ef
	blk *packed
}

// residentSections are read at open and kept for the life of the epoch: the
// indexes and filters every query starts from. Everything else is ranged.
// secTxidx is deliberately NOT here (see Epoch.txIndex).
var residentSections = []int{
	secDict, secBodiesIdx, secHeadersIdx, secSSTIdx, secDeletes,
	secTxBloom, secKeybloom, secLogsDict, secFullLogsIdx, secRcptIdx,
}

// End returns the last block in the epoch (inclusive).
func (e *Epoch) End() uint64 { return e.Start + e.Count - 1 }

// Dict returns the epoch's trained zstd dictionary (empty for dict-less
// epochs).
func (e *Epoch) Dict() []byte { return e.sec[secDict] }

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

func openEpochBlob(blob *dist.Blob, hash string) (*Epoch, error) {
	size := blob.Size()
	if size < epochFooterSize {
		return nil, fmt.Errorf("epoch %s: too small", hash)
	}
	ft, err := blob.Slice(size-epochFooterSize, epochFooterSize)
	if err != nil {
		return nil, err
	}
	// v5 is the only supported format. Older files are recognized far enough
	// to say so and no further: there is no upgrade path (user ruling
	// 2026-07-28), the corpus is disposable and gets resynced.
	if !bytes.Equal(ft[0:4], epochMagic[:]) || !bytes.Equal(ft[epochFooterSize-4:], epochMagic[:]) {
		return nil, fmt.Errorf("epoch %s: unrecognized footer, not format v%d (older formats are unsupported: delete the corpus and resync)", hash, epochVersion)
	}
	if v := binary.LittleEndian.Uint32(ft[4:8]); v != epochVersion {
		return nil, fmt.Errorf("epoch %s is format v%d, unsupported: delete the corpus and resync", hash, v)
	}
	e := &Epoch{
		Start:   binary.LittleEndian.Uint64(ft[8:16]),
		Count:   binary.LittleEndian.Uint64(ft[16:24]),
		TxCount: binary.LittleEndian.Uint64(ft[24:32]),
		Hash:    hash,
		blob:    blob,
	}
	copy(e.Prev[:], ft[32:64])
	body := size - epochFooterSize
	for i := 0; i < epochNumSections; i++ {
		off := binary.LittleEndian.Uint64(ft[epochTableOff+i*16:])
		ln := binary.LittleEndian.Uint64(ft[epochTableOff+8+i*16:])
		if off > body || ln > body-off { // overflow-safe bounds check
			return nil, fmt.Errorf("epoch %s: section %d out of bounds", hash, i)
		}
		e.off[i] = [2]uint64{off, ln}
	}
	for _, id := range residentSections {
		if e.sec[id], err = e.read(id, 0, e.off[id][1]); err != nil {
			return nil, fmt.Errorf("epoch %s: section %d: %w", hash, id, err)
		}
	}
	var dicts [][]byte
	if len(e.sec[secDict]) > 0 {
		dicts = append(dicts, e.sec[secDict])
	}
	if len(e.sec[secLogsDict]) > 0 {
		dicts = append(dicts, e.sec[secLogsDict]) // DecodeAll picks by frame dictID
	}
	var decOpts []zstd.DOption
	if len(dicts) > 0 {
		decOpts = append(decOpts, zstd.WithDecoderDicts(dicts...))
	}
	if e.dec, err = zstd.NewReader(nil, decOpts...); err != nil {
		return nil, err
	}

	if e.keyBloom, err = parseBloom(e.sec[secKeybloom]); err != nil {
		e.Close()
		return nil, fmt.Errorf("epoch %s: keybloom: %w", hash, err)
	}
	if e.txBloom, err = parseBloom(e.sec[secTxBloom]); err != nil {
		e.Close()
		return nil, fmt.Errorf("epoch %s: txbloom: %w", hash, err)
	}
	return e, nil
}

func (e *Epoch) Close() {
	if e.dec != nil {
		e.dec.Close()
		e.dec = nil
	}
	if e.blob != nil {
		e.blob.Close()
		e.blob = nil
	}
	for i := range e.sec {
		e.sec[i] = nil
	}
	e.keyBloom, e.txBloom = bloomView{}, bloomView{}
	e.txMu.Lock()
	e.txIdx = nil
	e.txMu.Unlock()
}

// read returns bytes [off, off+n) of one section. The result may alias the
// mapping of a local file: read-only, and invalid after Close.
func (e *Epoch) read(id int, off, n uint64) ([]byte, error) {
	if off > e.off[id][1] || n > e.off[id][1]-off {
		return nil, fmt.Errorf("epoch %d: section %d range [%d,%d) outside %d bytes", e.Start, id, off, off+n, e.off[id][1])
	}
	return e.blob.Slice(e.off[id][0]+off, n)
}

// decodeAll is goroutine-safe: zstd Decoder.DecodeAll is documented
// concurrent-safe ("DecodeAll can be used concurrently").
func (e *Epoch) decodeAll(frame []byte) ([]byte, error) {
	return e.dec.DecodeAll(frame, nil)
}

// framedBlob returns entry (block - Start) from a framed section pair.
func (e *Epoch) framedBlob(dataSec int, index []byte, rel uint64) ([]byte, error) {
	frame := rel / framedGroup
	if int(frame+2)*8 > len(index) {
		return nil, fmt.Errorf("frame %d beyond index", frame)
	}
	lo := binary.LittleEndian.Uint64(index[frame*8:])
	hi := binary.LittleEndian.Uint64(index[(frame+1)*8:])
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
func (e *Epoch) sstBlock(idx []byte, bi, nEntries int) ([]byte, error) {
	lo := binary.LittleEndian.Uint64(idx[bi*sstIdxEntrySize+sortedKeySize+8:])
	hi := e.off[secSST][1]
	if bi+1 < nEntries {
		hi = binary.LittleEndian.Uint64(idx[(bi+1)*sstIdxEntrySize+sortedKeySize+8:])
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
	nEntries := len(idx) / sstIdxEntrySize
	if nEntries == 0 {
		return nil, 0, false, nil
	}
	entry := func(i int) []byte { return idx[i*sstIdxEntrySize : (i+1)*sstIdxEntrySize] }
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
	nEntries := len(idx) / sstIdxEntrySize
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
	for pos := 0; pos+deleteEntrySize <= len(d); pos += deleteEntrySize {
		fn(d[pos:pos+sortedKeySize], binary.BigEndian.Uint64(d[pos+sortedKeySize:]))
	}
}

// txIndex decodes this epoch's tx index, once. The section is NOT resident:
// it is ~6.4 B/tx of Elias-Fano that used to sit in the Go heap for every
// epoch of history, and it is only ever needed after the tx bloom says maybe.
// The bytes arrive through dist (an mmap slice without S3 credentials, a
// fresh copy with them) and readWords copies them into heap words either way,
// so the decoded index outlives the epoch's mapping safely.
//
// ponytail: the load runs under the epoch's own mutex, so concurrent RPC
// goroutines hitting the same cold epoch load it once and the others wait for
// that read. Split into a per-epoch sync.Once + refcount only if a cold
// ranged GET on this path ever shows up as tail latency.
func (e *Epoch) txIndex() (*epochTxIdx, error) {
	e.txMu.Lock()
	defer e.txMu.Unlock()
	if e.txIdx != nil {
		return e.txIdx, nil
	}
	tx, err := e.read(secTxidx, 0, e.off[secTxidx][1])
	if err != nil {
		return nil, err
	}
	if len(tx) < 16 {
		return nil, fmt.Errorf("epoch %d: truncated txidx", e.Start)
	}
	nTx := binary.LittleEndian.Uint64(tx[0:8])
	efL := uint(binary.LittleEndian.Uint32(tx[8:12]))
	blkBits := uint(binary.LittleEndian.Uint32(tx[12:16]))
	pos := 16
	var secs [5][]uint64
	for i := range secs {
		if secs[i], pos, err = readWords(tx, pos); err != nil {
			return nil, fmt.Errorf("epoch %d: txidx: %w", e.Start, err)
		}
	}
	idx := &epochTxIdx{
		ef: &ef{
			n:    int(nTx),
			l:    efL,
			lows: &packed{w: secs[0], bits: efL},
			high: secs[1],
			sel0: secs[2],
			sel1: secs[3],
		},
		blk: &packed{w: secs[4], bits: blkBits},
	}
	idx.ef.highBits = nTx + (uint64(1)<<fpBits)>>efL + 1
	e.txIdx = idx
	e.txLoads.Add(1)
	return idx, nil
}

// dropTxIndex releases the decoded tx index (LRU eviction).
func (e *Epoch) dropTxIndex() {
	e.txMu.Lock()
	e.txIdx = nil
	e.txMu.Unlock()
}

// TxCandidates returns absolute candidate blocks for a fingerprint (a tx
// hash, or since v6 a block hash mapping to its own height), descending.
// Bloom first: an unknown fingerprint never loads the index.
func (e *Epoch) TxCandidates(fp uint64) ([]uint64, error) {
	if !e.MayContainTx(fp) {
		return nil, nil
	}
	idx, err := e.txIndex()
	if err != nil {
		return nil, err
	}
	lo, hi := idx.ef.lookup(fp)
	var out []uint64
	for i := hi - 1; i >= lo; i-- {
		out = append(out, e.Start+idx.blk.get(i))
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
	hdr, err := e.read(secLogidx, 0, 4)
	if err != nil {
		return nil, err
	}
	nAddr := uint64(binary.LittleEndian.Uint32(hdr))
	topicHdr := 4 + nAddr*(20+8)
	if topicHdr+4 > total {
		return nil, fmt.Errorf("epoch %d: logidx truncated", e.Start)
	}
	hdr, err = e.read(secLogidx, topicHdr, 4)
	if err != nil {
		return nil, err
	}
	nTopic := uint64(binary.LittleEndian.Uint32(hdr))
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
	ef, _, err := efUnmarshal(raw, e.Count)
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
	return len(e.sec[secFullLogsIdx]) >= 4 && len(e.sec[secRcptIdx]) >= 4
}

// storedRecord fetches block n's record from a stored-frames section pair.
func (e *Epoch) storedRecord(dataSec int, index []byte, n uint64) ([]byte, bool, error) {
	if len(index) < 4 {
		return nil, false, fmt.Errorf("epoch %d: stored section absent", e.Start)
	}
	nMembers := int(binary.LittleEndian.Uint32(index[0:4]))
	members := index[4 : 4+nMembers*12]
	offs := index[4+nMembers*12:]
	rel := uint32(n - e.Start)
	i := sort.Search(nMembers, func(i int) bool {
		return binary.LittleEndian.Uint32(members[i*12:]) >= rel
	})
	if i == nMembers || binary.LittleEndian.Uint32(members[i*12:]) != rel {
		return nil, false, nil
	}
	frame := binary.LittleEndian.Uint32(members[i*12+4:])
	slot := binary.LittleEndian.Uint32(members[i*12+8:])
	lo := binary.LittleEndian.Uint64(offs[frame*8:])
	hi := binary.LittleEndian.Uint64(offs[(frame+1)*8:])
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
	nEntries := len(idx) / sstIdxEntrySize
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
type EpochSet struct {
	Epochs []*Epoch // ascending by Start

	covered  uint64 // last block of the contiguous prefix from genesis
	gapStart uint64 // expected Start of the first missing epoch (covered+1)
	gapped   bool   // true when at least one epoch above the prefix exists

	txHotMu sync.Mutex
	txHot   []*Epoch // epochs whose tx index is decoded, most recent first
}

// txHotEpochs bounds how many epochs keep a decoded tx index in the heap. A
// tx lookup that gets past the bloom is overwhelmingly recent, and recent
// epochs stay hot by themselves, so a handful is plenty: at mainnet shape
// (10M txs, ~6.4 B/tx) this is ~64MB per hot epoch against the ~7.1GB the
// whole history used to cost resident.
const txHotEpochs = 4

// touchTxIndex records e as the most recently used tx index and drops the
// least recently used beyond txHotEpochs.
func (s *EpochSet) touchTxIndex(e *Epoch) {
	s.txHotMu.Lock()
	defer s.txHotMu.Unlock()
	for i, h := range s.txHot {
		if h == e {
			copy(s.txHot[1:i+1], s.txHot[:i])
			s.txHot[0] = e
			return
		}
	}
	s.txHot = append(s.txHot, nil)
	copy(s.txHot[1:], s.txHot)
	s.txHot[0] = e
	for len(s.txHot) > txHotEpochs {
		s.txHot[len(s.txHot)-1].dropTxIndex()
		s.txHot = s.txHot[:len(s.txHot)-1]
	}
}

// OpenEpochSet opens every epoch the store's data directory indexes. An empty
// set is valid.
func OpenEpochSet(st *dist.Store) (*EpochSet, error) {
	dir := st.Dir()
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	s := &EpochSet{}
	for _, en := range entries {
		// A pre-casfs corpus (whole epoch files sitting in the data dir) is
		// refused by name: there is no migration, only delete and resync.
		if strings.HasPrefix(en.Name(), "epoch_") && strings.HasSuffix(en.Name(), ".epoch") {
			s.Close()
			return nil, fmt.Errorf("%s: %s is a pre-casfs epoch file; epochs are now content-addressed artifacts in %s/cas (no migration: delete the corpus and resync)", dir, en.Name(), dir)
		}
		start, count, ok := ParseEpochMarkerName(en.Name())
		if !ok {
			continue
		}
		hash, err := ReadMarker(dir, en.Name())
		if err != nil {
			s.Close()
			return nil, err
		}
		e, err := OpenEpoch(st, hash)
		if err != nil {
			s.Close()
			return nil, fmt.Errorf("%s: %w", en.Name(), err)
		}
		if e.Start != start || e.Count != count {
			e.Close()
			s.Close()
			return nil, fmt.Errorf("%s names blocks %d..%d but %s covers %d..%d", en.Name(), start, start+count-1, hash, e.Start, e.End())
		}
		s.Epochs = append(s.Epochs, e)
	}
	sort.Slice(s.Epochs, func(i, j int) bool { return s.Epochs[i].Start < s.Epochs[j].Start })
	s.computeCoverage()
	return s, nil
}

// computeCoverage walks the epochs upward from genesis. Anything after the
// first gap stays open for epoch-local reads but is outside coverage.
func (s *EpochSet) computeCoverage() {
	s.covered, s.gapped, s.gapStart = 0, false, 0
	for _, e := range s.Epochs {
		if e.End() <= s.covered {
			continue
		}
		want := s.covered + 1
		if s.covered == 0 && e.Start <= 1 { // block 0 is genesis (no container); sealing starts at 1
			want = e.Start
		}
		if e.Start > want {
			s.gapped, s.gapStart = true, want
			break
		}
		s.covered = e.End()
	}
	if !s.gapped {
		s.gapStart = s.covered + 1
	}
}

// CoveredEnd returns the last block of the contiguous sealed prefix from
// genesis (0 when nothing is covered).
func (s *EpochSet) CoveredEnd() uint64 { return s.covered }

// RequireCovered errors when block n is beyond the contiguous prefix while
// later epochs exist (a hole): state, receipt, and log reads at n would
// silently skip missing history. Bodies/tx-by-hash are epoch-local and may
// still be served above the gap without this check.
func (s *EpochSet) RequireCovered(n uint64) error {
	if s.gapped && n > s.covered {
		return fmt.Errorf("missing epoch epoch_%d: sealed coverage is contiguous only through block %d", s.gapStart, s.covered)
	}
	return nil
}

func (s *EpochSet) Close() {
	for _, e := range s.Epochs {
		e.Close()
	}
	s.Epochs = nil
	s.txHotMu.Lock()
	s.txHot = nil
	s.txHotMu.Unlock()
}

// SealedEnd returns the last sealed block, ok=false for an empty set.
func (s *EpochSet) SealedEnd() (uint64, bool) {
	if len(s.Epochs) == 0 {
		return 0, false
	}
	return s.Epochs[len(s.Epochs)-1].End(), true
}

// Head returns the newest epoch, ok=false for an empty set. Seal chains the
// next epoch's footer onto its hash.
func (s *EpochSet) Head() (*Epoch, bool) {
	if len(s.Epochs) == 0 {
		return nil, false
	}
	return s.Epochs[len(s.Epochs)-1], true
}

// At returns the epoch containing block n.
func (s *EpochSet) At(n uint64) (*Epoch, bool) {
	i := sort.Search(len(s.Epochs), func(i int) bool { return s.Epochs[i].End() >= n })
	if i == len(s.Epochs) || s.Epochs[i].Start > n {
		return nil, false
	}
	return s.Epochs[i], true
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
	for i := len(c.Epochs.Epochs) - 1; i >= 0; i-- {
		e := c.Epochs.Epochs[i]
		if !e.MayContainTx(fp) {
			continue
		}
		blocks, err := e.TxCandidates(fp)
		if err != nil {
			return err
		}
		c.Epochs.touchTxIndex(e)
		for _, blk := range blocks {
			stop, err := visit(blk)
			if stop || err != nil {
				return err
			}
		}
	}
	return nil
}

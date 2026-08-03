package state

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/bits"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"time"

	proposerblock "github.com/ava-labs/avalanchego/vms/proposervm/block"
	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/crypto"
	"github.com/ava-labs/libevm/rlp"
	"github.com/containerman17/epochdb/dist"
	"github.com/klauspost/compress/zstd"
)

// Hash -> block index over the fetch staging segments, one file per
// staging bucket: txidx_NNNNN.idx.
//
//	header (32B): magic "EDBX" | version u32 | nTx u64 | nRecords u64 |
//	              efL u32 | blkBits u32
//	then six length-prefixed little-endian u64 word sections:
//	  EF lows, EF high bitvector, EF select0 samples, EF select1 samples,
//	  bit-packed block-number offsets (height - bucket*SegmentBlocks),
//	  the has-tx bitmap (bit i = block bucket*SegmentBlocks+i has >=1 tx).
//
// Keys are 48-bit fingerprints (top 6 bytes) of TX HASHES AND BLOCK HASHES
// alike, in one Elias-Fano sequence: a block hash maps to that block's own
// height, which is what lets eth_getBlockByHash resolve without a
// floor-to-head map (DESIGN.md ruling 2 of 2026-07-31). Duplicate
// fingerprints map to multiple candidate blocks and the caller verifies
// candidates against the full block, so a collision between the two kinds
// costs one wasted read and nothing else. nRecords is the count of complete
// staging sidecar records consumed at cook time: a bucket whose visible
// record count is unchanged is skipped, growing buckets re-cook. Atomic txs
// live in extdata and are not indexed.
const (
	txidxMagic = "EDBX"
	// Version 2 folds block hashes in and adds the has-tx bitmap. An older
	// file is not read (openTxBucket refuses it) and cookTxBucket rebuilds
	// it: these are local derived files, never distributed.
	txidxVersion = 2
	txidxHdrSize = 32
	fpBits       = 48
	txBlkBits    = 17 // SegmentBlocks-1 fits; constant keeps the format dumb

	// fetch staging sidecar layout (fetch/store.go indexRecSize):
	// height(8 BE) + containerID(32) + blockHash(32) + off(8 BE) + len(4 BE).
	stagingRecSize = 84
)

func txidxName(bucket uint64) string { return fmt.Sprintf("txidx_%05d.idx", bucket) }

// txFingerprint is the top 48 bits of a tx or block hash.
func txFingerprint(h common.Hash) uint64 { return binary.BigEndian.Uint64(h[:8]) >> 16 }

// BlockHashFromHeaderRLP is the eth block hash of a stored RLP header. That
// IS the definition of types.Header.Hash() (keccak256 of the header's RLP),
// so the sealed index and the read path agree without decoding a header.
func BlockHashFromHeaderRLP(headerRLP []byte) common.Hash {
	return common.BytesToHash(crypto.Keccak256(headerRLP))
}

// ---------- words: a u64 array read in place ----------
//
// Every serialized index below is little-endian u64 words, and the READ side
// never copies them onto the Go heap: it indexes the section bytes straight
// out of a dist.View with binary.LittleEndian.Uint64, which needs no
// alignment. That is the whole of DESIGN.md ruling 3's "the Elias-Fano tx
// index becomes a mapped byte view"; since the window cache the view is one
// mapping per 4MB chunk, and a word that straddles a boundary is the one that
// gets copied.

type words struct {
	v   *dist.View
	off int64 // byte offset of word 0 inside v
	n   int   // words
}

func (w words) get(i int) uint64 {
	return binary.LittleEndian.Uint64(w.v.Slice(w.off+int64(i)*8, 8))
}

// sliceWords returns the length-prefixed word section at pos as a view.
func sliceWords(v *dist.View, pos int64) (words, int64, error) {
	if pos+8 > v.Len() {
		return words{}, 0, fmt.Errorf("truncated section length at %d", pos)
	}
	n := int64(binary.LittleEndian.Uint64(v.Slice(pos, 8)))
	pos += 8
	if n < 0 || pos+8*n > v.Len() {
		return words{}, 0, fmt.Errorf("truncated section (%d words) at %d", n, pos)
	}
	return words{v: v, off: pos, n: int(n)}, pos + 8*n, nil
}

// ---------- bit-packed array ----------

// packed reads a bit-packed array out of serialized words.
type packed struct {
	w    words
	bits uint
}

func (p packed) get(i int) uint64 {
	bit := uint64(i) * uint64(p.bits)
	w, off := bit/64, bit%64
	v := p.w.get(int(w)) >> off
	if off+uint64(p.bits) > 64 {
		v |= p.w.get(int(w)+1) << (64 - off)
	}
	return v & (1<<p.bits - 1)
}

// packedBuild is the seal/cook side: materialize in heap, then serialize.
type packedBuild struct {
	w    []uint64
	bits uint
}

func newPacked(n int, bitsPer uint) *packedBuild {
	// +1 word so unaligned tail reads in get never run off the array.
	return &packedBuild{w: make([]uint64, (uint64(n)*uint64(bitsPer)+63)/64+1), bits: bitsPer}
}

func (p *packedBuild) set(i int, v uint64) {
	bit := uint64(i) * uint64(p.bits)
	w, off := bit/64, bit%64
	p.w[w] |= v << off
	if off+uint64(p.bits) > 64 {
		p.w[w+1] |= v >> (64 - off)
	}
}

// ---------- Elias-Fano ----------

// ef encodes a non-decreasing sequence of n values in [0, u). Value i is
// split into l low bits (packed) and a high part encoded as the position of
// the i-th one in a bitvector: pos(i) = (v_i >> l) + i. Duplicate values
// are legal (consecutive ones in the same high bucket), which is exactly
// how duplicate fp48s map to multiple candidate blocks.
//
// This is the READER: every field is a view of the serialized bytes, so an
// open costs a header parse and four slices whatever the entry count.
type ef struct {
	n    int
	l    uint
	lows packed
	high words
	sel0 words // position of every sel0Sample-th zero (bucket skip)
	sel1 words // position of every sel1Sample-th one (for get)
}

// efBuild is the seal/cook side of the same structure.
type efBuild struct {
	n    int
	l    uint
	lows *packedBuild
	high []uint64
	sel0 []uint64
	sel1 []uint64
}

const (
	sel0Sample = 1024
	sel1Sample = 1024
)

func buildEF(vals []uint64, u uint64) *efBuild {
	n := len(vals)
	if n == 0 {
		return &efBuild{lows: newPacked(0, 0)}
	}
	// pick l minimizing total bits (floor(log2(u/n)) +-1 candidates)
	best, bestBits := uint(0), uint64(math.MaxUint64)
	guess := uint(0)
	if q := u / uint64(n); q > 1 {
		guess = uint(bits.Len64(q) - 1)
	}
	for _, l := range []uint{guess, guess + 1} {
		total := uint64(n)*uint64(l) + uint64(n) + (u >> l) + 1
		if total < bestBits {
			best, bestBits = l, total
		}
	}
	l := best
	e := &efBuild{n: n, l: l, lows: newPacked(n, l)}
	highBits := uint64(n) + (u >> l) + 1
	e.high = make([]uint64, (highBits+63)/64)
	for i, v := range vals {
		if l > 0 {
			e.lows.set(i, v&(1<<l-1))
		}
		pos := (v >> l) + uint64(i)
		e.high[pos/64] |= 1 << (pos % 64)
	}
	var ones, zeros uint64
	for w, word := range e.high {
		base := uint64(w) * 64
		limit := highBits - base
		for b := uint64(0); b < 64 && b < limit; b++ {
			if word&(1<<b) != 0 {
				if ones%sel1Sample == 0 {
					e.sel1 = append(e.sel1, base+b)
				}
				ones++
			} else {
				if zeros%sel0Sample == 0 {
					e.sel0 = append(e.sel0, base+b)
				}
				zeros++
			}
		}
	}
	return e
}

// select0 returns the position of the (k+1)-th zero (0-indexed k).
func (e *ef) select0(k uint64) uint64 {
	pos := e.sel0.get(int(k / sel0Sample))
	need := k%sel0Sample + 1
	w := pos / 64
	word := ^e.high.get(int(w)) >> (pos % 64) << (pos % 64)
	for {
		c := uint64(bits.OnesCount64(word))
		if c >= need {
			for i := uint64(1); i < need; i++ {
				word &= word - 1
			}
			return w*64 + uint64(bits.TrailingZeros64(word))
		}
		need -= c
		w++
		word = ^e.high.get(int(w))
	}
}

// select1 returns the position of the (k+1)-th one (0-indexed k).
func (e *ef) select1(k uint64) uint64 {
	pos := e.sel1.get(int(k / sel1Sample))
	need := k%sel1Sample + 1
	w := pos / 64
	word := e.high.get(int(w)) >> (pos % 64) << (pos % 64)
	for {
		c := uint64(bits.OnesCount64(word))
		if c >= need {
			for i := uint64(1); i < need; i++ {
				word &= word - 1
			}
			return w*64 + uint64(bits.TrailingZeros64(word))
		}
		need -= c
		w++
		word = e.high.get(int(w))
	}
}

func (e *ef) get(i int) uint64 {
	hi := e.select1(uint64(i)) - uint64(i)
	if e.l == 0 {
		return hi
	}
	return hi<<e.l | e.lows.get(i)
}

// lookup returns the index range [lo, hi) of entries equal to v.
func (e *ef) lookup(v uint64) (int, int) {
	if e.n == 0 {
		return 0, 0
	}
	b := v >> e.l
	var lo uint64
	if b > 0 {
		lo = e.select0(b-1) - (b - 1)
	}
	hi := e.select0(b) - b
	if lo >= hi {
		return 0, 0
	}
	lv := v & (1<<e.l - 1)
	// entries in a high bucket are sorted by low bits; linear scan, buckets
	// average ~1 entry
	s, eIdx := int(lo), int(lo)
	for i := int(lo); i < int(hi); i++ {
		x := e.lows.get(i)
		if x < lv {
			s, eIdx = i+1, i+1
		} else if x == lv {
			eIdx = i + 1
		} else {
			break
		}
	}
	return s, eIdx
}

// ---------- tx hash extraction ----------

// extractTxHashes pulls tx hashes out of an eth block RLP without decoding:
// block = [header, [tx...], uncles, ...extras]. Legacy txs are RLP lists
// (hash = keccak of the whole element), typed txs are RLP strings (hash =
// keccak of the string content, i.e. type byte || payload).
func extractTxHashes(blockRLP []byte, out []common.Hash) ([]common.Hash, error) {
	k, content, _, err := rlp.Split(blockRLP)
	if err != nil || k != rlp.List {
		return out, fmt.Errorf("block not a list: %v", err)
	}
	_, _, rest, err := rlp.Split(content) // skip header
	if err != nil {
		return out, fmt.Errorf("split header: %w", err)
	}
	k, txs, _, err := rlp.Split(rest)
	if err != nil || k != rlp.List {
		return out, fmt.Errorf("txs not a list: %v", err)
	}
	for len(txs) > 0 {
		k, c, rest, err := rlp.Split(txs)
		if err != nil {
			return out, fmt.Errorf("split tx: %w", err)
		}
		elem := txs[:len(txs)-len(rest)]
		var h common.Hash
		if k == rlp.List {
			copy(h[:], crypto.Keccak256(elem))
		} else {
			copy(h[:], crypto.Keccak256(c))
		}
		out = append(out, h)
		txs = rest
	}
	return out, nil
}

// innerEthBlock unwraps a staging container to the eth block RLP.
func innerEthBlock(raw []byte) []byte {
	if pb, err := proposerblock.ParseWithoutVerification(raw); err == nil {
		return pb.Block()
	}
	return raw // pre-ProposerVM: container IS the eth block RLP
}

// ---------- cook ----------

// CookTxIndex builds/refreshes txidx_NNNNN.idx for every staging bucket in
// dir. Strictly read-only on the staging files (they may be owned by a
// running fetch/exec): torn sidecar tails are simply not consumed yet.
// Buckets whose visible record count is unchanged are skipped.
func CookTxIndex(dir string) error {
	sidecars, err := filepath.Glob(filepath.Join(dir, "index_*.log"))
	if err != nil {
		return err
	}
	sort.Strings(sidecars)

	var (
		wg       sync.WaitGroup
		sem      = make(chan struct{}, runtime.NumCPU())
		mu       sync.Mutex
		firstErr error
		totTx    uint64
		totBytes int64
		cooked   int
	)
	start := time.Now()
	for _, sc := range sidecars {
		var bucket uint64
		if _, err := fmt.Sscanf(filepath.Base(sc), "index_%d.log", &bucket); err != nil {
			continue
		}
		wg.Add(1)
		sem <- struct{}{}
		go func(sc string, bucket uint64) {
			defer wg.Done()
			defer func() { <-sem }()
			nTx, size, did, err := cookTxBucket(dir, sc, bucket)
			mu.Lock()
			defer mu.Unlock()
			if err != nil && firstErr == nil {
				firstErr = fmt.Errorf("bucket %05d: %w", bucket, err)
			}
			if did {
				cooked++
			}
			totTx += nTx
			totBytes += size
		}(sc, bucket)
	}
	wg.Wait()
	if firstErr != nil {
		return firstErr
	}
	fmt.Printf("cook-txindex: %d buckets (%d rebuilt), %d entries, %.1fMB total, %.2f bits/entry, in %s\n",
		len(sidecars), cooked, totTx, float64(totBytes)/1e6,
		float64(totBytes*8)/float64(max(totTx, 1)), time.Since(start).Round(time.Millisecond))
	return nil
}

// visibleRecords counts complete sidecar records: appended sequentially, so
// stop at the first record whose container bytes have not fully landed.
func visibleRecords(idx []byte, arrivalSize uint64) int {
	n := 0
	for i := 0; i+stagingRecSize <= len(idx); i += stagingRecSize {
		off := binary.BigEndian.Uint64(idx[i+72 : i+80])
		ln := binary.BigEndian.Uint32(idx[i+80 : i+84])
		if off+uint64(ln) > arrivalSize {
			break
		}
		n++
	}
	return n
}

func cookTxBucket(dir, sidecarPath string, bucket uint64) (nTx uint64, fileSize int64, rebuilt bool, err error) {
	idx, err := os.ReadFile(sidecarPath)
	if err != nil {
		return 0, 0, false, err
	}
	arr, err := os.Open(filepath.Join(dir, fmt.Sprintf("arrival_%05d.log", bucket)))
	if err != nil {
		return 0, 0, false, err
	}
	defer arr.Close()
	ast, err := arr.Stat()
	if err != nil {
		return 0, 0, false, err
	}
	nRecords := visibleRecords(idx, uint64(ast.Size()))

	outPath := filepath.Join(dir, txidxName(bucket))
	if tb, err := openTxBucket(outPath, bucket); err == nil {
		if tb.nRecords == uint64(nRecords) {
			return uint64(tb.e.n), tb.fileSize, false, nil // unchanged: skip
		}
	}

	dec, err := zstd.NewReader(nil)
	if err != nil {
		return 0, 0, false, err
	}
	defer dec.Close()

	type pair struct{ fp, blk uint64 }
	var (
		pairs   []pair
		frame   []byte
		raw     []byte
		hashBuf []common.Hash
		base    = bucket * BucketBlocks
		hasTx   = make([]uint64, (BucketBlocks+63)/64)
	)
	for i := 0; i < nRecords; i++ {
		rec := idx[i*stagingRecSize : (i+1)*stagingRecSize]
		height := binary.BigEndian.Uint64(rec[0:8])
		off := binary.BigEndian.Uint64(rec[72:80])
		ln := binary.BigEndian.Uint32(rec[80:84])
		// The sidecar already carries the eth block hash, so folding block
		// hashes into the same fingerprint space is free here.
		pairs = append(pairs, pair{
			fp:  txFingerprint(common.BytesToHash(rec[40:72])),
			blk: height - base,
		})
		if cap(frame) < int(ln) {
			frame = make([]byte, ln)
		}
		frame = frame[:ln]
		if _, err := arr.ReadAt(frame, int64(off)); err != nil {
			return 0, 0, false, err
		}
		raw, err = dec.DecodeAll(frame, raw[:0])
		if err != nil {
			return 0, 0, false, fmt.Errorf("zstd height %d: %w", height, err)
		}
		hashBuf, err = extractTxHashes(innerEthBlock(raw), hashBuf[:0])
		if err != nil {
			return 0, 0, false, fmt.Errorf("height %d: %w", height, err)
		}
		if len(hashBuf) > 0 {
			rel := height - base
			hasTx[rel/64] |= 1 << (rel % 64)
		}
		for _, h := range hashBuf {
			pairs = append(pairs, pair{fp: txFingerprint(h), blk: height - base})
		}
	}

	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].fp != pairs[j].fp {
			return pairs[i].fp < pairs[j].fp
		}
		return pairs[i].blk < pairs[j].blk
	})
	fps := make([]uint64, len(pairs))
	for i, p := range pairs {
		fps[i] = p.fp
	}
	e := buildEF(fps, 1<<fpBits)
	blk := newPacked(len(pairs), txBlkBits)
	for i, p := range pairs {
		blk.set(i, p.blk)
	}

	size, err := writeTxIdx(outPath, uint64(len(pairs)), uint64(nRecords), e, blk, hasTx)
	return uint64(len(pairs)), size, true, err
}

func writeWords(buf []byte, ws []uint64) []byte {
	buf = binary.LittleEndian.AppendUint64(buf, uint64(len(ws)))
	for _, w := range ws {
		buf = binary.LittleEndian.AppendUint64(buf, w)
	}
	return buf
}

func writeTxIdx(path string, nTx, nRecords uint64, e *efBuild, blk *packedBuild, hasTx []uint64) (int64, error) {
	buf := make([]byte, 0, txidxHdrSize+8*(len(e.lows.w)+len(e.high)+len(e.sel0)+len(e.sel1)+len(blk.w)+len(hasTx)+6))
	buf = append(buf, txidxMagic...)
	buf = binary.LittleEndian.AppendUint32(buf, txidxVersion)
	buf = binary.LittleEndian.AppendUint64(buf, nTx)
	buf = binary.LittleEndian.AppendUint64(buf, nRecords)
	buf = binary.LittleEndian.AppendUint32(buf, uint32(e.l))
	buf = binary.LittleEndian.AppendUint32(buf, txBlkBits)
	buf = writeWords(buf, e.lows.w)
	buf = writeWords(buf, e.high)
	buf = writeWords(buf, e.sel0)
	buf = writeWords(buf, e.sel1)
	buf = writeWords(buf, blk.w)
	buf = writeWords(buf, hasTx)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, buf, 0o644); err != nil {
		return 0, err
	}
	return int64(len(buf)), os.Rename(tmp, path)
}

// ---------- lookup ----------

type txBucket struct {
	bucket   uint64
	nRecords uint64
	fileSize int64
	e        *ef
	blk      packed
	hasTx    words // bit i = block bucket*BucketBlocks+i has >=1 regular tx
}

// TxIndex answers hash -> candidate-block queries (tx hashes and block
// hashes share the fingerprint space) over every cooked bucket. Fully
// resident (the whole index is ~a few bits per entry); goroutine-safe,
// everything is immutable after open.
type TxIndex struct {
	buckets []*txBucket
	nTx     uint64
}

func OpenTxIndex(dir string) (*TxIndex, error) {
	files, err := filepath.Glob(filepath.Join(dir, "txidx_*.idx"))
	if err != nil {
		return nil, err
	}
	sort.Strings(files)
	t := &TxIndex{}
	for _, f := range files {
		var bucket uint64
		if _, err := fmt.Sscanf(filepath.Base(f), "txidx_%d.idx", &bucket); err != nil {
			continue
		}
		tb, err := openTxBucket(f, bucket)
		if err != nil {
			return nil, fmt.Errorf("open %s: %w", filepath.Base(f), err)
		}
		t.buckets = append(t.buckets, tb)
		t.nTx += uint64(tb.e.n)
	}
	return t, nil
}

// NumEntries returns the total indexed fingerprint count (txs plus one per
// block for its own block hash).
func (t *TxIndex) NumEntries() uint64 { return t.nTx }

func openTxBucket(path string, bucket uint64) (*txBucket, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if len(b) < txidxHdrSize || string(b[0:4]) != txidxMagic || binary.LittleEndian.Uint32(b[4:8]) != txidxVersion {
		return nil, fmt.Errorf("bad header")
	}
	nTx := binary.LittleEndian.Uint64(b[8:16])
	nRecords := binary.LittleEndian.Uint64(b[16:24])
	l := uint(binary.LittleEndian.Uint32(b[24:28]))
	blkBits := uint(binary.LittleEndian.Uint32(b[28:32]))

	// The local bucket index is a whole file already on the heap, so its view
	// is one part and nothing in it can straddle.
	v := dist.ViewOf(b)
	pos := int64(txidxHdrSize)
	var sections [6]words
	for i := range sections {
		if sections[i], pos, err = sliceWords(v, pos); err != nil {
			return nil, err
		}
	}
	e := &ef{
		n:    int(nTx),
		l:    l,
		lows: packed{w: sections[0], bits: l},
		high: sections[1],
		sel0: sections[2],
		sel1: sections[3],
	}
	return &txBucket{
		bucket:   bucket,
		nRecords: nRecords,
		fileSize: int64(len(b)),
		e:        e,
		blk:      packed{w: sections[4], bits: blkBits},
		hasTx:    sections[5],
	}, nil
}

// BlocksWithTxs returns a bitmap (indexed by height - bucket*BucketBlocks)
// of blocks in the bucket that contain at least one regular tx. Blocks
// without regular txs cannot emit logs, so the log backfill skips them
// without touching the container. It reads the cooked has-tx bitmap rather
// than the fingerprint index, because since v2 every block contributes its
// own block hash to that index and so appears in it whether it has txs or
// not.
func (t *TxIndex) BlocksWithTxs(bucket uint64) []bool {
	out := make([]bool, BucketBlocks)
	for _, tb := range t.buckets {
		if tb.bucket != bucket {
			continue
		}
		for i := range out {
			if w := i / 64; w < tb.hasTx.n && tb.hasTx.get(w)&(1<<(i%64)) != 0 {
				out[i] = true
			}
		}
	}
	return out
}

// Candidates returns every block height that may hold this hash (48-bit
// fingerprint match; the caller must verify against the full block).
// Duplicate fingerprints yield multiple candidates.
func (t *TxIndex) Candidates(hash common.Hash) []uint64 {
	return t.candidatesFP(txFingerprint(hash))
}

func (t *TxIndex) candidatesFP(fp uint64) []uint64 {
	var out []uint64
	for _, tb := range t.buckets {
		lo, hi := tb.e.lookup(fp)
		for i := lo; i < hi; i++ {
			out = append(out, tb.bucket*BucketBlocks+tb.blk.get(i))
		}
	}
	return out
}

// WalkCandidates calls fn for every candidate block of a tx OR block hash,
// newest first, stopping when fn returns true (rpc.TxCandidateSource: the raw
// index alone is the whole source on a node with no sealed epochs).
func (t *TxIndex) WalkCandidates(hash common.Hash, fn func(blk uint64) (bool, error)) error {
	_, err := t.walkCandidatesFP(txFingerprint(hash), fn)
	return err
}

func (t *TxIndex) walkCandidatesFP(fp uint64, fn func(blk uint64) (bool, error)) (bool, error) {
	for i := len(t.buckets) - 1; i >= 0; i-- {
		tb := t.buckets[i]
		lo, hi := tb.e.lookup(fp)
		for j := hi - 1; j >= lo; j-- {
			stop, err := fn(tb.bucket*BucketBlocks + tb.blk.get(j))
			if stop || err != nil {
				return stop, err
			}
		}
	}
	return false, nil
}

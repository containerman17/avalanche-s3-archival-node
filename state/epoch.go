package state

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"math/bits"
	"os"
	"os/exec"
	"path/filepath"
	"sort"

	"github.com/cespare/xxhash/v2"
	"github.com/klauspost/compress/zstd"
)

// trainDictCLI trains a zstd dictionary with the system zstd CLI (pinned:
// v1.5.7; rebuilds on another version are not guaranteed bit-identical,
// decompression always is). Returns nil when training is impossible
// (missing binary or a corpus too small to train on): the epoch is then
// written dict-less, which stays valid and deterministic.
func trainDictCLI(samples [][]byte, dictID uint32) []byte {
	tmp, err := os.MkdirTemp("", "epochdict")
	if err != nil {
		return nil
	}
	defer os.RemoveAll(tmp)
	args := []string{"--train", "-q", "-o", filepath.Join(tmp, "dict.bin"),
		fmt.Sprintf("--maxdict=%d", dictTargetSize),
		fmt.Sprintf("--dictID=%d", dictID)}
	for i, s := range samples {
		p := filepath.Join(tmp, fmt.Sprintf("s%06d", i))
		if err := os.WriteFile(p, s, 0o644); err != nil {
			return nil
		}
		args = append(args, p)
	}
	if out, err := exec.Command("zstd", args...).CombinedOutput(); err != nil {
		log.Printf("epoch: dict training skipped (%v: %s), sealing dict-less", err, bytes.TrimSpace(out))
		return nil
	}
	d, err := os.ReadFile(filepath.Join(tmp, "dict.bin"))
	if err != nil {
		return nil
	}
	return d
}

// Sealed epoch file: epoch_<startblock>_<blockcount>.epoch. Immutable,
// self-described, bit-identical when rebuilt from the same chain content.
// Sections in write order, fixed-size footer last (reader seeks from EOF):
//
//	dict       zstd dictionary trained AT SEAL TIME on this epoch's raw
//	           containers (klauspost dict.BuildZstdDict, pinned; determinism
//	           via a start-block-derived dictionary ID)
//	bodies     zstd frames of framedGroup containers, epoch dict
//	bodiesIdx  u64 LE frame offsets (nFrames+1, end sentinel)
//	headers    RLP headers, same framing
//	headersIdx u64 LE frame offsets
//	sst        post-image rows sorted by (key53, block), values inline,
//	           dict-compressed blocks of ~sstBlockTarget raw bytes
//	sstIdx     sparse index: 69B entries key53|firstBlock u64|off u64
//	deletes    raw sorted 61B rows key53|block u64 of account-delete
//	           records (rare; feeds the open-time delete map without
//	           decompressing the SST)
//	txidx      EF fp48 + bit-packed epoch-relative block numbers
//	logidx     posting lists addr->EF blocklist, topic->EF blocklist
//	keybloom   bloom over keys written in this epoch (bloomBitsPerKey)
//
// All hardcoded parameters below are pre-freeze tunables (user directive:
// constants in code, no config).
const (
	// EpochTxs is the epoch boundary: an epoch seals at the first block
	// whose cumulative tx count reaches it. PRE-FREEZE TUNABLE; sized for
	// the mainnet 0-100k measurement corpus (93,870 regular txs measured
	// 2026-07-18) so it yields 3 sealed epochs plus a raw tail. The
	// full-mainnet target per DESIGN.md is 10M.
	EpochTxs = 25_000

	framedGroup     = 16        // containers/headers per zstd frame (07-17: 2.24x, 34us access)
	dictTargetSize  = 512 << 10 // 07-17 experiment optimum
	dictMaxSamples  = 4096      // training sample cap, keeps seal-time bounded
	sstBlockTarget  = 64 << 10  // raw bytes per compressed SST block
	bloomBitsPerKey = 10
	bloomHashes     = 7

	epochVersion     = 1
	epochNumSections = 11
	epochFooterSize  = 4 + 4 + 8 + 8 + 8 + epochNumSections*16 + 4 // magics + version + start/count/txs + table
)

// Section indexes into the footer table.
const (
	secDict = iota
	secBodies
	secBodiesIdx
	secHeaders
	secHeadersIdx
	secSST
	secSSTIdx
	secDeletes
	secTxidx
	secLogidx
	secKeybloom
)

var epochMagic = [4]byte{'E', 'P', 'O', 'C'}

func EpochFileName(start, count uint64) string {
	return fmt.Sprintf("epoch_%d_%d.epoch", start, count)
}

// ParseEpochFileName returns (start, count, ok).
func ParseEpochFileName(name string) (uint64, uint64, bool) {
	var start, count uint64
	if n, err := fmt.Sscanf(name, "epoch_%d_%d.epoch", &start, &count); n != 2 || err != nil {
		return 0, 0, false
	}
	return start, count, true
}

// ---------- builder input ----------

// StateRow is one post-image write: key53 = kind+addr+slot (cook.go
// layout), Value nil for explicit deletes/zero writes.
type StateRow struct {
	Key   [sortedKeySize]byte
	Block uint64
	Value []byte
	Seq   int // frame order within the block, last write wins
}

// LogRec is one block's captured log tuple record (decoded logs frame).
type LogRec struct {
	Block  uint64
	Addrs  [][20]byte
	Topics [][32]byte
}

// EpochInput carries everything for one epoch, blocks
// [Start, Start+len(Containers)).
type EpochInput struct {
	Start      uint64
	Containers [][]byte // raw staging containers, one per block
	Headers    [][]byte // RLP headers, one per block
	StateRows  []StateRow
	TxHashes   map[uint64][][32]byte // block -> tx hashes (fp48 source)
	Logs       []LogRec
	TxCount    uint64
}

// ---------- builder ----------

// BuildEpoch assembles and atomically writes one epoch file into dir.
// Returns the final path.
func BuildEpoch(dir string, in *EpochInput) (string, error) {
	count := uint64(len(in.Containers))
	if count == 0 || len(in.Headers) != len(in.Containers) {
		return "", fmt.Errorf("epoch build: %d containers, %d headers", len(in.Containers), len(in.Headers))
	}

	// Deterministic dictionary: sample containers evenly, fixed dict ID,
	// zstd CLI trainer (pinned v1.5.7). klauspost's dict.BuildZstdDict is
	// internally map-iteration nondeterministic (verified 2026-07-18) and
	// would break bit-identical epoch files.
	samples := in.Containers
	if len(samples) > dictMaxSamples {
		step := len(samples) / dictMaxSamples
		sub := make([][]byte, 0, dictMaxSamples)
		for i := 0; i < len(samples); i += step {
			sub = append(sub, samples[i])
		}
		samples = sub
	}
	epochDict := trainDictCLI(samples, uint32(in.Start%0xfffffffe)+1)

	encOpts := []zstd.EOption{zstd.WithEncoderLevel(zstd.SpeedBestCompression)}
	if len(epochDict) > 0 {
		encOpts = append(encOpts, zstd.WithEncoderDict(epochDict))
	}
	enc, err := zstd.NewWriter(nil, encOpts...)
	if err != nil {
		return "", err
	}
	defer enc.Close()

	var (
		buf     bytes.Buffer
		offsets [epochNumSections][2]uint64 // off, len per section
	)
	section := func(id int, b []byte) {
		offsets[id][0] = uint64(buf.Len())
		offsets[id][1] = uint64(len(b))
		buf.Write(b)
	}

	section(secDict, epochDict)

	bodies, bodiesIdx := buildFramed(enc, in.Containers)
	section(secBodies, bodies)
	section(secBodiesIdx, bodiesIdx)

	headers, headersIdx := buildFramed(enc, in.Headers)
	section(secHeaders, headers)
	section(secHeadersIdx, headersIdx)

	sst, sstIdx, deletes, keys := buildSST(enc, in.StateRows)
	section(secSST, sst)
	section(secSSTIdx, sstIdx)
	section(secDeletes, deletes)

	section(secTxidx, buildEpochTxidx(in, count))
	section(secLogidx, buildLogidx(in.Logs, in.Start, count))
	section(secKeybloom, buildBloom(keys))

	// Footer.
	var ft [epochFooterSize]byte
	copy(ft[0:4], epochMagic[:])
	binary.LittleEndian.PutUint32(ft[4:8], epochVersion)
	binary.LittleEndian.PutUint64(ft[8:16], in.Start)
	binary.LittleEndian.PutUint64(ft[16:24], count)
	binary.LittleEndian.PutUint64(ft[24:32], in.TxCount)
	for i := 0; i < epochNumSections; i++ {
		binary.LittleEndian.PutUint64(ft[32+i*16:], offsets[i][0])
		binary.LittleEndian.PutUint64(ft[40+i*16:], offsets[i][1])
	}
	copy(ft[epochFooterSize-4:], epochMagic[:])
	buf.Write(ft[:])

	path := filepath.Join(dir, EpochFileName(in.Start, count))
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, buf.Bytes(), 0o644); err != nil {
		return "", err
	}
	return path, os.Rename(tmp, path)
}

// buildFramed packs blobs into zstd frames of framedGroup entries each:
// frame payload = per-blob uvarint length + bytes. Index = u64 LE offsets,
// nFrames+1 (end sentinel).
func buildFramed(enc *zstd.Encoder, blobs [][]byte) (data, index []byte) {
	var (
		out     []byte
		offs    []byte
		payload []byte
	)
	offs = binary.LittleEndian.AppendUint64(offs, 0)
	for i := 0; i < len(blobs); i += framedGroup {
		payload = payload[:0]
		for j := i; j < i+framedGroup && j < len(blobs); j++ {
			payload = binary.AppendUvarint(payload, uint64(len(blobs[j])))
			payload = append(payload, blobs[j]...)
		}
		out = enc.EncodeAll(payload, out)
		offs = binary.LittleEndian.AppendUint64(offs, uint64(len(out)))
	}
	return out, offs
}

const (
	sstIdxEntrySize = sortedKeySize + 8 + 8 // key, first block, section offset
	deleteEntrySize = sortedKeySize + 8
)

// buildSST sorts and dedupes the epoch's post-image rows, packs them into
// dict-compressed blocks, and returns the sst data, sparse index, the raw
// account-delete rows, and every unique written key (bloom input).
// Row wire format inside a block: key53 | block u64 BE | uvarint vlen | value.
func buildSST(enc *zstd.Encoder, rows []StateRow) (sst, sstIdx, deletes []byte, keys [][]byte) {
	sort.Slice(rows, func(i, j int) bool {
		if c := bytes.Compare(rows[i].Key[:], rows[j].Key[:]); c != 0 {
			return c < 0
		}
		if rows[i].Block != rows[j].Block {
			return rows[i].Block < rows[j].Block
		}
		return rows[i].Seq < rows[j].Seq
	})

	var (
		raw      []byte
		firstKey []byte
		firstBlk uint64
		lastKey  []byte
	)
	flush := func() {
		if len(raw) == 0 {
			return
		}
		sstIdx = append(sstIdx, firstKey...)
		sstIdx = binary.BigEndian.AppendUint64(sstIdx, firstBlk)
		sstIdx = binary.LittleEndian.AppendUint64(sstIdx, uint64(len(sst)))
		sst = enc.EncodeAll(raw, sst)
		raw = raw[:0]
		firstKey = nil
	}
	for i := range rows {
		r := &rows[i]
		// last write of the same (key, block) wins (post-image semantics)
		if i+1 < len(rows) && rows[i+1].Block == r.Block && rows[i+1].Key == r.Key {
			continue
		}
		if lastKey == nil || !bytes.Equal(lastKey, r.Key[:]) {
			k := append([]byte(nil), r.Key[:]...)
			keys = append(keys, k)
			lastKey = k
		}
		if r.Key[0] == recKindAccount && len(r.Value) == 0 {
			deletes = append(deletes, r.Key[:]...)
			deletes = binary.BigEndian.AppendUint64(deletes, r.Block)
		}
		if firstKey == nil {
			firstKey = append([]byte(nil), r.Key[:]...)
			firstBlk = r.Block
		}
		raw = append(raw, r.Key[:]...)
		raw = binary.BigEndian.AppendUint64(raw, r.Block)
		raw = binary.AppendUvarint(raw, uint64(len(r.Value)))
		raw = append(raw, r.Value...)
		if len(raw) >= sstBlockTarget {
			flush()
		}
	}
	flush()
	return sst, sstIdx, deletes, keys
}

// buildEpochTxidx encodes the epoch's tx fingerprints exactly like the raw
// txidx buckets (ef + packed blocks), with epoch-relative block numbers.
// Layout: nTx u64 | efL u32 | blkBits u32 | 5 length-prefixed word sections.
func buildEpochTxidx(in *EpochInput, count uint64) []byte {
	type pair struct{ fp, blk uint64 }
	var pairs []pair
	for blk, hashes := range in.TxHashes {
		for _, h := range hashes {
			pairs = append(pairs, pair{
				fp:  binary.BigEndian.Uint64(h[:8]) >> 16,
				blk: blk - in.Start,
			})
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
	blkBits := uint(bits.Len64(count - 1))
	if blkBits == 0 {
		blkBits = 1
	}
	blk := newPacked(len(pairs), blkBits)
	for i, p := range pairs {
		blk.set(i, p.blk)
	}
	out := binary.LittleEndian.AppendUint64(nil, uint64(len(pairs)))
	out = binary.LittleEndian.AppendUint32(out, uint32(e.l))
	out = binary.LittleEndian.AppendUint32(out, uint32(blkBits))
	out = writeWords(out, e.lows.w)
	out = writeWords(out, e.high)
	out = writeWords(out, e.sel0)
	out = writeWords(out, e.sel1)
	out = writeWords(out, blk.w)
	return out
}

// buildLogidx encodes position-agnostic posting lists. Layout:
//
//	nAddr u32 | addr entries (addr20 | listOff u64) |
//	nTopic u32 | topic entries (topic32 | listOff u64) |
//	lists blob (per list: EF over epoch-relative blocks, efMarshal)
//
// Entries sorted by key; listOff is relative to the lists blob.
func buildLogidx(logs []LogRec, start, count uint64) []byte {
	addrBlocks := map[[20]byte][]uint64{}
	topicBlocks := map[[32]byte][]uint64{}
	for _, lr := range logs {
		rel := lr.Block - start
		for _, a := range lr.Addrs {
			addrBlocks[a] = append(addrBlocks[a], rel)
		}
		for _, t := range lr.Topics {
			topicBlocks[t] = append(topicBlocks[t], rel)
		}
	}
	addrs := make([][20]byte, 0, len(addrBlocks))
	for a := range addrBlocks {
		addrs = append(addrs, a)
	}
	sort.Slice(addrs, func(i, j int) bool { return bytes.Compare(addrs[i][:], addrs[j][:]) < 0 })
	topics := make([][32]byte, 0, len(topicBlocks))
	for t := range topicBlocks {
		topics = append(topics, t)
	}
	sort.Slice(topics, func(i, j int) bool { return bytes.Compare(topics[i][:], topics[j][:]) < 0 })

	var lists []byte
	marshalList := func(blocks []uint64) uint64 {
		off := uint64(len(lists))
		sort.Slice(blocks, func(i, j int) bool { return blocks[i] < blocks[j] }) // EF needs non-decreasing
		lists = append(lists, efMarshal(buildEF(blocks, count))...)
		return off
	}
	var out []byte
	out = binary.LittleEndian.AppendUint32(out, uint32(len(addrs)))
	addrTable := make([]byte, 0, len(addrs)*(20+8))
	for _, a := range addrs {
		addrTable = append(addrTable, a[:]...)
		addrTable = binary.LittleEndian.AppendUint64(addrTable, marshalList(addrBlocks[a]))
	}
	out = append(out, addrTable...)
	out = binary.LittleEndian.AppendUint32(out, uint32(len(topics)))
	for _, t := range topics {
		out = append(out, t[:]...)
		out = binary.LittleEndian.AppendUint64(out, marshalList(topicBlocks[t]))
	}
	return append(out, lists...)
}

// efMarshal serializes an ef as: n u64 | l u32 | 4 length-prefixed word
// sections (lows, high, sel0, sel1).
func efMarshal(e *ef) []byte {
	out := binary.LittleEndian.AppendUint64(nil, uint64(e.n))
	out = binary.LittleEndian.AppendUint32(out, uint32(e.l))
	out = writeWords(out, e.lows.w)
	out = writeWords(out, e.high)
	out = writeWords(out, e.sel0)
	out = writeWords(out, e.sel1)
	return out
}

// efUnmarshal is the inverse; u is the universe the ef was built with.
func efUnmarshal(b []byte, u uint64) (*ef, int, error) {
	if len(b) < 12 {
		return nil, 0, fmt.Errorf("ef: truncated header")
	}
	n := binary.LittleEndian.Uint64(b[0:8])
	l := uint(binary.LittleEndian.Uint32(b[8:12]))
	pos := 12
	var secs [4][]uint64
	var err error
	for i := range secs {
		if secs[i], pos, err = readWords(b, pos); err != nil {
			return nil, 0, err
		}
	}
	e := &ef{
		n:    int(n),
		l:    l,
		lows: &packed{w: secs[0], bits: l},
		high: secs[1],
		sel0: secs[2],
		sel1: secs[3],
	}
	e.highBits = n + (u >> l) + 1
	return e, pos, nil
}

// values returns the decoded sequence (posting-list read path).
func (e *ef) values() []uint64 {
	out := make([]uint64, e.n)
	for i := range out {
		out[i] = e.get(i)
	}
	return out
}

// ---------- bloom ----------

// buildBloom: m = 10 bits/key rounded to words, k = bloomHashes,
// double hashing over xxhash. Layout: m u64 | k u32 | pad u32 | words.
func buildBloom(keys [][]byte) []byte {
	m := uint64(len(keys)) * bloomBitsPerKey
	if m < 64 {
		m = 64
	}
	m = (m + 63) / 64 * 64
	words := make([]uint64, m/64)
	for _, k := range keys {
		h1, h2 := bloomHash(k)
		for i := uint64(0); i < bloomHashes; i++ {
			bit := (h1 + i*h2) % m
			words[bit/64] |= 1 << (bit % 64)
		}
	}
	out := binary.LittleEndian.AppendUint64(nil, m)
	out = binary.LittleEndian.AppendUint32(out, bloomHashes)
	out = binary.LittleEndian.AppendUint32(out, 0)
	for _, w := range words {
		out = binary.LittleEndian.AppendUint64(out, w)
	}
	return out
}

func bloomHash(key []byte) (h1, h2 uint64) {
	h := xxhash.Sum64(key)
	h1 = h
	h2 = h>>33 | h<<31
	h2 |= 1
	return
}

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

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/rlp"
	"github.com/cespare/xxhash/v2"
	"github.com/containerman17/epochdb/dist"
	"github.com/klauspost/compress/zstd"
)

// trainDictCLI trains a zstd dictionary with the system zstd CLI (pinned:
// v1.5.7; rebuilds on another version are not guaranteed bit-identical,
// decompression always is). Returns nil when training is impossible
// (missing binary or a corpus too small to train on): the epoch is then
// written dict-less, which stays valid and deterministic.
func trainDictCLI(samples [][]byte, dictID uint32, maxDict int) []byte {
	tmp, err := os.MkdirTemp("", "epochdict")
	if err != nil {
		return nil
	}
	defer os.RemoveAll(tmp)
	args := []string{"--train", "-q", "-o", filepath.Join(tmp, "dict.bin"),
		fmt.Sprintf("--maxdict=%d", maxDict),
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

// Sealed epoch artifact, named by its own hex sha256 (the local index marker
// epoch_<startblock>_<blockcount>.cas carries that name). Immutable,
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
//	           dict-compressed blocks of ~sstBlockTarget raw bytes; v3 also
//	           carries the epoch's contract code as 'c' | hash rows here
//	sstIdx     sparse index: 69B entries key53|firstBlock u64|off u64
//	deletes    raw sorted 61B rows key53|block u64 of account-delete
//	           records (rare; feeds the open-time delete map without
//	           decompressing the SST)
//	txidx      EF fp48 + bit-packed epoch-relative block numbers
//	txbloom    bloom over exactly the fp48s in txidx (txBloomBitsPerTx), the
//	           reject filter that keeps txidx off the resident set
//	logidx     posting lists addr->EF blocklist, topic->EF blocklist
//	keybloom   bloom over keys written in this epoch (bloomBitsPerKey)
//
// The footer carries the HASH CHAIN: epoch K's footer embeds sha256 of epoch
// K-1's whole file, and the first epoch of a chain embeds the chain root,
// sha256 of that chain's genesis config (dist.ChainRoot). Prev-hash is chain
// content like everything else here, so two honest builders still emit
// identical bytes, and one head hash authenticates every epoch below it.
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
	// A key nothing ever wrote (an unset storage slot, the common case in
	// any eth_call) has NO early exit: History.searchAboveFloor probes
	// every epoch, so the expected number of wasted SST block reads per
	// miss is epochCount x the per-epoch false-positive rate. At the
	// original 10 bits / k=7 that was 0.98 on a 120-epoch mainnet, i.e.
	// EVERY unset-slot read paid a real block read, and under casfs that
	// read can be a cold 4MB chunk GET. Measured 2026-07-30, 40M probes
	// per setting, mainnet shape (120 epochs, 1.32B total entries):
	//
	//	bits/key  k   per-epoch FP   all blooms   wasted reads/miss
	//	      10   7      0.8172%       1.65 GB          0.98
	//	      16  11      0.0449%       2.64 GB          0.054
	//	      20  14      0.0069%       3.30 GB          0.0083
	//	      24  17      0.0009%       3.96 GB          0.0011
	//
	// 20/14 is the pick: 118x fewer wasted reads for 1.65 GB more resident
	// bloom and the same 1.65 GB spread across the whole published corpus,
	// which is noise against ~500 GB of history. Probe cost goes 60ns to
	// 99ns, irrelevant beside the block read it avoids. This also removes
	// the need for a separate global union filter over all keys ever.
	// Readers take m and k from the file, so only newly written epochs
	// change shape.
	bloomBitsPerKey = 20
	bloomHashes     = 14

	// The TX bloom (v5) is the reject filter in front of the per-epoch tx
	// index. Without it every eth_getTransactionByHash for a hash this node
	// does not know (a tx still in the mempool, i.e. every wallet polling
	// its own pending tx) walks every epoch's Elias-Fano index, which is why
	// that index used to be resident: 6.35 B/tx, ~7.1GB at mainnet scale.
	// 16 bits/tx with the matching optimal k = round(16*ln2) = 11 gives
	// 0.045% per epoch, i.e. ~0.05 wasted index loads per pending-tx poll on
	// a 120-epoch mainnet, for 2 B/tx of mmap'd page cache. The fingerprints
	// are exactly the fp48s buildEpochTxidx encodes, from the same slice.
	txBloomBitsPerTx = 16
	txBloomHashes    = 11

	// Format v5 is the ONLY supported format: stored-logs sections (v2,
	// 2026-07-20), contract code as 'c' rows in the SST (v3, 2026-07-28),
	// the HASH-CHAIN footer field (v4, 2026-07-30) that makes one head hash
	// authenticate all of a chain's history, and the tx-fingerprint bloom
	// (v5, 2026-07-30). There is no upgrade path: OpenEpoch refuses an older
	// file and the corpus is rebuilt by a fresh sync (user ruling
	// 2026-07-28).
	epochVersion     = 5
	epochNumSections = 17
	epochTableOff    = 4 + 4 + 8 + 8 + 8 + 32                  // magic, version, start, count, txs, prev hash
	epochFooterSize  = epochTableOff + epochNumSections*16 + 4 // + table + trailing magic

	logsDictTarget = 128 << 10 // dedicated logs dict (measured better than container dict)
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
	// v2 additions: full stored logs + per-tx receipt fields.
	secLogsDict
	secFullLogs
	secFullLogsIdx
	secRcpt
	secRcptIdx
	// v5 addition: the tx-fingerprint bloom.
	secTxBloom
)

var epochMagic = [4]byte{'E', 'P', 'O', 'C'}

// ---------- builder input ----------

// epochCodeKey keys a contract code blob by its hash inside the epoch's SST
// keyspace: 'c' | hash32 | 20 zero bytes. Since 'a' < 'c' < 's', code rows
// share the one sorted keyspace, sparse index and bloom with the state rows,
// the same trick state/base.go uses. The kind byte is free here because the
// write capture's code-USE records ('c' | addr | hash) are dropped at cook
// and seal time and never reach an SST.
func epochCodeKey(hash common.Hash) (k [sortedKeySize]byte) {
	k[0] = recKindCodeUse
	copy(k[1:33], hash[:])
	return
}

// accountCodeHash pulls field 3 (CodeHash) out of a captured account RLP
// without decoding the struct: libevm appends registered extras after the
// four core fields, so the code hash is neither the last field nor at a
// fixed offset. ok=false for a delete row or an unparsable value.
func accountCodeHash(val []byte) (common.Hash, bool) {
	var h common.Hash
	rest, _, err := rlp.SplitList(val)
	if err != nil {
		return h, false
	}
	for i := 0; i < 4; i++ {
		_, content, r, err := rlp.Split(rest)
		if err != nil {
			return h, false
		}
		if i == 3 {
			if len(content) != common.HashLength {
				return h, false
			}
			return common.BytesToHash(content), true
		}
		rest = r
	}
	return h, false
}

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

	// Prev is the hash-chain link: sha256 of epoch K-1's file, or the chain
	// root (sha256 of the genesis config) for a chain's first epoch.
	Prev [32]byte

	// Stored-logs sections (v2, unconditional): per-block encoded records
	// derived by re-execution (rpc.EncodeStoredLogs/EncodeStoredReceipts).
	// nil maps = seal without the sections (unit tests only; production
	// sealing always derives them). A non-nil empty map is valid (epoch
	// genuinely has no logs) and still marks the sections present.
	FullLogs map[uint64][]byte // log-bearing block -> logs record
	RcptRecs map[uint64][]byte // tx-bearing block -> receipt-fields record

	// Code (v3) is every contract code blob referenced by an account row
	// this epoch writes, keyed by hash. THE PLACEMENT RULE, user decision
	// 2026-07-28 after measuring both candidates on the mainnet 0-10M
	// corpus: an epoch carries the code of every account it wrote, NOT
	// only of the code first deployed inside it. Rule (a), first-deploying
	// epoch, would cost 288MB over the 7 production epochs; this rule
	// costs 478MB, i.e. +190MB (+0.6%) on a 32.6GB corpus, which buys the
	// invariant that WHICHEVER epoch answers an account read also carries
	// that account's code, derived from the epoch's own account rows (no
	// earlier epoch needed).
	//
	// DETERMINISM: the set is a pure function of this epoch's own post-image
	// rows (chain content), the blobs are content-addressed, and buildSST
	// sorts them by key, so map iteration order, wall time and local file
	// layout cannot reach the bytes.
	Code map[common.Hash][]byte
}

// HasStoredLogInputs reports whether this input seals the v2 sections.
func (in *EpochInput) HasStoredLogInputs() bool { return in.FullLogs != nil && in.RcptRecs != nil }

// codeCursor emits the v3 code rows in key order as an SST write walks past
// them ('c' sorts between the 'a' and 's' rows, so they form one contiguous
// run). Kept separate from the state rows because a production epoch holds
// ~100M of those in RAM and merging into that slice would reallocate it.
// The row's block number is the epoch start: a blob has no meaningful write
// height (the same bytes can be deployed by many blocks) and the code lookup
// ignores it, so the epoch's own first block is the deterministic choice.
type codeCursor struct {
	hashes []common.Hash // ascending
	code   map[common.Hash][]byte
	block  uint64
	i      int
}

func newCodeCursor(code map[common.Hash][]byte, block uint64) *codeCursor {
	hashes := make([]common.Hash, 0, len(code))
	for h := range code {
		hashes = append(hashes, h)
	}
	sort.Slice(hashes, func(i, j int) bool { return bytes.Compare(hashes[i][:], hashes[j][:]) < 0 })
	return &codeCursor{hashes: hashes, code: code, block: block}
}

// upTo writes every remaining code row that sorts before key (nil = all).
func (c *codeCursor) upTo(w *sstWriter, key []byte) {
	for c.i < len(c.hashes) {
		k := epochCodeKey(c.hashes[c.i])
		if key != nil && bytes.Compare(k[:], key) >= 0 {
			return
		}
		w.add(k[:], c.block, c.code[c.hashes[c.i]])
		c.i++
	}
}

// ---------- builder ----------

// BuildEpoch assembles one epoch and publishes it as a content-addressed
// artifact: the bytes land in the store's spool under their own sha256 and the
// data directory gets the local index marker naming it. Returns the hash.
func BuildEpoch(st *dist.Store, in *EpochInput) (string, error) {
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
	epochDict := trainDictCLI(samples, uint32(in.Start%0xfffffffe)+1, dictTargetSize)

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

	sst, sstIdx, deletes, keys := buildSST(enc, in.StateRows, newCodeCursor(in.Code, in.Start))
	section(secSST, sst)
	section(secSSTIdx, sstIdx)
	section(secDeletes, deletes)

	txidx, txbloom := buildEpochTxidx(in, count)
	section(secTxidx, txidx)
	section(secTxBloom, txbloom)
	section(secLogidx, buildLogidx(in.Logs, in.Start, count))
	section(secKeybloom, buildBloom(keys))

	// Stored-logs sections. Present (nonempty index headers) whenever the
	// inputs were supplied, even for epochs with zero logs. Without them
	// (unit tests only) the five sections are empty but still get their
	// offsets, so the section layout never varies.
	var stored [5][]byte
	if in.HasStoredLogInputs() {
		if stored, err = buildStoredSections(in, epochDict); err != nil {
			return "", err
		}
	}
	section(secLogsDict, stored[0])
	section(secFullLogs, stored[1])
	section(secFullLogsIdx, stored[2])
	section(secRcpt, stored[3])
	section(secRcptIdx, stored[4])

	// Footer.
	var ft [epochFooterSize]byte
	copy(ft[0:4], epochMagic[:])
	binary.LittleEndian.PutUint32(ft[4:8], epochVersion)
	binary.LittleEndian.PutUint64(ft[8:16], in.Start)
	binary.LittleEndian.PutUint64(ft[16:24], count)
	binary.LittleEndian.PutUint64(ft[24:32], in.TxCount)
	copy(ft[32:64], in.Prev[:])
	for i := 0; i < epochNumSections; i++ {
		binary.LittleEndian.PutUint64(ft[epochTableOff+i*16:], offsets[i][0])
		binary.LittleEndian.PutUint64(ft[epochTableOff+8+i*16:], offsets[i][1])
	}
	copy(ft[epochFooterSize-4:], epochMagic[:])
	buf.Write(ft[:])

	hash, err := st.Put(buf.Bytes())
	if err != nil {
		return "", err
	}
	return hash, WriteMarker(st.Dir(), EpochMarkerName(in.Start, count), hash)
}

// buildStoredSections derives the five v2 sections from filled stored-log
// inputs. containerDict compresses the receipt frames (measured fine);
// the logs get their own freshly trained dict.
func buildStoredSections(in *EpochInput, containerDict []byte) (secs [5][]byte, err error) {
	var recSamples [][]byte
	for _, r := range in.FullLogs {
		recSamples = append(recSamples, r)
	}
	sort.Slice(recSamples, func(i, j int) bool { return bytes.Compare(recSamples[i], recSamples[j]) < 0 })
	if len(recSamples) > dictMaxSamples {
		step := len(recSamples) / dictMaxSamples
		sub := make([][]byte, 0, dictMaxSamples)
		for i := 0; i < len(recSamples); i += step {
			sub = append(sub, recSamples[i])
		}
		recSamples = sub
	}
	var logsDict []byte
	if len(recSamples) > 0 {
		logsDict = trainDictCLI(recSamples, uint32(in.Start%0xfffffffe)+2, logsDictTarget)
	}
	newEnc := func(dict []byte) (*zstd.Encoder, error) {
		opts := []zstd.EOption{zstd.WithEncoderLevel(zstd.SpeedBestCompression)}
		if len(dict) > 0 {
			opts = append(opts, zstd.WithEncoderDict(dict))
		}
		return zstd.NewWriter(nil, opts...)
	}
	encL, err := newEnc(logsDict)
	if err != nil {
		return secs, err
	}
	defer encL.Close()
	encC, err := newEnc(containerDict)
	if err != nil {
		return secs, err
	}
	defer encC.Close()
	secs[0] = logsDict
	secs[1], secs[2] = buildStoredFrames(encL, in.Start, in.FullLogs)
	secs[3], secs[4] = buildStoredFrames(encC, in.Start, in.RcptRecs)
	return secs, nil
}

// buildStoredFrames packs sparse per-block records (stored logs / receipt
// fields) into zstd frames of framedGroup records. Index layout:
//
//	u32 nMembers | members (12B: relBlock u32, frame u32, slot u32) |
//	frame offsets u64 x (nFrames+1)
//
// Members sorted by relBlock; the index header is always written, so a
// present-but-empty section (epoch without logs) stays distinguishable
// from a v2 epoch sealed without the sections (unit tests only).
func buildStoredFrames(enc *zstd.Encoder, start uint64, recs map[uint64][]byte) (data, index []byte) {
	blocks := make([]uint64, 0, len(recs))
	for b := range recs {
		blocks = append(blocks, b)
	}
	sort.Slice(blocks, func(i, j int) bool { return blocks[i] < blocks[j] })

	var (
		members []byte
		offs    []byte
		payload []byte
		inFrame int
		frame   uint32
	)
	offs = binary.LittleEndian.AppendUint64(offs, 0)
	for _, b := range blocks {
		members = binary.LittleEndian.AppendUint32(members, uint32(b-start))
		members = binary.LittleEndian.AppendUint32(members, frame)
		members = binary.LittleEndian.AppendUint32(members, uint32(inFrame))
		payload = binary.AppendUvarint(payload, uint64(len(recs[b])))
		payload = append(payload, recs[b]...)
		if inFrame++; inFrame == framedGroup {
			data = enc.EncodeAll(payload, data)
			offs = binary.LittleEndian.AppendUint64(offs, uint64(len(data)))
			payload, inFrame = payload[:0], 0
			frame++
		}
	}
	if len(payload) > 0 {
		data = enc.EncodeAll(payload, data)
		offs = binary.LittleEndian.AppendUint64(offs, uint64(len(data)))
	}
	index = binary.LittleEndian.AppendUint32(nil, uint32(len(blocks)))
	index = append(index, members...)
	index = append(index, offs...)
	return data, index
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

// sstWriter packs rows into dict-compressed blocks. Rows must arrive in
// final (key, block) order, already deduped: the packing rule is what makes
// two independent seals of the same chain content produce the same bytes.
// Row wire format inside a block: key53 | block u64 BE | uvarint vlen | value.
type sstWriter struct {
	enc *zstd.Encoder

	sst      []byte
	sstIdx   []byte
	deletes  []byte
	keys     [][]byte
	raw      []byte
	firstKey []byte
	firstBlk uint64
	lastKey  []byte
}

func (w *sstWriter) flush() {
	if len(w.raw) == 0 {
		return
	}
	w.sstIdx = append(w.sstIdx, w.firstKey...)
	w.sstIdx = binary.BigEndian.AppendUint64(w.sstIdx, w.firstBlk)
	w.sstIdx = binary.LittleEndian.AppendUint64(w.sstIdx, uint64(len(w.sst)))
	w.sst = w.enc.EncodeAll(w.raw, w.sst)
	w.raw = w.raw[:0]
	w.firstKey = nil
}

func (w *sstWriter) add(key []byte, block uint64, val []byte) {
	if w.lastKey == nil || !bytes.Equal(w.lastKey, key) {
		k := append([]byte(nil), key...)
		w.keys = append(w.keys, k)
		w.lastKey = k
	}
	if key[0] == recKindAccount && len(val) == 0 {
		w.deletes = append(w.deletes, key...)
		w.deletes = binary.BigEndian.AppendUint64(w.deletes, block)
	}
	if w.firstKey == nil {
		w.firstKey = append([]byte(nil), key...)
		w.firstBlk = block
	}
	w.raw = append(w.raw, key...)
	w.raw = binary.BigEndian.AppendUint64(w.raw, block)
	w.raw = binary.AppendUvarint(w.raw, uint64(len(val)))
	w.raw = append(w.raw, val...)
	if len(w.raw) >= sstBlockTarget {
		w.flush()
	}
}

// buildSST sorts and dedupes the epoch's post-image rows, merges in the v3
// code rows, and returns the sst data, sparse index, the raw account-delete
// rows, and every unique written key (bloom input).
func buildSST(enc *zstd.Encoder, rows []StateRow, cc *codeCursor) (sst, sstIdx, deletes []byte, keys [][]byte) {
	sort.Slice(rows, func(i, j int) bool {
		if c := bytes.Compare(rows[i].Key[:], rows[j].Key[:]); c != 0 {
			return c < 0
		}
		if rows[i].Block != rows[j].Block {
			return rows[i].Block < rows[j].Block
		}
		return rows[i].Seq < rows[j].Seq
	})

	w := &sstWriter{enc: enc}
	for i := range rows {
		r := &rows[i]
		// last write of the same (key, block) wins (post-image semantics)
		if i+1 < len(rows) && rows[i+1].Block == r.Block && rows[i+1].Key == r.Key {
			continue
		}
		cc.upTo(w, r.Key[:])
		w.add(r.Key[:], r.Block, r.Value)
	}
	cc.upTo(w, nil)
	w.flush()
	return w.sst, w.sstIdx, w.deletes, w.keys
}

// buildEpochTxidx encodes the epoch's tx fingerprints exactly like the raw
// txidx buckets (ef + packed blocks), with epoch-relative block numbers.
// Layout: nTx u64 | efL u32 | blkBits u32 | 5 length-prefixed word sections.
// It also returns the tx bloom over the SAME fingerprint slice, so the filter
// and the index it guards cannot disagree.
func buildEpochTxidx(in *EpochInput, count uint64) (idx, bloom []byte) {
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
	return out, buildTxBloom(fps)
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

// bloomBits: m = bitsPerKey bits per key rounded up to whole words. Split out
// of buildBloom so a streaming writer that knows only the row count up front
// (state/base.go's baseWriter) sizes the filter identically.
func bloomBits(nKeys, bitsPerKey uint64) uint64 {
	m := nKeys * bitsPerKey
	if m < 64 {
		m = 64
	}
	return (m + 63) / 64 * 64
}

// encodeBloom serializes the filter: m u64 | k u32 | pad u32 | words.
// Readers take m and k from these bytes, so the two filters (keys, tx
// fingerprints) share one reader.
func encodeBloom(m uint64, k uint32, words []uint64) []byte {
	out := binary.LittleEndian.AppendUint64(nil, m)
	out = binary.LittleEndian.AppendUint32(out, k)
	out = binary.LittleEndian.AppendUint32(out, 0)
	for _, w := range words {
		out = binary.LittleEndian.AppendUint64(out, w)
	}
	return out
}

// bloomSet ORs one key's k bits in. OR-accumulated, so insertion order cannot
// reach the bytes.
func bloomSet(words []uint64, m, k uint64, key []byte) {
	h1, h2 := bloomHash(key)
	for i := uint64(0); i < k; i++ {
		bit := (h1 + i*h2) % m
		words[bit/64] |= 1 << (bit % 64)
	}
}

// buildBloom is the key bloom: k = bloomHashes, double hashing over xxhash.
func buildBloom(keys [][]byte) []byte {
	m := bloomBits(uint64(len(keys)), bloomBitsPerKey)
	words := make([]uint64, m/64)
	for _, k := range keys {
		bloomSet(words, m, bloomHashes, k)
	}
	return encodeBloom(m, bloomHashes, words)
}

// buildTxBloom is the tx-fingerprint bloom over the fp48s of one epoch, keyed
// by the fingerprint's 8 little-endian bytes (txBloomKey).
func buildTxBloom(fps []uint64) []byte {
	m := bloomBits(uint64(len(fps)), txBloomBitsPerTx)
	words := make([]uint64, m/64)
	for _, fp := range fps {
		k := txBloomKey(fp)
		bloomSet(words, m, txBloomHashes, k[:])
	}
	return encodeBloom(m, txBloomHashes, words)
}

// txBloomKey is the bloom key for a 48-bit tx fingerprint.
func txBloomKey(fp uint64) (k [8]byte) {
	binary.LittleEndian.PutUint64(k[:], fp)
	return
}

func bloomHash(key []byte) (h1, h2 uint64) {
	h := xxhash.Sum64(key)
	h1 = h
	h2 = h>>33 | h<<31
	h2 |= 1
	return
}

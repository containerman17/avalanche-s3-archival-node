package state

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math/bits"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/rlp"
	"github.com/cespare/xxhash/v2"
	"github.com/containerman17/epochdb/dist"
	"github.com/klauspost/compress/zstd"
)

// The dictionary trainer is the zstd CLI, and its ABSENCE USED TO BE SILENT:
// trainDictCLI logged one line and returned nil, so the epoch was written
// dict-less. That is a differently-shaped artifact from the same chain
// content, which is precisely what byte-identity across independent builders
// forbids (DESIGN's core promise). It shipped: the published image is
// distroless with no zstd, so every containerized seal diverged from every
// host seal, ~24% larger and a different hash, and the only trace was one log
// line nobody read. A trainer that cannot run is now a HARD SEAL ERROR. The
// seal contract already handles that well: SEAL FAILED, history stays raw,
// the chain keeps serving, the next tick retries.
//
// There is no opt-out env var. A deliberately dict-less deployment is not a
// thing anyone wants, and a knob that re-enables divergence is the bug.
const (
	// ZstdPinnedVersion is THE pin, and this line is its single source of
	// truth: the Dockerfile reads the version out of this file rather than
	// repeating it, so the image and the binary cannot drift apart. An exact
	// zstd is a HARD SYSTEM REQUIREMENT (user ruling 2026-08-05); rebuilds on
	// another version are not guaranteed bit-identical (decompression always
	// is), so a wrong version diverges exactly as silently as a missing one
	// and is refused exactly the same way.
	ZstdPinnedVersion = "1.5.7"

	// dictMinSamples is zstd --train's own floor ("nb of samples too low"
	// below 7, measured on v1.5.7). Checking it HERE keeps the dict-less
	// case a property of the INPUT, so every builder skips the same corpora
	// and still agrees byte for byte, and leaves every remaining training
	// failure environmental and therefore fatal.
	dictMinSamples = 7
)

// dictTrainerHowTo is what an operator needs to hear, once, on the way out:
// what is required, and the two ways to satisfy it.
const dictTrainerHowTo = "epochs carry a zstd dictionary trained at seal time by the zstd CLI, " +
	"and the artifact's bytes depend on the trainer, so zstd v" + ZstdPinnedVersion +
	" EXACTLY is a system requirement: install that version, or run the published " +
	"ghcr.io/containerman17/epochdb image, which ships it"

// zstdBanner pulls the version out of `zstd --version`, whose banner reads
// "*** Zstandard CLI (64-bit) v1.5.7, by Yann Collet ***". Older 1.5.x print a
// different sentence around the same "vX.Y.Z", which is why this matches the
// version token rather than the sentence.
var zstdBanner = regexp.MustCompile(`v(\d+\.\d+\.\d+)`)

// CheckDictTrainer reports whether this machine may seal. `serve` calls it at
// STARTUP and dies on any error: an operator learns at boot, not hours later
// at the first epoch boundary, and never after publishing a divergent epoch.
func CheckDictTrainer() error {
	if _, err := exec.LookPath("zstd"); err != nil {
		return fmt.Errorf("zstd CLI not on PATH (%w): %s", err, dictTrainerHowTo)
	}
	out, err := exec.Command("zstd", "--version").CombinedOutput()
	if err != nil {
		return fmt.Errorf("zstd --version failed (%w: %s): %s", err, bytes.TrimSpace(out), dictTrainerHowTo)
	}
	m := zstdBanner.FindSubmatch(out)
	if m == nil {
		return fmt.Errorf("zstd --version printed no version (%q): %s", bytes.TrimSpace(out), dictTrainerHowTo)
	}
	if got := string(m[1]); got != ZstdPinnedVersion {
		return fmt.Errorf("zstd is v%s, this build requires v%s exactly: %s",
			got, ZstdPinnedVersion, dictTrainerHowTo)
	}
	return nil
}

// trainDictCLI trains a zstd dictionary with the pinned zstd CLI. It returns
// (nil, nil) ONLY for a corpus below the trainer's own sample floor, which is
// deterministic and identical on every builder; every other failure is an
// error that fails the seal.
func trainDictCLI(samples [][]byte, dictID uint32, maxDict int) ([]byte, error) {
	if len(samples) < dictMinSamples {
		return nil, nil
	}
	if err := CheckDictTrainer(); err != nil {
		return nil, err
	}
	tmp, err := os.MkdirTemp("", "epochdict")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(tmp)
	args := []string{"--train", "-q", "-o", filepath.Join(tmp, "dict.bin"),
		fmt.Sprintf("--maxdict=%d", maxDict),
		fmt.Sprintf("--dictID=%d", dictID)}
	for i, s := range samples {
		p := filepath.Join(tmp, fmt.Sprintf("s%06d", i))
		if err := os.WriteFile(p, s, 0o644); err != nil {
			return nil, err
		}
		args = append(args, p)
	}
	if out, err := exec.Command("zstd", args...).CombinedOutput(); err != nil {
		return nil, fmt.Errorf("zstd --train (%d samples, dictID %d): %w: %s",
			len(samples), dictID, err, bytes.TrimSpace(out))
	}
	return os.ReadFile(filepath.Join(tmp, "dict.bin"))
}

// Sealed epoch artifact, named by sha256 of its own per-chunk hash list, which
// casfs keeps in a tail past the content (the local index marker
// epoch_<startblock>_<blockcount>.cas carries that name). Immutable,
// self-described, bit-identical when rebuilt from the same chain content.
// Sections in write order, fixed-size footer last. THE READER SEEKS FROM THE
// LOGICAL LENGTH casfs reports, NOT FROM EOF: the file on disk carries casfs's
// metadata past the footer, and nothing above dist ever sees it.
//
//	dict       zstd dictionary trained AT SEAL TIME on this epoch's raw
//	           containers (the pinned zstd CLI; determinism via a
//	           start-block-derived dictionary ID)
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
//	txidx      EF fp48 + bit-packed epoch-relative block numbers, over TX
//	           HASHES AND BLOCK HASHES alike (v6)
//	txbloom    bloom over exactly the fp48s in txidx (txBloomBitsPerTx), the
//	           reject filter that keeps txidx off the resident set
//	logidx     posting lists addr->EF blocklist, topic->EF blocklist
//	keybloom   bloom over keys written in this epoch (bloomBitsPerKey)
//
// The footer carries the HASH CHAIN: epoch K's footer embeds epoch K-1's NAME,
// and the first epoch of a chain embeds the chain root, sha256(genesisData)
// over the chain's on-chain CreateChainTx genesis (dist.ChainRoot). Prev-hash is
// chain content like everything else here, so two honest builders still emit
// identical bytes, and one head hash authenticates every epoch below it.
//
// All hardcoded parameters below are pre-freeze tunables (user directive:
// constants in code, no config).
const (
	// The epoch boundary schedule, see EpochTxsAt.
	epochTxsBase = 250_000
	epochTxsCap  = 5 // doublings before the size stops growing (250k << 5 = 8M)

	framedGroup    = 16        // containers/headers per zstd frame (07-17: 2.24x, 34us access)
	dictTargetSize = 512 << 10 // 07-17 experiment optimum
	dictMaxSamples = 4096      // training sample cap, keeps seal-time bounded
	sstBlockTarget = 64 << 10  // raw bytes per compressed SST block
	// A key nothing ever wrote (an unset storage slot, the common case in
	// any eth_call) has NO early exit: History.search probes
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

	// Format v6 is the ONLY supported format: stored-logs sections (v2,
	// 2026-07-20), contract code as 'c' rows in the SST (v3, 2026-07-28),
	// the HASH-CHAIN footer field (v4, 2026-07-30) that makes one head hash
	// authenticate all of a chain's history, the tx-fingerprint bloom (v5,
	// 2026-07-30), and BLOCK HASHES FOLDED INTO THAT SAME INDEX AND BLOOM
	// (v6, 2026-07-31), which deletes the floor-to-head block-hash map. Same
	// 17 sections: v6 changes only what goes into txidx/txbloom. There is no
	// upgrade path: OpenEpoch refuses an older file and the corpus is rebuilt
	// by a fresh sync (user ruling 2026-07-28).
	epochVersion     = 6
	epochNumSections = 17
	epochTableOff    = 4 + 4 + 8 + 8 + 8 + 32                  // magic, version, start, count, txs, prev hash
	epochFooterSize  = epochTableOff + epochNumSections*16 + 4 // + table + trailing magic

	logsDictTarget = 128 << 10 // dedicated logs dict (measured better than container dict)
)

// EpochTxsAt is the epoch boundary: epoch i seals at the first block whose
// cumulative tx count reaches min(8M, 250k << i), i.e. 250k, 500k, 1M, 2M,
// 4M, then 8M forever (cumulative boundaries 0.25M, 0.75M, 1.75M, 3.75M,
// 7.75M, 15.75M, +8M each). A PURE FUNCTION OF THE EPOCH INDEX, identical on
// every chain and every VM kind, with no flag and no config, so two honest
// nodes cut byte-identical boundaries without talking to each other. User
// ruling 2026-07-31 (cap lowered 16M to 8M same day: the worst-case unsealed
// tip tail halves to 8M txs, ~50GB disk and ~2GB heap at 1.2 tx/block L1
// density, while C-chain only goes ~77 to ~152 epochs, still the proven
// ~120-epoch miss shape). Rationale for the schedule itself (small chains
// seal early instead of hoarding an unevictable raw tail, giants converge to
// a fixed epoch shape) is in DESIGN.md.
func EpochTxsAt(i int) uint64 {
	if i > epochTxsCap {
		i = epochTxsCap
	}
	return epochTxsBase << i
}

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

// sectionPhase names the STREAMED sections in the seal's progress lines (the
// only sections written by a walk long enough to be worth reporting).
var sectionPhase = map[int]string{
	secBodies:   "bodies",
	secHeaders:  "headers",
	secFullLogs: "storedlogs",
	secRcpt:     "rcpt",
}

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

	// Prev is the hash-chain link: epoch K-1's artifact NAME, or the chain
	// root (dist.ChainRoot) for a chain's first epoch.
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
// run). It holds the HASHES and a getter, never the blobs: an epoch's code is
// hundreds of MB (80,291 blobs on Fuji's epoch 11) and one blob at a time is
// all a sorted write needs. The row's block number is the epoch start: a blob
// has no meaningful write height (the same bytes can be deployed by many
// blocks) and the code lookup ignores it, so the epoch's own first block is
// the deterministic choice.
type codeCursor struct {
	hashes []common.Hash // ascending
	get    func(common.Hash) ([]byte, error)
	block  uint64
	i      int
}

func newCodeCursor(hashes []common.Hash, get func(common.Hash) ([]byte, error), block uint64) *codeCursor {
	sort.Slice(hashes, func(i, j int) bool { return bytes.Compare(hashes[i][:], hashes[j][:]) < 0 })
	return &codeCursor{hashes: hashes, get: get, block: block}
}

// upTo writes every remaining code row that sorts before key (nil = all).
func (c *codeCursor) upTo(w *sstWriter, key []byte) error {
	for c.i < len(c.hashes) {
		k := epochCodeKey(c.hashes[c.i])
		if key != nil && bytes.Compare(k[:], key) >= 0 {
			return nil
		}
		blob, err := c.get(c.hashes[c.i])
		if err != nil {
			return err
		}
		if err := w.add(k[:], c.block, blob); err != nil {
			return err
		}
		c.i++
	}
	return nil
}

// ---------- builder ----------

// epochSrc is what the builder PULLS, and it is the whole of the seal's
// memory rule: nothing here hands over an epoch's worth of anything. The
// sections are written in file order, so every walk must be re-runnable (the
// sealer re-reads the raw corpus per family; an EpochInput re-walks its
// slices). Cheaper than it sounds: a raw family is read sequentially and the
// alternative is holding tens of GB, which is what OOM-killed three seals.
type epochSrc struct {
	Start, Count, TxCount uint64
	Prev                  [32]byte

	// Container is random access by epoch-relative index: the dict trains on
	// evenly spaced samples, which needs the block count first.
	Container func(i uint64) ([]byte, error)
	// Containers and Headers walk the epoch ascending.
	Containers func(yield func(b []byte) error) error
	Headers    func(yield func(b []byte) error) error
	// Rows yields every post-image write row, in any order (the builder sorts
	// them externally).
	Rows func(yield func(StateRow) error) error
	// Code is called AFTER Rows: only then does a sealer know which blobs this
	// epoch's account rows reference. Returns the hashes (any order) and a
	// getter for one blob at a time.
	Code func() ([]common.Hash, func(common.Hash) ([]byte, error), error)
	// Logs yields the per-block log tuple records (logidx input).
	Logs func(yield func(LogRec) error) error
	// Stored yields (block, stored-logs record, receipt-fields record)
	// ascending, either half possibly empty. nil = seal without the v2
	// sections (unit tests only). Walked three times: the logs dict is trained
	// on those records before either section can be compressed.
	Stored func(yield func(block uint64, logs, rcpt []byte) error) error
	// TxPairs yields (fingerprint, epoch-relative block) for every tx hash and
	// every block hash (v6).
	TxPairs func(yield func(fp, blk uint64) error) error
}

// src adapts the in-RAM input to the streaming builder: tests and small
// callers keep handing over slices, the sealer hands over walks over the raw
// corpus (state/seal.go). ONE builder either way, which is what makes the
// streaming path byte-identical to the slice path by construction.
func (in *EpochInput) src() *epochSrc {
	blobs := func(bs [][]byte) func(func([]byte) error) error {
		return func(yield func([]byte) error) error {
			for _, b := range bs {
				if err := yield(b); err != nil {
					return err
				}
			}
			return nil
		}
	}
	s := &epochSrc{
		Start:      in.Start,
		Count:      uint64(len(in.Containers)),
		TxCount:    in.TxCount,
		Prev:       in.Prev,
		Container:  func(i uint64) ([]byte, error) { return in.Containers[i], nil },
		Containers: blobs(in.Containers),
		Headers:    blobs(in.Headers),
		Rows: func(yield func(StateRow) error) error {
			for _, r := range in.StateRows {
				if err := yield(r); err != nil {
					return err
				}
			}
			return nil
		},
		Code: func() ([]common.Hash, func(common.Hash) ([]byte, error), error) {
			hashes := make([]common.Hash, 0, len(in.Code))
			for h := range in.Code {
				hashes = append(hashes, h)
			}
			return hashes, func(h common.Hash) ([]byte, error) { return in.Code[h], nil }, nil
		},
		Logs: func(yield func(LogRec) error) error {
			for _, lr := range in.Logs {
				if err := yield(lr); err != nil {
					return err
				}
			}
			return nil
		},
		TxPairs: func(yield func(fp, blk uint64) error) error {
			for i, hdr := range in.Headers {
				if err := yield(txFingerprint(BlockHashFromHeaderRLP(hdr)), uint64(i)); err != nil {
					return err
				}
			}
			for blk, hashes := range in.TxHashes {
				for _, h := range hashes {
					if err := yield(binary.BigEndian.Uint64(h[:8])>>16, blk-in.Start); err != nil {
						return err
					}
				}
			}
			return nil
		},
	}
	if in.HasStoredLogInputs() {
		s.Stored = func(yield func(uint64, []byte, []byte) error) error {
			blocks := make([]uint64, 0, len(in.FullLogs)+len(in.RcptRecs))
			for b := range in.FullLogs {
				blocks = append(blocks, b)
			}
			for b := range in.RcptRecs {
				if _, dup := in.FullLogs[b]; !dup {
					blocks = append(blocks, b)
				}
			}
			sort.Slice(blocks, func(i, j int) bool { return blocks[i] < blocks[j] })
			for _, b := range blocks {
				if err := yield(b, in.FullLogs[b], in.RcptRecs[b]); err != nil {
					return err
				}
			}
			return nil
		}
	}
	return s
}

// sealTmpPrefix names the seal's scratch directories. They live in the DATA
// DIR (same filesystem, never /tmp), one per build, removed when the build
// returns; sealEpochs sweeps whatever a killed build left behind.
const sealTmpPrefix = "sealtmp-"

// newEpochEncoder is the ONE place a seal-time zstd encoder is made, and
// WithEncoderConcurrency(1) is not tuning: EncodeAll compresses in the calling
// goroutine and takes one state from the pool, so the other GOMAXPROCS-1
// states are pure resident cost. Measured 2026-08-02 on a 16-core box, three
// dict-primed SpeedBestCompression encoders: 3.37 GB at the default
// concurrency, 0.10 GB at 1, byte-identical output either way.
func newEpochEncoder(dict []byte) (*zstd.Encoder, error) {
	opts := []zstd.EOption{
		zstd.WithEncoderLevel(zstd.SpeedBestCompression),
		zstd.WithEncoderConcurrency(1),
	}
	if len(dict) > 0 {
		opts = append(opts, zstd.WithEncoderDict(dict))
	}
	return zstd.NewWriter(nil, opts...)
}

// BuildEpoch assembles one epoch and publishes it as a content-addressed
// artifact: the bytes land in the store's spool under their own sha256 and the
// data directory gets the local index marker naming it. Returns the hash.
func BuildEpoch(st *dist.Store, in *EpochInput) (string, error) {
	if len(in.Containers) == 0 || len(in.Headers) != len(in.Containers) {
		return "", fmt.Errorf("epoch build: %d containers, %d headers", len(in.Containers), len(in.Headers))
	}
	return buildEpoch(st, in.src())
}

// buildEpoch writes the sections in file order STRAIGHT INTO THE ARTIFACT
// FILE, hashing as it goes, pulling each one from src when its turn comes.
// Nothing here is proportional to the epoch: the multi-GB sections stream, the
// index and bloom sections are megabytes, and the two orderings that cannot be
// streamed (post-image rows, logs-dict samples) go through extSort. Scratch
// lives in a directory under the data dir, removed however this returns.
func buildEpoch(st *dist.Store, src *epochSrc) (string, error) {
	if src.Count == 0 {
		return "", fmt.Errorf("epoch build: no blocks")
	}
	tmp, err := os.MkdirTemp(st.Dir(), sealTmpPrefix)
	if err != nil {
		return "", err
	}
	defer os.RemoveAll(tmp)

	// Deterministic dictionary: sample containers evenly, fixed dict ID,
	// zstd CLI trainer (pinned v1.5.7). klauspost's dict.BuildZstdDict is
	// internally map-iteration nondeterministic (verified 2026-07-18) and
	// would break bit-identical epoch files.
	samples, err := sampleContainers(src)
	if err != nil {
		return "", err
	}
	stopDict := LogProgress("seal: dict", func() string {
		return fmt.Sprintf("zstd --train on %d container samples", len(samples))
	})
	epochDict, err := trainDictCLI(samples, uint32(src.Start%0xfffffffe)+1, dictTargetSize)
	stopDict()
	if err != nil {
		return "", err
	}

	enc, err := newEpochEncoder(epochDict)
	if err != nil {
		return "", err
	}
	defer enc.Close()

	o, err := newEpochOut(st)
	if err != nil {
		return "", err
	}
	defer o.discard()

	if err := o.section(secDict, epochDict); err != nil {
		return "", err
	}
	bodiesIdx, err := o.framed(secBodies, enc, src.Containers)
	if err != nil {
		return "", err
	}
	if err := o.section(secBodiesIdx, bodiesIdx); err != nil {
		return "", err
	}
	headersIdx, err := o.framed(secHeaders, enc, src.Headers)
	if err != nil {
		return "", err
	}
	if err := o.section(secHeadersIdx, headersIdx); err != nil {
		return "", err
	}

	keys, nKeys, err := writeSST(o, tmp, enc, src)
	if err != nil {
		return "", err
	}
	if err := writeTxidx(o, src); err != nil {
		return "", err
	}
	logidx, err := buildLogidx(src.Logs, src.Start, src.Count)
	if err != nil {
		return "", err
	}
	if err := o.section(secLogidx, logidx); err != nil {
		return "", err
	}
	keybloom, err := buildBloomFile(keys, nKeys)
	if err != nil {
		return "", err
	}
	if err := o.section(secKeybloom, keybloom); err != nil {
		return "", err
	}
	if err := writeStored(o, tmp, enc, src); err != nil {
		return "", err
	}

	hash, err := o.finish(src)
	if err != nil {
		return "", err
	}
	return hash, WriteMarker(st.Dir(), EpochMarkerName(src.Start, src.Count), hash)
}

// sampleContainers picks the dict training set: every step-th container, step
// chosen so at most dictMaxSamples land in it (all of them below that cap).
func sampleContainers(src *epochSrc) ([][]byte, error) {
	step := uint64(1)
	if src.Count > dictMaxSamples {
		step = src.Count / dictMaxSamples
	}
	want := (src.Count + step - 1) / step
	var got atomic.Uint64
	defer LogProgress("seal: dict", func() string {
		return fmt.Sprintf("%d of %d containers sampled", got.Load(), want)
	})()
	out := make([][]byte, 0, min(src.Count, dictMaxSamples+1))
	for i := uint64(0); i < src.Count; i += step {
		c, err := src.Container(i)
		if err != nil {
			return nil, err
		}
		out = append(out, c)
		got.Add(1)
	}
	return out, nil
}

// ---------- the artifact file ----------

// epochOut is the artifact under construction: a file in the spool, the
// running chunk hash list that will NAME it, and the section table. It replaces a
// bytes.Buffer that held the WHOLE epoch (7.2 GB on Fuji's epoch 11, plus a
// doubling copy whenever it grew) and is why the sealed size no longer shows
// up in the sealer's resident set at all.
type epochOut struct {
	st   *dist.Store
	path string
	f    *os.File
	w    *bufio.Writer
	d    *dist.Hasher
	off  uint64
	tab  [epochNumSections][2]uint64 // off, len per section
}

// liveEpochBuilds names the spool files THIS process is currently writing, and
// it is the whole of the sweep's exclusion rule: the data dir has one writer
// (enforced by the process's lock on it), so an `epoch-*.tmp` that is not in
// here belongs to a build that is dead. Keyed by path; entries live from
// CreateTemp to Adopt or discard.
var liveEpochBuilds sync.Map

func newEpochOut(st *dist.Store) (*epochOut, error) {
	f, err := os.CreateTemp(st.SpoolDir(), "epoch-*.tmp")
	if err != nil {
		return nil, err
	}
	liveEpochBuilds.Store(f.Name(), struct{}{})
	return &epochOut{
		st: st, path: f.Name(), f: f,
		w: bufio.NewWriterSize(f, 1<<20), d: dist.NewHasher(),
	}, nil
}

func (o *epochOut) write(b []byte) error {
	if _, err := o.w.Write(b); err != nil {
		return err
	}
	o.d.Write(b)
	o.off += uint64(len(b))
	return nil
}

// begin/end bracket a section written in pieces; section writes a whole one.
// An empty section still gets its offset, so the layout never varies.
func (o *epochOut) begin(id int) { o.tab[id][0] = o.off }
func (o *epochOut) end(id int)   { o.tab[id][1] = o.off - o.tab[id][0] }

func (o *epochOut) section(id int, b []byte) error {
	o.begin(id)
	if err := o.write(b); err != nil {
		return err
	}
	o.end(id)
	return nil
}

// framed packs a walk's blobs into zstd frames of framedGroup entries each
// (frame payload = per-blob uvarint length + bytes), writes them as section
// id, and returns the frame offset index (u64 LE, nFrames+1 with an end
// sentinel). One frame of RAM, whatever the epoch's block count.
func (o *epochOut) framed(id int, enc *zstd.Encoder, walk func(func([]byte) error) error) ([]byte, error) {
	o.begin(id)
	base := o.off
	var payload, frame []byte
	offs := binary.LittleEndian.AppendUint64(nil, 0)
	n := 0
	var nIn, nBytes atomic.Uint64
	defer LogProgress("seal: "+sectionPhase[id], func() string {
		return fmt.Sprintf("%d entries in, %.1fMB compressed out", nIn.Load(), float64(nBytes.Load())/1e6)
	})()
	flush := func() error {
		frame = enc.EncodeAll(payload, frame[:0])
		if err := o.write(frame); err != nil {
			return err
		}
		nBytes.Add(uint64(len(frame)))
		offs = binary.LittleEndian.AppendUint64(offs, o.off-base)
		payload, n = payload[:0], 0
		return nil
	}
	if err := walk(func(b []byte) error {
		payload = binary.AppendUvarint(payload, uint64(len(b)))
		payload = append(payload, b...)
		nIn.Add(1)
		if n++; n == framedGroup {
			return flush()
		}
		return nil
	}); err != nil {
		return nil, err
	}
	if n > 0 {
		if err := flush(); err != nil {
			return nil, err
		}
	}
	o.end(id)
	return offs, nil
}

// storedFrames is framed for the sparse per-block records (stored logs /
// receipt fields), which carry a member table on top. Index layout:
//
//	u32 nMembers | members (12B: relBlock u32, frame u32, slot u32) |
//	frame offsets u64 x (nFrames+1)
//
// Members are in walk order, which is ascending by block.
func (o *epochOut) storedFrames(id int, enc *zstd.Encoder, start uint64, walk func(func(uint64, []byte) error) error) ([]byte, error) {
	o.begin(id)
	base := o.off
	var (
		members, payload, frame []byte
		inFrame                 int
		frameNo, n              uint32
	)
	offs := binary.LittleEndian.AppendUint64(nil, 0)
	var nIn, nBytes atomic.Uint64
	defer LogProgress("seal: "+sectionPhase[id], func() string {
		return fmt.Sprintf("%d records in, %.1fMB compressed out", nIn.Load(), float64(nBytes.Load())/1e6)
	})()
	flush := func() error {
		frame = enc.EncodeAll(payload, frame[:0])
		if err := o.write(frame); err != nil {
			return err
		}
		nBytes.Add(uint64(len(frame)))
		offs = binary.LittleEndian.AppendUint64(offs, o.off-base)
		payload, inFrame = payload[:0], 0
		frameNo++
		return nil
	}
	if err := walk(func(b uint64, rec []byte) error {
		members = binary.LittleEndian.AppendUint32(members, uint32(b-start))
		members = binary.LittleEndian.AppendUint32(members, frameNo)
		members = binary.LittleEndian.AppendUint32(members, uint32(inFrame))
		payload = binary.AppendUvarint(payload, uint64(len(rec)))
		payload = append(payload, rec...)
		n++
		nIn.Add(1)
		if inFrame++; inFrame == framedGroup {
			return flush()
		}
		return nil
	}); err != nil {
		return nil, err
	}
	if len(payload) > 0 {
		if err := flush(); err != nil {
			return nil, err
		}
	}
	o.end(id)
	index := binary.LittleEndian.AppendUint32(nil, n)
	index = append(index, members...)
	return append(index, offs...), nil
}

// finish writes the footer (magic, version, start, count, txs, the hash-chain
// link, the section table) and adopts the file into the spool under the name
// its chunk hash list produces. Returns that name. The footer is the last thing
// in the CONTENT; Adopt appends casfs's own tail after it.
func (o *epochOut) finish(src *epochSrc) (string, error) {
	var ft [epochFooterSize]byte
	copy(ft[0:4], epochMagic[:])
	binary.LittleEndian.PutUint32(ft[4:8], epochVersion)
	binary.LittleEndian.PutUint64(ft[8:16], src.Start)
	binary.LittleEndian.PutUint64(ft[16:24], src.Count)
	binary.LittleEndian.PutUint64(ft[24:32], src.TxCount)
	copy(ft[32:64], src.Prev[:])
	for i := 0; i < epochNumSections; i++ {
		binary.LittleEndian.PutUint64(ft[epochTableOff+i*16:], o.tab[i][0])
		binary.LittleEndian.PutUint64(ft[epochTableOff+8+i*16:], o.tab[i][1])
	}
	copy(ft[epochFooterSize-4:], epochMagic[:])
	if err := o.write(ft[:]); err != nil {
		return "", err
	}
	if err := o.w.Flush(); err != nil {
		return "", err
	}
	if err := o.f.Sync(); err != nil {
		return "", err
	}
	if err := o.f.Close(); err != nil {
		return "", err
	}
	o.f = nil
	hash, err := o.st.Adopt(o.path, o.d)
	if err != nil {
		return "", err
	}
	liveEpochBuilds.Delete(o.path)
	o.path = "" // adopted: not ours to remove any more
	return hash, nil
}

// discard drops a half-written artifact. Deferred by the builder: an error
// must not leave a multi-GB stray in the spool.
func (o *epochOut) discard() {
	if o.f != nil {
		o.f.Close()
		o.f = nil
	}
	if o.path != "" {
		os.Remove(o.path)
		liveEpochBuilds.Delete(o.path)
		o.path = ""
	}
}

// ---------- the state sections ----------

const (
	sstIdxEntrySize = sortedKeySize + 8 + 8 // key, first block, section offset
	deleteEntrySize = sortedKeySize + 8

	// sstRecKeySize is what orders an externally sorted post-image row:
	// key53 | block u64 BE | seq u32 BE, so plain byte order over the record
	// IS (key, block, seq) order and the value rides along untouched.
	sstRecKeySize = sortedKeySize + 8 + 4
)

// sstWriter packs rows into dict-compressed blocks. Rows must arrive in
// final (key, block) order, already deduped: the packing rule is what makes
// two independent seals of the same chain content produce the same bytes.
// Row wire format inside a block: key53 | block u64 BE | uvarint vlen | value.
//
// Blocks go straight into the artifact and unique keys straight into a scratch
// file (the bloom cannot be sized until the last one is known, and 30M+ keys
// on the heap waiting for that is 2 GB of nothing).
type sstWriter struct {
	enc  *zstd.Encoder
	o    *epochOut
	base uint64 // artifact offset the sst section starts at

	// Progress counters, read by the seal's phase line from another
	// goroutine (state/progress.go); everything else here is single-writer.
	nRows, nBytes atomic.Uint64

	sstIdx   []byte
	deletes  []byte
	keys     *bufio.Writer
	nKeys    uint64
	raw      []byte
	frame    []byte
	firstKey []byte
	firstBlk uint64
	lastKey  []byte
}

func (w *sstWriter) flush() error {
	if len(w.raw) == 0 {
		return nil
	}
	w.sstIdx = append(w.sstIdx, w.firstKey...)
	w.sstIdx = binary.BigEndian.AppendUint64(w.sstIdx, w.firstBlk)
	w.sstIdx = binary.LittleEndian.AppendUint64(w.sstIdx, w.o.off-w.base)
	w.frame = w.enc.EncodeAll(w.raw, w.frame[:0])
	if err := w.o.write(w.frame); err != nil {
		return err
	}
	w.nBytes.Add(uint64(len(w.frame)))
	w.raw = w.raw[:0]
	w.firstKey = nil
	return nil
}

func (w *sstWriter) add(key []byte, block uint64, val []byte) error {
	if w.lastKey == nil || !bytes.Equal(w.lastKey, key) {
		w.lastKey = append(w.lastKey[:0], key...)
		if _, err := w.keys.Write(key); err != nil {
			return err
		}
		w.nKeys++
	}
	if key[0] == recKindAccount && len(val) == 0 {
		w.deletes = append(w.deletes, key...)
		w.deletes = binary.BigEndian.AppendUint64(w.deletes, block)
	}
	if w.firstKey == nil {
		w.firstKey = append([]byte(nil), key...)
		w.firstBlk = block
	}
	w.nRows.Add(1)
	w.raw = append(w.raw, key...)
	w.raw = binary.BigEndian.AppendUint64(w.raw, block)
	w.raw = binary.AppendUvarint(w.raw, uint64(len(val)))
	w.raw = append(w.raw, val...)
	if len(w.raw) >= sstBlockTarget {
		return w.flush()
	}
	return nil
}

// writeSST externally sorts the epoch's post-image rows, dedupes them (last
// write of the same key+block wins, post-image semantics), merges in the v3
// code rows and writes the sst, sstIdx and deletes sections. It returns the
// scratch file of unique keys and their count, which is the key bloom's input.
func writeSST(o *epochOut, tmp string, enc *zstd.Encoder, src *epochSrc) (keysPath string, nKeys uint64, err error) {
	rows := newExtSort(tmp, "rows", sstRecKeySize)
	var rec []byte
	stopRows := LogProgress("seal: rows", func() string {
		return fmt.Sprintf("%d write rows read, %d chunks spilled", rows.recs.Load(), rows.chunks.Load())
	})
	err = src.Rows(func(r StateRow) error {
		rec = append(rec[:0], r.Key[:]...)
		rec = binary.BigEndian.AppendUint64(rec, r.Block)
		rec = binary.BigEndian.AppendUint32(rec, uint32(r.Seq))
		rec = append(rec, r.Value...)
		return rows.add(rec)
	})
	stopRows()
	if err != nil {
		return "", 0, err
	}
	hashes, getCode, err := src.Code()
	if err != nil {
		return "", 0, err
	}
	cc := newCodeCursor(hashes, getCode, src.Start)

	keysPath = filepath.Join(tmp, "sstkeys")
	kf, err := os.Create(keysPath)
	if err != nil {
		return "", 0, err
	}
	defer kf.Close()

	o.begin(secSST)
	w := &sstWriter{enc: enc, o: o, base: o.off, keys: bufio.NewWriterSize(kf, 1<<20)}
	// The merge phase: the sorted rows come back out, deduped, with the code
	// rows folded in, and go into the artifact compressed. "of N sorted" is the
	// input count, so the written count lands below it by however much the
	// dedupe removed.
	stopSST := LogProgress("seal: sst", func() string {
		return fmt.Sprintf("%d rows written of %d sorted, %.1fMB compressed out",
			w.nRows.Load(), rows.recs.Load(), float64(w.nBytes.Load())/1e6)
	})
	defer stopSST()
	emit := func(r []byte) error {
		key := r[:sortedKeySize]
		if err := cc.upTo(w, key); err != nil {
			return err
		}
		return w.add(key, binary.BigEndian.Uint64(r[sortedKeySize:]), r[sstRecKeySize:])
	}
	// One record of lookahead IS the dedupe: a row is written only once the
	// next one proves it was not overwritten in the same block.
	var pend []byte
	if err := rows.sorted(func(r []byte) error {
		if len(pend) > 0 && !bytes.Equal(pend[:sortedKeySize+8], r[:sortedKeySize+8]) {
			if err := emit(pend); err != nil {
				return err
			}
		}
		pend = append(pend[:0], r...)
		return nil
	}); err != nil {
		return "", 0, err
	}
	if len(pend) > 0 {
		if err := emit(pend); err != nil {
			return "", 0, err
		}
	}
	if err := cc.upTo(w, nil); err != nil {
		return "", 0, err
	}
	if err := w.flush(); err != nil {
		return "", 0, err
	}
	o.end(secSST)
	if err := o.section(secSSTIdx, w.sstIdx); err != nil {
		return "", 0, err
	}
	if err := o.section(secDeletes, w.deletes); err != nil {
		return "", 0, err
	}
	if err := w.keys.Flush(); err != nil {
		return "", 0, err
	}
	return keysPath, w.nKeys, kf.Close()
}

// ---------- the stored-logs sections ----------

// writeStored derives the five v2 sections. The logs dict is trained on the
// epoch's stored-logs records IN SORTED ORDER, so Stored is walked three
// times: once to sample, once per section. Re-reading the raw receipts family
// (sequential) is cheaper than holding several GB of records to walk twice.
// The receipt frames reuse the container dict encoder, as they always have.
func writeStored(o *epochOut, tmp string, enc *zstd.Encoder, src *epochSrc) error {
	if src.Stored == nil {
		for _, id := range []int{secLogsDict, secFullLogs, secFullLogsIdx, secRcpt, secRcptIdx} {
			if err := o.section(id, nil); err != nil {
				return err
			}
		}
		return nil
	}
	logsDict, err := trainLogsDict(tmp, src)
	if err != nil {
		return err
	}
	if err := o.section(secLogsDict, logsDict); err != nil {
		return err
	}
	encL, err := newEpochEncoder(logsDict)
	if err != nil {
		return err
	}
	defer encL.Close()

	half := func(logs bool) func(func(uint64, []byte) error) error {
		return func(yield func(uint64, []byte) error) error {
			return src.Stored(func(b uint64, l, r []byte) error {
				rec := r
				if logs {
					rec = l
				}
				if len(rec) == 0 {
					return nil
				}
				return yield(b, rec)
			})
		}
	}
	logsIdx, err := o.storedFrames(secFullLogs, encL, src.Start, half(true))
	if err != nil {
		return err
	}
	if err := o.section(secFullLogsIdx, logsIdx); err != nil {
		return err
	}
	rcptIdx, err := o.storedFrames(secRcpt, enc, src.Start, half(false))
	if err != nil {
		return err
	}
	return o.section(secRcptIdx, rcptIdx)
}

// trainLogsDict picks the logs dict's samples exactly as the whole-slice
// builder did: the epoch's stored-logs records in byte order, every step-th
// one, at most dictMaxSamples of them. The sort is external because the
// records themselves are gigabytes.
func trainLogsDict(tmp string, src *epochSrc) ([]byte, error) {
	recs := newExtSort(tmp, "logsdict", 0)
	defer LogProgress("seal: logsdict", func() string {
		return fmt.Sprintf("%d stored-logs records sorted, %d chunks spilled", recs.recs.Load(), recs.chunks.Load())
	})()
	n := 0
	if err := src.Stored(func(_ uint64, logs, _ []byte) error {
		if len(logs) == 0 {
			return nil
		}
		n++
		return recs.add(logs)
	}); err != nil {
		return nil, err
	}
	if n == 0 {
		return nil, nil
	}
	step := 1
	if n > dictMaxSamples {
		step = n / dictMaxSamples
	}
	var samples [][]byte
	i := 0
	if err := recs.sorted(func(rec []byte) error {
		if i%step == 0 {
			samples = append(samples, append([]byte(nil), rec...))
		}
		i++
		return nil
	}); err != nil {
		return nil, err
	}
	return trainDictCLI(samples, uint32(src.Start%0xfffffffe)+2, logsDictTarget)
}

// ---------- the tx and log indexes ----------

// writeTxidx writes the fingerprint index and the bloom that guards it.
func writeTxidx(o *epochOut, src *epochSrc) error {
	idx, bloom, err := buildEpochTxidx(src.TxPairs, src.Count, src.Count+src.TxCount)
	if err != nil {
		return err
	}
	if err := o.section(secTxidx, idx); err != nil {
		return err
	}
	return o.section(secTxBloom, bloom)
}

// buildEpochTxidx encodes the epoch's fingerprints exactly like the raw
// txidx buckets (ef + packed blocks), with epoch-relative block numbers.
// Layout: nEntries u64 | efL u32 | blkBits u32 | 5 length-prefixed word
// sections. It also returns the tx bloom over the SAME fingerprint slice, so
// the filter and the index it guards cannot disagree.
//
// v6: EVERY BLOCK HASH IS AN ENTRY TOO, mapping to that block's own height.
// A block hash and a tx hash are both just hashes that resolve to a block
// number, so one structure serves both and the floor-to-head
// map[common.Hash]uint64 serve used to hold (measured 100.7 B/entry, 9.07GB
// at mainnet) is deleted. +1 entry per block on ~10 txs per block is a few
// percent of the index; a fingerprint collision BETWEEN the two kinds is
// harmless because the caller knows which kind it wants and verifies against
// the header or the body, costing one wasted read exactly as a same-kind
// collision already did.
//
// ponytail: the pairs stay on the heap (16 B each, ~240 MB for an 8M-tx epoch
// of a sparse chain, against a few GB for everything else the seal does).
// External-sort them too if a chain is ever seen at 100M blocks per epoch.
func buildEpochTxidx(walk func(func(fp, blk uint64) error) error, count, hint uint64) (idx, bloom []byte, err error) {
	type pair struct{ fp, blk uint64 }
	pairs := make([]pair, 0, hint)
	var got atomic.Uint64
	defer LogProgress("seal: txidx", func() string {
		return fmt.Sprintf("%d of %d fingerprints (tx + block hashes)", got.Load(), hint)
	})()
	if err := walk(func(fp, blk uint64) error {
		pairs = append(pairs, pair{fp: fp, blk: blk})
		got.Add(1)
		return nil
	}); err != nil {
		return nil, nil, err
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
	return out, buildTxBloom(fps), nil
}

// buildLogidx encodes position-agnostic posting lists. Layout:
//
//	nAddr u32 | addr entries (addr20 | listOff u64) |
//	nTopic u32 | topic entries (topic32 | listOff u64) |
//	lists blob (per list: EF over epoch-relative blocks, efMarshal)
//
// Entries sorted by key; listOff is relative to the lists blob.
//
// ponytail: the posting lists are built on the heap (one uint64 per logged
// address per block). That is bounded by the epoch's log volume, not by its
// block count, and measured a distant second to everything this file now
// streams; external-sort it if a log-heavy chain ever tops the profile.
func buildLogidx(walk func(func(LogRec) error) error, start, count uint64) ([]byte, error) {
	addrBlocks := map[[20]byte][]uint64{}
	topicBlocks := map[[32]byte][]uint64{}
	var blocks atomic.Uint64
	defer LogProgress("seal: logidx", func() string {
		return fmt.Sprintf("%d log-bearing blocks of the epoch's %d indexed", blocks.Load(), count)
	})()
	if err := walk(func(lr LogRec) error {
		blocks.Add(1)
		rel := lr.Block - start
		for _, a := range lr.Addrs {
			addrBlocks[a] = append(addrBlocks[a], rel)
		}
		for _, t := range lr.Topics {
			topicBlocks[t] = append(topicBlocks[t], rel)
		}
		return nil
	}); err != nil {
		return nil, err
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
	return append(out, lists...), nil
}

// efMarshal serializes an ef as: n u64 | l u32 | 4 length-prefixed word
// sections (lows, high, sel0, sel1).
func efMarshal(e *efBuild) []byte {
	out := binary.LittleEndian.AppendUint64(nil, uint64(e.n))
	out = binary.LittleEndian.AppendUint32(out, uint32(e.l))
	out = writeWords(out, e.lows.w)
	out = writeWords(out, e.high)
	out = writeWords(out, e.sel0)
	out = writeWords(out, e.sel1)
	return out
}

// efUnmarshal is the inverse: a reader over v, copying nothing.
func efUnmarshal(v *dist.View) (*ef, int64, error) {
	if v.Len() < 12 {
		return nil, 0, fmt.Errorf("ef: truncated header")
	}
	n := binary.LittleEndian.Uint64(v.Slice(0, 8))
	l := uint(binary.LittleEndian.Uint32(v.Slice(8, 4)))
	pos := int64(12)
	var secs [4]words
	var err error
	for i := range secs {
		if secs[i], pos, err = sliceWords(v, pos); err != nil {
			return nil, 0, err
		}
	}
	return &ef{
		n:    int(n),
		l:    l,
		lows: packed{w: secs[0], bits: l},
		high: secs[1],
		sel0: secs[2],
		sel1: secs[3],
	}, pos, nil
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

// buildBloomFile is the key bloom, over the unique keys the SST streamed to a
// scratch file: m is a function of the key COUNT, which is not known until the
// last row is written, and tens of millions of 53-byte keys waiting on the
// heap for that is 2 GB of pure latency. k = bloomHashes, double hashing over
// xxhash, OR-accumulated so read order cannot reach the bytes.
func buildBloomFile(path string, nKeys uint64) ([]byte, error) {
	m := bloomBits(nKeys, bloomBitsPerKey)
	words := make([]uint64, m/64)
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	r := bufio.NewReaderSize(f, 1<<20)
	var key [sortedKeySize]byte
	var done atomic.Uint64
	defer LogProgress("seal: keybloom", func() string {
		return fmt.Sprintf("%d of %d keys hashed", done.Load(), nKeys)
	})()
	for i := uint64(0); i < nKeys; i++ {
		if _, err := io.ReadFull(r, key[:]); err != nil {
			return nil, fmt.Errorf("epoch bloom: key %d of %d: %w", i, nKeys, err)
		}
		bloomSet(words, m, bloomHashes, key[:])
		done.Store(i + 1)
	}
	return encodeBloom(m, bloomHashes, words), nil
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

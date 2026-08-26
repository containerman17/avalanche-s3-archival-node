// Package store is epochdb STORAGE VERSION 0: a deterministic LSM of standard
// SSTables (DESIGN.md, "Storage v0").
//
// A RUN is one immutable file with three SST sections (chain, state, lookup)
// and a footer carrying the section offsets, the TxNum/height range and the
// previous run's name. Runs are named by their TxNum range; the manifest lists
// the live ones. The memtable covers the unflushed window and serves it.
//
// Everything below the row level is Pebble's sstable package used as a LIBRARY:
// no live key-value store runs anywhere.
package store

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strconv"

	"github.com/cockroachdb/pebble/v2/bloom"
	"github.com/cockroachdb/pebble/v2/sstable"
	sstblock "github.com/cockroachdb/pebble/v2/sstable/block"
)

// StorageVersion is recorded in the manifest and in every run's footer.
//
// IT IS 1 BECAUSE THE TxNum LAYOUT CHANGED: every block now owns a boundary
// slot, so the same TxNum names a different thing in a version 0 artifact than
// it does here. There is NO migration and no compatibility reader anywhere
// (DESIGN's clean-slate ruling); the bump exists so the handful of already
// published v0 prefixes are REFUSED BY NAME at join and at open instead of
// being read with v1 semantics and silently answering the wrong state.
const StorageVersion = 1

// ---------------------------------------------------------------------------
// THE ONE DIMENSION IS TxNum, AND EVERY BLOCK OWNS A SLOT OF ITS OWN.
//
// A block with `first` and `count` from its blk/ row occupies the slots
// [first, first+count]: its transactions take first .. first+count-1, and
// first+count is its BOUNDARY SLOT, which no transaction owns and which carries
// the state the block wrote outside any transaction (coreth's atomic transfers,
// a fork activation, block finalisation). The next block starts at
// first+count+1, so TxNum stays monotone, a run's [FromTx, ToTx) still tiles
// the space with no gap and routes a lookup by range check alone, and no two
// blocks can ever share a slot.
//
// The tx-keyed families are therefore DENSE PER BLOCK, not dense over the run:
// tx/, rcpt/ and itx/ have a row at every slot except the boundary ones.
//
// KEY SCHEMA (DESIGN, "Storage v0")
//
// Chain section, height/TxNum ordered, arrives sorted:
//
//	blk/<height>   -> first TxNum of the block (8B) + tx count (4B)
//	hdr/<height>   -> header RLP verbatim
//	itx/<txnum>    -> the tx's call frames in enter order with depth
//	pvm/<height>   -> proposervm wrapper bytes verbatim (empty pre-fork)
//	rcpt/<txnum>   -> receipt + full logs
//	tx/<txnum>     -> tx RLP verbatim
//
// State section, sorted by account/slot so it merges:
//
//	code/<codehash>            -> code blob
//	state/<addr>/a/<txnum>     -> account RLP ("" = deleted)
//	state/<addr>/c/<txnum>     -> code hash (32B)
//	state/<addr>/s/<slot>/<txnum> -> post-tx slot value ("" = cleared; the EVM
//	                                 defines cleared as zero, so there is no
//	                                 tombstone mechanism)
//
// Lookup section, arrives unsorted, sorted in memory at flush:
//
//	txh/<txhash>            -> txnum (8B)
//	blkh/<blockhash>        -> height (8B)
//	addr/<address>/<txnum>  -> role bits (1B)
//	logaddr/<address>/<txnum> -> log index within the tx (1B)
//	topic/<value>/<txnum>   -> topic position (1B)
//
// The <txnum> suffix is EIGHT BIG-ENDIAN BYTES everywhere, and Split() keeps it
// out of the prefix blooms (the CockroachDB MVCC pattern).
// ---------------------------------------------------------------------------

const (
	PrefixBlk  = "blk/"
	PrefixHdr  = "hdr/"
	PrefixItx  = "itx/"
	PrefixPvm  = "pvm/"
	PrefixRcpt = "rcpt/"
	PrefixTx   = "tx/"

	PrefixCode  = "code/"
	PrefixState = "state/"

	PrefixTxHash  = "txh/"
	PrefixBlkHash = "blkh/"
	PrefixAddr    = "addr/"
	PrefixLogAddr = "logaddr/"
	PrefixTopic   = "topic/"
)

// Role bits for an addr/ row.
const (
	RoleSender    byte = 1 << 0
	RoleRecipient byte = 1 << 1
	RoleCreated   byte = 1 << 2
	RoleEmitter   byte = 1 << 3
	// RoleFrame is "this address was a party to a call frame inside the tx",
	// which is the whole point of storing traces: history of address X stops
	// being sender/recipient/emitter only.
	RoleFrame byte = 1 << 4
)

func beU64(n uint64) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], n)
	return b[:]
}

func numKey(prefix string, n uint64) []byte {
	k := make([]byte, 0, len(prefix)+8)
	k = append(k, prefix...)
	return binary.BigEndian.AppendUint64(k, n)
}

// BlkKey ... TxKey are the chain-section keys.
func BlkKey(height uint64) []byte { return numKey(PrefixBlk, height) }
func HdrKey(height uint64) []byte { return numKey(PrefixHdr, height) }
func ItxKey(txnum uint64) []byte  { return numKey(PrefixItx, txnum) }
func PvmKey(height uint64) []byte { return numKey(PrefixPvm, height) }
func RcptKey(txnum uint64) []byte { return numKey(PrefixRcpt, txnum) }
func TxKey(txnum uint64) []byte   { return numKey(PrefixTx, txnum) }
func TxHashKey(h []byte) []byte   { return append([]byte(PrefixTxHash), h...) }
func CodeKey(hash []byte) []byte  { return append([]byte(PrefixCode), hash...) }

// BlkHashKey is the BLOCK-HASH INDEX row, blkh/<blockhash> -> height. It is the
// txh/ family's twin in every respect: a lookup-section row with no TxNum
// suffix, so split() reports the whole key and the bloom is a WHOLE-KEY bloom;
// a block hash lives in exactly one run, so nearly every probe is a miss, which
// is the filter's best case.
//
// The hash is never carried in from the executor: a block hash IS
// keccak256(header RLP) (libevm's Header.Hash is rlpHash), and the header RLP
// is stored verbatim, so the row is derived at write time from bytes the store
// already holds.
//
// REINDEX: adding this family is a new key prefix over data that is already
// stored, so it is an IO-class rebuild (DESIGN's storage versioning). Storage
// version stays 0 because no corpus is published yet; every local corpus is
// rebuilt from staging, clean slate.
func BlkHashKey(h []byte) []byte { return append([]byte(PrefixBlkHash), h...) }

// AccountPrefix is state/<addr>/a/ : the key range that is one account's
// history.
func AccountPrefix(addr []byte) []byte {
	k := make([]byte, 0, len(PrefixState)+20+3)
	k = append(k, PrefixState...)
	k = append(k, addr...)
	return append(k, '/', 'a', '/')
}

// CodeRefPrefix is state/<addr>/c/ : the account's code-hash history.
func CodeRefPrefix(addr []byte) []byte {
	k := make([]byte, 0, len(PrefixState)+20+3)
	k = append(k, PrefixState...)
	k = append(k, addr...)
	return append(k, '/', 'c', '/')
}

// SlotPrefix is state/<addr>/s/<slot>/ : the key range that IS the slot's time
// series, one contiguous scan.
func SlotPrefix(addr, slot []byte) []byte {
	k := make([]byte, 0, len(PrefixState)+20+3+32+1)
	k = append(k, PrefixState...)
	k = append(k, addr...)
	k = append(k, '/', 's', '/')
	k = append(k, slot...)
	return append(k, '/')
}

// AddrPrefix, LogAddrPrefix, TopicPrefix are the lookup posting-row prefixes.
func AddrPrefix(addr []byte) []byte    { return append(append([]byte(PrefixAddr), addr...), '/') }
func LogAddrPrefix(addr []byte) []byte { return append(append([]byte(PrefixLogAddr), addr...), '/') }
func TopicPrefix(topic []byte) []byte  { return append(append([]byte(PrefixTopic), topic...), '/') }

// Suffixed appends the 8-byte big-endian TxNum suffix to a prefix.
func Suffixed(prefix []byte, txnum uint64) []byte {
	return binary.BigEndian.AppendUint64(append([]byte(nil), prefix...), txnum)
}

// TxNumOf reads the trailing TxNum of a suffixed key.
func TxNumOf(key []byte) uint64 { return binary.BigEndian.Uint64(key[len(key)-8:]) }

// NumOf reads the trailing big-endian number of a chain-section key.
func NumOf(key []byte) uint64 { return binary.BigEndian.Uint64(key[len(key)-8:]) }

// ---------------------------------------------------------------------------
// The comparer: bytewise order with a Split that strips the TxNum suffix.
// ---------------------------------------------------------------------------

// split returns the length of key's bloom prefix. A family whose keys carry the
// TxNum suffix reports the key WITHOUT it, so "does this address/slot exist in
// this run" answers regardless of position; a family without a suffix (txh/,
// code/) reports the whole key and therefore gets a whole-key bloom. One
// function, both policies, exactly as DESIGN's bloom rule asks.
func split(key []byte) int {
	switch {
	case bytes.HasPrefix(key, []byte(PrefixState)):
		// state/<20>/x/... : the shape byte is at 6+20+1 == 27.
		if len(key) > 27 && key[27] == 's' {
			// state/<20>/s/<32>/<8>
			if len(key) >= 62+8 {
				return 62
			}
			return len(key)
		}
		// state/<20>/a/<8> or state/<20>/c/<8>
		if len(key) >= 29+8 {
			return 29
		}
		return len(key)
	case bytes.HasPrefix(key, []byte(PrefixAddr)):
		if len(key) >= 26+8 {
			return 26
		}
	case bytes.HasPrefix(key, []byte(PrefixLogAddr)):
		if len(key) >= 29+8 {
			return 29
		}
	case bytes.HasPrefix(key, []byte(PrefixTopic)):
		if len(key) >= 39+8 {
			return 39
		}
	}
	return len(key)
}

// Comparer is THE comparer, pinned as a format constant. Separator and
// Successor are the identity: index-key shortening buys a little index space
// and costs a rule that has to stay consistent with Split forever, and this
// engine writes runs once and reads them for years.
//
// It is built by COPYING DefaultComparer and moving four fields rather than by
// naming every field: pebble's Comparer grows members (suffix comparison, point
// vs range) and a literal silently leaves the new ones nil.
var Comparer = func() *sstable.Comparer {
	c := *sstable.DefaultComparer
	c.Split = split
	c.Separator = func(dst, a, b []byte) []byte { return append(dst, a...) }
	c.Successor = func(dst, a []byte) []byte { return append(dst, a...) }
	c.Name = "epochdb.v0"
	return &c
}()

// ---------------------------------------------------------------------------
// FORMAT CONSTANTS. Pinned, not configured: same sorted rows in, identical
// bytes out. Changing any of these is a storage version bump.
// ---------------------------------------------------------------------------

// TableFormat is pinned. Pebblev2 is the newest format with no value blocks and
// no obsolete bits, i.e. the fewest moving parts that still carries a
// table-level bloom. It is also below TableFormatPebblev5, so no columnar key
// schema is in play and the row-block writer is the only encoder.
const TableFormat = sstable.TableFormatPebblev2

// PebbleModule and PebbleVersion pin the sstable library LIKE FORMAT CONSTANTS
// (DESIGN's determinism pins). The store reads and writes through ONE
// implementation, pebble v2's sstable package. libevm's own ethdb/pebble
// wrapper still links pebble v1; that is a different module path and it never
// touches a run.
const (
	PebbleModule  = "github.com/cockroachdb/pebble/v2"
	PebbleVersion = "v2.1.6"
)

// ZstdLevel is the pinned zstd level for every section, measured then pinned on
// numine (DESIGN, "Storage v0"). Level 9 is where the size curve flattens: 19
// buys a couple of percent for several times the write clock.
const ZstdLevel = 9

// CgoRequired records that cgo IS a format constant here. pebble v2 reaches
// zstd through github.com/DataDog/zstd under `cgo && !pebblegozstd` and through
// github.com/klauspost/compress/zstd otherwise, and the two produce DIFFERENT
// BYTES from the same rows (klauspost also collapses levels onto four speed
// classes). epochdb is always cgo because firewood is, so the DataDog path is
// the one that runs; a `-tags pebblegozstd` build would write runs that are
// valid but not byte-identical to everyone else's. TestZstdPinnedProfile
// asserts the cgo side of the pin.
const CgoRequired = true

// zstdProfile is the pinned codec, and the construction is forced: the level
// type lives in pebble's internal/compression package, so a level can only be
// named by COPYING an exported profile and moving the field.
//
// IT MUST BE BASED ON ZstdCompression. GoodCompression, FastCompression and
// BalancedCompression set OtherBlocks to the fastest algorithm, which is MinLZ
// on amd64, and MinLZ needs TableFormatPebblev6; with an older format
// WriterOptions.ensureDefaults silently REPLACES the whole profile with Snappy,
// so a run would be snappy under a zstd label.
func zstdProfile(level uint8) *sstable.CompressionProfile {
	p := *sstable.ZstdCompression
	p.Name = "zstd" + strconv.Itoa(int(level))
	p.DataBlocks.Level = level
	p.ValueBlocks.Level = level
	p.OtherBlocks.Level = level
	return &p
}

// Compression is pinned: ZSTD AT LEVEL 9 IN ALL THREE SECTIONS, measured on the
// numine corpus. The old snappy pin was not a taste but a library fact (pebble
// v1 reached zstd through DataDog/zstd v1.5.2, whose Decompress always
// reallocates, which pebble rejects as a corrupt block); pebble v2 raises that
// module past the bug and the write-then-read-back round trip passes in the
// binary that wrote it.
var Compression = zstdProfile(ZstdLevel)

// L0Compression is the codec of an L0 run, and it is NOT a format constant:
// an L0 run is local (never uploaded, run.go's upload gate) and every row in
// it is re-encoded by the merge under Compression before anything leaves the
// machine, so the bytes anyone else sees never depend on it. It is picked for
// the executor's clock instead: the run cut runs on the executor goroutine
// and zstd-9 was 9.2s of a 17s cut (mainnet C, 2026-08-26, 500k-tx window).
// zstd-1, not none: an uncompressed block is served straight out of the mmap
// and TestMergeKeepsItsIOOutOfThePageCache saw 10% of a scanned run stay
// resident after Cold on CI with NoCompression (1% here, 5% is the bar).
var L0Compression = zstdProfile(1)

// Section identifies one of a run's three SST sections.
type Section int

const (
	SecChain Section = iota
	SecState
	SecLookup
	numSections
)

func (s Section) String() string {
	switch s {
	case SecChain:
		return "chain"
	case SecState:
		return "state"
	case SecLookup:
		return "lookup"
	}
	return "?"
}

// BlockSize is the pinned per-section block size, MEASURED THEN PINNED on
// numine: chain 128KB, state 32KB, lookup 8KB. EVERY SECTION IS A POINT-READ
// SECTION, chain included: `hdr/`, `blk/`, `tx/` and `rcpt/` are separate Gets
// landing in different blocks, there is no block cache by design, and a point
// read decompresses ONE WHOLE BLOCK. So the block size is the smallest that
// still compresses well, per family: 128KB for chain, whose rows are large and
// whose scans are long; 32KB for state, where a slot's history is a contiguous
// scan; 8KB for lookup, where every probe is one random row.
//
// The chain size is the paid-for trade: 512KB costs 2.6x the latency of a warm
// `eth_getTransactionByHash`-shaped read (~173us vs ~67us) and buys back ~2% of
// corpus bytes. Latency on a hot query shape wins.
func BlockSize(s Section) int {
	switch s {
	case SecChain:
		return 128 << 10
	case SecState:
		return 32 << 10
	}
	return 8 << 10
}

// indexBlockSize keeps the index one block deep for as long as it can: twice
// the data block size, never below 64KB. Past that pebble builds a two-level
// index by itself, which is a property of the block size and not a knob.
func indexBlockSize(s Section) int { return max(64<<10, 2*BlockSize(s)) }

// writerOptions is the pinned per-section profile. Chain carries NO bloom:
// existence there is answered by the run's tx range before any file opens.
func writerOptions(s Section, level int) sstable.WriterOptions {
	o := sstable.WriterOptions{
		Comparer:             Comparer,
		TableFormat:          TableFormat,
		Compression:          Compression,
		Checksum:             sstblock.ChecksumTypeCRC32c,
		BlockRestartInterval: 16,
		MergerName:           "nullptr",
		BlockSize:            BlockSize(s),
		IndexBlockSize:       indexBlockSize(s),
	}
	if s != SecChain {
		o.FilterPolicy = bloom.FilterPolicy(20)
		o.FilterType = sstable.TableFilter
	}
	if level < TerminalLevel {
		o.Compression = L0Compression
	}
	return o
}

// readerOptions must name the filter policy, or the reader never records where
// the filter block is and the bloom gate silently degrades to "may have
// everything". The merger needs no name here: pebble accepts "nullptr" as "no
// merge operator" without a registration.
func readerOptions() sstable.ReaderOptions {
	return sstable.ReaderOptions{
		Comparer: Comparer,
		Filters:  map[string]sstable.FilterPolicy{filterPolicy.Name(): filterPolicy},
	}
}

// filterPolicy is the one used by the sections that have a filter; it is also
// what probes the filter block on the read side (pebble's sstable.Reader has no
// point-get of its own, so the bloom gate is ours to apply).
var filterPolicy = bloom.FilterPolicy(20)

func mayContain(filter, key []byte) bool {
	if len(filter) == 0 {
		return true
	}
	return filterPolicy.MayContain(sstable.TableFilter, filter, key[:split(key)])
}

var errNotFound = fmt.Errorf("store: not found")

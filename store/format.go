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

	"github.com/cockroachdb/pebble/bloom"
	"github.com/cockroachdb/pebble/sstable"
)

// StorageVersion is recorded in the manifest. A version bump plus a reindex
// pass is how a new index family arrives (DESIGN, storage versioning).
const StorageVersion = 0

// ---------------------------------------------------------------------------
// KEY SCHEMA (DESIGN, "Storage v0")
//
// Chain section, height/TxNum ordered, arrives sorted:
//
//	blk/<height>   -> first TxNum of the block (8B) + tx count (4B)
//	hdr/<height>   -> header RLP verbatim
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
	PrefixPvm  = "pvm/"
	PrefixRcpt = "rcpt/"
	PrefixTx   = "tx/"

	PrefixCode  = "code/"
	PrefixState = "state/"

	PrefixTxHash  = "txh/"
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
func BlkKey(height uint64) []byte  { return numKey(PrefixBlk, height) }
func HdrKey(height uint64) []byte  { return numKey(PrefixHdr, height) }
func PvmKey(height uint64) []byte  { return numKey(PrefixPvm, height) }
func RcptKey(txnum uint64) []byte  { return numKey(PrefixRcpt, txnum) }
func TxKey(txnum uint64) []byte    { return numKey(PrefixTx, txnum) }
func TxHashKey(h []byte) []byte    { return append([]byte(PrefixTxHash), h...) }
func CodeKey(hash []byte) []byte   { return append([]byte(PrefixCode), hash...) }

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
var Comparer = &sstable.Comparer{
	Compare:        bytes.Compare,
	Equal:          bytes.Equal,
	AbbreviatedKey: sstable.DefaultComparer.AbbreviatedKey,
	FormatKey:      sstable.DefaultComparer.FormatKey,
	Separator:      func(dst, a, b []byte) []byte { return append(dst, a...) },
	Successor:      func(dst, a []byte) []byte { return append(dst, a...) },
	Split:          split,
	Name:           "epochdb.v0",
}

// ---------------------------------------------------------------------------
// FORMAT CONSTANTS. Pinned, not configured: same sorted rows in, identical
// bytes out. Changing any of these is a storage version bump.
// ---------------------------------------------------------------------------

// TableFormat is pinned. Pebblev2 is the newest format with no value blocks and
// no obsolete bits, i.e. the fewest moving parts that still carries a
// table-level bloom.
const TableFormat = sstable.TableFormatPebblev2

// PebbleVersion is pinned LIKE A FORMAT CONSTANT (DESIGN's determinism pins),
// and the pin is not ours to choose freely: libevm's ethdb/pebble wrapper is
// written against this exact API, and a newer pebble stops the whole repo from
// compiling. Bumping it means bumping libevm too, and it is a storage version
// question either way.
const PebbleVersion = "v0.0.0-20230928194634-aa077af62593"

// Compression is pinned, and it is SNAPPY rather than the zstd DESIGN expects
// to win, for a reason that is a property of the libraries and not a taste:
//
// pebble's zstd path is `github.com/DataDog/zstd`, and avalanchego pins that
// module at v1.5.2, where Decompress sizes its own destination buffer from a
// >= 1MB hint and therefore ALWAYS reallocates. pebble then rejects the block
// ("decompressed into unexpected buffer"), so every zstd-compressed run is
// unreadable by the same binary that wrote it. On top of that, pebble picks
// DataDog under cgo and klauspost without it, and the two produce DIFFERENT
// BYTES from the same rows, which the byte-identity promise forbids outright.
// Snappy is pure Go, one code path, deterministic everywhere.
//
// The codec is a format constant, so moving to zstd later is a storage version
// bump plus an IO-class reindex, which is exactly the upgrade path DESIGN
// already provides. It needs pebble v2 (a different module path, so it can be
// linked beside the one libevm needs; its zstd is a rewritten package with a
// `pebblegozstd` escape hatch) plus the measurement pass DESIGN asks for.
const Compression = sstable.SnappyCompression

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

// writerOptions is the pinned per-section profile. Chain is write-once and read
// sequentially, so it takes big blocks, a high-ratio codec and NO bloom:
// existence there is answered by the run's tx range before any file opens.
// State and lookup are point-read families: small blocks and blooms.
func writerOptions(s Section) sstable.WriterOptions {
	o := sstable.WriterOptions{
		Comparer:             Comparer,
		TableFormat:          TableFormat,
		Compression:          Compression,
		Checksum:             sstable.ChecksumTypeCRC32c,
		BlockRestartInterval: 16,
		MergerName:           "nullptr",
	}
	switch s {
	case SecChain:
		o.BlockSize = 128 << 10
		o.IndexBlockSize = 256 << 10
	case SecState:
		o.BlockSize = 16 << 10
		o.IndexBlockSize = 64 << 10
		o.FilterPolicy = bloom.FilterPolicy(20)
		o.FilterType = sstable.TableFilter
	case SecLookup:
		o.BlockSize = 8 << 10
		o.IndexBlockSize = 64 << 10
		o.FilterPolicy = bloom.FilterPolicy(20)
		o.FilterType = sstable.TableFilter
	}
	return o
}

// readerOptions must name the filter policy, or the reader never records where
// the filter block is and the bloom gate silently degrades to "may have
// everything".
func readerOptions() sstable.ReaderOptions {
	return sstable.ReaderOptions{
		Comparer:   Comparer,
		MergerName: "nullptr",
		Filters:    map[string]sstable.FilterPolicy{filterPolicy.Name(): filterPolicy},
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

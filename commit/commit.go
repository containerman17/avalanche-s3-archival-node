// Package commit computes Ethereum state roots (secure Merkle Patricia trie,
// geth semantics) from a sorted flat latest state.
//
// Roll writes the trie's INTERNAL nodes (branches and extensions) to one
// immutable file in a single sequential pass; leaves are never stored, they
// are the flat rows. Between rolls, Dirty recomputes the root from an
// in-memory overlay of changed nodes over that file.
//
// Contract (shared with package latest): keys are keccak(addr)+0x00 for an
// account and keccak(addr)+0x01+keccak(slot) for a slot, sorted ascending
// bytewise. An account value is RLP[nonce, balance, codeHash] (no storage
// root: this package computes it); a slot value is the 32-byte word with
// leading zeros trimmed. An empty value never reaches Roll; in Dirty.Apply
// an empty value is a delete.
package commit

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/rlp"
	"github.com/ava-labs/libevm/trie"
	"github.com/holiman/uint256"
)

// Iterator is the sorted flat state. Key and Value are valid until the next
// Next.
type Iterator interface {
	Next() bool
	Key() []byte
	Value() []byte
	Err() error
}

// accountRow is the contract's account value.
type accountRow struct {
	Nonce    uint64
	Balance  *uint256.Int
	CodeHash []byte
}

// accountLeaf is the account trie's leaf value: geth's StateAccount without
// libevm's registered-extras payload, so the encoding never depends on which
// VM this process registered.
type accountLeaf struct {
	Nonce    uint64
	Balance  *uint256.Int
	Root     common.Hash
	CodeHash []byte
}

// File layout (all integers little-endian):
//
//	header: magic (8)
//	nodes:  tag (1: branch, 2: extension), uvarint blob length, blob,
//	        then 16 uvarint child offsets (branch) or 1 (extension);
//	        0 means the child is absent, embedded in the parent blob, or a leaf
//	index:  indexCount x {account hash 32, storage root 32, root offset 8},
//	        sorted by account hash, one entry per account whose storage root
//	        is an internal node
//	footer: magic 8, version 4, root offset 8, root hash 32, node count 8,
//	        key count 8, index offset 8, index count 8, user data 32, crc32 4
const (
	magic       = "EPCHCMT1"
	version     = 1
	tagBranch   = 1
	tagExt      = 2
	indexEntry  = 72
	footerSize  = 8 + 4 + 8 + 32 + 8 + 8 + 8 + 8 + 32 + 4
	userDataLen = 32
)

// Stats is what a Roll counted.
type Stats struct {
	Keys  uint64 // leaves fed to the tries (one keccak each)
	Nodes uint64 // internal nodes written (one keccak each)
	Bytes int64  // file size
}

// Roll builds every storage trie and the account trie from it in one pass,
// writes the internal nodes to path, and returns the state root. userData is
// stored verbatim in the footer (32 bytes, e.g. a height).
func Roll(it Iterator, path string, userData [32]byte) (common.Hash, Stats, error) {
	f, err := os.Create(path)
	if err != nil {
		return common.Hash{}, Stats{}, err
	}
	idxf, err := os.Create(path + ".idx")
	if err != nil {
		f.Close()
		return common.Hash{}, Stats{}, err
	}
	defer os.Remove(path + ".idx")
	defer idxf.Close()
	w := &writer{w: bufio.NewWriterSize(f, 1<<20), idx: bufio.NewWriterSize(idxf, 1<<20), idxf: idxf}
	w.w.WriteString(magic)
	w.off = int64(len(magic))

	root, err := w.roll(it)
	if err == nil {
		err = w.finish(userData)
	}
	if err == nil {
		err = w.w.Flush()
	}
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		os.Remove(path)
		return common.Hash{}, Stats{}, err
	}
	return root, Stats{Keys: w.keys, Nodes: w.nodes, Bytes: w.off}, nil
}

type writer struct {
	w     *bufio.Writer
	idx   *bufio.Writer
	idxf  *os.File
	off   int64
	keys  uint64
	nodes uint64
	idxN  uint64

	// pending maps a written node's path to its offset until its parent is
	// written; post-order emission keeps it at most one branch per level.
	accPending, stPending map[string]int64
	rootOff               int64
	rootHash              common.Hash
	scratch               []byte
	var32                 [binary.MaxVarintLen64]byte
}

// emit is the StackTrie writer callback. Leaves are skipped; the callback is
// only reached for hashed nodes (>= 32 bytes), so every offset recorded here
// is one the parent references by hash.
func (w *writer) emit(pending map[string]int64, path []byte, blob []byte) {
	content, _, err := rlp.SplitList(blob)
	if err != nil {
		panic("commit: stacktrie emitted a non-list node: " + err.Error())
	}
	n, _ := rlp.CountValues(content)
	var children [16]int64
	tag := byte(tagExt)
	child := append(w.scratch[:0], path...)
	switch n {
	case 17:
		tag = tagBranch
		for i := range 16 {
			child = append(child[:len(path)], byte(i))
			children[i] = pending[string(child)]
			delete(pending, string(child))
		}
	case 2:
		key, _, _ := rlp.SplitString(content)
		if key[0]&0x20 != 0 {
			return // leaf
		}
		child = append(child, compactToHex(key)...)
		children[0] = pending[string(child)]
		delete(pending, string(child))
	default:
		panic("commit: stacktrie emitted a node with an unexpected arity")
	}
	w.scratch = child
	off := w.off
	w.put([]byte{tag})
	w.putVarint(uint64(len(blob)))
	w.put(blob)
	if tag == tagBranch {
		for i := range 16 {
			w.putVarint(uint64(children[i]))
		}
	} else {
		w.putVarint(uint64(children[0]))
	}
	w.nodes++
	if len(path) == 0 {
		w.rootOff = off
		return
	}
	pending[string(path)] = off
}

func (w *writer) put(b []byte) {
	w.w.Write(b)
	w.off += int64(len(b))
}

func (w *writer) putVarint(v uint64) {
	n := binary.PutUvarint(w.var32[:], v)
	w.put(w.var32[:n])
}

func (w *writer) roll(it Iterator) (common.Hash, error) {
	w.accPending = map[string]int64{}
	w.stPending = map[string]int64{}
	acc := trie.NewStackTrie(trie.NewStackTrieOptions().WithWriter(func(path []byte, _ common.Hash, blob []byte) {
		w.emit(w.accPending, path, blob)
	}))
	stOpts := trie.NewStackTrieOptions().WithWriter(func(path []byte, _ common.Hash, blob []byte) {
		w.emit(w.stPending, path, blob)
	})
	var (
		cur      accountLeaf
		curHash  common.Hash
		have     bool
		st       *trie.StackTrie
		idxEntry [indexEntry]byte
	)
	flush := func() error {
		if !have {
			return nil
		}
		cur.Root = types.EmptyRootHash
		if st != nil {
			w.rootOff = 0
			cur.Root = st.Hash()
			if w.rootOff != 0 {
				copy(idxEntry[:32], curHash[:])
				copy(idxEntry[32:64], cur.Root[:])
				binary.LittleEndian.PutUint64(idxEntry[64:], uint64(w.rootOff))
				w.idx.Write(idxEntry[:])
				w.idxN++
			}
			clear(w.stPending)
			st = nil
		}
		val, err := rlp.EncodeToBytes(&cur)
		if err != nil {
			return err
		}
		w.keys++
		return acc.Update(curHash[:], val)
	}
	for it.Next() {
		k := it.Key()
		switch {
		case len(k) == 33 && k[32] == 0:
			if err := flush(); err != nil {
				return common.Hash{}, err
			}
			var row accountRow
			if err := rlp.DecodeBytes(it.Value(), &row); err != nil {
				return common.Hash{}, fmt.Errorf("commit: account %x: %w", k[:32], err)
			}
			cur = accountLeaf{Nonce: row.Nonce, Balance: row.Balance, CodeHash: row.CodeHash}
			copy(curHash[:], k[:32])
			have = true
		case len(k) == 65 && k[32] == 1:
			if !have || !bytes.Equal(k[:32], curHash[:]) {
				return common.Hash{}, fmt.Errorf("commit: slot row %x has no account row", k)
			}
			if st == nil {
				st = trie.NewStackTrie(stOpts)
			}
			val, err := rlp.EncodeToBytes(it.Value())
			if err != nil {
				return common.Hash{}, err
			}
			w.keys++
			if err := st.Update(k[33:], val); err != nil {
				return common.Hash{}, fmt.Errorf("commit: slot %x: %w", k, err)
			}
		default:
			return common.Hash{}, fmt.Errorf("commit: malformed key %x", k)
		}
	}
	if err := it.Err(); err != nil {
		return common.Hash{}, err
	}
	if err := flush(); err != nil {
		return common.Hash{}, err
	}
	w.rootOff = 0
	w.rootHash = acc.Hash()
	return w.rootHash, nil
}

func (w *writer) finish(userData [32]byte) error {
	rootOff := w.rootOff
	// The index was streamed to a side file so a roll never holds it in RAM;
	// it is appended here, after the last node.
	if err := w.idx.Flush(); err != nil {
		return err
	}
	if _, err := w.idxf.Seek(0, io.SeekStart); err != nil {
		return err
	}
	idxOff := w.off
	n, err := io.Copy(w.w, w.idxf)
	if err != nil {
		return err
	}
	w.off += n
	var ft [footerSize]byte
	b := ft[:0]
	b = append(b, magic...)
	b = binary.LittleEndian.AppendUint32(b, version)
	b = binary.LittleEndian.AppendUint64(b, uint64(rootOff))
	b = append(b, w.rootHash[:]...)
	b = binary.LittleEndian.AppendUint64(b, w.nodes)
	b = binary.LittleEndian.AppendUint64(b, w.keys)
	b = binary.LittleEndian.AppendUint64(b, uint64(idxOff))
	b = binary.LittleEndian.AppendUint64(b, w.idxN)
	b = append(b, userData[:]...)
	b = binary.LittleEndian.AppendUint32(b, crc32.ChecksumIEEE(b))
	w.put(b)
	return nil
}

// footer is the parsed trailer of a node file.
type footer struct {
	rootOff, idxOff   int64
	root              common.Hash
	nodes, keys, idxN uint64
	user              [32]byte
}

var errCorrupt = errors.New("commit: not a commit node file or corrupted footer")

func parseFooter(b []byte) (footer, error) {
	var f footer
	if len(b) != footerSize || string(b[:8]) != magic || binary.LittleEndian.Uint32(b[8:]) != version {
		return f, errCorrupt
	}
	if crc32.ChecksumIEEE(b[:footerSize-4]) != binary.LittleEndian.Uint32(b[footerSize-4:]) {
		return f, errCorrupt
	}
	f.rootOff = int64(binary.LittleEndian.Uint64(b[12:]))
	copy(f.root[:], b[20:52])
	f.nodes = binary.LittleEndian.Uint64(b[52:])
	f.keys = binary.LittleEndian.Uint64(b[60:])
	f.idxOff = int64(binary.LittleEndian.Uint64(b[68:]))
	f.idxN = binary.LittleEndian.Uint64(b[76:])
	copy(f.user[:], b[84:116])
	return f, nil
}

// compactToHex is trie's hex-prefix decoding: compact key bytes to nibbles,
// terminator flag dropped.
func compactToHex(compact []byte) []byte {
	nib := make([]byte, 0, len(compact)*2)
	for _, b := range compact {
		nib = append(nib, b>>4, b&15)
	}
	if nib[0]&1 == 1 {
		return nib[1:]
	}
	return nib[2:]
}

// hexToCompact is the inverse, with the terminator flag set for a leaf.
func hexToCompact(nibbles []byte, leaf bool) []byte {
	var flag byte
	if leaf {
		flag = 0x20
	}
	out := make([]byte, 0, len(nibbles)/2+1)
	if len(nibbles)%2 == 1 {
		out = append(out, flag|0x10|nibbles[0])
		nibbles = nibbles[1:]
	} else {
		out = append(out, flag)
	}
	for i := 0; i < len(nibbles); i += 2 {
		out = append(out, nibbles[i]<<4|nibbles[i+1])
	}
	return out
}

// packNibbles is the inverse of keyToNibbles; an odd tail is padded with 0.
func packNibbles(nib []byte) []byte {
	out := make([]byte, (len(nib)+1)/2)
	for i, n := range nib {
		out[i/2] |= n << (4 * uint(1-i%2))
	}
	return out
}

// keyToNibbles expands key bytes to nibbles, no terminator.
func keyToNibbles(key []byte) []byte {
	nib := make([]byte, 0, len(key)*2)
	for _, b := range key {
		nib = append(nib, b>>4, b&15)
	}
	return nib
}

package commit

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"sort"
	"syscall"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/rlp"
)

// File is an opened, mmapped node file.
type File struct {
	m  []byte
	ft footer
}

// Open mmaps a node file written by Roll and checks its footer.
func Open(path string) (*File, error) {
	fd, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer fd.Close()
	st, err := fd.Stat()
	if err != nil {
		return nil, err
	}
	size := st.Size()
	if size < int64(len(magic))+footerSize {
		return nil, errCorrupt
	}
	m, err := syscall.Mmap(int(fd.Fd()), 0, int(size), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, err
	}
	ft, err := parseFooter(m[size-footerSize:])
	if err == nil && (string(m[:8]) != magic || ft.rootOff >= size || ft.idxOff < int64(len(magic)) ||
		ft.idxOff+int64(ft.idxN)*indexEntry != size-footerSize) {
		err = errCorrupt
	}
	if err != nil {
		syscall.Munmap(m)
		return nil, fmt.Errorf("%w: %s", err, path)
	}
	return &File{m: m, ft: ft}, nil
}

// Close unmaps the file.
func (f *File) Close() error { return syscall.Munmap(f.m) }

// Root is the state root the file was rolled at.
func (f *File) Root() common.Hash { return f.ft.root }

// UserData is the 32-byte field Roll was given.
func (f *File) UserData() [32]byte { return f.ft.user }

// NodeCount is the number of internal nodes in the file; KeyCount the number
// of leaves the roll hashed; Size the file size in bytes.
func (f *File) NodeCount() uint64 { return f.ft.nodes }
func (f *File) KeyCount() uint64  { return f.ft.keys }
func (f *File) Size() int64       { return int64(len(f.m)) }

// StorageRoot is the storage root of an account at the roll: EmptyRootHash
// when the account had no storage, and ok=false when the root is a leaf (one
// slot) rather than an internal node, in which case the hash is unknown to
// the file and the caller derives it from the flat row.
func (f *File) StorageRoot(acct common.Hash) (common.Hash, bool) {
	e := f.index(acct)
	if e == nil {
		return types.EmptyRootHash, false
	}
	return common.BytesToHash(e[32:64]), true
}

// index returns the index entry of acct, nil if absent.
func (f *File) index(acct common.Hash) []byte {
	idx := f.m[f.ft.idxOff : f.ft.idxOff+int64(f.ft.idxN)*indexEntry]
	n := int(f.ft.idxN)
	i := sort.Search(n, func(i int) bool { return bytes.Compare(idx[i*indexEntry:i*indexEntry+32], acct[:]) >= 0 })
	if i == n || !bytes.Equal(idx[i*indexEntry:i*indexEntry+32], acct[:]) {
		return nil
	}
	return idx[i*indexEntry : (i+1)*indexEntry]
}

// Node returns the RLP blob of the internal node at path (nibbles, as the
// trie package's Reader is asked) in the trie of owner (the zero hash for the
// account trie, else the account hash). ok=false when nothing is stored
// there: an absent, embedded, or leaf node. The blob aliases the mmap.
func (f *File) Node(owner common.Hash, path []byte) ([]byte, bool) {
	off := f.ft.rootOff
	if owner != (common.Hash{}) {
		e := f.index(owner)
		if e == nil {
			return nil, false
		}
		off = int64(binary.LittleEndian.Uint64(e[64:]))
	}
	for off != 0 {
		tag := f.m[off]
		n, k := binary.Uvarint(f.m[off+1:])
		blob := f.m[off+1+int64(k) : off+1+int64(k)+int64(n)]
		rest := f.m[off+1+int64(k)+int64(n):]
		if len(path) == 0 {
			return blob, true
		}
		switch tag {
		case tagBranch:
			for i := 0; i < int(path[0]); i++ {
				_, k := binary.Uvarint(rest)
				rest = rest[k:]
			}
			child, _ := binary.Uvarint(rest)
			off = int64(child)
			path = path[1:]
		case tagExt:
			content, _, _ := rlp.SplitList(blob)
			key, _, _ := rlp.SplitString(content)
			nib := compactToHex(key)
			if !bytes.HasPrefix(path, nib) {
				return nil, false
			}
			child, _ := binary.Uvarint(rest)
			off = int64(child)
			path = path[len(nib):]
		default:
			return nil, false
		}
	}
	return nil, false
}

package exec

import (
	"encoding/binary"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/state"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/ethdb"
	"github.com/ava-labs/libevm/rlp"
	"github.com/ava-labs/libevm/trie"
	"github.com/ava-labs/libevm/trie/trienode"
	"github.com/ava-labs/libevm/triedb"
)

// Write-capture record kinds. Unlike the reference (old-value changesets),
// we record post-images: the NEW value being written. A deleted account or
// slot is an explicit zero-length value record.
const (
	kindAccount byte = 'a' // [kind][addr 20][uvarint vlen][account RLP]
	kindStorage byte = 's' // [kind][addr 20][slot key 32][uvarint vlen][value RLP]
	kindCodeUse byte = 'c' // [kind][addr 20][code hash 32][uvarint 0]
)

// blockFrame accumulates one block's write records.
type blockFrame struct {
	buf []byte
	n   int // record count
}

func (f *blockFrame) recordAccount(addr common.Address, valRLP []byte) {
	f.buf = append(f.buf, kindAccount)
	f.buf = append(f.buf, addr[:]...)
	f.buf = binary.AppendUvarint(f.buf, uint64(len(valRLP)))
	f.buf = append(f.buf, valRLP...)
	f.n++
}

func (f *blockFrame) recordStorage(addr common.Address, key []byte, valRLP []byte) {
	f.buf = append(f.buf, kindStorage)
	f.buf = append(f.buf, addr[:]...)
	var slot common.Hash
	copy(slot[:], key)
	f.buf = append(f.buf, slot[:]...)
	f.buf = binary.AppendUvarint(f.buf, uint64(len(valRLP)))
	f.buf = append(f.buf, valRLP...)
	f.n++
}

func (f *blockFrame) recordCodeUse(addr common.Address, codeHash common.Hash) {
	f.buf = append(f.buf, kindCodeUse)
	f.buf = append(f.buf, addr[:]...)
	f.buf = append(f.buf, codeHash[:]...)
	f.buf = binary.AppendUvarint(f.buf, 0)
	f.n++
}

// codeSink receives every contract code blob at write time.
type codeSink interface {
	PutCode(hash common.Hash, blob []byte) error
}

// wrapDatabase wraps an inner state.Database so that every account,
// storage, and contract-code write passes through a trie interceptor that
// records post-images into the current blockFrame. A nil frame disables
// recording (genesis commit).
func wrapDatabase(inner state.Database, code codeSink) *wrappedDatabase {
	return &wrappedDatabase{inner: inner, code: code}
}

type wrappedDatabase struct {
	inner state.Database
	code  codeSink
	frame *blockFrame
}

func (d *wrappedDatabase) setFrame(f *blockFrame) { d.frame = f }

func (d *wrappedDatabase) OpenTrie(root common.Hash) (state.Trie, error) {
	t, err := d.inner.OpenTrie(root)
	if err != nil {
		return nil, err
	}
	return &wrappingTrie{inner: t, db: d}, nil
}

func (d *wrappedDatabase) OpenStorageTrie(stateRoot common.Hash, addr common.Address, root common.Hash, parent state.Trie) (state.Trie, error) {
	innerParent := parent
	if w, ok := parent.(*wrappingTrie); ok {
		innerParent = w.inner
	}
	t, err := d.inner.OpenStorageTrie(stateRoot, addr, root, innerParent)
	if err != nil {
		return nil, err
	}
	return &wrappingTrie{inner: t, db: d}, nil
}

func (d *wrappedDatabase) CopyTrie(t state.Trie) state.Trie {
	inner := t
	if w, ok := t.(*wrappingTrie); ok {
		inner = w.inner
	}
	return &wrappingTrie{inner: d.inner.CopyTrie(inner), db: d}
}

func (d *wrappedDatabase) ContractCode(addr common.Address, codeHash common.Hash) ([]byte, error) {
	return d.inner.ContractCode(addr, codeHash)
}

func (d *wrappedDatabase) ContractCodeSize(addr common.Address, codeHash common.Hash) (int, error) {
	return d.inner.ContractCodeSize(addr, codeHash)
}

func (d *wrappedDatabase) DiskDB() ethdb.KeyValueStore { return d.inner.DiskDB() }
func (d *wrappedDatabase) TrieDB() *triedb.Database    { return d.inner.TrieDB() }

// wrappingTrie intercepts the write methods to capture post-images; all
// other methods pass through.
type wrappingTrie struct {
	inner state.Trie
	db    *wrappedDatabase
}

func (t *wrappingTrie) UpdateAccount(addr common.Address, acc *types.StateAccount) error {
	if f := t.db.frame; f != nil {
		valRLP, err := rlp.EncodeToBytes(acc)
		if err != nil {
			return err
		}
		f.recordAccount(addr, valRLP)
	}
	return t.inner.UpdateAccount(addr, acc)
}

func (t *wrappingTrie) DeleteAccount(addr common.Address) error {
	if f := t.db.frame; f != nil {
		f.recordAccount(addr, nil)
	}
	return t.inner.DeleteAccount(addr)
}

func (t *wrappingTrie) UpdateStorage(addr common.Address, key, value []byte) error {
	if f := t.db.frame; f != nil {
		f.recordStorage(addr, key, value)
	}
	return t.inner.UpdateStorage(addr, key, value)
}

func (t *wrappingTrie) DeleteStorage(addr common.Address, key []byte) error {
	if f := t.db.frame; f != nil {
		f.recordStorage(addr, key, nil)
	}
	return t.inner.DeleteStorage(addr, key)
}

func (t *wrappingTrie) UpdateContractCode(addr common.Address, codeHash common.Hash, code []byte) error {
	if f := t.db.frame; f != nil {
		f.recordCodeUse(addr, codeHash)
	}
	// Capture the blob at write time. The commit-phase rawdb.WriteCode
	// lands in the same code store via the ethdb adapter; both paths
	// dedup by hash.
	if err := t.db.code.PutCode(codeHash, code); err != nil {
		return err
	}
	return t.inner.UpdateContractCode(addr, codeHash, code)
}

// --- pure delegators --------------------------------------------------------

func (t *wrappingTrie) GetKey(k []byte) []byte { return t.inner.GetKey(k) }

func (t *wrappingTrie) GetAccount(addr common.Address) (*types.StateAccount, error) {
	return t.inner.GetAccount(addr)
}

func (t *wrappingTrie) GetStorage(addr common.Address, key []byte) ([]byte, error) {
	return t.inner.GetStorage(addr, key)
}

func (t *wrappingTrie) Hash() common.Hash { return t.inner.Hash() }

func (t *wrappingTrie) Commit(collectLeaf bool) (common.Hash, *trienode.NodeSet, error) {
	return t.inner.Commit(collectLeaf)
}

func (t *wrappingTrie) NodeIterator(startKey []byte) (trie.NodeIterator, error) {
	return t.inner.NodeIterator(startKey)
}

func (t *wrappingTrie) Prove(key []byte, proofDb ethdb.KeyValueWriter) error {
	return t.inner.Prove(key, proofDb)
}

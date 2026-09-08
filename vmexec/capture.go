package vmexec

import (
	"github.com/containerman17/avalanche-s3-archival-node/store"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/state"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/ethdb"
	"github.com/ava-labs/libevm/rlp"
	"github.com/ava-labs/libevm/trie"
	"github.com/ava-labs/libevm/trie/trienode"
	"github.com/ava-labs/libevm/triedb"
)

// STATE CAPTURE IS PER TRANSACTION, exactly as in exec/capture.go: the same
// trie interceptor, drained at every tx boundary, recording post-images into
// the store's rows. This copy drops what was Firewood's (the Go read cache,
// batch mode, the mid-block root short-circuit): the inner database is now
// flatDB, whose account trie's Hash is already the placeholder.
type capture struct {
	rows []store.StateRow
	code map[string][]byte
}

func (c *capture) recordAccount(addr common.Address, valRLP []byte) {
	c.rows = append(c.rows, store.StateRow{Kind: 'a', Addr: addr[:], Val: valRLP})
}

// recordStorage takes the RAW LEFT-TRIMMED slot value libevm hands the trie.
func (c *capture) recordStorage(addr common.Address, key []byte, val []byte) {
	var slot common.Hash
	copy(slot[:], key)
	c.rows = append(c.rows, store.StateRow{Kind: 's', Addr: addr[:], Slot: slot[:], Val: val})
}

func (c *capture) recordCodeUse(addr common.Address, codeHash common.Hash) {
	c.rows = append(c.rows, store.StateRow{Kind: 'c', Addr: addr[:], Val: codeHash[:]})
}

// take hands over the rows drained since the last take and starts a new run.
func (c *capture) take() []store.StateRow {
	rows := c.rows
	c.rows = nil
	return rows
}

// wrapDatabase wraps the inner state.Database so that every account,
// storage, and contract-code write passes through a trie interceptor that
// records post-images into the current capture. A nil capture disables
// recording.
func wrapDatabase(inner state.Database) *wrappedDatabase {
	return &wrappedDatabase{inner: inner, recent: map[string][]byte{}}
}

type wrappedDatabase struct {
	inner state.Database
	cap   *capture

	// recent holds contract code whose rows have NOT reached the state layer
	// yet (the open block): a later transaction calling a contract deployed a
	// moment ago would otherwise read straight past it. Cleared by
	// forgetRecentCode once the block's rows are in.
	recent map[string][]byte
}

func (d *wrappedDatabase) setCapture(c *capture) { d.cap = c }

func (d *wrappedDatabase) forgetRecentCode() { clear(d.recent) }

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

// ContractCode answers from the OPEN BLOCK'S OWN CODE FIRST (see recent).
func (d *wrappedDatabase) ContractCode(addr common.Address, codeHash common.Hash) ([]byte, error) {
	if blob, ok := d.recent[string(codeHash[:])]; ok {
		return blob, nil
	}
	return d.inner.ContractCode(addr, codeHash)
}

func (d *wrappedDatabase) ContractCodeSize(addr common.Address, codeHash common.Hash) (int, error) {
	if blob, ok := d.recent[string(codeHash[:])]; ok {
		return len(blob), nil
	}
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
	if f := t.db.cap; f != nil {
		valRLP, err := rlp.EncodeToBytes(acc)
		if err != nil {
			return err
		}
		f.recordAccount(addr, valRLP)
	}
	return t.inner.UpdateAccount(addr, acc)
}

func (t *wrappingTrie) DeleteAccount(addr common.Address) error {
	if f := t.db.cap; f != nil {
		f.recordAccount(addr, nil)
	}
	return t.inner.DeleteAccount(addr)
}

func (t *wrappingTrie) UpdateStorage(addr common.Address, key, value []byte) error {
	if f := t.db.cap; f != nil {
		f.recordStorage(addr, key, value)
	}
	return t.inner.UpdateStorage(addr, key, value)
}

func (t *wrappingTrie) DeleteStorage(addr common.Address, key []byte) error {
	if f := t.db.cap; f != nil {
		f.recordStorage(addr, key, nil)
	}
	return t.inner.DeleteStorage(addr, key)
}

func (t *wrappingTrie) UpdateContractCode(addr common.Address, codeHash common.Hash, code []byte) error {
	if f := t.db.cap; f != nil {
		f.recordCodeUse(addr, codeHash)
	}
	if c := t.db.cap; c != nil {
		if _, ok := c.code[string(codeHash[:])]; !ok {
			blob := append([]byte(nil), code...)
			c.code[string(codeHash[:])] = blob
			t.db.recent[string(codeHash[:])] = blob
		}
	}
	return t.inner.UpdateContractCode(addr, codeHash, code)
}

func (t *wrappingTrie) GetAccount(addr common.Address) (*types.StateAccount, error) {
	return t.inner.GetAccount(addr)
}

func (t *wrappingTrie) GetStorage(addr common.Address, key []byte) ([]byte, error) {
	return t.inner.GetStorage(addr, key)
}

func (t *wrappingTrie) GetKey(k []byte) []byte { return t.inner.GetKey(k) }
func (t *wrappingTrie) Hash() common.Hash      { return t.inner.Hash() }

func (t *wrappingTrie) Commit(collectLeaf bool) (common.Hash, *trienode.NodeSet, error) {
	return t.inner.Commit(collectLeaf)
}

func (t *wrappingTrie) NodeIterator(startKey []byte) (trie.NodeIterator, error) {
	return t.inner.NodeIterator(startKey)
}

func (t *wrappingTrie) Prove(key []byte, proofDb ethdb.KeyValueWriter) error {
	return t.inner.Prove(key, proofDb)
}

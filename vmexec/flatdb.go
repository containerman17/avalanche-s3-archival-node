package vmexec

import (
	"errors"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/state"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/crypto"
	"github.com/ava-labs/libevm/ethdb"
	"github.com/ava-labs/libevm/rlp"
	"github.com/ava-labs/libevm/trie"
	"github.com/ava-labs/libevm/trie/trienode"
	"github.com/ava-labs/libevm/triedb"
	"github.com/holiman/uint256"
)

// flatDB is the state.Database over the state engine: reads come from the
// engine's view (this block's own writes first), writes go to the open
// block's write set. Contract code stays where it was: a libevm cachingDB
// over the store's ethdb. It replaces the INNER database (Firewood via
// triedb) under the capture wrapper; nothing above it changes.
type flatDB struct {
	code state.Database
	eng  *engine
	ws   *writeSet   // the open block's writes; nil outside a block
	root common.Hash // the parent root: what the account trie's Hash reports
}

// writeSet is one block's contract writes in order (order matters: a delete
// before a recreate), with the last value per key for mid-block reads.
type writeSet struct {
	ops []kv
	m   map[string][]byte
}

type kv struct{ k, v []byte }

func newWriteSet() *writeSet { return &writeSet{m: map[string][]byte{}} }

func (w *writeSet) put(k, v []byte) {
	w.ops = append(w.ops, kv{k, v})
	w.m[string(k)] = v
}

func (d *flatDB) begin(parentRoot common.Hash) {
	d.ws = newWriteSet()
	d.root = parentRoot
}

func (d *flatDB) take() *writeSet {
	ws := d.ws
	d.ws = nil
	return ws
}

func (d *flatDB) lookup(key []byte) ([]byte, bool) {
	if d.ws != nil {
		if v, ok := d.ws.m[string(key)]; ok {
			return v, len(v) > 0
		}
	}
	return d.eng.get(key)
}

func (d *flatDB) OpenTrie(common.Hash) (state.Trie, error) { return &flatTrie{db: d}, nil }

func (d *flatDB) OpenStorageTrie(_ common.Hash, addr common.Address, _ common.Hash, _ state.Trie) (state.Trie, error) {
	return &flatTrie{db: d, storage: true, ah: crypto.Keccak256Hash(addr[:])}, nil
}

func (d *flatDB) CopyTrie(t state.Trie) state.Trie {
	c := *t.(*flatTrie)
	return &c
}

func (d *flatDB) ContractCode(addr common.Address, h common.Hash) ([]byte, error) {
	return d.code.ContractCode(addr, h)
}

func (d *flatDB) ContractCodeSize(addr common.Address, h common.Hash) (int, error) {
	return d.code.ContractCodeSize(addr, h)
}

func (d *flatDB) DiskDB() ethdb.KeyValueStore { return d.code.DiskDB() }

// TrieDB is a hash-scheme triedb over the same ethdb; statedb.Commit asks it
// only for its Scheme (hash: storage deletion is left to the trie, i.e. us).
func (d *flatDB) TrieDB() *triedb.Database { return d.code.TrieDB() }

// flatTrie is the account trie (storage=false) or one account's storage trie.
type flatTrie struct {
	db      *flatDB
	storage bool
	ah      common.Hash // keccak(addr) for a storage trie
}

func (t *flatTrie) GetKey([]byte) []byte { return nil }

func (t *flatTrie) GetAccount(addr common.Address) (*types.StateAccount, error) {
	val, ok := t.db.lookup(accountKey(crypto.Keccak256Hash(addr[:])))
	if !ok {
		return nil, nil
	}
	var row accountRow
	if err := rlp.DecodeBytes(val, &row); err != nil {
		return nil, err
	}
	// Root: the empty root. Nothing on the read path short-circuits on it
	// (no prefetcher, no snapshot) and the trie roots come from commit.
	return &types.StateAccount{Nonce: row.Nonce, Balance: row.Balance, Root: types.EmptyRootHash, CodeHash: row.CodeHash}, nil
}

func (t *flatTrie) GetStorage(_ common.Address, key []byte) ([]byte, error) {
	val, _ := t.db.lookup(slotKey(t.ah, crypto.Keccak256Hash(key)))
	return val, nil
}

func (t *flatTrie) UpdateAccount(addr common.Address, acc *types.StateAccount) error {
	bal := acc.Balance
	if bal == nil {
		bal = new(uint256.Int)
	}
	val, err := rlp.EncodeToBytes(&accountRow{Nonce: acc.Nonce, Balance: bal, CodeHash: acc.CodeHash})
	if err != nil {
		return err
	}
	t.db.ws.put(accountKey(crypto.Keccak256Hash(addr[:])), val)
	return nil
}

func (t *flatTrie) DeleteAccount(addr common.Address) error {
	t.db.ws.put(accountKey(crypto.Keccak256Hash(addr[:])), nil)
	return nil
}

func (t *flatTrie) UpdateStorage(_ common.Address, key, value []byte) error {
	t.db.ws.put(slotKey(t.ah, crypto.Keccak256Hash(key)), append([]byte(nil), value...))
	return nil
}

func (t *flatTrie) DeleteStorage(_ common.Address, key []byte) error {
	t.db.ws.put(slotKey(t.ah, crypto.Keccak256Hash(key)), nil)
	return nil
}

func (t *flatTrie) UpdateContractCode(common.Address, common.Hash, []byte) error { return nil }

// Hash is a placeholder: the parent root for the account trie (so the per-tx
// drain and the block's Commit never compute a root here, and statedb sees
// root == origin and skips its triedb update) and the zero hash for storage
// tries, as Firewood answered. The real root is commit.Dirty's.
func (t *flatTrie) Hash() common.Hash {
	if t.storage {
		return common.Hash{}
	}
	return t.db.root
}

func (t *flatTrie) Commit(bool) (common.Hash, *trienode.NodeSet, error) {
	return t.Hash(), nil, nil
}

func (t *flatTrie) NodeIterator([]byte) (trie.NodeIterator, error) {
	return nil, errors.New("vmexec: NodeIterator unsupported over the flat state")
}

func (t *flatTrie) Prove([]byte, ethdb.KeyValueWriter) error {
	return errors.New("vmexec: Prove unsupported over the flat state")
}

var (
	_ state.Database = (*flatDB)(nil)
	_ state.Trie     = (*flatTrie)(nil)
)

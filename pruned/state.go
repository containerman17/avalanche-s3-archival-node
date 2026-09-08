package pruned

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/state"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/crypto"
	"github.com/ava-labs/libevm/ethdb"
	"github.com/ava-labs/libevm/rlp"
	"github.com/ava-labs/libevm/trie"
	"github.com/ava-labs/libevm/trie/trienode"
	"github.com/holiman/uint256"
)

var (
	_ state.Database = (*stateAccessor)(nil)
	_ state.Trie     = (*accountTrie)(nil)
	_ state.Trie     = (*storageTrie)(nil)
)

type stateAccessor struct {
	state.Database
	db *DB
}

// NewStateAccessor serves state tries from an epochdb backend. Contract code
// and the underlying disk and trie databases remain with the original accessor.
// Other trie database backends are returned unchanged.
func NewStateAccessor(db state.Database) state.Database {
	backend, ok := db.TrieDB().Backend().(*DB)
	if !ok {
		return db
	}
	return &stateAccessor{Database: db, db: backend}
}

func (s *stateAccessor) OpenTrie(root common.Hash) (state.Trie, error) {
	parent, err := s.db.open(root)
	if err != nil {
		return nil, err
	}
	return &accountTrie{
		db:         s.db,
		parent:     parent,
		root:       root,
		values:     map[string][]byte{},
		wiped:      map[common.Hash]bool{},
		hasChanges: true,
	}, nil
}

func (*stateAccessor) OpenStorageTrie(_ common.Hash, _ common.Address, _ common.Hash, self state.Trie) (state.Trie, error) {
	accounts, ok := self.(*accountTrie)
	if !ok {
		return nil, fmt.Errorf("epochdb: invalid account trie type %T", self)
	}
	return &storageTrie{accountTrie: accounts}, nil
}

func (*stateAccessor) CopyTrie(t state.Trie) state.Trie {
	switch t := t.(type) {
	case *accountTrie:
		return t.copy()
	case *storageTrie:
		return nil // StateDB reopens storage tries against the copied account trie.
	default:
		panic(fmt.Errorf("epochdb: unknown trie type %T", t))
	}
}

// accountTrie holds one state's ordered mutations. Storage tries share these
// mutations, so account deletion and recreation also govern storage reads.
// Callers must synchronize access to a trie and its storage wrappers.
type accountTrie struct {
	db         *DB
	parent     *revision
	root       common.Hash
	ops        []operation
	values     map[string][]byte
	wiped      map[common.Hash]bool
	hasChanges bool
	hashErr    error
}

type accountRow struct {
	Nonce    uint64
	Balance  *uint256.Int
	CodeHash []byte
}

func accountKey(owner common.Hash) []byte {
	return append(bytes.Clone(owner[:]), 0)
}

func storageKey(owner common.Hash, key []byte) []byte {
	return append(append(bytes.Clone(owner[:]), 1), crypto.Keccak256(key)...)
}

func (a *accountTrie) get(key []byte) ([]byte, error) {
	if value, ok := a.values[string(key)]; ok {
		return bytes.Clone(value), nil
	}
	return a.db.get(a.parent, key)
}

func (a *accountTrie) GetAccount(addr common.Address) (*types.StateAccount, error) {
	value, err := a.get(accountKey(crypto.Keccak256Hash(addr[:])))
	if err != nil || len(value) == 0 {
		return nil, err
	}
	var row accountRow
	if err := rlp.DecodeBytes(value, &row); err != nil {
		return nil, err
	}
	return &types.StateAccount{
		Nonce:    row.Nonce,
		Balance:  row.Balance,
		Root:     types.EmptyRootHash,
		CodeHash: row.CodeHash,
	}, nil
}

func (a *accountTrie) GetStorage(addr common.Address, key []byte) ([]byte, error) {
	owner := crypto.Keccak256Hash(addr[:])
	if value, ok := a.values[string(accountKey(owner))]; ok && len(value) == 0 {
		return nil, nil
	}
	combined := storageKey(owner, key)
	if value, ok := a.values[string(combined)]; ok {
		return bytes.Clone(value), nil
	}
	if a.wiped[owner] {
		return nil, nil
	}
	return a.db.get(a.parent, combined)
}

func (a *accountTrie) put(key, value []byte) {
	op := operation{key: bytes.Clone(key), value: bytes.Clone(value)}
	a.ops = append(a.ops, op)
	a.values[string(op.key)] = op.value
	a.hasChanges = true
}

func (a *accountTrie) UpdateAccount(addr common.Address, account *types.StateAccount) error {
	balance := account.Balance
	if balance == nil {
		balance = new(uint256.Int)
	}
	value, err := rlp.EncodeToBytes(&accountRow{Nonce: account.Nonce, Balance: balance, CodeHash: account.CodeHash})
	if err != nil {
		return err
	}
	a.put(accountKey(crypto.Keccak256Hash(addr[:])), value)
	return nil
}

func (a *accountTrie) DeleteAccount(addr common.Address) error {
	owner := crypto.Keccak256Hash(addr[:])
	for key := range a.values {
		if len(key) == 65 && bytes.Equal([]byte(key[:32]), owner[:]) {
			delete(a.values, key)
		}
	}
	a.wiped[owner] = true
	a.put(accountKey(owner), nil)
	return nil
}

// ResetAccount receives finalized resets before a recreated account's writes.
func (a *accountTrie) ResetAccount(addr common.Address) error {
	return a.DeleteAccount(addr)
}

func (a *accountTrie) UpdateStorage(addr common.Address, key, value []byte) error {
	a.put(storageKey(crypto.Keccak256Hash(addr[:]), key), common.TrimLeftZeroes(value))
	return nil
}

func (a *accountTrie) DeleteStorage(addr common.Address, key []byte) error {
	a.put(storageKey(crypto.Keccak256Hash(addr[:]), key), nil)
	return nil
}

// Hash computes a proposal once per mutation and returns zero on error.
// Commit reports any hashing error, including one from an earlier Hash call.
func (a *accountTrie) Hash() common.Hash {
	root, _ := a.hash()
	return root
}

func (a *accountTrie) hash() (common.Hash, error) {
	if a.hashErr != nil {
		return common.Hash{}, a.hashErr
	}
	if a.hasChanges {
		root, err := a.db.propose(a.parent, a.ops)
		if err != nil {
			a.hashErr = err
			return common.Hash{}, err
		}
		a.root = root
		a.hasChanges = false
	}
	return a.root, nil
}

func (a *accountTrie) Commit(bool) (common.Hash, *trienode.NodeSet, error) {
	root, err := a.hash()
	if err != nil {
		return common.Hash{}, nil, err
	}
	return root, trienode.NewNodeSet(common.Hash{}), nil
}

func (a *accountTrie) copy() *accountTrie {
	copy := *a
	copy.ops = make([]operation, len(a.ops))
	for i, op := range a.ops {
		copy.ops[i] = operation{key: bytes.Clone(op.key), value: bytes.Clone(op.value)}
	}
	copy.values = make(map[string][]byte, len(a.values))
	for key, value := range a.values {
		copy.values[key] = bytes.Clone(value)
	}
	copy.wiped = make(map[common.Hash]bool, len(a.wiped))
	for owner, wiped := range a.wiped {
		copy.wiped[owner] = wiped
	}
	return &copy
}

// Contract code is written by StateDB through rawdb.
func (*accountTrie) UpdateContractCode(common.Address, common.Hash, []byte) error { return nil }
func (*accountTrie) GetKey([]byte) []byte                                         { return nil }

func (*accountTrie) NodeIterator([]byte) (trie.NodeIterator, error) {
	return nil, errors.New("epochdb: NodeIterator is unsupported")
}

func (*accountTrie) Prove([]byte, ethdb.KeyValueWriter) error {
	return errors.New("epochdb: Prove is unsupported")
}

type storageTrie struct {
	*accountTrie
}

// Storage roots are computed with the account proposal by commit.Dirty.
func (*storageTrie) Hash() common.Hash { return common.Hash{} }

func (*storageTrie) Commit(bool) (common.Hash, *trienode.NodeSet, error) {
	return common.Hash{}, nil, nil
}

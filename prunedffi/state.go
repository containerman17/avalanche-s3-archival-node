//go:build subnetbench

package prunedffi

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/state"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/crypto"
	"github.com/ava-labs/libevm/ethdb"
	"github.com/ava-labs/libevm/trie"
	"github.com/ava-labs/libevm/trie/trienode"
	"github.com/holiman/uint256"
)

var (
	_ state.Database = (*stateAccessor)(nil)
	_ state.Trie     = (*accountTrie)(nil)
	_ state.Trie     = (*storageTrie)(nil)
)

// Op stream tags shared with rust/prunedffi/src/store.rs.
const (
	opAccount = 1 // hashed(32) nonce(8) balance(32) codehash(32)
	opDelete  = 2 // hashed(32)
	opSlot    = 3 // hashed(32) slot(32) len(1) value(len); len 0 deletes
)

type stateAccessor struct {
	state.Database
	db *DB
}

// NewStateAccessor serves state tries from the Rust backend. Contract code
// and the underlying disk database remain with the original accessor.
func NewStateAccessor(db state.Database) state.Database {
	backend, ok := db.TrieDB().Backend().(*DB)
	if !ok {
		return db
	}
	return &stateAccessor{Database: db, db: backend}
}

func (s *stateAccessor) OpenTrie(root common.Hash) (state.Trie, error) {
	if err := s.db.open(root); err != nil {
		return nil, err
	}
	return &accountTrie{db: s.db, parent: root, root: root, values: map[string][]byte{}, wiped: map[common.Hash]bool{}, hasChanges: true}, nil
}

func (*stateAccessor) OpenStorageTrie(_ common.Hash, _ common.Address, _ common.Hash, self state.Trie) (state.Trie, error) {
	accounts, ok := self.(*accountTrie)
	if !ok {
		return nil, fmt.Errorf("prunedffi: invalid account trie type %T", self)
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
		panic(fmt.Errorf("prunedffi: unknown trie type %T", t))
	}
}

// accountTrie holds one state's ordered mutations as the op stream the Rust
// side applies, plus a map of the same writes for reads. Storage tries share
// it. Callers must synchronize access to a trie and its storage wrappers.
type accountTrie struct {
	db         *DB
	parent     common.Hash
	root       common.Hash
	ops        []byte
	values     map[string][]byte
	wiped      map[common.Hash]bool
	hasChanges bool
	hashErr    error
}

func accountKey(owner common.Hash) string { return string(owner[:]) + "\x00" }
func slotKey(owner, slot common.Hash) string {
	return string(owner[:]) + "\x01" + string(slot[:])
}

func encodeRow(account *types.StateAccount) []byte {
	row := make([]byte, 72)
	binary.BigEndian.PutUint64(row[:8], account.Nonce)
	if account.Balance != nil {
		b := account.Balance.Bytes32()
		copy(row[8:40], b[:])
	}
	copy(row[40:], account.CodeHash)
	return row
}

func decodeRow(row []byte) (*types.StateAccount, error) {
	if len(row) != 72 {
		return nil, fmt.Errorf("prunedffi: account row of %d bytes", len(row))
	}
	return &types.StateAccount{
		Nonce:    binary.BigEndian.Uint64(row[:8]),
		Balance:  new(uint256.Int).SetBytes(row[8:40]),
		Root:     types.EmptyRootHash,
		CodeHash: bytes.Clone(row[40:72]),
	}, nil
}

func (a *accountTrie) GetAccount(addr common.Address) (*types.StateAccount, error) {
	owner := crypto.Keccak256Hash(addr[:])
	value, ok := a.values[accountKey(owner)]
	if !ok {
		var err error
		if value, err = a.db.getAccount(a.parent, owner); err != nil {
			return nil, err
		}
	}
	if len(value) == 0 {
		return nil, nil
	}
	return decodeRow(value)
}

func (a *accountTrie) GetStorage(addr common.Address, key []byte) ([]byte, error) {
	owner := crypto.Keccak256Hash(addr[:])
	if value, ok := a.values[accountKey(owner)]; ok && len(value) == 0 {
		return nil, nil
	}
	slot := crypto.Keccak256Hash(key)
	if value, ok := a.values[slotKey(owner, slot)]; ok {
		return bytes.Clone(value), nil
	}
	if a.wiped[owner] {
		return nil, nil
	}
	return a.db.getStorage(a.parent, owner, slot)
}

func (a *accountTrie) UpdateAccount(addr common.Address, account *types.StateAccount) error {
	if len(account.CodeHash) != 32 {
		return fmt.Errorf("prunedffi: code hash of %d bytes", len(account.CodeHash))
	}
	owner := crypto.Keccak256Hash(addr[:])
	row := encodeRow(account)
	a.ops = append(append(append(a.ops, opAccount), owner[:]...), row...)
	a.values[accountKey(owner)] = row
	a.hasChanges = true
	return nil
}

func (a *accountTrie) DeleteAccount(addr common.Address) error {
	owner := crypto.Keccak256Hash(addr[:])
	prefix := string(owner[:]) + "\x01"
	for key := range a.values {
		if len(key) == 65 && key[:33] == prefix {
			delete(a.values, key)
		}
	}
	a.wiped[owner] = true
	a.ops = append(append(a.ops, opDelete), owner[:]...)
	a.values[accountKey(owner)] = nil
	a.hasChanges = true
	return nil
}

// ResetAccount receives finalized resets before a recreated account's writes.
func (a *accountTrie) ResetAccount(addr common.Address) error {
	return a.DeleteAccount(addr)
}

func (a *accountTrie) UpdateStorage(addr common.Address, key, value []byte) error {
	owner := crypto.Keccak256Hash(addr[:])
	slot := crypto.Keccak256Hash(key)
	value = common.TrimLeftZeroes(value)
	if len(value) > 32 {
		return fmt.Errorf("prunedffi: storage value of %d bytes", len(value))
	}
	a.ops = append(append(append(append(a.ops, opSlot), owner[:]...), slot[:]...), byte(len(value)))
	a.ops = append(a.ops, value...)
	a.values[slotKey(owner, slot)] = bytes.Clone(value)
	a.hasChanges = true
	return nil
}

func (a *accountTrie) DeleteStorage(addr common.Address, key []byte) error {
	return a.UpdateStorage(addr, key, nil)
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
	c := *a
	c.ops = bytes.Clone(a.ops)
	c.values = make(map[string][]byte, len(a.values))
	for k, v := range a.values {
		c.values[k] = bytes.Clone(v)
	}
	c.wiped = make(map[common.Hash]bool, len(a.wiped))
	for k, v := range a.wiped {
		c.wiped[k] = v
	}
	return &c
}

// Contract code is written by StateDB through rawdb.
func (*accountTrie) UpdateContractCode(common.Address, common.Hash, []byte) error { return nil }
func (*accountTrie) GetKey([]byte) []byte                                         { return nil }

func (*accountTrie) NodeIterator([]byte) (trie.NodeIterator, error) {
	return nil, errors.New("prunedffi: NodeIterator is unsupported")
}

func (*accountTrie) Prove([]byte, ethdb.KeyValueWriter) error {
	return errors.New("prunedffi: Prove is unsupported")
}

type storageTrie struct {
	*accountTrie
}

// Storage roots are computed with the account proposal by the Rust side.
func (*storageTrie) Hash() common.Hash { return common.Hash{} }

func (*storageTrie) Commit(bool) (common.Hash, *trienode.NodeSet, error) {
	return common.Hash{}, nil, nil
}

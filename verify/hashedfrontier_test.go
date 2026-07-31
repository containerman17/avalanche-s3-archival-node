package verify

// Does a Firewood frontier need key PREIMAGES?
//
// It is the question behind every frontier that is not built by replaying:
// exec.BuildFrontier hashes the epochs' preimage keys itself and puts them
// straight into Firewood. The trie API epochdb executes through is
// preimage-keyed (UpdateAccount takes an address), which is what made this
// look like a wall.
//
// It is not a wall. Firewood's own key space is ALREADY hashed:
// graft/evm/firewood/base_trie.go hashes on the way in and stores
// keccak(addr) for an account and keccak(addr)||keccak(slot) for a slot, with
// the account's trie leaf RLP and rlp(trimmed slot value) as the values, i.e.
// byte-for-byte the keys and values the frontier builder produces. So the preimage-keyed
// API is a convenience layer over a hash-keyed store, and a frontier can be
// built straight from hashed rows with ffi.Put.
//
// This test is the proof: it builds a state twice, once through a stack trie
// (the canonical MPT root) and once by putting the same hashed keys into a
// real Firewood, and asserts the two roots agree.

import (
	"bytes"
	"math/big"
	"sort"
	"testing"

	"github.com/ava-labs/avalanchego/graft/evm/firewood"
	"github.com/ava-labs/firewood-go-ethhash/ffi"
	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/crypto"
	"github.com/ava-labs/libevm/rlp"
	"github.com/ava-labs/libevm/trie"
	"github.com/holiman/uint256"

	"github.com/containerman17/epochdb/chain"
	"github.com/containerman17/epochdb/fetch"
)

func TestFirewoodFrontierFromHashedKeys(t *testing.T) {
	fetch.RegisterExtras(chain.Coreth)

	type kv struct{ key, val []byte }
	var (
		accountLeaves []kv // keccak(addr) -> account RLP
		slotLeaves    []kv // keccak(addr)||keccak(slot) -> rlp(trimmed value)
	)

	// One contract with storage plus two plain accounts, built exactly as a
	// state-sync response delivers them: hashed keys, trie leaf values.
	contract := crypto.Keccak256Hash([]byte("contract"))
	storage := trie.NewStackTrie(nil)
	var slotKeys []common.Hash
	for i := 1; i <= 5; i++ {
		slotKeys = append(slotKeys, crypto.Keccak256Hash(common.BigToHash(big.NewInt(int64(i))).Bytes()))
	}
	sort.Slice(slotKeys, func(i, j int) bool { return bytes.Compare(slotKeys[i][:], slotKeys[j][:]) < 0 })
	for i, sk := range slotKeys {
		enc, err := rlp.EncodeToBytes(common.BigToHash(big.NewInt(int64(i + 42))).Bytes())
		if err != nil {
			t.Fatal(err)
		}
		if err := storage.Update(sk[:], enc); err != nil {
			t.Fatal(err)
		}
		slotLeaves = append(slotLeaves, kv{append(append([]byte{}, contract[:]...), sk[:]...), enc})
	}
	storageRoot := storage.Hash()

	accounts := []struct {
		hash common.Hash
		acc  types.StateAccount
	}{
		{crypto.Keccak256Hash([]byte("a")), types.StateAccount{
			Nonce: 3, Balance: uint256.NewInt(11), Root: types.EmptyRootHash, CodeHash: types.EmptyCodeHash.Bytes(),
		}},
		{crypto.Keccak256Hash([]byte("b")), types.StateAccount{
			Nonce: 0, Balance: uint256.NewInt(999), Root: types.EmptyRootHash, CodeHash: types.EmptyCodeHash.Bytes(),
		}},
		{contract, types.StateAccount{
			Nonce: 1, Balance: new(uint256.Int), Root: storageRoot, CodeHash: crypto.Keccak256([]byte("code")),
		}},
	}
	sort.Slice(accounts, func(i, j int) bool { return bytes.Compare(accounts[i].hash[:], accounts[j].hash[:]) < 0 })
	stateTrie := trie.NewStackTrie(nil)
	for _, a := range accounts {
		enc, err := rlp.EncodeToBytes(&a.acc)
		if err != nil {
			t.Fatal(err)
		}
		if err := stateTrie.Update(a.hash[:], enc); err != nil {
			t.Fatal(err)
		}
		accountLeaves = append(accountLeaves, kv{a.hash[:], enc})
	}
	want := stateTrie.Hash()

	// Now the same rows into Firewood, hashed keys straight through.
	tdb, err := firewood.New(firewood.DefaultConfig(t.TempDir()))
	if err != nil {
		t.Fatalf("open firewood: %v", err)
	}
	defer tdb.Close()

	ops := make([]ffi.BatchOp, 0, len(accountLeaves)+len(slotLeaves))
	for _, l := range accountLeaves {
		ops = append(ops, ffi.Put(l.key, l.val))
	}
	for _, l := range slotLeaves {
		ops = append(ops, ffi.Put(l.key, l.val))
	}
	p, err := tdb.Firewood.Propose(ops)
	if err != nil {
		t.Fatalf("propose: %v", err)
	}
	if err := p.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if got := common.Hash(tdb.Firewood.Root()); got != want {
		t.Fatalf("firewood root from hashed keys is %x, canonical MPT root is %x", got, want)
	}
}

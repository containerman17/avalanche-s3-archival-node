package vmexec

import (
	"math/big"
	"math/rand"
	"testing"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/rawdb"
	ethstate "github.com/ava-labs/libevm/core/state"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/triedb"
	"github.com/holiman/uint256"

	"github.com/containerman17/avalanche-s3-archival-node/chain"
	"github.com/containerman17/avalanche-s3-archival-node/fetch"
)

// TestEngineAgainstGethTrie drives the same random state transitions through
// a geth hash trie and through flatDB + engine (with a forced roll in the
// middle) and compares every block's root. The oracle for everything above.
func TestEngineAgainstGethTrie(t *testing.T) {
	fetch.RegisterExtras(chain.SubnetEVM)
	rng := rand.New(rand.NewSource(1))
	addrs := make([]common.Address, 12)
	for i := range addrs {
		rng.Read(addrs[i][:])
	}
	alloc := types.GenesisAlloc{
		addrs[0]: {Balance: big.NewInt(1e18)},
		addrs[1]: {Balance: big.NewInt(5), Nonce: 3, Code: []byte{0x60, 0x00}, Storage: map[common.Hash]common.Hash{
			{1}: {7}, {2}: {9}, {3}: {},
		}},
		addrs[2]: {Balance: big.NewInt(0)},
	}

	// Reference: geth's own trie over a memory db.
	memdb := rawdb.NewMemoryDatabase()
	tdb := triedb.NewDatabase(memdb, triedb.HashDefaults)
	refDB := ethstate.NewDatabaseWithNodeDB(memdb, tdb)
	ref, err := ethstate.New(types.EmptyRootHash, refDB, nil)
	if err != nil {
		t.Fatal(err)
	}
	for addr, a := range alloc {
		ref.SetBalance(addr, uint256.MustFromBig(a.Balance))
		ref.SetNonce(addr, a.Nonce)
		if len(a.Code) > 0 {
			ref.SetCode(addr, a.Code)
		}
		for k, v := range a.Storage {
			ref.SetState(addr, k, v)
		}
	}
	root0, err := ref.Commit(0, false)
	if err != nil {
		t.Fatal(err)
	}
	tdb.Commit(root0, false)

	eng, err := newEngine(t.TempDir(), alloc, root0)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.close()
	code := ethstate.NewDatabaseWithNodeDB(memdb, triedb.NewDatabase(memdb, triedb.HashDefaults))
	flat := &flatDB{code: code, eng: eng}
	wrap := wrapDatabase(flat)

	root := root0
	for blk := uint64(1); blk <= 60; blk++ {
		ref, err := ethstate.New(root, refDB, nil)
		if err != nil {
			t.Fatal(err)
		}
		flat.begin(root)
		st, err := ethstate.New(root, wrap, nil)
		if err != nil {
			t.Fatal(err)
		}
		both := func(f func(s *ethstate.StateDB)) { f(ref); f(st) }
		for i := 0; i < 8; i++ {
			a := addrs[rng.Intn(len(addrs))]
			switch rng.Intn(6) {
			case 0:
				v := uint256.NewInt(uint64(rng.Intn(1000)))
				both(func(s *ethstate.StateDB) { s.AddBalance(a, v) })
			case 1, 2:
				k := common.Hash{byte(rng.Intn(4))}
				v := common.Hash{byte(rng.Intn(3))} // {0} clears the slot
				both(func(s *ethstate.StateDB) { s.SetState(a, k, v) })
			case 3:
				both(func(s *ethstate.StateDB) { s.SetNonce(a, s.GetNonce(a)+1) })
			case 4:
				if rng.Intn(4) == 0 {
					both(func(s *ethstate.StateDB) { s.SelfDestruct(a) })
				}
			case 5:
				c := []byte{0x60, byte(rng.Intn(256))}
				both(func(s *ethstate.StateDB) {
					if len(s.GetCode(a)) == 0 {
						s.SetCode(a, c)
					}
				})
			}
			// Per-tx drain, as the executor does.
			both(func(s *ethstate.StateDB) { s.IntermediateRoot(true) })
		}
		want, err := ref.Commit(blk, true)
		if err != nil {
			t.Fatal(err)
		}
		tdb.Commit(want, false)
		if _, err := st.Commit(blk, true); err != nil {
			t.Fatal(err)
		}
		ws := flat.take()
		if err := eng.apply(ws); err != nil {
			t.Fatal(err)
		}
		got, err := eng.root()
		if err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("block %d: root %x, want %x", blk, got, want)
		}
		root = want
		if blk == 20 || blk == 45 {
			eng.maybeRoll(1, blk, root) // budget 1: freeze now
			r := <-eng.rollDone         // wait, then put it back for finishRoll
			eng.rollDone <- r
			if blk == 45 {
				// Write a block while the roll is "in flight" (frozen still set).
				continue
			}
			if err := eng.finishRoll(); err != nil {
				t.Fatal(err)
			}
			if eng.file.Root() != root {
				t.Fatalf("rolled file root %x, want %x", eng.file.Root(), root)
			}
		}
		if blk == 47 {
			if err := eng.finishRoll(); err != nil {
				t.Fatal(err)
			}
			if eng.rolls != 2 {
				t.Fatalf("rolls = %d, want 2", eng.rolls)
			}
		}
	}
}

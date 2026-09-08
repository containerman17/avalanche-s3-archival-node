//go:build subnetbench

package pruned

import (
	"bytes"
	"errors"
	"testing"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/rawdb"
	"github.com/ava-labs/libevm/core/state"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/ethdb"
	"github.com/ava-labs/libevm/libevm/stateconf"
	"github.com/ava-labs/libevm/triedb"
	"github.com/holiman/uint256"
)

type stateHarness struct {
	db     *DB
	actual state.Database
	oracle state.Database
	root   common.Hash
	block  common.Hash
	height uint64
}

func newStateHarness(t *testing.T) *stateHarness {
	t.Helper()
	db := testDB(t, Config{})
	disk := rawdb.NewMemoryDatabase()
	t.Cleanup(func() { disk.Close() })
	config := triedb.Config{DBOverride: func(ethdb.Database) triedb.DBOverride { return db }}
	actual := NewStateAccessor(state.NewDatabaseWithNodeDB(disk, triedb.NewDatabase(disk, &config)))
	oracleDisk := rawdb.NewMemoryDatabase()
	t.Cleanup(func() { oracleDisk.Close() })
	oracleNodes := triedb.NewDatabase(oracleDisk, triedb.HashDefaults)
	t.Cleanup(func() { oracleNodes.Close() })
	oracle := state.NewDatabaseWithNodeDB(oracleDisk, oracleNodes)
	return &stateHarness{db: db, actual: actual, oracle: oracle, root: types.EmptyRootHash}
}

func (h *stateHarness) open(t *testing.T) (*state.StateDB, *state.StateDB) {
	t.Helper()
	actual, err := state.New(h.root, h.actual, nil)
	if err != nil {
		t.Fatal(err)
	}
	oracle, err := state.New(h.root, h.oracle, nil)
	if err != nil {
		t.Fatal(err)
	}
	return actual, oracle
}

func (h *stateHarness) commit(t *testing.T, actual, oracle *state.StateDB) {
	t.Helper()
	block := testHash(1000 + h.height)
	opts := stateconf.WithTrieDBUpdateOpts(stateconf.WithTrieDBUpdatePayload(h.block, block))
	want, err := oracle.Commit(h.height, true)
	if err != nil {
		t.Fatal(err)
	}
	got, err := actual.Commit(h.height, true, opts)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("block %d: epochdb root %x, hashdb root %x", h.height, got, want)
	}
	// The VM reports every block, including blocks for which StateDB skips
	// TrieDB.Update because their root did not change.
	if err := h.db.Update(got, h.root, h.height, nil, nil, stateconf.WithTrieDBUpdatePayload(h.block, block)); err != nil {
		t.Fatal(err)
	}
	if err := h.db.Commit(got, false); err != nil {
		t.Fatal(err)
	}
	h.root, h.block = got, block
	h.height++
	actual, oracle = h.open(t)
	compareStateReads(t, actual, oracle)
}

func stateAddress(n byte) common.Address { return common.Address{19: n} }

func seedState(st *state.StateDB) {
	for n := byte(1); n <= 3; n++ {
		addr := stateAddress(n)
		st.SetNonce(addr, uint64(n))
		st.SetBalance(addr, uint256.NewInt(uint64(n)*100))
		st.SetCode(addr, []byte{0x60, n, 0x60, 0, 0x55})
		for slot := uint64(1); slot <= 3; slot++ {
			st.SetState(addr, testHash(slot), testHash(uint64(n)*10+slot))
		}
	}
}

func compareStateReads(t *testing.T, actual, oracle *state.StateDB) {
	t.Helper()
	for n := byte(1); n <= 4; n++ {
		addr := stateAddress(n)
		if actual.Exist(addr) != oracle.Exist(addr) {
			t.Fatalf("account %d: existence differs", n)
		}
		if got, want := actual.GetNonce(addr), oracle.GetNonce(addr); got != want {
			t.Fatalf("account %d: nonce %d, want %d", n, got, want)
		}
		if got, want := actual.GetBalance(addr), oracle.GetBalance(addr); !got.Eq(want) {
			t.Fatalf("account %d: balance %s, want %s", n, got, want)
		}
		if got, want := actual.GetCode(addr), oracle.GetCode(addr); !bytes.Equal(got, want) {
			t.Fatalf("account %d: code %x, want %x", n, got, want)
		}
		if got, want := actual.GetCodeSize(addr), oracle.GetCodeSize(addr); got != want {
			t.Fatalf("account %d: code size %d, want %d", n, got, want)
		}
		for slot := uint64(1); slot <= 6; slot++ {
			if got, want := actual.GetState(addr, testHash(slot)), oracle.GetState(addr, testHash(slot)); got != want {
				t.Fatalf("account %d slot %d: %x, want %x", n, slot, got, want)
			}
		}
	}
	if err := actual.Error(); err != nil {
		t.Fatal(err)
	}
	if err := oracle.Error(); err != nil {
		t.Fatal(err)
	}
}

func TestStateDBMatchesHashDBAcrossBlocks(t *testing.T) {
	h := newStateHarness(t)
	actual, oracle := h.open(t)
	seedState(actual)
	seedState(oracle)
	h.commit(t, actual, oracle)

	actual, oracle = h.open(t)
	for _, st := range []*state.StateDB{actual, oracle} {
		addr := stateAddress(1)
		st.SetNonce(addr, 10)
		st.SetBalance(addr, uint256.NewInt(500))
		st.SetState(addr, testHash(1), testHash(999))
		st.SetState(addr, testHash(2), common.Hash{})
		st.SetState(addr, testHash(4), testHash(444))
		st.SetCode(addr, []byte{0x60, 0x42, 0x00})
		snapshot := st.Snapshot()
		st.SetState(addr, testHash(3), testHash(777))
		st.SetCode(addr, []byte{0xfe})
		st.SelfDestruct(stateAddress(2))
		st.CreateAccount(stateAddress(3))
		st.SetState(stateAddress(3), testHash(6), testHash(666))
		st.SetNonce(stateAddress(4), 1)
		st.RevertToSnapshot(snapshot)
	}
	compareStateReads(t, actual, oracle)
	h.commit(t, actual, oracle)

	actual, oracle = h.open(t)
	actual.SelfDestruct(stateAddress(2))
	oracle.SelfDestruct(stateAddress(2))
	h.commit(t, actual, oracle)
	actual, oracle = h.open(t)
	for _, st := range []*state.StateDB{actual, oracle} {
		addr := stateAddress(2)
		st.CreateAccount(addr)
		st.SetNonce(addr, 1)
		st.SetBalance(addr, uint256.NewInt(800))
		st.SetCode(addr, []byte{0x60, 1, 0x00})
		st.SetState(addr, testHash(5), testHash(555))
	}
	h.commit(t, actual, oracle)

	actual, oracle = h.open(t)
	oldRoot := h.root
	h.commit(t, actual, oracle)
	if h.root != oldRoot || h.db.current.height != 4 {
		t.Fatal("unchanged-root block was not accepted")
	}
	actual, oracle = h.open(t)
	actual.SetNonce(stateAddress(1), 11)
	oracle.SetNonce(stateAddress(1), 11)
	h.commit(t, actual, oracle)
}

func TestStateDBCopyIsIndependent(t *testing.T) {
	h := newStateHarness(t)
	actual, oracle := h.open(t)
	seedState(actual)
	seedState(oracle)
	h.commit(t, actual, oracle)
	actual, oracle = h.open(t)
	for _, st := range []*state.StateDB{actual, oracle} {
		st.SetState(stateAddress(1), testHash(1), testHash(444))
		st.SetNonce(stateAddress(1), 5)
	}
	before, wantBefore := actual.IntermediateRoot(true), oracle.IntermediateRoot(true)
	if before != wantBefore {
		t.Fatal("roots differ before copy")
	}
	actualCopy, oracleCopy := actual.Copy(), oracle.Copy()
	for _, st := range []*state.StateDB{actualCopy, oracleCopy} {
		st.SetState(stateAddress(1), testHash(1), testHash(555))
		st.SetState(stateAddress(1), testHash(4), testHash(444))
		st.SetNonce(stateAddress(2), 20)
	}
	if got, want := actualCopy.IntermediateRoot(true), oracleCopy.IntermediateRoot(true); got != want {
		t.Fatalf("copied root %x, want %x", got, want)
	}
	if actual.IntermediateRoot(true) != before {
		t.Fatal("copy mutations changed the original trie")
	}
	compareStateReads(t, actual, oracle)
	h.commit(t, actual, oracle)
}

func TestStateDBDestroyRecreateWithinBlock(t *testing.T) {
	for _, tc := range []struct {
		name        string
		flush       bool
		repeat      bool
		withStorage bool
	}{
		{name: "finalise_only", withStorage: true},
		{name: "intermediate_root_before_recreate", flush: true, withStorage: true},
		{name: "repeated_recreation", repeat: true, withStorage: true},
		{name: "recreate_without_storage"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newStateHarness(t)
			actual, oracle := h.open(t)
			seedState(actual)
			seedState(oracle)
			h.commit(t, actual, oracle)
			actual, oracle = h.open(t)
			for _, st := range []*state.StateDB{actual, oracle} {
				addr := stateAddress(1)
				st.SelfDestruct(addr)
				st.Finalise(true)
				if tc.flush {
					st.IntermediateRoot(true)
				}
				st.CreateAccount(addr)
				st.SetNonce(addr, 1)
				st.SetBalance(addr, uint256.NewInt(900))
				if tc.withStorage {
					st.SetState(addr, testHash(5), testHash(555))
				}
				st.Finalise(true)
				if tc.repeat {
					st.IntermediateRoot(true)
					st.SelfDestruct(addr)
					st.Finalise(true)
					st.CreateAccount(addr)
					st.SetNonce(addr, 2)
					st.SetBalance(addr, uint256.NewInt(901))
					st.SetState(addr, testHash(6), testHash(666))
					st.Finalise(true)
				}
			}
			compareStateReads(t, actual, oracle)
			h.commit(t, actual, oracle)
		})
	}
}

func TestStateDBResetExistingAccount(t *testing.T) {
	h := newStateHarness(t)
	actual, oracle := h.open(t)
	seedState(actual)
	seedState(oracle)
	h.commit(t, actual, oracle)
	actual, oracle = h.open(t)
	for _, st := range []*state.StateDB{actual, oracle} {
		addr := stateAddress(1)
		st.CreateAccount(addr)
		st.SetNonce(addr, 1)
		st.SetState(addr, testHash(6), testHash(666))
		st.Finalise(true)
	}
	compareStateReads(t, actual, oracle)
	h.commit(t, actual, oracle)
}

func TestStateDBResetCopiedBeforeFinalise(t *testing.T) {
	h := newStateHarness(t)
	actual, oracle := h.open(t)
	seedState(actual)
	seedState(oracle)
	h.commit(t, actual, oracle)
	actual, oracle = h.open(t)
	for _, st := range []*state.StateDB{actual, oracle} {
		addr := stateAddress(1)
		st.CreateAccount(addr)
		st.SetNonce(addr, 1)
		st.SetState(addr, testHash(6), testHash(666))
	}
	// StateDB.Copy drops its journal. A second copy also has to retain the
	// pending reset without depending on entries from the first StateDB.
	actualCopy, oracleCopy := actual.Copy().Copy(), oracle.Copy().Copy()
	h.commit(t, actualCopy, oracleCopy)
}

func TestStateDBRevertedResetPreservesStorage(t *testing.T) {
	h := newStateHarness(t)
	actual, oracle := h.open(t)
	seedState(actual)
	seedState(oracle)
	h.commit(t, actual, oracle)
	oldRoot := h.root
	actual, oracle = h.open(t)
	for _, st := range []*state.StateDB{actual, oracle} {
		addr := stateAddress(1)
		snapshot := st.Snapshot()
		st.SelfDestruct(addr)
		st.CreateAccount(addr)
		st.SetNonce(addr, 7)
		st.SetState(addr, testHash(6), testHash(666))
		st.RevertToSnapshot(snapshot)
		st.Finalise(true)
	}
	compareStateReads(t, actual, oracle)
	h.commit(t, actual, oracle)
	if h.root != oldRoot {
		t.Fatal("reverted reset changed the accepted state")
	}
}

func TestStateTrieHashErrorSurvivesCommit(t *testing.T) {
	h := newStateHarness(t)
	tr, err := h.actual.OpenTrie(h.root)
	if err != nil {
		t.Fatal(err)
	}
	failure := errors.New("test proposal failure")
	h.db.fault = failure
	if root := tr.Hash(); root != (common.Hash{}) {
		t.Fatalf("failed hash returned %x", root)
	}
	h.db.fault = nil
	if _, _, err := tr.Commit(false); !errors.Is(err, failure) {
		t.Fatalf("Commit lost the Hash failure: %v", err)
	}
}

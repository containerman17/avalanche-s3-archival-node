package store

import (
	"math/big"
	"testing"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/types"
)

// Genesis is no stored container, so a read at the end of height 0 must
// answer the genesis floor alone: not "block 0 is not stored", and never a
// row block 1 wrote (block 1 may start at TxNum 0, so no ceiling names
// "before it").
func TestStateAtHeightZeroIsTheGenesisFloor(t *testing.T) {
	db, _ := testDB(t)
	if err := db.WriteBlock(block(1, 1)); err != nil {
		t.Fatal(err)
	}
	a2 := common.BytesToAddress(addr(2))
	genesis := types.GenesisAlloc{a2: {Balance: big.NewInt(77)}}

	acc, err := db.Account(genesis, a2, 0)
	if err != nil {
		t.Fatalf("account at 0: %v", err)
	}
	if acc == nil || acc.Balance.Uint64() != 77 {
		t.Fatalf("account at 0 = %+v, want the genesis balance 77", acc)
	}
	val, err := db.Storage(genesis, a2, hash32(7), 0)
	if err != nil {
		t.Fatalf("storage at 0: %v", err)
	}
	if val != nil {
		t.Fatalf("storage at 0 = %x, want nil: block 1's row must not leak below it", val)
	}
	// And height 1 still sees block 1's rows.
	if val, err = db.Storage(genesis, a2, hash32(7), 1); err != nil || len(val) == 0 {
		t.Fatalf("storage at 1 = %x %v, want block 1's row", val, err)
	}
}

package state

import (
	"bytes"
	"encoding/binary"
	"math/big"
	"testing"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/rlp"
	"github.com/holiman/uint256"
)

// frame builders mirroring exec/capture.go's post-image layout.
func frAccount(f []byte, addr common.Address, valRLP []byte) []byte {
	f = append(f, recKindAccount)
	f = append(f, addr[:]...)
	f = binary.AppendUvarint(f, uint64(len(valRLP)))
	return append(f, valRLP...)
}

func frStorage(f []byte, addr common.Address, slot common.Hash, val []byte) []byte {
	f = append(f, recKindStorage)
	f = append(f, addr[:]...)
	f = append(f, slot[:]...)
	f = binary.AppendUvarint(f, uint64(len(val)))
	return append(f, val...)
}

func accRLP(t *testing.T, nonce uint64, balance int64) []byte {
	t.Helper()
	raw, err := rlp.EncodeToBytes(&types.StateAccount{
		Nonce:    nonce,
		Balance:  uint256.NewInt(uint64(balance)),
		Root:     types.EmptyRootHash,
		CodeHash: types.EmptyCodeHash.Bytes(),
	})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestOverlayBucketDescent(t *testing.T) {
	dir := t.TempDir()
	st, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	addr := common.HexToAddress("0x1111111111111111111111111111111111111111")
	addr2 := common.HexToAddress("0x2222222222222222222222222222222222222222")
	genAddr := common.HexToAddress("0x3333333333333333333333333333333333333333")
	slot := common.HexToHash("0x05")
	genSlot := common.HexToHash("0x07")
	v1 := []byte{0x2a}       // written at block 10 (bucket 0)
	v2 := []byte{0x82, 1, 2} // written at block 150000 (bucket 1)

	app := func(block uint64, frame []byte) {
		t.Helper()
		if err := st.AppendWrites(block, frame); err != nil {
			t.Fatal(err)
		}
	}
	// bucket 0
	app(10, frStorage(nil, addr, slot, v1))
	app(20, frAccount(nil, addr2, accRLP(t, 1, 100)))
	app(30, frStorage(nil, addr2, slot, v1))
	// bucket 1
	app(150000, frStorage(nil, addr, slot, v2))
	app(150010, frAccount(nil, addr2, nil)) // account delete
	app(150020, frAccount(nil, addr2, accRLP(t, 0, 7)))

	if err := st.FlushAndSetExecHead(190000); err != nil {
		t.Fatal(err)
	}
	if err := CookIndex(dir); err != nil {
		t.Fatal(err)
	}

	genVal := common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000063")
	alloc := types.GenesisAlloc{
		genAddr: types.Account{
			Balance: big.NewInt(55),
			Nonce:   9,
			Storage: map[common.Hash]common.Hash{genSlot: genVal},
		},
	}
	h, err := OpenHistory(dir, st, alloc)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()

	if h.Head() != 190000 {
		t.Fatalf("head=%d want 190000", h.Head())
	}

	getS := func(a common.Address, s common.Hash, n uint64) []byte {
		t.Helper()
		v, err := h.StorageAt(a, s[:], n)
		if err != nil {
			t.Fatal(err)
		}
		return v
	}

	// Exact boundary: write at 10 visible at 10, invisible at 9.
	if got := getS(addr, slot, 9); got != nil {
		t.Fatalf("at 9: got %x want nil", got)
	}
	if got := getS(addr, slot, 10); !bytes.Equal(got, v1) {
		t.Fatalf("at 10: got %x want %x", got, v1)
	}
	// Descent from bucket 1 into bucket 0.
	if got := getS(addr, slot, 149999); !bytes.Equal(got, v1) {
		t.Fatalf("at 149999: got %x want %x", got, v1)
	}
	if got := getS(addr, slot, 150000); !bytes.Equal(got, v2) {
		t.Fatalf("at 150000: got %x want %x", got, v2)
	}
	if got := getS(addr, slot, 190000); !bytes.Equal(got, v2) {
		t.Fatalf("at 190000: got %x want %x", got, v2)
	}

	// Account lifecycle: created 20, deleted 150010, recreated 150020.
	acc, err := h.AccountAt(addr2, 20)
	if err != nil || acc == nil {
		t.Fatalf("account at 20: %v %v", acc, err)
	}
	if acc.Balance.Uint64() != 100 || acc.Nonce != 1 {
		t.Fatalf("account at 20: %+v", acc)
	}
	// Sentinel root: never empty, never the trie's own root.
	if acc.Root == types.EmptyRootHash || acc.Root != SentinelStorageRoot {
		t.Fatalf("root=%x want sentinel %x", acc.Root, SentinelStorageRoot)
	}
	if acc, _ := h.AccountAt(addr2, 150015); acc != nil {
		t.Fatalf("deleted account visible at 150015: %+v", acc)
	}
	if acc, _ := h.AccountAt(addr2, 150025); acc == nil || acc.Balance.Uint64() != 7 {
		t.Fatalf("recreated account at 150025: %+v", acc)
	}
	// Storage written before the SELFDESTRUCT is dead after it.
	if got := getS(addr2, slot, 100000); !bytes.Equal(got, v1) {
		t.Fatalf("addr2 slot at 100000: got %x want %x", got, v1)
	}
	if got := getS(addr2, slot, 160000); got != nil {
		t.Fatalf("addr2 slot at 160000 (post-destruct): got %x want nil", got)
	}

	// Below-first-capture fallthrough: genesis alloc account + storage.
	gacc, err := h.AccountAt(genAddr, 1)
	if err != nil || gacc == nil {
		t.Fatalf("genesis account: %v %v", gacc, err)
	}
	if gacc.Balance.Uint64() != 55 || gacc.Nonce != 9 || gacc.Root != SentinelStorageRoot {
		t.Fatalf("genesis account: %+v", gacc)
	}
	wantGen, _ := rlp.EncodeToBytes(common.TrimLeftZeroes(genVal[:]))
	if got := getS(genAddr, genSlot, 1); !bytes.Equal(got, wantGen) {
		t.Fatalf("genesis storage: got %x want %x", got, wantGen)
	}
	// Unknown address and slot: clean zero.
	if acc, _ := h.AccountAt(common.HexToAddress("0x99"), 5); acc != nil {
		t.Fatalf("unknown account: %+v", acc)
	}
	if got := getS(genAddr, common.HexToHash("0x08"), 5); got != nil {
		t.Fatalf("unknown genesis slot: %x", got)
	}
}

func TestCookIdempotentAndTipRecook(t *testing.T) {
	dir := t.TempDir()
	st, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	addr := common.HexToAddress("0xaa")
	slot := common.HexToHash("0x01")
	if err := st.AppendWrites(5, frStorage(nil, addr, slot, []byte{1})); err != nil {
		t.Fatal(err)
	}
	if err := st.FlushAndSetExecHead(5); err != nil {
		t.Fatal(err)
	}
	if err := CookIndex(dir); err != nil {
		t.Fatal(err)
	}

	// Advance the head within the same (tip) bucket: re-cook must pick up
	// the new write; a second run with nothing new must be a no-op.
	if err := st.AppendWrites(6, frStorage(nil, addr, slot, []byte{2})); err != nil {
		t.Fatal(err)
	}
	if err := st.FlushAndSetExecHead(6); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ { // second pass exercises the skip path
		if err := CookIndex(dir); err != nil {
			t.Fatal(err)
		}
	}

	h, err := OpenHistory(dir, st, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	if got, _ := h.StorageAt(addr, slot[:], 6); !bytes.Equal(got, []byte{2}) {
		t.Fatalf("after recook at 6: %x", got)
	}
	if got, _ := h.StorageAt(addr, slot[:], 5); !bytes.Equal(got, []byte{1}) {
		t.Fatalf("after recook at 5: %x", got)
	}
}

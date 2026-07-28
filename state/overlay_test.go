package state

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math/big"
	"testing"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/crypto"
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

// fakeBase stands in for the base file (state/base.go owns the format):
// flat live state at a floor block, its code blobs and header window.
type fakeBase struct {
	block   uint64
	acc     map[common.Address][]byte
	slots   map[string][]byte
	code    map[common.Hash][]byte
	headers map[uint64][]byte
	closed  bool
}

func (b *fakeBase) Block() uint64 { return b.block }
func (b *fakeBase) Account(addr common.Address) ([]byte, bool, error) {
	v, ok := b.acc[addr]
	return v, ok, nil
}
func (b *fakeBase) Storage(addr common.Address, slot []byte) ([]byte, bool, error) {
	v, ok := b.slots[string(append(addr.Bytes(), slot...))]
	return v, ok, nil
}
func (b *fakeBase) Code(h common.Hash) ([]byte, bool, error) {
	v, ok := b.code[h]
	return v, ok, nil
}
func (b *fakeBase) HeaderRLP(n uint64) ([]byte, bool, error) {
	v, ok := b.headers[n]
	return v, ok, nil
}
func (b *fakeBase) Close() { b.closed = true }

// withBase makes OpenHistory find fb instead of a real base file.
func withBase(t *testing.T, fb *fakeBase) {
	t.Helper()
	prev := openBase
	openBase = func(string) (baseState, bool, error) { return fb, true, nil }
	t.Cleanup(func() { openBase = prev })
}

// TestOverlayBaseFloor is the safety property of limited-history mode:
// below the floor reads ERROR (never fall through to the genesis alloc),
// at or above it the base file answers what the epochs/buckets miss.
func TestOverlayBaseFloor(t *testing.T) {
	const floor = 150000
	dir := t.TempDir()
	st, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	baseAddr := common.HexToAddress("0x1111111111111111111111111111111111111111")
	liveAddr := common.HexToAddress("0x2222222222222222222222222222222222222222")
	genAddr := common.HexToAddress("0x3333333333333333333333333333333333333333")
	deadAddr := common.HexToAddress("0x4444444444444444444444444444444444444444")
	slot := common.HexToHash("0x05")
	baseVal := []byte{0x11}
	liveVal := []byte{0x22}

	// Everything captured is above the floor.
	app := func(block uint64, frame []byte) {
		t.Helper()
		if err := st.AppendWrites(block, frame); err != nil {
			t.Fatal(err)
		}
	}
	app(floor+10, frStorage(nil, liveAddr, slot, liveVal))
	app(floor+20, frStorage(nil, baseAddr, slot, liveVal)) // shadows the base
	app(floor+30, frAccount(nil, deadAddr, nil))           // SELFDESTRUCT
	if err := st.FlushAndSetExecHead(floor + 40); err != nil {
		t.Fatal(err)
	}
	if err := CookIndex(dir); err != nil {
		t.Fatal(err)
	}

	codeHash := crypto.Keccak256Hash([]byte{0xfe, 0xed})
	fb := &fakeBase{
		block: floor,
		acc:   map[common.Address][]byte{baseAddr: accRLP(t, 3, 42), deadAddr: accRLP(t, 1, 1)},
		slots: map[string][]byte{
			string(append(baseAddr.Bytes(), slot[:]...)): baseVal,
			string(append(deadAddr.Bytes(), slot[:]...)): baseVal,
		},
		code:    map[common.Hash][]byte{codeHash: {0xfe, 0xed}},
		headers: map[uint64][]byte{floor - 1: {0xaa}},
	}
	withBase(t, fb)

	// The genesis alloc is present and must be ignored entirely.
	alloc := types.GenesisAlloc{genAddr: types.Account{Balance: big.NewInt(55), Nonce: 9,
		Storage: map[common.Hash]common.Hash{slot: common.HexToHash("0x63")}}}
	h, err := OpenHistory(dir, st, alloc)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	if h.Floor() != floor {
		t.Fatalf("floor=%d want %d", h.Floor(), floor)
	}

	// 1. Below the floor: pruned error, never a genesis answer.
	for _, n := range []uint64{0, 1, floor - 1} {
		var pe *PrunedError
		if _, err := h.AccountAt(genAddr, n); !errors.As(err, &pe) {
			t.Fatalf("AccountAt(genesis addr, %d): err=%v, want *PrunedError", n, err)
		}
		if _, err := h.AccountAt(baseAddr, n); !errors.As(err, &pe) {
			t.Fatalf("AccountAt(base addr, %d): err=%v, want *PrunedError", n, err)
		}
		if _, err := h.StorageAt(genAddr, slot[:], n); !errors.As(err, &pe) {
			t.Fatalf("StorageAt(%d): err=%v, want *PrunedError", n, err)
		}
	}

	// 2. At and above the floor: the base answers what nothing else has.
	acc, err := h.AccountAt(baseAddr, floor)
	if err != nil || acc == nil {
		t.Fatalf("base account at floor: %+v %v", acc, err)
	}
	if acc.Nonce != 3 || acc.Balance.Uint64() != 42 || acc.Root != SentinelStorageRoot {
		t.Fatalf("base account: %+v", acc)
	}
	if got, err := h.StorageAt(baseAddr, slot[:], floor); err != nil || !bytes.Equal(got, baseVal) {
		t.Fatalf("base slot at floor: %x %v", got, err)
	}
	// 3. A write above the floor shadows the base.
	if got, _ := h.StorageAt(baseAddr, slot[:], floor+25); !bytes.Equal(got, liveVal) {
		t.Fatalf("shadowed slot: %x want %x", got, liveVal)
	}
	if got, _ := h.StorageAt(baseAddr, slot[:], floor+19); !bytes.Equal(got, baseVal) {
		t.Fatalf("slot just below the shadowing write: %x want %x", got, baseVal)
	}
	// 4. Missing everywhere is a clean zero, and the genesis alloc stays out
	// of it (the base file is the floor, genesis included).
	if acc, err := h.AccountAt(genAddr, floor+40); err != nil || acc != nil {
		t.Fatalf("genesis alloc must not resurface above the floor: %+v %v", acc, err)
	}
	if got, err := h.StorageAt(genAddr, slot[:], floor+40); err != nil || got != nil {
		t.Fatalf("genesis storage must not resurface: %x %v", got, err)
	}
	// 5. A SELFDESTRUCT above the floor kills the base's account+storage.
	if acc, _ := h.AccountAt(deadAddr, floor+35); acc != nil {
		t.Fatalf("destructed account visible: %+v", acc)
	}
	if got, _ := h.StorageAt(deadAddr, slot[:], floor+35); got != nil {
		t.Fatalf("destructed account's base storage visible: %x", got)
	}
	if got, _ := h.StorageAt(deadAddr, slot[:], floor+29); !bytes.Equal(got, baseVal) {
		t.Fatalf("base storage before the destruct: %x want %x", got, baseVal)
	}
	// 6. Code and headers fall back to the base.
	if blob, err := h.CodeByHash(codeHash); err != nil || !bytes.Equal(blob, []byte{0xfe, 0xed}) {
		t.Fatalf("code from base: %x %v", blob, err)
	}
	if _, err := h.CodeByHash(crypto.Keccak256Hash([]byte("absent"))); err == nil {
		t.Fatal("absent code must still fail loudly")
	}
	if raw, ok, err := h.HeaderRLP(floor - 1); err != nil || !ok || !bytes.Equal(raw, []byte{0xaa}) {
		t.Fatalf("header from base BLOCKHASH window: %x %v %v", raw, ok, err)
	}
}

// TestOpenHistoryBaseOnly: a freshly state-synced node has a base file and
// nothing else.
func TestOpenHistoryBaseOnly(t *testing.T) {
	dir := t.TempDir()
	st, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	fb := &fakeBase{block: 900, acc: map[common.Address][]byte{
		common.HexToAddress("0xaa"): accRLP(t, 1, 5),
	}}
	withBase(t, fb)

	h, err := OpenHistory(dir, st, nil)
	if err != nil {
		t.Fatalf("base-only open: %v", err)
	}
	if h.Floor() != 900 || h.Head() != 900 {
		t.Fatalf("floor=%d head=%d want 900/900", h.Floor(), h.Head())
	}
	if acc, err := h.AccountAt(common.HexToAddress("0xaa"), 900); err != nil || acc == nil || acc.Nonce != 1 {
		t.Fatalf("base account: %+v %v", acc, err)
	}
	h.Close()
	if !fb.closed {
		t.Fatal("History.Close must close the base file")
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

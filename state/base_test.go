package state

import (
	"bytes"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/crypto"
	"github.com/ava-labs/libevm/rlp"
	"github.com/holiman/uint256"
)

func baseHdr(t *testing.T, n uint64, root common.Hash) []byte {
	t.Helper()
	raw, err := rlp.EncodeToBytes(&types.Header{
		Number:     new(big.Int).SetUint64(n),
		Difficulty: big.NewInt(1),
		Root:       root,
	})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func baseRoot(n uint64) common.Hash { return common.BigToHash(new(big.Int).SetUint64(n + 1)) }

func codeAccRLP(t *testing.T, nonce uint64, balance int64, codeHash common.Hash) []byte {
	t.Helper()
	raw, err := rlp.EncodeToBytes(&types.StateAccount{
		Nonce:    nonce,
		Balance:  uint256.NewInt(uint64(balance)),
		Root:     types.EmptyRootHash,
		CodeHash: codeHash.Bytes(),
	})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// baseCorpus stages a small synthetic corpus covering every liveness rule
// and returns the opened History plus the genesis alloc it was opened with.
func baseCorpus(t *testing.T) (*History, string) {
	t.Helper()
	dir := t.TempDir()
	st, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })

	app := func(block uint64, frame []byte) {
		t.Helper()
		if err := st.AppendWrites(block, frame); err != nil {
			t.Fatal(err)
		}
	}
	app(10, frStorage(frAccount(nil, baseA, accRLP(t, 1, 100)), baseA, baseSlot1, baseV1))
	app(20, frStorage(nil, baseA, baseSlot2, baseV2))
	app(25, frStorage(nil, baseB, baseSlot1, baseV1))
	app(30, frAccount(nil, baseB, accRLP(t, 1, 5)))
	app(40, frAccount(nil, baseB, nil)) // SELFDESTRUCT: orphans slot1 at 25
	app(50, frAccount(nil, baseC, codeAccRLP(t, 1, 1, baseCodeHash)))
	app(60, frStorage(nil, baseA, baseSlot2, nil)) // zero write
	app(62, frAccount(nil, baseA, accRLP(t, 2, 200)))
	app(70, frAccount(nil, baseD, accRLP(t, 1, 1)))   // above the floor
	app(80, frStorage(nil, baseA, baseSlot1, baseV3)) // above the floor
	if err := st.PutCode(baseCodeHash, baseCodeBlob); err != nil {
		t.Fatal(err)
	}
	for n := uint64(0); n <= 90; n++ {
		if err := st.AppendHeader(n, baseHdr(t, n, baseRoot(n))); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.FlushAndSetExecHead(90); err != nil {
		t.Fatal(err)
	}
	if err := CookIndex(dir); err != nil {
		t.Fatal(err)
	}

	alloc := types.GenesisAlloc{baseGen: types.Account{
		Balance: big.NewInt(55), Nonce: 9, Code: baseGenCode,
	}}
	h, err := OpenHistory(dir, st, alloc)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(h.Close)
	return h, dir
}

var (
	baseA   = common.HexToAddress("0x1111111111111111111111111111111111111111")
	baseB   = common.HexToAddress("0x2222222222222222222222222222222222222222")
	baseC   = common.HexToAddress("0x3333333333333333333333333333333333333333")
	baseD   = common.HexToAddress("0x4444444444444444444444444444444444444444")
	baseGen = common.HexToAddress("0x5555555555555555555555555555555555555555")

	baseSlot1 = common.HexToHash("0x01")
	baseSlot2 = common.HexToHash("0x02")
	baseV1    = []byte{0x11}
	baseV2    = []byte{0x22}
	baseV3    = []byte{0x33}

	baseCodeBlob = []byte{0x60, 0x60, 0x60, 0x40}
	baseCodeHash = crypto.Keccak256Hash(baseCodeBlob)
	baseGenCode  = []byte{0xfe, 0xed}
	baseGenHash  = crypto.Keccak256Hash(baseGenCode)
)

func TestBaseBuildAndRead(t *testing.T) {
	h, _ := baseCorpus(t)
	out := t.TempDir()

	var sunk int
	path, err := BuildBase(h, out, 65, func(key [sortedKeySize]byte, val []byte) error {
		if len(val) == 0 {
			t.Fatalf("sink got an empty value for %x", key[:21])
		}
		sunk++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if filepath.Base(path) != "base_65" {
		t.Fatalf("path %s", path)
	}
	// A(acct+slot1), C(acct), gen(acct): code rows never reach the sink.
	if sunk != 4 {
		t.Fatalf("sink saw %d rows, want 4", sunk)
	}

	b, ok, err := OpenBase(out)
	if err != nil || !ok {
		t.Fatalf("OpenBase: ok=%v err=%v", ok, err)
	}
	defer b.Close()

	if b.Block() != 65 {
		t.Fatalf("block=%d want 65", b.Block())
	}
	if b.StateRoot() != baseRoot(65) {
		t.Fatalf("root=%x want %x", b.StateRoot(), baseRoot(65))
	}

	acct := func(a common.Address) []byte {
		t.Helper()
		v, ok, err := b.Account(a)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			return nil
		}
		return v
	}
	// Last write at or before B wins.
	if got := acct(baseA); !bytes.Equal(got, accRLP(t, 2, 200)) {
		t.Fatalf("account A: %x", got)
	}
	// Last write is a delete: absent, not a row with an empty value.
	if got := acct(baseB); got != nil {
		t.Fatalf("destructed account B present: %x", got)
	}
	if got := acct(baseC); !bytes.Equal(got, codeAccRLP(t, 1, 1, baseCodeHash)) {
		t.Fatalf("account C: %x", got)
	}
	// Written only above the floor: not in the base.
	if got := acct(baseD); got != nil {
		t.Fatalf("above-floor account D present: %x", got)
	}
	// Never written: the genesis alloc floor is folded in.
	gen := acct(baseGen)
	if gen == nil {
		t.Fatal("genesis alloc account missing from the base")
	}
	var ga types.StateAccount
	if err := rlp.DecodeBytes(gen, &ga); err != nil {
		t.Fatal(err)
	}
	if ga.Nonce != 9 || ga.Balance.Uint64() != 55 || !bytes.Equal(ga.CodeHash, baseGenHash[:]) {
		t.Fatalf("genesis account: %+v", ga)
	}

	slot := func(a common.Address, s common.Hash) []byte {
		t.Helper()
		v, ok, err := b.Storage(a, s[:])
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			return nil
		}
		return v
	}
	if got := slot(baseA, baseSlot1); !bytes.Equal(got, baseV1) {
		t.Fatalf("A/slot1: %x want %x (the write at 80 is above the floor)", got, baseV1)
	}
	if got := slot(baseA, baseSlot2); got != nil {
		t.Fatalf("zero-written A/slot2 present: %x", got)
	}
	if got := slot(baseB, baseSlot1); got != nil {
		t.Fatalf("orphaned B/slot1 present: %x (SELFDESTRUCT drops the storage trie)", got)
	}
	if got := slot(baseC, baseSlot1); got != nil {
		t.Fatalf("never-written slot present: %x", got)
	}

	for _, tc := range []struct {
		hash common.Hash
		want []byte
	}{{baseCodeHash, baseCodeBlob}, {baseGenHash, baseGenCode}} {
		blob, ok, err := b.Code(tc.hash)
		if err != nil || !ok || !bytes.Equal(blob, tc.want) {
			t.Fatalf("code %x: %x ok=%v err=%v", tc.hash, blob, ok, err)
		}
	}
	if _, ok, _ := b.Code(crypto.Keccak256Hash([]byte("absent"))); ok {
		t.Fatal("unknown code hash reported found")
	}

	// The BLOCKHASH window is the 256 blocks BELOW B (here 0..64).
	for _, n := range []uint64{0, 1, 64} {
		raw, ok, err := b.HeaderRLP(n)
		if err != nil || !ok || !bytes.Equal(raw, baseHdr(t, n, baseRoot(n))) {
			t.Fatalf("header %d: ok=%v err=%v", n, ok, err)
		}
	}
	for _, n := range []uint64{65, 66, 90} {
		if _, ok, _ := b.HeaderRLP(n); ok {
			t.Fatalf("header %d must be outside the window", n)
		}
	}
}

// TestBaseMergesEpochsAndBuckets: sealed epoch rows and raw bucket rows are
// merged by key, and the newest write at or before B wins across sources.
func TestBaseMergesEpochsAndBuckets(t *testing.T) {
	h, dir := baseCorpus(t)
	h.Close() // reopen below, after sealing an epoch into the same dir

	// Epoch rows for the same keys the buckets carry, plus one key that
	// exists ONLY in the epoch.
	onlyKey := storageKey(baseC, baseSlot2[:])
	in := &EpochInput{Start: 1, TxHashes: map[uint64][][32]byte{}}
	for i := 0; i < 20; i++ {
		in.Containers = append(in.Containers, []byte("container"))
		in.Headers = append(in.Headers, baseHdr(t, uint64(i+1), baseRoot(uint64(i+1))))
	}
	var k1, k2 [sortedKeySize]byte
	copy(k1[:], storageKey(baseA, baseSlot1[:]))
	copy(k2[:], onlyKey)
	in.StateRows = []StateRow{
		{Key: k1, Block: 5, Value: []byte{0xee}}, // shadowed by the bucket write at 10
		{Key: k2, Block: 6, Value: baseV2},       // only source for this key
	}
	if _, err := BuildEpoch(dir, in); err != nil {
		t.Fatal(err)
	}

	st, err := OpenReadOnly(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	h2, err := OpenHistory(dir, st, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer h2.Close()

	out := t.TempDir()
	if _, err := BuildBase(h2, out, 65, nil); err != nil {
		t.Fatal(err)
	}
	b, ok, err := OpenBase(out)
	if err != nil || !ok {
		t.Fatalf("OpenBase: ok=%v err=%v", ok, err)
	}
	defer b.Close()

	if v, ok, _ := b.Storage(baseA, baseSlot1[:]); !ok || !bytes.Equal(v, baseV1) {
		t.Fatalf("A/slot1 = %x ok=%v, want the bucket write %x", v, ok, baseV1)
	}
	if v, ok, _ := b.Storage(baseC, baseSlot2[:]); !ok || !bytes.Equal(v, baseV2) {
		t.Fatalf("epoch-only slot = %x ok=%v, want %x", v, ok, baseV2)
	}
}

func TestOpenBaseFailsLoudly(t *testing.T) {
	h, _ := baseCorpus(t)
	src := t.TempDir()
	path, err := BuildBase(h, src, 65, nil)
	if err != nil {
		t.Fatal(err)
	}
	good, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	if _, ok, err := OpenBase(t.TempDir()); ok || err != nil {
		t.Fatalf("empty dir: ok=%v err=%v, want false/nil", ok, err)
	}

	corrupt := func(name string, b []byte) {
		t.Helper()
		dir := t.TempDir()
		p := filepath.Join(dir, "base_65")
		if err := os.WriteFile(p, b, 0o644); err != nil {
			t.Fatal(err)
		}
		bs, ok, err := OpenBase(dir)
		if err == nil {
			if ok {
				bs.Close()
			}
			t.Fatalf("%s: opened without error", name)
		}
	}
	corrupt("truncated tail", good[:len(good)-8])
	corrupt("truncated head", good[64:])
	corrupt("empty", nil)
	bad := append([]byte(nil), good...)
	bad[len(bad)-baseFooterSize] ^= 0xff // head magic
	corrupt("footer magic", bad)
	bad = append([]byte(nil), good...)
	bad[len(bad)-baseFooterSize+4] = 9 // version
	corrupt("version", bad)
	bad = append([]byte(nil), good...)
	bad[len(bad)-baseFooterSize+60] = 0xff // section length
	corrupt("section bounds", bad)

	// Two base files in one directory: ambiguous floor, refuse to guess.
	two := t.TempDir()
	for _, n := range []string{"base_65", "base_66"} {
		if err := os.WriteFile(filepath.Join(two, n), good, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if _, _, err := OpenBase(two); err == nil || !strings.Contains(err.Error(), "ambiguous") {
		t.Fatalf("two base files: err=%v", err)
	}
}

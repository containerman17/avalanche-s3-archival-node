package state

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/crypto"
)

func codeBlob(seed byte, n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = seed ^ byte(i%251)
	}
	return b
}

// codeEpochInput is synthEpoch plus a code map and account rows referencing
// it, i.e. an epoch that deploys code.
func codeEpochInput(t *testing.T, start uint64) (*EpochInput, map[common.Hash][]byte) {
	t.Helper()
	dir := t.TempDir()
	in, _ := synthEpoch(t, dir, start)
	code := map[common.Hash][]byte{}
	for i := byte(1); i <= 5; i++ {
		blob := codeBlob(i, 200*int(i))
		hash := crypto.Keccak256Hash(blob)
		code[hash] = blob
		in.StateRows = append(in.StateRows, StateRow{
			Key:   accountKeyArr(common.Address{i}),
			Block: start + uint64(i),
			Value: codeAccRLP(t, uint64(i), 1, hash),
		})
	}
	in.Code = code
	return in, code
}

func accountKeyArr(addr common.Address) (k [sortedKeySize]byte) {
	k[0] = recKindAccount
	copy(k[1:21], addr[:])
	return
}

// TestEpochCodeRoundtrip: code rows survive seal and read, ride the bloom,
// and do not disturb the state rows sharing the keyspace.
func TestEpochCodeRoundtrip(t *testing.T) {
	dir := t.TempDir()
	in, code := codeEpochInput(t, 2000)
	path, err := BuildEpoch(dir, in)
	if err != nil {
		t.Fatal(err)
	}
	e, err := OpenEpoch(path)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	for hash, blob := range code {
		got, ok, err := e.Code(hash)
		if err != nil || !ok || !bytes.Equal(got, blob) {
			t.Fatalf("code %x: %x ok=%v err=%v", hash[:4], got, ok, err)
		}
	}
	if _, ok, err := e.Code(crypto.Keccak256Hash([]byte("never deployed"))); ok || err != nil {
		t.Fatalf("unknown code hash: ok=%v err=%v", ok, err)
	}
	// state rows still resolve across the inserted 'c' run
	k1 := synthKey('s', 1)
	if v, _, found, _ := e.StateSearch(k1[:], 2099); !found || !bytes.Equal(v, []byte{0x22}) {
		t.Fatalf("state row after code insertion: %x %v", v, found)
	}
	if v, _, found, _ := e.StateSearch(accountKeyArrSlice(common.Address{3}), 2099); !found || len(v) == 0 {
		t.Fatalf("account row after code insertion: %x %v", v, found)
	}
	// code rows are not state diffs: the verification spill must skip them
	c, err := e.SpillDiffs(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	var seen int
	for {
		blk, rows, ok, err := c.Next()
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		for _, r := range rows {
			if r.Key[0] == recKindCodeUse {
				t.Fatalf("code row leaked into the block %d diff", blk)
			}
			seen++
		}
	}
	if seen == 0 {
		t.Fatal("no state diffs at all")
	}
}

func accountKeyArrSlice(addr common.Address) []byte {
	k := accountKeyArr(addr)
	return k[:]
}

// TestEpochCodeDeterminism: two independent seals of the same input produce
// identical bytes. The code map is a Go map, so this is the guard that map
// iteration order never reaches the file.
func TestEpochCodeDeterminism(t *testing.T) {
	dirA, dirB := t.TempDir(), t.TempDir()
	inA, code := codeEpochInput(t, 3000)
	pathA, err := BuildEpoch(dirA, inA)
	if err != nil {
		t.Fatal(err)
	}
	// rebuild the input from scratch (fresh row slice, fresh map) so nothing
	// is carried over from the first build
	inB, _ := codeEpochInput(t, 3000)
	if len(inB.Code) != len(code) {
		t.Fatal("inputs diverged")
	}
	pathB, err := BuildEpoch(dirB, inB)
	if err != nil {
		t.Fatal(err)
	}
	a, err := os.ReadFile(pathA)
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(pathB)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(a, b) {
		t.Fatalf("independent seals differ: %d vs %d bytes", len(a), len(b))
	}
}

// TestSealCodeEndToEnd drives the real sealer over a small corpus: the code
// blobs land in the epochs following the placement rule, and History serves
// them through the descent.
func TestSealCodeEndToEnd(t *testing.T) {
	dir := t.TempDir()
	for n := uint64(1); n <= 8; n++ {
		writeStagingBlock(t, dir, 0, n, 3)
	}
	st, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	blobA, blobB := codeBlob(0xa1, 500), codeBlob(0xb2, 700)
	hashA, hashB := crypto.Keccak256Hash(blobA), crypto.Keccak256Hash(blobB)
	addrA, addrB := common.Address{0xa}, common.Address{0xb}
	for n := uint64(1); n <= 8; n++ {
		if err := st.AppendHeader(n, []byte{0x99, byte(n)}); err != nil {
			t.Fatal(err)
		}
		var frame []byte
		switch n {
		case 2:
			frame = frAccount(nil, addrA, codeAccRLP(t, 1, 10, hashA))
		case 6:
			frame = frAccount(nil, addrB, codeAccRLP(t, 1, 20, hashB))
		default:
			frame = frAccount(nil, addrA, accRLP(t, n, 1))
		}
		if err := st.AppendWrites(n, frame); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.PutCode(hashA, blobA); err != nil {
		t.Fatal(err)
	}
	if err := st.PutCode(hashB, blobB); err != nil {
		t.Fatal(err)
	}
	if err := st.SetLogsStart(1); err != nil {
		t.Fatal(err)
	}
	if err := st.FlushAndSetExecHead(8); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	// 3 txs/block, boundary 10 => epochs 1-4 and 5-8.
	if err := sealEpochs(dir, dir, 10, nil); err != nil {
		t.Fatal(err)
	}

	set, err := OpenEpochSet(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(set.Epochs) != 2 {
		t.Fatalf("epochs: %d", len(set.Epochs))
	}
	e1, e2 := set.Epochs[0], set.Epochs[1]
	// each epoch carries the code of the accounts IT wrote, and only those
	if got, ok, _ := e1.Code(hashA); !ok || !bytes.Equal(got, blobA) {
		t.Fatalf("epoch1 code A: %x ok=%v", got, ok)
	}
	if _, ok, _ := e1.Code(hashB); ok {
		t.Fatal("epoch1 must not carry code deployed in epoch2")
	}
	if got, ok, _ := e2.Code(hashB); !ok || !bytes.Equal(got, blobB) {
		t.Fatalf("epoch2 code B: %x ok=%v", got, ok)
	}
	set.Close()

	ro, err := OpenReadOnly(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer ro.Close()

	// and the served read path finds both blobs through the descent
	h, err := OpenHistory(dir, ro, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	// addrA carries code only at block 2 (later blocks rewrite it plain)
	if blob, err := h.CodeAt(addrA, 2); err != nil || !bytes.Equal(blob, blobA) {
		t.Fatalf("CodeAt A: %x %v", blob, err)
	}
	if blob, err := h.CodeAt(addrB, 8); err != nil || !bytes.Equal(blob, blobB) {
		t.Fatalf("CodeAt B: %x %v", blob, err)
	}
}

// TestCodeFromEpochsWithoutCodeLog: a node holding nothing but epoch files
// (the download-bootstrap case, no code.log, no writelogs, no misc.log) still
// answers eth_getCode. Models base_test's equivalent property for the base
// file.
func TestCodeFromEpochsWithoutCodeLog(t *testing.T) {
	dir := t.TempDir()
	in, code := codeEpochInput(t, 1)
	if _, err := BuildEpoch(dir, in); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "code.log")); !os.IsNotExist(err) {
		t.Fatal("fixture must have no code.log")
	}
	st, err := OpenReadOnly(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if st.CodeCount() != 0 {
		t.Fatalf("code.log absent but %d blobs", st.CodeCount())
	}
	genCode := []byte{0xfe, 0xed}
	h, err := OpenHistory(dir, st, types.GenesisAlloc{
		common.Address{0x99}: types.Account{Code: genCode},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()

	for hash, blob := range code {
		got, err := h.CodeByHash(hash)
		if err != nil || !bytes.Equal(got, blob) {
			t.Fatalf("CodeByHash %x: %x %v", hash[:4], got, err)
		}
	}
	// the account's own code resolves through the descent at a block inside
	// the epoch
	if got, err := h.CodeAt(common.Address{3}, in.Start+50); err != nil || !bytes.Equal(got, code[crypto.Keccak256Hash(codeBlob(3, 600))]) {
		t.Fatalf("CodeAt: %x %v", got, err)
	}
	// genesis alloc code is never deployed by a block, so no epoch has it
	if got, err := h.CodeByHash(crypto.Keccak256Hash(genCode)); err != nil || !bytes.Equal(got, genCode) {
		t.Fatalf("genesis code: %x %v", got, err)
	}
	if _, err := h.CodeByHash(crypto.Keccak256Hash([]byte("absent"))); err == nil {
		t.Fatal("absent code must fail loudly")
	}
}

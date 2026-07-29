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

var (
	baseA   = common.HexToAddress("0x1111111111111111111111111111111111111111")
	baseB   = common.HexToAddress("0x2222222222222222222222222222222222222222")
	baseC   = common.HexToAddress("0x3333333333333333333333333333333333333333")
	baseD   = common.HexToAddress("0x4444444444444444444444444444444444444444")

	baseSlot1 = common.HexToHash("0x01")
	baseSlot2 = common.HexToHash("0x02")
	baseV1    = []byte{0x11}
	baseV2    = []byte{0x22}

	baseCodeBlob = []byte{0x60, 0x60, 0x60, 0x40}
	baseCodeHash = crypto.Keccak256Hash(baseCodeBlob)
)

// writeTestBase builds a small base file covering all three row kinds.
// Rows are handed in unsorted on purpose: WriteBase owns the ordering.
func writeTestBase(t *testing.T, dir string, block uint64) string {
	t.Helper()
	row := func(key []byte, val []byte) BaseRow {
		var r BaseRow
		copy(r.Key[:], key)
		r.Val = val
		return r
	}
	ck := epochCodeKey(baseCodeHash)
	rows := []BaseRow{
		row(storageKey(baseA, baseSlot1[:]), baseV1),
		row(accountKey(baseC), codeAccRLP(t, 1, 1, baseCodeHash)),
		row(ck[:], baseCodeBlob),
		row(accountKey(baseA), accRLP(t, 2, 200)),
		row(storageKey(baseC, baseSlot2[:]), baseV2),
	}
	m := BaseMeta{Block: block, CumTx: 7777, Root: baseRoot(block)}
	if block > baseHeaderWindow {
		m.HdrFrom = block - baseHeaderWindow
	}
	for n := m.HdrFrom; n <= block; n++ {
		m.Headers = append(m.Headers, baseHdr(t, n, baseRoot(n)))
	}
	path, err := WriteBase(dir, m, rows)
	if err != nil {
		t.Fatal(err)
	}
	return path
}

func TestBaseWriteAndRead(t *testing.T) {
	dir := t.TempDir()
	if got := filepath.Base(writeTestBase(t, dir, 65)); got != "base_65" {
		t.Fatalf("path %s", got)
	}

	b, ok, err := OpenBase(dir)
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
	// The canonical-boundary anchor: nothing else in the file implies it.
	if b.CumTx() != 7777 {
		t.Fatalf("cumTx=%d want 7777", b.CumTx())
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
	if got := acct(baseA); !bytes.Equal(got, accRLP(t, 2, 200)) {
		t.Fatalf("account A: %x", got)
	}
	if got := acct(baseC); !bytes.Equal(got, codeAccRLP(t, 1, 1, baseCodeHash)) {
		t.Fatalf("account C: %x", got)
	}
	if got := acct(baseD); got != nil {
		t.Fatalf("absent account D present: %x", got)
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
		t.Fatalf("A/slot1: %x want %x", got, baseV1)
	}
	if got := slot(baseC, baseSlot2); !bytes.Equal(got, baseV2) {
		t.Fatalf("C/slot2: %x want %x", got, baseV2)
	}
	if got := slot(baseB, baseSlot1); got != nil {
		t.Fatalf("absent slot present: %x", got)
	}

	blob, ok, err := b.Code(baseCodeHash)
	if err != nil || !ok || !bytes.Equal(blob, baseCodeBlob) {
		t.Fatalf("code: %x ok=%v err=%v", blob, ok, err)
	}
	if _, ok, _ := b.Code(crypto.Keccak256Hash([]byte("absent"))); ok {
		t.Fatal("unknown code hash reported found")
	}

	// The BLOCKHASH window is [B-256, B] (here 0..65): header(B) is in the
	// file so a base-only node is self-contained at its own floor.
	for _, n := range []uint64{0, 1, 64, 65} {
		raw, ok, err := b.HeaderRLP(n)
		if err != nil || !ok || !bytes.Equal(raw, baseHdr(t, n, baseRoot(n))) {
			t.Fatalf("header %d: ok=%v err=%v", n, ok, err)
		}
	}
	for _, n := range []uint64{66, 90} {
		if _, ok, _ := b.HeaderRLP(n); ok {
			t.Fatalf("header %d must be outside the window", n)
		}
	}
}

// TestBaseReadsWithoutKeccak is the point of format v2: an account/storage
// probe is a direct preimage lookup. Reading through a stub keccak would need
// libevm surgery, so the check is structural: the key the reader probes with
// is byte-identical to the 53-byte epoch/bucket key, and the same value comes
// back through the raw row walk.
func TestBaseReadsWithoutKeccak(t *testing.T) {
	dir := t.TempDir()
	writeTestBase(t, dir, 65)
	b, ok, err := OpenBase(dir)
	if err != nil || !ok {
		t.Fatalf("OpenBase: ok=%v err=%v", ok, err)
	}
	defer b.Close()

	want := map[string][]byte{
		string(accountKey(baseA)):               accRLP(t, 2, 200),
		string(storageKey(baseA, baseSlot1[:])): baseV1,
	}
	got := map[string][]byte{}
	if err := b.walk(func(key, val []byte) error {
		if _, ok := want[string(key)]; ok {
			got[string(key)] = append([]byte(nil), val...)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for k, v := range want {
		if !bytes.Equal(got[k], v) {
			t.Fatalf("row %x on disk is %x, want %x (keys must be preimages)", k, got[k], v)
		}
		probe, _, err := b.lookup([]byte(k))
		if err != nil || !bytes.Equal(probe, v) {
			t.Fatalf("direct probe of %x: %x err=%v", k, probe, err)
		}
	}
}

// TestBaseWalkRowsHashes: the Firewood load path (exec.startFromBase) gets
// 65-byte HASHED keys computed at iteration time, while the disk stays
// preimage-keyed.
func TestBaseWalkRowsHashes(t *testing.T) {
	dir := t.TempDir()
	writeTestBase(t, dir, 65)
	b, ok, err := OpenBase(dir)
	if err != nil || !ok {
		t.Fatalf("OpenBase: ok=%v err=%v", ok, err)
	}
	defer b.Close()

	seen := map[string][]byte{}
	n := 0
	if err := b.WalkRows(func(key, val []byte) error {
		if len(key) != baseHashedKeySize {
			t.Fatalf("walk key is %d bytes, want %d", len(key), baseHashedKeySize)
		}
		n++
		seen[string(append([]byte(nil), key...))] = append([]byte(nil), val...)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if n != 5 {
		t.Fatalf("walked %d rows, want 5", n)
	}

	aHash := crypto.Keccak256Hash(baseA[:])
	wantAcct := append([]byte{'a'}, aHash[:]...)
	wantAcct = append(wantAcct, make([]byte, 32)...)
	if !bytes.Equal(seen[string(wantAcct)], accRLP(t, 2, 200)) {
		t.Fatalf("account row not keyed by keccak(addr)")
	}
	wantSlot := append([]byte{'s'}, aHash[:]...)
	wantSlot = append(wantSlot, crypto.Keccak256(baseSlot1[:])...)
	if !bytes.Equal(seen[string(wantSlot)], baseV1) {
		t.Fatalf("storage row not keyed by keccak(addr)||keccak(slot)")
	}
	wantCode := append([]byte{'c'}, baseCodeHash[:]...)
	wantCode = append(wantCode, make([]byte, 32)...)
	if !bytes.Equal(seen[string(wantCode)], baseCodeBlob) {
		t.Fatalf("code row not keyed by its content hash")
	}
}

func TestOpenBaseFailsLoudly(t *testing.T) {
	src := t.TempDir()
	good, err := os.ReadFile(writeTestBase(t, src, 65))
	if err != nil {
		t.Fatal(err)
	}

	if _, ok, err := OpenBase(t.TempDir()); ok || err != nil {
		t.Fatalf("empty dir: ok=%v err=%v, want false/nil", ok, err)
	}

	corrupt := func(name string, b []byte, wantMsg string) {
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
		if wantMsg != "" && !strings.Contains(err.Error(), wantMsg) {
			t.Fatalf("%s: %v", name, err)
		}
	}
	corrupt("truncated tail", good[:len(good)-8], "")
	corrupt("truncated head", good[64:], "")
	corrupt("empty", nil, "")
	bad := append([]byte(nil), good...)
	bad[len(bad)-baseFooterSize] ^= 0xff // head magic
	corrupt("footer magic", bad, "")
	bad = append([]byte(nil), good...)
	bad[len(bad)-baseFooterSize+68] = 0xff // section offset
	corrupt("section bounds", bad, "")

	// An older format is refused BY NAME: no migration exists, and a v1 file
	// is hash-keyed so nothing could re-key it anyway.
	bad = append([]byte(nil), good...)
	bad[len(bad)-baseFooterSize+4] = 1
	corrupt("v1 base file", bad, "format v1, unsupported")

	// Two base files in one directory: NEWEST WINS, not an error. The fold
	// renames the new snapshot in and unlinks the old one after, so this is
	// the normal transient state after a kill -9 in that window, and
	// refusing to guess left exec and serve unable to start at all. The
	// rename is ordered after the new file's fsync, so the highest B is
	// always the complete one.
	two := t.TempDir()
	writeTestBase(t, two, 65)
	writeTestBase(t, two, 66)
	b, ok, err := OpenBase(two)
	if err != nil || !ok {
		t.Fatalf("two base files: ok=%v err=%v", ok, err)
	}
	defer b.Close()
	if b.Block() != 66 {
		t.Fatalf("two base files: opened block %d, want the newest (66)", b.Block())
	}
	if blk, ok, err := PeekBase(two); err != nil || !ok || blk != 66 {
		t.Fatalf("PeekBase with two base files: %d ok=%v err=%v, want 66", blk, ok, err)
	}
}

// TestBaseWriterRejectsBadOrder pins the streaming writer's one invariant:
// the fold feeds it a merged stream and a duplicate or out-of-order key would
// make the file answer two values for one key (the sparse index and the
// lookup binary search both assume strict ascent).
func TestBaseWriterRejectsBadOrder(t *testing.T) {
	dir := t.TempDir()
	w, err := newBaseWriter(dir, BaseMeta{Block: 5}, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Abort()
	k := accountKey(baseB)
	if err := w.Add(k, []byte{1}); err != nil {
		t.Fatal(err)
	}
	if err := w.Add(k, []byte{2}); err == nil {
		t.Fatal("duplicate key accepted")
	}
	if err := w.Add(accountKey(baseA), []byte{3}); err == nil {
		t.Fatal("descending key accepted")
	}
	// The announced row count is what sizes the bloom: a mismatch would make
	// the file silently differ from an identical row set written elsewhere.
	if _, err := w.Finish(); err == nil || !strings.Contains(err.Error(), "announced") {
		t.Fatalf("wrong row count accepted: %v", err)
	}
}

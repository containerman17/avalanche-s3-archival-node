package verify

// THE ZERO-ROOT PREMISE, pinned.
//
// The snapshot fold emits captured post-images VERBATIM: no StackTrie pass,
// no storage-root reconstruction, no RLP patching. That is only sound because
// firewood-ethhash manages storage roots internally, so the StorageRoot field
// of a captured account RLP is the ZERO hash and Firewood substitutes the
// real one when it hashes. If that ever stopped being true, every snapshot
// would load to a wrong root and the fold's whole design would be wrong.
//
// TestZeroStorageRootPremise is the cheap permanent check: the same rows,
// once through the preimage-keyed UpdateAccount/UpdateStorage path the
// executor writes with, once through the raw ffi.Put path the snapshot load
// reads with, must produce the same root. The other net is CheckBase itself,
// which runs on the real artifact before it is ever named.

import (
	"bytes"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"

	ffi "github.com/ava-labs/firewood-go-ethhash/ffi"
	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/crypto"
	"github.com/ava-labs/libevm/rlp"
	"github.com/holiman/uint256"

	"github.com/containerman17/epochdb/fetch"
	"github.com/containerman17/epochdb/state"
)

var (
	vbA    = common.HexToAddress("0x1111111111111111111111111111111111111111")
	vbB    = common.HexToAddress("0x2222222222222222222222222222222222222222")
	vbC    = common.HexToAddress("0x3333333333333333333333333333333333333333")
	vbCode = []byte{0x60, 0x01, 0x60, 0x02}
)

// vbAccount is the capture encoding: ZERO storage root, always.
func vbAccount(t *testing.T, nonce uint64, bal int64, codeHash common.Hash) []byte {
	t.Helper()
	raw, err := rlp.EncodeToBytes(&types.StateAccount{
		Nonce: nonce, Balance: uint256.NewInt(uint64(bal)),
		Root: common.Hash{}, CodeHash: codeHash.Bytes(),
	})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// vbRows is one small state: two contracts with storage plus a plain account.
// Returned as state rows in the 53-byte preimage keyspace, key-sorted, which
// is exactly what an epoch SST, a raw bucket and a base file all hold.
func vbRows(t *testing.T) []state.StateRow {
	t.Helper()
	row := func(kind byte, addr common.Address, slot common.Hash, val []byte) state.StateRow {
		var r state.StateRow
		r.Key[0] = kind
		copy(r.Key[1:21], addr[:])
		if kind == 's' {
			copy(r.Key[21:53], slot[:])
		}
		r.Value = val
		r.Block = 1
		return r
	}
	slotVal := func(n int64) []byte {
		enc, err := rlp.EncodeToBytes(common.TrimLeftZeroes(common.BigToHash(big.NewInt(n)).Bytes()))
		if err != nil {
			t.Fatal(err)
		}
		return enc
	}
	rows := []state.StateRow{
		row('a', vbA, common.Hash{}, vbAccount(t, 3, 11, types.EmptyCodeHash)),
		row('a', vbB, common.Hash{}, vbAccount(t, 1, 0, crypto.Keccak256Hash(vbCode))),
		row('a', vbC, common.Hash{}, vbAccount(t, 0, 999, crypto.Keccak256Hash(vbCode))),
	}
	for i := int64(1); i <= 5; i++ {
		rows = append(rows, row('s', vbB, common.BigToHash(big.NewInt(i)), slotVal(i+41)))
		rows = append(rows, row('s', vbC, common.BigToHash(big.NewInt(i)), slotVal(i*7)))
	}
	return rows
}

// vbFirewoodRoot builds the state through the raw hashed-key ffi.Put path,
// which is what exec.startFromBase and CheckBase both do.
func vbFirewoodRoot(t *testing.T, rows []state.StateRow) common.Hash {
	t.Helper()
	tdb, fw, _, err := newThrowawayFirewood(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer tdb.Close()
	var batch []ffi.BatchOp
	for i := range rows {
		r := &rows[i]
		addrHash := crypto.Keccak256(r.Key[1:21])
		switch r.Key[0] {
		case 'a':
			batch = append(batch, ffi.Put(addrHash, bytes.Clone(r.Value)))
		case 's':
			key := append(addrHash, crypto.Keccak256(r.Key[21:53])...)
			enc, err := rlp.EncodeToBytes(r.Value)
			if err != nil {
				t.Fatal(err)
			}
			batch = append(batch, ffi.Put(key, enc))
		}
	}
	root, err := fw.Firewood.Update(batch)
	if err != nil {
		t.Fatal(err)
	}
	return common.Hash(root)
}

func TestZeroStorageRootPremise(t *testing.T) {
	fetch.RegisterExtras()
	rows := vbRows(t)

	// Path A: the preimage-keyed trie API the executor and the no-execution
	// verifier both write through.
	tdb, _, db, err := newThrowawayFirewood(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer tdb.Close()
	tr, err := db.OpenTrie(types.EmptyRootHash)
	if err != nil {
		t.Fatal(err)
	}
	if err := applyRows(tr, rows); err != nil {
		t.Fatal(err)
	}
	viaAPI := tr.Hash()

	// Path B: raw hashed-key puts of the SAME bytes, zero storage roots and
	// all, which is how a snapshot is loaded.
	viaPut := vbFirewoodRoot(t, rows)

	if viaAPI == (common.Hash{}) || viaAPI == types.EmptyRootHash {
		t.Fatalf("degenerate root %x: the test state is not being written", viaAPI)
	}
	if viaAPI != viaPut {
		t.Fatalf("ZERO-ROOT PREMISE BROKEN: UpdateAccount path gives %x, raw ffi.Put of the same zero-rooted rows gives %x. "+
			"The snapshot fold emits post-images verbatim on the strength of these being equal", viaAPI, viaPut)
	}
}

// writeVerifyBase turns the same rows into a real base file whose footer
// claims root.
func writeVerifyBase(t *testing.T, dir string, block uint64, rows []state.StateRow, root common.Hash) string {
	t.Helper()
	var brs []state.BaseRow
	for i := range rows {
		brs = append(brs, state.BaseRow{Key: rows[i].Key, Val: rows[i].Value})
	}
	hdr, err := rlp.EncodeToBytes(&types.Header{
		Number: new(big.Int).SetUint64(block), Difficulty: big.NewInt(1), Root: root,
	})
	if err != nil {
		t.Fatal(err)
	}
	path, err := state.WriteBase(dir, state.BaseMeta{
		Block: block, CumTx: 42, Root: root, HdrFrom: block, Headers: [][]byte{hdr},
	}, brs)
	if err != nil {
		t.Fatal(err)
	}
	return path
}

// TestCheckBaseTeeth: the producer's pre-rename gate must accept a snapshot
// whose rows rebuild the footer's root and reject one whose rows do not. This
// is the check that stands between a bad merge and a published manifest.
func TestCheckBaseTeeth(t *testing.T) {
	fetch.RegisterExtras()
	rows := vbRows(t)
	root := vbFirewoodRoot(t, rows)

	good := writeVerifyBase(t, t.TempDir(), 900, rows, root)
	if err := CheckBase(good); err != nil {
		t.Fatalf("a correct snapshot was rejected: %v", err)
	}

	// One corrupted row: the balance of an account nobody would look at.
	bad := make([]state.StateRow, len(rows))
	copy(bad, rows)
	bad[0].Value = vbAccount(t, 3, 12, types.EmptyCodeHash)
	badPath := writeVerifyBase(t, t.TempDir(), 900, bad, root)
	err := CheckBase(badPath)
	if err == nil || !strings.Contains(err.Error(), "rows rebuild root") {
		t.Fatalf("a snapshot with a corrupted row passed the gate: %v", err)
	}

	// And the gate has to work on the .tmp name, which is the only name the
	// file has when the fold calls it.
	dir := t.TempDir()
	src := writeVerifyBase(t, dir, 900, rows, root)
	tmp := filepath.Join(dir, "base_900.tmp")
	if err := os.Rename(src, tmp); err != nil {
		t.Fatal(err)
	}
	if err := CheckBase(tmp); err != nil {
		t.Fatalf("gate cannot verify a pre-rename temp file: %v", err)
	}
}

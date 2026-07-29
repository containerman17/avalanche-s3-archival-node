package verify

// THE ZERO-ROOT PREMISE, pinned.
//
// The snapshot fold emits captured post-images VERBATIM: no StackTrie pass,
// no storage-root reconstruction, no RLP patching. That is only sound because
// firewood-ethhash manages storage roots internally, so the StorageRoot field
// of a captured account RLP is the ZERO hash and Firewood substitutes the real
// one when it hashes. If that ever stopped being true, every snapshot would
// load to a wrong root.
//
// This is a LIBRARY-BEHAVIOUR pin, not a producer gate (the pre-rename fold
// gate was deleted 2026-07-29: verification is the load). It stays because
// exec.startFromBase depends on exactly this equality: it bulk-loads a
// snapshot's zero-rooted rows through raw ffi.Put and then expects the
// resulting frontier to answer as the canonical state at B. The same rows,
// once through the preimage-keyed UpdateAccount/UpdateStorage path the
// executor writes with, once through the raw ffi.Put path startFromBase reads
// with, must produce the same root.

import (
	"bytes"
	"math/big"
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
// which is what exec.startFromBase does when it loads a snapshot.
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
			"The snapshot fold emits post-images verbatim, and startFromBase loads them, on the strength of these being equal", viaAPI, viaPut)
	}
}

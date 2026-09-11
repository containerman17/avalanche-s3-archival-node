package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ava-labs/avalanchego/database/memdb"
	"github.com/ava-labs/avalanchego/database/prefixdb"
	"github.com/ava-labs/avalanchego/graft/coreth/plugin/evm/customtypes"
	"github.com/ava-labs/avalanchego/ids"
	evmdb "github.com/ava-labs/avalanchego/vms/evm/database"
	"github.com/ava-labs/avalanchego/vms/evm/sync/customrawdb"
	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/rawdb"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/crypto"
	"github.com/ava-labs/libevm/ethdb"
	"github.com/ava-labs/libevm/rlp"
	"github.com/ava-labs/libevm/trie"
	"github.com/holiman/uint256"
)

func TestMain(m *testing.M) {
	customtypes.Register()
	os.Exit(m.Run())
}

func hash(b byte) common.Hash { return common.Hash{b} }

func newDB() ethdb.Database {
	chainID := ids.FromStringOrPanic(cchainID)
	return rawdb.NewDatabase(evmdb.New(prefixdb.NewNested(ethDBPrefix, prefixdb.New(vmDBPrefix, prefixdb.New(chainID[:], memdb.New())))))
}

func TestExport(t *testing.T) {
	db := newDB()

	code := []byte{0x60, 0x00, 0x60, 0x00, 0xfd}
	codeHash := crypto.Keccak256Hash(code)
	rawdb.WriteCode(db, codeHash, code)

	bal3 := new(big.Int).Lsh(big.NewInt(1), 200)
	accts := []types.StateAccount{
		{Nonce: 1, Balance: uint256.NewInt(1e18), Root: hash(0xaa), CodeHash: codeHash[:]},
		{Nonce: 0, Balance: uint256.NewInt(0), Root: types.EmptyRootHash, CodeHash: types.EmptyCodeHash[:]},
		{Nonce: 7, Balance: uint256.MustFromBig(bal3), Root: hash(0xbb), CodeHash: codeHash[:]},
	}
	multi := []bool{false, false, true}
	for i, a := range accts {
		slim := types.SlimAccountRLP(a)
		if slim[len(slim)-1] != 0x80 {
			t.Fatalf("slim account %d does not end with the false multicoin flag: %x", i, slim)
		}
		if multi[i] {
			// customtypes has no exported setter for a bare StateAccount, so
			// hand-build the leaf coreth writes for a multicoin account.
			slim, _ = rlp.EncodeToBytes([]any{a.Nonce, a.Balance.ToBig(), a.Root[:], a.CodeHash, true})
		}
		rawdb.WriteAccountSnapshot(db, hash(byte(i+1)), slim)
	}
	slots := []struct {
		acct, slot byte
		val        []byte
	}{
		{1, 0x0a, []byte{1}},
		{1, 0x0b, bytes.Repeat([]byte{0xff}, 32)},
		{3, 0x01, []byte{0x12, 0x34}},
		{3, 0x02, append([]byte{0x80}, make([]byte, 31)...)},
	}
	for _, s := range slots {
		v, _ := rlp.EncodeToBytes(s.val)
		rawdb.WriteStorageSnapshot(db, hash(s.acct), hash(s.slot), v)
	}

	// Blocks 10 (snapshot) .. 12 (head).
	var blocks []*types.Block
	parent := hash(0x99)
	for n := uint64(10); n <= 12; n++ {
		h := &types.Header{ParentHash: parent, Number: new(big.Int).SetUint64(n), Root: hash(byte(n)), Time: n, GasLimit: 15_000_000, Difficulty: big.NewInt(1)}
		customtypes.SetHeaderExtra(h, &customtypes.HeaderExtra{ExtDataGasUsed: big.NewInt(int64(n)), BlockGasCost: big.NewInt(0)})
		var extdata []byte
		if n == 11 {
			extdata = []byte("atomic txs live here")
		}
		b := customtypes.NewBlockWithExtData(h, nil, nil, nil, trie.NewStackTrie(nil), extdata, true)
		rawdb.WriteBlock(db, b)
		blocks = append(blocks, b)
		parent = b.Hash()
	}
	rawdb.WriteHeadHeaderHash(db, blocks[2].Hash())
	rawdb.WriteSnapshotRoot(db, blocks[0].Root())
	if err := customrawdb.WriteSnapshotBlockHash(db, blocks[0].Hash()); err != nil {
		t.Fatal(err)
	}
	gen, _ := rlp.EncodeToBytes(journalGenerator{Marker: []byte{1}})
	rawdb.WriteSnapshotGenerator(db, gen)
	if err := export(db, t.TempDir()); err == nil || !strings.Contains(err.Error(), "not finished") {
		t.Fatalf("unfinished generator not rejected: %v", err)
	}
	gen, _ = rlp.EncodeToBytes(journalGenerator{Done: true})
	rawdb.WriteSnapshotGenerator(db, gen)

	out := t.TempDir()
	if err := export(db, out); err != nil {
		t.Fatal(err)
	}
	read := func(name string) []byte {
		b, err := os.ReadFile(filepath.Join(out, name))
		if err != nil {
			t.Fatal(err)
		}
		return b
	}

	var got meta
	if err := json.Unmarshal(read("meta.json"), &got); err != nil {
		t.Fatal(err)
	}
	want := meta{Height: 10, Hash: blocks[0].Hash(), StateRoot: blocks[0].Root(), HeadHeight: 12, Accounts: 3, Slots: 4, Codes: 1}
	if got != want {
		t.Fatalf("meta %+v want %+v", got, want)
	}

	hdr, _ := rlp.EncodeToBytes(blocks[0].Header())
	if !bytes.Equal(read("header.rlp"), hdr) {
		t.Fatal("header.rlp mismatch")
	}

	var wantAcc []byte
	for i, a := range accts {
		rec := make([]byte, 105)
		copy(rec, hash(byte(i+1)).Bytes())
		binary.LittleEndian.PutUint64(rec[32:], a.Nonce)
		a.Balance.ToBig().FillBytes(rec[40:72])
		copy(rec[72:104], a.CodeHash)
		if multi[i] {
			rec[104] = 1
		}
		wantAcc = append(wantAcc, rec...)
	}
	if !bytes.Equal(read("accounts.bin"), wantAcc) {
		t.Fatal("accounts.bin mismatch")
	}

	var wantSlots []byte
	for _, s := range slots {
		rec := make([]byte, 96)
		copy(rec, hash(s.acct).Bytes())
		copy(rec[32:], hash(s.slot).Bytes())
		copy(rec[96-len(s.val):], s.val)
		wantSlots = append(wantSlots, rec...)
	}
	if !bytes.Equal(read("storage.bin"), wantSlots) {
		t.Fatal("storage.bin mismatch")
	}

	wantCode := append(append(codeHash.Bytes(), byte(len(code)), 0, 0, 0), code...)
	if !bytes.Equal(read("code.bin"), wantCode) {
		t.Fatal("code.bin mismatch")
	}

	var wantBlocks []byte
	for _, b := range blocks {
		enc, err := rlp.EncodeToBytes(b)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Contains(enc, []byte("atomic txs live here")) && b.NumberU64() == 11 {
			t.Fatal("test block 11 lost its extdata")
		}
		wantBlocks = binary.LittleEndian.AppendUint64(wantBlocks, b.NumberU64())
		wantBlocks = binary.LittleEndian.AppendUint32(wantBlocks, uint32(len(enc)))
		wantBlocks = append(wantBlocks, enc...)
	}
	if !bytes.Equal(read("blocks.bin"), wantBlocks) {
		t.Fatal("blocks.bin mismatch")
	}
}

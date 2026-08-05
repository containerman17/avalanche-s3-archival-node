package verify

import (
	"encoding/binary"
	"math/big"
	"os"
	"strings"
	"testing"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/crypto"
	"github.com/ava-labs/libevm/libevm/stateconf"
	"github.com/ava-labs/libevm/rlp"
	"github.com/ava-labs/libevm/trie"
	"github.com/holiman/uint256"

	"github.com/containerman17/epochdb/chain"
	"github.com/containerman17/epochdb/dist"
	"github.com/containerman17/epochdb/fetch"
	"github.com/containerman17/epochdb/state"
)

// tblock is one synthetic block: real header, real txs, real receipts,
// and the post-image diff rows whose Firewood application produced
// header.Root (computed by a builder Firewood at construction time).
type tblock struct {
	n        uint64
	hdr      *types.Header
	txs      types.Transactions
	receipts types.Receipts
	rows     []state.StateRow
}

var (
	alice    = common.HexToAddress("0xa11ce00000000000000000000000000000000001")
	bob      = common.HexToAddress("0xb0b0000000000000000000000000000000000002")
	contract = common.HexToAddress("0xc0de000000000000000000000000000000000003")
	slot1    = common.HexToHash("0x01")
	slot2    = common.HexToHash("0x02")
)

func acctRow(blk uint64, addr common.Address, nonce uint64, bal int64, seq int) state.StateRow {
	acc := types.StateAccount{
		Nonce:    nonce,
		Balance:  uint256.NewInt(uint64(bal)),
		Root:     common.Hash{}, // firewood storage tries report the zero hash
		CodeHash: types.EmptyCodeHash.Bytes(),
	}
	val, err := rlp.EncodeToBytes(&acc)
	if err != nil {
		panic(err)
	}
	r := state.StateRow{Block: blk, Value: val, Seq: seq}
	r.Key[0] = 'a'
	copy(r.Key[1:21], addr[:])
	return r
}

func deleteRow(blk uint64, addr common.Address, seq int) state.StateRow {
	r := state.StateRow{Block: blk, Seq: seq}
	r.Key[0] = 'a'
	copy(r.Key[1:21], addr[:])
	return r
}

func storageRow(blk uint64, addr common.Address, slot common.Hash, val []byte, seq int) state.StateRow {
	r := state.StateRow{Block: blk, Value: val, Seq: seq}
	r.Key[0] = 's'
	copy(r.Key[1:21], addr[:])
	copy(r.Key[21:53], slot[:])
	return r
}

// buildChain constructs 12 blocks (1..12) chained off an arbitrary
// genesis hash, using its own throwaway Firewood to compute the real
// per-block state roots the headers carry.
func buildChain(t *testing.T) []tblock {
	t.Helper()
	fetch.RegisterExtras(chain.Coreth)
	tdb, fw, db, err := newThrowawayFirewood(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer tdb.Close()

	key, _ := crypto.GenerateKey()
	signer := types.HomesteadSigner{}
	genesisHash := common.HexToHash("0x9e9e515")
	fw.SetHashAndHeight(genesisHash, 0)

	tx := func(nonce uint64, data []byte) *types.Transaction {
		signed, err := types.SignNewTx(key, signer, &types.LegacyTx{
			Nonce: nonce, To: &bob, Gas: 21000, GasPrice: big.NewInt(1),
			Value: big.NewInt(1), Data: data,
		})
		if err != nil {
			t.Fatal(err)
		}
		return signed
	}
	rcpt := func(txType uint8, status uint64, gas uint64, cum uint64, logs ...*types.Log) *types.Receipt {
		r := &types.Receipt{Type: txType, Status: status, GasUsed: gas, CumulativeGasUsed: cum, Logs: logs}
		r.Bloom = types.CreateBloom(types.Receipts{r})
		return r
	}
	lg := func(data byte) *types.Log {
		return &types.Log{
			Address: contract,
			Topics:  []common.Hash{common.HexToHash("0x7091c"), common.HexToHash("0x7091d")},
			Data:    []byte{data, data + 1},
		}
	}

	specs := []struct {
		rows     []state.StateRow
		txs      types.Transactions
		receipts types.Receipts
	}{
		{ // 1: create alice + contract with storage
			rows: []state.StateRow{
				acctRow(1, alice, 1, 100, 0),
				acctRow(1, contract, 1, 0, 1),
				storageRow(1, contract, slot1, []byte{0x01}, 2),
			},
			txs:      types.Transactions{tx(0, nil)},
			receipts: types.Receipts{rcpt(types.LegacyTxType, 1, 21000, 21000)},
		},
		{}, // 2: empty
		{ // 3: rewrite slot1, add slot2, create bob; logs
			rows: []state.StateRow{
				acctRow(3, alice, 2, 90, 0),
				acctRow(3, bob, 1, 10, 1),
				storageRow(3, contract, slot1, []byte{0x02}, 2),
				storageRow(3, contract, slot2, []byte{0xbe, 0xef}, 3),
			},
			txs: types.Transactions{tx(1, []byte{1}), tx(2, []byte{2})},
			receipts: types.Receipts{
				rcpt(types.LegacyTxType, 1, 21000, 21000, lg(0x30)),
				rcpt(types.LegacyTxType, 0, 22000, 43000),
			},
		},
		{ // 4: SELFDESTRUCT bob
			rows:     []state.StateRow{deleteRow(4, bob, 0)},
			txs:      types.Transactions{tx(3, nil)},
			receipts: types.Receipts{rcpt(types.LegacyTxType, 1, 21000, 21000)},
		},
		{ // 5: delete slot2
			rows:     []state.StateRow{storageRow(5, contract, slot2, nil, 0)},
			txs:      types.Transactions{tx(4, nil)},
			receipts: types.Receipts{rcpt(types.LegacyTxType, 1, 21000, 21000, lg(0x50))},
		},
		{}, // 6: empty (epoch boundary next)
		{ // 7: recreate bob (across-block recreate)
			rows:     []state.StateRow{acctRow(7, bob, 1, 5, 0)},
			txs:      types.Transactions{tx(5, nil)},
			receipts: types.Receipts{rcpt(types.LegacyTxType, 1, 21000, 21000)},
		},
		{ // 8: contract storage + logs
			rows: []state.StateRow{
				acctRow(8, alice, 3, 80, 0),
				storageRow(8, contract, common.HexToHash("0x03"), []byte{0x08}, 1),
			},
			txs: types.Transactions{tx(6, []byte{6}), tx(7, []byte{7})},
			receipts: types.Receipts{
				rcpt(types.LegacyTxType, 1, 21000, 21000, lg(0x80), lg(0x81)),
				rcpt(types.LegacyTxType, 1, 30000, 51000, lg(0x82)),
			},
		},
		{}, // 9: empty
		{ // 10: alice nonce bump
			rows:     []state.StateRow{acctRow(10, alice, 4, 70, 0)},
			txs:      types.Transactions{tx(8, nil)},
			receipts: types.Receipts{rcpt(types.LegacyTxType, 1, 21000, 21000)},
		},
		{ // 11: delete slot1
			rows:     []state.StateRow{storageRow(11, contract, slot1, nil, 0)},
			txs:      types.Transactions{tx(9, nil)},
			receipts: types.Receipts{rcpt(types.LegacyTxType, 1, 21000, 21000, lg(0xb0))},
		},
		{ // 12: final write
			rows:     []state.StateRow{acctRow(12, bob, 2, 6, 0)},
			txs:      types.Transactions{tx(10, nil)},
			receipts: types.Receipts{rcpt(types.LegacyTxType, 1, 21000, 21000)},
		},
	}

	parentRoot := types.EmptyRootHash
	parentHash := genesisHash
	fwHeight := uint64(0)
	var out []tblock
	for i, sp := range specs {
		n := uint64(i + 1)
		root := parentRoot
		if len(sp.rows) > 0 {
			tr, err := db.OpenTrie(parentRoot)
			if err != nil {
				t.Fatalf("block %d: open trie: %v", n, err)
			}
			if err := applyRows(tr, sp.rows); err != nil {
				t.Fatalf("block %d: %v", n, err)
			}
			if root = tr.Hash(); root == (common.Hash{}) {
				t.Fatalf("block %d: proposal failed", n)
			}
		}
		var gasUsed uint64
		if len(sp.receipts) > 0 {
			gasUsed = sp.receipts[len(sp.receipts)-1].CumulativeGasUsed
		}
		hdr := &types.Header{
			ParentHash:  parentHash,
			Root:        root,
			TxHash:      types.DeriveSha(sp.txs, trie.NewStackTrie(nil)),
			ReceiptHash: types.DeriveSha(sp.receipts, trie.NewStackTrie(nil)),
			Bloom:       types.CreateBloom(sp.receipts),
			Number:      new(big.Int).SetUint64(n),
			GasLimit:    8_000_000,
			GasUsed:     gasUsed,
			Time:        n,
			Difficulty:  big.NewInt(1),
		}
		hdrHash := hdr.Hash()
		if len(sp.rows) > 0 {
			opt := stateconf.WithTrieDBUpdatePayload(hdr.ParentHash, hdrHash)
			if err := tdb.Update(root, parentRoot, fwHeight+1, nil, nil, opt); err != nil {
				t.Fatalf("block %d: update: %v", n, err)
			}
			if err := tdb.Commit(root, false); err != nil {
				t.Fatalf("block %d: commit: %v", n, err)
			}
			fwHeight++
		} else {
			fw.SetHashAndHeight(hdrHash, n)
			fwHeight = n
		}
		parentRoot, parentHash = root, hdrHash
		out = append(out, tblock{n: n, hdr: hdr, txs: sp.txs, receipts: sp.receipts, rows: sp.rows})
	}
	return out
}

func containerFor(t *testing.T, hdr *types.Header, txs types.Transactions) []byte {
	t.Helper()
	blk := types.NewBlockWithHeader(hdr).WithBody(types.Body{Transactions: txs})
	raw, err := rlp.EncodeToBytes(blk)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// sealEpochs writes the chain as two 6-block epochs into dir, applying
// mutate (may be nil) to each fresh EpochInput before sealing.
func sealEpochs(t *testing.T, dir string, blocks []tblock, mutate func(in *state.EpochInput)) {
	t.Helper()
	const perEpoch = 6
	for at := 0; at < len(blocks); at += perEpoch {
		chunk := blocks[at:min(at+perEpoch, len(blocks))]
		in := &state.EpochInput{
			Start:    chunk[0].n,
			TxHashes: map[uint64][][32]byte{},
			FullLogs: map[uint64][]byte{},
			RcptRecs: map[uint64][]byte{},
		}
		for _, b := range chunk {
			hdrRLP, err := rlp.EncodeToBytes(b.hdr)
			if err != nil {
				t.Fatal(err)
			}
			in.Headers = append(in.Headers, hdrRLP)
			in.Containers = append(in.Containers, containerFor(t, b.hdr, b.txs))
			in.StateRows = append(in.StateRows, b.rows...)
			for _, tx := range b.txs {
				in.TxHashes[b.n] = append(in.TxHashes[b.n], [32]byte(tx.Hash()))
				in.TxCount++
			}
			if rec := state.EncodeStoredReceipts(b.receipts); rec != nil {
				in.RcptRecs[b.n] = rec
			}
			if rec := state.EncodeStoredLogs(b.receipts); rec != nil {
				in.FullLogs[b.n] = rec
			}
		}
		if mutate != nil {
			mutate(in)
		}
		if _, err := state.BuildEpoch(testStore(t, dir), in); err != nil {
			t.Fatal(err)
		}
	}
}

// runVerify verifies dir's epoch set with a fresh anchorless Verifier.
func runVerify(t *testing.T, dir string) (*Verifier, error) {
	t.Helper()
	set, err := state.OpenEpochSet(testStore(t, dir))
	if err != nil {
		t.Fatal(err)
	}
	defer set.Close()
	v, err := newAnchorless(t.TempDir(), 2)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(v.Close)
	for _, e := range set.All() {
		if err := v.VerifyEpoch(e); err != nil {
			return v, err
		}
	}
	return v, nil
}

func TestVerifyCorruptions(t *testing.T) {
	blocks := buildChain(t)

	t.Run("clean", func(t *testing.T) {
		dir := t.TempDir()
		sealEpochs(t, dir, blocks, nil)
		v, err := runVerify(t, dir)
		if err != nil {
			t.Fatalf("clean chain must verify: %v", err)
		}
		if v.Blocks() != 12 || v.Next() != 13 {
			t.Fatalf("blocks=%d next=%d", v.Blocks(), v.Next())
		}
	})

	t.Run("flipped sst value breaks state root", func(t *testing.T) {
		dir := t.TempDir()
		sealEpochs(t, dir, blocks, func(in *state.EpochInput) {
			for i := range in.StateRows {
				r := &in.StateRows[i]
				if r.Block == 8 && r.Key[0] == 's' && len(r.Value) > 0 {
					bad := append([]byte(nil), r.Value...)
					bad[0] ^= 0xff
					r.Value = bad
					return
				}
			}
		})
		_, err := runVerify(t, dir)
		if err == nil || !strings.Contains(err.Error(), "state root mismatch at block 8") {
			t.Fatalf("want state root mismatch at block 8, got: %v", err)
		}
	})

	t.Run("flipped tx byte breaks tx root", func(t *testing.T) {
		dir := t.TempDir()
		sealEpochs(t, dir, blocks, func(in *state.EpochInput) {
			if in.Start != 7 {
				return
			}
			b := blocks[9] // block 10
			badTx := types.NewTx(&types.LegacyTx{
				Nonce: 99, To: &bob, Gas: 21000, GasPrice: big.NewInt(1),
				Value: big.NewInt(1), Data: []byte{0xde, 0xad},
			})
			in.Containers[b.n-in.Start] = containerFor(t, b.hdr, types.Transactions{badTx})
		})
		_, err := runVerify(t, dir)
		if err == nil || !strings.Contains(err.Error(), "tx root mismatch at block 10") {
			t.Fatalf("want tx root mismatch at block 10, got: %v", err)
		}
	})

	t.Run("flipped log breaks receipts root", func(t *testing.T) {
		dir := t.TempDir()
		sealEpochs(t, dir, blocks, func(in *state.EpochInput) {
			if rec, ok := in.FullLogs[8]; ok {
				bad := append([]byte(nil), rec...)
				bad[len(bad)-2] ^= 0xff // inside the last log's data
				in.FullLogs[8] = bad
			}
		})
		_, err := runVerify(t, dir)
		if err == nil || !strings.Contains(err.Error(), "receipts root mismatch at block 8") {
			t.Fatalf("want receipts root mismatch at block 8, got: %v", err)
		}
	})

	// THE ONE THAT USED TO PASS. A body frame that decodes to nothing (a
	// truncated or zero-length frame in a downloaded artifact) skipped the
	// container-hash, txRoot and receiptsRoot checks for every block it
	// covered, and the run still reported "PASS: 12 blocks": nothing counted
	// what the walk had actually visited.
	t.Run("emptied body frame must fail, not silently skip the checks", func(t *testing.T) {
		dir := t.TempDir()
		sealEpochs(t, dir, blocks, nil)
		emptyBodyFrame(t, dir, 7)
		v, err := runVerify(t, dir)
		if err == nil {
			t.Fatalf("emptied body frame reported PASS after %d blocks", v.Blocks())
		}
		if !strings.Contains(err.Error(), "containers: blocks [") {
			t.Fatalf("want the skipped container blocks named, got: %v", err)
		}
	})

	t.Run("broken parent hash breaks the header chain", func(t *testing.T) {
		dir := t.TempDir()
		sealEpochs(t, dir, blocks, func(in *state.EpochInput) {
			if in.Start != 7 {
				return
			}
			bad := *blocks[8].hdr // block 9 (empty)
			bad.ParentHash[0] ^= 0xff
			hdrRLP, err := rlp.EncodeToBytes(&bad)
			if err != nil {
				t.Fatal(err)
			}
			in.Headers[9-in.Start] = hdrRLP
			in.Containers[9-in.Start] = containerFor(t, &bad, nil)
		})
		_, err := runVerify(t, dir)
		if err == nil || !strings.Contains(err.Error(), "header chain broken at block 9") {
			t.Fatalf("want header chain broken at block 9, got: %v", err)
		}
	})
}

// emptyBodyFrame zeroes the length of the first containers frame of the epoch
// starting at block start, in place in the spool. The frame index still claims
// the blocks, the frame decodes to zero payloads: exactly a truncated download
// or a short-written artifact, and the one shape walkFramedRange used to treat
// as "nothing to do here".
func emptyBodyFrame(t *testing.T, dir string, start uint64) {
	t.Helper()
	st := testStore(t, dir)
	set, err := state.OpenEpochSet(st)
	if err != nil {
		t.Fatal(err)
	}
	var path string
	var sizes map[string]uint64
	for _, e := range set.All() {
		if e.Start == start {
			path, sizes = st.SpoolPath(e.Hash), e.SectionSizes()
		}
	}
	set.Close()
	if path == "" {
		t.Fatalf("no epoch starting at block %d", start)
	}
	// Sections are written in file order, so bodiesIdx starts right after
	// dict and bodies. Its layout is u64 LE frame offsets, nFrames+1.
	off := sizes["dict"] + sizes["bodies"]
	if sizes["bodiesIdx"] != 16 {
		t.Fatalf("expected a single-frame epoch, bodiesIdx is %d bytes", sizes["bodiesIdx"])
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if binary.LittleEndian.Uint64(raw[off+8:]) == 0 {
		t.Fatal("frame 0 already empty: wrong offset")
	}
	binary.LittleEndian.PutUint64(raw[off+8:], 0) // frame 0 ends where it starts
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}
}

// testStore is a credential-free artifact store over dir: the spool is the
// whole of it, exactly like a node with no bucket configured.
func testStore(t *testing.T, dir string) *dist.Store {
	t.Helper()
	st, err := dist.Local(dir)
	if err != nil {
		t.Fatal(err)
	}
	return st
}

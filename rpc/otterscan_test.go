package rpc

import (
	"encoding/binary"
	"math/big"
	"testing"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/common/hexutil"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/core/vm"
	"github.com/ava-labs/libevm/crypto"
	"github.com/ava-labs/libevm/rlp"

	"github.com/containerman17/epochdb/dist"
	"github.com/containerman17/epochdb/exec"
	"github.com/containerman17/epochdb/store"
)

// THE ots_ PAGINATION TEST RUNS ACROSS A FLUSH BOUNDARY, which is the only
// thing that can break keyset paging here: the postings of the same address
// live partly in a sealed run and partly in the window, and the descending walk
// has to yield them as one stream. A page that lands exactly on the seam is the
// case that fails when the two halves are walked in the wrong order.

const otsFlushAt = 5 // blocks 1..5 are sealed into a run, 6..10 stay in the window

// otsFixture writes 10 blocks, one transaction each, all from the same sender,
// nonces 0..9, and seals the first half into a run.
func otsFixture(t *testing.T) (*Server, common.Address, []common.Hash) {
	t.Helper()
	g, err := exec.ChainGenesis(nil)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	cas, err := dist.Local(dir)
	if err != nil {
		t.Fatal(err)
	}
	db, err := store.Open(dir, cas, [32]byte{7})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })

	key, _ := crypto.GenerateKey()
	sender := crypto.PubkeyToAddress(key.PublicKey)
	to := common.HexToAddress("0xbbbb000000000000000000000000000000000002")
	signer := types.MakeSigner(g.Config, big.NewInt(1), testBlockTime)

	var hashes []common.Hash
	for n := uint64(1); n <= 10; n++ {
		tx, err := types.SignNewTx(key, signer, &types.LegacyTx{
			Nonce: n - 1, To: &to, Gas: 21000, GasPrice: big.NewInt(25_000_000_000),
		})
		if err != nil {
			t.Fatal(err)
		}
		hashes = append(hashes, tx.Hash())
		raw, err := tx.MarshalBinary()
		if err != nil {
			t.Fatal(err)
		}
		hdr := &types.Header{
			Number: new(big.Int).SetUint64(n), Time: testBlockTime, GasLimit: 15_000_000,
			BaseFee: big.NewInt(25_000_000_000), Difficulty: big.NewInt(1), Extra: []byte{},
		}
		hdrRLP, err := rlp.EncodeToBytes(hdr)
		if err != nil {
			t.Fatal(err)
		}
		rcpt := &types.Receipt{Status: 1, GasUsed: 21000}
		if err := db.WriteBlock(&store.BlockWrite{
			Height: n, HeaderRLP: hdrRLP,
			Txs: []store.TxWrite{{
				Hash:    tx.Hash().Bytes(),
				RLP:     raw,
				Receipt: store.EncodeTxReceipt(rcpt, 21000),
				Frames:  otsFrameRecord(sender, to),
				Sender:  sender.Bytes(),
				To:      to.Bytes(),
			}},
		}); err != nil {
			t.Fatal(err)
		}
		if n == otsFlushAt {
			if err := db.Flush(); err != nil {
				t.Fatal(err)
			}
		}
	}
	return NewServer(db, g.TrieAlloc, StoreChainContext(db), g.Config), sender, hashes
}

// otsFrameRecord builds one itx/ row: a single CALL frame moving 1 wei. The
// layout mirrors exec/frames.go, which owns it.
func otsFrameRecord(from, to common.Address) []byte {
	rec := binary.AppendUvarint(nil, 1)
	rec = append(rec, byte(vm.CALL), 0)
	rec = append(rec, from[:]...)
	rec = append(rec, to[:]...)
	rec = binary.AppendUvarint(rec, 1)
	rec = append(rec, 1) // value: 1 wei
	rec = binary.AppendUvarint(rec, 100)
	rec = binary.AppendUvarint(rec, 50)
	rec = append(rec, 0)                // not failed
	rec = binary.AppendUvarint(rec, 0)  // no input
	return binary.AppendUvarint(rec, 0) // no output
}

func otsSearch(t *testing.T, s *Server, method string, addr common.Address, block uint64, size int) *otsSearchResult {
	t.Helper()
	res, rerr := call(t, s, method, addr, block, size)
	if rerr != nil {
		t.Fatalf("%s(%d): %v", method, block, rerr)
	}
	return res.(*otsSearchResult)
}

func TestOtsSearchPagesAcrossFlush(t *testing.T) {
	srv, sender, hashes := otsFixture(t)

	// Page backwards from the tip in pages of 3, which puts a page boundary
	// inside the window (10..8, 7..5) and one across the sealed run (7..5).
	var got []common.Hash
	cursor, pages := uint64(0), 0
	for {
		res := otsSearch(t, srv, "ots_searchTransactionsBefore", sender, cursor, 3)
		if pages == 0 && !res.FirstPage {
			t.Fatal("the first page must say so")
		}
		if len(res.Txs) != len(res.Receipts) {
			t.Fatalf("page %d: %d txs but %d receipts", pages, len(res.Txs), len(res.Receipts))
		}
		for _, tx := range res.Txs {
			got = append(got, tx.Hash)
		}
		pages++
		if res.LastPage {
			break
		}
		if len(res.Txs) == 0 {
			t.Fatal("a non-last page came back empty")
		}
		cursor = res.Txs[len(res.Txs)-1].BlockNumber.ToInt().Uint64()
		if pages > 10 {
			t.Fatal("pagination did not terminate")
		}
	}
	if len(got) != 10 {
		t.Fatalf("paged %d transactions, want 10: %v", len(got), got)
	}
	// Newest first, no gaps, no repeats: block 10 down to block 1.
	for i, h := range got {
		if want := hashes[len(hashes)-1-i]; h != want {
			t.Fatalf("position %d is %s, want %s (order or seam is wrong)", i, h, want)
		}
	}

	// searchTransactionsAfter reads the other direction and still answers
	// newest-first. Crossing the seam from below is the mirror case.
	res := otsSearch(t, srv, "ots_searchTransactionsAfter", sender, 3, 4)
	if len(res.Txs) != 4 {
		t.Fatalf("after(3): %d txs, want 4", len(res.Txs))
	}
	for i, want := range []uint64{7, 6, 5, 4} {
		if got := res.Txs[i].BlockNumber.ToInt().Uint64(); got != want {
			t.Fatalf("after(3) position %d is block %d, want %d", i, got, want)
		}
	}
	if res.FirstPage {
		t.Fatal("after(3) with 3 more transactions above must not be the first page")
	}
	if res := otsSearch(t, srv, "ots_searchTransactionsAfter", sender, 9, 5); !res.FirstPage ||
		len(res.Txs) != 1 || res.Txs[0].BlockNumber.ToInt().Uint64() != 10 {
		t.Fatalf("after(9): %+v", res)
	}
}

func TestOtsLookups(t *testing.T) {
	srv, sender, hashes := otsFixture(t)

	if res, rerr := call(t, srv, "ots_getApiLevel"); rerr != nil || res != OtsAPILevel {
		t.Fatalf("ots_getApiLevel: %v %v", res, rerr)
	}

	// Sender and nonce: nonce n was sent in block n+1.
	res, rerr := call(t, srv, "ots_getTransactionBySenderAndNonce", sender, 4)
	if rerr != nil {
		t.Fatalf("ots_getTransactionBySenderAndNonce: %v", rerr)
	}
	if got := *res.(*common.Hash); got != hashes[4] {
		t.Fatalf("nonce 4 is %s, want %s", got, hashes[4])
	}
	// A nonce this sender never used is null, never a wrong neighbour.
	if res, rerr := call(t, srv, "ots_getTransactionBySenderAndNonce", sender, 99); rerr != nil || res != nil {
		t.Fatalf("unused nonce: %v %v", res, rerr)
	}

	// Internal operations come from the STORED frames, and the read-time
	// filter keeps a valued CALL as a transfer.
	ops, rerr := call(t, srv, "ots_getInternalOperations", hashes[0])
	if rerr != nil {
		t.Fatalf("ots_getInternalOperations: %v", rerr)
	}
	list := ops.([]otsInternalOp)
	if len(list) != 1 || list[0].Type != otsOpTransfer || list[0].From != sender {
		t.Fatalf("internal operations: %+v", list)
	}
	if list[0].Value.ToInt().Uint64() != 1 {
		t.Fatalf("transfer value %v", list[0].Value)
	}
	// An unknown transaction is null, not an empty list: "no such tx" and "no
	// internal operations" are different answers.
	if res, rerr := call(t, srv, "ots_getInternalOperations", common.Hash{3}); rerr != nil || res != nil {
		t.Fatalf("unknown tx: %v %v", res, rerr)
	}

	// Block details: no transaction list, a count, and the summed fees.
	det, rerr := call(t, srv, "ots_getBlockDetails", "0x3")
	if rerr != nil {
		t.Fatalf("ots_getBlockDetails: %v", rerr)
	}
	m := det.(map[string]any)
	blk := m["block"].(map[string]any)
	if _, ok := blk["transactions"]; ok {
		t.Fatal("ots_getBlockDetails must not carry the transaction list")
	}
	if blk["transactionCount"].(int) != 1 {
		t.Fatalf("transactionCount: %v", blk["transactionCount"])
	}
	if got := m["totalFees"].(*hexutil.Big).ToInt(); got.Sign() <= 0 {
		t.Fatalf("totalFees: %v", got)
	}

	// Block transactions: page 0 has the block's only transaction, page 1 is
	// empty and still a valid answer.
	txs, rerr := call(t, srv, "ots_getBlockTransactions", "0x3", 0, 10)
	if rerr != nil {
		t.Fatalf("ots_getBlockTransactions: %v", rerr)
	}
	full := txs.(map[string]any)["fullblock"].(map[string]any)
	if len(full["transactions"].([]*rpcTransaction)) != 1 {
		t.Fatalf("page 0: %v", full["transactions"])
	}
	txs, rerr = call(t, srv, "ots_getBlockTransactions", "0x3", 1, 10)
	if rerr != nil {
		t.Fatalf("ots_getBlockTransactions page 1: %v", rerr)
	}
	full = txs.(map[string]any)["fullblock"].(map[string]any)
	if len(full["transactions"].([]*rpcTransaction)) != 0 {
		t.Fatal("page past the end must be empty, not wrapped")
	}
}

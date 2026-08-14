package main

import (
	"math/big"
	"testing"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/rlp"
	"github.com/ava-labs/libevm/trie"

	"github.com/containerman17/epochdb/dist"
	"github.com/containerman17/epochdb/store"
)

// VERIFICATION LAYER 1 HAD NO TEST OF ITS OWN, which is a strange gap in the
// one pass every corpus is trusted on. This builds a small REAL corpus (real
// header RLP, real transactions, real receipts, a container that round trips
// through SplitContainer) and walks it, so the lockstep cursor pass is exercised
// end to end rather than only in production.
//
// It matters most for the cursors: six chain families advance in lockstep off
// one `blk/` row each, and an off-by-one in any of them either passes everything
// or fails everything.

// verifyCorpus writes nBlocks blocks of nTxs transactions each and returns the
// open DB.
func verifyCorpus(t *testing.T, nBlocks, nTxs int) *store.DB {
	t.Helper()
	dir := t.TempDir()
	cas, err := dist.Local(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cas.Close() })
	db, err := store.Open(dir, cas, [32]byte{7})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })

	parent := common.Hash{}
	for h := 0; h < nBlocks; h++ {
		var (
			txs      types.Transactions
			receipts types.Receipts
			cum      uint64
		)
		for i := 0; i < nTxs; i++ {
			to := common.Address{byte(i + 1)}
			txs = append(txs, types.NewTx(&types.LegacyTx{
				Nonce: uint64(h*nTxs + i), To: &to, Gas: 21000,
				GasPrice: big.NewInt(1), Value: big.NewInt(int64(i)),
			}))
			cum += 21000
			receipts = append(receipts, &types.Receipt{
				Type: types.LegacyTxType, Status: types.ReceiptStatusSuccessful,
				CumulativeGasUsed: cum,
				Logs: []*types.Log{{
					Address: common.Address{byte(i + 100)},
					Topics:  []common.Hash{{byte(i + 1)}},
					Data:    []byte{byte(h)},
				}},
			})
			receipts[i].Bloom = types.CreateBloom(types.Receipts{receipts[i]})
		}
		hdr := &types.Header{
			ParentHash:  parent,
			Number:      big.NewInt(int64(h)),
			GasLimit:    8_000_000,
			Difficulty:  big.NewInt(1),
			TxHash:      types.DeriveSha(txs, trie.NewStackTrie(nil)),
			ReceiptHash: types.DeriveSha(receipts, trie.NewStackTrie(nil)),
			Bloom:       types.CreateBloom(receipts),
		}
		hdrRLP, err := rlp.EncodeToBytes(hdr)
		if err != nil {
			t.Fatal(err)
		}
		bw := &store.BlockWrite{Height: uint64(h), HeaderRLP: hdrRLP, Code: map[string][]byte{}}

		var txRLPs [][]byte
		for i, tx := range txs {
			raw, err := rlp.EncodeToBytes(tx)
			if err != nil {
				t.Fatal(err)
			}
			txRLPs = append(txRLPs, raw)
			// The FIRST transaction of every block made no nested call, which
			// is a legal empty itx row and must not read as a hole.
			var frames []byte
			if i > 0 {
				frames = frameRec(t, byte(i))
			}
			bw.Txs = append(bw.Txs, store.TxWrite{
				Hash:    tx.Hash().Bytes(),
				RLP:     raw,
				Receipt: store.EncodeTxReceipt(receipts[i], receipts[i].CumulativeGasUsed),
				Frames:  frames,
			})
		}
		// The pvm row is whatever SplitContainer makes of the real container,
		// which for this bare three-field block is the empty shorthand.
		raw, err := store.Reassemble(nil, hdrRLP, txRLPs)
		if err != nil {
			t.Fatal(err)
		}
		pvm, _, err := store.SplitContainer(raw)
		if err != nil {
			t.Fatal(err)
		}
		bw.Pvm = pvm
		if err := db.WriteBlock(bw); err != nil {
			t.Fatal(err)
		}
		parent = hdr.Hash()
	}
	return db
}

// frameRec builds one valid itx/ record: a single CALL frame.
func frameRec(t *testing.T, seed byte) []byte {
	t.Helper()
	rec := []byte{1} // uvarint frame count
	rec = append(rec, 0xF1, 0)
	rec = append(rec, common.Address{seed}.Bytes()...)
	rec = append(rec, common.Address{seed + 1}.Bytes()...)
	rec = append(rec, 1, seed)     // value len, value
	rec = append(rec, 100, 90)     // gas, gasUsed (uvarint)
	rec = append(rec, 0)           // not failed
	rec = append(rec, 2, 'i', 'n') // input
	rec = append(rec, 1, 'o')      // output
	if _, err := store.DecodeFrames(rec); err != nil {
		t.Fatalf("the test's own frame record does not decode: %v", err)
	}
	return rec
}

func TestVerifyRangeWalksARealCorpus(t *testing.T) {
	db := verifyCorpus(t, 6, 3)

	// Unanchored on purpose: this corpus has no chain root behind it, and the
	// header chain still has to link block to block.
	blocks, txs, err := verifyRange(db, 0, 5, common.Hash{})
	if err != nil {
		t.Fatalf("clean corpus failed layer 1: %v", err)
	}
	if blocks != 6 || txs != 18 {
		t.Fatalf("walked %d blocks / %d txs, want 6 / 18", blocks, txs)
	}

	// Flushing into a sealed run must change nothing: the same rows read back
	// through the SST cursors instead of the window.
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	blocks, txs, err = verifyRange(db, 0, 5, common.Hash{})
	if err != nil {
		t.Fatalf("the same corpus failed layer 1 once sealed into a run: %v", err)
	}
	if blocks != 6 || txs != 18 {
		t.Fatalf("sealed: walked %d blocks / %d txs, want 6 / 18", blocks, txs)
	}
}

// A BAD ANCHOR MUST FAIL. The genesis hash is the trust anchor, and block 0's
// ParentHash is where it is spent; nothing else in the pass looks at it.
func TestVerifyRangeRefusesAWrongAnchor(t *testing.T) {
	db := verifyCorpus(t, 3, 2)
	if _, _, err := verifyRange(db, 0, 2, common.Hash{0xAA}); err == nil {
		t.Fatal("a corpus that does not chain to the anchor passed layer 1")
	}
}

// AN EMPTY RANGE PROVES NOTHING, which is why verifyMain refuses a zero-block
// result rather than printing PASS.
func TestVerifyRangeOverAnEmptyRangeWalksNothing(t *testing.T) {
	db := verifyCorpus(t, 3, 2)
	blocks, _, err := verifyRange(db, 2, 1, common.Hash{})
	if err != nil {
		t.Fatalf("an inverted range errored instead of walking nothing: %v", err)
	}
	if blocks != 0 {
		t.Fatalf("walked %d blocks over an inverted range", blocks)
	}
}

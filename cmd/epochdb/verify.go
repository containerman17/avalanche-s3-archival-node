package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"iter"
	"log"
	"time"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/rlp"
	"github.com/ava-labs/libevm/trie"

	"github.com/containerman17/epochdb/dist"
	"github.com/containerman17/epochdb/exec"
	"github.com/containerman17/epochdb/store"
)

// VERIFICATION LAYER 1: CRYPTOGRAPHIC SELF-VERIFICATION, the strongest oracle
// (DESIGN, "Verification: three layers"). Everything here is recomputed from
// STORED BYTES and checked against consensus math, never against another
// epochdb build:
//
//   - the header hash chain: every header's ParentHash is the previous header's
//     hash, so one head hash pins every header below it;
//   - txRoot: derived from the `tx/` rows of the block;
//   - receiptsRoot and logsBloom: derived from the `rcpt/` rows;
//   - container reassembly: `hdr` + `pvm` + `tx` rows re-form the container
//     byte for byte (the day-one invariant, also unit-tested in store).
//
// State roots are layer 1 too and are checked WHERE THEY ARE PRODUCED: the
// executor hard-stops on any block whose Firewood root differs from the
// header's, so a corpus that exists has already had every state root verified
// against consensus math. There is nothing left for this pass to add there.
//
// A CHECK THAT CANNOT RUN IS A FAILURE, never a pass.
func verifyMain(args []string) {
	fs := flag.NewFlagSet("verify", flag.ExitOnError)
	dataDir := fs.String("data", "./data", "data directory")
	from := fs.Uint64("from", 1, "first height (genesis has no stored header: it is a pure function of the chain root)")
	to := fs.Uint64("to", 0, "last height (0 = the store's head)")
	_, resolveChain := chainFlags(fs)
	fs.Parse(args)

	c := resolveChain(*dataDir)
	g, err := exec.ChainGenesis(c)
	if err != nil {
		log.Fatalf("epochdb: verify: genesis: %v", err)
	}
	cas, err := dist.Local(*dataDir)
	if err != nil {
		log.Fatalf("epochdb: verify: %v", err)
	}
	defer cas.Close()
	db, err := store.Open(*dataDir, cas, c.Root())
	if err != nil {
		log.Fatalf("epochdb: verify: %v", err)
	}
	defer db.Close()

	head, ok := db.Head()
	if !ok {
		log.Fatalf("epochdb: verify: the store holds no blocks, so nothing can be verified: that is a FAILURE, not a pass")
	}
	if *to == 0 || *to > head {
		*to = head
	}
	start := time.Now()
	// THE CHAIN IS ANCHORED AT GENESIS, not at whatever the first stored header
	// happens to be: block 1's ParentHash is checked against the genesis hash
	// the chain root produces, so the hash chain this pass walks is rooted in
	// the trust anchor rather than in itself.
	anchor := g.Hash
	if *from > 1 {
		anchor = common.Hash{}
	}
	blocks, txs, err := verifyRange(db, *from, *to, anchor)
	if err != nil {
		log.Fatalf("epochdb: verify: FAIL after %d blocks: %v", blocks, err)
	}
	log.Printf("epochdb: verify: PASS, %d blocks / %d txs in %s (header chain, txRoot, receiptsRoot, logsBloom, container reassembly)",
		blocks, txs, time.Since(start).Round(time.Millisecond))
}

// cursor is one chain family read SEQUENTIALLY. THIS PASS IS THE WHOLE REASON
// ChainRows exists: verify touches every hdr/, blk/, pvm/, tx/ and rcpt/ row of
// the range, and reading them as point Gets decompresses the same 128KB zstd-9
// block once per row instead of once per block. Five cursors advance in
// lockstep, each family a contiguous key range of the chain section, so every
// data block is decoded exactly once for the whole pass.
type cursor struct {
	name string
	next func() (store.ChainRow, error, bool)
	stop func()
}

func newCursor(db *store.DB, name string, fam int, from, to uint64) *cursor {
	next, stop := iter.Pull2(db.ChainRows(fam, from, to))
	return &cursor{name: name, next: next, stop: stop}
}

// at returns the row numbered n. The families are dense over a stored range, so
// anything else is a hole and A CHECK THAT CANNOT RUN IS A FAILURE: it is
// reported rather than skipped.
func (c *cursor) at(n uint64) ([]byte, error) {
	row, err, ok := c.next()
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("%s%d missing", c.name, n)
	}
	if row.Num != n {
		return nil, fmt.Errorf("%s%d missing (the next stored row is %d)", c.name, n, row.Num)
	}
	return row.Val, nil
}

func verifyRange(db *store.DB, from, to uint64, anchor common.Hash) (blocks, txs uint64, err error) {
	// The tx-keyed families are bounded by the block range's own TxNums, which
	// the blk/ rows at either end name.
	firstTx, _, ok, err := db.BlockTxRange(from)
	if err != nil {
		return 0, 0, err
	}
	if !ok {
		return 0, 0, fmt.Errorf("block %d: no blk row", from)
	}
	lastFirst, lastCount, ok, err := db.BlockTxRange(to)
	if err != nil {
		return 0, 0, err
	}
	if !ok {
		return 0, 0, fmt.Errorf("block %d: no blk row", to)
	}
	lastTx := lastFirst + uint64(lastCount) // exclusive

	hdrs := newCursor(db, store.PrefixHdr, store.FamHdr, from, to)
	defer hdrs.stop()
	blks := newCursor(db, store.PrefixBlk, store.FamBlk, from, to)
	defer blks.stop()
	pvms := newCursor(db, store.PrefixPvm, store.FamPvm, from, to)
	defer pvms.stop()
	// The tx-keyed cursors span [firstTx, lastTx). A range with no transactions
	// at all is expressed as to < from, which yields nothing.
	txLo, txHi := firstTx, lastTx-1
	if lastTx <= firstTx {
		txLo, txHi = 1, 0
	}
	rawTx := newCursor(db, store.PrefixTx, store.FamTx, txLo, txHi)
	defer rawTx.stop()
	rcpts := newCursor(db, store.PrefixRcpt, store.FamRcpt, txLo, txHi)
	defer rcpts.stop()

	prevHash := anchor
	havePrev := anchor != (common.Hash{})
	for n := from; n <= to; n++ {
		hdrRLP, err := hdrs.at(n)
		if err != nil {
			return blocks, txs, err
		}
		var h types.Header
		if err := rlp.DecodeBytes(hdrRLP, &h); err != nil {
			return blocks, txs, fmt.Errorf("block %d: decode header: %w", n, err)
		}
		if havePrev && h.ParentHash != prevHash {
			return blocks, txs, fmt.Errorf("block %d: parent hash %x does not chain to %x", n, h.ParentHash, prevHash)
		}
		prevHash, havePrev = h.Hash(), true

		blkVal, err := blks.at(n)
		if err != nil {
			return blocks, txs, err
		}
		if len(blkVal) != 12 {
			return blocks, txs, fmt.Errorf("block %d: blk row is %d bytes, want 12", n, len(blkVal))
		}
		first := binary.BigEndian.Uint64(blkVal[:8])
		count := binary.BigEndian.Uint32(blkVal[8:])

		list := make(types.Transactions, 0, count)
		rawTxs := make([][]byte, 0, count)
		receipts := make(types.Receipts, 0, count)
		for i := uint64(0); i < uint64(count); i++ {
			raw, err := rawTx.at(first + i)
			if err != nil {
				return blocks, txs, fmt.Errorf("block %d: %w", n, err)
			}
			tx := new(types.Transaction)
			if err := rlp.DecodeBytes(raw, tx); err != nil {
				return blocks, txs, fmt.Errorf("block %d: decode tx/%d: %w", n, first+i, err)
			}
			list = append(list, tx)
			rawTxs = append(rawTxs, raw)

			rec, err := rcpts.at(first + i)
			if err != nil {
				return blocks, txs, fmt.Errorf("block %d: %w", n, err)
			}
			r, err := decodeReceipt(tx, rec)
			if err != nil {
				return blocks, txs, fmt.Errorf("block %d: rcpt/%d: %w", n, first+i, err)
			}
			receipts = append(receipts, r)
		}
		txs += uint64(count)

		if got := types.DeriveSha(list, trie.NewStackTrie(nil)); got != h.TxHash {
			return blocks, txs, fmt.Errorf("block %d: txRoot %x from stored rows, header says %x", n, got, h.TxHash)
		}
		if got := types.DeriveSha(receipts, trie.NewStackTrie(nil)); got != h.ReceiptHash {
			return blocks, txs, fmt.Errorf("block %d: receiptsRoot %x from stored rows, header says %x", n, got, h.ReceiptHash)
		}
		if got := types.CreateBloom(receipts); got != h.Bloom {
			return blocks, txs, fmt.Errorf("block %d: logsBloom from stored rows differs from the header", n)
		}

		// CONTAINER REASSEMBLY, spot-checked on every block of the range: the
		// pieces must re-form something whose inner block hashes to this header.
		pvm, err := pvms.at(n)
		if err != nil {
			return blocks, txs, err
		}
		raw, err := store.Reassemble(pvm, hdrRLP, rawTxs)
		if err != nil {
			return blocks, txs, fmt.Errorf("block %d: reassemble: %w", n, err)
		}
		_, inner, err := store.SplitContainer(raw)
		if err != nil {
			return blocks, txs, fmt.Errorf("block %d: reassembled container does not parse: %w", n, err)
		}
		if inner.Hash() != h.Hash() {
			return blocks, txs, fmt.Errorf("block %d: reassembled container holds block %x, want %x", n, inner.Hash(), h.Hash())
		}
		blocks++
	}
	return blocks, txs, nil
}

// decodeReceipt rebuilds the consensus receipt from a stored row. Only the
// fields the receipts trie covers are needed: type, status, cumulative gas,
// bloom and the logs.
func decodeReceipt(tx *types.Transaction, rec []byte) (*types.Receipt, error) {
	status, _, cum, logs, err := store.DecodeTxReceipt(rec)
	if err != nil {
		return nil, err
	}
	r := &types.Receipt{
		Type:              tx.Type(),
		Status:            status,
		CumulativeGasUsed: cum,
	}
	for _, l := range logs {
		r.Logs = append(r.Logs, &types.Log{Address: l.Address, Topics: l.Topics, Data: l.Data})
	}
	r.Bloom = types.CreateBloom(types.Receipts{r})
	return r, nil
}

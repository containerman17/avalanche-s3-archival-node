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
//   - TxNum contiguity: each block's `blk/` row starts exactly one slot past
//     where the previous block ended, so the one dimension has no hole and no
//     overlap and every slot belongs to exactly one block;
//   - state rows: a block whose header Root differs from its parent's changed
//     the state, so it must own a state row inside its own TxNum slots;
//   - txRoot: derived from the `tx/` rows of the block;
//   - receiptsRoot and logsBloom: derived from the `rcpt/` rows;
//   - the `itx/` call frames: one row per transaction, and it decodes;
//   - container reassembly: `hdr` + `pvm` + `tx` rows re-form the container
//     byte for byte (the day-one invariant, also unit-tested in store).
//
// FRAMES HAVE NO CRYPTOGRAPHIC ORACLE and cannot get one: no root commits to
// them, so nothing here can say a stored frame is the frame execution produced.
// What this pass CAN say is that the family is DENSE and WELL FORMED, which is
// the difference between "these frames are the chain's" and "some transactions
// have no frames at all". A capture hole is refused at execution (the fail-stop
// in exec/frames.go), and this is the second reader: it catches a corpus whose
// itx/ rows went missing after the fact, or which some earlier binary wrote
// without them. An EMPTY row is a real answer (a transaction that made no
// nested call) and only an ABSENT one is a hole, which is exactly the
// distinction the store keeps.
//
// STATE ROOTS THEMSELVES are checked WHERE THEY ARE PRODUCED: the executor
// hard-stops on any block whose Firewood root differs from the header's, and a
// JOINED corpus has the frontier build's one root at the height it joined at.
// Rebuilding every historical root here would mean feeding a throwaway trie in
// TxNum order out of a family sorted by key, i.e. an external sort of every row
// in the corpus, so what this pass adds instead is the cheap half that catches
// a whole class: a block that changed the root and stored nothing for it.
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
	cas, db, err := openCorpus(*dataDir, c.Root())
	if err != nil {
		log.Fatalf("epochdb: verify: %v", err)
	}
	defer cas.Close()
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
	// A CHECK THAT CANNOT RUN IS A FAILURE. An empty range walked zero blocks
	// and proved nothing, so it must not print the word PASS: --from above --to
	// used to do exactly that.
	if blocks == 0 {
		log.Fatalf("epochdb: verify: the range [%d..%d] holds no blocks, so nothing was verified: that is a FAILURE, not a pass", *from, *to)
	}
	// THE ANCHOR IS MANDATORY, so a run without one says so rather than
	// claiming the header chain. Unanchored, the walk proves only that the
	// stored headers chain to EACH OTHER, which is a property of any chain.
	anchored := "anchored at genesis"
	if anchor == (common.Hash{}) {
		anchored = "UNANCHORED (--from above 1 drops the genesis anchor, so the header chain is only self-consistent)"
	}
	log.Printf("epochdb: verify: PASS, %d blocks / %d txs in %s (header chain %s, TxNum contiguity, txRoot, receiptsRoot, logsBloom, frames present, state rows present, container reassembly)",
		blocks, txs, time.Since(start).Round(time.Millisecond), anchored)
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

// stateSlots is one bit per TxNum slot in the verified range: set means at
// least one state row sits at that slot. The state family is sorted by KEY, so
// its TxNums arrive in no order at all and there is nothing to walk in lockstep
// with the block loop; one bit per slot is the smallest thing that answers "did
// this block write anything" for every block in one pass (25MB per 200M slots).
type stateSlots struct {
	base uint64
	bits []uint64
}

func newStateSlots(lo, hi uint64) *stateSlots {
	return &stateSlots{base: lo, bits: make([]uint64, (hi-lo)/64+1)}
}

func (s *stateSlots) set(n uint64) {
	i := n - s.base
	s.bits[i/64] |= 1 << (i % 64)
}

func (s *stateSlots) any(lo, hi uint64) bool {
	for n := lo; n <= hi; n++ {
		i := n - s.base
		if i/64 < uint64(len(s.bits)) && s.bits[i/64]&(1<<(i%64)) != 0 {
			return true
		}
	}
	return false
}

func verifyRange(db *store.DB, from, to uint64, anchor common.Hash) (blocks, txs uint64, err error) {
	if to < from {
		return 0, 0, nil // an inverted range walks nothing; verifyMain calls that a FAILURE
	}
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
	// lastTx is the last block's BOUNDARY SLOT: the end of the tx rows and the
	// last slot of the whole range.
	lastTx := lastFirst + uint64(lastCount)

	// The state family in one sequential pass, before the block walk: which
	// slots carry a row.
	state := newStateSlots(firstTx, lastTx)
	if err := db.StateTxNums(firstTx, lastTx, state.set); err != nil {
		return 0, 0, fmt.Errorf("read the state family over TxNums [%d,%d]: %w", firstTx, lastTx, err)
	}

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
	itxs := newCursor(db, store.PrefixItx, store.FamItx, txLo, txHi)
	defer itxs.stop()

	prevHash := anchor
	havePrev := anchor != (common.Hash{})
	wantFirst := firstTx
	var prevRoot common.Hash
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

		// TxNum IS THE ONE DIMENSION, so it must have neither hole nor overlap.
		// Aiming the tx cursors at each block's OWN blk/ row made every block
		// self-consistent and left the joins between them unchecked: a range of
		// TxNums belonging to no block passed, because the iterator simply
		// yields the next stored row and the cursor asked for exactly that.
		if first != wantFirst {
			return blocks, txs, fmt.Errorf("block %d starts at TxNum %d, but the previous block ended at %d", n, first, wantFirst)
		}
		// A block owns [first, first+count]: its transactions, then its BOUNDARY
		// SLOT. The next block therefore starts one past that, and every slot in
		// the range belongs to exactly one block.
		bound := first + uint64(count)
		wantFirst = bound + 1

		// STATE ROWS, and this is the only check in layer 1 that reads one. A
		// block whose header Root differs from its parent's CHANGED STATE, which
		// is consensus math and not our bookkeeping, so it must own at least one
		// state row inside its own slots. That is exactly what a block-level
		// write losing its slot looks like from outside: the block changed the
		// state and the corpus has nothing to show for it. The first block of the
		// range has no stored parent root to compare against, so it is exempt.
		if n > from && h.Root != prevRoot && !state.any(first, bound) {
			return blocks, txs, fmt.Errorf("block %d changed the state root from %x to %x but wrote no state row in its own TxNum slots [%d,%d]",
				n, prevRoot, h.Root, first, bound)
		}
		prevRoot = h.Root

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

			// THE FRAMES, the one family that can never be backfilled by IO.
			// The row must EXIST (an absent one is a transaction whose trace
			// this corpus does not have) and it must decode. An empty row is a
			// transaction that made no nested call, which is a real answer.
			itxRec, err := itxs.at(first + i)
			if err != nil {
				return blocks, txs, fmt.Errorf("block %d: %w", n, err)
			}
			if _, err := store.DecodeFrames(itxRec); err != nil {
				return blocks, txs, fmt.Errorf("block %d: itx/%d: %w", n, first+i, err)
			}
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

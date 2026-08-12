package exec

import (
	"log"

	"github.com/ava-labs/libevm/common"
	ethstate "github.com/ava-labs/libevm/core/state"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/rlp"

	"github.com/containerman17/epochdb/store"
)

// dumpMismatch prints the full post-state dirty set of a block whose
// computed root did not match the header, so it can be diffed field by
// field against an archive RPC at the same height. Everything it prints is
// already in hand: the captured rows ARE the post-image set, and the
// receipts came back from runEVM.
//
// Reached only on a hard-stop root mismatch, so cost does not matter.
func dumpMismatch(blk *types.Block, rows []store.StateRow, receipts types.Receipts, statedb *ethstate.StateDB, computed, expected common.Hash) {
	n := blk.NumberU64()
	log.Printf("MISMATCH DUMP block=%d hash=%s time=%d txs=%d gasUsed=%d computed=%x expected=%x",
		n, blk.Hash(), blk.Time(), len(blk.Transactions()), blk.GasUsed(), computed, expected)

	for _, r := range rows {
		addr := common.BytesToAddress(r.Addr)
		switch r.Kind {
		case 'a':
			if len(r.Val) == 0 {
				log.Printf("  acct %s DELETED", addr)
				continue
			}
			var acct types.StateAccount
			if err := rlp.DecodeBytes(r.Val, &acct); err != nil {
				log.Printf("  acct %s undecodable rlp %x: %v", addr, r.Val, err)
				continue
			}
			codeLen := 0
			if statedb != nil {
				codeLen = len(statedb.GetCode(addr))
			}
			log.Printf("  acct %s nonce=%d balance=%s root=%x codehash=%x codelen=%d",
				addr, acct.Nonce, acct.Balance, acct.Root, acct.CodeHash, codeLen)
		case 's':
			log.Printf("  slot %s %x = %x", addr, r.Slot, r.Val)
		case 'c':
			log.Printf("  code %s hash=%x", addr, r.Val)
		}
	}

	for i, r := range receipts {
		log.Printf("  receipt %d tx=%s status=%d gasUsed=%d cumGas=%d contract=%s logs=%d",
			i, r.TxHash, r.Status, r.GasUsed, r.CumulativeGasUsed, r.ContractAddress, len(r.Logs))
		for j, l := range r.Logs {
			log.Printf("    log %d addr=%s topics=%x data=%x", j, l.Address, l.Topics, l.Data)
		}
	}
}

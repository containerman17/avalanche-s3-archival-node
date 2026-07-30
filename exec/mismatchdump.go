package exec

import (
	"encoding/binary"
	"log"

	"github.com/ava-labs/libevm/common"
	ethstate "github.com/ava-labs/libevm/core/state"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/rlp"
)

// dumpMismatch prints the full post-state dirty set of a block whose
// computed root did not match the header, so it can be diffed field by
// field against an archive RPC at the same height. Everything it prints is
// already in hand: the write-capture frame IS the post-image set, and the
// receipts came back from runEVM.
//
// Reached only on a hard-stop root mismatch, so cost does not matter.
func dumpMismatch(blk *types.Block, frame *blockFrame, receipts types.Receipts, statedb *ethstate.StateDB, computed, expected common.Hash) {
	n := blk.NumberU64()
	log.Printf("MISMATCH DUMP block=%d hash=%s time=%d txs=%d gasUsed=%d computed=%x expected=%x",
		n, blk.Hash(), blk.Time(), len(blk.Transactions()), blk.GasUsed(), computed, expected)

	pos := 0
	for pos < len(frame.buf) {
		kind := frame.buf[pos]
		pos++
		addr := common.BytesToAddress(frame.buf[pos : pos+20])
		pos += 20
		var slot common.Hash
		if kind == kindStorage || kind == kindCodeUse {
			slot = common.BytesToHash(frame.buf[pos : pos+32])
			pos += 32
		}
		vlen, w := binary.Uvarint(frame.buf[pos:])
		pos += w
		val := frame.buf[pos : pos+int(vlen)]
		pos += int(vlen)

		switch kind {
		case kindAccount:
			if len(val) == 0 {
				log.Printf("  acct %s DELETED", addr)
				continue
			}
			var acct types.StateAccount
			if err := rlp.DecodeBytes(val, &acct); err != nil {
				log.Printf("  acct %s undecodable rlp %x: %v", addr, val, err)
				continue
			}
			codeLen := 0
			if statedb != nil {
				codeLen = len(statedb.GetCode(addr))
			}
			log.Printf("  acct %s nonce=%d balance=%s root=%x codehash=%x codelen=%d",
				addr, acct.Nonce, acct.Balance, acct.Root, acct.CodeHash, codeLen)
		case kindStorage:
			log.Printf("  slot %s %x = %x", addr, slot, val)
		case kindCodeUse:
			log.Printf("  code %s hash=%x", addr, slot)
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

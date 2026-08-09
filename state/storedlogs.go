package state

import (
	"encoding/binary"
	"fmt"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/types"
)

// Stored-logs record codecs (epoch v2/v3 sections AND the live tail capture:
// ONE encoding, so a seal is a byte copy). Layout per log-bearing block:
// uvarint nLogs, then per log: addr20 | uvarint nTopics | topics | uvarint
// dataLen | data | uvarint txIndex. logIndex is the position in the record;
// txHash and blockHash are derived from the block body at read time (they are
// pure functions of data the epoch already stores). Receipt-fields record per
// tx-bearing block: per tx uvarint gasUsed + status byte; cumulativeGasUsed is
// the running sum.
//
// They live in state (not rpc) because the executor writes them and rpc,
// verify and seal all read them; exec cannot import rpc.

// EncodeStoredLogs flattens a block's receipts into the stored-logs record.
// nil = the block has no logs.
func EncodeStoredLogs(receipts types.Receipts) []byte {
	nLogs := 0
	for _, r := range receipts {
		nLogs += len(r.Logs)
	}
	if nLogs == 0 {
		return nil
	}
	rec := binary.AppendUvarint(nil, uint64(nLogs))
	for ti, r := range receipts {
		for _, l := range r.Logs {
			rec = append(rec, l.Address[:]...)
			rec = binary.AppendUvarint(rec, uint64(len(l.Topics)))
			for _, tp := range l.Topics {
				rec = append(rec, tp[:]...)
			}
			rec = binary.AppendUvarint(rec, uint64(len(l.Data)))
			rec = append(rec, l.Data...)
			rec = binary.AppendUvarint(rec, uint64(ti))
		}
	}
	return rec
}

// StoredLog is one decoded log tuple (block-relative).
type StoredLog struct {
	Address common.Address
	Topics  []common.Hash
	Data    []byte
	TxIndex uint
}

// DecodeStoredLogs is the inverse of EncodeStoredLogs.
func DecodeStoredLogs(rec []byte) ([]StoredLog, error) {
	n, pos := binary.Uvarint(rec)
	if pos <= 0 {
		return nil, fmt.Errorf("stored logs: bad count")
	}
	out := make([]StoredLog, 0, n)
	for i := uint64(0); i < n; i++ {
		var l StoredLog
		if pos+20 > len(rec) {
			return nil, fmt.Errorf("stored logs: truncated addr")
		}
		copy(l.Address[:], rec[pos:pos+20])
		pos += 20
		nt, k := binary.Uvarint(rec[pos:])
		if k <= 0 {
			return nil, fmt.Errorf("stored logs: bad topic count")
		}
		pos += k
		for j := uint64(0); j < nt; j++ {
			var t common.Hash
			copy(t[:], rec[pos:pos+32])
			l.Topics = append(l.Topics, t)
			pos += 32
		}
		dl, k2 := binary.Uvarint(rec[pos:])
		if k2 <= 0 {
			return nil, fmt.Errorf("stored logs: bad data len")
		}
		pos += k2
		l.Data = rec[pos : pos+int(dl)]
		pos += int(dl)
		ti, k3 := binary.Uvarint(rec[pos:])
		if k3 <= 0 {
			return nil, fmt.Errorf("stored logs: bad tx index")
		}
		pos += k3
		l.TxIndex = uint(ti)
		out = append(out, l)
	}
	return out, nil
}

// EncodeStoredReceipts packs per-tx gasUsed+status. nil for empty blocks.
func EncodeStoredReceipts(receipts types.Receipts) []byte {
	if len(receipts) == 0 {
		return nil
	}
	var rec []byte
	for _, r := range receipts {
		rec = binary.AppendUvarint(rec, r.GasUsed)
		rec = append(rec, byte(r.Status))
	}
	return rec
}

// StoredRcpt is one decoded per-tx receipt-fields entry.
type StoredRcpt struct {
	GasUsed       uint64
	CumulativeGas uint64
	Status        uint64
}

// DecodeStoredReceipts is the inverse of EncodeStoredReceipts, with the
// cumulative gas prefix sum filled in.
func DecodeStoredReceipts(rec []byte) ([]StoredRcpt, error) {
	var out []StoredRcpt
	pos, cum := 0, uint64(0)
	for pos < len(rec) {
		g, k := binary.Uvarint(rec[pos:])
		if k <= 0 || pos+k >= len(rec) {
			return nil, fmt.Errorf("stored receipts: truncated")
		}
		pos += k
		cum += g
		out = append(out, StoredRcpt{GasUsed: g, CumulativeGas: cum, Status: uint64(rec[pos])})
		pos++
	}
	return out, nil
}

// EncodeTailRcpt packs one block's two stored-section records into the single
// tail-family record: uvarint len(logsRec) | logsRec | rcptRec. Both halves
// are the epoch encodings VERBATIM, which is what lets seal copy them into the
// epoch sections instead of re-executing the block. nil = nothing to store
// (a block with no transactions).
func EncodeTailRcpt(logsRec, rcptRec []byte) []byte {
	if len(logsRec) == 0 && len(rcptRec) == 0 {
		return nil
	}
	rec := binary.AppendUvarint(make([]byte, 0, 2+len(logsRec)+len(rcptRec)), uint64(len(logsRec)))
	rec = append(rec, logsRec...)
	return append(rec, rcptRec...)
}

// DecodeTailRcpt splits a tail record back into its two epoch-encoded halves.
// Either half may be empty (a tx-bearing block that emitted no logs).
func DecodeTailRcpt(rec []byte) (logsRec, rcptRec []byte, err error) {
	n, pos := binary.Uvarint(rec)
	if pos <= 0 || pos+int(n) > len(rec) {
		return nil, nil, fmt.Errorf("tail receipts: bad framing")
	}
	return rec[pos : pos+int(n)], rec[pos+int(n):], nil
}

// EncodeTailItx packs one block's CALL FRAMES record and its address
// PARTICIPANTS record into the single `itx` tail-family record, same framing
// as EncodeTailRcpt. The frames half is the epoch encoding verbatim (seal
// copies it); the participants half feeds the address index and is dropped
// once the epoch is sealed. See exec/frames.go for both encodings.
func EncodeTailItx(framesRec, partsRec []byte) []byte {
	return EncodeTailRcpt(framesRec, partsRec)
}

// DecodeTailItx splits an `itx` tail record back into its two halves.
func DecodeTailItx(rec []byte) (framesRec, partsRec []byte, err error) {
	f, p, err := DecodeTailRcpt(rec)
	if err != nil {
		return nil, nil, fmt.Errorf("tail frames: bad framing")
	}
	return f, p, nil
}

// DecodeParticipants walks a participants record, calling yield once per
// TRANSACTION in tx order with that transaction's addresses. The group's
// position is its tx index, so every transaction has a group, possibly empty.
func DecodeParticipants(rec []byte, yield func(addrs [][20]byte)) error {
	var addrs [][20]byte
	for pos := 0; pos < len(rec); {
		n, k := binary.Uvarint(rec[pos:])
		if k <= 0 || pos+k+int(n)*20 > len(rec) {
			return fmt.Errorf("participants: truncated at %d", pos)
		}
		pos += k
		addrs = addrs[:0]
		for i := uint64(0); i < n; i++ {
			var a [20]byte
			copy(a[:], rec[pos:pos+20])
			addrs = append(addrs, a)
			pos += 20
		}
		yield(addrs)
	}
	return nil
}

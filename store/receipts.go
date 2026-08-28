package store

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
// They live in store (not rpc) because the executor writes them and rpc,
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

// ---------------------------------------------------------------------------
// PER-TRANSACTION receipt row (`rcpt/<txnum>`), storage v0.
//
// Storage v0 keys receipts by TxNum, not by height, so the block-level codecs
// above collapse into ONE record per transaction, with the same field set and
// the same varint style:
//
//	uvarint status | uvarint gasUsed | uvarint cumulativeGasUsed |
//	uvarint nLogs | per log: addr20 | uvarint nTopics | topics |
//	                         uvarint dataLen | data
//
// The tx index is implicit (it is the row's TxNum minus the block's first
// TxNum), so it is not stored. logIndex is the log's position in the record.
// ---------------------------------------------------------------------------

// EncodeTxReceipt packs one transaction's receipt row.
func EncodeTxReceipt(r *types.Receipt, cumulativeGasUsed uint64) []byte {
	rec := binary.AppendUvarint(nil, r.Status)
	rec = binary.AppendUvarint(rec, r.GasUsed)
	rec = binary.AppendUvarint(rec, cumulativeGasUsed)
	rec = binary.AppendUvarint(rec, uint64(len(r.Logs)))
	for _, l := range r.Logs {
		rec = append(rec, l.Address[:]...)
		rec = binary.AppendUvarint(rec, uint64(len(l.Topics)))
		for _, tp := range l.Topics {
			rec = append(rec, tp[:]...)
		}
		rec = binary.AppendUvarint(rec, uint64(len(l.Data)))
		rec = append(rec, l.Data...)
	}
	return rec
}

// DecodeTxReceipt is the inverse of EncodeTxReceipt. The returned logs carry
// no TxIndex: it is implicit in the row's key.
func DecodeTxReceipt(rec []byte) (status uint64, gasUsed, cumulativeGasUsed uint64, logs []StoredLog, err error) {
	pos := 0
	next := func(what string) (uint64, error) {
		v, k := binary.Uvarint(rec[pos:])
		if k <= 0 {
			return 0, fmt.Errorf("tx receipt: bad %s", what)
		}
		pos += k
		return v, nil
	}
	if status, err = next("status"); err != nil {
		return
	}
	if gasUsed, err = next("gas used"); err != nil {
		return
	}
	if cumulativeGasUsed, err = next("cumulative gas"); err != nil {
		return
	}
	n, err := next("log count")
	if err != nil {
		return
	}
	logs = make([]StoredLog, 0, n)
	for i := uint64(0); i < n; i++ {
		var l StoredLog
		if pos+20 > len(rec) {
			return 0, 0, 0, nil, fmt.Errorf("tx receipt: truncated addr")
		}
		copy(l.Address[:], rec[pos:pos+20])
		pos += 20
		nt, e := next("topic count")
		if e != nil {
			return 0, 0, 0, nil, e
		}
		if pos+int(nt)*32 > len(rec) {
			return 0, 0, 0, nil, fmt.Errorf("tx receipt: truncated topics")
		}
		// One allocation for the topics, not one per append growth step: a
		// terminal run's migration decodes tens of millions of logs. A log
		// with no topic keeps its NIL slice, which is what an encoder that
		// tells nil from empty (rpc's log JSON) already sees.
		if nt > 0 {
			l.Topics = make([]common.Hash, nt)
		}
		for j := uint64(0); j < nt; j++ {
			copy(l.Topics[j][:], rec[pos:pos+32])
			pos += 32
		}
		dl, e := next("data len")
		if e != nil {
			return 0, 0, 0, nil, e
		}
		if pos+int(dl) > len(rec) {
			return 0, 0, 0, nil, fmt.Errorf("tx receipt: truncated data")
		}
		l.Data = rec[pos : pos+int(dl)]
		pos += int(dl)
		logs = append(logs, l)
	}
	return status, gasUsed, cumulativeGasUsed, logs, nil
}

package rpc

import (
	"encoding/binary"
	"fmt"
	"sort"
	"sync"

	corethcore "github.com/ava-labs/avalanchego/graft/coreth/core"
	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/params"

	"github.com/containerman17/epochdb/state"
)

// Stored-logs record codecs (epoch v2 sections). Layout per log-bearing
// block: uvarint nLogs, then per log: addr20 | uvarint nTopics | topics |
// uvarint dataLen | data | uvarint txIndex. logIndex is the position in
// the record; txHash and blockHash are derived from the block body at read
// time (they are pure functions of data the epoch already stores).
// Receipt-fields record per tx-bearing block: per tx uvarint gasUsed +
// status byte; cumulativeGasUsed is the running sum.

// EncodeStoredLogs flattens re-executed receipts into the stored-logs
// record. nil = the block has no logs.
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

// NewDeriveStored returns the seal hook that fills the stored-logs inputs
// by re-executing every tx-bearing block of the epoch over the overlay.
// When in.Logs is nil (re-seal of a v1 epoch, raw capture consumed long
// ago) the posting-list tuples are rebuilt from the derived logs too:
// unique addrs/topics in first-appearance order, exactly the capture
// semantics (byte parity proven by the Fuji backfill gate).
func NewDeriveStored(hist *state.History, chainCtx corethcore.ChainContext, cfg *params.ChainConfig, parse ContainerParser, workers int) state.DeriveStored {
	return func(in *state.EpochInput) error {
		in.FullLogs = map[uint64][]byte{}
		in.RcptRecs = map[uint64][]byte{}
		rebuildTuples := in.Logs == nil
		var tuples []state.LogRec

		var (
			mu    sync.Mutex
			wg    sync.WaitGroup
			sem   = make(chan struct{}, workers)
			fatal error
		)
		for i := range in.Containers {
			wg.Add(1)
			sem <- struct{}{}
			go func(i int) {
				defer wg.Done()
				defer func() { <-sem }()
				if fatal != nil {
					return
				}
				n := in.Start + uint64(i)
				blk, err := parse(in.Containers[i])
				if err != nil {
					mu.Lock()
					fatal = fmt.Errorf("derive: parse %d: %w", n, err)
					mu.Unlock()
					return
				}
				if len(blk.Transactions()) == 0 {
					return
				}
				receipts, err := ReExecuteBlock(hist, chainCtx, cfg, blk)
				if err != nil {
					mu.Lock()
					fatal = fmt.Errorf("derive: block %d: %w", n, err)
					mu.Unlock()
					return
				}
				logsRec := EncodeStoredLogs(receipts)
				rcptRec := EncodeStoredReceipts(receipts)
				mu.Lock()
				if logsRec != nil {
					in.FullLogs[n] = logsRec
				}
				if rcptRec != nil {
					in.RcptRecs[n] = rcptRec
				}
				if rebuildTuples && logsRec != nil {
					tuples = append(tuples, tupleFromReceipts(n, receipts))
				}
				mu.Unlock()
			}(i)
		}
		wg.Wait()
		if fatal != nil {
			return fatal
		}
		if rebuildTuples {
			sort.Slice(tuples, func(a, b int) bool { return tuples[a].Block < tuples[b].Block })
			in.Logs = tuples
		}
		return nil
	}
}

// tupleFromReceipts mirrors exec's capture: unique addrs and topics in
// first-appearance order.
func tupleFromReceipts(n uint64, receipts types.Receipts) state.LogRec {
	lr := state.LogRec{Block: n}
	seenA := map[[20]byte]bool{}
	seenT := map[[32]byte]bool{}
	for _, r := range receipts {
		for _, l := range r.Logs {
			a := [20]byte(l.Address)
			if !seenA[a] {
				seenA[a] = true
				lr.Addrs = append(lr.Addrs, a)
			}
			for _, tp := range l.Topics {
				t := [32]byte(tp)
				if !seenT[t] {
					seenT[t] = true
					lr.Topics = append(lr.Topics, t)
				}
			}
		}
	}
	return lr
}

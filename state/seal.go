package state

import (
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/containerman17/epochdb/fetch"
)

// SealEpochs cuts and writes sealed epochs from the raw staging + capture
// files, strictly behind the durable exec head. Idempotent and resumable:
// the next epoch always starts right after the last sealed one, files land
// via tmp+rename. With deleteRaw, raw bucket files whose whole range is
// sealed are removed afterwards; NEVER enable that next to a running
// fetch/exec (they own those files).
func SealEpochs(dir string, deleteRaw bool) error {
	return sealEpochs(dir, deleteRaw, EpochTxs)
}

func sealEpochs(dir string, deleteRaw bool, epochTxs uint64) error {
	store, err := OpenReadOnly(dir)
	if err != nil {
		return err
	}
	defer store.Close()
	execHead, ok := store.ExecHead()
	if !ok || execHead == 0 {
		return fmt.Errorf("seal: no exec head, nothing replayed yet")
	}
	reader, err := fetch.OpenReader(dir)
	if err != nil {
		return err
	}
	defer reader.Close()

	set, err := OpenEpochSet(dir)
	if err != nil {
		return err
	}
	next := uint64(1) // block 0 is genesis: no container, state in the alloc
	if end, ok := set.SealedEnd(); ok {
		next = end + 1
	}
	set.Close()

	if ls, ok := store.LogsStart(); !ok || ls > next {
		return fmt.Errorf("seal: log capture starts at %d (ok=%v), cannot seal from %d", ls, ok, next)
	}

	for {
		in, rawBytes, err := gatherEpoch(store, reader, next, execHead, epochTxs)
		if err != nil {
			return err
		}
		if in == nil {
			log.Printf("seal: tail %d..%d stays raw (below EpochTxs=%d)", next, execHead, epochTxs)
			break
		}
		t0 := time.Now()
		path, err := BuildEpoch(dir, in)
		if err != nil {
			return fmt.Errorf("seal epoch at %d: %w", in.Start, err)
		}
		st, _ := os.Stat(path)
		log.Printf("seal: %s blocks=%d txs=%d raw=%.1fMB sealed=%.1fMB (%.2fx) in %s",
			filepath.Base(path), len(in.Containers), in.TxCount,
			float64(rawBytes)/1e6, float64(st.Size())/1e6,
			float64(rawBytes)/float64(st.Size()), time.Since(t0).Round(time.Millisecond))
		next = in.Start + uint64(len(in.Containers))
	}

	if deleteRaw {
		return deleteSealedRaw(dir, next-1)
	}
	return nil
}

// gatherEpoch collects blocks from start until the cumulative tx count
// reaches EpochTxs (that block included). nil input = not enough txs
// materialized yet (tail stays raw). rawBytes counts the uncompressed raw
// equivalents (containers + headers + writelog frames + logs records) for
// the compression scoreboard.
func gatherEpoch(store *Store, reader *fetch.Reader, start, execHead, epochTxs uint64) (*EpochInput, uint64, error) {
	in := &EpochInput{Start: start, TxHashes: map[uint64][][32]byte{}}
	var rawBytes uint64
	for n := start; n <= execHead; n++ {
		container, ok, err := reader.GetByHeight(n)
		if err != nil {
			return nil, 0, err
		}
		if !ok {
			return nil, 0, nil // staging gap below exec head should not happen, but never seal past one
		}
		headerRLP, ok, err := store.HeaderRLP(n)
		if err != nil || !ok {
			return nil, 0, fmt.Errorf("seal: header %d: ok=%v err=%v", n, ok, err)
		}
		in.Containers = append(in.Containers, container)
		in.Headers = append(in.Headers, headerRLP)
		rawBytes += uint64(len(container) + len(headerRLP))

		hashes, err := extractTxHashes(innerEthBlock(container), nil)
		if err != nil {
			return nil, 0, fmt.Errorf("seal: block %d txs: %w", n, err)
		}
		if len(hashes) > 0 {
			hs := make([][32]byte, len(hashes))
			for i, h := range hashes {
				hs[i] = [32]byte(h)
			}
			in.TxHashes[n] = hs
			in.TxCount += uint64(len(hashes))
		}

		if frame, ok, err := store.wl.Get(n); err != nil {
			return nil, 0, err
		} else if ok {
			rawBytes += uint64(len(frame))
			seq := 0
			if err := parseFrame(frame, func(kind byte, key [sortedKeySize]byte, valOff int, vlen uint32) {
				if kind == recKindCodeUse {
					return
				}
				in.StateRows = append(in.StateRows, StateRow{
					Key:   key,
					Block: n,
					Value: append([]byte(nil), frame[valOff:valOff+int(vlen)]...),
					Seq:   seq,
				})
				seq++
			}); err != nil {
				return nil, 0, fmt.Errorf("seal: writelog frame %d: %w", n, err)
			}
		}

		if rec, ok, err := store.LogsRecord(n); err != nil {
			return nil, 0, err
		} else if ok {
			rawBytes += uint64(len(rec))
			lr, err := decodeLogRec(n, rec)
			if err != nil {
				return nil, 0, fmt.Errorf("seal: logs record %d: %w", n, err)
			}
			in.Logs = append(in.Logs, lr)
		}

		if in.TxCount >= epochTxs {
			return in, rawBytes, nil
		}
	}
	return nil, 0, nil // ran out of replayed blocks before the boundary
}

// decodeLogRec decodes one capture-format logs record (exec encodeLogsFrame
// layout: uvarint nAddr + 20B addrs + uvarint nTopic + 32B topics).
func decodeLogRec(block uint64, rec []byte) (LogRec, error) {
	lr := LogRec{Block: block}
	nA, off := binary.Uvarint(rec)
	if off <= 0 {
		return lr, fmt.Errorf("bad addr count")
	}
	for i := uint64(0); i < nA; i++ {
		if off+20 > len(rec) {
			return lr, fmt.Errorf("truncated addr")
		}
		var a [20]byte
		copy(a[:], rec[off:off+20])
		lr.Addrs = append(lr.Addrs, a)
		off += 20
	}
	nT, k := binary.Uvarint(rec[off:])
	if k <= 0 {
		return lr, fmt.Errorf("bad topic count")
	}
	off += k
	for i := uint64(0); i < nT; i++ {
		if off+32 > len(rec) {
			return lr, fmt.Errorf("truncated topic")
		}
		var t [32]byte
		copy(t[:], rec[off:off+32])
		lr.Topics = append(lr.Topics, t)
		off += 32
	}
	return lr, nil
}

// deleteSealedRaw removes raw bucket files whose entire block range is at
// or below sealedEnd.
func deleteSealedRaw(dir string, sealedEnd uint64) error {
	patterns := []string{
		"arrival_%05d.log", "index_%05d.log",
		"writelog_%05d.log", "writelog_idx_%05d.log",
		"headers_%05d.log", "headers_idx_%05d.log",
		"logs_%05d.log", "logs_idx_%05d.log",
		"logsbf_%05d.log", "logsbf_idx_%05d.log", "logsbf_done_%05d",
		"sorted_%05d.idx", "txidx_%05d.idx",
	}
	for b := uint64(0); (b+1)*BucketBlocks-1 <= sealedEnd; b++ {
		for _, p := range patterns {
			path := filepath.Join(dir, fmt.Sprintf(p, b))
			if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
				return err
			}
		}
		log.Printf("seal: raw bucket %05d removed (sealed through %d)", b, sealedEnd)
	}
	return nil
}

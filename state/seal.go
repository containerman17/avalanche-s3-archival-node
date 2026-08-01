package state

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/types"
	"github.com/containerman17/epochdb/dist"
	"github.com/containerman17/epochdb/fetch"
)

// SealEpochs cuts and writes sealed epochs from the raw staging + capture
// files, strictly behind the durable exec head. Idempotent and resumable:
// the next epoch always starts right after the last sealed one, files land
// via tmp+rename. Raw bucket files whose WHOLE range is sealed are removed
// afterwards, unconditionally (user ruling 2026-07-29: --delete-raw was the
// only sane setting, so the flag is gone). NEVER run seal next to a running
// fetch/exec: they own those files.
//
// The stored-logs/receipt sections come from the executor's LIVE CAPTURE (the
// rcpt tail family, same encoding), never from re-execution: the DeriveStored
// seal stage is gone, proven byte-identical before deletion. A range whose
// tx-bearing blocks have no captured records is REFUSED, not backfilled
// (corpora are disposable by ruling; see gatherEpoch).
//
// Epoch boundaries come from EpochTxsAt alone (no flag, no config): the epoch
// index is how many epochs are already sealed, so a resumed seal cuts exactly
// where an uninterrupted one would.
//
// outDir is where sealed epoch files land (usually dir itself; a separate dir
// rebuilds the epochs from the same raw captures without touching the
// existing ones).
//
// This is the OFFLINE all-in-one (`epochdb seal` on a dir nobody is serving).
// A live serve/fleet process seals through History.SealTail, which splits the
// same work so the new epochs reach its readers before the raw goes.
func SealEpochs(dir string, out *dist.Store, chainRoot [32]byte) error {
	run, err := sealEpochs(dir, out, chainRoot)
	if err != nil {
		return err
	}
	return DeleteSealedRaw(dir, run.SealedEnd)
}

// sealRun is what one seal pass did.
type sealRun struct {
	// Cut is how many epochs this pass wrote.
	Cut int
	// SealedEnd is the last sealed block after it (0 = nothing is sealed).
	SealedEnd uint64
	// RetryAt is the exec head below which another pass cannot cut anything,
	// extrapolated from the tx rate of the tail it just measured. It is what
	// lets a live process attempt a seal on every cook tick without paying for
	// a full gather every time.
	RetryAt uint64
}

// SealTail is the LIVE process's seal, and the ORDER is the whole point:
//
//  1. cut whatever whole epochs the durable exec head allows,
//  2. publish them to this History's readers (refreshEpochs),
//  3. only then delete the raw buckets they replace, and drop this process's
//     own handles on those files so the space is actually freed.
//
// Between 1 and 2 the raw is still there and answers everything; after 2 the
// epoch answers everything; so no read of a sealed height is ever without a
// source. Cheap when there is nothing to do: below RetryAt it opens nothing.
//
// Single-caller by contract (the cook loop), like AdvanceTail/PruneTail.
func (h *History) SealTail(chainRoot [32]byte) (epochs int, sealedEnd uint64, err error) {
	execHead, ok := h.store.ExecHead()
	if !ok || execHead < h.sealRetryAt {
		return 0, 0, nil
	}
	run, err := sealEpochs(h.dir, h.store.Cas(), chainRoot)
	if err != nil {
		// A corpus seal cannot read (no capture, a staging gap) must not
		// re-scan every cook tick: log once per bucket of new blocks.
		h.sealRetryAt = execHead + BucketBlocks
		return 0, 0, err
	}
	h.sealRetryAt = run.RetryAt
	if run.Cut == 0 {
		return 0, run.SealedEnd, nil
	}
	if _, err := h.refreshEpochs(); err != nil {
		return run.Cut, run.SealedEnd, fmt.Errorf("publish sealed epochs: %w", err)
	}
	if err := DeleteSealedRaw(h.dir, run.SealedEnd); err != nil {
		return run.Cut, run.SealedEnd, err
	}
	h.dropSealedBuckets(run.SealedEnd)
	return run.Cut, run.SealedEnd, h.store.RetireBuckets(run.SealedEnd)
}

// epochTxsAt is the schedule the seal loop reads, indirected ONLY so package
// tests can cut epochs of ten txs instead of a quarter million. The production
// path has no knob.
var epochTxsAt = EpochTxsAt

// bucketBlocks is the raw-retirement granularity, indirected for the same
// reason and nothing else: a package test retires a bucket of a few blocks
// instead of 100,000. Everything that decides whether a bucket is fully sealed
// reads it (the unlink, this process's handles, the mapped sorted buckets), so
// the three stay in step.
var bucketBlocks uint64 = BucketBlocks

// hashBytes decodes a hex sha256 artifact name into the footer's link field.
func hashBytes(hash string) ([32]byte, error) {
	var out [32]byte
	b, err := hex.DecodeString(hash)
	if err != nil || len(b) != len(out) {
		return out, fmt.Errorf("state: %q is not a hex sha256", hash)
	}
	copy(out[:], b)
	return out, nil
}

// setLatestEpoch advances the `latest` pointer: it names the newest epoch
// and nothing else.
func setLatestEpoch(st *dist.Store, hash string) error {
	l, err := st.Latest()
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	l.Epoch = hash
	return st.SetLatest(l)
}

// sealEpochs cuts every whole epoch the durable exec head allows and writes
// them to out. It NEVER deletes anything: the caller does that, after
// whatever it must do to make the new epochs readable.
//
// It reads the corpus through OpenReadOnly + fetch.OpenReader, which never
// truncate and never create, so it is safe beside the live writer that owns
// those files.
func sealEpochs(dir string, out *dist.Store, chainRoot [32]byte) (sealRun, error) {
	var run sealRun
	store, err := OpenReadOnly(dir)
	if err != nil {
		return run, err
	}
	defer store.Close()
	execHead, ok := store.ExecHead()
	if !ok || execHead == 0 {
		return run, fmt.Errorf("seal: no exec head, nothing replayed yet")
	}
	reader, err := fetch.OpenReader(dir)
	if err != nil {
		return run, err
	}
	defer reader.Close()

	set, err := OpenEpochSet(out)
	if err != nil {
		return run, err
	}
	// prev is the hash-chain link the next epoch's footer carries: the head
	// epoch's own hash, or the chain root for the very first epoch.
	prev := chainRoot
	next := uint64(1) // block 0 is genesis: no container, state in the alloc
	// The index of the epoch about to be cut, which is what picks its size:
	// sealing is strictly sequential from block 1, so it is simply how many
	// epochs are already there.
	idx := len(set.All())
	if head, ok := set.Head(); ok {
		next = head.End() + 1
		if prev, err = hashBytes(head.Hash); err != nil {
			return run, err
		}
	}
	set.Close()
	run.SealedEnd = next - 1

	if ls, ok := store.LogsStart(); !ok || ls > next {
		return run, fmt.Errorf("seal: log capture starts at %d (ok=%v), cannot seal from %d", ls, ok, next)
	}

	for {
		epochTxs := epochTxsAt(idx)
		in, full, rawBytes, err := gatherEpoch(store, reader, next, execHead, epochTxs)
		if err != nil {
			return run, err
		}
		if !full {
			log.Printf("seal: tail %d..%d stays raw (below epoch %d's %d txs)", next, execHead, idx, epochTxs)
			run.RetryAt = retryAt(in, execHead, epochTxs)
			break
		}
		t0 := time.Now()
		in.Prev = prev
		hash, err := BuildEpoch(out, in)
		if err != nil {
			return run, fmt.Errorf("seal epoch at %d: %w", in.Start, err)
		}
		if prev, err = hashBytes(hash); err != nil {
			return run, err
		}
		if err := setLatestEpoch(out, hash); err != nil {
			return run, err
		}
		st, _ := os.Stat(out.SpoolPath(hash))
		log.Printf("seal: %s (%s) blocks=%d txs=%d code=%d raw=%.1fMB sealed=%.1fMB (%.2fx) in %s",
			EpochMarkerName(in.Start, uint64(len(in.Containers))), hash[:12], len(in.Containers), in.TxCount, len(in.Code),
			float64(rawBytes.total())/1e6, float64(st.Size())/1e6,
			float64(rawBytes.total())/float64(st.Size()), time.Since(t0).Round(time.Millisecond))
		if e, err := OpenEpoch(out, hash); err == nil {
			s := e.SectionSizes()
			log.Printf("seal:   raw: bodies=%.2fMB headers=%.2fMB writelog=%.2fMB logs=%.2fMB rcpt=%.2fMB",
				float64(rawBytes.containers)/1e6, float64(rawBytes.headers)/1e6,
				float64(rawBytes.writelog)/1e6, float64(rawBytes.logs)/1e6,
				float64(rawBytes.rcpt)/1e6)
			log.Printf("seal:   sealed: dict=%.2fMB bodies=%.2fMB(+idx %.2f) headers=%.2fMB(+idx %.2f) sst=%.2fMB(+idx %.2f) deletes=%.2fMB txidx=%.2fMB(+bloom %.2f) logidx=%.2fMB bloom=%.2fMB",
				float64(s["dict"])/1e6,
				float64(s["bodies"])/1e6, float64(s["bodiesIdx"])/1e6,
				float64(s["headers"])/1e6, float64(s["headersIdx"])/1e6,
				float64(s["sst"])/1e6, float64(s["sstIdx"])/1e6,
				float64(s["deletes"])/1e6, float64(s["txidx"])/1e6,
				float64(s["txbloom"])/1e6,
				float64(s["logidx"])/1e6, float64(s["keybloom"])/1e6)
			e.Close()
		}
		next = in.Start + uint64(len(in.Containers))
		idx++
		run.Cut++
		run.SealedEnd = next - 1
	}

	return run, nil
}

// retryAt extrapolates the exec head at which the tail in reaches epochTxs,
// from the tx rate of the tail itself. Being late costs only a bigger raw
// tail (the boundary is cut exactly where it always was, just on a later
// tick), being early costs a whole wasted gather, so the estimate is
// deliberately halved rather than doubled: a tail whose tx rate doubles still
// gets its epoch on the first tick after the boundary.
//
// ponytail: a rate guess, not a counter. The exact answer needs a per-block
// tx count nothing durable carries; add one only if a real chain is ever seen
// spending several gathers per epoch.
func retryAt(in *EpochInput, execHead, epochTxs uint64) uint64 {
	need := epochTxs - min(in.TxCount, epochTxs)
	blocks := uint64(len(in.Containers))
	ahead := need // no txs in the whole tail: no rate to extrapolate from
	if in.TxCount > 0 && blocks > 0 {
		ahead = need * blocks / in.TxCount / 2
	}
	return execHead + max(ahead, 1)
}

// rawSizes are the uncompressed raw equivalents consumed by one epoch, for
// the compression scoreboard.
type rawSizes struct{ containers, headers, writelog, logs, rcpt uint64 }

func (r rawSizes) total() uint64 {
	return r.containers + r.headers + r.writelog + r.logs + r.rcpt
}

// gatherEpoch collects blocks from start until the cumulative tx count
// reaches epochTxs (that block included). full=false means the boundary was
// not reached (the tail stays raw); the input is returned anyway, holding
// everything scanned, because its tx-per-block rate is what dates the next
// attempt (retryAt).
func gatherEpoch(store *Store, reader *fetch.Reader, start, execHead, epochTxs uint64) (*EpochInput, bool, rawSizes, error) {
	in := &EpochInput{
		Start:    start,
		TxHashes: map[uint64][][32]byte{},
		FullLogs: map[uint64][]byte{},
		RcptRecs: map[uint64][]byte{},
	}
	var rawBytes rawSizes
	for n := start; n <= execHead; n++ {
		container, ok, err := reader.GetByHeight(n)
		if err != nil {
			return in, false, rawSizes{}, err
		}
		if !ok {
			return in, false, rawSizes{}, nil // staging gap below exec head should not happen, but never seal past one
		}
		headerRLP, ok, err := store.HeaderRLP(n)
		if err != nil || !ok {
			return in, false, rawSizes{}, fmt.Errorf("seal: header %d: ok=%v err=%v", n, ok, err)
		}
		in.Containers = append(in.Containers, container)
		in.Headers = append(in.Headers, headerRLP)
		rawBytes.containers += uint64(len(container))
		rawBytes.headers += uint64(len(headerRLP))

		hashes, err := extractTxHashes(innerEthBlock(container), nil)
		if err != nil {
			return in, false, rawSizes{}, fmt.Errorf("seal: block %d txs: %w", n, err)
		}
		if len(hashes) > 0 {
			hs := make([][32]byte, len(hashes))
			for i, h := range hashes {
				hs[i] = [32]byte(h)
			}
			in.TxHashes[n] = hs
			in.TxCount += uint64(len(hashes))
		}

		// THE NO-RECORDS RULE: the stored sections are a byte copy of the
		// executor's live capture. A tx-bearing block without one cannot be
		// sealed, and there is deliberately no backfill-by-re-execution
		// crutch: corpora are disposable (DESIGN.md work order item 2), so
		// the answer to a pre-capture corpus is a fresh sync, not a patch.
		rcptRaw, hasRcpt, err := store.RcptRecord(n)
		if err != nil {
			return in, false, rawSizes{}, err
		}
		if len(hashes) > 0 && !hasRcpt {
			return in, false, rawSizes{}, fmt.Errorf(
				"seal: block %d has %d txs but no captured receipts record: this corpus was executed before live receipt capture, resync it (there is no backfill)",
				n, len(hashes))
		}
		if hasRcpt {
			logsRec, rcptRec, err := DecodeTailRcpt(rcptRaw)
			if err != nil {
				return in, false, rawSizes{}, fmt.Errorf("seal: block %d: %w", n, err)
			}
			rawBytes.rcpt += uint64(len(rcptRaw))
			if len(logsRec) > 0 {
				in.FullLogs[n] = logsRec
			}
			if len(rcptRec) > 0 {
				in.RcptRecs[n] = rcptRec
			}
		}

		if frame, ok, err := store.wl.Get(n); err != nil {
			return in, false, rawSizes{}, err
		} else if ok {
			rawBytes.writelog += uint64(len(frame))
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
				return in, false, rawSizes{}, fmt.Errorf("seal: writelog frame %d: %w", n, err)
			}
		}

		if rec, ok, err := store.LogsRecord(n); err != nil {
			return in, false, rawSizes{}, err
		} else if ok {
			rawBytes.logs += uint64(len(rec))
			lr, err := decodeLogRec(n, rec)
			if err != nil {
				return in, false, rawSizes{}, fmt.Errorf("seal: logs record %d: %w", n, err)
			}
			in.Logs = append(in.Logs, lr)
		}

		if in.TxCount >= epochTxs {
			return in, true, rawBytes, fillEpochCode(in, store)
		}
	}
	return in, false, rawSizes{}, nil // ran out of replayed blocks before the boundary
}

// fillEpochCode resolves the code of every account row this epoch writes,
// pulled from code.log. This loop IS the v3 placement rule (see
// EpochInput.Code).
func fillEpochCode(in *EpochInput, store *Store) error {
	in.Code = map[common.Hash][]byte{}
	for i := range in.StateRows {
		r := &in.StateRows[i]
		if r.Key[0] != recKindAccount || len(r.Value) == 0 {
			continue
		}
		hash, ok := accountCodeHash(r.Value)
		if !ok || hash == types.EmptyCodeHash || hash == (common.Hash{}) {
			continue
		}
		if _, done := in.Code[hash]; done {
			continue
		}
		blob, ok, err := store.code.Get(hash)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("seal epoch at %d: code %x referenced by an account row is not in code.log", in.Start, hash)
		}
		in.Code[hash] = blob
	}
	return nil
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

// DeleteSealedRaw removes raw bucket files whose entire block range is at
// or below sealedEnd. Unconditional (user ruling 2026-07-29) and safe beside
// the live process's own readers: a retired bucket reads as "not here", so
// every descent falls through to the epoch that replaced it. In a live
// process it must run AFTER those epochs are published (History.SealTail).
func DeleteSealedRaw(dir string, sealedEnd uint64) error {
	patterns := []string{
		"arrival_%05d.log", "index_%05d.log",
		"writelog_%05d.log", "writelog_idx_%05d.log",
		"headers_%05d.log", "headers_idx_%05d.log",
		"logs_%05d.log", "logs_idx_%05d.log",
		"rcpt_%05d.log", "rcpt_idx_%05d.log",
		"logsbf_%05d.log", "logsbf_idx_%05d.log", "logsbf_done_%05d",
		"sorted_%05d.idx", "txidx_%05d.idx",
	}
	for b := uint64(0); (b+1)*bucketBlocks-1 <= sealedEnd; b++ {
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

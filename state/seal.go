package state

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
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
// AFTER EACH EPOCH, unconditionally (user ruling 2026-07-29: --delete-raw was
// the only sane setting, so the flag is gone), so a long backfill crunch frees
// disk as it goes instead of holding raw and sealed side by side until the last
// epoch. NEVER run seal next to a running fetch/exec: they own those files.
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
	_, err := sealEpochs(context.Background(), dir, out, chainRoot, func(sealedEnd uint64) error {
		return DeleteSealedRaw(dir, sealedEnd)
	})
	return err
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

// SealTail is the LIVE process's seal, and the ORDER is the whole point,
// PER EPOCH:
//
//  1. cut ONE whole epoch, if the durable exec head allows one,
//  2. publish it to this History's readers (refreshEpochs),
//  3. only then delete the raw buckets it replaces, and drop this process's
//     own handles on those files so the space is actually freed,
//  4. onEpoch (may be nil), for whatever the caller owns outside `state`: the
//     serve node raises the fetcher's floor and retires its staging segments
//     there. Then back to 1 for the next epoch.
//
// Between 1 and 2 the raw is still there and answers everything; after 2 the
// epoch answers everything; so no read of a sealed height is ever without a
// source. Per epoch rather than per pass because a backlog crunch that cuts 14
// epochs before it deletes anything holds the whole raw corpus AND its sealed
// replacement at once (Fuji, 2026-08-01: ~100G sealed on top of 411G raw).
//
// ctx is observed BETWEEN epochs: a canceled pass returns what it has already
// cut, published and retired, and the next call resumes at the next unsealed
// block. Cheap when there is nothing to do: below RetryAt it opens nothing.
//
// Single-caller by contract (the cook loop), like AdvanceTail/PruneTail.
func (h *History) SealTail(ctx context.Context, chainRoot [32]byte, onEpoch func(sealedEnd uint64)) (epochs int, sealedEnd uint64, err error) {
	execHead, ok := h.store.ExecHead()
	if !ok || execHead < h.sealRetryAt {
		return 0, 0, nil
	}
	run, err := sealEpochs(ctx, h.dir, h.store.Cas(), chainRoot, func(end uint64) error {
		if _, err := h.refreshEpochs(); err != nil {
			return fmt.Errorf("publish sealed epochs: %w", err)
		}
		if err := DeleteSealedRaw(h.dir, end); err != nil {
			return err
		}
		h.dropSealedBuckets(end)
		if err := h.store.RetireBuckets(end); err != nil {
			return err
		}
		if onEpoch != nil {
			onEpoch(end)
		}
		return nil
	})
	if err != nil {
		// A FAILED PASS LEAVES THE GATE OPEN: the next cook tick tries again
		// (user ruling 2026-08-02). This used to push the gate a whole bucket
		// of blocks ahead, which on Fuji meant three failures and then
		// silence, because a node at the tip needs a day and a half to make
		// 100,000 more blocks. Nothing here can tell a permanent corpus defect
		// from a transient one, and a stall that lasts until someone reads the
		// log is the worse failure of the two.
		//
		// ponytail: a permanently unsealable corpus therefore pays a full
		// gather per cook tick. Add a backoff only if a real node is ever seen
		// doing that, and never one that outlives the condition.
		return run.Cut, run.SealedEnd, err
	}
	// A canceled pass measured no boundary, so it dates nothing: keep the
	// existing gate rather than lowering it to zero.
	if ctx.Err() == nil {
		h.sealRetryAt = run.RetryAt
	}
	return run.Cut, run.SealedEnd, nil
}

// epochTxsAt is the schedule the seal loop reads, indirected ONLY so package
// tests can cut epochs of ten txs instead of a quarter million. The production
// path has no knob.
var epochTxsAt = EpochTxsAt

// bucketBlocks is the raw-retirement granularity, indirected for the same
// reason and nothing else: a package test retires a bucket of a few blocks
// instead of 100,000. Everything that decides whether a bucket is fully sealed
// reads it (the unlink, this process's handles, the mapped sorted buckets), so
// the three stay in step. WHERE a raw record lives is still BucketBlocks, so a
// test that shrinks this retires buckets whose files hold blocks above the
// sealed end too; only the seal's own open handles keep those readable, which
// is fine for one pass and is why no test resumes across a shrunk bucket.
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

// setLatestEpoch advances the `latest` pointer: it names the newest epoch and
// nothing else, which is why it does not read the old value first. That read
// was the seal's last reachable network call (a fresh producer has no local
// copy, so it fell through to the bucket), and it was pointless: the pointer
// carries one field and this overwrites it.
func setLatestEpoch(st *dist.Store, hash string) error {
	return st.SetLatest(dist.Latest{Epoch: hash})
}

// sealedHead reads the LOCAL INDEX ALONE and answers what the next epoch needs
// to know: how many epochs are already sealed, the last block of the newest
// one, and its hash. No artifact is opened, so this costs one ReadDir and one
// small file read whether the epochs are spooled here or long since released
// to a bucket.
//
// Newest is by Start, matching EpochSet.Head over the same markers.
func sealedHead(dir string) (count int, end uint64, hash string, err error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0, 0, "", err
	}
	var headName string
	var headStart uint64
	for _, en := range entries {
		// A pre-casfs corpus (whole epoch files in the data dir) is refused by
		// name here as it is on the read path: there is no migration, and
		// sealing over one would silently re-cut epochs it cannot see.
		if strings.HasPrefix(en.Name(), "epoch_") && strings.HasSuffix(en.Name(), ".epoch") {
			return 0, 0, "", fmt.Errorf("%s: %s is a pre-casfs epoch file; epochs are now content-addressed artifacts in %s/cas (no migration: delete the corpus and resync)", dir, en.Name(), dir)
		}
		start, n, ok := ParseEpochMarkerName(en.Name())
		if !ok {
			continue
		}
		count++
		if headName == "" || start >= headStart {
			headName, headStart, end = en.Name(), start, start+n-1
		}
	}
	if headName == "" {
		return 0, 0, "", nil
	}
	hash, err = ReadMarker(dir, headName)
	return count, end, hash, err
}

// sealEpochs cuts every whole epoch the durable exec head allows and writes
// them to out, calling onEpoch after each one is durable. It NEVER deletes
// anything itself: onEpoch does that, after whatever it must do to make the
// new epoch readable (the live process publishes it first, History.SealTail).
//
// It reads the corpus through OpenReadOnly + fetch.OpenReader, which never
// truncate and never create, so it is safe beside the live writer that owns
// those files.
//
// ctx is checked BETWEEN epochs only: a gather is one indivisible unit of
// several minutes, and dropping a nearly finished one buys nothing (the next
// pass would gather it again from the same block).
func sealEpochs(ctx context.Context, dir string, out *dist.Store, chainRoot [32]byte, onEpoch func(sealedEnd uint64) error) (sealRun, error) {
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

	// THE MARKERS, NOT THE EPOCHS. Everything this needs is in the local index
	// (how many epochs exist, where the last one ends, what it hashes to), and
	// opening the epochs to learn it would read their footers, which on a node
	// whose artifacts have been uploaded and released means a HEAD and ranged
	// GETs per epoch per pass. That is how a seal could still fail on expired
	// credentials with nothing left to upload. It also stops re-opening every
	// epoch on every tick of a crunch.
	//
	// idx is the index of the epoch about to be cut, which is what picks its
	// size: sealing is strictly sequential from block 1, so it is simply how
	// many epochs are already there. prev is the hash-chain link the next
	// epoch's footer carries: the head epoch's own hash, or the chain root for
	// the very first epoch.
	idx, end, headHash, err := sealedHead(out.Dir())
	if err != nil {
		return run, err
	}
	prev := chainRoot
	next := uint64(1) // block 0 is genesis: no container, state in the alloc
	if idx > 0 {
		next = end + 1
		if prev, err = hashBytes(headHash); err != nil {
			return run, err
		}
	}
	run.SealedEnd = next - 1

	if ls, ok := store.LogsStart(); !ok || ls > next {
		return run, fmt.Errorf("seal: log capture starts at %d (ok=%v), cannot seal from %d", ls, ok, next)
	}

	for {
		if err := ctx.Err(); err != nil {
			// Cancellation lands between epochs, on purpose: everything cut so
			// far is durable, published and its raw retired, and seal resumes
			// at the next unsealed block by construction, so the next call
			// finishes the backlog exactly where this one left it.
			log.Printf("seal: stopping after %d epoch(s), sealed through %d: %v", run.Cut, run.SealedEnd, err)
			return run, nil
		}
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
		// Publish and retire this epoch before gathering the next one. The
		// unlink onEpoch does frees the disk even though this pass is still
		// reading the same dir: both readers it holds are LRU-capped (4 bucket
		// pairs per raw family, 4 staging segments) and it walks heights
		// ascending, so a retired bucket's handle goes on its own.
		if err := onEpoch(run.SealedEnd); err != nil {
			return run, err
		}
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
//
// Idempotent, and called once per sealed epoch, so it rescans buckets it has
// already emptied: only a bucket that actually lost a file is logged.
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
		removed := 0
		for _, p := range patterns {
			path := filepath.Join(dir, fmt.Sprintf(p, b))
			if err := os.Remove(path); err != nil {
				if !os.IsNotExist(err) {
					return err
				}
				continue
			}
			removed++
		}
		if removed > 0 {
			log.Printf("seal: raw bucket %05d removed (%d files, sealed through %d)", b, removed, sealedEnd)
		}
	}
	return nil
}

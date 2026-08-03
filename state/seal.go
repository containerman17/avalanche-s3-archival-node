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
// (corpora are disposable by ruling; see scanEpoch).
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
// A live serve process seals through History.SealTail, which splits the
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
	// a full scan every time.
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
func setLatestEpoch(st *dist.Store, chainRoot [32]byte, hash string) error {
	return st.SetLatest(chainRoot, dist.Latest{Epoch: hash})
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
// ctx is checked BETWEEN epochs only: cutting one epoch is an indivisible
// unit of several minutes, and dropping a nearly finished one buys nothing
// (the next pass would build it again from the same block).
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
	sweepSealScratch(out)

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
		s, full, err := scanEpoch(store, reader, next, execHead, epochTxs)
		if err != nil {
			return run, err
		}
		if !full {
			log.Printf("seal: tail %d..%d stays raw (below epoch %d's %d txs)", next, execHead, idx, epochTxs)
			run.RetryAt = retryAt(s.txs, s.count, execHead, epochTxs)
			break
		}
		t0 := time.Now()
		src := s.src()
		src.Prev = prev
		hash, err := buildEpoch(out, src)
		if err != nil {
			return run, fmt.Errorf("seal epoch at %d: %w", s.start, err)
		}
		if prev, err = hashBytes(hash); err != nil {
			return run, err
		}
		if err := setLatestEpoch(out, chainRoot, hash); err != nil {
			return run, err
		}
		st, _ := os.Stat(out.SpoolPath(hash))
		log.Printf("seal: %s (%s) blocks=%d txs=%d code=%d raw=%.1fMB sealed=%.1fMB (%.2fx) in %s",
			EpochMarkerName(s.start, s.count), hash[:12], s.count, s.txs, len(s.code),
			float64(s.raw.total())/1e6, float64(st.Size())/1e6,
			float64(s.raw.total())/float64(st.Size()), time.Since(t0).Round(time.Millisecond))
		if e, err := OpenEpoch(out, hash); err == nil {
			sz := e.SectionSizes()
			log.Printf("seal:   raw: bodies=%.2fMB headers=%.2fMB writelog=%.2fMB logs=%.2fMB rcpt=%.2fMB",
				float64(s.raw.containers)/1e6, float64(s.raw.headers)/1e6,
				float64(s.raw.writelog)/1e6, float64(s.raw.logs)/1e6,
				float64(s.raw.rcpt)/1e6)
			log.Printf("seal:   sealed: dict=%.2fMB bodies=%.2fMB(+idx %.2f) headers=%.2fMB(+idx %.2f) sst=%.2fMB(+idx %.2f) deletes=%.2fMB txidx=%.2fMB(+bloom %.2f) logidx=%.2fMB bloom=%.2fMB",
				float64(sz["dict"])/1e6,
				float64(sz["bodies"])/1e6, float64(sz["bodiesIdx"])/1e6,
				float64(sz["headers"])/1e6, float64(sz["headersIdx"])/1e6,
				float64(sz["sst"])/1e6, float64(sz["sstIdx"])/1e6,
				float64(sz["deletes"])/1e6, float64(sz["txidx"])/1e6,
				float64(sz["txbloom"])/1e6,
				float64(sz["logidx"])/1e6, float64(sz["keybloom"])/1e6)
			e.Close()
		}
		next = s.start + s.count
		idx++
		run.Cut++
		run.SealedEnd = next - 1
		// Publish and retire this epoch before scanning the next one. The
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

// retryAt extrapolates the exec head at which a tail of blocks blocks
// carrying txCount txs reaches epochTxs, from the tx rate of the tail itself.
// Being late costs only a bigger raw tail (the boundary is cut exactly where
// it always was, just on a later tick), being early costs a whole wasted
// scan, so the estimate is deliberately halved rather than doubled: a tail
// whose tx rate doubles still gets its epoch on the first tick after the
// boundary.
//
// ponytail: a rate guess, not a counter. The exact answer needs a per-block
// tx count nothing durable carries; add one only if a real chain is ever seen
// spending several scans per epoch.
func retryAt(txCount, blocks, execHead, epochTxs uint64) uint64 {
	need := epochTxs - min(txCount, epochTxs)
	ahead := need // no txs in the whole tail: no rate to extrapolate from
	if txCount > 0 && blocks > 0 {
		ahead = need * blocks / txCount / 2
	}
	return execHead + max(ahead, 1)
}

// rawSizes are the uncompressed raw equivalents consumed by one epoch, for
// the compression scoreboard.
type rawSizes struct{ containers, headers, writelog, logs, rcpt uint64 }

func (r rawSizes) total() uint64 {
	return r.containers + r.headers + r.writelog + r.logs + r.rcpt
}

// txPair is one (fingerprint, epoch-relative block) entry of the tx index.
type txPair struct{ fp, blk uint64 }

// epochStream is the sealer's BOUNDED source for one epoch. It holds only
// what is not re-derivable cheaply (the block count, the tx/block-hash
// fingerprints, the set of code hashes) and RE-READS THE RAW CORPUS once per
// section family, instead of gathering an epoch's containers, headers, write
// rows, log records and receipt records into RAM first.
//
// That gather cost a measured 2.75 KB PER BLOCK (2026-08-02, synthetic sparse
// corpus), which a sparse L1 turns into ~18 GB for the 6.7M blocks its 8M-tx
// epoch spans, and is what OOM-killed the Fuji crunch three times in 24h. The
// re-reads are sequential over files whose handles are LRU-capped, and the
// blocks are walked ascending, so a pass costs disk bandwidth and no memory.
type epochStream struct {
	store  *Store
	reader *fetch.Reader

	start, count, txs uint64
	pairs             []txPair
	code              map[common.Hash]struct{}

	raw rawSizes
	// A family is walked more than once (the builder writes sections in file
	// order, the logs dict wants its records before they are compressed), so
	// only the first pass over each one counts its bytes into raw.
	rowsPass, logsPass, storedPass int
}

// scanEpoch walks blocks ascending from start until the cumulative tx count
// reaches epochTxs (that block included), and is the ONLY pass that decodes a
// block body: it fixes the epoch's block count and collects the tx and block
// hash fingerprints, so nothing later has to keccak a transaction again.
// full=false means the boundary was not reached (the tail stays raw); the
// stream comes back anyway, because its tx-per-block rate is what dates the
// next attempt (retryAt).
func scanEpoch(store *Store, reader *fetch.Reader, start, execHead, epochTxs uint64) (*epochStream, bool, error) {
	s := &epochStream{store: store, reader: reader, start: start, code: map[common.Hash]struct{}{}}
	var hashes []common.Hash
	for n := start; n <= execHead; n++ {
		container, ok, err := reader.GetByHeight(n)
		if err != nil {
			return s, false, err
		}
		if !ok {
			return s, false, nil // staging gap below exec head should not happen, but never seal past one
		}
		headerRLP, ok, err := store.HeaderRLP(n)
		if err != nil || !ok {
			return s, false, fmt.Errorf("seal: header %d: ok=%v err=%v", n, ok, err)
		}
		s.count++
		s.raw.containers += uint64(len(container))
		s.raw.headers += uint64(len(headerRLP))
		s.pairs = append(s.pairs, txPair{fp: txFingerprint(BlockHashFromHeaderRLP(headerRLP)), blk: n - start})

		hashes, err = extractTxHashes(innerEthBlock(container), hashes[:0])
		if err != nil {
			return s, false, fmt.Errorf("seal: block %d txs: %w", n, err)
		}
		for _, h := range hashes {
			s.pairs = append(s.pairs, txPair{fp: txFingerprint(h), blk: n - start})
		}
		s.txs += uint64(len(hashes))

		// THE NO-RECORDS RULE: the stored sections are a byte copy of the
		// executor's live capture. A tx-bearing block without one cannot be
		// sealed, and there is deliberately no backfill-by-re-execution
		// crutch: corpora are disposable (DESIGN.md work order item 2), so
		// the answer to a pre-capture corpus is a fresh sync, not a patch.
		// Checked here against the index alone: the records themselves are
		// read by the stored sections, which is the only pass that needs them.
		if len(hashes) > 0 && !store.rc.Has(n) {
			return s, false, fmt.Errorf(
				"seal: block %d has %d txs but no captured receipts record: this corpus was executed before live receipt capture, resync it (there is no backfill)",
				n, len(hashes))
		}

		if s.txs >= epochTxs {
			return s, true, nil
		}
	}
	return s, false, nil // ran out of replayed blocks before the boundary
}

// src is the builder's pull side over this stream: one walk per section
// family, each re-reading the raw files ascending.
func (s *epochStream) src() *epochSrc {
	end := s.start + s.count
	container := func(n uint64) ([]byte, error) {
		c, ok, err := s.reader.GetByHeight(n)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf("seal: container %d vanished mid-seal", n)
		}
		return c, nil
	}
	return &epochSrc{
		Start:   s.start,
		Count:   s.count,
		TxCount: s.txs,
		Container: func(i uint64) ([]byte, error) {
			return container(s.start + i)
		},
		Containers: func(yield func([]byte) error) error {
			for n := s.start; n < end; n++ {
				c, err := container(n)
				if err != nil {
					return err
				}
				if err := yield(c); err != nil {
					return err
				}
			}
			return nil
		},
		Headers: func(yield func([]byte) error) error {
			for n := s.start; n < end; n++ {
				hdr, ok, err := s.store.HeaderRLP(n)
				if err != nil || !ok {
					return fmt.Errorf("seal: header %d: ok=%v err=%v", n, ok, err)
				}
				if err := yield(hdr); err != nil {
					return err
				}
			}
			return nil
		},
		Rows: s.rows,
		Code: func() ([]common.Hash, func(common.Hash) ([]byte, error), error) {
			hashes := make([]common.Hash, 0, len(s.code))
			for h := range s.code {
				hashes = append(hashes, h)
			}
			return hashes, s.codeBlob, nil
		},
		Logs:   s.logs,
		Stored: s.stored,
		TxPairs: func(yield func(fp, blk uint64) error) error {
			for _, p := range s.pairs {
				if err := yield(p.fp, p.blk); err != nil {
					return err
				}
			}
			return nil
		},
	}
}

// rows walks the write-capture frames ascending and yields their post-image
// rows, collecting on the way the code hashes this epoch's account rows
// reference (the v3 placement rule, EpochInput.Code). Values are views into
// the frame, which the builder copies into its sort at once.
func (s *epochStream) rows(yield func(StateRow) error) error {
	first := s.rowsPass == 0
	s.rowsPass++
	for n := s.start; n < s.start+s.count; n++ {
		frame, ok, err := s.store.wl.Get(n)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		if first {
			s.raw.writelog += uint64(len(frame))
		}
		seq := 0
		var yerr error
		if err := parseFrame(frame, func(kind byte, key [sortedKeySize]byte, valOff int, vlen uint32) {
			if kind == recKindCodeUse || yerr != nil {
				return
			}
			val := frame[valOff : valOff+int(vlen)]
			if kind == recKindAccount && vlen > 0 {
				if h, ok := accountCodeHash(val); ok && h != types.EmptyCodeHash && h != (common.Hash{}) {
					s.code[h] = struct{}{}
				}
			}
			yerr = yield(StateRow{Key: key, Block: n, Value: val, Seq: seq})
			seq++
		}); err != nil {
			return fmt.Errorf("seal: writelog frame %d: %w", n, err)
		}
		if yerr != nil {
			return yerr
		}
	}
	return nil
}

// codeBlob resolves one code blob out of code.log. A referenced blob that is
// not there is a corpus defect, not a hole to paper over.
func (s *epochStream) codeBlob(h common.Hash) ([]byte, error) {
	blob, ok, err := s.store.code.Get(h)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("seal epoch at %d: code %x referenced by an account row is not in code.log", s.start, h)
	}
	return blob, nil
}

// logs walks the per-block log tuple records (the logidx input).
func (s *epochStream) logs(yield func(LogRec) error) error {
	first := s.logsPass == 0
	s.logsPass++
	for n := s.start; n < s.start+s.count; n++ {
		rec, ok, err := s.store.LogsRecord(n)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		if first {
			s.raw.logs += uint64(len(rec))
		}
		lr, err := decodeLogRec(n, rec)
		if err != nil {
			return fmt.Errorf("seal: logs record %d: %w", n, err)
		}
		if err := yield(lr); err != nil {
			return err
		}
	}
	return nil
}

// stored walks the live capture's receipt records and splits each into the
// two epoch-encoded halves it already holds (state/storedlogs.go). No
// re-execution, here or anywhere else in a seal.
func (s *epochStream) stored(yield func(uint64, []byte, []byte) error) error {
	first := s.storedPass == 0
	s.storedPass++
	for n := s.start; n < s.start+s.count; n++ {
		raw, ok, err := s.store.RcptRecord(n)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		if first {
			s.raw.rcpt += uint64(len(raw))
		}
		logsRec, rcptRec, err := DecodeTailRcpt(raw)
		if err != nil {
			return fmt.Errorf("seal: block %d: %w", n, err)
		}
		if err := yield(n, logsRec, rcptRec); err != nil {
			return err
		}
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

// sweepSealScratch drops what a KILLED build left behind. The external sorts
// spill into a scratch directory in the data dir, and the artifact itself is
// built as a file in the spool; a finished build removes both, an OOM kill
// (three of those on this corpus in 24h) removes neither, and a multi-GB stray
// per kill is exactly what a crunch cannot afford.
//
// The data dir belongs to this seal alone (single-caller by contract), so its
// scratch goes unconditionally. The spool is this chain's alone since 2026-08-04, but a
// half-written artifact still goes only once it is a day old and therefore
// cannot belong to a live build (a second tool over the same dir) (an epoch takes ~2h at the worst
// size the schedule allows).
func sweepSealScratch(out *dist.Store) {
	dirs, _ := filepath.Glob(filepath.Join(out.Dir(), sealTmpPrefix+"*"))
	for _, d := range dirs {
		if err := os.RemoveAll(d); err == nil {
			log.Printf("seal: removed stale scratch %s", d)
		}
	}
	files, _ := filepath.Glob(filepath.Join(out.SpoolDir(), "epoch-*.tmp"))
	for _, f := range files {
		st, err := os.Stat(f)
		if err != nil || time.Since(st.ModTime()) < 24*time.Hour {
			continue
		}
		if err := os.Remove(f); err == nil {
			log.Printf("seal: removed abandoned epoch build %s", f)
		}
	}
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

package state

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/types"
	"github.com/containerman17/epochdb/dist"
	"github.com/containerman17/epochdb/fetch"
)

// THE BYTE-IDENTITY PROOF for the bounded sealer (2026-08-02). The whole-epoch
// gather below is the OLD seal path, kept here verbatim as the reference: it
// loads an epoch's containers, headers, write rows, log records and receipt
// records into RAM and hands them to the builder as one EpochInput. The
// streaming sealer must produce THE SAME EPOCH HASHES from the same corpus,
// which is what TestSealStreamingMatchesGather asserts, epoch by epoch,
// including the hash chain that links them.
//
// It is deliberately not shared with the production path: nothing but this
// test may hold an epoch in RAM again.

// gatherEpoch collects blocks from start until the cumulative tx count
// reaches epochTxs (that block included). full=false means the boundary was
// not reached (the tail stays raw); the input is returned anyway, holding
// everything scanned, because its tx-per-block rate is what dates the next
// attempt (retryAt).
func gatherEpochRAM(store *Store, reader *fetch.Reader, start, execHead, epochTxs uint64) (*EpochInput, bool, rawSizes, error) {
	in := &EpochInput{
		Start:    start,
		TxHashes: map[uint64][][32]byte{},
		FullLogs: map[uint64][]byte{},
		RcptRecs: map[uint64][]byte{},
		Frames:   map[uint64][]byte{},
		Parts:    map[uint64][]byte{},
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

		// Same rule, and same gather, for the V2 call-frames family.
		itxRaw, hasItx, err := store.ItxRecord(n)
		if err != nil {
			return in, false, rawSizes{}, err
		}
		if len(hashes) > 0 && !hasItx {
			return in, false, rawSizes{}, fmt.Errorf(
				"seal: block %d has %d txs but no captured call-frames record: this corpus was executed before frame capture, resync it (there is no backfill)",
				n, len(hashes))
		}
		if hasItx {
			framesRec, partsRec, err := DecodeTailItx(itxRaw)
			if err != nil {
				return in, false, rawSizes{}, fmt.Errorf("seal: block %d: %w", n, err)
			}
			rawBytes.itx += uint64(len(itxRaw))
			if len(framesRec) > 0 {
				in.Frames[n] = framesRec
			}
			in.Parts[n] = partsRec
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
			return in, true, rawBytes, fillEpochCodeRAM(in, store)
		}
	}
	return in, false, rawSizes{}, nil // ran out of replayed blocks before the boundary
}

// fillEpochCode resolves the code of every account row this epoch writes,
// pulled from code.log. This loop IS the v3 placement rule (see
// EpochInput.Code).
func fillEpochCodeRAM(in *EpochInput, store *Store) error {
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

// sealEpochsRAM is the old seal loop: gather a whole epoch, build it, chain
// the next one onto its hash. Same boundaries and same order as sealEpochs,
// with none of the retirement (the corpus stays intact for the second run).
func sealEpochsRAM(t *testing.T, dir string, out *dist.Store, chainRoot [32]byte) []string {
	t.Helper()
	store, err := OpenReadOnly(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	reader, err := fetch.OpenReader(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	execHead, _ := store.ExecHead()

	var hashes []string
	prev, next, idx := chainRoot, uint64(1), 0
	for {
		in, full, _, err := gatherEpochRAM(store, reader, next, execHead, epochTxsAt(idx))
		if err != nil {
			t.Fatal(err)
		}
		if !full {
			return hashes
		}
		in.Prev = prev
		hash, err := BuildEpoch(out, in)
		if err != nil {
			t.Fatal(err)
		}
		if prev, err = hashBytes(hash); err != nil {
			t.Fatal(err)
		}
		hashes = append(hashes, hash)
		next = in.Start + uint64(len(in.Containers))
		idx++
	}
}

// TestSealStreamingMatchesGather seals one synthetic sparse corpus twice, once
// through the old whole-epoch gather and once through the streaming sealer,
// and requires the epoch hashes to match exactly. Byte-identical output is the
// hard constraint on the bounded seal: data-fuji's epoch 12 has to chain onto
// epoch 11 exactly as the old sealer would have cut it.
func TestSealStreamingMatchesGather(t *testing.T) {
	dir := t.TempDir()
	writeSparseCorpus(t, dir, 2600)
	fixedEpochTxs(t, 1000) // 1.2 txs/block: epochs of ~834 blocks

	ram := testStore(t, t.TempDir())
	want := sealEpochsRAM(t, dir, ram, [32]byte{9})
	if len(want) < 2 {
		t.Fatalf("reference cut %d epochs, want at least 2 (the hash chain has to be exercised)", len(want))
	}

	stream := testStore(t, t.TempDir())
	run, err := sealEpochs(context.Background(), dir, stream, [32]byte{9}, func(uint64) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if run.Cut != len(want) {
		t.Fatalf("streaming cut %d epochs, reference cut %d", run.Cut, len(want))
	}
	set, err := OpenEpochSet(stream)
	if err != nil {
		t.Fatal(err)
	}
	defer set.Close()
	got := set.All()
	if len(got) != len(want) {
		t.Fatalf("streaming wrote %d epochs, want %d", len(got), len(want))
	}
	for i, e := range got {
		if e.Hash != want[i] {
			t.Fatalf("epoch %d: streaming %s, whole-epoch gather %s", i, e.Hash, want[i])
		}
	}

	// ...and no scratch survives a finished seal, in either directory.
	for _, d := range []string{dir, stream.Dir()} {
		ents, err := os.ReadDir(d)
		if err != nil {
			t.Fatal(err)
		}
		for _, en := range ents {
			if len(en.Name()) >= len(sealTmpPrefix) && en.Name()[:len(sealTmpPrefix)] == sealTmpPrefix {
				t.Fatalf("%s: seal scratch %s left behind", d, en.Name())
			}
		}
	}
}

// TestSealProgressSaysWhatItIsDoingAndCannotReachTheBytes covers the whole of
// the 2026-08-06 observability fix in one corpus: a seal announces itself, a
// seal announces what it FINISHED, it reports periodically in between, and
// none of that changes a byte of the artifact. The last one is the constraint
// that matters: the same corpus is sealed twice, once at the production
// cadence (where no phase line fires at all) and once at 1ms (where every
// phase hook fires repeatedly), and the epoch hashes must be identical.
func TestSealProgressSaysWhatItIsDoingAndCannotReachTheBytes(t *testing.T) {
	dir := t.TempDir()
	writeSparseCorpus(t, dir, 2600)
	fixedEpochTxs(t, 1000) // 1.2 txs/block: epochs of ~834 blocks

	seal := func(t *testing.T) []string {
		t.Helper()
		out := testStore(t, t.TempDir())
		if _, err := sealEpochs(context.Background(), dir, out, [32]byte{9}, func(uint64) error { return nil }); err != nil {
			t.Fatal(err)
		}
		set, err := OpenEpochSet(out)
		if err != nil {
			t.Fatal(err)
		}
		defer set.Close()
		var hashes []string
		for _, e := range set.All() {
			hashes = append(hashes, e.Hash)
		}
		return hashes
	}

	quiet := seal(t)
	if len(quiet) < 2 {
		t.Fatalf("sealed %d epochs, want at least 2", len(quiet))
	}

	// Every phase hook fires many times per epoch at this cadence. Capturing
	// the log is safe because LogProgress's stop waits for its goroutine, so
	// nothing is still writing once sealEpochs has returned.
	old := progressEvery
	progressEvery = time.Millisecond
	t.Cleanup(func() { progressEvery = old })
	var logged bytes.Buffer
	oldOut, oldFlags := log.Writer(), log.Flags()
	log.SetOutput(&logged)
	log.SetFlags(0)
	t.Cleanup(func() { log.SetOutput(oldOut); log.SetFlags(oldFlags) })

	loud := seal(t)

	log.SetOutput(oldOut)
	log.SetFlags(oldFlags)
	lines := strings.Split(logged.String(), "\n")

	if len(loud) != len(quiet) {
		t.Fatalf("loud seal cut %d epochs, quiet seal cut %d", len(loud), len(quiet))
	}
	for i := range quiet {
		if loud[i] != quiet[i] {
			t.Fatalf("epoch %d: hash %s with progress logging, %s without: LOGGING CHANGED THE ARTIFACT", i, loud[i], quiet[i])
		}
	}

	has := func(sub string) bool {
		for _, l := range lines {
			if strings.Contains(l, sub) {
				return true
			}
		}
		return false
	}
	// The start of a seal: the scan, then the build with its exact range.
	for _, want := range []string{
		"seal: epoch 0: scanning from block 1",
		"seal: epoch 0: building blocks 1..",
		"blocks=", "txs=", // the completion line, unchanged
	} {
		if !has(want) {
			t.Errorf("no log line contains %q", want)
		}
	}
	// ...and the periodic phase lines in between. Not every phase of a 2600
	// block corpus outlasts a 1ms tick, but the scan of the first epoch does.
	phases := map[string]int{}
	example := map[string]string{}
	for _, l := range lines {
		if _, rest, ok := strings.Cut(l, "seal: "); ok {
			if name, _, ok := strings.Cut(rest, ": "); ok {
				phases[name]++
				if _, seen := example[name]; !seen {
					example[name] = l
				}
			}
		}
	}
	if phases["scan"] == 0 {
		t.Errorf("no periodic scan progress line in %d log lines: %q", len(lines), logged.String())
	}
	for name, n := range phases {
		t.Logf("%s x%d: %s", name, n, example[name])
	}
}

// TestSealRefusesWithoutDictTrainer pins the OTHER half of byte-identity: not
// that two builders agree, but that a builder which CANNOT agree refuses to
// publish. A missing zstd used to log one line and seal dict-less, which is
// how the published distroless image shipped ~24% larger, differently-hashed
// epochs of the same mainnet blocks for weeks. The seal must fail instead.
func TestSealRefusesWithoutDictTrainer(t *testing.T) {
	t.Setenv("PATH", t.TempDir()) // an empty dir: no zstd anywhere
	if err := CheckDictTrainer(); err == nil {
		t.Fatal("CheckDictTrainer accepted a PATH with no zstd on it")
	}
	dir := t.TempDir()
	st := testStore(t, dir)
	in := &EpochInput{Start: 1, TxHashes: map[uint64][][32]byte{}}
	for i := 0; i < 64; i++ { // well over dictMinSamples
		in.Containers = append(in.Containers, []byte(fmt.Sprintf("container-%d-aaaaaaaaaaaaaaaa", i)))
		in.Headers = append(in.Headers, []byte(fmt.Sprintf("header-%d-bbbbbbbbbbbbbbbb", i)))
	}
	if _, err := BuildEpoch(st, in); err == nil {
		t.Fatal("BuildEpoch sealed an epoch with no dictionary trainer available")
	} else if !strings.Contains(err.Error(), "zstd") {
		t.Fatalf("seal failed for the wrong reason: %v", err)
	}
}

// TestSealedEpochCarriesADict pins the dict section's PRESENCE. Every other
// section would still be there in a dict-less epoch, and every read would
// still work, which is exactly why the divergence went unnoticed.
func TestSealedEpochCarriesADict(t *testing.T) {
	dir := t.TempDir()
	st := testStore(t, dir)
	_, hash := synthEpoch(t, st, 1)
	e, err := OpenEpoch(st, hash)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if n := e.SectionSizes()["dict"]; n == 0 {
		t.Fatal("sealed epoch has an empty dict section (dict-less seal)")
	}
}

// TestDictTrainerRequiresTheExactPinnedVersion. A zstd of the wrong version
// diverges exactly as silently as no zstd at all: it trains a DIFFERENT
// dictionary from the same samples, so the epoch gets a different hash while
// every read still works. An exact pin is a hard system requirement (user
// ruling 2026-08-05), so the check refuses anything but ZstdPinnedVersion and
// says which version it found.
func TestDictTrainerRequiresTheExactPinnedVersion(t *testing.T) {
	fakeZstd := func(t *testing.T, banner string) {
		t.Helper()
		dir := t.TempDir()
		script := "#!/bin/sh\necho '" + banner + "'\n"
		if err := os.WriteFile(dir+"/zstd", []byte(script), 0o755); err != nil {
			t.Fatal(err)
		}
		t.Setenv("PATH", dir)
	}
	t.Run("wrong version", func(t *testing.T) {
		fakeZstd(t, "*** Zstandard CLI (64-bit) v1.5.4, by Yann Collet ***")
		err := CheckDictTrainer()
		if err == nil {
			t.Fatal("CheckDictTrainer accepted zstd v1.5.4")
		}
		for _, want := range []string{"1.5.4", ZstdPinnedVersion} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("error does not name %q: %v", want, err)
			}
		}
	})
	t.Run("no version in banner", func(t *testing.T) {
		fakeZstd(t, "not a zstd at all")
		if err := CheckDictTrainer(); err == nil {
			t.Fatal("CheckDictTrainer accepted a binary that printed no version")
		}
	})
	t.Run("pinned version", func(t *testing.T) {
		fakeZstd(t, "*** Zstandard CLI (64-bit) v"+ZstdPinnedVersion+", by Yann Collet ***")
		if err := CheckDictTrainer(); err != nil {
			t.Fatalf("CheckDictTrainer rejected the pinned version: %v", err)
		}
	})
}

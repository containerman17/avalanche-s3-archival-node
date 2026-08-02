package state

import (
	"context"
	"fmt"
	"os"
	"testing"

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

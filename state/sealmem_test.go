package state

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/crypto"
	"github.com/ava-labs/libevm/rlp"
	"github.com/containerman17/epochdb/dist"
	"github.com/containerman17/epochdb/fetch"
	"github.com/klauspost/compress/zstd"
)

// writeSparseCorpus writes a SPARSE synthetic chain (the L1 shape that OOMs
// the sealer: millions of tiny blocks carrying ~1.2 txs each) into dir:
// staging containers + headers + write frames + receipt/log capture, exactly
// as fetch and exec would leave them. Deterministic, so two runs over the
// same block count produce byte-identical epochs.
func writeSparseCorpus(t *testing.T, dir string, blocks uint64) {
	t.Helper()
	st, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	enc, err := zstd.NewWriter(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer enc.Close()
	to := common.HexToAddress("0xbeef")
	extra := bytes.Repeat([]byte("extradata"), 44) // ~400B header extra

	var (
		arr, idxF *os.File
		bucket    = ^uint64(0)
		off       uint64
	)
	closeBucket := func() {
		if arr != nil {
			arr.Close()
			idxF.Close()
		}
	}
	defer closeBucket()

	for n := uint64(1); n <= blocks; n++ {
		if b := n / BucketBlocks; b != bucket {
			closeBucket()
			bucket = b
			if arr, err = os.Create(filepath.Join(dir, fmt.Sprintf("arrival_%05d.log", bucket))); err != nil {
				t.Fatal(err)
			}
			if idxF, err = os.Create(filepath.Join(dir, fmt.Sprintf("index_%05d.log", bucket))); err != nil {
				t.Fatal(err)
			}
			off = 0
		}
		nTx := 1
		if n%5 == 0 {
			nTx = 2 // 1.2 txs/block, the measured sparse-L1 density
		}
		txs := make([]*types.Transaction, nTx)
		for i := range txs {
			txs[i] = types.NewTx(&types.LegacyTx{
				Nonce: n*100 + uint64(i), GasPrice: big.NewInt(1), Gas: 21000,
				To: &to, Value: big.NewInt(int64(i)),
			})
		}
		header := &types.Header{Number: new(big.Int).SetUint64(n), Extra: extra}
		blockRLP, err := rlp.EncodeToBytes([]any{header, txs, []*types.Header{}})
		if err != nil {
			t.Fatal(err)
		}
		frame := enc.EncodeAll(blockRLP, nil)
		if _, err := arr.Write(frame); err != nil {
			t.Fatal(err)
		}
		var rec [stagingRecSize]byte
		binary.BigEndian.PutUint64(rec[0:8], n)
		copy(rec[40:72], header.Hash().Bytes())
		binary.BigEndian.PutUint64(rec[72:80], off)
		binary.BigEndian.PutUint32(rec[80:84], uint32(len(frame)))
		if _, err := idxF.Write(rec[:]); err != nil {
			t.Fatal(err)
		}
		off += uint64(len(frame))

		hdrRLP, err := rlp.EncodeToBytes(header)
		if err != nil {
			t.Fatal(err)
		}
		if err := st.AppendHeader(n, hdrRLP); err != nil {
			t.Fatal(err)
		}

		// An account row every 100th block, carrying code (the v3 placement
		// rule) and deleted again every 500th, so the SST, the code cursor and
		// the deletes section all see real input.
		var frameW []byte
		if n%100 == 0 {
			blob := append([]byte("code"), byte(n), byte(n>>8), byte(n>>16))
			codeHash := crypto.Keccak256Hash(blob)
			if err := st.PutCode(codeHash, blob); err != nil {
				t.Fatal(err)
			}
			acct, err := rlp.EncodeToBytes([]any{
				uint64(n), big.NewInt(int64(n)), types.EmptyRootHash, codeHash,
			})
			if err != nil {
				t.Fatal(err)
			}
			if n%500 == 0 {
				acct = nil // account delete row
			}
			var key [sortedKeySize]byte
			key[0] = recKindAccount
			binary.BigEndian.PutUint64(key[1:9], n%977)
			frameW = append(frameW, key[0])
			frameW = append(frameW, key[1:21]...) // account records carry addr only
			frameW = binary.AppendUvarint(frameW, uint64(len(acct)))
			frameW = append(frameW, acct...)
		}
		// two storage writes per block, keys spread over the whole space so
		// the SST sort is a real sort.
		for j := 0; j < 2; j++ {
			var key [sortedKeySize]byte
			key[0] = recKindStorage
			binary.BigEndian.PutUint64(key[1:9], n*2654435761+uint64(j))
			binary.BigEndian.PutUint64(key[21:29], n)
			frameW = append(frameW, key[0])
			frameW = append(frameW, key[1:53]...)
			frameW = binary.AppendUvarint(frameW, 32)
			var val [32]byte
			binary.BigEndian.PutUint64(val[24:], n+uint64(j))
			frameW = append(frameW, val[:]...)
		}
		if err := st.AppendWrites(n, frameW); err != nil {
			t.Fatal(err)
		}

		var rr []byte
		for i := 0; i < nTx; i++ {
			rr = binary.AppendUvarint(rr, 21000+n)
			rr = append(rr, 1)
		}
		var storedLogs []byte
		if n%4 == 0 { // one event log on a quarter of the blocks
			storedLogs = binary.AppendUvarint(nil, 1)
			var a [20]byte
			binary.BigEndian.PutUint64(a[:8], n%1024)
			storedLogs = append(storedLogs, a[:]...)
			storedLogs = binary.AppendUvarint(storedLogs, 1)
			var tp [32]byte
			binary.BigEndian.PutUint64(tp[:8], n%64)
			storedLogs = append(storedLogs, tp[:]...)
			storedLogs = binary.AppendUvarint(storedLogs, 32)
			storedLogs = append(storedLogs, tp[:]...)
			storedLogs = binary.AppendUvarint(storedLogs, 0)

			lrec := binary.AppendUvarint(nil, 1)
			lrec = append(lrec, a[:]...)
			lrec = binary.AppendUvarint(lrec, 1)
			lrec = append(lrec, tp[:]...)
			if err := st.AppendLogs(n, lrec); err != nil {
				t.Fatal(err)
			}
		}
		if err := st.AppendRcpt(n, EncodeTailRcpt(storedLogs, rr)); err != nil {
			t.Fatal(err)
		}
		if rec := synthItx(nTx, n); rec != nil {
			if err := st.AppendItx(n, rec); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := st.SetLogsStart(1); err != nil {
		t.Fatal(err)
	}
	if err := st.FlushAndSetExecHead(blocks); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
}

// peakRSS is the process high-water mark in bytes (VmHWM).
func peakRSS(t *testing.T) uint64 {
	t.Helper()
	b, err := os.ReadFile("/proc/self/status")
	if err != nil {
		t.Skip("no /proc")
	}
	for _, line := range bytes.Split(b, []byte("\n")) {
		if !bytes.HasPrefix(line, []byte("VmHWM:")) {
			continue
		}
		f := bytes.Fields(line)
		kb, err := strconv.ParseUint(string(f[1]), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		return kb * 1024
	}
	t.Fatal("no VmHWM")
	return 0
}

// resetPeakRSS clears the kernel's high-water mark so the number the seal
// reports is the seal's own, not the corpus generator's.
func resetPeakRSS(t *testing.T) {
	t.Helper()
	if err := os.WriteFile("/proc/self/clear_refs", []byte("5\n"), 0o200); err != nil {
		t.Logf("clear_refs: %v (peak includes corpus generation)", err)
	}
}

// TestSealSparsePeakRSS is the memory harness, off by default (it wants
// millions of blocks and minutes of wall time):
//
//	EPOCHDB_SEALMEM_BLOCKS=2000000 EPOCHDB_SEALMEM_DIR=/some/scratch/corpus \
//	  go test ./state -run TestSealSparsePeakRSS -v -timeout 3h
//
// The corpus dir is reused when it already holds one, and never sealed in
// place: the epoch goes to a throwaway out dir, so the same corpus measures
// the old and the new sealer.
func TestSealSparsePeakRSS(t *testing.T) {
	blocks, _ := strconv.ParseUint(os.Getenv("EPOCHDB_SEALMEM_BLOCKS"), 10, 64)
	if blocks == 0 {
		t.Skip("set EPOCHDB_SEALMEM_BLOCKS to run the seal memory harness")
	}
	dir := os.Getenv("EPOCHDB_SEALMEM_DIR")
	if dir == "" {
		dir = t.TempDir()
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, execHeadFile)); err != nil {
		t0 := time.Now()
		writeSparseCorpus(t, dir, blocks)
		t.Logf("corpus: %d blocks in %s", blocks, time.Since(t0).Round(time.Second))
	}

	// one epoch over the whole corpus: 1.2 txs/block
	fixedEpochTxs(t, blocks*6/5)
	out := testStore(t, t.TempDir())

	resetPeakRSS(t)
	t0 := time.Now()
	run, err := sealEpochs(context.Background(), dir, out, [32]byte{}, func(uint64) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	peak := peakRSS(t)
	_, _, hash, err := sealedHead(out.Dir())
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("SEALED %d epoch(s) through %d in %s, head=%s, PEAK RSS %.2f GB (%d B/block)",
		run.Cut, run.SealedEnd, time.Since(t0).Round(time.Second), hash,
		float64(peak)/1e9, peak/blocks)
	if run.Cut != 1 {
		t.Fatalf("cut %d epochs, want 1", run.Cut)
	}
	_ = dist.Latest{}
}

// TestSealSparseStages splits the peak of the OLD whole-epoch path between
// its gather (input side) and its build (output side): it is what attributed
// the 2.75 KB per block. Same env vars as TestSealSparsePeakRSS.
func TestSealSparseStages(t *testing.T) {
	blocks, _ := strconv.ParseUint(os.Getenv("EPOCHDB_SEALMEM_BLOCKS"), 10, 64)
	if blocks == 0 {
		t.Skip("set EPOCHDB_SEALMEM_BLOCKS")
	}
	dir := os.Getenv("EPOCHDB_SEALMEM_DIR")
	if _, err := os.Stat(filepath.Join(dir, execHeadFile)); err != nil {
		writeSparseCorpus(t, dir, blocks)
	}
	live := func(tag string) {
		var m runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&m)
		t.Logf("%-22s live heap %7.2f GB   sys %7.2f GB   peak RSS %7.2f GB",
			tag, float64(m.HeapAlloc)/1e9, float64(m.Sys)/1e9, float64(peakRSS(t))/1e9)
	}
	resetPeakRSS(t)
	live("start")
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
	live("stores open")

	in, full, _, err := gatherEpochRAM(store, reader, 1, blocks, blocks*6/5)
	if err != nil || !full {
		t.Fatalf("gather: full=%v err=%v", full, err)
	}
	live("after gather")
	t.Logf("  containers=%d headers=%d rows=%d txhashes=%d logs=%d fulllogs=%d rcpt=%d code=%d",
		len(in.Containers), len(in.Headers), len(in.StateRows), len(in.TxHashes),
		len(in.Logs), len(in.FullLogs), len(in.RcptRecs), len(in.Code))
	out := testStore(t, t.TempDir())
	hash, err := BuildEpoch(out, in)
	if err != nil {
		t.Fatal(err)
	}
	live("after build")
	t.Logf("epoch %s", hash)
}

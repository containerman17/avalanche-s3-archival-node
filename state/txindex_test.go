package state

import (
	"encoding/binary"
	"fmt"
	"math/big"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/rlp"
	"github.com/klauspost/compress/zstd"
)

func TestEFRoundtripAndDuplicates(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	vals := make([]uint64, 50_000)
	for i := range vals {
		vals[i] = rng.Uint64() & (1<<fpBits - 1)
	}
	// inject duplicate runs
	vals[100], vals[101], vals[102] = 7777777, 7777777, 7777777
	sort.Slice(vals, func(i, j int) bool { return vals[i] < vals[j] })

	e := buildEF(vals, 1<<fpBits)
	for i, v := range vals {
		if got := e.get(i); got != v {
			t.Fatalf("get(%d)=%d want %d", i, got, v)
		}
	}
	// every value must be found and cover its full duplicate run
	for i, v := range vals {
		lo, hi := e.lookup(v)
		if i < lo || i >= hi {
			t.Fatalf("lookup(%d) = [%d,%d) does not cover index %d", v, lo, hi, i)
		}
		for j := lo; j < hi; j++ {
			if vals[j] != v {
				t.Fatalf("lookup(%d) range [%d,%d) includes vals[%d]=%d", v, lo, hi, j, vals[j])
			}
		}
	}
	// duplicate run must come back as a multi-entry candidate range
	lo, hi := e.lookup(7777777)
	if hi-lo != 3 {
		t.Fatalf("duplicate fp: got range [%d,%d), want width 3", lo, hi)
	}
	// absent values must miss (probe random non-members)
	member := map[uint64]bool{}
	for _, v := range vals {
		member[v] = true
	}
	misses := 0
	for i := 0; i < 100_000; i++ {
		v := rng.Uint64() & (1<<fpBits - 1)
		if member[v] {
			continue
		}
		if lo, hi := e.lookup(v); lo != hi {
			misses++
		}
	}
	if misses != 0 {
		t.Fatalf("%d phantom matches on absent values", misses)
	}
	// empty EF must not blow up
	if lo, hi := buildEF(nil, 1<<fpBits).lookup(42); lo != hi {
		t.Fatal("empty EF matched something")
	}
}

// writeStagingBlock appends one synthetic eth block to a fake staging
// bucket (zstd frame + 84B sidecar record), returning its txs' hashes.
func writeStagingBlock(t *testing.T, dir string, bucket, height uint64, nTxs int) []common.Hash {
	t.Helper()
	to := common.HexToAddress("0xbeef")
	var txs []*types.Transaction
	for i := 0; i < nTxs; i++ {
		txs = append(txs, types.NewTx(&types.LegacyTx{
			Nonce:    height*100 + uint64(i),
			GasPrice: big.NewInt(1),
			Gas:      21000,
			To:       &to,
			Value:    big.NewInt(int64(i)),
		}))
	}
	blockRLP, err := rlp.EncodeToBytes([]any{
		&types.Header{Number: new(big.Int).SetUint64(height)},
		txs,
		[]*types.Header{},
	})
	if err != nil {
		t.Fatal(err)
	}
	enc, _ := zstd.NewWriter(nil)
	frame := enc.EncodeAll(blockRLP, nil)

	arrPath := filepath.Join(dir, fmt.Sprintf("arrival_%05d.log", bucket))
	arr, err := os.OpenFile(arrPath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	st, _ := arr.Stat()
	off := uint64(st.Size())
	if _, err := arr.Write(frame); err != nil {
		t.Fatal(err)
	}
	arr.Close()

	var rec [stagingRecSize]byte
	binary.BigEndian.PutUint64(rec[0:8], height)
	binary.BigEndian.PutUint64(rec[72:80], off)
	binary.BigEndian.PutUint32(rec[80:84], uint32(len(frame)))
	idxPath := filepath.Join(dir, fmt.Sprintf("index_%05d.log", bucket))
	idxF, err := os.OpenFile(idxPath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := idxF.Write(rec[:]); err != nil {
		t.Fatal(err)
	}
	idxF.Close()

	var out []common.Hash
	for _, tx := range txs {
		out = append(out, tx.Hash())
	}
	return out
}

func TestCookTxIndexAndSkip(t *testing.T) {
	dir := t.TempDir()
	h1 := writeStagingBlock(t, dir, 0, 5, 3)
	h2 := writeStagingBlock(t, dir, 1, 150_000, 2)

	if err := CookTxIndex(dir); err != nil {
		t.Fatal(err)
	}
	idx, err := OpenTxIndex(dir)
	if err != nil {
		t.Fatal(err)
	}
	if idx.NumTx() != 5 {
		t.Fatalf("NumTx=%d want 5", idx.NumTx())
	}
	wantHit := func(h common.Hash, height uint64) {
		t.Helper()
		cands := idx.Candidates(h)
		for _, c := range cands {
			if c == height {
				return
			}
		}
		t.Fatalf("hash %x: candidates %v missing height %d", h, cands, height)
	}
	for _, h := range h1 {
		wantHit(h, 5)
	}
	for _, h := range h2 {
		wantHit(h, 150_000)
	}

	// Skip logic: unchanged bucket must not be rewritten.
	before, _ := os.Stat(filepath.Join(dir, txidxName(0)))
	if err := CookTxIndex(dir); err != nil {
		t.Fatal(err)
	}
	after, _ := os.Stat(filepath.Join(dir, txidxName(0)))
	if !after.ModTime().Equal(before.ModTime()) {
		t.Fatal("unchanged bucket was rewritten")
	}

	// A grown bucket must re-cook and index the new txs.
	h3 := writeStagingBlock(t, dir, 0, 9, 4)
	if err := CookTxIndex(dir); err != nil {
		t.Fatal(err)
	}
	idx2, err := OpenTxIndex(dir)
	if err != nil {
		t.Fatal(err)
	}
	if idx2.NumTx() != 9 {
		t.Fatalf("after grow NumTx=%d want 9", idx2.NumTx())
	}
	for _, h := range h3 {
		cands := idx2.Candidates(h)
		found := false
		for _, c := range cands {
			found = found || c == 9
		}
		if !found {
			t.Fatalf("new tx not indexed after re-cook: %v", cands)
		}
	}
}

package store

import (
	"bytes"
	"fmt"
	"math/big"
	"math/rand"
	"os"
	"testing"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/common/lru"
	"github.com/ava-labs/libevm/core/vm"
	"github.com/ava-labs/libevm/core/vm/runtime"
	"github.com/ava-labs/libevm/crypto"
	"github.com/containerman17/epochdb/dist"
)

// A WRONG BITMAP MAKES AN INVALID JUMP LOOK VALID, which changes execution and
// diverges a state root, so this is the test the cache exists to pass: the
// bitmap served out of the cache must be THE SAME BYTES libevm would have
// computed, and the execution it drives must be the same execution.
//
// The comparison is made from outside libevm, which is the only place we sit:
// run the code with cache A registered, then with a fresh cache B, and compare
// what the two caches ended up holding for the same code hash. B's entry is by
// definition a freshly computed analysis, A's is the one the run before it
// stored, and a third run off the warm cache A must still return the same
// output, gas and error.
func TestJumpDestBitmapIsIdenticalToAFreshOne(t *testing.T) {
	codes := jumpdestEdgeCases()
	codes = append(codes, corpusCode(t)...)

	var exercised int
	for i, code := range codes {
		hash := crypto.Keccak256Hash(code)
		outA, gasA, errA, a := runWithCache(t, code)
		outB, gasB, errB, b := runWithCache(t, code)
		// A third pass off a cache that is already warm: this one is served
		// entirely by the entry the first pass stored.
		warm := freshCache()
		outC, gasC, errC := runUnder(t, warm, code)
		outD, gasD, errD := runUnder(t, warm, code)

		for _, got := range []struct {
			name string
			out  []byte
			gas  uint64
			err  error
		}{{"B", outB, gasB, errB}, {"C", outC, gasC, errC}, {"D", outD, gasD, errD}} {
			if !bytes.Equal(got.out, outA) || got.gas != gasA || fmt.Sprint(got.err) != fmt.Sprint(errA) {
				t.Fatalf("code %d (%d bytes, %s): pass %s ran differently: %x/%d/%v against %x/%d/%v",
					i, len(code), hash, got.name, got.out, got.gas, got.err, outA, gasA, errA)
			}
		}

		bitA, okA := a.GetAnalysis(hash)
		bitB, okB := b.GetAnalysis(hash)
		if okA != okB {
			t.Fatalf("code %d: one run analysed the code and the other did not (%v, %v)", i, okA, okB)
		}
		if !okA {
			continue // never jumped, so nothing was analysed: nothing to compare
		}
		exercised++
		if !bytes.Equal(bitA, bitB) {
			t.Fatalf("code %d (%d bytes, %s): the cached bitmap is %x, a fresh one is %x",
				i, len(code), hash, bitA, bitB)
		}
	}
	t.Logf("%d code blobs, %d of them reached the JUMPDEST analysis", len(codes), exercised)
	if exercised == 0 {
		t.Fatal("no code blob reached the analysis, so this test proved nothing")
	}
}

// TestJumpDestCacheHandsBackTheExactSlice is our half of libevm's contract: the
// cache MUST return the bytes it was given, unmodified and uncopied.
func TestJumpDestCacheHandsBackTheExactSlice(t *testing.T) {
	c := freshCache()
	want := []byte{1, 2, 3, 4, 5}
	h := common.Hash{9}
	c.AddAnalysis(h, want)
	got, ok := c.GetAnalysis(h)
	if !ok || &got[0] != &want[0] || len(got) != len(want) {
		t.Fatalf("the cache did not hand back the same slice: %v %v", ok, got)
	}
	if _, ok := c.GetAnalysis(common.Hash{8}); ok {
		t.Fatal("a hash nothing stored answered")
	}
}

func TestJumpDestCacheBudget(t *testing.T) {
	t.Setenv("EPOCHDB_JUMPDEST_CACHE", "")
	if n, _, err := jumpDestCacheBytes(8 << 20); err != nil || n != 1<<20 {
		t.Fatalf("default is %d bytes (%v), want an eighth of the code cache's", n, err)
	}
	t.Setenv("EPOCHDB_JUMPDEST_CACHE", "0")
	if n, why, err := jumpDestCacheBytes(8 << 20); err != nil || n != 0 || why == "" {
		t.Fatalf("0 did not turn the cache off: %d %q %v", n, why, err)
	}
	t.Setenv("EPOCHDB_JUMPDEST_CACHE", "64MB")
	if _, _, err := jumpDestCacheBytes(8 << 20); err == nil {
		t.Fatal("a value that is not a byte count started anyway")
	}
}

// --- helpers -------------------------------------------------------------------

func freshCache() jumpDests {
	return jumpDests{lru.NewSizeConstrainedCache[common.Hash, []byte](1 << 30)}
}

// runUnder executes code with `c` as the registered bitmap cache. The
// registration is temporary, because it is process-wide and at-most-once.
func runUnder(t *testing.T, c jumpDests, code []byte) ([]byte, uint64, error) {
	t.Helper()
	var (
		out []byte
		gas uint64
		err error
	)
	// Empty calldata: a solc contract's dispatcher still runs, and its first
	// JUMPI is what forces the analysis. Execute plants the code and Call runs
	// it again for the gas figure, which is the finest divergence detector
	// there is: a jump taken differently costs different gas.
	run := func() error {
		cfg := &runtime.Config{GasLimit: 10_000_000, BlockNumber: new(big.Int)}
		_, cfg.State, _ = runtime.Execute(code, nil, cfg)
		out, gas, err = runtime.Call(common.BytesToAddress([]byte("contract")), nil, cfg)
		return nil
	}
	if e := vm.WithTempRegisteredJumpDestCache(c, run); e != nil {
		t.Fatal(e)
	}
	return out, gas, err
}

func runWithCache(t *testing.T, code []byte) ([]byte, uint64, error, jumpDests) {
	t.Helper()
	c := freshCache()
	out, gas, err := runUnder(t, c, code)
	return out, gas, err, c
}

// jumpdestEdgeCases are the shapes a bitmap gets wrong if it is wrong at all.
func jumpdestEdgeCases() [][]byte {
	const (
		jumpdest = 0x5b
		push1    = 0x60
		push32   = 0x7f
		jump     = 0x56
		stop     = 0x00
		maxCode  = 24576 // EIP-170
	)
	// A jump to a 0x5b that is PUSH1's immediate: the jump MUST fail. If the
	// bitmap were wrong the other way this would run to STOP instead.
	intoPushData := []byte{push1, 0x03, jump, jumpdest, stop}
	// The same, but the 0x5b is somewhere inside a PUSH32 immediate.
	intoPush32Data := append([]byte{push1, 0x05, jump},
		append([]byte{push32}, bytes.Repeat([]byte{jumpdest}, 32)...)...)
	// A real jump to a real JUMPDEST, so the good path is covered too.
	realJump := []byte{push1, 0x04, jump, stop, jumpdest, stop}

	rng := rand.New(rand.NewSource(7))
	atLimit := make([]byte, 0, maxCode)
	atLimit = append(atLimit, push1, 0x05, jump, stop, stop, jumpdest)
	for len(atLimit) < maxCode-33 {
		n := 1 + rng.Intn(32)
		atLimit = append(atLimit, byte(push1+n-1))
		for i := 0; i < n; i++ {
			if rng.Intn(3) == 0 {
				atLimit = append(atLimit, jumpdest) // 0x5b as PUSH data
			} else {
				atLimit = append(atLimit, byte(rng.Intn(256)))
			}
		}
	}
	for len(atLimit) < maxCode {
		atLimit = append(atLimit, jumpdest)
	}

	return [][]byte{
		nil,
		{},
		intoPushData,
		intoPush32Data,
		realJump,
		{push1, 0x05, jump, stop, jumpdest, push32}, // truncated PUSH32 at the end
		{push1, 0x05, jump, stop, jumpdest, push1},  // truncated PUSH1 at the end
		atLimit,
		append(atLimit[:len(atLimit):len(atLimit)], jumpdest), // one byte over the limit
	}
}

// corpusCode reads every distinct code blob out of a real corpus's runs, which
// is the "spread of real contract bytecodes" this test wants. Skipped, not
// failed, when there is no corpus: the edge cases still run.
func corpusCode(t *testing.T) [][]byte {
	dir := os.Getenv("EPOCHDB_CORPUS_DIR")
	if dir == "" {
		t.Log("EPOCHDB_CORPUS_DIR is unset: only the edge cases run")
		return nil
	}
	cas, err := dist.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer cas.Close()
	man, err := loadManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	var (
		out  [][]byte
		seen = map[string]bool{}
	)
	lo, hi := []byte(PrefixCode), []byte(PrefixCode+"\xff\xff\xff\xff")
	for _, ref := range man.Runs {
		r, err := OpenRun(cas, ref.Name)
		if err != nil {
			t.Fatal(err)
		}
		err = r.ScanRange(SecState, lo, hi, func(key, val []byte) bool {
			if len(val) > 0 && !seen[string(key)] {
				seen[string(key)] = true
				out = append(out, bytes.Clone(val))
			}
			return true
		})
		r.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("%s: %d distinct code blobs", dir, len(out))
	return out
}

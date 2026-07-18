package rpc

import "testing"

func TestSearchGasConvergence(t *testing.T) {
	for _, threshold := range []uint64{21000, 53212, 4_999_999, 5_000_000} {
		got := searchGas(20999, 5_000_000, func(g uint64) bool { return g >= threshold })
		if got != threshold {
			t.Fatalf("threshold %d: got %d", threshold, got)
		}
	}
	// Everything executes: converges to lo+1.
	if got := searchGas(20999, 100000, func(uint64) bool { return true }); got != 21000 {
		t.Fatalf("all-executable: got %d", got)
	}
}

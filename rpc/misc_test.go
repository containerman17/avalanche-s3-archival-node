package rpc

import "testing"

func TestSearchGasConvergence(t *testing.T) {
	for _, threshold := range []uint64{21000, 53212, 4_999_999, 5_000_000} {
		got, rerr := searchGas(20999, 5_000_000, func(g uint64) (bool, *rpcError) { return g >= threshold, nil })
		if rerr != nil {
			t.Fatal(rerr)
		}
		if got != threshold {
			t.Fatalf("threshold %d: got %d", threshold, got)
		}
	}
	// Everything executes: converges to lo+1.
	got, rerr := searchGas(20999, 100000, func(uint64) (bool, *rpcError) { return true, nil })
	if rerr != nil {
		t.Fatal(rerr)
	}
	if got != 21000 {
		t.Fatalf("all-executable: got %d", got)
	}
}

// A probe that cannot answer aborts the search: a transport or state failure
// used to read as "not executable" and pushed the estimate to the cap.
func TestSearchGasAbortsOnError(t *testing.T) {
	boom := &rpcError{Code: -32000, Message: "state unavailable"}
	got, rerr := searchGas(20999, 5_000_000, func(uint64) (bool, *rpcError) { return false, boom })
	if rerr != boom {
		t.Fatalf("want the probe error, got %d / %v", got, rerr)
	}
}

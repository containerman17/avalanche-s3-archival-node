package validator

import (
	"context"
	"math/big"
	"testing"
	"time"

	ethcommon "github.com/ava-labs/libevm/common"
)

// TestBuildFillPolicy: a fresh preferred parent holds the wake until the
// pool has build-fill-target free txs or build-fill-wait-ms passed since the
// preference moved (a lone tx still wakes within the wait); a repeated build
// on the same parent keeps the 100 ms retry gap instead.
func TestBuildFillPolicy(t *testing.T) {
	if !realEngine {
		t.Skip("stub engine")
	}
	const target, wait = 30, 1000 * time.Millisecond
	h := newHarnessWith(t, testGenesis, `{"build-fill-target":30,"build-fill-wait-ms":1000}`)
	to := ethcommon.HexToAddress("0x1000000000000000000000000000000000000006")
	ctx := context.Background()
	wake := func() time.Duration {
		t.Helper()
		t0 := time.Now()
		if _, err := h.vm.WaitForEvent(ctx); err != nil {
			t.Fatal(err)
		}
		return time.Since(t0)
	}

	// Fresh parent, one tx: mined at the wait, not before, not never.
	h.vm.b.preferenceChanged()
	h.transfer(to, big.NewInt(1))
	if d := wake(); d < wait-200*time.Millisecond || d > wait+500*time.Millisecond {
		t.Fatalf("lone tx on a fresh parent woke after %v, want ~%v", d, wait)
	}
	if _, err := h.vm.BuildBlock(ctx); err != nil {
		t.Fatal(err)
	}

	// Same parent again (our block is out, consensus has not moved): the
	// retry gap, not the fill wait.
	h.transfer(to, big.NewInt(1))
	if d := wake(); d > 400*time.Millisecond {
		t.Fatalf("repeated parent woke after %v, want the 100 ms retry gap", d)
	}

	// A fresh parent with the target reached adds no fill wait (the wake
	// still sits out the genesis's 2 s Granite min delay after block 1).
	h.buildAccept() // moves the preference (the clock) and empties the pool
	for i := 0; i < target; i++ {
		h.transfer(to, big.NewInt(1))
	}
	wake()
	h.vm.b.mu.Lock()
	free, waited := h.vm.b.lastFree, h.vm.b.lastFillWait
	h.vm.b.mu.Unlock()
	if free < target || waited != 0 {
		t.Fatalf("fresh parent with %d free txs: free at wake %d, fill wait %v, want >= %d and 0", target, free, waited, target)
	}
}

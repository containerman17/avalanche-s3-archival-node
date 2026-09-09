package validator

import (
	"context"
	"encoding/json"
	"math/big"
	"testing"
	"time"

	ethcommon "github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/common/hexutil"
	slog "golang.org/x/exp/slog"
)

// TestPoolHeadMovesWithAccept: once Accept returned, no pool read answers
// with the mined txs still pending. Before Accept marked the head move, the
// reset ran on txpool.TxPool's loop after a ChainHeadEvent and txpool_status
// kept reporting the mined txs as pending (a client that saw the new height
// first then waited for a block no tx justified), and WaitForEvent woke
// consensus into builds whose every candidate the engine skipped as mined.
func TestPoolHeadMovesWithAccept(t *testing.T) {
	if !realEngine {
		t.Skip("stub engine")
	}
	h := newHarness(t)
	to := ethcommon.HexToAddress("0x1000000000000000000000000000000000000006")
	const n = 7
	for i := 0; i < n; i++ {
		h.transfer(to, big.NewInt(1))
	}
	blk, _, _ := h.buildAccept()
	// No Sync, no sleep: what the RPC answers right after the accept (it
	// waits for the reset to land).
	var st struct{ Pending, Queued hexutil.Uint }
	if err := json.Unmarshal(h.call("txpool_status"), &st); err != nil {
		t.Fatal(err)
	}
	if st.Pending+st.Queued != 0 {
		t.Fatalf("block %d accepted, txpool_status still pending=%d queued=%d", blk.Height(), st.Pending, st.Queued)
	}
	var nonce hexutil.Uint64
	json.Unmarshal(h.call("eth_getTransactionCount", h.addr, "pending"), &nonce)
	if nonce != n {
		t.Fatalf("pending nonce %d, want %d", nonce, n)
	}
	// Nothing to build: consensus must not be woken.
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if msg, err := h.vm.WaitForEvent(ctx); err == nil {
		t.Fatalf("WaitForEvent returned %v with an empty pool", msg)
	}
	if got := h.vm.drops.counts(); got["old"] != n || len(got) != 1 {
		t.Fatalf("removal counts %v, want old=%d only", got, n)
	}
}

// TestRemovalReason: legacypool's removal lines map to a reason and a count.
func TestRemovalReason(t *testing.T) {
	rec := func(msg string, attrs ...any) slog.Record {
		r := slog.NewRecord(time.Now(), slog.LevelDebug, msg, 0)
		r.Add(attrs...)
		return r
	}
	cases := []struct {
		r      slog.Record
		reason string
		n      uint64
	}{
		{rec("Removed old pending transaction", "hash", "0x1"), "old", 1},
		{rec("Removed unpayable pending transaction", "hash", "0x1"), "unpayable", 1},
		{rec("Removed old queued transactions", "count", 3), "old", 3},
		{rec("Removed unpayable queued transactions", "count", 0), "unpayable", 0},
		{rec("Removed cap-exceeding queued transaction", "hash", "0x1"), "cap-exceeding", 1},
		{rec("Removed fairness-exceeding pending transaction", "hash", "0x1"), "fairness-exceeding", 1},
		{rec("Demoting invalidated transaction", "hash", "0x1"), "invalidated", 1},
		{rec("Demoting pending transaction", "hash", "0x1"), "", 0},
		{rec("Pooled new executable transaction", "hash", "0x1"), "", 0},
	}
	for _, c := range cases {
		if reason, n := removalReason(c.r); reason != c.reason || n != c.n {
			t.Errorf("%q: got %q %d, want %q %d", c.r.Message, reason, n, c.reason, c.n)
		}
	}
	h := &dropLogHandler{}
	for _, c := range cases {
		h.Handle(context.Background(), c.r)
	}
	if got := h.counts(); got["old"] != 4 || got["unpayable"] != 1 || got["invalidated"] != 1 {
		t.Fatalf("counts %v", got)
	}
}

package validator

import (
	"context"
	"encoding/json"
	"math/big"
	"testing"
	"time"

	ethcommon "github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/common/hexutil"
)

// TestPoolHeadMovesWithAccept: once Accept returned, no pool read answers
// with the mined txs still pending (the pool moves inside epochdb_accept,
// for the block's senders only), the pending nonce is the state's, and
// WaitForEvent does not wake consensus into a build with nothing to build.
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
	// A rebuilt-then-dropped block leaves its txs in the pool: build twice
	// on the same parent, accept the second, the pool is empty either way.
	h.transfer(to, big.NewInt(1))
	ctx2 := context.Background()
	if _, err := h.vm.WaitForEvent(ctx2); err != nil {
		t.Fatal(err)
	}
	a, err := h.vm.BuildBlock(ctx2)
	if err != nil {
		t.Fatal(err)
	}
	if p, _ := h.vm.eng.poolStatus(); p != 1 {
		t.Fatalf("a built (unaccepted) block must leave its txs pending, got %d", p)
	}
	time.Sleep(3 * time.Millisecond)
	b, err := h.vm.BuildBlock(ctx2)
	if err != nil {
		t.Fatal(err)
	}
	for _, x := range []interface{ Verify(context.Context) error }{a, b} {
		if err := x.Verify(ctx2); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.Accept(ctx2); err != nil {
		t.Fatal(err)
	}
	if err := a.Reject(ctx2); err != nil {
		t.Fatal(err)
	}
	if p, q := h.vm.eng.poolStatus(); p+q != 0 {
		t.Fatalf("pool after the sibling's accept: pending=%d queued=%d", p, q)
	}
}

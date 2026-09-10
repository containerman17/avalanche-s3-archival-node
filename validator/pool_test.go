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

// TestBuildAfterAcceptSeesNoMinedTx: the pool moves inside epochdb_accept,
// so a build right after an accept never offers a mined tx (skip code 1),
// and a build on the preferred, not yet accepted block offers only what
// that block does not hold.
func TestBuildAfterAcceptSeesNoMinedTx(t *testing.T) {
	if !realEngine {
		t.Skip("stub engine")
	}
	h := newHarness(t)
	to := ethcommon.HexToAddress("0x100000000000000000000000000000000000000b")
	for i := 0; i < 50; i++ {
		h.transfer(to, big.NewInt(1))
	}
	h.buildAccept()
	for i := 0; i < 30; i++ {
		h.transfer(to, big.NewInt(1))
	}
	skipsOf := func(out buildOut) (nonceLow int) {
		for _, c := range out.skipped {
			if c == 1 {
				nonceLow++
			}
		}
		return
	}
	_, headID := h.vm.current()
	ts := uint64(time.Now().UnixMilli())
	a, err := h.vm.eng.build(headID, ts, h.vm.coinbase(), 0, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if a.included != 30 || skipsOf(a) != 0 || len(a.skipped) != 30 {
		t.Fatalf("build after accept: included=%d candidates=%d nonceLow=%d, want 30/30/0", a.included, len(a.skipped), skipsOf(a))
	}
	// 10 more txs; a build on the unaccepted block a offers only those.
	for i := 0; i < 10; i++ {
		h.transfer(to, big.NewInt(1))
	}
	b, err := h.vm.eng.build(a.id, ts+1, h.vm.coinbase(), 0, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if b.included != 10 || skipsOf(b) != 0 || len(b.skipped) != 10 {
		t.Fatalf("build on the preferred block: included=%d candidates=%d nonceLow=%d, want 10/10/0", b.included, len(b.skipped), skipsOf(b))
	}
	if p, _ := h.vm.eng.poolStatus(); p != 40 {
		t.Fatalf("pending after two unaccepted builds: %d, want 40", p)
	}
}

package validator

import (
	"context"
	"errors"
	"math/big"
	"testing"
	"time"

	"github.com/ava-labs/avalanchego/database"
	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/snow/engine/common"
	ethcommon "github.com/ava-labs/libevm/common"
)

// TestSiblingStaysRetrievableAfterAccept: avalanchego's rpcchainvm server
// calls VM.GetBlock on a block before Reject, and a "not found" for a block
// consensus knows shuts the chain down. Two siblings on the head, one
// accepted: the loser must still answer GetBlock, its Reject must succeed,
// and a repeated Verify must fail cleanly.
func TestSiblingStaysRetrievableAfterAccept(t *testing.T) {
	if !realEngine {
		t.Skip("stub engine")
	}
	h := newHarness(t)
	ctx := context.Background()
	h.transfer(ethcommon.HexToAddress("0x1000000000000000000000000000000000000001"), big.NewInt(1))
	if msg, err := h.vm.WaitForEvent(ctx); err != nil || msg != common.PendingTxs {
		t.Fatalf("WaitForEvent: %v %v", msg, err)
	}
	a, err := h.vm.BuildBlock(ctx)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(3 * time.Millisecond) // a different ms timestamp: a sibling, not the same block
	b, err := h.vm.BuildBlock(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if a.ID() == b.ID() || a.Parent() != b.Parent() {
		t.Fatalf("want two siblings, got %s and %s", a.ID(), b.ID())
	}
	for _, blk := range []interface{ Verify(context.Context) error }{a, b} {
		if err := blk.Verify(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if err := a.Accept(ctx); err != nil {
		t.Fatal(err)
	}
	got, err := h.vm.GetBlock(ctx, b.ID())
	if errors.Is(err, database.ErrNotFound) {
		t.Fatalf("GetBlock(loser %s) = not found after the sibling's accept: fatal to the chain in avalanchego", b.ID())
	}
	if err != nil || string(got.Bytes()) != string(b.Bytes()) {
		t.Fatalf("GetBlock(loser): %v", err)
	}
	if err := b.Verify(ctx); err == nil {
		t.Fatal("Verify of the loser after the sibling's accept succeeded, want a clean error")
	} else {
		t.Logf("verify of the loser: %v", err)
	}
	if err := b.Reject(ctx); err != nil {
		t.Fatalf("Reject(loser): %v", err)
	}
	if err := b.Reject(ctx); err != nil {
		t.Fatalf("second Reject(loser): %v", err)
	}
	if _, err := h.vm.GetBlock(ctx, b.ID()); err != nil {
		t.Fatalf("GetBlock(loser) after Reject: %v", err)
	}
	// A block nobody ever saw is a true not-found.
	if _, err := h.vm.GetBlock(ctx, ids.ID{0x42}); !errors.Is(err, database.ErrNotFound) {
		t.Fatalf("GetBlock(unknown) = %v, want ErrNotFound", err)
	}
	if last, _ := h.vm.LastAccepted(ctx); last != a.ID() {
		t.Fatalf("last accepted %s, want %s", last, a.ID())
	}
}

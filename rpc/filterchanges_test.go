package rpc

// getFilterChanges is incremental, so the cursor it moves IS the delivery: it
// used to move before the query ran, which turned one failed read into a
// permanent hole (the client got a single error, the retry returned []).
// filters_test.go is an external-package corpus test and cannot reach the
// injectable block source this needs, so this one lives here.

import (
	"testing"

	"github.com/containerman17/epochdb/exec"
	"github.com/containerman17/epochdb/state"
)

func TestFilterChangesRetriesAfterReadError(t *testing.T) {
	env := newBlockHashEnv(t)
	env.hist.SetHead(bhSealedEnd)
	gate := &gatedBlocks{hidden: bhSealedEnd + 2} // block 10 unreadable
	env.srv.EnableTxAPIs(
		state.CombinedTxIndex{Epochs: env.hist.Epochs()},
		SealedBlocks{Epochs: env.hist.Epochs(), Blocks: gate},
		exec.ParseEthBlock,
	)

	res, rerr := env.srv.newBlockFilter()
	if rerr != nil {
		t.Fatal(rerr)
	}
	id := res.(string)

	env.hist.SetHead(bhTailEnd)
	if _, rerr := env.srv.getFilterChanges(mustParams(t, id)); rerr == nil {
		t.Fatal("an unreadable block returned changes instead of an error")
	}
	gate.hide(0)

	res, rerr = env.srv.getFilterChanges(mustParams(t, id))
	if rerr != nil {
		t.Fatal(rerr)
	}
	hashes, ok := res.([]string)
	if !ok {
		t.Fatalf("result type %T", res)
	}
	if want := int(bhTailEnd - bhSealedEnd); len(hashes) != want {
		t.Fatalf("retry returned %d block hashes, want %d: the failed poll consumed them", len(hashes), want)
	}
	for i, h := range hashes {
		if got, want := h, env.hashes[uint64(bhSealedEnd+1+i)].Hex(); got != want {
			t.Fatalf("hash %d is %s, want %s", i, got, want)
		}
	}
}

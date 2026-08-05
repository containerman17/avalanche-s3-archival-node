package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/ava-labs/libevm/common"
)

// TestTxIndexHolderRefusesWhenNeverLoaded: a holder whose OpenTxIndex failed
// has NO index, and a walk that visits zero candidates is indistinguishable
// from "that hash is not on this chain". One corrupt txidx bucket used to
// make every eth_getTransactionByHash / eth_getTransactionReceipt /
// eth_getBlockByHash on the whole chain answer null on a node reporting
// serving:true. It must be an error instead, so dispatch's refusal fires.
func TestTxIndexHolderRefusesWhenNeverLoaded(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "txidx_0.idx"), []byte("garbage"), 0o644); err != nil {
		t.Fatal(err)
	}

	var h txIndexHolder
	// The zero value, before any reopen: still not an empty chain.
	if err := h.WalkCandidates(common.Hash{}, func(uint64) (bool, error) { return false, nil }); err == nil {
		t.Fatal("a never-loaded tx index walked zero candidates instead of erroring")
	}

	h.reopen(dir, nil) // logs and leaves cur nil: the open cannot succeed
	var visits int
	err := h.WalkCandidates(common.Hash{1}, func(uint64) (bool, error) { visits++; return false, nil })
	if err == nil {
		t.Fatal("a failed tx index open answered 'no such tx' for every hash")
	}
	if visits != 0 {
		t.Fatalf("walked %d candidates without an index", visits)
	}
}

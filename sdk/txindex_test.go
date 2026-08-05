package sdk

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/ava-labs/libevm/common"
)

// TestTxIndexRefusesWhenNeverLoaded: identical contract to the serve node's
// holder. A handle whose index failed to open answered TransactionByHash and
// BlockByHash as "unknown hash" FOREVER (only a cook watermark advance calls
// reopen again), which a consumer reads as "that transaction does not exist".
func TestTxIndexRefusesWhenNeverLoaded(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "txidx_0.idx"), []byte("garbage"), 0o644); err != nil {
		t.Fatal(err)
	}

	var idx txIndex
	if err := idx.WalkCandidates(common.Hash{}, func(uint64) (bool, error) { return false, nil }); err == nil {
		t.Fatal("a never-loaded tx index walked zero candidates instead of erroring")
	}

	idx.reopen(dir, nil)
	var visits int
	err := idx.WalkCandidates(common.Hash{1}, func(uint64) (bool, error) { visits++; return false, nil })
	if err == nil {
		t.Fatal("a failed tx index open answered 'no such tx' for every hash")
	}
	if visits != 0 {
		t.Fatalf("walked %d candidates without an index", visits)
	}
}

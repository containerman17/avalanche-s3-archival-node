package epochdb_test

import (
	"bytes"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/containerman17/avalanche-s3-archival-node/rpc"
)

// TestStoredCallTraceParity is the stored-frames renderer against a real
// callTracer re-execution over a corpus, newest blocks first, EPOCHDB_TRACE_SWEEP
// transactions (default 300). Every difference is reported, none is fatal here:
// the serving path is what dies (rpc/callframes.go).
//
//	EPOCHDB_CORPUS_DIR=$PWD/data-kite go test -run TestStoredCallTraceParity -v .
func TestStoredCallTraceParity(t *testing.T) {
	n := corpusNode(t)
	want := 300
	if v, err := strconv.Atoi(os.Getenv("EPOCHDB_TRACE_SWEEP")); err == nil {
		want = v
	}
	head, err := n.Head()
	if err != nil {
		t.Fatal(err)
	}
	seen, bad, failed, nested := 0, 0, 0, 0
	kinds := map[string]int{}
	norm := regexp.MustCompile(`\[[0-9]+\]|"0x[0-9a-f]*"|[0-9]+ entries`)
	for h := head.Number; h > 0 && seen < want; h-- {
		blk, err := n.Core().BlockAt(h)
		if err != nil {
			t.Fatal(err)
		}
		if len(blk.Transactions()) == 0 {
			continue
		}
		fresh, err := n.Core().TraceBlock(h, "callTracer", nil)
		if err != nil {
			t.Fatalf("block %d: re-execution: %v", h, err)
		}
		rcpts, err := n.Core().BlockReceipts(blk)
		if err != nil {
			t.Fatal(err)
		}
		for i := range blk.Transactions() {
			stored, err := n.Core().StoredCallTrace(blk, i, rcpts[i])
			if err != nil {
				t.Fatalf("block %d tx %d: render: %v", h, i, err)
			}
			seen++
			if rcpts[i].Status == 0 {
				failed++
			}
			if bytes.Contains(stored, []byte(`"calls"`)) {
				nested++
			}
			if !bytes.Equal(fresh[i], stored) {
				d := rpc.JSONDiff(fresh[i], stored)
				if d == "" {
					d = "same structure, different bytes"
				}
				bad++
				for _, line := range strings.Split(d, "\n") {
					kinds[norm.ReplaceAllString(line, "*")]++
				}
				if bad <= 20 {
					t.Errorf("block %d tx %d %s:\n%s\nre-executed: %s\nstored:      %s", h, i, blk.Transactions()[i].Hash(), d, fresh[i], stored)
				}
			}
		}
	}
	t.Logf("%d transactions compared (%d failed, %d with subcalls), %d differ", seen, failed, nested, bad)
	for k, c := range kinds {
		t.Logf("%7d  %s", c, k)
	}
}

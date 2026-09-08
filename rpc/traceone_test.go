package rpc

import (
	"strings"
	"testing"

	"github.com/ava-labs/libevm/common"
)

// Block 1's parent is genesis, which no container carries: the chain
// context must hand it out or every trace of block 1 fails.
func TestTraceBlockOneHasGenesisParent(t *testing.T) {
	s, _, _, _ := testServer(t)
	traceMode = "reexec"
	if h := s.chainCtx.GetHeader(common.Hash{}, 0); h == nil {
		t.Fatal("chain context has no genesis header")
	}
	// The harness funds nobody, so the replay itself fails on gas: the
	// point is that it gets that far, past the parent lookup.
	if _, rerr := call(t, s, "debug_traceBlockByNumber", "0x1", map[string]any{"tracer": "callTracer"}); rerr != nil && strings.Contains(rerr.Message, "parent header") {
		t.Fatalf("trace block 1: %v", rerr.Message)
	}
}

// eth_getBlockByNumber(0) answers from the same genesis header: indexers
// walk from 0 and stock subnet-evm serves it.
func TestGetBlockZeroIsGenesis(t *testing.T) {
	s, _, _, _ := testServer(t)
	res, rerr := call(t, s, "eth_getBlockByNumber", "0x0", false)
	if rerr != nil {
		t.Fatalf("block 0: %v", rerr.Message)
	}
	m := res.(map[string]any)
	want := s.chainCtx.GetHeader(common.Hash{}, 0).Hash()
	if got := m["hash"].(common.Hash); got != want {
		t.Fatalf("block 0 hash %s, genesis header hash %s", got, want)
	}
	if txs := m["transactions"].([]any); len(txs) != 0 {
		t.Fatalf("block 0 has %d txs", len(txs))
	}
}

// edb_checkTraces reports instead of dying: on the test block (one tx, no
// stored frames, sender unfunded) it must either answer a report with one
// mismatch entry naming the missing frames or a plain RPC error, never exit.
func TestCheckTracesReportsInsteadOfDying(t *testing.T) {
	s, _, _, _ := testServer(t)
	res, rerr := call(t, s, "edb_checkTraces", "0x1")
	if rerr != nil {
		return // a re-execution refusal is an answer too
	}
	r, ok := res.(traceCheckResult)
	if !ok {
		t.Fatalf("result type %T", res)
	}
	if r.Block != 1 || r.Txs != len(r.Mismatches) || len(r.Mismatches) == 0 {
		t.Fatalf("want every tx reported as missing frames, got %+v", r)
	}
	if !strings.Contains(r.Mismatches[0].Diff, "no stored frames") {
		t.Fatalf("diff %q", r.Mismatches[0].Diff)
	}
}

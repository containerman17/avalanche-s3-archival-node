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

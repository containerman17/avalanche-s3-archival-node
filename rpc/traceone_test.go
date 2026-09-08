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

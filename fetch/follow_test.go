package fetch

import (
	"testing"

	"github.com/ava-labs/avalanchego/ids"
)

// The agreement rule differs by subnet on purpose (see crossCheckedWeights):
// the primary network absorbs delegation churn, an ACP-77 L1's static
// registration weights must match exactly.
func TestWeightsAgree(t *testing.T) {
	a := ids.GenerateTestNodeID()
	b := ids.GenerateTestNodeID()
	small := ids.GenerateTestNodeID()

	base := map[ids.NodeID]uint64{a: 1000, b: 1000, small: 20}
	churned := map[ids.NodeID]uint64{a: 1000, b: 1000} // 1% of stake left

	if err := weightsAgree(base, churned, false); err != nil {
		t.Fatalf("primary network should absorb 1%% churn: %v", err)
	}
	if err := weightsAgree(base, churned, true); err == nil {
		t.Fatal("an L1's static weights must match exactly")
	}
	if err := weightsAgree(base, base, true); err != nil {
		t.Fatalf("identical sets must agree: %v", err)
	}
	// A quarter of the stake vanishing is a bad source on any subnet.
	if err := weightsAgree(base, map[ids.NodeID]uint64{a: 1000, b: 500, small: 20}, false); err == nil {
		t.Fatal("24%% stake disagreement must fail the 95%% rule")
	}
	if err := weightsAgree(nil, nil, false); err == nil {
		t.Fatal("empty sets must fail")
	}
}

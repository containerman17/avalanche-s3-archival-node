package fetch

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ava-labs/avalanchego/ids"
	avaconstants "github.com/ava-labs/avalanchego/utils/constants"
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

// vdrServer is a P-chain endpoint that answers platform.getCurrentValidators
// with these weights, after refusing the first `rateLimited` requests with a
// 429 the way a public node refuses a box running dozens of chains.
func vdrServer(weights map[ids.NodeID]uint64, rateLimited int32) *httptest.Server {
	left := atomic.Int32{}
	left.Store(rateLimited)
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if left.Add(-1) >= 0 {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		var vs []string
		for id, weight := range weights {
			vs = append(vs, fmt.Sprintf(
				`{"txID":%q,"startTime":"0","endTime":"0","weight":"%d","nodeID":%q}`,
				ids.Empty, weight, id))
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"jsonrpc":"2.0","id":1,"result":{"validators":[%s]}}`, strings.Join(vs, ","))
	}))
}

// THE BUG THIS PINS (mainnet C, 2026-08-13): a 429 from the public node was a
// fatal startup error, so the orchestrator restarted the chain straight into
// the next 429 and it never came back. A rate-limited source must cost a wait.
func TestValidatorSetSurvivesRateLimit(t *testing.T) {
	a := ids.GenerateTestNodeID()
	srv := vdrServer(map[ids.NodeID]uint64{a: 100}, 2)
	defer srv.Close()

	w, err := crossCheckedWeights(context.Background(), []string{srv.URL}, ids.GenerateTestID())
	if err != nil {
		t.Fatalf("two 429s must not stop a start: %v", err)
	}
	if w[a] != 100 {
		t.Fatalf("weights = %v", w)
	}
}

// A source that stays rate-limited is skipped, not fatal, as long as another
// one answers.
func TestValidatorSetFailsOverToHealthySource(t *testing.T) {
	a := ids.GenerateTestNodeID()
	dead := vdrServer(nil, 1<<30)
	defer dead.Close()
	good := vdrServer(map[ids.NodeID]uint64{a: 100}, 0)
	defer good.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	w, err := crossCheckedWeights(ctx, []string{dead.URL, good.URL}, ids.GenerateTestID())
	if err != nil {
		t.Fatalf("one dead source must not stop a start: %v", err)
	}
	if w[a] != 100 {
		t.Fatalf("weights = %v", w)
	}
}

// FAILOVER MUST NOT WEAKEN THE CROSS-CHECK: an L1's static weights still have
// to match EXACTLY between the sources that answer, and a third source being
// unreachable does not turn a disagreement into an accepted answer.
func TestFailoverKeepsTheCrossCheckRule(t *testing.T) {
	a, b, small := ids.GenerateTestNodeID(), ids.GenerateTestNodeID(), ids.GenerateTestNodeID()
	// 1% of the stake between them: churn on the primary network, a
	// registration on an L1.
	one := vdrServer(map[ids.NodeID]uint64{a: 1000, b: 1000, small: 20}, 0)
	defer one.Close()
	other := vdrServer(map[ids.NodeID]uint64{a: 1000, b: 1000}, 0)
	defer other.Close()
	dead := vdrServer(nil, 1<<30)
	defer dead.Close()

	// A disagreement IS retried (a registration can land mid-round), so this
	// one is given a short budget and has to still be an error at the end of
	// it. The dead third source is there to prove failover cannot turn a
	// disagreement into an accepted answer.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := crossCheckedWeights(ctx, []string{one.URL, other.URL, dead.URL}, ids.GenerateTestID()); err == nil {
		t.Fatal("an L1 whose sources disagree must fail, failover or not")
	}
	// The same two sources on the PRIMARY network are inside the 95% band and
	// must still be accepted: the rule is per subnet, and it did not move.
	if _, err := crossCheckedWeights(context.Background(), []string{one.URL, other.URL}, avaconstants.PrimaryNetworkID); err != nil {
		t.Fatalf("the primary network absorbs delegation churn: %v", err)
	}
}

// When nothing answers, a node says so and dies loudly rather than following
// the tip with a validator set it does not have.
func TestValidatorSetAllSourcesDown(t *testing.T) {
	dead := vdrServer(nil, 1<<30)
	defer dead.Close()
	other := vdrServer(nil, 1<<30)
	defer other.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := crossCheckedWeights(ctx, []string{dead.URL, other.URL}, ids.GenerateTestID())
	if err == nil || !strings.Contains(err.Error(), "all 2 validator sources failed") {
		t.Fatalf("want a loud all-sources-down failure, got %v", err)
	}
}

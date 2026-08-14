package dist

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

// A 429 is a wait, never a death: the call that used to kill a container's
// startup has to come back on the next round.
func TestTryRetriesThenSucceeds(t *testing.T) {
	fast(t)
	calls := 0
	err := Try(context.Background(), "info.getNetworkID", []string{"one"}, func(context.Context, string) error {
		calls++
		if calls < 3 {
			return fmt.Errorf("received status code: 429")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("two 429s must not be fatal: %v", err)
	}
	if calls != 3 {
		t.Fatalf("calls = %d, want 3", calls)
	}
}

// One rate-limited host must cost a request, not a node: the next source is
// tried in the same round.
func TestTryRotatesToHealthySource(t *testing.T) {
	fast(t)
	var seen []string
	err := Try(context.Background(), "platform.getTx", []string{"limited", "healthy"}, func(_ context.Context, src string) error {
		seen = append(seen, src)
		if src == "limited" {
			return fmt.Errorf("received status code: 429")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("a healthy second source must answer: %v", err)
	}
	if len(seen) != 2 || seen[1] != "healthy" {
		t.Fatalf("sources tried = %v, want [limited healthy]", seen)
	}
}

// The budget is bounded, and what it says when it runs out is the operator's
// only signal: every source, and what each one answered.
func TestTryGivesUpLoudly(t *testing.T) {
	fast(t)
	err := Try(context.Background(), "info.peers", []string{"a", "b"}, func(context.Context, string) error {
		return fmt.Errorf("received status code: 429")
	})
	if err == nil {
		t.Fatal("all sources down must fail")
	}
	for _, want := range []string{"info.peers", "gave up", "2 source", "a: ", "b: ", "429"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q does not name %q", err, want)
		}
	}
}

func TestTryStopsOnContext(t *testing.T) {
	fast(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := Try(ctx, "info.peers", []string{"a"}, func(context.Context, string) error {
		return fmt.Errorf("received status code: 503")
	})
	if err == nil || !strings.Contains(err.Error(), "context canceled") {
		t.Fatalf("a cancelled context must end the retries, got %v", err)
	}
}

func TestSources(t *testing.T) {
	got := Sources(" https://a , ,https://b ")
	if len(got) != 2 || got[0] != "https://a" || got[1] != "https://b" {
		t.Fatalf("Sources = %v", got)
	}
	if len(Sources("")) != 0 {
		t.Fatal("empty list is no sources")
	}
}

// fast spends the whole retry budget in milliseconds.
func fast(t *testing.T) {
	t.Helper()
	rounds, backoff, maxWait := upstreamRounds, upstreamBackoff, upstreamMaxWait
	upstreamRounds, upstreamBackoff, upstreamMaxWait = 4, time.Millisecond, time.Millisecond
	t.Cleanup(func() { upstreamRounds, upstreamBackoff, upstreamMaxWait = rounds, backoff, maxWait })
}

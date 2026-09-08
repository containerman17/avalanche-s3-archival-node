package vmexec

import "testing"

// TestTipGateHysteresis drives the gate with a fake lag source both ways:
// TIP at lag <= limit, back to catch-up only past 2*limit.
func TestTipGateHysteresis(t *testing.T) {
	lag := uint64(0)
	g := tipGate{lag: func() uint64 { return lag }, limit: 5000}
	step := func(l uint64, wantTip, wantChanged bool) {
		t.Helper()
		lag = l
		got, changed := g.update()
		if got != l || g.tip != wantTip || changed != wantChanged {
			t.Fatalf("lag %d: got tip=%v changed=%v, want tip=%v changed=%v", l, g.tip, changed, wantTip, wantChanged)
		}
	}
	step(100000, false, false) // far behind: catch-up from the start
	step(5001, false, false)   // one past the limit: still catching up
	step(5000, true, true)     // at the limit: the tip
	step(7000, true, false)    // fell behind, but within 2x: hysteresis holds
	step(10000, true, false)   // exactly 2x: still the tip
	step(10001, false, true)   // past 2x: catch-up again
	step(6000, false, false)   // must get back within the limit first
	step(0, true, true)
}

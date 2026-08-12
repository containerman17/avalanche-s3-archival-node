package epochdb

import "testing"

// TestLiveNodeSeparatesTheNameableHeadFromTheGoal pins the 2026-08-05 fix:
// under --tip-override the query surface gets no follower height at all, so
// `pending` and the block-number ceiling stay at the executed head (every
// height above it is either unstaged or a leftover from an earlier run), while
// eth_syncing advertises the walk's ceiling.
func TestLiveNodeSeparatesTheNameableHeadFromTheGoal(t *testing.T) {
	at := func(n uint64) func() uint64 { return func() uint64 { return n } }

	bf := liveNode{live: at(7_548_834), settled: at(7_548_834), target: at(10_129_485)}
	if got := bf.AcceptedHead(); got != 7_548_834 {
		t.Fatalf("backfill AcceptedHead=%d, want the executed head", got)
	}
	if got := bf.SyncTarget(); got != 10_129_485 {
		t.Fatalf("backfill SyncTarget=%d, want the override ceiling", got)
	}

	// Following: both are the follower's accepted head, exactly as before.
	fw := liveNode{live: at(100), settled: at(100), accepted: at(200)}
	if got, want := fw.AcceptedHead(), uint64(200); got != want {
		t.Fatalf("follow AcceptedHead=%d, want %d", got, want)
	}
	if got, want := fw.SyncTarget(), uint64(200); got != want {
		t.Fatalf("follow SyncTarget=%d, want %d", got, want)
	}
	// A follower behind the executor (a resumed dir) never drags a label down.
	behind := liveNode{live: at(300), settled: at(300), accepted: at(200)}
	if got := behind.AcceptedHead(); got != 300 {
		t.Fatalf("AcceptedHead=%d below the executed head", got)
	}
}

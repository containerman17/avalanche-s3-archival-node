package rpc

import "testing"

// fakeLive is the height-label surface with no executor behind it.
type fakeLive struct{ live, accepted, settled, target uint64 }

func (f fakeLive) LiveHead() uint64      { return f.live }
func (f fakeLive) AcceptedHead() uint64  { return f.accepted }
func (f fakeLive) SettledHeight() uint64 { return f.settled }
func (f fakeLive) SyncTarget() uint64    { return f.target }

// TestSyncingReportsTheTargetNotTheNameableHead pins the split of 2026-08-05:
// highestBlock is the height this node is syncing TOWARD, which in a bounded
// backfill is the walk's ceiling and sits far above AcceptedHead, the bound on
// what a read may name. Before the split both came off the staging store's
// head, so a --tip-override run advertised a highestBlock (66,854,601, left in
// the dir by an earlier follow run) that it would never reach.
func TestSyncingReportsTheTargetNotTheNameableHead(t *testing.T) {
	syncing := func(l Live) map[string]string {
		t.Helper()
		res, rerr := (&Server{live: l}).dispatch(&rpcRequest{Method: "eth_syncing"})
		if rerr != nil {
			t.Fatalf("eth_syncing: %v", rerr)
		}
		obj, _ := res.(map[string]string)
		return obj
	}

	// Backfill: executed 7,548,834 of a 10,129,485 ceiling, and nothing above
	// the executed head is nameable.
	got := syncing(fakeLive{live: 7_548_834, accepted: 7_548_834, settled: 7_548_834, target: 10_129_485})
	if got["highestBlock"] != "0x9a904d" || got["currentBlock"] != "0x732fa2" {
		t.Fatalf("backfill eth_syncing: %v", got)
	}

	// Following: target and accepted are the same height, unchanged behaviour.
	got = syncing(fakeLive{live: 100, accepted: 200, settled: 100, target: 200})
	if got["highestBlock"] != "0xc8" || got["currentBlock"] != "0x64" {
		t.Fatalf("follow eth_syncing: %v", got)
	}

	// Caught up: false, not an object, whatever the other labels say.
	res, rerr := (&Server{live: fakeLive{live: 200, accepted: 200, settled: 200, target: 200}}).
		dispatch(&rpcRequest{Method: "eth_syncing"})
	if rerr != nil || res != false {
		t.Fatalf("caught-up eth_syncing: %v %v", res, rerr)
	}
}

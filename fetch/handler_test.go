package fetch

import (
	"context"
	"testing"

	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/message"
	"github.com/ava-labs/avalanchego/proto/pb/p2p"
	"github.com/ava-labs/avalanchego/utils/set"
)

// TestMalformedInboundIsCounted: dropping a malformed message is right, but a
// silent drop is a timeout as far as anything downstream can tell. After an
// avalanchego protocol bump the only symptom used to be polls_failed climbing
// with no way to tell a decode fault from a slow peer.
func TestMalformedInboundIsCounted(t *testing.T) {
	h := newHandler(set.Of(ids.GenerateTestNodeID()), newPeerPool())
	h.setConsensusCallbacks(
		func(ids.NodeID, []byte) { t.Error("a malformed container reached the engine") },
		func(ids.NodeID, uint32, ids.ID, ids.ID, ids.ID, uint64) { t.Error("a malformed vote reached the engine") },
	)
	deliver := func(op message.Op, m *p2p.Chits) {
		h.HandleInbound(context.Background(), &message.InboundMessage{
			NodeID: ids.GenerateTestNodeID(), Op: op, Message: m,
		})
	}

	// Right op, wrong body: the wire format moved under us.
	for _, op := range []message.Op{message.PutOp, message.PushQueryOp, message.ChitsOp, message.AcceptedFrontierOp, message.AncestorsOp} {
		h.HandleInbound(context.Background(), &message.InboundMessage{
			NodeID: ids.GenerateTestNodeID(), Op: op, Message: &p2p.Ping{},
		})
	}
	if got := h.drops.badPayload.Load(); got != 5 {
		t.Fatalf("badPayload = %d, want 5", got)
	}

	// Right body, an ID field that is not 32 bytes.
	good := ids.GenerateTestID()
	deliver(message.ChitsOp, &p2p.Chits{PreferredId: []byte{1, 2, 3}})
	deliver(message.ChitsOp, &p2p.Chits{PreferredId: good[:], PreferredIdAtHeight: []byte{1, 2, 3}})
	deliver(message.ChitsOp, &p2p.Chits{PreferredId: good[:], AcceptedId: []byte{1, 2, 3}})
	if got := h.drops.badID.Load(); got != 3 {
		t.Fatalf("badID = %d, want 3", got)
	}

	// An answer nobody could take delivery of: the waiter times out on a
	// message that DID arrive, which is the opposite diagnosis.
	full := make(chan ancestorsResponse) // unbuffered, no receiver
	h.registerRoute(7, full)
	h.HandleInbound(context.Background(), &message.InboundMessage{
		NodeID: ids.GenerateTestNodeID(), Op: message.AncestorsOp,
		Message: &p2p.Ancestors{RequestId: 7, Containers: [][]byte{{1}}},
	})
	if got := h.drops.noRoute.Load(); got != 1 {
		t.Fatalf("noRoute = %d, want 1", got)
	}

	p := Progress{DropsBadPayload: h.drops.badPayload.Load(), DropsBadID: h.drops.badID.Load(), DropsNoRoute: h.drops.noRoute.Load()}
	if p.DropSuffix() == "" {
		t.Fatal("drops are counted but never surface in the progress line")
	}
	if (Progress{}).DropSuffix() != "" {
		t.Fatal("a clean node prints a drop suffix it has no reason to")
	}
}

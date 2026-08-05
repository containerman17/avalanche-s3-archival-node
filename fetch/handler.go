package fetch

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/message"
	"github.com/ava-labs/avalanchego/proto/pb/p2p"
	"github.com/ava-labs/avalanchego/utils/set"
	"github.com/ava-labs/avalanchego/version"
)

type ancestorsResponse struct {
	nodeID    ids.NodeID
	requestID uint32
	blocks    [][]byte
}

// Drop counters. Dropping a malformed inbound message is RIGHT (it arrives
// from an untrusted peer and nothing here can be reconstructed from it), but a
// silent drop is indistinguishable from a timeout: after an avalanchego
// protocol bump the symptom is polls_failed climbing with no cause. One
// counter per reason is the whole difference between "the wire format moved"
// and "the peer is slow".
type dropCounts struct {
	// badPayload: the message arrived under an op whose body it does not
	// carry. That is a protocol mismatch, not a peer problem.
	badPayload atomic.Uint64
	// badID: a 32-byte ID field that is not 32 bytes.
	badID atomic.Uint64
	// noRoute: the answer arrived but its delivery channel was full, so the
	// waiter will time out on a message that did reach this box.
	noRoute atomic.Uint64
}

type inboundHandler struct {
	connectedCh chan ids.NodeID
	peers       set.Set[ids.NodeID]
	pool        *peerPool

	drops dropCounts

	routeMu     sync.Mutex
	routeMap    map[uint32]chan ancestorsResponse
	frontierMap map[uint32]chan ids.ID

	// Consensus follower callbacks (nil = drop). Set once via
	// setConsensusCallbacks before the follower starts.
	cbMu        sync.RWMutex
	onContainer func(nodeID ids.NodeID, container []byte)
	onChits     func(nodeID ids.NodeID, requestID uint32, preferred, preferredAtHeight, accepted ids.ID, acceptedHeight uint64)
}

func (h *inboundHandler) setConsensusCallbacks(
	onContainer func(ids.NodeID, []byte),
	onChits func(ids.NodeID, uint32, ids.ID, ids.ID, ids.ID, uint64),
) {
	h.cbMu.Lock()
	h.onContainer = onContainer
	h.onChits = onChits
	h.cbMu.Unlock()
}

func newHandler(peers set.Set[ids.NodeID], pool *peerPool) *inboundHandler {
	return &inboundHandler{
		connectedCh: make(chan ids.NodeID, peers.Len()+4),
		peers:       peers,
		pool:        pool,
		routeMap:    make(map[uint32]chan ancestorsResponse),
		frontierMap: make(map[uint32]chan ids.ID),
	}
}

func (h *inboundHandler) Connected(nodeID ids.NodeID, _ *version.Application, _ ids.ID) {
	if !h.peers.Contains(nodeID) {
		return
	}
	h.pool.connected(nodeID)
	select {
	case h.connectedCh <- nodeID:
	default:
	}
}

func (h *inboundHandler) Disconnected(nodeID ids.NodeID) {
	h.pool.disconnected(nodeID)
}

func (h *inboundHandler) HandleInbound(_ context.Context, msg *message.InboundMessage) {
	defer msg.OnFinishedHandling()

	switch msg.Op {
	case message.PutOp:
		h.cbMu.RLock()
		cb := h.onContainer
		h.cbMu.RUnlock()
		p, ok := msg.Message.(*p2p.Put)
		if !ok {
			h.drops.badPayload.Add(1)
			return
		}
		if cb != nil {
			cb(msg.NodeID, p.Container)
		}
		return
	case message.PushQueryOp:
		h.cbMu.RLock()
		cb := h.onContainer
		h.cbMu.RUnlock()
		p, ok := msg.Message.(*p2p.PushQuery)
		if !ok {
			h.drops.badPayload.Add(1)
			return
		}
		if cb != nil {
			cb(msg.NodeID, p.Container)
		}
		return
	case message.ChitsOp:
		h.cbMu.RLock()
		cb := h.onChits
		h.cbMu.RUnlock()
		p, ok := msg.Message.(*p2p.Chits)
		if !ok {
			h.drops.badPayload.Add(1)
			return
		}
		if cb == nil {
			return
		}
		preferred, err := ids.ToID(p.PreferredId)
		if err != nil {
			h.drops.badID.Add(1)
			return
		}
		// An EMPTY field is the compat shim: a peer that does not fill it in
		// has no separate preference at the requested height, so its overall
		// preference is the honest reading of its vote. A field that is
		// present and malformed is a bad message and goes the way every other
		// decode failure here goes, because substituting a value would deliver
		// a vote to consensus that the peer never cast.
		preferredAtHeight := preferred
		if len(p.PreferredIdAtHeight) > 0 {
			if preferredAtHeight, err = ids.ToID(p.PreferredIdAtHeight); err != nil {
				h.drops.badID.Add(1)
				return
			}
		}
		accepted, err := ids.ToID(p.AcceptedId)
		if err != nil {
			h.drops.badID.Add(1)
			return
		}
		cb(msg.NodeID, p.RequestId, preferred, preferredAtHeight, accepted, p.AcceptedHeight)
		return
	}

	if msg.Op == message.AcceptedFrontierOp {
		payload, ok := msg.Message.(*p2p.AcceptedFrontier)
		if !ok {
			h.drops.badPayload.Add(1)
			return
		}
		id, err := ids.ToID(payload.ContainerId)
		if err != nil {
			h.drops.badID.Add(1)
			return
		}
		h.routeMu.Lock()
		ch, routed := h.frontierMap[payload.RequestId]
		if routed {
			delete(h.frontierMap, payload.RequestId)
		}
		h.routeMu.Unlock()
		if routed {
			select {
			case ch <- id:
			default:
				h.drops.noRoute.Add(1)
			}
		}
		return
	}

	if msg.Op != message.AncestorsOp {
		return
	}
	payload, ok := msg.Message.(*p2p.Ancestors)
	if !ok {
		h.drops.badPayload.Add(1)
		return
	}
	resp := ancestorsResponse{
		nodeID:    msg.NodeID,
		requestID: payload.RequestId,
		blocks:    payload.Containers,
	}

	h.routeMu.Lock()
	ch, routed := h.routeMap[payload.RequestId]
	if routed {
		delete(h.routeMap, payload.RequestId)
	}
	h.routeMu.Unlock()
	if routed {
		select {
		case ch <- resp:
		default:
			h.drops.noRoute.Add(1)
		}
	}
}

func (h *inboundHandler) registerRoute(reqID uint32, ch chan ancestorsResponse) {
	h.routeMu.Lock()
	h.routeMap[reqID] = ch
	h.routeMu.Unlock()
}

func (h *inboundHandler) unregisterRoute(reqID uint32) {
	h.routeMu.Lock()
	delete(h.routeMap, reqID)
	h.routeMu.Unlock()
}

func (h *inboundHandler) registerFrontierRoute(reqID uint32, ch chan ids.ID) {
	h.routeMu.Lock()
	h.frontierMap[reqID] = ch
	h.routeMu.Unlock()
}

func (h *inboundHandler) unregisterFrontierRoute(reqID uint32) {
	h.routeMu.Lock()
	delete(h.frontierMap, reqID)
	h.routeMu.Unlock()
}

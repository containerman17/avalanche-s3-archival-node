package fetch

import (
	"context"
	"sync"

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

type inboundHandler struct {
	connectedCh chan ids.NodeID
	peers       set.Set[ids.NodeID]
	pool        *peerPool

	routeMu  sync.Mutex
	routeMap map[uint32]chan ancestorsResponse
}

func newHandler(peers set.Set[ids.NodeID], pool *peerPool) *inboundHandler {
	return &inboundHandler{
		connectedCh: make(chan ids.NodeID, peers.Len()+4),
		peers:       peers,
		pool:        pool,
		routeMap:    make(map[uint32]chan ancestorsResponse),
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

	if msg.Op != message.AncestorsOp {
		return
	}
	payload, ok := msg.Message.(*p2p.Ancestors)
	if !ok {
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

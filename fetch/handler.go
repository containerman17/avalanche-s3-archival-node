package fetch

import (
	"context"
	"log"
	"sync"
	"sync/atomic"

	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/message"
	"github.com/ava-labs/avalanchego/proto/pb/p2p"
	avaconstants "github.com/ava-labs/avalanchego/utils/constants"
	"github.com/ava-labs/avalanchego/utils/set"
	"github.com/ava-labs/avalanchego/version"
)

type ancestorsResponse struct {
	nodeID    ids.NodeID
	requestID uint32
	blocks    [][]byte
}

// chitsAnswer is one peer's answer to a height-resolution poll: the block ID
// it holds at the requested height (its LAST ACCEPTED id when it has pruned
// that height, which the fetched container's own height exposes) and how far
// it has accepted, which is the window's ceiling for free.
type chitsAnswer struct {
	nodeID         ids.NodeID
	atHeight       ids.ID
	accepted       ids.ID
	acceptedHeight uint64
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
	// chitsMap routes the forward fetch's own height-resolution polls. A poll
	// asks several peers under ONE request ID, so its channel stays registered
	// until the caller unregisters it and takes every answer that arrives.
	chitsMap map[uint32]chan chitsAnswer

	// Consensus follower callbacks (nil = drop). Set once via
	// setConsensusCallbacks before the follower starts.
	cbMu        sync.RWMutex
	onContainer func(nodeID ids.NodeID, container []byte)
	onChits     func(nodeID ids.NodeID, requestID uint32, preferred, preferredAtHeight, accepted ids.ID, acceptedHeight uint64)

	// Serving side (nil = a peer's Get/GetAncestors is dropped, which is what
	// this node did for its whole life before storage v4). Set once via
	// setServer once the network exists and the store is open.
	srvMu  sync.RWMutex
	source ContainerSource
	reply  func(to ids.NodeID, msg *message.OutboundMessage)
	// answer builds the outbound message; a var so the walk is testable
	// without a message.Creator.
	answer answerer
}

// ContainerSource is what serving a peer needs from the store: the container
// bytes at a height, and the height a container ID names.
type ContainerSource interface {
	ContainerAt(height uint64) ([]byte, error)
	HeightByContainerID(id []byte) (uint64, bool, error)
}

type answerer interface {
	Put(chainID ids.ID, requestID uint32, container []byte) (*message.OutboundMessage, error)
	Ancestors(chainID ids.ID, requestID uint32, containers [][]byte) (*message.OutboundMessage, error)
}

// ancestorsMaxContainers is avalanchego's own AncestorsMaxContainersReceived
// default: a requester drops anything longer.
const ancestorsMaxContainers = 2000

func (h *inboundHandler) setServer(src ContainerSource, ans answerer, reply func(ids.NodeID, *message.OutboundMessage)) {
	h.srvMu.Lock()
	h.source, h.answer, h.reply = src, ans, reply
	h.srvMu.Unlock()
}

// ancestorsOf walks DOWN from the container id names, newest first, exactly
// as a bootstrapping peer consumes it, and stops at height 0, at
// ancestorsMaxContainers, or when the next container would push the batch
// over avaconstants.MaxContainersLen. An unknown id is an empty batch.
func ancestorsOf(src ContainerSource, id []byte) ([][]byte, error) {
	h, ok, err := src.HeightByContainerID(id)
	if err != nil || !ok {
		return nil, err
	}
	var out [][]byte
	size := 0
	for len(out) < ancestorsMaxContainers {
		c, err := src.ContainerAt(h)
		if err != nil {
			return out, err
		}
		if size += len(c); size > avaconstants.MaxContainersLen && len(out) > 0 {
			break
		}
		out = append(out, c)
		if h == 0 {
			break
		}
		h--
	}
	return out, nil
}

// serve answers one Get or GetAncestors on its own goroutine: store reads
// must never hold the network's inbound dispatcher.
func (h *inboundHandler) serve(nodeID ids.NodeID, chainIDBytes []byte, requestID uint32, containerID []byte, ancestors bool) {
	chainID, err := ids.ToID(chainIDBytes)
	if err != nil {
		h.drops.badPayload.Add(1)
		return
	}
	h.srvMu.RLock()
	src, ans, reply := h.source, h.answer, h.reply
	h.srvMu.RUnlock()
	if src == nil {
		return
	}
	go func() {
		var (
			msg *message.OutboundMessage
			err error
		)
		if ancestors {
			cs, werr := ancestorsOf(src, containerID)
			if werr != nil {
				log.Printf("fetch: serve GetAncestors for %s: %v", nodeID, werr)
			}
			msg, err = ans.Ancestors(chainID, requestID, cs)
		} else {
			n, ok, gerr := src.HeightByContainerID(containerID)
			if gerr != nil || !ok {
				return // avalanchego semantics: an unknown Get is a timeout
			}
			c, gerr := src.ContainerAt(n)
			if gerr != nil {
				log.Printf("fetch: serve Get %d for %s: %v", n, nodeID, gerr)
				return
			}
			msg, err = ans.Put(chainID, requestID, c)
		}
		if err != nil {
			log.Printf("fetch: serve reply build: %v", err)
			return
		}
		reply(nodeID, msg)
	}()
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
		chitsMap:    make(map[uint32]chan chitsAnswer),
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
	case message.GetOp:
		if g, ok := msg.Message.(*p2p.Get); ok {
			h.serve(msg.NodeID, g.ChainId, g.RequestId, g.ContainerId, false)
		} else {
			h.drops.badPayload.Add(1)
		}
		return
	case message.GetAncestorsOp:
		if g, ok := msg.Message.(*p2p.GetAncestors); ok {
			h.serve(msg.NodeID, g.ChainId, g.RequestId, g.ContainerId, true)
		} else {
			h.drops.badPayload.Add(1)
		}
		return
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
		// A height-resolution poll owns its request ID; anything else is the
		// consensus follower's.
		h.routeMu.Lock()
		ch, routed := h.chitsMap[p.RequestId]
		h.routeMu.Unlock()
		if routed {
			select {
			case ch <- chitsAnswer{nodeID: msg.NodeID, atHeight: preferredAtHeight, accepted: accepted, acceptedHeight: p.AcceptedHeight}:
			default:
				h.drops.noRoute.Add(1)
			}
			return
		}
		if cb != nil {
			cb(msg.NodeID, p.RequestId, preferred, preferredAtHeight, accepted, p.AcceptedHeight)
		}
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

func (h *inboundHandler) registerChitsRoute(reqID uint32, ch chan chitsAnswer) {
	h.routeMu.Lock()
	h.chitsMap[reqID] = ch
	h.routeMu.Unlock()
}

func (h *inboundHandler) unregisterChitsRoute(reqID uint32) {
	h.routeMu.Lock()
	delete(h.chitsMap, reqID)
	h.routeMu.Unlock()
}

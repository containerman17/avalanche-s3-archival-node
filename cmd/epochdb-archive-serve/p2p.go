package main

import (
	"context"
	"crypto"
	"fmt"
	"log"
	gonet "net"
	"net/netip"
	"os"
	"path/filepath"

	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/message"
	"github.com/ava-labs/avalanchego/network"
	"github.com/ava-labs/avalanchego/network/dialer"
	"github.com/ava-labs/avalanchego/network/peer"
	"github.com/ava-labs/avalanchego/proto/pb/p2p"
	avacommon "github.com/ava-labs/avalanchego/snow/engine/common"
	"github.com/ava-labs/avalanchego/snow/validators"
	"github.com/ava-labs/avalanchego/staking"
	"github.com/ava-labs/avalanchego/subnets"
	"github.com/ava-labs/avalanchego/upgrade"
	"github.com/ava-labs/avalanchego/utils/compression"
	avaconstants "github.com/ava-labs/avalanchego/utils/constants"
	"github.com/ava-labs/avalanchego/utils/logging"
	"github.com/ava-labs/avalanchego/utils/set"
	"github.com/ava-labs/avalanchego/version"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/containerman17/avalanche-s3-archival-node/chain"
)

// THE LISTENER IS OUR OWN, not fetch.Fetcher's, for two reasons a serving-only
// node has: fetch's handler drops PullQuery, and a chain whose validators
// state-synced answers a height poll with the pruned fallback (their last
// accepted block), so the forward fetch on the other side can never seed a
// span from them. This node answers PullQuery with Chits naming the container
// AT THE REQUESTED HEIGHT, which is exactly the one thing a bootstrapping
// epochdb needs and nobody else on the chain has. The network setup is
// fetch.dial's (NewTestNetworkConfig, persisted staking cert, MyIPPort
// 127.0.0.1), minus the validator dial: peers dial us.
func listen(port int, dir string, c *chain.Chain, src *archive) (network.Network, ids.NodeID, error) {
	vdrs := &permissiveValidators{Manager: validators.NewManager()}
	netCfg, err := network.NewTestNetworkConfig(prometheus.NewRegistry(), c.NetworkID, vdrs, set.Of(c.SubnetID))
	if err != nil {
		return nil, ids.EmptyNodeID, fmt.Errorf("NewTestNetworkConfig: %w", err)
	}
	keyPath, certPath := filepath.Join(dir, "staker.key"), filepath.Join(dir, "staker.crt")
	if _, serr := os.Stat(keyPath); serr != nil {
		if err := staking.InitNodeStakingKeyPair(keyPath, certPath); err != nil {
			return nil, ids.EmptyNodeID, fmt.Errorf("staking key pair: %w", err)
		}
	}
	tlsCert, err := staking.LoadTLSCertFromFiles(keyPath, certPath)
	if err != nil {
		return nil, ids.EmptyNodeID, fmt.Errorf("staking cert: %w", err)
	}
	netCfg.TLSConfig = peer.TLSConfig(*tlsCert, nil)
	netCfg.TLSKey = tlsCert.PrivateKey.(crypto.Signer)
	netCfg.MyIPPort.Set(netip.AddrPortFrom(netip.AddrFrom4([4]byte{127, 0, 0, 1}), uint16(port)))
	stakingCert, err := staking.ParseCertificate(netCfg.TLSConfig.Certificates[0].Leaf.Raw)
	if err != nil {
		return nil, ids.EmptyNodeID, fmt.Errorf("ParseCertificate: %w", err)
	}
	netCfg.MyNodeID = ids.NodeIDFromCert(stakingCert)

	ln, err := gonet.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return nil, ids.EmptyNodeID, fmt.Errorf("p2p listen: %w", err)
	}
	msgCreator, err := message.NewCreator(prometheus.NewRegistry(), avaconstants.DefaultNetworkCompressionType, avaconstants.DefaultNetworkMaximumInboundTimeout)
	if err != nil {
		return nil, ids.EmptyNodeID, fmt.Errorf("message.NewCreator: %w", err)
	}
	h := &handler{src: src, subnetID: c.SubnetID}
	if h.creator, err = message.NewCreator(prometheus.NewRegistry(), compression.TypeZstd, avaconstants.DefaultNetworkMaximumInboundTimeout); err != nil {
		return nil, ids.EmptyNodeID, fmt.Errorf("message.NewCreator: %w", err)
	}
	net, err := network.NewNetwork(
		netCfg,
		upgrade.GetConfig(c.NetworkID).GraniteTime,
		msgCreator,
		prometheus.NewRegistry(),
		logging.NoLog{},
		ln,
		dialer.NewDialer(avaconstants.NetworkType, netCfg.DialerConfig, logging.NoLog{}),
		h,
	)
	if err != nil {
		return nil, ids.EmptyNodeID, fmt.Errorf("NewNetwork: %w", err)
	}
	h.net = net
	go func() { log.Printf("archive-serve: network stopped: %v", net.Dispatch()) }()
	log.Printf("archive-serve: serving p2p on :%d as %s", port, netCfg.MyNodeID)
	return net, netCfg.MyNodeID, nil
}

// permissiveValidators answers "yes" to every membership check so this node
// accepts messages from any peer (fetch's, verbatim).
type permissiveValidators struct {
	validators.Manager
}

func (*permissiveValidators) Contains(ids.ID, ids.NodeID) bool { return true }

type handler struct {
	src      *archive
	subnetID ids.ID
	creator  message.Creator
	net      network.Network
}

func (h *handler) Connected(id ids.NodeID, _ *version.Application, _ ids.ID) {
	log.Printf("archive-serve: peer %s connected", id)
}

func (h *handler) Disconnected(id ids.NodeID) { log.Printf("archive-serve: peer %s disconnected", id) }

// HandleInbound answers on its own goroutine: store reads must never hold the
// network's inbound dispatcher.
func (h *handler) HandleInbound(_ context.Context, msg *message.InboundMessage) {
	defer msg.OnFinishedHandling()
	var build func(chainID ids.ID) (*message.OutboundMessage, error)
	var chainIDBytes []byte
	switch m := msg.Message.(type) {
	case *p2p.Get:
		chainIDBytes = m.ChainId
		build = func(chainID ids.ID) (*message.OutboundMessage, error) {
			n, ok, err := h.src.HeightByContainerID(m.ContainerId)
			if err != nil || !ok {
				return nil, err // avalanchego semantics: an unknown Get is a timeout
			}
			c, err := h.src.ContainerAt(n)
			if err != nil {
				return nil, err
			}
			return h.creator.Put(chainID, m.RequestId, c)
		}
	case *p2p.GetAncestors:
		chainIDBytes = m.ChainId
		build = func(chainID ids.ID) (*message.OutboundMessage, error) {
			cs, err := ancestorsOf(h.src, m.ContainerId)
			if err != nil {
				log.Printf("archive-serve: GetAncestors for %s: %v", msg.NodeID, err)
			}
			return h.creator.Ancestors(chainID, m.RequestId, cs)
		}
	case *p2p.PullQuery:
		chainIDBytes = m.ChainId
		build = func(chainID ids.ID) (*message.OutboundMessage, error) {
			top := h.src.top()
			topID, err := h.src.idAt(top)
			if err != nil {
				return nil, err
			}
			at := topID // above our top: the pruned-peer fallback, which the asker detects by height
			if m.RequestedHeight <= top {
				if at, err = h.src.idAt(m.RequestedHeight); err != nil {
					return nil, err
				}
			}
			h.src.polls.Add(1)
			return h.creator.Chits(chainID, m.RequestId, topID, at, topID, top)
		}
	default:
		return
	}
	chainID, err := ids.ToID(chainIDBytes)
	if err != nil {
		return
	}
	go func() {
		out, err := build(chainID)
		if err != nil {
			log.Printf("archive-serve: answer %s for %s: %v", msg.Op, msg.NodeID, err)
		}
		if out == nil {
			return
		}
		if sent := h.net.Send(out, avacommon.SendConfig{NodeIDs: set.Of(msg.NodeID)}, h.subnetID, subnets.NoOpAllower); sent.Len() == 0 {
			log.Printf("archive-serve: %s reply to %s never left this box", msg.Op, msg.NodeID)
		}
	}()
}

// ancestorsMaxContainers is avalanchego's own AncestorsMaxContainersReceived
// default: a requester drops anything longer.
const ancestorsMaxContainers = 2000

// ancestorsOf walks DOWN from the container id names, newest first, and stops
// at height 0, at ancestorsMaxContainers, or when the next container would
// push the batch over avaconstants.MaxContainersLen (fetch's, verbatim).
func ancestorsOf(src *archive, id []byte) ([][]byte, error) {
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

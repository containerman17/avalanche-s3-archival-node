//go:build exp

// peercrawl: connect to one avalanchego node over RAW p2p and ask it for its
// peer list. No HTTP. Prints the handshake (version, trackedSubnets) and every
// IP the node gossips back, so we can see WHO a validator will actually
// advertise. PeerList gossip carries validator IPs only, so a non-validator
// archive node should never appear here: this proves that by observation.
//
//	go run -tags exp ./exp/peercrawl -peer 54.161.148.48:9651 [-look <nodeID>] [-wait 8s]
package main

import (
	"context"
	"crypto"
	"flag"
	"fmt"
	"log"
	"net"
	"net/netip"
	"sync"
	"time"

	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/message"
	"github.com/ava-labs/avalanchego/network/peer"
	"github.com/ava-labs/avalanchego/network/throttling"
	"github.com/ava-labs/avalanchego/proto/pb/p2p"
	"github.com/ava-labs/avalanchego/snow/networking/router"
	"github.com/ava-labs/avalanchego/snow/networking/tracker"
	"github.com/ava-labs/avalanchego/snow/uptime"
	"github.com/ava-labs/avalanchego/snow/validators"
	"github.com/ava-labs/avalanchego/staking"
	"github.com/ava-labs/avalanchego/upgrade"
	"github.com/ava-labs/avalanchego/utils"
	"github.com/ava-labs/avalanchego/utils/bloom"
	"github.com/ava-labs/avalanchego/utils/constants"
	"github.com/ava-labs/avalanchego/utils/crypto/bls/signer/localsigner"
	"github.com/ava-labs/avalanchego/utils/ips"
	"github.com/ava-labs/avalanchego/utils/logging"
	"github.com/ava-labs/avalanchego/utils/math/meter"
	"github.com/ava-labs/avalanchego/utils/resource"
	"github.com/ava-labs/avalanchego/utils/set"
	"github.com/ava-labs/avalanchego/version"
	"github.com/prometheus/client_golang/prometheus"
)

// gossipSink implements peer.Network and records every gossiped IP.
type gossipSink struct {
	mu   sync.Mutex
	seen map[ids.NodeID]netip.AddrPort
}

func (s *gossipSink) Connected(ids.NodeID)            {}
func (s *gossipSink) AllowConnection(ids.NodeID) bool { return true }
func (s *gossipSink) Disconnected(ids.NodeID)         {}
func (s *gossipSink) Track(peers []*ips.ClaimedIPPort) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, p := range peers {
		s.seen[p.NodeID] = p.AddrPort
	}
	return nil
}
func (s *gossipSink) KnownPeers() ([]byte, []byte) {
	f, _ := bloom.New(8, 1024)
	salt := make([]byte, 32)
	return f.Marshal(), salt
}
func (s *gossipSink) Peers(ids.NodeID, set.Set[ids.ID], bool, *bloom.ReadFilter, []byte) []*ips.ClaimedIPPort {
	return nil
}

func main() {
	peerIP := flag.String("peer", "", "ip:port to connect to")
	look := flag.String("look", "", "nodeID to specifically watch for in the gossip")
	wait := flag.Duration("wait", 8*time.Second, "how long to collect gossip after handshake")
	chainStr := flag.String("chain", "", "blockchainID: enables -floor, the lowest height the peer still serves")
	subnetStr := flag.String("subnet", "", "subnetID the chain lives on (advertised in the handshake)")
	lo := flag.Uint64("lo", 1, "floor search lower bound")
	flag.Parse()
	addr, err := netip.ParseAddrPort(*peerIP)
	if err != nil {
		log.Fatalf("bad -peer: %v", err)
	}
	const networkID = constants.MainnetID

	ctx, cancel := context.WithTimeout(context.Background(), *wait+15*time.Second)
	defer cancel()

	conn, err := (&net.Dialer{}).DialContext(ctx, constants.NetworkType, addr.String())
	if err != nil {
		log.Fatalf("dial: %v", err)
	}
	tlsCert, err := staking.NewTLSCert()
	if err != nil {
		log.Fatal(err)
	}
	upg := peer.NewTLSClientUpgrader(peer.TLSConfig(*tlsCert, nil), prometheus.NewCounter(prometheus.CounterOpts{}))
	peerID, conn, cert, err := upg.Upgrade(conn)
	if err != nil {
		log.Fatalf("tls upgrade: %v", err)
	}
	mc, err := message.NewCreator(prometheus.NewRegistry(), constants.DefaultNetworkCompressionType, 10*time.Second)
	if err != nil {
		log.Fatal(err)
	}
	metrics, err := peer.NewMetrics(prometheus.NewRegistry())
	if err != nil {
		log.Fatal(err)
	}
	rt, err := tracker.NewResourceTracker(prometheus.NewRegistry(), resource.NoUsage, meter.ContinuousFactory{}, 10*time.Second)
	if err != nil {
		log.Fatal(err)
	}
	tlsKey := tlsCert.PrivateKey.(crypto.Signer)
	blsKey, err := localsigner.New()
	if err != nil {
		log.Fatal(err)
	}
	myCert, err := staking.ParseCertificate(tlsCert.Leaf.Raw)
	if err != nil {
		log.Fatal(err)
	}
	myID := ids.NodeIDFromCert(myCert)

	// Pretend to be a primary-network validator so the node returns an
	// unfiltered peer list (the old 08_node_versions trick).
	fake := validators.NewManager()
	_ = fake.AddStaker(constants.PrimaryNetworkID, myID, nil, ids.Empty, 1)

	mySubnets := set.Set[ids.ID]{}
	if *subnetStr != "" {
		sid, err := ids.FromString(*subnetStr)
		if err != nil {
			log.Fatalf("bad -subnet: %v", err)
		}
		mySubnets.Add(sid)
	}
	sink := &gossipSink{seen: map[ids.NodeID]netip.AddrPort{}}
	cfg := &peer.Config{
		Metrics:              metrics,
		MessageCreator:       mc,
		Log:                  logging.NoLog{},
		InboundMsgThrottler:  throttling.NewNoInboundThrottler(),
		Network:              sink,
		Router:               router.InboundHandlerFunc(onInbound),
		VersionCompatibility: version.GetCompatibility(upgrade.GetConfig(networkID).EtnaTime),
		MyNodeID:             myID,
		MySubnets:            mySubnets,
		Beacons:              validators.NewManager(),
		Validators:           fake,
		NetworkID:            networkID,
		PingFrequency:        constants.DefaultPingFrequency,
		PongTimeout:          constants.DefaultPingPongTimeout,
		MaxClockDifference:   time.Minute,
		ResourceTracker:      rt,
		UptimeCalculator:     uptime.TestCalculator{},
		IPSigner:             peer.NewIPSigner(utils.NewAtomic(netip.AddrPortFrom(netip.IPv4Unspecified(), 9651)), tlsKey, blsKey),
	}
	p := peer.Start(cfg, conn, cert, peerID, peer.NewBlockingMessageQueue(metrics, logging.NoLog{}, 1024), false)
	if err := p.AwaitReady(ctx); err != nil {
		log.Fatalf("handshake: %v", err)
	}
	subs := p.TrackedSubnets()
	subList := make([]string, 0, subs.Len())
	for s := range subs {
		subList = append(subList, s.String())
	}
	fmt.Printf("CONNECTED %s ver=%s trackedSubnets=%v\n", p.ID(), p.Version(), subList)

	if *chainStr != "" {
		chainID, err := ids.FromString(*chainStr)
		if err != nil {
			log.Fatalf("bad -chain: %v", err)
		}
		floor(ctx, p, mc, chainID, *lo)
		p.StartClose()
		return
	}

	// Ask for ALL peers (AllSubnets=true), reaching the unexported Config on
	// the concrete peer via reflection like the 08_node_versions spike.
	kf, ks := sink.KnownPeers()
	if msg, err := mc.GetPeerList(kf, ks, true); err == nil {
		sctx, sc := context.WithTimeout(ctx, 3*time.Second)
		p.Send(sctx, msg)
		sc()
	}

	time.Sleep(*wait)
	p.StartClose()

	sink.mu.Lock()
	defer sink.mu.Unlock()
	fmt.Printf("GOSSIPED %d ips\n", len(sink.seen))
	var lookID ids.NodeID
	haveLook := false
	if *look != "" {
		if id, err := ids.NodeIDFromString(*look); err == nil {
			lookID, haveLook = id, true
		}
	}
	for nid, ap := range sink.seen {
		tag := ""
		if haveLook && nid == lookID {
			tag = "  <<< TARGET ARCHIVE NODE"
		}
		fmt.Printf("  %s %s%s\n", nid, ap, tag)
	}
	if haveLook {
		if _, ok := sink.seen[lookID]; !ok {
			fmt.Printf("TARGET %s was NOT gossiped by this validator\n", *look)
		}
	}
}

// ---- floor search ----

type chit struct {
	atHeight, accepted ids.ID
	acceptedHeight     uint64
}

var (
	chitMu sync.Mutex
	chits  = map[uint32]chan chit{}
)

func onInbound(_ context.Context, m *message.InboundMessage) {
	defer m.OnFinishedHandling()
	c, ok := m.Message.(*p2p.Chits)
	if !ok {
		return
	}
	pref, _ := ids.ToID(c.PreferredId)
	at := pref
	if len(c.PreferredIdAtHeight) > 0 {
		at, _ = ids.ToID(c.PreferredIdAtHeight)
	}
	acc, _ := ids.ToID(c.AcceptedId)
	chitMu.Lock()
	ch := chits[c.RequestId]
	chitMu.Unlock()
	if ch != nil {
		select {
		case ch <- chit{at, acc, c.AcceptedHeight}:
		default:
		}
	}
}

// has asks the peer for the block ID at height h. A peer that has pruned h
// answers with its own last accepted ID (the sendChits fallback), so
// "atHeight != accepted" means it still holds the height.
func has(ctx context.Context, p *peer.Peer, mc message.Creator, chainID ids.ID, reqID uint32, h uint64) (bool, uint64, error) {
	ch := make(chan chit, 1)
	chitMu.Lock()
	chits[reqID] = ch
	chitMu.Unlock()
	defer func() { chitMu.Lock(); delete(chits, reqID); chitMu.Unlock() }()
	msg, err := mc.PullQuery(chainID, reqID, 10*time.Second, ids.Empty, h)
	if err != nil {
		return false, 0, err
	}
	if !p.Send(ctx, msg) {
		return false, 0, fmt.Errorf("send failed")
	}
	select {
	case c := <-ch:
		return c.atHeight != c.accepted, c.acceptedHeight, nil
	case <-time.After(10 * time.Second):
		return false, 0, fmt.Errorf("timeout at height %d", h)
	}
}

func floor(ctx context.Context, p *peer.Peer, mc message.Creator, chainID ids.ID, lo uint64) {
	reqID := uint32(1)
	// First poll: learn the peer's accepted height (the search ceiling).
	ok, tip, err := has(ctx, p, mc, chainID, reqID, lo)
	if err != nil {
		log.Fatalf("poll: %v", err)
	}
	reqID++
	fmt.Printf("accepted_height=%d has(%d)=%v\n", tip, lo, ok)
	if ok {
		fmt.Printf("FLOOR <= %d (serves the lower bound)\n", lo)
		return
	}
	hi := tip
	// invariant: !has(lo), has(hi) assumed (it is the tip)
	for hi-lo > 1 {
		mid := lo + (hi-lo)/2
		ok, _, err := has(ctx, p, mc, chainID, reqID, mid)
		reqID++
		if err != nil {
			log.Printf("poll %d: %v (retrying)", mid, err)
			continue
		}
		if ok {
			hi = mid
		} else {
			lo = mid
		}
	}
	fmt.Printf("FLOOR=%d (lowest height served) tip=%d missing_below=%d\n", hi, tip, hi-1)
}

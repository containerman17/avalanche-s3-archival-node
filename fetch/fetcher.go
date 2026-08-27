package fetch

import (
	"context"
	"fmt"
	"log"
	"net/netip"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ava-labs/avalanchego/api/info"
	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/message"
	"github.com/ava-labs/avalanchego/network"
	"github.com/ava-labs/avalanchego/proto/pb/p2p"
	avacommon "github.com/ava-labs/avalanchego/snow/engine/common"
	"github.com/ava-labs/avalanchego/snow/validators"
	"github.com/ava-labs/avalanchego/staking"
	"github.com/ava-labs/avalanchego/subnets"
	"github.com/ava-labs/avalanchego/utils/compression"
	avaconstants "github.com/ava-labs/avalanchego/utils/constants"
	"github.com/ava-labs/avalanchego/utils/logging"
	"github.com/ava-labs/avalanchego/utils/set"
	"github.com/ava-labs/avalanchego/vms/platformvm"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/containerman17/avalanche-s3-archival-node/chain"
	"github.com/containerman17/avalanche-s3-archival-node/dist"
)

const (
	DefaultNodeURI = "https://api.avax-test.network"

	defaultPeerWarmup = 5 * time.Second
	// Archival peers answer ancient GetAncestors in 0.4-2s (measured);
	// anything slower than 5s is treated as a miss and the peer demoted.
	defaultRequestTimeout = 5 * time.Second
	// probeInterval paces the background re-discovery of archival peers.
	probeInterval = 15 * time.Second
	// probeFanout is how many non-archival peers each probe round asks.
	probeFanout = 3
	// frontierFanout is how many peers a frontier query asks.
	frontierFanout = 8
	// upstreamBudget is the ceiling on the WHOLE bootstrap RPC phase, retries
	// over every source included. Startup is allowed to be slow while a public
	// endpoint is rate-limiting this box; it is not allowed to be endless.
	upstreamBudget = 5 * time.Minute
)

// Config for opening a Fetcher.
type Config struct {
	// NodeURI is an Avalanche public node used only for bootstrap RPC
	// (validator list, peer IPs). A COMMA-SEPARATED LIST is several of them,
	// tried in turn: one rate-limited or broken host must not stop a node.
	// Empty falls back to DefaultNodeURI.
	NodeURI string
	// PerPeer is the max outstanding GetAncestors requests per archival
	// peer. 0 means 1.
	PerPeer int
	// VdrSources are platform RPC URIs used ONLY to load and cross-check
	// the validator set for consensus tip following (Follow). Empty falls
	// back to NodeURI alone, with a warning: the recorded ruling wants
	// multiple independent sources.
	VdrSources []string
	// Chain is the descriptor for an Avalanche L1. nil (or a descriptor whose
	// SubnetID is the primary network) keeps the C-chain path exactly as it
	// was: ids come from the bootstrap node's RPC, no tracked subnets, coreth.
	Chain *chain.Chain
}

// sources is NodeURI as the list it is: every bootstrap call tries them in
// turn (dist.Try), so a 429 from one endpoint costs a request, not a node.
func (c Config) sources() []string {
	if out := dist.Sources(c.NodeURI); len(out) > 0 {
		return out
	}
	return []string{DefaultNodeURI}
}

// vmKind is the libevm extras registration this config needs.
func (c Config) vmKind() chain.VMKind {
	if c.Chain != nil {
		return c.Chain.VMKind
	}
	return chain.Coreth
}

// l1 reports whether this config names a non-primary subnet, which is the one
// bit that changes the network wiring (tracked subnets, validator-driven IP
// discovery, the subnet every Send is keyed on).
func (c Config) l1() bool {
	return c.Chain != nil && c.Chain.SubnetID != avaconstants.PrimaryNetworkID
}

// Fetcher owns a P2P connection to one Avalanche chain and the ascending
// fetch over it. StartForward opens the RAM queue the executor reads;
// Follow runs the consensus follower that owns the real tip zone.
type Fetcher struct {
	cfg       Config
	q         *Queue
	net       network.Network
	handler   *inboundHandler
	pool      *peerPool
	creator   message.Creator
	networkID uint32
	chainID   ids.ID
	// subnetID is the subnet EVERY outbound chain message is keyed on. The
	// network drops a peer that does not track it, which is the correct
	// filter: for the C-chain it is the primary network, for an L1 its own
	// subnet.
	subnetID ids.ID

	dispatchErrCh chan error
	reqIDCounter  atomic.Uint32

	// bootstrapMu serialises the race-like probe round used when the
	// archival set is empty, so concurrent walks don't all fan out.
	bootstrapMu sync.Mutex
	// lastTip is the most recently requested container ID; the background
	// prober reuses it instead of fabricating ranges.
	lastTip atomic.Value // ids.ID

	// tip is the highest accepted height the network has reported: the
	// follower's own accepted head once it is live, and before that whatever
	// a Chits answer or the accepted frontier has already said. It is the
	// window rule's "real tip".
	tip atomic.Uint64
	// ceiling is --tip-override: the height forward fetch stops at, 0 when
	// following. syncTarget is the same number for reporting.
	ceiling    atomic.Uint64
	syncTarget atomic.Uint64

	// Stats for progress logging.
	requestsSent    atomic.Uint64
	answersTotal    atomic.Uint64
	answersNonEmpty atomic.Uint64
	pollsSent       atomic.Uint64
	prunedSeeds     atomic.Uint64
	badLinks        atomic.Uint64
	inFlight        atomic.Int64
}

// New dials the network named by cfg (the primary network's C-chain by
// default, an L1 when cfg.Chain names one) and waits for a peer to connect. It
// does NOT start fetching; call StartForward for that.
func New(cfg Config) (*Fetcher, error) {
	RegisterExtras(cfg.vmKind())
	if cfg.NodeURI == "" {
		cfg.NodeURI = DefaultNodeURI
	}
	f, err := dial(cfg)
	if err != nil {
		return nil, err
	}
	f.cfg = cfg
	return f, nil
}

// dial performs the network-only part of New: bootstrap RPC, P2P dial,
// peer warmup, genesis ID computation. Used directly by the spike probe.
func dial(cfg Config) (*Fetcher, error) {
	RegisterExtras(cfg.vmKind())
	if cfg.NodeURI == "" {
		cfg.NodeURI = DefaultNodeURI
	}

	// An L1's validators are not in the bootstrap node's peer list at all; they
	// arrive through ordinary PeerList gossip 10-15s after the primary peers
	// connect (measured on FIFA), so the dial budget is wider there.
	dialTimeout := 30 * time.Second
	if cfg.l1() {
		dialTimeout = 90 * time.Second
	}

	// THE BOOTSTRAP RPC GETS ITS OWN BUDGET, not the dial timeout: a public
	// endpoint that rate-limits this box is retried over every source (see
	// dist.Try), which is minutes in the worst case, while the p2p dial below
	// still has to fail in 30s when nothing answers.
	rpcCtx, rpcCancel := context.WithTimeout(context.Background(), upstreamBudget)
	defer rpcCancel()
	sources := cfg.sources()

	var (
		networkID uint32
		chainID   ids.ID
		subnetID  = avaconstants.PrimaryNetworkID
	)
	if cfg.Chain != nil {
		// THE DESCRIPTOR ALREADY KNOWS, so nothing is asked. A network ID is a
		// constant per network and the C-chain's blockchainID comes out of
		// avalanchego's embedded genesis, so the two calls that used to open
		// every chain's startup (info.getNetworkID, info.getBlockchainID) are
		// gone: they were two public-API requests per container per restart
		// for values that cannot change. A --node pointing at the wrong
		// network is no longer named by a startup error; it surfaces as the
		// peer-connect timeout below, because no peer of that network will
		// complete a handshake with us.
		networkID, chainID, subnetID = cfg.Chain.NetworkID, cfg.Chain.BlockchainID, cfg.Chain.SubnetID
	} else {
		if err := dist.Try(rpcCtx, "info.getNetworkID", sources, func(ctx context.Context, uri string) (err error) {
			networkID, err = info.NewClient(uri).GetNetworkID(ctx)
			return
		}); err != nil {
			return nil, err
		}
		if err := dist.Try(rpcCtx, "info.getBlockchainID(C)", sources, func(ctx context.Context, uri string) (err error) {
			chainID, err = info.NewClient(uri).GetBlockchainID(ctx, "C")
			return
		}); err != nil {
			return nil, err
		}
	}
	log.Printf("fetch: network_id=%d chain_id=%s subnet_id=%s", networkID, chainID, subnetID)

	var vdrList []platformvm.ClientPermissionlessValidator
	if err := dist.Try(rpcCtx, "platform.getCurrentValidators", sources, func(ctx context.Context, uri string) (err error) {
		vdrList, err = platformvm.NewClient(uri).GetCurrentValidators(ctx, subnetID, nil)
		return
	}); err != nil {
		return nil, err
	}
	validatorIDs := sortedNodeIDs(vdrList)
	log.Printf("fetch: validator_set_size=%d", len(validatorIDs))

	var peerInfos []info.Peer
	if err := dist.Try(rpcCtx, "info.peers", sources, func(ctx context.Context, uri string) error {
		p, err := discoverPeers(ctx, info.NewClient(uri), validatorIDs)
		if err != nil {
			return err
		}
		// An empty list is a source that cannot seed this dial, not a network
		// without peers: try the next one rather than dying on it.
		if len(p) == 0 {
			return fmt.Errorf("no peers returned from info.peers")
		}
		peerInfos = p
		return nil
	}); err != nil {
		return nil, err
	}
	log.Printf("fetch: peer_candidates=%d", len(peerInfos))

	// The pool holds the peers that can actually serve this chain. On the
	// primary network that is everything the bootstrap node knows. On an L1 it
	// is exactly the subnet's validators: no other peer tracks the subnet, and
	// the network would drop every message we addressed to one.
	peerIDs := set.NewSet[ids.NodeID](len(peerInfos))
	for _, p := range peerInfos {
		peerIDs.Add(p.ID)
	}
	if cfg.l1() {
		peerIDs = set.Of(validatorIDs...)
	}

	pool := newPeerPool()
	handler := newHandler(peerIDs, pool)

	// Tracking the subnet is what makes the handshake advertise it and the
	// network keep those peers. NEVER the primary ID: avalanchego rejects a
	// tracked set containing it (errTrackingPrimaryNetwork).
	tracked := set.Set[ids.ID]{}
	if cfg.l1() {
		tracked = set.Of(subnetID)
	}

	vdrs := &permissiveValidators{Manager: validators.NewManager()}
	netCfg, err := network.NewTestNetworkConfig(
		prometheus.NewRegistry(),
		networkID,
		vdrs,
		tracked,
	)
	if err != nil {
		return nil, fmt.Errorf("NewTestNetworkConfig: %w", err)
	}
	stakingCert, err := staking.ParseCertificate(netCfg.TLSConfig.Certificates[0].Leaf.Raw)
	if err != nil {
		return nil, fmt.Errorf("ParseCertificate: %w", err)
	}
	netCfg.MyNodeID = ids.NodeIDFromCert(stakingCert)

	net, err := network.NewTestNetwork(
		logging.NoLog{},
		prometheus.NewRegistry(),
		netCfg,
		handler,
	)
	if err != nil {
		return nil, fmt.Errorf("NewTestNetwork: %w", err)
	}

	creator, err := message.NewCreator(
		prometheus.NewRegistry(),
		compression.TypeZstd,
		avaconstants.DefaultNetworkMaximumInboundTimeout,
	)
	if err != nil {
		net.StartClose()
		return nil, fmt.Errorf("message.NewCreator: %w", err)
	}

	dispatchErrCh := make(chan error, 1)
	go func() {
		dispatchErrCh <- net.Dispatch()
	}()

	// Loading the L1's validators into the manager AFTER the network exists is
	// the whole mechanism: it fires ipTracker.OnValidatorAdded, so the network
	// starts wanting those nodes' signed IPs and dials them the moment ordinary
	// PeerList gossip carries one. Nothing else reaches an ACP-77 L1 validator,
	// which need not be a primary-network validator and is absent from every
	// public node's info.peers.
	if cfg.l1() {
		added := 0
		for _, v := range vdrList {
			validationID := v.TxID
			if v.ValidationID != nil {
				validationID = *v.ValidationID
			}
			if err := vdrs.AddStaker(subnetID, v.NodeID, nil, validationID, v.Weight); err == nil {
				added++
			}
		}
		log.Printf("fetch: registered %d/%d subnet validators for IP discovery", added, len(vdrList))
	}

	// The bootstrap node's peers are the gossip entry point either way: on an
	// L1 none of them tracks the subnet, they just carry the PeerList messages
	// that name the validators.
	for _, p := range peerInfos {
		net.ManuallyTrack(p.ID, peerAddr(p))
	}

	// The dial budget starts HERE, after the bootstrap RPC: a retried upstream
	// must not eat the time the peers get to connect.
	ctx, cancel := context.WithTimeout(context.Background(), dialTimeout)
	defer cancel()
	if err := waitForPeer(ctx, dispatchErrCh, handler.connectedCh, peerIDs, dialTimeout); err != nil {
		net.StartClose()
		return nil, fmt.Errorf("connect peer: %w", err)
	}
	if err := warmupPeers(ctx, dispatchErrCh, handler.connectedCh, peerIDs, defaultPeerWarmup); err != nil {
		net.StartClose()
		return nil, fmt.Errorf("peer warmup: %w", err)
	}

	return &Fetcher{
		net:           net,
		handler:       handler,
		pool:          pool,
		creator:       creator,
		networkID:     networkID,
		chainID:       chainID,
		subnetID:      subnetID,
		dispatchErrCh: dispatchErrCh,
	}, nil
}

// Close tears down the P2P network.
func (f *Fetcher) Close() error {
	f.net.StartClose()
	return nil
}

// Progress is a snapshot of fetch counters for logging.
type Progress struct {
	Head       uint64 // highest height in the RAM queue
	QueueBytes uint64 // containers held in RAM right now
	PeakBytes  uint64 // the run's high-water mark
	Requests   uint64 // GetAncestors requests sent
	Answers    uint64 // answers received (any content)
	NonEmpty   uint64 // answers carrying at least one container
	Polls      uint64 // height-resolution PullQuery polls sent
	// PrunedSeeds is how often a peer answered a height poll with its last
	// accepted block instead of the requested height, i.e. the documented
	// pruned-peer fallback, seen in the wild.
	PrunedSeeds uint64
	// BadLinks is spans discarded because a container did not link to the one
	// below it.
	BadLinks uint64
	Archival int   // current archival peer set size
	InFlight int64 // outstanding GetAncestors requests
	// Inbound messages dropped at the trust boundary, by reason. See
	// dropCounts: badPayload is a protocol mismatch, badID a malformed ID
	// field, noRoute an answer whose waiter was not ready for it.
	DropsBadPayload uint64
	DropsBadID      uint64
	DropsNoRoute    uint64
}

// DropSuffix is the drop counts as a log fragment, EMPTY when nothing has been
// dropped, so the normal progress line is unchanged and any non-zero count is
// visible where the operator already looks. Without it a protocol bump reads
// as peers timing out.
func (p Progress) DropSuffix() string {
	if p.DropsBadPayload|p.DropsBadID|p.DropsNoRoute == 0 {
		return ""
	}
	return fmt.Sprintf(" drops=payload:%d,id:%d,route:%d",
		p.DropsBadPayload, p.DropsBadID, p.DropsNoRoute)
}

func (f *Fetcher) Progress() Progress {
	p := Progress{
		Requests:    f.requestsSent.Load(),
		Answers:     f.answersTotal.Load(),
		NonEmpty:    f.answersNonEmpty.Load(),
		Polls:       f.pollsSent.Load(),
		PrunedSeeds: f.prunedSeeds.Load(),
		BadLinks:    f.badLinks.Load(),
		Archival:    f.pool.archivalCount(),
		InFlight:    f.inFlight.Load(),

		DropsBadPayload: f.handler.drops.badPayload.Load(),
		DropsBadID:      f.handler.drops.badID.Load(),
		DropsNoRoute:    f.handler.drops.noRoute.Load(),
	}
	if f.q != nil {
		p.Head = f.q.Head()
		p.QueueBytes, p.PeakBytes = f.q.Bytes()
	}
	return p
}

// ---- peer selection ----
//
// ~15 Fuji peers are stable archival peers that answer every ancient
// GetAncestors non-empty in 0.4-2s; the rest answer empty fast or never.
// We keep a self-managed runtime set of archival peers (answered non-empty
// recently), give each span fetch the least-busy archival peer with one
// outstanding request per peer by default, and demote peers on an empty
// answer or timeout. A background probe round over non-archival peers
// (reusing the last requested container) discovers new archival peers and
// re-promotes demoted ones; when the set is empty an initial race-like round
// over all connected peers bootstraps it.

type peerState struct {
	archival bool
	busy     int
	// rate is an EWMA of blocks/sec per answered request; acquire uses it
	// as a tie-break so a single walk sticks to the fastest peer.
	rate float64
}

type peerPool struct {
	mu       sync.Mutex
	peers    map[ids.NodeID]*peerState
	archival int
}

func newPeerPool() *peerPool {
	return &peerPool{peers: make(map[ids.NodeID]*peerState)}
}

func (p *peerPool) connected(id ids.NodeID) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.peers[id]; !ok {
		p.peers[id] = &peerState{}
	}
}

func (p *peerPool) disconnected(id ids.NodeID) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if s, ok := p.peers[id]; ok {
		if s.archival {
			p.archival--
		}
		delete(p.peers, id)
	}
}

// acquire returns the least-busy archival peer with capacity, incrementing
// its busy count. ok=false with haveArchival=false means the archival set
// is empty (caller should bootstrap); ok=false with haveArchival=true means
// all archival peers are at capacity (caller should wait and retry).
func (p *peerPool) acquire(maxBusy int) (peer ids.NodeID, ok, haveArchival bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	best := (*peerState)(nil)
	for id, s := range p.peers {
		if !s.archival {
			continue
		}
		haveArchival = true
		if s.busy >= maxBusy {
			continue
		}
		if best == nil || s.busy < best.busy ||
			(s.busy == best.busy && s.rate > best.rate) {
			best, peer = s, id
		}
	}
	if best == nil {
		return ids.EmptyNodeID, false, haveArchival
	}
	best.busy++
	return peer, true, true
}

func (p *peerPool) release(id ids.NodeID) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if s, ok := p.peers[id]; ok && s.busy > 0 {
		s.busy--
	}
}

func (p *peerPool) setArchival(id ids.NodeID, archival bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if s, ok := p.peers[id]; ok && s.archival != archival {
		s.archival = archival
		if archival {
			p.archival++
		} else {
			p.archival--
		}
	}
}

// observe folds a successful answer into the peer's rate EWMA.
func (p *peerPool) observe(id ids.NodeID, blocks int, elapsed time.Duration) {
	sec := elapsed.Seconds()
	if sec < 1e-3 {
		sec = 1e-3
	}
	r := float64(blocks) / sec
	p.mu.Lock()
	defer p.mu.Unlock()
	if s, ok := p.peers[id]; ok {
		if s.rate == 0 {
			s.rate = r
		} else {
			s.rate = 0.8*s.rate + 0.2*r
		}
	}
}

func (p *peerPool) isConnected(id ids.NodeID) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	_, ok := p.peers[id]
	return ok
}

func (p *peerPool) archivalCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.archival
}

// peersForFrontier returns up to n connected peers, archival first: any
// peer answers GetAcceptedFrontier, archival ones are just known-alive.
func (p *peerPool) peersForFrontier(n int) []ids.NodeID {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]ids.NodeID, 0, n)
	for id, s := range p.peers {
		if s.archival && len(out) < n {
			out = append(out, id)
		}
	}
	for id, s := range p.peers {
		if !s.archival && len(out) < n {
			out = append(out, id)
		}
	}
	return out
}

// nonArchival returns up to n non-archival peers (map order, effectively
// random).
func (p *peerPool) nonArchival(n int) []ids.NodeID {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]ids.NodeID, 0, n)
	for id, s := range p.peers {
		if s.archival {
			continue
		}
		out = append(out, id)
		if len(out) == n {
			break
		}
	}
	return out
}

// request sends one GetAncestors to peer and waits up to
// defaultRequestTimeout for the answer. ok=false means error or timeout;
// local=true means the failure happened inside this process and the peer
// never saw a message, so it must not be blamed for it.
func (f *Fetcher) request(ctx context.Context, peer ids.NodeID, tip ids.ID) (resp ancestorsResponse, ok, local bool) {
	reqID := f.reqIDCounter.Add(1)
	ch := make(chan ancestorsResponse, 1)
	f.handler.registerRoute(reqID, ch)
	defer f.handler.unregisterRoute(reqID)

	msg, err := f.creator.GetAncestors(f.chainID, reqID, defaultRequestTimeout, tip, p2p.EngineType_ENGINE_TYPE_CHAIN)
	if err != nil {
		logLocalBug("GetAncestors", err)
		return ancestorsResponse{}, false, true
	}

	f.requestsSent.Add(1)
	f.inFlight.Add(1)
	defer f.inFlight.Add(-1)
	to := set.Of(peer)
	noteSendGap("GetAncestors", to, f.net.Send(msg, avacommon.SendConfig{NodeIDs: to}, f.subnetID, subnets.NoOpAllower))

	t := time.NewTimer(defaultRequestTimeout)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ancestorsResponse{}, false, false
	case <-t.C:
		return ancestorsResponse{}, false, false
	case resp := <-ch:
		f.answersTotal.Add(1)
		if len(resp.blocks) > 0 && len(resp.blocks[0]) > 0 {
			f.answersNonEmpty.Add(1)
		}
		return resp, true, false
	}
}

// logLocalBug reports a failure that is entirely ours ONCE per kind. Message
// creation failing is deterministic, so logging it per request would bury the
// log, and blaming the peer it was addressed to (which never saw anything)
// demotes every peer in turn until archival drains to zero and the operator
// chases a network problem that does not exist.
func logLocalBug(op string, err error) {
	if _, seen := localBugSeen.LoadOrStore(op, struct{}{}); seen {
		return
	}
	log.Printf("fetch: BUG: %s could not be built locally, no peer is at fault: %v", op, err)
}

var localBugSeen sync.Map // op -> struct{}

// sendGapInterval throttles noteSendGap per op. A saturated outbound queue
// fails every poll, so an unthrottled line would be one per poll.
const sendGapInterval = 30 * time.Second

var sendGapLast sync.Map // op -> *atomic.Int64 (unix nanos of the last line)

// noteSendGap reports the peers a Send did not actually reach. Send returns
// the set it really wrote to, and the difference against the set we asked for
// is the difference between a request that went out and timed out on the wire
// and one that NEVER LEFT THIS BOX (peer not connected, or its outbound queue
// full). Without it the second case costs the full request timeout and then
// demotes a peer that was never asked anything; in the consensus path it is
// the difference between a poll that failed and a poll that was never sent.
//
// Diagnosis only: nothing here retries. A send that did not happen is already
// handled by the timeout above it.
func noteSendGap(op string, want, sent set.Set[ids.NodeID]) {
	if sent.Len() >= want.Len() {
		return
	}
	v, _ := sendGapLast.LoadOrStore(op, new(atomic.Int64))
	last := v.(*atomic.Int64)
	prev, now := last.Load(), time.Now().UnixNano()
	if now-prev < int64(sendGapInterval) || !last.CompareAndSwap(prev, now) {
		return
	}
	var missing []ids.NodeID
	for id := range want {
		if !sent.Contains(id) {
			missing = append(missing, id)
		}
	}
	log.Printf("fetch: %s reached %d/%d peers, the rest never left this box (not connected, or the outbound queue is full): %v",
		op, sent.Len(), want.Len(), missing)
}

// nonEmpty reports whether resp carries at least one container.
func nonEmpty(resp ancestorsResponse) bool {
	return len(resp.blocks) > 0 && len(resp.blocks[0]) > 0
}

// fetchAncestors picks an archival peer and asks it for tip, demoting the
// peer and retrying on another one when it answers empty or times out.
// With an empty archival set it falls back to a bootstrap round.
func (f *Fetcher) fetchAncestors(ctx context.Context, tip ids.ID) (ancestorsResponse, ids.NodeID, bool) {
	f.lastTip.Store(tip)
	for {
		if ctx.Err() != nil {
			return ancestorsResponse{}, ids.EmptyNodeID, false
		}
		peer, ok, haveArchival := f.pool.acquire(f.maxPerPeer())
		if !ok {
			if !haveArchival {
				if resp, p, got := f.bootstrapRound(ctx, tip); got {
					return resp, p, true
				}
			} else {
				// All archival peers saturated; wait for a slot.
				time.Sleep(20 * time.Millisecond)
			}
			continue
		}
		start := time.Now()
		resp, got, local := f.request(ctx, peer, tip)
		f.pool.release(peer)
		if local {
			// Nothing left this process, so the peer keeps its standing. The
			// pause keeps a deterministic local failure from spinning.
			time.Sleep(100 * time.Millisecond)
			continue
		}
		if !got || !nonEmpty(resp) {
			f.pool.setArchival(peer, false)
			continue
		}
		f.pool.observe(peer, len(resp.blocks), time.Since(start))
		return resp, peer, true
	}
}

func (f *Fetcher) maxPerPeer() int {
	if f.cfg.PerPeer > 0 {
		return f.cfg.PerPeer
	}
	return 1
}

// bootstrapRound fans tip out to every connected non-archival peer at once,
// promotes every peer that answers non-empty, and returns the first useful
// response. Serialised so concurrent walks don't all fan out.
func (f *Fetcher) bootstrapRound(ctx context.Context, tip ids.ID) (ancestorsResponse, ids.NodeID, bool) {
	f.bootstrapMu.Lock()
	defer f.bootstrapMu.Unlock()
	if f.pool.archivalCount() > 0 {
		return ancestorsResponse{}, ids.EmptyNodeID, false // someone else bootstrapped; re-acquire
	}
	// Capped fanout: peers throttle a zero-weight non-validator hard, and a
	// full-network burst (582 peers x ~1.6MB on mainnet) got every response
	// stream cut off for minutes, stalling consensus polls (measured live
	// 2026-07-18). 64 concurrent probes bootstrap plenty of archival peers.
	peers := f.pool.nonArchival(64)
	if len(peers) == 0 {
		time.Sleep(200 * time.Millisecond)
		return ancestorsResponse{}, ids.EmptyNodeID, false
	}
	log.Printf("fetch: bootstrap probe round over %d peers", len(peers))
	type outcome struct {
		peer ids.NodeID
		resp ancestorsResponse
		good bool
	}
	results := make(chan outcome, len(peers))
	for _, p := range peers {
		go func(p ids.NodeID) {
			resp, got, _ := f.request(ctx, p, tip)
			results <- outcome{peer: p, resp: resp, good: got && nonEmpty(resp)}
		}(p)
	}
	var best outcome
	for range peers {
		o := <-results
		if !o.good {
			continue
		}
		f.pool.setArchival(o.peer, true)
		if !best.good {
			best = o
		}
	}
	log.Printf("fetch: bootstrap round done, archival=%d", f.pool.archivalCount())
	return best.resp, best.peer, best.good
}

// probeLoop periodically asks a few non-archival peers for the most recently
// requested container to discover new archival peers and re-promote demoted
// ones. Responses are used only for classification; the span fetch that asked
// for it keeps its own copy.
func (f *Fetcher) probeLoop(ctx context.Context) {
	t := time.NewTicker(probeInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		tip, ok := f.lastTip.Load().(ids.ID)
		if !ok || f.pool.archivalCount() == 0 {
			continue // nothing pending yet, or bootstrap round will run anyway
		}
		var wg sync.WaitGroup
		for _, p := range f.pool.nonArchival(probeFanout) {
			wg.Add(1)
			go func(p ids.NodeID) {
				defer wg.Done()
				if resp, got, _ := f.request(ctx, p, tip); got && nonEmpty(resp) {
					f.pool.setArchival(p, true)
				}
			}(p)
		}
		wg.Wait()
	}
}

// acceptedFrontier asks frontierFanout peers for the chain's accepted
// frontier and returns the container ID named by the most peers. Peers may
// disagree by a block or two as the frontier advances; any answer is a
// valid walk anchor.
func (f *Fetcher) acceptedFrontier(ctx context.Context) (ids.ID, error) {
	peers := f.pool.peersForFrontier(frontierFanout)
	if len(peers) == 0 {
		return ids.Empty, fmt.Errorf("no connected peers")
	}
	results := make(chan ids.ID, len(peers))
	for _, peer := range peers {
		go func(peer ids.NodeID) {
			reqID := f.reqIDCounter.Add(1)
			ch := make(chan ids.ID, 1)
			f.handler.registerFrontierRoute(reqID, ch)
			defer f.handler.unregisterFrontierRoute(reqID)
			msg, err := f.creator.GetAcceptedFrontier(f.chainID, reqID, defaultRequestTimeout)
			if err != nil {
				logLocalBug("GetAcceptedFrontier", err)
				results <- ids.Empty
				return
			}
			to := set.Of(peer)
			noteSendGap("GetAcceptedFrontier", to, f.net.Send(msg, avacommon.SendConfig{NodeIDs: to}, f.subnetID, subnets.NoOpAllower))
			t := time.NewTimer(defaultRequestTimeout)
			defer t.Stop()
			select {
			case <-ctx.Done():
				results <- ids.Empty
			case <-t.C:
				results <- ids.Empty
			case id := <-ch:
				results <- id
			}
		}(peer)
	}
	votes := make(map[ids.ID]int, len(peers))
	var best ids.ID
	for range peers {
		id := <-results
		if id == ids.Empty {
			continue
		}
		votes[id]++
		if votes[id] > votes[best] {
			best = id
		}
	}
	if best == ids.Empty {
		return ids.Empty, fmt.Errorf("no frontier answers from %d peers", len(peers))
	}
	return best, nil
}

// ---- small helpers ----

func sortedNodeIDs(list []platformvm.ClientPermissionlessValidator) []ids.NodeID {
	seen := set.NewSet[ids.NodeID](len(list))
	out := make([]ids.NodeID, 0, len(list))
	for _, v := range list {
		if seen.Contains(v.NodeID) {
			continue
		}
		seen.Add(v.NodeID)
		out = append(out, v.NodeID)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].String() < out[j].String() })
	return out
}

// discoverPeers returns the bootstrap node's peers, preferring the ones that
// are validators of our subnet. Zero matches is NORMAL for an L1 (post-ACP-77
// its validators need not validate the primary network and no public node's
// info.peers lists them), so it falls back to the full peer list, which is only
// ever the gossip entry point.
func discoverPeers(ctx context.Context, c *info.Client, validatorIDs []ids.NodeID) ([]info.Peer, error) {
	if len(validatorIDs) > 0 {
		peers, err := c.Peers(ctx, validatorIDs)
		if err == nil && len(peers) > 0 {
			sort.Slice(peers, func(i, j int) bool { return peers[i].ID.String() < peers[j].ID.String() })
			return peers, nil
		}
		log.Printf("fetch: none of the %d subnet validators are in the bootstrap node's peer list, using it for gossip only", len(validatorIDs))
	}
	peers, err := c.Peers(ctx, nil)
	if err != nil {
		return nil, err
	}
	sort.Slice(peers, func(i, j int) bool { return peers[i].ID.String() < peers[j].ID.String() })
	return peers, nil
}

func peerAddr(p info.Peer) netip.AddrPort {
	if p.PublicIP.IsValid() {
		return p.PublicIP
	}
	return p.IP
}

func waitForPeer(ctx context.Context, errCh <-chan error, connCh <-chan ids.NodeID, allowed set.Set[ids.NodeID], timeout time.Duration) error {
	t := time.NewTimer(timeout)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-errCh:
			return fmt.Errorf("network stopped: %w", err)
		case <-t.C:
			return fmt.Errorf("timed out after %s", timeout)
		case id := <-connCh:
			if allowed.Contains(id) {
				return nil
			}
		}
	}
}

// warmupPeers collects connections for window, and RETURNS THE DISPATCH ERROR
// rather than eating it. errCh is buffered 1 and written exactly once, so a
// receive here that threw the value away left every later select on it (the
// walk, FollowTip, Follow) unable to ever fire: the node came up, ran against
// a dead network forever, and the one error explaining why was gone.
func warmupPeers(ctx context.Context, errCh <-chan error, connCh <-chan ids.NodeID, allowed set.Set[ids.NodeID], window time.Duration) error {
	t := time.NewTimer(window)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			return nil
		case err := <-errCh:
			return fmt.Errorf("network stopped: %w", err)
		case <-connCh:
		}
	}
}

// permissiveValidators is a ValidatorManager that answers "yes" to every
// membership check so our transient node accepts messages from any peer.
type permissiveValidators struct {
	validators.Manager
}

func (*permissiveValidators) Contains(ids.ID, ids.NodeID) bool { return true }

package fetch

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/netip"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ava-labs/avalanchego/api/info"
	"github.com/ava-labs/avalanchego/genesis"
	corethcore "github.com/ava-labs/avalanchego/graft/coreth/core"
	cparams "github.com/ava-labs/avalanchego/graft/coreth/params"
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
)

const (
	DefaultNodeURI  = "https://api.avax-test.network"
	primarySubnetID = "11111111111111111111111111111111LpoYY"

	defaultConnectTimeout = 30 * time.Second
	defaultPeerWarmup     = 5 * time.Second
	// Archival peers answer ancient GetAncestors in 0.4-2s (measured);
	// anything slower than 5s is treated as a miss and the peer demoted.
	defaultRequestTimeout = 5 * time.Second
	// probeInterval paces the background re-discovery of archival peers.
	probeInterval = 15 * time.Second
	// probeFanout is how many non-archival peers each probe round asks.
	probeFanout = 3
	// tipPollInterval paces GetAcceptedFrontier polling in FollowTip mode.
	tipPollInterval = 30 * time.Second
	// frontierFanout is how many peers a frontier query asks.
	frontierFanout = 8
)

// Config for opening a Fetcher.
type Config struct {
	// DataDir is the on-disk location for the flat-file store holding raw
	// containers (arrival.log + index.log).
	DataDir string
	// NodeURI is an Avalanche public node used only for bootstrap RPC
	// (network ID, C-Chain ID, validator list, peer IPs). If empty, falls
	// back to DefaultNodeURI.
	NodeURI string
	// PerPeer is the max outstanding GetAncestors requests per archival
	// peer. 0 means 1.
	PerPeer int
	// VdrSources are platform RPC URIs used ONLY to load and cross-check
	// the validator set for consensus tip following (Follow). Empty falls
	// back to NodeURI alone, with a warning: the recorded ruling wants
	// multiple independent sources.
	VdrSources []string
}

// Fetcher owns a flat-file store of raw C-Chain containers and a P2P
// connection to the Avalanche Fuji network. Start by calling Sync to
// populate the store backward from the embedded checkpoints; read with
// the Store methods once Sync has finished (or in parallel, over whatever
// range is already stored).
type Fetcher struct {
	cfg       Config
	store     *Store
	net       network.Network
	handler   *inboundHandler
	pool      *peerPool
	creator   message.Creator
	networkID uint32
	chainID   ids.ID

	// Genesis block of the C-Chain is not served over GetAncestors, so we
	// short-circuit the walk when we hit its container ID.
	genesisID ids.ID

	dispatchErrCh chan error
	reqIDCounter  atomic.Uint32

	// bootstrapMu serialises the race-like probe round used when the
	// archival set is empty, so concurrent walks don't all fan out.
	bootstrapMu sync.Mutex
	// lastTip is the most recently requested container ID; the background
	// prober reuses it instead of fabricating ranges.
	lastTip atomic.Value // ids.ID

	// Stats for progress logging.
	requestsSent    atomic.Uint64
	answersTotal    atomic.Uint64
	answersNonEmpty atomic.Uint64
	activeWalks     atomic.Int64
	inFlight        atomic.Int64
}

// New opens the flat-file store, dials the Fuji primary network, and waits
// for a peer to connect. It does NOT start syncing; call Sync for that.
func New(cfg Config) (*Fetcher, error) {
	registerExtras()

	if cfg.NodeURI == "" {
		cfg.NodeURI = DefaultNodeURI
	}
	if cfg.DataDir == "" {
		return nil, fmt.Errorf("config: DataDir required")
	}

	store, err := OpenStore(cfg.DataDir)
	if err != nil {
		return nil, fmt.Errorf("open store: %w", err)
	}

	f, err := dial(cfg)
	if err != nil {
		store.Close()
		return nil, err
	}
	f.cfg = cfg
	f.store = store
	return f, nil
}

// dial performs the network-only part of New: bootstrap RPC, P2P dial,
// peer warmup, genesis ID computation. Used directly by the spike probe.
func dial(cfg Config) (*Fetcher, error) {
	registerExtras()
	if cfg.NodeURI == "" {
		cfg.NodeURI = DefaultNodeURI
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	infoClient := info.NewClient(cfg.NodeURI)
	pClient := platformvm.NewClient(cfg.NodeURI)

	networkID, err := infoClient.GetNetworkID(ctx)
	if err != nil {
		return nil, fmt.Errorf("info.getNetworkID: %w", err)
	}
	chainID, err := infoClient.GetBlockchainID(ctx, "C")
	if err != nil {
		return nil, fmt.Errorf("info.getBlockchainID(C): %w", err)
	}
	log.Printf("fetch: network_id=%d c_chain_id=%s", networkID, chainID)

	subnetID, err := ids.FromString(primarySubnetID)
	if err != nil {
		return nil, fmt.Errorf("parse primary subnet: %w", err)
	}
	validatorIDs, err := loadValidatorIDs(ctx, pClient, subnetID)
	if err != nil {
		return nil, fmt.Errorf("get validators: %w", err)
	}
	log.Printf("fetch: validator_set_size=%d", len(validatorIDs))

	peerInfos, err := discoverPeers(ctx, infoClient, validatorIDs)
	if err != nil {
		return nil, fmt.Errorf("discover peers: %w", err)
	}
	if len(peerInfos) == 0 {
		return nil, fmt.Errorf("no peers returned from info.peers")
	}
	log.Printf("fetch: peer_candidates=%d", len(peerInfos))

	peerIDs := set.NewSet[ids.NodeID](len(peerInfos))
	for _, p := range peerInfos {
		peerIDs.Add(p.ID)
	}

	pool := newPeerPool()
	handler := newHandler(peerIDs, pool)

	vdrs := &permissiveValidators{Manager: validators.NewManager()}
	netCfg, err := network.NewTestNetworkConfig(
		prometheus.NewRegistry(),
		networkID,
		vdrs,
		set.Set[ids.ID]{},
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

	for _, p := range peerInfos {
		net.ManuallyTrack(p.ID, peerAddr(p))
	}

	if err := waitForPeer(ctx, dispatchErrCh, handler.connectedCh, peerIDs, defaultConnectTimeout); err != nil {
		net.StartClose()
		return nil, fmt.Errorf("connect peer: %w", err)
	}
	warmupPeers(ctx, dispatchErrCh, handler.connectedCh, peerIDs, defaultPeerWarmup)

	genesisID, err := computeGenesisID(networkID)
	if err != nil {
		net.StartClose()
		return nil, fmt.Errorf("compute genesis id: %w", err)
	}
	log.Printf("fetch: genesis_container_id=%s", genesisID)

	return &Fetcher{
		net:           net,
		handler:       handler,
		pool:          pool,
		creator:       creator,
		networkID:     networkID,
		chainID:       chainID,
		genesisID:     genesisID,
		dispatchErrCh: dispatchErrCh,
	}, nil
}

// computeGenesisID reads the embedded C-Chain genesis for the network from
// avalanchego's genesis package and returns the container ID (which for
// the pre-ProposerVM genesis equals the eth block hash). Requires
// cparams.SetEthUpgrades so Genesis.ToBlock can resolve precompile
// activations on the chain config.
func computeGenesisID(networkID uint32) (ids.ID, error) {
	cfg := genesis.GetConfig(networkID)
	if cfg == nil {
		return ids.Empty, fmt.Errorf("no embedded genesis config for network %d", networkID)
	}
	var g corethcore.Genesis
	if err := json.Unmarshal([]byte(cfg.CChainGenesis), &g); err != nil {
		return ids.Empty, err
	}
	if err := cparams.SetEthUpgrades(g.Config); err != nil {
		return ids.Empty, fmt.Errorf("set eth upgrades: %w", err)
	}
	return ids.ID(g.ToBlock().Hash()), nil
}

// Close tears down the P2P network and closes the store.
func (f *Fetcher) Close() error {
	f.net.StartClose()
	if f.store == nil {
		return nil
	}
	return f.store.Close()
}

// Store exposes the underlying flat-file store for readers.
func (f *Fetcher) Store() *Store { return f.store }

// WalkFrom walks backward from an arbitrary container ID down to block 0
// (short-circuiting over already-stored contiguous runs), storing every
// container on the way. Used to backfill a specific historical range:
// pre-ProposerVM container IDs equal the eth block hash, so any early
// block hash from a trusted source is a valid anchor.
func (f *Fetcher) WalkFrom(ctx context.Context, tip ids.ID) error {
	f.activeWalks.Add(1)
	defer f.activeWalks.Add(-1)
	return f.walkSpan(ctx, tip, 0)
}

// ---- peer selection ----
//
// ~15 Fuji peers are stable archival peers that answer every ancient
// GetAncestors non-empty in 0.4-2s; the rest answer empty fast or never.
// We keep a self-managed runtime set of archival peers (answered non-empty
// recently), give each walk the least-busy archival peer with one
// outstanding request per peer by default, and demote peers on an empty
// answer or timeout. A background probe round over non-archival peers
// (reusing the last walk tip) discovers new archival peers and re-promotes
// demoted ones; when the set is empty an initial race-like round over all
// connected peers bootstraps it.

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
// defaultRequestTimeout for the answer. ok=false means error or timeout.
func (f *Fetcher) request(ctx context.Context, peer ids.NodeID, tip ids.ID) (ancestorsResponse, bool) {
	reqID := f.reqIDCounter.Add(1)
	ch := make(chan ancestorsResponse, 1)
	f.handler.registerRoute(reqID, ch)
	defer f.handler.unregisterRoute(reqID)

	msg, err := f.creator.GetAncestors(f.chainID, reqID, defaultRequestTimeout, tip, p2p.EngineType_ENGINE_TYPE_CHAIN)
	if err != nil {
		return ancestorsResponse{}, false
	}

	f.requestsSent.Add(1)
	f.inFlight.Add(1)
	defer f.inFlight.Add(-1)
	f.net.Send(msg, avacommon.SendConfig{NodeIDs: set.Of(peer)}, avaconstants.PrimaryNetworkID, subnets.NoOpAllower)

	t := time.NewTimer(defaultRequestTimeout)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ancestorsResponse{}, false
	case <-t.C:
		return ancestorsResponse{}, false
	case resp := <-ch:
		f.answersTotal.Add(1)
		if len(resp.blocks) > 0 && len(resp.blocks[0]) > 0 {
			f.answersNonEmpty.Add(1)
		}
		return resp, true
	}
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
		resp, got := f.request(ctx, peer, tip)
		f.pool.release(peer)
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
	peers := f.pool.nonArchival(1 << 30)
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
			resp, got := f.request(ctx, p, tip)
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

// probeLoop periodically asks a few non-archival peers the most recent walk
// tip to discover new archival peers and re-promote demoted ones. Responses
// are used only for classification; the walk that owns the tip stores its
// own copy.
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
				if resp, got := f.request(ctx, p, tip); got && nonEmpty(resp) {
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
				results <- ids.Empty
				return
			}
			f.net.Send(msg, avacommon.SendConfig{NodeIDs: set.Of(peer)}, avaconstants.PrimaryNetworkID, subnets.NoOpAllower)
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

// FollowTip anchors at the network's accepted frontier and walks backward
// with the archival-peer pipeline until it connects to already-stored
// history, then keeps polling the frontier every tipPollInterval and
// backfilling from each new frontier, so the store continuously tracks the
// live tip.
func (f *Fetcher) FollowTip(ctx context.Context) error {
	go f.probeLoop(ctx)
	var floor uint64 // first walk connects to the checkpoint-synced history
	for {
		select {
		case err := <-f.dispatchErrCh:
			return fmt.Errorf("network stopped: %w", err)
		default:
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		tipID, err := f.acceptedFrontier(ctx)
		if err != nil {
			log.Printf("fetch: accepted frontier: %v", err)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(5 * time.Second):
			}
			continue
		}
		parsed, err := f.getContainer(ctx, tipID)
		if err != nil {
			return fmt.Errorf("fetch frontier container %s: %w", tipID, err)
		}
		tipH := parsed.blockNumber
		if behind := int64(tipH) + 1 - int64(f.store.Count()); behind > 0 {
			log.Printf("fetch: tip height=%d gap=%d blocks behind", tipH, behind)
		}
		f.activeWalks.Add(1)
		err = f.walkSpan(ctx, tipID, floor)
		f.activeWalks.Add(-1)
		if err != nil {
			return err
		}
		floor = tipH
		log.Printf("fetch: caught up to tip height=%d stored=%d", tipH, f.store.Count())
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(tipPollInterval):
		}
	}
}

// ---- small helpers ----

func loadValidatorIDs(ctx context.Context, c *platformvm.Client, subnetID ids.ID) ([]ids.NodeID, error) {
	list, err := c.GetCurrentValidators(ctx, subnetID, nil)
	if err != nil {
		return nil, err
	}
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
	return out, nil
}

func discoverPeers(ctx context.Context, c *info.Client, validatorIDs []ids.NodeID) ([]info.Peer, error) {
	if len(validatorIDs) > 0 {
		peers, err := c.Peers(ctx, validatorIDs)
		if err == nil && len(peers) > 0 {
			sort.Slice(peers, func(i, j int) bool { return peers[i].ID.String() < peers[j].ID.String() })
			return peers, nil
		}
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

func warmupPeers(ctx context.Context, errCh <-chan error, connCh <-chan ids.NodeID, allowed set.Set[ids.NodeID], window time.Duration) {
	t := time.NewTimer(window)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			return
		case <-errCh:
			return
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

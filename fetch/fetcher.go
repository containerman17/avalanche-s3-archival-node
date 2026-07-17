package fetch

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/netip"
	"sort"
	"sync/atomic"
	"time"

	"github.com/ava-labs/avalanchego/api/info"
	"github.com/ava-labs/avalanchego/genesis"
	corethcore "github.com/ava-labs/avalanchego/graft/coreth/core"
	cparams "github.com/ava-labs/avalanchego/graft/coreth/params"
	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/message"
	"github.com/ava-labs/avalanchego/network"
	avap2p "github.com/ava-labs/avalanchego/network/p2p"
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
	defaultRequestTimeout = 20 * time.Second
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
	tracker   *avap2p.PeerTracker
	creator   message.Creator
	networkID uint32
	chainID   ids.ID

	// Genesis block of the C-Chain is not served over GetAncestors, so we
	// short-circuit the walk when we hit its container ID.
	genesisID ids.ID

	dispatchErrCh chan error
	reqIDCounter  atomic.Uint32

	// Stats for progress logging.
	requestsSent    atomic.Uint64
	answersNonEmpty atomic.Uint64
	activeWalks     atomic.Int64
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

	tracker, err := avap2p.NewPeerTracker(
		logging.NoLog{},
		"fetch",
		prometheus.NewRegistry(),
		set.Set[ids.NodeID]{},
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("NewPeerTracker: %w", err)
	}
	handler := newHandler(peerIDs, tracker)

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
		tracker:       tracker,
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

// parallelRequests controls how many concurrent GetAncestors requests we
// fan out to different peers for the same tip. Many peers have pruned
// ancient C-Chain history and respond with an empty container list;
// racing a handful of peers dramatically shortens the time to find one
// that actually has the data.
const parallelRequests = 8

// raceAncestors fans out parallelRequests GetAncestors calls for tip to
// different peers, returning the first non-empty response. Peers that
// answer empty or time out are penalised via the PeerTracker.
func (f *Fetcher) raceAncestors(ctx context.Context, tip ids.ID) (ancestorsResponse, ids.NodeID, bool) {
	rctx, cancel := context.WithTimeout(ctx, defaultRequestTimeout)
	defer cancel()

	results := make(chan raceOutcome, parallelRequests)

	picked := make(map[ids.NodeID]struct{}, parallelRequests)
	launched := 0
	for launched < parallelRequests {
		peer, ok := f.tracker.SelectPeer()
		if !ok {
			break
		}
		if _, dup := picked[peer]; dup {
			continue
		}
		picked[peer] = struct{}{}
		launched++
		go f.singleRequest(rctx, peer, tip, results)
	}
	if launched == 0 {
		time.Sleep(100 * time.Millisecond)
		return ancestorsResponse{}, ids.EmptyNodeID, false
	}

	for i := 0; i < launched; i++ {
		select {
		case <-ctx.Done():
			return ancestorsResponse{}, ids.EmptyNodeID, false
		case o := <-results:
			if !o.ok || len(o.resp.blocks) == 0 || len(o.resp.blocks[0]) == 0 {
				f.tracker.RegisterFailure(o.peer)
				continue
			}
			return o.resp, o.peer, true
		}
	}
	return ancestorsResponse{}, ids.EmptyNodeID, false
}

type raceOutcome struct {
	peer ids.NodeID
	resp ancestorsResponse
	ok   bool
}

func (f *Fetcher) singleRequest(ctx context.Context, peer ids.NodeID, tip ids.ID, results chan<- raceOutcome) {
	reqID := f.reqIDCounter.Add(1)
	ch := make(chan ancestorsResponse, 1)
	f.handler.registerRoute(reqID, ch)
	defer f.handler.unregisterRoute(reqID)

	msg, err := f.creator.GetAncestors(f.chainID, reqID, defaultRequestTimeout, tip, p2p.EngineType_ENGINE_TYPE_CHAIN)
	if err != nil {
		results <- raceOutcome{peer: peer}
		return
	}

	f.tracker.RegisterRequest(peer)
	f.requestsSent.Add(1)
	sendStart := time.Now()
	f.net.Send(msg, avacommon.SendConfig{NodeIDs: set.Of(peer)}, avaconstants.PrimaryNetworkID, subnets.NoOpAllower)

	select {
	case <-ctx.Done():
		results <- raceOutcome{peer: peer}
	case resp := <-ch:
		bytes := 0
		for _, b := range resp.blocks {
			bytes += len(b)
		}
		if bytes > 0 {
			f.answersNonEmpty.Add(1)
			elapsed := time.Since(sendStart).Seconds()
			if elapsed <= 0 {
				elapsed = 1e-9
			}
			f.tracker.RegisterResponse(peer, float64(bytes)/elapsed)
		}
		results <- raceOutcome{peer: peer, resp: resp, ok: true}
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

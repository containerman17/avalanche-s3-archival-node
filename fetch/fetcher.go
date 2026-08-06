package fetch

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/netip"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
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

	"github.com/containerman17/epochdb/chain"
)

const (
	DefaultNodeURI = "https://api.avax-test.network"

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
	// Chain is the descriptor for an Avalanche L1. nil (or a descriptor whose
	// SubnetID is the primary network) keeps the C-chain path exactly as it
	// was: ids come from the bootstrap node's RPC, no tracked subnets, coreth.
	Chain *chain.Chain
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
	// subnetID is the subnet EVERY outbound chain message is keyed on. The
	// network drops a peer that does not track it, which is the correct
	// filter: for the C-chain it is the primary network, for an L1 its own
	// subnet.
	subnetID ids.ID

	// Genesis block of the C-Chain is not served over GetAncestors, so we
	// short-circuit the walk when we hit its container ID. Empty on an L1,
	// where the walk terminates on height 1 instead (see walkSpan).
	genesisID ids.ID

	dispatchErrCh chan error
	reqIDCounter  atomic.Uint32

	// bootstrapMu serialises the race-like probe round used when the
	// archival set is empty, so concurrent walks don't all fan out.
	bootstrapMu sync.Mutex
	// lastTip is the most recently requested container ID; the background
	// prober reuses it instead of fabricating ranges.
	lastTip atomic.Value // ids.ID

	// floor is the sealed end: no walk descends to it and no block at or
	// below it is ever requested. See SetFloor.
	floor atomic.Uint64

	// ceiling is the retained-staging budget in bytes, 0 = unbounded, and
	// paused is the log latch of a walk stalled on it. See SetCeiling.
	ceiling atomic.Uint64
	paused  atomic.Bool

	// syncTarget is the ceiling of a bounded backfill (SyncTo's highest
	// anchor), 0 while following or before the anchors resolve. REPORTING
	// ONLY: nothing in the walk reads it. It exists because the store's head
	// is the FOLLOWER's number and a backfill has no follower.
	syncTarget atomic.Uint64

	// Stats for progress logging.
	requestsSent    atomic.Uint64
	answersTotal    atomic.Uint64
	answersNonEmpty atomic.Uint64
	activeWalks     atomic.Int64
	inFlight        atomic.Int64
}

// New opens the flat-file store, dials the network named by cfg (the primary
// network's C-chain by default, an L1 when cfg.Chain names one), and waits for
// a peer to connect. It does NOT start syncing; call Sync for that.
func New(cfg Config) (*Fetcher, error) {
	RegisterExtras(cfg.vmKind())

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
	ceiling, err := stagingCeiling(cfg.DataDir)
	if err != nil {
		store.Close()
		return nil, err
	}

	f, err := dial(cfg)
	if err != nil {
		store.Close()
		return nil, err
	}
	f.cfg = cfg
	f.store = store
	f.SetCeiling(ceiling)
	if ceiling > 0 {
		log.Printf("fetch: staging ceiling %d MB, retained now %d MB (walks pause above the ceiling until sealing drains it)",
			ceiling>>20, store.StagedBytes()>>20)
	}
	return f, nil
}

// stagingFreeShare is the fraction of the data dir's FREE SPACE the default
// ceiling takes. A quarter: the arrival log is the only raw family a runaway
// walk grows, but the executor's families and the chunk cache live on the same
// filesystem, so three quarters stay theirs.
const stagingFreeShare = 4

// stagingCeiling derives the default retained-staging budget from free space,
// following the chunk cache's precedent (DESIGN: a byte budget nobody can set
// is the wrong instrument, so admission control measures the filesystem). Safe
// on a small disk because it is a fraction OF that disk, and invisible on a big
// one: 1.5 TB free gives a 375 GB ceiling, tens of hours of executor runway,
// against the single epoch a node at the tip retains.
//
// EPOCHDB_MAX_STAGING overrides it in bytes; 0 disables the bound outright.
//
// ponytail: measured once, at open. A live statfs would shrink as our own
// staging grows, so the bound would tighten against itself; the rest of the
// disk is the chunk cache's own live watermark to defend.
func stagingCeiling(dir string) (uint64, error) {
	if v := os.Getenv("EPOCHDB_MAX_STAGING"); v != "" {
		n, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("EPOCHDB_MAX_STAGING=%q is not a byte count (0 disables the bound)", v)
		}
		return n, nil
	}
	var st syscall.Statfs_t
	if err := syscall.Statfs(dir, &st); err != nil {
		// An unreadable filesystem is not a full one. Fail OPEN and say so:
		// pausing a walk that has no reason to pause would be the worse guess.
		log.Printf("fetch: free space of %s unreadable (%v), staging is UNBOUNDED", dir, err)
		return 0, nil
	}
	// f_bavail, not f_bfree: the reserved blocks are not ours.
	return uint64(st.Bavail) * uint64(st.Bsize) / stagingFreeShare, nil
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
	ctx, cancel := context.WithTimeout(context.Background(), dialTimeout)
	defer cancel()

	infoClient := info.NewClient(cfg.NodeURI)
	pClient := platformvm.NewClient(cfg.NodeURI)

	networkID, err := infoClient.GetNetworkID(ctx)
	if err != nil {
		return nil, fmt.Errorf("info.getNetworkID: %w", err)
	}
	var (
		chainID  ids.ID
		subnetID = avaconstants.PrimaryNetworkID
	)
	if cfg.l1() {
		if cfg.Chain.NetworkID != networkID {
			return nil, fmt.Errorf("chain descriptor is network %d but %s is network %d",
				cfg.Chain.NetworkID, cfg.NodeURI, networkID)
		}
		chainID, subnetID = cfg.Chain.BlockchainID, cfg.Chain.SubnetID
	} else if chainID, err = infoClient.GetBlockchainID(ctx, "C"); err != nil {
		return nil, fmt.Errorf("info.getBlockchainID(C): %w", err)
	}
	log.Printf("fetch: network_id=%d chain_id=%s subnet_id=%s", networkID, chainID, subnetID)

	vdrList, err := pClient.GetCurrentValidators(ctx, subnetID, nil)
	if err != nil {
		return nil, fmt.Errorf("get validators: %w", err)
	}
	validatorIDs := sortedNodeIDs(vdrList)
	log.Printf("fetch: validator_set_size=%d", len(validatorIDs))

	peerInfos, err := discoverPeers(ctx, infoClient, validatorIDs)
	if err != nil {
		return nil, fmt.Errorf("discover peers: %w", err)
	}
	if len(peerInfos) == 0 {
		return nil, fmt.Errorf("no peers returned from info.peers")
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

	if err := waitForPeer(ctx, dispatchErrCh, handler.connectedCh, peerIDs, dialTimeout); err != nil {
		net.StartClose()
		return nil, fmt.Errorf("connect peer: %w", err)
	}
	if err := warmupPeers(ctx, dispatchErrCh, handler.connectedCh, peerIDs, defaultPeerWarmup); err != nil {
		net.StartClose()
		return nil, fmt.Errorf("peer warmup: %w", err)
	}

	// The C-chain's genesis container ID is computable offline and is the walk
	// terminator there. An L1's genesis is not needed for that: no chain serves
	// genesis over GetAncestors, so the walk ends on the block at height 1
	// whatever the chain (see walkSpan).
	var genesisID ids.ID
	if !cfg.l1() {
		if genesisID, err = computeGenesisID(networkID); err != nil {
			net.StartClose()
			return nil, fmt.Errorf("compute genesis id: %w", err)
		}
		log.Printf("fetch: genesis_container_id=%s", genesisID)
	}

	return &Fetcher{
		net:           net,
		handler:       handler,
		pool:          pool,
		creator:       creator,
		networkID:     networkID,
		chainID:       chainID,
		subnetID:      subnetID,
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

// SetFloor raises the backfill floor to the SEALED END. Every walk treats it
// as its floor, so no block at or below it is ever walked or requested.
//
// It is what stops a restarted follower re-downloading all of sealed history:
// walks short-circuit on the STAGING store alone, and seal legitimately
// deleted the raw buckets those blocks used to live in, so without the floor
// the walk drops off the bottom of the retained tail and re-fetches to height
// 1 (DESIGN.md, was OPEN 2026-07-31).
//
// The floor RISES while the node runs, because sealing is in-process: the
// caller sets it at startup from the sealed epoch set and again after every
// seal. Monotonic, and written by one goroutine (the cook loop).
func (f *Fetcher) SetFloor(sealedEnd uint64) { f.floor.Store(max(sealedEnd, f.floor.Load())) }

// SetCeiling bounds the RETAINED STAGING every walk may hold: once the raw
// staging on disk (Store.StagedBytes, i.e. what the seal has not retired)
// reaches this many bytes, walks PAUSE until sealing has drained it back under.
// 0 disables the bound. Set from the environment at New; nothing raises it
// later.
//
// THE FLOOR'S SYMMETRIC TWIN. The floor stops a walk descending into history
// the seal has already made durable; the ceiling stops it running so far ahead
// of the executor that the raw it stages fills the disk. Fetch runs ~1,650
// blk/s against the executor's ~105 on mainnet, so an unbounded walk to the
// live tip finishes all 91.7M blocks in ~15 hours with execution ~5M blocks in,
// leaving ~86M blocks staged at once: 1-2 TB at the ~28 KB/block of epoch 9
// (15,798 MB for 564,413 blocks), against 1.5 TB free. Same shape on DFK
// (~410 GB just to stage) and Bnry, so it gates every giant.
//
// EXCEEDING IT IS NORMAL OPERATION ON A BIG CHAIN, not an error: the walk
// stalls, logs once, and resumes by itself as the cook loop's seal retires the
// staging behind the executed point.
func (f *Fetcher) SetCeiling(bytes uint64) { f.ceiling.Store(bytes) }

// StagedBytes, StagingCeiling and Paused are what the node's status line and
// /status report, so a stalled walk is visible without grepping the log.
func (f *Fetcher) StagedBytes() uint64    { return f.store.StagedBytes() }
func (f *Fetcher) StagingCeiling() uint64 { return f.ceiling.Load() }
func (f *Fetcher) Paused() bool           { return f.paused.Load() }

// stagingPollInterval is how often a paused walk re-checks whether sealing has
// drained enough to continue. Slow on purpose: draining an epoch is minutes to
// hours, and the pause must cost nothing while it lasts. A var only so tests
// need not wait it out.
var stagingPollInterval = 5 * time.Second

// awaitStagingRoom is the ceiling's enforcement point: every walk calls it
// before every step, so nothing can add to staging without passing here. The
// fast path is one atomic load and a comparison, which is what makes it
// affordable per block.
//
// IT HOLDS NO LOCK AND WAITS ON NOTHING OF OURS. The cook loop, the seal and
// the executor are other goroutines and they are exactly what frees the space,
// so a paused walk must not be in their way; it sleeps on a timer and on ctx,
// so a stopped executor costs one idle goroutine (no spin) and a SIGINT still
// returns immediately. A node that has paused fetching and keeps executing and
// serving RPC is healthy.
//
// NOT ON THE TIP PATH. Consensus acceptance appends through appendContainer,
// which never comes here: a node at the tip stages a handful of blocks a second
// against a ceiling sized in hundreds of gigabytes, and is never throttled by
// this even if a backfill walk beside it is paused.
func (f *Fetcher) awaitStagingRoom(ctx context.Context) error {
	ceiling := f.ceiling.Load()
	if ceiling == 0 || f.store.StagedBytes() < ceiling {
		return nil
	}
	// Once per pause, not per block: with 16 concurrent walks all stalling on
	// the same ceiling, a line per walk per block IS the log.
	if !f.paused.Swap(true) {
		log.Printf("fetch: PAUSED, %d MB of raw staging retained against a %d MB ceiling; "+
			"execution and sealing keep running and the walk resumes as they drain it (EPOCHDB_MAX_STAGING overrides)",
			f.store.StagedBytes()>>20, ceiling>>20)
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(stagingPollInterval):
		}
		if staged := f.store.StagedBytes(); staged < ceiling {
			if f.paused.Swap(false) {
				log.Printf("fetch: resumed, %d MB of raw staging retained against a %d MB ceiling",
					staged>>20, ceiling>>20)
			}
			return nil
		}
	}
}

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

// Command epochdb-host runs ONE Avalanche L1's VM plugin the way avalanchego
// would, minus consensus, minus the P-chain: it fetches accepted containers
// from the L1's validators (the fetch package), unwraps the proposervm header
// and feeds the inner block to a stock plugin over avalanchego's own
// rpcchainvm client, then mounts the plugin's HTTP handlers.
//
// PROTOTYPE. It stores nothing of its own beyond the plugin's database and the
// staking identity; the executor and the epochdb store are not involved.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/ava-labs/avalanchego/api/metrics"
	"github.com/ava-labs/avalanchego/cache/lru"
	avaatomic "github.com/ava-labs/avalanchego/chains/atomic"
	"github.com/ava-labs/avalanchego/database/pebbledb"
	"github.com/ava-labs/avalanchego/database/prefixdb"
	"github.com/ava-labs/avalanchego/genesis"
	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/snow"
	"github.com/ava-labs/avalanchego/snow/engine/common"
	"github.com/ava-labs/avalanchego/snow/engine/snowman/block"
	"github.com/ava-labs/avalanchego/snow/validators"
	"github.com/ava-labs/avalanchego/staking"
	"github.com/ava-labs/avalanchego/upgrade"
	"github.com/ava-labs/avalanchego/utils/constants"
	"github.com/ava-labs/avalanchego/utils/crypto/bls/signer/localsigner"
	"github.com/ava-labs/avalanchego/utils/logging"
	"github.com/ava-labs/avalanchego/utils/set"
	"github.com/ava-labs/avalanchego/vms/platformvm"
	platformapi "github.com/ava-labs/avalanchego/vms/platformvm/api"
	"github.com/ava-labs/avalanchego/vms/platformvm/warp"
	proposerblock "github.com/ava-labs/avalanchego/vms/proposervm/block"
	"github.com/ava-labs/avalanchego/vms/rpcchainvm"
	"github.com/ava-labs/avalanchego/vms/rpcchainvm/runtime"
	"github.com/ava-labs/libevm/rlp"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/containerman17/avalanche-s3-archival-node/chain"
	"github.com/containerman17/avalanche-s3-archival-node/dist"
	"github.com/containerman17/avalanche-s3-archival-node/fetch"
)

func main() {
	fs := flag.NewFlagSet("epochdb-host", flag.ExitOnError)
	chainSpec := fs.String("chain", "", "the L1's blockchainID")
	network := fs.String("network", "mainnet", "network: fuji|mainnet")
	vmPath := fs.String("vm", "", "plugin binary (a stock subnet-evm built for this avalanchego's rpcchainvm protocol)")
	dataDir := fs.String("data", "./data", "data directory: plugin db, staking identity, chain.json, optional upgrade.json")
	nodeURI := fs.String("node", "", "comma-separated Avalanche RPC node URIs (bootstrap peers, P-chain validator state)")
	httpAddr := fs.String("http", "127.0.0.1:19900", "HTTP listen address; the plugin's handlers mount at /ext/bc/<chainID>/<path>")
	p2pPort := fs.Int("p2p-port", 0, "p2p listen port (persists staker.key/.crt under --data, so the NodeID is stable)")
	fs.Parse(os.Args[1:])
	if *chainSpec == "" || *vmPath == "" {
		log.Fatal("epochdb-host: --chain and --vm are required")
	}
	var networkID uint32
	switch *network {
	case "mainnet":
		networkID = constants.MainnetID
	case "fuji":
		networkID = constants.FujiID
	default:
		log.Fatalf("epochdb-host: unknown --network %q", *network)
	}
	if *nodeURI == "" {
		*nodeURI = map[uint32]string{constants.MainnetID: "https://api.avax.network", constants.FujiID: "https://api.avax-test.network"}[networkID]
	}
	sources := dist.Sources(*nodeURI)
	if err := os.MkdirAll(*dataDir, 0o755); err != nil {
		log.Fatal(err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	rctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	c, err := chain.Resolve(rctx, *chainSpec, networkID, *dataDir, sources...)
	cancel()
	if err != nil {
		log.Fatalf("epochdb-host: --chain: %v", err)
	}
	if c.SubnetID == constants.PrimaryNetworkID {
		log.Fatal("epochdb-host: the primary network's C-chain is not an L1 (coreth is not a plugin)")
	}

	// The fetcher dials first: it is what persists the staking identity, and
	// the dial is the slow part of startup anyway.
	f, err := fetch.New(fetch.Config{NodeURI: *nodeURI, Chain: c, ListenPort: *p2pPort, DataDir: *dataDir})
	if err != nil {
		log.Fatalf("epochdb-host: fetch: %v", err)
	}
	defer f.Close()

	nodeID, err := nodeIDFrom(*dataDir)
	if err != nil {
		log.Fatalf("epochdb-host: staking identity: %v", err)
	}
	blsKey, err := localsigner.FromFileOrPersistNew(filepath.Join(*dataDir, "signer.key"))
	if err != nil {
		log.Fatalf("epochdb-host: bls key: %v", err)
	}
	xChainID, cChainID, avaxAssetID, err := primaryIDs(networkID)
	if err != nil {
		log.Fatal(err)
	}

	logger := logging.NewLogger("host", logging.NewWrappedCore(logging.Info, os.Stderr, logging.Plain.ConsoleEncoder()))
	tracker := &pidTracker{}
	v, err := rpcchainvm.NewFactory(*vmPath, tracker, runtime.NewManager(), metrics.NewPrefixGatherer()).New(logger)
	if err != nil {
		log.Fatalf("epochdb-host: launch plugin: %v", err)
	}
	vm := v.(block.ChainVM)

	db, err := pebbledb.New(filepath.Join(*dataDir, "db"), nil, logger, prometheus.NewRegistry())
	if err != nil {
		log.Fatalf("epochdb-host: db: %v", err)
	}
	defer db.Close()

	chainDataDir := filepath.Join(*dataDir, "chainData")
	os.MkdirAll(chainDataDir, 0o755)
	snowCtx := &snow.Context{
		NetworkID:       networkID,
		SubnetID:        c.SubnetID,
		ChainID:         c.BlockchainID,
		NodeID:          nodeID,
		PublicKey:       blsKey.PublicKey(),
		NetworkUpgrades: upgrade.GetConfig(networkID),
		XChainID:        xChainID,
		CChainID:        cChainID,
		AVAXAssetID:     avaxAssetID,
		Log:             logger,
		SharedMemory:    avaatomic.NewMemory(prefixdb.New([]byte("atomic"), db)).NewSharedMemory(c.BlockchainID),
		BCLookup:        ids.NewAliaser(),
		Metrics:         metrics.NewPrefixGatherer(),
		WarpSigner:      warp.NewSigner(blsKey, networkID, c.BlockchainID),
		ValidatorState:  newRPCValidatorState(sources, c.BlockchainID, c.SubnetID),
		ChainDataDir:    chainDataDir,
	}
	// state-sync off: the plugin must execute every block, that is the point.
	// Pruning stays at the plugin's default.
	if err := vm.Initialize(ctx, snowCtx, prefixdb.New([]byte("vm"), db), c.GenesisJSON, c.UpgradeJSON,
		[]byte(`{"state-sync-enabled":false}`), nil, noopSender{}); err != nil {
		log.Fatalf("epochdb-host: Initialize: %v", err)
	}
	if err := vm.SetState(ctx, snow.Bootstrapping); err != nil {
		log.Fatalf("epochdb-host: SetState(Bootstrapping): %v", err)
	}

	// HTTP before the sync, so eth_blockNumber answers while it runs.
	handlers, err := vm.CreateHandlers(ctx)
	if err != nil {
		log.Fatalf("epochdb-host: CreateHandlers: %v", err)
	}
	mux := http.NewServeMux()
	for ext, h := range handlers {
		mux.Handle("/ext/bc/"+c.BlockchainID.String()+ext, h)
		log.Printf("epochdb-host: mounted /ext/bc/%s%s", c.BlockchainID, ext)
	}
	go func() {
		if err := http.ListenAndServe(*httpAddr, mux); err != nil {
			log.Printf("epochdb-host: http: %v", err)
			stop()
		}
	}()

	// Resume from the plugin's own last accepted block: its ID is the inner
	// eth hash, which is exactly the anchor the forward fetch verifies the next
	// block's parent against.
	lastID, err := vm.LastAccepted(ctx)
	if err != nil {
		log.Fatalf("epochdb-host: LastAccepted: %v", err)
	}
	last, err := vm.GetBlock(ctx, lastID)
	if err != nil {
		log.Fatalf("epochdb-host: GetBlock(last): %v", err)
	}
	from := last.Height() + 1
	log.Printf("epochdb-host: plugin last accepted height=%d id=%s, fetching from %d", last.Height(), lastID, from)
	blocks := f.StartForward(ctx, from, lastID)
	go func() {
		if err := f.Follow(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("epochdb-host: follower: %v", err)
			stop()
		}
	}()

	var height atomic.Uint64
	height.Store(last.Height())
	go rateLoop(ctx, &height, f, tracker)

	err = drive(ctx, vm, blocks, f, from, &height, &snowCtx.NetworkUpgrades)
	if err != nil && !errors.Is(err, context.Canceled) {
		log.Printf("epochdb-host: FATAL: %v", err)
	}
	sctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := vm.Shutdown(sctx); err != nil {
		log.Printf("epochdb-host: plugin shutdown: %v", err)
	}
	log.Printf("epochdb-host: stopped at height=%d", height.Load())
}

// drive is the whole host loop: for every container in height order, unwrap
// the proposervm header, ParseBlock the inner bytes, Verify (with the P-chain
// height the proposervm would hand the inner VM), Accept.
//
// The block.Context rule is proposervm's (vms/proposervm/block.go Verify):
// Granite active at the block's timestamp: the epoch's P-chain height; Etna:
// the header's own P-chain height; earlier: the PARENT header's. A pre-fork
// container has no header and gets 0. Proposer signature, timing and epoch
// checks are NOT redone here: the follower took the container from the
// validators' accepted chain.
func drive(ctx context.Context, vm block.ChainVM, blocks *fetch.Queue, f *fetch.Fetcher, from uint64, height *atomic.Uint64, upgrades *upgrade.Config) error {
	var (
		parentPCH uint64
		normal    bool
	)
	for h := from; ; h++ {
		raw, _, err := blocks.GetByHeight(h)
		if err != nil {
			return err
		}
		inner, pch := unwrap(raw, upgrades, &parentPCH)
		blk, err := vm.ParseBlock(ctx, inner)
		if err != nil {
			return fmt.Errorf("height %d: ParseBlock: %w", h, err)
		}
		if blk.Height() != h {
			return fmt.Errorf("height %d: plugin parsed it as height %d", h, blk.Height())
		}
		verified := false
		if wc, ok := blk.(block.WithVerifyContext); ok {
			should, err := wc.ShouldVerifyWithContext(ctx)
			if err != nil {
				return fmt.Errorf("height %d: ShouldVerifyWithContext: %w", h, err)
			}
			if should {
				if err := wc.VerifyWithContext(ctx, &block.Context{PChainHeight: pch}); err != nil {
					return fmt.Errorf("height %d: VerifyWithContext(pChainHeight=%d): %w", h, pch, err)
				}
				verified = true
			}
		}
		if !verified {
			if err := blk.Verify(ctx); err != nil {
				return fmt.Errorf("height %d: Verify: %w", h, err)
			}
		}
		if err := blk.Accept(ctx); err != nil {
			return fmt.Errorf("height %d: Accept: %w", h, err)
		}
		height.Store(h)
		// The tip is where the follower says it is. At it, the plugin goes to
		// normal operation and the preference follows every block, as under
		// avalanchego; during catch-up it is refreshed now and then.
		tip := f.AcceptedHead()
		if !normal && tip > 0 && h >= tip {
			if err := vm.SetState(ctx, snow.NormalOp); err != nil {
				return fmt.Errorf("SetState(NormalOp): %w", err)
			}
			normal = true
			log.Printf("epochdb-host: caught up at height=%d, plugin in NormalOp", h)
		}
		if normal || h%1000 == 0 {
			if err := vm.SetPreference(ctx, blk.ID()); err != nil {
				return fmt.Errorf("height %d: SetPreference: %w", h, err)
			}
		}
	}
}

// unwrap returns the inner VM bytes of a container and the P-chain height for
// its block.Context, updating parentPCH to this header's own P-chain height.
func unwrap(raw []byte, upgrades *upgrade.Config, parentPCH *uint64) ([]byte, uint64) {
	pb, err := proposerblock.ParseWithoutVerification(raw)
	if err != nil {
		// Pre-proposervm: the container is the RLP eth block, possibly with
		// trailing bytes (same strip as exec/parse.go).
		if _, _, rest, err := rlp.Split(raw); err == nil {
			raw = raw[:len(raw)-len(rest)]
		}
		return raw, 0
	}
	signed, ok := pb.(proposerblock.SignedBlock)
	if !ok {
		// An option block carries no header of its own; subnet-evm never
		// produces one.
		return pb.Block(), *parentPCH
	}
	var pch uint64
	switch ts := signed.Timestamp(); {
	case upgrades.IsGraniteActivated(ts):
		pch = signed.PChainEpoch().PChainHeight
	case upgrades.IsEtnaActivated(ts):
		pch = signed.PChainHeight()
	default:
		pch = *parentPCH
	}
	*parentPCH = signed.PChainHeight()
	return pb.Block(), pch
}

// rateLoop is the one-line summary the benchmark reads, every 30s: height,
// blocks/s over the window, host RSS, plugin RSS.
func rateLoop(ctx context.Context, height *atomic.Uint64, f *fetch.Fetcher, tracker *pidTracker) {
	const window = 30 * time.Second
	t := time.NewTicker(window)
	defer t.Stop()
	prev := height.Load()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		cur := height.Load()
		p := f.Progress()
		log.Printf("host: height=%d rate=%.1f blk/s fetched=%d accepted=%d host_rss=%dMB plugin_rss=%dMB queue=%.0fMB",
			cur, float64(cur-prev)/window.Seconds(), p.Head, f.AcceptedHead(),
			rssMB(os.Getpid()), rssMB(int(tracker.pid.Load())), float64(p.QueueBytes)/1e6)
		prev = cur
	}
}

// rssMB reads a process's resident set from /proc; 0 when it cannot.
func rssMB(pid int) int64 {
	if pid <= 0 {
		return 0
	}
	b, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/statm")
	if err != nil {
		return 0
	}
	fields := strings.Fields(string(b))
	if len(fields) < 2 {
		return 0
	}
	pages, _ := strconv.ParseInt(fields[1], 10, 64)
	return pages * int64(os.Getpagesize()) >> 20
}

// pidTracker is the resource.ProcessTracker the factory reports the plugin's
// pid to. Nothing is tracked but the pid, for the RSS line.
type pidTracker struct{ pid atomic.Int64 }

func (t *pidTracker) TrackProcess(pid int) { t.pid.Store(int64(pid)) }
func (t *pidTracker) UntrackProcess(int)   { t.pid.Store(0) }

// nodeIDFrom derives the NodeID from the staking cert the fetcher persisted.
func nodeIDFrom(dataDir string) (ids.NodeID, error) {
	tlsCert, err := staking.LoadTLSCertFromFiles(filepath.Join(dataDir, "staker.key"), filepath.Join(dataDir, "staker.crt"))
	if err != nil {
		return ids.EmptyNodeID, err
	}
	cert, err := staking.ParseCertificate(tlsCert.Leaf.Raw)
	if err != nil {
		return ids.EmptyNodeID, err
	}
	return ids.NodeIDFromCert(cert), nil
}

// primaryIDs returns the network's X-chain ID, C-chain ID and AVAX asset ID
// out of avalanchego's embedded genesis, the way the node computes them.
func primaryIDs(networkID uint32) (x, c, avax ids.ID, err error) {
	cfg := genesis.GetConfig(networkID)
	if cfg == nil {
		return x, c, avax, fmt.Errorf("no embedded genesis for network %d", networkID)
	}
	genesisBytes, avax, err := genesis.FromConfig(cfg)
	if err != nil {
		return x, c, avax, err
	}
	xTx, err := genesis.VMGenesis(genesisBytes, constants.AVMID)
	if err != nil {
		return x, c, avax, err
	}
	cTx, err := genesis.VMGenesis(genesisBytes, constants.EVMID)
	if err != nil {
		return x, c, avax, err
	}
	return xTx.ID(), cTx.ID(), avax, nil
}

// noopSender: a follower gossips nothing and answers no one.
type noopSender struct{}

func (noopSender) SendAppRequest(context.Context, set.Set[ids.NodeID], uint32, []byte) error {
	return nil
}
func (noopSender) SendAppResponse(context.Context, ids.NodeID, uint32, []byte) error     { return nil }
func (noopSender) SendAppError(context.Context, ids.NodeID, uint32, int32, string) error { return nil }
func (noopSender) SendAppGossip(context.Context, common.SendConfig, []byte) error        { return nil }

// rpcValidatorState is validators.State over an RPC node's platform API, in
// place of a local P-chain. Every answer is cached: heights are asked per
// block in NormalOp, validator sets per warp-carrying block.
//
// ponytail: GetWarpValidatorSets can only answer for the subnets it knows to
// ask about (this chain's own and the primary network); a warp message from a
// third subnet fails verification here. Enumerating every subnet's validators
// per height is not a thing an RPC node answers in one call.
type rpcValidatorState struct {
	sources  []string
	chainID  ids.ID
	subnetID ids.ID

	mu       sync.Mutex
	subnets  map[ids.ID]ids.ID // chainID -> subnetID
	vdrSets  *lru.Cache[vdrKey, map[ids.NodeID]*validators.GetValidatorOutput]
	heightAt time.Time
	height   uint64
}

type vdrKey struct {
	height   uint64
	subnetID ids.ID
}

func newRPCValidatorState(sources []string, chainID, subnetID ids.ID) *rpcValidatorState {
	return &rpcValidatorState{
		sources:  sources,
		chainID:  chainID,
		subnetID: subnetID,
		subnets: map[ids.ID]ids.ID{
			chainID:                   subnetID,
			constants.PlatformChainID: constants.PrimaryNetworkID,
		},
		vdrSets: lru.NewCache[vdrKey, map[ids.NodeID]*validators.GetValidatorOutput](64),
	}
}

// try runs fn against each source in turn until one answers.
func (s *rpcValidatorState) try(ctx context.Context, fn func(context.Context, *platformvm.Client) error) error {
	var err error
	for _, uri := range s.sources {
		cctx, cancel := context.WithTimeout(ctx, 60*time.Second)
		err = fn(cctx, platformvm.NewClient(uri))
		cancel()
		if err == nil {
			return nil
		}
		log.Printf("epochdb-host: validator state via %s: %v", uri, err)
	}
	return err
}

func (s *rpcValidatorState) GetMinimumHeight(ctx context.Context) (uint64, error) {
	return s.GetCurrentHeight(ctx)
}

// GetCurrentHeight is platform.getHeight, held for 2s: the proposervm would ask
// once per verified block and the P-chain moves slower than that.
func (s *rpcValidatorState) GetCurrentHeight(ctx context.Context) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if time.Since(s.heightAt) < 2*time.Second {
		return s.height, nil
	}
	var h uint64
	err := s.try(ctx, func(ctx context.Context, c *platformvm.Client) (err error) {
		h, err = c.GetHeight(ctx)
		return
	})
	if err != nil {
		return 0, err
	}
	s.height, s.heightAt = h, time.Now()
	return h, nil
}

func (s *rpcValidatorState) GetSubnetID(ctx context.Context, chainID ids.ID) (ids.ID, error) {
	s.mu.Lock()
	if id, ok := s.subnets[chainID]; ok {
		s.mu.Unlock()
		return id, nil
	}
	s.mu.Unlock()
	var chains []platformvm.APIBlockchain
	err := s.try(ctx, func(ctx context.Context, c *platformvm.Client) (err error) {
		chains, err = c.GetBlockchains(ctx)
		return
	})
	if err != nil {
		return ids.Empty, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, bc := range chains {
		s.subnets[bc.ID] = bc.SubnetID
	}
	id, ok := s.subnets[chainID]
	if !ok {
		return ids.Empty, fmt.Errorf("chain %s is on no subnet platform.getBlockchains lists", chainID)
	}
	return id, nil
}

func (s *rpcValidatorState) GetValidatorSet(ctx context.Context, height uint64, subnetID ids.ID) (map[ids.NodeID]*validators.GetValidatorOutput, error) {
	key := vdrKey{height, subnetID}
	s.mu.Lock()
	vdrs, ok := s.vdrSets.Get(key)
	s.mu.Unlock()
	if ok {
		return vdrs, nil
	}
	err := s.try(ctx, func(ctx context.Context, c *platformvm.Client) (err error) {
		vdrs, err = c.GetValidatorsAt(ctx, subnetID, platformapi.Height(height))
		return
	})
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.vdrSets.Put(key, vdrs)
	s.mu.Unlock()
	return vdrs, nil
}

func (s *rpcValidatorState) GetWarpValidatorSets(ctx context.Context, height uint64) (map[ids.ID]validators.WarpSet, error) {
	out := make(map[ids.ID]validators.WarpSet, 2)
	for _, subnetID := range []ids.ID{s.subnetID, constants.PrimaryNetworkID} {
		vdrs, err := s.GetValidatorSet(ctx, height, subnetID)
		if err != nil {
			return nil, err
		}
		ws, err := validators.FlattenValidatorSet(vdrs)
		if err != nil {
			return nil, err
		}
		out[subnetID] = ws
	}
	return out, nil
}

// GetCurrentValidatorSet is platform.getCurrentValidators keyed by validation
// ID (the tx ID for a pre-ACP-77 validator). Public keys are left nil: the
// plugin's uptime tracker, its one caller, does not read them.
func (s *rpcValidatorState) GetCurrentValidatorSet(ctx context.Context, subnetID ids.ID) (map[ids.ID]*validators.GetCurrentValidatorOutput, uint64, error) {
	height, err := s.GetCurrentHeight(ctx)
	if err != nil {
		return nil, 0, err
	}
	var list []platformvm.ClientPermissionlessValidator
	err = s.try(ctx, func(ctx context.Context, c *platformvm.Client) (err error) {
		list, err = c.GetCurrentValidators(ctx, subnetID, nil)
		return
	})
	if err != nil {
		return nil, 0, err
	}
	out := make(map[ids.ID]*validators.GetCurrentValidatorOutput, len(list))
	for _, v := range list {
		o := &validators.GetCurrentValidatorOutput{
			ValidationID: v.TxID,
			NodeID:       v.NodeID,
			Weight:       v.Weight,
			StartTime:    v.StartTime,
			IsActive:     true,
		}
		if v.ValidationID != nil {
			o.ValidationID = *v.ValidationID
			o.IsL1Validator = true
			o.IsActive = v.Balance != nil && *v.Balance > 0
			if v.MinNonce != nil {
				o.MinNonce = *v.MinNonce
			}
		}
		out[o.ValidationID] = o
	}
	return out, height, nil
}

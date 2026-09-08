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
	"github.com/ava-labs/avalanchego/snow/consensus/snowman"
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
	ethtypes "github.com/ava-labs/libevm/core/types"
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
	holdSpec := fs.String("hold-until", "", "hand the VM its first block only once this much is fetched ahead of it: N blocks, or N txs as Ntx (100000, 250000tx)")
	queueAhead := fs.Int("queue-ahead", 100_000, "blocks the host keeps fetched ahead of the VM in its own ring, in front of the fetch package's fixed window")
	batch := fs.Int("batch", 256, "blocks per BatchedParseBlock; up to 2 parsed batches wait ahead of verification")
	corpus := fs.String("corpus", "", "local EPCORP01 container file; disables fetching and following")
	configPath := fs.String("config", "", "VM config JSON file (required with --corpus)")
	stopHeight := fs.Uint64("stop", 0, "last corpus height to accept; keep RPC open at this height")
	fs.Parse(os.Args[1:])
	if *chainSpec == "" || *vmPath == "" {
		log.Fatal("epochdb-host: --chain and --vm are required")
	}
	if *queueAhead < 1 || *batch < 1 {
		log.Fatal("epochdb-host: --queue-ahead and --batch must be at least 1")
	}
	holdN, holdTx, err := parseHold(*holdSpec)
	if err != nil {
		log.Fatalf("epochdb-host: --hold-until: %v", err)
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
	if *corpus != "" {
		if *configPath == "" || *stopHeight == 0 {
			log.Fatal("epochdb-host: --corpus requires --config and --stop greater than zero")
		}
		if _, err := os.Stat(filepath.Join(*dataDir, "chain.json")); err != nil {
			log.Fatalf("epochdb-host: corpus mode requires local chain.json: %v", err)
		}
	}

	rctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	c, err := chain.Resolve(rctx, *chainSpec, networkID, *dataDir, sources...)
	cancel()
	if err != nil {
		log.Fatalf("epochdb-host: --chain: %v", err)
	}
	if c.SubnetID == constants.PrimaryNetworkID {
		log.Fatal("epochdb-host: the primary network's C-chain is not an L1 (coreth is not a plugin)")
	}
	if *corpus != "" {
		if err := runCorpus(ctx, c, sources, *vmPath, *dataDir, *httpAddr, *corpus, *configPath, *stopHeight, *queueAhead, *batch); err != nil && !errors.Is(err, context.Canceled) {
			log.Fatalf("epochdb-host: corpus: %v", err)
		}
		return
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

	p := &pipe{
		q: blocks, f: f, from: from, batch: *batch,
		holdSpec: *holdSpec, holdN: holdN, holdTx: holdTx,
		ring:    make(chan item, *queueAhead),
		batches: make(chan []parsed, 2),
		stop:    stop,
	}
	b := &bench{tracker: tracker, ring: p.ring}
	b.height.Store(last.Height())
	go b.loop(ctx)
	go p.pull(ctx, &snowCtx.NetworkUpgrades)
	go p.parse(ctx, vm)

	err = p.drive(ctx, vm, b)
	if err != nil && !errors.Is(err, context.Canceled) {
		log.Printf("epochdb-host: FATAL: %v", err)
	}
	b.exit()
	sctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := vm.Shutdown(sctx); err != nil {
		log.Printf("epochdb-host: plugin shutdown: %v", err)
	}
	log.Printf("epochdb-host: stopped at height=%d", b.height.Load())
}

// item is one container after unwrap: the inner VM bytes, the P-chain height
// for its block.Context, and the header facts the bench line wants, decoded
// once here (the libevm extras fetch.New registered make it the fetcher's
// decode).
type item struct {
	h     uint64
	inner []byte
	pch   uint64
	txs   uint64
	gas   uint64
}

// parsed is an item the VM has parsed, ahead of its verification.
type parsed struct {
	item
	blk snowman.Block
}

// pipe is the host loop in three stages: pull (fetch queue -> ring, in
// height order, unwrapped), parse (ring -> batches of VM-parsed blocks, up to
// 2 waiting) and drive (Verify + Accept, strictly sequential per block). The
// first error stops the host; each stage closes its output so drive sees it.
//
// vmMu: the VM sees ONE call at a time, the way avalanchego's engine calls
// it (under ctx.Lock). A parse batch lands between two of drive's calls,
// never during one: the rpcchainvm client's block cache (chain.State) is not
// goroutine-safe and a concurrent parse died on its map. The overlap a VM
// gets is its own parse-time work (a sender recovery pool) running while
// the host verifies the previous batch.
type pipe struct {
	q        *fetch.Queue
	f        *fetch.Fetcher
	from     uint64
	batch    int
	holdSpec string
	holdN    uint64
	holdTx   bool
	ring     chan item
	batches  chan []parsed

	pulledTxs atomic.Uint64
	vmMu      sync.Mutex // one call into the VM at a time, as under avalanchego
	stop      context.CancelFunc
	once      sync.Once
	err       error
}

func (p *pipe) fail(err error) {
	p.once.Do(func() {
		p.err = err
		p.stop()
	})
}

// parseHold reads --hold-until: "N" blocks or "Ntx" transactions.
func parseHold(spec string) (n uint64, txs bool, err error) {
	if spec == "" {
		return 0, false, nil
	}
	txs = strings.HasSuffix(spec, "tx")
	n, err = strconv.ParseUint(strings.TrimSuffix(spec, "tx"), 10, 64)
	return n, txs, err
}

// pull moves containers from the fetch queue into the host ring. The
// fetcher's window is a constant of that package (fetch.WindowBlocks ahead of
// what GetByHeight handed out, refilling under fetch.RefillBelow), so the
// runway ahead of the VM is this ring plus that window: draining the queue
// into the ring is what makes the fetcher refill earlier.
func (p *pipe) pull(ctx context.Context, upgrades *upgrade.Config) {
	defer close(p.ring)
	var parentPCH uint64
	for h := p.from; ; h++ {
		raw, _, err := p.q.GetByHeight(h)
		if err != nil {
			p.fail(err)
			return
		}
		inner, pch := unwrap(raw, upgrades, &parentPCH)
		it := item{h: h, inner: inner, pch: pch}
		var blk ethtypes.Block
		if err := rlp.DecodeBytes(inner, &blk); err == nil {
			it.txs, it.gas = uint64(len(blk.Transactions())), blk.GasUsed()
		}
		p.pulledTxs.Add(it.txs)
		select {
		case p.ring <- it:
		case <-ctx.Done():
			return
		}
	}
}

// hold is the --hold-until gate: it blocks until the runway fetched ahead of
// the VM reaches the target, N blocks (fetch queue plus ring) or N txs (ring
// only: the fetch queue does not count txs), or until nothing more can
// arrive (the ring is full and, for a block target, the fetch window is full
// or at the tip).
func (p *pipe) hold(ctx context.Context) error {
	if p.holdN == 0 {
		return nil
	}
	t0 := time.Now()
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for i := 0; ; i++ {
		head := p.q.Head()
		blocks, txs := head-(p.from-1), p.pulledTxs.Load()
		have := blocks
		if p.holdTx {
			have = txs
		}
		tip := p.f.AcceptedHead()
		capped := len(p.ring) == cap(p.ring) &&
			(p.holdTx || head-p.q.Consumed() >= fetch.WindowBlocks || (tip > 0 && head >= tip))
		if have >= p.holdN || capped {
			log.Printf("epochdb-host: feeding starts after %s: %d blocks, %d txs fetched ahead (target %s, capped=%v)",
				time.Since(t0).Round(time.Second), blocks, txs, p.holdSpec, capped)
			return nil
		}
		if i%10 == 0 {
			log.Printf("epochdb-host: holding: %d blocks, %d txs fetched ahead (target %s)", blocks, txs, p.holdSpec)
		}
		select {
		case <-tick.C:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// take is the next batch: it blocks for one item, then takes what the ring
// holds, up to n. ok is false once the ring is closed and drained.
func take(ring <-chan item, n int) (items []item, ok bool) {
	first, ok := <-ring
	if !ok {
		return nil, false
	}
	items = append(make([]item, 0, n), first)
	for len(items) < n {
		select {
		case it, ok := <-ring:
			if !ok {
				return items, true
			}
			items = append(items, it)
		default:
			return items, true
		}
	}
	return items, true
}

// parse hands the VM the ring's contents in batches of up to p.batch blocks,
// one BatchedParseBlock per batch (one round trip to a plugin; the
// rpcchainvm client always offers it and returns ErrRemoteVMNotImplemented
// for a plugin without it, then it is ParseBlock one by one), and queues
// the parsed batches for drive. A batch is what the ring holds when the
// previous one is done, so at the tip it is one block. A VM that does work
// at parse time (sender recovery) gets it up to 3 batches ahead of Verify.
func (p *pipe) parse(ctx context.Context, vm block.ChainVM) {
	defer close(p.batches)
	if err := p.hold(ctx); err != nil {
		return
	}
	bvm, _ := vm.(block.BatchedChainVM)
	log.Printf("epochdb-host: batched parse=%v batch=%d ring=%d", bvm != nil, p.batch, cap(p.ring))
	for {
		items, ok := take(p.ring, p.batch)
		if !ok {
			return
		}
		raws := make([][]byte, len(items))
		for i := range items {
			raws[i] = items[i].inner
		}
		var (
			blks []snowman.Block
			err  error
		)
		p.vmMu.Lock()
		if bvm != nil {
			blks, err = bvm.BatchedParseBlock(ctx, raws)
			if errors.Is(err, block.ErrRemoteVMNotImplemented) {
				log.Print("epochdb-host: plugin has no BatchedParseBlock, parsing one block at a time")
				bvm, err = nil, nil
			} else if err != nil {
				err = fmt.Errorf("heights %d..%d: BatchedParseBlock: %w", items[0].h, items[len(items)-1].h, err)
			}
		}
		if bvm == nil && err == nil {
			blks = make([]snowman.Block, len(raws))
			for i := range raws {
				if blks[i], err = vm.ParseBlock(ctx, raws[i]); err != nil {
					err = fmt.Errorf("height %d: ParseBlock: %w", items[i].h, err)
					break
				}
			}
		}
		p.vmMu.Unlock()
		if err != nil {
			p.fail(err)
			return
		}
		out := make([]parsed, len(items))
		for i, it := range items {
			if blks[i].Height() != it.h {
				p.fail(fmt.Errorf("height %d: plugin parsed it as height %d", it.h, blks[i].Height()))
				return
			}
			out[i] = parsed{it, blks[i]}
		}
		select {
		case p.batches <- out:
		case <-ctx.Done():
			return
		}
	}
}

// drive is the VM loop proper: for every parsed block in height order,
// Verify (with the P-chain height the proposervm would hand the inner VM),
// then Accept. Strictly sequential: a block is verified only after its
// parent is accepted.
//
// The block.Context rule is proposervm's (vms/proposervm/block.go Verify):
// Granite active at the block's timestamp: the epoch's P-chain height; Etna:
// the header's own P-chain height; earlier: the PARENT header's. A pre-fork
// container has no header and gets 0. Proposer signature, timing and epoch
// checks are NOT redone here: the follower took the container from the
// validators' accepted chain.
func (p *pipe) drive(ctx context.Context, vm block.ChainVM, b *bench) error {
	var normal, fed bool
	for {
		// The wait for the FIRST batch is the --hold-until gate plus the
		// fetcher's seeding, not starvation: it stays off the books.
		t0 := time.Now()
		if fed {
			b.waitSince.Store(t0.UnixNano())
		}
		batch, ok := <-p.batches
		if fed {
			b.waitSince.Store(0)
			b.waitNs.Add(int64(time.Since(t0)))
		}
		fed = true
		if !ok {
			if p.err != nil {
				return p.err
			}
			return ctx.Err()
		}
		for _, x := range batch {
			if err := p.step(ctx, vm, x, b, &normal); err != nil {
				return err
			}
		}
	}
}

// step is one block under vmMu: Verify, Accept, and the tip bookkeeping.
func (p *pipe) step(ctx context.Context, vm block.ChainVM, x parsed, b *bench, normal *bool) error {
	p.vmMu.Lock()
	defer p.vmMu.Unlock()
	h, blk := x.h, x.blk
	verified := false
	if wc, ok := blk.(block.WithVerifyContext); ok {
		should, err := wc.ShouldVerifyWithContext(ctx)
		if err != nil {
			return fmt.Errorf("height %d: ShouldVerifyWithContext: %w", h, err)
		}
		if should {
			if err := wc.VerifyWithContext(ctx, &block.Context{PChainHeight: x.pch}); err != nil {
				return fmt.Errorf("height %d: VerifyWithContext(pChainHeight=%d): %w", h, x.pch, err)
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
	b.accepted(x.item)
	// The tip is where the follower says it is. At it, the plugin
	// goes to normal operation and the preference follows every
	// block, as under avalanchego; during catch-up it is refreshed
	// now and then.
	tip := uint64(0)
	if p.f != nil {
		tip = p.f.AcceptedHead()
	}
	if !*normal && tip > 0 && h >= tip {
		if err := vm.SetState(ctx, snow.NormalOp); err != nil {
			return fmt.Errorf("SetState(NormalOp): %w", err)
		}
		*normal = true
		log.Printf("epochdb-host: caught up at height=%d, plugin in NormalOp", h)
	}
	if *normal || h%1000 == 0 {
		if err := vm.SetPreference(ctx, blk.ID()); err != nil {
			return fmt.Errorf("height %d: SetPreference: %w", h, err)
		}
	}
	return nil
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

// bench is the grep-friendly 10s sample line the A/B against `epochdb serve`
// reads: height, blocks, txs and gas in the window, cumulative mgas/s since
// the first accepted block, and who starved whom. wait is the time the
// verify loop spent blocked for its next parsed batch, counted from the
// first batch on (fetch or parse is the limiter); full is the time the host ring
// was at capacity, sampled at 100ms (the VM is the limiter).
type bench struct {
	tracker *pidTracker
	ring    chan item

	height    atomic.Uint64
	blocks    atomic.Uint64
	txs       atomic.Uint64
	gas       atomic.Uint64
	waitNs    atomic.Int64
	waitSince atomic.Int64 // unix nanos since the VM loop began its current wait, 0 while it runs
	fullNs    atomic.Int64
	firstAt   atomic.Int64 // unix nanos of the first accepted block, 0 before
}

// waited is the blocked time so far, the wait in progress included, so a
// long wait lands in the windows it spans rather than in the one it ends in.
func (b *bench) waited() int64 {
	w := b.waitNs.Load()
	if since := b.waitSince.Load(); since > 0 {
		w += time.Now().UnixNano() - since
	}
	return w
}

// accepted folds one accepted block in.
func (b *bench) accepted(it item) {
	b.firstAt.CompareAndSwap(0, time.Now().UnixNano())
	b.height.Store(it.h)
	b.blocks.Add(1)
	b.txs.Add(it.txs)
	b.gas.Add(it.gas)
}

func (b *bench) loop(ctx context.Context) {
	const window = 10 * time.Second
	const sample = 100 * time.Millisecond
	print := time.NewTicker(window)
	defer print.Stop()
	probe := time.NewTicker(sample)
	defer probe.Stop()
	var (
		pBlocks, pTxs, pGas uint64
		pWait, pFull        int64
	)
	for {
		select {
		case <-ctx.Done():
			return
		case <-probe.C:
			if len(b.ring) == cap(b.ring) {
				b.fullNs.Add(int64(sample))
			}
		case <-print.C:
			blocks, txs, gas := b.blocks.Load(), b.txs.Load(), b.gas.Load()
			wait, full := b.waited(), b.fullNs.Load()
			log.Print(b.line("", blocks-pBlocks, txs-pTxs, gas-pGas, wait-pWait, full-pFull, window))
			pBlocks, pTxs, pGas, pWait, pFull = blocks, txs, gas, wait, full
		}
	}
}

// exit prints the whole run as one window.
func (b *bench) exit() {
	log.Print(b.line("exit ", b.blocks.Load(), b.txs.Load(), b.gas.Load(), b.waited(), b.fullNs.Load(), b.elapsed()))
}

func (b *bench) elapsed() time.Duration {
	if first := b.firstAt.Load(); first > 0 {
		return time.Since(time.Unix(0, first))
	}
	return 0
}

func (b *bench) line(tag string, blocks, txs, gas uint64, waitNs, fullNs int64, window time.Duration) string {
	elapsed := b.elapsed()
	var cum, rate float64
	if elapsed > 0 {
		cum = float64(b.gas.Load()) / 1e6 / elapsed.Seconds()
	}
	if window > 0 {
		rate = float64(gas) / 1e6 / window.Seconds()
	}
	return fmt.Sprintf("bench %st=%ds h=%d blk=%d tx=%d mgas/s=%.1f cum=%.1f wait=%.1fs full=%.1fs host_rss=%dMB vm_rss=%dMB",
		tag, int(elapsed.Seconds()), b.height.Load(), blocks, txs,
		rate, cum,
		float64(waitNs)/1e9, float64(fullNs)/1e9,
		rssMB(os.Getpid()), rssMB(int(b.tracker.pid.Load())))
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

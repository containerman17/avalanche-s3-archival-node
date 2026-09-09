// Package validator is the Go shell of the epochdb validator: an avalanchego
// block.ChainVM that owns the mempool (subnet-evm's legacy txpool), tx gossip
// (avalanchego's p2p gossip SDK, subnet-evm's codec) and BuildBlock
// orchestration, and drives the Rust engine (rs/ffi, libepochdb_engine.a)
// over a block-granular C ABI for everything else: parse, verify, accept,
// build, state reads, JSON-RPC. No Go execution, no Go state, no Go trie.
package validator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"sync"
	"time"

	"github.com/ava-labs/avalanchego/database"
	"github.com/ava-labs/avalanchego/graft/subnet-evm/core/txpool"
	"github.com/ava-labs/avalanchego/graft/subnet-evm/core/txpool/legacypool"
	sevmparams "github.com/ava-labs/avalanchego/graft/subnet-evm/params"
	"github.com/ava-labs/avalanchego/graft/subnet-evm/plugin/evm"
	"github.com/ava-labs/avalanchego/graft/subnet-evm/plugin/evm/config"
	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/network/p2p"
	"github.com/ava-labs/avalanchego/network/p2p/gossip"
	"github.com/ava-labs/avalanchego/snow"
	"github.com/ava-labs/avalanchego/snow/consensus/snowman"
	"github.com/ava-labs/avalanchego/snow/engine/common"
	"github.com/ava-labs/avalanchego/snow/engine/snowman/block"
	"github.com/ava-labs/avalanchego/version"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/rlp"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"

	"github.com/containerman17/avalanche-s3-archival-node/chain"
	"github.com/containerman17/avalanche-s3-archival-node/vmexec"
)

// Version is what avalanchego's version API reports for this VM.
var Version = fmt.Sprintf("epochdb-validator/0.1 [rpcchainvm=%d]", version.RPCChainVMProtocol)

var (
	_ block.ChainVM                      = (*VM)(nil)
	_ block.BuildBlockWithContextChainVM = (*VM)(nil)
	_ block.WithVerifyContext            = (*Block)(nil)
)

// VM is the ChainVM. Zero value, then Initialize.
type VM struct {
	eng   *engine
	g     *vmexec.Genesis
	cfg   config.Config
	ctx   *snow.Context
	chain *poolChain
	pool  *txpool.TxPool
	net   *p2p.Network
	push  *gossip.PushGossiper[*evm.GossipEthTx]
	b     *builder
	m     *metrics

	mu        sync.Mutex
	preferred ids.ID
	lastX     [nCrossing]uint64 // crossing counters at the previous Accept

	gossipOnce sync.Once
	cancel     context.CancelFunc
	bg         context.Context
	wg         sync.WaitGroup
	closeOnce  sync.Once
}

func (vm *VM) Initialize(_ context.Context, chainCtx *snow.Context, _ database.Database,
	genesisBytes, upgradeBytes, configBytes []byte, _ []*common.Fx, appSender common.AppSender,
) error {
	if chainCtx.ChainDataDir == "" {
		return errors.New("validator: chain data dir is required")
	}
	c := &chain.Chain{
		GenesisJSON: genesisBytes, UpgradeJSON: upgradeBytes,
		NetworkID: chainCtx.NetworkID, SubnetID: chainCtx.SubnetID, BlockchainID: chainCtx.ChainID,
		VMKind: chain.SubnetEVM,
	}
	g, err := vmexec.ChainGenesis(c) // registers the libevm extras too
	if err != nil {
		return err
	}
	cfg, _, err := config.GetConfig(configBytes, chainCtx.NetworkID)
	if err != nil {
		return fmt.Errorf("validator: config: %w", err)
	}
	eng, err := open(chainCtx.ChainDataDir, genesisBytes, upgradeBytes, configBytes, chainCtx.ChainID, chainCtx.SubnetID, chainCtx.NetworkID)
	if err != nil {
		return err
	}
	vm.eng, vm.g, vm.cfg, vm.ctx = eng, g, cfg, chainCtx
	vm.m = newMetrics()
	if chainCtx.Metrics != nil {
		if err := chainCtx.Metrics.Register("epochdb", vm.m.reg); err != nil {
			eng.close()
			return err
		}
	}

	headID, _, err := eng.lastAccepted()
	if err != nil {
		eng.close()
		return err
	}
	head, err := vm.headHeader()
	if err != nil {
		eng.close()
		return err
	}
	vm.chain = newPoolChain(eng, g.Config, head, headID)
	vm.preferred = headID

	legacy := legacypool.New(legacypool.Config{
		Locals: cfg.PriorityRegossipAddresses, PriceLimit: cfg.TxPoolPriceLimit, PriceBump: cfg.TxPoolPriceBump,
		AccountSlots: cfg.TxPoolAccountSlots, GlobalSlots: cfg.TxPoolGlobalSlots,
		AccountQueue: cfg.TxPoolAccountQueue, GlobalQueue: cfg.TxPoolGlobalQueue,
		Lifetime: cfg.TxPoolLifetime.Duration, Rejournal: time.Hour,
	}, vm.chain)
	vm.pool, err = txpool.New(cfg.TxPoolPriceLimit, vm.chain, []txpool.SubPool{legacy})
	if err != nil {
		eng.close()
		return fmt.Errorf("validator: txpool: %w", err)
	}
	vm.pool.SetMinFee(sevmparams.GetExtra(g.Config).FeeConfig.MinBaseFee)
	vm.pool.SetGasTip(big.NewInt(0))

	vm.net, err = p2p.NewNetwork(chainCtx.Log, appSender, vm.m.reg, "p2p")
	if err != nil {
		eng.close()
		return err
	}
	vm.bg, vm.cancel = context.WithCancel(context.Background())
	vm.b = newBuilder(vm.pool, head.Hash())
	vm.wg.Add(1)
	go func() { defer vm.wg.Done(); vm.b.run(vm.bg) }()

	chainCtx.Log.Info("validator: engine open", zap.Stringer("chain", chainCtx.ChainID),
		zap.Stringer("chainId", g.Config.ChainID), zap.Uint64("height", head.Number.Uint64()), zap.String("data", chainCtx.ChainDataDir))
	return nil
}

// headHeader decodes the engine's accepted head header (one crossing).
func (vm *VM) headHeader() (*types.Header, error) {
	raw, err := vm.eng.headHeader()
	if err != nil {
		return nil, err
	}
	h := new(types.Header)
	if err := rlp.DecodeBytes(raw, h); err != nil {
		return nil, fmt.Errorf("validator: decode head header: %w", err)
	}
	return h, nil
}

// SetState: 1 bootstrapping, 2 normal op (snow.State values). Gossip and
// block building start at NormalOp, as in subnet-evm.
func (vm *VM) SetState(_ context.Context, st snow.State) error {
	if err := vm.eng.setState(uint32(st)); err != nil {
		return err
	}
	if st == snow.NormalOp {
		var err error
		vm.gossipOnce.Do(func() { err = vm.startGossip() })
		return err
	}
	return nil
}

// startGossip wires subnet-evm's tx gossip (same codec as a stock node) over
// avalanchego's p2p gossip SDK: pull + push gossipers, the bloom-backed set.
func (vm *VM) startGossip() error {
	set, err := evm.NewGossipEthTxPool(vm.pool, vm.m.reg)
	if err != nil {
		return err
	}
	validators := p2p.NewValidators(vm.ctx.Log, vm.ctx.SubnetID, vm.ctx.ValidatorState, time.Minute)
	handler, pull, push, err := gossip.NewSystem(vm.ctx.NodeID, vm.net, validators, set, evm.GossipEthTxMarshaller{},
		gossip.SystemConfig{
			Log: vm.ctx.Log, Registry: vm.m.reg, Namespace: "eth_tx_gossip",
			RequestPeriod: vm.cfg.PullGossipFrequency.Duration,
			PushGossipParams: gossip.BranchingFactor{
				StakePercentage: vm.cfg.PushGossipPercentStake, Validators: vm.cfg.PushGossipNumValidators, Peers: vm.cfg.PushGossipNumPeers,
			},
			PushRegossipParams: gossip.BranchingFactor{Validators: vm.cfg.PushRegossipNumValidators, Peers: vm.cfg.PushRegossipNumPeers},
			RegossipPeriod:     vm.cfg.RegossipFrequency.Duration,
		})
	if err != nil {
		return err
	}
	if err := vm.net.AddHandler(p2p.TxGossipHandlerID, handler); err != nil {
		return err
	}
	vm.push = push
	vm.b.setNormalOp()
	vm.wg.Add(3)
	go func() { defer vm.wg.Done(); set.Subscribe(vm.bg) }()
	go func() { defer vm.wg.Done(); gossip.Every(vm.bg, vm.ctx.Log, push, vm.cfg.PushGossipFrequency.Duration) }()
	go func() { defer vm.wg.Done(); gossip.Every(vm.bg, vm.ctx.Log, pull, vm.cfg.PullGossipFrequency.Duration) }()
	return nil
}

func (vm *VM) Shutdown(context.Context) error {
	if vm.eng == nil {
		return nil
	}
	vm.closeOnce.Do(func() {
		vm.cancel()
		vm.wg.Wait()
		vm.pool.Close()
		vm.eng.close()
	})
	return nil
}

func (vm *VM) Version(context.Context) (string, error) { return Version, nil }

func (vm *VM) HealthCheck(context.Context) (interface{}, error) {
	raw, err := vm.eng.health()
	if err != nil {
		return nil, err
	}
	pending, queued := vm.pool.Stats()
	return map[string]any{"engine": json.RawMessage(raw), "pending": pending, "queued": queued}, nil
}

func (vm *VM) Connected(ctx context.Context, id ids.NodeID, v *version.Application) error {
	return vm.net.Connected(ctx, id, v)
}
func (vm *VM) Disconnected(ctx context.Context, id ids.NodeID) error {
	return vm.net.Disconnected(ctx, id)
}
func (vm *VM) AppRequest(ctx context.Context, id ids.NodeID, req uint32, deadline time.Time, msg []byte) error {
	return vm.net.AppRequest(ctx, id, req, deadline, msg)
}
func (vm *VM) AppResponse(ctx context.Context, id ids.NodeID, req uint32, msg []byte) error {
	return vm.net.AppResponse(ctx, id, req, msg)
}
func (vm *VM) AppRequestFailed(ctx context.Context, id ids.NodeID, req uint32, e *common.AppError) error {
	return vm.net.AppRequestFailed(ctx, id, req, e)
}
func (vm *VM) AppGossip(ctx context.Context, id ids.NodeID, msg []byte) error {
	return vm.net.AppGossip(ctx, id, msg)
}

func (vm *VM) CreateHandlers(context.Context) (map[string]http.Handler, error) {
	// ponytail: no /ws. The engine's ws server needs a socket the FFI has no
	// door for; add one when a subscriber shows up.
	return map[string]http.Handler{"/rpc": http.HandlerFunc(vm.serveRPC)}, nil
}
func (vm *VM) NewHTTPHandler(context.Context) (http.Handler, error) { return nil, nil }

func (vm *VM) WaitForEvent(ctx context.Context) (common.Message, error) {
	return vm.b.waitForEvent(ctx, vm.chain.CurrentBlock())
}

func (vm *VM) SetPreference(_ context.Context, id ids.ID) error {
	vm.mu.Lock()
	vm.preferred = id
	vm.mu.Unlock()
	return nil
}

func (vm *VM) LastAccepted(context.Context) (ids.ID, error) {
	id, _, err := vm.eng.lastAccepted()
	return id, err
}

func (vm *VM) GetBlockIDAtHeight(_ context.Context, height uint64) (ids.ID, error) {
	id, err := vm.eng.blockIDAtHeight(height)
	if err != nil {
		return ids.Empty, database.ErrNotFound
	}
	return id, nil
}

// GetBlock: the engine's stored bytes, re-parsed for the metadata (a cache
// hit inside the engine).
func (vm *VM) GetBlock(_ context.Context, id ids.ID) (snowman.Block, error) {
	if id == ids.ID(vm.g.Hash) {
		return &Block{vm: vm, id: id, time: vm.g.Timestamp}, nil
	}
	raw, err := vm.eng.getBlock(id)
	if err != nil {
		return nil, database.ErrNotFound
	}
	m, err := vm.eng.parse(raw)
	if err != nil {
		return nil, err
	}
	return &Block{vm: vm, raw: raw, id: m.id, parent: m.parent, height: m.height, time: m.time}, nil
}

// ParseBlock hands the inner block bytes to the engine, which keeps the
// decoded block; Go keeps only the metadata and the slice it was given.
func (vm *VM) ParseBlock(_ context.Context, raw []byte) (snowman.Block, error) {
	m, err := vm.eng.parse(raw)
	if err != nil {
		return nil, err
	}
	return &Block{vm: vm, raw: raw, id: m.id, parent: m.parent, height: m.height, time: m.time}, nil
}

func (vm *VM) BuildBlock(context.Context) (snowman.Block, error) { return vm.buildBlock(0) }
func (vm *VM) BuildBlockWithContext(_ context.Context, bc *block.Context) (snowman.Block, error) {
	return vm.buildBlock(bc.PChainHeight)
}

// Block is one block the engine knows; raw is its inner bytes (nil for the
// genesis, which avalanchego never asks the bytes of).
type Block struct {
	vm     *VM
	raw    []byte
	id     ids.ID
	parent ids.ID
	height uint64
	time   uint64
}

func (b *Block) ID() ids.ID           { return b.id }
func (b *Block) Parent() ids.ID       { return b.parent }
func (b *Block) Height() uint64       { return b.height }
func (b *Block) Timestamp() time.Time { return time.Unix(int64(b.time), 0) }
func (b *Block) Bytes() []byte        { return b.raw }

func (b *Block) Verify(context.Context) error { return b.verify(0) }
func (b *Block) ShouldVerifyWithContext(context.Context) (bool, error) {
	return true, nil
}
func (b *Block) VerifyWithContext(_ context.Context, bc *block.Context) error {
	return b.verify(bc.PChainHeight)
}

// verify executes the block in the engine, state root inline; a block the
// engine built is a lookup.
func (b *Block) verify(pchainHeight uint64) error {
	if b.height == 0 {
		return nil
	}
	start := time.Now()
	_, _, txs, err := b.vm.eng.verify(b.id, pchainHeight)
	b.vm.m.verify.Observe(time.Since(start).Seconds())
	if err != nil {
		b.vm.ctx.Log.Warn("validator: verify failed", zap.Uint64("height", b.height), zap.Stringer("id", b.id), zap.Error(err))
		return err
	}
	b.vm.m.verifyTxs.Observe(float64(txs))
	return nil
}

// Accept applies the pending state in the engine, then moves the pool's head
// (one header crossing, one batched account refresh) and logs the block's
// crossings.
func (b *Block) Accept(context.Context) error {
	if b.height == 0 {
		return nil
	}
	vm := b.vm
	start := time.Now()
	if err := vm.eng.accept(b.id); err != nil {
		return err
	}
	h, err := vm.headHeader()
	if err != nil {
		return err
	}
	vm.chain.setHead(h, b.id)
	vm.b.setChainHead(h.Hash())
	vm.m.accept.Observe(time.Since(start).Seconds())

	now := vm.eng.snapshot()
	vm.mu.Lock()
	prev := vm.lastX
	vm.lastX = now
	vm.mu.Unlock()
	fields := make([]zap.Field, 0, nCrossing+3)
	fields = append(fields, zap.Uint64("height", b.height), zap.Uint64("gasUsed", h.GasUsed))
	total := uint64(0)
	for i := range now {
		d := now[i] - prev[i]
		total += d
		vm.m.crossings.WithLabelValues(crossingNames[i]).Add(float64(d))
		if d > 0 {
			fields = append(fields, zap.Uint64("x_"+crossingNames[i], d))
		}
	}
	fields = append(fields, zap.Uint64("x_total", total))
	vm.ctx.Log.Info("validator: accepted", fields...)
	return nil
}

func (b *Block) Reject(context.Context) error {
	if b.height == 0 {
		return nil
	}
	return b.vm.eng.reject(b.id)
}

// metrics: what a validator is measured by, on the chain's registry.
type metrics struct {
	reg                   *prometheus.Registry
	verify, build, accept prometheus.Histogram
	verifyTxs, buildTxs   prometheus.Histogram
	crossings             *prometheus.CounterVec
	admitted              prometheus.Counter
}

func newMetrics() *metrics {
	ms := func(name, help string) prometheus.Histogram {
		return prometheus.NewHistogram(prometheus.HistogramOpts{Name: name, Help: help,
			Buckets: []float64{.001, .002, .005, .01, .02, .05, .1, .2, .5, 1, 2, 5}})
	}
	n := func(name, help string) prometheus.Histogram {
		return prometheus.NewHistogram(prometheus.HistogramOpts{Name: name, Help: help,
			Buckets: []float64{0, 10, 50, 100, 200, 500, 1000, 2000, 5000}})
	}
	m := &metrics{
		reg:       prometheus.NewRegistry(),
		verify:    ms("epochdb_verify_seconds", "engine verify (execution + state root inline)"),
		build:     ms("epochdb_build_seconds", "BuildBlock: selection + engine build"),
		accept:    ms("epochdb_accept_seconds", "engine accept + pool head move"),
		verifyTxs: n("epochdb_verify_txs", "txs per verified block"),
		buildTxs:  n("epochdb_build_txs", "txs per built block"),
		crossings: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "epochdb_crossings_total", Help: "cgo calls into the engine"}, []string{"kind"}),
		admitted:  prometheus.NewCounter(prometheus.CounterOpts{Name: "epochdb_txs_admitted_total", Help: "txs the pool promoted to pending"}),
	}
	m.reg.MustRegister(m.verify, m.build, m.accept, m.verifyTxs, m.buildTxs, m.crossings, m.admitted)
	return m
}

// Package validator is the Go shell of the epochdb validator: an avalanchego
// block.ChainVM that owns tx gossip (avalanchego's p2p gossip SDK,
// subnet-evm's wire format) and BuildBlock orchestration, and drives the Rust
// engine (rs/ffi, libepochdb_engine.a) over a block-granular C ABI for
// everything else: parse, verify, accept, build, the mempool, JSON-RPC. No Go
// execution, no Go state, no Go trie, no Go pool.
package validator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	runtimemetrics "runtime/metrics"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ava-labs/avalanchego/database"
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
	"github.com/ava-labs/libevm/params"
	"github.com/ava-labs/libevm/rlp"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
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
	eng        *engine
	config     *params.ChainConfig
	cfg        config.Config
	ctx        *snow.Context
	net        *p2p.Network
	push       *gossip.PushGossiper[*gossipTx]
	set        *gossipSet
	ingest     ingestStats
	pushTarget int // push-gossip-target-bytes (0 = the SDK default)
	b          *builder
	m          *metrics

	mu        sync.Mutex
	head      *types.Header // the accepted head's header (Accept decodes it once)
	headID    ids.ID
	preferred ids.ID
	built     map[ids.ID]time.Time // blocks we built and when buildBlock returned them (Accept logs "proposer": "self" and the consensus latency; a node builds h+1 on its own unaccepted h, so one id is not enough)
	lastX     [nCrossing]uint64    // crossing counters at the previous Accept

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
	chainConfig, err := parseChainConfig(genesisBytes, upgradeBytes, chainCtx.NetworkUpgrades)
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
	vm.eng, vm.config, vm.cfg, vm.ctx = eng, chainConfig, cfg, chainCtx
	// "pprof-addr" in the chain config (e.g. "127.0.0.1:0") serves Go pprof
	// there; the bound address is logged (avalanchego passes the plugin no env).
	// "push-gossip-target-bytes": one push gossip message per push-gossip-frequency
	// tick (the SDK's 20 KiB default, ~180 transfers, is 1.8k tx/s at 100 ms;
	// 65536 at "25ms" carried 20k tx/s to each peer, but needs the node's
	// throttler-inbound-bandwidth-refill-rate raised above the 512 KiB/s
	// default, or consensus messages queue behind the gossip and the chain stalls).
	var dbg struct {
		Pprof      string `json:"pprof-addr"`
		PushTarget int    `json:"push-gossip-target-bytes"`
		Direct     string `json:"rpc-direct-addr"`
		Public     bool   `json:"rpc-direct-allow-public"`
	}
	_ = json.Unmarshal(configBytes, &dbg)
	vm.pushTarget = dbg.PushTarget
	// "rpc-direct-addr" (e.g. "127.0.0.1:0"): MEASUREMENT ONLY, off by default.
	// A plain net/http server inside the plugin process serving the same /rpc
	// handler without avalanchego's HTTP server -> gRPC ghttp hop, so a
	// client's round trip can be split into the hop and the plugin. It
	// bypasses avalanchego's HTTP auth, API throttling and TLS, so it binds
	// loopback or a private (RFC 1918 / link-local) address only, unless
	// "rpc-direct-allow-public": true is set as well. Never on a production node.
	if dbg.Direct != "" {
		if err := vm.serveDirect(dbg.Direct, dbg.Public); err != nil {
			chainCtx.Log.Warn("validator: rpc-direct-addr refused", zap.Error(err))
		}
	}
	if dbg.Pprof != "" {
		if l, err := net.Listen("tcp", dbg.Pprof); err == nil {
			chainCtx.Log.Info("validator: pprof", zap.Stringer("addr", l.Addr()))
			go http.Serve(l, nil)
		}
	}
	vm.m = newMetrics(eng)
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
	vm.head, vm.headID, vm.preferred = head, headID, headID
	vm.built = map[ids.ID]time.Time{}

	vm.net, err = p2p.NewNetwork(chainCtx.Log, appSender, vm.m.reg, "p2p")
	if err != nil {
		eng.close()
		return err
	}
	vm.bg, vm.cancel = context.WithCancel(context.Background())
	vm.b = newBuilder(eng, chainCtx.Log)

	chainCtx.Log.Info("validator: engine open", zap.Stringer("chain", chainCtx.ChainID),
		zap.Stringer("chainId", chainConfig.ChainID), zap.Uint64("height", head.Number.Uint64()), zap.String("data", chainCtx.ChainDataDir))
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

// current: the accepted head's header and id.
func (vm *VM) current() (*types.Header, ids.ID) {
	vm.mu.Lock()
	defer vm.mu.Unlock()
	return vm.head, vm.headID
}

// SetState: the engine takes 1 = bootstrapping (roots checked one block
// behind), 2 = normal op (root inline, build allowed). Gossip and block
// building start at NormalOp, as in subnet-evm.
func (vm *VM) SetState(_ context.Context, st snow.State) error {
	engState := uint32(1)
	if st == snow.NormalOp {
		engState = 2
	}
	if err := vm.eng.setState(engState); err != nil {
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
// avalanchego's p2p gossip SDK: pull + push gossipers, the bloom-backed set
// over the engine's pool. Inbound push messages are admitted per message
// (one crossing), the pool's new txs are drained into the push gossiper on
// every push tick.
func (vm *VM) startGossip() error {
	set, err := newGossipSet(vm.eng, vm.m.reg)
	if err != nil {
		return err
	}
	vm.set = set
	validators := p2p.NewValidators(vm.ctx.Log, vm.ctx.SubnetID, vm.ctx.ValidatorState, time.Minute)
	handler, pull, push, err := gossip.NewSystem(vm.ctx.NodeID, vm.net, validators, set, gossipMarshaller{},
		gossip.SystemConfig{
			Log: vm.ctx.Log, Registry: vm.m.reg, Namespace: "eth_tx_gossip",
			RequestPeriod: vm.cfg.PullGossipFrequency.Duration, TargetMessageSize: vm.pushTarget,
			PushGossipParams: gossip.BranchingFactor{
				StakePercentage: vm.cfg.PushGossipPercentStake, Validators: vm.cfg.PushGossipNumValidators, Peers: vm.cfg.PushGossipNumPeers,
			},
			PushRegossipParams: gossip.BranchingFactor{Validators: vm.cfg.PushRegossipNumValidators, Peers: vm.cfg.PushRegossipNumPeers},
			RegossipPeriod:     vm.cfg.RegossipFrequency.Duration,
		})
	if err != nil {
		return err
	}
	if err := vm.net.AddHandler(p2p.TxGossipHandlerID, &txHandler{Handler: handler, set: set, log: vm.ctx.Log}); err != nil {
		return err
	}
	vm.push = push
	vm.b.setNormalOp()
	vm.wg.Add(2)
	go func() { defer vm.wg.Done(); vm.pushLoop(push, vm.cfg.PushGossipFrequency.Duration) }()
	go func() { defer vm.wg.Done(); gossip.Every(vm.bg, vm.ctx.Log, pull, vm.cfg.PullGossipFrequency.Duration) }()
	return nil
}

// pushLoop: every push-gossip-frequency tick, the pool's newly admitted txs
// (one crossing) go to the push gossiper and the bloom filter, then ONE push
// round of at most push-gossip-target-bytes: the round size and the tick
// bound what a node sends each peer. Draining everything at once is NOT an
// option: one round per 64 KiB of new bytes shipped a 400k-tx pool in one
// tick (44 MB per peer), consensus messages queued behind it and the chain
// stalled ("block processing too long"); the same happens at a steady rate
// above the peer's inbound bandwidth throttle.
func (vm *VM) pushLoop(push *gossip.PushGossiper[*gossipTx], period time.Duration) {
	if period <= 0 {
		period = 100 * time.Millisecond
	}
	t := time.NewTicker(period)
	defer t.Stop()
	for {
		select {
		case <-vm.bg.Done():
			return
		case <-t.C:
		}
		raws, gone, err := vm.eng.poolDrainGossip()
		if err != nil {
			vm.ctx.Log.Warn("validator: pool drain failed", zap.Error(err))
		} else if len(raws) > 0 || len(gone) > 0 {
			txs := make([]*gossipTx, len(raws))
			for i, r := range raws {
				txs[i] = newGossipTx(r)
			}
			vm.set.added(txs, gone)
			push.Add(txs...)
		}
		if err := push.Gossip(vm.bg); err != nil && vm.bg.Err() == nil {
			vm.ctx.Log.Warn("validator: push gossip failed", zap.Error(err))
		}
	}
}

func (vm *VM) Shutdown(context.Context) error {
	if vm.eng == nil {
		return nil
	}
	vm.closeOnce.Do(func() {
		vm.cancel()
		vm.wg.Wait()
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
	pending, queued := vm.eng.poolStatus()
	var eh struct {
		AddMS uint64 `json:"pool-add-ms"`
	}
	_ = json.Unmarshal(raw, &eh)
	return map[string]any{"engine": json.RawMessage(raw), "pending": pending, "queued": queued, "ingest": vm.ingest.health(eh.AddMS), "build": vm.b.stats.health()}, nil
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
	return vm.b.waitForEvent(ctx, func() (*types.Header, ids.ID) {
		vm.mu.Lock()
		defer vm.mu.Unlock()
		return vm.head, vm.preferred
	})
}

func (vm *VM) SetPreference(_ context.Context, id ids.ID) error {
	vm.mu.Lock()
	changed := vm.preferred != id
	vm.preferred = id
	vm.mu.Unlock()
	if changed {
		vm.b.preferenceChanged()
	}
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

// GetBlock: the engine's stored bytes (the genesis assembled), re-parsed for
// the metadata (a cache hit inside the engine).
func (vm *VM) GetBlock(_ context.Context, id ids.ID) (snowman.Block, error) {
	raw, err := vm.eng.getBlock(id)
	if errors.Is(err, errNotFound) {
		// Only a true unknown id: consensus fetches it from a peer. Any
		// other error for a block consensus knows shuts the chain down
		// (avalanchego's rpcchainvm server calls GetBlock before Reject).
		return nil, database.ErrNotFound
	}
	if err != nil {
		vm.ctx.Log.Error("validator: GetBlock failed", zap.Stringer("id", id), zap.Error(err))
		return nil, err
	}
	start := time.Now()
	m, err := vm.eng.parse(raw)
	if err != nil {
		return nil, err
	}
	vm.ctx.Log.Info("validator: getblock", zap.Uint64("height", m.height), zap.Duration("took", time.Since(start)))
	return &Block{vm: vm, raw: raw, id: m.id, parent: m.parent, height: m.height, time: m.time}, nil
}

// ParseBlock hands the inner block bytes to the engine, which keeps the
// decoded block; Go keeps only the metadata and the slice it was given.
// "parsed" is logged TWICE per height on a peer: rpcchainvm's ParseBlock
// (the PushQuery/Put bytes, through proposervm's inner-block parse) and its
// BlockVerify, which re-parses the bytes it was handed before calling Verify.
// On the proposer only BlockVerify parses (BuildBlock returned the block).
// "getblock" is once per height: BlockAccept fetches the block by id.
func (vm *VM) ParseBlock(_ context.Context, raw []byte) (snowman.Block, error) {
	start := time.Now()
	m, err := vm.eng.parse(raw)
	if err != nil {
		return nil, err
	}
	vm.ctx.Log.Info("validator: parsed", zap.Uint64("height", m.height), zap.Int("bytes", len(raw)), zap.Duration("took", time.Since(start)))
	return &Block{vm: vm, raw: raw, id: m.id, parent: m.parent, height: m.height, time: m.time}, nil
}

func (vm *VM) BuildBlock(context.Context) (snowman.Block, error) { return vm.buildBlock(0) }
func (vm *VM) BuildBlockWithContext(_ context.Context, bc *block.Context) (snowman.Block, error) {
	return vm.buildBlock(bc.PChainHeight)
}

// Block is one block the engine knows; raw is its inner bytes.
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
	// A block we built: this Verify is avalanchego handing the bytes back
	// (rpcchainvm's BlockVerify re-parses them, then verifies) right before
	// consensus adds the block and PushQueries it to the peers, so it is the
	// closest observable point to "sent" (BuildBlock's response already
	// carried the bytes, there is no later Bytes() fetch). Once per height.
	b.vm.mu.Lock()
	builtAt, self := b.vm.built[b.id]
	b.vm.mu.Unlock()
	if self {
		b.vm.ctx.Log.Info("validator: block-sent", zap.Uint64("height", b.height), zap.Stringer("id", b.id),
			zap.Int64("t_since_built_ms", time.Since(builtAt).Milliseconds()))
	}
	start := time.Now()
	_, _, txs, err := b.vm.eng.verify(b.id, pchainHeight)
	b.vm.m.verify.Observe(time.Since(start).Seconds())
	if err != nil {
		b.vm.ctx.Log.Warn("validator: verify failed", zap.Uint64("height", b.height), zap.Stringer("id", b.id), zap.Error(err))
		return err
	}
	b.vm.m.verifyTxs.Observe(float64(txs))
	b.vm.ctx.Log.Info("validator: verified", zap.Uint64("height", b.height), zap.Uint64("txs", txs), zap.Duration("took", time.Since(start)))
	return nil
}

// Accept applies the pending state in the engine (which moves its pool for
// the block's senders in the same call), decodes the new head header (one
// crossing) and logs the block's crossings.
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
	vm.m.accept.Observe(time.Since(start).Seconds())

	now := vm.eng.snapshot()
	vm.mu.Lock()
	vm.head, vm.headID = h, b.id
	prev := vm.lastX
	vm.lastX = now
	builtAt, self := vm.built[b.id]
	delete(vm.built, b.id)
	for id, t := range vm.built { // built blocks consensus never named (superseded before they were proposed)
		if start.Sub(t) > time.Minute {
			delete(vm.built, id)
		}
	}
	vm.mu.Unlock()
	fields := make([]zap.Field, 0, nCrossing+5)
	fields = append(fields, zap.Uint64("height", b.height), zap.Uint64("gasUsed", h.GasUsed), zap.Duration("took", time.Since(start)))
	if self {
		fields = append(fields, zap.String("proposer", "self"), zap.Duration("buildToAccept", start.Sub(builtAt)))
	}
	total := uint64(0)
	for i := range now {
		d := now[i] - prev[i]
		total += d
		vm.m.crossings.WithLabelValues(crossingNames[i]).Add(float64(d))
		if d > 0 {
			fields = append(fields, zap.Uint64("x_"+crossingNames[i], d))
		}
	}
	pending, queued := vm.eng.poolStatus()
	fields = append(fields, zap.Uint64("x_total", total), zap.Uint64("pending", pending), zap.Uint64("queued", queued))
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
	buildEmpty            prometheus.Counter
}

func newMetrics(eng *engine) *metrics {
	ms := func(name, help string) prometheus.Histogram {
		return prometheus.NewHistogram(prometheus.HistogramOpts{Name: name, Help: help,
			Buckets: []float64{.001, .002, .005, .01, .02, .05, .1, .2, .5, 1, 2, 5}})
	}
	n := func(name, help string) prometheus.Histogram {
		return prometheus.NewHistogram(prometheus.HistogramOpts{Name: name, Help: help,
			Buckets: []float64{0, 10, 50, 100, 200, 500, 1000, 2000, 5000, 10000, 20000, 50000}})
	}
	m := &metrics{
		reg:        prometheus.NewRegistry(),
		verify:     ms("epochdb_verify_seconds", "engine verify (execution + state root inline)"),
		build:      ms("epochdb_build_seconds", "the engine's build call (candidates from the pool, execution, header, state root)"),
		accept:     ms("epochdb_accept_seconds", "engine accept (state + pool head move) + head header"),
		verifyTxs:  n("epochdb_verify_txs", "txs per verified block"),
		buildTxs:   n("epochdb_build_txs", "txs per built block"),
		crossings:  prometheus.NewCounterVec(prometheus.CounterOpts{Name: "epochdb_crossings_total", Help: "cgo calls into the engine"}, []string{"kind"}),
		buildEmpty: prometheus.NewCounter(prometheus.CounterOpts{Name: "epochdb_build_empty_total", Help: "BuildBlock calls whose every candidate the engine skipped (no block)"}),
	}
	m.reg.MustRegister(m.verify, m.build, m.accept, m.verifyTxs, m.buildTxs, m.crossings, m.buildEmpty)
	// The Go heap, GC share and RSS of the plugin process. (The stock Go and
	// process collectors do not survive the rpcchainvm gatherer, so: gauges.)
	gauge := func(name, help string, f func() float64) {
		m.reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: name, Help: help}, f))
	}
	gauge("epochdb_go_heap_alloc_bytes", "live Go heap objects", func() float64 { return runtimeMetric("/memory/classes/heap/objects:bytes") })
	gauge("epochdb_go_gc_cycles_total", "GC cycles", func() float64 { return runtimeMetric("/gc/cycles/total:gc-cycles") })
	gauge("epochdb_go_gc_cpu_fraction", "GC CPU seconds / total CPU seconds since start",
		func() float64 {
			return runtimeMetric("/cpu/classes/gc/total:cpu-seconds") / max(runtimeMetric("/cpu/classes/total:cpu-seconds"), 1e-9)
		})
	gauge("epochdb_process_rss_bytes", "resident set size of the plugin process", processRSS)
	gauge("epochdb_pool_pending", "executable txs in the engine's pool", func() float64 { p, _ := eng.poolStatus(); return float64(p) })
	gauge("epochdb_pool_queued", "non-executable txs in the engine's pool", func() float64 { _, q := eng.poolStatus(); return float64(q) })
	return m
}

func runtimeMetric(name string) float64 {
	s := []runtimemetrics.Sample{{Name: name}}
	runtimemetrics.Read(s)
	switch s[0].Value.Kind() {
	case runtimemetrics.KindUint64:
		return float64(s[0].Value.Uint64())
	case runtimemetrics.KindFloat64:
		return s[0].Value.Float64()
	}
	return 0
}

func processRSS() float64 {
	raw, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return 0
	}
	f := strings.Fields(string(raw))
	if len(f) < 2 {
		return 0
	}
	pages, _ := strconv.ParseFloat(f[1], 64)
	return pages * float64(os.Getpagesize())
}

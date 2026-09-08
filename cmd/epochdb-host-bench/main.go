// Command epochdb-host-bench is cmd/epochdb-host with the fetcher replaced
// by a container dump file (cmd/epochdb-dump-containers' format): it drives
// a VM plugin over avalanchego's own rpcchainvm client exactly as the host
// does (Initialize with every host-side service, SetState, BatchedParseBlock
// or ParseBlock, Verify, Accept, SetPreference, CreateHandlers mounted, Shutdown),
// prints the host's bench line, and at the end of the dump checks the
// plugin's JSON-RPC answers against the dump. Runs the stock subnet-evm
// plugin and epochdb-rs alike; no network unless the plugin asks the
// validator state (then --node is used).
//
// The dump dir must hold chain.json (genesisData base64, blockchainID,
// subnetID, networkID) and optionally upgrade.json, as the dump tool leaves
// them.
package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
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
	"github.com/containerman17/avalanche-s3-archival-node/fetch"
)

func main() {
	fs := flag.NewFlagSet("epochdb-host-bench", flag.ExitOnError)
	dumpPath := fs.String("dump", "", "container dump file ([u64 height][u32 len][container], heights from 1); chain.json and upgrade.json beside it")
	from := fs.Uint64("from", 1, "first height to feed (the plugin's last accepted height + 1 must equal it)")
	to := fs.Uint64("to", 0, "last height to feed (0 = the file's end)")
	vmPath := fs.String("vm", "", "plugin binary (stock subnet-evm or epochdb-rs)")
	dataDir := fs.String("data", "./data", "data directory: plugin db, chain data dir, BLS key")
	nodeURI := fs.String("node", "https://api.avax.network", "comma-separated Avalanche RPC node URIs for the validator state (only if the plugin asks)")
	httpAddr := fs.String("http", "127.0.0.1:19900", "HTTP listen address; the plugin's handlers mount at /ext/bc/<chainID>/<path>")
	batch := fs.Int("batch", 256, "blocks per BatchedParseBlock (1 = ParseBlock one at a time); up to 2 parsed batches wait ahead of Verify")
	configJSON := fs.String("config", `{"state-sync-enabled":false}`, "the VM's config bytes (JSON)")
	serve := fs.Bool("serve", false, "after the dump, keep serving HTTP until SIGINT instead of shutting down")
	fs.Parse(os.Args[1:])
	if *dumpPath == "" || *vmPath == "" {
		log.Fatal("epochdb-host-bench: --dump and --vm are required")
	}
	if *batch < 1 {
		log.Fatal("epochdb-host-bench: --batch must be at least 1")
	}
	if err := os.MkdirAll(*dataDir, 0o755); err != nil {
		log.Fatal(err)
	}
	c, err := loadChain(filepath.Dir(*dumpPath))
	if err != nil {
		log.Fatalf("epochdb-host-bench: %v", err)
	}
	// The libevm extras: subnet-evm headers decode (tx and gas counts, the
	// header hash of the check).
	fetch.RegisterExtras(chain.SubnetEVM)
	d, err := openDump(*dumpPath, *from, *to)
	if err != nil {
		log.Fatalf("epochdb-host-bench: %v", err)
	}
	log.Printf("epochdb-host-bench: dump %s heights %d..%d chain %s subnet %s network %d", *dumpPath, *from, d.Last(), c.blockchainID, c.subnetID, c.networkID)
	// The genesis hash, for epochdb-rs's TrivialEngine (it executes nothing,
	// so it cannot compute the genesis header): block 1's parent, added to the
	// config bytes as "genesis-id". Stock subnet-evm ignores unknown keys.
	if raw, ok, err := d.GetByHeight(1); err == nil && ok {
		inner, _ := unwrap(raw, &upgrade.Config{}, new(uint64))
		var eb ethtypes.Block
		if err := rlp.DecodeBytes(inner, &eb); err == nil {
			var cfg map[string]any
			if err := json.Unmarshal([]byte(*configJSON), &cfg); err != nil {
				log.Fatalf("epochdb-host-bench: --config: %v", err)
			}
			cfg["genesis-id"] = eb.ParentHash().Hex()
			b, _ := json.Marshal(cfg)
			*configJSON = string(b)
			log.Printf("epochdb-host-bench: genesis hash %s (block 1's parent), config %s", eb.ParentHash().Hex(), *configJSON)
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	nodeID := ids.GenerateTestNodeID()
	blsKey, err := localsigner.FromFileOrPersistNew(filepath.Join(*dataDir, "signer.key"))
	if err != nil {
		log.Fatalf("epochdb-host-bench: bls key: %v", err)
	}
	xChainID, cChainID, avaxAssetID, err := primaryIDs(c.networkID)
	if err != nil {
		log.Fatal(err)
	}

	logger := logging.NewLogger("host", logging.NewWrappedCore(logging.Info, os.Stderr, logging.Plain.ConsoleEncoder()))
	tracker := &pidTracker{}
	v, err := rpcchainvm.NewFactory(*vmPath, tracker, runtime.NewManager(), metrics.NewPrefixGatherer()).New(logger)
	if err != nil {
		log.Fatalf("epochdb-host-bench: launch plugin: %v", err)
	}
	vm := v.(block.ChainVM)
	if ver, err := vm.Version(ctx); err == nil {
		log.Printf("epochdb-host-bench: plugin version %q", ver)
	}

	db, err := pebbledb.New(filepath.Join(*dataDir, "db"), nil, logger, prometheus.NewRegistry())
	if err != nil {
		log.Fatalf("epochdb-host-bench: db: %v", err)
	}
	defer db.Close()

	chainDataDir := filepath.Join(*dataDir, "chainData")
	os.MkdirAll(chainDataDir, 0o755)
	snowCtx := &snow.Context{
		NetworkID:       c.networkID,
		SubnetID:        c.subnetID,
		ChainID:         c.blockchainID,
		NodeID:          nodeID,
		PublicKey:       blsKey.PublicKey(),
		NetworkUpgrades: upgrade.GetConfig(c.networkID),
		XChainID:        xChainID,
		CChainID:        cChainID,
		AVAXAssetID:     avaxAssetID,
		Log:             logger,
		SharedMemory:    avaatomic.NewMemory(prefixdb.New([]byte("atomic"), db)).NewSharedMemory(c.blockchainID),
		BCLookup:        ids.NewAliaser(),
		Metrics:         metrics.NewPrefixGatherer(),
		WarpSigner:      warp.NewSigner(blsKey, c.networkID, c.blockchainID),
		ValidatorState:  newRPCValidatorState(strings.Split(*nodeURI, ","), c.blockchainID, c.subnetID),
		ChainDataDir:    chainDataDir,
	}
	t0 := time.Now()
	if err := vm.Initialize(ctx, snowCtx, prefixdb.New([]byte("vm"), db), c.genesis, c.upgrade,
		[]byte(*configJSON), nil, noopSender{}); err != nil {
		log.Fatalf("epochdb-host-bench: Initialize: %v", err)
	}
	log.Printf("epochdb-host-bench: Initialize took %s", time.Since(t0).Round(time.Millisecond))
	if err := vm.SetState(ctx, snow.Bootstrapping); err != nil {
		log.Fatalf("epochdb-host-bench: SetState(Bootstrapping): %v", err)
	}

	handlers, err := vm.CreateHandlers(ctx)
	if err != nil {
		log.Fatalf("epochdb-host-bench: CreateHandlers: %v", err)
	}
	mux := http.NewServeMux()
	rpcPath := ""
	for ext, h := range handlers {
		mux.Handle("/ext/bc/"+c.blockchainID.String()+ext, h)
		log.Printf("epochdb-host-bench: mounted /ext/bc/%s%s", c.blockchainID, ext)
		if ext == "/rpc" {
			rpcPath = "/ext/bc/" + c.blockchainID.String() + ext
		}
	}
	go func() {
		if err := http.ListenAndServe(*httpAddr, mux); err != nil {
			log.Printf("epochdb-host-bench: http: %v", err)
			stop()
		}
	}()

	lastID, err := vm.LastAccepted(ctx)
	if err != nil {
		log.Fatalf("epochdb-host-bench: LastAccepted: %v", err)
	}
	last, err := vm.GetBlock(ctx, lastID)
	if err != nil {
		log.Fatalf("epochdb-host-bench: GetBlock(last): %v", err)
	}
	log.Printf("epochdb-host-bench: plugin last accepted height=%d id=%s", last.Height(), lastID)
	if last.Height()+1 != *from {
		log.Fatalf("epochdb-host-bench: plugin is at height %d, --from is %d: use --from %d", last.Height(), *from, last.Height()+1)
	}

	p := &pipe{
		d: d, from: *from, batch: *batch,
		ring:    make(chan item, 4096),
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
		log.Printf("epochdb-host-bench: FATAL: %v", err)
	}
	b.exit()
	if err == nil {
		check(ctx, vm, mux, rpcPath, d, b.height.Load())
		if *serve {
			log.Printf("epochdb-host-bench: --serve: HTTP stays up at %s until SIGINT", *httpAddr)
			<-ctx.Done()
		}
	}
	sctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	t0 = time.Now()
	if err := vm.Shutdown(sctx); err != nil {
		log.Printf("epochdb-host-bench: plugin shutdown: %v", err)
	}
	log.Printf("epochdb-host-bench: stopped at height=%d, Shutdown took %s", b.height.Load(), time.Since(t0).Round(time.Millisecond))
}

// chainDesc is chain.json (the chain package's cache file) plus upgrade.json.
type chainDesc struct {
	networkID    uint32
	subnetID     ids.ID
	blockchainID ids.ID
	genesis      []byte
	upgrade      []byte
}

func loadChain(dir string) (*chainDesc, error) {
	raw, err := os.ReadFile(filepath.Join(dir, "chain.json"))
	if err != nil {
		return nil, err
	}
	var f struct {
		NetworkID    uint32 `json:"networkID"`
		BlockchainID string `json:"blockchainID"`
		SubnetID     string `json:"subnetID"`
		GenesisData  string `json:"genesisData"`
	}
	if err := json.Unmarshal(raw, &f); err != nil {
		return nil, fmt.Errorf("chain.json: %w", err)
	}
	c := &chainDesc{networkID: f.NetworkID}
	if c.blockchainID, err = ids.FromString(f.BlockchainID); err != nil {
		return nil, fmt.Errorf("chain.json: blockchainID: %w", err)
	}
	if c.subnetID, err = ids.FromString(f.SubnetID); err != nil {
		return nil, fmt.Errorf("chain.json: subnetID: %w", err)
	}
	if c.genesis, err = base64.StdEncoding.DecodeString(f.GenesisData); err != nil {
		return nil, fmt.Errorf("chain.json: genesisData: %w", err)
	}
	c.upgrade, err = os.ReadFile(filepath.Join(dir, "upgrade.json"))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	return c, nil
}

// check asks the mounted /rpc handler what the plugin says about the head and
// compares it with the dump: eth_blockNumber == the last fed height, and
// eth_getBlockByNumber(head).hash == keccak(header) of that container.
func check(ctx context.Context, vm block.ChainVM, mux *http.ServeMux, rpcPath string, d *dumpSource, head uint64) {
	if rpcPath == "" {
		log.Print("check: no /rpc handler mounted")
		return
	}
	call := func(method string, params ...any) (json.RawMessage, error) {
		body, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
		req := httptest.NewRequest(http.MethodPost, "http://host"+rpcPath, bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)
		var resp struct {
			Result json.RawMessage `json:"result"`
			Error  *struct {
				Message string `json:"message"`
			} `json:"error"`
		}
		if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
			return nil, fmt.Errorf("%s: status %d body %q: %w", method, w.Code, w.Body.String(), err)
		}
		if resp.Error != nil {
			return nil, fmt.Errorf("%s: %s", method, resp.Error.Message)
		}
		return resp.Result, nil
	}
	var numHex string
	if r, err := call("eth_blockNumber"); err != nil {
		log.Printf("check: %v", err)
		return
	} else if err := json.Unmarshal(r, &numHex); err != nil {
		log.Printf("check: eth_blockNumber: %v", err)
		return
	}
	num, _ := strconv.ParseUint(strings.TrimPrefix(numHex, "0x"), 16, 64)
	var blk struct {
		Hash string `json:"hash"`
	}
	if r, err := call("eth_getBlockByNumber", numHex, false); err != nil {
		log.Printf("check: %v", err)
		return
	} else if err := json.Unmarshal(r, &blk); err != nil {
		log.Printf("check: eth_getBlockByNumber: %v", err)
		return
	}
	raw, ok, err := d.GetByHeight(num)
	want := "(not in the dump)"
	if err == nil && ok {
		inner, _ := unwrap(raw, &upgrade.Config{}, new(uint64))
		var eb ethtypes.Block
		if err := rlp.DecodeBytes(inner, &eb); err == nil {
			want = eb.Hash().Hex()
		}
	}
	lastID, _ := vm.LastAccepted(ctx)
	log.Printf("check eth_blockNumber=%d want=%d hash=%s keccak(header)=%s last_accepted=%s match=%v",
		num, head, blk.Hash, want, lastID, num == head && blk.Hash == want)
}

// item is one container after unwrap: the inner VM bytes, the P-chain height
// for its block.Context, and the header facts the bench line wants.
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

// pipe is the host loop in three stages: pull (dump -> ring, unwrapped),
// parse (ring -> batches of VM-parsed blocks, up to 2 waiting) and drive
// (Verify + Accept, strictly sequential per block). vmMu: ONE VM call at a
// time, the way avalanchego's engine calls it (under ctx.Lock); the
// rpcchainvm client's block cache is not goroutine-safe.
type pipe struct {
	d       *dumpSource
	from    uint64
	batch   int
	ring    chan item
	batches chan []parsed

	vmMu sync.Mutex
	stop context.CancelFunc
	once sync.Once
	err  error
}

func (p *pipe) fail(err error) {
	p.once.Do(func() {
		p.err = err
		p.stop()
	})
}

// pull moves containers from the dump into the ring, in height order.
func (p *pipe) pull(ctx context.Context, upgrades *upgrade.Config) {
	defer close(p.ring)
	var parentPCH uint64
	for h := p.from; ; h++ {
		raw, ok, err := p.d.GetByHeight(h)
		if err != nil {
			p.fail(err)
			return
		}
		if !ok {
			return
		}
		inner, pch := unwrap(raw, upgrades, &parentPCH)
		it := item{h: h, inner: inner, pch: pch}
		var blk ethtypes.Block
		if err := rlp.DecodeBytes(inner, &blk); err == nil {
			it.txs, it.gas = uint64(len(blk.Transactions())), blk.GasUsed()
		}
		select {
		case p.ring <- it:
		case <-ctx.Done():
			return
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
// one BatchedParseBlock per batch (--batch 1: ParseBlock, one round trip per
// block, what avalanchego's bootstrapper does), and queues the parsed batches
// for drive.
func (p *pipe) parse(ctx context.Context, vm block.ChainVM) {
	defer close(p.batches)
	bvm, _ := vm.(block.BatchedChainVM)
	if p.batch == 1 {
		bvm = nil
	}
	log.Printf("epochdb-host-bench: batched parse=%v batch=%d", bvm != nil, p.batch)
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
				log.Print("epochdb-host-bench: plugin has no BatchedParseBlock, parsing one block at a time")
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

// drive is the VM loop proper: for every parsed block in height order, Verify
// (with the P-chain height the proposervm would hand the inner VM), then
// Accept; at the dump's last block SetState(NormalOp) and SetPreference, as
// the host does at the follower's tip.
func (p *pipe) drive(ctx context.Context, vm block.ChainVM, b *bench) error {
	var normal bool
	for {
		t0 := time.Now()
		b.waitSince.Store(t0.UnixNano())
		batch, ok := <-p.batches
		b.waitSince.Store(0)
		b.waitNs.Add(int64(time.Since(t0)))
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
	tip := p.d.Last()
	if !*normal && h >= tip {
		if err := vm.SetState(ctx, snow.NormalOp); err != nil {
			return fmt.Errorf("SetState(NormalOp): %w", err)
		}
		*normal = true
		log.Printf("epochdb-host-bench: caught up at height=%d, plugin in NormalOp", h)
	}
	if *normal || h%1000 == 0 {
		if err := vm.SetPreference(ctx, blk.ID()); err != nil {
			return fmt.Errorf("height %d: SetPreference: %w", h, err)
		}
	}
	return nil
}

// unwrap returns the inner VM bytes of a container and the P-chain height for
// its block.Context (proposervm's rule: Granite: the epoch's; Etna: the
// header's own; earlier: the parent header's; pre-fork container: 0).
func unwrap(raw []byte, upgrades *upgrade.Config, parentPCH *uint64) ([]byte, uint64) {
	pb, err := proposerblock.ParseWithoutVerification(raw)
	if err != nil {
		if _, _, rest, err := rlp.Split(raw); err == nil {
			raw = raw[:len(raw)-len(rest)]
		}
		return raw, 0
	}
	signed, ok := pb.(proposerblock.SignedBlock)
	if !ok {
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

// bench is the host's 10s sample line: height, blocks, txs and gas in the
// window, cumulative mgas/s since the first accepted block, wait (the verify
// loop blocked for its next parsed batch) and full (the ring at capacity).
type bench struct {
	tracker *pidTracker
	ring    chan item

	height    atomic.Uint64
	blocks    atomic.Uint64
	txs       atomic.Uint64
	gas       atomic.Uint64
	waitNs    atomic.Int64
	waitSince atomic.Int64
	fullNs    atomic.Int64
	firstAt   atomic.Int64
}

func (b *bench) waited() int64 {
	w := b.waitNs.Load()
	if since := b.waitSince.Load(); since > 0 {
		w += time.Now().UnixNano() - since
	}
	return w
}

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

// exit prints the whole run as one window, plus blocks per second.
func (b *bench) exit() {
	el := b.elapsed()
	var bps float64
	if el > 0 {
		bps = float64(b.blocks.Load()) / el.Seconds()
	}
	log.Printf("%s blk/s=%.1f", b.line("exit ", b.blocks.Load(), b.txs.Load(), b.gas.Load(), b.waited(), b.fullNs.Load(), el), bps)
}

func (b *bench) elapsed() time.Duration {
	if first := b.firstAt.Load(); first > 0 {
		return time.Since(time.Unix(0, first))
	}
	return 0
}

func (b *bench) line(tag string, blocks, txs, gas uint64, waitNs, fullNs int64, window time.Duration) string {
	elapsed := b.elapsed()
	var cum float64
	if elapsed > 0 {
		cum = float64(b.gas.Load()) / 1e6 / elapsed.Seconds()
	}
	return fmt.Sprintf("bench %st=%ds h=%d blk=%d tx=%d mgas/s=%.1f cum=%.1f wait=%.1fs full=%.1fs host_rss=%dMB vm_rss=%dMB",
		tag, int(elapsed.Seconds()), b.height.Load(), blocks, txs,
		float64(gas)/1e6/window.Seconds(), cum,
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
// place of a local P-chain (copied from cmd/epochdb-host). Every answer is
// cached; only used if the plugin asks.
type rpcValidatorState struct {
	sources  []string
	chainID  ids.ID
	subnetID ids.ID

	mu       sync.Mutex
	subnets  map[ids.ID]ids.ID
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

func (s *rpcValidatorState) try(ctx context.Context, fn func(context.Context, *platformvm.Client) error) error {
	var err error
	for _, uri := range s.sources {
		cctx, cancel := context.WithTimeout(ctx, 60*time.Second)
		err = fn(cctx, platformvm.NewClient(uri))
		cancel()
		if err == nil {
			return nil
		}
		log.Printf("epochdb-host-bench: validator state via %s: %v", uri, err)
	}
	return err
}

func (s *rpcValidatorState) GetMinimumHeight(ctx context.Context) (uint64, error) {
	return s.GetCurrentHeight(ctx)
}

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

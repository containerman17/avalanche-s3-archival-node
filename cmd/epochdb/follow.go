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
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/rlp"

	"github.com/containerman17/epochdb/exec"
	"github.com/containerman17/epochdb/fetch"
	"github.com/containerman17/epochdb/rpc"
	"github.com/containerman17/epochdb/state"
)

// serveMain is `epochdb serve`: ONE process that follows the chain, executes,
// and serves RPC continuously. No restarts, ever, and no static mode: following
// is what a node does (ruling 2026-07-29). The serve process is the sole
// writer and sole server of its data dir; side consumers use its RPC port.
//
// Who owns what:
//
//	fetch.Fetcher   consensus follower goroutine; sole writer of the staging
//	                store (arrival/index). Its in-RAM store IS the executor's
//	                block source, so an accepted container is executable the
//	                moment it is durable.
//	exec.Executor   the only holder of the Firewood handle; sole writer of the
//	                state layer (writelog/headers/logs/rcpt/code). Runs one
//	                goroutine, publishes its committed frontier after every
//	                block.
//	rpc.Server      N request goroutines. latest/pending state comes from the
//	                executor's frontier (no cook wait); historical heights use
//	                the descent; blocks/txs/receipts/logs come from the same
//	                live files the executor is appending to.
//	exec OnBlock    runs ON the executor goroutine: publishes the serving head
//	                and the block-hash entry for the block just committed, so
//	                eth_blockNumber and the frontier can never disagree by more
//	                than the microseconds between two stores.
//	cook loop       cook-index + cook-txindex + History.Refresh on a cadence,
//	                so the historical window chases the head. Async and
//	                fail-loud: a cook failure is logged, never stalls the chain.
//
// Seal (archival) and fold (pruning) deliberately stay OUT of this process;
// see the comment on cookLoop.
func serveMain(args []string) {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	dataDir := fs.String("data", "./data", "shared data directory")
	port := fs.Int("port", 9650, "HTTP listen port")
	network := fs.String("network", "fuji", "network: fuji|mainnet")
	vdrSources := fs.String("vdr-sources", "", "comma-separated platform RPC URIs for the cross-checked validator set; default: --node URI only, with a warning")
	nodeURI := fs.String("node", "", "bootstrap RPC node URI (default per --network)")
	stateCacheGiB := fs.Int("state-cache", 1, "executor Go-side read cache in GiB (0 disables)")
	cookEvery := fs.Duration("cook-every", time.Minute, "cadence for the in-process cook (index + txindex) that drags the historical window up to the head")
	perPeer := fs.Int("per-peer", 1, "max outstanding requests per archival peer")
	tipOverride := fs.String("tip-override", "", "run the in-process fetcher as a BACKFILL to this height instead of a consensus follower (fixed-corpus builds and integration runs: the executor still chases staging live)")
	walks := fs.Int("walks", 16, "concurrent backward walks (--tip-override)")
	pprofAddr := fs.String("pprof", "", "serve net/http/pprof on this address")
	fs.Parse(args)

	networkID, defNode, rpcURL := netParams(*network)
	if *nodeURI == "" {
		*nodeURI = defNode
	}
	if *pprofAddr != "" {
		go func() { log.Printf("epochdb: pprof: %v", http.ListenAndServe(*pprofAddr, nil)) }()
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	fatal := make(chan error, 4)

	// --- staging: the follower writes it, the executor reads it ---------------
	cfg := fetch.Config{DataDir: *dataDir, NodeURI: *nodeURI, PerPeer: *perPeer}
	if *vdrSources != "" {
		cfg.VdrSources = strings.Split(*vdrSources, ",")
	} else if *tipOverride == "" {
		log.Printf("epochdb: serve: no --vdr-sources, validator set cross-checked against the --node URI only")
	}
	fetcher, err := fetch.New(cfg)
	if err != nil {
		log.Fatalf("epochdb: serve: fetch: %v", err)
	}
	var blocks rpc.BlockSource = fetcher.Store()
	accepted := func() uint64 { n, _ := fetcher.Store().Head(); return n }
	if *tipOverride != "" {
		// Same process, same staging store, different source of blocks: a
		// bounded backfill instead of the consensus tip (fixed-corpus builds
		// and integration runs). Everything below is identical.
		anchors := resolveTipOverride(ctx, fetcher, rpcURL, *tipOverride, networkID)
		go func() { report(fatal, "backfill", fetcher.SyncTo(ctx, anchors, *walks)) }()
	} else {
		go func() { report(fatal, "follower", fetcher.Follow(ctx)) }()
	}

	// --- state layer: the executor owns the writer, the RPC shares it ---------
	store, err := state.Open(*dataDir)
	if err != nil {
		log.Fatalf("epochdb: open state layer: %v", err)
	}
	defer store.Close()

	g, err := exec.NetworkGenesis(networkID)
	if err != nil {
		log.Fatalf("epochdb: genesis: %v", err)
	}

	// onBlock is late-bound: the executor is built before the RPC server, and
	// exec.New's reconcile must not publish into a nil server.
	var onBlock atomic.Pointer[func(uint64, common.Hash)]
	e, err := exec.New(exec.Config{
		DataDir: *dataDir,
		Blocks:  blocks,
		Store:   store,
		OnBlock: func(n uint64, h common.Hash) {
			if f := onBlock.Load(); f != nil {
				(*f)(n, h)
			}
		},
		StateCacheBytes: uint64(*stateCacheGiB) << 30,
		// One Firewood proposal per block: the published frontier must be a
		// COMMITTED revision, and batching would leave latest reads up to
		// CommitEvery blocks stale. At chain pace the batching win is nil.
		CommitEvery: 1,
		NetworkID:   networkID,
	})
	if err != nil {
		log.Fatalf("epochdb: exec.New: %v", err)
	}
	defer e.Close()

	hist, err := state.OpenHistory(*dataDir, store, g.Alloc)
	if err != nil {
		log.Fatalf("epochdb: open history: %v", err)
	}
	defer hist.Close()

	// --- RPC -----------------------------------------------------------------
	srv := rpc.NewServer(hist, rpc.HistoryChainContext(hist), g.Config)
	srv.EnableLive(liveNode{Executor: e, accepted: accepted})

	txidx := &txIndexHolder{}
	txidx.reopen(*dataDir, hist.Epochs())
	srv.EnableTxAPIs(txidx, sealedBlocks{epochs: hist.Epochs(), blocks: blocks}, exec.ParseEthBlock)

	byHash, err := fetch.BlockHashes(*dataDir)
	if err != nil {
		log.Fatalf("epochdb: block hash index: %v", err)
	}
	idx := make(map[common.Hash]uint64, len(byHash))
	for h, n := range byHash {
		idx[common.Hash(h)] = n
	}
	srv.EnableBlockAPIs(idx)
	// Sealed heights whose raw sidecars seal deleted still need hashes: fill
	// those from the epoch-served headers, exactly like static serve.
	// Everything from the head up arrives through OnBlock.
	if uint64(len(idx)) < hist.Head()+1-hist.Floor() {
		for n := hist.Floor(); n <= hist.Head(); n++ {
			if raw, ok, err := hist.HeaderRLP(n); err == nil && ok {
				var h types.Header
				if rlp.DecodeBytes(raw, &h) == nil {
					srv.AddBlockHash(h.Hash(), n)
				}
			}
		}
	}
	log.Printf("epochdb: block hash index: %d blocks", srv.BlockHashCount())

	advance := func(n uint64, h common.Hash) {
		srv.AddBlockHash(h, n)
		hist.SetHead(n)
	}
	onBlock.Store(&advance)
	hist.SetHead(e.LiveHead())

	go func() { report(fatal, "rpc server", srv.ListenAndServe(fmt.Sprintf(":%d", *port))) }()
	go func() { report(fatal, "executor", e.Run(ctx)) }()
	go cookLoop(ctx, *dataDir, hist, txidx, *cookEvery)
	go statusLoop(ctx, e, hist, accepted)

	if floor := hist.Floor(); floor > 0 {
		log.Printf("epochdb: LIMITED HISTORY: floor=%d (base file), nothing below block %d is served", floor, floor)
	}
	log.Printf("epochdb: serve on :%d, executed=%d cooked=%d floor=%d chainId=%s (cook every %s)",
		*port, e.LiveHead(), hist.StateHead(), hist.Floor(), g.Config.ChainID, *cookEvery)

	select {
	case <-ctx.Done():
		log.Printf("epochdb: shutting down, flushing")
	case err := <-fatal:
		// Close the writers before dying so the restart is a clean resume
		// rather than a crash walk-back (the deferred Closes run on return).
		log.Printf("epochdb: FATAL: %v", err)
		fetcher.Close()
		e.Close()
		store.Close()
		os.Exit(1)
	}
	if err := fetcher.Close(); err != nil {
		log.Printf("epochdb: fetch close: %v", err)
	}
	log.Printf("epochdb: stopped at executed=%d", e.LiveHead())
}

// report routes a component goroutine's exit. Only a REAL error is fatal: a
// clean finish (the bounded backfill running out of spans, exec hitting a stop
// height) and a cancellation both leave the rest of the node serving. Wrapping
// a nil error here is what killed the first gate run: fmt.Errorf("...: %w",
// nil) is a non-nil error, so "backfill complete" read as "node dead".
func report(fatal chan<- error, what string, err error) {
	switch {
	case err == nil:
		log.Printf("epochdb: %s finished; the rest of the node keeps serving", what)
	case errors.Is(err, context.Canceled):
		log.Printf("epochdb: %s stopped: %v", what, err)
	default:
		fatal <- fmt.Errorf("%s: %w", what, err)
	}
}

// liveNode is the executor plus the follower's accepted height: the rpc.Live
// surface (SAE labels pending/latest/safe).
type liveNode struct {
	*exec.Executor
	accepted func() uint64
}

func (l liveNode) AcceptedHead() uint64 { return max(l.accepted(), l.Executor.LiveHead()) }

// cookLoop drags the historical read window up to the head: cook-index makes
// state at newly executed heights answerable by the descent, cook-txindex
// makes their txs findable by hash, and History.Refresh publishes both.
//
// Deliberately NOT in this loop: seal and fold. Both DELETE raw buckets, and
// this process is the live writer of exactly those files. seal drops a bucket
// only once every block in it is sealed (so it can only ever target buckets
// far below the writer's tip bucket), and fold's retirement guard additionally
// requires exechead >= B + BucketBlocks, so both are SAFE in principle beside
// this writer; what is NOT safe is the state.Open handle: our bucketLog holds
// open fds and an in-RAM index of files a sibling would unlink, and cook's
// tmp+rename races another cook. The documented shape stays: run seal or fold
// as the external sibling on its own cadence, one at a time. With EPOCH_TXS at
// 10M an epoch boundary is ~10 days away, so nothing is lost by leaving it out
// of the tip loop.
func cookLoop(ctx context.Context, dir string, hist *state.History, txidx *txIndexHolder, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		start := time.Now()
		if err := state.CookIndex(dir); err != nil {
			log.Printf("epochdb: COOK FAILED (historical window is frozen at %d, chain unaffected): %v", hist.StateHead(), err)
			continue
		}
		if err := state.CookTxIndex(dir); err != nil {
			log.Printf("epochdb: COOK-TXINDEX FAILED (tail tx lookups frozen, chain unaffected): %v", err)
		}
		if err := hist.Refresh(); err != nil {
			log.Printf("epochdb: HISTORY REFRESH FAILED (historical window is frozen at %d, chain unaffected): %v", hist.StateHead(), err)
			continue
		}
		txidx.reopen(dir, hist.Epochs())
		log.Printf("epochdb: cook: historical window now reaches %d (in %s)", hist.StateHead(), time.Since(start).Round(time.Millisecond))
	}
}

func statusLoop(ctx context.Context, e *exec.Executor, hist *state.History, accepted func() uint64) {
	t := time.NewTicker(10 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		acc, ex := accepted(), e.LiveHead()
		log.Printf("epochdb: serve: accepted=%d executed=%d served=%d cooked=%d settled=%d exec_lag=%d",
			acc, ex, hist.Head(), hist.StateHead(), e.SettledHeight(), int64(acc)-int64(ex))
	}
}

// txIndexHolder swaps the raw tx index under the RPC server as cook rebuilds
// it. The index is heap-only (no mmap, no fds), so a replaced one is just
// garbage.
type txIndexHolder struct {
	mu  sync.RWMutex
	cur state.CombinedTxIndex
}

func (t *txIndexHolder) Candidates(hash common.Hash) []uint64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.cur.Epochs == nil {
		return nil
	}
	return t.cur.Candidates(hash)
}

func (t *txIndexHolder) reopen(dir string, epochs *state.EpochSet) {
	raw, err := state.OpenTxIndex(dir)
	if err != nil {
		log.Printf("epochdb: raw tx index unavailable: %v", err)
		return
	}
	t.mu.Lock()
	t.cur = state.CombinedTxIndex{Raw: raw, Epochs: epochs}
	t.mu.Unlock()
}

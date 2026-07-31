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

	avaconstants "github.com/ava-labs/avalanchego/utils/constants"
	"github.com/ava-labs/libevm/common"

	"github.com/containerman17/epochdb/dist"
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
//	exec.Executor   the only holder of the Firewood handle, which it uses ONLY
//	                to verify roots; sole writer of the state layer
//	                (writelog/headers/logs/rcpt/code) and, through
//	                AppendWrites, of the tail overlay. Runs one goroutine,
//	                publishes its executed height after every committed block.
//	rpc.Server      N request goroutines. latest/pending state is the uncooked
//	                tail overlay over the descent (no cook wait, and no
//	                Firewood: it is verify-only and has no readers);
//	                historical heights use the descent alone;
//	                blocks/txs/receipts/logs come from the same live files the
//	                executor is appending to.
//	exec OnBlock    runs ON the executor goroutine: publishes the serving head
//	                and the block-hash entry for the block just committed, so
//	                eth_blockNumber and the frontier can never disagree by more
//	                than the microseconds between two stores.
//	cook loop       cook-index + cook-txindex + History.Refresh on a cadence,
//	                so the historical window chases the head. Async and
//	                fail-loud: a cook failure is logged, never stalls the chain.
//
// Seal deliberately stays OUT of this process; see the comment on cookLoop.
func serveMain(args []string) {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	dataDir := fs.String("data", "./data", "shared data directory")
	port := fs.Int("port", 9650, "HTTP listen port")
	network, resolveChain := chainFlags(fs)
	vdrSources := fs.String("vdr-sources", "", "comma-separated platform RPC URIs for the cross-checked validator set; default: --node URI only, with a warning")
	nodeURI := fs.String("node", "", "bootstrap RPC node URI (default per --network)")
	stateCacheGiB := fs.Int("state-cache", 1, "executor Go-side read cache in GiB (0 disables)")
	cookEvery := fs.Duration("cook-every", time.Minute, "cadence for the in-process cook (index + txindex) that drags the historical window up to the head")
	perPeer := fs.Int("per-peer", 1, "max outstanding requests per archival peer")
	tipOverride := fs.String("tip-override", "", "run the in-process fetcher as a BACKFILL to this height instead of a consensus follower (fixed-corpus builds and integration runs: the executor still chases staging live)")
	walks := fs.Int("walks", 16, "concurrent backward walks (--tip-override)")
	syncEvery := fs.Duration("sync-every", 5*time.Minute, "cadence for uploading spooled artifacts to the bucket and releasing the local copies (no-op without S3 credentials)")
	pprofAddr := fs.String("pprof", "", "serve net/http/pprof on this address")
	fs.Parse(args)

	// The descriptor comes FIRST: on `--chain` it, not `--network`, names the
	// network to dial, and it is what the fetcher, the executor and the genesis
	// all key on. An L1 on mainnet with the default `--network fuji` would
	// otherwise dial the wrong network's bootstrap node.
	c := resolveChain()
	if c.SubnetID != avaconstants.PrimaryNetworkID {
		*network = avaconstants.NetworkIDToNetworkName[c.NetworkID]
	}
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
	// Chain is what makes the follower track the L1's subnet and register the
	// subnet-evm extras (M5); nil-equivalent for the C-chain, whose descriptor
	// carries the primary subnet.
	cfg := fetch.Config{DataDir: *dataDir, NodeURI: *nodeURI, PerPeer: *perPeer, Chain: c}
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
		if c.SubnetID != avaconstants.PrimaryNetworkID {
			// resolveTipOverride's whole job is finding the pre-ProposerVM
			// ceiling, below which a container ID equals the eth block hash.
			// An L1 is ProposerVM-wrapped from block 1, so there is none.
			log.Fatalf("epochdb: serve: --tip-override is C-chain only (an L1 is ProposerVM-wrapped from block 1); serve an L1 by following its tip")
		}
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

	g, err := exec.ChainGenesis(c)
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
		// UNPINNED 2026-07-29: this was 1 only so the served frontier was a
		// committed Firewood revision. Nothing reads Firewood any more, so
		// batching costs nothing in freshness: head, block-hash index and the
		// tail overlay all advance together at the batch boundary, because
		// publishLive/OnBlock and the state-layer appends all happen in
		// flushBatch. 64 is invisible at tip pace (Run flushes the open batch
		// the moment staging runs dry, which at the tip is after every block,
		// so a following node still commits and root-verifies per block) and
		// is what makes catch-up fast. Ceiling is 1498 (exec.New's walk-back
		// guard); 64 keeps the crash walk-back at 8k blocks, well inside one
		// 100k raw bucket.
		CommitEvery: 64,
		Chain:       c,
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

	// --- the one read path: uncooked tail overlay, then the descent ----------
	// Cook once before serving so the descent reaches as high as it can, then
	// hand the residue (executed but not indexed, and therefore answerable
	// only from the raw writelog) to the overlay. Both must happen before the
	// executor and the RPC goroutines start.
	txidx := &txIndexHolder{}
	cookOnce(*dataDir, hist, txidx)
	tailBlocks, tailEntries, tailBytes, err := hist.EnableTail(e.LiveHead())
	if err != nil {
		log.Fatalf("epochdb: tail overlay: %v", err)
	}
	log.Printf("epochdb: tail overlay: cooked=%d executed=%d, backfilled %d uncooked blocks (%d entries, %.1fMB)",
		hist.StateHead(), e.LiveHead(), tailBlocks, tailEntries, float64(tailBytes)/1e6)

	// --- RPC -----------------------------------------------------------------
	srv := rpc.NewServer(hist, rpc.HistoryChainContext(hist), g.Config)
	srv.EnableLive(liveNode{Executor: e, accepted: accepted})

	txidx.reopen(*dataDir, hist.Epochs())
	srv.EnableTxAPIs(txidx, rpc.SealedBlocks{Epochs: hist.Epochs(), Blocks: blocks}, exec.ParseEthBlock)

	// NO floor-to-head block-hash map (deleted 2026-07-31, DESIGN.md ruling
	// 2): block hashes live in the same fp48 index as tx hashes, sealed and
	// raw alike, so eth_getBlockByHash resolves through WalkCandidates. Only
	// the blocks accepted since the last cook need in-process tracking, and
	// those arrive through OnBlock into a bounded ring.
	advance := func(n uint64, h common.Hash) {
		srv.AddBlockHash(h, n)
		hist.SetHead(n)
	}
	onBlock.Store(&advance)
	hist.SetHead(e.LiveHead())

	go func() { report(fatal, "rpc server", srv.ListenAndServe(fmt.Sprintf(":%d", *port))) }()
	go func() { report(fatal, "executor", e.Run(ctx)) }()
	go cookLoop(ctx, *dataDir, hist, txidx, *cookEvery)
	go syncLoop(ctx, store.Cas(), *syncEvery)
	go statusLoop(ctx, e, hist, accepted)

	log.Printf("epochdb: serve on :%d, executed=%d cooked=%d chainId=%s (cook every %s)",
		*port, e.LiveHead(), hist.StateHead(), g.Config.ChainID, *cookEvery)

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
// Deliberately NOT in this loop: seal. It DELETES raw buckets, and this
// process is the live writer of exactly those files. Seal drops a bucket only
// once every block in it is sealed (so it can only ever target buckets far
// below the writer's tip bucket) and is SAFE in principle beside this writer;
// what is NOT safe is the state.Open handle: our bucketLog holds open fds and
// an in-RAM index of files a sibling would unlink, and cook's tmp+rename races
// another cook. The documented shape stays: run seal as the external sibling
// on its own cadence. With EPOCH_TXS at 10M an epoch boundary is ~10 days
// away, so nothing is lost by leaving it out of the tip loop.
func cookLoop(ctx context.Context, dir string, hist *state.History, txidx *txIndexHolder, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		cookOnce(dir, hist, txidx)
	}
}

// syncLoop pushes whatever the producers left in the spool to the bucket and
// unlinks the local copy once the bucket confirms it. Fail-loud but never
// fatal: an upload that cannot happen leaves the artifact durable in the spool
// and the next tick retries. Without S3 credentials it never does anything.
func syncLoop(ctx context.Context, st *dist.Store, every time.Duration) {
	if !st.Remote() {
		return
	}
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		if err := st.Sync(); err != nil {
			log.Printf("epochdb: SYNC FAILED (artifacts stay in the spool, chain unaffected): %v", err)
		}
	}
}

// cookOnce is one pass of that loop, also run once at startup so the node
// serves with the smallest possible uncooked tail.
//
// The overlay prune comes LAST, strictly after Refresh: Refresh publishes the
// new cooked watermark only once the new sorted buckets are readable, so
// everything dropped here is already answerable by the descent, and a latest
// read that misses the overlay picks up its descent target afterwards. See
// state/tail.go prune for the full race argument.
func cookOnce(dir string, hist *state.History, txidx *txIndexHolder) {
	start := time.Now()
	if err := state.CookIndex(dir); err != nil {
		log.Printf("epochdb: COOK FAILED (historical window is frozen at %d, chain unaffected): %v", hist.StateHead(), err)
		return
	}
	if err := state.CookTxIndex(dir); err != nil {
		log.Printf("epochdb: COOK-TXINDEX FAILED (tail tx lookups frozen, chain unaffected): %v", err)
	}
	if err := hist.Refresh(); err != nil {
		log.Printf("epochdb: HISTORY REFRESH FAILED (historical window is frozen at %d, chain unaffected): %v", hist.StateHead(), err)
		return
	}
	txidx.reopen(dir, hist.Epochs())
	entries, size := hist.PruneTail()
	log.Printf("epochdb: cook: historical window now reaches %d (in %s); tail overlay %d entries %.1fMB",
		hist.StateHead(), time.Since(start).Round(time.Millisecond), entries, float64(size)/1e6)
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
		entries, size := hist.TailStats()
		log.Printf("epochdb: serve: accepted=%d executed=%d served=%d cooked=%d settled=%d exec_lag=%d tail=%d/%.1fMB",
			acc, ex, hist.Head(), hist.StateHead(), e.SettledHeight(), int64(acc)-int64(ex), entries, float64(size)/1e6)
	}
}

// txIndexHolder swaps the raw tx index under the RPC server as cook rebuilds
// it. The index is heap-only (no mmap, no fds), so a replaced one is just
// garbage.
type txIndexHolder struct {
	mu  sync.RWMutex
	cur state.CombinedTxIndex
}

func (t *txIndexHolder) WalkCandidates(hash common.Hash, fn func(blk uint64) (bool, error)) error {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.cur.Epochs == nil {
		return nil
	}
	return t.cur.WalkCandidates(hash, fn)
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

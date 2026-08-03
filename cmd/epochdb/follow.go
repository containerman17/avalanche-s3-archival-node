package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
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

	"github.com/containerman17/epochdb/chain"
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
//	                so the historical window chases the head, then seal: whole
//	                epochs are cut, published and their raw retired on the same
//	                tick. Async and fail-loud: a cook or seal failure is
//	                logged, never stalls the chain.
//
// Sealing is automatic and in-process (ruling 2026-08-01); see cookLoop for
// the ordering that makes deleting raw safe under a live reader.
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
	tipOverride := fs.String("tip-override", "", "run the in-process fetcher as a BACKFILL down from this CONTAINER ID instead of a consensus follower (cb58, or 0x-hex eth block hash for pre-ProposerVM blocks; fixed-corpus builds and integration runs: the executor still chases staging live)")
	walks := fs.Int("walks", 16, "concurrent backward walks (--tip-override)")
	syncEvery := fs.Duration("sync-every", 5*time.Minute, "cadence for uploading spooled artifacts to the bucket and releasing the local copies (no-op without S3 credentials)")
	pprofAddr := fs.String("pprof", "", "serve net/http/pprof on this address")
	fs.Parse(args)

	// The descriptor comes FIRST: on `--chain` it, not `--network`, names the
	// network to dial, and it is what the fetcher, the executor and the genesis
	// all key on. An L1 on mainnet with the default `--network fuji` would
	// otherwise dial the wrong network's bootstrap node.
	c := resolveChain(*dataDir)
	if c.SubnetID != avaconstants.PrimaryNetworkID {
		*network = avaconstants.NetworkIDToNetworkName[c.NetworkID]
	}
	_, defNode, _ := netParams(*network)
	if *nodeURI == "" {
		*nodeURI = defNode
	}
	if *pprofAddr != "" {
		go func() { log.Printf("epochdb: pprof: %v", http.ListenAndServe(*pprofAddr, nil)) }()
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	fatal := make(chan error, 4)

	cfg := nodeConfig{
		DataDir:       *dataDir,
		Chain:         c,
		NodeURI:       *nodeURI,
		StateCacheGiB: *stateCacheGiB,
		PerPeer:       *perPeer,
		CookEvery:     *cookEvery,
	}
	if *vdrSources != "" {
		cfg.VdrSources = strings.Split(*vdrSources, ",")
	} else if *tipOverride == "" {
		log.Printf("epochdb: serve: no --vdr-sources, validator set cross-checked against the --node URI only")
	}
	if *tipOverride != "" {
		// Same process, same staging store, different source of blocks: a
		// bounded backfill instead of the consensus tip (fixed-corpus builds
		// and integration runs). Everything else is identical.
		id, err := parseTipOverride(*tipOverride)
		if err != nil {
			log.Fatalf("epochdb: serve: --tip-override: %v", err)
		}
		cfg.Backfill = func(ctx context.Context, f *fetch.Fetcher) error {
			return f.SyncTo(ctx, resolveTipOverride(ctx, f, id), *walks)
		}
	}

	n, ln, err := serveOn(ctx, cfg, *port, func(what string, err error) { report(fatal, what, err) })
	if err != nil {
		log.Fatalf("epochdb: serve: %v", err)
	}

	go func() { report(fatal, "rpc server", http.Serve(ln, n.srv)) }()
	go syncLoop(ctx, n.store.Cas(), *syncEvery)

	log.Printf("epochdb: serve on :%d, executed=%d cooked=%d chainId=%s (cook every %s)",
		*port, n.e.LiveHead(), n.hist.StateHead(), n.chainID, *cookEvery)

	select {
	case <-ctx.Done():
		log.Printf("epochdb: shutting down, flushing")
	case err := <-fatal:
		// Close the writers before dying so the restart is a clean resume
		// rather than a crash walk-back. stop() cancels ctx first, because
		// closeAll waits for the executor goroutine to return before it
		// releases the executor's writers.
		log.Printf("epochdb: FATAL: %v", err)
		stop()
		n.closeAll()
		os.Exit(1)
	}
	n.closeAll()
	log.Printf("epochdb: stopped at executed=%d", n.e.LiveHead())
}

// serveOn binds the RPC port and ONLY THEN starts the node on it. The order is
// the whole point: startNode is an hour of work on a big corpus (joinChain's
// walk and, on an empty dir, its whole frontier build, then the exec open, the
// startup cook and the tail overlay), and while
// it ran nothing had touched the port, so a collision surfaced as FATAL 68
// minutes in (Fuji, 2026-08-01, twice in one night). A bad port now fails in
// milliseconds.
//
// Connections that arrive before the node is up wait in the kernel's accept
// backlog until http.Serve starts reading it: zero code, and unlike a 503
// nothing a client has to learn to retry.
func serveOn(ctx context.Context, cfg nodeConfig, port int, report func(string, error)) (*chainNode, net.Listener, error) {
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return nil, nil, err
	}
	n, err := startNode(ctx, cfg, report)
	if err != nil {
		ln.Close()
		return nil, nil, err
	}
	return n, ln, nil
}

// nodeConfig is everything one chain's node needs. `serve` fills one from its
// flags; `fleet` fills one per descriptor (fleet.go).
type nodeConfig struct {
	DataDir       string
	Chain         *chain.Chain
	NodeURI       string
	VdrSources    []string
	StateCacheGiB int
	PerPeer       int
	CookEvery     time.Duration
	// Label prefixes this node's log lines so a fleet's chains are
	// distinguishable. Empty keeps single-chain serve's log format verbatim.
	Label string
	// Backfill replaces the consensus follower with a bounded walk
	// (serve --tip-override). nil follows the live tip.
	Backfill func(context.Context, *fetch.Fetcher) error
}

func (c nodeConfig) logf(format string, args ...any) {
	if c.Label != "" {
		format = c.Label + ": " + format
	}
	log.Printf("epochdb: "+format, args...)
}

// chainNode is ONE chain's running stack: follower, executor, state layer,
// history and RPC handler, all on the caller's context. `serve` runs one;
// `fleet` runs N of them side by side in one process, which is the entire
// difference between the two commands.
type chainNode struct {
	cfg      nodeConfig
	fetcher  *fetch.Fetcher
	e        *exec.Executor
	store    *state.Store
	hist     *state.History
	srv      *rpc.Server
	txidx    *txIndexHolder
	accepted func() uint64
	chainID  fmt.Stringer // eth chainId, for the startup line
	stopOnce sync.Once
	// execDone closes when the executor goroutine has RETURNED. Closing the
	// executor while Run is still in a block would corrupt exactly the writers
	// the flush exists to protect, so stop waits on this. Cancel the node's
	// context first or the wait never ends.
	execDone chan struct{}
}

// startNode wires one chain's goroutines onto ctx and returns with it running.
// Every component goroutine's exit goes to report, which decides what a
// failure means: fatal for `serve` (one chain IS the process), per-chain in the
// fleet. Nothing here calls log.Fatal, because a fleet must survive one chain's
// bad data directory.
func startNode(ctx context.Context, cfg nodeConfig, report func(what string, err error)) (n *chainNode, err error) {
	// THE START SEQUENCE, before a single file of this dir is opened: resolve
	// the chain's `latest` pointer, refuse to start if it names history we
	// cannot assemble, and frontier-build if the dir has none of its own
	// (joinChain, RULING 2026-08-03). An empty data dir with credentials joins
	// the published chain here; there is no bootstrap step to remember.
	if err := joinChain(cfg, buildFrontier); err != nil {
		return nil, err
	}

	// Unwind whatever is already open if a later step fails: in the fleet this
	// runs while sibling chains are serving, so leaking fds is not an option.
	var closers []func()
	defer func() {
		if err == nil {
			return
		}
		for i := len(closers) - 1; i >= 0; i-- {
			closers[i]()
		}
	}()

	// --- staging: the follower writes it, the executor reads it ---------------
	// Chain is what makes the follower track the L1's subnet and register the
	// subnet-evm extras (M5); nil-equivalent for the C-chain, whose descriptor
	// carries the primary subnet.
	fetcher, err := fetch.New(fetch.Config{
		DataDir:    cfg.DataDir,
		NodeURI:    cfg.NodeURI,
		PerPeer:    cfg.PerPeer,
		Chain:      cfg.Chain,
		VdrSources: cfg.VdrSources,
	})
	if err != nil {
		return nil, fmt.Errorf("fetch: %w", err)
	}
	closers = append(closers, func() { fetcher.Close() })
	var blocks rpc.BlockSource = fetcher.Store()
	accepted := func() uint64 { h, _ := fetcher.Store().Head(); return h }

	// --- state layer: the executor owns the writer, the RPC shares it ---------
	store, err := state.Open(cfg.DataDir)
	if err != nil {
		return nil, fmt.Errorf("open state layer: %w", err)
	}
	closers = append(closers, func() { store.Close() })

	g, err := exec.ChainGenesis(cfg.Chain)
	if err != nil {
		return nil, fmt.Errorf("genesis: %w", err)
	}

	// onBlock is late-bound: the executor is built before the RPC server, and
	// exec.New's reconcile must not publish into a nil server.
	var onBlock atomic.Pointer[func(uint64, common.Hash)]
	// exec.New stamps the data dir with the VM kind and refuses a dir built by
	// the other one (state.Store.BindVMKind): that check is what keeps a
	// subnet-evm fleet from opening a coreth corpus.
	e, err := exec.New(exec.Config{
		DataDir: cfg.DataDir,
		Blocks:  blocks,
		Store:   store,
		OnBlock: func(h uint64, hash common.Hash) {
			if f := onBlock.Load(); f != nil {
				(*f)(h, hash)
			}
		},
		StateCacheBytes: uint64(cfg.StateCacheGiB) << 30,
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
		Chain:       cfg.Chain,
	})
	if err != nil {
		return nil, fmt.Errorf("exec.New: %w", err)
	}
	closers = append(closers, func() { e.Close() })

	hist, err := state.OpenHistory(cfg.DataDir, store, g.Alloc)
	if err != nil {
		return nil, fmt.Errorf("open history: %w", err)
	}
	closers = append(closers, func() { hist.Close() })

	// The sealed history IS history: the follower must not walk below it,
	// whatever the raw staging store still happens to hold. sealOnce raises
	// this again on every epoch cut, including the one the startup cook below
	// may do.
	//
	// Never above the exec head, though: an SAE bootstrap parks the frontier
	// at the settled height a header attests, up to a settlement lag BELOW the
	// sealed end (exec.BuildFrontier), and those few blocks have to come back
	// down the wire to be re-executed. A dir with no exec head at all keeps the
	// sealed floor: it has not been frontier-built and cannot serve anyway.
	floor := hist.Epochs().CoveredEnd()
	if head := e.LiveHead(); head > 0 && head < floor {
		floor = head
	}
	fetcher.SetFloor(floor)

	n = &chainNode{
		cfg: cfg, fetcher: fetcher, e: e, store: store, hist: hist,
		txidx: &txIndexHolder{}, accepted: accepted, chainID: g.Config.ChainID,
		execDone: make(chan struct{}),
	}

	// --- the one read path: uncooked tail overlay, then the descent ----------
	// Cook once before serving so the descent reaches as high as it can, then
	// hand the residue (executed but not indexed, and therefore answerable
	// only from the raw writelog) to the overlay. Both must happen before the
	// executor and the RPC goroutines start.
	n.cookOnce(ctx)
	tailBlocks, tailEntries, tailBytes, err := hist.EnableTail(e.LiveHead())
	if err != nil {
		return nil, fmt.Errorf("tail overlay: %w", err)
	}
	cfg.logf("tail overlay: cooked=%d executed=%d, backfilled %d uncooked blocks (%d entries, %.1fMB)",
		hist.StateHead(), e.LiveHead(), tailBlocks, tailEntries, float64(tailBytes)/1e6)

	// --- RPC -----------------------------------------------------------------
	n.srv = rpc.NewServer(hist, rpc.HistoryChainContext(hist), g.Config)
	n.srv.EnableLive(liveNode{Executor: e, accepted: accepted})

	n.txidx.reopen(cfg.DataDir, hist.Epochs())
	n.srv.EnableTxAPIs(n.txidx, rpc.SealedBlocks{Epochs: hist.Epochs(), Blocks: blocks}, exec.ParseEthBlock)

	// NO floor-to-head block-hash map (deleted 2026-07-31, DESIGN.md ruling
	// 2): block hashes live in the same fp48 index as tx hashes, sealed and
	// raw alike, so eth_getBlockByHash resolves through WalkCandidates. Only
	// the blocks accepted since the last cook need in-process tracking, and
	// those arrive through OnBlock into a bounded ring.
	advance := func(h uint64, hash common.Hash) {
		n.srv.AddBlockHash(hash, h)
		hist.SetHead(h)
	}
	onBlock.Store(&advance)
	hist.SetHead(e.LiveHead())

	if cfg.Backfill != nil {
		go func() { report("backfill", cfg.Backfill(ctx, fetcher)) }()
	} else {
		go func() { report("follower", fetcher.Follow(ctx)) }()
	}
	go func() {
		err := e.Run(ctx)
		close(n.execDone) // BEFORE report: report may flush, and flushing waits
		report("executor", err)
	}()
	go n.cookLoop(ctx)
	go n.statusLoop(ctx)
	return n, nil
}

// stop flushes and releases what a STOPPED chain must not leave dirty: the
// executor's open batch, the state watermark and the Firewood handle, all of
// which the executor alone touches. It deliberately does NOT close the state
// layer, the history or the staging store: the RPC goroutines read their
// mmaps, and a use-after-close there is a segfault, i.e. exactly the
// process-wide failure the per-chain boundary exists to prevent. A stopped
// chain keeps answering frozen data until the process restarts.
func (n *chainNode) stop() {
	n.stopOnce.Do(func() {
		<-n.execDone
		if err := n.e.Close(); err != nil {
			n.cfg.logf("executor close: %v", err)
		}
	})
}

// closeAll is the clean process-shutdown path: every writer flushed and
// released, executor first (it is the only writer of the state layer). Safe
// only once the RPC listener is done with this node.
func (n *chainNode) closeAll() {
	n.stop()
	if err := n.fetcher.Close(); err != nil {
		n.cfg.logf("fetch close: %v", err)
	}
	n.hist.Close()
	n.store.Close()
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
// SEAL RIDES THIS LOOP TOO (ruling 2026-08-01: sealing is automatic, there is
// no seal process and no cron). It supersedes "seal stays out of the process",
// whose objection was the DELETE of raw buckets this process reads: an
// external sibling unlinked files while our fds, mmaps and in-RAM bucket index
// still pointed at them, and there was no way to tell the reader first. In
// process there is, and the ordering IS the safety (state.History.SealTail),
// per epoch rather than per pass, so a backlog crunch frees disk as it goes:
//
//	seal    write ONE epoch, delete nothing. Both sources exist.
//	refresh publish it into the live EpochSet every reader already holds,
//	        so a sealed height now has a sealed answer.
//	delete  only then unlink the raw buckets it replaced, and drop our own
//	        handles on them so the space is actually returned. Then the next
//	        epoch, until the exec head has no whole epoch left or ctx is done.
//
// Nothing races a sibling because there is no sibling: one process owns the
// dir, and cook and seal are the same goroutine, so cook's tmp+rename cannot
// overlap a seal. Cost is gated by the exec head (SealTail opens nothing below
// the extrapolated boundary), so riding the cook cadence is free.
func (n *chainNode) cookLoop(ctx context.Context) {
	t := time.NewTicker(n.cfg.CookEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		n.cookOnce(ctx)
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
func (n *chainNode) cookOnce(ctx context.Context) {
	start := time.Now()
	dir, hist := n.cfg.DataDir, n.hist
	if err := state.CookIndex(dir); err != nil {
		n.cfg.logf("COOK FAILED (historical window is frozen at %d, chain unaffected): %v", hist.StateHead(), err)
		return
	}
	if err := state.CookTxIndex(dir); err != nil {
		n.cfg.logf("COOK-TXINDEX FAILED (tail tx lookups frozen, chain unaffected): %v", err)
	}
	if err := hist.Refresh(); err != nil {
		n.cfg.logf("HISTORY REFRESH FAILED (historical window is frozen at %d, chain unaffected): %v", hist.StateHead(), err)
		return
	}
	n.txidx.reopen(dir, hist.Epochs())
	entries, size := hist.PruneTail()
	n.cfg.logf("cook: historical window now reaches %d (in %s); tail overlay %d entries %.1fMB",
		hist.StateHead(), time.Since(start).Round(time.Millisecond), entries, float64(size)/1e6)
	n.sealOnce(ctx)
}

// sealOnce is the cook pass's seal step: cut whatever epochs the durable exec
// head has made whole, publish them, drop the raw they replace, ONE EPOCH AT A
// TIME. Same fail-loud-but-keep-serving contract as cook, and for the same
// reason: the chain does not stop because history could not be compacted. The
// sealed artifacts land in the same spool the syncLoop uploads from, so they
// reach the bucket on the next sync tick with no extra wiring.
//
// ctx is the node's: a shutdown mid-crunch stops the pass at the next epoch
// boundary instead of after all of them (Fuji, 2026-08-01: a SIGINT during a
// 14-epoch backlog left the node sealing for hours), and the next start resumes
// where it stopped.
func (n *chainNode) sealOnce(ctx context.Context) {
	start := time.Now()
	// Per epoch, right after state published it and deleted its raw: the
	// staging segments are the fetcher's, not the state layer's, so their
	// handles are released here (an unlinked arrival log this process still
	// holds open frees no disk), and the same call raises the follower's
	// backfill floor, so a walk started after this epoch never asks for what it
	// just deleted.
	epochs, sealedEnd, err := n.hist.SealTail(ctx, n.cfg.Chain.Root(), func(end uint64) {
		n.fetcher.SetFloor(end)
		if err := n.fetcher.Store().Retire(end); err != nil {
			n.cfg.logf("seal: retiring staging segments: %v", err)
		}
	})
	if err != nil {
		n.cfg.logf("SEAL FAILED (history stays raw, chain unaffected): %v", err)
		return
	}
	if epochs == 0 {
		return
	}
	n.cfg.logf("seal: %d epoch(s) cut, sealed through %d, raw retired (in %s)",
		epochs, sealedEnd, time.Since(start).Round(time.Millisecond))
}

func (n *chainNode) statusLoop(ctx context.Context) {
	t := time.NewTicker(10 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		n.cfg.logf("serve: %s", n.status())
	}
}

// status is the one-line health of this chain, shared by the log loop and the
// fleet's /status endpoint.
func (n *chainNode) status() string {
	acc, ex := n.accepted(), n.e.LiveHead()
	entries, size := n.hist.TailStats()
	return fmt.Sprintf("accepted=%d executed=%d served=%d cooked=%d settled=%d exec_lag=%d tail=%d/%.1fMB",
		acc, ex, n.hist.Head(), n.hist.StateHead(), n.e.SettledHeight(), int64(acc)-int64(ex), entries, float64(size)/1e6)
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

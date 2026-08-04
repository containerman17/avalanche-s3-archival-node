package main

import (
	"context"
	"encoding/json"
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

	"github.com/ava-labs/avalanchego/ids"
	avaconstants "github.com/ava-labs/avalanchego/utils/constants"
	"github.com/ava-labs/libevm/common"

	"github.com/containerman17/epochdb/chain"
	"github.com/containerman17/epochdb/dist"
	"github.com/containerman17/epochdb/exec"
	"github.com/containerman17/epochdb/fetch"
	"github.com/containerman17/epochdb/rpc"
	"github.com/containerman17/epochdb/state"
	"github.com/containerman17/epochdb/verify"
)

// serveMain is `epochdb serve`, THE ONLY OPERATOR COMMAND (RULING 2026-08-03),
// hosting exactly ONE CHAIN (RULING 2026-08-04). One process that follows the
// chain, executes, indexes, seals, publishes and serves RPC continuously. No
// restarts, ever, and no static mode: following is what a node does (ruling
// 2026-07-29). The serve process is the sole writer and sole server of its data
// dir; side consumers use its RPC port.
//
// SEVERAL CHAINS ON A BOX ARE SEVERAL PROCESSES, one container each, sharing
// EPOCHDB_CACHE_DIR. Nothing here coordinates them: the windowed chunk cache is
// cross-process by construction, so the fleet supervisor that used to be the
// coordination layer is gone, and per-chain failure isolation is the OS's job.
// A chain that cannot start exits nonzero and the orchestrator restarts it.
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
	dataDir := fs.String("data", "./data", "data directory; ONE chain owns it, and this process serves that one chain")
	port := fs.Int("port", 9650, "HTTP listen port; the chain answers at / and at /ext/bc/<blockchainID>/rpc, and /status reports it")
	network := fs.String("network", "fuji", "network: fuji|mainnet (the network --chain lives on)")
	chainSpec := fs.String("chain", "C", "chain: C for --network's primary C-chain, or an L1's blockchainID")
	doVerify := fs.Bool("verify", false, "before serving, re-verify every SEALED epoch with the full no-execution engine (diff-applied state roots, txRoot, receiptsRoot reconstructed from the stored logs, header chain). Pulls every byte of the corpus; a failure means the process does not start")
	vdrSources := fs.String("vdr-sources", "", "comma-separated platform RPC URIs for the cross-checked validator set; default: --node URI only, with a warning")
	nodeURI := fs.String("node", "", "bootstrap RPC node URI (default: the public endpoint for the chain's networkID)")
	stateCacheGiB := fs.Int("state-cache", 1, "executor Go-side read cache in GiB (0 disables)")
	cookEvery := fs.Duration("cook-every", time.Minute, "cadence for the in-process cook (index + txindex) that drags the historical window up to the head, and for the seal that rides it")
	perPeer := fs.Int("per-peer", 1, "max outstanding requests per archival peer")
	tipOverride := fs.String("tip-override", "", "run the in-process fetcher as a BACKFILL down from this CONTAINER ID instead of a consensus follower (cb58, or 0x-hex eth block hash for pre-ProposerVM blocks)")
	walks := fs.Int("walks", 16, "concurrent backward walks (--tip-override)")
	syncEvery := fs.Duration("sync-every", 5*time.Minute, "cadence for uploading spooled artifacts to the bucket and releasing the local copies (no-op without S3 credentials)")
	pprofAddr := fs.String("pprof", "", "serve net/http/pprof on this address")
	fs.Parse(args)

	// Bad flag values die before anything binds or dials.
	var tip ids.ID
	if *tipOverride != "" {
		var err error
		if tip, err = parseTipOverride(*tipOverride); err != nil {
			log.Fatalf("epochdb: serve: --tip-override: %v", err)
		}
	}
	if *pprofAddr != "" {
		go func() { log.Printf("epochdb: pprof: %v", http.ListenAndServe(*pprofAddr, nil)) }()
	}
	if *vdrSources == "" && *tipOverride == "" {
		log.Printf("epochdb: serve: no --vdr-sources, validator set cross-checked against the --node URI only")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// THE FAILURE MODEL, whole (RULING 2026-08-04): a component that dies takes
	// the process with it, nonzero, after flushing. There is nothing left to
	// serve and nothing to keep alive for a sibling, so restart-on-exit is the
	// isolation, and that is the orchestrator's job.
	dead := make(chan error, 1)
	report := func(what string, err error) {
		switch {
		case err == nil:
			log.Printf("epochdb: serve: %s finished; the chain keeps serving", what)
		case errors.Is(err, context.Canceled):
			log.Printf("epochdb: serve: %s stopped: %v", what, err)
		default:
			select {
			case dead <- fmt.Errorf("%s: %w", what, err):
			default:
			}
		}
	}

	// /status answers from the bind, i.e. through the hour of startup work, so
	// the handler reads the node through a pointer that is nil until it is up.
	var node atomic.Pointer[chainNode]
	mux := http.NewServeMux()
	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(statusOf(*chainSpec, node.Load()))
	})
	srv := &http.Server{Handler: mux}
	exit := 0

	err := serveOn(*port, func(ln net.Listener) {
		go func() {
			if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Printf("epochdb: serve: FATAL rpc listener: %v", err)
				stop()
			}
		}()

		rctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		c, err := chain.Resolve(rctx, *chainSpec, execNetID(*network), *dataDir)
		cancel()
		if err != nil {
			log.Fatalf("epochdb: serve: --chain %s: %v", *chainSpec, err)
		}
		id := c.BlockchainID.String()

		cfg := nodeConfig{
			DataDir:       *dataDir,
			Chain:         c,
			NodeURI:       *nodeURI,
			StateCacheGiB: *stateCacheGiB,
			PerPeer:       *perPeer,
			CookEvery:     *cookEvery,
			Verify:        *doVerify,
		}
		// The descriptor, not --network, names the network to dial: it is what
		// the dir was built with, and an L1 on mainnet under the default
		// `--network fuji` would otherwise dial the wrong network's bootstrap
		// node.
		if cfg.NodeURI == "" {
			_, cfg.NodeURI, _ = netParams(avaconstants.NetworkIDToNetworkName[c.NetworkID])
		}
		if *vdrSources != "" {
			cfg.VdrSources = strings.Split(*vdrSources, ",")
		}
		if *tipOverride != "" {
			// Same process, same staging store, different source of blocks: a
			// bounded backfill instead of the consensus tip (fixed-corpus builds
			// and integration runs). Everything else is identical.
			log.Printf("epochdb: serve: %s backfills to container %s instead of following", id, tip)
			cfg.Backfill = func(ctx context.Context, fe *fetch.Fetcher) error {
				return fe.SyncTo(ctx, resolveTipOverride(ctx, fe, tip), *walks)
			}
		}
		if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
			log.Fatalf("epochdb: serve: %v", err)
		}
		n, err := startNode(ctx, cfg, report)
		if err != nil {
			log.Fatalf("epochdb: serve: %s did not start: %v", id, err)
		}
		node.Store(n)
		// / is what every existing script, bench and docker run points at;
		// /ext/bc/<id>/rpc is avalanchego's own routing, so wallets,
		// subnets.avax.network tooling and the ab-benches point at this chain
		// unchanged. /ws is the same handler: rpc.Server upgrades websockets
		// itself.
		mux.Handle("/ext/bc/"+id+"/rpc", n.srv)
		mux.Handle("/ext/bc/"+id+"/ws", n.srv)
		mux.Handle("/", n.srv)
		log.Printf("epochdb: serve: %s on :%d (also /ext/bc/%s/rpc), executed=%d cooked=%d chainId=%s (cook every %s)",
			id, *port, id, n.e.LiveHead(), n.hist.StateHead(), n.chainID, *cookEvery)
		go syncLoop(ctx, n.store.Cas(), *syncEvery)

		select {
		case <-ctx.Done():
			log.Printf("epochdb: serve: shutting down, flushing")
		case err := <-dead:
			// The writers close before we die, so the restart is a clean resume
			// rather than a crash walk-back. stop() cancels ctx first, because
			// closeAll waits for the executor goroutine to return before it
			// releases the executor's writers.
			log.Printf("epochdb: serve: FATAL: %v", err)
			stop()
			exit = 1
		}
		// Stop serving BEFORE closing the read side: closeAll unmaps files the
		// RPC goroutines read.
		sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		srv.Shutdown(sctx)
		cancel()
		n.closeAll()
	})
	if err != nil {
		log.Fatalf("epochdb: serve: %v", err)
	}
	if exit != 0 {
		os.Exit(exit)
	}
}

// serveStatus is /status: ONE chain, because a process is one chain (RULING
// 2026-08-04). An aggregate over the chains of a box is an orchestrator's
// concern, not ours: it polls one of these per container.
//
// The cache block is the chunk tier's own account of itself. CacheHorizon is
// the age of the window its eviction worker last deleted from, i.e. how far
// back this node's disk actually reaches before a read costs a GET, and it
// counts what SIBLING PROCESSES evicted too, since the cache is shared. Empty
// until something has been evicted, which is the honest answer for a node whose
// cache has never filled.
//
// TWO MODES, TWO FIELDS, because they are two different numbers and one field
// meaning either would be a lie in one of them: `accepted` is the follower's
// accepted head and exists only when following; `target`/`stored` are the
// --tip-override ceiling and the staged container count, and exist only when
// backfilling. A follow-mode answer is byte-identical to what it always was.
type serveStatus struct {
	Chain        string `json:"chain"`
	Serving      bool   `json:"serving"`
	Accepted     uint64 `json:"accepted"`
	Target       uint64 `json:"target,omitempty"`
	Stored       uint64 `json:"stored,omitempty"`
	Executed     uint64 `json:"executed"`
	Cooked       uint64 `json:"cooked"`
	CacheHorizon string `json:"cacheHorizon,omitempty"`
	CacheEvicted uint64 `json:"cacheEvictions,omitempty"`
	CacheRefused uint64 `json:"cacheRefusals,omitempty"`
	CacheFree    int64  `json:"cacheFreeBytes,omitempty"`
	CacheMinFree int64  `json:"cacheMinFreeBytes,omitempty"`
}

// statusOf builds that answer. n is nil between the bind and the moment the
// chain is up, which is minutes to hours on a cold dir: serving=false and the
// spec the operator asked for is the whole honest answer there.
func statusOf(spec string, n *chainNode) serveStatus {
	if n == nil {
		return serveStatus{Chain: spec}
	}
	s := n.snapshot().serveStatus(n.cfg.Chain.BlockchainID.String())
	if cs, ok := n.store.Cas().CacheStats(); ok {
		if cs.VictimAge > 0 {
			s.CacheHorizon = cs.VictimAge.String()
		}
		s.CacheEvicted, s.CacheRefused = cs.Evictions, cs.Refusals
		s.CacheFree, s.CacheMinFree = cs.FreeBytes, cs.MinFree
	}
	return s
}

// serveOn binds the RPC port and ONLY THEN does the startup work on it. The
// order is the whole point: starting a chain is an hour of work on a big corpus
// (joinChain's walk and, on an empty dir, its whole frontier build, then the
// exec open, the startup cook and the tail overlay), and while it ran nothing
// had touched the port, so a collision surfaced as FATAL 68 minutes in (Fuji,
// 2026-08-01, twice in one night). A bad port now fails in milliseconds.
//
// Connections that arrive before a chain is up wait in the kernel's accept
// backlog until its handler registers: zero code, and unlike a 503 nothing a
// client has to learn to retry. /status answers from the moment of the bind, so
// an operator can watch the chains come up instead of guessing.
func serveOn(port int, run func(net.Listener)) error {
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return err
	}
	defer ln.Close()
	run(ln)
	return nil
}

// verifySealed re-verifies what has been SEALED (and therefore published): the
// full no-execution engine over this chain's epoch set, `epochdb dev verify` and
// its log half in one, subnet-evm included because the verifier takes its VM
// kind off the descriptor.
//
// IT DELIBERATELY DOES NOT VERIFY THE LOCAL RAW TAIL [RULING 2026-08-03]. The
// honest way to verify raw is to re-download and re-execute it, which is what a
// from-scratch start already does, and the tail is at most one epoch (8M txs).
// So from-scratch reprocessing IS the local-raw verification story, and the
// sealed epochs, the artifacts other people consume, are the thing that needs a
// dedicated verifier.
//
// It runs at startup, after joinChain has walked the epoch chain (the local
// index is what names the epochs) and before the chain serves anything. A
// failure is an error out of startNode, i.e. THAT chain refuses to start.
func verifySealed(cfg nodeConfig) error {
	st, err := dist.Open(cfg.DataDir)
	if err != nil {
		return err
	}
	defer st.Close()
	tmp, err := os.MkdirTemp(cfg.DataDir, ".verify-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmp)
	logf("verify: re-verifying the sealed epochs before serving; this reads the whole corpus")
	blocks, wall, err := verify.VerifySet(st, tmp, cfg.Chain, 0)
	if err != nil {
		return fmt.Errorf("FAIL after %d blocks in %s: %w", blocks, wall.Round(time.Second), err)
	}
	logf("verify: PASS, %d sealed blocks in %s", blocks, wall.Round(time.Second))
	return nil
}

// nodeConfig is everything the node needs. `serve` fills exactly one.
type nodeConfig struct {
	DataDir       string
	Chain         *chain.Chain
	NodeURI       string
	VdrSources    []string
	StateCacheGiB int
	PerPeer       int
	CookEvery     time.Duration
	// Backfill replaces the consensus follower with a bounded walk
	// (serve --tip-override). nil follows the live tip.
	Backfill func(context.Context, *fetch.Fetcher) error
	// Verify re-verifies the SEALED epochs before the node serves anything
	// (serve --verify). A failure means the process does not start.
	Verify bool
}

// logf is every node log line. One chain per process means the line needs no
// chain label: the container is the label.
func logf(format string, args ...any) { log.Printf("epochdb: "+format, args...) }

// chainNode is the chain's running stack: follower, executor, state layer,
// history and RPC handler, all on the caller's context. `serve` runs exactly
// one.
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
	// execDone closes when the executor goroutine has RETURNED. Closing the
	// executor while Run is still in a block would corrupt exactly the writers
	// the flush exists to protect, so closeAll waits on this. Cancel the node's
	// context first or the wait never ends.
	execDone chan struct{}
}

// startNode wires one chain's goroutines onto ctx and returns with it running.
// Every component goroutine's exit goes to report, which takes the process down
// nonzero on a real failure. Nothing here calls log.Fatal: a start failure is an
// error the caller reports and exits on, so the same code is usable from a test
// and the flush on the way out is never skipped.
func startNode(ctx context.Context, cfg nodeConfig, report func(what string, err error)) (n *chainNode, err error) {
	// THE START SEQUENCE, before a single file of this dir is opened: resolve
	// the chain's `latest` pointer, refuse to start if it names history we
	// cannot assemble, and frontier-build if the dir has none of its own
	// (joinChain, RULING 2026-08-03). An empty data dir with credentials joins
	// the published chain here; there is no bootstrap step to remember.
	if err := joinChain(cfg, buildFrontier); err != nil {
		return nil, err
	}
	// AFTER the join, because the join is what makes the epoch set nameable
	// (and, on a cold dir, present at all), and before anything of this dir is
	// opened for serving.
	if cfg.Verify {
		if err := verifySealed(cfg); err != nil {
			return nil, fmt.Errorf("verify: %w", err)
		}
	}

	// Unwind whatever is already open if a later step fails, so a start that
	// gives up releases the Firewood handle and the mmaps it took.
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
	// The FOLLOWER's accepted head, and only the follower's: the staging store
	// keeps every index sidecar the dir ever got, so under --tip-override this
	// is a leftover height and not this run's ceiling. Nothing reads it in that
	// mode (see nodeStatus and liveNode).
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
	// subnet-evm node from opening a coreth corpus.
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

	hist, err := state.OpenHistory(cfg.DataDir, store, g.TrieAlloc)
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
	logf("tail overlay: cooked=%d executed=%d, backfilled %d uncooked blocks (%d entries, %.1fMB)",
		hist.StateHead(), e.LiveHead(), tailBlocks, tailEntries, float64(tailBytes)/1e6)

	// --- RPC -----------------------------------------------------------------
	n.srv = rpc.NewServer(hist, rpc.HistoryChainContext(hist), g.Config)
	live := liveNode{live: e.LiveHead, settled: e.SettledHeight}
	if cfg.Backfill != nil {
		live.target = fetcher.SyncTarget
	} else {
		live.accepted = accepted
	}
	n.srv.EnableLive(live)

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

// closeAll is the clean process-shutdown path: every writer flushed and
// released, executor first (it is the only writer of the state layer, and the
// holder of the open batch, the state watermark and the Firewood handle).
//
// CANCEL THE NODE'S CONTEXT FIRST or the wait on execDone never ends: closing
// the executor while Run is still inside a block would corrupt exactly the
// writers the flush exists to protect. Safe only once the RPC listener is done
// with this node, because it unmaps files those goroutines read.
func (n *chainNode) closeAll() {
	<-n.execDone
	if err := n.e.Close(); err != nil {
		logf("executor close: %v", err)
	}
	if err := n.fetcher.Close(); err != nil {
		logf("fetch close: %v", err)
	}
	n.hist.Close()
	n.store.Close()
}

// liveNode is the rpc.Live surface (SAE labels pending/latest/safe) plus the
// height eth_syncing advertises.
//
// TWO QUESTIONS, TWO VALUES (2026-08-05), because making one number answer both
// is what put a leftover height in front of clients. AcceptedHead bounds what a
// read may NAME (`pending`, the block-number ceiling), so it is only ever a
// height whose container this node holds; SyncTarget is only the goal, and may
// sit millions of blocks above anything answerable. Following they coincide, so
// both funcs below are the follower's accepted head. Under --tip-override there
// is NO follower: the staging store's head is whatever an earlier run left in
// the dir, so nothing above the executed head may be named, while the goal is
// the walk's ceiling.
type liveNode struct {
	live     func() uint64 // executed head
	settled  func() uint64
	accepted func() uint64 // the follower's accepted head; nil when backfilling
	target   func() uint64 // the backfill ceiling; nil when following
}

func (l liveNode) LiveHead() uint64      { return l.live() }
func (l liveNode) SettledHeight() uint64 { return l.settled() }

func (l liveNode) AcceptedHead() uint64 {
	if l.accepted == nil {
		return l.live()
	}
	return max(l.accepted(), l.live())
}

func (l liveNode) SyncTarget() uint64 {
	if l.target == nil {
		return l.AcceptedHead()
	}
	return l.target()
}

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
		logf("COOK FAILED (historical window is frozen at %d, chain unaffected): %v", hist.StateHead(), err)
		return
	}
	if err := state.CookTxIndex(dir); err != nil {
		logf("COOK-TXINDEX FAILED (tail tx lookups frozen, chain unaffected): %v", err)
	}
	if err := hist.Refresh(); err != nil {
		logf("HISTORY REFRESH FAILED (historical window is frozen at %d, chain unaffected): %v", hist.StateHead(), err)
		return
	}
	n.txidx.reopen(dir, hist.Epochs())
	entries, size := hist.PruneTail()
	logf("cook: historical window now reaches %d (in %s); tail overlay %d entries %.1fMB",
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
			logf("seal: retiring staging segments: %v", err)
		}
	})
	if err != nil {
		logf("SEAL FAILED (history stays raw, chain unaffected): %v", err)
		return
	}
	if epochs == 0 {
		return
	}
	logf("seal: %d epoch(s) cut, sealed through %d, raw retired (in %s)",
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
		logf("serve: %s", n.snapshot().status())
	}
}

// nodeStatus is the chain's numbers, read once and rendered two ways: the log
// line (status) and /status (serveStatus).
//
// head IS THE FIX OF 2026-08-05: it is the height execution is measured
// against, and where it comes from depends on the mode. Following, it is the
// follower's accepted head. Backfilling (--tip-override) there IS no follower,
// and the staging store's head is the follower's number: a dir that once
// followed still holds index sidecars far above a later override's ceiling, so
// a 10,129,485-block mainnet backfill reported accepted=66854601 and an
// exec_lag of 59.3M against a height nothing in the run would ever reach. A
// backfill measures against the ceiling its walk was given.
type nodeStatus struct {
	backfill bool
	head     uint64 // accepted head (follow) or override ceiling (backfill)
	stored   uint64 // containers in staging; backfill progress toward head
	executed uint64
	served   uint64
	cooked   uint64
	settled  uint64
	entries  int
	bytes    uint64
}

func (n *chainNode) snapshot() nodeStatus {
	entries, size := n.hist.TailStats()
	s := nodeStatus{
		backfill: n.cfg.Backfill != nil,
		executed: n.e.LiveHead(),
		served:   n.hist.Head(),
		cooked:   n.hist.StateHead(),
		settled:  n.e.SettledHeight(),
		entries:  entries,
		bytes:    size,
	}
	if s.backfill {
		// Count, not a contiguous-run scan: the run above the exec head is
		// millions of heights wide during a stage-1 walk and probing it every
		// tick would hold the store's lock against the fetcher.
		s.head, s.stored = n.fetcher.SyncTarget(), n.fetcher.Store().Count()
	} else {
		s.head = n.accepted()
	}
	return s
}

// status is the one-line health of this chain, for the log loop. /status
// answers with the structured twin (statusOf).
func (s nodeStatus) status() string {
	lead, lag := fmt.Sprintf("accepted=%d", s.head), fmt.Sprintf("%d", int64(s.head)-int64(s.executed))
	if s.backfill {
		lead = fmt.Sprintf("target=%d stored=%d", s.head, s.stored)
		if s.head == 0 { // the walk has not resolved its anchors yet
			lead, lag = fmt.Sprintf("target=? stored=%d", s.stored), "?"
		}
	}
	return fmt.Sprintf("%s executed=%d served=%d cooked=%d settled=%d exec_lag=%s tail=%d/%.1fMB",
		lead, s.executed, s.served, s.cooked, s.settled, lag, s.entries, float64(s.bytes)/1e6)
}

func (s nodeStatus) serveStatus(chain string) serveStatus {
	out := serveStatus{Chain: chain, Serving: true, Executed: s.executed, Cooked: s.cooked}
	if s.backfill {
		out.Target, out.Stored = s.head, s.stored
	} else {
		out.Accepted = s.head
	}
	return out
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

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
	"sync/atomic"
	"syscall"
	"time"

	"github.com/ava-labs/avalanchego/ids"
	avaconstants "github.com/ava-labs/avalanchego/utils/constants"

	"github.com/containerman17/epochdb"
	"github.com/containerman17/epochdb/chain"
	"github.com/containerman17/epochdb/dist"
	"github.com/containerman17/epochdb/grpcapi"
	"github.com/containerman17/epochdb/plainhttp"
)

// serveMain is `epochdb serve`, THE ONLY OPERATOR COMMAND (RULING 2026-08-03),
// hosting exactly ONE CHAIN (RULING 2026-08-04). One process that follows the
// chain, executes, indexes, flushes, publishes and answers queries
// continuously. No restarts, ever, and no static mode: following is what a node
// does (ruling 2026-07-29). The serve process is the sole writer and sole
// server of its data dir; side consumers use its ports.
//
// IT IS A THIN MAIN OVER THE LIBRARY: everything below the flags is
// epochdb.Open, which is the entry point a Go consumer links directly (DESIGN
// "Entry points and adapters"). One lifecycle, so an in-process consumer and
// this command cannot drift apart.
//
// SEVERAL CHAINS ON A BOX ARE SEVERAL PROCESSES, one container each, sharing
// EPOCHDB_CACHE_DIR. Nothing here coordinates them: the windowed chunk cache is
// cross-process by construction, so the fleet supervisor that used to be the
// coordination layer is gone, and per-chain failure isolation is the OS's job.
// A chain that cannot start exits nonzero and the orchestrator restarts it.
//
// Who owns what:
//
//	fetch.Fetcher   the ascending fetch plus the consensus follower. Its
//	                bounded RAM queue IS the executor's block source, and the
//	                only place an unexecuted container lives: nothing
//	                pre-execution touches disk.
//	exec.Executor   the only holder of the Firewood handle, which it uses ONLY
//	                to verify roots; sole writer of the state layer and of the
//	                unflushed window. Runs one goroutine, publishes its executed
//	                height after every committed block.
//	rpc.Server      THE CORE QUERY LAYER, N request goroutines. JSON-RPC,
//	                gRPC and plain HTTP are three PEER adapters over it, none
//	                stacked on another.
func serveMain(args []string) {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	dataDir := fs.String("data", "./data", "data directory; ONE chain owns it, and this process serves that one chain")
	port := fs.Int("port", 9650, "HTTP listen port: JSON-RPC at / and /ext/bc/<blockchainID>/rpc, the plain HTTP adapter at /v0/<method>, /status")
	grpcPort := fs.Int("grpc-port", 9660, "gRPC listen port, THE PRIMARY REMOTE API (0 disables)")
	network := fs.String("network", "fuji", "network: fuji|mainnet (the network --chain lives on)")
	chainSpec := fs.String("chain", "C", "chain: C for --network's primary C-chain, or an L1's blockchainID")
	vdrSources := fs.String("vdr-sources", "", "comma-separated platform RPC URIs for the cross-checked validator set; default: the --node URIs, with a warning when that is one host")
	nodeURI := fs.String("node", "", "comma-separated bootstrap RPC node URIs, tried in turn (default: the public endpoint for the chain's networkID)")
	stateCacheGiB := fs.Int("state-cache", 1, "executor Go-side read cache in GiB (0 disables)")
	perPeer := fs.Int("per-peer", 1, "max outstanding requests per archival peer")
	tipOverride := fs.String("tip-override", "", "run the in-process fetcher as a BACKFILL down from this CONTAINER ID instead of a consensus follower (cb58, or 0x-hex eth block hash for pre-ProposerVM blocks)")
	syncEvery := fs.Duration("sync-every", 5*time.Minute, "cadence for uploading spooled artifacts to the bucket and releasing the local copies (no-op without S3 credentials)")
	pprofAddr := fs.String("pprof", "", "serve net/http/pprof on this address")
	fs.Parse(args)

	// Before the port, before the dial, before a byte of this dir is opened:
	// this process is now the dir's one writer, or it does not run.
	release, lockErr := lockDataDir(*dataDir)
	if lockErr != nil {
		log.Fatalf("epochdb: serve: %v", lockErr)
	}
	defer release()
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
	if *vdrSources == "" && *tipOverride == "" && len(dist.Sources(*nodeURI)) < 2 {
		log.Printf("epochdb: serve: no --vdr-sources and one --node URI, so the validator set is cross-checked against nothing. " +
			"Several endpoints are also the failover the bootstrap RPC uses when one of them rate-limits this box.")
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
	var node atomic.Pointer[epochdb.Node]
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

		// Minutes, not 30 seconds: a first resolution asks a public P-chain
		// endpoint for the CreateChainTx, and that endpoint rate-limits a box
		// running dozens of chains, so the call is retried over every source
		// (dist.Try) instead of killing the process. A resolved dir answers
		// from chain.json and never gets here.
		rctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
		c, err := chain.Resolve(rctx, *chainSpec, execNetID(*network), *dataDir, dist.Sources(*nodeURI)...)
		cancel()
		if err != nil {
			log.Fatalf("epochdb: serve: --chain %s: %v", *chainSpec, err)
		}
		id := c.BlockchainID.String()

		cfg := epochdb.Config{
			DataDir:       *dataDir,
			Chain:         c,
			NodeURI:       *nodeURI,
			StateCacheGiB: *stateCacheGiB,
			PerPeer:       *perPeer,
			OnExit:        report,
		}
		// The descriptor, not --network, names the network to dial: it is what
		// the dir was built with, and an L1 on mainnet under the default
		// `--network fuji` would otherwise dial the wrong network's bootstrap
		// node.
		if cfg.NodeURI == "" {
			_, cfg.NodeURI, _ = netParams(avaconstants.NetworkIDToNetworkName[c.NetworkID])
		}
		if *vdrSources != "" {
			cfg.VdrSources = dist.Sources(*vdrSources)
		}
		if *tipOverride != "" {
			// Same process, same forward fetch, a fixed ceiling instead of the
			// live tip (fixed-corpus builds and integration runs). Everything
			// else is identical.
			log.Printf("epochdb: serve: %s fetches forward to container %s and stops there instead of following", id, tip)
			cfg.TipOverride = tip
		}
		n, err := epochdb.Open(ctx, cfg)
		if err != nil {
			log.Fatalf("epochdb: serve: %s did not start: %v", id, err)
		}
		node.Store(n)
		// / is what every existing script, bench and docker run points at;
		// /ext/bc/<id>/rpc is avalanchego's own routing, so wallets,
		// subnets.avax.network tooling and the ab-benches point at this chain
		// unchanged. /ws is the same handler: rpc.Server upgrades websockets
		// itself. /v0/ is the plain HTTP adapter, a PEER of JSON-RPC over the
		// same core layer and never a proxy to it.
		mux.Handle("/ext/bc/"+id+"/rpc", n.Core())
		mux.Handle("/ext/bc/"+id+"/ws", n.Core())
		mux.Handle("/v0/", http.StripPrefix("/v0", plainhttp.Handler(n)))
		mux.Handle("/", n.Core())
		head, _ := n.Head()
		log.Printf("epochdb: serve: %s on :%d (also /ext/bc/%s/rpc, plain HTTP /v0/), executed=%d runs=%d chainId=%s",
			id, *port, id, head.Number, n.Status().Runs, n.ChainID())
		if *grpcPort > 0 {
			gsrv, err := grpcapi.Serve(*grpcPort, n)
			if err != nil {
				log.Fatalf("epochdb: serve: gRPC: %v", err)
			}
			defer gsrv.Stop()
			log.Printf("epochdb: serve: gRPC on :%d", *grpcPort)
		}
		go syncLoop(ctx, n.CAS(), *syncEvery)

		select {
		case <-ctx.Done():
			log.Printf("epochdb: serve: shutting down, flushing")
		case err := <-dead:
			// The writers close before we die, so the restart is a clean resume
			// rather than a crash walk-back. stop() cancels ctx first, because
			// Close waits for the executor goroutine to return before it
			// releases the executor's writers.
			log.Printf("epochdb: serve: FATAL: %v", err)
			stop()
			exit = 1
		}
		// Stop serving BEFORE closing the read side: Close unmaps files the
		// request goroutines read.
		sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		srv.Shutdown(sctx)
		cancel()
		n.Close()
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
	Fetched      uint64 `json:"fetched"`
	Executed     uint64 `json:"executed"`
	Cooked       uint64 `json:"cooked"`
	CacheHorizon string `json:"cacheHorizon,omitempty"`
	CacheEvicted uint64 `json:"cacheEvictions,omitempty"`
	CacheRefused uint64 `json:"cacheRefusals,omitempty"`
	CacheFree    int64  `json:"cacheFreeBytes,omitempty"`
	CacheMinFree int64  `json:"cacheMinFreeBytes,omitempty"`
	// The cache-plane failures, which are the ones that leave a node working
	// and slow rather than broken: a fill that cannot land, an eviction that
	// cannot run, a cached chunk that cannot be read. Every one of them still
	// returns right bytes over the network, so this is the only place a node
	// that has silently become an S3 passthrough can be told from a healthy
	// one. All omitempty: a clean node's answer is byte-identical to what it
	// always was.
	CacheFillErrors  uint64 `json:"cacheFillErrors,omitempty"`
	CacheEvictErrors uint64 `json:"cacheEvictErrors,omitempty"`
	CacheReadErrors  uint64 `json:"cacheReadErrors,omitempty"`
	CacheLastError   string `json:"cacheLastError,omitempty"`
	// The RAM the fetched-not-executed containers occupy, and this run's peak.
	// Nothing pre-execution touches disk, so this is the entire cost of the
	// fetch running ahead of the executor.
	QueueBytes     uint64 `json:"queueBytes,omitempty"`
	PeakQueueBytes uint64 `json:"peakQueueBytes,omitempty"`
	// The fetch-plane equivalents of the cache failures above: a node that is
	// polling hard and landing nothing. PrunedSeeds counts peers answering a
	// height poll with their own last accepted block (they pruned the height),
	// BadLinks spans discarded for not linking. Both omitempty, so a healthy
	// answer is byte-identical to what it always was.
	PrunedSeeds uint64 `json:"prunedSeeds,omitempty"`
	BadLinks    uint64 `json:"badLinks,omitempty"`
}

// statusOf builds that answer. n is nil between the bind and the moment the
// chain is up, which is minutes to hours on a cold dir: serving=false and the
// spec the operator asked for is the whole honest answer there.
func statusOf(spec string, n *epochdb.Node) serveStatus {
	if n == nil {
		return serveStatus{Chain: spec}
	}
	s := serveStatusOf(n.Status(), spec)
	if cs, ok := n.CAS().CacheStats(); ok {
		if cs.VictimAge > 0 {
			s.CacheHorizon = cs.VictimAge.String()
		}
		s.CacheEvicted, s.CacheRefused = cs.Evictions, cs.Refusals
		s.CacheFree, s.CacheMinFree = cs.FreeBytes, cs.MinFree
		s.CacheFillErrors, s.CacheEvictErrors = cs.FillErrors, cs.EvictErrors
		s.CacheReadErrors, s.CacheLastError = cs.CacheReadErrors, cs.LastError
	}
	return s
}

// serveOn binds the RPC port and ONLY THEN does the startup work on it. The
// order is the whole point: starting a chain is an hour of work on a big corpus
// (the join walk and, on an empty dir, its whole frontier build, then the exec
// open), and while it ran nothing had touched the port, so a collision surfaced
// as FATAL 68 minutes in (Fuji, 2026-08-01, twice in one night). A bad port now
// fails in milliseconds.
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

// statusLine is the one-line health of this chain, for the log loop. /status
// answers with the structured twin (serveStatusOf).
func statusLine(s epochdb.Status) string {
	lead, lag := fmt.Sprintf("accepted=%d", s.Head), fmt.Sprintf("%d", int64(s.Head)-int64(s.Executed))
	if s.Backfill {
		lead = fmt.Sprintf("target=%d", s.Head)
		if s.Head == 0 { // the override container has not resolved yet
			lead, lag = "target=?", "?"
		}
	}
	line := fmt.Sprintf("%s fetched=%d executed=%d served=%d cooked=%d settled=%d exec_lag=%s tail=%d/%.1fMB queue=%.0f/%.0fMB",
		lead, s.Fetched, s.Executed, s.Served, s.Flushed, s.Settled, lag, s.Runs, float64(s.Bytes)/1e6,
		float64(s.QueueBytes)/1e6, float64(s.PeakQueueBytes)/1e6)
	// The two counters that separate "fetching nothing because there is
	// nothing to fetch" from "fetching nothing because no peer will serve
	// it". Absent while both are zero, so a healthy line is unchanged.
	if s.PrunedSeeds|s.BadLinks != 0 {
		line += fmt.Sprintf(" pruned_seeds=%d bad_links=%d", s.PrunedSeeds, s.BadLinks)
	}
	return line
}

func serveStatusOf(s epochdb.Status, chain string) serveStatus {
	out := serveStatus{Chain: chain, Serving: true, Executed: s.Executed, Cooked: s.Flushed,
		Fetched: s.Fetched, QueueBytes: s.QueueBytes, PeakQueueBytes: s.PeakQueueBytes,
		PrunedSeeds: s.PrunedSeeds, BadLinks: s.BadLinks}
	if s.Backfill {
		out.Target = s.Head
	} else {
		out.Accepted = s.Head
	}
	return out
}

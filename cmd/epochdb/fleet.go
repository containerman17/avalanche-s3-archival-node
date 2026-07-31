package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	avaconstants "github.com/ava-labs/avalanchego/utils/constants"

	"github.com/containerman17/epochdb/chain"
	"github.com/containerman17/epochdb/dist"
)

// fleetMain is THE FLEET (DESIGN.md "THE FLEET"): ONE process hosting every
// subnet-evm chain on the box. It is not a new node, it is N of `serve`'s stack
// (startNode, follow.go) side by side, and the only three things that are
// genuinely fleet-shaped are:
//
//   - ONE VM KIND. libevm's extras registry is process-global and panics on
//     re-registration, so every descriptor must name the same kind. That is
//     checked up front, before anything opens, so the answer is an error
//     message and not a panic 40 seconds into a dial.
//   - ONE CASFS. dist.SetRoot puts the spool and the chunk cache at the fleet
//     root instead of inside each chain's dir, which makes the SSD-tier LRU
//     global across chains. Safe because artifacts are named by content hash.
//     One syncLoop, not N: the stores share the underlying casfs, so a sibling's
//     Sync would upload the same spool again.
//   - ONE LISTENER, path-routed per chain, and a per-chain failure boundary
//     (fleetChain): an executor hard-stop cancels that chain's goroutines and
//     nothing else.
//
// Anon memory stays bounded by construction: each chain's Firewood cache takes
// the clamp formula off its own dir size and the page cache is the read cache.
func fleetMain(args []string) {
	fs := flag.NewFlagSet("fleet", flag.ExitOnError)
	chains := fs.String("chains", "", "comma-separated chain descriptor JSONs, one per hosted chain")
	root := fs.String("data", "./data", "fleet root: <root>/<blockchainID> per chain, <root>/cas and <root>/chunks shared by all of them")
	port := fs.Int("port", 9650, "HTTP listen port; every chain answers at /ext/bc/<blockchainID>/rpc")
	nodeURI := fs.String("node", "", "bootstrap RPC node URI for every chain (default: the public endpoint for each descriptor's networkID)")
	vdrSources := fs.String("vdr-sources", "", "comma-separated platform RPC URIs for the cross-checked validator set")
	stateCacheGiB := fs.Int("state-cache", 1, "executor Go-side read cache in GiB, PER CHAIN (0 disables)")
	cookEvery := fs.Duration("cook-every", time.Minute, "cadence for the in-process cook that drags each chain's historical window up to its head")
	perPeer := fs.Int("per-peer", 1, "max outstanding requests per archival peer")
	syncEvery := fs.Duration("sync-every", 5*time.Minute, "cadence for uploading spooled artifacts to the bucket (no-op without S3 credentials)")
	pprofAddr := fs.String("pprof", "", "serve net/http/pprof on this address")
	fs.Parse(args)

	if *chains == "" {
		log.Fatalf("epochdb: fleet: --chains is required (comma-separated descriptor JSONs)")
	}
	if err := os.MkdirAll(*root, 0o755); err != nil {
		log.Fatalf("epochdb: fleet: %v", err)
	}
	// Before any store opens: one spool and one chunk cache for the whole
	// process.
	dist.SetRoot(*root)

	if *pprofAddr != "" {
		go func() { log.Printf("epochdb: pprof: %v", http.ListenAndServe(*pprofAddr, nil)) }()
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	descs := loadFleetChains(ctx, strings.Split(*chains, ","))

	f := &fleet{}
	mux := http.NewServeMux()
	mux.HandleFunc("/status", f.statusHandler)
	for _, c := range descs {
		id := c.BlockchainID.String()
		cfg := nodeConfig{
			DataDir:       filepath.Join(*root, id),
			Chain:         c,
			NodeURI:       *nodeURI,
			StateCacheGiB: *stateCacheGiB,
			PerPeer:       *perPeer,
			CookEvery:     *cookEvery,
			Label:         id[:8],
		}
		if cfg.NodeURI == "" {
			_, cfg.NodeURI, _ = netParams(avaconstants.NetworkIDToNetworkName[c.NetworkID])
		}
		if *vdrSources != "" {
			cfg.VdrSources = strings.Split(*vdrSources, ",")
		}
		if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
			log.Printf("epochdb: fleet: %s: %v (chain not started, siblings unaffected)", id, err)
			continue
		}
		// A chain that cannot start is a chain that does not start. The fleet
		// is not allowed to die of one bad data directory, at boot any more
		// than at runtime.
		fc, err := f.add(ctx, id, cfg)
		if err != nil {
			log.Printf("epochdb: fleet: %s NOT STARTED: %v (siblings unaffected)", id, err)
			continue
		}
		// avalanchego's own routing, so wallets, subnets.avax.network tooling
		// and the ab-benches all point at a fleet chain unchanged. /ws is the
		// same handler: rpc.Server upgrades websockets itself.
		mux.Handle("/ext/bc/"+id+"/rpc", fc.node.srv)
		mux.Handle("/ext/bc/"+id+"/ws", fc.node.srv)
		log.Printf("epochdb: fleet: %s serving at /ext/bc/%s/rpc, executed=%d chainId=%s",
			id, id, fc.node.e.LiveHead(), fc.node.chainID)
	}
	if len(f.chains) == 0 {
		log.Fatalf("epochdb: fleet: no chain started")
	}

	// One syncLoop for the shared spool; any chain's store reaches the same
	// casfs.
	go syncLoop(ctx, f.chains[0].node.store.Cas(), *syncEvery)
	srv := &http.Server{Addr: fmt.Sprintf(":%d", *port), Handler: mux}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("epochdb: fleet: FATAL rpc listener: %v", err)
			stop()
		}
	}()
	log.Printf("epochdb: fleet on :%d with %d chains (cook every %s)", *port, len(f.chains), *cookEvery)

	<-ctx.Done()
	log.Printf("epochdb: fleet: shutting down, flushing")
	// Stop serving BEFORE closing the read side: closeAll unmaps files the RPC
	// goroutines read.
	sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	srv.Shutdown(sctx)
	cancel()
	f.closeAll()
}

// loadFleetChains reads every descriptor and refuses the mixes that cannot
// work in one process, BEFORE anything opens or dials.
func loadFleetChains(ctx context.Context, paths []string) []*chain.Chain {
	var out []*chain.Chain
	seen := map[string]bool{}
	for _, p := range paths {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		lctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		c, err := chain.Load(lctx, p)
		cancel()
		if err != nil {
			log.Fatalf("epochdb: fleet: %v", err)
		}
		// libevm's extras registry is process-global: a second kind in this
		// process would panic on registration, which is exactly why the ruling
		// is one process per VM kind. The C-chain runs alone.
		if len(out) > 0 && c.VMKind != out[0].VMKind {
			log.Fatalf("epochdb: fleet: %s is %s but this fleet is already %s: libevm extras are process-global, so the C-chain (coreth) runs in its OWN process",
				p, c.VMKind, out[0].VMKind)
		}
		if id := c.BlockchainID.String(); seen[id] {
			log.Fatalf("epochdb: fleet: %s listed twice", id)
		} else {
			seen[id] = true
		}
		out = append(out, c)
	}
	if len(out) == 0 {
		log.Fatalf("epochdb: fleet: --chains named no descriptor")
	}
	return out
}

// fleet is the supervisor: the chains and the failure boundary between them.
type fleet struct {
	mu     sync.Mutex
	chains []*fleetChain
}

// fleetChain is ONE chain's failure domain. Everything about isolation lives
// here: the chain's own cancellable context, the flush that runs when it dies,
// and the recorded cause. A sibling's fleetChain is untouched by any of it.
type fleetChain struct {
	id     string
	node   *chainNode
	cancel context.CancelFunc
	// stop flushes this chain's writers. Called at most once, on the first
	// failure (or by closeAll at shutdown).
	stop func()
	once sync.Once

	mu  sync.Mutex
	err error // non-nil once the chain has stopped
}

func (f *fleet) add(ctx context.Context, id string, cfg nodeConfig) (*fleetChain, error) {
	// The per-chain context is the isolation mechanism: cancelling it stops
	// this chain's follower, executor and cook loop and reaches nothing else.
	cctx, cancel := context.WithCancel(ctx)
	fc := &fleetChain{id: id, cancel: cancel}
	n, err := startNode(cctx, cfg, fc.report)
	if err != nil {
		cancel()
		return nil, err
	}
	fc.node, fc.stop = n, n.stop
	f.mu.Lock()
	f.chains = append(f.chains, fc)
	f.mu.Unlock()
	return fc, nil
}

// report routes one component goroutine's exit for one chain: report's
// per-chain twin (follow.go). A clean finish and a cancellation are not
// failures; anything else stops THIS chain and leaves the process and its
// siblings serving.
func (c *fleetChain) report(what string, err error) {
	switch {
	case err == nil:
		log.Printf("epochdb: fleet: %s: %s finished; the chain keeps serving", c.id, what)
		return
	case errors.Is(err, context.Canceled):
		log.Printf("epochdb: fleet: %s: %s stopped: %v", c.id, what, err)
		return
	}
	c.mu.Lock()
	first := c.err == nil
	if first {
		c.err = fmt.Errorf("%s: %w", what, err)
	}
	c.mu.Unlock()
	if !first {
		return
	}
	log.Printf("epochdb: fleet: %s STOPPED: %s: %v (the process and its sibling chains keep serving)", c.id, what, err)
	c.cancel()
	c.once.Do(c.stop)
}

// stopped returns the cause, nil while the chain is running.
func (c *fleetChain) stopped() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err
}

// statusHandler is /status: what every chain in the process is doing, and which
// ones have stopped and why. The one place an operator learns that chain 3 is
// dead while 1, 2 and 4 are at tip.
func (f *fleet) statusHandler(w http.ResponseWriter, r *http.Request) {
	type chainStatus struct {
		Chain    string `json:"chain"`
		Stopped  bool   `json:"stopped"`
		Error    string `json:"error,omitempty"`
		Accepted uint64 `json:"accepted"`
		Executed uint64 `json:"executed"`
		Cooked   uint64 `json:"cooked"`
	}
	f.mu.Lock()
	chains := append([]*fleetChain(nil), f.chains...)
	f.mu.Unlock()
	out := make([]chainStatus, 0, len(chains))
	for _, c := range chains {
		s := chainStatus{Chain: c.id}
		if err := c.stopped(); err != nil {
			s.Stopped, s.Error = true, err.Error()
		}
		if c.node != nil {
			s.Accepted, s.Executed, s.Cooked = c.node.accepted(), c.node.e.LiveHead(), c.node.hist.StateHead()
		}
		out = append(out, s)
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(out)
}

// closeAll is the clean shutdown: flush and release every chain. Call it only
// after the listener is down.
func (f *fleet) closeAll() {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, c := range f.chains {
		if c.node == nil {
			continue
		}
		c.node.closeAll() // chainNode.stopOnce keeps a dead chain from double-closing
		log.Printf("epochdb: fleet: %s stopped at executed=%d", c.id, c.node.e.LiveHead())
	}
}

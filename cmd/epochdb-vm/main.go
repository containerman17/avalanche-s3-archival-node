// Command epochdb-vm is a subnet-evm archival node on the state engine
// (latest + commit): epochdb serve's fetch, store and RPC with vmexec in
// place of exec. No Firewood, no triedb. A restart resumes from the store's
// head: the rolled state is reopened and the rest rebuilt from the rows.
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
	_ "net/http/pprof"
	"os"
	"os/signal"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"time"

	avaconstants "github.com/ava-labs/avalanchego/utils/constants"

	"github.com/containerman17/avalanche-s3-archival-node/chain"
	"github.com/containerman17/avalanche-s3-archival-node/dist"
	"github.com/containerman17/avalanche-s3-archival-node/fetch"
	"github.com/containerman17/avalanche-s3-archival-node/rpc"
	"github.com/containerman17/avalanche-s3-archival-node/store"
	"github.com/containerman17/avalanche-s3-archival-node/vmexec"
)

func main() {
	fs := flag.NewFlagSet("epochdb-vm", flag.ExitOnError)
	dataDir := fs.String("data", "./data", "data directory")
	port := fs.Int("port", 9650, "HTTP listen port: JSON-RPC at / and /ext/bc/<blockchainID>/rpc, /status")
	p2pPort := fs.Int("p2p-port", 0, "listen for avalanchego peers on this port (0 disables)")
	peers := fs.String("peers", "", "comma-separated extra archival peers, NodeID-...@host:port each (an epochdb --p2p-port or epochdb-archive-serve)")
	network := fs.String("network", "fuji", "network: fuji|mainnet")
	chainSpec := fs.String("chain", "", "the L1's blockchainID (subnet-evm only)")
	nodeURI := fs.String("node", "", "comma-separated bootstrap RPC node URIs")
	vdrSources := fs.String("vdr-sources", "", "comma-separated platform RPC URIs for the validator set")
	perPeer := fs.Int("per-peer", 1, "max outstanding requests per archival peer")
	rollSync := fs.Int64("roll-budget-sync", 2<<30, "overlay bytes that trigger a background merge + trie roll while catching up")
	fs.Int64Var(rollSync, "roll-budget", 2<<30, "alias of --roll-budget-sync")
	rollTip := fs.Int64("roll-budget-tip", 128<<20, "overlay bytes that trigger a roll at the tip")
	tipLag := fs.Uint64("tip-lag", 5000, "blocks behind the accepted head that count as the tip; catch-up resumes past twice this")
	stopAt := fs.Uint64("stop", 0, "stop after executing this height (0 = follow)")
	pprofAddr := fs.String("pprof", "", "serve net/http/pprof on this address")
	gogcSync := fs.Int("gogc-sync", 400, "GOGC while catching up; the memory limit still caps the heap")
	fs.IntVar(gogcSync, "gogc", 400, "alias of --gogc-sync")
	gogcTip := fs.Int("gogc-tip", 100, "GOGC at the tip")
	fs.Parse(os.Args[1:])
	if *chainSpec == "" || *chainSpec == "C" {
		log.Fatalf("epochdb-vm: --chain must be an L1's blockchainID (subnet-evm only)")
	}
	release, err := lockDataDir(*dataDir)
	if err != nil {
		log.Fatalf("epochdb-vm: %v", err)
	}
	defer release()
	if *pprofAddr != "" {
		go func() { log.Printf("epochdb-vm: pprof: %v", http.ListenAndServe(*pprofAddr, nil)) }()
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	netID, defaultNode := netParams(*network)
	rctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	c, err := chain.Resolve(rctx, *chainSpec, netID, *dataDir, dist.Sources(*nodeURI)...)
	cancel()
	if err != nil {
		log.Fatalf("epochdb-vm: --chain %s: %v", *chainSpec, err)
	}
	if *nodeURI == "" {
		_, *nodeURI = netParams(avaconstants.NetworkIDToNetworkName[c.NetworkID])
		if *nodeURI == "" {
			*nodeURI = defaultNode
		}
	}
	id := c.BlockchainID.String()

	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("epochdb-vm: %v", err)
	}
	defer ln.Close()

	g, err := vmexec.ChainGenesis(c)
	if err != nil {
		log.Fatalf("epochdb-vm: genesis: %v", err)
	}
	fetcher, err := fetch.New(fetch.Config{
		NodeURI: *nodeURI, PerPeer: *perPeer, Chain: c, VdrSources: dist.Sources(*vdrSources),
		ListenPort: *p2pPort, DataDir: *dataDir, Peers: dist.Sources(*peers),
	})
	if err != nil {
		log.Fatalf("epochdb-vm: fetch: %v", err)
	}
	defer fetcher.Close()
	cas, err := dist.Open(*dataDir)
	if err != nil {
		log.Fatalf("epochdb-vm: open artifact store: %v", err)
	}
	defer cas.Close()
	if err := store.Join(cas, *dataDir, c.Root()); err != nil {
		log.Fatalf("epochdb-vm: join chain: %v", err)
	}
	db, err := store.Open(*dataDir, cas, c.Root())
	if err != nil {
		log.Fatalf("epochdb-vm: open storage v0: %v", err)
	}
	defer db.Close()
	misc, err := store.OpenMisc(*dataDir)
	if err != nil {
		log.Fatalf("epochdb-vm: open misc store: %v", err)
	}
	defer misc.Close()

	from, anchor, err := vmexec.FetchStart(db, g.Hash)
	if err != nil {
		log.Fatalf("epochdb-vm: %v", err)
	}
	blocks := fetcher.StartForward(ctx, from, anchor)

	srv := rpc.NewServer(db, g.TrieAlloc, rpc.StoreChainContext(db), g.Config)
	fetcher.Serve(srv)

	e, err := vmexec.New(vmexec.Config{
		DataDir: *dataDir, Blocks: blocks, Store: db, CAS: cas, Misc: misc, Chain: c,
		StopAt: *stopAt,
		Budget: vmexec.Budget{
			Accepted: fetcher.AcceptedHead, TipLag: *tipLag,
			SyncGOGC: *gogcSync, TipGOGC: *gogcTip, SyncRoll: int(*rollSync), TipRoll: int(*rollTip),
		},
	})
	if err != nil {
		log.Fatalf("epochdb-vm: vmexec.New: %v", err)
	}
	srv.EnableLive(liveNode{live: e.LiveHead, accepted: fetcher.AcceptedHead})

	dead := make(chan error, 1)
	report := func(what string, err error) {
		switch {
		case err == nil:
			log.Printf("epochdb-vm: %s finished", what)
		case errors.Is(err, context.Canceled):
			log.Printf("epochdb-vm: %s stopped: %v", what, err)
		default:
			select {
			case dead <- fmt.Errorf("%s: %w", what, err):
			default:
			}
		}
	}
	var executed atomic.Uint64
	mux := http.NewServeMux()
	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		p := fetcher.Progress()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"chain": id, "serving": true, "accepted": fetcher.AcceptedHead(), "fetched": p.Head,
			"executed": e.LiveHead(), "queueBytes": p.QueueBytes,
		})
	})
	mux.Handle("/ext/bc/"+id+"/rpc", srv)
	mux.Handle("/ext/bc/"+id+"/ws", srv)
	mux.Handle("/", srv)
	hsrv := &http.Server{Handler: mux}
	go func() {
		if err := hsrv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("epochdb-vm: FATAL rpc listener: %v", err)
			stop()
		}
	}()
	log.Printf("epochdb-vm: %s on :%d chainId=%s roll-budget=%dMB/%dMB tip-lag=%d", id, *port, g.Config.ChainID, *rollSync>>20, *rollTip>>20, *tipLag)

	go func() { report("follower", fetcher.Follow(ctx)) }()
	execDone := make(chan struct{})
	go func() {
		err := e.Run(ctx)
		close(execDone)
		report("executor", err)
	}()
	bench := newBench(e, fetcher, &executed)
	go bench.loop(ctx)

	exit := 0
	select {
	case <-ctx.Done():
		log.Printf("epochdb-vm: shutting down, flushing")
	case err := <-dead:
		log.Printf("epochdb-vm: FATAL: %v", err)
		stop()
		exit = 1
	}
	sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	hsrv.Shutdown(sctx)
	cancel()
	<-execDone
	bench.line("exit")
	if err := e.Close(); err != nil {
		log.Printf("epochdb-vm: executor close: %v", err)
	}
	if exit != 0 {
		os.Exit(exit)
	}
}

// bench prints one grep-friendly line every 10 s and at exit:
//
//	bench t=<s> h=<height> blk=<n> tx=<n> mgas/s=<window> cum=<since first block>
//	wait=<s waiting for a container> full=<s fetch buffer was full> rss=<MB>
//	overlay=<MB> dirty=<MB> rolls=<n>
type bench struct {
	e        *vmexec.Executor
	f        *fetch.Fetcher
	executed *atomic.Uint64
	t0       time.Time
	first    time.Time // first executed block
	last     vmexec.Stats
	lastT    time.Time
	fullNs   int64
}

func newBench(e *vmexec.Executor, f *fetch.Fetcher, executed *atomic.Uint64) *bench {
	now := time.Now()
	return &bench{e: e, f: f, executed: executed, t0: now, lastT: now}
}

func (b *bench) loop(ctx context.Context) {
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	n := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		// full: the fetch pauses at queueMaxBytes (1 GB) or WindowBlocks ahead.
		p := b.f.Progress()
		s := b.e.Stats()
		if p.QueueBytes >= 9<<26 || (p.Head > s.Height && p.Head-s.Height >= fetch.WindowBlocks*9/10) {
			b.fullNs += int64(time.Second)
		}
		if b.first.IsZero() && s.Blocks > 0 {
			b.first = time.Now()
		}
		if n++; n%10 == 0 {
			b.line("bench")
		}
	}
}

func (b *bench) line(tag string) {
	s := b.e.Stats()
	now := time.Now()
	dt := now.Sub(b.lastT).Seconds()
	var window, cum float64
	if dt > 0 {
		window = float64(s.Gas-b.last.Gas) / dt / 1e6
	}
	if !b.first.IsZero() {
		if since := now.Sub(b.first).Seconds(); since > 0 {
			cum = float64(s.Gas) / since / 1e6
		}
	}
	log.Printf("%s t=%.0f h=%d blk=%d tx=%d mgas/s=%.2f cum=%.2f wait=%.1f full=%.0f rss=%d overlay=%d dirty=%d rolls=%d rolling=%v",
		tag, now.Sub(b.t0).Seconds(), s.Height, s.Blocks, s.Txs, window, cum,
		s.Wait.Seconds(), float64(b.fullNs)/1e9, vmexec.RSSMB(), s.Overlay>>20, s.Dirty>>20, s.Rolls, s.Rolling)
	b.last, b.lastT = s, now
}

// liveNode is the rpc.Live surface: no SAE, so settled == live.
type liveNode struct {
	live     func() uint64
	accepted func() uint64
}

func (l liveNode) LiveHead() uint64      { return l.live() }
func (l liveNode) SettledHeight() uint64 { return l.live() }
func (l liveNode) AcceptedHead() uint64  { return max(l.accepted(), l.live()) }
func (l liveNode) SyncTarget() uint64    { return l.AcceptedHead() }

func netParams(network string) (uint32, string) {
	switch network {
	case "fuji":
		return avaconstants.FujiID, "https://api.avax-test.network"
	case "mainnet":
		return avaconstants.MainnetID, "https://api.avax.network"
	}
	log.Fatalf("epochdb-vm: unknown --network %q (fuji|mainnet)", network)
	return 0, ""
}

// lockDataDir takes the dir's exclusive writer flock for the life of the process.
func lockDataDir(dir string) (func(), error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(dir, ".epochdb.lock"), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, fmt.Errorf("data dir %s is already held by another epochdb process", dir)
	}
	return func() { f.Close() }, nil
}

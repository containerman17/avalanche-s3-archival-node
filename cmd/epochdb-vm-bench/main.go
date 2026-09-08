// Command epochdb-vm-bench is epochdb-vm fed from a container dump file
// instead of the p2p fetcher: the same executor, store writes, checker and
// bench line, so a Rust node reading the same file can be compared with it
// on one machine. No fetch, no p2p, no S3, no P-chain call: the data dir
// must already hold chain.json (and upgrade.json) copied from the dump dir.
// The process exits when the executor reaches --to.
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
	"github.com/containerman17/avalanche-s3-archival-node/rpc"
	"github.com/containerman17/avalanche-s3-archival-node/store"
	"github.com/containerman17/avalanche-s3-archival-node/vmexec"
)

func main() {
	fs := flag.NewFlagSet("epochdb-vm-bench", flag.ExitOnError)
	dataDir := fs.String("data", "./data", "data directory (must hold chain.json and upgrade.json)")
	port := fs.Int("port", 9650, "HTTP listen port: JSON-RPC at / and /ext/bc/<blockchainID>/rpc, /status")
	network := fs.String("network", "fuji", "network: fuji|mainnet")
	chainSpec := fs.String("chain", "", "the L1's blockchainID (subnet-evm only)")
	dumpPath := fs.String("dump", "", "container dump file: [u64 LE height][u32 LE len][container] records from height 1")
	from := fs.Uint64("from", 1, "first height the dump serves (the executor starts at the store's head+1, which must not be below it)")
	to := fs.Uint64("to", 0, "last height to execute, then exit (0 = the dump's last)")
	fs.Uint64Var(to, "stop", 0, "alias of --to")
	fs.Uint64Var(to, "stop-at", 0, "alias of --to")
	rollSync := fs.Int64("roll-budget-sync", 2<<30, "overlay bytes that trigger a background merge + trie roll while catching up")
	fs.Int64Var(rollSync, "roll-budget", 2<<30, "alias of --roll-budget-sync")
	rollTip := fs.Int64("roll-budget-tip", 128<<20, "overlay bytes that trigger a roll at the tip")
	tipLag := fs.Uint64("tip-lag", 5000, "blocks behind the dump's last height that count as the tip; catch-up resumes past twice this")
	pprofAddr := fs.String("pprof", "", "serve net/http/pprof on this address")
	gogcSync := fs.Int("gogc-sync", 400, "GOGC while catching up; the memory limit still caps the heap")
	fs.IntVar(gogcSync, "gogc", 400, "alias of --gogc-sync")
	gogcTip := fs.Int("gogc-tip", 100, "GOGC at the tip")
	fs.Parse(os.Args[1:])
	if *chainSpec == "" || *chainSpec == "C" {
		log.Fatalf("epochdb-vm-bench: --chain must be an L1's blockchainID (subnet-evm only)")
	}
	if *dumpPath == "" {
		log.Fatalf("epochdb-vm-bench: --dump is required")
	}
	if _, err := os.Stat(filepath.Join(*dataDir, "chain.json")); err != nil {
		log.Fatalf("epochdb-vm-bench: %v: copy chain.json (and upgrade.json) from the dump dir so no P-chain call is needed", err)
	}
	release, err := lockDataDir(*dataDir)
	if err != nil {
		log.Fatalf("epochdb-vm-bench: %v", err)
	}
	defer release()
	if *pprofAddr != "" {
		go func() { log.Printf("epochdb-vm-bench: pprof: %v", http.ListenAndServe(*pprofAddr, nil)) }()
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	netID := netParams(*network)
	c, err := chain.Resolve(ctx, *chainSpec, netID, *dataDir)
	if err != nil {
		log.Fatalf("epochdb-vm-bench: --chain %s: %v", *chainSpec, err)
	}
	id := c.BlockchainID.String()

	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("epochdb-vm-bench: %v", err)
	}
	defer ln.Close()

	g, err := vmexec.ChainGenesis(c)
	if err != nil {
		log.Fatalf("epochdb-vm-bench: genesis: %v", err)
	}
	src, err := openDump(*dumpPath, *from, *to)
	if err != nil {
		log.Fatalf("epochdb-vm-bench: dump: %v", err)
	}
	defer src.close()
	cas, err := dist.Local(*dataDir) // never S3, whatever EPOCHDB_S3_* says
	if err != nil {
		log.Fatalf("epochdb-vm-bench: open artifact store: %v", err)
	}
	defer cas.Close()
	if err := store.Join(cas, *dataDir, c.Root()); err != nil {
		log.Fatalf("epochdb-vm-bench: join chain: %v", err)
	}
	db, err := store.Open(*dataDir, cas, c.Root())
	if err != nil {
		log.Fatalf("epochdb-vm-bench: open storage v0: %v", err)
	}
	defer db.Close()
	misc, err := store.OpenMisc(*dataDir)
	if err != nil {
		log.Fatalf("epochdb-vm-bench: open misc store: %v", err)
	}
	defer misc.Close()

	first, _, err := vmexec.FetchStart(db, g.Hash)
	if err != nil {
		log.Fatalf("epochdb-vm-bench: %v", err)
	}
	if first < *from {
		log.Fatalf("epochdb-vm-bench: the store's head is %d but the dump serves from %d", first-1, *from)
	}

	srv := rpc.NewServer(db, g.TrieAlloc, rpc.StoreChainContext(db), g.Config)

	e, err := vmexec.New(vmexec.Config{
		DataDir: *dataDir, Blocks: src, Store: db, CAS: cas, Misc: misc, Chain: c,
		StopAt: src.Last(),
		Budget: vmexec.Budget{
			Accepted: src.Last, TipLag: *tipLag,
			SyncGOGC: *gogcSync, TipGOGC: *gogcTip, SyncRoll: int(*rollSync), TipRoll: int(*rollTip),
		},
	})
	if err != nil {
		log.Fatalf("epochdb-vm-bench: vmexec.New: %v", err)
	}
	srv.EnableLive(liveNode{live: e.LiveHead, accepted: src.Last})

	dead := make(chan error, 1)
	report := func(what string, err error) {
		switch {
		case err == nil:
			log.Printf("epochdb-vm-bench: %s finished", what)
		case errors.Is(err, context.Canceled):
			log.Printf("epochdb-vm-bench: %s stopped: %v", what, err)
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
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"chain": id, "serving": true, "accepted": src.Last(), "fetched": src.Last(),
			"executed": e.LiveHead(), "queueBytes": 0,
		})
	})
	mux.Handle("/ext/bc/"+id+"/rpc", srv)
	mux.Handle("/ext/bc/"+id+"/ws", srv)
	mux.Handle("/", srv)
	hsrv := &http.Server{Handler: mux}
	go func() {
		if err := hsrv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("epochdb-vm-bench: FATAL rpc listener: %v", err)
			stop()
		}
	}()
	log.Printf("epochdb-vm-bench: %s on :%d chainId=%s dump=%s heights=%d..%d roll-budget=%dMB/%dMB tip-lag=%d", id, *port, g.Config.ChainID, *dumpPath, first, src.Last(), *rollSync>>20, *rollTip>>20, *tipLag)

	execDone := make(chan struct{})
	go func() {
		err := e.Run(ctx)
		close(execDone)
		report("executor", err)
	}()
	bench := newBench(e, &executed)
	go bench.loop(ctx)

	exit := 0
	select {
	case <-ctx.Done():
		log.Printf("epochdb-vm-bench: shutting down, flushing")
	case <-execDone:
		log.Printf("epochdb-vm-bench: executor done, flushing")
		stop()
	case err := <-dead:
		log.Printf("epochdb-vm-bench: FATAL: %v", err)
		stop()
		exit = 1
	}
	sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	hsrv.Shutdown(sctx)
	cancel()
	<-execDone
	bench.line("bench exit")
	if err := e.Close(); err != nil {
		log.Printf("epochdb-vm-bench: executor close: %v", err)
	}
	if exit != 0 {
		os.Exit(exit)
	}
}

// bench prints one grep-friendly line every 10 s and at exit, epochdb-vm's
// line verbatim; full= is always 0 (a file is never full):
//
//	bench t=<s> h=<height> blk=<n> tx=<n> mgas/s=<window> cum=<since first block>
//	wait=<s waiting for a container> full=0 rss=<MB>
//	overlay=<MB> dirty=<MB> rolls=<n>
type bench struct {
	e        *vmexec.Executor
	executed *atomic.Uint64
	t0       time.Time
	first    time.Time // first executed block
	last     vmexec.Stats
	lastT    time.Time
}

func newBench(e *vmexec.Executor, executed *atomic.Uint64) *bench {
	now := time.Now()
	return &bench{e: e, executed: executed, t0: now, lastT: now}
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
		s := b.e.Stats()
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
	log.Printf("%s t=%.0f h=%d blk=%d tx=%d mgas/s=%.2f cum=%.2f wait=%.1f full=0 rss=%d overlay=%d dirty=%d rolls=%d rolling=%v",
		tag, now.Sub(b.t0).Seconds(), s.Height, s.Blocks, s.Txs, window, cum,
		s.Wait.Seconds(), vmexec.RSSMB(), s.Overlay>>20, s.Dirty>>20, s.Rolls, s.Rolling)
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

func netParams(network string) uint32 {
	switch network {
	case "fuji":
		return avaconstants.FujiID
	case "mainnet":
		return avaconstants.MainnetID
	}
	log.Fatalf("epochdb-vm-bench: unknown --network %q (fuji|mainnet)", network)
	return 0
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

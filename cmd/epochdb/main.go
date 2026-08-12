// Command epochdb runs the compact C-chain historical node.
package main

import (
	"context"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/ava-labs/avalanchego/ids"
	avaconstants "github.com/ava-labs/avalanchego/utils/constants"

	"github.com/containerman17/epochdb/chain"
	"github.com/containerman17/epochdb/dist"
	"github.com/containerman17/epochdb/exec"
	"github.com/containerman17/epochdb/fetch"
	"github.com/containerman17/epochdb/store"
)

// parseContainerID accepts a cb58 container ID or a 0x-hex 32-byte eth
// block hash (valid as a container ID for pre-ProposerVM blocks).
func parseContainerID(s string) (ids.ID, error) {
	if strings.HasPrefix(s, "0x") {
		b, err := hex.DecodeString(s[2:])
		if err != nil {
			return ids.Empty, err
		}
		return ids.ToID(b)
	}
	return ids.FromString(s)
}

// chainFlags registers --network and --chain on fs. A chain is named by its ID
// and nothing else: `C` (the default) is the primary network's C-chain for
// --network, out of avalanchego's embedded config; anything else is an L1's
// blockchainID, resolved off the P-chain on first start and cached in the data
// dir (see chain.Resolve). --network still picks the network to dial and the
// P-chain endpoint to resolve against, so a mainnet L1 needs --network mainnet.
//
// Returns the --network value (some commands still need it for the default
// node/archive URLs) and a resolver that takes the chain's data dir, which is
// where both the cache and the optional upgrade.json live.
func chainFlags(fs *flag.FlagSet) (*string, func(dataDir string) *chain.Chain) {
	network := fs.String("network", "fuji", "network: fuji|mainnet (the network --chain lives on)")
	spec := fs.String("chain", "C", "chain: C for --network's primary C-chain, or an L1's blockchainID")
	return network, func(dataDir string) *chain.Chain {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		c, err := chain.Resolve(ctx, *spec, execNetID(*network), dataDir)
		if err != nil {
			log.Fatalf("epochdb: --chain: %v", err)
		}
		return c
	}
}

// mustCChain is the descriptor for --network's primary-network C-chain, for
// the commands that are C-chain-only by nature (the A/B benches against a
// public archive RPC, the event-log backfill).
func mustCChain(network string) *chain.Chain {
	c, err := chain.CChain(execNetID(network))
	if err != nil {
		log.Fatalf("epochdb: --network: %v", err)
	}
	return c
}

// netParams resolves --network into (networkID, node URI, archive RPC URL).
func netParams(network string) (uint32, string, string) {
	switch network {
	case "fuji":
		return avaconstants.FujiID, "https://api.avax-test.network", "https://api.avax-test.network/ext/bc/C/rpc"
	case "mainnet":
		return avaconstants.MainnetID, "https://api.avax.network", "https://api.avax.network/ext/bc/C/rpc"
	default:
		log.Fatalf("epochdb: unknown --network %q (fuji|mainnet)", network)
		return 0, "", ""
	}
}

// parseTipOverride turns a --tip-override value into a container ID. The
// override is a PHYSICAL container ID and never a height (RULING 2026-08-01):
// resolving a height needed the ProposerVM activation constant plus the
// embedded checkpoint table, and an operator must not depend on that magic.
func parseTipOverride(v string) (ids.ID, error) {
	if _, err := strconv.ParseUint(v, 10, 64); err == nil {
		return ids.Empty, fmt.Errorf("%q is a block height; --tip-override takes a container ID: %s", v, tipOverrideHowTo)
	}
	id, err := parseContainerID(v)
	if err != nil {
		return ids.Empty, fmt.Errorf("%q is not a container ID (%v): %s", v, err, tipOverrideHowTo)
	}
	return id, nil
}

// tipOverrideHowTo is the only thing an operator needs to hear after getting
// the value wrong: where the IDs are printed.
const tipOverrideHowTo = "cb58, or a 0x-hex eth block hash for pre-ProposerVM blocks. " +
	"The fetch log prints one per accepted block, 'consensus: accepted height=N container=<ID>', " +
	"and any checkpoint anchor line ('fetch: tip-override <ID> at height N') carries one too"

// devCommands are the pipeline stages and the benches. They are NOT operator
// surface (RULING 2026-08-03: the operator surface is `serve` and nothing else)
// and they are deliberately absent from `usage`, but they still exist, because
// our own release gates and experiments drive them: `epochdb dev seal ...`,
// `epochdb dev ab-bench ...`. Flag sets are unchanged.
var devCommands = map[string]func([]string){
	"fetch":   fetchMain,
	"exec":    execMain,
	"verify":  verifyMain,
	"rpcdiff": rpcdiffMain,
	"probe":   probeMain,
	"publish": publishMain,
}

func main() {
	setGoMemLimit()
	setGoGC()
	warnIfHeapPagesAreKept()
	os.Exit(dispatch(os.Args[1:], os.Stderr))
}

// warnIfHeapPagesAreKept says so when the runtime will hold freed heap pages
// resident instead of handing them back.
//
// Go's default is MADV_FREE: a freed page stays in RSS until the kernel wants
// it, so the heap arena ratchets to its HIGH-WATER MARK and never comes down.
// On a node whose throughput comes from the page cache that is the worst
// possible trade, and it is invisible in `docker stats` (the total looks
// steady) and invisible in gctrace (the LIVE heap really is small). Measured on
// mainnet C: an 18.4GB arena against a live heap oscillating 12-13GB, and
// MADV_DONTNEED moved container anon 35.34 -> 21.83GB, page cache
// 20.24 -> 24.45GB, mgas 229.6 -> 302.7.
//
// The image sets GODEBUG=madvdontneed=1, but docker REPLACES that variable
// wholesale when a compose file names it, so an operator adding gctrace can
// silently drop this. Warn rather than fail: the node still runs, just slower,
// and the line names the fix.
func warnIfHeapPagesAreKept() {
	if strings.Contains(os.Getenv("GODEBUG"), "madvdontneed=1") {
		return
	}
	log.Printf("epochdb: WARNING GODEBUG lacks madvdontneed=1, so the Go heap will keep freed pages RESIDENT " +
		"and the page cache this node reads through will shrink as the heap's high-water mark grows. " +
		"Set GODEBUG=madvdontneed=1 (add it to any GODEBUG you set, docker replaces the variable rather than merging).")
}

// defaultGOGC is deliberately below Go's own 100.
//
// THIS PROCESS RUNS ON THE PAGE CACHE, and the Go pacer does not know that.
// GOGC=100 sizes the heap at twice the live set, and on a node whose speed
// comes from file-backed reads (Firewood's trie, the epoch mappings, the
// arrival log) every byte of that headroom is a byte the kernel cannot hold
// those files in. The collector optimises the wrong resource here.
//
// MEASURED ON MAINNET C, 2026-08-07, with GODEBUG=gctrace=1:
//
//	gc 54: 16054->16062->9004 MB, 17731 MB goal
//
// i.e. a LIVE heap of ~9GB while the runtime grew to ~17.4GB before collecting.
// Dropping to 40 moved the container's anon 32.7 -> 23.2GB and its page cache
// 17.5 -> 26.7GB, and the block rate 56 -> 80. The trade cost nothing: GC ran
// 46-128ms every ~50s at 0% of CPU both before and after, because collection
// was never this workload's bottleneck. Chain state only grows, so the gap
// this closes grows with it.
//
// 50 rather than the 40 measured there: 40 is tuned for the largest chain we
// run and is set explicitly for it, while this default also covers small L1s
// whose heaps are a few hundred MB and which gain little from the last step.
// GOGC in the environment always wins, and an operator who typed a number
// meant it.
const defaultGOGC = 50

func setGoGC() {
	if os.Getenv("GOGC") != "" {
		return
	}
	prev := debug.SetGCPercent(defaultGOGC)
	log.Printf("epochdb: GOGC %d (Go's default is %d): heap headroom is page cache this node reads through, and GC is not its bottleneck",
		defaultGOGC, prev)
}

// setGoMemLimit derives GOMEMLIMIT from the container's own ceiling, so a Go
// heap that grows toward that ceiling COLLECTS HARDER instead of being killed
// by the cgroup. It is the member-level half of the fleet memory rule (DESIGN
// "The fleet memory budget"): the box-level slice bounds the sum, this keeps an
// individual chain from spending its way to the top of its own limit.
//
// SEVEN TENTHS, because a third of what the container spends is invisible to
// the Go runtime and no soft limit can reach it: Firewood's node cache is
// DECODED nodes in Rust (an eighth of this same ceiling, see
// exec.firewoodCacheBytes), zstd holds encoder state per seal, and thread
// stacks and the epoch mappings are outside the heap too. 7/10 + 1/8 = 82.5%
// of the ceiling written down, and the residue is deliberate.
// A limit set at the full ceiling would therefore be no limit at all, and one
// set far below it would spin the collector for nothing.
//
// An explicit GOMEMLIMIT in the environment always wins: the Go runtime has
// already applied it, and an operator who typed a number meant it.
func setGoMemLimit() {
	if os.Getenv("GOMEMLIMIT") != "" {
		return
	}
	limit, ok := exec.CgroupMemoryLimit()
	if !ok {
		return
	}
	soft := int64(limit / 10 * 7)
	debug.SetMemoryLimit(soft)
	log.Printf("epochdb: GOMEMLIMIT %d MB (7/10 of this container's %d MB ceiling; Firewood's cgo cache is outside it)",
		soft>>20, limit>>20)
}

// dispatch returns the process exit code for a command line that never reached
// a command; 0 means a command ran (and owns its own exit from there).
//
// A retired top-level name FAILS LOUDLY instead of running: a stale script that
// still says `epochdb seal` must stop with a pointer, not silently do the thing
// the node now does for itself.
func dispatch(args []string, w io.Writer) int {
	if len(args) == 0 {
		usage(w)
		return 2
	}
	switch args[0] {
	case "serve":
		serveMain(args[1:])
		return 0
	case "dev":
		if len(args) < 2 {
			devUsage(w)
			return 2
		}
		cmd, ok := devCommands[args[1]]
		if !ok {
			fmt.Fprintf(w, "epochdb dev: no such stage %q\n", args[1])
			devUsage(w)
			return 2
		}
		cmd(args[2:])
		return 0
	case "fleet":
		fmt.Fprintln(w, "epochdb: `fleet` is gone, and so is hosting several chains in one process. ONE PROCESS SERVES ONE CHAIN:")
		fmt.Fprintln(w, "         run one `epochdb serve --data <dir> --chain <C|blockchainID>` per chain, one container each, and")
		fmt.Fprintln(w, "         point them all at the same EPOCHDB_CACHE_DIR to share one chunk cache.")
		return 2
	}
	if _, ok := devCommands[args[0]]; ok {
		fmt.Fprintf(w, "epochdb: `%s` is not an operator command. `epochdb serve` is the whole surface: it fetches, executes,\n", args[0])
		fmt.Fprintf(w, "         indexes, cuts and publishes epoch files and answers RPC by itself, and recovers on its own.\n")
		fmt.Fprintf(w, "         The stage still exists for our own gates as `epochdb dev %s` (undocumented, unsupported, same flags).\n", args[0])
		return 2
	}
	usage(w)
	return 2
}

func usage(w io.Writer) {
	fmt.Fprintln(w, "usage: epochdb serve [--data <dir>] [--network mainnet] [--chain C|<blockchainID>]")
	fmt.Fprintln(w, "                     [--port 9650] [--vdr-sources <p-chain rpcs>] [--verify] [--tip-override <containerID>]")
	fmt.Fprintln(w, "")
	fmt.Fprintln(w, "serve is the only command. It follows the chain, executes it, indexes it, cuts and publishes")
	fmt.Fprintln(w, "epoch files, uploads them, and answers RPC, in one process that recovers by itself.")
	fmt.Fprintln(w, "ONE PROCESS SERVES ONE CHAIN: several chains on a box are several processes (one container each),")
	fmt.Fprintln(w, "sharing one chunk cache through EPOCHDB_CACHE_DIR. It answers at / and at /ext/bc/<blockchainID>/rpc,")
	fmt.Fprintln(w, "and /status reports this chain. A start that fails exits nonzero: restarting it is the supervisor's job.")
	fmt.Fprintln(w, "--verify re-verifies the SEALED epochs before serving them; a failure means the node does not start.")
}

// devUsage lists what `dev` still carries. Names only: the stages are ours, and
// each one prints its own flags with -h.
func devUsage(w io.Writer) {
	names := make([]string, 0, len(devCommands))
	for name := range devCommands {
		names = append(names, name)
	}
	sort.Strings(names)
	fmt.Fprintln(w, "usage: epochdb dev <stage> [flags]   (NOT operator surface: `epochdb serve` does all of this by itself)")
	fmt.Fprintln(w, "stages:", strings.Join(names, " "))
}

// execMain replays blocks ascending from genesis out of the (possibly
// still filling) staging dir, verifying every state root.
func execMain(args []string) {
	fs := flag.NewFlagSet("exec", flag.ExitOnError)
	dataDir := fs.String("data", "./data", "shared data directory (staging segments + state layer)")
	pprofAddr := fs.String("pprof", "", "serve net/http/pprof on this address (e.g. localhost:6060)")
	_, resolveChain := chainFlags(fs)
	stateCacheGiB := fs.Int("state-cache", 6, "Go-side EVM read cache size in GiB (0 disables)")
	verifyCache := fs.Bool("verify-cache", false, "re-read every cache hit through Firewood and panic on mismatch (slow, validation only)")
	commitEvery := fs.Int("commit-every", 1000, "blocks per Firewood proposal (root verification at batch boundaries, per-block bisect on mismatch; 1 = classic per-block)")
	stopAt := fs.Uint64("stop", 0, "stop after executing this height (0 = follow staging forever)")
	fs.Parse(args)
	defer mustLockDataDir("exec", *dataDir)()

	if *pprofAddr != "" {
		go func() { log.Printf("epochdb: pprof: %v", http.ListenAndServe(*pprofAddr, nil)) }()
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	reader, err := fetch.OpenReader(*dataDir)
	if err != nil {
		log.Fatalf("epochdb: open staging reader: %v", err)
	}
	defer reader.Close()

	cas, err := dist.Open(*dataDir)
	if err != nil {
		log.Fatalf("epochdb: open artifact store: %v", err)
	}
	defer cas.Close()
	c := resolveChain(*dataDir)
	db, err := store.Open(*dataDir, cas, c.Root())
	if err != nil {
		log.Fatalf("epochdb: open storage v0: %v", err)
	}
	defer db.Close()
	misc, err := store.OpenMisc(*dataDir)
	if err != nil {
		log.Fatalf("epochdb: open misc store: %v", err)
	}
	defer misc.Close()

	e, err := exec.New(exec.Config{
		DataDir:         *dataDir,
		Blocks:          reader,
		Store:           db,
		Misc:            misc,
		StateCacheBytes: uint64(*stateCacheGiB) << 30,
		VerifyCache:     *verifyCache,
		CommitEvery:     *commitEvery,
		Chain:           c,
		StopAt:          *stopAt,
	})
	if err != nil {
		log.Fatalf("epochdb: exec.New: %v", err)
	}
	defer e.Close()

	if err := e.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatalf("epochdb: exec: %v", err)
	}
	log.Printf("epochdb: exec stopped at height=%d, state flushed", e.Head())
}

// resolveTipOverride turns a --tip-override container ID into SyncTo anchors:
// the container itself, plus every embedded checkpoint below its height as a
// parallel walk seed (an L1 has no checkpoints and gets the one anchor). Both
// heights come from the fetched container over p2p, so nothing here depends on
// an archive RPC, on the ProposerVM activation constant, or on a checkpoint
// happening to sit at the right height (RULING 2026-08-01).
// tipAnchorTimeout bounds the wait for the override container itself.
// getContainer RETRIES UNTIL IT ARRIVES, which never happens for an ID from
// another network, and the failure is silent: the fleet ran 28 chains with a
// MAINNET container ID as their --tip-override and every one sat in "waiting
// for block 1 to land in staging" forever, looking merely slow. A seed nobody
// can serve is a typo, not a slow peer, so bound it and name the likely cause.
// Generous because peer warmup precedes it; the checkpoints that follow keep
// the caller's unbounded ctx, since 86 real fetches legitimately take a while.
const tipAnchorTimeout = 5 * time.Minute

func resolveTipOverride(ctx context.Context, f *fetch.Fetcher, id ids.ID) []fetch.Anchor {
	actx, cancel := context.WithTimeout(ctx, tipAnchorTimeout)
	defer cancel()
	tip, err := f.ResolveAnchor(actx, id)
	if err != nil {
		if ctx.Err() == nil && actx.Err() != nil {
			log.Fatalf("epochdb: --tip-override %s: no peer served this container in %s. Is it an ID from a DIFFERENT NETWORK than --network?", id, tipAnchorTimeout)
		}
		log.Fatalf("epochdb: --tip-override: %v", err)
	}
	anchors := []fetch.Anchor{tip}
	if len(f.Checkpoints()) > 0 {
		cps, err := f.ResolveCheckpoints(ctx)
		if err != nil {
			log.Fatalf("epochdb: --tip-override: %v", err)
		}
		for _, cp := range cps {
			if cp.Height > 0 && cp.Height < tip.Height {
				anchors = append(anchors, cp)
			}
		}
	}
	log.Printf("fetch: tip-override %s at height %d, %d seeds below", tip.ID, tip.Height, len(anchors)-1)
	return anchors
}

// execNetID maps --network to a network ID.
func execNetID(network string) uint32 {
	id, _, _ := netParams(network)
	return id
}

func fetchMain(args []string) {
	fs := flag.NewFlagSet("fetch", flag.ExitOnError)
	dataDir := fs.String("data", "./data", "directory for the segment files")
	network, resolveChain := chainFlags(fs)
	nodeURI := fs.String("node", "", "bootstrap RPC node URI (default per --network)")
	walks := fs.Int("walks", 16, "concurrent backward walks")
	perPeer := fs.Int("per-peer", 1, "max outstanding requests per archival peer")
	tip := fs.String("tip", "", "walk down from this container ID instead of the embedded checkpoints (cb58, or 0x-hex eth block hash for pre-ProposerVM blocks)")
	fromTip := fs.Bool("from-tip", false, "anchor at the network's accepted frontier, backfill down to stored history, then keep following the live tip")
	tipOverride := fs.String("tip-override", "", "fixed corpus ceiling replacing frontier following: a CONTAINER ID (cb58, or 0x-hex eth block hash for pre-ProposerVM blocks); backfills [0..that block] using the embedded checkpoints below it as parallel seeds, then exits")
	follow := fs.Bool("follow", false, "consensus-verified tip following: real snowman polls against the weighted validator set (replaces --from-tip's frontier voting)")
	vdrSources := fs.String("vdr-sources", "", "comma-separated platform RPC URIs for the cross-checked validator set (--follow); default: --node URI only, with a warning")
	fs.Parse(args)
	defer mustLockDataDir("fetch", *dataDir)()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Bad flag values die before anything dials.
	var overrideID ids.ID
	if *tipOverride != "" {
		var err error
		if overrideID, err = parseTipOverride(*tipOverride); err != nil {
			log.Fatalf("epochdb: --tip-override: %v", err)
		}
	}

	// The descriptor comes FIRST, and for a cached L1 it, not --network, names
	// the network to dial: the cache is what the data dir was built with.
	c := resolveChain(*dataDir)
	if c.SubnetID != avaconstants.PrimaryNetworkID {
		*network = avaconstants.NetworkIDToNetworkName[c.NetworkID]
	}
	_, defNode, _ := netParams(*network)
	if *nodeURI == "" {
		*nodeURI = defNode
	}

	cfg := fetch.Config{DataDir: *dataDir, NodeURI: *nodeURI, PerPeer: *perPeer, Chain: c}
	if *vdrSources != "" {
		cfg.VdrSources = strings.Split(*vdrSources, ",")
	}
	f, err := fetch.New(cfg)
	if err != nil {
		log.Fatalf("epochdb: %v", err)
	}
	if floor := flushedFloor2(*dataDir, c); floor > 0 {
		// Nothing flushes in this process, so the startup value is the whole
		// story here: below it the staging is gone and the runs answer.
		f.SetFloor(floor)
		log.Printf("epochdb: flushed through %d, backfilling only above it", floor)
	}

	done := make(chan error, 1)
	switch {
	case *follow:
		go func() { done <- f.Follow(ctx) }()
	case *tipOverride != "":
		anchors := resolveTipOverride(ctx, f, overrideID)
		go func() { done <- f.SyncTo(ctx, anchors, *walks) }()
	case *fromTip:
		go func() { done <- f.FollowTip(ctx) }()
	case *tip != "":
		id, err := parseContainerID(*tip)
		if err != nil {
			log.Fatalf("epochdb: --tip: %v", err)
		}
		go func() { done <- f.WalkFrom(ctx, id) }()
	default:
		go func() { done <- f.Sync(ctx, *walks) }()
	}

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	prev := f.Progress()
	prevT := time.Now()
	for {
		select {
		case err := <-done:
			// Close flushes the store before releasing it.
			if cerr := f.Close(); cerr != nil {
				log.Printf("epochdb: close: %v", cerr)
			}
			if err != nil && !errors.Is(err, context.Canceled) {
				log.Fatalf("epochdb: sync: %v", err)
			}
			if err != nil {
				log.Printf("epochdb: interrupted, state flushed")
			} else {
				log.Printf("epochdb: sync complete")
			}
			return
		case <-ticker.C:
			cur := f.Progress()
			now := time.Now()
			dt := now.Sub(prevT).Seconds()
			rate := float64(cur.Stored-prev.Stored) / dt
			var pct float64
			if da := cur.Answers - prev.Answers; da > 0 {
				pct = 100 * float64(cur.NonEmpty-prev.NonEmpty) / float64(da)
			}
			gap := ""
			if *fromTip {
				if missing := int64(cur.Head) + 1 - int64(cur.Stored); missing > 0 {
					gap = fmt.Sprintf(" gap=%d", missing)
				}
			}
			log.Printf("epochdb: stored=%d rate=%.0f blk/s written=%.1f MB raw=%.1f MB walks=%d archival=%d inflight=%d answers_nonempty=%.0f%%%s%s",
				cur.Stored, rate, float64(cur.SessionBytes)/1e6, float64(cur.SessionRaw)/1e6, cur.ActiveWalks, cur.Archival, cur.InFlight, pct, gap, cur.DropSuffix())
			prev, prevT = cur, now
		}
	}
}

// flushedFloor2 reads the last flushed run boundary without opening the writer
// side of the store: `fetch` runs beside an executor, not instead of one.
func flushedFloor2(dataDir string, c *chain.Chain) uint64 {
	cas, err := dist.Local(dataDir)
	if err != nil {
		return 0
	}
	defer cas.Close()
	db, err := store.Open(dataDir, cas, c.Root())
	if err != nil {
		return 0
	}
	defer db.Close()
	return db.FlushedFloor()
}

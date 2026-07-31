// Command epochdb runs the compact C-chain historical node.
package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/upgrade"
	avaconstants "github.com/ava-labs/avalanchego/utils/constants"
	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/common/hexutil"

	"github.com/containerman17/epochdb/chain"
	"github.com/containerman17/epochdb/dist"
	"github.com/containerman17/epochdb/exec"
	"github.com/containerman17/epochdb/fetch"
	"github.com/containerman17/epochdb/state"
	"github.com/containerman17/epochdb/verify"
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

// chainFlags registers --network and --chain on fs. --network names a primary
// network, whose C-chain descriptor comes from avalanchego's embedded config;
// --chain points at a descriptor JSON for an Avalanche L1 (see chain.Load) and
// wins when both are given. Returns the --network value (some commands still
// need it for the default node/archive URLs) and a resolver.
func chainFlags(fs *flag.FlagSet) (*string, func() *chain.Chain) {
	network := fs.String("network", "fuji", "network: fuji|mainnet (the primary network's C-chain)")
	path := fs.String("chain", "", "chain descriptor JSON for an Avalanche L1, instead of --network")
	return network, func() *chain.Chain {
		if *path == "" {
			c, err := chain.CChain(execNetID(*network))
			if err != nil {
				log.Fatalf("epochdb: --network: %v", err)
			}
			return c
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		c, err := chain.Load(ctx, *path)
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

// rpcBlockHash resolves a block number to its hash via the archive RPC.
func rpcBlockHash(rpcURL string, height uint64) (common.Hash, error) {
	res, err := rpcHeaderCall(rpcURL, "eth_getBlockByNumber", fmt.Sprintf("%q", hexutil.EncodeUint64(height)))
	if err != nil {
		return common.Hash{}, err
	}
	if res == nil || res.Hash == "" {
		return common.Hash{}, fmt.Errorf("block %d not found via %s", height, rpcURL)
	}
	return common.HexToHash(res.Hash), nil
}

// rpcBlockNumberByHash resolves a block hash to its height via the archive
// RPC. ok=false when the chain does not know the hash (e.g. the hash is a
// post-ProposerVM container ID, not an eth block hash).
func rpcBlockNumberByHash(rpcURL string, h common.Hash) (uint64, bool, error) {
	res, err := rpcHeaderCall(rpcURL, "eth_getBlockByHash", fmt.Sprintf("%q", h.Hex()))
	if err != nil {
		return 0, false, err
	}
	if res == nil || res.Number == "" {
		return 0, false, nil
	}
	n, err := hexutil.DecodeUint64(res.Number)
	return n, err == nil, err
}

type rpcBlockHeader struct {
	Hash      string `json:"hash"`
	Number    string `json:"number"`
	Timestamp string `json:"timestamp"`
}

func rpcHeaderCall(rpcURL, method, param string) (*rpcBlockHeader, error) {
	body := fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":"%s","params":[%s,false]}`, method, param)
	resp, err := http.Post(rpcURL, "application/json", strings.NewReader(body))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var out struct {
		Result *rpcBlockHeader `json:"result"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	if out.Result == nil {
		return nil, nil
	}
	return out.Result, nil
}

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	switch os.Args[1] {
	case "fetch":
		fetchMain(os.Args[2:])
	case "exec":
		execMain(os.Args[2:])
	case "cook-index":
		cookMain(os.Args[2:])
	case "cook-txindex":
		cookTxMain(os.Args[2:])
	case "serve":
		serveMain(os.Args[2:])
	case "ab-bench":
		benchMain(os.Args[2:])
	case "ab-bench-tx":
		benchTxMain(os.Args[2:])
	case "ab-bench-rpc":
		benchRPCMain(os.Args[2:])
	case "ab-bench-logs":
		benchLogsMain(os.Args[2:])
	case "rpc-bench":
		rpcBenchMain(os.Args[2:])
	case "seal":
		sealMain(os.Args[2:])
	case "bootstrap":
		bootstrapMain(os.Args[2:])
	case "verify":
		verifyMain(os.Args[2:])
	case "backfill-logs":
		backfillLogsMain(os.Args[2:])
	case "verify-logs":
		verifyLogsMain(os.Args[2:])
	default:
		usage()
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: epochdb fetch [--data <dir>] [--node <uri>] [--tip <containerID>]")
	fmt.Fprintln(os.Stderr, "       epochdb exec  [--data <dir>]")
	fmt.Fprintln(os.Stderr, "       epochdb cook-index [--data <dir>]")
	fmt.Fprintln(os.Stderr, "       epochdb cook-txindex [--data <dir>]")
	fmt.Fprintln(os.Stderr, "       epochdb serve [--data <dir>] [--port 9650] [--vdr-sources <p-chain rpcs>] [--tip-override N]")
	fmt.Fprintln(os.Stderr, "       epochdb ab-bench [--data <dir>] [--local <url>] [--remote <url>] [--n 1000]")
	fmt.Fprintln(os.Stderr, "       epochdb ab-bench-tx [--data <dir>] [--local <url>] [--remote <url>] [--n 600]")
	fmt.Fprintln(os.Stderr, "       epochdb ab-bench-rpc [--local <url>] [--remote <url>] [--n 300]")
	fmt.Fprintln(os.Stderr, "       epochdb ab-bench-logs [--data <dir>] [--local <url>] [--remote <url>] [--n 120]")
	fmt.Fprintln(os.Stderr, "       epochdb seal [--data <dir>] [--out <dir>] [--network mainnet | --chain <path.json>] [--epoch-txs <n>]")
	fmt.Fprintln(os.Stderr, "       epochdb bootstrap [--data <dir>] [--network mainnet | --chain <path.json>] [--frontier] [--verify]")
	fmt.Fprintln(os.Stderr, "       epochdb verify [--data <dir>] [--network mainnet | --chain <path.json>] [--workers N]")
	fmt.Fprintln(os.Stderr, "       epochdb backfill-logs [--data <dir>] [--workers 12]")
	fmt.Fprintln(os.Stderr, "       epochdb verify-logs [--data <dir>] [--remote <url>] [--n 300] [--parity 50]")
	os.Exit(2)
}

// cookMain external-sorts the writelog buckets into sorted_NNNNN.idx.
func cookMain(args []string) {
	fs := flag.NewFlagSet("cook-index", flag.ExitOnError)
	dataDir := fs.String("data", "./data", "shared data directory")
	fs.Parse(args)
	if err := state.CookIndex(*dataDir); err != nil {
		log.Fatalf("epochdb: cook-index: %v", err)
	}
}

// sealMain cuts sealed epoch files from the raw staging + capture files,
// strictly behind the exec head, then deletes every fully sealed raw bucket
// (unconditional since 2026-07-29). NEVER run it beside a live fetch/exec.
func sealMain(args []string) {
	fs := flag.NewFlagSet("seal", flag.ExitOnError)
	dataDir := fs.String("data", "./data", "shared data directory")
	outDir := fs.String("out", "", "directory the sealed epochs are published into (default --data; a separate dir cuts an alternate epoch size from the same raws)")
	_, resolveChain := chainFlags(fs)
	epochTxs := fs.Uint64("epoch-txs", state.EpochTxs, "epoch boundary tx count override")
	fs.Parse(args)
	if *outDir == "" {
		*outDir = *dataDir
	} else if err := os.MkdirAll(*outDir, 0o755); err != nil {
		log.Fatalf("epochdb: seal: %v", err)
	}
	c := resolveChain()
	fetch.RegisterExtras(c.VMKind)

	// Still no overlay and no EVM: the stored-logs/receipt sections are a byte
	// copy of the executor's live capture, so seal never re-executes (the
	// --workers flag went with the DeriveStored stage, 2026-07-29). The chain
	// descriptor is back only for the chain root the first epoch's footer
	// links to.
	chainRoot := c.Root()
	out, err := dist.Open(*outDir)
	if err != nil {
		log.Fatalf("epochdb: seal: %v", err)
	}
	// Close marks the chunk cache clean, so the next process starts warm.
	defer out.Close()
	if err := state.SealEpochs(*dataDir, out, *epochTxs, chainRoot); err != nil {
		log.Fatalf("epochdb: seal: %v", err)
	}
}

// cookTxMain builds the per-bucket tx-hash indexes over the staging
// segments. Read-only on the staging files, safe next to running processes.
func cookTxMain(args []string) {
	fs := flag.NewFlagSet("cook-txindex", flag.ExitOnError)
	dataDir := fs.String("data", "./data", "shared data directory")
	fs.Parse(args)
	if err := state.CookTxIndex(*dataDir); err != nil {
		log.Fatalf("epochdb: cook-txindex: %v", err)
	}
}

// bootstrapMain learns a chain's history from the `latest` pointer: walk the
// epoch hash chain backward, write the local index, done. Nothing is
// downloaded eagerly (with credentials the node reads history lazily; without
// them the artifacts are already in the spool). Operator flow on a new
// machine: bootstrap --frontier, then serve.
func bootstrapMain(args []string) {
	fs := flag.NewFlagSet("bootstrap", flag.ExitOnError)
	dataDir := fs.String("data", "./data", "shared data directory")
	_, resolveChain := chainFlags(fs)
	frontier := fs.Bool("frontier", false, "after the walk, MERGE the epochs into a Firewood state frontier at the last sealed block, so serve can start executing at H+1 (reads every epoch's SST section once)")
	doVerify := fs.Bool("verify", false, "after the walk, run the no-execution verification over the whole indexed set (pulls every byte)")
	workers := fs.Int("workers", 0, "body/receipt verification workers (0 = GOMAXPROCS)")
	fs.Parse(args)
	c := resolveChain()
	fetch.RegisterExtras(c.VMKind)

	chainRoot := c.Root()
	st, err := dist.Open(*dataDir)
	if err != nil {
		log.Fatalf("epochdb: bootstrap: %v", err)
	}
	defer st.Close()
	epochs, err := bootstrapChain(st, chainRoot)
	if err != nil {
		log.Fatalf("epochdb: bootstrap: %v", err)
	}
	log.Printf("bootstrap: %d epochs indexed, chain rooted at %x", epochs, chainRoot[:8])
	if *frontier {
		buildFrontier(*dataDir, st, c)
	}
	if !*doVerify {
		return
	}
	tmp, err := os.MkdirTemp(*dataDir, ".verify-")
	if err != nil {
		log.Fatalf("epochdb: bootstrap: %v", err)
	}
	defer os.RemoveAll(tmp)
	blocks, wall, err := verify.VerifySet(st, tmp, c, *workers)
	if err != nil {
		os.RemoveAll(tmp)
		log.Fatalf("epochdb: bootstrap: verify FAIL after %d blocks in %s: %v", blocks, wall.Round(time.Second), err)
	}
	log.Printf("bootstrap: %d epochs downloaded and verified (%d blocks in %s)", epochs, blocks, wall.Round(time.Second))
}

// verifyMain runs the no-execution verification over an already-downloaded
// epoch set: diff-applied state roots, txRoot, reconstructed receiptsRoot,
// and the header parent-hash chain, per block from genesis.
func verifyMain(args []string) {
	fs := flag.NewFlagSet("verify", flag.ExitOnError)
	dataDir := fs.String("data", "./data", "directory with the sealed epochs")
	_, resolveChain := chainFlags(fs)
	workers := fs.Int("workers", 0, "body/receipt verification workers (0 = GOMAXPROCS)")
	fs.Parse(args)
	c := resolveChain()

	tmp, err := os.MkdirTemp(*dataDir, ".verify-")
	if err != nil {
		log.Fatalf("epochdb: verify: %v", err)
	}
	defer os.RemoveAll(tmp)
	st, err := dist.Open(*dataDir)
	if err != nil {
		log.Fatalf("epochdb: verify: %v", err)
	}
	defer st.Close()
	blocks, wall, err := verify.VerifySet(st, tmp, c, *workers)
	if err != nil {
		os.RemoveAll(tmp)
		log.Fatalf("epochdb: verify: FAIL after %d blocks in %s: %v", blocks, wall.Round(time.Second), err)
	}
	log.Printf("verify: PASS %d blocks in %s (%.0f blk/s)", blocks, wall.Round(time.Second), float64(blocks)/wall.Seconds())
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

	store, err := state.Open(*dataDir)
	if err != nil {
		log.Fatalf("epochdb: open state layer: %v", err)
	}
	defer store.Close()

	e, err := exec.New(exec.Config{
		DataDir:         *dataDir,
		Blocks:          reader,
		Store:           store,
		StateCacheBytes: uint64(*stateCacheGiB) << 30,
		VerifyCache:     *verifyCache,
		CommitEvery:     *commitEvery,
		Chain:           resolveChain(),
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

// resolveTipOverride turns a --tip-override value (block height or
// container ID) into SyncTo anchors: the tip itself plus every embedded
// checkpoint at or below its height. Heights resolve via the archive RPC;
// pre-ProposerVM container IDs equal the eth block hash, so checkpoint
// heights resolve the same way (post-ProposerVM checkpoints don't resolve
// and are skipped, they sit above any pre-fork override by construction).
func resolveTipOverride(ctx context.Context, f *fetch.Fetcher, rpcURL, v string, networkID uint32) []fetch.Anchor {
	ceiling := preForkCeiling(rpcURL, networkID)

	var (
		tipID     ids.ID
		tipHeight uint64
		anchors   []fetch.Anchor
		seen      = map[uint64]bool{}
	)
	if height, herr := strconv.ParseUint(v, 10, 64); herr == nil && height >= ceiling {
		// Post-ProposerVM target: heights don't RPC-resolve to container
		// IDs. Anchor at the nearest embedded checkpoint at/above the
		// target (blocks between target and checkpoint stage as
		// disposable extra; exec --stop caps the corpus).
		cps, err := f.ResolveCheckpoints(ctx)
		if err != nil {
			log.Fatalf("epochdb: --tip-override: %v", err)
		}
		for _, cp := range cps {
			switch {
			case cp.Height >= height && tipHeight == 0:
				tipID, tipHeight = cp.ID, cp.Height
				anchors = append(anchors, cp)
				seen[cp.Height] = true
			case cp.Height < height && cp.Height > 0 && !seen[cp.Height]:
				anchors = append(anchors, cp)
				seen[cp.Height] = true
			}
		}
		if tipHeight == 0 {
			log.Fatalf("epochdb: --tip-override: no embedded checkpoint at/above %d", height)
		}
		log.Printf("fetch: tip-override %d is post-ProposerVM (ceiling %d): anchored at checkpoint %s height %d",
			height, ceiling, tipID, tipHeight)
	} else {
		if herr == nil {
			h, err := rpcBlockHash(rpcURL, height)
			if err != nil {
				log.Fatalf("epochdb: --tip-override: resolve height %d: %v", height, err)
			}
			tipID = ids.ID(h)
			log.Printf("fetch: tip-override height %d -> container %s", height, tipID)
		} else {
			var err error
			if tipID, err = parseContainerID(v); err != nil {
				log.Fatalf("epochdb: --tip-override: %v", err)
			}
		}
		var ok bool
		var err error
		tipHeight, ok, err = rpcBlockNumberByHash(rpcURL, common.Hash(tipID))
		if err != nil || !ok {
			log.Fatalf("epochdb: --tip-override: cannot determine height of %s via %s (post-ProposerVM container? use --tip): ok=%v err=%v", tipID, rpcURL, ok, err)
		}
		anchors = append(anchors, fetch.Anchor{ID: tipID, Height: tipHeight})
		seen[tipHeight] = true
		for _, cp := range f.Checkpoints() {
			h, ok, err := rpcBlockNumberByHash(rpcURL, common.Hash(cp))
			if err != nil || !ok || h > tipHeight || h == 0 || seen[h] {
				continue // post-ProposerVM, above the override, or genesis
			}
			seen[h] = true
			anchors = append(anchors, fetch.Anchor{ID: cp, Height: h})
		}
	}

	// The walks are round-trip-latency-bound, so synthesize evenly spaced
	// pre-fork anchors (below the ProposerVM ceiling every height
	// RPC-resolves to a container ID for free).
	const wantAnchors = 24
	if fillTop := min(tipHeight, ceiling); len(anchors) < wantAnchors {
		step := fillTop / wantAnchors
		for h := step; h < fillTop && step > 0; h += step {
			if seen[h] {
				continue
			}
			bh, err := rpcBlockHash(rpcURL, h)
			if err != nil {
				log.Fatalf("epochdb: --tip-override: resolve filler anchor %d: %v", h, err)
			}
			seen[h] = true
			anchors = append(anchors, fetch.Anchor{ID: ids.ID(bh), Height: h})
		}
	}
	log.Printf("fetch: tip-override %s at height %d, %d seeds below", tipID, tipHeight, len(anchors)-1)
	return anchors
}

// preForkCeiling binary-searches the archive for the first block at/after
// the network's ApricotPhase4 activation (ProposerVM starts there):
// below it, container ID == eth block hash.
func preForkCeiling(rpcURL string, networkID uint32) uint64 {
	ap4 := uint64(upgrade.GetConfig(networkID).ApricotPhase4Time.Unix())
	// Find an upper bound: double until the block is missing (beyond
	// head) or its timestamp reaches AP4.
	lo, hi := uint64(1), uint64(0)
	for probe := uint64(1 << 20); ; probe <<= 1 {
		ts, ok := rpcBlockTime(rpcURL, probe)
		if !ok || ts >= ap4 {
			hi = probe
			break
		}
		lo = probe
	}
	for lo < hi {
		mid := (lo + hi) / 2
		ts, ok := rpcBlockTime(rpcURL, mid)
		if ok && ts >= ap4 {
			hi = mid
		} else {
			lo = mid + 1
		}
	}
	return lo
}

func rpcBlockTime(rpcURL string, height uint64) (uint64, bool) {
	res, err := rpcHeaderCall(rpcURL, "eth_getBlockByNumber", fmt.Sprintf("%q", hexutil.EncodeUint64(height)))
	if err != nil || res == nil || res.Timestamp == "" {
		return 0, false
	}
	ts, err := hexutil.DecodeUint64(res.Timestamp)
	return ts, err == nil
}

// execNetID maps --network to a network ID.
func execNetID(network string) uint32 {
	id, _, _ := netParams(network)
	return id
}

func fetchMain(args []string) {
	fs := flag.NewFlagSet("fetch", flag.ExitOnError)
	dataDir := fs.String("data", "./data", "directory for the segment files")
	network := fs.String("network", "fuji", "network: fuji|mainnet (sets default node URI)")
	nodeURI := fs.String("node", "", "bootstrap RPC node URI (default per --network)")
	walks := fs.Int("walks", 16, "concurrent backward walks")
	perPeer := fs.Int("per-peer", 1, "max outstanding requests per archival peer")
	tip := fs.String("tip", "", "walk down from this container ID instead of the embedded checkpoints (cb58, or 0x-hex eth block hash for pre-ProposerVM blocks)")
	fromTip := fs.Bool("from-tip", false, "anchor at the network's accepted frontier, backfill down to stored history, then keep following the live tip")
	tipOverride := fs.String("tip-override", "", "fixed corpus ceiling replacing frontier following: a block HEIGHT (RPC-resolved; pre-ProposerVM only) or a container ID; backfills [0..height] using checkpoints at or below it as parallel seeds, then exits")
	follow := fs.Bool("follow", false, "consensus-verified tip following: real snowman polls against the weighted validator set (replaces --from-tip's frontier voting)")
	vdrSources := fs.String("vdr-sources", "", "comma-separated platform RPC URIs for the cross-checked validator set (--follow); default: --node URI only, with a warning")
	fs.Parse(args)

	_, defNode, rpcURL := netParams(*network)
	if *nodeURI == "" {
		*nodeURI = defNode
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg := fetch.Config{DataDir: *dataDir, NodeURI: *nodeURI, PerPeer: *perPeer}
	if *vdrSources != "" {
		cfg.VdrSources = strings.Split(*vdrSources, ",")
	}
	f, err := fetch.New(cfg)
	if err != nil {
		log.Fatalf("epochdb: %v", err)
	}

	done := make(chan error, 1)
	switch {
	case *follow:
		go func() { done <- f.Follow(ctx) }()
	case *tipOverride != "":
		anchors := resolveTipOverride(ctx, f, rpcURL, *tipOverride, execNetID(*network))
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
			log.Printf("epochdb: stored=%d rate=%.0f blk/s written=%.1f MB raw=%.1f MB walks=%d archival=%d inflight=%d answers_nonempty=%.0f%%%s",
				cur.Stored, rate, float64(cur.SessionBytes)/1e6, float64(cur.SessionRaw)/1e6, cur.ActiveWalks, cur.Archival, cur.InFlight, pct, gap)
			prev, prevT = cur, now
		}
	}
}

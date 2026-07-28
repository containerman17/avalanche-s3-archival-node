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
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/upgrade"
	avaconstants "github.com/ava-labs/avalanchego/utils/constants"
	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/common/hexutil"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/rlp"

	"github.com/containerman17/epochdb/dist"
	"github.com/containerman17/epochdb/exec"
	"github.com/containerman17/epochdb/fetch"
	"github.com/containerman17/epochdb/rpc"
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
	case "manifest":
		manifestMain(os.Args[2:])
	case "seed":
		seedMain(os.Args[2:])
	case "bootstrap":
		bootstrapMain(os.Args[2:])
	case "verify":
		verifyMain(os.Args[2:])
	case "snapshot":
		snapshotMain(os.Args[2:])
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
	fmt.Fprintln(os.Stderr, "       epochdb serve [--data <dir>] [--port 9650]")
	fmt.Fprintln(os.Stderr, "       epochdb ab-bench [--data <dir>] [--local <url>] [--remote <url>] [--n 1000] [--floor N]")
	fmt.Fprintln(os.Stderr, "       epochdb ab-bench-tx [--data <dir>] [--local <url>] [--remote <url>] [--n 600] [--floor N]")
	fmt.Fprintln(os.Stderr, "       epochdb ab-bench-rpc [--local <url>] [--remote <url>] [--n 300] [--floor N]")
	fmt.Fprintln(os.Stderr, "       epochdb ab-bench-logs [--data <dir>] [--local <url>] [--remote <url>] [--n 120] [--floor N]")
	fmt.Fprintln(os.Stderr, "       epochdb seal [--data <dir>] [--out <dir>] [--epoch-txs <n>] [--delete-raw]")
	fmt.Fprintln(os.Stderr, "       epochdb manifest [--data <dir>] [--out <file>]")
	fmt.Fprintln(os.Stderr, "       epochdb seed [--data <dir>] [--manifest <file>] [--listen-port 42069]")
	fmt.Fprintln(os.Stderr, "       epochdb bootstrap [--data <dir>] [--manifest <file>] [--rounds 20] [--verify] [--network mainnet]")
	fmt.Fprintln(os.Stderr, "       epochdb verify [--data <dir>] [--network mainnet] [--workers N]")
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
// strictly behind the exec head.
func sealMain(args []string) {
	fs := flag.NewFlagSet("seal", flag.ExitOnError)
	dataDir := fs.String("data", "./data", "shared data directory")
	deleteRaw := fs.Bool("delete-raw", false, "remove fully sealed raw buckets afterwards (NEVER next to a running fetch/exec)")
	network := fs.String("network", "fuji", "network: fuji|mainnet")
	workers := fs.Int("workers", 12, "re-execution workers for the stored-logs derivation")
	outDir := fs.String("out", "", "directory for the sealed epoch files (default --data; a separate dir cuts an alternate epoch size from the same raws)")
	epochTxs := fs.Uint64("epoch-txs", state.EpochTxs, "epoch boundary tx count override")
	fs.Parse(args)
	if *outDir == "" {
		*outDir = *dataDir
	} else if err := os.MkdirAll(*outDir, 0o755); err != nil {
		log.Fatalf("epochdb: seal: %v", err)
	}
	fetch.RegisterExtras()

	// Stored logs are unconditional (user ruling 2026-07-20): every seal
	// derives the sections, and v1 epochs are upgraded in place first.
	g, err := exec.NetworkGenesis(execNetID(*network))
	if err != nil {
		log.Fatalf("epochdb: seal: genesis: %v", err)
	}
	store, err := state.OpenReadOnly(*dataDir)
	if err != nil {
		log.Fatalf("epochdb: seal: %v", err)
	}
	defer store.Close()
	hist, err := state.OpenHistory(*dataDir, store, g.Alloc)
	if err != nil {
		log.Fatalf("epochdb: seal: %v", err)
	}
	defer hist.Close()
	derive := rpc.NewDeriveStored(hist, rpc.HistoryChainContext(hist), g.Config, exec.ParseEthBlock, *workers)
	if err := state.ReSealStoredLogs(*outDir, derive); err != nil {
		log.Fatalf("epochdb: reseal: %v", err)
	}
	if err := state.SealEpochs(*dataDir, *outDir, *deleteRaw, *epochTxs, derive); err != nil {
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

// manifestMain hashes every sealed epoch into the shipped manifest:
// name, size, sha256, v1 torrent infohash. Epochs are bit-identical
// across builders, so these are stable facts.
func manifestMain(args []string) {
	fs := flag.NewFlagSet("manifest", flag.ExitOnError)
	dataDir := fs.String("data", "./data", "shared data directory")
	out := fs.String("out", "", "output path (default <data>/epochs.manifest)")
	fs.Parse(args)
	if *out == "" {
		*out = filepath.Join(*dataDir, "epochs.manifest")
	}
	entries, err := dist.BuildManifest(*dataDir)
	if err != nil {
		log.Fatalf("epochdb: manifest: %v", err)
	}
	if err := dist.WriteManifest(*out, entries); err != nil {
		log.Fatalf("epochdb: manifest: %v", err)
	}
	var total int64
	for _, e := range entries {
		total += e.Size
	}
	log.Printf("manifest: %d epochs, %.1f GB, wrote %s", len(entries), float64(total)/1e9, *out)
}

// seedMain seeds every local epoch matching the manifest until killed.
func seedMain(args []string) {
	fs := flag.NewFlagSet("seed", flag.ExitOnError)
	dataDir := fs.String("data", "./data", "shared data directory")
	manifestPath := fs.String("manifest", "", "epoch manifest path (default <data>/epochs.manifest)")
	listenPort := fs.Int("listen-port", 42069, "BitTorrent listen port")
	noDHT := fs.Bool("no-dht", false, "disable DHT+trackers (tests)")
	fs.Parse(args)
	if *manifestPath == "" {
		*manifestPath = filepath.Join(*dataDir, "epochs.manifest")
	}
	if err := dist.Seed(*dataDir, *manifestPath, dist.ClientOpts{
		ListenPort: *listenPort, NoDHT: *noDHT, NoTrackers: *noDHT, NoUpnp: *noDHT, DisableIPv6: *noDHT,
	}); err != nil {
		log.Fatalf("epochdb: seed: %v", err)
	}
}

// bootstrapMain leeches until the local epoch set exactly matches the
// manifest (retrying in rounds, seeding completed epochs meanwhile), then
// exits 0. Operator flow on a new machine: bootstrap, then serve.
func bootstrapMain(args []string) {
	fs := flag.NewFlagSet("bootstrap", flag.ExitOnError)
	dataDir := fs.String("data", "./data", "shared data directory")
	manifestPath := fs.String("manifest", "", "epoch manifest path (default <data>/epochs.manifest)")
	epochTimeout := fs.Duration("epoch-timeout", 5*time.Minute, "per-epoch attempt timeout within a round")
	rounds := fs.Int("rounds", 20, "give up after this many retry rounds")
	peer := fs.String("peer", "", "explicit seeder host:port (tests, boost)")
	noDHT := fs.Bool("no-dht", false, "disable DHT+trackers (tests)")
	listenPort := fs.Int("listen-port", 0, "BitTorrent listen port (0 = random)")
	doVerify := fs.Bool("verify", false, "pipelined no-execution verification: verify the contiguous epoch prefix as downloads complete; exit 0 only when all manifest epochs are downloaded AND verified")
	network := fs.String("network", "fuji", "network: fuji|mainnet (--verify genesis anchor)")
	workers := fs.Int("workers", 0, "body/receipt verification workers (0 = GOMAXPROCS)")
	fs.Parse(args)
	if *manifestPath == "" {
		*manifestPath = filepath.Join(*dataDir, "epochs.manifest")
	}
	opts := dist.FetchOpts{
		Timeout: *epochTimeout,
		Peer:    *peer,
		Client:  dist.ClientOpts{ListenPort: *listenPort, NoDHT: *noDHT, NoTrackers: *noDHT, NoUpnp: *noDHT, DisableIPv6: *noDHT},
	}
	if !*doVerify {
		if err := dist.Bootstrap(*dataDir, *manifestPath, *rounds, opts); err != nil {
			log.Fatalf("epochdb: bootstrap: %v", err)
		}
		return
	}

	entries, err := dist.LoadManifest(*manifestPath)
	if err != nil {
		log.Fatalf("epochdb: bootstrap: %v", err)
	}
	tmp, err := os.MkdirTemp(*dataDir, ".verify-")
	if err != nil {
		log.Fatalf("epochdb: bootstrap: %v", err)
	}
	defer os.RemoveAll(tmp)
	cur := verify.StartCursor(*dataDir, tmp, execNetID(*network), *workers, len(entries))
	opts.OnEpoch = cur.Notify
	bootErr := make(chan error, 1)
	go func() { bootErr <- dist.Bootstrap(*dataDir, *manifestPath, *rounds, opts) }()
	bootDone, verDone := false, false
	for !bootDone || !verDone {
		select {
		case err := <-bootErr:
			if err != nil {
				os.RemoveAll(tmp)
				log.Fatalf("epochdb: bootstrap: %v", err)
			}
			bootDone = true
		case err := <-cur.Done():
			if err != nil {
				os.RemoveAll(tmp)
				log.Fatalf("epochdb: bootstrap: %v", err)
			}
			verDone = true
		}
	}
	log.Printf("bootstrap: all %d epochs downloaded and verified", len(entries))
}

// verifyMain runs the no-execution verification over an already-downloaded
// epoch set: diff-applied state roots, txRoot, reconstructed receiptsRoot,
// and the header parent-hash chain, per block from genesis.
func verifyMain(args []string) {
	fs := flag.NewFlagSet("verify", flag.ExitOnError)
	dataDir := fs.String("data", "./data", "directory with the sealed epochs")
	network := fs.String("network", "fuji", "network: fuji|mainnet (genesis anchor)")
	workers := fs.Int("workers", 0, "body/receipt verification workers (0 = GOMAXPROCS)")
	fs.Parse(args)

	tmp, err := os.MkdirTemp(*dataDir, ".verify-")
	if err != nil {
		log.Fatalf("epochdb: verify: %v", err)
	}
	defer os.RemoveAll(tmp)
	blocks, wall, err := verify.VerifySet(*dataDir, tmp, execNetID(*network), *workers)
	if err != nil {
		os.RemoveAll(tmp)
		log.Fatalf("epochdb: verify: FAIL after %d blocks in %s: %v", blocks, wall.Round(time.Second), err)
	}
	log.Printf("verify: PASS %d blocks in %s (%.0f blk/s)", blocks, wall.Round(time.Second), float64(blocks)/wall.Seconds())
}

// serveMain serves historical JSON-RPC reads over the cooked indexes. The
// state layer is opened read-only, so it is safe next to a running
// fetch/exec; the view is pinned to what was durable at startup.
func serveMain(args []string) {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	dataDir := fs.String("data", "./data", "shared data directory")
	port := fs.Int("port", 9650, "HTTP listen port")
	network := fs.String("network", "fuji", "network: fuji|mainnet")
	fetchEpochs := fs.Bool("fetch-epochs", false, "at startup, torrent-leech manifest epochs missing from --data")
	manifestPath := fs.String("manifest", "", "epoch manifest path (default <data>/epochs.manifest)")
	epochTimeout := fs.Duration("epoch-timeout", 5*time.Minute, "per-epoch give-up when leeching")
	fetchPeer := fs.String("fetch-peer", "", "explicit seeder host:port (tests, boost)")
	fetchMax := fs.Int("fetch-max", 0, "leech at most N missing epochs (0 = all; tests)")
	fetchNoDHT := fs.Bool("fetch-no-dht", false, "disable DHT+trackers when leeching (tests: --fetch-peer only)")
	fs.Parse(args)

	if *fetchEpochs {
		mp := *manifestPath
		if mp == "" {
			mp = filepath.Join(*dataDir, "epochs.manifest")
		}
		still, err := dist.FetchMissing(*dataDir, mp, dist.FetchOpts{
			Timeout: *epochTimeout,
			Peer:    *fetchPeer,
			Max:     *fetchMax,
			Client:  dist.ClientOpts{NoDHT: *fetchNoDHT, NoTrackers: *fetchNoDHT, NoUpnp: *fetchNoDHT, DisableIPv6: *fetchNoDHT},
		})
		if err != nil {
			log.Fatalf("epochdb: fetch-epochs: %v", err)
		}
		if len(still) > 0 {
			log.Printf("epochdb: fetch-epochs: still missing after pass: %v", still)
			log.Printf("epochdb: serving the contiguous prefix; state/receipt/log queries above the first gap return a missing-epoch error (bodies/tx-by-hash above the gap remain epoch-local and may serve)")
		}
	}

	store, err := state.OpenReadOnly(*dataDir)
	if err != nil {
		log.Fatalf("epochdb: open state layer: %v", err)
	}
	defer store.Close()

	g, err := exec.NetworkGenesis(execNetID(*network))
	if err != nil {
		log.Fatalf("epochdb: genesis: %v", err)
	}
	hist, err := state.OpenHistory(*dataDir, store, g.Alloc)
	if err != nil {
		log.Fatalf("epochdb: open history: %v", err)
	}
	defer hist.Close()

	// Stored logs are unconditional (user ruling 2026-07-20): a sealed
	// epoch without the sections is an unsupported format.
	for _, e := range hist.Epochs().Epochs {
		if !e.HasStoredLogs() {
			log.Fatalf("epochdb: epoch %s is unsupported format v1 (no stored-logs sections); upgrade it in place: epochdb seal --data %s --network %s", state.EpochFileName(e.Start, e.Count), *dataDir, *network)
		}
	}

	srv := rpc.NewServer(hist, rpc.HistoryChainContext(hist), g.Config)

	epochs := hist.Epochs()
	rawIdx, err := state.OpenTxIndex(*dataDir)
	if err != nil {
		log.Printf("epochdb: raw tx index unavailable: %v", err)
	}
	if (rawIdx != nil && rawIdx.NumTx() > 0) || len(epochs.Epochs) > 0 {
		reader, err := fetch.OpenReader(*dataDir)
		if err != nil {
			log.Fatalf("epochdb: open staging reader: %v", err)
		}
		defer reader.Close()
		blocks := sealedBlocks{epochs: epochs, reader: reader}
		srv.EnableTxAPIs(state.CombinedTxIndex{Raw: rawIdx, Epochs: epochs}, blocks, exec.ParseEthBlock)
		var rawTx uint64
		if rawIdx != nil {
			rawTx = rawIdx.NumTx()
		}
		log.Printf("epochdb: tx lookup: %d raw-indexed txs, %d sealed epochs", rawTx, len(epochs.Epochs))
	}

	// eth block hash -> height for the *ByHash methods, from the staging
	// sidecars (~40B/block resident). After --delete-raw the sidecars only
	// cover the raw tail, so fill the sealed range from epoch-served
	// headers (one decode+hash pass at startup).
	if hashes, err := fetch.BlockHashes(*dataDir); err != nil {
		log.Printf("epochdb: block hash index unavailable: %v", err)
	} else {
		// Nothing below the floor exists, so the scan starts there.
		floor := hist.Floor()
		byHash := make(map[common.Hash]uint64, hist.Head()+1-floor)
		for h, n := range hashes {
			byHash[common.Hash(h)] = n
		}
		if uint64(len(byHash)) < hist.Head()+1-floor {
			filled := 0
			for n := floor; n <= hist.Head(); n++ {
				raw, ok, err := hist.HeaderRLP(n)
				if err != nil || !ok {
					continue
				}
				var h types.Header
				if rlp.DecodeBytes(raw, &h) == nil {
					if _, dup := byHash[h.Hash()]; !dup {
						byHash[h.Hash()] = n
						filled++
					}
				}
			}
			log.Printf("epochdb: block hash index: filled %d heights from sealed headers", filled)
		}
		srv.EnableBlockAPIs(byHash)
		log.Printf("epochdb: block hash index: %d blocks", len(byHash))
	}

	addr := fmt.Sprintf(":%d", *port)
	if floor := hist.Floor(); floor > 0 {
		log.Printf("epochdb: LIMITED HISTORY: floor=%d (base file), nothing below block %d is served", floor, floor)
	}
	log.Printf("epochdb: serving historical RPC on %s, head=%d floor=%d chainId=%s", addr, hist.Head(), hist.Floor(), g.Config.ChainID)
	log.Fatal(srv.ListenAndServe(addr))
}

// sealedBlocks serves containers from sealed epochs first, raw staging as
// the fallback for the unsealed tail.
type sealedBlocks struct {
	epochs *state.EpochSet
	reader *fetch.Reader
}

func (s sealedBlocks) GetByHeight(n uint64) ([]byte, bool, error) {
	if raw, ok, err := s.epochs.GetByHeight(n); ok || err != nil {
		return raw, ok, err
	}
	return s.reader.GetByHeight(n)
}

// execMain replays blocks ascending from genesis out of the (possibly
// still filling) staging dir, verifying every state root.
func execMain(args []string) {
	fs := flag.NewFlagSet("exec", flag.ExitOnError)
	dataDir := fs.String("data", "./data", "shared data directory (staging segments + state layer)")
	pprofAddr := fs.String("pprof", "", "serve net/http/pprof on this address (e.g. localhost:6060)")
	network := fs.String("network", "fuji", "network: fuji|mainnet")
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
		NetworkID:       execNetID(*network),
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

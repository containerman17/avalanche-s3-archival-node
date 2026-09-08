// epochdb-dump-fetch writes the containers of an L1's height window, fetched
// from its validators over p2p (the fetch package, exactly as epochdb-vm
// drives it), to one flat file, the block source of the Rust node:
//
//	[u64 LE height][u32 LE len][container bytes] ...
//
// heights ascending and contiguous. chain.json and upgrade.json (when the
// data dir holds one) are copied beside the dump. The forward fetch is
// anchored on the genesis block hash at height 1, or on --anchor (the eth
// hash of block --from minus one) for a later window.
package main

import (
	"bufio"
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/ava-labs/avalanchego/ids"
	avaconstants "github.com/ava-labs/avalanchego/utils/constants"
	"github.com/ava-labs/libevm/common"

	"github.com/containerman17/avalanche-s3-archival-node/chain"
	"github.com/containerman17/avalanche-s3-archival-node/dist"
	"github.com/containerman17/avalanche-s3-archival-node/fetch"
	"github.com/containerman17/avalanche-s3-archival-node/vmexec"
)

func main() {
	dataDir := flag.String("data", "./data", "data directory: chain.json cache, optional upgrade.json, staker key")
	network := flag.String("network", "mainnet", "network: fuji|mainnet")
	chainSpec := flag.String("chain", "", "the L1's blockchainID (subnet-evm only)")
	nodeURI := flag.String("node", "", "comma-separated bootstrap RPC node URIs")
	vdrSources := flag.String("vdr-sources", "", "comma-separated platform RPC URIs for the validator set")
	perPeer := flag.Int("per-peer", 1, "max outstanding requests per archival peer")
	p2pPort := flag.Int("p2p-port", 0, "listen for avalanchego peers on this port (0 disables)")
	from := flag.Uint64("from", 1, "first height")
	anchor := flag.String("anchor", "", "eth block hash of height --from minus one (required when --from > 1)")
	to := flag.Uint64("to", 0, "last height, inclusive")
	out := flag.String("out", "", "output file")
	flag.Parse()
	if *chainSpec == "" || *out == "" || *to < *from || *from == 0 {
		log.Fatal("dump-fetch: need --chain, --out, 1 <= --from <= --to")
	}
	var netID uint32
	var defaultNode string
	switch *network {
	case "fuji":
		netID, defaultNode = avaconstants.FujiID, "https://api.avax-test.network"
	case "mainnet":
		netID, defaultNode = avaconstants.MainnetID, "https://api.avax.network"
	default:
		log.Fatalf("dump-fetch: unknown --network %q", *network)
	}
	if *nodeURI == "" {
		*nodeURI = defaultNode
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	rctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	c, err := chain.Resolve(rctx, *chainSpec, netID, *dataDir, dist.Sources(*nodeURI)...)
	cancel()
	check(err)
	g, err := vmexec.ChainGenesis(c)
	check(err)
	anchorID := ids.ID(g.Hash)
	if *from > 1 {
		if *anchor == "" {
			log.Fatal("dump-fetch: --from > 1 needs --anchor")
		}
		anchorID = ids.ID(common.HexToHash(*anchor))
	}

	fetcher, err := fetch.New(fetch.Config{
		NodeURI: *nodeURI, PerPeer: *perPeer, Chain: c, VdrSources: dist.Sources(*vdrSources),
		ListenPort: *p2pPort, DataDir: *dataDir,
	})
	check(err)
	defer fetcher.Close()
	q := fetcher.StartForward(ctx, *from, anchorID)
	fetcher.SetCeiling(*to)
	go func() {
		if err := fetcher.Follow(ctx); err != nil && ctx.Err() == nil {
			log.Printf("dump-fetch: follower: %v", err)
		}
	}()

	for _, name := range []string{"chain.json", "upgrade.json"} {
		raw, err := os.ReadFile(filepath.Join(*dataDir, name))
		if err != nil {
			continue
		}
		check(os.WriteFile(filepath.Join(filepath.Dir(*out), name), raw, 0o644))
	}

	f, err := os.Create(*out + ".tmp")
	check(err)
	w := bufio.NewWriterSize(f, 4<<20)
	t0 := time.Now()
	var hdr [12]byte
	var bytes uint64
	for h := *from; h <= *to; h++ {
		raw, ok, err := q.GetByHeight(h)
		if err != nil || !ok {
			log.Fatalf("dump-fetch: height %d: ok=%v err=%v", h, ok, err)
		}
		binary.LittleEndian.PutUint64(hdr[:8], h)
		binary.LittleEndian.PutUint32(hdr[8:], uint32(len(raw)))
		_, err = w.Write(hdr[:])
		check(err)
		_, err = w.Write(raw)
		check(err)
		bytes += uint64(len(raw))
		if (h-*from+1)%10_000 == 0 {
			log.Printf("dump-fetch: height %d, %d MB, %.0f blk/s", h, bytes>>20, float64(h-*from+1)/time.Since(t0).Seconds())
		}
	}
	check(w.Flush())
	check(f.Close())
	check(os.Rename(*out+".tmp", *out))
	log.Printf("dump-fetch: %s: blocks [%d,%d], %d MB, %s", *out, *from, *to, bytes>>20, time.Since(t0).Round(time.Second))
	stop()
}

func check(err error) {
	if err != nil {
		log.Fatal(fmt.Errorf("dump-fetch: %w", err))
	}
}

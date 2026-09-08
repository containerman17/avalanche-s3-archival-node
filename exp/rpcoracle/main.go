// TEMPORARY differential-test harness (rs-rpc port of ots_/edb_): serves the
// Go rpc.Server over an existing storage dir, read-only, no executor.
package main

import (
	"context"
	"flag"
	"log"
	"net/http"

	"github.com/containerman17/avalanche-s3-archival-node/chain"
	"github.com/containerman17/avalanche-s3-archival-node/dist"
	"github.com/containerman17/avalanche-s3-archival-node/rpc"
	"github.com/containerman17/avalanche-s3-archival-node/store"
	"github.com/containerman17/avalanche-s3-archival-node/vmexec"
)

func main() {
	dir := flag.String("data", "", "data dir")
	chainSpec := flag.String("chain", "", "blockchainID")
	addr := flag.String("addr", "127.0.0.1:19906", "listen")
	flag.Parse()
	c, err := chain.Resolve(context.Background(), *chainSpec, 1, *dir)
	if err != nil {
		log.Fatal(err)
	}
	cas, err := dist.Local(*dir)
	if err != nil {
		log.Fatal(err)
	}
	db, err := store.Open(*dir, cas, c.Root())
	if err != nil {
		log.Fatal(err)
	}
	g, err := vmexec.ChainGenesis(c)
	if err != nil {
		log.Fatal(err)
	}
	srv := rpc.NewServer(db, g.TrieAlloc, rpc.StoreChainContext(db), g.Config)
	log.Printf("oracle on %s head-ready", *addr)
	log.Fatal(http.ListenAndServe(*addr, srv))
}

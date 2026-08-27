package main

import (
	"flag"
	"log"
	"time"

	"github.com/containerman17/avalanche-s3-archival-node/dist"
	"github.com/containerman17/avalanche-s3-archival-node/store"
)

// publishMain is `epochdb dev publish`: one shot of what a serving node's
// syncLoop does, plus the manifest pointer. A serving node publishes on every
// flush; this exists for a dir that already holds its runs and just needs to
// get them into the bucket (an offline build, or a corpus somebody wants to
// hand to a joiner).
//
// ONLY TERMINAL RUNS ARE PUBLISHABLE (DESIGN: the bucket is write-once and
// forever). A dir whose L0 tail has not reached the terminal boundary has
// NOTHING to publish, and this says so instead of inventing something to
// upload.
//
// PUBLISHING DESTROYS THE PRODUCER'S LOCAL COPY of what it uploads: dist.Sync
// uploads and then unlinks each spool file. To publish a corpus you want to
// keep, hardlink the spool into a throwaway dir and publish THAT
// (`cp -al <data>/cas/. $tmp/cas/`). L0 runs live in `<data>/runs` and are never
// touched by any of this.
func publishMain(args []string) {
	fs := flag.NewFlagSet("publish", flag.ExitOnError)
	dataDir := fs.String("data", "./data", "data directory")
	_, resolveChain := chainFlags(fs)
	fs.Parse(args)

	c := resolveChain(*dataDir)
	cas, err := dist.Open(*dataDir)
	if err != nil {
		log.Fatalf("epochdb: publish: %v", err)
	}
	defer cas.Close()
	if !cas.Remote() {
		log.Fatalf("epochdb: publish: no EPOCHDB_S3_ENDPOINT, so there is no bucket to publish to")
	}
	db, err := store.Open(*dataDir, cas, c.Root())
	if err != nil {
		log.Fatalf("epochdb: publish: %v", err)
	}
	defer db.Close()
	man := db.Manifest()
	if len(man.Runs) == 0 {
		log.Fatalf("epochdb: publish: %s holds no runs", *dataDir)
	}
	terminals := 0
	for _, r := range man.Runs {
		if r.Terminal() {
			terminals++
		}
	}
	if terminals == 0 {
		log.Fatalf("epochdb: publish: %s holds %d L0 runs and no terminal run, so there is nothing to publish: this chain is joined over p2p until it reaches %d transactions",
			*dataDir, len(man.Runs), store.TerminalTxs)
	}
	man.Runs = man.Runs[:terminals]
	// The pointer is written locally and marked dirty; Sync uploads CONTENT
	// FIRST AND POINTERS LAST, so a consumer never sees a pointer to runs the
	// bucket does not have.
	if err := db.Publish(); err != nil {
		log.Fatalf("epochdb: publish: %v", err)
	}
	t0 := time.Now()
	if err := db.SyncArtifacts(); err != nil {
		log.Fatalf("epochdb: publish: sync: %v", err)
	}
	log.Printf("epochdb: published %d terminal runs (blocks %d..%d, %d txs) in %s",
		len(man.Runs), man.Runs[0].FromHeight, man.Runs[len(man.Runs)-1].ToHeight,
		man.Runs[len(man.Runs)-1].ToTx, time.Since(t0).Round(time.Millisecond))
}

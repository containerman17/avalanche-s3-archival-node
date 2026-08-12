package main

import (
	"flag"
	"log"
	"time"

	"github.com/containerman17/epochdb/dist"
	"github.com/containerman17/epochdb/store"
)

// publishMain is `epochdb dev publish`: one shot of what a serving node's
// syncLoop does, plus the manifest pointer. A serving node publishes on every
// flush; this exists for a dir that already holds its runs and just needs to
// get them into the bucket (an offline build, or a corpus somebody wants to
// hand to a joiner).
//
// PUBLISHING DESTROYS THE PRODUCER'S LOCAL COPY: dist.Sync uploads and then
// unlinks each spool file. To publish a corpus you want to keep, hardlink the
// spool into a throwaway dir and publish THAT (`cp -al <data>/cas/. $tmp/cas/`).
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
	// The pointer is written locally and marked dirty; Sync uploads CONTENT
	// FIRST AND POINTERS LAST, so a consumer never sees a pointer to runs the
	// bucket does not have.
	if err := db.Publish(); err != nil {
		log.Fatalf("epochdb: publish: %v", err)
	}
	t0 := time.Now()
	if err := cas.Sync(); err != nil {
		log.Fatalf("epochdb: publish: sync: %v", err)
	}
	log.Printf("epochdb: published %d runs (blocks %d..%d, %d txs) in %s",
		len(man.Runs), man.Runs[0].FromHeight, man.Runs[len(man.Runs)-1].ToHeight,
		man.Runs[len(man.Runs)-1].ToTx, time.Since(t0).Round(time.Millisecond))
}

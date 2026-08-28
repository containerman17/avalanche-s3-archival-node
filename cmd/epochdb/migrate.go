package main

import (
	"flag"
	"log"
	"time"

	"github.com/containerman17/avalanche-s3-archival-node/dist"
	"github.com/containerman17/avalanche-s3-archival-node/store"
)

// migrateMain is `epochdb dev migrate --data <dir>`: rewrite a storage v1 or
// v2 data dir as the current version in place, with the node STOPPED (it
// takes the dir lock). It is
// resumable and deletes nothing; see store.Migrate. With EPOCHDB_S3_* set it
// reads the terminal runs through the bucket, uploads each new one as it
// lands, and republishes the manifest pointer at the end.
func migrateMain(args []string) {
	fs := flag.NewFlagSet("migrate", flag.ExitOnError)
	dataDir := fs.String("data", "./data", "data directory")
	fs.Parse(args)

	release := mustLockDataDir("migrate", *dataDir)
	defer release()
	cas, err := dist.Open(*dataDir)
	if err != nil {
		log.Fatalf("epochdb: migrate: %v", err)
	}
	defer cas.Close()
	t0 := time.Now()
	if err := store.Migrate(*dataDir, cas, func(f string, a ...any) { log.Printf("epochdb: migrate: "+f, a...) }); err != nil {
		log.Fatalf("epochdb: migrate: %v", err)
	}
	log.Printf("epochdb: migrate: done in %s", time.Since(t0).Round(time.Second))
}

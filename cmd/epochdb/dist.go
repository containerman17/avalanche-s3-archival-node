package main

// Bootstrap over the content-addressed store (DESIGN.md "Distribution").
//
// There is no manifest. The bucket carries exactly one mutable object per
// chain, its `latest-<chain root>` pointer (dist.LatestPointer), and it is a
// HINT: boot reads it, then walks the epoch
// footers BACKWARD by their embedded prev-hash, which roots at this chain's
// CHAIN ROOT (dist.ChainRoot). Everything the walk learns is written to the local
// index (one marker per epoch), and nothing is downloaded eagerly: with S3
// credentials the reads that follow pull only the chunks they touch, and
// without credentials the artifacts are already in the spool.

import (
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"log"

	"github.com/containerman17/epochdb/chain"
	"github.com/containerman17/epochdb/dist"
	"github.com/containerman17/epochdb/exec"
	"github.com/containerman17/epochdb/state"
)

// publishMain uploads whatever the spool holds and unlinks the local copy once
// the bucket confirms it: one shot of a serving node's syncLoop, for a producer
// box that only seals and never serves. Idempotent (an artifact the bucket
// already has costs one HEAD) and safe to repeat.
func publishMain(args []string) {
	fs := flag.NewFlagSet("publish", flag.ExitOnError)
	dataDir := fs.String("data", "./data", "directory whose spool is uploaded")
	fs.Parse(args)
	st, err := dist.Open(*dataDir)
	if err != nil {
		log.Fatalf("epochdb: publish: %v", err)
	}
	if !st.Remote() {
		st.Close()
		log.Fatalf("epochdb: publish: no EPOCHDB_S3_ENDPOINT configured, there is nowhere to publish to")
	}
	if err := st.Sync(); err != nil {
		st.Close()
		log.Fatalf("epochdb: publish: %v", err)
	}
	if err := st.Close(); err != nil {
		log.Fatalf("epochdb: publish: %v", err)
	}
	log.Printf("publish: spool uploaded and released")
}

// bootstrapChain walks the hash chain from the `latest` pointer and writes the
// local index. Returns the number of epochs indexed. `latest` names ONE thing,
// the newest epoch: snapshots are dead (DESIGN.md ruling 1 of 2026-07-31) and
// a bootstrapped node builds its state frontier out of the epochs themselves
// (epochdb bootstrap --frontier).
func bootstrapChain(st *dist.Store, chainRoot [32]byte) (epochs int, err error) {
	l, err := st.Latest(chainRoot)
	if err != nil {
		return 0, fmt.Errorf("read the `%s` pointer: %w", dist.LatestPointer(chainRoot), err)
	}
	hash := l.Epoch
	var below uint64 // Start of the epoch already indexed, for the contiguity check
	for hash != "" {
		e, err := state.OpenEpoch(st, hash)
		if err != nil {
			return epochs, fmt.Errorf("epoch %s: %w", hash, err)
		}
		start, end, prev := e.Start, e.End(), e.Prev
		e.Close()
		if below != 0 && end != below-1 {
			return epochs, fmt.Errorf("epoch %s covers %d..%d but the chain expected it to end at %d", hash, start, end, below-1)
		}
		if err := state.WriteMarker(st.Dir(), state.EpochMarkerName(start, end-start+1), hash); err != nil {
			return epochs, err
		}
		epochs++
		below = start
		if start <= 1 {
			if prev != chainRoot {
				return epochs, fmt.Errorf("epoch %s claims prev %x, but this network's chain root is %x: wrong chain", hash, prev, chainRoot)
			}
			break
		}
		hash = hex.EncodeToString(prev[:])
	}
	return epochs, nil
}

// validateChain is the SAME walk, run at every node start (ruling 2026-08-01):
// existence and linkage of every epoch from `latest` back to the chain root,
// and NOTHING else. No content is rehashed: that is `epochdb verify`, a full
// pass over the corpus, and this must stay O(epochs) metadata reads. Serving a
// corrupt epoch set silently is the one unforgivable failure, so a break here
// refuses to start.
//
// A missing `latest` is not a break: it is a data dir that has never sealed.
func validateChain(st *dist.Store, chainRoot [32]byte) (int, error) {
	if _, err := st.Latest(chainRoot); errors.Is(err, fs.ErrNotExist) {
		return 0, nil
	}
	return bootstrapChain(st, chainRoot)
}

// buildFrontier is `bootstrap --frontier`: the state half of bootstrapping.
// The chain walk above only writes the local index; this merges every epoch's
// SST section into a fresh Firewood at H (the last sealed block), checks the
// result against header(H).Root and parks the exec head there, so `epochdb
// serve` starts executing at H+1 and follows. Snapshots are dead; the epochs
// ARE the state (DESIGN.md ruling 1 of 2026-07-31).
//
// Runs as its own step, not inside serve: it is a full pass over the corpus
// (hours at mainnet scale) and a node has no business starting an RPC port
// while it happens.
func buildFrontier(dataDir string, st *dist.Store, c *chain.Chain) {
	set, err := state.OpenEpochSet(st)
	if err != nil {
		log.Fatalf("epochdb: bootstrap --frontier: %v", err)
	}
	defer set.Close()
	store, err := state.Open(dataDir)
	if err != nil {
		log.Fatalf("epochdb: bootstrap --frontier: open state layer: %v", err)
	}
	defer store.Close()
	e, err := exec.New(exec.Config{
		DataDir: dataDir,
		Blocks:  set, // containers come from the epochs; nothing is staged yet
		Store:   store,
		Chain:   c,
	})
	if err != nil {
		log.Fatalf("epochdb: bootstrap --frontier: exec.New: %v", err)
	}
	defer e.Close()
	if err := e.BuildFrontier(set); err != nil {
		log.Fatalf("epochdb: bootstrap --frontier: %v", err)
	}
}

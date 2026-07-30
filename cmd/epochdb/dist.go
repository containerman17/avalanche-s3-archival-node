package main

// Bootstrap over the content-addressed store (DESIGN.md "Distribution").
//
// There is no manifest. The bucket carries exactly one mutable object, the
// `latest` pointer, and it is a HINT: boot reads it, then walks the epoch
// footers BACKWARD by their embedded prev-hash, which roots at this chain's
// CHAIN ROOT (dist.ChainRoot). Everything the walk learns is written to the local
// index (one marker per epoch), and nothing is downloaded eagerly: with S3
// credentials the reads that follow pull only the chunks they touch, and
// without credentials the artifacts are already in the spool.

import (
	"encoding/hex"
	"fmt"
	"log"

	"github.com/containerman17/epochdb/dist"
	"github.com/containerman17/epochdb/exec"
	"github.com/containerman17/epochdb/state"
)

// bootstrapChain walks the hash chain from the `latest` pointer and writes the
// local index. Returns the number of epochs indexed. `latest` names ONE thing,
// the newest epoch: snapshots are dead (DESIGN.md ruling 1 of 2026-07-31) and
// a bootstrapped node builds its state frontier out of the epochs themselves
// (epochdb bootstrap --frontier).
func bootstrapChain(st *dist.Store, chainRoot [32]byte) (epochs int, err error) {
	l, err := st.Latest()
	if err != nil {
		return 0, fmt.Errorf("read the `latest` pointer: %w", err)
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
func buildFrontier(dataDir string, st *dist.Store, networkID uint32) {
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
		DataDir:   dataDir,
		Blocks:    set, // containers come from the epochs; nothing is staged yet
		Store:     store,
		NetworkID: networkID,
	})
	if err != nil {
		log.Fatalf("epochdb: bootstrap --frontier: exec.New: %v", err)
	}
	defer e.Close()
	if err := e.BuildFrontier(set); err != nil {
		log.Fatalf("epochdb: bootstrap --frontier: %v", err)
	}
}

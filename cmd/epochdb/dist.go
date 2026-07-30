package main

// Bootstrap over the content-addressed store (DESIGN.md "Distribution").
//
// There is no manifest. The bucket carries exactly one mutable object, the
// `latest` pointer, and it is a HINT: boot reads it, then walks the epoch
// footers BACKWARD by their embedded prev-hash, which roots at sha256 of this
// chain's genesis config. Everything the walk learns is written to the local
// index (one marker per epoch), and nothing is downloaded eagerly: with S3
// credentials the reads that follow pull only the chunks they touch, and
// without credentials the artifacts are already in the spool.

import (
	"encoding/hex"
	"fmt"

	"github.com/containerman17/epochdb/dist"
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
				return epochs, fmt.Errorf("epoch %s claims prev %x, but this network's genesis config hashes to %x: wrong chain", hash, prev, chainRoot)
			}
			break
		}
		hash = hex.EncodeToString(prev[:])
	}
	return epochs, nil
}

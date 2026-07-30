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
	"log"

	"github.com/containerman17/epochdb/dist"
	"github.com/containerman17/epochdb/state"
)

// bootstrapChain walks the hash chain from the `latest` pointer and writes the
// local index. Returns the number of epochs indexed and the floor (0 on a full
// node, B when latest names a snapshot).
func bootstrapChain(st *dist.Store, chainRoot [32]byte) (epochs int, floor uint64, err error) {
	l, err := st.Latest()
	if err != nil {
		return 0, 0, fmt.Errorf("read the `latest` pointer: %w", err)
	}
	if l.Snapshot != "" {
		b, err := state.OpenBaseHash(st, l.Snapshot)
		if err != nil {
			return 0, 0, fmt.Errorf("snapshot %s: %w", l.Snapshot, err)
		}
		floor = b.Block()
		b.Close()
		if err := state.WriteMarker(st.Dir(), state.BaseMarkerName(floor), l.Snapshot); err != nil {
			return 0, 0, err
		}
		log.Printf("bootstrap: snapshot %s at block %d", l.Snapshot[:12], floor)
	}
	hash := l.Epoch
	var below uint64 // Start of the epoch already indexed, for the contiguity check
	for hash != "" {
		e, err := state.OpenEpoch(st, hash)
		if err != nil {
			return epochs, floor, fmt.Errorf("epoch %s: %w", hash, err)
		}
		start, end, prev := e.Start, e.End(), e.Prev
		e.Close()
		if below != 0 && end != below-1 {
			return epochs, floor, fmt.Errorf("epoch %s covers %d..%d but the chain expected it to end at %d", hash, start, end, below-1)
		}
		if err := state.WriteMarker(st.Dir(), state.EpochMarkerName(start, end-start+1), hash); err != nil {
			return epochs, floor, err
		}
		epochs++
		below = start
		if start <= floor+1 {
			if floor == 0 && prev != chainRoot {
				return epochs, floor, fmt.Errorf("epoch %s claims prev %x, but this network's genesis config hashes to %x: wrong chain", hash, prev, chainRoot)
			}
			break
		}
		hash = hex.EncodeToString(prev[:])
	}
	return epochs, floor, nil
}

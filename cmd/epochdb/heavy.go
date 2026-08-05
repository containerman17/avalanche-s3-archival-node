package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"syscall"
	"time"
)

// THE HEAVY GATE: how many chains on one box may run a FRONTIER BUILD at the
// same time. One, unless the operator says otherwise.
//
// WHY A GATE AND NOT A BUDGET. Every other memory term of a serving chain is
// small and steady; the frontier build is neither. It is a k-way merge over
// every epoch's SST section streaming into Firewood, measured at 21.54 GiB of
// peak RSS on a SIX-epoch Fuji corpus even with the build's own Firewood
// profile (2 revisions, persist every batch), and it is the one event whose
// peak is set by CORPUS SIZE rather than by tip pace. Two of them on a 61 GiB
// box is an OOM whatever the per-container limits say, and the box does not
// have to be cold for this to happen: a stop/start wipes the instance store, so
// EVERY chain joins from the bucket at once and every one of them builds.
//
// It is a QUEUE rather than an accounting system on purpose. Nothing here
// measures memory, predicts a peak, or decides whether a build "fits": those
// are the knobs nobody can set correctly. It answers one question an operator
// can hold in their head, "how many chains may do the expensive thing at once",
// the answer degrades by WAITING rather than by dying, and a chain that waits
// is doing what it would do anyway on a box that is thrashing, only without
// taking its siblings down with it.
//
// THE RENDEZVOUS IS THE SHARED CHUNK CACHE DIR, which is the one directory the
// fleet already agrees on (EPOCHDB_CACHE_DIR, dist.CacheRoot). Chains are
// separate processes by ruling, so the lock has to live in the filesystem;
// flock is the whole mechanism, and the kernel releases it when the process
// dies, which is what makes a killed build free its slot with no cleanup path
// to get wrong. A box whose chains do NOT share a cache dir gets a per-dir gate
// that never blocks, which is the right answer there: one chain per dir is one
// build.
const heavyLockPrefix = "heavy."

// heavySlotPoll is how often a waiting build re-sweeps the slots. A build runs
// for minutes to hours, so seconds of latency on the handover cost nothing, and
// polling (rather than blocking in flock) is what lets the wait honour ctx and
// say out loud that it is waiting.
var heavySlotPoll = 5 * time.Second

// heavySlots reads EPOCHDB_HEAVY_SLOTS. Default 1: one frontier build per box.
func heavySlots() (int, error) {
	v := os.Getenv("EPOCHDB_HEAVY_SLOTS")
	if v == "" {
		return 1, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 {
		return 0, fmt.Errorf("EPOCHDB_HEAVY_SLOTS=%q is not a positive count of concurrent frontier builds", v)
	}
	return n, nil
}

// acquireHeavySlot blocks until this process holds one of the box's build
// slots, and returns the release. The returned closer is safe to defer; closing
// the file is what drops the flock.
func acquireHeavySlot(ctx context.Context, dir string) (func(), error) {
	slots, err := heavySlots()
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("heavy gate: %w", err)
	}
	waiting := false
	for {
		for i := range slots {
			name := filepath.Join(dir, fmt.Sprintf("%s%d.lock", heavyLockPrefix, i))
			f, err := os.OpenFile(name, os.O_CREATE|os.O_RDWR, 0o644)
			if err != nil {
				return nil, fmt.Errorf("heavy gate: %w", err)
			}
			if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
				f.Close()
				continue
			}
			if waiting {
				logf("heavy gate: slot %d of %d acquired, building the frontier now", i, slots)
			}
			return func() { f.Close() }, nil
		}
		if !waiting {
			logf("heavy gate: all %d frontier-build slot(s) in %s are held by sibling chains on this box, waiting."+
				" The RPC port is bound and requests wait in the accept backlog; raise EPOCHDB_HEAVY_SLOTS to allow more at once.", slots, dir)
			waiting = true
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(heavySlotPoll):
		}
	}
}

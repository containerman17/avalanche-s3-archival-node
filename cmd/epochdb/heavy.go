package main

import (
	"context"
	"fmt"
	"log"
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

// THE DATA DIR HAS EXACTLY ONE WRITER, AND THIS IS WHAT MAKES IT TRUE. One
// chain per process has been the rule since 2026-08-04 and the serve process is
// the sole writer and sole owner of its dir, but until now that was a
// CONVENTION with nothing enforcing it: two writers on one dir both append to
// the raw families, both truncate a torn tail at open, and both want Firewood's
// exclusive handle, i.e. silent corruption in exchange for a typo. It is also
// the premise the unconditional scratch sweep rests on (state.SweepSealScratch):
// if nobody else can be building here, every stray `epoch-*.tmp` this process
// did not open is dead.
//
// Same mechanism as the heavy gate above and for the same reason: flock is held
// by the process, so a kill -9 leaves no stale lock to clean up. It FAILS,
// never waits: a second writer has nothing to wait for.
const dataDirLockFile = ".epochdb.lock"

// lockDataDir takes the dir's exclusive writer lock for the life of the
// process. The returned closer releases it; dropping it on the floor is fine
// too, since exiting does the same thing.
//
// READ-ONLY OPENERS DO NOT CALL THIS and must not: state.OpenReadOnly, the SDK
// and `dev probe` read a live chain's dir beside its writer on purpose.
func lockDataDir(dir string) (func(), error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	name := filepath.Join(dir, dataDirLockFile)
	f, err := os.OpenFile(name, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, fmt.Errorf("data dir %s is already held by another epochdb process (%s is flocked): one process writes one data dir."+
			" Stop that process (or point this one at another --data dir) and start again;"+
			" read-only users (the SDK, `epochdb dev probe`) need no lock and work beside it", dir, name)
	}
	return func() { f.Close() }, nil
}

// mustLockDataDir is lockDataDir for the dev stages, which write the dir and so
// take the same lock `serve` does: DESIGN warns that no dev stage may run
// beside a live serve, and this is that warning with teeth. One line per stage,
// `defer mustLockDataDir("seal", *dataDir)()`.
func mustLockDataDir(stage, dir string) func() {
	release, err := lockDataDir(dir)
	if err != nil {
		log.Fatalf("epochdb: %s: %v", stage, err)
	}
	return release
}

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

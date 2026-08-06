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
	"context"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"log"
	"os"

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
	defer mustLockDataDir("publish", *dataDir)()
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
// a bootstrapped node builds its state frontier out of the epochs themselves.
//
// METADATA ONLY, and that is a hard constraint (RULING 2026-08-03), because
// this runs at every cold start: per epoch it costs a size probe (a HEAD under
// casfs) plus its ~1KB footer, which is the only place the prev-hash lives. No
// section is pinned, no content is rehashed and no artifact is downloaded to be
// validated: the bytes come down when the frontier build or a read actually
// wants them, and full content verification stays `serve --verify`.
func bootstrapChain(st *dist.Store, chainRoot [32]byte) (epochs int, err error) {
	l, err := st.Latest(chainRoot)
	if err != nil {
		return 0, fmt.Errorf("read the `%s` pointer: %w", dist.LatestPointer(chainRoot), err)
	}
	hash := l.Epoch
	var below uint64 // Start of the epoch already indexed, for the contiguity check
	for hash != "" {
		e, err := state.ReadEpochLink(st, hash)
		if err != nil {
			return epochs, fmt.Errorf("epoch %s: %w", hash, err)
		}
		start, end, prev := e.Start, e.End(), e.Prev
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

// joinChain IS A NODE'S START SEQUENCE, and there is no other one [RULING
// 2026-08-03: THERE IS NO SEPARATE BOOTSTRAP STORY]. Every `epochdb serve` and
// runs it before anything opens the data dir,
// and the whole decision is these four cases:
//
//  1. No `latest` pointer, locally or in the bucket -> a genuinely fresh chain.
//     Fetch from genesis and execute, the path that always existed.
//  2. A pointer exists but the chain it names cannot be walked -> HARD ERROR,
//     refuse to start. NEVER fall back to from-scratch: a typo'd
//     EPOCHDB_S3_PREFIX would silently make a provider re-replay weeks of
//     history, and that is a disaster, not a degraded mode.
//  3. The chain walks clean and this dir has never executed -> build the state
//     frontier out of the sealed epochs right here, automatically. That is the
//     k-way merge `epochdb dev bootstrap --frontier` does by hand; it takes minutes
//     to hours, which is fine, because serveOn bound the listener
//     first and requests wait in the accept backlog.
//  4. The chain walks clean and the dir has state -> today's warm start,
//     untouched.
//
// VALIDATION IS CHEAP, at every start, and that is a hard constraint of the
// same ruling. A warm dir pays the FILESYSTEM ONLY (checkLocalIndex:
// milliseconds, zero requests). A cold one pays metadata: one pointer GET, then
// per epoch a size probe and its ~1KB footer, which is where the prev-hash
// lives. Neither ever rehashes content or downloads an artifact to validate it;
// bytes come down when the frontier build or a read actually wants them, and
// full content verification is `serve --verify` and nothing else.
//
// Providers bootstrap from their OWN bucket, and that is the only case: seeding
// a new box by copying a bucket is an operator's business, so nothing here
// knows about a second, foreign or read-only bucket.
//
// build is the frontier seam (buildFrontier in production, a stub in tests).
func joinChain(ctx context.Context, cfg nodeConfig, build func(ctx context.Context, dataDir string, st *dist.Store, c *chain.Chain) error) error {
	st, err := dist.Open(cfg.DataDir)
	if err != nil {
		return fmt.Errorf("open artifact store: %w", err)
	}
	defer st.Close()
	// FIRST THING THE START DOES WITH THIS DIR: give back the disk a killed
	// build is holding. This process now owns the dir (lockDataDir), so nothing
	// here can be someone's work in progress, and the moment of a restart is
	// exactly when the box is short of disk, with the frontier build below
	// about to want more of it (19.9 GB of `epoch-*.tmp` across seven chains
	// after the outages of 2026-08-05).
	state.SweepSealScratch(st)
	root := cfg.Chain.Root()
	ptr := dist.LatestPointer(root)

	// (1) Nothing has ever been published for this chain root. The pointer read
	// is local-first with a bucket fallback, so this really does mean nowhere.
	l, err := st.Latest(root)
	if errors.Is(err, fs.ErrNotExist) {
		logf("join: no `%s` pointer locally or in the bucket: fresh chain, starting from genesis", ptr)
		return nil
	} else if err != nil {
		return fmt.Errorf("read the `%s` pointer: %w", ptr, err)
	}

	// Which dir this is decides which check it pays, and it is one 8-byte file
	// (state.Open would index every unsealed raw bucket, and startNode is about
	// to do that once already).
	head, _, err := state.ExecHead(cfg.DataDir)
	if err != nil {
		return err
	}

	// (4) WARM: the filesystem alone, milliseconds, no request. This dir walked
	// the hash chain when it joined and wrote every epoch it has sealed since,
	// so the local index IS the linkage record.
	if head > 0 {
		epochs, covered, err := checkLocalIndex(cfg.DataDir, l.Epoch)
		if err == nil {
			logf("join: warm start, local index covers blocks 1..%d in %d epochs (executed to %d)", covered, epochs, head)
			return nil
		}
		// Self-healing, and the only reason this is not fatal: a lost marker is
		// re-derivable from the chain itself, so fall through to the walk.
		logf("join: local index unusable (%v), re-walking the epoch chain", err)
	}

	// (2) A pointer exists, so this chain HAS published history: either every
	// linked artifact is there and we serve all of it, or we refuse. Existence
	// and linkage only, no content read, no artifact downloaded (bootstrapChain).
	epochs, err := bootstrapChain(st, root)
	if err != nil {
		return fmt.Errorf("the `%s` pointer names history this node cannot assemble (%d epochs walked before the break): %w"+
			"\nREFUSING TO START. Re-executing from genesis instead would silently re-replay the whole chain, so fix the source:"+
			" check EPOCHDB_S3_ENDPOINT/BUCKET/PREFIX point at this chain's own bucket, and that the artifact above is in it",
			ptr, epochs, err)
	}
	logf("join: epoch chain intact, %d epochs linked from `%s` to the chain root", epochs, ptr)
	if head > 0 {
		return nil // a warm dir whose index just healed; nothing to build
	}

	// (3) COLD: the chain is whole but this dir has no state of its own.
	logf("join: chain intact but this dir has no execution state: merging %d sealed epochs into a state frontier."+
		" This is minutes to hours; the RPC port is already bound and requests wait.", epochs)
	if err := build(ctx, cfg.DataDir, st, cfg.Chain); err != nil {
		return fmt.Errorf("build the state frontier: %w", err)
	}
	return nil
}

// checkLocalIndex is the warm start's whole validation, and it touches NOTHING
// but the filesystem (RULING 2026-08-03: startup validation is cheap, target
// one second): the local markers must name the epoch `latest` points at and
// must cover blocks 1..N with no gap and nothing stranded above one. No
// artifact is opened, no footer is read, no request is made, and no content is
// ever rehashed (that is `serve --verify`, and only ever that).
//
// An error here is not fatal to the caller: the local index is derivable from
// the chain, so a bad one means walk again, not refuse.
func checkLocalIndex(dir, latest string) (epochs int, covered uint64, err error) {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return 0, 0, err
	}
	count := map[uint64]uint64{} // start -> blocks
	haveLatest := false
	for _, en := range ents {
		start, n, ok := state.ParseEpochMarkerName(en.Name())
		if !ok {
			continue
		}
		hash, err := state.ReadMarker(dir, en.Name())
		if err != nil {
			return 0, 0, err
		}
		haveLatest = haveLatest || hash == latest
		count[start] = n
	}
	if !haveLatest {
		return 0, 0, fmt.Errorf("no marker names the epoch `latest` points at (%s)", latest)
	}
	// Sealing starts at block 1 (block 0 is genesis and has no container).
	for at := uint64(1); ; epochs++ {
		n, ok := count[at]
		if !ok {
			if len(count) != epochs {
				return 0, 0, fmt.Errorf("the index has a hole at block %d with %d epoch(s) stranded above it", at, len(count)-epochs)
			}
			return epochs, at - 1, nil
		}
		at += n
	}
}

// buildFrontier is the state half of joining: the chain walk above only writes
// the local index, this merges every epoch's SST section into a fresh Firewood
// at H (the last sealed block), checks the result against the header that
// settles H and parks the exec head there, so the node starts executing at H+1
// and follows. Snapshots are dead; the epochs ARE the state (DESIGN.md ruling 1
// of 2026-07-31).
//
// It opens and releases its own state layer and executor, because it must be
// done with the Firewood handle before startNode takes it. A previous build
// that was killed halfway is wiped first (exec.HealTornFrontier): its Firewood
// is a partial frontier no one can start on, and it is derived state, so the
// node organizes itself instead of crash-looping.
// The join seam passes its own artifact store; this takes the epoch set off
// the state layer instead, so the merge and the executor that follows it share
// one set (state.Store.Epochs).
//
// IT WAITS FOR A BOX SLOT FIRST (see heavy.go), because this is the largest
// memory event on the box by an order of magnitude and a box runs one process
// per chain: bounding each build was not enough, N of them at once is N times
// the peak whatever each container's ceiling says. The wait happens before
// HealTornFrontier so a queued chain touches nothing at all while it waits.
func buildFrontier(ctx context.Context, dataDir string, _ *dist.Store, c *chain.Chain) error {
	release, err := acquireHeavySlot(ctx, dist.CacheRoot(dataDir))
	if err != nil {
		return err
	}
	defer release()
	if _, err := exec.HealTornFrontier(dataDir); err != nil {
		return err
	}
	store, err := state.Open(dataDir)
	if err != nil {
		return fmt.Errorf("open state layer: %w", err)
	}
	defer store.Close()
	set, err := store.Epochs()
	if err != nil {
		return err
	}
	e, err := exec.New(exec.Config{
		DataDir:       dataDir,
		Blocks:        set, // containers come from the epochs; nothing is staged yet
		Store:         store,
		Chain:         c,
		FrontierBuild: true, // Firewood keeps no history for a merge, see exec.New
	})
	if err != nil {
		return fmt.Errorf("exec.New: %w", err)
	}
	defer e.Close()
	return e.BuildFrontier(set)
}

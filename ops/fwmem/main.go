// Command fwmem is a standalone reproduction harness for Firewood's Rust-side
// memory growth under sustained write replay. It is NOT part of epochdb: it is
// its own module (ops/fwmem/go.mod), it imports nothing from epochdb, and
// nothing in epochdb imports it. It exists to be pasted into an upstream issue.
//
// It opens a Firewood database through the same Go FFI epochdb runs, then
// commits synthetic batches shaped like an EVM state write stream: 32-byte
// hashed account keys with small RLP-ish values, 64-byte hashed storage keys
// with tiny values, a keyspace that grows as new accounts appear, and a mix of
// inserts, updates and deletes over it. Every -sample commits it prints one
// line: wall clock, ops, process RSS, the Go heap, and Firewood's own jemalloc
// gauges, which are what split "Firewood holds this memory" from "the allocator
// is sitting on freed pages".
//
// THE PAIR THAT SHOWS THE PROBLEM, five minutes each, under 1GB of RAM and
// about 1.2GB of disk:
//
//	go run . -dur 5m -batch 50000 -cache-mb 128
//	go run . -dur 5m -batch 50000 -cache-mb 128 -prefill 2000000 -new 0 -del 0
//
// The first writes a keyspace that keeps growing and its memory climbs without
// settling. The second does the same amount of work against a keyspace that
// never grows and its memory is flat. The Go heap is flat in both, so
// everything that moves is on the Rust side of the FFI.
//
// Other knobs worth turning:
//
//	-revisions 128 -deferred 64   the serving profile; retention is enormous
//	                              and obvious, and is NOT the same effect
//	-cache-mb 256 vs 2048         the slope barely moves, so the node cache
//	                              limit is not what bounds this
//	-quiesce 2m                   stop writing and watch whether it comes back
//	-propose -abandon 1           split Propose from Commit and forget one
//	                              proposal per commit, so only the GC cleanup
//	                              can free its Rust memory
//
// Ctrl-C stops it cleanly at the next commit boundary and prints the summary.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"flag"
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/ava-labs/firewood-go-ethhash/ffi"
)

var (
	dir       = flag.String("dir", "", "database directory (default: a temp dir, removed at exit)")
	keep      = flag.Bool("keep", false, "keep the database directory on exit")
	batch     = flag.Int("batch", 200_000, "operations per commit")
	commits   = flag.Int("commits", 0, "stop after this many commits (0 = no limit)")
	dur       = flag.Duration("dur", 30*time.Minute, "stop after this much wall clock")
	revisions = flag.Uint("revisions", 2, "RevisionsInMemory")
	deferred  = flag.Uint64("deferred", 1, "deferred persistence commit count (clamped below revisions)")
	cacheMB   = flag.Uint("cache-mb", 1024, "node cache size in MB")
	rootStore = flag.Bool("root-store", false, "open in RootStore (archival) mode")
	sample    = flag.Int("sample", 5, "print a sample every N commits")
	prefill   = flag.Int("prefill", 0, "create this many keys before measuring (use with -new 0 to hold the keyspace fixed)")
	newFrac   = flag.Float64("new", 0.10, "fraction of ops that create a new key")
	delFrac   = flag.Float64("del", 0.05, "fraction of ops that delete an existing key")
	slotRatio = flag.Int("slots-per-account", 4, "storage-slot keys per account key in the mix")
	rssStopMB = flag.Uint64("rss-stop-mb", 12_000, "abort if process RSS exceeds this, so a runaway run cannot take the box")
	propose   = flag.Bool("propose", false, "commit via Propose+Commit (the production path) instead of Update")
	abandon   = flag.Int("abandon", 0, "with -propose, create this many extra proposals per commit and drop the Go references without calling Drop, so only the GC cleanup can free the Rust memory")
	quiesce   = flag.Duration("quiesce", 0, "after the run, sit idle this long and keep sampling")
	seed      = flag.Uint64("seed", 1, "PRNG seed")
)

func main() {
	flag.Parse()
	log.SetFlags(0)

	dbDir := *dir
	if dbDir == "" {
		tmp, err := os.MkdirTemp("", "fwmem-")
		if err != nil {
			log.Fatal(err)
		}
		dbDir = tmp
		if !*keep {
			defer os.RemoveAll(tmp)
		}
	}

	// The Rust metrics recorder is a process global and may only be started
	// once. Without it the jemalloc gauges are unreadable, which costs the run
	// its most important column, so say so rather than carrying on silently.
	if err := ffi.StartMetrics(); err != nil {
		log.Printf("warning: StartMetrics: %v (jemalloc gauges will be unavailable)", err)
	}

	opts := []ffi.Option{
		ffi.WithNodeCacheSizeInBytes(*cacheMB << 20),
		ffi.WithRevisions(*revisions),
		ffi.WithReadCacheStrategy(ffi.CacheAllReads),
		// Firewood requires this below RevisionsInMemory; graft clamps it the
		// same way, so mirror the clamp instead of failing on a flag combo.
		ffi.WithDeferredPersistenceCommitCount(min(*deferred, uint64(*revisions)-1)),
	}
	if *rootStore {
		opts = append(opts, ffi.WithRootStore())
	}
	db, err := ffi.New(filepath.Join(dbDir, "firewood"), ffi.EthereumNodeHashing, opts...)
	if err != nil {
		log.Fatalf("open: %v", err)
	}
	defer db.Close(context.Background()) //nolint:errcheck // best effort at exit

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Printf("fwmem: dir=%s batch=%d revisions=%d deferred=%d cache=%dMB root-store=%v new=%.2f del=%.2f slots/acct=%d",
		dbDir, *batch, *revisions, min(*deferred, uint64(*revisions)-1), *cacheMB, *rootStore, *newFrac, *delFrac, *slotRatio)
	log.Printf("%8s %8s %12s %10s %10s  %s", "elapsed", "commits", "ops", "rss_mb", "go_mb", "jemalloc (MB)")

	var (
		start   = time.Now()
		rng     = rand.New(rand.NewPCG(*seed, 0x9e3779b9))
		ks      = &keyspace{}
		ops     = make([]ffi.BatchOp, 0, *batch)
		samples []point
		nOps    uint64
	)

	// The prefill is deliberately outside the measured loop and outside the
	// clock: it exists so a "-new 0" run has a keyspace to update, which is the
	// control that separates "the state is genuinely getting bigger" from
	// "memory grows while the state does not".
	for done := 0; done < *prefill; {
		ops = ops[:0]
		for len(ops) < *batch && done < *prefill {
			ks.accounts++
			ops = append(ops, ffi.Put(accountKey(ks.accounts), accountValue(ks.accounts)))
			for j := 0; j < *slotRatio && len(ops) < *batch && done < *prefill; j++ {
				ks.slots++
				ops = append(ops, ffi.Put(slotKey(ks.slots, ks.accounts), slotValue(ks.slots)))
				done++
			}
			done++
		}
		if _, err := db.Update(ops); err != nil {
			log.Fatalf("prefill: %v", err)
		}
	}
	if *prefill > 0 {
		log.Printf("prefilled %d accounts / %d slots in %s, rss %.0f MB", ks.accounts, ks.slots, time.Since(start).Round(time.Second), mb(rss()))
		start = time.Now()
	}

	for n := 1; ; n++ {
		if ctx.Err() != nil || time.Since(start) > *dur || (*commits > 0 && n > *commits) {
			break
		}
		ops = ops[:0]
		for len(ops) < *batch {
			ops = append(ops, ks.op(rng))
		}
		if err := commit(db, ops); err != nil {
			log.Fatalf("commit %d: %v", n, err)
		}
		nOps += uint64(len(ops))

		if n%*sample == 0 {
			p := measure(time.Since(start), n, nOps)
			samples = append(samples, p)
			log.Printf("%8s %8d %12d %10.0f %10.0f  %s",
				p.elapsed.Round(time.Second), p.commits, p.ops, mb(p.rss), mb(p.goSys), renderJemalloc(p.je))
			if p.rss>>20 > *rssStopMB {
				log.Printf("RSS above -rss-stop-mb=%d, stopping", *rssStopMB)
				break
			}
		}
	}
	summarise(samples, dbDir)

	// The idle tail. jemalloc runs its decay on allocator activity in the arena
	// that owns the pages, and background threads are off by default, so a
	// process that stops writing may never hand dirty pages back. If resident
	// falls here while allocated does not, the growth was retention; if neither
	// moves, the memory is genuinely still referenced.
	for end := time.Now().Add(*quiesce); time.Now().Before(end) && ctx.Err() == nil; {
		time.Sleep(min(30*time.Second, time.Until(end)))
		p := measure(time.Since(start), 0, nOps)
		log.Printf("%8s %8s %12s %10.0f %10.0f  %s",
			p.elapsed.Round(time.Second), "idle", "-", mb(p.rss), mb(p.goSys), renderJemalloc(p.je))
	}
}

// commit applies one batch. Update is Propose+Commit in one call and holds no
// Go-side handle; -propose splits them, which is what a real node does, and
// -abandon adds proposals that are created and then simply forgotten. That
// second case is the interesting one: ffi.Proposal releases its Rust memory
// from a runtime.AddCleanup, and the Go GC schedules on the Go heap alone, so
// it cannot see how many Rust bytes a forgotten proposal is holding.
func commit(db *ffi.Database, ops []ffi.BatchOp) error {
	if !*propose {
		_, err := db.Update(ops)
		return err
	}
	for range *abandon {
		if _, err := db.Propose(ops); err != nil {
			return err
		}
	}
	p, err := db.Propose(ops)
	if err != nil {
		return err
	}
	return p.Commit()
}

// ---------------------------------------------------------------------------
// the workload
// ---------------------------------------------------------------------------

// keyspace models a growing EVM state: accounts under 32-byte hashed keys and
// storage slots under 64-byte (account hash || slot hash) keys. It stores only
// counters, never the keys themselves, because every key is a pure function of
// its index. That keeps the Go side of this harness flat by construction, so
// any growth the run shows is on the Rust side and not an artefact of the
// generator.
type keyspace struct {
	accounts uint64 // accounts created so far
	slots    uint64 // storage slots created so far
}

func (k *keyspace) op(rng *rand.Rand) ffi.BatchOp {
	r := rng.Float64()
	isAccount := rng.IntN(*slotRatio+1) == 0

	switch {
	case r < *newFrac || k.slots == 0:
		if isAccount {
			k.accounts++
			return ffi.Put(accountKey(k.accounts), accountValue(k.accounts))
		}
		k.slots++
		return ffi.Put(slotKey(k.slots, k.accounts), slotValue(k.slots))

	case r < *newFrac+*delFrac:
		// Deleting a storage slot is the common EVM delete (SSTORE to zero);
		// an account delete is a prefix delete of its whole subtree, which is
		// what SELFDESTRUCT does, so keep a few of those in the mix too.
		if isAccount && k.accounts > 1 {
			return ffi.PrefixDelete(accountKey(1 + rng.Uint64N(k.accounts)))
		}
		return ffi.Delete(slotKey(1+rng.Uint64N(k.slots), k.accounts))

	default:
		if isAccount && k.accounts > 0 {
			i := 1 + rng.Uint64N(k.accounts)
			return ffi.Put(accountKey(i), accountValue(i+k.slots))
		}
		i := 1 + rng.Uint64N(k.slots)
		return ffi.Put(slotKey(i, k.accounts), slotValue(i+k.slots))
	}
}

// accountKey is keccak-shaped: 32 bytes with no exploitable prefix structure,
// which is what makes the trie wide and shallow the way a real one is. sha256
// stands in for keccak so this file has no dependency but the FFI itself.
func accountKey(i uint64) []byte {
	h := sha256.Sum256(binary.LittleEndian.AppendUint64([]byte("acct"), i))
	return h[:]
}

func slotKey(slot, accounts uint64) []byte {
	// Spread slots over the accounts that exist, so storage writes keep
	// landing under many different account subtrees rather than one.
	acct := uint64(1)
	if accounts > 0 {
		acct = 1 + slot%accounts
	}
	k := make([]byte, 64)
	copy(k, accountKey(acct))
	h := sha256.Sum256(binary.LittleEndian.AppendUint64([]byte("slot"), slot))
	copy(k[32:], h[:])
	return k
}

// accountValue is the size of an RLP-encoded StateAccount (nonce, balance,
// two 32-byte hashes), which is ~70 bytes with small numbers.
func accountValue(i uint64) []byte {
	v := make([]byte, 70)
	binary.LittleEndian.PutUint64(v, i)
	h := sha256.Sum256(v[:8])
	copy(v[8:], h[:])
	copy(v[40:], h[:30])
	return v
}

// slotValue is an RLP-encoded left-trimmed slot value. Real ones are mostly
// short: booleans, small counters, the occasional address or full word.
func slotValue(i uint64) []byte {
	n := 1 + int(i%32)
	var word [8]byte
	binary.LittleEndian.PutUint64(word[:], i)
	v := make([]byte, n)
	copy(v, word[:])
	return v
}

// ---------------------------------------------------------------------------
// measurement
// ---------------------------------------------------------------------------

type point struct {
	elapsed time.Duration
	commits int
	ops     uint64
	rss     uint64
	goSys   uint64
	je      map[string]float64
}

func measure(elapsed time.Duration, commits int, ops uint64) point {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return point{
		elapsed: elapsed,
		commits: commits,
		ops:     ops,
		rss:     rss(),
		// The same split epochdb's production sampler uses: pages the Go
		// runtime holds and has not handed back.
		goSys: ms.HeapSys - ms.HeapReleased,
		je:    jemalloc(),
	}
}

func rss() uint64 {
	b, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return 0
	}
	var size, resident uint64
	fmt.Sscan(string(b), &size, &resident)
	return resident * uint64(os.Getpagesize())
}

// jemalloc reads Firewood's own allocator gauges. fwd_gather_rendered refreshes
// jemalloc's epoch on the way in, so these are current at each call.
func jemalloc() map[string]float64 {
	fams, err := ffi.GatherRenderedMetrics()
	if err != nil {
		return nil
	}
	out := map[string]float64{}
	for _, f := range fams {
		name, ok := strings.CutPrefix(f.GetName(), "jemalloc_")
		if !ok {
			continue
		}
		for _, m := range f.GetMetric() {
			out[strings.TrimSuffix(name, "_bytes")] = m.GetGauge().GetValue()
		}
	}
	return out
}

func renderJemalloc(je map[string]float64) string {
	if len(je) == 0 {
		return "unavailable"
	}
	parts := make([]string, 0, len(je))
	for k, v := range je {
		parts = append(parts, fmt.Sprintf("%s=%.0f", k, v/(1<<20)))
	}
	sort.Strings(parts)
	return strings.Join(parts, " ")
}

func mb(b uint64) float64 { return float64(b) / (1 << 20) }

// summarise fits a slope from the second half of the run, so the open-and-warm
// transient does not flatter or inflate it.
func summarise(s []point, dbDir string) {
	if len(s) < 4 {
		log.Printf("not enough samples for a slope")
		return
	}
	a, b := s[len(s)/2], s[len(s)-1]
	hours := (b.elapsed - a.elapsed).Hours()
	if hours <= 0 {
		return
	}
	dOps := float64(b.ops - a.ops)
	dCommits := float64(b.commits - a.commits)

	log.Printf("")
	log.Printf("SLOPE over the second half (%s to %s):", a.elapsed.Round(time.Second), b.elapsed.Round(time.Second))
	row := func(name string, x, y float64) {
		d := y - x
		log.Printf("  %-12s %8.0f -> %8.0f MB   %+8.2f GB/h   %+7.1f B/op   %+9.0f B/commit",
			name, x/(1<<20), y/(1<<20), d/(1<<30)/hours, d/dOps, d/dCommits)
	}
	row("rss", float64(a.rss), float64(b.rss))
	row("go_heap", float64(a.goSys), float64(b.goSys))
	for _, k := range []string{"allocated", "active", "resident", "mapped", "retained", "metadata"} {
		if _, ok := a.je[k]; ok {
			row("je_"+k, a.je[k], b.je[k])
		}
	}
	log.Printf("  ops/s %.0f, commits %d, db on disk %s", dOps/(b.elapsed-a.elapsed).Seconds(), b.commits, diskSize(dbDir))
}

func diskSize(dir string) string {
	var total int64
	_ = filepath.Walk(dir, func(_ string, fi os.FileInfo, err error) error {
		if err == nil && !fi.IsDir() {
			total += fi.Size()
		}
		return nil
	})
	return fmt.Sprintf("%.1f GB", float64(total)/(1<<30))
}

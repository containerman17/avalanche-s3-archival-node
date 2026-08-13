package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"os"

	"github.com/containerman17/epochdb/dist"
)

// JOINING A CHAIN (DESIGN, "The read path" and the join/start decision tree).
// A node that only has a bucket has no history at all. It resolves the chain's
// pointer, reads the manifest that pointer names, and then WALKS THE RUNS
// BACKWARD BY PREV-NAME to the chain root, checking every link and every range
// boundary. Anything missing or broken is a HARD ERROR that names the artifact:
// never a silent fall back to re-syncing history from p2p, because a typo'd
// EPOCHDB_S3_PREFIX would then make a provider re-replay weeks of execution.
//
// THE POINTER IS A HINT, NEVER AN AUTHORITY. It names a manifest by its casfs
// name, the manifest names runs by their casfs names, and each run's footer
// names the previous run; the first run's footer names the CHAIN ROOT. One
// pointer read plus one footer per run authenticates the whole corpus, and the
// only thing taken on trust from the bucket is each run's merge LEVEL, which is
// scheduling bookkeeping that cannot change any run's bytes.
//
// COST RULE, carried from the epoch join: the walk reads FOOTERS ONLY. Opening
// a run pulls its blooms and index blocks, which is right for serving and
// absurd for proving files exist.

// ErrNoPointer is a chain with no published pointer.
var ErrNoPointer = errors.New("store: this chain has no published manifest pointer")

// ReadFooter reads one run's footer and nothing else: one small ranged GET at
// the tail of the artifact.
func ReadFooter(cas *dist.Store, name RunName) (*Footer, error) {
	blob, err := cas.Open(name)
	if err != nil {
		return nil, fmt.Errorf("store: run %s: %w", name, err)
	}
	defer blob.Close()
	if blob.Size() < uint64(footerSize) {
		return nil, fmt.Errorf("store: run %s is %d bytes, too small for a footer", name, blob.Size())
	}
	b, err := blob.Read(blob.Size()-uint64(footerSize), uint64(footerSize))
	if err != nil {
		return nil, fmt.Errorf("store: run %s footer: %w", name, err)
	}
	f, err := decodeFooter(b)
	if err != nil {
		return nil, fmt.Errorf("store: run %s: %w", name, err)
	}
	return f, nil
}

// Publish writes the PUBLISHED manifest into the artifact store and points the
// chain's `latest` at it. Content goes up before pointers on every Sync, so a
// consumer never sees a pointer to runs that are not in the bucket yet.
//
// THE PUBLISHED MANIFEST LISTS TERMINAL RUNS AND NOTHING ELSE, because they are
// the only artifacts that ever upload (run.go's gate). The local live manifest
// stays ahead with its L0 tail and its window; the bucket's view of the chain
// ends at the last terminal boundary and is COMPLETE AND FINAL there, which is
// the whole point of a write-once bucket: everything the pointer names is
// resident, nothing it names will ever be superseded, and a consumer that lands
// mid-flight sees an older complete corpus rather than a newer torn one.
//
// THE POINTER THEREFORE ADVANCES ONCE PER TERMINAL RUN. A chain that has not
// earned one publishes NOTHING AT ALL: no manifest object, no pointer, an empty
// prefix. That is not a degraded state, it is the answer for every chain under
// eight million transactions, which joins over p2p in minutes.
func (d *DB) Publish() error {
	m := d.Manifest()
	m.Runs = publishable(m.Runs)
	if len(m.Runs) == 0 {
		return nil
	}
	head := m.Runs[len(m.Runs)-1].Name
	d.mu.RLock()
	unchanged := head == d.published
	d.mu.RUnlock()
	if unchanged {
		return nil
	}
	raw, err := json.Marshal(m)
	if err != nil {
		return err
	}
	name, err := d.cas.Put(raw)
	if err != nil {
		return err
	}
	if err := d.cas.SetLatest(d.chainRoot, dist.Latest{Manifest: name}); err != nil {
		return err
	}
	// The superseded manifest's LOCAL copy goes only now, after the pointer
	// moved: the same publish-before-delete order everything else here uses.
	d.mu.Lock()
	old := d.lastManifest
	d.lastManifest, d.published = name, head
	d.mu.Unlock()
	if old != "" && old != name {
		if err := os.Remove(d.cas.SpoolPath(old)); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

// publishable is the leading run of terminals: the part of the live set that is
// in the bucket. It stops at the first L0 rather than filtering, because the
// published set must be a CONTIGUOUS PREFIX FROM TxNum 0 for the join walk to
// reach the chain root, and a gap would be a corpus nobody can verify.
func publishable(runs []RunRef) []RunRef {
	for i, r := range runs {
		if !r.Terminal() {
			return runs[:i]
		}
	}
	return runs
}

// Join makes dir able to serve a published chain. It is a no-op on a dir that
// already holds runs (a warm start costs ZERO bucket requests), and on a dir
// with no pointer it decides between a genuinely fresh chain and a broken
// publish rather than guessing.
func Join(cas *dist.Store, dir string, chainRoot [32]byte) error {
	local, err := loadManifest(dir)
	if err != nil {
		return err
	}
	if len(local.Runs) > 0 {
		return nil // warm dir: it walked the chain when it joined
	}

	l, err := cas.Latest(chainRoot)
	if errors.Is(err, fs.ErrNotExist) {
		return freshOrBroken(cas)
	}
	if err != nil {
		return err
	}

	// The pointer names a manifest; the manifest is content-addressed, so a
	// corrupt one cannot survive its own name.
	blob, err := cas.Open(l.Manifest)
	if err != nil {
		return fmt.Errorf("store: the pointer names manifest %s, which is not in the bucket: %w", l.Manifest, err)
	}
	raw, err := blob.Read(0, blob.Size())
	blob.Close()
	if err != nil {
		return err
	}
	man := new(Manifest)
	if err := json.Unmarshal(raw, man); err != nil {
		return fmt.Errorf("store: manifest %s: %w", l.Manifest, err)
	}
	if man.StorageVersion != StorageVersion {
		return fmt.Errorf("store: published manifest %s is storage version %d, this binary is %d", l.Manifest, man.StorageVersion, StorageVersion)
	}
	if man.ChainRoot != hexName(chainRoot) {
		return fmt.Errorf("store: published manifest %s belongs to chain root %s, this node is %s", l.Manifest, man.ChainRoot, hexName(chainRoot))
	}
	if len(man.Runs) == 0 {
		return fmt.Errorf("store: published manifest %s lists no runs", l.Manifest)
	}

	if err := WalkRuns(cas, man.Runs, chainRoot); err != nil {
		return err
	}
	log.Printf("store: joined a published chain: %d runs, blocks %d..%d, %d txs, head run %s",
		len(man.Runs), man.Runs[0].FromHeight, man.Runs[len(man.Runs)-1].ToHeight,
		man.Runs[len(man.Runs)-1].ToTx, man.Head())
	man.StorageVersion = StorageVersion
	return man.save(dir)
}

// WalkRuns verifies a run list the way DESIGN's decision tree asks: BACKWARD by
// prev-name from the head to the chain root, with every range boundary checked.
// It is the whole trust story, and every failure names the artifact.
func WalkRuns(cas *dist.Store, runs []RunRef, chainRoot [32]byte) error {
	for i := len(runs) - 1; i >= 0; i-- {
		f, err := ReadFooter(cas, runs[i].Name)
		if err != nil {
			return err
		}
		if f.FromTx != runs[i].FromTx || f.ToTx != runs[i].ToTx || f.FromHeight != runs[i].FromHeight || f.ToHeight != runs[i].ToHeight {
			return fmt.Errorf("store: run %s covers tx [%d,%d) blocks [%d,%d] but the manifest claims tx [%d,%d) blocks [%d,%d]",
				runs[i].Name, f.FromTx, f.ToTx, f.FromHeight, f.ToHeight, runs[i].FromTx, runs[i].ToTx, runs[i].FromHeight, runs[i].ToHeight)
		}
		if i == 0 {
			if f.Prev != chainRoot {
				return fmt.Errorf("store: the oldest run %s links back to %x, but this chain's root is %x: the corpus is not this chain's, or its first run is missing",
					runs[i].Name, f.Prev, chainRoot)
			}
			if f.FromTx != 0 {
				return fmt.Errorf("store: the oldest run %s starts at TxNum %d, so history below it is missing", runs[i].Name, f.FromTx)
			}
			continue
		}
		want, err := decodeRoot(runs[i-1].Name)
		if err != nil {
			return err
		}
		if f.Prev != want {
			return fmt.Errorf("store: run %s links back to %x, but the run before it is %s: the hash chain is broken",
				runs[i].Name, f.Prev, runs[i-1].Name)
		}
		if runs[i-1].ToTx != f.FromTx || runs[i-1].ToHeight+1 != f.FromHeight {
			return fmt.Errorf("store: run %s starts at tx %d block %d but %s ends at tx %d block %d: there is a hole",
				runs[i].Name, f.FromTx, f.FromHeight, runs[i-1].Name, runs[i-1].ToTx, runs[i-1].ToHeight)
		}
	}
	return nil
}

// freshOrBroken is the one place a missing pointer is interpreted. A prefix
// that already holds objects means somebody published here and the POINTER is
// what is missing, which is a broken publish and not an empty chain; re-syncing
// from genesis there would silently re-replay a corpus that already exists.
func freshOrBroken(cas *dist.Store) error {
	if !cas.Remote() || os.Getenv("EPOCHDB_NEW_CHAIN") == "1" {
		return nil
	}
	has, err := cas.PrefixHasObjects()
	if err != nil {
		return fmt.Errorf("store: no pointer for this chain, and listing the bucket prefix to tell a fresh chain from a broken publish failed: %w", err)
	}
	if has {
		return fmt.Errorf("store: this chain has no published manifest pointer, but the bucket prefix ALREADY HOLDS OBJECTS: refusing to re-sync a chain somebody has already published (set EPOCHDB_NEW_CHAIN=1 if this really is a new chain going into a shared prefix)")
	}
	log.Printf("store: no pointer and an empty bucket prefix: this is a fresh chain, starting from genesis")
	return nil
}

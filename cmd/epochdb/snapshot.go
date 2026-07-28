package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/ava-labs/avalanchego/graft/coreth/core/extstate"
	"github.com/ava-labs/avalanchego/graft/evm/firewood"
	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/rawdb"
	ethstate "github.com/ava-labs/libevm/core/state"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/rlp"
	"github.com/ava-labs/libevm/triedb"

	"github.com/containerman17/epochdb/exec"
	"github.com/containerman17/epochdb/fetch"
	"github.com/containerman17/epochdb/state"
)

// snapshotMain folds the corpus into a base_<block> file: the flat live
// state at B that replaces the genesis-alloc floor in limited-history mode.
// The data dir is opened READ-ONLY, so this is safe next to a running
// fetch/exec; --out must be a different directory.
func snapshotMain(args []string) {
	fs := flag.NewFlagSet("snapshot", flag.ExitOnError)
	dataDir := fs.String("data", "./data", "corpus directory (opened read-only)")
	outDir := fs.String("out", "", "directory for the base file (required, must differ from --data)")
	at := fs.Uint64("at", 0, "floor block B: the base file is live state at the end of B")
	network := fs.String("network", "fuji", "network: fuji|mainnet")
	doVerify := fs.Bool("verify", false, "rebuild the live rows into a throwaway Firewood and assert the root equals header(B).Root")
	fs.Parse(args)

	if *at == 0 || *outDir == "" || *outDir == *dataDir {
		log.Fatalf("epochdb: snapshot: --at <block> and --out <dir> are required, and --out must differ from --data")
	}
	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		log.Fatalf("epochdb: snapshot: %v", err)
	}

	g, err := exec.NetworkGenesis(execNetID(*network))
	if err != nil {
		log.Fatalf("epochdb: snapshot: genesis: %v", err)
	}
	store, err := state.OpenReadOnly(*dataDir)
	if err != nil {
		log.Fatalf("epochdb: snapshot: %v", err)
	}
	defer store.Close()
	hist, err := state.OpenHistory(*dataDir, store, g.Alloc)
	if err != nil {
		log.Fatalf("epochdb: snapshot: %v", err)
	}
	defer hist.Close()

	var (
		check *rootCheck
		sink  state.BaseSink
	)
	if *doVerify {
		// The base file is hash-keyed, so nothing can rebuild a trie from it
		// after the fact: the root check has to ride along with the build,
		// which still catches the failure that matters (a wrong row set).
		tmp, err := os.MkdirTemp(*outDir, ".basefw-")
		if err != nil {
			log.Fatalf("epochdb: snapshot: %v", err)
		}
		defer os.RemoveAll(tmp)
		if check, err = newRootCheck(tmp); err != nil {
			log.Fatalf("epochdb: snapshot: %v", err)
		}
		defer check.Close()
		sink = check.row
	}

	t0 := time.Now()
	path, err := state.BuildBase(hist, *outDir, *at, sink)
	if err != nil {
		log.Fatalf("epochdb: snapshot: %v", err)
	}
	st, err := os.Stat(path)
	if err != nil {
		log.Fatalf("epochdb: snapshot: %v", err)
	}
	log.Printf("snapshot: wrote %s (%.2f GB) in %s", path, float64(st.Size())/1e9, time.Since(t0).Round(time.Second))

	// Readback: the file must open and describe the floor it was asked for.
	b, ok, err := state.OpenBase(*outDir)
	if err != nil || !ok {
		log.Fatalf("epochdb: snapshot: readback: ok=%v err=%v", ok, err)
	}
	root := b.StateRoot()
	b.Close()
	if !*doVerify {
		log.Printf("snapshot: floor block %d, state root %x (NOT verified: pass --verify)", *at, root)
		return
	}

	raw, hdrOK, err := hist.HeaderRLP(*at)
	if err != nil || !hdrOK {
		log.Fatalf("epochdb: snapshot: header %d: ok=%v err=%v", *at, hdrOK, err)
	}
	var hdr types.Header
	if err := rlp.DecodeBytes(raw, &hdr); err != nil {
		log.Fatalf("epochdb: snapshot: decode header %d: %v", *at, err)
	}
	t1 := time.Now()
	got, err := check.root()
	if err != nil {
		log.Fatalf("epochdb: snapshot: verify: %v", err)
	}
	if got != hdr.Root {
		log.Fatalf("epochdb: snapshot: VERIFY FAILED: %d live rows hash to %x, header(%d).Root is %x",
			check.rows, got, *at, hdr.Root)
	}
	log.Printf("snapshot: verify PASS: %d live rows rebuild header(%d).Root %x in %s",
		check.rows, *at, got, time.Since(t1).Round(time.Second))
}

// rootCheck rebuilds the base's live rows into a throwaway Firewood. Rows
// arrive preimage-keyed in key53 order (every account before any storage),
// which is the trie-op order the epoch verifier uses too.
type rootCheck struct {
	tdb  *triedb.Database
	tr   ethstate.Trie
	rows uint64
}

func newRootCheck(tmpDir string) (*rootCheck, error) {
	fetch.RegisterExtras()
	fwCfg := firewood.DefaultConfig(tmpDir)
	fwCfg.CacheSizeBytes = 4 << 30 // ponytail: exec's proven value on this box
	memdb := rawdb.NewMemoryDatabase()
	tdb := triedb.NewDatabase(memdb, &triedb.Config{DBOverride: fwCfg.BackendConstructor})
	if _, ok := tdb.Backend().(*firewood.TrieDB); !ok {
		tdb.Close()
		return nil, fmt.Errorf("triedb backend is %T, want *firewood.TrieDB", tdb.Backend())
	}
	tr, err := extstate.NewDatabaseWithNodeDB(memdb, tdb).OpenTrie(types.EmptyRootHash)
	if err != nil {
		tdb.Close()
		return nil, fmt.Errorf("open empty trie: %w", err)
	}
	return &rootCheck{tdb: tdb, tr: tr}, nil
}

// row applies one live row (state/cook.go key layout: kind | addr | slot).
func (c *rootCheck) row(key [53]byte, val []byte) error {
	addr := common.BytesToAddress(key[1:21])
	c.rows++
	switch key[0] {
	case 'a':
		var acc types.StateAccount
		if err := rlp.DecodeBytes(val, &acc); err != nil {
			return fmt.Errorf("decode account %x: %w", addr, err)
		}
		return c.tr.UpdateAccount(addr, &acc)
	case 's':
		return c.tr.UpdateStorage(addr, key[21:53], val)
	default:
		return fmt.Errorf("unknown row kind %q", key[0])
	}
}

func (c *rootCheck) root() (common.Hash, error) {
	h := c.tr.Hash()
	if h == (common.Hash{}) {
		return h, fmt.Errorf("firewood proposal over %d rows failed", c.rows)
	}
	return h, nil
}

func (c *rootCheck) Close() { c.tdb.Close() }

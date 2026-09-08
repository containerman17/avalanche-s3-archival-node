// epochdb-archive-serve is a READ-ONLY p2p archive over a storage v3 corpus:
// it answers Get/GetAncestors from the runs named by a manifest, so a node
// can sync a chain whose validators no longer serve history. No DB is opened
// and nothing is written into the corpus: containers are reassembled from the
// chain-section rows (blk/hdr/pvm/tx, store.Reassemble), and the container
// id -> height index a v3 corpus lacks is built once at startup and kept in
// --index.
//
// Same environment as cmd/epochdb: EPOCHDB_S3_* and EPOCHDB_CACHE_DIR, and a
// --dir whose basename is the container's data dir name ("data") so the chunk
// cache namespace matches. Level-0 runs are local-only files: copy them into
// <dir>/runs/ first.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	avaconstants "github.com/ava-labs/avalanchego/utils/constants"
	"github.com/ava-labs/libevm/crypto"

	"github.com/containerman17/avalanche-s3-archival-node/chain"
	"github.com/containerman17/avalanche-s3-archival-node/dist"
	"github.com/containerman17/avalanche-s3-archival-node/fetch"
	"github.com/containerman17/avalanche-s3-archival-node/store"
)

const (
	// winBlocks is how many consecutive containers one cache window holds.
	// A GetAncestors walk descends ~800 heights per answer, so one window is
	// one or two answers built from four range scans instead of four point
	// reads (four 128KB block decompressions) per container.
	winBlocks = 1024
	// maxWins bounds the cache: 16 spans in flight on the fetching side, a
	// window each plus the one they are crossing into.
	maxWins = 48
	// recSize is one index record: sha256 container id (32) + height (8 BE).
	recSize = 40
)

func main() {
	dir := flag.String("dir", "", "data dir for dist.Open (basename = chunk-cache namespace); l0 runs under <dir>/runs")
	manifest := flag.String("manifest", "", "corpus manifest.json")
	version := flag.Uint("version", 3, "storage version the runs were written under")
	chainSpec := flag.String("chain", "", "the L1's blockchainID")
	network := flag.String("network", "mainnet", "network the chain lives on")
	nodeURI := flag.String("node", "", "comma-separated bootstrap RPC node URIs")
	p2pPort := flag.Int("p2p-port", 0, "listen for avalanchego peers on this port")
	index := flag.String("index", "", "container id -> height index file, built on first start")
	flag.Parse()
	if *dir == "" || *manifest == "" || *chainSpec == "" || *p2pPort == 0 || *index == "" {
		log.Fatal("archive-serve: need --dir, --manifest, --chain, --p2p-port, --index")
	}

	raw, err := os.ReadFile(*manifest)
	check(err)
	var m store.Manifest
	check(json.Unmarshal(raw, &m))
	cas, err := dist.Open(*dir)
	check(err)
	defer cas.Close()

	a := &archive{win: map[uint64]*window{}}
	var total uint64
	for _, r := range m.Runs {
		run, err := store.OpenRunVersion(cas, r.Name, uint32(*version))
		check(err)
		defer run.Close()
		a.runs = append(a.runs, runRange{r, run})
		total += r.ToHeight - r.FromHeight + 1
	}
	log.Printf("archive-serve: %d runs, heights [%d,%d], %d blocks", len(a.runs), a.runs[0].FromHeight, a.runs[len(a.runs)-1].ToHeight, total)

	a.idx, err = loadIndex(*index, total)
	if err != nil {
		log.Printf("archive-serve: index %s: %v; building", *index, err)
		check(a.buildIndex(*index, total))
		a.idx, err = loadIndex(*index, total)
		check(err)
	}
	log.Printf("archive-serve: index %s: %d records, %d MB", *index, len(a.idx)/recSize, len(a.idx)>>20)

	netID, err := avaconstants.NetworkID(*network)
	check(err)
	rctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	c, err := chain.Resolve(rctx, *chainSpec, netID, *dir, dist.Sources(*nodeURI)...)
	cancel()
	check(err)
	f, err := fetch.New(fetch.Config{NodeURI: *nodeURI, Chain: c, ListenPort: *p2pPort, DataDir: *dir})
	check(err)
	defer f.Close()
	f.Serve(a)
	log.Printf("archive-serve: serving %s on :%d", c.BlockchainID, *p2pPort)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	t := time.NewTicker(30 * time.Second)
	defer t.Stop()
	t0, last := time.Now(), uint64(0)
	for {
		select {
		case <-ctx.Done():
			log.Printf("archive-serve: stopping")
			return
		case <-t.C:
		}
		served := a.served.Load()
		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)
		log.Printf("archive-serve: served=%d (%.0f/s over 30s, %.0f/s avg) bytes=%dMB lookups=%d misses=%d windows=%d heap=%dMB",
			served, float64(served-last)/30, float64(served)/time.Since(t0).Seconds(), a.bytes.Load()>>20,
			a.lookups.Load(), a.lookupMiss.Load(), a.windows.Load(), ms.HeapAlloc>>20)
		last = served
	}
}

type runRange struct {
	store.RunRef
	run *store.Run
}

// archive is the fetch.ContainerSource over the runs.
type archive struct {
	runs []runRange
	idx  []byte // mmapped index, recSize records sorted by id

	mu    sync.Mutex
	win   map[uint64]*window // key = height / winBlocks
	order []uint64           // insertion order, for eviction

	served, bytes, lookups, lookupMiss, windows atomic.Uint64
}

type window struct {
	ready chan struct{}
	c     [][]byte // container bytes by height - lo, nil where the runs have no such height
	err   error
}

func (a *archive) ContainerAt(h uint64) ([]byte, error) {
	k := h / winBlocks
	a.mu.Lock()
	w := a.win[k]
	if w == nil {
		w = &window{ready: make(chan struct{})}
		a.win[k] = w
		a.order = append(a.order, k)
		for len(a.order) > maxWins {
			delete(a.win, a.order[0])
			a.order = a.order[1:]
		}
		a.mu.Unlock()
		w.c, w.err = a.buildWindow(k*winBlocks, k*winBlocks+winBlocks-1)
		a.windows.Add(1)
		close(w.ready)
	} else {
		a.mu.Unlock()
		<-w.ready
	}
	if w.err != nil {
		return nil, w.err
	}
	c := w.c[h-k*winBlocks]
	if c == nil {
		return nil, fmt.Errorf("archive-serve: height %d is not in the runs", h)
	}
	a.served.Add(1)
	a.bytes.Add(uint64(len(c)))
	return c, nil
}

func (a *archive) HeightByContainerID(id []byte) (uint64, bool, error) {
	a.lookups.Add(1)
	n := len(a.idx) / recSize
	i := sort.Search(n, func(i int) bool { return bytes.Compare(a.idx[i*recSize:i*recSize+32], id) >= 0 })
	if i == n || !bytes.Equal(a.idx[i*recSize:i*recSize+32], id) {
		a.lookupMiss.Add(1)
		return 0, false, nil
	}
	return binary.BigEndian.Uint64(a.idx[i*recSize+32 : (i+1)*recSize]), true, nil
}

// buildWindow reassembles every container in [lo, hi] the runs hold.
func (a *archive) buildWindow(lo, hi uint64) ([][]byte, error) {
	out := make([][]byte, hi-lo+1)
	for _, r := range a.runs {
		if r.ToHeight < lo || r.FromHeight > hi {
			continue
		}
		err := scanBlocks(r.run, max(lo, r.FromHeight), min(hi, r.ToHeight), func(h uint64, hdr, pvm []byte, txs [][]byte) error {
			c, err := store.Reassemble(pvm, hdr, txs)
			out[h-lo] = c
			return err
		})
		if err != nil {
			return nil, fmt.Errorf("archive-serve: window [%d,%d] run %s: %w", lo, hi, r.Name[:8], err)
		}
	}
	return out, nil
}

// containerID is what a peer names the container by: sha256 of the wrapped
// bytes for a proposervm block, the eth block hash for a bare pre-fork one.
func containerID(pvm, hdr, container []byte) []byte {
	if len(pvm) == 0 {
		return crypto.Keccak256(hdr)
	}
	s := sha256.Sum256(container)
	return s[:]
}

// buildIndex streams every block of every run once, ids it, sorts the records
// by id and writes them to path (tmp + rename).
func (a *archive) buildIndex(path string, total uint64) error {
	t0 := time.Now()
	recs := make([][recSize]byte, 0, total)
	for _, r := range a.runs {
		err := scanBlocks(r.run, r.FromHeight, r.ToHeight, func(h uint64, hdr, pvm []byte, txs [][]byte) error {
			c, err := store.Reassemble(pvm, hdr, txs)
			if err != nil {
				return err
			}
			var rec [recSize]byte
			copy(rec[:32], containerID(pvm, hdr, c))
			binary.BigEndian.PutUint64(rec[32:], h)
			recs = append(recs, rec)
			if len(recs)%1_000_000 == 0 {
				log.Printf("archive-serve: index %d/%d blocks, height %d, %.0f blk/s, %s", len(recs), total, h,
					float64(len(recs))/time.Since(t0).Seconds(), time.Since(t0).Round(time.Second))
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("archive-serve: index run %s: %w", r.Name[:8], err)
		}
	}
	scanned := time.Since(t0)
	slices.SortFunc(recs, func(x, y [recSize]byte) int { return bytes.Compare(x[:32], y[:32]) })
	f, err := os.Create(path + ".tmp")
	if err != nil {
		return err
	}
	for i := 0; i < len(recs); i += 1 << 16 {
		chunk := recs[i:min(i+1<<16, len(recs))]
		buf := make([]byte, 0, len(chunk)*recSize)
		for _, r := range chunk {
			buf = append(buf, r[:]...)
		}
		if _, err := f.Write(buf); err != nil {
			return err
		}
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(path+".tmp", path); err != nil {
		return err
	}
	log.Printf("archive-serve: index built: %d blocks, scan %s, sort+write %s, %d MB",
		len(recs), scanned.Round(time.Second), (time.Since(t0) - scanned).Round(time.Second), len(recs)*recSize>>20)
	return nil
}

// loadIndex maps the index file; an error means "build it".
func loadIndex(path string, total uint64) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if st.Size() != int64(total*recSize) {
		return nil, fmt.Errorf("%d bytes, want %d records", st.Size(), total)
	}
	return syscall.Mmap(int(f.Fd()), 0, int(st.Size()), syscall.PROT_READ, syscall.MAP_SHARED)
}

type row struct {
	n uint64
	v []byte
}

// blkRow is blk/<h> by range scan, not Get: a point read probes the section
// bloom, and an older version's bloom split is not this binary's.
func blkRow(r *store.Run, h uint64) ([]byte, error) {
	var v []byte
	err := r.ScanRange(store.SecChain, store.BlkKey(h), store.BlkKey(h+1), func(_, val []byte) bool {
		v = append([]byte(nil), val...)
		return false
	})
	if err != nil || len(v) != 12 {
		return nil, fmt.Errorf("blk/%d: len=%d err=%v", h, len(v), err)
	}
	return v, nil
}

// scanBlocks streams the blocks [lo, hi] of one run in height order: four
// range scans (blk, hdr, pvm, tx), one goroutine each, zipped here. Values
// are copied out of the iterator, and the tx scan covers exactly the first
// block's first TxNum to the last block's boundary slot.
func scanBlocks(r *store.Run, lo, hi uint64, fn func(h uint64, hdr, pvm []byte, txs [][]byte) error) error {
	first, err := blkRow(r, lo)
	if err != nil {
		return err
	}
	last, err := blkRow(r, hi)
	if err != nil {
		return err
	}
	txLo := binary.BigEndian.Uint64(first)
	txHi := binary.BigEndian.Uint64(last) + uint64(binary.BigEndian.Uint32(last[8:]))

	done := make(chan struct{})
	defer close(done)
	scan := func(loKey, hiKey []byte) (<-chan row, *error) {
		ch := make(chan row, 256)
		var serr error
		go func() {
			defer close(ch)
			serr = r.ScanRange(store.SecChain, loKey, hiKey, func(k, v []byte) bool {
				select {
				case ch <- row{store.NumOf(k), append([]byte(nil), v...)}:
					return true
				case <-done:
					return false
				}
			})
		}()
		return ch, &serr
	}
	blk, blkErr := scan(store.BlkKey(lo), store.BlkKey(hi+1))
	hdr, hdrErr := scan(store.HdrKey(lo), store.HdrKey(hi+1))
	pvm, pvmErr := scan(store.PvmKey(lo), store.PvmKey(hi+1))
	tx, txErr := scan(store.TxKey(txLo), store.TxKey(txHi))
	next := func(ch <-chan row, want uint64, fam string, errp *error) ([]byte, error) {
		x, ok := <-ch
		if !ok {
			if *errp != nil {
				return nil, *errp
			}
			return nil, fmt.Errorf("%s/%d: missing", fam, want)
		}
		if x.n != want {
			return nil, fmt.Errorf("%s/%d: got %s/%d", fam, want, fam, x.n)
		}
		return x.v, nil
	}
	for h := lo; h <= hi; h++ {
		b, err := next(blk, h, "blk", blkErr)
		if err != nil {
			return err
		}
		hv, err := next(hdr, h, "hdr", hdrErr)
		if err != nil {
			return err
		}
		pv, err := next(pvm, h, "pvm", pvmErr)
		if err != nil {
			return err
		}
		f, n := binary.BigEndian.Uint64(b), binary.BigEndian.Uint32(b[8:])
		txs := make([][]byte, n)
		for i := range txs {
			if txs[i], err = next(tx, f+uint64(i), "tx", txErr); err != nil {
				return err
			}
		}
		if err := fn(h, hv, pv, txs); err != nil {
			return err
		}
	}
	return nil
}

func check(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

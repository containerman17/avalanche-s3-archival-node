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
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/ava-labs/avalanchego/ids"
	avaconstants "github.com/ava-labs/avalanchego/utils/constants"
	proposerblock "github.com/ava-labs/avalanchego/vms/proposervm/block"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/crypto"
	"github.com/ava-labs/libevm/rlp"

	"github.com/containerman17/avalanche-s3-archival-node/chain"
	"github.com/containerman17/avalanche-s3-archival-node/dist"
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
	identities := flag.Int("identities", 1, "listeners to open, on consecutive ports from --p2p-port, one NodeID each: avalanchego's inbound bandwidth throttler caps every peer at 512 KiB/s, so a syncing node fetches from N of us at N times that")
	sumGas := flag.String("sum-gas", "", "benchmark helper: print gas used and tx count of heights lo-hi per 50k blocks, then exit")
	flag.Parse()
	if *dir == "" || *manifest == "" || *index == "" || (*sumGas == "" && (*chainSpec == "" || *p2pPort == 0)) {
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

	if *sumGas != "" {
		var lo, hi uint64
		if _, err := fmt.Sscanf(*sumGas, "%d-%d", &lo, &hi); err != nil {
			log.Fatal(err)
		}
		check(a.sumGas(lo, hi))
		return
	}
	a.idx, err = loadIndex(*index, total)
	if err != nil {
		log.Printf("archive-serve: index %s: %v; building", *index, err)
		check(a.buildIndex(*index, total))
		if *sumGas != "" {
			var lo, hi uint64
			if _, err := fmt.Sscanf(*sumGas, "%d-%d", &lo, &hi); err != nil {
				log.Fatal(err)
			}
			check(a.sumGas(lo, hi))
			return
		}
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
	var peers []string
	for i := 0; i < *identities; i++ {
		idDir := *dir
		if i > 0 {
			idDir = filepath.Join(*dir, fmt.Sprintf("id%d", i))
			check(os.MkdirAll(idDir, 0o755))
		}
		net, nodeID, err := listen(*p2pPort+i, idDir, c, a)
		check(err)
		defer net.StartClose()
		peers = append(peers, fmt.Sprintf("%s@198.18.0.1:%d", nodeID, *p2pPort+i))
	}
	log.Printf("archive-serve: serving %s on :%d..%d; --peers %s", c.BlockchainID, *p2pPort, *p2pPort+*identities-1, strings.Join(peers, ","))

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
		cs, _ := cas.CacheStats()
		log.Printf("archive-serve: served=%d (%.0f/s over 30s, %.0f/s avg) bytes=%dMB lookups=%d misses=%d polls=%d windows=%d build=%s heap=%dMB cache=%+v",
			served, float64(served-last)/30, float64(served)/time.Since(t0).Seconds(), a.bytes.Load()>>20,
			a.lookups.Load(), a.lookupMiss.Load(), a.polls.Load(), a.windows.Load(), time.Duration(a.buildNanos.Load()).Round(time.Millisecond), ms.HeapAlloc>>20, cs)
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

	served, bytes, lookups, lookupMiss, polls, windows, buildNanos atomic.Uint64
}

// top is the highest height the runs hold.
func (a *archive) top() uint64 { return a.runs[len(a.runs)-1].ToHeight }

type window struct {
	ready chan struct{}
	c     [][]byte   // container bytes by height - lo, nil where the runs have no such height
	id    [][32]byte // their container ids
	err   error
}

func (a *archive) ContainerAt(h uint64) ([]byte, error) {
	w, err := a.windowFor(h, false)
	if err != nil {
		return nil, err
	}
	c := w.c[h%winBlocks]
	if c == nil {
		return nil, fmt.Errorf("archive-serve: height %d is not in the runs", h)
	}
	a.served.Add(1)
	a.bytes.Add(uint64(len(c)))
	return c, nil
}

// idAt is the container id at height h, what a PullQuery for h is answered with.
func (a *archive) idAt(h uint64) (ids.ID, error) {
	w, err := a.windowFor(h, false)
	if err != nil {
		return ids.Empty, err
	}
	if w.c[h%winBlocks] == nil {
		return ids.Empty, fmt.Errorf("archive-serve: height %d is not in the runs", h)
	}
	return w.id[h%winBlocks], nil
}

// windowFor is the window holding h, built once and shared by every caller
// that asks while it builds.
func (a *archive) windowFor(h uint64, prefetch bool) (*window, error) {
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
		t0 := time.Now()
		w.c, w.id, w.err = a.buildWindow(k*winBlocks, k*winBlocks+winBlocks-1)
		a.windows.Add(1)
		a.buildNanos.Add(uint64(time.Since(t0)))
		close(w.ready)
		// A GetAncestors walk descends, so the window below is the next one
		// asked for: build it now, off the request's path.
		if k > 0 && !prefetch {
			go a.windowFor(k*winBlocks-1, true)
		}
	} else {
		a.mu.Unlock()
		<-w.ready
	}
	if w.err != nil {
		return nil, w.err
	}
	return w, nil
}

func (a *archive) HeightByContainerID(id []byte) (uint64, bool, error) {
	a.lookups.Add(1)
	n := len(a.idx) / recSize
	i := sort.Search(n, func(i int) bool { return bytes.Compare(a.idx[i*recSize:i*recSize+32], id) >= 0 })
	if i == n || !bytes.Equal(a.idx[i*recSize:i*recSize+32], id) {
		if a.lookupMiss.Add(1) <= 20 {
			log.Printf("archive-serve: lookup miss %x", id)
		}
		return 0, false, nil
	}
	h := binary.BigEndian.Uint64(a.idx[i*recSize+32 : (i+1)*recSize])
	if a.lookups.Load()-a.lookupMiss.Load() <= 20 {
		log.Printf("archive-serve: lookup %x = height %d", id, h)
	}
	return h, true, nil
}

// buildWindow reassembles every container in [lo, hi] the runs hold.
func (a *archive) buildWindow(lo, hi uint64) ([][]byte, [][32]byte, error) {
	out, id := make([][]byte, hi-lo+1), make([][32]byte, hi-lo+1)
	for _, r := range a.runs {
		if r.ToHeight < lo || r.FromHeight > hi {
			continue
		}
		err := scanBlocks(r.run, max(lo, r.FromHeight), min(hi, r.ToHeight), func(h uint64, hdr, pvm []byte, txs [][]byte) error {
			c, err := store.Reassemble(pvm, hdr, txs)
			if err != nil {
				return err
			}
			out[h-lo] = c
			id[h-lo] = containerID(hdr, c)
			return nil
		})
		if err != nil {
			return nil, nil, fmt.Errorf("archive-serve: window [%d,%d] run %s: %w", lo, hi, r.Name[:8], err)
		}
	}
	return out, id, nil
}

// containerID is what a peer names the container by, fetch.parseContainer's
// rule: a proposervm block's id is sha256 of its UNSIGNED bytes (the
// trailing signature stripped, what proposerblock.Parse computes; NOT sha256
// of the whole container, which is what store v4's cid/ row holds), and a
// bare pre-fork block's id is its eth block hash.
func containerID(hdr, container []byte) ids.ID {
	if blk, err := proposerblock.ParseWithoutVerification(container); err == nil {
		return blk.ID()
	}
	return ids.ID(crypto.Keccak256(hdr))
}

// buildIndex streams every block of every run once, ids it, sorts the records
// by id and writes them to path (tmp + rename).
func (a *archive) buildIndex(path string, total uint64) error {
	t0 := time.Now()
	recs := make([][recSize]byte, total)
	// The scan is one goroutine (IO and decompression); the ids are a codec
	// parse and a hash each, so batches of blocks go to a worker pool and
	// land in their own slots of recs.
	type block struct {
		h      uint64
		hdr, c []byte
	}
	var (
		wg    sync.WaitGroup
		sem   = make(chan struct{}, runtime.NumCPU())
		n     uint64
		batch []block
	)
	flush := func(at uint64, bs []block) {
		sem <- struct{}{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			for k, b := range bs {
				id := containerID(b.hdr, b.c)
				copy(recs[at+uint64(k)][:32], id[:])
				binary.BigEndian.PutUint64(recs[at+uint64(k)][32:], b.h)
			}
		}()
	}
	for _, r := range a.runs {
		err := scanBlocks(r.run, r.FromHeight, r.ToHeight, func(h uint64, hdr, pvm []byte, txs [][]byte) error {
			c, err := store.Reassemble(pvm, hdr, txs)
			if err != nil {
				return err
			}
			if n >= total {
				return fmt.Errorf("more blocks than the manifest's %d", total)
			}
			batch = append(batch, block{h, hdr, c})
			if len(batch) == 4096 {
				flush(n+1-uint64(len(batch)), batch)
				batch = nil
			}
			if n++; n%1_000_000 == 0 {
				log.Printf("archive-serve: index %d/%d blocks, height %d, %.0f blk/s, %s", n, total, h,
					float64(n)/time.Since(t0).Seconds(), time.Since(t0).Round(time.Second))
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("archive-serve: index run %s: %w", r.Name[:8], err)
		}
	}
	flush(n-uint64(len(batch)), batch)
	wg.Wait()
	if n != total {
		return fmt.Errorf("archive-serve: index: %d blocks scanned, manifest says %d", n, total)
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

// sumGas prints gas used and tx count over [lo, hi] from the header rows,
// cumulative per 50k blocks (the benchmark's segments), and the total.
func (a *archive) sumGas(lo, hi uint64) error {
	var gas, txs uint64
	for _, r := range a.runs {
		if r.ToHeight < lo || r.FromHeight > hi {
			continue
		}
		err := scanBlocks(r.run, max(lo, r.FromHeight), min(hi, r.ToHeight), func(h uint64, hdr, pvm []byte, tx [][]byte) error {
			var head types.Header
			if err := rlp.DecodeBytes(hdr, &head); err != nil {
				return fmt.Errorf("header %d: %w", h, err)
			}
			gas += head.GasUsed
			txs += uint64(len(tx))
			if h%50000 == 0 || h == hi {
				fmt.Printf("upto %d gas %d txs %d\n", h, gas, txs)
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	fmt.Printf("TOTAL %d %d gas %d txs %d\n", lo, hi, gas, txs)
	return nil
}

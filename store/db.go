package store

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"iter"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/common/lru"
	"github.com/containerman17/epochdb/dist"
)

// FLUSH TRIGGERS (DESIGN rule 3): every 500,000 txs or 50,000 blocks, whichever
// first, aligned to a block boundary. Both are format constants: the run
// boundaries are a pure function of chain content, so two builders cut at the
// same places.
//
// THE TX TRIGGER COUNTS SLOTS, and a slot is a transaction OR a block boundary,
// because TxNum is the dimension and every block owns a boundary slot. So a run
// holds 500,000 minus its own block count in transactions: at mainnet C's ~16
// txs/block that is ~6% fewer txs per run, and the same 6% at the terminal
// boundary. It is still a pure function of chain content, which is all the
// determinism promise asks of it.
const (
	FlushTxs    = 500_000
	FlushBlocks = 50_000
)

// RunRef is one live run in the manifest: its TxNum range routes reads with no
// bloom and no search, and its name is the casfs artifact name.
type RunRef struct {
	FromTx     uint64 `json:"from_tx"`
	ToTx       uint64 `json:"to_tx"`
	FromHeight uint64 `json:"from_height"`
	ToHeight   uint64 `json:"to_height"`
	Name       string `json:"name"`
	// Level is the merge level, and there are exactly two: 0 is a flush, which
	// is LOCAL FOREVER, and TerminalLevel is the merged run, which is the only
	// thing that ever reaches the bucket. It is manifest bookkeeping, not run
	// content, so it can never move a merged run's bytes.
	Level int `json:"level"`
}

// Terminal reports whether this run is a terminal one: merged, published, and
// never merged again.
func (r RunRef) Terminal() bool { return r.Level >= TerminalLevel }

// Manifest is the live run set. It is a hash list, exactly like every other
// casfs structure here: one head name authenticates all of history through the
// runs' prev links.
type Manifest struct {
	StorageVersion int      `json:"storage_version"`
	ChainRoot      string   `json:"chain_root"`
	Runs           []RunRef `json:"runs"`
}

const manifestFile = "manifest.json"

func loadManifest(dir string) (*Manifest, error) {
	raw, err := os.ReadFile(filepath.Join(dir, manifestFile))
	if os.IsNotExist(err) {
		return &Manifest{StorageVersion: StorageVersion}, nil
	}
	if err != nil {
		return nil, err
	}
	m := new(Manifest)
	if err := json.Unmarshal(raw, m); err != nil {
		return nil, fmt.Errorf("store: manifest: %w", err)
	}
	if m.StorageVersion != StorageVersion {
		return nil, fmt.Errorf("store: manifest is storage version %d, this binary is %d", m.StorageVersion, StorageVersion)
	}
	if err := m.checkTiling(); err != nil {
		return nil, err
	}
	return m, nil
}

// checkTiling refuses a run set that does not TILE both dimensions: run i must
// start exactly where run i-1 ended. Overlapping runs are not a survivable
// state, they are a state where two readers disagree: the point read walks the
// runs newest-first and the sequential ChainRows walks them oldest-first, so
// under an overlap verify validates rows no query will ever return. Merge
// already refuses such a pair forever; refusing it at open is where the corpus
// says so out loud instead of serving on.
func (m *Manifest) checkTiling() error {
	for i := 1; i < len(m.Runs); i++ {
		a, b := m.Runs[i-1], m.Runs[i]
		if b.FromTx != a.ToTx || b.FromHeight != a.ToHeight+1 {
			return fmt.Errorf("store: runs do not tile: %s covers tx [%d,%d) blocks [%d,%d] and %s starts at tx %d block %d",
				a.Name, a.FromTx, a.ToTx, a.FromHeight, a.ToHeight, b.Name, b.FromTx, b.FromHeight)
		}
	}
	return nil
}

// save lands the manifest by tmp+fsync+rename. This IS the publish step: until
// it returns, the run is bytes nobody points at; after it returns, the run is
// live and the staging behind it may go.
func (m *Manifest) save(dir string) error {
	raw, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	path := filepath.Join(dir, manifestFile)
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err = f.Write(append(raw, '\n')); err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// decodeRoot parses a 32-byte hash out of its hex name.
func decodeRoot(s string) ([32]byte, error) {
	var h [32]byte
	raw, err := hex.DecodeString(s)
	if err != nil || len(raw) != 32 {
		return h, fmt.Errorf("store: %q is not a 32-byte hash", s)
	}
	copy(h[:], raw)
	return h, nil
}

// Head is the name of the newest run: one hash that authenticates all of
// history.
func (m *Manifest) Head() string {
	if len(m.Runs) == 0 {
		return ""
	}
	return m.Runs[len(m.Runs)-1].Name
}

// DB is storage v0 for one chain: the memtable over the unflushed window plus
// the live runs, newest-to-oldest.
type DB struct {
	dir       string
	cas       *dist.Store
	chainRoot [32]byte

	mem *memtable

	// ceil memoizes the last txCeiling answer (statedb.go), which is the read
	// path's entry point for every account and slot.
	ceil atomic.Pointer[txCeil]
	// flat is the latest-state cache in front of the descent at the head, and
	// flatWhy is where its byte budget came from, for the operator's one log
	// line.
	flat    *flatCache
	flatWhy string
	// code is the contract-code cache in front of the Code descent (flat.go),
	// keyed by code hash, nil when its budget is 0; codeWhy is where that budget
	// came from, for the same one log line.
	code       *lru.SizeConstrainedCache[common.Hash, []byte]
	codeBudget uint64
	codeWhy    string

	// flushTxs, flushBlocks and terminalTxs ARE the format constants in every
	// build there is. They live on the DB because a test in this package lowers
	// them (export_test.go): a real terminal is eight million transactions and
	// no test is going to execute eight million transactions to prove the
	// publish path. Nothing in a production path writes them, there is no env
	// knob, and the constants themselves stay pinned.
	flushTxs, flushBlocks, terminalTxs uint64

	// THE TERMINAL MERGE RUNS ON ITS OWN GOROUTINE (merge.go). mergeDone is the
	// in-flight merge's completion channel and nil when none runs, so it is both
	// the "one at a time" flag and what WaitMerge blocks on; mergeErr is what the
	// last merge did, and it is STICKY because the goroutine has nobody to return
	// an error to, so MaybeMerge hands it to the executor at the next flush. Both
	// are under mu.
	mergeDone chan struct{}
	mergeErr  error
	// pubMu serialises Publish. The merge goroutine publishes the terminal it
	// just made while the executor's flushes call Publish too, and two of them
	// racing would move the chain pointer twice for one head.
	pubMu sync.Mutex

	mu   sync.RWMutex
	man  *Manifest
	runs []*Run // ascending TxNum range, same order as man.Runs
	// lastManifest is the published manifest artifact, so the superseded one's
	// local copy can go once the pointer no longer names it.
	lastManifest string
	// published is the terminal run the pointer currently names. The pointer
	// advances ONCE PER TERMINAL and nothing else moves it.
	published RunName
}

// Open opens (or creates) storage v0 inside dir.
func Open(dir string, cas *dist.Store, chainRoot [32]byte) (*DB, error) {
	return open(dir, cas, chainRoot, false)
}

// OpenReadOnly opens a corpus BESIDE ITS LIVE WRITER, which is what every
// opener that does not hold the dir's flock must use: `dev verify`, `dev
// rpcdiff`, `dev probe` and the library's Config.ReadOnly. The difference is
// one thing and it is not a policy: the window log's torn tail is NOT cut.
// Cutting it is a write, and a reader that writes to a log the sole writer is
// still appending to silently zero-fills the writer's own rows (see
// memtable.readOnly).
func OpenReadOnly(dir string, cas *dist.Store, chainRoot [32]byte) (*DB, error) {
	return open(dir, cas, chainRoot, true)
}

func open(dir string, cas *dist.Store, chainRoot [32]byte, readOnly bool) (*DB, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	man, err := loadManifest(dir)
	if err != nil {
		return nil, err
	}
	if man.ChainRoot == "" {
		man.ChainRoot = hexName(chainRoot)
	} else if man.ChainRoot != hexName(chainRoot) {
		return nil, fmt.Errorf("store: data dir holds chain root %s, refusing to open it as %s", man.ChainRoot, hexName(chainRoot))
	}
	mem, err := newMemtable(filepath.Join(dir, "window"), readOnly)
	if err != nil {
		return nil, err
	}
	budget, why, err := flatCacheBytes()
	if err != nil {
		mem.close()
		return nil, err
	}
	codeBudget, codeWhy, err := codeCacheBytes(budget)
	if err != nil {
		mem.close()
		return nil, err
	}
	d := &DB{dir: dir, cas: cas, chainRoot: chainRoot, mem: mem, man: man, flat: newFlatCache(budget)}
	d.flushTxs, d.flushBlocks, d.terminalTxs = FlushTxs, FlushBlocks, TerminalTxs
	d.flatWhy, d.codeBudget, d.codeWhy = why, codeBudget, codeWhy
	if codeBudget > 0 {
		d.code = lru.NewSizeConstrainedCache[common.Hash, []byte](codeBudget)
	}
	for _, ref := range man.Runs {
		r, err := OpenRun(cas, ref.Name)
		if err != nil {
			d.Close()
			return nil, err
		}
		d.runs = append(d.runs, r)
	}
	// The window is replayed back into RAM from its own log and cut at the last
	// COMPLETE block; the executor's reconcile walk-back re-executes from there
	// out of staging.
	if err := mem.recover(d.flushedTx(), d.flushedHeight()); err != nil {
		d.Close()
		return nil, err
	}
	// An EMPTY cache is consistent with any head, so pinning it to the head at
	// open is free and it is the only way a corpus nobody executes into (a
	// read-only reader, a bench) ever serves out of it.
	if h, ok := d.Head(); ok {
		d.flat.adopt(h)
	}
	return d, nil
}

// FlatCacheBudget reports the flat latest-state cache's byte budget and where
// the number came from, so one log line tells an operator why they got it.
func (d *DB) FlatCacheBudget() (uint64, string) { return d.flat.budget, d.flatWhy }

// CodeCacheBudget reports the contract-code cache's byte budget and where the
// number came from, beside FlatCacheBudget's.
func (d *DB) CodeCacheBudget() (uint64, string) { return d.codeBudget, d.codeWhy }

func (d *DB) Close() error {
	// A MERGE GOROUTINE MUST NOT OUTLIVE THE DB IT WRITES INTO: it swaps the
	// manifest and closes runs. Waiting here is the whole shutdown rule, and it
	// costs at most one merge. A kill instead of a shutdown is equally safe, by
	// the write order (merge.go's crash points).
	d.WaitMerge()
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, r := range d.runs {
		r.Close()
	}
	d.runs = nil
	if d.mem != nil {
		d.mem.close()
		d.mem = nil
	}
	return nil
}

// Manifest returns a copy of the live run list.
func (d *DB) Manifest() Manifest {
	d.mu.RLock()
	defer d.mu.RUnlock()
	m := *d.man
	m.Runs = append([]RunRef(nil), d.man.Runs...)
	return m
}

// NextTx is the TxNum the next transaction takes: the flushed end plus the
// window.
func (d *DB) NextTx() uint64 {
	_, next, _, _, started := d.mem.window()
	if started {
		return next
	}
	return d.flushedTx()
}

func (d *DB) flushedTx() uint64 {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if len(d.man.Runs) == 0 {
		return 0
	}
	return d.man.Runs[len(d.man.Runs)-1].ToTx
}

// NextHeight is the height the next block takes.
func (d *DB) NextHeight() uint64 {
	_, _, _, next, started := d.mem.window()
	if started {
		return next
	}
	return d.flushedHeight()
}

func (d *DB) flushedHeight() uint64 {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if len(d.man.Runs) == 0 {
		return 0
	}
	return d.man.Runs[len(d.man.Runs)-1].ToHeight + 1
}

// Sync makes the window durable. THE ORDER RULE: the executor calls this before
// it lets Firewood persist, so the state layer is never behind Firewood and a
// restart always has rows for every block Firewood committed.
func (d *DB) Sync() error { return d.mem.sync() }

// Head is the highest stored block height, ok=false on an empty store.
func (d *DB) Head() (uint64, bool) {
	n := d.NextHeight()
	if n == 0 {
		return 0, false
	}
	return n - 1, true
}

// FlushedFloor is the height staging may be retired behind: the end of the last
// FLUSHED run, 0 when nothing has flushed. Not the executed head and not the
// window start, because crash recovery re-executes out of staging from wherever
// the reconcile walk-back lands, and a run boundary is the lowest point that
// can ever be.
func (d *DB) FlushedFloor() uint64 {
	runs := d.Manifest().Runs
	if len(runs) == 0 {
		return 0
	}
	return runs[len(runs)-1].ToHeight
}

// WriteBlock folds one block into the window and flushes when a trigger fires.
// The caller must have appended the block's per-tx state rows into b BEFORE it
// commits Firewood (DESIGN's standing order rule).
func (d *DB) WriteBlock(b *BlockWrite) error {
	if err := d.mem.add(b); err != nil {
		return err
	}
	// THE FLAT LATEST-STATE CACHE MOVES HERE AND NOWHERE ELSE (flat.go): the
	// same critical path that publishes the block, after the window holds it, so
	// a reader is never told the head is at a block the descent cannot answer.
	d.flat.apply(b)
	return d.MaybeFlush()
}

// MaybeFlush cuts a run when a trigger has fired. Both triggers are checked at
// a block boundary, which is where the window always is.
func (d *DB) MaybeFlush() error {
	baseTx, nextTx, baseHeight, nextHeight, started := d.mem.window()
	if !started {
		return nil
	}
	if nextTx-baseTx < d.flushTxs && nextHeight-baseHeight < d.flushBlocks {
		return nil
	}
	return d.Flush()
}

// Flush cuts the window into a run and retires it, in the publish-before-delete
// order the whole design runs on: cut, fsync, publish by atomic snapshot swap,
// only then unlink staging.
func (d *DB) Flush() error {
	baseTx, nextTx, baseHeight, nextHeight, started := d.mem.window()
	if !started || nextHeight == baseHeight {
		return nil
	}
	prev := d.chainRoot
	d.mu.RLock()
	if n := len(d.man.Runs); n > 0 {
		var err error
		if prev, err = decodeRoot(d.man.Runs[n-1].Name); err != nil {
			d.mu.RUnlock()
			return err
		}
	}
	d.mu.RUnlock()

	// AN L0 RUN IS BUILT IN THE LOCAL DIRECTORY AND STAYS THERE. It is the
	// machine's own working set: only the terminal run it eventually merges into
	// is ever uploaded, so nothing in the bucket is ever superseded.
	path := RunFileName(d.cas.LocalDir(), 0, baseTx, nextTx)
	w, err := NewRunWriter(path, prev, 0)
	if err != nil {
		return err
	}
	if err := d.writeSections(w); err != nil {
		w.Abort()
		return err
	}
	name, _, err := w.Finish(d.cas, baseTx, nextTx, baseHeight, nextHeight-1)
	if err != nil {
		w.Abort()
		return err
	}

	run, err := OpenRun(d.cas, name)
	if err != nil {
		return err
	}
	ref := RunRef{FromTx: baseTx, ToTx: nextTx, FromHeight: baseHeight, ToHeight: nextHeight - 1, Name: name}

	// PUBLISH: the manifest lands durably first, then the in-memory snapshot
	// swaps. Only after both is the staging behind it allowed to go.
	d.mu.Lock()
	d.man.Runs = append(d.man.Runs, ref)
	if err := d.man.save(d.dir); err != nil {
		d.man.Runs = d.man.Runs[:len(d.man.Runs)-1]
		d.mu.Unlock()
		run.Close()
		return err
	}
	d.runs = append(d.runs, run)
	d.mu.Unlock()

	// Only now may the window's staging go.
	if err := d.mem.reset(nextTx, nextHeight); err != nil {
		return err
	}
	// THE MERGE BOUNDARY IS A FLUSH BOUNDARY, known in advance. This STARTS the
	// merge and returns; the terminal lands on its own goroutine minutes later
	// and publishes itself. It is also the retry after a kill, and it needs no
	// special position for that any more: the span is fixed by content, so a
	// process that finds seventeen L0 runs still merges the sixteen the boundary
	// names (merge.go's mergeSpanLocked).
	if err := d.MaybeMerge(); err != nil {
		return err
	}
	// The manifest is published last, and Sync uploads content before pointers,
	// so a consumer never sees a pointer to runs that are not up yet. Publish is
	// a no-op except on the flush that just earned a terminal run.
	return d.Publish()
}

// writeSections streams the window into the three sections in order. Chain
// arrives sorted (it is read back out of the spills in key order); state and
// lookup are sorted here.
func (d *DB) writeSections(w *RunWriter) error {
	m := d.mem

	if err := w.Begin(SecChain); err != nil {
		return err
	}
	for fam := range m.chain {
		pfx := famPrefix[fam]
		if err := m.eachChain(fam, func(n uint64, val []byte) error {
			return w.Set(numKey(pfx, n), val)
		}); err != nil {
			return err
		}
	}
	if err := w.End(); err != nil {
		return err
	}

	if err := w.Begin(SecState); err != nil {
		return err
	}
	codeHashes := make([]string, 0, len(m.code))
	for h := range m.code {
		codeHashes = append(codeHashes, h)
	}
	sort.Strings(codeHashes)
	for _, h := range codeHashes {
		if err := w.Set(CodeKey([]byte(h)), m.code[h]); err != nil {
			return err
		}
	}
	prefixes := make([]string, 0, len(m.state))
	for k := range m.state {
		prefixes = append(prefixes, k)
	}
	sort.Strings(prefixes)
	for _, p := range prefixes {
		for _, e := range m.state[p] {
			if err := w.Set(Suffixed([]byte(p), e.txnum), e.val); err != nil {
				return err
			}
		}
	}
	if err := w.End(); err != nil {
		return err
	}

	if err := w.Begin(SecLookup); err != nil {
		return err
	}
	rows := make([]posting, 0, len(m.post)+len(m.nums))
	rows = append(rows, m.post...)
	for k, n := range m.nums {
		rows = append(rows, posting{key: []byte(k), num: n, isNum: true})
	}
	sort.Slice(rows, func(i, j int) bool { return string(rows[i].key) < string(rows[j].key) })
	for _, r := range rows {
		var val []byte
		if r.isNum {
			val = beU64(r.num)
		} else {
			val = []byte{r.val}
		}
		if err := w.Set(r.key, val); err != nil {
			return err
		}
	}
	return w.End()
}

// ---------------------------------------------------------------------------
// THE READ PATH: memtable, then live runs newest-to-oldest, bloom-gated, first
// hit wins. ONE DESCENT FOR EVERYTHING.
// ---------------------------------------------------------------------------

func (d *DB) snapshot() []*Run {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.runs
}

// chainRow resolves a chain-section row: the window's spill first, then the run
// whose range covers the number. Chain rows have no bloom because the run's
// range answers existence before any file opens.
func (d *DB) chainRow(fam int, n uint64, byHeight bool) ([]byte, bool, error) {
	if v, ok, err := d.mem.chainGet(fam, n); err != nil || ok {
		return v, ok, err
	}
	runs := d.snapshot()
	for i := len(runs) - 1; i >= 0; i-- {
		f := runs[i].Footer
		in := n >= f.FromTx && n < f.ToTx
		if byHeight {
			in = n >= f.FromHeight && n <= f.ToHeight
		}
		if !in {
			continue
		}
		return runs[i].Get(SecChain, numKey(famPrefix[fam], n))
	}
	return nil, false, nil
}

// Chain families, exported for the sequential readers below. Which of the two
// numbering dimensions a family is keyed by is a property of the family, so
// nothing outside has to remember it.
const (
	FamBlk  = famBlk
	FamHdr  = famHdr
	FamItx  = famItx
	FamPvm  = famPvm
	FamRcpt = famRcpt
	FamTx   = famTx
)

func famByHeight(fam int) bool { return fam == famBlk || fam == famHdr || fam == famPvm }

// ChainRow is one row of a chain family, in number order.
type ChainRow struct {
	Num uint64
	Val []byte
}

// ChainRows streams one chain family's rows over [from, to] IN ONE SEQUENTIAL
// PASS: the runs whose range covers it, oldest-first, then the window. This is
// the only way to read a whole range without paying the codec per row: a chain
// data block is 128KB of zstd-9, so N point Gets into one block decompress it N
// times, while one iterator decodes it exactly once. Whole-range readers (the
// layer 1 verify pass) use this; a single row is still chainRow's job.
//
// The range is a pure filter over what is stored: a missing row simply does not
// appear, so a caller that requires density checks the numbers it gets.
func (d *DB) ChainRows(fam int, from, to uint64) iter.Seq2[ChainRow, error] {
	return func(yield func(ChainRow, error) bool) {
		if to < from {
			return
		}
		byHeight := famByHeight(fam)
		prefix := famPrefix[fam]
		next := from
		for _, r := range d.snapshot() { // ascending; chain ranges never overlap
			f := r.Footer
			lo, hi := f.FromTx, f.ToTx-1
			if byHeight {
				lo, hi = f.FromHeight, f.ToHeight
			}
			if f.ToTx == f.FromTx && !byHeight {
				continue // a run with no transactions holds no txnum-keyed rows
			}
			if hi < next || lo > to {
				continue
			}
			stopped := false
			err := r.ScanRange(SecChain, numKey(prefix, next), numKey(prefix, to+1), func(k, v []byte) bool {
				n := TxNumOf(k)
				next = n + 1
				if !yield(ChainRow{Num: n, Val: append([]byte(nil), v...)}, nil) {
					stopped = true
					return false
				}
				return true
			})
			if err != nil {
				yield(ChainRow{}, err)
				return
			}
			if stopped {
				return
			}
		}
		// The window's rows are uncompressed offsets into the append-only log,
		// so there is no block to decode once and nothing to iterate: reading
		// them one by one IS the sequential read.
		for n := next; n <= to; n++ {
			v, ok, err := d.mem.chainGet(fam, n)
			if err != nil {
				yield(ChainRow{}, err)
				return
			}
			if !ok {
				continue
			}
			if !yield(ChainRow{Num: n, Val: v}, nil) {
				return
			}
		}
	}
}

// HeaderRLP returns a block's header RLP verbatim.
func (d *DB) HeaderRLP(height uint64) ([]byte, bool, error) {
	return d.chainRow(famHdr, height, true)
}

// Pvm returns a block's proposervm wrapper bytes verbatim (empty pre-fork).
func (d *DB) Pvm(height uint64) ([]byte, bool, error) {
	return d.chainRow(famPvm, height, true)
}

// BlockTxRange returns a block's first TxNum and tx count. The block occupies
// the slots [first, first+count]: its transactions, then its BOUNDARY SLOT.
func (d *DB) BlockTxRange(height uint64) (first uint64, count uint32, ok bool, err error) {
	v, ok, err := d.chainRow(famBlk, height, true)
	if err != nil || !ok {
		return 0, 0, ok, err
	}
	if len(v) != 12 {
		return 0, 0, false, fmt.Errorf("store: blk/%d is %d bytes, want 12", height, len(v))
	}
	return beDecode(v[:8]), beDecode32(v[8:]), true, nil
}

// TxRLP returns a transaction's RLP verbatim.
func (d *DB) TxRLP(txnum uint64) ([]byte, bool, error) { return d.chainRow(famTx, txnum, false) }

// Frames returns a transaction's stored call frames.
func (d *DB) Frames(txnum uint64) ([]byte, bool, error) { return d.chainRow(famItx, txnum, false) }

// Receipt returns a transaction's stored receipt + full logs.
func (d *DB) Receipt(txnum uint64) ([]byte, bool, error) { return d.chainRow(famRcpt, txnum, false) }

// lookupNum resolves one suffix-free lookup row: the window, then the runs
// newest-to-oldest, whole-key-bloom gated. A hash lives in exactly one run, so
// nearly every probe is a bloom miss: the filter's best case.
func (d *DB) lookupNum(key []byte) (uint64, bool, error) {
	if n, ok := d.mem.lookupNum(key); ok {
		return n, true, nil
	}
	runs := d.snapshot()
	for i := len(runs) - 1; i >= 0; i-- {
		v, ok, err := runs[i].Get(SecLookup, key)
		if err != nil {
			return 0, false, err
		}
		if ok {
			if len(v) != 8 {
				return 0, false, fmt.Errorf("store: %s row is %d bytes, want 8", key[:5], len(v))
			}
			return beDecode(v), true, nil
		}
	}
	return 0, false, nil
}

// TxNumByHash resolves a tx hash to its TxNum.
func (d *DB) TxNumByHash(hash []byte) (uint64, bool, error) { return d.lookupNum(TxHashKey(hash)) }

// HeightByHash resolves a BLOCK HASH to its height. ok=false is a clean "no
// such block on this chain", never "I could not read it": a read failure comes
// back as the error.
func (d *DB) HeightByHash(hash []byte) (uint64, bool, error) { return d.lookupNum(BlkHashKey(hash)) }

// latest walks the descent for the newest value under prefix at or below txnum.
func (d *DB) latest(sec Section, prefix []byte, at uint64) ([]byte, bool, error) {
	if v, _, ok := d.mem.latestState(prefix, at); ok {
		return v, true, nil
	}
	runs := d.snapshot()
	for i := len(runs) - 1; i >= 0; i-- {
		if runs[i].Footer.FromTx > at {
			continue
		}
		v, _, ok, err := runs[i].Latest(sec, prefix, at)
		if err != nil || ok {
			return v, ok, err
		}
	}
	return nil, false, nil
}

// AccountAt returns an account's RLP at TxNum at. An empty value with ok=true
// means the account was DELETED at that point, which is not the same answer as
// "never written here" (ok=false), and the caller must not collapse the two.
func (d *DB) AccountAt(addr []byte, at uint64) ([]byte, bool, error) {
	return d.latest(SecState, AccountPrefix(addr), at)
}

// StorageAt returns a slot's post-tx value at TxNum at. An empty value is a
// cleared slot, which the EVM defines as zero.
func (d *DB) StorageAt(addr, slot []byte, at uint64) ([]byte, bool, error) {
	return d.latest(SecState, SlotPrefix(addr, slot), at)
}

// CodeHashAt returns an account's code hash at TxNum at.
func (d *DB) CodeHashAt(addr []byte, at uint64) ([]byte, bool, error) {
	return d.latest(SecState, CodeRefPrefix(addr), at)
}

// Code resolves a code blob by hash: the code cache, then window, then runs
// newest-to-oldest, bloom-gated. ONE FUNCTION, ALL CALLERS.
//
// THE CACHE NEEDS NO CONSISTENCY RULE AT ALL (flat.go): bytecode is immutable
// by hash, so a hit is the right answer at every height and there is nothing to
// invalidate. Only a run hit is filled: a window hit is already RAM, and the
// blob a run hands back is a copy this DB owns.
func (d *DB) Code(hash []byte) ([]byte, bool, error) {
	h := common.BytesToHash(hash)
	if d.code != nil {
		if b, ok := d.code.Get(h); ok {
			return b, true, nil
		}
	}
	if b, ok := d.mem.codeBlob(hash); ok {
		return b, true, nil
	}
	key := CodeKey(hash)
	runs := d.snapshot()
	for i := len(runs) - 1; i >= 0; i-- {
		v, ok, err := runs[i].Get(SecState, key)
		if err != nil {
			return nil, false, err
		}
		if ok {
			if d.code != nil {
				d.code.Add(h, v)
			}
			return v, true, nil
		}
	}
	return nil, false, nil
}

// StateTxNums calls fn with the TxNum of every state row in [lo, hi]: the runs
// whose range covers it, then the window. It is what layer 1 reads to answer
// "did this block's state writes actually land at this block's own slots", and
// it is a SEQUENTIAL PASS of the state family: that family is sorted by key
// rather than by TxNum, so the numbers arrive in no useful order and the caller
// collects them. Code rows carry no TxNum and are skipped.
func (d *DB) StateTxNums(lo, hi uint64, fn func(txnum uint64)) error {
	for _, r := range d.snapshot() {
		if r.Footer.ToTx <= lo || r.Footer.FromTx > hi {
			continue
		}
		if err := r.ScanRange(SecState, nil, nil, func(k, _ []byte) bool {
			if bytes.HasPrefix(k, []byte(PrefixState)) {
				if n := TxNumOf(k); n >= lo && n <= hi {
					fn(n)
				}
			}
			return true
		}); err != nil {
			return err
		}
	}
	d.mem.mu.RLock()
	defer d.mem.mu.RUnlock()
	for _, h := range d.mem.state {
		for _, e := range h {
			if e.txnum >= lo && e.txnum <= hi {
				fn(e.txnum)
			}
		}
	}
	return nil
}

// Postings scans one lookup family's posting rows in [loTx, hiTx] ascending.
func (d *DB) Postings(prefix []byte, loTx, hiTx uint64, fn func(txnum uint64, val byte) bool) error {
	runs := d.snapshot()
	stop := false
	for _, r := range runs {
		if stop || r.Footer.ToTx <= loTx || r.Footer.FromTx > hiTx {
			continue
		}
		if err := r.Scan(SecLookup, prefix, loTx, hiTx, func(n uint64, v []byte) bool {
			var b byte
			if len(v) > 0 {
				b = v[0]
			}
			if !fn(n, b) {
				stop = true
				return false
			}
			return true
		}); err != nil {
			return err
		}
	}
	if stop {
		return nil
	}
	// The window's postings are unsorted in memory; scan them in TxNum order.
	d.mem.mu.RLock()
	rows := append([]posting(nil), d.mem.post...)
	d.mem.mu.RUnlock()
	sort.Slice(rows, func(i, j int) bool { return TxNumOf(rows[i].key) < TxNumOf(rows[j].key) })
	for _, p := range rows {
		if len(p.key) != len(prefix)+8 || string(p.key[:len(prefix)]) != string(prefix) {
			continue
		}
		n := TxNumOf(p.key)
		if n < loTx || n > hiTx {
			continue
		}
		if !fn(n, p.val) {
			return nil
		}
	}
	return nil
}

// PostingsDesc is Postings in reverse: one lookup family's posting rows in
// [loTx, hiTx] DESCENDING, window first (it holds the newest rows), then the
// runs newest-to-oldest. It is what newest-first keyset pagination reads: a
// page is the first pageSize rows this walk yields, and the cursor for the next
// page is the last TxNum it returned. fn returning false stops the walk.
func (d *DB) PostingsDesc(prefix []byte, loTx, hiTx uint64, fn func(txnum uint64, val byte) bool) error {
	d.mem.mu.RLock()
	rows := append([]posting(nil), d.mem.post...)
	d.mem.mu.RUnlock()
	sort.Slice(rows, func(i, j int) bool { return TxNumOf(rows[i].key) > TxNumOf(rows[j].key) })
	for _, p := range rows {
		if len(p.key) != len(prefix)+8 || string(p.key[:len(prefix)]) != string(prefix) {
			continue
		}
		n := TxNumOf(p.key)
		if n < loTx || n > hiTx {
			continue
		}
		if !fn(n, p.val) {
			return nil
		}
	}
	runs := d.snapshot()
	for i := len(runs) - 1; i >= 0; i-- {
		r := runs[i]
		if r.Footer.ToTx <= loTx || r.Footer.FromTx > hiTx {
			continue
		}
		stop := false
		if err := r.ScanDesc(SecLookup, prefix, loTx, hiTx, func(n uint64, v []byte) bool {
			var b byte
			if len(v) > 0 {
				b = v[0]
			}
			if !fn(n, b) {
				stop = true
				return false
			}
			return true
		}); err != nil {
			return err
		}
		if stop {
			return nil
		}
	}
	return nil
}

// HeightOfTx finds the block a TxNum belongs to. The blk/ rows are a monotone
// map from height to first TxNum, so this is a binary search over the run that
// covers the TxNum plus a walk inside it.
func (d *DB) HeightOfTx(txnum uint64) (uint64, bool, error) {
	lo, hi, ok := d.heightBounds(txnum)
	if !ok {
		return 0, false, nil
	}
	for lo < hi {
		mid := (lo + hi + 1) / 2
		first, _, ok, err := d.BlockTxRange(mid)
		if err != nil {
			return 0, false, err
		}
		if !ok {
			return 0, false, nil
		}
		if first <= txnum {
			lo = mid
		} else {
			hi = mid - 1
		}
	}
	return lo, true, nil
}

func (d *DB) heightBounds(txnum uint64) (lo, hi uint64, ok bool) {
	baseTx, nextTx, baseHeight, nextHeight, started := d.mem.window()
	if started && txnum >= baseTx && txnum < nextTx {
		return baseHeight, nextHeight - 1, true
	}
	d.mu.RLock()
	defer d.mu.RUnlock()
	for _, r := range d.man.Runs {
		if txnum >= r.FromTx && txnum < r.ToTx {
			return r.FromHeight, r.ToHeight, true
		}
	}
	return 0, 0, false
}

// SectionSizes reports the on-disk bytes per section across the live runs.
func (d *DB) SectionSizes() [numSections]uint64 {
	var out [numSections]uint64
	for _, r := range d.snapshot() {
		for s := Section(0); s < numSections; s++ {
			out[s] += r.SectionSize(s)
		}
	}
	return out
}

func beDecode(b []byte) uint64 {
	var n uint64
	for _, c := range b {
		n = n<<8 | uint64(c)
	}
	return n
}

func beDecode32(b []byte) uint32 { return uint32(beDecode(b)) }

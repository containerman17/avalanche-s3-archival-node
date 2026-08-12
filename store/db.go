package store

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/containerman17/epochdb/dist"
)

// FLUSH TRIGGERS (DESIGN rule 3): every 500,000 txs or 50,000 blocks, whichever
// first, aligned to a block boundary. Both are format constants: the run
// boundaries are a pure function of chain content, so two builders cut at the
// same places.
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
	// Level is the merge level: 0 is a flush, and MergeFanIn runs of one level
	// become one run of the next. It is manifest bookkeeping, not run content,
	// so it can never move a merged run's bytes.
	Level int `json:"level"`
}

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
	return m, nil
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

	mu   sync.RWMutex
	man  *Manifest
	runs []*Run // ascending TxNum range, same order as man.Runs
	// lastManifest is the published manifest artifact, so the superseded one's
	// local copy can go once the pointer no longer names it.
	lastManifest string
}

// Open opens (or creates) storage v0 inside dir.
func Open(dir string, cas *dist.Store, chainRoot [32]byte) (*DB, error) {
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
	mem, err := newMemtable(filepath.Join(dir, "window"))
	if err != nil {
		return nil, err
	}
	d := &DB{dir: dir, cas: cas, chainRoot: chainRoot, mem: mem, man: man}
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
	return d, nil
}

func (d *DB) Close() error {
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

// WriteBlock folds one block into the window and flushes when a trigger fires.
// The caller must have appended the block's per-tx state rows into b BEFORE it
// commits Firewood (DESIGN's standing order rule).
func (d *DB) WriteBlock(b *BlockWrite) error {
	if err := d.mem.add(b); err != nil {
		return err
	}
	return d.MaybeFlush()
}

// MaybeFlush cuts a run when a trigger has fired. Both triggers are checked at
// a block boundary, which is where the window always is.
func (d *DB) MaybeFlush() error {
	baseTx, nextTx, baseHeight, nextHeight, started := d.mem.window()
	if !started {
		return nil
	}
	if nextTx-baseTx < FlushTxs && nextHeight-baseHeight < FlushBlocks {
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

	path := RunFileName(d.cas.SpoolDir(), baseTx, nextTx)
	w, err := NewRunWriter(path, prev)
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
	// THE MERGE BOUNDARY IS THE FLUSH BOUNDARY, known in advance.
	if err := d.MaybeMerge(); err != nil {
		return err
	}
	// The manifest is published last, and Sync uploads content before
	// pointers, so a consumer never sees a pointer to runs that are not up yet.
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
	rows := make([]posting, 0, len(m.post)+len(m.txh))
	rows = append(rows, m.post...)
	for h, n := range m.txh {
		rows = append(rows, posting{key: TxHashKey([]byte(h)), val: 0, num: n, isTxh: true})
	}
	sort.Slice(rows, func(i, j int) bool { return string(rows[i].key) < string(rows[j].key) })
	for _, r := range rows {
		var val []byte
		if r.isTxh {
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

// HeaderRLP returns a block's header RLP verbatim.
func (d *DB) HeaderRLP(height uint64) ([]byte, bool, error) {
	return d.chainRow(famHdr, height, true)
}

// Pvm returns a block's proposervm wrapper bytes verbatim (empty pre-fork).
func (d *DB) Pvm(height uint64) ([]byte, bool, error) {
	return d.chainRow(famPvm, height, true)
}

// BlockTxRange returns a block's first TxNum and tx count.
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

// TxNumByHash resolves a tx hash to its TxNum. A hash lives in exactly one run,
// so nearly every probe is a bloom miss: the filter's best case.
func (d *DB) TxNumByHash(hash []byte) (uint64, bool, error) {
	if n, ok := d.mem.txNum(hash); ok {
		return n, true, nil
	}
	key := TxHashKey(hash)
	runs := d.snapshot()
	for i := len(runs) - 1; i >= 0; i-- {
		v, ok, err := runs[i].Get(SecLookup, key)
		if err != nil {
			return 0, false, err
		}
		if ok {
			if len(v) != 8 {
				return 0, false, fmt.Errorf("store: txh row is %d bytes, want 8", len(v))
			}
			return beDecode(v), true, nil
		}
	}
	return 0, false, nil
}

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

// Code resolves a code blob by hash: window, then runs newest-to-oldest,
// bloom-gated. ONE FUNCTION, ALL CALLERS.
func (d *DB) Code(hash []byte) ([]byte, bool, error) {
	if b, ok := d.mem.codeBlob(hash); ok {
		return b, true, nil
	}
	key := CodeKey(hash)
	runs := d.snapshot()
	for i := len(runs) - 1; i >= 0; i-- {
		v, ok, err := runs[i].Get(SecState, key)
		if err != nil || ok {
			return v, ok, err
		}
	}
	return nil, false, nil
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

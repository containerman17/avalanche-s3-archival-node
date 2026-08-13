package store

import (
	"container/heap"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/cockroachdb/pebble/v2/sstable"
	"github.com/containerman17/epochdb/dist"
)

// THE DETERMINISTIC MERGER (DESIGN rule 4): THERE IS ONE MERGE LEVEL. The L0
// runs since the last terminal merge into one TERMINAL run at the tx boundary
// known in advance, and a terminal run NEVER MERGES AGAIN. A merge is a PURE
// FUNCTION of its inputs, so two independent builders produce the same bytes
// and the same casfs name, and a consumer holding the small runs can recompute
// the merged run locally instead of downloading it.
//
// THE BOUNDARY IS EIGHT MILLION TRANSACTIONS, and the fan-in is what that comes
// to at the flush trigger: 8,000,000 / 500,000 = SIXTEEN L0 runs on a chain
// dense enough to flush by transactions, which is every chain that will ever
// earn a terminal (mainnet C: ~190 terminals over 1.5B txs, artifacts of
// 5-10GB). TxNum is the trigger rather than the run count because TxNum is THE
// dimension: a sparse chain flushes on the 50,000-block trigger instead, and
// counting runs there would cut tiny terminals out of a chain that has not
// earned one. numine at 1.07M txs has twenty L0 runs and no terminal, which is
// exactly the intent: it publishes nothing and a joiner takes it over p2p in
// twenty minutes.
//
// THE SPAN IS ALWAYS THE NEWEST RUNS, and that is not a preference: a run's
// footer names the PREVIOUS run, so the live set is a prev-linked list, and
// replacing a span in the middle of a linked list orphans its successor. The
// newest span has no successor, so merging there keeps the list intact at every
// instant, which is what the join walk (join.go) reads. It is also why the span
// is EVERY L0 since the last terminal rather than a fixed count: the terminals
// then tile the chain from TxNum 0 with no gap, which is what lets the
// published manifest list terminals alone and still walk back to the chain
// root.
//
// RETIREMENT IS PUBLISH-BEFORE-DELETE (DESIGN, "all data must always be safe"):
// the merged run is written, fsynced, reopened and verified against the rows
// that went into it; only then does the manifest swap; only after the swap do
// the inputs' LOCAL copies go. Nothing in the bucket is ever deleted, because
// nothing superseded was ever uploaded.
const (
	// TerminalTxs is the terminal boundary, a pinned format constant: move it
	// and two builders cut different runs.
	TerminalTxs = 8_000_000
	// MergeFanIn is that boundary in runs on a tx-triggered chain. It is a
	// derived number, stated because it is the shape everything is sized for.
	MergeFanIn = TerminalTxs / FlushTxs
	// TerminalLevel is the level a merged run carries. There is exactly one
	// merge level, so "level 1" and "terminal" are the same statement.
	TerminalLevel = 1
)

// mergeCrash is a TEST HOOK and nothing else. A crash test re-executes this
// binary with EPOCHDB_MERGE_CRASH set to a stage name and the process dies
// there, which is the only honest way to test a kill: an in-process fake would
// still run deferred cleanups a SIGKILL never runs.
var mergeCrash = func(stage string) {
	if os.Getenv("EPOCHDB_MERGE_CRASH") == stage {
		log.Printf("store: EPOCHDB_MERGE_CRASH=%s, dying here on purpose", stage)
		os.Exit(9)
	}
}

// MaybeMerge cuts a terminal run when the L0 tail has reached the boundary.
// There is one level, so one merge is all there can ever be to do.
func (d *DB) MaybeMerge() error {
	start, ok := d.mergeSpan()
	if !ok {
		return nil
	}
	return d.merge(start)
}

// mergeSpan reports where the L0 tail starts, once that tail holds a terminal
// run's worth of transactions. That is the trigger and the whole schedule.
func (d *DB) mergeSpan() (start int, ok bool) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	runs := d.man.Runs
	start = len(runs)
	for start > 0 && runs[start-1].Level == 0 {
		start--
	}
	if start == len(runs) {
		return 0, false
	}
	if runs[len(runs)-1].ToTx-runs[start].FromTx < d.terminalTxs {
		return 0, false
	}
	return start, true
}

// merge folds every L0 run from start onward into one terminal run.
func (d *DB) merge(start int) error {
	d.mu.RLock()
	refs := append([]RunRef(nil), d.man.Runs[start:]...)
	inputs := append([]*Run(nil), d.runs[start:]...)
	prev := d.chainRoot
	if start > 0 {
		var err error
		if prev, err = decodeRoot(d.man.Runs[start-1].Name); err != nil {
			d.mu.RUnlock()
			return err
		}
	}
	d.mu.RUnlock()

	from, to := refs[0], refs[len(refs)-1]
	for i := 1; i < len(refs); i++ {
		if refs[i].FromTx != refs[i-1].ToTx || refs[i].FromHeight != refs[i-1].ToHeight+1 {
			return fmt.Errorf("store: merge inputs are not contiguous: %v then %v", refs[i-1], refs[i])
		}
	}

	t0 := time.Now()
	// A TERMINAL RUN IS BUILT IN THE SPOOL, which is the one directory Sync
	// uploads from: this run is the only artifact of the whole engine that ever
	// reaches the bucket.
	path := RunFileName(d.cas.SpoolDir(), TerminalLevel, from.FromTx, to.ToTx)
	w, err := NewRunWriter(path, prev, TerminalLevel)
	if err != nil {
		return err
	}
	var rows [numSections]uint64
	for s := Section(0); s < numSections; s++ {
		if err := w.Begin(s); err != nil {
			w.Abort()
			return err
		}
		n, err := mergeSection(inputs, s, w)
		if err != nil {
			w.Abort()
			return err
		}
		rows[s] = n
		if err := w.End(); err != nil {
			w.Abort()
			return err
		}
	}
	// WRITTEN AND FSYNCED (RunWriter.Finish syncs before the adopting rename).
	name, _, err := w.Finish(d.cas, from.FromTx, to.ToTx, from.FromHeight, to.ToHeight)
	if err != nil {
		w.Abort()
		return err
	}
	mergeCrash("written")

	// VERIFIED: reopen the merged run and count every row back out of it. A
	// merged run that cannot be read back is not a replacement for anything,
	// and nothing below may retire until this passes.
	merged, err := OpenRun(d.cas, name)
	if err != nil {
		return fmt.Errorf("store: merged run %s does not reopen: %w", name, err)
	}
	if err := verifyRun(merged, rows, from, to); err != nil {
		merged.Close()
		return err
	}
	mergeCrash("verified")

	// PUBLISH: the manifest lands durably, then the in-memory snapshot swaps.
	ref := RunRef{FromTx: from.FromTx, ToTx: to.ToTx, FromHeight: from.FromHeight, ToHeight: to.ToHeight, Name: name, Level: TerminalLevel}
	d.mu.Lock()
	old := d.man.Runs
	d.man.Runs = append(append([]RunRef(nil), old[:start]...), ref)
	if err := d.man.save(d.dir); err != nil {
		d.man.Runs = old
		d.mu.Unlock()
		merged.Close()
		return err
	}
	d.runs = append(d.runs[:start:start], merged)
	d.mu.Unlock()
	mergeCrash("swapped")

	// ONLY NOW may the inputs go. They only ever existed locally, so this is an
	// unlink of files nobody else has a copy of, and there is nothing in any
	// bucket to reconcile: what is published is final, forever.
	for i, r := range inputs {
		r.Close()
		if err := d.cas.DropLocal(refs[i].Name); err != nil {
			return fmt.Errorf("store: retire run %s: %w", refs[i].Name, err)
		}
	}
	mergeCrash("deleted")
	log.Printf("store: merged %d L0 runs into terminal run %s [tx %d..%d, blocks %d..%d]: %d chain + %d state + %d lookup rows in %s",
		len(inputs), name, from.FromTx, to.ToTx, from.FromHeight, to.ToHeight,
		rows[SecChain], rows[SecState], rows[SecLookup], time.Since(t0).Round(time.Millisecond))
	return nil
}

// verifyRun re-reads a freshly written run and checks it holds exactly the rows
// and the range it was built from.
func verifyRun(r *Run, rows [numSections]uint64, from, to RunRef) error {
	f := r.Footer
	if f.FromTx != from.FromTx || f.ToTx != to.ToTx || f.FromHeight != from.FromHeight || f.ToHeight != to.ToHeight {
		return fmt.Errorf("store: merged run %s covers tx [%d,%d) blocks [%d,%d], want tx [%d,%d) blocks [%d,%d]",
			r.Name, f.FromTx, f.ToTx, f.FromHeight, f.ToHeight, from.FromTx, to.ToTx, from.FromHeight, to.ToHeight)
	}
	for s := Section(0); s < numSections; s++ {
		var n uint64
		if err := r.ScanRange(s, nil, nil, func(k, v []byte) bool { n++; return true }); err != nil {
			return fmt.Errorf("store: merged run %s section %v does not read back: %w", r.Name, s, err)
		}
		if n != rows[s] {
			return fmt.Errorf("store: merged run %s section %v reads back %d rows, %d went in", r.Name, s, n, rows[s])
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// the k-way merge
// ---------------------------------------------------------------------------

// cursor is one input run's position in one section.
type cursor struct {
	idx      int // input index: higher is newer, and newer wins a tie
	it       sstable.Iterator
	key, val []byte
}

// next advances the cursor. The two bodies below are duplicated on purpose:
// pebble's *base.InternalKV lives in an internal package, so the type cannot be
// named in a shared helper's signature, and the KV is only valid until the next
// positioning call anyway, which is why the bytes are copied here.
func (c *cursor) next() error {
	kv := c.it.Next()
	if kv == nil {
		c.key, c.val = nil, nil
		return nil
	}
	raw, _, err := kv.Value(nil)
	if err != nil {
		return err
	}
	c.key = append(c.key[:0], kv.K.UserKey...)
	c.val = append(c.val[:0], raw...)
	return nil
}

// first positions the cursor on the section's first row.
func (c *cursor) first() error {
	kv := c.it.First()
	if kv == nil {
		c.key, c.val = nil, nil
		return nil
	}
	raw, _, err := kv.Value(nil)
	if err != nil {
		return err
	}
	c.key = append(c.key[:0], kv.K.UserKey...)
	c.val = append(c.val[:0], raw...)
	return nil
}

type cursorHeap []*cursor

func (h cursorHeap) Len() int { return len(h) }
func (h cursorHeap) Less(i, j int) bool {
	if c := Comparer.Compare(h[i].key, h[j].key); c != 0 {
		return c < 0
	}
	// A tie is only ever a code/ row, which is content-addressed and therefore
	// the same bytes in both runs. Newest first anyway, so the merged run
	// answers exactly what the descent would have answered.
	return h[i].idx > h[j].idx
}
func (h cursorHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *cursorHeap) Push(x any)   { *h = append(*h, x.(*cursor)) }
func (h *cursorHeap) Pop() any     { old := *h; n := len(old); x := old[n-1]; *h = old[:n-1]; return x }

// mergeSection streams one section of every input into w in key order and
// returns the row count. Ranges never overlap in the chain section, so this is
// concatenation there; the state and lookup families genuinely interleave.
func mergeSection(inputs []*Run, s Section, w *RunWriter) (uint64, error) {
	var h cursorHeap
	defer func() {
		for _, c := range h {
			c.it.Close()
		}
	}()
	for i, r := range inputs {
		it, err := r.newIter(s)
		if err != nil {
			return 0, err
		}
		c := &cursor{idx: i, it: it}
		if err := c.first(); err != nil {
			it.Close()
			return 0, err
		}
		if c.key == nil {
			it.Close()
			continue
		}
		h = append(h, c)
	}
	heap.Init(&h)
	var n uint64
	var last []byte
	for len(h) > 0 {
		c := h[0]
		if last == nil || !Comparer.Equal(last, c.key) {
			if err := w.Set(c.key, c.val); err != nil {
				return 0, err
			}
			last = append(last[:0], c.key...)
			n++
		}
		if err := c.next(); err != nil {
			return 0, err
		}
		if c.key == nil {
			c.it.Close()
			h[0] = h[len(h)-1]
			h = h[:len(h)-1]
			heap.Init(&h)
			continue
		}
		heap.Fix(&h, 0)
	}
	return n, nil
}

// RecomputeMerge rebuilds the merged run of the named inputs in dir and returns
// the casfs name it lands under, WITHOUT touching any manifest. This is
// DESIGN's "compaction without re-download": a consumer holding the small runs
// runs this and compares the name to what the producer published.
func RecomputeMerge(cas *dist.Store, dir string, prev [32]byte, names []RunName) (RunName, error) {
	inputs := make([]*Run, 0, len(names))
	defer func() {
		for _, r := range inputs {
			r.Close()
		}
	}()
	for _, n := range names {
		r, err := OpenRun(cas, n)
		if err != nil {
			return "", err
		}
		inputs = append(inputs, r)
	}
	first, last := inputs[0].Footer, inputs[len(inputs)-1].Footer
	w, err := NewRunWriter(RunFileName(dir, TerminalLevel, first.FromTx, last.ToTx), prev, TerminalLevel)
	if err != nil {
		return "", err
	}
	for s := Section(0); s < numSections; s++ {
		if err := w.Begin(s); err != nil {
			w.Abort()
			return "", err
		}
		if _, err := mergeSection(inputs, s, w); err != nil {
			w.Abort()
			return "", err
		}
		if err := w.End(); err != nil {
			w.Abort()
			return "", err
		}
	}
	name, _, err := w.Finish(cas, first.FromTx, last.ToTx, first.FromHeight, last.ToHeight)
	if err != nil {
		w.Abort()
		return "", err
	}
	return name, nil
}

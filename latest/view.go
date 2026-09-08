package latest

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"slices"
	"sync"
)

// Overlay holds the writes since the last checkpoint. An empty value is a
// tombstone: it shadows the runs below and Merge drops the key.
// Single writer; readers may run concurrently with it.
//
// Entries live in one byte slab behind a pointer-free index (fixed-size
// array keys), so the GC has nothing to scan here. A slab entry is
// [klen u8][vlen u8][vcap u8][key][value, padded to vcap]; a Put of an
// existing key rewrites the value in place when it fits, else appends a
// new entry and leaves the old one dead. ponytail: dead entries are never
// reclaimed (a value outgrows its 16-byte slack rarely); compact if the
// slab ever runs far past Bytes.
type Overlay struct {
	mu    sync.RWMutex
	idx   map[[fixedKey]byte]uint64 // [len u8][key...] zero padded -> slab offset
	long  map[string]uint64         // keys of fixedKey-1 bytes or more
	slab  []byte
	bytes int
}

// entryOverhead is the accounted per-entry cost on top of key+value bytes.
const entryOverhead = 64

// fixedKey is the index key width: keys shorter than it are inlined.
const fixedKey = 72

func NewOverlay() *Overlay {
	return &Overlay{idx: map[[fixedKey]byte]uint64{}, long: map[string]uint64{}}
}

func fixed(key []byte) (k [fixedKey]byte, ok bool) {
	if len(key) >= fixedKey {
		return k, false
	}
	k[0] = byte(len(key))
	copy(k[1:], key)
	return k, true
}

func (o *Overlay) find(key []byte) (uint64, bool) {
	if k, ok := fixed(key); ok {
		off, ok := o.idx[k]
		return off, ok
	}
	off, ok := o.long[string(key)]
	return off, ok
}

func (o *Overlay) set(key []byte, off uint64) {
	if k, ok := fixed(key); ok {
		o.idx[k] = off
	} else {
		o.long[string(key)] = off
	}
}

func keyAt(slab []byte, off uint64) []byte {
	return slab[off+3 : off+3+uint64(slab[off])]
}

func valueAt(slab []byte, off uint64) []byte {
	k := off + 3 + uint64(slab[off])
	return slab[k : k+uint64(slab[off+1])]
}

var zeros [maxValue]byte

// Put copies key and value in. An empty value is a tombstone.
func (o *Overlay) Put(key, value []byte) {
	if len(key) == 0 || len(key) > maxKey || len(value) > maxValue {
		panic(fmt.Sprintf("latest: key %d bytes, value %d bytes (limits 1..%d and 0..%d)", len(key), len(value), maxKey, maxValue))
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if off, ok := o.find(key); ok {
		o.bytes += len(value) - int(o.slab[off+1])
		if len(value) <= int(o.slab[off+2]) {
			o.slab[off+1] = byte(len(value))
			copy(o.slab[off+3+uint64(len(key)):], value)
			return
		}
	} else {
		o.bytes += len(key) + len(value) + entryOverhead
	}
	vcap := min(maxValue, (len(value)+15)&^15)
	off := uint64(len(o.slab))
	o.slab = append(o.slab, byte(len(key)), byte(len(value)), byte(vcap))
	o.slab = append(o.slab, key...)
	o.slab = append(o.slab, value...)
	o.slab = append(o.slab, zeros[:vcap-len(value)]...)
	o.set(key, off)
}

// Get returns the stored value; tombstone is true when the key is present
// as a deletion. val aliases the slab: a later Put of the same key may
// rewrite it in place, so use it before that.
func (o *Overlay) Get(key []byte) (val []byte, ok, tombstone bool) {
	o.mu.RLock()
	off, ok := o.find(key)
	if ok {
		val = valueAt(o.slab, off)
	}
	o.mu.RUnlock()
	return val, ok, ok && len(val) == 0
}

func (o *Overlay) Len() int {
	o.mu.RLock()
	defer o.mu.RUnlock()
	return len(o.idx) + len(o.long)
}

// Bytes is the accounted size: key + value + entryOverhead per entry.
func (o *Overlay) Bytes() int {
	o.mu.RLock()
	defer o.mu.RUnlock()
	return o.bytes
}

// Iter walks a sorted snapshot of the keys in [lo, hi) taken now; nil means
// unbounded. Tombstones are included (empty values). Values are read from
// the slab as the iterator advances.
func (o *Overlay) Iter(lo, hi []byte) Iterator {
	in := func(k []byte) bool {
		return (lo == nil || bytes.Compare(k, lo) >= 0) && (hi == nil || bytes.Compare(k, hi) < 0)
	}
	o.mu.RLock()
	slab := o.slab
	offs := make([]uint64, 0, len(o.idx)+len(o.long))
	for k, off := range o.idx {
		if in(k[1 : 1+k[0]]) {
			offs = append(offs, off)
		}
	}
	for k, off := range o.long {
		if in([]byte(k)) {
			offs = append(offs, off)
		}
	}
	o.mu.RUnlock()
	slices.SortFunc(offs, func(a, b uint64) int { return bytes.Compare(keyAt(slab, a), keyAt(slab, b)) })
	return &slabIter{slab: slab, offs: offs, i: -1}
}

type slabIter struct {
	slab []byte
	offs []uint64
	i    int
}

func (it *slabIter) Next() bool {
	it.i++
	return it.i < len(it.offs)
}

// Key and Value alias the slab; callers must not write to them.
func (it *slabIter) Key() []byte   { return keyAt(it.slab, it.offs[it.i]) }
func (it *slabIter) Value() []byte { return valueAt(it.slab, it.offs[it.i]) }
func (it *slabIter) Err() error    { return nil }

// View is overlays (newest first, may be empty) over runs, newest first.
type View struct {
	overlays []*Overlay
	runs     []*Run
}

func NewView(o *Overlay, runs ...*Run) *View {
	if o == nil {
		return &View{runs: runs}
	}
	return &View{overlays: []*Overlay{o}, runs: runs}
}

// NewMultiView stacks several overlays (newest first) over runs: a fresh
// overlay over a frozen one being merged, over the base.
func NewMultiView(overlays []*Overlay, runs ...*Run) *View {
	return &View{overlays: overlays, runs: runs}
}

// Get consults the overlays, then the runs in order. A tombstone or an empty
// value at any level ends the descent as not found. val aliases the level
// it came from (see Overlay.Get and Run.Get).
func (v *View) Get(key []byte) (val []byte, ok bool) {
	for _, o := range v.overlays {
		if val, ok, dead := o.Get(key); ok {
			if dead {
				return nil, false
			}
			return val, true
		}
	}
	for _, r := range v.runs {
		if val, ok := r.Get(key); ok {
			if len(val) == 0 {
				return nil, false
			}
			return val, true
		}
	}
	return nil, false
}

// Iter merges all levels over [lo, hi), newest winning, tombstones and
// empty values dropped.
func (v *View) Iter(lo, hi []byte) Iterator {
	var its []Iterator
	for _, o := range v.overlays {
		its = append(its, o.Iter(lo, hi))
	}
	for _, r := range v.runs {
		its = append(its, r.Iter(lo, hi))
	}
	m := &mergeIter{its: its, live: make([]bool, len(its))}
	for i, it := range its {
		m.live[i] = it.Next()
	}
	return m
}

// mergeIter is a k-way merge over a handful of levels. ponytail: linear
// scan of the heads per step, a heap if k ever grows past a few runs.
type mergeIter struct {
	its      []Iterator // newest first
	live     []bool
	key, val []byte // key is the merge's own copy, so advancing a level cannot change it
	have     bool
	err      error
}

func (m *mergeIter) Next() bool {
	for {
		if m.have { // advance every level still sitting on the emitted key
			for i, it := range m.its {
				if m.live[i] && bytes.Equal(it.Key(), m.key) {
					m.live[i] = it.Next()
					if err := it.Err(); err != nil {
						m.err = err
						return false
					}
				}
			}
		}
		best := -1
		for i, it := range m.its {
			if m.live[i] && (best < 0 || bytes.Compare(it.Key(), m.its[best].Key()) < 0) {
				best = i
			}
		}
		if best < 0 {
			m.have = false
			return false
		}
		m.key = append(m.key[:0], m.its[best].Key()...)
		m.val = m.its[best].Value()
		m.have = true
		if len(m.val) > 0 {
			return true
		}
	}
}

func (m *mergeIter) Key() []byte   { return m.key }
func (m *mergeIter) Value() []byte { return m.val }
func (m *mergeIter) Err() error    { return m.err }

// Merge writes the view's merged contents as a new run at dst (the
// checkpoint), streaming, and opens it. user goes into the footer.
func Merge(dst string, v *View, user [32]byte) (*Run, error) {
	w, err := NewWriter(dst)
	if err != nil {
		return nil, err
	}
	w.SetUserData(user)
	it := v.Iter(nil, nil)
	for it.Next() {
		if err = w.Add(it.Key(), it.Value()); err != nil {
			break
		}
	}
	if err == nil {
		err = it.Err()
	}
	if err != nil {
		w.f.Close()
		os.Remove(dst)
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, errors.Join(err, os.Remove(dst))
	}
	return Open(dst)
}

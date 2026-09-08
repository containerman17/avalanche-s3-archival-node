package latest

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"sync"
	"unsafe"
)

// Overlay holds the writes since the last checkpoint. An empty value is a
// tombstone: it shadows the runs below and Merge drops the key.
// Single writer; readers may run concurrently with it.
type Overlay struct {
	mu    sync.RWMutex
	m     map[string][]byte
	bytes int
}

// entryOverhead is the accounted per-entry cost on top of key+value bytes.
const entryOverhead = 64

func NewOverlay() *Overlay { return &Overlay{m: map[string][]byte{}} }

// Put copies key and value in. An empty value is a tombstone.
func (o *Overlay) Put(key, value []byte) {
	if len(key) == 0 || len(key) > maxKey || len(value) > maxValue {
		panic(fmt.Sprintf("latest: key %d bytes, value %d bytes (limits 1..%d and 0..%d)", len(key), len(value), maxKey, maxValue))
	}
	v := append(make([]byte, 0, len(value)), value...)
	o.mu.Lock()
	if old, ok := o.m[string(key)]; ok {
		o.bytes -= len(key) + len(old) + entryOverhead
	}
	o.m[string(key)] = v
	o.bytes += len(key) + len(value) + entryOverhead
	o.mu.Unlock()
}

// Get returns the stored value; tombstone is true when the key is present
// as a deletion. val aliases the stored copy, which is never mutated, so it
// stays valid after later Puts.
func (o *Overlay) Get(key []byte) (val []byte, ok, tombstone bool) {
	o.mu.RLock()
	val, ok = o.m[string(key)]
	o.mu.RUnlock()
	return val, ok, ok && len(val) == 0
}

func (o *Overlay) Len() int {
	o.mu.RLock()
	defer o.mu.RUnlock()
	return len(o.m)
}

// Bytes is the accounted size: key + value + entryOverhead per entry.
func (o *Overlay) Bytes() int {
	o.mu.RLock()
	defer o.mu.RUnlock()
	return o.bytes
}

type kv struct {
	k string
	v []byte
}

// Iter walks a sorted snapshot of [lo, hi) taken now; nil means unbounded.
// Tombstones are included (empty values).
func (o *Overlay) Iter(lo, hi []byte) Iterator {
	los, his := string(lo), string(hi)
	o.mu.RLock()
	s := make([]kv, 0, len(o.m))
	for k, v := range o.m {
		if (lo == nil || k >= los) && (hi == nil || k < his) {
			s = append(s, kv{k, v})
		}
	}
	o.mu.RUnlock()
	slices.SortFunc(s, func(a, b kv) int { return strings.Compare(a.k, b.k) })
	return &sliceIter{s: s, i: -1}
}

type sliceIter struct {
	s []kv
	i int
}

func (it *sliceIter) Next() bool {
	it.i++
	return it.i < len(it.s)
}

// Key aliases the string's bytes; callers must not write to it.
func (it *sliceIter) Key() []byte {
	k := it.s[it.i].k
	return unsafe.Slice(unsafe.StringData(k), len(k))
}
func (it *sliceIter) Value() []byte { return it.s[it.i].v }
func (it *sliceIter) Err() error    { return nil }

// View is an overlay (may be nil) over runs, newest first.
type View struct {
	overlay *Overlay
	runs    []*Run
}

func NewView(o *Overlay, runs ...*Run) *View { return &View{overlay: o, runs: runs} }

// Get consults the overlay, then the runs in order. A tombstone or an empty
// value at any level ends the descent as not found. val aliases the level
// it came from (see Overlay.Get and Run.Get).
func (v *View) Get(key []byte) (val []byte, ok bool) {
	if v.overlay != nil {
		if val, ok, dead := v.overlay.Get(key); ok {
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
	if v.overlay != nil {
		its = append(its, v.overlay.Iter(lo, hi))
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

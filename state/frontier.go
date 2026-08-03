package state

// The BOOTSTRAP FRONTIER MERGE (DESIGN.md ruling 1 of 2026-07-31, which
// killed the published snapshot): a node that downloaded only epochs builds
// its state frontier at H by merging the epochs THEMSELVES, one linear pass
// per epoch.
//
// Every epoch's SST rows are sorted by (key53, block), so the whole corpus is
// one k-way merge over ~120 cursors marching the same key space: rows for a
// key arrive oldest first across all epochs, and the last row at or below H
// wins. One decoded SST block is resident per cursor (64KB), so the working
// set is bounded by the epoch COUNT, not by the corpus size, and no SST block
// is ever revisited.
//
// TOMBSTONES follow the descent's rules (overlay.go search/StorageAt), which
// they must, because the frontier has to equal what the descent answers at H:
//   - an account row with an empty value is a SELFDESTRUCT; the account is
//     gone unless a later row recreates it,
//   - a storage row is dead if the account was destructed AFTER it was
//     written, exactly History.lastAccountDelete's rule. 'a' < 'c' < 's' in
//     the one sorted key space, so every account row of the corpus is merged
//     before the first storage row and the delete index is complete by then.
//   - 'c' rows are code blobs, not state: skipped. Firewood holds accounts and
//     storage only, and a bootstrapped node reads code out of the epochs.

import (
	"bytes"
	"container/heap"
	"encoding/binary"
	"fmt"

	"github.com/containerman17/epochdb/dist"

	"github.com/ava-labs/libevm/common"
)

// FrontierRow is one winning row of the merge: the newest post-image of key
// at or below the target height. An EMPTY Value means the key is NOT part of
// the frontier (an explicit delete, a zero write, or storage killed by a
// later SELFDESTRUCT); the caller only has to act on those for keys the
// genesis alloc put in the trie.
//
// Key and Value alias decode buffers: copy to retain.
type FrontierRow struct {
	Key   []byte
	Value []byte
	Block uint64
}

// sstCursor is a pull iterator over one epoch's SST rows in (key, block)
// order, holding exactly one decoded SST block at a time.
type sstCursor struct {
	e        *Epoch
	idx      *dist.View
	nEntries int
	bi       int // next sparse-index block to decode
	raw      []byte
	pos      int

	key   []byte
	block uint64
	val   []byte
}

func newSSTCursor(e *Epoch) *sstCursor {
	idx := e.sec[secSSTIdx]
	return &sstCursor{e: e, idx: idx, nEntries: int(vlen(idx)) / sstIdxEntrySize}
}

// next advances to the following row. ok=false = the epoch is exhausted.
func (c *sstCursor) next() (ok bool, err error) {
	for c.pos >= len(c.raw) {
		if c.bi >= c.nEntries {
			return false, nil
		}
		if c.raw, err = c.e.sstBlock(c.idx, c.bi, c.nEntries); err != nil {
			return false, fmt.Errorf("epoch_%d_%d: sst block %d: %w", c.e.Start, c.e.Count, c.bi, err)
		}
		c.bi++
		c.pos = 0
	}
	p := c.pos
	c.key = c.raw[p : p+sortedKeySize]
	c.block = binary.BigEndian.Uint64(c.raw[p+sortedKeySize:])
	vlen, vn := binary.Uvarint(c.raw[p+sortedKeySize+8:])
	vstart := p + sortedKeySize + 8 + vn
	c.val = nil
	if vlen > 0 {
		c.val = c.raw[vstart : vstart+int(vlen)]
	}
	c.pos = vstart + int(vlen)
	return true, nil
}

// cursorHeap orders live cursors by (key, block): the merge pops rows in
// exactly the order one giant sorted SST would have.
type cursorHeap []*sstCursor

func (h cursorHeap) Len() int { return len(h) }
func (h cursorHeap) Less(i, j int) bool {
	if c := bytes.Compare(h[i].key, h[j].key); c != 0 {
		return c < 0
	}
	return h[i].block < h[j].block
}
func (h cursorHeap) Swap(i, j int)         { h[i], h[j] = h[j], h[i] }
func (h *cursorHeap) Push(x any)           { *h = append(*h, x.(*sstCursor)) }
func (h *cursorHeap) Pop() (x any)         { old := *h; x = old[len(old)-1]; *h = old[:len(old)-1]; return x }
func (h cursorHeap) peek() *sstCursor      { return h[0] }
func (h cursorHeap) sameKey(k []byte) bool { return len(h) > 0 && bytes.Equal(h[0].key, k) }

// MergeFrontier k-way merges the epochs' SST sections and calls fn once per
// distinct state key with the newest row at or below h, in key order:
// accounts first, then storage. Rows above h and code rows are skipped;
// tombstoned keys are reported with a nil Value.
//
// RAM: one decoded SST block per epoch, plus the SELFDESTRUCT index (one
// entry per account ever destructed, built while the account rows stream
// past). Nothing else accumulates.
func MergeFrontier(epochs []*Epoch, h uint64, fn func(FrontierRow) error) error {
	hp := make(cursorHeap, 0, len(epochs))
	for _, e := range epochs {
		if e.Start > h {
			continue
		}
		c := newSSTCursor(e)
		ok, err := c.next()
		if err != nil {
			return err
		}
		if ok {
			hp = append(hp, c)
		}
	}
	heap.Init(&hp)

	// destroyed: address -> highest SELFDESTRUCT block at or below h. Built
	// during the account phase, read during the storage phase.
	destroyed := make(map[common.Address]uint64)

	advance := func() error {
		c := hp.peek()
		ok, err := c.next()
		if err != nil {
			return err
		}
		if ok {
			heap.Fix(&hp, 0)
		} else {
			heap.Pop(&hp)
		}
		return nil
	}

	// Both buffers are reused across keys: the winning row is copied out of
	// its cursor because advancing that cursor may decode the next SST block
	// over it. fn must not retain either (documented on FrontierRow).
	var keyBuf, valBuf []byte
	for len(hp) > 0 {
		// Collect every row of the smallest key; the last one at or below h
		// wins (the heap yields ascending blocks within a key).
		key := append(keyBuf[:0], hp.peek().key...)
		keyBuf = key
		var win FrontierRow
		seen := false
		for hp.sameKey(key) {
			c := hp.peek()
			if c.block <= h {
				valBuf = append(valBuf[:0], c.val...)
				win = FrontierRow{Key: key, Value: valBuf, Block: c.block}
				seen = true
				if key[0] == recKindAccount && len(c.val) == 0 {
					addr := common.Address(key[1:21])
					if c.block > destroyed[addr] {
						destroyed[addr] = c.block
					}
				}
			}
			if err := advance(); err != nil {
				return err
			}
		}
		if !seen || key[0] == recKindCodeUse {
			continue
		}
		if key[0] == recKindStorage && len(win.Value) > 0 {
			if d, ok := destroyed[common.Address(key[1:21])]; ok && d > win.Block {
				win.Value = nil // written, then the account was destructed
			}
		}
		if err := fn(win); err != nil {
			return err
		}
	}
	return nil
}

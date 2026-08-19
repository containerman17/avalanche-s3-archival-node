package store

import (
	"bytes"
	"container/heap"
	"fmt"
)

// THE FRONTIER MERGE (DESIGN, "Execution and state history"): a k-way merge
// over the runs' STATE FAMILY that yields the LATEST VALUE PER KEY at a TxNum,
// streamed out in key order so the caller can push it straight into Firewood in
// batches and never hold the whole state in RAM.
//
// It works because the key IS the ordering: every row of one account or slot
// sits in one contiguous, TxNum-ascending range, so the winner of a group is
// simply the last row of that group at or below the target, and the group ends
// when the prefix changes.
//
// RUNS ONLY, no window: a node that just joined has nothing unflushed, and a
// node that has been executing does not need a frontier built for it.

// FrontierRow is one key's winning value.
type FrontierRow struct {
	// Key is the row's key WITHOUT the 8-byte TxNum suffix, i.e. one of
	// state/<addr>/a/, state/<addr>/c/, state/<addr>/s/<slot>/.
	Key   []byte
	TxNum uint64
	// Val is the post-tx value. EMPTY MEANS DELETED for an account row and
	// CLEARED (which the EVM defines as zero) for a slot row; the caller must
	// not collapse the two into "absent".
	Val []byte
}

// MergeFrontier calls fn once per state key, in key order, with that key's
// value at TxNum atTx.
func (d *DB) MergeFrontier(atTx uint64, fn func(FrontierRow) error) error {
	runs, done := d.snapshot()
	defer done()
	var h cursorHeap
	defer func() {
		for _, c := range h {
			c.it.Close()
		}
	}()
	for i, r := range runs {
		if r.Footer.FromTx > atTx {
			continue
		}
		it, err := r.newIter(SecState)
		if err != nil {
			return err
		}
		c := &cursor{idx: i, it: it}
		if err := c.first(); err != nil {
			it.Close()
			return err
		}
		if c.key == nil {
			it.Close()
			continue
		}
		h = append(h, c)
	}
	heap.Init(&h)

	var held FrontierRow
	var have bool
	emit := func() error {
		if !have {
			return nil
		}
		have = false
		return fn(held)
	}
	for len(h) > 0 {
		c := h[0]
		// code/ rows are not state: they are the code blobs that ride in the
		// same section, and the frontier trie has no place for them.
		if bytes.HasPrefix(c.key, []byte(PrefixState)) && len(c.key) > 8 {
			key, num := c.key[:len(c.key)-8], TxNumOf(c.key)
			if !have || !bytes.Equal(held.Key, key) {
				if err := emit(); err != nil {
					return err
				}
			}
			if num <= atTx {
				held = FrontierRow{
					Key:   append(held.Key[:0], key...),
					TxNum: num,
					Val:   append(held.Val[:0], c.val...),
				}
				have = true
			}
		}
		if err := c.next(); err != nil {
			return err
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
	return emit()
}

// TxNumAtEndOf is the highest TxNum that belongs to a block: its BOUNDARY SLOT,
// the one slot past its transactions that every block owns. It is the bound a
// frontier at that height merges to, and the ceiling every state read at that
// height uses, because state written outside any transaction lives there.
func (d *DB) TxNumAtEndOf(height uint64) (uint64, bool, error) {
	first, count, ok, err := d.BlockTxRange(height)
	if err != nil || !ok {
		return 0, ok, err
	}
	return first + uint64(count), true, nil
}

// SplitStateKey takes a state row key (without its TxNum suffix) apart into its
// kind and parts: 'a' and 'c' rows carry an address, 's' rows an address and a
// slot.
func SplitStateKey(key []byte) (kind byte, addr, slot []byte, err error) {
	const addrEnd = len(PrefixState) + 20
	if len(key) < addrEnd+3 || !bytes.HasPrefix(key, []byte(PrefixState)) || key[addrEnd] != '/' {
		return 0, nil, nil, fmt.Errorf("store: %q is not a state key", key)
	}
	kind = key[addrEnd+1]
	addr = key[len(PrefixState):addrEnd]
	switch kind {
	case 'a', 'c':
		if len(key) != addrEnd+3 {
			return 0, nil, nil, fmt.Errorf("store: %q is not a state %c key", key, kind)
		}
	case 's':
		if len(key) != addrEnd+3+32+1 {
			return 0, nil, nil, fmt.Errorf("store: %q is not a state s key", key)
		}
		slot = key[addrEnd+3 : addrEnd+3+32]
	default:
		return 0, nil, nil, fmt.Errorf("store: %q has unknown state kind %q", key, kind)
	}
	return kind, addr, slot, nil
}

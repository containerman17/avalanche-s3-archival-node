package state

import (
	"bytes"
	"sync"

	"github.com/ava-labs/libevm/common"
)

// tail is the UNCOOKED-WRITE OVERLAY: every account and storage post-image
// the executor has written since the last cook, newest value wins. It is the
// first half of the ONE read path (user ruling 2026-07-29, "Firewood is a
// dumb hash verifying machine"): `latest` state = this overlay, then the
// historical descent at the cooked watermark. No read anywhere touches
// Firewood.
//
// Written on the executor goroutine (Store.AppendWrites, the single choke
// point every execution path already routes through: per-block, batched and
// SAE), read by every RPC goroutine, pruned on the cook goroutine.
type tail struct {
	mu    sync.RWMutex
	m     map[string]tailEnt        // 53-byte sorted key -> newest write
	del   map[common.Address]uint64 // highest uncooked account delete per addr
	bytes uint64                    // keys + values, for the memory report
}

type tailEnt struct {
	blk uint64
	val []byte // nil = explicit delete (account gone) or zero write
}

func newTail() *tail {
	return &tail{m: make(map[string]tailEnt), del: make(map[common.Address]uint64)}
}

func entBytes(val []byte) uint64 { return uint64(sortedKeySize + len(val)) }

// apply folds one block's write frame into the overlay. Same frame the
// writelog gets, parsed by the same parseFrame: nothing is re-derived.
func (t *tail) apply(block uint64, frame []byte) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	return parseFrame(frame, func(kind byte, key [sortedKeySize]byte, valOff int, vlen uint32) {
		if kind == recKindCodeUse {
			return // code blobs land in code.log, which readers already share live
		}
		if kind == recKindAccount && vlen == 0 {
			// SELFDESTRUCT: remembered separately because a later recreation
			// overwrites the account entry, and the destructed account's
			// storage must still read as gone. Mirrors the descent's
			// lastAccountDelete rule exactly.
			var a common.Address
			copy(a[:], key[1:21])
			if d, ok := t.del[a]; !ok || block > d {
				t.del[a] = block
			}
		}
		k := string(key[:])
		old, had := t.m[k]
		if had && old.blk > block {
			return // out-of-order re-append (crash walk-back): newest wins
		}
		var val []byte
		if vlen > 0 {
			val = bytes.Clone(frame[valOff : valOff+int(vlen)])
		}
		if had {
			t.bytes -= entBytes(old.val)
		}
		t.m[k] = tailEnt{blk: block, val: val}
		t.bytes += entBytes(val)
	})
}

// account returns addr's newest uncooked account post-image. hit=false means
// the descent must answer. An empty val is an explicit delete.
func (t *tail) account(addr common.Address) (val []byte, hit bool) {
	k := string(accountKey(addr))
	t.mu.RLock()
	defer t.mu.RUnlock()
	e, ok := t.m[k]
	return e.val, ok
}

// storage returns (addr, slot)'s newest uncooked value. dead=true (with
// hit=false) means the account was destructed up here and never rewrote this
// slot, so the read is zero and must NOT fall through to the descent, which
// would answer with the pre-destruct value.
func (t *tail) storage(addr common.Address, slot []byte) (val []byte, hit, dead bool) {
	k := string(storageKey(addr, slot))
	t.mu.RLock()
	defer t.mu.RUnlock()
	e, ok := t.m[k]
	d, destructed := t.del[addr]
	if ok && (!destructed || e.blk >= d) {
		return e.val, true, false
	}
	return nil, false, destructed
}

// prune drops every entry at or below through, the cooked watermark
// History.Refresh has just published.
//
// THE RACE ARGUMENT (a reader must never see a window where a write is
// neither here nor cooked). Two facts close it, with no swap and no double
// buffer. (1) prune runs only AFTER Refresh returned, and Refresh publishes
// stateHead only after the new sorted buckets are already swapped in, so
// every entry dropped here is answerable by the descent before the drop.
// (2) the latest read path probes this map FIRST and loads stateHead only
// after the miss; since the store of stateHead happens-before the prune that
// removed the entry, and the load happens-after that prune, the descent
// target the reader picks is >= the pruned entry's block. So a write is
// always in at least one of the two layers, from every reader's point of
// view.
//
// ponytail: one lock held for the whole rebuild; at 900 blk/s catch-up that
// is a few tens of ms of executor stall per cook. Shard the map if it ever
// shows up in a profile.
func (t *tail) prune(through uint64) (entries int, size uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	// Rebuild instead of deleting in place: a Go map never gives its buckets
	// back, and this one grows to catch-up size (tens of thousands of blocks
	// of writes) before the first prune.
	m := make(map[string]tailEnt, len(t.m)/8)
	t.bytes = 0
	for k, e := range t.m {
		if e.blk > through {
			m[k] = e
			t.bytes += entBytes(e.val)
		}
	}
	t.m = m
	for a, d := range t.del {
		if d <= through {
			delete(t.del, a)
		}
	}
	return len(t.m), t.bytes
}

// stats reports the overlay's size for the status line.
func (t *tail) stats() (entries int, size uint64) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.m), t.bytes
}

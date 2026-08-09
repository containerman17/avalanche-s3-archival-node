package state

import (
	"sort"
	"sync"
)

// THE HOT-TAIL LOG INDEX: the unsealed counterpart of the sealed epochs' log
// posting lists. The main /sql and eth_getLogs workload is "the latest N
// events of X" (user ruling 2026-08-09, "mostly ones that just happened"), and
// the newest blocks are exactly the ones no sealed posting list covers. Without
// this, the tail is answered by reading every block's captured logs record in
// the range, which is why the tail half of a query was capped at 10,000 blocks.
//
// It is the SAME SHAPE as the sealed lists (key -> ascending BLOCK numbers, a
// candidate superset, never a positional match), so one caller intersects both
// and the exact filter still runs on the rows.
//
// MEMORY BOUND. Entries are (key, block) pairs, where a key is a 20-byte log
// address or a 32-byte topic value, deduped WITHIN a block by the capture
// format itself (exec's logs frame stores each address and each topic once per
// block). THE TAIL IS AT MOST ONE EPOCH OF TRANSACTIONS, so the pair count is
// bounded by EpochTxsAt(i) (250,000 at the first epoch, 8,000,000 at the flat
// ceiling) times the distinct addresses plus topic values one transaction
// contributes in its own block.
//
// MEASURED on the grotto corpus (a real mainnet L1, 61,515 unsealed blocks,
// 110,555 txs): 6,233 distinct keys, 264,608 pairs, 2.31 MB of posting slices.
// That is 2.39 pairs and 21 BYTES PER TRANSACTION, which puts the bound at
// ~5 MB for a 250k-tx epoch and ~170 MB at the 8M-tx ceiling, plus one map
// entry per distinct key (keys grow far slower than pairs: a key here already
// averages 42 blocks). A chain that emitted four fresh topic values per tx,
// each in its own block, would reach ~5 pairs/tx and roughly double that. It
// is always ONE EPOCH of tail, never the corpus, and the seal gives it back.
//
// The seal retires it: entries at or below the sealed end are dropped, because
// from that moment the epoch's own posting lists answer them AND the raw logs
// records they were built from are unlinked (the fetch floor, see the wiki note
// on sealing retiring staging). Nothing here ever reads below the floor.
//
// ALL DATA MUST ALWAYS BE SAFE (user ruling 2026-08-09). This index is PURELY
// DERIVED: it opens nothing for writing, deletes nothing, truncates nothing,
// and holds no file handles of its own; every byte it reads comes through
// Store.LogsRecord. Its one invariant is that it is EXACT over
// (floor, through]. A read error or a record that will not decode stops the
// catch-up where it is and returns the error, so a query is refused loudly
// rather than answered from a tail with a hole in it; the blocks above
// `through` are then answered by the caller's per-block walk, which fails on
// the same damage rather than skipping it.
type tailLogIndex struct {
	mu    sync.Mutex
	addr  map[[20]byte][]uint64
	topic map[[32]byte][]uint64
	// through is the highest block folded in. It is the logs family's own max
	// block, never the serving head: a header is appended BEFORE its logs
	// record, so a block visible by height may not have its logs on disk yet,
	// and marking it done would drop its events forever.
	through uint64
	floor   uint64 // sealed end: nothing at or below is indexed any more
	pairs   int
}

func newTailLogIndex() *tailLogIndex {
	return &tailLogIndex{addr: map[[20]byte][]uint64{}, topic: map[[32]byte][]uint64{}}
}

// add folds one block's captured record in. Blocks arrive ascending, so each
// posting list stays sorted by construction, and a repeat of the newest block
// is idempotent (which is what makes a re-scan after a restart safe).
func (t *tailLogIndex) add(lr LogRec) {
	for _, a := range lr.Addrs {
		l := t.addr[a]
		if len(l) > 0 && l[len(l)-1] >= lr.Block {
			continue
		}
		t.addr[a] = append(l, lr.Block)
		t.pairs++
	}
	for _, tp := range lr.Topics {
		l := t.topic[tp]
		if len(l) > 0 && l[len(l)-1] >= lr.Block {
			continue
		}
		t.topic[tp] = append(l, lr.Block)
		t.pairs++
	}
}

// retire drops everything at or below the sealed end. Posting lists are
// ascending, so the survivors are one suffix per key.
func (t *tailLogIndex) retire(sealedEnd uint64) {
	if sealedEnd <= t.floor {
		return
	}
	t.floor = sealedEnd
	if t.through < sealedEnd {
		t.through = sealedEnd
	}
	for k, l := range t.addr {
		i := sort.Search(len(l), func(i int) bool { return l[i] > sealedEnd })
		t.pairs -= i
		if i == len(l) {
			delete(t.addr, k)
			continue
		}
		t.addr[k] = l[i:]
	}
	for k, l := range t.topic {
		i := sort.Search(len(l), func(i int) bool { return l[i] > sealedEnd })
		t.pairs -= i
		if i == len(l) {
			delete(t.topic, k)
			continue
		}
		t.topic[k] = l[i:]
	}
}

// blocks intersects the index the way epochLogCandidates intersects the sealed
// lists: union over addresses, AND union per topic position, clipped to
// [lo, hi]. No matcher at all means "every block in range", which the caller
// resolves itself (this index cannot enumerate "any log" cheaply and does not
// try).
func (t *tailLogIndex) blocks(lo, hi uint64, addrs [][20]byte, topics [][][32]byte) []uint64 {
	var sets []map[uint64]bool
	if len(addrs) > 0 {
		set := map[uint64]bool{}
		for _, a := range addrs {
			for _, b := range t.addr[a] {
				set[b] = true
			}
		}
		sets = append(sets, set)
	}
	for _, want := range topics {
		if want == nil {
			continue
		}
		set := map[uint64]bool{}
		for _, tp := range want {
			for _, b := range t.topic[tp] {
				set[b] = true
			}
		}
		sets = append(sets, set)
	}
	if len(sets) == 0 {
		return nil
	}
	smallest := sets[0]
	for _, s := range sets[1:] {
		if len(s) < len(smallest) {
			smallest = s
		}
	}
	var out []uint64
	for b := range smallest {
		if b < lo || b > hi {
			continue
		}
		all := true
		for _, s := range sets {
			all = all && s[b]
		}
		if all {
			out = append(out, b)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// TailLogCandidates answers the unsealed half of a log query from the hot-tail
// index, catching the index up first: on the first call after start that is one
// scan of the whole tail, afterwards only the blocks captured since.
//
// covered is the highest block the index actually answers for. Above it the
// caller must fall back to reading records per block, which is cheap because
// the gap is only what the executor appended between the logs family's max
// block and the serving head. matched=false means the query had nothing to
// narrow on, so every block in range is a candidate and this index says nothing.
//
// Goroutine-safe. ponytail: one mutex covers both the catch-up scan and the
// lookups, so a query that triggers the first full scan blocks the others; the
// scan is a map probe per block and one decode per log-bearing block, and the
// tail is one epoch. Shard or move the scan behind a background tick only if a
// profile ever blames it.
func (h *History) TailLogCandidates(from, to uint64, addrs [][20]byte, topics [][][32]byte) (blocks []uint64, covered uint64, matched bool, err error) {
	t := h.taillog
	t.mu.Lock()
	defer t.mu.Unlock()

	if end, ok := h.epochs.SealedEnd(); ok {
		t.retire(end)
	}
	// The ceiling is the logs family's own max block, clamped to the serving
	// head; see tailLogIndex.through.
	ceiling := h.head.Load()
	if n, ok := h.store.LogsMax(); !ok {
		ceiling = t.through
	} else if n < ceiling {
		ceiling = n
	}
	if t.through < t.floor {
		t.through = t.floor // never scan below the retirement floor: there is
	} //                      nothing to read there and the epochs answer it
	for n := t.through + 1; n <= ceiling; n++ {
		rec, ok, err := h.store.LogsRecord(n)
		if err != nil {
			return nil, 0, false, err
		}
		if ok {
			lr, err := decodeLogRec(n, rec)
			if err != nil {
				return nil, 0, false, err
			}
			t.add(lr)
		}
		t.through = n
	}
	if len(addrs) == 0 && !hasTopicMatcher(topics) {
		return nil, t.through, false, nil
	}
	return t.blocks(from, to, addrs, topics), t.through, true, nil
}

func hasTopicMatcher(topics [][][32]byte) bool {
	for _, w := range topics {
		if w != nil {
			return true
		}
	}
	return false
}

// RetireTailLogs drops the hot-tail index entries a seal has just replaced, so
// the memory goes back at the seal rather than at the next query.
func (h *History) RetireTailLogs(sealedEnd uint64) {
	h.taillog.mu.Lock()
	defer h.taillog.mu.Unlock()
	h.taillog.retire(sealedEnd)
}

// TailLogStats reports the hot-tail index size for the status line: distinct
// keys, (key, block) pairs, and the posting bytes those pairs cost.
func (h *History) TailLogStats() (keys, pairs int, bytes uint64) {
	h.taillog.mu.Lock()
	defer h.taillog.mu.Unlock()
	keys = len(h.taillog.addr) + len(h.taillog.topic)
	pairs = h.taillog.pairs
	return keys, pairs, uint64(pairs)*8 + uint64(len(h.taillog.addr))*20 + uint64(len(h.taillog.topic))*32
}

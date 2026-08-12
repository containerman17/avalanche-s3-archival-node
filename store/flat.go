package store

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
)

// THE FLAT LATEST-STATE CACHE (user ruling 2026-08-13: "the flat state, the
// latest cache, should be part of the design, 100%"). DESIGN's fast path runs
// against flat RAM maps of current state maintained by the follower, not
// against the descent; this is that map.
//
// WHAT IT IS: account/slot -> the latest stored row, maintained by the executor
// as blocks land, serving reads AT THE HEAD ONLY. A miss falls through to the
// normal descent and populates it, so it warms with no special path and a
// read-only corpus warms it the same way a live node does.
//
// EXACT CONSISTENCY WITH THE HEAD, and the whole argument is two rules:
//
//  1. The cache carries the HEIGHT it is the latest state of, and it is moved
//     by exactly one thing: applying a block's own rows, under the cache's write
//     lock, inside WriteBlock, which is the critical path that publishes a
//     block. A read serves from the cache ONLY when the height it asks for is
//     the height the cache holds, checked under the same lock as the lookup, so
//     a block landing mid-read can never hand a reader the wrong block's value.
//  2. A fill from a miss is accepted ONLY IF the height has not moved since the
//     descent read that produced it. If it moved, the value may already be one
//     block stale, and the fill is DROPPED rather than reasoned about. Same rule
//     one level up: a block that is not the cache's height plus one (a
//     re-execution replay, a gap) drops the whole cache instead of arguing.
//
// A DROPPED ENTRY IS ONLY A CACHE MISS. The descent underneath is always the
// truth, so eviction, rotation and every refusal above cost latency and can
// never cost correctness. That is what makes the budget safe to enforce
// bluntly.
type flatCache struct {
	mu     sync.RWMutex
	budget uint64 // 0 disables the cache entirely
	half   uint64 // rotation threshold: hot may reach half the budget

	// TWO GENERATIONS, hot in front of cold. A Go map has no eviction, and
	// keeping one costs a per-entry list the read path would have to write to,
	// which is exactly the write lock this whole round removed. Rotation drops
	// the older generation whole and keeps the working set that is still being
	// touched, which on this workload is ~8,000 DeFi contracts' regions.
	//
	// ponytail: no promotion on a cold hit, so a key that is read but never
	// written ages out and pays one descent read to come back. Promote on read
	// (behind the write lock, or with a striped map) if that miss rate shows up.
	hot   map[string]flatVal
	cold  map[string]flatVal
	bytes uint64 // hot's accounted bytes

	height uint64
	set    bool
}

// flatVal is one state row as the descent reports it. found=false is the
// descent's "never written at or below here", which sends the caller to the
// genesis floor: caching it is what keeps a genesis-era account off a walk
// through every run. An empty val with found=true is a delete or a cleared
// slot, which the EVM defines as zero, and the two must never collapse.
type flatVal struct {
	val   []byte
	found bool
}

// flatEntryOverhead is the per-entry cost the map itself carries: the key
// string header and its bytes are counted separately, this is the bucket slot,
// the value struct and the slice header. Approximate on purpose; a byte budget
// that needed to be exact would need a heap profiler, not a constant.
const flatEntryOverhead = 64

func flatEntryBytes(k string, v flatVal) uint64 {
	return uint64(len(k) + len(v.val) + flatEntryOverhead)
}

// cgroupMemMaxPath is this process's own container ceiling in the cgroup v2
// unified hierarchy. It is read here rather than through exec.CgroupMemoryLimit
// because exec imports store; the two copies are six lines and must agree.
const cgroupMemMaxPath = "/sys/fs/cgroup/memory.max"

// flatCacheShare is the container's fraction, and it is deliberately the same
// eighth the exec state cache and the Firewood node cache already take: one
// arithmetic for every anon grant on the box. This one is a Go map, so it is
// spent out of the heap GOMEMLIMIT already reserved rather than being a fourth
// claim on the container.
const flatCacheShare = 8

// flatCacheNoContainer is the default where there is no ceiling written down
// (a laptop, this dev box). An unknown ceiling is not a small one, but it is
// not a licence either.
const flatCacheNoContainer = 1 << 30

// flatCacheBytes sizes the cache and says where the number came from.
// EPOCHDB_FLAT_CACHE, plain bytes with no suffix like every other size here,
// wins over the formula; 0 turns the cache off. A value that is not a byte
// count REFUSES TO START rather than falling back silently.
func flatCacheBytes() (uint64, string, error) {
	if v := os.Getenv("EPOCHDB_FLAT_CACHE"); v != "" {
		n, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			return 0, "", fmt.Errorf("EPOCHDB_FLAT_CACHE=%q is not a byte count", v)
		}
		if n == 0 {
			return 0, "EPOCHDB_FLAT_CACHE=0, the flat latest-state cache is off", nil
		}
		return n, "EPOCHDB_FLAT_CACHE override", nil
	}
	if limit, ok := containerMemoryLimit(); ok {
		return limit / flatCacheShare, fmt.Sprintf("an eighth of this container's %d MB ceiling", limit>>20), nil
	}
	return flatCacheNoContainer, "1GB, no container ceiling to size against", nil
}

func containerMemoryLimit() (uint64, bool) {
	b, err := os.ReadFile(cgroupMemMaxPath)
	if err != nil {
		return 0, false
	}
	s := strings.TrimSpace(string(b))
	if s == "max" {
		return 0, false
	}
	n, err := strconv.ParseUint(s, 10, 64)
	if err != nil || n == 0 {
		return 0, false
	}
	return n, true
}

func newFlatCache(budget uint64) *flatCache {
	return &flatCache{budget: budget, half: budget / 2, hot: map[string]flatVal{}}
}

// adopt pins the cache to a height while it is EMPTY, which is trivially
// consistent with any head. It is called once, at open, off the store's head:
// without it a read-only corpus (a bench, an offline reader) would never have a
// block land and the cache would never serve anything.
func (c *flatCache) adopt(height uint64) {
	if c == nil || c.budget == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.height, c.set = height, true
}

// get serves a state row IF the cache is the latest state of exactly the height
// asked for. Any other height is a clean miss.
func (c *flatCache) get(prefix []byte, height uint64) (val []byte, found, hit bool) {
	if c == nil || c.budget == 0 {
		return nil, false, false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	if !c.set || c.height != height {
		return nil, false, false
	}
	if e, ok := c.hot[string(prefix)]; ok {
		return e.val, e.found, true
	}
	if e, ok := c.cold[string(prefix)]; ok {
		return e.val, e.found, true
	}
	return nil, false, false
}

// fill records what the descent answered for a miss. THE HEIGHT IT WAS READ AT
// IS THE PROOF: if the head moved while the descent ran, the answer may already
// be one block stale, and it is dropped.
func (c *flatCache) fill(prefix []byte, val []byte, found bool, height uint64) {
	if c == nil || c.budget == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.set || c.height != height {
		return
	}
	c.put(string(prefix), flatVal{val: val, found: found})
}

// apply folds one landed block's state writes in and moves the cache's height.
// The caller is WriteBlock, so this runs in the same critical path that
// publishes the block.
func (c *flatCache) apply(b *BlockWrite) {
	if c == nil || c.budget == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.set && b.Height != c.height+1 {
		// NOT THE NEXT BLOCK: a walk-back replay, or a gap. Consistency here
		// cannot be argued cheaply, so it is refused: everything is dropped and
		// the reads that follow rebuild it.
		c.hot, c.cold, c.bytes = map[string]flatVal{}, nil, 0
	}
	for i := range b.Txs {
		for _, r := range b.Txs[i].State {
			c.put(string(stateKeyPrefix(r)), flatVal{val: r.Val, found: true})
		}
	}
	for _, r := range b.Tail {
		c.put(string(stateKeyPrefix(r)), flatVal{val: r.Val, found: true})
	}
	c.height, c.set = b.Height, true
}

// put writes one entry into the hot generation and rotates when it is full. The
// caller holds the write lock.
func (c *flatCache) put(k string, v flatVal) {
	if old, ok := c.hot[k]; ok {
		c.bytes -= flatEntryBytes(k, old)
	}
	c.hot[k] = v
	c.bytes += flatEntryBytes(k, v)
	if c.bytes > c.half {
		c.cold, c.hot, c.bytes = c.hot, make(map[string]flatVal, len(c.hot)), 0
	}
}

package store

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/common/lru"
	"github.com/ava-labs/libevm/core/vm"
)

// THE JUMPDEST BITMAP CACHE IS THE CODE CACHE'S TWIN AND FOLLOWS ITS RULE
// EXACTLY (flat.go): a bitmap is a PURE FUNCTION of bytecode and bytecode is
// IMMUTABLE BY HASH, so one hash names one bitmap at every height forever,
// there is nothing to invalidate and a hit is correct at any height. libevm
// shares a bitmap only inside ONE top-level call, so a replay that hits the
// same few thousand DeFi contracts millions of times rebuilds the same bitmaps
// millions of times: measured on the mainnet C rebuild that is 7.2% of process
// CPU and ~24% of EVM compute, about 70us per transaction.
//
// THE ONE DIFFERENCE FROM THE CODE CACHE is who fills it. We do not compute
// bitmaps, libevm does, so this cache cannot be filled by a code load; it is
// REGISTERED with libevm (vm.RegisterJumpDestCache) and libevm asks it before
// running the analysis and offers the result after.
//
// REGISTRATION IS PROCESS-WIDE, which is what the content addressing allows:
// a bitmap for a code hash is the same bitmap for every chain, so one cache per
// process is correct even where one process opened several DBs (the tests do).
// libevm's seam is process-wide for the same reason, and because saexec still
// hardcodes vm.Config{}, so a per-EVM knob would not reach the C-chain replay
// that pays the cost.
//
// A WRONG BITMAP MAKES AN INVALID JUMP LOOK VALID and changes execution, so the
// cache hands back the exact slice it was given and never touches the bytes:
// lru.SizeConstrainedCache stores by reference, does not copy, and is already
// documented for content-addressed values.

// jumpDestShare makes the budget AN EIGHTH of the code cache's: libevm's bitmap
// is len(code)/8+5 bytes, so an eighth of the code cache's budget holds the
// analyses of the same contracts the code cache holds the code of.
const jumpDestShare = 8

// jumpDestCacheBytes sizes the bitmap cache exactly the way codeCacheBytes
// sizes the code one: EPOCHDB_JUMPDEST_CACHE in plain bytes wins over the
// formula, 0 turns the cache off, and a value that is not a byte count REFUSES
// TO START.
func jumpDestCacheBytes(code uint64) (uint64, string, error) {
	if v := os.Getenv("EPOCHDB_JUMPDEST_CACHE"); v != "" {
		n, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			return 0, "", fmt.Errorf("EPOCHDB_JUMPDEST_CACHE=%q is not a byte count", v)
		}
		if n == 0 {
			return 0, "EPOCHDB_JUMPDEST_CACHE=0, the JUMPDEST bitmap cache is off", nil
		}
		return n, "EPOCHDB_JUMPDEST_CACHE override", nil
	}
	return code / jumpDestShare, "an eighth of the contract-code cache's budget", nil
}

// jumpDests is the process-wide bitmap cache, installed once. It is a package
// variable and not a DB field because libevm's registration is process-wide.
type jumpDests struct {
	c *lru.SizeConstrainedCache[common.Hash, []byte]
}

func (j jumpDests) GetAnalysis(h common.Hash) ([]byte, bool) { return j.c.Get(h) }
func (j jumpDests) AddAnalysis(h common.Hash, b []byte)      { j.c.Add(h, b) }

var (
	jumpDestOnce   sync.Once
	jumpDestBudget uint64
	jumpDestWhy    string
)

// registerJumpDests installs the bitmap cache with libevm the FIRST time a DB
// is opened, and never again: vm.RegisterJumpDestCache MUST NOT be called
// twice, and a second DB in the same process shares the first one's cache,
// which is correct because a code hash means the same bitmap everywhere.
func registerJumpDests(budget uint64, why string) {
	jumpDestOnce.Do(func() {
		jumpDestBudget, jumpDestWhy = budget, why
		if budget == 0 {
			return
		}
		vm.RegisterJumpDestCache(jumpDests{lru.NewSizeConstrainedCache[common.Hash, []byte](budget)})
	})
	if jumpDestBudget != budget {
		log.Printf("store: the JUMPDEST bitmap cache is already %d MB (%s), this DB asked for %d MB",
			jumpDestBudget>>20, jumpDestWhy, budget>>20)
	}
}

// JumpDestCacheBudget reports the JUMPDEST bitmap cache's byte budget and where
// the number came from, beside CodeCacheBudget's.
func (d *DB) JumpDestCacheBudget() (uint64, string) { return jumpDestBudget, jumpDestWhy }

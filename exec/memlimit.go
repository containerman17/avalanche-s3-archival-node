package exec

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

// cgroupMemMaxPath is the memory ceiling of THIS process's cgroup in the v2
// unified hierarchy. Inside a container the cgroup mount is namespaced, so the
// file is the CONTAINER's own limit (docker `mem_limit`), not the box's.
const cgroupMemMaxPath = "/sys/fs/cgroup/memory.max"

// CgroupMemoryLimit reports the container's own memory ceiling, and it is the
// one box fact a chain process is allowed to size itself against.
//
// WHY A PROCESS READS ITS OWN CEILING AT ALL. A box runs one process per chain
// (DESIGN "serve: one process, one chain"), so every per-chain grant written as
// a constant is silently multiplied by the number of chains: the 1GiB exec
// state cache is 1GiB on a laptop and 22GiB across the Tokyo fleet, which is
// a third of that box on its own. The Firewood clamp already refused to be a
// constant and sizes itself off the chain's own dir; this is the same rule for
// the terms that scale with the BOX instead of with the chain. A ceiling the
// operator already had to type once (`mem_limit`) is the honest input: it is
// the only place where "how big may this chain get" is written down.
//
// It reports false when there is no limit ("max", the normal reading outside a
// container), when the file is absent (cgroup v1 or no cgroupfs, where the
// answer is simply unknown), or when it is unparseable. Every caller must then
// use its unclamped default: an unknown ceiling is not a small one.
func CgroupMemoryLimit() (uint64, bool) { return readCgroupMemMax(cgroupMemMaxPath) }

// clampStateCache is the Go-side EVM read cache's share of the container.
//
// An eighth, and the reasoning is the fleet's rather than the chain's: this
// cache is anon (a Go map of decoded accounts), it fills during CATCH-UP, and
// catch-up is exactly when every chain on a cold box is doing it at once. An
// eighth of the ceiling the operator wrote leaves the other seven eighths for
// the terms nobody can shrink, which are the per-block tail index (~182 B per
// block of unsealed tail), the epoch blooms on the heap and Firewood's own cgo
// cache. The flag stays the ceiling, so a laptop and a big container are
// unchanged and a small container stops writing a cheque the box cannot cover.
// A zero limit is an unknown one and changes nothing.
func clampStateCache(want, limit uint64) uint64 {
	if limit == 0 {
		return want
	}
	return min(want, limit/8)
}

const (
	// fwCacheFloor is the smallest node cache worth having: below it the Rust
	// library is back to re-walking the trie interior per SLOAD.
	fwCacheFloor = 64 << 20
	// fwCacheDiskCap bounds the NO-CONTAINER fallback only. With no ceiling
	// written down anywhere, 4GB is the largest anon grant we are willing to
	// take without being told we may.
	fwCacheDiskCap = 4 << 30
	// fwCacheShare is the container's fraction, and it is the state cache's
	// eighth for the state cache's reason: one arithmetic for both anon grants.
	fwCacheShare = 8
)

// firewoodCacheBytes sizes Firewood's node cache and says WHERE the number came
// from, so an operator reading one log line knows why they got what they got.
//
// THE CONTAINER IS THE BUDGET, NOT THE DIRECTORY. The old rule was 3% of the
// Firewood dir measured at open, calibrated so a full 157GB mainnet state
// reached a 4GB cap. It sizes from HISTORY while the cost it pays for is the
// WORKING SET, and the two diverge exactly during catch-up: mid-sync the dir is
// small but every SLOAD walks the whole live trie at random, so the formula
// under-provisions precisely when the chain is working hardest. Measured on
// mainnet-c at height 11.07M: a 6.5GB dir bought a 194MB cache, Firewood grew
// to 52% of exec wall clock, 19k reads/s at 77% disk utilisation, throughput
// fell 262 -> 96 -> 48 mgas/s over three hours, and 13GiB of the container's
// 34GiB sat unused. Because the dir is measured once at open, it could not even
// correct itself without a restart.
//
// AN EIGHTH OF THE CONTAINER'S OWN `memory.max`, for three reasons. It is the
// same fraction the exec state cache already takes (clampStateCache), so the
// two anon grants share one rule. It composes with GOMEMLIMIT: 7/10 of the
// ceiling is already reserved for the Go heap, so 7/10 + 1/8 = 82.5% written
// down, leaving ~17.5% for the terms nobody sizes (Firewood's in-memory
// revisions, zstd encoder state, thread stacks) and for the page cache the
// kernel actually wants. And it SCALES with the one number the operator typed,
// which is what makes it safe in the other direction: this cache holds DECODED
// nodes, so it is anon and unevictable, and it was made a formula rather than a
// constant because a per-chain constant is multiplied by the chain count. The
// same rule on the 24GiB ceiling that OOM-killed this container yields 3GB, not
// 4. Firewood treats the number as a LIMIT and fills it lazily, so an idle
// chain in a big container never pays for its share.
//
// No max() with the disk term: at every realistic point the container term
// already dominates (157GB of dir capped to 4GB against 34GiB/8 = 4.25GiB), and
// where the disk term would win it wins by overspending the container, which is
// the OOM. The disk formula survives only where there is no ceiling to read.
//
// EPOCHDB_FIREWOOD_CACHE, in bytes, wins over both. Plain bytes with no size
// suffix, matching EPOCHDB_MAX_STAGING and EPOCHDB_CACHE_MIN_FREE. A value that
// is not a byte count, or one below the floor (the shape of a "512" that meant
// megabytes), REFUSES TO START rather than falling back silently: a cache
// sized by accident is the failure this whole function exists to end. An
// override above the container's ceiling is allowed and logged, because an
// operator who typed a number meant it.
func firewoodCacheBytes(dirBytes, limit uint64) (uint64, string, error) {
	if v := os.Getenv("EPOCHDB_FIREWOOD_CACHE"); v != "" {
		n, err := strconv.ParseUint(v, 10, 64)
		if err != nil || n < fwCacheFloor {
			return 0, "", fmt.Errorf("EPOCHDB_FIREWOOD_CACHE=%q is not a byte count of at least %d (the floor)", v, fwCacheFloor)
		}
		return n, "EPOCHDB_FIREWOOD_CACHE override", nil
	}
	if limit == 0 {
		// An unknown ceiling is not a small one, but it is not a licence
		// either, so fall back to the dir formula it always was.
		return min(max(dirBytes*3/100, fwCacheFloor), fwCacheDiskCap),
			fmt.Sprintf("3%% of %d MB on disk, no container ceiling to size against, capped at 4GB", dirBytes>>20), nil
	}
	// max(), not min(): the floor wins only under a 512MB ceiling, which is
	// below this binary's own runtime baseline and cannot run a chain anyway.
	return max(limit/fwCacheShare, fwCacheFloor),
		fmt.Sprintf("an eighth of this container's %d MB ceiling", limit>>20), nil
}

func readCgroupMemMax(path string) (uint64, bool) {
	b, err := os.ReadFile(path)
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

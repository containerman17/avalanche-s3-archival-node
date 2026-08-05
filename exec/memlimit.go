package exec

import (
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

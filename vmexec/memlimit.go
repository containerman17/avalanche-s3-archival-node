package vmexec

import (
	"log"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
)

// cgroupMemMaxPath is the memory ceiling of THIS process's cgroup (v2).
const cgroupMemMaxPath = "/sys/fs/cgroup/memory.max"

// CgroupMemoryLimit reports the container's own memory ceiling; false when
// there is none ("max"), the file is absent, or it is unparseable. Copied
// from exec/memlimit.go so this package does not link Firewood.
func CgroupMemoryLimit() (uint64, bool) {
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

// setMemLimit is the backstop under the budget profiles: 7/10 of the
// container's ceiling as the Go soft limit, unless GOMEMLIMIT is set.
func setMemLimit() {
	if os.Getenv("GOMEMLIMIT") != "" {
		return
	}
	limit, ok := CgroupMemoryLimit()
	if !ok {
		return
	}
	soft := int64(limit / 10 * 7)
	debug.SetMemoryLimit(soft)
	log.Printf("vmexec: GOMEMLIMIT %d MB (7/10 of this container's %d MB ceiling)", soft>>20, limit>>20)
}

// RSSMB is this process's resident set in MB (0 when /proc is unreadable).
func RSSMB() int {
	b, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return 0
	}
	f := strings.Fields(string(b))
	if len(f) < 2 {
		return 0
	}
	pages, _ := strconv.Atoi(f[1])
	return pages * os.Getpagesize() >> 20
}

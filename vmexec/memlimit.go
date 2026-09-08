package vmexec

import (
	"os"
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

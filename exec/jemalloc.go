package exec

import (
	"fmt"
	"sort"
	"strings"

	"github.com/ava-labs/firewood-go-ethhash/ffi"
)

// jemallocLine renders Firewood's Rust-side allocator gauges as one log line.
//
// WHY THIS LINE EXISTS. Everything above it in the status block measures the Go
// side, and the Go side has never been the memory question: it is pinned by
// GOMEMLIMIT and it plateaus. The growth that ends a long replay is cgo-side,
// and until now the only instrument for it was cgroup anon minus a Go estimate,
// which says a number is climbing but never which allocator owns it.
//
// The recorder these gauges come from is ALREADY RUNNING in every epochdb
// process: the graft firewood package calls ffi.StartMetrics() in its init(),
// and this package imports it. So nothing is being turned on here, only read,
// and ffi.GatherRenderedMetrics refreshes jemalloc's epoch on the way in, so
// the values are current at each call rather than at process start.
//
// THE TWO NUMBERS THAT SETTLE THE QUESTION are allocated and resident.
// allocated is what the application asked for and has not freed, so if it
// tracks the anon growth, Firewood is genuinely holding the memory and it is a
// leak or an unbounded cache. resident is what the OS has in physical pages,
// so if allocated stays flat while resident climbs, nothing is leaked and the
// allocator is merely sitting on dirty pages it has not purged, which is a
// configuration answer rather than a code one. retained is virtual only and
// never appears in cgroup anon, which is why it can look alarming and cost
// nothing.
//
// Read, not gated: a diagnostic that has to be switched on is a diagnostic
// nobody has during the incident that needed it, and this is one line per ten
// seconds beside three that are already there.
func jemallocLine() string {
	fams, err := ffi.GatherRenderedMetrics()
	if err != nil {
		return "unavailable: " + err.Error()
	}
	var parts []string
	for _, f := range fams {
		short, ok := strings.CutPrefix(f.GetName(), "jemalloc_")
		if !ok {
			continue
		}
		short = strings.TrimSuffix(short, "_bytes")
		for _, m := range f.GetMetric() {
			parts = append(parts, fmt.Sprintf("%s=%.0fMB", short, m.GetGauge().GetValue()/1e6))
		}
	}
	if len(parts) == 0 {
		return "unavailable: no jemalloc_* gauges in the recorder"
	}
	// Sorted so the columns hold still across samples and a reader can diff
	// two lines by eye.
	sort.Strings(parts)
	return strings.Join(parts, " ")
}

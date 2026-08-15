package exec

import (
	"strings"
	"testing"
)

// TestJemallocLine is the check that the whole instrument rests on: that the
// Rust recorder really is already running in an ordinary epochdb process, with
// nothing enabled and no Firewood database opened, purely because the graft
// firewood package's init() started it. If that ever stops being true this
// fails here rather than as an "unavailable" string in a production log during
// the next memory incident.
func TestJemallocLine(t *testing.T) {
	got := jemallocLine()
	t.Log(got)
	if strings.HasPrefix(got, "unavailable") {
		t.Fatalf("jemalloc gauges not readable: %s", got)
	}
	// allocated and resident are the two that answer leak versus retention;
	// a line without them is not worth logging.
	for _, want := range []string{"allocated=", "resident="} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %q in %q", want, got)
		}
	}
}

package main

import (
	"bytes"
	"log"
	"os"
	"regexp"
	"runtime/debug"
	"strings"
	"testing"
)

// One container ID in both spellings --tip-override accepts: a 0x-hex eth
// block hash (pre-ProposerVM, where it IS the container ID) and cb58.
const hexTip = "0x0f2ce4f0e7b6e1b0e2e0b3ca7f9a0ad2a9f6f1a4b8c3d2e1f0a9b8c7d6e5f4a3"

// TestParseTipOverrideRefusesAHeight pins the ruling: --tip-override is a
// PHYSICAL container ID, one bare value for the one chain this process serves
// (the per-chain <chain>=<id> form died with the fleet, 2026-08-04), and a
// height is refused with the place to find a real one.
func TestParseTipOverrideRefusesAHeight(t *testing.T) {
	for _, v := range []string{"3000000", "0"} {
		_, err := parseTipOverride(v)
		if err == nil {
			t.Fatalf("--tip-override %s accepted a height", v)
		}
		if !strings.Contains(err.Error(), "block height") || !strings.Contains(err.Error(), "consensus: accepted height=N container=") {
			t.Fatalf("%s: error does not say how to get an ID: %v", v, err)
		}
	}
	if _, err := parseTipOverride("not-an-id"); err == nil || !strings.Contains(err.Error(), "consensus: accepted height=N container=") {
		t.Fatalf("garbage value: %v", err)
	}
	id, err := parseTipOverride(hexTip)
	if err != nil {
		t.Fatalf("0x-hex eth block hash refused: %v", err)
	}
	if got, err := parseTipOverride(id.String()); err != nil || got != id {
		t.Fatalf("cb58 round trip: %v %v", got, err)
	}
	// The keyed form is not a spelling any more: it is a bad container ID.
	if _, err := parseTipOverride("C=" + hexTip); err == nil {
		t.Fatal("the fleet's <chain>=<containerID> form was accepted")
	}
}

// retired is every name that used to be operator surface and is not any more
// (RULING 2026-08-03): the 7 pipeline stages, the 2 verifiers and the 6 benches.
// They still exist under `dev`, and none of them may appear in `usage`.
var retired = []string{"fetch", "exec", "verify", "probe"}

// TestUsageIsServeOnly pins the surface: `epochdb serve` and nothing else, with
// no retired name and no `fleet` anywhere in what an operator is shown.
func TestUsageIsServeOnly(t *testing.T) {
	var buf bytes.Buffer
	if code := dispatch(nil, &buf); code != 2 {
		t.Fatalf("no arguments exited %d, want 2", code)
	}
	out := buf.String()
	if !strings.Contains(out, "epochdb serve") {
		t.Fatalf("usage does not name serve:\n%s", out)
	}
	if strings.Contains(out, "fleet") {
		t.Fatalf("usage still names fleet:\n%s", out)
	}
	// As a WORD, so that prose ("it fetches, executes") and the surviving flags
	// (--verify) do not read as commands, and `cook-index` still would.
	for _, name := range retired {
		if regexp.MustCompile(`(^|[^a-z0-9-])` + name + `($|[^a-z0-9-])`).MatchString(out) {
			t.Fatalf("usage still names the retired command %q:\n%s", name, out)
		}
	}
	if strings.Contains(out, "--chains") {
		t.Fatalf("usage still offers --chains:\n%s", out)
	}
	for _, want := range []string{"--chain ", "--verify", "/status", "ONE PROCESS SERVES ONE CHAIN"} {
		if !strings.Contains(out, want) {
			t.Fatalf("usage does not mention %q:\n%s", want, out)
		}
	}
}

// TestRetiredNamesFailLoudly pins the other half: a stale script that still says
// `epochdb seal` STOPS with a pointer instead of running a stage the node now
// does for itself.
func TestRetiredNamesFailLoudly(t *testing.T) {
	for _, name := range retired {
		if _, ok := devCommands[name]; !ok {
			t.Fatalf("%q is not reachable as `epochdb dev %s` any more", name, name)
		}
		var buf bytes.Buffer
		if code := dispatch([]string{name}, &buf); code != 2 {
			t.Fatalf("top-level %q exited %d, want 2", name, code)
		}
		out := buf.String()
		if !strings.Contains(out, "epochdb serve") {
			t.Fatalf("%q does not point at serve:\n%s", name, out)
		}
		if !strings.Contains(out, "epochdb dev "+name) {
			t.Fatalf("%q does not say where the stage went:\n%s", name, out)
		}
	}

	// `fleet` died twice over: the command in 2026-08-03, the whole idea of one
	// process hosting several chains in 2026-08-04. Its pointer says so.
	var buf bytes.Buffer
	if code := dispatch([]string{"fleet"}, &buf); code != 2 {
		t.Fatalf("fleet exited %d, want 2", code)
	}
	if out := buf.String(); !strings.Contains(out, "ONE PROCESS SERVES ONE CHAIN") || !strings.Contains(out, "EPOCHDB_CACHE_DIR") {
		t.Fatalf("fleet does not point at one process per chain:\n%s", out)
	}

	// `dev` itself is not a command, and an unknown stage lists the real ones.
	for _, args := range [][]string{{"dev"}, {"dev", "nope"}, {"nope"}} {
		var buf bytes.Buffer
		if code := dispatch(args, &buf); code != 2 {
			t.Fatalf("%v exited %d, want 2", args, code)
		}
	}
	buf.Reset()
	dispatch([]string{"dev"}, &buf)
	for _, name := range []string{"exec", "fetch", "verify"} {
		if !strings.Contains(buf.String(), name) {
			t.Fatalf("dev usage does not list %q:\n%s", name, buf.String())
		}
	}
}

// TestSetGoGCIsBelowTheGoDefaultAndYieldsToTheOperator: the whole point of the
// lower default is that heap headroom is page cache this node reads through
// (measured on mainnet C: live heap ~9GB, runtime grown to ~17.4GB, and giving
// that back moved the block rate 56 -> 80). But an operator who set GOGC meant
// it, so the runtime must be left alone then.
func TestSetGoGCIsBelowTheGoDefaultAndYieldsToTheOperator(t *testing.T) {
	if defaultGOGC >= 100 {
		t.Fatalf("defaultGOGC = %d, not below Go's 100; it would give the page cache nothing back", defaultGOGC)
	}
	orig := debug.SetGCPercent(123)
	t.Cleanup(func() { debug.SetGCPercent(orig) })

	t.Setenv("GOGC", "77")
	setGoGC()
	if got := debug.SetGCPercent(123); got != 123 {
		t.Fatalf("GC percent moved to %d while GOGC was set in the environment", got)
	}
}

// TestWarnIfHeapPagesAreKept: the warning must fire exactly when the runtime
// will hold freed heap pages resident. It is easy to lose madvdontneed=1 by
// accident because docker REPLACES GODEBUG rather than merging, and the
// symptom (page cache shrinking as the heap high-water mark grows) is
// invisible in both `docker stats` and gctrace.
func TestWarnIfHeapPagesAreKept(t *testing.T) {
	for _, tc := range []struct {
		godebug string
		warn    bool
	}{
		{"", true},                          // unset: Go's MADV_FREE default
		{"gctrace=1", true},                 // the accidental replacement
		{"madvdontneed=1", false},           // what the image ships
		{"gctrace=1,madvdontneed=1", false}, // operator kept it
		{"madvdontneed=0", true},            // explicitly turned back off
	} {
		var buf bytes.Buffer
		log.SetOutput(&buf)
		t.Setenv("GODEBUG", tc.godebug)
		warnIfHeapPagesAreKept()
		log.SetOutput(os.Stderr)

		warned := strings.Contains(buf.String(), "madvdontneed")
		if warned != tc.warn {
			t.Errorf("GODEBUG=%q: warned=%v, want %v", tc.godebug, warned, tc.warn)
		}
	}
}

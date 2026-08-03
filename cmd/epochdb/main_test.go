package main

import (
	"bytes"
	"regexp"
	"strings"
	"testing"
)

// retired is every name that used to be operator surface and is not any more
// (RULING 2026-08-03): the 7 pipeline stages, the 2 verifiers and the 6 benches.
// They still exist under `dev`, and none of them may appear in `usage`.
var retired = []string{
	"fetch", "exec", "cook-index", "cook-txindex", "seal", "publish", "bootstrap",
	"verify", "verify-logs",
	"ab-bench", "ab-bench-tx", "ab-bench-rpc", "ab-bench-logs", "rpc-bench", "backfill-logs",
}

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
	// The two halves of the fold have to be visible, or --chains is unfindable.
	for _, want := range []string{"--chain ", "--chains", "--verify", "/status"} {
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

	// fleet folded into serve --chains, so its pointer names the flag.
	var buf bytes.Buffer
	if code := dispatch([]string{"fleet"}, &buf); code != 2 {
		t.Fatalf("fleet exited %d, want 2", code)
	}
	if out := buf.String(); !strings.Contains(out, "serve --chains") {
		t.Fatalf("fleet does not point at serve --chains:\n%s", out)
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
	for _, name := range []string{"seal", "ab-bench", "bootstrap"} {
		if !strings.Contains(buf.String(), name) {
			t.Fatalf("dev usage does not list %q:\n%s", name, buf.String())
		}
	}
}

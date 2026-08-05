package exec

import (
	"os"
	"path/filepath"
	"testing"
)

// TestReadCgroupMemMax pins the readings that must NOT produce a limit: an
// unlimited cgroup, a missing file and garbage all mean "unknown", and a caller
// that treated unknown as zero would shrink every cache to nothing off a box
// that has no cgroup at all.
func TestReadCgroupMemMax(t *testing.T) {
	dir := t.TempDir()
	for _, tc := range []struct {
		name    string
		content string
		want    uint64
		wantOK  bool
	}{
		{name: "limit", content: "3221225472\n", want: 3 << 30, wantOK: true},
		{name: "no trailing newline", content: "1073741824", want: 1 << 30, wantOK: true},
		{name: "unlimited", content: "max\n"},
		{name: "zero", content: "0\n"},
		{name: "garbage", content: "not a number\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := filepath.Join(dir, tc.name)
			if err := os.WriteFile(p, []byte(tc.content), 0o644); err != nil {
				t.Fatal(err)
			}
			got, ok := readCgroupMemMax(p)
			if got != tc.want || ok != tc.wantOK {
				t.Fatalf("readCgroupMemMax(%q) = %d, %v; want %d, %v", tc.content, got, ok, tc.want, tc.wantOK)
			}
		})
	}
	if got, ok := readCgroupMemMax(filepath.Join(dir, "absent")); ok {
		t.Fatalf("a missing file must read as unknown, got %d", got)
	}
}

// TestStateCacheClampsToTheContainer pins the arithmetic exec.New applies: the
// flag is a ceiling, the container's eighth is the other, and the smaller wins.
func TestStateCacheClampsToTheContainer(t *testing.T) {
	const flagDefault = 1 << 30 // serve --state-cache 1
	for _, tc := range []struct {
		limit uint64
		want  uint64
	}{
		{limit: 3 << 30, want: 384 << 20},    // tiny chain: 3g container
		{limit: 6 << 30, want: 768 << 20},    // mid chain
		{limit: 24 << 30, want: flagDefault}, // mainnet-c: the flag is smaller
	} {
		if got := clampStateCache(flagDefault, tc.limit); got != tc.want {
			t.Fatalf("clampStateCache(1GiB, %d) = %d, want %d", tc.limit, got, tc.want)
		}
	}
	if got := clampStateCache(flagDefault, 0); got != flagDefault {
		t.Fatalf("no container limit must leave the flag alone, got %d", got)
	}
}

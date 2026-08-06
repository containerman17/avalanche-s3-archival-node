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

// TestFirewoodCacheDefault pins the derivation at the points that matter: a
// fresh dir, the mid-sync mainnet-c that measured the collapse (6.5GB of dir in
// a 34GiB container, where the old 3%-of-disk rule bought 194MB), and a full
// mainnet state, whose 157GB no longer buys anything because the container is
// the budget. Plus the invariant no path may break.
func TestFirewoodCacheDefault(t *testing.T) {
	const gib = 1 << 30
	for _, tc := range []struct {
		name  string
		dir   uint64
		limit uint64
		want  uint64
	}{
		{name: "fresh dir, no container", want: fwCacheFloor},
		{name: "fresh dir in a container", limit: 34 * gib, want: 34 * gib / 8},
		{name: "mid-sync mainnet-c", dir: 6492 << 20, limit: 34 * gib, want: 34 * gib / 8},
		{name: "full mainnet, same container", dir: 157 * gib, limit: 34 * gib, want: 34 * gib / 8},
		{name: "the ceiling that OOMed", dir: 6492 << 20, limit: 24 * gib, want: 3 * gib},
		{name: "no cgroup keeps the dir formula", dir: 6492 << 20, want: (6492 << 20) * 3 / 100},
		{name: "no cgroup still caps at 4GB", dir: 157 * gib, want: fwCacheDiskCap},
		{name: "absurdly small container takes the floor", limit: 128 << 20, want: fwCacheFloor},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, why, err := firewoodCacheBytes(tc.dir, tc.limit)
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.want {
				t.Fatalf("firewoodCacheBytes(%d, %d) = %d, want %d", tc.dir, tc.limit, got, tc.want)
			}
			if why == "" {
				t.Fatal("every size must say where it came from")
			}
			// Never below the floor, and never above the ceiling the operator
			// wrote down (a container too small to run the binary excepted).
			if got < fwCacheFloor {
				t.Fatalf("%d is below the %d floor", got, uint64(fwCacheFloor))
			}
			if tc.limit >= fwCacheFloor*fwCacheShare && got > tc.limit {
				t.Fatalf("%d exceeds the container's own %d ceiling", got, tc.limit)
			}
		})
	}
}

// TestFirewoodCacheOverride pins that the knob wins over both derivations and
// that a bad value refuses to start instead of silently falling back, which is
// the whole point of having it.
func TestFirewoodCacheOverride(t *testing.T) {
	t.Setenv("EPOCHDB_FIREWOOD_CACHE", "8589934592")
	got, why, err := firewoodCacheBytes(157<<30, 34<<30)
	if err != nil || got != 8<<30 {
		t.Fatalf("override = %d, %v; want %d", got, err, uint64(8<<30))
	}
	if why != "EPOCHDB_FIREWOOD_CACHE override" {
		t.Fatalf("the source must name the override, got %q", why)
	}
	// "512" and "67108863" are the units mistake: a number that meant megabytes,
	// or one just under the floor. Both are refused rather than run slowly.
	for _, bad := range []string{"4GB", "-1", "0", "67108863", "512"} {
		t.Setenv("EPOCHDB_FIREWOOD_CACHE", bad)
		if _, _, err := firewoodCacheBytes(0, 34<<30); err == nil {
			t.Fatalf("EPOCHDB_FIREWOOD_CACHE=%q was accepted", bad)
		}
	}
}

// TestFirewoodCacheNoCeilingKeepsThe4GBDefault: verify's throwaway has no
// directory to measure, so where there is also no container ceiling the shared
// helper must still land on the 4GB that path used to hardcode. Pins the
// relationship between the two constants that makes the substitution safe.
func TestFirewoodCacheNoCeilingKeepsThe4GBDefault(t *testing.T) {
	got, _, err := firewoodCacheBytes(0, fwCacheDiskCap*fwCacheShare)
	if err != nil || got != fwCacheDiskCap {
		t.Fatalf("no-ceiling throwaway cache = %d, %v; want %d", got, err, uint64(fwCacheDiskCap))
	}
	if fwCacheDiskCap != 4<<30 {
		t.Fatalf("the historical default moved: fwCacheDiskCap = %d, want %d", uint64(fwCacheDiskCap), uint64(4<<30))
	}
}

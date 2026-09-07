package fetch

import (
	"crypto/sha256"
	"encoding/binary"
	"strings"
	"testing"

	avaconstants "github.com/ava-labs/avalanchego/utils/constants"
)

// fakeSource is N containers of `size` bytes each, container i at height i.
type fakeSource struct {
	n, size int
}

func (s fakeSource) ContainerAt(h uint64) ([]byte, error) {
	c := make([]byte, s.size)
	binary.BigEndian.PutUint64(c, h)
	return c, nil
}

func (s fakeSource) HeightByContainerID(id []byte) (uint64, bool, error) {
	for h := 0; h < s.n; h++ {
		c, _ := s.ContainerAt(uint64(h))
		if sum := sha256.Sum256(c); string(sum[:]) == string(id) {
			return uint64(h), true, nil
		}
	}
	return 0, false, nil
}

func idOf(s fakeSource, h uint64) []byte {
	c, _ := s.ContainerAt(h)
	sum := sha256.Sum256(c)
	return sum[:]
}

// THE WALK IS WHAT A BOOTSTRAPPING PEER CONSUMES: newest first, down to
// height 0, cut by avalanchego's own two limits.
func TestAncestorsWalk(t *testing.T) {
	src := fakeSource{n: 10, size: 16}

	// Unknown id: an empty batch, no error.
	if out, err := ancestorsOf(src, make([]byte, 32)); err != nil || len(out) != 0 {
		t.Fatalf("unknown id: %d containers, err %v", len(out), err)
	}
	// From height 3: 3, 2, 1, 0 and stop.
	out, err := ancestorsOf(src, idOf(src, 3))
	if err != nil || len(out) != 4 {
		t.Fatalf("from 3: %d containers, err %v", len(out), err)
	}
	for i, c := range out {
		if got := binary.BigEndian.Uint64(c); got != uint64(3-i) {
			t.Fatalf("out[%d] is height %d, want %d", i, got, 3-i)
		}
	}

	// The byte cap: containers of a fifth of the limit fit four, never five.
	big := fakeSource{n: 100, size: avaconstants.MaxContainersLen/5 + 1}
	out, err = ancestorsOf(big, idOf(big, 50))
	if err != nil || len(out) != 4 {
		t.Fatalf("byte cap: %d containers, err %v", len(out), err)
	}

	// The count cap.
	many := fakeSource{n: ancestorsMaxContainers + 10, size: 8}
	out, err = ancestorsOf(many, idOf(many, uint64(ancestorsMaxContainers+5)))
	if err != nil || len(out) != ancestorsMaxContainers {
		t.Fatalf("count cap: %d containers, err %v", len(out), err)
	}
}

func TestParsePeersRefusesBadShapesByName(t *testing.T) {
	good := "NodeID-7Xhw2mDxuDS44j42TCB6U5579esbSt3Lg@127.0.0.1:29638"
	m, err := parsePeers([]string{good})
	if err != nil || len(m) != 1 {
		t.Fatalf("good peer refused: %v", err)
	}
	for _, bad := range []string{"NodeID-7Xhw2mDxuDS44j42TCB6U5579esbSt3Lg", "nope@127.0.0.1:1", good[:len(good)-6] + ":x"} {
		if _, err := parsePeers([]string{bad}); err == nil || !strings.Contains(err.Error(), bad) {
			t.Errorf("%q: err %v, want a refusal naming it", bad, err)
		}
	}
}

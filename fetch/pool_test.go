package fetch

import (
	"testing"
	"time"

	"github.com/ava-labs/avalanchego/ids"
)

func TestPoolAcquirePrefersIdleThenFastest(t *testing.T) {
	p := newPeerPool()
	slow, fast, busy := ids.GenerateTestNodeID(), ids.GenerateTestNodeID(), ids.GenerateTestNodeID()
	for _, id := range []ids.NodeID{slow, fast, busy} {
		p.connected(id)
		p.setArchival(id, true)
	}
	p.observe(slow, 100, time.Second) // 100 blk/s
	p.observe(fast, 400, time.Second) // 400 blk/s
	p.observe(busy, 900, time.Second) // fastest but will be busy
	if got, ok, _ := p.acquire(1); !ok || got != busy {
		t.Fatalf("first acquire = %s, want fastest %s", got, busy)
	}
	// busy is now at capacity; next best rate wins.
	if got, ok, _ := p.acquire(1); !ok || got != fast {
		t.Fatalf("second acquire = %s, want %s", got, fast)
	}
	if got, ok, _ := p.acquire(1); !ok || got != slow {
		t.Fatalf("third acquire = %s, want %s", got, slow)
	}
	if _, ok, have := p.acquire(1); ok || !have {
		t.Fatalf("all busy: ok=%v haveArchival=%v, want false/true", ok, have)
	}
	p.release(fast)
	p.setArchival(slow, false)
	if got, ok, _ := p.acquire(1); !ok || got != fast {
		t.Fatalf("after release acquire = %s, want %s", got, fast)
	}
	if p.archivalCount() != 2 {
		t.Fatalf("archivalCount = %d, want 2", p.archivalCount())
	}
}

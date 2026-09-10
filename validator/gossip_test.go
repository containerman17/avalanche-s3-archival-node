package validator

import (
	"crypto/rand"
	"testing"

	"github.com/ava-labs/avalanchego/graft/subnet-evm/plugin/evm/config"
	"github.com/ava-labs/avalanchego/network/p2p/gossip"
	"github.com/prometheus/client_golang/prometheus"
)

// The gossip bloom filter is a hint for peers, not a membership test: at
// its own target size it answers Has for about 1% of txs it never saw, and
// up to 5% before a reset. That is why inbound push txs are not filtered by
// it (a false positive would silently drop a first-time tx); the pool's
// exact hash check answers the duplicates.
func TestGossipBloomFalsePositives(t *testing.T) {
	b, err := gossip.NewBloomFilter(prometheus.NewRegistry(), "t", config.TxGossipBloomMinTargetElements,
		config.TxGossipBloomTargetFalsePositiveRate, config.TxGossipBloomResetFalsePositiveRate)
	if err != nil {
		t.Fatal(err)
	}
	raw := func() []byte { r := make([]byte, 100); rand.Read(r); return r }
	for i := 0; i < config.TxGossipBloomMinTargetElements; i++ {
		b.Add(newGossipTx(raw()))
	}
	const probes = 100_000
	fp := 0
	for i := 0; i < probes; i++ {
		if b.Has(newGossipTx(raw())) {
			fp++
		}
	}
	t.Logf("bloom at %d elements: %d of %d unseen txs answered Has (%.2f%%)", config.TxGossipBloomMinTargetElements, fp, probes, 100*float64(fp)/probes)
	if fp == 0 {
		t.Fatal("a bloom filter with a 1% target and no false positives: the test is not probing it")
	}
}

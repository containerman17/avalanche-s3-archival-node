package validator

// Adapted from subnet-evm plugin/evm/eth_gossiper.go (same wire format:
// gossip id = tx hash, payload = tx.MarshalBinary), over libevm's txpool.
// Importing plugin/evm would link firewood's Rust runtime next to the
// engine's, which the linker refuses.

import (
	"context"
	"fmt"
	"sync"

	"github.com/ava-labs/avalanchego/graft/subnet-evm/plugin/evm/config"
	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/network/p2p/gossip"
	"github.com/ava-labs/avalanchego/utils/bloom"
	ethcommon "github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core"
	"github.com/ava-labs/libevm/core/txpool"
	"github.com/ava-labs/libevm/core/types"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	_ gossip.Gossipable            = (*gossipTx)(nil)
	_ gossip.Marshaller[*gossipTx] = gossipMarshaller{}
	_ gossip.SystemSet[*gossipTx]  = (*gossipSet)(nil)
)

type gossipTx struct{ tx *types.Transaction }

func (g *gossipTx) GossipID() ids.ID { return ids.ID(g.tx.Hash()) }

type gossipMarshaller struct{}

func (gossipMarshaller) MarshalGossip(g *gossipTx) ([]byte, error) { return g.tx.MarshalBinary() }
func (gossipMarshaller) UnmarshalGossip(b []byte) (*gossipTx, error) {
	tx := new(types.Transaction)
	return &gossipTx{tx: tx}, tx.UnmarshalBinary(b)
}

// gossipSet is the gossip SDK's view of the pool: a bloom filter of what we
// hold (so peers skip it), Add for what they send, Iterate for what we push.
type gossipSet struct {
	pool  *txpool.TxPool
	bloom *gossip.BloomFilter
	mu    sync.RWMutex
}

func newGossipSet(pool *txpool.TxPool, reg prometheus.Registerer) (*gossipSet, error) {
	b, err := gossip.NewBloomFilter(reg, "eth_tx_bloom_filter", config.TxGossipBloomMinTargetElements,
		config.TxGossipBloomTargetFalsePositiveRate, config.TxGossipBloomResetFalsePositiveRate)
	if err != nil {
		return nil, fmt.Errorf("bloom filter: %w", err)
	}
	return &gossipSet{pool: pool, bloom: b}, nil
}

// subscribe keeps the bloom filter in step with the pool's promotions.
func (g *gossipSet) subscribe(ctx context.Context) {
	ch := make(chan core.NewTxsEvent, 16)
	sub := g.pool.SubscribeTransactions(ch, false)
	defer sub.Unsubscribe()
	for {
		select {
		case <-ctx.Done():
			return
		case ev := <-ch:
			g.mu.Lock()
			optimal := (g.pendingSize() + len(ev.Txs)) * config.TxGossipBloomChurnMultiplier
			for _, tx := range ev.Txs {
				g.bloom.Add(&gossipTx{tx: tx})
				if reset, err := gossip.ResetBloomFilterIfNeeded(g.bloom, optimal); err == nil && reset {
					g.Iterate(func(t *gossipTx) bool { g.bloom.Add(t); return true })
				}
			}
			g.mu.Unlock()
		}
	}
}

func (g *gossipSet) pendingSize() int {
	n := 0
	for _, txs := range g.pool.Pending(txpool.PendingFilter{}) {
		n += len(txs)
	}
	return n
}

func (g *gossipSet) Add(t *gossipTx) error {
	return g.pool.Add([]*types.Transaction{t.tx}, false, false)[0]
}

func (g *gossipSet) Has(id ids.ID) bool { return g.pool.Has(ethcommon.Hash(id)) }

func (g *gossipSet) Iterate(f func(*gossipTx) bool) {
	for _, txs := range g.pool.Pending(txpool.PendingFilter{}) {
		for _, lazy := range txs {
			if tx := lazy.Resolve(); tx != nil && !f(&gossipTx{tx: tx}) {
				return
			}
		}
	}
}

func (g *gossipSet) BloomFilter() (*bloom.Filter, ids.ID) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.bloom.BloomFilter()
}

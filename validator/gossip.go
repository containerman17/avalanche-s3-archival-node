package validator

// Adapted from subnet-evm plugin/evm/eth_gossiper.go (same wire format:
// gossip id = tx hash, payload = tx.MarshalBinary), over the engine's pool.
// Go never decodes a gossiped tx: the payload is the envelope the pool
// takes and hands back, the id is its keccak.

import (
	"context"
	"fmt"
	"sync"

	"github.com/ava-labs/avalanchego/graft/subnet-evm/plugin/evm/config"
	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/network/p2p"
	"github.com/ava-labs/avalanchego/network/p2p/gossip"
	"github.com/ava-labs/avalanchego/utils/bloom"
	"github.com/ava-labs/avalanchego/utils/logging"
	"github.com/ava-labs/libevm/crypto"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

var (
	_ gossip.Gossipable            = (*gossipTx)(nil)
	_ gossip.Marshaller[*gossipTx] = gossipMarshaller{}
	_ gossip.SystemSet[*gossipTx]  = (*gossipSet)(nil)
	_ p2p.Handler                  = (*txHandler)(nil)
)

// gossipTx is one tx envelope with its hash.
type gossipTx struct {
	raw []byte
	id  ids.ID
}

func newGossipTx(raw []byte) *gossipTx {
	return &gossipTx{raw: raw, id: ids.ID(crypto.Keccak256Hash(raw))}
}

func (g *gossipTx) GossipID() ids.ID { return g.id }

type gossipMarshaller struct{}

func (gossipMarshaller) MarshalGossip(g *gossipTx) ([]byte, error) { return g.raw, nil }
func (gossipMarshaller) UnmarshalGossip(b []byte) (*gossipTx, error) {
	if len(b) == 0 {
		return nil, fmt.Errorf("empty tx")
	}
	return newGossipTx(b), nil
}

// How many pool txs one Iterate walks (pull responses stop at the SDK's
// response size target long before; the bloom reset re-adds this many).
const gossipIterateMax = 50_000

// gossipSet is the gossip SDK's view of the engine's pool: a bloom filter of
// what we hold (so peers skip it), Add for what they send, Iterate for what
// we serve to pull requests.
type gossipSet struct {
	eng   *engine
	bloom *gossip.BloomFilter
	mu    sync.RWMutex
}

func newGossipSet(eng *engine, reg prometheus.Registerer) (*gossipSet, error) {
	b, err := gossip.NewBloomFilter(reg, "eth_tx_bloom_filter", config.TxGossipBloomMinTargetElements,
		config.TxGossipBloomTargetFalsePositiveRate, config.TxGossipBloomResetFalsePositiveRate)
	if err != nil {
		return nil, fmt.Errorf("bloom filter: %w", err)
	}
	return &gossipSet{eng: eng, bloom: b}, nil
}

// added keeps the bloom filter in step with the pool's admissions (the
// push loop drains them).
func (g *gossipSet) added(txs []*gossipTx) {
	g.mu.Lock()
	defer g.mu.Unlock()
	pending, _ := g.eng.poolStatus()
	optimal := (int(pending) + len(txs)) * config.TxGossipBloomChurnMultiplier
	for _, tx := range txs {
		g.bloom.Add(tx)
		if reset, err := gossip.ResetBloomFilterIfNeeded(g.bloom, optimal); err == nil && reset {
			g.Iterate(func(t *gossipTx) bool { g.bloom.Add(t); return true })
		}
	}
}

// Add: one tx from a pull response (the SDK calls it per element); a push
// message is admitted whole by txHandler.
func (g *gossipSet) Add(t *gossipTx) error {
	res, err := g.eng.poolAdd([][]byte{t.raw}, false)
	if err != nil {
		return err
	}
	return res[0].err()
}

func (g *gossipSet) Has(id ids.ID) bool { return g.eng.poolHas(id) }

func (g *gossipSet) Iterate(f func(*gossipTx) bool) {
	raws, err := g.eng.poolContent(nil, gossipIterateMax)
	if err != nil {
		return
	}
	for _, r := range raws {
		if !f(newGossipTx(r)) {
			return
		}
	}
}

func (g *gossipSet) BloomFilter() (*bloom.Filter, ids.ID) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.bloom.BloomFilter()
}

// txHandler is the SDK's gossip handler with AppGossip replaced: a push
// message's txs go to the pool in ONE crossing instead of one per tx.
type txHandler struct {
	p2p.Handler
	set *gossipSet
	log logging.Logger
}

func (h *txHandler) AppGossip(_ context.Context, nodeID ids.NodeID, gossipBytes []byte) {
	raws, err := gossip.ParseAppGossip(gossipBytes)
	if err != nil {
		h.log.Debug("failed to unmarshal gossip", zap.Error(err))
		return
	}
	if len(raws) == 0 {
		return
	}
	if _, err := h.set.eng.poolAdd(raws, false); err != nil {
		h.log.Debug("failed to add gossip to the pool", zap.Stringer("nodeID", nodeID), zap.Error(err))
	}
}

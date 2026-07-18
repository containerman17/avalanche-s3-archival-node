package fetch

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/ava-labs/avalanchego/ids"
	avacommon "github.com/ava-labs/avalanchego/snow/engine/common"
	"github.com/ava-labs/avalanchego/snow/validators"
	"github.com/ava-labs/avalanchego/subnets"
	avaconstants "github.com/ava-labs/avalanchego/utils/constants"
	"github.com/ava-labs/avalanchego/utils/rpc"
	"github.com/ava-labs/avalanchego/utils/set"
	"github.com/ava-labs/avalanchego/vms/platformvm"

	"github.com/containerman17/epochdb/fetch/consensus"
)

// vdrRefreshInterval is how often the validator set is re-fetched and
// cross-checked. RPC is never on the tip-latency path: the engine samples
// the in-memory manager; this loop runs in the background.
const vdrRefreshInterval = time.Hour

// Follow runs consensus-verified tip following: real snowman polls (ported
// flatstate follower engine) find and track the network's accepted frontier;
// every accepted container is appended to the staging store, and the gap
// between stored history and the anchor is backfilled by the regular
// archival-pool walk.
func (f *Fetcher) Follow(ctx context.Context) error {
	vdrs := validators.NewManager()
	weights, err := crossCheckedWeights(ctx, f.vdrSources())
	if err != nil {
		return fmt.Errorf("validator set: %w", err)
	}
	if err := reconcileValidators(vdrs, weights); err != nil {
		return err
	}
	totalWeight, _ := vdrs.TotalWeight(avaconstants.PrimaryNetworkID)
	log.Printf("fetch: validator set loaded validators=%d total_weight=%d",
		len(weights), totalWeight)
	go f.refreshValidators(ctx, vdrs)

	eng, err := consensus.New(consensus.Config{
		Net:   &consensusNet{f: f, vdrs: vdrs},
		Parse: parseForConsensus,
		OnAnchor: func(c *consensus.Container) {
			// Persist the anchor, then backfill everything below it with the
			// archival-pool walk until it short-circuits on stored history.
			if err := f.appendContainer(c); err != nil {
				log.Printf("fetch: store anchor: %v", err)
				return
			}
			go func() {
				f.activeWalks.Add(1)
				defer f.activeWalks.Add(-1)
				if err := f.walkSpan(ctx, c.ID, 0); err != nil && ctx.Err() == nil {
					log.Printf("fetch: anchor backfill walk: %v", err)
					return
				}
				log.Printf("fetch: anchor backfill complete height=%d", c.Height)
			}()
		},
		OnAccept: func(c *consensus.Container) error {
			if err := f.appendContainer(c); err != nil {
				return err
			}
			log.Printf("consensus: accepted height=%d container=%s", c.Height, c.ID)
			return nil
		},
	})
	if err != nil {
		return err
	}
	f.handler.setConsensusCallbacks(eng.OnContainer, eng.OnChits)
	defer f.handler.setConsensusCallbacks(nil, nil)
	go f.probeLoop(ctx) // keeps the archival pool healthy for the backfill walk

	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	status := time.NewTicker(10 * time.Second)
	defer status.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-eng.Fatal():
			return err
		case err := <-f.dispatchErrCh:
			return fmt.Errorf("network stopped: %w", err)
		case <-tick.C:
			eng.Tick()
		case <-status.C:
			s := eng.Stats()
			var lag int64
			if s.PeerAcceptedMax > 0 {
				lag = int64(s.PeerAcceptedMax) - int64(s.AcceptedHeight)
			}
			log.Printf("consensus: status live=%v accepted=%d lag=%d processing=%d polls_ok=%d polls_failed=%d parked=%d gets=%d stored=%d",
				s.Live, s.AcceptedHeight, lag, s.Processing, s.PollsOK, s.PollsFailed, s.ParkedVotes, s.OutstandingGets, f.store.Count())
		}
	}
}

// appendContainer persists a consensus container into the staging store.
func (f *Fetcher) appendContainer(c *consensus.Container) error {
	if err := f.store.Append(parsedContainer{
		containerID: c.ID,
		blockNumber: c.Height,
		blockHash:   c.EthHash,
		parentID:    c.ParentID,
		parentHash:  c.EthParentHash,
		blockTime:   c.Time,
	}, c.Bytes); err != nil {
		return fmt.Errorf("store append %d: %w", c.Height, err)
	}
	return f.store.Flush()
}

func parseForConsensus(raw []byte) (*consensus.Container, error) {
	p, err := parseContainer(raw)
	if err != nil {
		return nil, err
	}
	return &consensus.Container{
		ID:            p.containerID,
		ParentID:      p.parentID,
		Height:        p.blockNumber,
		EthHash:       p.blockHash,
		EthParentHash: p.parentHash,
		Time:          p.blockTime,
		Bytes:         raw,
	}, nil
}

func (f *Fetcher) vdrSources() []string {
	if len(f.cfg.VdrSources) > 0 {
		return f.cfg.VdrSources
	}
	return []string{f.cfg.NodeURI}
}

// consensusNet adapts the fetcher's transport and peer pool to the consensus
// engine's Net interface.
type consensusNet struct {
	f    *Fetcher
	vdrs validators.Manager
}

func (n *consensusNet) NextRequestID() uint32 { return n.f.reqIDCounter.Add(1) }

func (n *consensusNet) SampleValidators(k int) ([]ids.NodeID, error) {
	return n.vdrs.Sample(avaconstants.PrimaryNetworkID, k)
}

func (n *consensusNet) IsConnected(nodeID ids.NodeID) bool {
	return n.f.pool.isConnected(nodeID)
}

func (n *consensusNet) SelectPeer() (ids.NodeID, bool) {
	peers := n.f.pool.peersForFrontier(1) // archival (known-responsive) first
	if len(peers) == 0 {
		return ids.EmptyNodeID, false
	}
	return peers[0], true
}

func (n *consensusNet) SendGet(nodeID ids.NodeID, requestID uint32, containerID ids.ID) error {
	msg, err := n.f.creator.Get(n.f.chainID, requestID, consensus.RequestTimeout, containerID)
	if err != nil {
		return err
	}
	n.f.net.Send(msg, avacommon.SendConfig{NodeIDs: set.Of(nodeID)}, avaconstants.PrimaryNetworkID, subnets.NoOpAllower)
	return nil
}

func (n *consensusNet) SendPullQuery(nodeIDs set.Set[ids.NodeID], requestID uint32, containerID ids.ID, requestedHeight uint64) error {
	msg, err := n.f.creator.PullQuery(n.f.chainID, requestID, consensus.RequestTimeout, containerID, requestedHeight)
	if err != nil {
		return err
	}
	n.f.net.Send(msg, avacommon.SendConfig{NodeIDs: nodeIDs}, avaconstants.PrimaryNetworkID, subnets.NoOpAllower)
	return nil
}

// --- validator set: multiple cross-checked RPC sources (recorded ruling:
// no single external RPC decides tip trust; RPC stays off the latency path) ---

func fetchWeights(ctx context.Context, uri string) (map[ids.NodeID]uint64, error) {
	// A bare node URI gets the standard /ext/P path; a URI already naming an
	// /ext/... path is used verbatim (some public providers only serve
	// /ext/bc/P).
	client := platformvm.NewClient(uri)
	if strings.Contains(uri, "/ext/") {
		client = &platformvm.Client{Requester: rpc.NewEndpointRequester(uri)}
	}
	list, err := client.GetCurrentValidators(ctx, avaconstants.PrimaryNetworkID, nil)
	if err != nil {
		return nil, err
	}
	weights := make(map[ids.NodeID]uint64, len(list))
	for _, v := range list {
		weights[v.NodeID] += v.Weight
	}
	return weights, nil
}

// crossCheckedWeights fetches the validator set from every source and
// requires all successful answers to agree on >=95% of stake (boundary churn
// between two RPC calls legitimately moves a sliver of stake).
func crossCheckedWeights(ctx context.Context, uris []string) (map[ids.NodeID]uint64, error) {
	var (
		first    map[ids.NodeID]uint64
		firstURI string
		ok       int
	)
	for _, uri := range uris {
		cctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		w, err := fetchWeights(cctx, uri)
		cancel()
		if err != nil {
			log.Printf("fetch: validator source %s failed: %v", uri, err)
			continue
		}
		ok++
		if first == nil {
			first, firstURI = w, uri
			continue
		}
		if err := weightsAgree(first, w); err != nil {
			return nil, fmt.Errorf("validator sets disagree between %s and %s: %w", firstURI, uri, err)
		}
	}
	if first == nil {
		return nil, fmt.Errorf("all %d validator sources failed", len(uris))
	}
	if ok < 2 {
		log.Printf("fetch: WARNING: validator set from a single source (%s), cross-check unavailable", firstURI)
	}
	return first, nil
}

func weightsAgree(a, b map[ids.NodeID]uint64) error {
	var totalA, totalB, agreed uint64
	for id, w := range a {
		totalA += w
		if b[id] == w {
			agreed += w
		}
	}
	for _, w := range b {
		totalB += w
	}
	maxTotal := max(totalA, totalB)
	if maxTotal == 0 {
		return fmt.Errorf("empty validator sets")
	}
	if float64(agreed) < 0.95*float64(maxTotal) {
		return fmt.Errorf("only %.1f%% of stake agrees", 100*float64(agreed)/float64(maxTotal))
	}
	return nil
}

// reconcileValidators diffs the manager's primary-network set against the
// fetched weights: new validators are added, changed weights adjusted,
// missing validators removed. Weights of zero are dropped. (Ported verbatim
// from flatstate follower/net.)
func reconcileValidators(m validators.Manager, weights map[ids.NodeID]uint64) error {
	subnetID := avaconstants.PrimaryNetworkID
	for _, id := range m.GetValidatorIDs(subnetID) {
		cur := m.GetWeight(subnetID, id)
		want := weights[id]
		delete(weights, id)
		switch {
		case want == cur:
		case want > cur:
			if err := m.AddWeight(subnetID, id, want-cur); err != nil {
				return err
			}
		default:
			if err := m.RemoveWeight(subnetID, id, cur-want); err != nil {
				return err
			}
		}
	}
	for id, w := range weights {
		if w == 0 {
			continue
		}
		if err := m.AddStaker(subnetID, id, nil, ids.Empty, w); err != nil {
			return err
		}
	}
	return nil
}

func (f *Fetcher) refreshValidators(ctx context.Context, vdrs validators.Manager) {
	t := time.NewTicker(vdrRefreshInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		weights, err := crossCheckedWeights(ctx, f.vdrSources())
		if err != nil {
			log.Printf("fetch: validator refresh failed (keeping last-good set): %v", err)
			continue
		}
		if err := reconcileValidators(vdrs, weights); err != nil {
			log.Printf("fetch: validator reconcile failed: %v", err)
		}
	}
}

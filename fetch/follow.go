package fetch

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync/atomic"
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
// followBackfillWalks is the concurrency of the anchor backfill, matching the
// --walks default of the tip-override path: the two do the same work.
const followBackfillWalks = 16

func (f *Fetcher) Follow(ctx context.Context) error {
	weights, err := crossCheckedWeights(ctx, f.vdrSources(), f.subnetID)
	if err != nil {
		return fmt.Errorf("validator set: %w", err)
	}
	vdrs, err := managerFor(weights, f.subnetID)
	if err != nil {
		return fmt.Errorf("validator set: %w", err)
	}
	cnet := &consensusNet{f: f}
	cnet.vdrs.Store(vdrs)
	totalWeight, _ := vdrs.TotalWeight(f.subnetID)
	log.Printf("fetch: validator set loaded validators=%d total_weight=%d",
		len(weights), totalWeight)
	go f.refreshValidators(ctx, cnet)

	eng, err := consensus.New(consensus.Config{
		Net:   cnet,
		Parse: parseForConsensus,
		OnAnchor: func(c *consensus.Container) error {
			// Persist the anchor, then backfill everything below it with the
			// archival-pool walk until it short-circuits on stored history.
			// AN ANCHOR THAT DID NOT PERSIST IS FATAL: logging and returning
			// left the anchor unstored and the backfill never started, on a
			// node that otherwise looked live.
			if err := f.appendContainer(c); err != nil {
				return fmt.Errorf("store anchor at height %d: %w", c.Height, err)
			}
			go func() {
				// SEED FROM THE CHECKPOINTS, like SyncTo does. One walk from the
				// anchor down to genesis is serial: measured at 139 blk/s on
				// mainnet C, which is a 6.8-day stall with the executor idle
				// behind it. The embedded checkpoints split the same history into
				// spans that walk concurrently. An L1 has none, so anchors is
				// just the tip there and this is exactly the old behaviour.
				anchors, err := f.ResolveCheckpoints(ctx)
				if err != nil {
					if ctx.Err() != nil {
						return
					}
					// Not fatal: one serial walk is slow, not wrong.
					log.Printf("fetch: anchor backfill: checkpoints unresolved (%v), falling back to a single serial walk", err)
					anchors = nil
				}
				anchors = append(anchors, Anchor{ID: c.ID, Height: c.Height})
				log.Printf("fetch: anchor backfill height=%d spans=%d walks=%d", c.Height, len(anchors), followBackfillWalks)
				if err := f.walkSpans(ctx, anchors, followBackfillWalks); err != nil && ctx.Err() == nil {
					log.Printf("fetch: anchor backfill walk: %v", err)
					return
				}
				log.Printf("fetch: anchor backfill complete height=%d", c.Height)
			}()
			return nil
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
			log.Printf("consensus: status live=%v accepted=%d lag=%d processing=%d polls_ok=%d polls_failed=%d parked=%d gets=%d stored=%d%s",
				s.Live, s.AcceptedHeight, lag, s.Processing, s.PollsOK, s.PollsFailed, s.ParkedVotes, s.OutstandingGets, f.store.Count(),
				f.Progress().DropSuffix())
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
	f *Fetcher
	// vdrs is swapped WHOLE by every refresh, never mutated in place: see
	// managerFor.
	vdrs atomic.Value // validators.Manager
}

func (n *consensusNet) manager() validators.Manager {
	return n.vdrs.Load().(validators.Manager)
}

func (n *consensusNet) NextRequestID() uint32 { return n.f.reqIDCounter.Add(1) }

func (n *consensusNet) SampleValidators(k int) ([]ids.NodeID, error) {
	return n.manager().Sample(n.f.subnetID, k)
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
	to := set.Of(nodeID)
	noteSendGap("Get", to, n.f.net.Send(msg, avacommon.SendConfig{NodeIDs: to}, n.f.subnetID, subnets.NoOpAllower))
	return nil
}

func (n *consensusNet) SendPullQuery(nodeIDs set.Set[ids.NodeID], requestID uint32, containerID ids.ID, requestedHeight uint64) error {
	msg, err := n.f.creator.PullQuery(n.f.chainID, requestID, consensus.RequestTimeout, containerID, requestedHeight)
	if err != nil {
		return err
	}
	noteSendGap("PullQuery", nodeIDs, n.f.net.Send(msg, avacommon.SendConfig{NodeIDs: nodeIDs}, n.f.subnetID, subnets.NoOpAllower))
	return nil
}

// --- validator set: multiple cross-checked RPC sources (recorded ruling:
// no single external RPC decides tip trust; RPC stays off the latency path) ---

func fetchWeights(ctx context.Context, uri string, subnetID ids.ID) (map[ids.NodeID]uint64, error) {
	// A bare node URI gets the standard /ext/P path; a URI already naming an
	// /ext/... path is used verbatim (some public providers only serve
	// /ext/bc/P).
	client := platformvm.NewClient(uri)
	if strings.Contains(uri, "/ext/") {
		client = &platformvm.Client{Requester: rpc.NewEndpointRequester(uri)}
	}
	list, err := client.GetCurrentValidators(ctx, subnetID, nil)
	if err != nil {
		return nil, err
	}
	weights := make(map[ids.NodeID]uint64, len(list))
	for _, v := range list {
		weights[v.NodeID] += v.Weight
	}
	return weights, nil
}

// crossCheckedWeights fetches the validator set from every source and requires
// all successful answers to agree.
//
// HOW MUCH THEY MUST AGREE DEPENDS ON THE SUBNET, and it is not a tuning knob:
//   - Primary network: >=95% of stake. Delegations start and stop every block,
//     so two RPC calls seconds apart legitimately differ by a sliver, and with
//     ~600 validators no single one is anywhere near 5% of stake.
//   - An L1 (ACP-77): EXACT. Its weights are the static registration weights
//     from RegisterL1ValidatorTx, not a balance: the continuous fee drains the
//     separate `balance` field and never moves `weight`. So there is no churn
//     to absorb, and the 95% band is mis-calibrated anyway (FIFA has 25
//     validators, one of them 38% of the total weight, so a single validator
//     joining can blow past 5% while a large one leaving cannot be
//     distinguished from a lying source). Exact agreement is the honest rule:
//     any difference is a real registration event, and the caller retries.
func crossCheckedWeights(ctx context.Context, uris []string, subnetID ids.ID) (map[ids.NodeID]uint64, error) {
	var (
		first    map[ids.NodeID]uint64
		firstURI string
		ok       int
	)
	exact := subnetID != avaconstants.PrimaryNetworkID
	for _, uri := range uris {
		cctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		w, err := fetchWeights(cctx, uri, subnetID)
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
		if err := weightsAgree(first, w, exact); err != nil {
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

func weightsAgree(a, b map[ids.NodeID]uint64, exact bool) error {
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
	if exact {
		if len(a) != len(b) || agreed != maxTotal {
			return fmt.Errorf("sets differ (%d vs %d validators, %d of %d weight agrees) and this subnet's weights are static, so a difference is a real registration or a bad source",
				len(a), len(b), agreed, maxTotal)
		}
		return nil
	}
	if float64(agreed) < 0.95*float64(maxTotal) {
		return fmt.Errorf("only %.1f%% of stake agrees", 100*float64(agreed)/float64(maxTotal))
	}
	return nil
}

// managerFor builds a manager holding exactly these weights (zero weights
// dropped).
//
// A FRESH MANAGER, NOT A DIFF APPLIED TO THE LIVE ONE. The old reconcile
// mutated the manager consensus was sampling as it walked, so a failure
// halfway through left a HALF-APPLIED validator set in place, and the next
// refresh was an hour away. Building the new set separately makes a failure
// leave the last-good one exactly as it was, and the swap is one store.
func managerFor(weights map[ids.NodeID]uint64, subnetID ids.ID) (validators.Manager, error) {
	m := validators.NewManager()
	for id, w := range weights {
		if w == 0 {
			continue
		}
		if err := m.AddStaker(subnetID, id, nil, ids.Empty, w); err != nil {
			return nil, fmt.Errorf("add validator %s: %w", id, err)
		}
	}
	if len(m.GetValidatorIDs(subnetID)) == 0 {
		return nil, fmt.Errorf("no validator in the set carries any weight")
	}
	return m, nil
}

func (f *Fetcher) refreshValidators(ctx context.Context, cnet *consensusNet) {
	t := time.NewTicker(vdrRefreshInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		weights, err := crossCheckedWeights(ctx, f.vdrSources(), f.subnetID)
		if err != nil {
			log.Printf("fetch: validator refresh failed (keeping last-good set): %v", err)
			continue
		}
		vdrs, err := managerFor(weights, f.subnetID)
		if err != nil {
			log.Printf("fetch: validator refresh built no usable set (keeping last-good): %v", err)
			continue
		}
		cnet.vdrs.Store(vdrs)
	}
}

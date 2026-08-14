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

	"github.com/containerman17/epochdb/dist"
	"github.com/containerman17/epochdb/fetch/consensus"
)

// vdrRefreshInterval is how often the validator set is re-fetched and
// cross-checked, JITTERED per round (dist.Jitter). RPC is never on the
// tip-latency path: the engine samples the in-memory manager; this loop runs
// in the background.
//
// FOUR HOURS, NOT ONE (2026-08-14). `platform.getCurrentValidators` on the
// primary network is the heaviest call this node makes of a public endpoint,
// and dozens of containers on one box made it every hour on the same tick,
// which is a self-inflicted rate limit for data that moves in days. A stake
// change we miss for a few hours changes nothing: the manager only has to be
// a fair sample of who is validating, and a mid-refresh failure keeps the
// last-good set anyway.
const vdrRefreshInterval = 4 * time.Hour

// Follow runs consensus-verified tip following: real snowman polls (ported
// flatstate follower engine) find and track the network's accepted frontier.
// THE FOLLOWER OWNS THE REAL TIP ZONE and nothing else: its anchor tells the
// forward fetch where the chain ends, and every accepted container is offered
// to the RAM queue, which takes it only when the window has climbed to it.
// Until then the offer is dropped and the forward fetch will ask for that
// height in its own turn, so the handover needs no state of its own.
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
			// The anchor is where the chain ends, which is the window rule's
			// ceiling. The forward fetch climbs to it on its own.
			f.noteTip(c.Height)
			f.offer(c)
			log.Printf("fetch: consensus anchor height=%d, forward fetch is at %d", c.Height, f.q.Head())
			return nil
		},
		OnAccept: func(c *consensus.Container) error {
			f.noteTip(c.Height)
			if f.offer(c) {
				log.Printf("consensus: accepted height=%d container=%s", c.Height, c.ID)
			}
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
			p := f.Progress()
			log.Printf("consensus: status live=%v accepted=%d lag=%d processing=%d polls_ok=%d polls_failed=%d parked=%d gets=%d fetched=%d%s",
				s.Live, s.AcceptedHeight, lag, s.Processing, s.PollsOK, s.PollsFailed, s.ParkedVotes, s.OutstandingGets, p.Head,
				p.DropSuffix())
		}
	}
}

// offer hands an accepted container to the RAM queue. It is taken only if it
// is the next block the executor needs; while the window is still climbing
// through history the offer is dropped, and the forward fetch asks for that
// height when it gets there. THAT IS THE WHOLE HANDOVER.
func (f *Fetcher) offer(c *consensus.Container) bool {
	if f.q == nil {
		return false
	}
	return f.q.Append(parsedContainer{
		containerID: c.ID,
		blockNumber: c.Height,
		blockHash:   c.EthHash,
		parentID:    c.ParentID,
		parentHash:  c.EthParentHash,
		blockTime:   c.Time,
	}, c.Bytes)
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
	// A comma-separated --node is already several sources, so it cross-checks
	// like --vdr-sources does. One host is still one host, and still warns.
	return f.cfg.sources()
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
//
// THE WHOLE PASS IS THE RETRY UNIT, not the individual source (2026-08-14).
// Retrying each source in turn would let one dead endpoint spend the entire
// budget before the healthy one is ever asked, which is a slow start for a
// node that had a good answer waiting. So a round asks everyone once, and only
// a round that produced NO usable answer is retried: a source that fails is
// skipped exactly as it always was, loudly, and the cross-check rule below it
// is untouched. A disagreement is retried too, because on an L1 it is most
// often a registration landing mid-round.
func crossCheckedWeights(ctx context.Context, uris []string, subnetID ids.ID) (map[ids.NodeID]uint64, error) {
	var (
		first    map[ids.NodeID]uint64
		firstURI string
		ok       int
	)
	exact := subnetID != avaconstants.PrimaryNetworkID
	err := dist.Try(ctx, "cross-checked validator set", []string{strings.Join(uris, " ")}, func(ctx context.Context, _ string) error {
		first, firstURI, ok = nil, "", 0
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
				return fmt.Errorf("validator sets disagree between %s and %s: %w", firstURI, uri, err)
			}
		}
		if first == nil {
			return fmt.Errorf("all %d validator sources failed", len(uris))
		}
		return nil
	})
	if err != nil {
		return nil, err
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
	t := time.NewTimer(dist.Jitter(vdrRefreshInterval))
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		t.Reset(dist.Jitter(vdrRefreshInterval))
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

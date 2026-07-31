package consensus

import (
	"fmt"
	"testing"
	"time"

	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/snow/consensus/snowball"
	"github.com/ava-labs/avalanchego/utils/set"
)

// fakeNet records outbound traffic.
type fakeNet struct {
	reqID uint32
	vdrs  []ids.NodeID
	// sample, when set, is returned verbatim by SampleValidators (duplicates
	// included), modelling weighted sampling with replacement.
	sample      []ids.NodeID
	pullQueries []pullQuery
	gets        []ids.ID
}

type pullQuery struct {
	reqID     uint32
	container ids.ID
	height    uint64
}

func (f *fakeNet) NextRequestID() uint32 { f.reqID++; return f.reqID }
func (f *fakeNet) SampleValidators(k int) ([]ids.NodeID, error) {
	if f.sample != nil {
		return f.sample, nil
	}
	out := make([]ids.NodeID, k)
	for i := range out {
		out[i] = f.vdrs[i%len(f.vdrs)]
	}
	return out, nil
}
func (f *fakeNet) IsConnected(ids.NodeID) bool    { return true }
func (f *fakeNet) SelectPeer() (ids.NodeID, bool) { return f.vdrs[0], true }
func (f *fakeNet) SendGet(_ ids.NodeID, _ uint32, containerID ids.ID) error {
	f.gets = append(f.gets, containerID)
	return nil
}
func (f *fakeNet) SendPullQuery(_ set.Set[ids.NodeID], reqID uint32, containerID ids.ID, height uint64) error {
	f.pullQueries = append(f.pullQueries, pullQuery{reqID, containerID, height})
	return nil
}

// chain builds anchor + n children; raw bytes are synthetic and resolved by
// the injected Parse.
func chain(n int) (containers []*Container, parse func([]byte) (*Container, error)) {
	byRaw := make(map[string]*Container)
	var parentID, parentHash ids.ID
	for i := 0; i <= n; i++ {
		id := ids.ID{byte(i + 1)}
		ethHash := ids.ID{0xee, byte(i + 1)}
		c := &Container{
			ID:            id,
			ParentID:      parentID,
			Height:        uint64(100 + i),
			EthHash:       ethHash,
			EthParentHash: parentHash,
			Time:          uint64(time.Now().Unix()),
			Bytes:         []byte(fmt.Sprintf("raw-%d", i)),
		}
		byRaw[string(c.Bytes)] = c
		containers = append(containers, c)
		parentID, parentHash = id, ethHash
	}
	return containers, func(raw []byte) (*Container, error) {
		c, ok := byRaw[string(raw)]
		if !ok {
			return nil, fmt.Errorf("unknown raw")
		}
		return c, nil
	}
}

func params(k, alpha, beta int) snowball.Parameters {
	return snowball.Parameters{
		K: k, AlphaPreference: alpha, AlphaConfidence: alpha, Beta: beta,
		ConcurrentRepolls: 1, OptimalProcessing: 1,
		MaxOutstandingItems: 16, MaxItemProcessingTime: time.Minute,
	}
}

// bootstrapped builds an engine, drives it through anchor discovery, and
// returns it live at containers[0] as the accepted anchor.
func bootstrapped(t *testing.T, net *fakeNet, p snowball.Parameters, cs []*Container, parse func([]byte) (*Container, error), accepted *[]uint64) *Engine {
	t.Helper()
	e, err := New(Config{
		Net:   net,
		Parse: parse,
		OnAccept: func(c *Container) error {
			*accepted = append(*accepted, c.Height)
			return nil
		},
		Params: p,
	})
	if err != nil {
		t.Fatal(err)
	}
	e.Tick() // sends bootstrap poll
	if len(net.pullQueries) != 1 || net.pullQueries[0].container != ids.Empty {
		t.Fatalf("expected 1 bootstrap PullQuery(Empty), got %+v", net.pullQueries)
	}
	bsReq := net.pullQueries[0].reqID
	anchor := cs[0]
	for i := 0; i < p.AlphaPreference; i++ {
		e.OnChits(net.vdrs[i%len(net.vdrs)], bsReq, anchor.ID, anchor.ID, anchor.ID, anchor.Height)
	}
	if len(net.gets) == 0 || net.gets[len(net.gets)-1] != anchor.ID {
		t.Fatalf("expected anchor Get for %s, got %v", anchor.ID, net.gets)
	}
	e.OnContainer(net.vdrs[0], anchor.Bytes)
	if !e.Stats().Live {
		t.Fatal("engine not live after anchor container")
	}
	return e
}

// TestBootstrapAnchorOnSkewedValidatorSet: the K=20 sample of a stake-skewed
// L1 collapses onto a few nodes (FIFA: 25 validators, 5 holding 78.5% of the
// weight, mean 8.7 distinct per sample). The anchor tally must weigh each
// chit by that node's draws, or AlphaPreference is unreachable and the engine
// never goes live.
func TestBootstrapAnchorOnSkewedValidatorSet(t *testing.T) {
	heavy, light := ids.BuildTestNodeID([]byte{1}), ids.BuildTestNodeID([]byte{2})
	sample := make([]ids.NodeID, 20)
	for i := range sample {
		sample[i] = heavy // 16 draws
		if i >= 16 {
			sample[i] = light // 4 draws
		}
	}
	net := &fakeNet{vdrs: []ids.NodeID{heavy, light}, sample: sample}
	cs, parse := chain(0)
	anchor := cs[0]

	e, err := New(Config{
		Net: net, Parse: parse,
		OnAccept: func(*Container) error { return nil },
		Params:   params(20, 15, 20),
	})
	if err != nil {
		t.Fatal(err)
	}
	e.Tick()
	bsReq := net.pullQueries[0].reqID

	// The light node alone is 4 of 20 draws: nowhere near alpha.
	e.OnChits(light, bsReq, anchor.ID, anchor.ID, anchor.ID, anchor.Height)
	if len(net.gets) != 0 {
		t.Fatalf("anchored on 4 of 20 draws: %v", net.gets)
	}
	// The heavy node's 16 draws clear alpha=15 on their own, even though only
	// two distinct nodes ever answered.
	e.OnChits(heavy, bsReq, anchor.ID, anchor.ID, anchor.ID, anchor.Height)
	if len(net.gets) != 1 || net.gets[0] != anchor.ID {
		t.Fatalf("expected anchor Get for %s after 20 of 20 draws agreed, got %v", anchor.ID, net.gets)
	}
	e.OnContainer(heavy, anchor.Bytes)
	if !e.Stats().Live {
		t.Fatal("engine not live after the anchor container arrived")
	}
}

// TestAcceptanceThreshold: a block is accepted only once alphaConfidence
// distinct validators vote for it in one poll; a lone vote in a K=2 poll
// must not accept.
func TestAcceptanceThreshold(t *testing.T) {
	v1, v2 := ids.BuildTestNodeID([]byte{1}), ids.BuildTestNodeID([]byte{2})
	net := &fakeNet{vdrs: []ids.NodeID{v1, v2}}
	cs, parse := chain(1)
	var accepted []uint64
	e := bootstrapped(t, net, params(2, 2, 1), cs, parse, &accepted)

	blk := cs[1]
	e.OnContainer(v1, blk.Bytes) // issue the block; going live sent a poll
	if len(net.pullQueries) < 2 {
		e.Tick()
	}
	q := net.pullQueries[len(net.pullQueries)-1]
	e.OnChits(v1, q.reqID, blk.ID, blk.ID, cs[0].ID, cs[0].Height)
	if len(accepted) != 0 {
		t.Fatalf("accepted with 1 of 2 required votes: %v", accepted)
	}
	e.OnChits(v2, q.reqID, blk.ID, blk.ID, cs[0].ID, cs[0].Height)
	if len(accepted) != 1 || accepted[0] != blk.Height {
		t.Fatalf("expected height %d accepted after alpha votes, got %v", blk.Height, accepted)
	}
}

// TestVoteParking: a chit naming an unknown block triggers a Get and parks
// the vote; the vote applies (and accepts) when the container arrives.
func TestVoteParking(t *testing.T) {
	v1 := ids.BuildTestNodeID([]byte{1})
	net := &fakeNet{vdrs: []ids.NodeID{v1}}
	cs, parse := chain(1)
	var accepted []uint64
	e := bootstrapped(t, net, params(1, 1, 1), cs, parse, &accepted)

	blk := cs[1]
	q := net.pullQueries[len(net.pullQueries)-1] // poll sent on going live
	getsBefore := len(net.gets)
	e.OnChits(v1, q.reqID, blk.ID, blk.ID, cs[0].ID, cs[0].Height)
	if len(accepted) != 0 {
		t.Fatalf("accepted a vote for an unknown block: %v", accepted)
	}
	if len(net.gets) <= getsBefore || net.gets[len(net.gets)-1] != blk.ID {
		t.Fatalf("expected a Get for the unknown block %s, got %v", blk.ID, net.gets)
	}
	if e.Stats().ParkedVotes != 1 {
		t.Fatalf("expected 1 parked vote, got %d", e.Stats().ParkedVotes)
	}
	e.OnContainer(v1, blk.Bytes) // container arrives; parked vote applies
	if len(accepted) != 1 || accepted[0] != blk.Height {
		t.Fatalf("expected height %d accepted after container arrival, got %v", blk.Height, accepted)
	}
	if e.Stats().ParkedVotes != 0 {
		t.Fatalf("parked votes not drained: %d", e.Stats().ParkedVotes)
	}
}

// TestParkedVoteExpiry: a parked vote whose container never arrives expires
// fail-safe (poll fails, nothing accepted).
func TestParkedVoteExpiry(t *testing.T) {
	v1 := ids.BuildTestNodeID([]byte{1})
	net := &fakeNet{vdrs: []ids.NodeID{v1}}
	cs, parse := chain(1)
	var accepted []uint64
	e := bootstrapped(t, net, params(1, 1, 1), cs, parse, &accepted)

	blk := cs[1]
	q := net.pullQueries[len(net.pullQueries)-1]
	e.OnChits(v1, q.reqID, blk.ID, blk.ID, cs[0].ID, cs[0].Height)
	// Force the parked vote past its deadline and sweep.
	e.mu.Lock()
	for id, votes := range e.parked {
		for i := range votes {
			votes[i].deadline = time.Now().Add(-time.Second)
		}
		e.parked[id] = votes
	}
	e.expireParked(time.Now())
	e.mu.Unlock()
	if len(accepted) != 0 {
		t.Fatalf("expired parked vote accepted a block: %v", accepted)
	}
	if e.Stats().ParkedVotes != 0 {
		t.Fatalf("parked votes not expired: %d", e.Stats().ParkedVotes)
	}
}

// Package consensus is a snowman finality engine for tip following, ported
// from flatstate's battle-tested follower (github.com/containerman17/flatstate
// follower/consensus, proven 24h+ on mainnet tip): real snowman sampling
// against the weighted validator set. Gossip and Get responses provide
// containers; our own PullQuery polls decide preference and acceptance
// locally.
//
// Like the original, it reuses avalanchego's snow/consensus/snowman.Topological
// consensus object and its poll.Set / early-term poll accounting verbatim,
// and reimplements only the thin engine shell around them (K-sampling from
// the validator manager, PullQuery out, Chits in, vote bubbling to the
// nearest processing ancestor, poll expiry). Embedding snow/engine/snowman
// was rejected there (and here) because it drags in block.ChainVM, the
// common.Sender/router timeout machinery, and the bootstrap engines to
// replace ~300 lines of shell. Topological is the part with actual consensus
// semantics (vote transitivity, preference switching, commit rule); that is
// what must not be reimplemented, and it is not.
//
// Deviations from flatstate's shell (all epochdb adaptations, not semantics):
//   - No Executor/Sink/Resume: epochdb does not execute blocks at the tip;
//     accepted containers are offered to the fetch queue via OnAccept, and
//     the history below the anchor is fetched forward by the window (which
//     OnAnchor gives the tip to), not by the engine. Backfill here fetches
//     only the anchor container itself.
//   - Containers are parsed by an injected Parse (epochdb's parser), so this
//     package does not import libevm.
//
// Deviations from the real avalanchego engine, all fail-safe (they can only
// delay acceptance, never accept something the network did not):
//   - Chits votes naming an unknown block are parked while the block is
//     fetched (Get) and applied when it arrives, or dropped on timeout.
//   - No push gossip and no query serving: we never answer PullQuery (we
//     are not a validator; nobody samples us).
//   - Poll expiry is a coarse per-tick sweep instead of a per-request
//     timeout manager.
package consensus

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/snow"
	"github.com/ava-labs/avalanchego/snow/consensus/snowball"
	"github.com/ava-labs/avalanchego/snow/consensus/snowman"
	"github.com/ava-labs/avalanchego/snow/consensus/snowman/poll"
	"github.com/ava-labs/avalanchego/utils/bag"
	"github.com/ava-labs/avalanchego/utils/logging"
	"github.com/ava-labs/avalanchego/utils/set"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	// RequestTimeout is the deadline advertised on outbound requests; the
	// engine owns actual timeout handling (there is no router here).
	RequestTimeout = 10 * time.Second
	// minPollTimeout is a hard floor: expiring a poll whose real chits are
	// still in flight fails it, and every failed poll resets snowball
	// confidence (beta consecutive successes required), which is how
	// acceptance stalls. Flatstate measured live 2026-07-16: 3s stalled,
	// 10s tracked the chain.
	minPollTimeout = 10 * time.Second
	// concurrentRepolls: our polls complete in ~2-3s (chits plus
	// parked-vote resolution), not the ~500ms of a well-connected
	// validator; more polls in flight compensate so beta accumulates at
	// chain pace. Flatstate measured live 2026-07-16: 4 in flight =
	// 1.4 polls/s = crawl-and-burst; 16 tracked.
	concurrentRepolls = 16
	// maxOutstandingGets caps container fetches in flight: validators
	// throttle a zero-weight non-validator hard, and over-subscription
	// degenerates into timeout churn (flatstate measured live: a ~4000
	// outstanding fan-out collapsed to <20 req/s). Parked votes expire
	// fail-safe when a Get cannot be sent.
	maxOutstandingGets = 64
	// getRetry is the per-Get retry deadline: an unanswered Get parks
	// votes; re-ask another peer quickly instead of letting them expire.
	getRetry    = 2500 * time.Millisecond
	maxGetTries = 10
)

// Container is a parsed C-chain consensus container: the ProposerVM wrapper
// identity plus what the engine needs of the inner eth block.
type Container struct {
	ID            ids.ID
	ParentID      ids.ID
	Height        uint64 // inner eth block number
	EthHash       ids.ID // inner eth block hash
	EthParentHash ids.ID // inner eth block's parent hash
	Time          uint64 // inner eth block timestamp (unix seconds)
	Bytes         []byte
}

// Net is the outbound side the engine needs; the fetcher's adapter
// implements it over the epochdb peer pool.
type Net interface {
	NextRequestID() uint32
	// SampleValidators samples k validators by stake weight (duplicates
	// possible for heavy validators, matching the snowman engine).
	SampleValidators(k int) ([]ids.NodeID, error)
	IsConnected(nodeID ids.NodeID) bool
	// SelectPeer picks a responsive peer for a Get-style request.
	SelectPeer() (ids.NodeID, bool)
	SendGet(nodeID ids.NodeID, requestID uint32, containerID ids.ID) error
	SendPullQuery(nodeIDs set.Set[ids.NodeID], requestID uint32, containerID ids.ID, requestedHeight uint64) error
}

type Config struct {
	Net Net
	// Parse decodes a raw container; injected so this package stays free of
	// eth decoding.
	Parse func(raw []byte) (*Container, error)
	// OnAnchor is called once, when the network's accepted anchor has been
	// fetched and consensus goes live. The caller backfills the gap between
	// its stored history and the anchor (the engine does not). A non-nil
	// error is fatal, exactly as for OnAccept: an anchor the caller could not
	// persist is one no backfill will ever start from.
	OnAnchor func(c *Container) error
	// OnAccept is called for every block consensus accepts, in height
	// order, starting just above the anchor. A non-nil error is fatal.
	OnAccept func(c *Container) error

	Params snowball.Parameters // zero = snowball.DefaultParameters
	// PollInterval is the live poll cadence while blocks are processing;
	// IdlePollInterval is the discovery cadence while quiesced. PollTimeout
	// bounds how long a poll that cannot early-terminate holds one of the
	// ConcurrentRepolls slots; it is floored at minPollTimeout.
	PollInterval     time.Duration // default 100ms
	IdlePollInterval time.Duration // default 1s
	PollTimeout      time.Duration // default RequestTimeout
}

type phase int

const (
	phasePollAnchor phase = iota // polling validators for the accepted anchor
	phaseAnchorGet               // fetching the anchor container via Get
	phaseLive
)

type rec struct {
	c      *Container
	issued bool
}

type pollState struct {
	remaining set.Set[ids.NodeID]
	deadline  time.Time
}

type getReq struct {
	deadline time.Time
	tries    int
}

// parkedVote is a chit whose blocks were unknown when it arrived; it is
// retried when the blocking container shows up (mirrors the real engine's
// blocked-jobs scheduling, minimally).
type parkedVote struct {
	requestID uint32
	nodeID    ids.NodeID
	options   []ids.ID // decreasing height: preferred, preferredAtHeight
	deadline  time.Time
}

// Stats is a snapshot for progress logging.
type Stats struct {
	Live            bool
	AcceptedHeight  uint64
	PeerAcceptedMax uint64 // highest accepted height any peer reported
	Processing      int
	PollsOK         uint64
	PollsFailed     uint64
	ParkedVotes     int
	OutstandingGets int
}

// Engine drives consensus. All exported methods are concurrency-safe; wire
// OnContainer/OnChits to the inbound handler and call Tick on a ~50ms timer.
type Engine struct {
	cfg Config

	mu sync.Mutex

	phase phase
	fatal chan error

	// bootstrap (phasePollAnchor)
	bsReqID uint32
	bsVotes map[ids.ID]int
	// bsSample is this poll's K draws per node. Sampling is weighted WITH
	// replacement, so a node drawn n times casts n votes, exactly as
	// poll.Vote credits bag multiplicity in the live path.
	bsSample map[ids.NodeID]int
	bsVoters set.Set[ids.NodeID]
	bsHeight map[ids.ID]uint64
	lastBSAt time.Time

	// anchor fetch (phaseAnchorGet)
	anchorID ids.ID
	agDue    time.Time

	// live
	cons        *snowman.Topological
	polls       poll.Set
	pollVdrs    map[uint32]*pollState
	recs        map[ids.ID]*rec
	pendingByPa map[ids.ID][]ids.ID     // children container IDs waiting for parent
	gets        map[ids.ID]*getReq      // outstanding container fetches
	parked      map[ids.ID][]parkedVote // votes blocked on an unknown container
	frontiers   map[ids.NodeID]ids.ID   // per-peer last known accepted frontier
	lastPollAt  time.Time
	pollsOK     uint64
	pollsFailed uint64
	peerAccMax  uint64

	lastAcceptedID      ids.ID
	lastAcceptedHeight  uint64
	lastAcceptedEthHash ids.ID
	lastAcceptAt        time.Time
}

func New(cfg Config) (*Engine, error) {
	if cfg.Net == nil || cfg.Parse == nil || cfg.OnAccept == nil {
		return nil, errors.New("consensus: Net, Parse, OnAccept required")
	}
	if cfg.Params == (snowball.Parameters{}) {
		cfg.Params = snowball.DefaultParameters
		cfg.Params.ConcurrentRepolls = concurrentRepolls
	}
	if err := cfg.Params.Verify(); err != nil {
		return nil, err
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 100 * time.Millisecond
	}
	if cfg.IdlePollInterval <= 0 {
		cfg.IdlePollInterval = time.Second
	}
	if cfg.PollTimeout < minPollTimeout {
		cfg.PollTimeout = max(RequestTimeout, minPollTimeout)
	}
	return &Engine{
		cfg:         cfg,
		phase:       phasePollAnchor,
		fatal:       make(chan error, 1),
		bsVotes:     make(map[ids.ID]int),
		bsSample:    make(map[ids.NodeID]int),
		bsHeight:    make(map[ids.ID]uint64),
		pollVdrs:    make(map[uint32]*pollState),
		recs:        make(map[ids.ID]*rec),
		pendingByPa: make(map[ids.ID][]ids.ID),
		gets:        make(map[ids.ID]*getReq),
		parked:      make(map[ids.ID][]parkedVote),
		frontiers:   make(map[ids.NodeID]ids.ID),
	}, nil
}

// Fatal reports the first unrecoverable error; the caller must halt on it.
func (e *Engine) Fatal() <-chan error { return e.fatal }

func (e *Engine) fail(err error) {
	log.Printf("consensus: fatal: %v", err)
	select {
	case e.fatal <- err:
	default:
	}
}

// LastAccepted returns the engine's current accepted frontier.
func (e *Engine) LastAccepted() (uint64, ids.ID) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.lastAcceptedHeight, e.lastAcceptedEthHash
}

func (e *Engine) Stats() Stats {
	e.mu.Lock()
	defer e.mu.Unlock()
	s := Stats{
		Live:            e.phase == phaseLive,
		AcceptedHeight:  e.lastAcceptedHeight,
		PeerAcceptedMax: e.peerAccMax,
		PollsOK:         e.pollsOK,
		PollsFailed:     e.pollsFailed,
		OutstandingGets: len(e.gets),
	}
	if e.cons != nil {
		s.Processing = e.cons.NumProcessing()
	}
	for _, votes := range e.parked {
		s.ParkedVotes += len(votes)
	}
	return s
}

// --- inbound (wired to the fetch handler) ---

func (e *Engine) OnContainer(nodeID ids.NodeID, raw []byte) {
	c, err := e.cfg.Parse(raw)
	if err != nil {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	delete(e.gets, c.ID)
	switch e.phase {
	case phaseAnchorGet:
		if c.ID == e.anchorID {
			e.goLive(c)
		}
	case phaseLive:
		e.addContainer(c)
	}
}

func (e *Engine) OnChits(nodeID ids.NodeID, requestID uint32, preferred, preferredAtHeight, accepted ids.ID, acceptedHeight uint64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if acceptedHeight > e.peerAccMax {
		e.peerAccMax = acceptedHeight
	}
	if e.phase == phasePollAnchor {
		e.bootstrapChit(nodeID, requestID, accepted, acceptedHeight)
		return
	}
	if e.phase != phaseLive {
		return
	}
	e.frontiers[nodeID] = accepted
	ps, ok := e.pollVdrs[requestID]
	if !ok || !ps.remaining.Contains(nodeID) {
		return
	}
	ps.remaining.Remove(nodeID)
	if ps.remaining.Len() == 0 {
		delete(e.pollVdrs, requestID)
	}

	// Fetch unknown blocks named by the chits so the vote can be applied.
	e.fetchIfUnknown(preferred)
	if preferredAtHeight != preferred {
		e.fetchIfUnknown(preferredAtHeight)
	}
	e.applyVote(parkedVote{
		requestID: requestID,
		nodeID:    nodeID,
		options:   []ids.ID{preferred, preferredAtHeight},
		deadline:  time.Now().Add(RequestTimeout),
	}, true)
}

// applyVote bubbles the vote to the nearest block consensus knows about
// (response options in decreasing-height order, first applicable wins,
// mirroring snow/engine/snowman voter.go). A vote blocked on an unknown
// container is parked until the container arrives or the deadline passes.
// A vote with no applicable option falls back to the peer's last known
// accepted frontier (the engine's QueryFailed behavior); only then Drop.
// Dropped and fallback votes can only fail a poll, never accept for it.
func (e *Engine) applyVote(v parkedVote, allowPark bool) {
	for i, opt := range v.options {
		vote, blocker, ok := e.bubble(opt)
		if ok {
			e.recordPolls(e.polls.Vote(v.requestID, v.nodeID, vote))
			return
		}
		if allowPark && i == 0 && blocker != ids.Empty {
			// Park on the highest option's blocker; retried on arrival.
			e.fetchIfUnknown(blocker)
			e.parked[blocker] = append(e.parked[blocker], v)
			return
		}
	}
	if fid, ok := e.frontiers[v.nodeID]; ok {
		if vote, _, ok := e.bubble(fid); ok {
			e.recordPolls(e.polls.Vote(v.requestID, v.nodeID, vote))
			return
		}
	}
	e.recordPolls(e.polls.Drop(v.requestID, v.nodeID))
}

// Tick drives timeouts, polls, and bootstrap retries. Call every ~50-100ms.
func (e *Engine) Tick() {
	e.mu.Lock()
	defer e.mu.Unlock()
	now := time.Now()
	switch e.phase {
	case phasePollAnchor:
		if now.Sub(e.lastBSAt) >= 2*time.Second {
			e.sendBootstrapPoll()
		}
	case phaseAnchorGet:
		if now.After(e.agDue) {
			e.sendAnchorGet()
		}
	case phaseLive:
		e.expirePolls(now)
		e.expireParked(now)
		e.retryGets(now)
		// Keep up to ConcurrentRepolls polls in flight while blocks are
		// processing (like the engine's repoll); idle, poll for discovery.
		if e.cons.NumProcessing() > 0 {
			for e.polls.Len() < e.cfg.Params.ConcurrentRepolls && now.Sub(e.lastPollAt) >= e.cfg.PollInterval {
				if !e.sendPoll() {
					break
				}
			}
		} else if now.Sub(e.lastPollAt) >= e.cfg.IdlePollInterval {
			e.sendPoll()
		}
		if !e.lastAcceptAt.IsZero() && now.Sub(e.lastAcceptAt) > 30*time.Second {
			log.Printf("consensus: no block accepted for %s (processing=%d polls_ok=%d polls_failed=%d)",
				now.Sub(e.lastAcceptAt).Round(time.Second), e.cons.NumProcessing(), e.pollsOK, e.pollsFailed)
			e.lastAcceptAt = now // rate-limit the warning
		}
	}
}

// --- bootstrap: find the network's accepted anchor via polls ---

func (e *Engine) sendBootstrapPoll() {
	vdrs, err := e.cfg.Net.SampleValidators(e.cfg.Params.K)
	if err != nil || len(vdrs) == 0 {
		log.Printf("consensus: bootstrap: cannot sample validators: %v", err)
		return
	}
	e.bsReqID = e.cfg.Net.NextRequestID()
	clear(e.bsVotes)
	clear(e.bsHeight)
	clear(e.bsSample)
	e.bsVoters.Clear()
	targets := set.NewSet[ids.NodeID](len(vdrs))
	for _, v := range vdrs {
		e.bsSample[v]++
		if e.cfg.Net.IsConnected(v) {
			targets.Add(v)
		}
	}
	if targets.Len() == 0 {
		log.Printf("consensus: bootstrap: no sampled validator connected")
		return
	}
	e.lastBSAt = time.Now()
	if err := e.cfg.Net.SendPullQuery(targets, e.bsReqID, ids.Empty, 0); err != nil {
		log.Printf("consensus: bootstrap: pull query send failed: %v", err)
	}
}

// bootstrapChit counts a chit toward the anchor with the WEIGHT OF ITS DRAWS
// in the sample, not one vote per node. K is a number of weighted draws with
// replacement, not a number of distinct nodes: on a small, stake-skewed
// validator set a K=20 sample collapses onto a handful of nodes (measured on
// FIFA mainnet: 25 validators, 5 of them holding 78.5% of the weight, mean
// 8.7 distinct nodes per sample), so a one-vote-per-node tally can never
// reach AlphaPreference=15 and the engine never leaves phasePollAnchor. This
// is exactly what poll.Vote does for every live poll (bag multiplicity), and
// on the primary network the two rules coincide because ~600 near-uniform
// validators make 20 draws ~20 distinct nodes.
func (e *Engine) bootstrapChit(nodeID ids.NodeID, requestID uint32, accepted ids.ID, acceptedHeight uint64) {
	draws := e.bsSample[nodeID]
	if requestID != e.bsReqID || draws == 0 || e.bsVoters.Contains(nodeID) {
		return
	}
	e.bsVoters.Add(nodeID)
	e.bsVotes[accepted] += draws
	e.bsHeight[accepted] = acceptedHeight
	if e.bsVotes[accepted] < e.cfg.Params.AlphaPreference {
		return
	}
	// Alpha of a K-sample agrees on the accepted frontier: that is our anchor.
	e.anchorID = accepted
	e.phase = phaseAnchorGet
	log.Printf("consensus: anchor found container=%s height=%d votes=%d", accepted, acceptedHeight, e.bsVotes[accepted])
	e.sendAnchorGet()
}

func (e *Engine) sendAnchorGet() {
	peer, ok := e.cfg.Net.SelectPeer()
	if !ok {
		log.Printf("consensus: anchor fetch: no peer available")
		return
	}
	e.agDue = time.Now().Add(RequestTimeout)
	if err := e.cfg.Net.SendGet(peer, e.cfg.Net.NextRequestID(), e.anchorID); err != nil {
		log.Printf("consensus: anchor fetch: send failed: %v", err)
	}
}

// goLive initializes real snowman consensus over containers above the anchor.
func (e *Engine) goLive(anchor *Container) {
	e.lastAcceptedID = anchor.ID
	e.lastAcceptedHeight = anchor.Height
	e.lastAcceptedEthHash = anchor.EthHash
	e.lastAcceptAt = time.Now()

	consCtx := &snow.ConsensusContext{
		Context:       &snow.Context{Log: logging.NoLog{}},
		Registerer:    prometheus.NewRegistry(),
		BlockAcceptor: noOpAcceptor{},
	}
	// SnowballFactory is what production avalanchego uses (snowflake+ballot).
	cons := &snowman.Topological{Factory: snowball.SnowballFactory}
	if err := cons.Initialize(consCtx, e.cfg.Params, anchor.ID, anchor.Height, time.Unix(int64(anchor.Time), 0)); err != nil {
		e.fail(err)
		return
	}
	factory, err := poll.NewEarlyTermFactory(e.cfg.Params.AlphaPreference, e.cfg.Params.AlphaConfidence, prometheus.NewRegistry(), cons)
	if err != nil {
		e.fail(err)
		return
	}
	polls, err := poll.NewSet(factory, logging.NoLog{}, prometheus.NewRegistry())
	if err != nil {
		e.fail(err)
		return
	}
	e.cons = cons
	e.polls = polls
	e.phase = phaseLive
	log.Printf("consensus: live anchor_height=%d anchor=%s", anchor.Height, anchor.ID)
	if e.cfg.OnAnchor != nil {
		if err := e.cfg.OnAnchor(anchor); err != nil {
			e.fail(err)
			return
		}
	}
	e.sendPoll()
}

// --- live: containers ---

func (e *Engine) addContainer(c *Container) {
	if _, ok := e.recs[c.ID]; ok {
		return
	}
	if c.ID == e.lastAcceptedID || c.Height <= e.lastAcceptedHeight {
		return
	}
	r := &rec{c: c}
	e.recs[c.ID] = r
	e.tryIssue(r)
	// Votes parked on this container can bubble now (through it, even if it
	// could not be issued yet).
	if votes, ok := e.parked[c.ID]; ok {
		delete(e.parked, c.ID)
		for _, v := range votes {
			e.applyVote(v, false)
		}
	}
}

// tryIssue feeds a block to consensus once its parent is known and issued,
// then drains children that were waiting on it.
func (e *Engine) tryIssue(r *rec) {
	if r.issued {
		return
	}
	parentID := r.c.ParentID
	var parentEthHash ids.ID
	var parentHeight uint64
	switch {
	case parentID == e.lastAcceptedID:
		parentEthHash = e.lastAcceptedEthHash
		parentHeight = e.lastAcceptedHeight
	default:
		pr, ok := e.recs[parentID]
		if !ok || !pr.issued {
			if !ok {
				e.fetchIfUnknown(parentID)
			}
			e.pendingByPa[parentID] = append(e.pendingByPa[parentID], r.c.ID)
			return
		}
		parentEthHash = pr.c.EthHash
		parentHeight = pr.c.Height
	}

	// Sanity: the inner eth chain must mirror the container chain.
	if r.c.EthParentHash != parentEthHash || r.c.Height != parentHeight+1 {
		log.Printf("consensus: container inner chain does not match container parent; dropping container=%s height=%d",
			r.c.ID, r.c.Height)
		delete(e.recs, r.c.ID)
		return
	}

	if err := e.cons.Add(&blockAdapter{e: e, r: r}); err != nil {
		e.fail(fmt.Errorf("consensus: add %s: %w", r.c.ID, err))
		return
	}
	r.issued = true

	children := e.pendingByPa[r.c.ID]
	delete(e.pendingByPa, r.c.ID)
	for _, childID := range children {
		if cr, ok := e.recs[childID]; ok {
			e.tryIssue(cr)
		}
	}
}

func (e *Engine) fetchIfUnknown(id ids.ID) {
	if id == ids.Empty || id == e.lastAcceptedID {
		return
	}
	if _, ok := e.recs[id]; ok {
		return
	}
	if g, ok := e.gets[id]; ok && time.Now().Before(g.deadline) {
		return
	}
	if len(e.gets) >= maxOutstandingGets {
		return // parked votes expire fail-safe; never over-subscribe peers
	}
	peer, ok := e.cfg.Net.SelectPeer()
	if !ok {
		return
	}
	g := e.gets[id]
	if g == nil {
		g = &getReq{}
		e.gets[id] = g
	}
	g.deadline = time.Now().Add(getRetry)
	g.tries++
	if err := e.cfg.Net.SendGet(peer, e.cfg.Net.NextRequestID(), id); err != nil {
		log.Printf("consensus: get send failed: %v", err)
	}
}

func (e *Engine) retryGets(now time.Time) {
	for id, g := range e.gets {
		if now.Before(g.deadline) {
			continue
		}
		if g.tries >= maxGetTries {
			log.Printf("consensus: giving up on container fetch container=%s tries=%d", id, g.tries)
			delete(e.gets, id)
			continue
		}
		e.fetchIfUnknown(id)
	}
}

// --- live: polls ---

func (e *Engine) sendPoll() bool {
	vdrs, err := e.cfg.Net.SampleValidators(e.cfg.Params.K)
	if err != nil || len(vdrs) == 0 {
		log.Printf("consensus: poll: cannot sample validators: %v", err)
		return false
	}
	var vdrBag bag.Bag[ids.NodeID]
	vdrBag.Add(vdrs...)
	reqID := e.cfg.Net.NextRequestID()
	if !e.polls.Add(reqID, vdrBag) {
		return false
	}
	e.lastPollAt = time.Now()

	targets := set.NewSet[ids.NodeID](len(vdrs))
	var disconnected []ids.NodeID
	for _, v := range vdrBag.List() {
		if e.cfg.Net.IsConnected(v) {
			targets.Add(v)
		} else {
			disconnected = append(disconnected, v)
		}
	}
	ps := &pollState{remaining: targets, deadline: time.Now().Add(e.cfg.PollTimeout)}
	if targets.Len() > 0 {
		e.pollVdrs[reqID] = ps
		if err := e.cfg.Net.SendPullQuery(targets, reqID, e.cons.Preference(), e.lastAcceptedHeight+1); err != nil {
			log.Printf("consensus: poll: send failed: %v", err)
		}
	}
	// Unconnected samples can never answer: their last known frontier (if
	// any) votes for them, else drop immediately.
	for _, v := range disconnected {
		e.applyVote(parkedVote{requestID: reqID, nodeID: v}, false)
	}
	return true
}

func (e *Engine) expireParked(now time.Time) {
	for id, votes := range e.parked {
		kept := votes[:0]
		for _, v := range votes {
			if now.After(v.deadline) {
				v.options = nil
				e.applyVote(v, false) // frontier fallback, else drop
			} else {
				kept = append(kept, v)
			}
		}
		if len(kept) == 0 {
			delete(e.parked, id)
		} else {
			e.parked[id] = kept
		}
	}
}

func (e *Engine) expirePolls(now time.Time) {
	for reqID, ps := range e.pollVdrs {
		if now.Before(ps.deadline) {
			continue
		}
		delete(e.pollVdrs, reqID)
		for _, v := range ps.remaining.List() {
			// QueryFailed equivalent: the peer's last known accepted
			// frontier votes in its stead, else drop.
			e.applyVote(parkedVote{requestID: reqID, nodeID: v}, false)
		}
	}
}

// bubble walks id up through known containers to the nearest one consensus
// is processing (or the accepted frontier). ok=false when the walk dead-ends
// at an unknown (or known-but-unissued) block, returned as blocker.
func (e *Engine) bubble(id ids.ID) (vote ids.ID, blocker ids.ID, ok bool) {
	for {
		if id == e.lastAcceptedID || e.cons.Processing(id) {
			return id, ids.Empty, true
		}
		r, exists := e.recs[id]
		if !exists {
			return ids.Empty, id, false
		}
		if r.c.Height <= e.lastAcceptedHeight {
			// A stale fork below the accepted frontier: nothing to vote for.
			return ids.Empty, ids.Empty, false
		}
		id = r.c.ParentID
	}
}

func (e *Engine) recordPolls(results []bag.Bag[ids.ID]) {
	for _, votes := range results {
		// Poll health accounting: a poll whose top vote misses
		// alphaPreference resets snowball confidence; chronic failures are
		// the one way this follower falls behind, so track and report them.
		_, topCount := votes.Mode()
		if topCount >= e.cfg.Params.AlphaPreference {
			e.pollsOK++
		} else {
			e.pollsFailed++
		}
		if err := e.cons.RecordPoll(context.Background(), votes); err != nil {
			e.fail(fmt.Errorf("consensus: record poll: %w", err))
			return
		}
	}
}

// --- decision callbacks (invoked by Topological.RecordPoll, mu held) ---

func (e *Engine) onAccept(r *rec) error {
	h := r.c.Height
	e.lastAcceptedID = r.c.ID
	e.lastAcceptedHeight = h
	e.lastAcceptedEthHash = r.c.EthHash
	e.lastAcceptAt = time.Now()
	delete(e.recs, r.c.ID)
	// Anything at or below the accepted height is decided; drop leftovers.
	for id, o := range e.recs {
		if !o.issued && o.c.Height <= h {
			delete(e.recs, id)
		}
	}
	return e.cfg.OnAccept(r.c)
}

func (e *Engine) onReject(r *rec) {
	log.Printf("consensus: rejected height=%d container=%s", r.c.Height, r.c.ID)
	delete(e.recs, r.c.ID)
	delete(e.pendingByPa, r.c.ID)
}

type noOpAcceptor struct{}

func (noOpAcceptor) Accept(*snow.ConsensusContext, ids.ID, []byte) error { return nil }

// blockAdapter exposes a container to snowman.Topological.
type blockAdapter struct {
	e *Engine
	r *rec
}

func (b *blockAdapter) ID() ids.ID     { return b.r.c.ID }
func (b *blockAdapter) Parent() ids.ID { return b.r.c.ParentID }
func (b *blockAdapter) Height() uint64 { return b.r.c.Height }
func (b *blockAdapter) Timestamp() time.Time {
	return time.Unix(int64(b.r.c.Time), 0)
}
func (b *blockAdapter) Bytes() []byte { return b.r.c.Bytes }
func (b *blockAdapter) Verify(context.Context) error {
	return nil // parent-linkage checked in tryIssue; epochdb does not execute at tip
}
func (b *blockAdapter) Accept(context.Context) error { return b.e.onAccept(b.r) }
func (b *blockAdapter) Reject(context.Context) error { b.e.onReject(b.r); return nil }

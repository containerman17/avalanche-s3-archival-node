// Package epochdb is the node as a Go LIBRARY, the first of the entry points
// (DESIGN "Entry points and adapters") and the fast one.
//
// Open starts the SAME full node `epochdb serve` runs: follower, executor,
// flush, publish, JSON-RPC. `serve` is a thin main over this package, so there
// is ONE lifecycle and no second wiring to drift from it. On top of that the
// handle gives raw, zero-serialization access: Head, StateAt, CallAt, TraceTx,
// and the store's own readers (blocks, txs, receipts, logs, frames, postings)
// through Store(). The algorithm is Go and links this; nothing serialises.
//
// Core() is THE CORE QUERY LAYER every wire format sits on as a PEER: gRPC and
// plain HTTP call the same methods this handle does, and none of them routes
// through another (user ruling 2026-08-12: JSON-RPC is wasteful, so nothing
// stacks on it).
package epochdb

import (
	"context"
	"fmt"
	"log"
	"math/big"
	"os"
	"time"

	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/libevm/common"
	ethstate "github.com/ava-labs/libevm/core/state"

	"github.com/containerman17/avalanche-s3-archival-node/chain"
	"github.com/containerman17/avalanche-s3-archival-node/dist"
	"github.com/containerman17/avalanche-s3-archival-node/exec"
	"github.com/containerman17/avalanche-s3-archival-node/fetch"
	"github.com/containerman17/avalanche-s3-archival-node/rpc"
	"github.com/containerman17/avalanche-s3-archival-node/store"
)

// commitEvery is how many blocks go into one Firewood proposal. It is also
// how far below the head a crash can leave Firewood, and therefore how far
// back the fetch restarts (fetchStart).
const commitEvery = 64

// tipAnchorTimeout bounds the wait for the --tip-override container itself. A
// seed nobody can serve is a typo, not a slow peer: the fleet once ran 28
// chains with a MAINNET container ID as their override and every one sat
// waiting forever, looking merely slow. Generous because peer warmup precedes
// it.
const tipAnchorTimeout = 5 * time.Minute

// Config is everything a node needs. `serve` fills exactly one.
type Config struct {
	DataDir       string
	Chain         *chain.Chain
	NodeURI       string
	VdrSources    []string
	StateCacheGiB int
	PerPeer       int
	// TipOverride replaces the consensus follower with a bounded forward
	// fetch that stops at this CONTAINER's height (serve --tip-override): a
	// fixed, reproducible corpus. Empty follows the live tip.
	TipOverride ids.ID
	// ReadOnly opens the corpus and the query layer and NOTHING ELSE: no
	// follower, no executor, no writers, so it neither takes the data dir's
	// lock nor touches the network. It is what the benches and the adapter
	// tests open a finished corpus with; a real node leaves it false.
	ReadOnly bool
	// OnExit reports a component goroutine's exit. `serve` takes the process
	// down nonzero on a real failure (DESIGN's failure model); nil ignores
	// them, which is what a ReadOnly open wants.
	OnExit func(what string, err error)
}

// Node is one chain's running stack: follower, executor, store, and the core
// query layer. One process runs exactly one (DESIGN, ruling 2026-08-04).
type Node struct {
	cfg      Config
	fetcher  *fetch.Fetcher
	e        *exec.Executor
	store    *store.DB
	cas      *dist.Store
	srv      *rpc.Server
	accepted func() uint64
	chainID  *big.Int
	// execDone closes when the executor goroutine has RETURNED. Closing the
	// executor while Run is still in a block would corrupt exactly the writers
	// the flush exists to protect, so Close waits on this. Cancel the node's
	// context first or the wait never ends.
	execDone chan struct{}
}

// Open wires one chain's goroutines onto ctx and returns with it running.
// Every component goroutine's exit goes to cfg.OnExit. Nothing here calls
// log.Fatal: a start failure is an error the caller reports and exits on, so
// the same code is usable from a test and the flush on the way out is never
// skipped.
func Open(ctx context.Context, cfg Config) (n *Node, err error) {
	// THE JOIN PATH runs below, between opening the artifact store and opening
	// the executor: store.Join resolves this chain's pointer, walks the run
	// footers backward to the chain root and writes the manifest, and a dir
	// that came up with runs but no executed state builds its frontier out of
	// them. Both are no-ops on a warm dir and cost it zero bucket requests.

	// Unwind whatever is already open if a later step fails, so a start that
	// gives up releases the Firewood handle and the mmaps it took.
	var closers []func()
	defer func() {
		if err == nil {
			return
		}
		for i := len(closers) - 1; i >= 0; i-- {
			closers[i]()
		}
	}()
	if cfg.OnExit == nil {
		cfg.OnExit = func(string, error) {}
	}
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return nil, err
	}

	// --- the fetch: it dials here, and starts once the runs say where from ---
	// Chain is what makes the follower track the L1's subnet and register the
	// subnet-evm extras (M5); nil-equivalent for the C-chain, whose descriptor
	// carries the primary subnet.
	var (
		fetcher  *fetch.Fetcher
		blocks   exec.BlockSource
		accepted = func() uint64 { return 0 }
	)
	if !cfg.ReadOnly {
		fetcher, err = fetch.New(fetch.Config{
			NodeURI:    cfg.NodeURI,
			PerPeer:    cfg.PerPeer,
			Chain:      cfg.Chain,
			VdrSources: cfg.VdrSources,
		})
		if err != nil {
			return nil, fmt.Errorf("fetch: %w", err)
		}
		closers = append(closers, func() { fetcher.Close() })
		// The network's accepted head, which under --tip-override is the
		// override ceiling. Nothing reads it in that mode (see Status).
		accepted = fetcher.AcceptedHead
	}

	// --- storage v0: the executor owns the writer, the RPC shares it ---------
	g, err := exec.ChainGenesis(cfg.Chain)
	if err != nil {
		return nil, fmt.Errorf("genesis: %w", err)
	}
	cas, err := dist.Open(cfg.DataDir)
	if err != nil {
		return nil, fmt.Errorf("open artifact store: %w", err)
	}
	closers = append(closers, func() { cas.Close() })
	// JOIN BEFORE OPEN: the manifest is what store.Open reads, so a fresh dir
	// joining a published chain has to have it on disk first. A missing pointer
	// over a populated prefix is a HARD ERROR here, never a silent re-sync.
	if !cfg.ReadOnly {
		if err := store.Join(cas, cfg.DataDir, cfg.Chain.Root()); err != nil {
			return nil, fmt.Errorf("join chain: %w", err)
		}
	}
	// A ReadOnly handle TAKES NO LOCK and is opened beside a live writer on
	// purpose, so it must not cut that writer's window log: store.OpenReadOnly
	// is the same open minus that one write.
	openStore := store.Open
	if cfg.ReadOnly {
		openStore = store.OpenReadOnly
	}
	db, err := openStore(cfg.DataDir, cas, cfg.Chain.Root())
	if err != nil {
		return nil, fmt.Errorf("open storage v0: %w", err)
	}
	closers = append(closers, func() { db.Close() })

	// THE FETCH STARTS HERE, before the executor opens, because the executor's
	// crash walk-back reads containers inside exec.New and the RAM queue is the
	// only place they can come from now.
	if !cfg.ReadOnly {
		if cfg.TipOverride != ids.Empty {
			// --tip-override names where forward fetch stops trusting the
			// network: resolve it BEFORE the window opens, or the fetch would
			// climb past the ceiling in the seconds it takes to ask.
			actx, cancel := context.WithTimeout(ctx, tipAnchorTimeout)
			a, aerr := fetcher.ResolveAnchor(actx, cfg.TipOverride)
			cancel()
			if aerr != nil {
				return nil, fmt.Errorf("--tip-override %s: no peer served this container in %s (is it an ID from a DIFFERENT NETWORK?): %w",
					cfg.TipOverride, tipAnchorTimeout, aerr)
			}
			log.Printf("epochdb: tip-override %s is height %d: fetching forward to it and stopping there, no follower", a.ID, a.Height)
			fetcher.SetCeiling(a.Height)
		}
		from, anchor, ferr := exec.FetchStart(db, g.Hash, commitEvery)
		if ferr != nil {
			return nil, ferr
		}
		blocks = fetcher.StartForward(ctx, from, anchor)
	}

	n = &Node{
		cfg: cfg, fetcher: fetcher, store: db, cas: cas,
		accepted: accepted, chainID: g.Config.ChainID,
		execDone: make(chan struct{}),
	}
	n.srv = rpc.NewServer(db, g.TrieAlloc, rpc.StoreChainContext(db), g.Config)

	if cfg.ReadOnly {
		close(n.execDone)
		return n, nil
	}

	misc, err := store.OpenMisc(cfg.DataDir)
	if err != nil {
		return nil, fmt.Errorf("open misc store: %w", err)
	}
	closers = append(closers, func() { misc.Close() })

	// THE FRONTIER BUILD, on its own Executor that is closed before the real
	// one opens: that is what unpins Firewood's in-memory history. A torn build
	// heals itself first.
	if err := buildFrontier(cfg, blocks, db, misc); err != nil {
		return nil, err
	}

	// exec.New stamps the data dir with the VM kind and refuses a dir built by
	// the other one (state.Store.BindVMKind): that check is what keeps a
	// subnet-evm node from opening a coreth corpus.
	e, err := exec.New(exec.Config{
		DataDir:         cfg.DataDir,
		Blocks:          blocks,
		Store:           db,
		Misc:            misc,
		StateCacheBytes: uint64(cfg.StateCacheGiB) << 30,
		// UNPINNED 2026-07-29: this was 1 only so the served frontier was a
		// committed Firewood revision. Nothing reads Firewood any more, so
		// batching costs nothing in freshness: head, block-hash index and the
		// tail overlay all advance together at the batch boundary, because
		// publishLive/OnBlock and the state-layer appends all happen in
		// flushBatch. 64 is invisible at tip pace (Run flushes the open batch
		// the moment the fetch runs dry, which at the tip is after every block,
		// so a following node still commits and root-verifies per block) and
		// is what makes catch-up fast. It also sets how far below the head a
		// crash can leave Firewood, which is how far back the fetch restarts
		// (fetchStart): at 64 that is 8,192 blocks, seconds of re-fetching.
		CommitEvery: commitEvery,
		Chain:       cfg.Chain,
	})
	if err != nil {
		return nil, fmt.Errorf("exec.New: %w", err)
	}
	closers = append(closers, func() { e.Close() })
	n.e = e

	live := liveNode{live: e.LiveHead, settled: e.SettledHeight}
	if cfg.TipOverride != ids.Empty {
		live.target = fetcher.SyncTarget
	} else {
		live.accepted = accepted
	}
	n.srv.EnableLive(live)

	// THE FOLLOWER OWNS THE REAL TIP ZONE, and under --tip-override there is no
	// tip zone to own: the forward fetch stops at the override and the run is a
	// fixed corpus.
	if cfg.TipOverride == ids.Empty {
		go func() { cfg.OnExit("follower", fetcher.Follow(ctx)) }()
	}
	go func() {
		err := e.Run(ctx)
		close(n.execDone) // BEFORE OnExit: it may flush, and flushing waits
		cfg.OnExit("executor", err)
	}()
	return n, nil
}

// Close is the clean shutdown path: every writer flushed and released,
// executor first (it is the only writer of the state layer, and the holder of
// the open batch, the state watermark and the Firewood handle).
//
// CANCEL THE NODE'S CONTEXT FIRST or the wait on the executor never ends:
// closing it while Run is still inside a block would corrupt exactly the
// writers the flush exists to protect. Safe only once the listeners are done
// with this node, because it unmaps files those goroutines read.
func (n *Node) Close() {
	<-n.execDone
	if n.e != nil {
		if err := n.e.Close(); err != nil {
			log.Printf("epochdb: executor close: %v", err)
		}
	}
	if n.fetcher != nil {
		if err := n.fetcher.Close(); err != nil {
			log.Printf("epochdb: fetch close: %v", err)
		}
	}
	n.store.Close()
	n.cas.Close()
}

// --- the raw access, no serialization anywhere on these paths ----------------

// Core is THE CORE QUERY LAYER: the reads every adapter makes, in Go. It is
// also the JSON-RPC http.Handler, which is one wire format over it and not a
// layer under anything.
func (n *Node) Core() *rpc.Server { return n.srv }

// Store is the corpus itself: headers, txs, receipts, frames, state rows and
// the posting lists, as stored (rows in, rows out, no decode this side).
func (n *Node) Store() *store.DB { return n.store }

// CAS is the artifact store, for the caller that publishes or syncs.
func (n *Node) CAS() *dist.Store { return n.cas }

// ChainID is the EVM chain ID.
func (n *Node) ChainID() *big.Int { return n.chainID }

// Head is the serving frontier: what `latest` means right now.
func (n *Node) Head() (rpc.Head, error) { return n.srv.Head() }

// StateAt opens the state at height h. The returned StateDB is single-use and
// NOT goroutine-safe: one per call, per goroutine.
func (n *Node) StateAt(h uint64) (*ethstate.StateDB, error) { return n.srv.StateAt(h) }

// CallAt runs an eth_call-shaped message at height h and returns its return
// data. THE FAST PATH: no JSON, no HTTP, no copy of the arguments.
func (n *Node) CallAt(msg *rpc.CallMsg, h uint64) ([]byte, error) { return n.srv.Call(msg, h) }

// TraceTx returns a transaction's STORED call frames (DESIGN: traces are
// stored, so this is a read and never a re-execution). found=false is a clean
// "unknown transaction".
func (n *Node) TraceTx(hash common.Hash) (frames []store.Frame, found bool, err error) {
	return n.srv.Frames(hash)
}

// --- status -------------------------------------------------------------------

// Status is the chain's numbers, read once. `serve` renders it two ways (the
// log line and /status).
//
// Head IS THE FIX OF 2026-08-05: it is the height execution is measured
// against, and where it comes from depends on the mode. Following, it is the
// network's accepted head. Under --tip-override there IS no follower and no
// accepted head to measure against, so a run measures against the ceiling it
// was given: reporting anything else once put a 59.3M-block exec_lag against a
// height nothing in the run would ever reach.
type Status struct {
	Backfill bool
	Head     uint64 // accepted head (follow) or override ceiling (backfill)
	Executed uint64
	Served   uint64
	Flushed  uint64
	Settled  uint64
	Runs     int
	Bytes    uint64
	Fetched  uint64 // highest height in the fetch queue
	// QueueBytes is the RAM the fetched-not-executed containers occupy right
	// now, PeakQueueBytes the high-water mark of this run. Nothing
	// pre-execution touches disk, so this is the whole cost of being ahead.
	QueueBytes     uint64
	PeakQueueBytes uint64
	// PrunedSeeds and BadLinks are the two ways the fetch can be BUSY AND
	// GETTING NOWHERE: peers answering a height poll with their own last
	// accepted block because they pruned the height, and spans thrown away
	// because a container did not link to the one below it. Both were visible
	// only in `dev exec` until 2026-08-14, so a node that could not seed a
	// height looked exactly like a node with nothing to do.
	PrunedSeeds uint64
	BadLinks    uint64
}

func (n *Node) Status() Status {
	head, _ := n.store.Head()
	s := Status{
		Backfill: n.cfg.TipOverride != ids.Empty,
		Served:   head,
		Flushed:  n.store.FlushedFloor(),
		Runs:     len(n.store.Manifest().Runs),
		Bytes:    n.store.SectionSizes()[0] + n.store.SectionSizes()[1] + n.store.SectionSizes()[2],
	}
	if n.e != nil {
		s.Executed, s.Settled = n.e.LiveHead(), n.e.SettledHeight()
	}
	if n.fetcher != nil {
		p := n.fetcher.Progress()
		s.Fetched, s.QueueBytes, s.PeakQueueBytes = p.Head, p.QueueBytes, p.PeakBytes
		s.PrunedSeeds, s.BadLinks = p.PrunedSeeds, p.BadLinks
	}
	if s.Backfill {
		s.Head = n.fetcher.SyncTarget()
	} else {
		s.Head = n.accepted()
	}
	return s
}

// liveNode is the rpc.Live surface (SAE labels pending/latest/safe) plus the
// height eth_syncing advertises.
//
// TWO QUESTIONS, TWO VALUES (2026-08-05), because making one number answer both
// is what put a leftover height in front of clients. AcceptedHead bounds what a
// read may NAME (`pending`, the block-number ceiling); SyncTarget is only the
// goal, and may sit millions of blocks above anything answerable. Following
// they coincide, so both funcs below are the network's accepted head. Under
// --tip-override there is NO follower, so nothing above the executed head may
// be named, while the goal is the fetch ceiling.
type liveNode struct {
	live     func() uint64 // executed head
	settled  func() uint64
	accepted func() uint64 // the follower's accepted head; nil when backfilling
	target   func() uint64 // the backfill ceiling; nil when following
}

func (l liveNode) LiveHead() uint64      { return l.live() }
func (l liveNode) SettledHeight() uint64 { return l.settled() }

func (l liveNode) AcceptedHead() uint64 {
	if l.accepted == nil {
		return l.live()
	}
	return max(l.accepted(), l.live())
}

func (l liveNode) SyncTarget() uint64 {
	if l.target == nil {
		return l.AcceptedHead()
	}
	return l.target()
}

// buildFrontier gives a dir that joined a published chain the state to serve
// it. It is a no-op on every other dir: a dir that executed its own history
// already has a Firewood, and a dir with no runs has nothing to merge.
//
// The build runs on its OWN Executor, opened with FrontierBuild and closed
// before the serving one opens, which is what unpins Firewood's in-memory
// history (a merge holds a revision per batch otherwise).
func buildFrontier(cfg Config, blocks exec.BlockSource, db *store.DB, misc *store.MiscStore) error {
	head, ok := db.Head()
	if !ok || head == 0 {
		return nil
	}
	if _, err := exec.HealTornFrontier(cfg.DataDir, misc); err != nil {
		return err
	}
	need, err := exec.FrontierNeeded(cfg.DataDir)
	if err != nil || !need {
		return err
	}
	log.Printf("epochdb: this dir holds runs through block %d and no state: building the frontier out of them", head)
	e, err := exec.New(exec.Config{
		DataDir:       cfg.DataDir,
		Blocks:        blocks,
		Store:         db,
		Misc:          misc,
		Chain:         cfg.Chain,
		FrontierBuild: true,
	})
	if err != nil {
		return fmt.Errorf("frontier build: exec.New: %w", err)
	}
	defer e.Close()
	return e.BuildFrontier()
}

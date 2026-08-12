package exec

import (
	"context"
	"fmt"
	"io/fs"
	"log"
	"path/filepath"
	goatomic "sync/atomic"
	"time"

	"github.com/ava-labs/avalanchego/graft/evm/firewood"
	"github.com/ava-labs/avalanchego/snow"
	avaconstants "github.com/ava-labs/avalanchego/utils/constants"
	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/rawdb"
	ethstate "github.com/ava-labs/libevm/core/state"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/libevm/stateconf"
	"github.com/ava-labs/libevm/params"
	"github.com/ava-labs/libevm/rlp"
	"github.com/ava-labs/libevm/triedb"

	"github.com/containerman17/epochdb/chain"
	"github.com/containerman17/epochdb/fetch"
	"github.com/containerman17/epochdb/store"
)

// flushEvery is the group-fsync cadence in blocks: writelog/headers/code/
// misc are fsynced and executorHead advanced every flushEvery executed
// blocks (and on shutdown). Cheap; recovery walk-back covers the tail.
const flushEvery = 256

// walkBackBudget bounds how far reconcile scans for Firewood's durable
// root. Firewood's DeferredCommitInterval default is 64 and our exechead
// can lag appends by flushEvery; the budget comfortably covers both.
const walkBackBudget = 4096

// walkBackBudgetFor is how far back reconcile may need CONTAINERS to still
// exist: the base budget plus 64 deferred commits of CommitEvery blocks each.
// Both reconcile and New's refusal read it, so they cannot drift apart.
func walkBackBudgetFor(commitEvery int) uint64 {
	budget := uint64(walkBackBudget)
	if commitEvery > 1 {
		budget += 64 * uint64(commitEvery)
	}
	return budget
}

// BlockSource yields raw containers by height. ok=false means the height
// is not available yet (the fetch walk has not landed it).
type BlockSource interface {
	GetByHeight(n uint64) ([]byte, bool, error)
}

// Config is the opening configuration for an Executor.
type Config struct {
	// DataDir is the shared data directory. Firewood lives in
	// DataDir/firewood/, the state layer files at the root.
	DataDir string
	// Blocks is the container source (a fetch.Reader over the staging dir).
	Blocks BlockSource
	// Store is storage v0. Required.
	Store *store.DB
	// Misc is the small durable key store rawdb needs beside the runs
	// (the vm-kind stamp and the few non-code rawdb keys). Required.
	Misc *store.MiscStore
	// StateCacheBytes sizes the Go-side EVM read cache (0 disables).
	StateCacheBytes uint64
	// VerifyCache re-reads every cache hit through Firewood and panics
	// on mismatch. Validation harness only.
	VerifyCache bool
	// CommitEvery batches this many blocks into one Firewood proposal
	// (root verification moves to batch boundaries, with automatic
	// per-block bisect on a boundary mismatch). <= 1 means one proposal
	// per block, the classic path.
	CommitEvery int
	// Chain is the chain descriptor (genesis bytes, ids, VM kind).
	// nil defaults to the Fuji C-chain.
	Chain *chain.Chain
	// FrontierBuild says this Executor exists only to run BuildFrontier and
	// then close, which is what unpins Firewood's in-memory history (see
	// New).
	FrontierBuild bool
	// StopAt makes Run return cleanly after executing this height
	// (fixed-corpus builds; staging above it is disposable). 0 = never.
	StopAt uint64
	// OnBlock, if set, is called on the executor goroutine right after each
	// block's frontier is published: serve uses it to advance the
	// serving head and the block-hash index in lockstep with execution, so
	// `latest` never names a height the frontier is not at. Must not block.
	OnBlock func(num uint64, hash common.Hash)
}

// Executor replays Fuji C-Chain blocks against Firewood-backed frontier
// state, verifies every computed state root against the header root, and
// captures post-image write frames, headers, and code into the state layer.
type Executor struct {
	cfg Config
	// vm is the execution backend seam (vm.go): coreth or subnet-evm.
	vm        vmBackend
	chainCfg  *params.ChainConfig
	wrapDB    *wrappedDatabase
	triedb    *triedb.Database
	fwBackend *firewood.TrieDB
	snowCtx   *snow.Context
	chainCtx  chainContext

	genesisRoot common.Hash
	genesisHash common.Hash
	headRoot    common.Hash
	headNum     uint64
	headTime    uint64 // timestamp of headNum: the transitionvm boundary test
	totalGas    uint64 // session gas, for mgas/s
	// totalTxs is the session transaction count, for tx/s. blk/s alone is
	// a poor progress metric on this chain: block density swings by an
	// order of magnitude between eras (measured 19.99 txs/block around
	// height 14.3M against ~1 in the light era just above it), so the same
	// work reads as 4x the blocks. tx/s tracks the work; mgas/s is still
	// the truest of the three.
	totalTxs uint64

	// SAE (ACP-194) side, live only above the Helicon boundary. ring
	// records our own post-execution roots and gas clocks, which
	// post-Helicon headers no longer carry per block; sae holds the
	// engine state and is built on the first post-boundary block.
	ring *saeRing
	sae  *saeExec

	// live is the last executed height, published after each committed block.
	// It is a HEIGHT and nothing else: since the 2026-07-29 ruling nothing
	// reads state from Firewood, so there is no frontier handle to publish.
	live goatomic.Uint64

	// Firewood bookkeeping. fwHeight mirrors Firewood's internal
	// proposal-height chain (parent.height+1 per committed proposal);
	// with batching it deliberately diverges from block heights.
	// lastFwHash is the block hash Firewood's tree currently has
	// registered, i.e. the parent hash the next proposal must reference.
	fwHeight   uint64
	lastFwHash common.Hash

	// Open-batch state (CommitEvery > 1).
	batchOpen      bool
	batchStartNum  uint64
	batchStartRoot common.Hash
	batchStartTime uint64      // headTime when the batch opened, so bisect can rewind it
	batchLastHash  common.Hash // hash of the last executed batch block
	batchDirty     bool        // any non-empty block drained this batch
	batchCount     int
	batchBuf       []batchItem

	// Per-block capture seam: the tx hook needs the block it is inside.
	capture    *capture
	curBW      *store.BlockWrite
	curHeader  *types.Header
	curStatedb *ethstate.StateDB
	signer     types.Signer

	// spl accumulates exec-goroutine wall time per status interval:
	// where the serial thread's time goes (throughput experiment).
	spl struct{ read, evm, hash, fw, fsync time.Duration }

	// Async group flush (throughput experiment): maybeFlush enqueues the
	// watermark, a flusher goroutine started by Run does ring.sync +
	// FlushAndSetExecHead. Coalescing: if the flusher is busy the enqueue
	// is skipped and the next flushEvery multiple carries the watermark
	// forward. Correctness is unchanged because everything at or below an
	// enqueued watermark was appended before the enqueue, so the flusher's
	// fsync covers it; exechead just lags a little more, well inside the
	// walk-back budget.
	flushReq    chan uint64
	flushErr    chan error
	flushDone   chan struct{}
	flushBusyNs goatomic.Int64 // flusher-goroutine fsync time, for the status line
}

// batchItem is one block's buffered state-layer output. Appends are held
// back until the batch boundary root verifies so a bad batch never leaves
// unverified frames on disk.
type batchItem struct {
	num  uint64
	hash common.Hash
	bw   *store.BlockWrite
}

// chainContext is the minimal coreth ChainContext for
// corethcore.NewEVMBlockContext / ApplyTransaction. GetHeader is served
// from the headers log so the BLOCKHASH opcode returns real hashes across
// its 256-block window (the reference's zero-hash stub bug documented in
// its LOG.md is exactly what this avoids). With batching, headers of the
// OPEN batch are buffered rather than appended, so those must be served
// from the buffer: a BLOCKHASH reaching into the open batch otherwise
// returned the zero hash and diverged execution (second live batch
// mismatch at Fuji ~7220368, per-block re-execution clean).
type chainContext struct {
	e     *Executor // nil outside the executor (historical eth_call)
	store *store.DB // the chain section
}

// Engine lives in vm_coreth.go / sevmChainContext: the consensus engine type
// is one of the few things the two VMs do not share.

// GetHeader returns nil ONLY for a genuine miss, because nil is libevm's "no
// such header" and BLOCKHASH turns it into the zero hash inside a running
// contract. A read or decode FAILURE is not a miss and must never be aliased
// to one: the signature has no error to return, so it panics. A wrong
// BLOCKHASH answer is silent, permanent, and gets written into logsbf as if it
// were real; a panic is a restartable stop.
//
// The store read goes through Store.HeaderRLP, which falls back to the sealed
// epochs. The raw-only read this used to do answered "absent" for every header
// below the sealed end on a node whose raw buckets seal had retired.
func (c chainContext) GetHeader(_ common.Hash, num uint64) *types.Header {
	var (
		raw []byte
		ok  bool
	)
	if c.e != nil {
		raw, ok = c.e.batchHeaderRLP(num)
	}
	if !ok {
		if c.store == nil {
			return nil
		}
		var err error
		raw, ok, err = c.store.HeaderRLP(num)
		if err != nil {
			panic(fmt.Sprintf("exec: read header %d for BLOCKHASH: %v", num, err))
		}
		if !ok {
			return nil
		}
	}
	var h types.Header
	if err := rlp.DecodeBytes(raw, &h); err != nil {
		panic(fmt.Sprintf("exec: decode header %d for BLOCKHASH: %v", num, err))
	}
	return &h
}

// batchHeaderRLP serves a header buffered in the open batch. Batch blocks
// are contiguous from batchStartNum+1, so the buffer index is direct.
func (e *Executor) batchHeaderRLP(num uint64) ([]byte, bool) {
	if !e.batchOpen || num <= e.batchStartNum {
		return nil, false
	}
	idx := num - e.batchStartNum - 1
	if idx >= uint64(len(e.batchBuf)) {
		return nil, false
	}
	return e.batchBuf[idx].bw.HeaderRLP, true
}

// dirBytes is the size of dir on disk, 0 if it does not exist yet. Best
// effort: it sizes a cache, so an unreadable entry is skipped rather than
// failed on.
func dirBytes(dir string) uint64 {
	var total uint64
	filepath.WalkDir(dir, func(_ string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil //nolint:nilerr // missing or unreadable: count nothing
		}
		if info, err := d.Info(); err == nil {
			total += uint64(info.Size())
		}
		return nil
	})
	return total
}

// New opens Firewood under cfg.DataDir/firewood, materialises the Fuji
// C-Chain genesis if needed, reconciles any crash gap against the state
// layer, and returns an Executor ready to Run.
func New(cfg Config) (*Executor, error) {
	if cfg.Blocks == nil {
		return nil, fmt.Errorf("config: Blocks required")
	}
	if cfg.DataDir == "" {
		return nil, fmt.Errorf("config: DataDir required")
	}
	if cfg.Store == nil {
		return nil, fmt.Errorf("config: Store required")
	}
	// The crash walk-back re-reads containers from staging, and both raw
	// deleters (seal, and the pruning node's fold) retire whole 100k-block
	// buckets behind the sealed/folded end. If the budget can reach past one
	// bucket, a crash right after a retirement lands on "container missing
	// from staging" and the node cannot start. Unconditional: seal has the
	// same latent exposure, it was only ever hidden by a numeric coincidence.
	if budget := walkBackBudgetFor(cfg.CommitEvery); budget >= fetch.SegmentBlocks {
		return nil, fmt.Errorf("config: commit-every %d puts the crash walk-back %d blocks back, past one raw bucket (%d): max is %d",
			cfg.CommitEvery, budget, fetch.SegmentBlocks, (fetch.SegmentBlocks-1-walkBackBudget)/64)
	}
	c := cfg.Chain
	if c == nil {
		var err error
		if c, err = chain.CChain(avaconstants.FujiID); err != nil {
			return nil, err
		}
		cfg.Chain = c
	}
	fetch.RegisterExtras(c.VMKind)
	backend := vmFor(c.VMKind)
	// A data directory is single-kind for life: the two header encodings are
	// mutually exclusive, so a dir built by one is unreadable by the other and
	// there is no migration (delete and resync).
	if err := cfg.Misc.BindVMKind(string(c.VMKind)); err != nil {
		return nil, err
	}
	snowCtx, err := snowContextFor(c)
	if err != nil {
		return nil, err
	}

	g, err := backend.genesis(c, snowCtx)
	if err != nil {
		return nil, err
	}

	// firewood.DefaultConfig nests its own "firewood" subdirectory under
	// the path we give it, so DataDir -> DataDir/firewood/.
	fwCfg := firewood.DefaultConfig(cfg.DataDir)
	// Firewood's DeferredCommitInterval counts COMMITS. With batching,
	// one commit covers CommitEvery blocks, so the default 64 would let
	// the persisted root lag 64*CommitEvery blocks behind the walk-back
	// window (observed live at N=1000: root 64k blocks back, reconcile
	// failed). Scale it so the persisted root lags at most ~64 blocks'
	// worth of commits.
	if cfg.CommitEvery > 1 {
		fwCfg.DeferredCommitInterval = max(1, 64/uint64(cfg.CommitEvery))
	}
	// A FRONTIER BUILD KEEPS NO HISTORY AT ALL, and that is the difference
	// between a merge that fits in a container and one the kernel kills.
	// Firewood retains RevisionsInMemory committed revisions and defers
	// persisting DeferredCommitInterval of them, so the serving profile (128
	// and 64) holds up to 128 batches' worth of trie deltas in Rust memory:
	// measured 470MB per 200k-op batch here, i.e. tens of GB long before the
	// merge ends, which is what OOM-killed the first real join (63.9GB anon on
	// a 64GB box). Nothing reads a historical revision during a build and the
	// build is all-or-nothing anyway (a torn one is wiped, see
	// HealTornFrontier), so 2 revisions is all it needs; graft clamps the
	// deferred count to revisions-1, i.e. persist every batch. Measured over 8
	// 100k-op batches: 1483MB retained at 128 revisions, 688MB at 2, and the
	// per-batch cost keeps falling instead of staying linear.
	// AND THE SAME APPLIES TO SERVING, which nobody carried across. A retained
	// revision holds its trie deltas in RUST memory, outside GOMEMLIMIT and
	// outside the Go heap entirely, so none of the Go-side tuning reaches it.
	// Measured on mainnet C 2026-08-08: the Go arena stayed FLAT at 15-16GB
	// while non-arena anon grew ~2.6 GB/h to 12.3GB in under three hours, and
	// that growth is what squeezes the page cache until Firewood reads start
	// missing and throughput halves. 128 revisions is the only unbounded term
	// left on this path.
	//
	// NOTHING READS A HISTORICAL REVISION HERE. Every Firewood read in this
	// process opens the CURRENT head root (the executor's parentRoot, the
	// batch's start root, SAE's parent root); rpc.Server is constructed with
	// no Firewood handle at all and Overlay.TrieDB() returns nil precisely so
	// that a served read reaching a triedb fails loudly; historical state
	// comes from the sealed epochs plus the overlay. Nor is there a rollback:
	// reconcile re-executes FORWARD from the durable root by walking headers
	// on disk, and HealTornFrontier WIPES rather than rewinds. The backward
	// budget is in headers, never revisions.
	//
	// GATED ON CommitEvery, because graft upholds
	// DeferredCommitInterval < RevisionsInMemory by clamping the former. At
	// CommitEvery > 1 the block above has already set the deferred interval to
	// 1, so 2 revisions changes nothing about persistence. At CommitEvery == 1
	// the interval is still Firewood's default 64, and 2 revisions would
	// silently clamp it to 1, i.e. an fsync per block. serve runs 64 and the
	// standalone exec defaults to 1000, so both take this path and lose
	// nothing.
	if cfg.FrontierBuild || cfg.CommitEvery > 1 {
		fwCfg.RevisionsInMemory = 2
	}
	// The default 1MB node cache collapses once state outgrows it: at
	// height ~3.7M a 60s CPU profile showed ~50% of samples inside the
	// Rust library (per-SLOAD trie walks re-reading upper nodes), disk
	// well under saturation. A real node cache keeps the hot trie
	// interior resident.
	//
	// SIZED AGAINST THE CONTAINER, not the directory, and not a constant: see
	// firewoodCacheBytes for why the dir formula under-provisioned a
	// mid-sync chain and why an eighth of `memory.max` is the number.
	limit, _ := CgroupMemoryLimit() // 0 when there is no container ceiling
	fwBytes := dirBytes(filepath.Join(cfg.DataDir, firewood.Directory))
	fwSize, fwWhy, err := firewoodCacheBytes(fwBytes, limit)
	if err != nil {
		return nil, err
	}
	fwCfg.CacheSizeBytes = uint(fwSize)
	log.Printf("exec: firewood node cache %d MB (%s)", fwSize>>20, fwWhy)
	// THE THIRD ANON GRANT, logged beside the other two so one line says where
	// every number came from. It is a Go map, so it is spent out of the heap
	// GOMEMLIMIT already reserved rather than being a fourth claim on the box.
	if n, why := cfg.Store.FlatCacheBudget(); n > 0 {
		log.Printf("exec: flat latest-state cache %d MB (%s)", n>>20, why)
	} else {
		log.Printf("exec: flat latest-state cache OFF (%s), every state read takes the descent", why)
	}

	ethdbKV := store.EthDB(cfg.Store, cfg.Misc, g.TrieAlloc)
	memdb := rawdb.NewDatabase(ethdbKV)

	tdb := triedb.NewDatabase(memdb, &triedb.Config{
		DBOverride: fwCfg.BackendConstructor,
	})

	// Every node materialises the genesis alloc: it is the bottom of the
	// state both for a replaying node and for one whose frontier is merged
	// out of the epochs (BuildFrontier applies the winning rows on top of it).
	genesisRoot, err := commitGenesisIfNeeded(tdb, g, ethdbKV)
	if err != nil {
		tdb.Close()
		return nil, err
	}

	inner := backend.newStateDatabase(memdb, tdb)
	// THE SECOND ANON CLAMP, and it is the same rule as the Firewood one above:
	// a per-chain grant written as a constant is multiplied by the chain count
	// on a fleet box, so the state cache takes an eighth of the container's own
	// ceiling when there is one (see clampStateCache).
	stateCache := clampStateCache(cfg.StateCacheBytes, limit)
	if stateCache != cfg.StateCacheBytes {
		log.Printf("exec: state cache %d MB (an eighth of this container's %d MB ceiling, asked for %d MB)",
			stateCache>>20, limit>>20, cfg.StateCacheBytes>>20)
	}
	wrapDB := wrapDatabase(inner, stateCache, cfg.VerifyCache)

	fwBackend, ok := tdb.Backend().(*firewood.TrieDB)
	if !ok {
		tdb.Close()
		return nil, fmt.Errorf("triedb backend is %T, want *firewood.TrieDB", tdb.Backend())
	}

	ring, err := openSAERing(cfg.DataDir)
	if err != nil {
		tdb.Close()
		return nil, err
	}

	e := &Executor{
		cfg:         cfg,
		vm:          backend,
		chainCfg:    g.Config,
		ring:        ring,
		wrapDB:      wrapDB,
		triedb:      tdb,
		fwBackend:   fwBackend,
		snowCtx:     snowCtx,
		chainCtx:    chainContext{},
		genesisRoot: genesisRoot,
		genesisHash: g.Hash,
		headRoot:    genesisRoot,
		headNum:     0,
		headTime:    g.Timestamp,
		lastFwHash:  g.Hash,
	}

	e.chainCtx = chainContext{e: e, store: cfg.Store}

	// A FRONTIER BUILD SKIPS THE WALK-BACK, and it has to: a joined dir holds
	// runs up to H with an empty Firewood, so reconcile would walk H down
	// looking for a root nothing here ever produced, and then demand containers
	// for blocks this node deliberately never fetched. BuildFrontier is about
	// to write that Firewood and park the head itself.
	if !cfg.FrontierBuild {
		if err := e.reconcile(); err != nil {
			tdb.Close()
			return nil, err
		}
	}

	e.publishLive()

	// Event-log capture starts wherever this build first executes; blocks
	// below the marker are the backfill job's range. Write-once.
	return e, nil
}

// reconcile aligns the in-memory head with what is actually durable.
//
//   - top = the highest header on disk (>= exechead: appends are visible
//     after kill -9 even before the group fsync that advances exechead).
//   - firewood.Root() is the only durable fact Firewood hands back; its
//     deferred commits mean it can sit anywhere within the walk-back
//     budget below top.
//
// Walk headers down from top until one's Root matches firewood.Root(),
// SetHashAndHeight there, then re-execute the gap through the normal
// executeBlock path. All state-layer appends are idempotent (block-frame
// skip, code dedup, misc same-value skip) so replays are free.
func (e *Executor) reconcile() error {
	// ONE HEAD, not two. Storage v0 has no separate durable watermark: the
	// window log is the state layer's own tail and DB.Head is the highest
	// block in it, run or window alike.
	top, topOK := e.cfg.Store.Head()
	if !topOK {
		e.fwBackend.SetHashAndHeight(e.genesisHash, 0)
		log.Printf("exec: fresh state, genesis root=%x", e.genesisRoot)
		return nil
	}

	rootFW := common.Hash(e.fwBackend.Firewood.Root())

	if top == 0 {
		if rootFW != e.genesisRoot {
			return fmt.Errorf("head=0 but firewood root %x != genesis %x", rootFW, e.genesisRoot)
		}
		// Resume at genesis: a fresh-opened Firewood only knows its disk
		// root, not any block hash (tree.blockHashes = {zero hash}), so
		// block 1's Update could not resolve its parent hash. Register
		// the genesis block hash ("must be called at startup" per
		// SetHashAndHeight's contract). Without this, replay dies
		// deterministically at the first non-empty block after a restart
		// at height 0.
		e.fwBackend.SetHashAndHeight(e.genesisHash, 0)
		return nil
	}

	// Walk-back: highest block i <= top whose POST-EXECUTION root is the
	// firewood root. Below the Helicon boundary that is header.Root; above
	// it, header.Root belongs to the settled block, so the executor's own
	// ring is the only place its roots live (see saering.go).
	var (
		fwN    uint64
		fwHash common.Hash
		found  bool
	)
	// Budget covers the worst persisted-root lag even for data written
	// before the DeferredCommitInterval scaling: 64 deferred commits of
	// CommitEvery blocks each, plus the open batch and fsync-group slack.
	budget := walkBackBudgetFor(e.cfg.CommitEvery)
	lo := uint64(0)
	if top > budget {
		lo = top - budget
	}
	for i := top; ; i-- {
		h, err := e.loadHeader(i)
		if err != nil {
			return fmt.Errorf("reconcile: %w", err)
		}
		if root, ok := e.ownRootAt(h, i); ok && root == rootFW {
			fwN = i
			fwHash = h.Hash()
			found = true
			break
		}
		if i == lo {
			break
		}
	}
	if !found {
		if rootFW == e.genesisRoot {
			// Firewood opened against pristine disk; its revisions are
			// gone. Re-execute everything from genesis, whose block hash
			// Firewood needs registered to resolve block 1's parent.
			fwN = 0
			fwHash = e.genesisHash
		} else {
			return fmt.Errorf("reconcile: firewood root %x not found in headers [%d..%d]", rootFW, lo, top)
		}
	}

	// Tell Firewood what height its on-disk root corresponds to before
	// proposing new blocks on top of it.
	e.fwBackend.SetHashAndHeight(fwHash, fwN)
	e.fwHeight = fwN
	e.lastFwHash = fwHash

	e.headNum = fwN
	e.headRoot = e.genesisRoot
	if fwN > 0 {
		h, err := e.loadHeader(fwN)
		if err != nil {
			return fmt.Errorf("reconcile: %w", err)
		}
		// The walk-back matched here, so this is the same root; taking it
		// from firewood keeps the SAE case (header.Root is the settled
		// block's) from sneaking in.
		e.headRoot = rootFW
		e.headTime = h.Time
	}

	if fwN == top {
		log.Printf("exec: resume head=%d root=%x", top, e.headRoot)
		return nil
	}

	log.Printf("exec: walk-back reconcile: firewood at %d, state layer at %d, re-executing %d blocks",
		fwN, top, top-fwN)
	for i := fwN + 1; i <= top; i++ {
		raw, ok, err := e.cfg.Blocks.GetByHeight(i)
		if err != nil {
			return fmt.Errorf("reconcile: read container %d: %w", i, err)
		}
		if !ok {
			return fmt.Errorf("reconcile: container %d missing from staging", i)
		}
		if err := e.executeRaw(i, raw); err != nil {
			return fmt.Errorf("reconcile: reexecute block %d: %w", i, err)
		}
	}
	// Close any batch the gap re-execution left open, then make the
	// re-executed tail durable (exechead must never claim buffered,
	// unappended blocks).
	if e.batchOpen && e.batchCount > 0 {
		if err := e.flushBatch(); err != nil {
			return fmt.Errorf("reconcile: flush tail batch: %w", err)
		}
	}
	if err := e.cfg.Store.Sync(); err != nil {
		return err
	}
	log.Printf("exec: resume head=%d root=%x", e.headNum, e.headRoot)
	return nil
}

// loadHeader reads a header for the reconcile walk-back and the SAE ring.
// Store.HeaderRLP descends into the sealed epochs, which is what makes this
// work at all on a node whose raw header buckets were retired by seal (it used
// to fail loudly with "missing from state layer" there, never silently).
func (e *Executor) loadHeader(blockNum uint64) (*types.Header, error) {
	raw, ok, err := e.cfg.Store.HeaderRLP(blockNum)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("header %d missing from state layer", blockNum)
	}
	var h types.Header
	if err := rlp.DecodeBytes(raw, &h); err != nil {
		return nil, fmt.Errorf("decode header %d: %w", blockNum, err)
	}
	return &h, nil
}

// Close flushes any open batch and the state layer watermark, then
// releases Firewood.
func (e *Executor) Close() error {
	var flushErr error
	if e.batchOpen && e.batchCount > 0 {
		flushErr = e.flushBatch()
	}
	// The open batch's shared account trie is a FIREWOOD KEEP-ALIVE HANDLE,
	// and triedb.Close waits 5s for every one of them to be finalized. On the
	// error path (Run returned mid-batch, or the flush above failed) nothing
	// else drops it, so the handle is still reachable from this Executor and
	// the wait can only time out: every death of the Tokyo crash-loop also
	// logged "cannot close database with active keep-alive handles".
	e.wrapDB.endBatch()
	if flushErr != nil {
		e.triedb.Close()
		return fmt.Errorf("close: flush batch: %w", flushErr)
	}
	if err := e.ring.close(); err != nil {
		e.triedb.Close()
		return err
	}
	if err := e.cfg.Store.Sync(); err != nil {
		e.triedb.Close()
		return err
	}
	return e.triedb.Close()
}

// Head returns the executor's current head height.
func (e *Executor) Head() uint64 { return e.headNum }

// Run executes blocks ascending from headNum+1, polling the block source
// for heights the fetch walk has not landed yet. Returns on ctx cancel or
// on the first error (a root mismatch is an error: hard stop).
func (e *Executor) Run(ctx context.Context) (err error) {
	start := time.Now()
	lastLog := start
	lastGas, lastBlocks, lastTxs := uint64(0), uint64(0), uint64(0)
	blocksDone := uint64(0)
	lastWait := time.Time{}

	next := e.headNum + 1

	// Async group flusher (throughput experiment): see maybeFlush.
	e.flushReq = make(chan uint64, 1)
	e.flushErr = make(chan error, 1)
	e.flushDone = make(chan struct{})
	go func() {
		defer close(e.flushDone)
		for range e.flushReq {
			t0 := time.Now()
			if err := e.ring.sync(); err != nil {
				e.flushErr <- err
				return
			}
			if err := e.cfg.Store.Sync(); err != nil {
				e.flushErr <- err
				return
			}
			e.flushBusyNs.Add(int64(time.Since(t0)))
		}
	}()
	defer func() {
		close(e.flushReq)
		<-e.flushDone
		e.flushReq = nil
		// Drain the flusher's death. Without this a failed group fsync or
		// exechead write was dropped whenever Run returned for another reason
		// first (ctx cancel, StopAt), and only Close would resurface it.
		select {
		case ferr := <-e.flushErr:
			if err == nil {
				err = fmt.Errorf("flusher: %w", ferr)
			}
		default:
		}
	}()

	// Prefetcher (throughput experiment): container reads and their
	// decompression overlap EVM execution. The fetch reader is
	// mutex-guarded, so bisect's direct GetByHeight calls stay safe.
	type pfItem struct {
		n   uint64
		raw []byte
		err error
	}
	pfCtx, pfCancel := context.WithCancel(ctx)
	defer pfCancel()
	pf := make(chan pfItem, 512)
	go func() {
		defer close(pf)
		for h := next; ; h++ {
			if e.cfg.StopAt > 0 && h > e.cfg.StopAt {
				return
			}
			raw, ok, err := e.cfg.Blocks.GetByHeight(h)
			if err != nil {
				select {
				case pf <- pfItem{n: h, err: err}:
				case <-pfCtx.Done():
				}
				return
			}
			if !ok {
				select {
				case <-pfCtx.Done():
					return
				case <-time.After(200 * time.Millisecond):
				}
				h--
				continue
			}
			select {
			case pf <- pfItem{n: h, raw: raw}:
			case <-pfCtx.Done():
				return
			}
		}
	}()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if e.cfg.StopAt > 0 && next > e.cfg.StopAt {
			if e.batchOpen && e.batchCount > 0 {
				if err := e.flushBatch(); err != nil {
					return err
				}
			}
			log.Printf("exec: reached --stop height %d", e.cfg.StopAt)
			return nil
		}
		tRead := time.Now()
		var raw []byte
		ok := false
		select {
		case it, alive := <-pf:
			if !alive {
				// Prefetcher finished (StopAt delivered): the loop-top
				// check ends the run once next passes StopAt.
			} else if it.err != nil {
				e.spl.read += time.Since(tRead)
				return it.err
			} else if it.n != next {
				return fmt.Errorf("prefetch out of order: got %d want %d", it.n, next)
			} else {
				raw, ok = it.raw, true
			}
		case <-time.After(200 * time.Millisecond):
			// Staging dry or prefetcher behind: same stall path as before.
		}
		e.spl.read += time.Since(tRead)
		if !ok {
			// Stall: close the open batch so tip-following and crash
			// windows stay bounded even when staging runs dry mid-batch.
			if e.batchOpen && e.batchCount > 0 {
				if err := e.flushBatch(); err != nil {
					return err
				}
			}
			if time.Since(lastWait) > 30*time.Second {
				log.Printf("exec: waiting for block %d to land in staging", next)
				lastWait = time.Now()
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(200 * time.Millisecond):
			}
			continue
		}
		if err := e.executeRaw(next, raw); err != nil {
			return err
		}
		blocksDone++
		next++

		if since := time.Since(lastLog); since >= 10*time.Second {
			dt := since.Seconds()
			hits, misses := e.wrapDB.CacheStats()
			sz := e.cfg.Store.SectionSizes()
			var hitPct float64
			if hits+misses > 0 {
				hitPct = 100 * float64(hits) / float64(hits+misses)
			}
			log.Printf("exec: height=%d blk/s=%.0f tx/s=%.0f mgas/s=%.2f chain=%.1fMB state=%.1fMB lookup=%.1fMB runs=%d cache_hit=%.1f%% cache=%.0fMB",
				e.headNum,
				float64(blocksDone-lastBlocks)/dt,
				float64(e.totalTxs-lastTxs)/dt,
				float64(e.totalGas-lastGas)/dt/1e6,
				float64(sz[store.SecChain])/1e6,
				float64(sz[store.SecState])/1e6,
				float64(sz[store.SecLookup])/1e6,
				len(e.cfg.Store.Manifest().Runs),
				hitPct,
				float64(e.wrapDB.cacheSize)/1e6,
			)
			log.Printf("exec: split read=%.2fs evm=%.2fs hash=%.2fs fw=%.2fs fsync=%.2fs flusher=%.2fs of %.1fs",
				e.spl.read.Seconds(), e.spl.evm.Seconds(), e.spl.hash.Seconds(),
				e.spl.fw.Seconds(), e.spl.fsync.Seconds(),
				time.Duration(e.flushBusyNs.Swap(0)).Seconds(), dt)
			e.spl = struct{ read, evm, hash, fw, fsync time.Duration }{}
			if e.sae != nil && e.sae.settledSeen {
				log.Printf("sae: settled=%d (lag %d..%d over %d settlements)",
					e.sae.settledHeight, e.sae.lagMin, e.sae.lagMax, e.sae.settlements)
			}
			lastLog = time.Now()
			lastGas, lastBlocks, lastTxs = e.totalGas, blocksDone, e.totalTxs
		}
	}
}

// ownRootAt returns the post-execution state root THIS executor computed
// for block n. Below the Helicon boundary that is the block's own
// header.Root; above it, header.Root is the root of the block this one
// SETTLES, so the value comes from the root ring. ok=false means an SAE
// height that has aged out of the ring.
func (e *Executor) ownRootAt(hdr *types.Header, n uint64) (common.Hash, bool) {
	if !e.vm.hasSettledMarkers(hdr) {
		return hdr.Root, true
	}
	root, _, ok := e.ring.get(n)
	return root, ok
}

// executeRaw parses and executes one container at the expected height,
// picking the engine by the transitionvm boundary rule (see saeExecuted).
func (e *Executor) executeRaw(blockNum uint64, raw []byte) error {
	// NO CONTAINER DUPLICATION: the proposervm wrapper is stored beside the
	// header and the container is reassembled on demand, so this split is the
	// only place the two halves are told apart.
	pvm, blk, err := store.SplitContainer(raw)
	if err != nil {
		return fmt.Errorf("block %d parse: %w", blockNum, err)
	}
	if got := blk.NumberU64(); got != blockNum {
		return fmt.Errorf("block %d has internal number %d", blockNum, got)
	}
	sae, err := e.saeExecuted(blk, e.headTime)
	if err != nil {
		return err
	}
	switch {
	case sae:
		// The coreth-side batch must close at the boundary: SAE commits
		// one Firewood proposal per block.
		if e.batchOpen && e.batchCount > 0 {
			if err := e.flushBatch(); err != nil {
				return err
			}
		}
		if err := e.executeSAEBlock(blk, pvm); err != nil {
			return fmt.Errorf("block %d: %w", blockNum, err)
		}
	case e.cfg.CommitEvery > 1:
		if err := e.executeBatched(blk, pvm); err != nil {
			return fmt.Errorf("block %d: %w", blockNum, err)
		}
	default:
		newRoot, err := e.executeBlock(blk, pvm)
		if err != nil {
			return fmt.Errorf("block %d: %w", blockNum, err)
		}
		e.headRoot = newRoot
		e.headNum = blockNum
	}
	e.headTime = blk.Time()
	e.totalGas += blk.GasUsed()
	e.totalTxs += uint64(len(blk.Transactions()))
	// Mid-batch (--commit-every > 1) the head root has no Firewood revision
	// yet, so the frontier only advances at batch boundaries. serve
	// pins CommitEvery=1, which makes that every block.
	if !e.batchOpen {
		e.publishLive()
		if e.cfg.OnBlock != nil {
			e.cfg.OnBlock(e.headNum, blk.Hash())
		}
	}
	return nil
}

// publishLive republishes the executed head for RPC goroutines.
func (e *Executor) publishLive() { e.live.Store(e.headNum) }

// LiveHead returns the last executed height (0 before the first publish).
func (e *Executor) LiveHead() uint64 { return e.live.Load() }

// SettledHeight returns the last SAE-settled height. Below the Helicon
// boundary settlement does not exist and the executed head IS the settled
// head, which is what the safe/finalized labels report.
func (e *Executor) SettledHeight() uint64 {
	if e.sae != nil && e.sae.settledSeen {
		return e.sae.settledHeight
	}
	return e.LiveHead()
}

// maybeFlush advances the durable watermark every flushEvery blocks. The
// SAE root ring is fsynced with the group: exechead must never claim a
// height whose recorded root did not reach disk, or the next restart could
// not identify the frontier.
func (e *Executor) maybeFlush(blockNum uint64) error {
	if blockNum%flushEvery != 0 {
		return nil
	}
	tSync := time.Now()
	defer func() { e.spl.fsync += time.Since(tSync) }()
	if e.flushReq != nil {
		select {
		case err := <-e.flushErr:
			return err
		default:
		}
		select {
		case e.flushReq <- blockNum:
		default: // flusher busy; coalesce into the next multiple
		}
		return nil
	}
	if err := e.ring.sync(); err != nil {
		return err
	}
	return e.cfg.Store.Sync()
}

// executeBlock runs the EVM + atomic txs for blk, verifies the computed
// state root against header.Root (hard stop on mismatch), hands the block's
// rows to the state layer, then commits Firewood.
//
// THAT ORDER IS THE STANDING RULE: rows first, Firewood last, so a kill -9 can
// only ever leave Firewood BEHIND the state layer, which is what reconcile's
// backward walk recovers from.
func (e *Executor) executeBlock(blk *types.Block, pvm []byte) (common.Hash, error) {
	header := blk.Header()
	parentRoot := e.headRoot
	blockNum := blk.NumberU64()

	headerRLP, err := rlp.EncodeToBytes(header)
	if err != nil {
		return common.Hash{}, fmt.Errorf("encode header: %w", err)
	}
	bw := &store.BlockWrite{Height: blockNum, HeaderRLP: headerRLP, Pvm: pvm, Code: map[string][]byte{}}

	// Empty-block fast path (HOT on Fuji): if the header claims no state
	// change AND the block carries no transactions, running the EVM would be a
	// no-op and Firewood's Commit rejects mid-chain identity proposals
	// ("committable proposal not found"). Persist the chain rows only and
	// advance Firewood's in-memory block-hash tracking so the next non-empty
	// block's Update resolves its parent hash.
	if header.Root == parentRoot && len(blk.Transactions()) == 0 {
		if err := e.cfg.Store.WriteBlock(bw); err != nil {
			return common.Hash{}, err
		}
		if err := e.maybeFlush(blockNum); err != nil {
			return common.Hash{}, err
		}
		e.fwBackend.SetHashAndHeight(blk.Hash(), blockNum)
		e.fwHeight = blockNum
		e.lastFwHash = blk.Hash()
		return parentRoot, nil
	}

	cap := &capture{code: map[string][]byte{}}
	e.beginCapture(cap, bw, header, parentRoot)
	defer e.endCapture()

	statedb, err := ethstate.New(parentRoot, e.wrapDB, nil)
	if err != nil {
		return common.Hash{}, fmt.Errorf("open statedb: %w", err)
	}
	e.curStatedb = statedb
	receipts, err := e.runEVM(blk, statedb)
	if err != nil {
		return common.Hash{}, err
	}

	// The block's own Commit must compute the REAL root, so the per-tx
	// short-circuit comes off first.
	e.wrapDB.endBlock()

	// Commit triggers the trie interceptor for whatever the last tx boundary
	// did not already drain (block-final writes: engine finalisation, coreth's
	// atomic transfers). The block number handed to Commit is Firewood's
	// proposal height (tree height + 1), which equals the real block height in
	// per-block mode.
	triedbOpt := stateconf.WithTrieDBUpdatePayload(header.ParentHash, blk.Hash())
	newRoot, err := statedb.Commit(
		e.fwHeight+1,
		e.chainCfg.IsEIP158(header.Number),
		stateconf.WithTrieDBUpdateOpts(triedbOpt),
	)
	if err != nil {
		return common.Hash{}, fmt.Errorf("statedb commit: %w", err)
	}

	if newRoot != header.Root {
		dumpMismatch(blk, cap.rows, receipts, statedb, newRoot, header.Root)
		return common.Hash{}, fmt.Errorf("state root mismatch: computed %x, expected %x", newRoot, header.Root)
	}

	bw.Tail = cap.take()
	bw.Code = cap.code
	if err := e.cfg.Store.WriteBlock(bw); err != nil {
		return common.Hash{}, err
	}
	e.wrapDB.forgetRecentCode()
	if err := e.maybeFlush(blockNum); err != nil {
		return common.Hash{}, err
	}

	// Firewood commit last. Its DeferredCommitInterval means this may or
	// may not persist; reconcile() walks back on the next startup.
	if err := e.triedb.Commit(newRoot, false); err != nil {
		return common.Hash{}, fmt.Errorf("triedb commit: %w", err)
	}
	e.fwHeight++
	e.lastFwHash = blk.Hash()
	return newRoot, nil
}

// beginCapture arms the per-tx drain for one block.
func (e *Executor) beginCapture(c *capture, bw *store.BlockWrite, header *types.Header, parentRoot common.Hash) {
	e.capture, e.curBW, e.curHeader = c, bw, header
	e.signer = types.MakeSigner(e.chainCfg, header.Number, header.Time)
	e.wrapDB.setCapture(c)
	e.wrapDB.beginBlock(parentRoot)
}

func (e *Executor) endCapture() {
	e.wrapDB.setCapture(nil)
	e.wrapDB.endBlock()
	e.capture, e.curBW, e.curHeader, e.curStatedb = nil, nil, nil, nil
}

// captureTx closes one transaction: it drains that transaction's post-images
// and turns the execution outputs into the row set the state layer stores.
//
// THE DRAIN IS A PER-TX IntermediateRoot. Finalise already runs per tx inside
// ApplyTransaction; IntermediateRoot adds the trie update pass, which is what
// pushes the post-images through the capture interceptor. geth itself calls it
// per transaction on pre-Byzantium chains, so it is a supported operation and
// not a trick, and the account trie's Hash is short-circuited for the duration
// so Firewood is never asked for a root nobody wants. The block's final Commit
// still computes the real root and the executor still hard-stops on a mismatch,
// which is what makes this safe to assert rather than hope.
func (e *Executor) captureTx(_ int, tx *types.Transaction, r *types.Receipt) error {
	e.curStatedb.IntermediateRoot(e.chainCfg.IsEIP158(e.curHeader.Number))

	// THE VERBATIM FORM IS THE ONE INSIDE THE CONTAINER'S TX LIST, which for a
	// typed transaction is the opaque bytes wrapped in an RLP string, not the
	// bare MarshalBinary bytes. Storing the container's own form is what lets
	// Reassemble be pure concatenation, and rlp.DecodeBytes reads both shapes
	// back into a Transaction.
	raw, err := rlp.EncodeToBytes(tx)
	if err != nil {
		return fmt.Errorf("encode tx %s: %w", tx.Hash(), err)
	}
	// FRAMES ARE TAKEN BEFORE ANYTHING ELSE, and a miss is DEATH. Frames can
	// never be backfilled by IO, so a block stored without them is a block that
	// has to be re-executed to ever have them, which is exactly the corpus this
	// design refuses to produce.
	frameRec, frameAddrs, ok := frames.take()
	if !ok {
		e.dieOnUncapturedTrace(tx.Hash().String())
	}
	tw := store.TxWrite{
		Hash:       tx.Hash().Bytes(),
		RLP:        raw,
		Receipt:    store.EncodeTxReceipt(r, r.CumulativeGasUsed),
		Frames:     frameRec,
		FrameAddrs: frameAddrs,
		State:      e.capture.take(),
	}
	// NO ECDSA RECOVERY HERE: ApplyTransaction already recovered the sender and
	// cached it on the transaction, so this is a field read.
	if from, err := types.Sender(e.signer, tx); err == nil {
		tw.Sender = from.Bytes()
	}
	if to := tx.To(); to != nil {
		tw.To = to.Bytes()
	} else if r.ContractAddress != (common.Address{}) {
		tw.Created = r.ContractAddress.Bytes()
	}
	seenAddr := make(map[common.Address]struct{}, len(r.Logs))
	seenTopic := make(map[common.Hash]struct{}, len(r.Logs))
	for _, l := range r.Logs {
		if _, ok := seenAddr[l.Address]; !ok {
			seenAddr[l.Address] = struct{}{}
			tw.LogAddrs = append(tw.LogAddrs, l.Address.Bytes())
		}
		for _, tp := range l.Topics {
			if _, ok := seenTopic[tp]; !ok {
				seenTopic[tp] = struct{}{}
				tw.Topics = append(tw.Topics, tp.Bytes())
			}
		}
	}
	e.curBW.Txs = append(e.curBW.Txs, tw)
	return nil
}

// dieOnUncapturedTrace is THE FAIL-STOP, and it is the design rather than an
// accident (DESIGN principles, "traces are stored, and capture failure is
// death"; user, verbatim: "We can just die... we are going to reach the Fuji
// C-chain block at a certain height, and we are going to die. And this is
// okay."). It is reached when a transaction executed without the frame tracer
// attached, which at this dependency pin means the SAE path: saexec.Execute
// hardcodes vm.Config{}, so a post-Helicon C-chain block has no tracer seam.
// The upstream passthrough PR is what unblocks it; until then this death is
// expected at Fuji C-chain's Helicon activation height and surprises nobody.
func (e *Executor) dieOnUncapturedTrace(what string) {
	log.Fatalf("epochdb: exec: block %d (%s): THE CALL TRACE WAS NOT CAPTURED, so this block cannot be stored.\n"+
		"  Cause at this dependency pin: saexec.Execute hardcodes vm.Config{}, so a post-Helicon C-chain block has no tracer seam.\n"+
		"  This death is the design, written in advance: see DESIGN.md, the principles section, \"TRACES ARE STORED, AND CAPTURE FAILURE IS DEATH\".\n"+
		"  Frames can never be backfilled by IO, so storing the block without them would produce exactly the corpus that rule forbids.\n"+
		"  The fix is the upstream vm.Config/tracer passthrough for saexec; L1s and pre-Helicon blocks capture fine today.",
		e.curHeader.Number, what)
}

// runEVM applies the block's upgrades and every transaction onto statedb,
// returning every receipt produced (they already exist in memory: capture is
// free). Shared by the per-block and batched paths; the VM-specific half
// (and, on coreth, the atomic ExtData transfers) lives behind the seam.
//
// e.headTime IS the parent's timestamp here: executeRaw advances it only
// after the block returns, and bisect rewinds it with batchStartTime.
func (e *Executor) runEVM(blk *types.Block, statedb *ethstate.StateDB) (types.Receipts, error) {
	return e.vm.runEVM(e.chainCtx, e.chainCfg, e.snowCtx, blk, e.headTime, statedb, e.captureTx)
}

// executeBatched accumulates blk into the open batch (opening one if
// needed) and, when the batch reaches CommitEvery blocks, flushes it as a
// single Firewood proposal. Mid-batch blocks drain their writes through
// the capture wrapper into the shared trie (Firewood untouched); reads
// see them via the read cache and the shared trie's dirtyKeys overlay.
// State-layer appends are buffered until the boundary root verifies.
func (e *Executor) executeBatched(blk *types.Block, pvm []byte) error {
	header := blk.Header()
	blockNum := blk.NumberU64()

	if !e.batchOpen {
		e.batchOpen = true
		e.batchStartNum = e.headNum
		e.batchStartRoot = e.headRoot
		e.batchStartTime = e.headTime
		e.batchDirty = false
		e.batchCount = 0
		e.wrapDB.beginBatch(e.headRoot)
	}

	headerRLP, err := rlp.EncodeToBytes(header)
	if err != nil {
		return fmt.Errorf("encode header: %w", err)
	}

	bw := &store.BlockWrite{Height: blockNum, HeaderRLP: headerRLP, Pvm: pvm, Code: map[string][]byte{}}
	if header.Root == e.headRoot && len(blk.Transactions()) == 0 {
		// Empty block: chain rows only, no EVM, no Firewood.
		e.batchBuf = append(e.batchBuf, batchItem{num: blockNum, hash: blk.Hash(), bw: bw})
	} else {
		tEVM := time.Now()
		cap := &capture{code: map[string][]byte{}}
		e.beginCapture(cap, bw, header, e.batchStartRoot)
		statedb, err := ethstate.New(e.batchStartRoot, e.wrapDB, nil)
		if err != nil {
			e.endCapture()
			return fmt.Errorf("open statedb: %w", err)
		}
		e.curStatedb = statedb
		receipts, err := e.runEVM(blk, statedb)
		if err != nil {
			e.endCapture()
			return err
		}
		_ = receipts
		// Drain commit: the batch-account trie reports Hash == origin, so
		// statedb skips TrieDB().Update entirely; writes flow through the
		// capture wrapper into the shared trie's pending ops.
		if _, err := statedb.Commit(blockNum, e.chainCfg.IsEIP158(header.Number)); err != nil {
			e.endCapture()
			return fmt.Errorf("statedb drain commit: %w", err)
		}
		bw.Tail = cap.take()
		bw.Code = cap.code
		e.endCapture()
		e.spl.evm += time.Since(tEVM)
		e.batchDirty = true
		e.batchBuf = append(e.batchBuf, batchItem{num: blockNum, hash: blk.Hash(), bw: bw})
	}

	// Intra-batch chaining trusts header roots; the boundary proposal
	// verifies the whole chain (roots chain, so any bad write corrupts
	// the boundary root).
	e.headRoot = header.Root
	e.headNum = blockNum
	e.batchLastHash = blk.Hash()
	e.batchCount++

	if e.batchCount >= e.cfg.CommitEvery {
		return e.flushBatch()
	}
	return nil
}

// flushBatch closes the open batch: the boundary root is verified against the
// boundary block's header root (bisecting per block on mismatch), then the
// buffered state-layer appends land, then ONE Firewood proposal for all the
// accumulated writes.
//
// THAT ORDER IS LOAD-BEARING, and it mirrors the per-block path: the state
// layer is written first and Firewood is committed LAST, so a kill -9 can only
// ever leave Firewood BEHIND the headers, which is exactly what reconcile's
// backward walk recovers from. The other order (commit, then append) leaves
// Firewood one batch AHEAD of the highest header after a crash, and the node
// then refuses to start with "firewood root not found in headers": caught by
// a kill -9 at 1,621,503 during the 2026-07-29 gate. The root is verified
// before either half, so no unverified frame reaches disk either way.
func (e *Executor) flushBatch() error {
	boundaryNum, boundaryRoot, boundaryHash := e.headNum, e.headRoot, e.batchLastHash

	computed := e.batchStartRoot
	if e.batchDirty {
		tHash := time.Now()
		computed = e.wrapDB.batchProposalRoot()
		e.spl.hash += time.Since(tHash)
	}
	if computed != boundaryRoot {
		return e.bisect(computed, boundaryNum, boundaryRoot)
	}

	for _, it := range e.batchBuf {
		if err := e.cfg.Store.WriteBlock(it.bw); err != nil {
			return err
		}
		if err := e.maybeFlush(it.num); err != nil {
			return err
		}
		// Publish PER BLOCK, right after that block's appends: this loop is
		// where a batch's writes become visible (state layer, and with them
		// the tail overlay), so the served head has to walk up with them.
		// Publishing only at the boundary left `latest` naming the previous
		// boundary while the overlay already answered from the flushed
		// blocks, which the A/B probe caught as a nonce one write into the
		// future; it also left 63 of every 64 block hashes out of the hash
		// index. The remaining exposure is one block wide, exactly as in the
		// per-block path. executeRaw republishes the boundary right after,
		// which is idempotent (same height, same hash).
		e.live.Store(it.num)
		if e.cfg.OnBlock != nil {
			e.cfg.OnBlock(it.num, it.hash)
		}
	}

	if e.batchDirty {
		tFw := time.Now()
		opt := stateconf.WithTrieDBUpdatePayload(e.lastFwHash, boundaryHash)
		if err := e.triedb.Update(computed, e.batchStartRoot, e.fwHeight+1, nil, nil, opt); err != nil {
			return fmt.Errorf("batch update [%d..%d]: %w", e.batchStartNum+1, boundaryNum, err)
		}
		if err := e.triedb.Commit(computed, false); err != nil {
			return fmt.Errorf("batch commit [%d..%d]: %w", e.batchStartNum+1, boundaryNum, err)
		}
		e.spl.fw += time.Since(tFw)
		e.fwHeight++
	} else {
		// Whole batch empty: no proposal, just register the boundary hash.
		e.fwBackend.SetHashAndHeight(boundaryHash, boundaryNum)
		e.fwHeight = boundaryNum
	}
	e.lastFwHash = boundaryHash

	e.batchOpen = false
	e.batchDirty = false
	e.batchBuf = nil
	e.wrapDB.forgetRecentCode()
	e.wrapDB.endBatch()
	return nil
}

// bisect re-executes a root-mismatched batch per block to name the exact
// offending block, then errors out either way (hard stop preserved).
func (e *Executor) bisect(computed common.Hash, boundaryNum uint64, boundaryRoot common.Hash) error {
	from, to := e.batchStartNum+1, boundaryNum
	log.Printf("exec: BATCH ROOT MISMATCH [%d..%d]: computed %x want %x; bisecting per block", from, to, computed, boundaryRoot)

	// Discard the batch and any possibly poisoned cache entries; Firewood
	// was never updated, so per-block re-execution starts clean from the
	// last committed boundary.
	e.batchOpen = false
	e.batchDirty = false
	e.batchBuf = nil
	e.wrapDB.endBatch()
	e.wrapDB.resetCache()
	e.headNum = e.batchStartNum
	e.headRoot = e.batchStartRoot
	// executeRaw advanced headTime for every buffered block, so it now names
	// the batch TAIL. Re-executing from the batch start with a parent
	// timestamp in the future would hide a precompile activation inside the
	// batch and diverge the re-execution from the batch it is diagnosing.
	e.headTime = e.batchStartTime

	for i := from; i <= to; i++ {
		raw, ok, err := e.cfg.Blocks.GetByHeight(i)
		if err != nil {
			return fmt.Errorf("bisect: read container %d: %w", i, err)
		}
		if !ok {
			// Not an error value: printed as `err=<nil>` it told an operator
			// nothing, and it fires while they are already staring at a root
			// mismatch.
			return fmt.Errorf("bisect: container %d is not in staging, so the batch [%d..%d] that just mismatched cannot be re-executed per block", i, from, to)
		}
		pvm, blk, err := store.SplitContainer(raw)
		if err != nil {
			return fmt.Errorf("bisect: parse block %d: %w", i, err)
		}
		newRoot, err := e.executeBlock(blk, pvm)
		if err != nil {
			return fmt.Errorf("bisect: offending block %d: %w", i, err)
		}
		e.headRoot = newRoot
		e.headNum = i
		e.headTime = blk.Time()
	}
	return fmt.Errorf("batch [%d..%d] root mismatch (computed %x want %x) but per-block re-execution verified clean: batching bug", from, to, computed, boundaryRoot)
}

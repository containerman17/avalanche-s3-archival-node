package exec

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/ava-labs/avalanchego/graft/coreth/consensus"
	"github.com/ava-labs/avalanchego/graft/coreth/consensus/dummy"
	corethcore "github.com/ava-labs/avalanchego/graft/coreth/core"
	"github.com/ava-labs/avalanchego/graft/coreth/core/extstate"
	cparams "github.com/ava-labs/avalanchego/graft/coreth/params"
	"github.com/ava-labs/avalanchego/graft/coreth/plugin/evm/atomic"
	ccustomtypes "github.com/ava-labs/avalanchego/graft/coreth/plugin/evm/customtypes"
	"github.com/ava-labs/avalanchego/graft/evm/firewood"
	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/snow"
	avaconstants "github.com/ava-labs/avalanchego/utils/constants"
	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/rawdb"
	ethstate "github.com/ava-labs/libevm/core/state"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/core/vm"
	"github.com/ava-labs/libevm/libevm/stateconf"
	"github.com/ava-labs/libevm/params"
	"github.com/ava-labs/libevm/rlp"
	"github.com/ava-labs/libevm/triedb"

	"github.com/containerman17/epochdb/fetch"
	"github.com/containerman17/epochdb/state"
)

// FujiAVAXAssetID is required by the atomic-tx state-transfer path to
// credit imported AVAX to the right account.
const FujiAVAXAssetID = "U8iRqJoiJm8xZHAacmvYyZVwqQx6uDNtQeP3CQ6fcgQk3JqnK"

// flushEvery is the group-fsync cadence in blocks: writelog/headers/code/
// misc are fsynced and executorHead advanced every flushEvery executed
// blocks (and on shutdown). Cheap; recovery walk-back covers the tail.
const flushEvery = 256

// walkBackBudget bounds how far reconcile scans for Firewood's durable
// root. Firewood's DeferredCommitInterval default is 64 and our exechead
// can lag appends by flushEvery; the budget comfortably covers both.
const walkBackBudget = 4096

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
	// Store is the flat-file state layer. Required.
	Store *state.Store
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
}

// Executor replays Fuji C-Chain blocks against Firewood-backed frontier
// state, verifies every computed state root against the header root, and
// captures post-image write frames, headers, and code into the state layer.
type Executor struct {
	cfg       Config
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
	totalGas    uint64 // session gas, for mgas/s

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
	batchLastHash  common.Hash // hash of the last executed batch block
	batchDirty     bool        // any non-empty block drained this batch
	batchCount     int
	batchBuf       []batchItem
}

// batchItem is one block's buffered state-layer output. Appends are held
// back until the batch boundary root verifies so a bad batch never leaves
// unverified frames on disk.
type batchItem struct {
	num       uint64
	headerRLP []byte
	frame     []byte
	hasFrame  bool
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
	e     *Executor    // nil outside the executor (historical eth_call)
	store *state.Store // headers log
}

func (c chainContext) Engine() consensus.Engine { return dummy.NewFullFaker() }

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
		if err != nil || !ok {
			return nil
		}
	}
	var h types.Header
	if err := rlp.DecodeBytes(raw, &h); err != nil {
		return nil
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
	return e.batchBuf[idx].headerRLP, true
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
	fetch.RegisterExtras()

	avaxAssetID, err := ids.FromString(FujiAVAXAssetID)
	if err != nil {
		return nil, fmt.Errorf("parse AVAX asset id: %w", err)
	}
	snowCtx := &snow.Context{
		NetworkID:   avaconstants.FujiID,
		AVAXAssetID: avaxAssetID,
	}

	g, err := loadFujiCChainGenesis(snowCtx)
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
	// The default 1MB node cache collapses once state outgrows it: at
	// height ~3.7M a 60s CPU profile showed ~50% of samples inside the
	// Rust library (per-SLOAD trie walks re-reading upper nodes), disk
	// well under saturation. A real node cache keeps the hot trie
	// interior resident.
	// ponytail: fixed 4GB, sized for a 25GB box; make it a flag if this
	// ever runs somewhere smaller.
	fwCfg.CacheSizeBytes = 4 << 30

	ethdbKV := cfg.Store.EthDB()
	memdb := rawdb.NewDatabase(ethdbKV)

	tdb := triedb.NewDatabase(memdb, &triedb.Config{
		DBOverride: fwCfg.BackendConstructor,
	})

	genesisRoot, err := commitGenesisIfNeeded(tdb, g, ethdbKV)
	if err != nil {
		tdb.Close()
		return nil, err
	}

	inner := extstate.NewDatabaseWithNodeDB(memdb, tdb)
	wrapDB := wrapDatabase(inner, cfg.Store, cfg.StateCacheBytes, cfg.VerifyCache)

	fwBackend, ok := tdb.Backend().(*firewood.TrieDB)
	if !ok {
		tdb.Close()
		return nil, fmt.Errorf("triedb backend is %T, want *firewood.TrieDB", tdb.Backend())
	}

	e := &Executor{
		cfg:         cfg,
		chainCfg:    g.Config,
		wrapDB:      wrapDB,
		triedb:      tdb,
		fwBackend:   fwBackend,
		snowCtx:     snowCtx,
		chainCtx:    chainContext{},
		genesisRoot: genesisRoot,
		genesisHash: g.ToBlock().Hash(),
		headRoot:    genesisRoot,
		headNum:     0,
		lastFwHash:  g.ToBlock().Hash(),
	}

	e.chainCtx = chainContext{e: e, store: cfg.Store}

	if err := e.reconcile(); err != nil {
		tdb.Close()
		return nil, err
	}
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
	execN, execOK := e.cfg.Store.ExecHead()
	hdN, hdOK := e.cfg.Store.HeadersMax()

	if !execOK && !hdOK {
		// Fresh store: seed exechead=0 so a crash before the first flush
		// still finds a consistent state file.
		if err := e.cfg.Store.FlushAndSetExecHead(0); err != nil {
			return fmt.Errorf("seed exechead: %w", err)
		}
		e.fwBackend.SetHashAndHeight(e.genesisHash, 0)
		log.Printf("exec: fresh state, genesis root=%x", e.genesisRoot)
		return nil
	}

	top := execN
	if hdOK && hdN > top {
		top = hdN
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

	// Walk-back: highest block i <= top whose header.Root == firewood root.
	var (
		fwN    uint64
		fwHash common.Hash
		found  bool
	)
	// Budget covers the worst persisted-root lag even for data written
	// before the DeferredCommitInterval scaling: 64 deferred commits of
	// CommitEvery blocks each, plus the open batch and fsync-group slack.
	budget := uint64(walkBackBudget)
	if e.cfg.CommitEvery > 1 {
		budget += 64 * uint64(e.cfg.CommitEvery)
	}
	lo := uint64(0)
	if top > budget {
		lo = top - budget
	}
	for i := top; ; i-- {
		h, err := e.loadHeader(i)
		if err != nil {
			return fmt.Errorf("reconcile: %w", err)
		}
		if h.Root == rootFW {
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
		e.headRoot = h.Root
	}

	if fwN == top {
		log.Printf("exec: resume head=%d root=%x", top, e.headRoot)
		return nil
	}

	log.Printf("exec: walk-back reconcile: firewood at %d, state layer at %d (exechead=%d), re-executing %d blocks",
		fwN, top, execN, top-fwN)
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
	if err := e.cfg.Store.FlushAndSetExecHead(e.headNum); err != nil {
		return err
	}
	log.Printf("exec: resume head=%d root=%x", e.headNum, e.headRoot)
	return nil
}

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
	if e.batchOpen && e.batchCount > 0 {
		if err := e.flushBatch(); err != nil {
			e.triedb.Close()
			return fmt.Errorf("close: flush batch: %w", err)
		}
	}
	if err := e.cfg.Store.FlushAndSetExecHead(e.headNum); err != nil {
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
func (e *Executor) Run(ctx context.Context) error {
	start := time.Now()
	lastLog := start
	lastGas, lastBlocks := uint64(0), uint64(0)
	blocksDone := uint64(0)
	lastWait := time.Time{}

	next := e.headNum + 1
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		raw, ok, err := e.cfg.Blocks.GetByHeight(next)
		if err != nil {
			return err
		}
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
			var hitPct float64
			if hits+misses > 0 {
				hitPct = 100 * float64(hits) / float64(hits+misses)
			}
			log.Printf("exec: height=%d blk/s=%.0f mgas/s=%.2f writelog=%.1fMB code_entries=%d cache_hit=%.1f%% cache=%.0fMB",
				e.headNum,
				float64(blocksDone-lastBlocks)/dt,
				float64(e.totalGas-lastGas)/dt/1e6,
				float64(e.cfg.Store.WritelogBytes())/1e6,
				e.cfg.Store.CodeCount(),
				hitPct,
				float64(e.wrapDB.cacheSize)/1e6,
			)
			lastLog = time.Now()
			lastGas, lastBlocks = e.totalGas, blocksDone
		}
	}
}

// executeRaw parses and executes one container at the expected height.
func (e *Executor) executeRaw(blockNum uint64, raw []byte) error {
	blk, err := parseEthBlock(raw)
	if err != nil {
		return fmt.Errorf("block %d parse: %w", blockNum, err)
	}
	if got := blk.NumberU64(); got != blockNum {
		return fmt.Errorf("block %d has internal number %d", blockNum, got)
	}
	if e.cfg.CommitEvery > 1 {
		if err := e.executeBatched(blk); err != nil {
			return fmt.Errorf("block %d: %w", blockNum, err)
		}
	} else {
		newRoot, err := e.executeBlock(blk)
		if err != nil {
			return fmt.Errorf("block %d: %w", blockNum, err)
		}
		e.headRoot = newRoot
		e.headNum = blockNum
	}
	e.totalGas += blk.GasUsed()
	return nil
}

// maybeFlush advances the durable watermark every flushEvery blocks.
func (e *Executor) maybeFlush(blockNum uint64) error {
	if blockNum%flushEvery != 0 {
		return nil
	}
	return e.cfg.Store.FlushAndSetExecHead(blockNum)
}

// executeBlock runs the EVM + atomic txs for blk, verifies the computed
// state root against header.Root (hard stop on mismatch), appends the
// write frame + header to the state layer, then commits Firewood.
func (e *Executor) executeBlock(blk *types.Block) (common.Hash, error) {
	header := blk.Header()
	parentRoot := e.headRoot
	blockNum := blk.NumberU64()

	headerRLP, err := rlp.EncodeToBytes(header)
	if err != nil {
		return common.Hash{}, fmt.Errorf("encode header: %w", err)
	}

	// Empty-block fast path (HOT on Fuji): if the header claims no state
	// change, running the EVM would be a no-op and Firewood's Commit
	// rejects mid-chain identity proposals ("committable proposal not
	// found" wall in the reference, LOG.md block 33405). Persist the
	// header only and advance Firewood's in-memory block-hash tracking so
	// the next non-empty block's Update resolves its parent hash.
	if header.Root == parentRoot {
		if err := e.cfg.Store.AppendHeader(blockNum, headerRLP); err != nil {
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

	frame := &blockFrame{}
	e.wrapDB.setFrame(frame)
	defer e.wrapDB.setFrame(nil)

	statedb, err := ethstate.New(parentRoot, e.wrapDB, nil)
	if err != nil {
		return common.Hash{}, fmt.Errorf("open statedb: %w", err)
	}
	if err := e.runEVM(blk, statedb); err != nil {
		return common.Hash{}, err
	}

	// Commit triggers the trie interceptor: post-images accumulate into
	// frame during this call. The block number handed to Commit is
	// Firewood's proposal height (tree height + 1), which equals the
	// real block height in per-block mode.
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
		return common.Hash{}, fmt.Errorf("state root mismatch: computed %x, expected %x", newRoot, header.Root)
	}

	if err := e.cfg.Store.AppendWrites(blockNum, frame.buf); err != nil {
		return common.Hash{}, err
	}
	if err := e.cfg.Store.AppendHeader(blockNum, headerRLP); err != nil {
		return common.Hash{}, err
	}
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

// runEVM applies upgrades, all transactions, and the atomic ExtData
// transfers of blk onto statedb. Shared by the per-block and batched
// paths.
func (e *Executor) runEVM(blk *types.Block, statedb *ethstate.StateDB) error {
	header := blk.Header()

	upgradeBlockCtx := corethcore.NewBlockContext(header.Number, header.Time)
	if err := corethcore.ApplyUpgrades(e.chainCfg, nil, upgradeBlockCtx, statedb); err != nil {
		return fmt.Errorf("apply upgrades: %w", err)
	}

	blockCtx := corethcore.NewEVMBlockContext(header, e.chainCtx, nil)
	gp := new(corethcore.GasPool).AddGas(header.GasLimit)
	var usedGas uint64

	for txIndex, tx := range blk.Transactions() {
		statedb.SetTxContext(tx.Hash(), txIndex)
		if _, err := corethcore.ApplyTransaction(
			e.chainCfg, e.chainCtx, blockCtx, gp, statedb,
			header, tx, &usedGas, vm.Config{},
		); err != nil {
			return fmt.Errorf("tx %d: %w", txIndex, err)
		}
	}

	// Atomic txs ride in the block's ExtData; Fuji has real imports and
	// exports, so this path is mandatory for root correctness.
	if extData := ccustomtypes.BlockExtData(blk); len(extData) > 0 {
		rules := e.chainCfg.Rules(header.Number, cparams.IsMergeTODO, header.Time)
		isAP5 := false
		if rulesExtra := cparams.GetRulesExtra(rules); rulesExtra != nil {
			isAP5 = rulesExtra.AvalancheRules.IsApricotPhase5
		}
		atomicTxs, err := atomic.ExtractAtomicTxs(extData, isAP5, atomic.Codec)
		if err != nil {
			return fmt.Errorf("extract atomic txs: %w", err)
		}
		wrapped := extstate.New(statedb)
		for i, atx := range atomicTxs {
			if err := atx.UnsignedAtomicTx.EVMStateTransfer(e.snowCtx, wrapped); err != nil {
				return fmt.Errorf("atomic tx %d: %w", i, err)
			}
		}
	}
	return nil
}

// executeBatched accumulates blk into the open batch (opening one if
// needed) and, when the batch reaches CommitEvery blocks, flushes it as a
// single Firewood proposal. Mid-batch blocks drain their writes through
// the capture wrapper into the shared trie (Firewood untouched); reads
// see them via the read cache and the shared trie's dirtyKeys overlay.
// State-layer appends are buffered until the boundary root verifies.
func (e *Executor) executeBatched(blk *types.Block) error {
	header := blk.Header()
	blockNum := blk.NumberU64()

	if !e.batchOpen {
		e.batchOpen = true
		e.batchStartNum = e.headNum
		e.batchStartRoot = e.headRoot
		e.batchDirty = false
		e.batchCount = 0
		e.wrapDB.beginBatch(e.headRoot)
	}

	headerRLP, err := rlp.EncodeToBytes(header)
	if err != nil {
		return fmt.Errorf("encode header: %w", err)
	}

	if header.Root == e.headRoot {
		// Empty block: header only, no EVM, no Firewood.
		e.batchBuf = append(e.batchBuf, batchItem{num: blockNum, headerRLP: headerRLP})
	} else {
		frame := &blockFrame{}
		e.wrapDB.setFrame(frame)
		statedb, err := ethstate.New(e.batchStartRoot, e.wrapDB, nil)
		if err != nil {
			e.wrapDB.setFrame(nil)
			return fmt.Errorf("open statedb: %w", err)
		}
		if err := e.runEVM(blk, statedb); err != nil {
			e.wrapDB.setFrame(nil)
			return err
		}
		// Drain commit: the batch-account trie reports Hash == origin, so
		// statedb skips TrieDB().Update entirely; writes flow through the
		// capture wrapper into the shared trie's pending ops.
		if _, err := statedb.Commit(blockNum, e.chainCfg.IsEIP158(header.Number)); err != nil {
			e.wrapDB.setFrame(nil)
			return fmt.Errorf("statedb drain commit: %w", err)
		}
		e.wrapDB.setFrame(nil)
		e.batchDirty = true
		e.batchBuf = append(e.batchBuf, batchItem{num: blockNum, headerRLP: headerRLP, frame: frame.buf, hasFrame: true})
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

// flushBatch closes the open batch: ONE Firewood proposal for all
// accumulated writes, boundary root verified against the boundary block's
// header root (bisecting per block on mismatch), then the buffered
// state-layer appends land.
func (e *Executor) flushBatch() error {
	boundaryNum, boundaryRoot, boundaryHash := e.headNum, e.headRoot, e.batchLastHash

	computed := e.batchStartRoot
	if e.batchDirty {
		computed = e.wrapDB.batchProposalRoot()
	}
	if computed != boundaryRoot {
		return e.bisect(computed, boundaryNum, boundaryRoot)
	}

	if e.batchDirty {
		opt := stateconf.WithTrieDBUpdatePayload(e.lastFwHash, boundaryHash)
		if err := e.triedb.Update(computed, e.batchStartRoot, e.fwHeight+1, nil, nil, opt); err != nil {
			return fmt.Errorf("batch update [%d..%d]: %w", e.batchStartNum+1, boundaryNum, err)
		}
		if err := e.triedb.Commit(computed, false); err != nil {
			return fmt.Errorf("batch commit [%d..%d]: %w", e.batchStartNum+1, boundaryNum, err)
		}
		e.fwHeight++
	} else {
		// Whole batch empty: no proposal, just register the boundary hash.
		e.fwBackend.SetHashAndHeight(boundaryHash, boundaryNum)
		e.fwHeight = boundaryNum
	}
	e.lastFwHash = boundaryHash

	for _, it := range e.batchBuf {
		if it.hasFrame {
			if err := e.cfg.Store.AppendWrites(it.num, it.frame); err != nil {
				return err
			}
		}
		if err := e.cfg.Store.AppendHeader(it.num, it.headerRLP); err != nil {
			return err
		}
		if err := e.maybeFlush(it.num); err != nil {
			return err
		}
	}

	e.batchOpen = false
	e.batchDirty = false
	e.batchBuf = nil
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

	for i := from; i <= to; i++ {
		raw, ok, err := e.cfg.Blocks.GetByHeight(i)
		if err != nil || !ok {
			return fmt.Errorf("bisect: read container %d: ok=%v err=%v", i, ok, err)
		}
		blk, err := parseEthBlock(raw)
		if err != nil {
			return fmt.Errorf("bisect: parse block %d: %w", i, err)
		}
		newRoot, err := e.executeBlock(blk)
		if err != nil {
			return fmt.Errorf("bisect: offending block %d: %w", i, err)
		}
		e.headRoot = newRoot
		e.headNum = i
	}
	return fmt.Errorf("batch [%d..%d] root mismatch (computed %x want %x) but per-block re-execution verified clean: batching bug", from, to, computed, boundaryRoot)
}

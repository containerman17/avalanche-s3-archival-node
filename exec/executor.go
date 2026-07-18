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
}

// chainContext is the minimal coreth ChainContext for
// corethcore.NewEVMBlockContext / ApplyTransaction. GetHeader is served
// from the headers log so the BLOCKHASH opcode returns real hashes across
// its 256-block window (the reference's zero-hash stub bug documented in
// its LOG.md is exactly what this avoids).
type chainContext struct {
	store *state.Store
}

func (c chainContext) Engine() consensus.Engine { return dummy.NewFullFaker() }

func (c chainContext) GetHeader(_ common.Hash, num uint64) *types.Header {
	if c.store == nil {
		return nil
	}
	raw, ok, err := c.store.HeaderRLP(num)
	if err != nil || !ok {
		return nil
	}
	var h types.Header
	if err := rlp.DecodeBytes(raw, &h); err != nil {
		return nil
	}
	return &h
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
	wrapDB := wrapDatabase(inner, cfg.Store)

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
		chainCtx:    chainContext{store: cfg.Store},
		genesisRoot: genesisRoot,
		genesisHash: g.ToBlock().Hash(),
		headRoot:    genesisRoot,
		headNum:     0,
	}

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
	lo := uint64(0)
	if top > walkBackBudget {
		lo = top - walkBackBudget
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
	// Make the re-executed tail durable immediately.
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

// Close flushes the state layer watermark and releases Firewood.
func (e *Executor) Close() error {
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
			log.Printf("exec: height=%d blk/s=%.0f mgas/s=%.2f writelog=%.1fMB code_entries=%d",
				e.headNum,
				float64(blocksDone-lastBlocks)/dt,
				float64(e.totalGas-lastGas)/dt/1e6,
				float64(e.cfg.Store.WritelogBytes())/1e6,
				e.cfg.Store.CodeCount(),
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
	newRoot, err := e.executeBlock(blk)
	if err != nil {
		return fmt.Errorf("block %d: %w", blockNum, err)
	}
	e.headRoot = newRoot
	e.headNum = blockNum
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
		return parentRoot, nil
	}

	frame := &blockFrame{}
	e.wrapDB.setFrame(frame)
	defer e.wrapDB.setFrame(nil)

	statedb, err := ethstate.New(parentRoot, e.wrapDB, nil)
	if err != nil {
		return common.Hash{}, fmt.Errorf("open statedb: %w", err)
	}

	upgradeBlockCtx := corethcore.NewBlockContext(header.Number, header.Time)
	if err := corethcore.ApplyUpgrades(e.chainCfg, nil, upgradeBlockCtx, statedb); err != nil {
		return common.Hash{}, fmt.Errorf("apply upgrades: %w", err)
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
			return common.Hash{}, fmt.Errorf("tx %d: %w", txIndex, err)
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
			return common.Hash{}, fmt.Errorf("extract atomic txs: %w", err)
		}
		wrapped := extstate.New(statedb)
		for i, atx := range atomicTxs {
			if err := atx.UnsignedAtomicTx.EVMStateTransfer(e.snowCtx, wrapped); err != nil {
				return common.Hash{}, fmt.Errorf("atomic tx %d: %w", i, err)
			}
		}
	}

	// Commit triggers the trie interceptor: post-images accumulate into
	// frame during this call.
	triedbOpt := stateconf.WithTrieDBUpdatePayload(header.ParentHash, blk.Hash())
	newRoot, err := statedb.Commit(
		blockNum,
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
	return newRoot, nil
}

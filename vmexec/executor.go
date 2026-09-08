// Package vmexec is a COPY of exec's subnet-evm path with the state engine
// (latest + commit) in place of Firewood: no triedb, no cgo, no walk-back.
// Everything the store receives (receipts, frames, state rows, headers, code)
// is produced exactly as exec produces it; only the inner state database and
// the root check changed. Restart/recovery is out of scope: a start rebuilds
// the state from genesis and refuses a dir that already holds blocks.
package vmexec

import (
	"context"
	"fmt"
	"log"
	"path/filepath"
	goatomic "sync/atomic"
	"time"

	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/rawdb"
	ethstate "github.com/ava-labs/libevm/core/state"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/params"
	"github.com/ava-labs/libevm/rlp"
	"github.com/ava-labs/libevm/triedb"

	"github.com/containerman17/avalanche-s3-archival-node/chain"
	"github.com/containerman17/avalanche-s3-archival-node/store"
)

// flushEvery is the group-fsync cadence in blocks.
const flushEvery = 256

// BlockSource yields raw containers by height (the fetcher's RAM queue).
type BlockSource interface {
	GetByHeight(n uint64) ([]byte, bool, error)
}

// Config is the opening configuration for an Executor.
type Config struct {
	DataDir string
	Blocks  BlockSource
	Store   *store.DB
	Misc    *store.MiscStore
	Chain   *chain.Chain
	// RollBudget is the overlay size (accounted bytes) that triggers a
	// background merge + trie roll.
	RollBudget int
	StopAt     uint64
	OnBlock    func(num uint64, hash common.Hash)
}

// Stats is the bench snapshot.
type Stats struct {
	Height, Blocks, Txs, Gas uint64
	Wait                     time.Duration // executor time spent waiting for a container
	Overlay, Dirty           int
	Rolls                    int
	Rolling                  bool
}

// Executor replays blocks against the state engine, verifies every computed
// state root against the header root, and captures post-image rows, headers
// and code into the store.
type Executor struct {
	cfg      Config
	chainCfg *params.ChainConfig
	chainCtx chainContext
	wrapDB   *wrappedDatabase
	flat     *flatDB
	eng      *engine

	genesisHash common.Hash
	headRoot    common.Hash
	headNum     uint64
	headTime    uint64
	totalGas    uint64
	totalTxs    uint64
	blocksDone  uint64
	waitNs      goatomic.Int64
	live        goatomic.Uint64
	statsMu     goatomic.Pointer[Stats]

	capture    *capture
	curBW      *store.BlockWrite
	curHeader  *types.Header
	curStatedb *ethstate.StateDB
	signer     types.Signer

	spl struct{ read, evm, hash, write time.Duration }

	flushReq  chan uint64
	flushErr  chan error
	flushDone chan struct{}
}

// FetchStart: a fresh dir starts at block 1, anchored on the genesis hash.
func FetchStart(genesisHash common.Hash) (from uint64, anchor ids.ID) {
	return 1, ids.ID(genesisHash)
}

// New builds the genesis state, checks its root, and returns an Executor
// ready to Run. The store must be empty (no restart in this prototype).
func New(cfg Config) (*Executor, error) {
	if cfg.Blocks == nil || cfg.DataDir == "" || cfg.Store == nil || cfg.Misc == nil || cfg.Chain == nil {
		return nil, fmt.Errorf("config: Blocks, DataDir, Store, Misc and Chain are required")
	}
	if head, ok := cfg.Store.Head(); ok && head > 0 {
		return nil, fmt.Errorf("data dir already holds blocks through %d: epochdb-vm has no restart path yet, start from an empty dir", head)
	}
	if cfg.RollBudget <= 0 {
		cfg.RollBudget = 2 << 30
	}
	g, err := ChainGenesis(cfg.Chain)
	if err != nil {
		return nil, err
	}
	if err := cfg.Misc.BindVMKind(string(cfg.Chain.VMKind)); err != nil {
		return nil, err
	}
	eng, err := newEngine(filepath.Join(cfg.DataDir, "vmstate"), g.TrieAlloc, g.Root)
	if err != nil {
		return nil, err
	}
	ethdbKV := store.EthDB(cfg.Store, cfg.Misc, g.TrieAlloc)
	memdb := rawdb.NewDatabase(ethdbKV)
	// Code reads go through libevm's cachingDB over the store's ethdb, as
	// before; its hash-scheme triedb is only ever asked for its Scheme.
	code := ethstate.NewDatabaseWithNodeDB(memdb, triedb.NewDatabase(memdb, triedb.HashDefaults))
	flat := &flatDB{code: code, eng: eng}
	e := &Executor{
		cfg:         cfg,
		chainCfg:    g.Config,
		chainCtx:    chainContext{store: cfg.Store},
		wrapDB:      wrapDatabase(flat),
		flat:        flat,
		eng:         eng,
		genesisHash: g.Hash,
		headRoot:    g.Root,
		headTime:    g.Timestamp,
	}
	e.live.Store(0)
	return e, nil
}

// Close flushes the store and releases the engine's mmaps.
func (e *Executor) Close() error {
	err := e.cfg.Store.Sync()
	e.eng.close()
	return err
}

func (e *Executor) Head() uint64     { return e.headNum }
func (e *Executor) LiveHead() uint64 { return e.live.Load() }

// Stats is the bench snapshot, safe from any goroutine (published per block).
func (e *Executor) Stats() Stats {
	if s := e.statsMu.Load(); s != nil {
		return *s
	}
	return Stats{}
}

func (e *Executor) publish() {
	e.live.Store(e.headNum)
	e.statsMu.Store(&Stats{
		Height: e.headNum, Blocks: e.blocksDone, Txs: e.totalTxs, Gas: e.totalGas,
		Wait: time.Duration(e.waitNs.Load()), Overlay: e.eng.overlay.Bytes(), Dirty: e.eng.dirty.Bytes(),
		Rolls: e.eng.rolls, Rolling: e.eng.frozen != nil,
	})
}

// Run executes blocks ascending from headNum+1. Returns on ctx cancel or on
// the first error; a root mismatch is log.Fatalf.
func (e *Executor) Run(ctx context.Context) (err error) {
	start := time.Now()
	lastLog := start
	lastGas, lastBlocks, lastTxs := uint64(0), uint64(0), uint64(0)
	lastWait := time.Time{}
	next := e.headNum + 1

	e.flushReq = make(chan uint64, 1)
	e.flushErr = make(chan error, 1)
	e.flushDone = make(chan struct{})
	go func() {
		defer close(e.flushDone)
		for range e.flushReq {
			if err := e.cfg.Store.Sync(); err != nil {
				e.flushErr <- err
				return
			}
		}
	}()
	defer func() {
		close(e.flushReq)
		<-e.flushDone
		e.flushReq = nil
		select {
		case ferr := <-e.flushErr:
			if err == nil {
				err = fmt.Errorf("flusher: %w", ferr)
			}
		default:
		}
	}()

	// Prefetcher: container reads, decoding and sender recovery overlap
	// execution (exec's, minus the warm stage, which is a no-op on subnet-evm).
	type pfItem struct {
		n   uint64
		pvm []byte
		blk *types.Block
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
			pvm, blk, perr := store.SplitContainer(raw)
			if perr != nil {
				select {
				case pf <- pfItem{n: h, err: fmt.Errorf("block %d parse: %w", h, perr)}:
				case <-pfCtx.Done():
				}
				return
			}
			signer := types.MakeSigner(e.chainCfg, blk.Number(), blk.Time())
			for _, tx := range blk.Transactions() {
				types.Sender(signer, tx)
			}
			select {
			case pf <- pfItem{n: h, pvm: pvm, blk: blk}:
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
			log.Printf("vmexec: reached --stop height %d", e.cfg.StopAt)
			return nil
		}
		tRead := time.Now()
		var pvm []byte
		var blk *types.Block
		ok := false
		select {
		case it, alive := <-pf:
			if !alive {
			} else if it.err != nil {
				return it.err
			} else if it.n != next {
				return fmt.Errorf("prefetch out of order: got %d want %d", it.n, next)
			} else {
				pvm, blk, ok = it.pvm, it.blk, true
			}
		case <-time.After(200 * time.Millisecond):
		}
		w := time.Since(tRead)
		e.spl.read += w
		e.waitNs.Add(int64(w))
		if !ok {
			if time.Since(lastWait) > 30*time.Second {
				log.Printf("vmexec: waiting for block %d to be fetched", next)
				lastWait = time.Now()
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(200 * time.Millisecond):
			}
			continue
		}
		if err := e.executeDecoded(next, pvm, blk); err != nil {
			return err
		}
		e.blocksDone++
		next++

		if since := time.Since(lastLog); since >= 10*time.Second {
			dt := since.Seconds()
			sz := e.cfg.Store.SectionSizes()
			log.Printf("vmexec: height=%d blk/s=%.0f tx/s=%.0f mgas/s=%.2f chain=%.1fMB state=%.1fMB lookup=%.1fMB runs=%d overlay=%.0fMB dirty=%.0fMB rolls=%d",
				e.headNum,
				float64(e.blocksDone-lastBlocks)/dt,
				float64(e.totalTxs-lastTxs)/dt,
				float64(e.totalGas-lastGas)/dt/1e6,
				float64(sz[store.SecChain])/1e6,
				float64(sz[store.SecState])/1e6,
				float64(sz[store.SecLookup])/1e6,
				len(e.cfg.Store.Manifest().Runs),
				float64(e.eng.overlay.Bytes())/1e6, float64(e.eng.dirty.Bytes())/1e6, e.eng.rolls,
			)
			log.Printf("vmexec: split read=%.2fs evm=%.2fs hash=%.2fs write=%.2fs of %.1fs",
				e.spl.read.Seconds(), e.spl.evm.Seconds(), e.spl.hash.Seconds(), e.spl.write.Seconds(), dt)
			e.spl = struct{ read, evm, hash, write time.Duration }{}
			lastLog = time.Now()
			lastGas, lastBlocks, lastTxs = e.totalGas, e.blocksDone, e.totalTxs
		}
	}
}

func (e *Executor) executeDecoded(blockNum uint64, pvm []byte, blk *types.Block) error {
	if got := blk.NumberU64(); got != blockNum {
		return fmt.Errorf("block %d has internal number %d", blockNum, got)
	}
	if err := e.eng.finishRoll(); err != nil {
		log.Fatalf("vmexec: %v", err)
	}
	newRoot, err := e.executeBlock(blk, pvm)
	if err != nil {
		return fmt.Errorf("block %d: %w", blockNum, err)
	}
	e.headRoot = newRoot
	e.headNum = blockNum
	e.headTime = blk.Time()
	e.totalGas += blk.GasUsed()
	e.totalTxs += uint64(len(blk.Transactions()))
	e.eng.maybeRoll(e.cfg.RollBudget, blockNum, newRoot)
	e.publish()
	if e.cfg.OnBlock != nil {
		e.cfg.OnBlock(e.headNum, blk.Hash())
	}
	return nil
}

// maybeFlush advances the durable watermark every flushEvery blocks.
func (e *Executor) maybeFlush(blockNum uint64) error {
	if blockNum%flushEvery != 0 {
		return nil
	}
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

// executeBlock runs the EVM for blk, applies its write set to the engine,
// verifies the computed root against header.Root (a mismatch is death), then
// hands the block's rows to the store. Publish happens after the root matched.
func (e *Executor) executeBlock(blk *types.Block, pvm []byte) (common.Hash, error) {
	header := blk.Header()
	parentRoot := e.headRoot
	blockNum := blk.NumberU64()

	headerRLP, err := rlp.EncodeToBytes(header)
	if err != nil {
		return common.Hash{}, fmt.Errorf("encode header: %w", err)
	}
	bw := &store.BlockWrite{Height: blockNum, HeaderRLP: headerRLP, Pvm: pvm, Code: map[string][]byte{}}

	// Empty-block fast path: no state change claimed and no transactions.
	if header.Root == parentRoot && len(blk.Transactions()) == 0 {
		tW := time.Now()
		if err := e.cfg.Store.WriteBlock(bw); err != nil {
			return common.Hash{}, err
		}
		e.spl.write += time.Since(tW)
		return parentRoot, e.maybeFlush(blockNum)
	}

	tEVM := time.Now()
	cap := &capture{code: map[string][]byte{}}
	e.beginCapture(cap, bw, header, parentRoot)
	defer e.endCapture()

	statedb, err := ethstate.New(parentRoot, e.wrapDB, nil)
	if err != nil {
		return common.Hash{}, fmt.Errorf("open statedb: %w", err)
	}
	e.curStatedb = statedb
	receipts, err := runEVM(e.chainCtx, e.chainCfg, blk, e.headTime, statedb, e.captureTx)
	if err != nil {
		return common.Hash{}, err
	}
	// Commit drains whatever the last tx boundary did not (block-final
	// writes) through the interceptor into the write set. The account trie
	// answers the parent root, so statedb skips its triedb update.
	if _, err := statedb.Commit(blockNum, e.chainCfg.IsEIP158(header.Number)); err != nil {
		return common.Hash{}, fmt.Errorf("statedb commit: %w", err)
	}
	e.spl.evm += time.Since(tEVM)

	tHash := time.Now()
	ws := e.flat.take()
	if err := e.eng.apply(ws); err != nil {
		return common.Hash{}, fmt.Errorf("apply write set: %w", err)
	}
	newRoot, err := e.eng.root()
	if err != nil {
		return common.Hash{}, fmt.Errorf("state root: %w", err)
	}
	e.spl.hash += time.Since(tHash)

	if newRoot != header.Root {
		dumpMismatch(blk, cap.rows, receipts, statedb, newRoot, header.Root)
		log.Fatalf("vmexec: block %d: state root mismatch: computed %x, header %x", blockNum, newRoot, header.Root)
	}

	tW := time.Now()
	bw.Tail = cap.take()
	bw.Code = cap.code
	if err := e.cfg.Store.WriteBlock(bw); err != nil {
		return common.Hash{}, err
	}
	e.wrapDB.forgetRecentCode()
	e.spl.write += time.Since(tW)
	return newRoot, e.maybeFlush(blockNum)
}

func (e *Executor) beginCapture(c *capture, bw *store.BlockWrite, header *types.Header, parentRoot common.Hash) {
	e.capture, e.curBW, e.curHeader = c, bw, header
	e.signer = types.MakeSigner(e.chainCfg, header.Number, header.Time)
	e.wrapDB.setCapture(c)
	e.flat.begin(parentRoot)
}

func (e *Executor) endCapture() {
	e.wrapDB.setCapture(nil)
	e.flat.ws = nil
	e.capture, e.curBW, e.curHeader, e.curStatedb = nil, nil, nil, nil
}

// captureTx closes one transaction: a per-tx IntermediateRoot drains its
// post-images through the interceptor, then the execution outputs become
// the row set the store keeps. Verbatim from exec.
func (e *Executor) captureTx(_ int, tx *types.Transaction, r *types.Receipt) error {
	e.curStatedb.IntermediateRoot(e.chainCfg.IsEIP158(e.curHeader.Number))

	raw, err := rlp.EncodeToBytes(tx)
	if err != nil {
		return fmt.Errorf("encode tx %s: %w", tx.Hash(), err)
	}
	frameRec, frameAddrs, why := frames.take()
	if why != "" {
		e.dieOnUncapturedTrace(tx.Hash().String(), why,
			"The contract this capture models is in vmexec/frames.go: every CaptureEnter is paired with a CaptureExit. "+
				"An unpaired shape is an execution path the capture does not model yet.")
	}
	tw := store.TxWrite{
		Hash:       tx.Hash().Bytes(),
		RLP:        raw,
		Receipt:    store.EncodeTxReceipt(r, r.CumulativeGasUsed),
		Frames:     frameRec,
		FrameAddrs: frameAddrs,
		State:      e.capture.take(),
	}
	if from, err := types.Sender(e.signer, tx); err == nil {
		tw.Sender = from.Bytes()
	}
	if to := tx.To(); to != nil {
		tw.To = to.Bytes()
	} else if r.ContractAddress != (common.Address{}) {
		tw.Created = r.ContractAddress.Bytes()
	}
	for _, l := range r.Logs {
		lw := store.LogWrite{Emitter: l.Address.Bytes()}
		for _, tp := range l.Topics {
			lw.Topics = append(lw.Topics, tp.Bytes())
		}
		tw.Logs = append(tw.Logs, lw)
	}
	e.curBW.Txs = append(e.curBW.Txs, tw)
	return nil
}

// dieOnUncapturedTrace is THE FAIL-STOP (DESIGN: traces are stored, and
// capture failure is death).
func (e *Executor) dieOnUncapturedTrace(what, why, fix string) {
	log.Fatalf("epochdb-vm: block %d (%s): THE CALL TRACE WAS NOT CAPTURED, so this block cannot be stored.\n  Cause: %s.\n  %s",
		e.curHeader.Number, what, why, fix)
}

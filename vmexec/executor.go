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

	spl             struct{ read, evm time.Duration }
	hashNs, writeNs goatomic.Int64 // the checker's split
	dirtyBytes      goatomic.Int64

	// The root check runs one block behind on its own goroutine: the
	// executor applies a block's write set to the overlay, hands the block
	// to checkQ and executes the next one. The checker applies the write
	// set to Dirty, checks the root, writes the block to the store and
	// publishes it. Dirty (and the store's WriteBlock) belong to the
	// checker; the executor touches Dirty only while the checker is parked
	// at a sync item (the roll swap).
	checkQ    chan *checkItem
	checkDone chan struct{}
	checkDead chan struct{} // closed when the checker stopped on an error
	checkErr  error
	// unpublished is what block N+1 needs from block N before the checker
	// has written N to the store: its header (BLOCKHASH) and the hashes of
	// the code it deployed (served from wrapDB.recent until then).
	unpublished []unpublishedBlock
	recentHdr   map[uint64]*types.Header

	flushReq  chan uint64
	flushErr  chan error
	flushDone chan struct{}
}

// checkDepth is how many executed blocks may wait for their root check.
const checkDepth = 4

type checkItem struct {
	blk      *types.Block
	bw       *store.BlockWrite
	ws       *writeSet // nil for an empty block: nothing to hash
	receipts types.Receipts
	statedb  *ethstate.StateDB
	stats    Stats         // the executor's side of the snapshot; Dirty is filled in by the checker
	sync     chan struct{} // a sync item: the checker parks on it until told to go on
}

type unpublishedBlock struct {
	n    uint64
	code []string
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
		chainCtx:    chainContext{store: cfg.Store, recent: map[uint64]*types.Header{}},
		wrapDB:      wrapDatabase(flat),
		flat:        flat,
		eng:         eng,
		genesisHash: g.Hash,
		headRoot:    g.Root,
		headTime:    g.Timestamp,
	}
	e.live.Store(0)
	e.recentHdr = e.chainCtx.recent
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

// publish is the checker's: a block is visible only after its root matched.
func (e *Executor) publish(st Stats) {
	st.Wait = time.Duration(e.waitNs.Load())
	st.Dirty = int(e.dirtyBytes.Load())
	e.statsMu.Store(&st)
	e.live.Store(st.Height)
}

// checker is the root-check goroutine: see checkQ.
func (e *Executor) checker() {
	defer close(e.checkDone)
	for it := range e.checkQ {
		if it.sync != nil {
			it.sync <- struct{}{}
			<-it.sync
			continue
		}
		if err := e.check(it); err != nil {
			e.checkErr = err
			close(e.checkDead)
			for range e.checkQ { // let the executor's sends through
			}
			return
		}
	}
}

func (e *Executor) check(it *checkItem) error {
	blockNum := it.blk.NumberU64()
	if it.ws != nil {
		t0 := time.Now()
		if err := e.eng.applyDirty(it.ws); err != nil {
			return fmt.Errorf("block %d: apply write set: %w", blockNum, err)
		}
		root, err := e.eng.root()
		if err != nil {
			return fmt.Errorf("block %d: state root: %w", blockNum, err)
		}
		e.hashNs.Add(int64(time.Since(t0)))
		if want := it.blk.Root(); root != want {
			dumpMismatch(it.blk, it.bw.Tail, it.receipts, it.statedb, root, want)
			log.Fatalf("vmexec: block %d: state root mismatch: computed %x, header %x", blockNum, root, want)
		}
	}
	e.dirtyBytes.Store(int64(e.eng.dirty.Bytes()))
	t0 := time.Now()
	if err := e.cfg.Store.WriteBlock(it.bw); err != nil {
		return err
	}
	e.writeNs.Add(int64(time.Since(t0)))
	if err := e.maybeFlush(blockNum); err != nil {
		return err
	}
	e.publish(it.stats)
	if e.cfg.OnBlock != nil {
		e.cfg.OnBlock(blockNum, it.blk.Hash())
	}
	return nil
}

// enqueue hands a checked-later block to the checker; it blocks while
// checkDepth blocks are waiting.
func (e *Executor) enqueue(it *checkItem) error {
	select {
	case e.checkQ <- it:
		return nil
	case <-e.checkDead:
		return e.checkErr
	}
}

// parkChecker waits until the checker has processed every block handed to
// it and parks it; the returned func resumes it. Between the two the caller
// owns Dirty and the store.
func (e *Executor) parkChecker() (resume func(), err error) {
	ch := make(chan struct{})
	if err := e.enqueue(&checkItem{sync: ch}); err != nil {
		return nil, err
	}
	select {
	case <-ch:
		return func() { ch <- struct{}{} }, nil
	case <-e.checkDead:
		return nil, e.checkErr
	}
}

// forgetPublished drops what block N+1 no longer needs to see from blocks
// the checker has published.
func (e *Executor) forgetPublished() {
	live := e.live.Load()
	for len(e.unpublished) > 0 && e.unpublished[0].n <= live {
		u := e.unpublished[0]
		e.unpublished = e.unpublished[1:]
		delete(e.recentHdr, u.n)
		for _, h := range u.code {
			delete(e.wrapDB.recent, h)
		}
	}
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
	e.checkQ = make(chan *checkItem, checkDepth)
	e.checkDone = make(chan struct{})
	e.checkDead = make(chan struct{})
	go e.checker()
	defer func() {
		close(e.checkQ)
		<-e.checkDone
		if e.checkErr != nil && err == nil {
			err = fmt.Errorf("checker: %w", e.checkErr)
		}
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
				float64(e.eng.overlay.Bytes())/1e6, float64(e.dirtyBytes.Load())/1e6, e.eng.rolls,
			)
			log.Printf("vmexec: split read=%.2fs evm=%.2fs | checker hash=%.2fs write=%.2fs of %.1fs",
				e.spl.read.Seconds(), e.spl.evm.Seconds(), float64(e.hashNs.Swap(0))/1e9, float64(e.writeNs.Swap(0))/1e9, dt)
			e.spl = struct{ read, evm time.Duration }{}
			lastLog = time.Now()
			lastGas, lastBlocks, lastTxs = e.totalGas, e.blocksDone, e.totalTxs
		}
	}
}

func (e *Executor) executeDecoded(blockNum uint64, pvm []byte, blk *types.Block) error {
	if got := blk.NumberU64(); got != blockNum {
		return fmt.Errorf("block %d has internal number %d", blockNum, got)
	}
	if e.eng.rollReady() {
		// The swap rebases Dirty and reads the store: park the checker,
		// which has then verified every block up to this one.
		resume, err := e.parkChecker()
		if err != nil {
			return err
		}
		err = e.eng.finishRoll()
		resume()
		if err != nil {
			log.Fatalf("vmexec: %v", err)
		}
	}
	e.forgetPublished()
	it, err := e.executeBlock(blk, pvm)
	if err != nil {
		return fmt.Errorf("block %d: %w", blockNum, err)
	}
	// From here the block's root is the header's or the checker dies.
	e.headRoot = blk.Root()
	e.headNum = blockNum
	e.headTime = blk.Time()
	e.totalGas += blk.GasUsed()
	e.totalTxs += uint64(len(blk.Transactions()))
	e.eng.maybeRoll(e.cfg.RollBudget, blockNum, e.headRoot)
	it.stats = Stats{
		Height: blockNum, Blocks: e.blocksDone + 1, Txs: e.totalTxs, Gas: e.totalGas,
		Overlay: e.eng.overlay.Bytes(), Rolls: e.eng.rolls, Rolling: e.eng.frozen != nil,
	}
	return e.enqueue(it)
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

// executeBlock runs the EVM for blk and applies its write set to the
// overlay, so the next block reads it; the root check and the store write
// are the checker's (see checkQ), which is where the block is published.
func (e *Executor) executeBlock(blk *types.Block, pvm []byte) (*checkItem, error) {
	header := blk.Header()
	parentRoot := e.headRoot
	blockNum := blk.NumberU64()

	headerRLP, err := rlp.EncodeToBytes(header)
	if err != nil {
		return nil, fmt.Errorf("encode header: %w", err)
	}
	bw := &store.BlockWrite{Height: blockNum, HeaderRLP: headerRLP, Pvm: pvm, Code: map[string][]byte{}}
	it := &checkItem{blk: blk, bw: bw}
	e.recentHdr[blockNum] = header
	u := unpublishedBlock{n: blockNum}

	// Empty-block fast path: no state change claimed and no transactions.
	if header.Root == parentRoot && len(blk.Transactions()) == 0 {
		e.unpublished = append(e.unpublished, u)
		return it, nil
	}

	tEVM := time.Now()
	cap := &capture{code: map[string][]byte{}}
	e.beginCapture(cap, bw, header, parentRoot)
	defer e.endCapture()

	statedb, err := ethstate.New(parentRoot, e.wrapDB, nil)
	if err != nil {
		return nil, fmt.Errorf("open statedb: %w", err)
	}
	e.curStatedb = statedb
	receipts, err := runEVM(e.chainCtx, e.chainCfg, blk, e.headTime, statedb, e.captureTx)
	if err != nil {
		return nil, err
	}
	// Commit drains whatever the last tx boundary did not (block-final
	// writes) through the interceptor into the write set. The account trie
	// answers the parent root, so statedb skips its triedb update.
	if _, err := statedb.Commit(blockNum, e.chainCfg.IsEIP158(header.Number)); err != nil {
		return nil, fmt.Errorf("statedb commit: %w", err)
	}
	ws := e.flat.take()
	e.eng.applyOverlay(ws)
	e.spl.evm += time.Since(tEVM)

	bw.Tail = cap.take()
	bw.Code = cap.code
	for h := range cap.code {
		u.code = append(u.code, h)
	}
	e.unpublished = append(e.unpublished, u)
	it.ws, it.receipts, it.statedb = ws, receipts, statedb
	return it, nil
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

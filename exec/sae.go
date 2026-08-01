package exec

import (
	"fmt"
	"log"
	"math"
	"sync/atomic"
	"time"

	"github.com/ava-labs/avalanchego/database"
	"github.com/ava-labs/avalanchego/utils/logging"
	"github.com/ava-labs/avalanchego/vms/components/gas"
	"github.com/ava-labs/avalanchego/vms/saevm/blocks"
	"github.com/ava-labs/avalanchego/vms/saevm/gastime"
	"github.com/ava-labs/avalanchego/vms/saevm/hook"
	"github.com/ava-labs/avalanchego/vms/saevm/saexec"
	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/consensus"
	ethstate "github.com/ava-labs/libevm/core/state"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/ethdb"
	"github.com/ava-labs/libevm/libevm/stateconf"
	"github.com/ava-labs/libevm/rlp"
	"github.com/containerman17/epochdb/state"

	saetypes "github.com/ava-labs/avalanchego/vms/saevm/types"
)

// saeExecuted reports whether blk must be executed by the SAE engine.
//
// SAE IS CORETH-ONLY, and it is neutralized structurally rather than with a
// kind check: the subnet-evm backend reports "transition never scheduled" and
// "no settlement markers", so sae is always false there and nothing below this
// line (executeSAEBlock, initSAE, saehooks.go, saering.go) is ever reached.
//
// THE RULE, from vms/transitionvm: the transition block is the first block
// whose timestamp is at or past TransitionTime; the pre-transition VM
// (coreth) executes blocks up to AND INCLUDING it, and the post-transition
// VM (saevm) executes every block after. Restated per block, and the way
// transitionvm's own dispatch reads it: a block is SAE i.f.f. its PARENT's
// timestamp is at or past TransitionTime. On Fuji that puts the last
// coreth block at 57,403,046 and the first SAE block at 57,403,047.
//
// parentTime is the timestamp of the block this executor last executed,
// which is blk's parent (the executor only ever runs a contiguous chain).
// The settlement markers must agree with the timestamp rule; a
// disagreement means our idea of the boundary is wrong and replay stops.
func (e *Executor) saeExecuted(blk *types.Block, parentTime uint64) (bool, error) {
	markers := e.vm.hasSettledMarkers(blk.Header())
	ts, scheduled := e.vm.transitionTimestamp(e.chainCfg)
	sae := scheduled && parentTime >= ts
	if sae != markers {
		return false, fmt.Errorf(
			"block %d (time %d, parent time %d): SAE settlement markers present=%v but the transitionvm rule says SAE=%v (transition time %d, scheduled=%v)",
			blk.NumberU64(), blk.Time(), parentTime, markers, sae, ts, scheduled)
	}
	return sae, nil
}

// saeExec is the SAE-side executor state.
type saeExec struct {
	hooks *saeHooks
	// parent is the last executed block as an SAE block object: saexec.
	// Execute reads the parent post-execution state root and gas clock
	// from it. Its own ancestry is severed as soon as the next block is
	// built on it, or the executed chain would never be collectable.
	parent *blocks.Block
	// db and xdb absorb blocks.Block.MarkExecuted's persistence. The
	// executor keeps its own artefacts (frames, headers, the root ring),
	// so the SAE VM's receipt rows and head markers are discarded.
	db  ethdb.Database
	xdb saetypes.ExecutionResults

	settledHeight uint64 // highest SettledHeight seen (monotonic)
	settledSeen   bool
	settlements   uint64 // settlement markers observed this session
	lagMin        uint64
	lagMax        uint64

	// dropped is the sink blocks.Block.MarkSettled insists on. Its value
	// is never read: severing the ancestry is the whole point.
	dropped atomic.Pointer[blocks.Block]
}

// StateDB implements saedb.StateDBOpener: saexec.Execute opens the parent
// post-execution state through it, so our capture wrapper and Firewood
// frontier stay exactly where they are on the coreth path.
func (e *Executor) StateDB(root common.Hash) (*ethstate.StateDB, error) {
	return ethstate.New(root, e.wrapDB, nil)
}

// saeChainContext adapts the executor's headers-log ChainContext to
// libevm's core.ChainContext, which is what saexec.Execute takes (the
// coreth one is a distinct type with a coreth consensus.Engine).
type saeChainContext struct{ e *Executor }

func (c saeChainContext) GetHeader(h common.Hash, n uint64) *types.Header {
	return c.e.chainCtx.GetHeader(h, n)
}

// Engine is never called: saexec.Execute passes an explicit author to
// core.NewEVMBlockContext, which is the only consumer.
func (saeChainContext) Engine() consensus.Engine { return nil }

// initSAE prepares the SAE side on the first post-boundary block: the hook
// fork, the discard sinks, and the parent block object rebuilt from the
// executor's own head.
func (e *Executor) initSAE() error {
	h := &saeHooks{chainCfg: e.chainCfg, avaxAssetID: e.snowCtx.AVAXAssetID}
	s := &saeExec{
		hooks: h,
		db:    discardEthDB{},
		xdb:   saetypes.ExecutionResults{HeightIndex: discardHeightIndex{}},
	}
	parent, err := e.saeParentAt(e.headNum, h, s)
	if err != nil {
		return err
	}
	s.parent = parent
	e.sae = s
	return nil
}

// saeParentAt rebuilds the executed-block object for height n from the
// stored header plus this executor's own post-execution artefacts.
func (e *Executor) saeParentAt(n uint64, h *saeHooks, s *saeExec) (*blocks.Block, error) {
	hdr, err := e.loadHeader(n)
	if err != nil {
		return nil, err
	}
	root, clock, err := e.postExecutionAt(n, hdr, h)
	if err != nil {
		return nil, err
	}
	// The parent object only ever serves its header, hash, post-execution
	// root and gas clock, so a header-only block is enough and saves
	// re-reading the container.
	b, err := blocks.New(types.NewBlockWithHeader(hdr), nil, nil, logging.NoLog{})
	if err != nil {
		return nil, fmt.Errorf("sae: parent block %d: %w", n, err)
	}
	if err := b.MarkExecuted(s.db, s.xdb, clock, time.Now(), nil, nil, root, nil); err != nil {
		return nil, fmt.Errorf("sae: mark parent %d executed: %w", n, err)
	}
	return b, nil
}

// postExecutionAt returns THIS executor's post-execution state root and gas
// clock for height n, which is what a later header's settlement markers
// attest. Post-Helicon heights come from the root ring; at or below the
// transition block, execution was synchronous, so the block's own header
// carries its post-execution root and seeds the gas clock exactly as
// blocks.(*Block).synchronousExecutionResults does upstream.
func (e *Executor) postExecutionAt(n uint64, hdr *types.Header, h *saeHooks) (common.Hash, *gastime.Time, error) {
	if root, clockBytes, ok := e.ring.get(n); ok {
		clock := new(gastime.Time)
		if err := clock.UnmarshalCanoto(clockBytes); err != nil {
			return common.Hash{}, nil, fmt.Errorf("sae: decode gas clock at %d: %w", n, err)
		}
		return root, clock, nil
	}
	if e.vm.hasSettledMarkers(hdr) {
		return common.Hash{}, nil, fmt.Errorf(
			"sae: block %d was executed by SAE but is not in the root ring (window %d blocks): its post-execution root cannot be established without re-executing it",
			n, saeRingSlots)
	}
	clock, err := h.worstCaseGasTime(hdr)
	if err != nil {
		return common.Hash{}, nil, fmt.Errorf("sae: gas clock at synchronous block %d: %w", n, err)
	}
	return hdr.Root, clock, nil
}

// worstCaseGasTime mirrors blocks.(*Block).WorstCaseGasTime: the gas clock
// a synchronous (pre-SAE) block committed to, reconstructed from its base
// fee and the gas config after it. This is the handoff point where ACP-176
// state in header.Extra becomes the ACP-194 gas clock.
func (h *saeHooks) worstCaseGasTime(hdr *types.Header) (*gastime.Time, error) {
	target, cfg := h.GasConfigAfter(hdr)
	if err := h.takeErr(); err != nil {
		return nil, err
	}
	var price uint64
	if bf := hdr.BaseFee; bf != nil {
		if bf.IsUint64() {
			price = bf.Uint64()
		} else {
			price = math.MaxUint64
		}
	}
	return gastime.New(h.BlockTime(hdr), target, gas.Price(price), cfg)
}

// executeSAEBlock executes one post-Helicon block through saexec.Execute,
// commits the resulting state into Firewood, and verifies it against the
// header the ACP-194 way.
//
// The guarantee is the same as the coreth path's, moved by the settlement
// lag: a block's own header cannot attest its own state root, so the root
// goes into the ring and the header that later carries SettledHeight == N
// must present exactly that root (plus the gas clock at N, which the same
// markers pin). Nothing advances past a mismatch.
func (e *Executor) executeSAEBlock(blk *types.Block) error {
	if e.sae == nil {
		if err := e.initSAE(); err != nil {
			return err
		}
	}
	header := blk.Header()
	blockNum := blk.NumberU64()
	headerRLP, err := rlp.EncodeToBytes(header)
	if err != nil {
		return fmt.Errorf("encode header: %w", err)
	}

	parent := e.sae.parent
	b, err := blocks.New(blk, parent, nil, logging.NoLog{})
	if err != nil {
		return err
	}
	// Sever the parent's own ancestry: block objects point at their
	// parents, so without this the entire executed chain stays reachable.
	// Everything Execute reads off the parent (header, hash, post-exec
	// root, gas clock) survives settlement.
	if !parent.Settled() {
		if err := parent.MarkSettled(&e.sae.dropped); err != nil {
			return err
		}
	}

	frame := &blockFrame{}
	e.wrapDB.setFrame(frame)
	res, err := saexec.Execute(
		b, e, math.MaxInt, nil, e.sae.hooks,
		e.chainCfg, saeChainContext{e}, &saexec.NullReceiptStore{}, logging.NoLog{},
	)
	if err != nil {
		e.wrapDB.setFrame(nil)
		return err
	}
	if err := e.sae.hooks.takeErr(); err != nil {
		e.wrapDB.setFrame(nil)
		return err
	}

	// Firewood's proposal height, exactly as on the coreth path (one
	// proposal per block: SAE runs at chain pace, and CommitEvery batching
	// stays below the boundary).
	root, err := res.StateDB.Commit(
		e.fwHeight+1, true,
		stateconf.WithTrieDBUpdateOpts(stateconf.WithTrieDBUpdatePayload(header.ParentHash, blk.Hash())),
	)
	e.wrapDB.setFrame(nil)
	if err != nil {
		return fmt.Errorf("statedb commit: %w", err)
	}

	if err := e.checkSAEGasUsed(b, header); err != nil {
		return err
	}
	clockBytes := res.FinishBy.Gas.MarshalCanoto()
	if err := e.ring.put(blockNum, root, clockBytes); err != nil {
		return err
	}
	if err := e.checkSettled(blockNum, header); err != nil {
		return err
	}

	// Frames stay per-block post-images, exactly as below the boundary; a
	// block that wrote nothing gets no record, which is the convention the
	// cook/seal and verify paths read.
	if len(frame.buf) > 0 {
		if err := e.cfg.Store.AppendWrites(blockNum, frame.buf); err != nil {
			return err
		}
	}
	if err := e.cfg.Store.AppendHeader(blockNum, headerRLP); err != nil {
		return err
	}
	if rec := encodeLogsFrame(receiptLogs(res.Receipts)); rec != nil {
		if err := e.cfg.Store.AppendLogs(blockNum, rec); err != nil {
			return err
		}
	}
	if rec := storedTailRecord(res.Receipts); rec != nil {
		if err := e.cfg.Store.AppendRcpt(blockNum, rec); err != nil {
			return err
		}
	}
	if err := e.maybeFlush(blockNum); err != nil {
		return err
	}

	if root == e.headRoot {
		// No state change: Firewood rejects an identity proposal and
		// statedb.Commit never proposed one (root == origin), so only the
		// block-hash tracking advances. Same shape as the coreth path's
		// empty-block fast path, but decided AFTER execution, because a
		// post-Helicon header.Root says nothing about this block.
		e.fwBackend.SetHashAndHeight(blk.Hash(), blockNum)
		e.fwHeight = blockNum
	} else {
		if err := e.triedb.Commit(root, false); err != nil {
			return fmt.Errorf("triedb commit: %w", err)
		}
		e.fwHeight++
	}
	e.lastFwHash = blk.Hash()

	if err := b.MarkExecuted(
		e.sae.db, e.sae.xdb, res.FinishBy.Gas, res.FinishBy.Wall,
		res.BaseFee.ToBig(), res.Receipts, root, nil,
	); err != nil {
		return err
	}
	e.sae.parent = b
	e.headRoot, e.headNum = root, blockNum
	return nil
}

// checkSAEGasUsed verifies the one consensus quantity a post-Helicon block
// commits to about ITSELF: header.GasUsed is the worst-case charged gas of
// its own body, i.e. the sum of transaction gas LIMITS plus the gas of the
// end-of-block (atomic) ops. Actual consumption is not in the header at
// all under SAE.
func (e *Executor) checkSAEGasUsed(b *blocks.Block, header *types.Header) error {
	var want uint64
	for _, tx := range b.Transactions() {
		want += tx.Gas()
	}
	ops, err := e.sae.hooks.EndOfBlockOps(b.EthBlock())
	if err != nil {
		return err
	}
	for _, op := range ops {
		want += uint64(op.Gas)
	}
	if want != header.GasUsed {
		return fmt.Errorf("block %d: worst-case charged gas %d != header.GasUsed %d",
			b.NumberU64(), want, header.GasUsed)
	}
	return nil
}

// checkSettled is the ACP-194 replacement for the per-block root check.
// The header of block n names the block it settles; that block's
// post-execution state root MUST be header.Root, and the gas clock after
// executing it MUST be the one the markers carry. Both come from this
// executor's own execution, so together they attest state and fee
// evolution up to the settled height. The marker must also be monotonic
// and strictly behind the block carrying it.
func (e *Executor) checkSettled(n uint64, header *types.Header) error {
	s := settledBy(header)
	if s.Height >= n {
		return fmt.Errorf("block %d settles height %d: a block cannot settle itself or the future", n, s.Height)
	}
	if e.sae.settledSeen && s.Height < e.sae.settledHeight {
		return fmt.Errorf("block %d settles height %d, below the previously settled %d: settlement is monotonic",
			n, s.Height, e.sae.settledHeight)
	}
	if floor, ok := e.cfg.Store.FrontierFloor(); ok && s.Height < floor {
		// A bootstrapped node's state came from a frontier merge at floor,
		// verified against the header that settles it (exec/frontier.go), and
		// it never executed anything below that: there is no root of ours to
		// present here, and the settlement lag means the first few headers
		// after the frontier all name heights inside that gap. Everything from
		// the floor up is attested normally.
		e.sae.settledHeight, e.sae.settledSeen = s.Height, true
		return nil
	}

	hdr, err := e.loadHeader(s.Height)
	if err != nil {
		return fmt.Errorf("block %d settles height %d: %w", n, s.Height, err)
	}
	root, clock, err := e.postExecutionAt(s.Height, hdr, e.sae.hooks)
	if err != nil {
		return err
	}
	if root != header.Root {
		return fmt.Errorf("settled state root mismatch: block %d settles height %d, computed %x, header %x",
			n, s.Height, root, header.Root)
	}
	// The same markers pin the gas clock, which is what the base fee (and
	// therefore execution itself) is derived from. hook.SettledGasTime is
	// upstream's own reconstruction of it.
	want, err := hook.SettledGasTime(e.sae.hooks, hdr, header)
	if err != nil {
		return fmt.Errorf("block %d: settled gas time: %w", n, err)
	}
	if err := e.sae.hooks.takeErr(); err != nil {
		return err
	}
	if got := clock; got.Unix() != want.Unix() ||
		got.Fraction().Numerator != want.Fraction().Numerator ||
		got.Excess() != want.Excess() {
		return fmt.Errorf("settled gas clock mismatch: block %d settles height %d, computed (unix=%d frac=%d excess=%d), header (unix=%d frac=%d excess=%d)",
			n, s.Height, got.Unix(), got.Fraction().Numerator, got.Excess(),
			want.Unix(), want.Fraction().Numerator, want.Excess())
	}

	lag := n - s.Height
	if !e.sae.settledSeen || lag < e.sae.lagMin {
		e.sae.lagMin = lag
	}
	if lag > e.sae.lagMax {
		e.sae.lagMax = lag
	}
	e.sae.settledHeight, e.sae.settledSeen = s.Height, true
	if e.sae.settlements++; e.sae.settlements <= 16 {
		log.Printf("sae: block %d settles %d (lag %d) root=%x", n, s.Height, lag, header.Root)
	}
	return nil
}

// receiptLogs flattens receipt logs in block order, the slice both execution
// paths hand to encodeLogsFrame.
func receiptLogs(receipts types.Receipts) []*types.Log {
	var logs []*types.Log
	for _, r := range receipts {
		logs = append(logs, r.Logs...)
	}
	return logs
}

// storedTailRecord builds the tail receipts+full-logs record from receipts the
// executor already holds, in the EPOCH stored-section encoding. Both execution
// paths call it; sealing copies the result into the epoch's sections byte for
// byte, so no block is ever re-executed to derive them. SAE-proof by
// construction: receipts are per-block deterministic at execution regardless
// of when a header settles them.
func storedTailRecord(receipts types.Receipts) []byte {
	return state.EncodeTailRcpt(state.EncodeStoredLogs(receipts), state.EncodeStoredReceipts(receipts))
}

// --- discard sinks -----------------------------------------------------------

// discardEthDB and discardHeightIndex swallow the persistence that
// blocks.Block.MarkExecuted performs (receipt rows, chain head markers,
// canoto execution results). This executor keeps its own artefacts and
// none of those rows are ever read back here.
//
// The embedded nil interface is deliberate: at v1.15.0-fuji MarkExecuted
// only ever calls ethdb.Database.NewBatch, so any other use panics loudly
// on a dep bump instead of silently dropping data.
type discardEthDB struct{ ethdb.Database }

func (discardEthDB) NewBatch() ethdb.Batch { return discardBatch{} }

type discardBatch struct{ ethdb.Batch }

func (discardBatch) Put([]byte, []byte) error { return nil }
func (discardBatch) Delete([]byte) error      { return nil }
func (discardBatch) ValueSize() int           { return 0 }
func (discardBatch) Write() error             { return nil }
func (discardBatch) Reset()                   {}

type discardHeightIndex struct{}

func (discardHeightIndex) Put(uint64, []byte) error { return nil }
func (discardHeightIndex) Get(uint64) ([]byte, error) {
	return nil, database.ErrNotFound
}
func (discardHeightIndex) Has(uint64) (bool, error)  { return false, nil }
func (discardHeightIndex) Sync(uint64, uint64) error { return nil }
func (discardHeightIndex) Close() error              { return nil }

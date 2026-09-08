package vmexec

import (
	"bytes"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"

	sevmcore "github.com/ava-labs/avalanchego/graft/subnet-evm/core"
	ethstate "github.com/ava-labs/libevm/core/state"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/core/vm"
	"github.com/ava-labs/libevm/rlp"

	"github.com/containerman17/avalanche-s3-archival-node/store"
)

// TestRenderFrames: the checker renders every tx's callTracer JSON into the
// itx row, and the bytes are libevm's own (GetResult on the same tracer).
func TestRenderFrames(t *testing.T) {
	bc := newBenchChain(t, 3)
	e := newBenchExecutor(t, bc)
	_, items := runBlocks(t, e, bc, benchBlocks(t, bc, 2))
	for _, it := range items {
		if len(it.tracers) != 3 {
			t.Fatalf("block %d: %d tracers", it.blk.NumberU64(), len(it.tracers))
		}
		want := make([][]byte, 3)
		for i, tr := range it.tracers {
			res, err := tr.GetResult()
			if err != nil {
				t.Fatal(err)
			}
			want[i] = []byte(res)
		}
		e.renderFrames(it)
		for i, tw := range it.bw.Txs {
			if !bytes.Equal(tw.Frames, want[i]) || !bytes.Contains(tw.Frames, []byte(`"calls":[{`)) {
				t.Fatalf("block %d tx %d: frames %s", it.blk.NumberU64(), i, tw.Frames)
			}
		}
	}
}

// executeBlockWith is executeBlock's body with the EVM loop swapped in.
func executeBlockWith(e *Executor, blk *types.Block, run func(*ethstate.StateDB) (types.Receipts, error)) (*checkItem, error) {
	header := blk.Header()
	parentRoot := e.headRoot
	headerRLP, err := rlp.EncodeToBytes(header)
	if err != nil {
		return nil, err
	}
	bw := &store.BlockWrite{Height: blk.NumberU64(), HeaderRLP: headerRLP, Pvm: []byte{0}, Code: map[string][]byte{}}
	it := &checkItem{blk: blk, bw: bw}
	e.recentHdr[blk.NumberU64()] = header
	cap := &capture{code: map[string][]byte{}}
	e.beginCapture(cap, bw, header, parentRoot)
	defer e.endCapture()
	statedb, err := ethstate.New(parentRoot, e.wrapDB, nil)
	if err != nil {
		return nil, err
	}
	e.curStatedb = statedb
	receipts, err := run(statedb)
	if err != nil {
		return nil, err
	}
	if _, err := statedb.Commit(blk.NumberU64(), e.chainCfg.IsEIP158(header.Number)); err != nil {
		return nil, err
	}
	ws := e.flat.take()
	e.eng.applyOverlay(ws)
	bw.Tail = cap.take()
	bw.Code = cap.code
	it.ws, it.receipts, it.statedb = ws, receipts, statedb
	it.tracers, e.curTracers = e.curTracers, nil
	return it, nil
}

// TestApplyTxParity drives the same blocks through subnet-evm's exported
// ApplyTransaction (what runEVM called before: a new EVM and a header hash
// per tx) and through runEVM's copied applyTx, and compares receipts, state
// rows, frames and write sets byte for byte.
func TestApplyTxParity(t *testing.T) {
	bc := newBenchChain(t, 5)
	blocks := benchBlocks(t, bc, 4)
	ref := newBenchExecutor(t, bc)
	var refItems []*checkItem
	for _, blk := range blocks {
		it, err := executeBlockWith(ref, blk, func(statedb *ethstate.StateDB) (types.Receipts, error) {
			header := blk.Header()
			parentTime := ref.headTime
			if err := sevmcore.ApplyUpgrades(ref.chainCfg, &parentTime, sevmcore.NewBlockContext(header.Number, header.Time), statedb); err != nil {
				return nil, err
			}
			blockCtx := sevmcore.NewEVMBlockContext(header, ref.chainCtx, nil)
			gp := new(sevmcore.GasPool).AddGas(header.GasLimit)
			var usedGas uint64
			var receipts types.Receipts
			for i, tx := range blk.Transactions() {
				statedb.SetTxContext(tx.Hash(), i)
				r, err := sevmcore.ApplyTransaction(ref.chainCfg, ref.chainCtx, blockCtx, gp, statedb, header, tx, &usedGas, vm.Config{Tracer: frameTracer()})
				if err != nil {
					return nil, err
				}
				receipts = append(receipts, r)
				if err := ref.captureTx(i, tx, r); err != nil {
					return nil, err
				}
			}
			return receipts, nil
		})
		if err != nil {
			t.Fatal(err)
		}
		ref.headTime = blk.Time()
		refItems = append(refItems, it)
	}
	e := newBenchExecutor(t, bc)
	_, items := runBlocks(t, e, bc, blocks)
	for b := range blocks {
		a, c := refItems[b], items[b]
		ref.renderFrames(a)
		e.renderFrames(c)
		ra, _ := rlp.EncodeToBytes(a.receipts)
		rc, _ := rlp.EncodeToBytes(c.receipts)
		if !bytes.Equal(ra, rc) {
			t.Fatalf("block %d: receipts differ", b+1)
		}
		for i := range a.receipts {
			x, y := a.receipts[i], c.receipts[i]
			if x.BlockHash != y.BlockHash || x.BlockNumber.Cmp(y.BlockNumber) != 0 || x.TransactionIndex != y.TransactionIndex ||
				x.ContractAddress != y.ContractAddress || x.GasUsed != y.GasUsed || x.TxHash != y.TxHash || len(x.Logs) != len(y.Logs) {
				t.Fatalf("block %d tx %d: receipt fields differ:\n%+v\n%+v", b+1, i, x, y)
			}
			for j := range x.Logs {
				if fmt.Sprint(*x.Logs[j]) != fmt.Sprint(*y.Logs[j]) {
					t.Fatalf("block %d tx %d log %d differ", b+1, i, j)
				}
			}
		}
		// Row and write-set order within a tx follows libevm's map walk of
		// pending objects (random per run); only the last value per key
		// matters, so compare in key order.
		sortRows := func(rows []store.StateRow) {
			slices.SortStableFunc(rows, func(x, y store.StateRow) int {
				return strings.Compare(string(x.Kind)+string(x.Addr)+string(x.Slot), string(y.Kind)+string(y.Addr)+string(y.Slot))
			})
		}
		for _, bw := range []*store.BlockWrite{a.bw, c.bw} {
			for i := range bw.Txs {
				sortRows(bw.Txs[i].State)
			}
			sortRows(bw.Tail)
		}
		if len(a.bw.Txs) != 5 || !reflect.DeepEqual(a.bw.Txs, c.bw.Txs) || !reflect.DeepEqual(a.bw.Tail, c.bw.Tail) {
			for i := range a.bw.Txs {
				if !reflect.DeepEqual(a.bw.Txs[i], c.bw.Txs[i]) {
					t.Fatalf("block %d tx %d rows differ:\n%+v\n%+v", b+1, i, a.bw.Txs[i], c.bw.Txs[i])
				}
			}
			t.Fatalf("block %d: tail differs", b+1)
		}
		for _, ws := range []*writeSet{a.ws, c.ws} {
			slices.SortStableFunc(ws.ops, func(x, y kv) int { return bytes.Compare(x.k, y.k) })
		}
		if len(a.ws.ops) == 0 || !reflect.DeepEqual(a.ws.ops, c.ws.ops) {
			t.Fatalf("block %d: write sets differ", b+1)
		}
	}
}

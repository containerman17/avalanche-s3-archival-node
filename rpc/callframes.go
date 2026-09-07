package rpc

// STORED FRAMES RENDERED AS geth's callTracer. The `itx/` family holds every
// call frame in enter order with depth (DESIGN: traces are stored), and enter
// order plus depth is the pre-order DFS callTracer itself replays, so its JSON
// is a rendering of stored rows, not a re-execution.
//
// THE DOUBLE RENDER IS THE CURRENT MODE (user ruling 2026-09-07): every
// callTracer request still re-executes, the stored frames are rendered beside
// it, and a difference KILLS THE PROCESS with the request and the diff in the
// log. Crashing is for visibility: a stored trace that does not match a fresh
// one is exactly the class of defect no consensus root ever objects to
// (DESIGN, "stored traces are unverified data"), and a per-request error would
// be read by nobody.
//
// KNOWN GAPS OF THE STORED RECORD, expected to trip the comparator until the
// format grows: a frame carries `failed`, not the error string (callTracer
// prints "out of gas", "invalid opcode: ..."), and the top-level frame is not
// stored at all, so its return data is unknown. Both are named in the crash
// message when they are the difference.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"math/big"
	"os"
	"reflect"
	"sort"
	"strings"

	"github.com/ava-labs/libevm/accounts/abi"
	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/common/hexutil"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/core/vm"

	"github.com/containerman17/avalanche-s3-archival-node/store"
)

// callNode mirrors native.callFrame's generated JSON FIELD FOR FIELD AND IN
// ORDER (gen_callframe_json.go): the comparison is byte-for-byte, so the
// order, the omitempty set and the hex encodings are the contract.
type callNode struct {
	From         common.Address  `json:"from"`
	Gas          hexutil.Uint64  `json:"gas"`
	GasUsed      hexutil.Uint64  `json:"gasUsed"`
	To           *common.Address `json:"to,omitempty"`
	Input        hexutil.Bytes   `json:"input"`
	Output       hexutil.Bytes   `json:"output,omitempty"`
	Error        string          `json:"error,omitempty"`
	RevertReason string          `json:"revertReason,omitempty"`
	Calls        []*callNode     `json:"calls,omitempty"`
	Value        *hexutil.Big    `json:"value,omitempty"`
	Type         string          `json:"type"`
}

// traceMode is EPOCHDB_TRACE_MODE: "check" (default) re-executes AND renders
// the stored frames and dies on a difference; "stored" answers from the
// frames alone, no re-execution (the measurement the format decision waits
// on); "reexec" is the old path, no stored render at all.
var traceMode = func() string {
	switch m := os.Getenv("EPOCHDB_TRACE_MODE"); m {
	case "", "check":
		return "check"
	case "stored", "reexec":
		return m
	default:
		log.Fatalf("epochdb: EPOCHDB_TRACE_MODE=%q: want check, stored or reexec", m)
		return ""
	}
}()

// storedCallTraces renders the callTracer answer for tx target of blk, or
// every tx when target < 0, from stored rows alone.
func (s *Server) storedCallTraces(blk *types.Block, target int) ([]json.RawMessage, *rpcError) {
	rcpts, rerr := s.storedBlockReceipts(blk)
	if rerr != nil {
		return nil, rerr
	}
	var out []json.RawMessage
	for i := range blk.Transactions() {
		if target >= 0 && i != target {
			continue
		}
		res, err := s.StoredCallTrace(blk, i, rcpts[i])
		if err != nil {
			return nil, &rpcError{Code: -32000, Message: err.Error()}
		}
		out = append(out, res)
	}
	return out, nil
}

// isPlainCallTracer says the request is the callTracer with no option that
// the stored frames cannot answer (onlyTopCall drops subcalls, withLog
// interleaves logs by position).
func isPlainCallTracer(cfg *traceConfig) bool {
	if cfg == nil || cfg.Tracer == nil || *cfg.Tracer != "callTracer" {
		return false
	}
	if len(cfg.TracerConfig) == 0 || string(cfg.TracerConfig) == "null" {
		return true
	}
	var opts struct {
		OnlyTopCall bool `json:"onlyTopCall"`
		WithLog     bool `json:"withLog"`
	}
	if err := json.Unmarshal(cfg.TracerConfig, &opts); err != nil {
		return false
	}
	return !opts.OnlyTopCall && !opts.WithLog
}

// StoredCallTrace renders transaction i of blk from its stored frames as
// callTracer JSON. Exported for the parity sweep (callframes_test at the root).
func (s *Server) StoredCallTrace(blk *types.Block, i int, rcpt *types.Receipt) (json.RawMessage, error) {
	tx := blk.Transactions()[i]
	signer := types.MakeSigner(s.chainCfg, blk.Number(), blk.Time())
	from, err := types.Sender(signer, tx)
	if err != nil {
		return nil, fmt.Errorf("sender of %s: %v", tx.Hash(), err)
	}
	first, _, ok, err := s.db.BlockTxRange(blk.NumberU64())
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("block %d has no blk row", blk.NumberU64())
	}
	rec, ok, err := s.db.Frames(first + uint64(i))
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("tx %s has no stored frames", tx.Hash())
	}
	frames, err := store.DecodeFrames(rec)
	if err != nil {
		return nil, err
	}

	// The top-level frame IS the transaction: callTracer's CaptureStart/End.
	top := &callNode{
		Type:    "CALL",
		From:    from,
		Value:   (*hexutil.Big)(tx.Value()),
		Gas:     hexutil.Uint64(tx.Gas()),
		GasUsed: hexutil.Uint64(rcpt.GasUsed),
		Input:   tx.Data(),
	}
	if tx.To() == nil {
		top.Type = "CREATE"
		if rcpt.Status == types.ReceiptStatusSuccessful {
			created := rcpt.ContractAddress
			top.To = &created
		}
	} else {
		to := *tx.To()
		top.To = &to
	}
	if rcpt.Status != types.ReceiptStatusSuccessful {
		// The record holds no top-level output or error: "execution
		// reverted" is the common case, and the comparator names the rest.
		top.Error = vm.ErrExecutionReverted.Error()
	}

	// Enter order plus depth is a pre-order DFS: stack[d] is the open frame
	// at depth d, the top-level frame being depth 0's parent.
	stack := []*callNode{top}
	for k, f := range frames {
		d := int(f.Depth) + 1
		if d > len(stack) {
			return nil, fmt.Errorf("tx %s frame %d: depth %d with %d open frames", tx.Hash(), k, f.Depth, len(stack)-1)
		}
		stack = stack[:d]
		n := &callNode{
			Type:    vm.OpCode(f.Kind).String(),
			From:    f.From,
			Gas:     hexutil.Uint64(f.Gas),
			GasUsed: hexutil.Uint64(f.GasUsed),
			Input:   f.Input,
		}
		to := f.To
		n.To = &to
		// libevm hands the tracer a value for every kind but STATICCALL
		// (evm.go: DELEGATECALL carries the parent's), and a zero one is
		// printed as 0x0, so absence is the kind, not the amount.
		if vm.OpCode(f.Kind) != vm.STATICCALL {
			v := f.Value
			if v == nil {
				v = new(big.Int)
			}
			n.Value = (*hexutil.Big)(v)
		}
		if !f.Failed {
			n.Output = f.Output
		} else {
			// callTracer: a failed CREATE has no `to`; output is kept only
			// on a revert, and a revert is the one failure that returns
			// data, so data present means reverted here.
			if vm.OpCode(f.Kind) == vm.CREATE || vm.OpCode(f.Kind) == vm.CREATE2 {
				n.To = nil
			}
			if len(f.Output) > 0 {
				n.Error = vm.ErrExecutionReverted.Error()
				n.Output = f.Output
				if len(f.Output) >= 4 {
					if reason, err := abi.UnpackRevert(f.Output); err == nil {
						n.RevertReason = reason
					}
				}
			} else {
				n.Error = "<stored frame failed: error string is not recorded>"
			}
		}
		stack[d-1].Calls = append(stack[d-1].Calls, n)
		stack = append(stack, n)
	}
	return json.Marshal(top)
}

// assertStoredTraceMatches renders the stored frames of every traced
// transaction and dies on the first difference. fresh is traceTxsInBlock's
// output: one entry per traced tx, the last one being tx target when target
// is set, else block order.
func (s *Server) assertStoredTraceMatches(method string, params []json.RawMessage, blk *types.Block, target int, fresh []json.RawMessage) {
	rcpts, rerr := s.storedBlockReceipts(blk)
	if rerr != nil {
		log.Fatalf("epochdb: stored-trace check: %s %s: block %d receipts: %s", method, params, blk.NumberU64(), rerr.Message)
	}
	for k, got := range fresh {
		i := k
		if target >= 0 {
			i = target
		}
		stored, err := s.StoredCallTrace(blk, i, rcpts[i])
		if err != nil {
			log.Fatalf("epochdb: stored-trace check: %s %s: block %d tx %d (%s): render: %v",
				method, params, blk.NumberU64(), i, blk.Transactions()[i].Hash(), err)
		}
		// BYTE FOR BYTE (user ruling 2026-09-07): field order, omitted
		// against empty, hex widths, all of it. The structural diff is only
		// the hint that says where.
		if !bytes.Equal(got, stored) {
			diff := JSONDiff(got, stored)
			if diff == "" {
				diff = "same structure, different bytes (field order or encoding)"
			}
			log.Fatalf("epochdb: STORED TRACE DIFFERS FROM RE-EXECUTION (rpc/callframes.go)\n"+
				"request: %s %s\nblock %d tx %d hash %s\n%s\nre-executed: %s\nstored:      %s",
				method, params, blk.NumberU64(), i, blk.Transactions()[i].Hash(), diff, got, stored)
		}
	}
}

// JSONDiff compares two JSON documents structurally and names every leaf
// path that differs, "" when equal.
func JSONDiff(a, b json.RawMessage) string {
	var x, y any
	if err := json.Unmarshal(a, &x); err != nil {
		return "left is not JSON: " + err.Error()
	}
	if err := json.Unmarshal(b, &y); err != nil {
		return "right is not JSON: " + err.Error()
	}
	var out []string
	diffValues("$", x, y, &out)
	return strings.Join(out, "\n")
}

func diffValues(path string, x, y any, out *[]string) {
	if len(*out) >= 50 {
		return
	}
	switch xv := x.(type) {
	case map[string]any:
		yv, ok := y.(map[string]any)
		if !ok {
			*out = append(*out, fmt.Sprintf("%s: object vs %T", path, y))
			return
		}
		keys := map[string]bool{}
		for k := range xv {
			keys[k] = true
		}
		for k := range yv {
			keys[k] = true
		}
		sorted := make([]string, 0, len(keys))
		for k := range keys {
			sorted = append(sorted, k)
		}
		sort.Strings(sorted)
		for _, k := range sorted {
			xk, xok := xv[k]
			yk, yok := yv[k]
			switch {
			case !xok:
				*out = append(*out, fmt.Sprintf("%s.%s: missing in re-execution, stored has %s", path, k, short(yk)))
			case !yok:
				*out = append(*out, fmt.Sprintf("%s.%s: re-execution has %s, missing in stored", path, k, short(xk)))
			default:
				diffValues(path+"."+k, xk, yk, out)
			}
		}
	case []any:
		yv, ok := y.([]any)
		if !ok {
			*out = append(*out, fmt.Sprintf("%s: array vs %T", path, y))
			return
		}
		if len(xv) != len(yv) {
			*out = append(*out, fmt.Sprintf("%s: %d entries re-executed, %d stored", path, len(xv), len(yv)))
		}
		for i := 0; i < len(xv) && i < len(yv); i++ {
			diffValues(fmt.Sprintf("%s[%d]", path, i), xv[i], yv[i], out)
		}
	default:
		if !reflect.DeepEqual(x, y) {
			*out = append(*out, fmt.Sprintf("%s: re-executed %s, stored %s", path, short(x), short(y)))
		}
	}
}

func short(v any) string {
	b, _ := json.Marshal(v)
	if len(b) > 120 {
		return string(b[:117]) + "..."
	}
	return string(bytes.TrimSpace(b))
}

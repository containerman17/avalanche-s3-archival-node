package rpc

// STORED callTracer ANSWERS. The `itx/` row IS libevm's callTracer JSON,
// captured at execution (exec/frames.go, storage v4), so a plain callTracer
// request is a copy of stored bytes and nothing of ours stands between the
// tracer and the wire.
//
// THE DOUBLE RENDER IS THE CURRENT MODE (user ruling 2026-09-07): every
// callTracer request still re-executes, the stored frames are rendered beside
// it, and a difference KILLS THE PROCESS with the request and the diff in the
// log. Crashing is for visibility: a stored trace that does not match a fresh
// one is exactly the class of defect no consensus root ever objects to
// (DESIGN, "stored traces are unverified data"), and a per-request error would
// be read by nobody.
//
// A DIFFERENCE HERE MEANS THE PINNED libevm NO LONGER PRODUCES THE BYTES IT
// PRODUCED AT CAPTURE (or execution diverged, which the state root would have
// caught first): the crash message carries both documents and the request.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"reflect"
	"sort"
	"strings"

	"github.com/ava-labs/libevm/core/types"
)

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
	var out []json.RawMessage
	for i := range blk.Transactions() {
		if target >= 0 && i != target {
			continue
		}
		res, err := s.StoredCallTrace(blk, i)
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

// StoredCallTrace is transaction i of blk's stored callTracer answer, the
// bytes libevm's tracer produced at execution (storage v4). Exported for the
// parity sweep (callframes_test at the root).
func (s *Server) StoredCallTrace(blk *types.Block, i int) (json.RawMessage, error) {
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
		return nil, fmt.Errorf("tx %s has no stored frames", blk.Transactions()[i].Hash())
	}
	return json.RawMessage(rec), nil
}

// assertStoredTraceMatches renders the stored frames of every traced
// transaction and dies on the first difference. fresh is traceTxsInBlock's
// output: one entry per traced tx, the last one being tx target when target
// is set, else block order.
func (s *Server) assertStoredTraceMatches(method string, params []json.RawMessage, blk *types.Block, target int, fresh []json.RawMessage) {
	for k, got := range fresh {
		i := k
		if target >= 0 {
			i = target
		}
		stored, err := s.StoredCallTrace(blk, i)
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

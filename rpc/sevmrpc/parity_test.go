package sevmrpc

// The parity gap the 2026-08-01 audit closed, tested on the same real
// subnet-evm chain as sevmrpc_test.go: eth_call's override objects (parsed
// NOWHERE before, so they were silently ignored), geth's BlockNumberOrHash
// object form, and the methods that were missing or unnamed in the dispatch
// table. Everything here goes through the real HTTP handler.

import (
	"encoding/json"
	"math/big"
	"strings"
	"testing"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/common/hexutil"
)

// overrideTarget is an address with nothing at it: every code override below
// installs its body there, so no real account is disturbed.
var overrideTarget = common.HexToAddress("0x000000000000000000000000000000000000dead")

// Two 9-byte contracts, hand-written for the same reason storageContract is:
//
//	returnSelfBalance: SELFBALANCE; PUSH1 0; MSTORE; PUSH1 32; PUSH1 0; RETURN
//	returnNumber:      NUMBER;      PUSH1 0; MSTORE; PUSH1 32; PUSH1 0; RETURN
const (
	returnSelfBalance = "0x4760005260206000f3"
	returnNumber      = "0x4360005260206000f3"
)

func callWord(t *testing.T, e *env, raw json.RawMessage) *big.Int {
	t.Helper()
	b, err := hexutil.Decode(decodeString(t, raw))
	if err != nil {
		t.Fatalf("decode call result: %v", err)
	}
	return new(big.Int).SetBytes(b)
}

// TestStateOverrideChangesTheAnswer: a stateDiff on the deployed contract's
// slot 0 must change what eth_call returns, and must NOT persist into the
// next call. Before the override objects were parsed, the first assertion
// failed by returning the real stored value, which is the worst possible
// failure: it looks like a successful answer.
func TestStateOverrideChangesTheAnswer(t *testing.T) {
	e := newEnv(t)
	args := map[string]any{"from": fundedAddr(), "to": e.contract, "data": "0x"}

	if got := callWord(t, e, e.mustCall(t, "eth_call", args, "latest")); got.Uint64() != storedValue {
		t.Fatalf("plain call returned %d, want %d", got, storedValue)
	}

	const want = 0x99
	res := e.mustCall(t, "eth_call", args, "latest", map[string]any{
		e.contract.Hex(): map[string]any{
			"stateDiff": map[string]any{
				common.Hash{}.Hex(): common.BigToHash(big.NewInt(want)).Hex(),
			},
		},
	})
	if got := callWord(t, e, res); got.Uint64() != want {
		t.Fatalf("overridden call returned %d, want %d", got, want)
	}

	if got := callWord(t, e, e.mustCall(t, "eth_call", args, "latest")); got.Uint64() != storedValue {
		t.Fatalf("override leaked into the next call: got %d, want %d", got, storedValue)
	}
}

// TestCodeAndBalanceOverride installs a body at an empty address and funds it
// through the override alone, then has that body report its own balance.
func TestCodeAndBalanceOverride(t *testing.T) {
	e := newEnv(t)
	args := map[string]any{"from": fundedAddr(), "to": overrideTarget, "data": "0x"}

	// With no override the address is empty: a call to it returns nothing.
	if got := decodeString(t, e.mustCall(t, "eth_call", args, "latest")); got != "0x" {
		t.Fatalf("empty address returned %q", got)
	}

	const balance = 0x123456
	res := e.mustCall(t, "eth_call", args, "latest", map[string]any{
		overrideTarget.Hex(): map[string]any{
			"code":    returnSelfBalance,
			"balance": hexutil.EncodeUint64(balance),
		},
	})
	if got := callWord(t, e, res); got.Uint64() != balance {
		t.Fatalf("SELFBALANCE under override = %d, want %d", got, balance)
	}
}

// TestBlockOverride: the block context a call runs in is overridable, proven
// with a body that returns NUMBER.
func TestBlockOverride(t *testing.T) {
	e := newEnv(t)
	args := map[string]any{"from": fundedAddr(), "to": overrideTarget, "data": "0x"}
	stateOv := map[string]any{overrideTarget.Hex(): map[string]any{"code": returnNumber}}

	got := callWord(t, e, e.mustCall(t, "eth_call", args, "latest", stateOv))
	if got.Uint64() != headBlock {
		t.Fatalf("NUMBER without override = %d, want %d", got, headBlock)
	}

	const pretend = 0xbeef
	got = callWord(t, e, e.mustCall(t, "eth_call", args, "latest", stateOv,
		map[string]any{"number": hexutil.EncodeUint64(pretend)}))
	if got.Uint64() != pretend {
		t.Fatalf("NUMBER with block override = %d, want %d", got, pretend)
	}
}

// TestEstimateGasAndTraceCallHonorOverrides: the same override objects reach
// the other two executing methods (estimateGas's third param, traceCall's
// config fields). Estimating against an empty address is the cheapest
// observable difference: with a body installed the estimate must exceed the
// bare 21000 of a call to nothing.
func TestEstimateGasAndTraceCallHonorOverrides(t *testing.T) {
	e := newEnv(t)
	args := map[string]any{"from": fundedAddr(), "to": overrideTarget, "data": "0x"}
	stateOv := map[string]any{overrideTarget.Hex(): map[string]any{"code": returnSelfBalance}}

	bare := decodeUint(t, e.mustCall(t, "eth_estimateGas", args, "latest"))
	withCode := decodeUint(t, e.mustCall(t, "eth_estimateGas", args, "latest", stateOv))
	if withCode <= bare {
		t.Fatalf("estimateGas ignored the code override: %d with, %d without", withCode, bare)
	}

	// debug_traceCall carries its overrides inside the trace config.
	raw := e.mustCall(t, "debug_traceCall", args, "latest", map[string]any{
		"tracer":         "callTracer",
		"stateOverrides": stateOv,
	})
	var trace struct {
		Output string `json:"output"`
	}
	if err := json.Unmarshal(raw, &trace); err != nil {
		t.Fatal(err)
	}
	if trace.Output == "" || trace.Output == "0x" {
		t.Fatalf("traceCall ignored the code override: output %q", trace.Output)
	}
}

// TestBlockNumberOrHashObject: every blockNrOrHash position accepts geth's
// object form, and it resolves to the same answer as the plain tag.
func TestBlockNumberOrHashObject(t *testing.T) {
	e := newEnv(t)
	blk := e.block(blkDeploy)
	want := decodeString(t, e.mustCall(t, "eth_getCode", e.contract, numTag(headBlock)))

	for _, tag := range []any{
		map[string]any{"blockNumber": numTag(headBlock)},
		map[string]any{"blockNumber": "latest"},
		map[string]any{"blockHash": e.block(headBlock).Hash()},
		map[string]any{"blockHash": e.block(headBlock).Hash(), "requireCanonical": true},
	} {
		if got := decodeString(t, e.mustCall(t, "eth_getCode", e.contract, tag)); got != want {
			t.Fatalf("eth_getCode at %v: got %d bytes of code, want the same as the plain tag", tag, len(got))
		}
	}

	// A historical hash resolves to that height: the contract does not exist
	// one block before its own deployment.
	before := map[string]any{"blockHash": e.block(blkDeploy - 1).Hash()}
	if got := decodeString(t, e.mustCall(t, "eth_getCode", e.contract, before)); got != "0x" {
		t.Fatalf("code before deployment: %q", got)
	}
	// And eth_call accepts it too.
	if _, rerr := e.call(t, "eth_call",
		map[string]any{"from": fundedAddr(), "to": e.contract, "data": "0x"},
		map[string]any{"blockHash": blk.Hash()}); rerr != nil {
		t.Fatalf("eth_call at a block-hash tag: %v", rerr)
	}
	// An unknown hash is a named error, not a silent latest.
	if _, rerr := e.call(t, "eth_getCode", e.contract,
		map[string]any{"blockHash": common.HexToHash("0xdead")}); rerr == nil {
		t.Fatal("unknown block hash was accepted")
	}
	// So is an object with neither field.
	if _, rerr := e.call(t, "eth_getCode", e.contract, map[string]any{}); rerr == nil {
		t.Fatal("empty block tag object was accepted")
	}
}

// TestCallDetailedShape: coreth's eth_callDetailed returns a failed execution
// as a RESULT, where eth_call returns a JSON-RPC error.
func TestCallDetailedShape(t *testing.T) {
	e := newEnv(t)
	raw := e.mustCall(t, "eth_callDetailed", map[string]any{
		"from": fundedAddr(), "to": e.contract, "data": "0x",
	}, "latest")
	var out struct {
		Gas        uint64 `json:"gas"`
		ErrCode    int    `json:"errCode"`
		Err        string `json:"err"`
		ReturnData string `json:"returnData"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatal(err)
	}
	if out.Err != "" || out.ErrCode != 0 {
		t.Fatalf("successful call reported err %q code %d", out.Err, out.ErrCode)
	}
	if out.Gas == 0 {
		t.Fatal("callDetailed reported zero gas")
	}
	got, err := hexutil.Decode(out.ReturnData)
	if err != nil {
		t.Fatal(err)
	}
	if new(big.Int).SetBytes(got).Uint64() != storedValue {
		t.Fatalf("returnData = %x, want the stored word", got)
	}
}

// TestNewlyServedMethods: the small methods the audit found missing all
// answer on a real chain.
func TestNewlyServedMethods(t *testing.T) {
	e := newEnv(t)

	// eth_getChainConfig / debug_chainConfig marshal through libevm, so the
	// subnet-evm extras payload comes out of the VM's own marshaller.
	for _, m := range []string{"eth_getChainConfig", "debug_chainConfig"} {
		var cfg struct {
			ChainID *big.Int `json:"chainId"`
		}
		if err := json.Unmarshal(e.mustCall(t, m), &cfg); err != nil {
			t.Fatalf("%s: %v", m, err)
		}
		if cfg.ChainID == nil || cfg.ChainID.Uint64() != testChainID {
			t.Fatalf("%s: chainId %v, want %d", m, cfg.ChainID, testChainID)
		}
	}

	dump := decodeString(t, e.mustCall(t, "debug_printBlock", blkCall))
	if !strings.Contains(dump, "Transaction") {
		t.Fatalf("debug_printBlock dump does not mention the block's transactions:\n%s", dump)
	}

	if got := decodeUint(t, e.mustCall(t, "debug_getAccessibleState", 1, headBlock)); got != 1 {
		t.Fatalf("debug_getAccessibleState(1, head) = %d, want 1", got)
	}
	// Above the executed head there is no state, and that is an error.
	if _, rerr := e.call(t, "debug_getAccessibleState", headBlock+10, headBlock+20); rerr == nil {
		t.Fatal("getAccessibleState answered above the head")
	}

	var opts struct {
		Slow, Normal, Fast *struct {
			Tip string `json:"maxPriorityFeePerGas"`
			Fee string `json:"maxFeePerGas"`
		}
	}
	if err := json.Unmarshal(e.mustCall(t, "eth_suggestPriceOptions"), &opts); err != nil {
		t.Fatal(err)
	}
	if opts.Slow == nil || opts.Normal == nil || opts.Fast == nil {
		t.Fatalf("suggestPriceOptions returned a partial answer: %+v", opts)
	}
	slow, normal, fast := decodeBigStr(t, opts.Slow.Tip), decodeBigStr(t, opts.Normal.Tip), decodeBigStr(t, opts.Fast.Tip)
	if slow.Cmp(normal) > 0 || normal.Cmp(fast) > 0 {
		t.Fatalf("price options out of order: slow %v, normal %v, fast %v", slow, normal, fast)
	}
	if decodeBigStr(t, opts.Normal.Fee).Cmp(normal) <= 0 {
		t.Fatal("maxFeePerGas does not exceed the tip")
	}
}

func decodeBigStr(t *testing.T, s string) *big.Int {
	t.Helper()
	b, err := hexutil.DecodeBig(s)
	if err != nil {
		t.Fatalf("decode %q: %v", s, err)
	}
	return b
}

// TestRefusalsAreNamed: everything an archival node cannot serve refuses with
// a message that says WHY, and nothing falls through to a bare "method not
// found" that leaves the caller guessing whether the method exists at all.
func TestRefusalsAreNamed(t *testing.T) {
	e := newEnv(t)
	for _, tc := range []struct {
		method string
		params []any
		want   string // a phrase the refusal must contain
	}{
		{"eth_sign", []any{fundedAddr(), "0x00"}, "keystore"},
		{"eth_signTransaction", []any{map[string]any{}}, "keystore"},
		{"personal_newAccount", []any{"pw"}, "archive node"},
		{"miner_start", nil, "archive node"},
		{"admin_peers", nil, "archive node"},
		{"debug_intermediateRoots", []any{numTag(blkCall)}, "no tries"},
		{"debug_preimage", []any{common.Hash{}}, "preimage"},
		{"debug_traceBadBlock", []any{common.Hash{}}, "bad blocks"},
		{"debug_traceChain", []any{"0x1", "0x2"}, "traceBlockByNumber"},
		{"eth_getProof", []any{e.contract, []any{}, "latest"}, "no tries"},
		{"eth_sendRawTransaction", []any{"0x00"}, "read-only archive node"},
	} {
		_, rerr := e.call(t, tc.method, tc.params...)
		if rerr == nil {
			t.Errorf("%s: answered instead of refusing", tc.method)
			continue
		}
		if !strings.Contains(rerr.Message, tc.want) {
			t.Errorf("%s: refusal %q does not explain %q", tc.method, rerr.Message, tc.want)
		}
	}
	// A genuinely unknown method is still the plain -32601.
	if _, rerr := e.call(t, "eth_notAMethod"); rerr == nil || rerr.Code != -32601 ||
		!strings.Contains(rerr.Message, "method not found") {
		t.Fatalf("unknown method: %v", rerr)
	}
}

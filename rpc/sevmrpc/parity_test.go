package sevmrpc

// The parity gap the 2026-08-01 audit closed, tested on the same real
// subnet-evm chain as sevmrpc_test.go: eth_call's override objects (parsed
// NOWHERE before, so they were silently ignored), geth's BlockNumberOrHash
// object form, and the methods that were missing or unnamed in the dispatch
// table. Everything here goes through the real HTTP handler.

import (
	"encoding/json"
	"math/big"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/common/hexutil"

	"github.com/containerman17/epochdb/rpc"
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
	// The three are ordered, with coreth's one exception reproduced verbatim:
	// slow is floored at 1 wei while normal is not, so on a chain whose
	// transactions all bid a ZERO TIP (this one) slow is 1 and normal is 0.
	// That ordering only looked total while the "tip" was the whole sampled
	// gas price, which is the defect this chain now pins.
	if normal.Cmp(fast) > 0 || (slow.Cmp(normal) > 0 && slow.Cmp(big.NewInt(1)) != 0) {
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

// --- JSON-RPC batching, on the same real chain ------------------------------

// postRaw pushes one VERBATIM body through the real handler. e.call speaks
// single requests only, and every batch edge case here is about bodies a
// well-formed marshaller would never produce.
func postRaw(t *testing.T, e *env, body string) (int, []byte) {
	t.Helper()
	rec := httptest.NewRecorder()
	e.srv.ServeHTTP(rec, httptest.NewRequest("POST", "/", strings.NewReader(body)))
	return rec.Code, rec.Body.Bytes()
}

// batchReply is one element of a batch response: the id stays raw, because a
// client is entitled to use a string, a number or null and gets its own back.
type batchReply struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  json.RawMessage `json:"result"`
	Error   *jsonRPCError   `json:"error"`
}

func postBatch(t *testing.T, e *env, body string) []batchReply {
	t.Helper()
	code, raw := postRaw(t, e, body)
	if code != 200 {
		t.Fatalf("batch status %d, body %q", code, raw)
	}
	var out []batchReply
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("decode batch reply %q: %v", raw, err)
	}
	return out
}

// TestBatchAnswersEveryElementByID: N requests in, N responses out, each
// carrying its own id, and one element's failure is its own.
func TestBatchAnswersEveryElementByID(t *testing.T) {
	e := newEnv(t)

	replies := postBatch(t, e, `[
		{"jsonrpc":"2.0","id":1,"method":"eth_chainId"},
		{"jsonrpc":"2.0","id":"two","method":"eth_blockNumber"},
		{"jsonrpc":"2.0","id":3,"method":"eth_notAMethod"},
		{"jsonrpc":"2.0","id":4,"method":"eth_getBlockByNumber","params":["0x4",false]}
	]`)
	if len(replies) != 4 {
		t.Fatalf("got %d replies for 4 requests", len(replies))
	}
	byID := map[string]batchReply{}
	for _, r := range replies {
		if r.JSONRPC != "2.0" {
			t.Fatalf("reply %s: jsonrpc %q", r.ID, r.JSONRPC)
		}
		byID[string(r.ID)] = r
	}
	if len(byID) != 4 {
		t.Fatalf("ids collided: %v", byID)
	}
	if got := decodeUint(t, byID[`1`].Result); got != testChainID {
		t.Fatalf("batched eth_chainId = %d, want %d", got, testChainID)
	}
	if got := decodeUint(t, byID[`"two"`].Result); got != headBlock {
		t.Fatalf("batched eth_blockNumber = %d, want %d", got, headBlock)
	}
	// The failing element fails ALONE: an error object for it, answers for the
	// rest, and the batch itself is a 200 with four entries.
	if rerr := byID[`3`].Error; rerr == nil || rerr.Code != -32601 {
		t.Fatalf("unknown method in a batch: %v", rerr)
	}
	if byID[`3`].Result != nil {
		t.Fatalf("errored element also carried a result: %s", byID[`3`].Result)
	}
	var blk struct {
		Hash common.Hash `json:"hash"`
	}
	if err := json.Unmarshal(byID[`4`].Result, &blk); err != nil {
		t.Fatal(err)
	}
	if blk.Hash != e.block(blkCall).Hash() {
		t.Fatalf("batched getBlockByNumber hash %s, want %s", blk.Hash, e.block(blkCall).Hash())
	}
}

// TestBatchNotifications: an element with no id produces no response entry,
// and a batch of nothing but notifications produces no body at all.
func TestBatchNotifications(t *testing.T) {
	e := newEnv(t)

	replies := postBatch(t, e, `[
		{"jsonrpc":"2.0","method":"eth_chainId"},
		{"jsonrpc":"2.0","id":7,"method":"eth_blockNumber"},
		{"jsonrpc":"2.0","method":"eth_notAMethod"}
	]`)
	// The failing notification is silent too: a notification has no id to
	// answer to, so there is nowhere to put its error.
	if len(replies) != 1 || string(replies[0].ID) != "7" {
		t.Fatalf("notifications answered: %+v", replies)
	}

	// An explicit null id is a REQUEST, not a notification, and is answered.
	replies = postBatch(t, e, `[{"jsonrpc":"2.0","id":null,"method":"eth_chainId"}]`)
	if len(replies) != 1 || string(replies[0].ID) != "null" {
		t.Fatalf("explicit null id: %+v", replies)
	}

	// Nothing but notifications: an empty 200, no JSON at all.
	code, raw := postRaw(t, e, `[
		{"jsonrpc":"2.0","method":"eth_chainId"},
		{"jsonrpc":"2.0","method":"eth_blockNumber"}
	]`)
	if code != 200 || len(raw) != 0 {
		t.Fatalf("all-notification batch: status %d, body %q", code, raw)
	}
}

// TestBatchMalformedElements: an element that is not a request object errors
// on its own, and the whole-batch failures (empty array, over the caps) are
// single named errors rather than a truncated answer.
func TestBatchMalformedElements(t *testing.T) {
	e := newEnv(t)

	replies := postBatch(t, e, `[
		{"jsonrpc":"2.0","id":1,"method":"eth_chainId"},
		42,
		"not a request",
		{"jsonrpc":"2.0","id":9}
	]`)
	if len(replies) != 4 {
		t.Fatalf("got %d replies, want one per element: %+v", len(replies), replies)
	}
	if replies[0].Error != nil {
		t.Fatalf("the good element failed with the bad ones: %v", replies[0].Error)
	}
	for _, r := range replies[1:] {
		if r.Error == nil || r.Error.Code != -32600 {
			t.Fatalf("malformed element answered %+v", r)
		}
	}

	// An empty array is an invalid request, not an empty answer.
	code, raw := postRaw(t, e, `[]`)
	var single batchReply
	if err := json.Unmarshal(raw, &single); err != nil {
		t.Fatalf("empty batch: status %d, body %q: %v", code, raw, err)
	}
	if single.Error == nil || single.Error.Code != -32600 {
		t.Fatalf("empty batch answered %+v", single)
	}

	// Over the batch cap: refused by name, NOT truncated to the cap.
	elems := make([]string, rpc.MaxBatchSize+1)
	for i := range elems {
		elems[i] = `{"jsonrpc":"2.0","id":1,"method":"eth_chainId"}`
	}
	_, raw = postRaw(t, e, "["+strings.Join(elems, ",")+"]")
	single = batchReply{}
	if err := json.Unmarshal(raw, &single); err != nil {
		t.Fatalf("oversized batch returned a list, not a refusal: %q", raw[:min(len(raw), 200)])
	}
	if single.Error == nil || single.Error.Code != -32600 ||
		!strings.Contains(single.Error.Message, "exceeds the limit") {
		t.Fatalf("oversized batch answered %+v", single)
	}

	// Over the body cap: refused before anything is parsed.
	_, raw = postRaw(t, e, `{"jsonrpc":"2.0","id":1,"method":"eth_chainId","params":["`+
		strings.Repeat("a", rpc.MaxRequestBytes)+`"]}`)
	single = batchReply{}
	if err := json.Unmarshal(raw, &single); err != nil {
		t.Fatal(err)
	}
	if single.Error == nil || !strings.Contains(single.Error.Message, "request body exceeds") {
		t.Fatalf("oversized body answered %+v", single)
	}
}

// --- eth_getLogs blockHash form ---------------------------------------------

// TestGetLogsByBlockHash: the filter form that names a block instead of a
// range. It must return exactly that block's matching logs, refuse an unknown
// hash by name instead of answering [], and refuse the range bounds beside it.
func TestGetLogsByBlockHash(t *testing.T) {
	e := newEnv(t)
	type rpcLog struct {
		Address     common.Address `json:"address"`
		Topics      []common.Hash  `json:"topics"`
		BlockNumber string         `json:"blockNumber"`
		BlockHash   common.Hash    `json:"blockHash"`
	}
	get := func(filter map[string]any) []rpcLog {
		t.Helper()
		var logs []rpcLog
		if err := json.Unmarshal(e.mustCall(t, "eth_getLogs", filter), &logs); err != nil {
			t.Fatal(err)
		}
		return logs
	}

	callHash := e.block(blkCall).Hash()
	logs := get(map[string]any{"blockHash": callHash})
	if len(logs) != 1 {
		t.Fatalf("got %d logs for the block that emitted one", len(logs))
	}
	switch l := logs[0]; {
	case l.BlockHash != callHash:
		t.Fatalf("log blockHash %s, want %s", l.BlockHash, callHash)
	case l.BlockNumber != numTag(blkCall):
		t.Fatalf("log blockNumber %s, want %s", l.BlockNumber, numTag(blkCall))
	case l.Address != e.contract:
		t.Fatalf("log address %s, want %s", l.Address, e.contract)
	case len(l.Topics) != 1 || l.Topics[0] != logTopic:
		t.Fatalf("log topics %v", l.Topics)
	}

	// The address and topic filters apply exactly as on the range form.
	if got := get(map[string]any{"blockHash": callHash, "address": e.contract,
		"topics": []any{logTopic}}); len(got) != 1 {
		t.Fatalf("matching address+topic returned %d logs", len(got))
	}
	if got := get(map[string]any{"blockHash": callHash,
		"topics": []any{common.HexToHash("0xfeed")}}); len(got) != 0 {
		t.Fatalf("non-matching topic returned %d logs", len(got))
	}
	if got := get(map[string]any{"blockHash": callHash,
		"address": overrideTarget}); len(got) != 0 {
		t.Fatalf("non-matching address returned %d logs", len(got))
	}

	// A block with no logs at all is an empty array, which is the truthful
	// answer for a block this node HAS.
	if got := get(map[string]any{"blockHash": e.block(blkEmpty).Hash()}); len(got) != 0 {
		t.Fatalf("empty block returned %d logs", len(got))
	}

	// An unknown hash is an error NAMING it. [] would say the block exists and
	// has no matching logs, which is a different and wrong answer.
	unknown := common.HexToHash("0xdeadbeef")
	_, rerr := e.call(t, "eth_getLogs", map[string]any{"blockHash": unknown})
	if rerr == nil || !strings.Contains(rerr.Message, unknown.Hex()) {
		t.Fatalf("unknown blockHash answered %v", rerr)
	}

	// blockHash is mutually exclusive with the range bounds.
	for _, extra := range []map[string]any{
		{"blockHash": callHash, "fromBlock": "0x1"},
		{"blockHash": callHash, "toBlock": "latest"},
		{"blockHash": callHash, "fromBlock": "0x1", "toBlock": "latest"},
	} {
		if _, rerr := e.call(t, "eth_getLogs", extra); rerr == nil ||
			!strings.Contains(rerr.Message, "mutually exclusive") {
			t.Fatalf("%v answered %v", extra, rerr)
		}
	}
	// An explicit null bound is not a bound, and does not trip the refusal.
	if got := get(map[string]any{"blockHash": callHash, "fromBlock": nil, "toBlock": nil}); len(got) != 1 {
		t.Fatalf("explicit null bounds returned %d logs", len(got))
	}
}

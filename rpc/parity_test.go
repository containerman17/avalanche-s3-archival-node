package rpc

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ava-labs/libevm/common"
)

// THE PARITY BUG-RULINGS, one test each, each naming the bug it exists for
// (DESIGN, "RPC surface"). These are the rules a refactor silently breaks:
// nothing here is a shape check, every case is a rule that was once wrong.

// A batch over the cap is REFUSED BY NAME. The bug: silent truncation, where a
// caller who sent 1500 requests and got 1000 answers has no way to notice the
// other 500 are missing.
func TestBatchCapsAreRefusedByName(t *testing.T) {
	srv, _, _, _ := testServer(t)

	elems := make([]string, MaxBatchSize+1)
	for i := range elems {
		elems[i] = `{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber"}`
	}
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/", strings.NewReader("["+strings.Join(elems, ",")+"]")))
	var reply struct {
		Error  *rpcError       `json:"error"`
		Result json.RawMessage `json:"result"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &reply); err != nil {
		t.Fatal(err)
	}
	if reply.Error == nil || !strings.Contains(reply.Error.Message, "exceeds the limit of 1000") {
		t.Fatalf("oversized batch: %+v (must name the cap, never truncate)", reply)
	}

	// The body cap is the same rule one level up.
	big := bytes.Repeat([]byte("a"), MaxRequestBytes+1)
	rec = httptest.NewRecorder()
	srv.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(big)))
	if err := json.Unmarshal(rec.Body.Bytes(), &reply); err != nil {
		t.Fatal(err)
	}
	if reply.Error == nil || !strings.Contains(reply.Error.Message, "exceeds the") {
		t.Fatalf("oversized body: %+v", reply)
	}
}

// NULL MEANS "NOT ON THIS CHAIN", NEVER "I COULD NOT READ IT". The unknown
// lookups answer null; the reads that were ASKED FOR at a named block answer an
// error, because "at latest instead" is a different question.
func TestNullMeansNotOnThisChain(t *testing.T) {
	srv, _, _, _ := testServer(t)

	for _, c := range []struct {
		method string
		params []any
	}{
		{"eth_getTransactionByHash", []any{common.Hash{9}}},
		{"eth_getTransactionReceipt", []any{common.Hash{9}}},
		{"eth_getBlockByHash", []any{common.Hash{9}, false}},
		{"eth_getHeaderByHash", []any{common.Hash{9}}},
		{"eth_getTransactionByBlockHashAndIndex", []any{common.Hash{9}, "0x0"}},
	} {
		res, rerr := call(t, srv, c.method, c.params...)
		if rerr != nil || res != nil {
			t.Fatalf("%s on an unknown hash: res=%v err=%v (want null)", c.method, res, rerr)
		}
	}

	// A STATE read at an unknown block hash must NOT fall back to latest.
	if _, rerr := call(t, srv, "eth_getBalance",
		common.HexToAddress("0x1"), map[string]any{"blockHash": common.Hash{9}}); rerr == nil {
		t.Fatal("eth_getBalance at an unknown block hash answered instead of erroring")
	}
	// The bare-hash spelling of the same tag is the same answer.
	if _, rerr := call(t, srv, "eth_getBalance", common.HexToAddress("0x1"), common.Hash{9}); rerr == nil {
		t.Fatal("eth_getBalance at a bare unknown block hash answered instead of erroring")
	}
}

// THE REFUSAL CLASSES ARE NAMED, and they say WHY. The bug this guards: a
// refusal that reads as a temporary gap ("not implemented") when it is a
// permanent design decision, or the other way round.
func TestRefusalClassesAreNamed(t *testing.T) {
	srv, _, _, _ := testServer(t)

	for method, want := range map[string]string{
		"eth_getProof":                      "stores no tries",
		"debug_storageRangeAt":              "stores no tries",
		"debug_accountRange":                "stores no tries",
		"debug_intermediateRoots":           "stores no tries",
		"debug_preimage":                    "preimage table",
		"debug_traceBadBlock":               "no bad blocks are retained",
		"debug_traceChain":                  "debug_traceBlockByNumber",
		"debug_getModifiedAccountsByNumber": "no touched-account index",
		"debug_getModifiedAccountsByHash":   "no touched-account index",
		"eth_sendRawTransaction":            "read-only archive node",
		"eth_sign":                          "keystore",
		"personal_unlockAccount":            "personal_unlockAccount",
		"miner_start":                       "miner_start",
	} {
		_, rerr := call(t, srv, method)
		if rerr == nil {
			t.Fatalf("%s answered instead of refusing", method)
		}
		if !strings.Contains(rerr.Message, want) {
			t.Fatalf("%s refusal does not name its class: %q", method, rerr.Message)
		}
	}

	// A method that simply does not exist is -32601, not a refusal class.
	if _, rerr := call(t, srv, "eth_notAMethod"); rerr == nil || rerr.Code != -32601 {
		t.Fatalf("unknown method: %v", rerr)
	}
}

// A MALFORMED fullTx FLAG IS AN ERROR. The bug: swallowing the parse failure
// meant false, so a caller who asked for full transactions silently got hashes.
func TestMalformedFullTxFlagIsAnError(t *testing.T) {
	srv, _, _, _ := testServer(t)
	if _, rerr := call(t, srv, "eth_getBlockByNumber", "0x1", "yes"); rerr == nil {
		t.Fatal("a malformed fullTx flag was swallowed")
	}
}

// OVERRIDES ARE HONOURED FIELD FOR FIELD. The bug: the override objects were
// parsed nowhere, so a call that sent them got an answer computed without them,
// which looks like it worked.
func TestOverridesAreHonouredFieldForField(t *testing.T) {
	raw := []json.RawMessage{
		nil, nil,
		json.RawMessage(`{"0x0000000000000000000000000000000000000001":{"nonce":"0x7","balance":"0x2a","code":"0x6001","stateDiff":{"0x0000000000000000000000000000000000000000000000000000000000000001":"0x0000000000000000000000000000000000000000000000000000000000000002"}}}`),
		json.RawMessage(`{"number":"0x9","time":"0x64","gasLimit":"0x1e8480","coinbase":"0x0000000000000000000000000000000000000002","baseFee":"0x5","blobBaseFee":"0x6"}`),
	}
	ov, rerr := parseOverrides(raw, 2, 3)
	if rerr != nil {
		t.Fatal(rerr)
	}
	acct := (*ov.state)[common.HexToAddress("0x1")]
	switch {
	case acct.Nonce == nil || uint64(*acct.Nonce) != 7:
		t.Fatalf("nonce override: %v", acct.Nonce)
	case acct.Balance == nil || *acct.Balance == nil || (**acct.Balance).ToInt().Uint64() != 42:
		t.Fatalf("balance override: %v", acct.Balance)
	case acct.Code == nil || len(*acct.Code) != 2:
		t.Fatalf("code override: %v", acct.Code)
	case acct.StateDiff == nil || len(*acct.StateDiff) != 1:
		t.Fatalf("stateDiff override: %v", acct.StateDiff)
	}
	b := ov.block
	if b.Number.ToInt().Uint64() != 9 || uint64(*b.Time) != 100 || uint64(*b.GasLimit) != 2_000_000 ||
		*b.Coinbase != common.HexToAddress("0x2") || b.BaseFee.ToInt().Uint64() != 5 ||
		b.BlobBaseFee.ToInt().Uint64() != 6 {
		t.Fatalf("block overrides: %+v", b)
	}

	// `state` and `stateDiff` together is a contradiction, not a precedence rule.
	both := []json.RawMessage{nil, nil, json.RawMessage(
		`{"0x0000000000000000000000000000000000000001":{"state":{},"stateDiff":{}}}`)}
	ov, rerr = parseOverrides(both, 2, -1)
	if rerr != nil {
		t.Fatal(rerr)
	}
	if err := ov.stateDiff().apply(nil); err == nil {
		t.Fatal("state + stateDiff must be refused")
	}
}

package rpc_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/ava-labs/libevm/common/hexutil"
	"github.com/gorilla/websocket"
)

// rpcDo posts one request, returns raw result or the error message.
func rpcDo(t *testing.T, url, method string, params ...any) (json.RawMessage, string) {
	t.Helper()
	body, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
	resp, err := http.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("%s: %v", method, err)
	}
	defer resp.Body.Close()
	var reply struct {
		Result json.RawMessage `json:"result"`
		Error  *struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&reply); err != nil {
		t.Fatalf("%s: %v", method, err)
	}
	if reply.Error != nil {
		return nil, reply.Error.Message
	}
	return reply.Result, ""
}

// TestFilterLifecycleOnCorpus: getFilterLogs == eth_getLogs for the same
// criteria; changes are empty on a static head (correct incremental
// semantics: nothing happened since installation); uninstall works once.
func TestFilterLifecycleOnCorpus(t *testing.T) {
	ts, head, _ := newCorpusServer(t)

	// find a window with logs
	var window map[string]string
	for from := uint64(100_000); from < head; from += 77_777 {
		w := map[string]string{
			"fromBlock": hexutil.EncodeUint64(from),
			"toBlock":   hexutil.EncodeUint64(from + 200),
		}
		if res, _ := rpcDo(t, ts.URL, "eth_getLogs", w); res != nil && string(res) != "[]" {
			window = w
			break
		}
	}
	if window == nil {
		t.Fatal("no log-bearing window found")
	}
	want, errMsg := rpcDo(t, ts.URL, "eth_getLogs", window)
	if errMsg != "" {
		t.Fatal(errMsg)
	}

	idRaw, errMsg := rpcDo(t, ts.URL, "eth_newFilter", window)
	if errMsg != "" {
		t.Fatal(errMsg)
	}
	var id string
	json.Unmarshal(idRaw, &id)
	if !strings.HasPrefix(id, "0x") {
		t.Fatalf("filter id %q", id)
	}
	got, errMsg := rpcDo(t, ts.URL, "eth_getFilterLogs", id)
	if errMsg != "" || !bytes.Equal(got, want) {
		t.Fatalf("getFilterLogs != getLogs (err=%s)", errMsg)
	}
	// static corpus: nothing since installation
	if ch, _ := rpcDo(t, ts.URL, "eth_getFilterChanges", id); string(ch) != "[]" {
		t.Fatalf("changes on static head: %s", ch)
	}
	if ok, _ := rpcDo(t, ts.URL, "eth_uninstallFilter", id); string(ok) != "true" {
		t.Fatalf("uninstall: %s", ok)
	}
	if ok, _ := rpcDo(t, ts.URL, "eth_uninstallFilter", id); string(ok) != "false" {
		t.Fatalf("second uninstall: %s", ok)
	}
	if _, errMsg := rpcDo(t, ts.URL, "eth_getFilterChanges", id); errMsg == "" {
		t.Fatal("changes on uninstalled filter must error")
	}

	// block + pending filters
	bIDRaw, _ := rpcDo(t, ts.URL, "eth_newBlockFilter")
	var bID string
	json.Unmarshal(bIDRaw, &bID)
	if ch, _ := rpcDo(t, ts.URL, "eth_getFilterChanges", bID); string(ch) != "[]" {
		t.Fatalf("block filter changes on static head: %s", ch)
	}
	pIDRaw, _ := rpcDo(t, ts.URL, "eth_newPendingTransactionFilter")
	var pID string
	json.Unmarshal(pIDRaw, &pID)
	if ch, _ := rpcDo(t, ts.URL, "eth_getFilterChanges", pID); string(ch) != "[]" {
		t.Fatalf("pending filter changes: %s", ch)
	}

	// registry under concurrency (race detector food)
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 10; i++ {
				r, _ := rpcDo(t, ts.URL, "eth_newBlockFilter")
				var fid string
				json.Unmarshal(r, &fid)
				rpcDo(t, ts.URL, "eth_getFilterChanges", fid)
				rpcDo(t, ts.URL, "eth_uninstallFilter", fid)
			}
		}()
	}
	wg.Wait()
}

// TestWebSocketOnCorpus: JSON-RPC over WS incl. subscribe/unsubscribe.
func TestWebSocketOnCorpus(t *testing.T) {
	ts, _, _ := newCorpusServer(t)
	url := "ws" + strings.TrimPrefix(ts.URL, "http")
	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	do := func(method string, params ...any) (json.RawMessage, string) {
		t.Helper()
		if err := conn.WriteJSON(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params}); err != nil {
			t.Fatal(err)
		}
		var reply struct {
			Result json.RawMessage `json:"result"`
			Error  *struct {
				Message string `json:"message"`
			} `json:"error"`
		}
		if err := conn.ReadJSON(&reply); err != nil {
			t.Fatal(err)
		}
		if reply.Error != nil {
			return nil, reply.Error.Message
		}
		return reply.Result, ""
	}

	if res, errMsg := do("eth_blockNumber"); errMsg != "" || !bytes.HasPrefix(res, []byte(`"0x`)) {
		t.Fatalf("blockNumber over ws: %s %s", res, errMsg)
	}
	for _, kind := range []string{"newHeads", "newPendingTransactions", "logs"} {
		res, errMsg := do("eth_subscribe", kind)
		if errMsg != "" {
			t.Fatalf("subscribe %s: %s", kind, errMsg)
		}
		var id string
		json.Unmarshal(res, &id)
		if ok, _ := do("eth_unsubscribe", id); string(ok) != "true" {
			t.Fatalf("unsubscribe %s: %s", kind, ok)
		}
	}
	if _, errMsg := do("eth_subscribe", "bogusKind"); errMsg == "" {
		t.Fatal("bogus subscription kind must error")
	}
	// subscribe over plain HTTP must be rejected
	if _, errMsg := rpcDo(t, ts.URL, "eth_subscribe", "newHeads"); !strings.Contains(errMsg, "WebSocket") {
		t.Fatalf("http subscribe: %q", errMsg)
	}
}

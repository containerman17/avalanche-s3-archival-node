package rpc

// Subscriptions used to be acknowledged and then stay silent forever. These
// tests are about the one property that changed: a subscription DELIVERS when
// the serving head advances, in geth's notification shape, and stops when it
// is unsubscribed.
//
// The corpus here is blockhash_test's synthetic chain (blocks 1..12, sealed
// 1..8, tail 9..12), because it is the only harness in this package whose
// head can be moved: the cook covers 1..8, so the serving head can be parked
// at 8 and then advanced.

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/common/hexutil"
	"github.com/gorilla/websocket"

	"github.com/containerman17/epochdb/exec"
	"github.com/containerman17/epochdb/state"
)

type wsNotification struct {
	Method string `json:"method"`
	Params struct {
		Subscription string          `json:"subscription"`
		Result       json.RawMessage `json:"result"`
	} `json:"params"`
	ID     json.RawMessage `json:"id"`
	Result json.RawMessage `json:"result"`
	Error  *rpcError       `json:"error"`
}

// wsDial opens a WebSocket against the server under test. The returned close
// func WAITS for the handler to return: httptest forgets hijacked connections,
// so without the wait the session (and its poller) can still be reading the
// History that t.Cleanup is about to close.
func wsDial(t *testing.T, env *bhEnv) (*websocket.Conn, func()) {
	t.Helper()
	var wg sync.WaitGroup
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		wg.Add(1)
		defer wg.Done()
		env.srv.ServeHTTP(w, r)
	}))
	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(ts.URL, "http"), nil)
	if err != nil {
		ts.Close()
		t.Fatalf("dial: %v", err)
	}
	return conn, func() { conn.Close(); ts.Close(); wg.Wait() }
}

func wsCall(t *testing.T, conn *websocket.Conn, id int, method string, params ...any) wsNotification {
	t.Helper()
	if params == nil {
		params = []any{}
	}
	if err := conn.WriteJSON(map[string]any{
		"jsonrpc": "2.0", "id": id, "method": method, "params": params,
	}); err != nil {
		t.Fatalf("%s: write: %v", method, err)
	}
	// The reply to a call may arrive after notifications already queued.
	for {
		msg := wsRead(t, conn)
		if msg.Method != "eth_subscription" {
			return msg
		}
	}
}

func wsRead(t *testing.T, conn *websocket.Conn) wsNotification {
	t.Helper()
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	var msg wsNotification
	if err := conn.ReadJSON(&msg); err != nil {
		t.Fatalf("read: %v", err)
	}
	return msg
}

// TestSubscriptionNewHeadsFires parks the head at the sealed end, subscribes,
// advances the head, and requires one notification per new block in order,
// each one the real header of that block.
func TestSubscriptionNewHeadsFires(t *testing.T) {
	env := newBlockHashEnv(t)
	env.hist.SetHead(bhSealedEnd)
	if got := env.hist.Head(); got != bhSealedEnd {
		t.Fatalf("head parked at %d, want %d (stateHead %d)", got, bhSealedEnd, env.hist.StateHead())
	}
	conn, closeAll := wsDial(t, env)
	defer closeAll()

	reply := wsCall(t, conn, 1, "eth_subscribe", "newHeads")
	if reply.Error != nil {
		t.Fatalf("subscribe: %v", reply.Error)
	}
	var subID string
	if err := json.Unmarshal(reply.Result, &subID); err != nil {
		t.Fatal(err)
	}

	env.hist.SetHead(bhTailEnd)

	for n := uint64(bhSealedEnd + 1); n <= bhTailEnd; n++ {
		msg := wsRead(t, conn)
		if msg.Method != "eth_subscription" {
			t.Fatalf("block %d: got a %q message, want a notification", n, msg.Method)
		}
		if msg.Params.Subscription != subID {
			t.Fatalf("notification for %q, want %q", msg.Params.Subscription, subID)
		}
		var head struct {
			Number string      `json:"number"`
			Hash   common.Hash `json:"hash"`
		}
		if err := json.Unmarshal(msg.Params.Result, &head); err != nil {
			t.Fatalf("block %d: %v", n, err)
		}
		if want := env.hashes[n]; head.Hash != want {
			t.Fatalf("block %d: notified hash %x, want %x", n, head.Hash, want)
		}
		// A header notification carries no transaction list (geth's shape).
		var raw map[string]any
		json.Unmarshal(msg.Params.Result, &raw)
		if _, ok := raw["transactions"]; ok {
			t.Fatalf("block %d: newHeads carried a transactions field", n)
		}
	}
}

// TestSubscriptionUnsubscribeStops: after unsubscribing, a further head
// advance delivers nothing. Proven by asking a normal request afterwards and
// requiring the reply, not a notification, to be the next thing on the wire.
func TestSubscriptionUnsubscribeStops(t *testing.T) {
	env := newBlockHashEnv(t)
	env.hist.SetHead(bhSealedEnd)
	conn, closeAll := wsDial(t, env)
	defer closeAll()

	var subID string
	if err := json.Unmarshal(wsCall(t, conn, 1, "eth_subscribe", "newHeads").Result, &subID); err != nil {
		t.Fatal(err)
	}
	var ok bool
	if err := json.Unmarshal(wsCall(t, conn, 2, "eth_unsubscribe", subID).Result, &ok); err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("unsubscribe returned false for a live subscription")
	}

	env.hist.SetHead(bhTailEnd)
	time.Sleep(4 * wsPollInterval) // a live subscription would have fired by now

	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	if err := conn.WriteJSON(map[string]any{
		"jsonrpc": "2.0", "id": 3, "method": "eth_blockNumber", "params": []any{},
	}); err != nil {
		t.Fatal(err)
	}
	var msg wsNotification
	if err := conn.ReadJSON(&msg); err != nil {
		t.Fatal(err)
	}
	if msg.Method == "eth_subscription" {
		t.Fatal("a notification arrived after unsubscribe")
	}

	// Unsubscribing twice is false, not an error.
	if err := json.Unmarshal(wsCall(t, conn, 4, "eth_unsubscribe", subID).Result, &ok); err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("second unsubscribe returned true")
	}
}

// TestSubscriptionKinds: logs installs and survives a head advance with no
// matching logs (this corpus has none), pending txs install and can never
// fire, and an unknown kind is refused.
func TestSubscriptionKinds(t *testing.T) {
	env := newBlockHashEnv(t)
	env.hist.SetHead(bhSealedEnd)
	conn, closeAll := wsDial(t, env)
	defer closeAll()

	for _, kind := range []any{"logs", "newPendingTransactions", "newAcceptedTransactions"} {
		if reply := wsCall(t, conn, 1, "eth_subscribe", kind); reply.Error != nil {
			t.Fatalf("subscribe %v: %v", kind, reply.Error)
		}
	}
	if reply := wsCall(t, conn, 2, "eth_subscribe", "notAKind"); reply.Error == nil {
		t.Fatal("unknown subscription kind was accepted")
	}
	// A logs subscription with criteria parses through the same matcher path
	// as eth_getLogs.
	if reply := wsCall(t, conn, 3, "eth_subscribe", "logs", map[string]any{
		"address": common.Address{0x01},
		"topics":  []any{common.Hash{0x02}},
	}); reply.Error != nil {
		t.Fatalf("subscribe logs with criteria: %v", reply.Error)
	}

	env.hist.SetHead(bhTailEnd)
	time.Sleep(4 * wsPollInterval)

	// The blocks are empty, so nothing may have been delivered on any of them.
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	if err := conn.WriteJSON(map[string]any{
		"jsonrpc": "2.0", "id": 9, "method": "eth_blockNumber", "params": []any{},
	}); err != nil {
		t.Fatal(err)
	}
	var msg wsNotification
	if err := conn.ReadJSON(&msg); err != nil {
		t.Fatal(err)
	}
	if msg.Method == "eth_subscription" {
		t.Fatalf("empty blocks produced a %s notification: %s", msg.Method, msg.Params.Result)
	}
}

// gatedBlocks serves the tail containers and can make ONE height unreadable,
// which is what a cook gap or a not-yet-local container looks like.
type gatedBlocks struct {
	mu     sync.Mutex
	hidden uint64
}

func (g *gatedBlocks) hide(n uint64) {
	g.mu.Lock()
	g.hidden = n
	g.mu.Unlock()
}

func (g *gatedBlocks) GetByHeight(n uint64) ([]byte, bool, error) {
	g.mu.Lock()
	hidden := g.hidden
	g.mu.Unlock()
	if n == hidden {
		return nil, false, fmt.Errorf("container %d temporarily unreadable", n)
	}
	if n <= bhSealedEnd || n > bhTailEnd {
		return nil, false, nil
	}
	raw, _ := bhBlock(n)
	return raw, true, nil
}

// TestSubscriptionRetriesAfterReadError is the silent gap: the cursor used to
// advance over the whole batch BEFORE delivery, so one unreadable block
// dropped it and every block behind it in that batch, with no retry and no
// notice. A subscriber cannot tell that apart from "no matching events".
func TestSubscriptionRetriesAfterReadError(t *testing.T) {
	env := newBlockHashEnv(t)
	env.hist.SetHead(bhSealedEnd)
	gate := &gatedBlocks{hidden: bhSealedEnd + 2} // block 10 unreadable
	env.srv.EnableTxAPIs(
		state.CombinedTxIndex{Epochs: env.hist.Epochs()},
		SealedBlocks{Epochs: env.hist.Epochs(), Blocks: gate},
		exec.ParseEthBlock,
	)
	conn, closeAll := wsDial(t, env)
	defer closeAll()

	if reply := wsCall(t, conn, 1, "eth_subscribe", "newHeads"); reply.Error != nil {
		t.Fatalf("subscribe: %v", reply.Error)
	}
	env.hist.SetHead(bhTailEnd)

	number := func() uint64 {
		msg := wsRead(t, conn)
		var head struct {
			Number hexutil.Uint64 `json:"number"`
		}
		if err := json.Unmarshal(msg.Params.Result, &head); err != nil {
			t.Fatal(err)
		}
		return uint64(head.Number)
	}
	if got := number(); got != bhSealedEnd+1 {
		t.Fatalf("first notification is block %d, want %d", got, bhSealedEnd+1)
	}
	time.Sleep(4 * wsPollInterval) // the batch is stuck on the unreadable block
	gate.hide(0)

	// Every block from the failure on must still arrive, in order.
	for n := uint64(bhSealedEnd + 2); n <= bhTailEnd; n++ {
		if got := number(); got != n {
			t.Fatalf("after the read error: got block %d, want %d (blocks were dropped)", got, n)
		}
	}
}

// TestSubscribeOverHTTPRefused: the two transport-level methods still need a
// WebSocket, and say so.
func TestSubscribeOverHTTPRefused(t *testing.T) {
	env := newBlockHashEnv(t)
	for _, m := range []string{"eth_subscribe", "eth_unsubscribe"} {
		_, rerr := env.srv.dispatch(&rpcRequest{Method: m, Params: mustParams(t, "newHeads")})
		if rerr == nil || !strings.Contains(rerr.Message, "WebSocket") {
			t.Fatalf("%s over HTTP: %v", m, rerr)
		}
	}
}

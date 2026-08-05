package rpc

import (
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"github.com/ava-labs/libevm/common"
	"github.com/gorilla/websocket"
)

var wsUpgrader = websocket.Upgrader{
	CheckOrigin: func(*http.Request) bool { return true },
}

// wsPollInterval is how often a connection with live subscriptions looks for
// a new head.
//
// ponytail: subscriptions POLL the serving head rather than being pushed from
// the executor. The head is one read shared by every connection's ticker, and
// the latency it adds is bounded by this interval, well under one Avalanche
// block time. Replace with a broadcast off the accept path if a consumer ever
// needs sub-100ms head notification.
const wsPollInterval = 250 * time.Millisecond

// wsMaxCatchup bounds how many blocks one tick may emit per subscription, so
// a connection opened during a catch-up (where the head moves thousands of
// blocks a second) streams steadily instead of blocking the writer inside one
// tick. The remainder goes out on the following ticks.
const wsMaxCatchup = 128

// wsSub is one live subscription on one connection.
type wsSub struct {
	kind   string
	fullTx bool
	addrs  []common.Address
	topics [][]common.Hash
	last   uint64 // highest head already delivered
}

// wsSession is one WebSocket connection: the JSON-RPC loop, its subscription
// set, and the single poller goroutine that feeds them.
type wsSession struct {
	srv  *Server
	conn *websocket.Conn

	writeMu sync.Mutex

	mu      sync.Mutex
	subs    map[string]*wsSub
	polling bool

	done     chan struct{} // closed to stop the poller
	pollDone chan struct{} // closed BY the current poller when it has returned
}

// stop ends the poller and WAITS for it. The wait is the point: the poller
// reads the History (blocks, logs, headers), and serveHTTP's caller is free
// to close that History the moment the handler returns, so a detached
// goroutine here is a use-after-close, not just an untidy exit.
func (w *wsSession) stop() {
	close(w.done)
	w.mu.Lock()
	polling, done := w.polling, w.pollDone
	w.mu.Unlock()
	if polling {
		<-done
	}
}

func (w *wsSession) send(v any) error {
	w.writeMu.Lock()
	defer w.writeMu.Unlock()
	return w.conn.WriteJSON(v)
}

// notify writes one eth_subscription notification in geth's wire shape.
func (w *wsSession) notify(id string, result any) error {
	return w.send(map[string]any{
		"jsonrpc": "2.0",
		"method":  "eth_subscription",
		"params":  map[string]any{"subscription": id, "result": result},
	})
}

// serveWS runs the JSON-RPC loop over one WebSocket connection.
// eth_subscribe / eth_unsubscribe are transport-level and handled here; every
// other method goes through the shared dispatch.
//
// SUBSCRIPTIONS FIRE (they used to be acknowledged and then stay silent
// forever). newHeads, logs and newAcceptedTransactions are driven by the
// serving head advancing, which is the same source the filter API polls and
// the only head whose blocks, logs and receipts this node can actually answer
// for. newPendingTransactions never fires and that is CORRECT, not a gap:
// there is no mempool on an archival node, so nothing is ever pending.
func (s *Server) serveWS(w http.ResponseWriter, r *http.Request) {
	conn, err := wsUpgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()
	sess := &wsSession{
		srv: s, conn: conn, subs: map[string]*wsSub{},
		done: make(chan struct{}), pollDone: make(chan struct{}),
	}
	defer sess.stop()

	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			return
		}
		var req rpcRequest
		if err := json.Unmarshal(msg, &req); err != nil {
			sess.send(map[string]any{"jsonrpc": "2.0", "id": nil,
				"error": &rpcError{Code: -32700, Message: "parse error"}})
			continue
		}
		var (
			result any
			rerr   *rpcError
		)
		switch req.Method {
		case "eth_subscribe":
			result, rerr = sess.subscribe(req.Params)
		case "eth_unsubscribe":
			result, rerr = sess.unsubscribe(req.Params)
		default:
			result, rerr = s.dispatch(&req)
		}
		id := req.ID
		if id == nil {
			id = json.RawMessage("null")
		}
		reply := map[string]any{"jsonrpc": "2.0", "id": id}
		if rerr != nil {
			reply["error"] = rerr
		} else {
			reply["result"] = result
		}
		if sess.send(reply) != nil {
			return
		}
	}
}

func (w *wsSession) subscribe(params []json.RawMessage) (any, *rpcError) {
	if len(params) < 1 {
		return nil, errInvalid("need [subscriptionKind, ...]")
	}
	var kind string
	if err := json.Unmarshal(params[0], &kind); err != nil {
		return nil, errInvalid("bad subscription kind: %v", err)
	}
	sub := &wsSub{kind: kind, last: w.srv.hist.Head()}
	switch kind {
	case "newHeads":
	case "newPendingTransactions", "newAcceptedTransactions":
		// geth/coreth signature: an optional fullTx bool.
		if raw := optParam(params, 1); raw != nil {
			if err := json.Unmarshal(raw, &sub.fullTx); err != nil {
				return nil, errInvalid("bad fullTx flag: %v", err)
			}
		}
	case "logs":
		if raw := optParam(params, 1); raw != nil {
			var f logsFilter
			if err := json.Unmarshal(raw, &f); err != nil {
				return nil, errInvalid("bad logs criteria: %v", err)
			}
			addrs, topics, rerr := parseLogMatchers(&f)
			if rerr != nil {
				return nil, rerr
			}
			sub.addrs, sub.topics = addrs, topics
		}
	default:
		return nil, errInvalid("unknown subscription %q", kind)
	}
	id := newFilterID()
	w.mu.Lock()
	w.subs[id] = sub
	// A pending-tx-only connection needs no poller: nothing can ever fire.
	// A poller that has ALREADY RETURNED (write error) leaves polling false,
	// so this subscription starts a replacement instead of handing the client
	// an id that can never fire. Each poller gets its own pollDone: stop()
	// waits on the one that is running.
	needPoll := !w.polling && kind != "newPendingTransactions"
	var done chan struct{}
	if needPoll {
		w.polling = true
		done = make(chan struct{})
		w.pollDone = done
	}
	w.mu.Unlock()
	if needPoll {
		go w.poll(done)
	}
	return id, nil
}

func (w *wsSession) unsubscribe(params []json.RawMessage) (any, *rpcError) {
	if len(params) < 1 {
		return nil, errInvalid("need [subscriptionID]")
	}
	var id string
	if err := json.Unmarshal(params[0], &id); err != nil {
		return nil, errInvalid("bad subscription id: %v", err)
	}
	w.mu.Lock()
	_, ok := w.subs[id]
	delete(w.subs, id)
	w.mu.Unlock()
	return ok, nil
}

// poll drives every head-driven subscription on this connection until the
// connection closes. One goroutine per connection, not per subscription.
func (w *wsSession) poll(done chan struct{}) {
	defer close(done)
	defer func() {
		w.mu.Lock()
		w.polling = false
		w.mu.Unlock()
	}()
	t := time.NewTicker(wsPollInterval)
	defer t.Stop()
	for {
		select {
		case <-w.done:
			return
		case <-t.C:
			if !w.tick() {
				return
			}
		}
	}
}

// tick delivers one round of notifications. It returns false when the
// connection is gone and the poller should stop.
func (w *wsSession) tick() bool {
	head := w.srv.hist.Head()

	// Snapshot under the lock; the deliveries themselves do disk reads and
	// must not hold it. The cursor advances AFTER delivery, over the range
	// that actually went out: a read error used to advance it anyway, so the
	// blocks it covered were dropped with no retry and no notice, which a
	// subscriber cannot tell apart from "no matching events". Only this
	// goroutine ticks, so there is no second delivery to race with.
	type job struct {
		id       string
		sub      wsSub
		from, to uint64
	}
	var jobs []job
	w.mu.Lock()
	for id, sub := range w.subs {
		if sub.kind == "newPendingTransactions" || sub.last >= head {
			continue
		}
		jobs = append(jobs, job{id: id, sub: *sub, from: sub.last + 1, to: min(head, sub.last+wsMaxCatchup)})
	}
	w.mu.Unlock()

	for _, j := range jobs {
		delivered, alive := w.deliver(j.id, &j.sub, j.from, j.to)
		w.mu.Lock()
		if sub, ok := w.subs[j.id]; ok && delivered > sub.last {
			sub.last = delivered
		}
		w.mu.Unlock()
		if !alive {
			return false
		}
	}
	return true
}

// deliver emits one subscription's notifications for blocks [from, to]. It
// returns the highest block it fully delivered (from-1 for none), and false
// only when the WRITE failed, which is the one condition that ends the
// connection: a read error is transient (a cook gap, a container not yet
// local), so it stops this batch and leaves the cursor where it was, and the
// next tick retries from the same block.
func (w *wsSession) deliver(id string, sub *wsSub, from, to uint64) (uint64, bool) {
	switch sub.kind {
	case "logs":
		// runGetLogs answers the whole range or nothing, so a failure delivers
		// nothing and the cursor does not move.
		logs, rerr := w.srv.runGetLogs(from, to, sub.addrs, sub.topics)
		if rerr != nil {
			return from - 1, true
		}
		for _, l := range logs {
			if w.notify(id, l) != nil {
				return from - 1, false
			}
		}
	default: // newHeads, newAcceptedTransactions
		for n := from; n <= to; n++ {
			blk, rerr := w.srv.blockAt(n)
			if rerr != nil {
				return n - 1, true
			}
			if sub.kind == "newHeads" {
				if w.notify(id, w.srv.marshalHeader(blk)) != nil {
					return n - 1, false
				}
				continue
			}
			// newAcceptedTransactions: the txs of a block that just became
			// visible. That is coreth's semantics (accepted, not pending),
			// and it is exactly what an archival follower can answer.
			for i, tx := range blk.Transactions() {
				var payload any = tx.Hash()
				if sub.fullTx {
					payload = newRPCTransaction(tx, blk.Hash(), n, blk.Time(),
						uint64(i), blk.BaseFee(), w.srv.chainCfg)
				}
				if w.notify(id, payload) != nil {
					return n - 1, false
				}
			}
		}
	}
	return to, true
}

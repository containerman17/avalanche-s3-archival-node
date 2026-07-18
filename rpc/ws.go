package rpc

import (
	"encoding/json"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
)

var wsUpgrader = websocket.Upgrader{
	CheckOrigin: func(*http.Request) bool { return true },
}

// serveWS runs the JSON-RPC loop over one WebSocket connection.
// eth_subscribe / eth_unsubscribe are transport-level and handled here;
// every other method goes through the shared dispatch. Subscriptions
// (newHeads, logs, newPendingTransactions, newAcceptedTransactions) are
// registered and acknowledged; on a static corpus nothing ever fires, and
// phase 3 plugs the live tip follower into the notifier. One reader
// goroutine per connection; writeMu exists for that future notifier.
func (s *Server) serveWS(w http.ResponseWriter, r *http.Request) {
	conn, err := wsUpgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()
	var (
		writeMu sync.Mutex
		subs    = map[string]string{} // subscription id -> kind
	)
	send := func(v any) error {
		writeMu.Lock()
		defer writeMu.Unlock()
		return conn.WriteJSON(v)
	}
	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			return
		}
		var req rpcRequest
		if err := json.Unmarshal(msg, &req); err != nil {
			send(map[string]any{"jsonrpc": "2.0", "id": nil,
				"error": &rpcError{Code: -32700, Message: "parse error"}})
			continue
		}
		var (
			result any
			rerr   *rpcError
		)
		switch req.Method {
		case "eth_subscribe":
			result, rerr = wsSubscribe(req.Params, subs)
		case "eth_unsubscribe":
			result, rerr = wsUnsubscribe(req.Params, subs)
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
		if send(reply) != nil {
			return
		}
	}
}

func wsSubscribe(params []json.RawMessage, subs map[string]string) (any, *rpcError) {
	if len(params) < 1 {
		return nil, errInvalid("need [subscriptionKind, ...]")
	}
	var kind string
	if err := json.Unmarshal(params[0], &kind); err != nil {
		return nil, errInvalid("bad subscription kind: %v", err)
	}
	switch kind {
	case "newHeads", "newPendingTransactions", "newAcceptedTransactions":
	case "logs":
		if len(params) > 1 { // validate criteria shape up front
			var f logsFilter
			if err := json.Unmarshal(params[1], &f); err != nil {
				return nil, errInvalid("bad logs criteria: %v", err)
			}
		}
	default:
		return nil, errInvalid("unknown subscription %q", kind)
	}
	id := newFilterID()
	subs[id] = kind
	return id, nil
}

func wsUnsubscribe(params []json.RawMessage, subs map[string]string) (any, *rpcError) {
	if len(params) < 1 {
		return nil, errInvalid("need [subscriptionID]")
	}
	var id string
	if err := json.Unmarshal(params[0], &id); err != nil {
		return nil, errInvalid("bad subscription id: %v", err)
	}
	_, ok := subs[id]
	delete(subs, id)
	return ok, nil
}

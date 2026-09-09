package validator

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	ethcommon "github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/common/hexutil"
	"github.com/ava-labs/libevm/core/types"
)

const maxRPCBody = 16 << 20

func bytesReader(b []byte) *bytes.Reader { return bytes.NewReader(b) }

type rpcReq struct {
	ID     json.RawMessage   `json:"id"`
	Method string            `json:"method"`
	Params []json.RawMessage `json:"params"`
}

// serveRPC answers the pool's methods in Go and forwards everything else to
// the engine's JSON-RPC as the raw body (no JSON re-encoding on the hot
// path). A batch is forwarded whole unless it holds a pool method.
func (vm *VM) serveRPC(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, maxRPCBody))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	trim := bytes.TrimLeft(body, " \t\r\n")
	if len(trim) > 0 && trim[0] == '[' {
		var reqs []json.RawMessage
		if err := json.Unmarshal(trim, &reqs); err != nil {
			w.Write(rpcError(nil, -32700, "parse error"))
			return
		}
		split := false
		for _, raw := range reqs {
			if _, ok := vm.poolMethod(raw); ok {
				split = true
				break
			}
		}
		if !split {
			vm.forward(w, body)
			return
		}
		w.Write([]byte{'['})
		for i, raw := range reqs {
			if i > 0 {
				w.Write([]byte{','})
			}
			if req, ok := vm.poolMethod(raw); ok {
				w.Write(vm.answerPool(req))
			} else if resp, err := vm.eng.rpc(raw); err == nil {
				w.Write(resp)
			} else {
				w.Write(rpcError(nil, -32603, err.Error()))
			}
		}
		w.Write([]byte{']'})
		return
	}
	if req, ok := vm.poolMethod(trim); ok {
		w.Write(vm.answerPool(req))
		return
	}
	vm.forward(w, body)
}

func (vm *VM) forward(w http.ResponseWriter, body []byte) {
	resp, err := vm.eng.rpc(body)
	if err != nil {
		w.Write(rpcError(nil, -32603, err.Error()))
		return
	}
	w.Write(resp)
}

// poolMethod decodes just enough of a request to route it.
func (vm *VM) poolMethod(raw []byte) (*rpcReq, bool) {
	var req rpcReq
	if json.Unmarshal(raw, &req) != nil {
		return nil, false
	}
	switch req.Method {
	case "eth_sendRawTransaction", "eth_sendTransaction", "txpool_status", "txpool_content", "txpool_contentFrom", "eth_pendingTransactions":
		return &req, true
	case "eth_getTransactionCount":
		var tag string
		if len(req.Params) == 2 && json.Unmarshal(req.Params[1], &tag) == nil && tag == "pending" {
			return &req, true
		}
	}
	return nil, false
}

func (vm *VM) answerPool(req *rpcReq) []byte {
	switch req.Method {
	case "eth_sendRawTransaction":
		var raw hexutil.Bytes
		if len(req.Params) != 1 || json.Unmarshal(req.Params[0], &raw) != nil {
			return rpcError(req.ID, -32602, "invalid params")
		}
		tx := new(types.Transaction)
		if err := tx.UnmarshalBinary(raw); err != nil {
			return rpcError(req.ID, -32602, err.Error())
		}
		// Remote, not local: locals bypass the pool's per-account and global
		// caps, and a flood through RPC then grows the queue (and the Go heap)
		// without bound. Remote admission gives the sender "txpool is full".
		if err := vm.pool.Add([]*types.Transaction{tx}, false, false)[0]; err != nil {
			return rpcError(req.ID, -32000, err.Error())
		}
		if vm.push != nil {
			vm.push.Add(&gossipTx{tx: tx})
		}
		return rpcResult(req.ID, tx.Hash())
	case "eth_sendTransaction":
		return rpcError(req.ID, -32601, "eth_sendTransaction is not supported: sign locally and use eth_sendRawTransaction")
	case "eth_getTransactionCount":
		var addr ethcommon.Address
		if json.Unmarshal(req.Params[0], &addr) != nil {
			return rpcError(req.ID, -32602, "invalid address")
		}
		return rpcResult(req.ID, hexutil.Uint64(vm.pool.Nonce(addr)))
	case "txpool_status":
		p, q := vm.pool.Stats()
		return rpcResult(req.ID, map[string]hexutil.Uint{"pending": hexutil.Uint(p), "queued": hexutil.Uint(q)})
	case "txpool_content":
		p, q := vm.pool.Content()
		return rpcResult(req.ID, map[string]any{"pending": byNonce(p), "queued": byNonce(q)})
	case "txpool_contentFrom":
		var addr ethcommon.Address
		if len(req.Params) != 1 || json.Unmarshal(req.Params[0], &addr) != nil {
			return rpcError(req.ID, -32602, "invalid address")
		}
		p, q := vm.pool.ContentFrom(addr)
		return rpcResult(req.ID, map[string]any{"pending": nonceMap(p), "queued": nonceMap(q)})
	case "eth_pendingTransactions":
		p, _ := vm.pool.Content()
		out := types.Transactions{}
		for _, txs := range p {
			out = append(out, txs...)
		}
		return rpcResult(req.ID, out)
	}
	return rpcError(req.ID, -32601, "method not found")
}

// txpool_content shape: address -> nonce -> tx. ponytail: txs are the
// libevm JSON form (hash, no from/blockHash), not ethapi's RPCTransaction.
func byNonce(m map[ethcommon.Address][]*types.Transaction) map[string]map[string]*types.Transaction {
	out := make(map[string]map[string]*types.Transaction, len(m))
	for addr, txs := range m {
		out[addr.Hex()] = nonceMap(txs)
	}
	return out
}

func nonceMap(txs []*types.Transaction) map[string]*types.Transaction {
	out := make(map[string]*types.Transaction, len(txs))
	for _, tx := range txs {
		out[fmt.Sprint(tx.Nonce())] = tx
	}
	return out
}

func rpcResult(id json.RawMessage, v any) []byte {
	if id == nil {
		id = json.RawMessage("null")
	}
	b, err := json.Marshal(struct {
		JSONRPC string          `json:"jsonrpc"`
		ID      json.RawMessage `json:"id"`
		Result  any             `json:"result"`
	}{"2.0", id, v})
	if err != nil {
		return rpcError(id, -32603, err.Error())
	}
	return b
}

func rpcError(id json.RawMessage, code int, msg string) []byte {
	if id == nil {
		id = json.RawMessage("null")
	}
	msg = strings.ReplaceAll(msg, `"`, `'`)
	return []byte(fmt.Sprintf(`{"jsonrpc":"2.0","id":%s,"error":{"code":%d,"message":%q}}`, id, code, msg))
}

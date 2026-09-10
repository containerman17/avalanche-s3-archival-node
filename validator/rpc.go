package validator

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"runtime"
	"strings"
	"sync"

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
		parsed := make([]*rpcReq, len(reqs))
		split := false
		for i, raw := range reqs {
			parsed[i], _ = vm.poolMethod(raw)
			split = split || parsed[i] != nil
		}
		if !split {
			vm.forward(w, body)
			return
		}
		// Every eth_sendRawTransaction of the batch goes to the pool in one
		// call (one lock, one promotion round); each element keeps its own
		// answer, in order, and one bad element refuses only itself.
		resps := make([][]byte, len(reqs))
		var txs []*types.Transaction
		var at []int
		for i, req := range parsed {
			if req == nil || req.Method != "eth_sendRawTransaction" {
				continue
			}
			tx, err := decodeRawTx(req)
			if err != nil {
				resps[i] = rpcError(req.ID, -32602, err.Error())
				continue
			}
			txs = append(txs, tx)
			at = append(at, i)
		}
		for j, err := range vm.admit(txs) {
			if err != nil {
				resps[at[j]] = rpcError(parsed[at[j]].ID, -32000, err.Error())
			} else {
				resps[at[j]] = rpcResult(parsed[at[j]].ID, txs[j].Hash())
			}
		}
		w.Write([]byte{'['})
		for i, raw := range reqs {
			if i > 0 {
				w.Write([]byte{','})
			}
			switch {
			case resps[i] != nil:
				w.Write(resps[i])
			case parsed[i] != nil:
				w.Write(vm.answerPool(parsed[i]))
			default:
				if resp, err := vm.eng.rpc(raw); err == nil {
					w.Write(resp)
				} else {
					w.Write(rpcError(nil, -32603, err.Error()))
				}
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
	if req.Method != "eth_sendRawTransaction" && req.Method != "eth_sendTransaction" {
		vm.chain.settle() // a read waits for the pool to reach the accepted head (E2E.md lesson 5)
	}
	switch req.Method {
	case "eth_sendRawTransaction":
		tx, err := decodeRawTx(req)
		if err != nil {
			return rpcError(req.ID, -32602, err.Error())
		}
		if err := vm.admit([]*types.Transaction{tx})[0]; err != nil {
			return rpcError(req.ID, -32000, err.Error())
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

func decodeRawTx(req *rpcReq) (*types.Transaction, error) {
	var raw hexutil.Bytes
	if len(req.Params) != 1 || json.Unmarshal(req.Params[0], &raw) != nil {
		return nil, errors.New("invalid params")
	}
	tx := new(types.Transaction)
	return tx, tx.UnmarshalBinary(raw)
}

// admit hands txs to the pool in one call: the senders are recovered here
// in parallel (the pool's own recovery, once per tx on the caller's
// goroutine, is then a cache hit), their accounts read in one crossing (the
// pool reads each sender under its lock otherwise), then one pool.Add (one
// lock, one promotion round; sync=false: the round runs on the reorg loop,
// which merges the rounds of concurrent calls) and one push.
//
// Remote, not local: locals bypass the pool's per-account and global caps,
// and a flood through RPC then grows the queue (and the Go heap) without
// bound. Remote admission gives the sender "txpool is full".
func (vm *VM) admit(txs []*types.Transaction) []error {
	if len(txs) == 0 {
		return nil
	}
	vm.chain.acct.warm(recoverSenders(vm.signer, txs))
	errs := vm.pool.Add(txs, false, false)
	if vm.push != nil {
		ok := make([]*gossipTx, 0, len(txs))
		for i, err := range errs {
			if err == nil {
				ok = append(ok, &gossipTx{tx: txs[i]})
			}
		}
		if len(ok) > 0 {
			vm.push.Add(ok...)
		}
	}
	return errs
}

// recoverSenders: types.Sender of every tx (cached in the tx), in parallel
// from 32 txs up; a tx whose signature does not recover gets the zero
// address (the pool reports it).
func recoverSenders(signer types.Signer, txs []*types.Transaction) []ethcommon.Address {
	out := make([]ethcommon.Address, len(txs))
	work := func(lo, hi int) {
		for i := lo; i < hi; i++ {
			out[i], _ = types.Sender(signer, txs[i])
		}
	}
	n := min(runtime.GOMAXPROCS(0), (len(txs)+31)/32)
	if n <= 1 {
		work(0, len(txs))
		return out
	}
	step := (len(txs) + n - 1) / n
	var wg sync.WaitGroup
	for lo := 0; lo < len(txs); lo += step {
		wg.Add(1)
		go func(lo, hi int) {
			defer wg.Done()
			work(lo, hi)
		}(lo, min(lo+step, len(txs)))
	}
	wg.Wait()
	return out
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

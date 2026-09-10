package validator

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
)

const maxRPCBody = 16 << 20

func bytesReader(b []byte) *bytes.Reader { return bytes.NewReader(b) }

// serveRPC forwards every request body to the engine's JSON-RPC as is (no
// JSON decoding in Go): the engine answers the pool methods too
// (eth_sendRawTransaction, batched per body into one pool call, txpool_*,
// eth_pendingTransactions, the "pending" nonce).
func (vm *VM) serveRPC(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, maxRPCBody))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	resp, err := vm.eng.rpc(body)
	if err != nil {
		w.Write([]byte(fmt.Sprintf(`{"jsonrpc":"2.0","id":null,"error":{"code":-32603,"message":%q}}`, err.Error())))
		return
	}
	w.Write(resp)
}

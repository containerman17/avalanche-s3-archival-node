package validator

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

const maxRPCBody = 16 << 20

var sendRawTx = []byte(`"eth_sendRawTransaction"`)

func bytesReader(b []byte) *bytes.Reader { return bytes.NewReader(b) }

// ingestStats: every JSON-RPC body carrying eth_sendRawTransaction, as the
// plugin sees it (the HTTP request's own start time is not known here: the
// body arrives through avalanchego's gRPC HandleSimple hop). read = the body
// read, rpc = the epochdb_rpc crossing (JSON + hex decode and pool.add in the
// engine; the engine's pool-add-ms is the add part).
type ingestStats struct {
	batches, txs, readNS, rpcNS atomic.Uint64
	inflight, inflightMax       atomic.Int64
	mu                          sync.Mutex
	ring                        [1024]time.Duration // the last rpc walls
	n                           int
}

func (s *ingestStats) record(txs int, read, rpc time.Duration) {
	s.batches.Add(1)
	s.txs.Add(uint64(txs))
	s.readNS.Add(uint64(read))
	s.rpcNS.Add(uint64(rpc))
	s.mu.Lock()
	s.ring[s.n%len(s.ring)] = rpc
	s.n++
	s.mu.Unlock()
}

func (s *ingestStats) enter() {
	n := s.inflight.Add(1)
	for {
		m := s.inflightMax.Load()
		if n <= m || s.inflightMax.CompareAndSwap(m, n) {
			return
		}
	}
}

// quantiles of the recent rpc walls: p50, p99 (ms).
func (s *ingestStats) quantiles() (p50, p99 float64) {
	s.mu.Lock()
	n := min(s.n, len(s.ring))
	v := make([]time.Duration, n)
	copy(v, s.ring[:n])
	s.mu.Unlock()
	if n == 0 {
		return 0, 0
	}
	sort.Slice(v, func(i, j int) bool { return v[i] < v[j] })
	ms := func(d time.Duration) float64 { return float64(d.Microseconds()) / 1e3 }
	return ms(v[n/2]), ms(v[min(n-1, n*99/100)])
}

// health: the fields under "ingest". poolAddMS is the engine's pool-add-ms
// (the time inside Pool::add), so decode = the crossing minus it.
func (s *ingestStats) health(poolAddMS uint64) map[string]any {
	p50, p99 := s.quantiles()
	rpcMS := s.rpcNS.Load() / 1e6
	return map[string]any{
		"ingest-batches": s.batches.Load(), "ingest-txs": s.txs.Load(),
		"ingest-read-ms": s.readNS.Load() / 1e6, "ingest-rpc-ms": rpcMS,
		"ingest-add-ms": poolAddMS, "ingest-decode-ms": int64(rpcMS) - int64(poolAddMS),
		"ingest-rpc-p50-ms": p50, "ingest-rpc-p99-ms": p99,
		"ingest-inflight": s.inflight.Load(), "ingest-concurrency-max": s.inflightMax.Load(),
	}
}

// summary: one field for the built line.
func (s *ingestStats) summary() string {
	p50, p99 := s.quantiles()
	return fmt.Sprintf("batches=%d txs=%d read=%dms rpc=%dms p50=%.1fms p99=%.1fms inflight=%d max=%d",
		s.batches.Load(), s.txs.Load(), s.readNS.Load()/1e6, s.rpcNS.Load()/1e6, p50, p99, s.inflight.Load(), s.inflightMax.Load())
}

// serveRPC forwards every request body to the engine's JSON-RPC as is (no
// JSON decoding in Go): the engine answers the pool methods too
// (eth_sendRawTransaction, batched per body into one pool call, txpool_*,
// eth_pendingTransactions, the "pending" nonce).
func (vm *VM) serveRPC(w http.ResponseWriter, r *http.Request) {
	t0 := time.Now()
	body, err := io.ReadAll(io.LimitReader(r.Body, maxRPCBody))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	tRead := time.Since(t0)
	ntx := bytes.Count(body, sendRawTx)
	if ntx > 0 {
		vm.ingest.enter()
	}
	t1 := time.Now()
	w.Header().Set("Content-Type", "application/json")
	resp, err := vm.eng.rpc(body)
	if ntx > 0 {
		vm.ingest.inflight.Add(-1)
		vm.ingest.record(ntx, tRead, time.Since(t1))
	}
	if err != nil {
		w.Write([]byte(fmt.Sprintf(`{"jsonrpc":"2.0","id":null,"error":{"code":-32603,"message":%q}}`, err.Error())))
		return
	}
	w.Write(resp)
}

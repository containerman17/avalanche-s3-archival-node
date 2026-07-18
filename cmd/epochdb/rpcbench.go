package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/common/hexutil"

	"github.com/containerman17/epochdb/exec"
	"github.com/containerman17/epochdb/fetch"
	"github.com/containerman17/epochdb/state"
)

// rpcBenchMain: per-method latency (p50/p95/p99) and throughput at c=1 and
// c=8 against a warm local server, plus an A/B correctness pass for the
// newly implemented phase-1 methods vs the archive RPC. Emits a markdown
// table.
func rpcBenchMain(args []string) {
	fs := flag.NewFlagSet("rpc-bench", flag.ExitOnError)
	dataDir := fs.String("data", "./data", "shared data directory (for probe sampling)")
	local := fs.String("local", "http://127.0.0.1:9650", "local epochdb serve URL")
	remote := fs.String("remote", "", "reference archive RPC (default per --network)")
	network := fs.String("network", "fuji", "network: fuji|mainnet")
	dur := fs.Duration("dur", 3*time.Second, "timed window per method per concurrency")
	seed := fs.Int64("seed", 42, "sampling RNG seed")
	abN := fs.Int("ab", 20, "A/B comparisons per newly implemented method")
	fs.Parse(args)
	if *remote == "" {
		_, _, *remote = netParams(*network)
	}
	fetch.RegisterExtras()
	rng := rand.New(rand.NewSource(*seed))

	// ---- sample real inputs ----
	store, err := state.OpenReadOnly(*dataDir)
	if err != nil {
		log.Fatalf("rpc-bench: %v", err)
	}
	defer store.Close()
	g, err := exec.NetworkGenesis(execNetID(*network))
	if err != nil {
		log.Fatalf("rpc-bench: %v", err)
	}
	hist, err := state.OpenHistory(*dataDir, store, g.Alloc)
	if err != nil {
		log.Fatalf("rpc-bench: %v", err)
	}
	defer hist.Close()
	fr, err := fetch.OpenReader(*dataDir)
	if err != nil {
		log.Fatalf("rpc-bench: %v", err)
	}
	defer fr.Close()
	blocks := sealedBlocks{epochs: hist.Epochs(), reader: fr}
	head := hist.Head()

	var (
		addrs       []common.Address
		slots       []struct{ addr, slot, tag string }
		txHashes    []common.Hash
		blockHashes []common.Hash
	)
	for tries := 0; tries < 20000 && (len(txHashes) < 64 || len(slots) < 64); tries++ {
		if kind, a, sl, blk, ok := hist.SampleRecord(rng); ok && kind == 's' && len(slots) < 64 {
			slots = append(slots, struct{ addr, slot, tag string }{
				a.Hex(), sl.Hex(), hexutil.EncodeUint64(min(blk+uint64(rng.Intn(1000)), head))})
		}
		n := 1 + uint64(rng.Int63n(int64(head)))
		raw, ok, err := blocks.GetByHeight(n)
		if err != nil || !ok {
			continue
		}
		blk, err := exec.ParseEthBlock(raw)
		if err != nil {
			continue
		}
		if len(blockHashes) < 64 {
			blockHashes = append(blockHashes, blk.Hash())
		}
		if txs := blk.Transactions(); len(txs) > 0 && len(txHashes) < 64 {
			tx := txs[rng.Intn(len(txs))]
			txHashes = append(txHashes, tx.Hash())
			if to := tx.To(); to != nil {
				addrs = append(addrs, *to)
			}
		}
	}
	if len(txHashes) < 16 || len(slots) < 16 || len(addrs) < 8 {
		log.Fatalf("rpc-bench: sampling too thin (tx=%d slots=%d addrs=%d)", len(txHashes), len(slots), len(addrs))
	}
	randTag := func(r *rand.Rand) string { return hexutil.EncodeUint64(1 + uint64(r.Int63n(int64(head)))) }
	logsWindow := func(r *rand.Rand) map[string]string {
		from := 1 + uint64(r.Int63n(int64(head-200)))
		return map[string]string{
			"fromBlock": hexutil.EncodeUint64(from),
			"toBlock":   hexutil.EncodeUint64(from + 100),
			"address":   addrs[r.Intn(len(addrs))].Hex(),
		}
	}

	type bench struct {
		method string
		params func(*rand.Rand) []any
		ab     string // A/B mode: "exact", "" (previously gated), or a tolerance note
	}
	benches := []bench{
		{"eth_chainId", func(*rand.Rand) []any { return nil }, "exact"},
		{"eth_blockNumber", func(*rand.Rand) []any { return nil }, "local-only: remote head is live"},
		{"eth_getBalance", func(r *rand.Rand) []any { return []any{addrs[r.Intn(len(addrs))].Hex(), randTag(r)} }, "prior gate"},
		{"eth_getTransactionCount", func(r *rand.Rand) []any { return []any{addrs[r.Intn(len(addrs))].Hex(), randTag(r)} }, "prior gate"},
		{"eth_getCode", func(r *rand.Rand) []any { return []any{addrs[r.Intn(len(addrs))].Hex(), randTag(r)} }, "prior gate"},
		{"eth_getStorageAt", func(r *rand.Rand) []any { s := slots[r.Intn(len(slots))]; return []any{s.addr, s.slot, s.tag} }, "prior gate"},
		{"eth_call", func(r *rand.Rand) []any {
			return []any{map[string]string{"to": addrs[r.Intn(len(addrs))].Hex(), "data": "0x"}, randTag(r)}
		}, "prior gate"},
		{"eth_estimateGas", func(r *rand.Rand) []any {
			return []any{map[string]string{"to": addrs[r.Intn(len(addrs))].Hex()}, randTag(r)}
		}, "prior gate"},
		{"eth_getBlockByNumber", func(r *rand.Rand) []any { return []any{randTag(r), true} }, "prior gate"},
		{"eth_getBlockByHash", func(r *rand.Rand) []any { return []any{blockHashes[r.Intn(len(blockHashes))].Hex(), false} }, "prior gate"},
		{"eth_getHeaderByNumber", func(r *rand.Rand) []any { return []any{randTag(r)} }, "local-only: public API lacks the method (-32601)"},
		{"eth_getHeaderByHash", func(r *rand.Rand) []any { return []any{blockHashes[r.Intn(len(blockHashes))].Hex()} }, "local-only: public API lacks the method (-32601)"},
		{"eth_getBlockTransactionCountByNumber", func(r *rand.Rand) []any { return []any{randTag(r)} }, "prior gate"},
		{"eth_getBlockReceipts", func(r *rand.Rand) []any { return []any{randTag(r)} }, "prior gate"},
		{"eth_getTransactionByHash", func(r *rand.Rand) []any { return []any{txHashes[r.Intn(len(txHashes))].Hex()} }, "prior gate"},
		{"eth_getTransactionReceipt", func(r *rand.Rand) []any { return []any{txHashes[r.Intn(len(txHashes))].Hex()} }, "prior gate"},
		{"eth_getRawTransactionByHash", func(r *rand.Rand) []any { return []any{txHashes[r.Intn(len(txHashes))].Hex()} }, "exact"},
		{"eth_getLogs", func(r *rand.Rand) []any { return []any{logsWindow(r)} }, "prior gate"},
		{"eth_gasPrice", func(*rand.Rand) []any { return nil }, "local-only: live oracle"},
		{"eth_baseFee", func(*rand.Rand) []any { return nil }, "local-only: live value"},
		{"eth_maxPriorityFeePerGas", func(*rand.Rand) []any { return nil }, "local-only: live oracle"},
		{"eth_feeHistory", func(r *rand.Rand) []any { return []any{"0x10", randTag(r), []float64{25, 75}} }, "prior gate"},
		{"eth_syncing", func(*rand.Rand) []any { return nil }, "tolerated: local false; coreth errors .not implemented."},
		{"net_version", func(*rand.Rand) []any { return nil }, "exact"},
		{"net_peerCount", func(*rand.Rand) []any { return nil }, "local-only: remote reports its peers"},
		{"net_listening", func(*rand.Rand) []any { return nil }, "local-only: role differs"},
		{"web3_sha3", func(r *rand.Rand) []any {
			b := make([]byte, 32)
			r.Read(b)
			return []any{hexutil.Encode(b)}
		}, "exact"},
		{"txpool_status", func(*rand.Rand) []any { return nil }, "local-only: remote pool is live"},
		{"txpool_content", func(*rand.Rand) []any { return nil }, "local-only: remote pool is live"},
		{"eth_newFilter+uninstall", nil, "local semantics test (stateful)"},
		{"eth_getFilterChanges", nil, "local semantics test (stateful)"},
	}

	post := func(url string, body []byte) ([]byte, error) {
		resp, err := http.Post(url, "application/json", bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()
		return io.ReadAll(resp.Body)
	}
	buildBody := func(method string, params []any) []byte {
		b, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
		return b
	}
	// filter benches need per-request setup; special-cased generators.
	makeBody := func(b bench, r *rand.Rand) []byte {
		switch b.method {
		case "eth_newFilter+uninstall":
			return buildBody("eth_newBlockFilter", nil) // representative registry op
		case "eth_getFilterChanges":
			return buildBody("eth_getFilterChanges", []any{benchFilterID})
		default:
			return buildBody(b.method, b.params(r))
		}
	}

	// one long-lived filter for the changes bench
	if res, err := post(*local, buildBody("eth_newBlockFilter", nil)); err == nil {
		var reply struct {
			Result string `json:"result"`
		}
		json.Unmarshal(res, &reply)
		benchFilterID = reply.Result
	}

	run := func(b bench, conc int) (p50, p95, p99 time.Duration, rps float64) {
		var (
			mu   sync.Mutex
			lats []time.Duration
		)
		stopAt := time.Now().Add(*dur)
		var wg sync.WaitGroup
		for w := 0; w < conc; w++ {
			wg.Add(1)
			go func(seed int64) {
				defer wg.Done()
				r := rand.New(rand.NewSource(seed))
				var local_ []time.Duration
				for time.Now().Before(stopAt) {
					body := makeBody(b, r)
					t0 := time.Now()
					if _, err := post(*local, body); err == nil {
						local_ = append(local_, time.Since(t0))
					}
				}
				mu.Lock()
				lats = append(lats, local_...)
				mu.Unlock()
			}(int64(w)*7919 + 1)
		}
		wg.Wait()
		if len(lats) == 0 {
			return 0, 0, 0, 0
		}
		sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
		pct := func(f float64) time.Duration { return lats[min(int(f*float64(len(lats))), len(lats)-1)] }
		return pct(0.50), pct(0.95), pct(0.99), float64(len(lats)) / dur.Seconds()
	}

	abCheck := func(b bench) string {
		if b.ab != "exact" {
			return b.ab
		}
		for i := 0; i < *abN; i++ {
			body := makeBody(b, rng)
			lRes, lErr := post(*local, body)
			rRes, rErr := post(*remote, body)
			if lErr != nil || rErr != nil {
				return fmt.Sprintf("A/B transport error: %v %v", lErr, rErr)
			}
			var l, r struct {
				Result json.RawMessage `json:"result"`
				Error  any             `json:"error"`
			}
			json.Unmarshal(lRes, &l)
			json.Unmarshal(rRes, &r)
			if (l.Error != nil) != (r.Error != nil) || !jsonEqual(l.Result, r.Result) {
				return fmt.Sprintf("MISMATCH: local=%.120s remote=%.120s", l.Result, r.Result)
			}
		}
		return fmt.Sprintf("exact(%d) OK", *abN)
	}

	fmt.Printf("| method | p50 | p95 | p99 | rps@c1 | rps@c8 | A/B |\n|---|---|---|---|---|---|---|\n")
	for _, b := range benches {
		// warmup
		warmStop := time.Now().Add(300 * time.Millisecond)
		r := rand.New(rand.NewSource(99))
		for time.Now().Before(warmStop) {
			post(*local, makeBody(b, r))
		}
		p50, p95, p99, rps1 := run(b, 1)
		_, _, _, rps8 := run(b, 8)
		verdict := abCheck(b)
		fmt.Printf("| %s | %s | %s | %s | %.0f | %.0f | %s |\n",
			b.method, p50.Round(time.Microsecond), p95.Round(time.Microsecond),
			p99.Round(time.Microsecond), rps1, rps8, verdict)
	}
}

var benchFilterID string

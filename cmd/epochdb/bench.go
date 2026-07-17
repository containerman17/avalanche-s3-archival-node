package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/big"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/common/hexutil"

	"github.com/containerman17/epochdb/exec"
	"github.com/containerman17/epochdb/state"
)

// benchMain A/B-probes the local historical server against a public Fuji
// archive RPC at random heights within the cooked range. Probe keys are
// sampled from the cooked index itself, so every probe hits a key that was
// actually written on-chain (plus below-first-write heights for the
// genesis/zero fallthrough path). Any mismatch is a hard failure.
func benchMain(args []string) {
	fs := flag.NewFlagSet("ab-bench", flag.ExitOnError)
	dataDir := fs.String("data", "./data", "shared data directory (for probe sampling)")
	local := fs.String("local", "http://127.0.0.1:9650", "local epochdb serve URL")
	remote := fs.String("remote", "https://api.avax-test.network/ext/bc/C/rpc", "reference archive RPC")
	n := fs.Int("n", 1000, "total probes")
	seed := fs.Int64("seed", time.Now().UnixNano(), "probe RNG seed")
	workers := fs.Int("workers", 8, "concurrent probe workers")
	fs.Parse(args)

	store, err := state.Open(*dataDir)
	if err != nil {
		log.Fatalf("ab-bench: open state layer: %v", err)
	}
	defer store.Close()
	g, err := exec.FujiGenesis()
	if err != nil {
		log.Fatalf("ab-bench: genesis: %v", err)
	}
	hist, err := state.OpenHistory(*dataDir, store, g.Alloc)
	if err != nil {
		log.Fatalf("ab-bench: open history: %v", err)
	}
	defer hist.Close()
	head := hist.Head()
	rng := rand.New(rand.NewSource(*seed))
	log.Printf("ab-bench: head=%d seed=%d local=%s remote=%s", head, *seed, *local, *remote)

	erc20s := discoverERC20s(rng, hist, *remote, head)
	log.Printf("ab-bench: discovered %d ERC20-ish contracts for eth_call probes: %v", len(erc20s), erc20s)

	probes := buildProbes(rng, hist, erc20s, head, *n)

	type outcome struct {
		method   string
		mismatch string // empty = match
		localDur time.Duration
	}
	var (
		wg  sync.WaitGroup
		mu  sync.Mutex
		out []outcome
	)
	ch := make(chan probe)
	for w := 0; w < *workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for p := range ch {
				o := outcome{method: p.method}
				t0 := time.Now()
				lRes, lErr := rpcCall(*local, p.method, p.params, 1)
				o.localDur = time.Since(t0)
				rRes, rErr := rpcCall(*remote, p.method, p.params, 6)
				o.mismatch = compare(p, lRes, lErr, rRes, rErr)
				mu.Lock()
				out = append(out, o)
				mu.Unlock()
			}
		}()
	}
	start := time.Now()
	for _, p := range probes {
		ch <- p
	}
	close(ch)
	wg.Wait()
	wall := time.Since(start)

	perMethod := map[string]int{}
	perMethodBad := map[string]int{}
	var localTotal time.Duration
	bad := 0
	for _, o := range out {
		perMethod[o.method]++
		localTotal += o.localDur
		if o.mismatch != "" {
			bad++
			perMethodBad[o.method]++
			log.Printf("MISMATCH %s: %s", o.method, o.mismatch)
		}
	}
	fmt.Printf("\nab-bench: %d probes in %s (incl. remote round-trips)\n", len(out), wall.Round(time.Millisecond))
	for m, c := range perMethod {
		fmt.Printf("  %-24s %5d probes, %d mismatches\n", m, c, perMethodBad[m])
	}
	fmt.Printf("  local latency: total=%s avg=%s (%.0f calls/s local-only)\n",
		localTotal.Round(time.Millisecond),
		(localTotal / time.Duration(len(out))).Round(time.Microsecond),
		float64(len(out))/localTotal.Seconds())
	if bad > 0 {
		fmt.Printf("RESULT: %d MISMATCHES\n", bad)
		os.Exit(1)
	}
	fmt.Println("RESULT: ZERO mismatches")
}

type probe struct {
	method string
	params []any
}

func hexBlock(n uint64) string { return hexutil.EncodeUint64(n) }

// probeHeight mixes uniform-random heights with exact write-boundary
// heights and just-below-boundary heights.
func probeHeight(rng *rand.Rand, writeBlock, head uint64) uint64 {
	switch rng.Intn(4) {
	case 0:
		if writeBlock <= head {
			return writeBlock // exact boundary: write must be visible
		}
	case 1:
		if writeBlock > 1 && writeBlock-1 <= head {
			return writeBlock - 1 // one below: write must NOT be visible
		}
	}
	return 1 + uint64(rng.Int63n(int64(head)))
}

func buildProbes(rng *rand.Rand, hist *state.History, erc20s []common.Address, head uint64, n int) []probe {
	var probes []probe
	balanceOfSel := "0x70a08231"
	totalSupplySel := "0x18160ddd"
	for len(probes) < n {
		kind, addr, slot, blk, ok := hist.SampleRecord(rng)
		if !ok {
			log.Fatal("ab-bench: no records to sample")
		}
		h := hexBlock(probeHeight(rng, blk, head))
		switch {
		case kind == 's':
			probes = append(probes, probe{"eth_getStorageAt", []any{addr.Hex(), common.Hash(slot).Hex(), h}})
		case rng.Intn(4) == 0 && len(erc20s) > 0:
			c := erc20s[rng.Intn(len(erc20s))]
			data := totalSupplySel
			if rng.Intn(2) == 0 {
				data = balanceOfSel + fmt.Sprintf("%064x", addr.Bytes())
			}
			probes = append(probes, probe{"eth_call", []any{map[string]string{"to": c.Hex(), "data": data}, h}})
		default:
			switch rng.Intn(3) {
			case 0:
				probes = append(probes, probe{"eth_getBalance", []any{addr.Hex(), h}})
			case 1:
				probes = append(probes, probe{"eth_getTransactionCount", []any{addr.Hex(), h}})
			default:
				probes = append(probes, probe{"eth_getCode", []any{addr.Hex(), h}})
			}
		}
	}
	return probes
}

// discoverERC20s samples cooked account records, keeps addresses whose
// totalSupply() at head answers 32 non-zero bytes on the archive RPC.
func discoverERC20s(rng *rand.Rand, hist *state.History, remote string, head uint64) []common.Address {
	seen := map[common.Address]bool{}
	var found []common.Address
	for tries := 0; tries < 400 && len(found) < 6; tries++ {
		kind, addr, _, _, ok := hist.SampleRecord(rng)
		if !ok || kind != 'a' || seen[addr] {
			continue
		}
		seen[addr] = true
		code, err := hist.CodeAt(addr, head)
		if err != nil || len(code) == 0 {
			continue
		}
		res, rpcErr := rpcCall(remote, "eth_call",
			[]any{map[string]string{"to": addr.Hex(), "data": "0x18160ddd"}, hexBlock(head)}, 4)
		if rpcErr != nil {
			continue
		}
		var out string
		if json.Unmarshal(res, &out) != nil || len(out) != 66 || out == "0x"+strings.Repeat("0", 64) {
			continue
		}
		found = append(found, addr)
	}
	return found
}

type remoteError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// rpcCall performs one JSON-RPC call with retries on transport errors and
// rate limiting.
func rpcCall(url, method string, params []any, tries int) (json.RawMessage, *remoteError) {
	body, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
	for attempt := 0; ; attempt++ {
		resp, err := http.Post(url, "application/json", bytes.NewReader(body))
		if err == nil {
			var reply struct {
				Result json.RawMessage `json:"result"`
				Error  *remoteError    `json:"error"`
			}
			decErr := json.NewDecoder(resp.Body).Decode(&reply)
			resp.Body.Close()
			transient := resp.StatusCode == 429 || resp.StatusCode >= 500
			if decErr == nil && !transient {
				return reply.Result, reply.Error
			}
		}
		if attempt+1 >= tries {
			return nil, &remoteError{Code: -1, Message: fmt.Sprintf("transport failure after %d tries: %v", tries, err)}
		}
		time.Sleep(time.Duration(200*(attempt+1)) * time.Millisecond)
	}
}

// compare normalizes both answers per method. Both-error counts as a match
// on error presence (revert messages differ across implementations).
func compare(p probe, lRes json.RawMessage, lErr *remoteError, rRes json.RawMessage, rErr *remoteError) string {
	if (lErr != nil) != (rErr != nil) {
		return fmt.Sprintf("params=%v error asymmetry: local=%v remote=%v localRes=%s remoteRes=%s",
			p.params, lErr, rErr, lRes, rRes)
	}
	if lErr != nil {
		return "" // both errored
	}
	var ls, rs string
	if json.Unmarshal(lRes, &ls) != nil || json.Unmarshal(rRes, &rs) != nil {
		return fmt.Sprintf("params=%v non-string results local=%s remote=%s", p.params, lRes, rRes)
	}
	var eq bool
	switch p.method {
	case "eth_getBalance", "eth_getTransactionCount":
		lb, ok1 := new(big.Int).SetString(strings.TrimPrefix(ls, "0x"), 16)
		rb, ok2 := new(big.Int).SetString(strings.TrimPrefix(rs, "0x"), 16)
		eq = ok1 && ok2 && lb.Cmp(rb) == 0
	case "eth_getStorageAt":
		eq = common.HexToHash(ls) == common.HexToHash(rs)
	default: // eth_getCode, eth_call: raw bytes
		eq = strings.EqualFold(ls, rs)
	}
	if !eq {
		return fmt.Sprintf("params=%v local=%s remote=%s", p.params, ls, rs)
	}
	return ""
}

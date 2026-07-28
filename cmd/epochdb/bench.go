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
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/common/hexutil"

	"github.com/containerman17/epochdb/exec"
	"github.com/containerman17/epochdb/fetch"
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
	remote := fs.String("remote", "", "reference archive RPC (default per --network)")
	n := fs.Int("n", 1000, "total probes")
	seed := fs.Int64("seed", time.Now().UnixNano(), "probe RNG seed")
	network := fs.String("network", "fuji", "network: fuji|mainnet")
	workers := fs.Int("workers", 8, "concurrent probe workers")
	floor := fs.Uint64("floor", 0, "limited-history floor of --local: probe only at or above it, and assert --local refuses below it (point --remote at a full epochdb node)")
	fs.Parse(args)
	if *remote == "" {
		_, _, *remote = netParams(*network)
	}

	store, err := state.OpenReadOnly(*dataDir)
	if err != nil {
		log.Fatalf("ab-bench: open state layer: %v", err)
	}
	defer store.Close()
	g, err := exec.NetworkGenesis(execNetID(*network))
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

	probes := buildProbes(rng, hist, erc20s, head, *n, *floor)
	if *floor > 0 {
		probes = append(probes, floorProbes(rng, hist, *floor, head)...)
	}

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
	if *floor > 0 {
		bad += belowFloorRefusals(rng, hist, *local, *floor)
	}
	if bad > 0 {
		fmt.Printf("RESULT: %d MISMATCHES\n", bad)
		os.Exit(1)
	}
	fmt.Println("RESULT: ZERO mismatches")
}

// belowFloorRefusals asserts the limited-history node REFUSES state reads
// below its floor. Answering (a zero balance, empty code) instead of
// erroring is the silent-corruption failure this whole mode exists to
// avoid, so an answer counts as a mismatch.
func belowFloorRefusals(rng *rand.Rand, hist *state.History, local string, floor uint64) int {
	bad, probes := 0, 0
	for i := 0; i < 20; i++ {
		_, addr, _, _, ok := hist.SampleRecord(rng)
		if !ok {
			break
		}
		h := hexBlock(uint64(rng.Int63n(int64(floor))))
		for _, m := range []string{"eth_getBalance", "eth_getCode"} {
			probes++
			res, err := rpcCall(local, m, []any{addr.Hex(), h}, 1)
			if err == nil {
				bad++
				log.Printf("MISMATCH %s at %s: pruned node ANSWERED below floor %d: %s", m, h, floor, res)
			}
		}
	}
	fmt.Printf("  below-floor refusals: %d probes, %d answered (want 0)\n", probes, bad)
	return bad
}

type probe struct {
	method string
	params []any
}

func hexBlock(n uint64) string { return hexutil.EncodeUint64(n) }

// probeHeight mixes uniform-random heights with exact write-boundary
// heights and just-below-boundary heights, never below floor (a
// limited-history --local refuses those by design; floorProbes covers them).
func probeHeight(rng *rand.Rand, writeBlock, head, floor uint64) uint64 {
	switch rng.Intn(4) {
	case 0:
		if writeBlock <= head && writeBlock >= floor {
			return writeBlock // exact boundary: write must be visible
		}
	case 1:
		if writeBlock > 1 && writeBlock-1 <= head && writeBlock-1 >= floor {
			return writeBlock - 1 // one below: write must NOT be visible
		}
	}
	return floor + uint64(rng.Int63n(int64(head-floor)+1))
}

// floorProbes pins the heights a limited-history node is most likely to get
// wrong: the floor itself, and the edge of the 256-header BLOCKHASH window
// the base file has to carry.
func floorProbes(rng *rand.Rand, hist *state.History, floor, head uint64) []probe {
	var probes []probe
	for _, h := range []uint64{floor, floor + 1, floor + 255, floor + 256} {
		if h > head {
			continue
		}
		_, addr, _, _, ok := hist.SampleRecord(rng)
		if !ok {
			break
		}
		probes = append(probes,
			probe{"eth_getBalance", []any{addr.Hex(), hexBlock(h)}},
			probe{"eth_getCode", []any{addr.Hex(), hexBlock(h)}})
	}
	return probes
}

func buildProbes(rng *rand.Rand, hist *state.History, erc20s []common.Address, head uint64, n int, floor uint64) []probe {
	var probes []probe
	balanceOfSel := "0x70a08231"
	totalSupplySel := "0x18160ddd"
	for len(probes) < n {
		kind, addr, slot, blk, ok := hist.SampleRecord(rng)
		if !ok {
			log.Fatal("ab-bench: no records to sample")
		}
		h := hexBlock(probeHeight(rng, blk, head, floor))
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

// benchTxMain A/B-probes eth_getTransactionByHash and
// eth_getTransactionReceipt against the archive RPC. Tx hashes are sampled
// from random staged blocks: receipts only within the replayed head (they
// need state), byHash also above it. Results are compared as decoded JSON
// (reflect.DeepEqual); no field normalization beyond JSON decoding.
func benchTxMain(args []string) {
	fs := flag.NewFlagSet("ab-bench-tx", flag.ExitOnError)
	dataDir := fs.String("data", "./data", "shared data directory")
	local := fs.String("local", "http://127.0.0.1:9650", "local epochdb serve URL")
	remote := fs.String("remote", "", "reference archive RPC (default per --network)")
	n := fs.Int("n", 600, "sampled txs (each probes both methods where state allows)")
	network := fs.String("network", "fuji", "network: fuji|mainnet")
	seed := fs.Int64("seed", time.Now().UnixNano(), "probe RNG seed")
	workers := fs.Int("workers", 8, "concurrent probe workers")
	fs.Parse(args)
	if *remote == "" {
		_, _, *remote = netParams(*network)
	}

	fetch.RegisterExtras() // ParseEthBlock needs the coreth/libevm extras
	rng := rand.New(rand.NewSource(*seed))
	fr, err := fetch.OpenReader(*dataDir)
	if err != nil {
		log.Fatalf("ab-bench-tx: open reader: %v", err)
	}
	defer fr.Close()
	epochs, err := state.OpenEpochSet(*dataDir)
	if err != nil {
		log.Fatalf("ab-bench-tx: open epochs: %v", err)
	}
	defer epochs.Close()
	reader := sealedBlocks{epochs: epochs, reader: fr} // sample sealed + raw

	// Replayed head from the local server, staging ceiling from the bucket files.
	res, rerr := rpcCall(*local, "eth_blockNumber", nil, 2)
	if rerr != nil {
		log.Fatalf("ab-bench-tx: local eth_blockNumber: %v", rerr)
	}
	var headHex string
	json.Unmarshal(res, &headHex)
	head, _ := hexutil.DecodeUint64(headHex)
	buckets, _ := filepath.Glob(filepath.Join(*dataDir, "index_*.log"))
	var maxStaged uint64
	for _, b := range buckets {
		var bn uint64
		if _, err := fmt.Sscanf(filepath.Base(b), "index_%d.log", &bn); err == nil && (bn+1)*100_000 > maxStaged {
			maxStaged = (bn + 1) * 100_000
		}
	}
	// Receipts are stored-only: probe them within the sealed range (the
	// raw tail above it errors by design).
	receiptCeil := head
	if end, ok := epochs.SealedEnd(); ok && end < receiptCeil {
		receiptCeil = end
	}
	log.Printf("ab-bench-tx: head=%d receiptCeil=%d maxStaged~%d seed=%d local=%s", head, receiptCeil, maxStaged, *seed, *local)

	// Sample txs. withState => also probe the receipt.
	type txProbe struct {
		hash      common.Hash
		withState bool
	}
	var samples []txProbe
	aboveMisses, attempts := 0, 0
	if maxStaged <= head+1 {
		aboveMisses = 1 << 30 // nothing staged above head, all probes in range
	}
	for len(samples) < *n {
		if attempts++; attempts > 200*(*n)+10_000 {
			log.Fatalf("ab-bench-tx: sampling stuck after %d attempts (%d/%d sampled)", attempts, len(samples), *n)
		}
		withState := len(samples)%5 != 4 // ~80% within the replayed range
		if aboveMisses > 1000 {
			withState = true // nothing usable staged above head, stop trying
		}
		var h uint64
		if withState {
			h = 1 + uint64(rng.Int63n(int64(receiptCeil)))
		} else {
			h = head + 1 + uint64(rng.Int63n(int64(maxStaged-head)))
		}
		raw, ok, err := reader.GetByHeight(h)
		if err != nil || !ok {
			if !withState {
				aboveMisses++
			}
			continue // staging gap
		}
		blk, err := exec.ParseEthBlock(raw)
		if err != nil || len(blk.Transactions()) == 0 {
			if !withState {
				aboveMisses++
			}
			continue
		}
		tx := blk.Transactions()[rng.Intn(len(blk.Transactions()))]
		samples = append(samples, txProbe{hash: tx.Hash(), withState: withState})
	}

	type job struct {
		method string
		hash   common.Hash
	}
	var jobs []job
	for _, s := range samples {
		jobs = append(jobs, job{"eth_getTransactionByHash", s.hash})
		if s.withState {
			jobs = append(jobs, job{"eth_getTransactionReceipt", s.hash})
		}
	}

	var (
		wg         sync.WaitGroup
		mu         sync.Mutex
		perMethod  = map[string]int{}
		mismatches = map[string]int{}
		localTotal time.Duration
	)
	ch := make(chan job)
	for w := 0; w < *workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range ch {
				params := []any{j.hash.Hex()}
				t0 := time.Now()
				lRes, lErr := rpcCall(*local, j.method, params, 1)
				ld := time.Since(t0)
				rRes, rErr := rpcCall(*remote, j.method, params, 6)
				bad := ""
				switch {
				case (lErr != nil) != (rErr != nil):
					bad = fmt.Sprintf("error asymmetry local=%v remote=%v localRes=%s remoteRes=%s", lErr, rErr, lRes, rRes)
				case lErr == nil && !jsonEqual(lRes, rRes):
					bad = fmt.Sprintf("local=%s\nremote=%s", lRes, rRes)
				}
				mu.Lock()
				perMethod[j.method]++
				localTotal += ld
				if bad != "" {
					mismatches[j.method]++
					log.Printf("MISMATCH %s %s: %s", j.method, j.hash, bad)
				}
				mu.Unlock()
			}
		}()
	}
	start := time.Now()
	for _, j := range jobs {
		ch <- j
	}
	close(ch)
	wg.Wait()

	total, bad := 0, 0
	fmt.Printf("\nab-bench-tx: %d probes (%d txs) in %s\n", len(jobs), len(samples), time.Since(start).Round(time.Millisecond))
	for m, c := range perMethod {
		fmt.Printf("  %-28s %5d probes, %d mismatches\n", m, c, mismatches[m])
		total += c
		bad += mismatches[m]
	}
	fmt.Printf("  local latency: avg=%s (%.0f calls/s local-only)\n",
		(localTotal / time.Duration(total)).Round(time.Microsecond), float64(total)/localTotal.Seconds())
	if bad > 0 {
		fmt.Printf("RESULT: %d MISMATCHES\n", bad)
		os.Exit(1)
	}
	fmt.Println("RESULT: ZERO mismatches")
}

// benchLogsMain A/B-probes eth_getLogs vs the archive RPC. Query seeds are
// real logs pulled from the local server at log-bearing blocks (raw
// capture records tell us where those are); mixes address-only,
// topic0-only, address+topic0, address+topic0+topic2 over random ranges.
// Remote caps ranges at 2048 blocks, so A/B ranges stay <=2000; the 10k
// local cap is asserted locally.
func benchLogsMain(args []string) {
	fs := flag.NewFlagSet("ab-bench-logs", flag.ExitOnError)
	dataDir := fs.String("data", "./data", "shared data directory")
	local := fs.String("local", "http://127.0.0.1:9650", "local epochdb serve URL")
	remote := fs.String("remote", "", "reference archive RPC (default per --network)")
	n := fs.Int("n", 120, "queries")
	seed := fs.Int64("seed", time.Now().UnixNano(), "RNG seed")
	network := fs.String("network", "fuji", "network: fuji|mainnet")
	fs.Parse(args)
	if *remote == "" {
		_, _, *remote = netParams(*network)
	}
	rng := rand.New(rand.NewSource(*seed))

	store, err := state.OpenReadOnly(*dataDir)
	if err != nil {
		log.Fatalf("ab-bench-logs: %v", err)
	}
	defer store.Close()
	res, rerr := rpcCall(*local, "eth_blockNumber", nil, 2)
	if rerr != nil {
		log.Fatalf("ab-bench-logs: local head: %v", rerr)
	}
	var headHex string
	json.Unmarshal(res, &headHex)
	head, _ := hexutil.DecodeUint64(headHex)
	// Logs are stored-only: sample and query within the sealed range (the
	// raw tail above it errors by design). Epoch files are self-described,
	// so the filenames alone give the sealed end.
	if names, _ := filepath.Glob(filepath.Join(*dataDir, "epoch_*.epoch")); len(names) > 0 {
		var end uint64
		for _, p := range names {
			if start, count, ok := state.ParseEpochFileName(filepath.Base(p)); ok && start+count-1 > end {
				end = start + count - 1
			}
		}
		if end < head {
			head = end
		}
	}
	log.Printf("ab-bench-logs: head=%d (sealed) seed=%d local=%s remote=%s", head, *seed, *local, *remote)

	rawLogBuckets, _ := filepath.Glob(filepath.Join(*dataDir, "logs_*"))
	hasRawLogs := len(rawLogBuckets) > 0

	// seed logs from log-bearing blocks
	type seedLog struct {
		addr   string
		topics []string
		block  uint64
	}
	var seeds []seedLog
	for tries := 0; tries < 3000 && len(seeds) < 50; tries++ {
		b := 1 + uint64(rng.Int63n(int64(head)))
		// LogsRecord only sees raw staging sidecars; on an epoch-only dir
		// it is always empty, so treat it as advisory and let the local
		// eth_getLogs answer (served from sealed epochs) decide.
		if _, ok, _ := store.LogsRecord(b); !ok && hasRawLogs {
			continue
		}
		q := map[string]string{"fromBlock": hexutil.EncodeUint64(b), "toBlock": hexutil.EncodeUint64(b)}
		res, rerr := rpcCall(*local, "eth_getLogs", []any{q}, 1)
		if rerr != nil {
			log.Fatalf("ab-bench-logs: seed query: %v", rerr)
		}
		var logs []struct {
			Address string   `json:"address"`
			Topics  []string `json:"topics"`
		}
		if json.Unmarshal(res, &logs) != nil || len(logs) == 0 {
			continue
		}
		l := logs[rng.Intn(len(logs))]
		seeds = append(seeds, seedLog{addr: l.Address, topics: l.Topics, block: b})
	}
	if len(seeds) == 0 {
		log.Fatal("ab-bench-logs: no seed logs found")
	}

	// local cap check
	if _, rerr := rpcCall(*local, "eth_getLogs",
		[]any{map[string]string{"fromBlock": "0x1", "toBlock": hexutil.EncodeUint64(10_002)}}, 1); rerr == nil {
		log.Fatal("ab-bench-logs: >10k range must error locally")
	}

	kinds := []string{"addr", "topic0", "addr+topic0", "addr+topic0+topic2"}
	perKind := map[string]int{}
	bad := 0
	widths := []uint64{10, 200, 2000}
	for i := 0; i < *n; i++ {
		sl := seeds[rng.Intn(len(seeds))]
		w := widths[rng.Intn(len(widths))]
		from := uint64(1)
		if sl.block > uint64(rng.Int63n(int64(w))) {
			from = sl.block - uint64(rng.Int63n(int64(w)))
		}
		to := min(from+w-1, head)
		q := map[string]any{
			"fromBlock": hexutil.EncodeUint64(from),
			"toBlock":   hexutil.EncodeUint64(to),
		}
		kind := kinds[i%4]
		if kind == "addr+topic0+topic2" && len(sl.topics) < 3 {
			kind = "addr+topic0"
		}
		switch kind {
		case "addr":
			q["address"] = sl.addr
		case "topic0":
			q["topics"] = []any{sl.topics[0]}
		case "addr+topic0":
			q["address"] = sl.addr
			q["topics"] = []any{sl.topics[0]}
		case "addr+topic0+topic2":
			q["address"] = sl.addr
			q["topics"] = []any{sl.topics[0], nil, sl.topics[2]}
		}
		lRes, lErr := rpcCall(*local, "eth_getLogs", []any{q}, 1)
		rRes, rErr := rpcCall(*remote, "eth_getLogs", []any{q}, 6)
		perKind[kind]++
		switch {
		case (lErr != nil) != (rErr != nil):
			bad++
			log.Printf("MISMATCH %s %v: error asymmetry local=%v remote=%v", kind, q, lErr, rErr)
		case lErr == nil && !jsonEqual(lRes, rRes):
			bad++
			log.Printf("MISMATCH %s %v:\n local=%s\n remote=%s", kind, q, lRes, rRes)
		}
	}
	fmt.Printf("\nab-bench-logs: %d queries", *n)
	for k, c := range perKind {
		fmt.Printf("  %s=%d", k, c)
	}
	fmt.Println()
	if bad > 0 {
		fmt.Printf("RESULT: %d MISMATCHES\n", bad)
		os.Exit(1)
	}
	fmt.Println("RESULT: ZERO mismatches")
}

// jsonEqual compares two JSON documents structurally.
func jsonEqual(a, b json.RawMessage) bool {
	var av, bv any
	if json.Unmarshal(a, &av) != nil || json.Unmarshal(b, &bv) != nil {
		return false
	}
	return reflect.DeepEqual(av, bv)
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

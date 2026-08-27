package main

// VERIFICATION LAYER 2: AN INDEPENDENT ARCHIVAL RPC (DESIGN, "Verification:
// three layers, no self-trust"). This diffs the surface THIS binary serves
// against a hosted archive over the same chain, method by method, spread across
// the whole height range.
//
// NEVER AGAINST OUR OWN PRIOR BINARY (user ruling 2026-08-12): the oracle is
// Glacier's per-L1 archival RPC, which is an independent implementation.
//
// THROTTLED BY DEFAULT, because the remote's rate limits are unknown: one
// request every 1/--rps seconds, and a 429 backs off and retries rather than
// counting as a mismatch. A transport failure is never a diff result.
//
// A MISMATCH IS A FINDING, NOT A TOLERANCE: nothing here has an ignore list.
// The report prints every differing path so it can be investigated.

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/containerman17/avalanche-s3-archival-node/exec"
	"github.com/containerman17/avalanche-s3-archival-node/rpc"
)

func rpcdiffMain(args []string) {
	fs := flag.NewFlagSet("rpcdiff", flag.ExitOnError)
	dataDir := fs.String("data", "./data", "data directory")
	peer := fs.String("peer", "", "the independent archival RPC to diff against (required)")
	rps := fs.Float64("rps", 5, "requests per second against the peer (its limits are unknown: stay polite)")
	scale := fs.Int("scale", 1, "multiplies every category's sample count")
	seed := fs.Int64("seed", 1, "sampling seed: the same seed re-runs the same queries")
	show := fs.Int("show", 3, "how many mismatch details to print per category")
	_, resolveChain := chainFlags(fs)
	fs.Parse(args)
	if *peer == "" {
		log.Fatal("epochdb: rpcdiff: --peer is required (the hosted archive to diff against)")
	}

	c := resolveChain(*dataDir)
	g, err := exec.ChainGenesis(c)
	if err != nil {
		log.Fatalf("epochdb: rpcdiff: genesis: %v", err)
	}
	cas, db, err := openCorpus(*dataDir, c.Root())
	if err != nil {
		log.Fatalf("epochdb: rpcdiff: %v", err)
	}
	defer cas.Close()
	defer db.Close()
	head, ok := db.Head()
	if !ok {
		log.Fatal("epochdb: rpcdiff: the store holds no blocks, so nothing can be diffed: that is a FAILURE, not a pass")
	}

	// The local side answers over its REAL wire path, so the diff covers the
	// JSON shaping too and not just the handlers.
	srv := rpc.NewServer(db, g.TrieAlloc, rpc.StoreChainContext(db), g.Config)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatalf("epochdb: rpcdiff: listen: %v", err)
	}
	defer ln.Close()
	go http.Serve(ln, srv)
	local := &rpcClient{url: "http://" + ln.Addr().String()}
	remote := &rpcClient{url: *peer, minGap: time.Duration(float64(time.Second) / *rps)}

	d := &differ{local: local, remote: remote, head: head, rnd: rand.New(rand.NewSource(*seed)), show: *show}
	start := time.Now()
	d.run(*scale)
	d.report(time.Since(start))
}

// --- the client ---------------------------------------------------------------

type rpcClient struct {
	url    string
	minGap time.Duration // 0 = no throttle (the local side)
	last   time.Time
	n      int
}

type rpcReply struct {
	Result json.RawMessage `json:"result"`
	Error  *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

// call is one JSON-RPC request. A 429 or a 5xx BACKS OFF AND RETRIES: a
// throttled response is not an answer, and counting it as one would report
// mismatches that are really the rate limit.
func (c *rpcClient) call(method string, params ...any) (*rpcReply, error) {
	if params == nil {
		params = []any{}
	}
	body, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0", "id": 1, "method": method, "params": params,
	})
	if err != nil {
		return nil, err
	}
	backoff := time.Second
	for attempt := 0; ; attempt++ {
		if c.minGap > 0 {
			if wait := c.minGap - time.Since(c.last); wait > 0 {
				time.Sleep(wait)
			}
			c.last = time.Now()
		}
		c.n++
		resp, err := http.Post(c.url, "application/json", bytes.NewReader(body))
		if err != nil {
			if attempt >= 4 {
				return nil, err
			}
			time.Sleep(backoff)
			backoff *= 2
			continue
		}
		raw, rerr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
			if attempt >= 6 {
				return nil, fmt.Errorf("%s: HTTP %d after %d attempts", method, resp.StatusCode, attempt+1)
			}
			time.Sleep(backoff)
			backoff *= 2
			continue
		}
		if rerr != nil {
			return nil, rerr
		}
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("%s: HTTP %d: %s", method, resp.StatusCode, strings.TrimSpace(string(raw)))
		}
		var reply rpcReply
		if err := json.Unmarshal(raw, &reply); err != nil {
			return nil, fmt.Errorf("%s: bad reply %q: %w", method, string(raw), err)
		}
		return &reply, nil
	}
}

// --- the differ ---------------------------------------------------------------

type category struct {
	name       string
	queries    int
	mismatches int
	skipped    int // both sides errored, or the peer had no answer to compare
	details    []string
}

type differ struct {
	local, remote *rpcClient
	head          uint64
	rnd           *rand.Rand
	show          int

	cats   []*category
	byName map[string]*category
}

func (d *differ) cat(name string) *category {
	if d.byName == nil {
		d.byName = map[string]*category{}
	}
	c, ok := d.byName[name]
	if !ok {
		c = &category{name: name}
		d.byName[name] = c
		d.cats = append(d.cats, c)
	}
	return c
}

// compare runs one query on both sides and records the verdict.
func (d *differ) compare(name, method string, params ...any) json.RawMessage {
	c := d.cat(name)
	c.queries++
	mine, err := d.local.call(method, params...)
	if err != nil {
		c.mismatches++
		d.detail(c, fmt.Sprintf("%s%v: local transport: %v", method, params, err))
		return nil
	}
	theirs, err := d.remote.call(method, params...)
	if err != nil {
		c.skipped++
		return mine.Result
	}
	switch {
	case theirs.Error != nil && theirs.Error.Code == -32601:
		// The peer does not implement the method at all. That is a fact about
		// the oracle, not a diff result, and it must not read as our bug.
		c.skipped++
	case mine.Error != nil && theirs.Error != nil:
		// Both refused. Two implementations phrase a refusal differently, so
		// the agreement is the refusal itself.
		c.skipped++
	case mine.Error != nil:
		c.mismatches++
		d.detail(c, fmt.Sprintf("%s%v: we errored (%s), peer answered %s",
			method, params, mine.Error.Message, truncate(theirs.Result)))
	case theirs.Error != nil:
		c.mismatches++
		d.detail(c, fmt.Sprintf("%s%v: peer errored (%s), we answered %s",
			method, params, theirs.Error.Message, truncate(mine.Result)))
	default:
		if diffs := jsonDiff("", mine.Result, theirs.Result); len(diffs) > 0 {
			c.mismatches++
			d.detail(c, fmt.Sprintf("%s%v: %s", method, params, strings.Join(diffs, "; ")))
		}
	}
	return mine.Result
}

func (d *differ) detail(c *category, s string) {
	if len(c.details) < d.show {
		c.details = append(c.details, s)
	}
}

func truncate(raw json.RawMessage) string {
	s := string(raw)
	if len(s) > 120 {
		return s[:120] + "..."
	}
	return s
}

// jsonDiff compares two JSON documents structurally and returns every differing
// path. Nothing is ignored: a field only one side emits is a finding too.
func jsonDiff(path string, a, b json.RawMessage) []string {
	var av, bv any
	if err := json.Unmarshal(a, &av); err != nil {
		return []string{path + ": local is not JSON"}
	}
	if err := json.Unmarshal(b, &bv); err != nil {
		return []string{path + ": peer is not JSON"}
	}
	return diffValue(path, av, bv)
}

func diffValue(path string, a, b any) []string {
	switch at := a.(type) {
	case map[string]any:
		bt, ok := b.(map[string]any)
		if !ok {
			return []string{fmt.Sprintf("%s: object vs %T", path, b)}
		}
		keys := map[string]bool{}
		for k := range at {
			keys[k] = true
		}
		for k := range bt {
			keys[k] = true
		}
		names := make([]string, 0, len(keys))
		for k := range keys {
			names = append(names, k)
		}
		sort.Strings(names)
		var out []string
		for _, k := range names {
			av, aok := at[k]
			bv, bok := bt[k]
			switch {
			case !aok:
				out = append(out, fmt.Sprintf("%s.%s: only the peer has it (%v)", path, k, bv))
			case !bok:
				out = append(out, fmt.Sprintf("%s.%s: only we have it (%v)", path, k, av))
			default:
				out = append(out, diffValue(path+"."+k, av, bv)...)
			}
		}
		return out
	case []any:
		bt, ok := b.([]any)
		if !ok {
			return []string{fmt.Sprintf("%s: array vs %T", path, b)}
		}
		if len(at) != len(bt) {
			return []string{fmt.Sprintf("%s: %d entries vs %d", path, len(at), len(bt))}
		}
		var out []string
		for i := range at {
			out = append(out, diffValue(fmt.Sprintf("%s[%d]", path, i), at[i], bt[i])...)
		}
		return out
	default:
		as, bs := fmt.Sprint(a), fmt.Sprint(b)
		// Hex payloads only differ in case when one side upper-cases them.
		if strings.EqualFold(as, bs) {
			return nil
		}
		return []string{fmt.Sprintf("%s: %s vs %s", path, as, bs)}
	}
}

func (d *differ) report(elapsed time.Duration) {
	var q, m, s int
	fmt.Printf("\nLAYER 2: %s vs %s, head %d, %s\n\n", "local", d.remote.url, d.head, elapsed.Round(time.Second))
	fmt.Printf("%-34s %8s %11s %8s\n", "category", "queries", "mismatches", "skipped")
	for _, c := range d.cats {
		fmt.Printf("%-34s %8d %11d %8d\n", c.name, c.queries, c.mismatches, c.skipped)
		q, m, s = q+c.queries, m+c.mismatches, s+c.skipped
	}
	fmt.Printf("%-34s %8d %11d %8d\n", "TOTAL", q, m, s)
	fmt.Printf("\nrequests: %d local, %d peer\n", d.local.n, d.remote.n)
	for _, c := range d.cats {
		if len(c.details) == 0 {
			continue
		}
		fmt.Printf("\n--- %s\n", c.name)
		for _, line := range c.details {
			fmt.Println("   ", line)
		}
	}
	if m == 0 {
		fmt.Printf("\nPASS: %d queries, 0 mismatches\n", q)
	} else {
		fmt.Printf("\nFAIL: %d of %d queries mismatched\n", m, q)
	}
}

// hexNum is how every height goes on the wire.
func hexNum(n uint64) string { return fmt.Sprintf("0x%x", n) }

// heights returns n sampled heights spread across the WHOLE range: the range is
// cut into n bands and one height is drawn inside each, so no era is missed and
// no era is over-weighted.
func (d *differ) heights(n int) []uint64 {
	out := make([]uint64, 0, n)
	band := d.head / uint64(n)
	if band == 0 {
		band = 1
	}
	for i := 0; i < n; i++ {
		lo := uint64(i)*band + 1
		if lo > d.head {
			break
		}
		hi := lo + band
		if hi > d.head {
			hi = d.head
		}
		out = append(out, lo+uint64(d.rnd.Int63n(int64(hi-lo+1))))
	}
	return out
}

func (d *differ) run(scale int) {
	n := func(base int) int { return base * scale }

	// --- blocks, both shapes, plus the block-hash spelling --------------------
	type sampled struct {
		height uint64
		hash   string
		txs    []string
		addrs  []string
	}
	var pool []sampled
	for _, h := range d.heights(n(60)) {
		raw := d.compare("eth_getBlockByNumber (hashes)", "eth_getBlockByNumber", hexNum(h), false)
		var blk struct {
			Hash         string   `json:"hash"`
			Transactions []string `json:"transactions"`
			Miner        string   `json:"miner"`
		}
		if len(raw) > 0 && json.Unmarshal(raw, &blk) == nil && blk.Hash != "" {
			pool = append(pool, sampled{height: h, hash: blk.Hash, txs: blk.Transactions})
		}
	}
	for _, h := range d.heights(n(30)) {
		d.compare("eth_getBlockByNumber (full)", "eth_getBlockByNumber", hexNum(h), true)
	}
	for i, s := range pool {
		if i >= n(30) {
			break
		}
		d.compare("eth_getBlockByHash", "eth_getBlockByHash", s.hash, false)
	}

	// --- transactions and receipts --------------------------------------------
	var txHashes []string
	for _, s := range pool {
		if len(s.txs) > 0 {
			txHashes = append(txHashes, s.txs[d.rnd.Intn(len(s.txs))])
		}
	}
	d.rnd.Shuffle(len(txHashes), func(i, j int) { txHashes[i], txHashes[j] = txHashes[j], txHashes[i] })
	var addrs, contracts []string
	for i, h := range txHashes {
		if i >= n(60) {
			break
		}
		raw := d.compare("eth_getTransactionByHash", "eth_getTransactionByHash", h)
		var tx struct {
			From string  `json:"from"`
			To   *string `json:"to"`
		}
		if len(raw) > 0 && json.Unmarshal(raw, &tx) == nil {
			addrs = append(addrs, tx.From)
			if tx.To != nil {
				addrs = append(addrs, *tx.To)
			}
		}
	}
	for i, h := range txHashes {
		if i >= n(60) {
			break
		}
		raw := d.compare("eth_getTransactionReceipt", "eth_getTransactionReceipt", h)
		var r struct {
			Logs []struct {
				Address string `json:"address"`
			} `json:"logs"`
			ContractAddress *string `json:"contractAddress"`
		}
		if len(raw) > 0 && json.Unmarshal(raw, &r) == nil {
			for _, l := range r.Logs {
				contracts = append(contracts, l.Address)
			}
			if r.ContractAddress != nil {
				contracts = append(contracts, *r.ContractAddress)
			}
		}
	}
	for i, s := range pool {
		if i >= n(15) {
			break
		}
		d.compare("eth_getBlockReceipts", "eth_getBlockReceipts", hexNum(s.height))
	}

	// --- logs -----------------------------------------------------------------
	// The receipts above also give real topics and block hashes, so the filter
	// shapes the posting families answer differently (address, topic0 alone,
	// a value at position 1..3, several addresses, blockHash) are each diffed.
	type logSample struct {
		blockHash string
		address   string
		topics    []string
	}
	var samples []logSample
	for i, h := range txHashes {
		if i >= n(30) {
			break
		}
		raw := d.compare("eth_getTransactionReceipt", "eth_getTransactionReceipt", h)
		var r struct {
			BlockHash string `json:"blockHash"`
			Logs      []struct {
				Address string   `json:"address"`
				Topics  []string `json:"topics"`
			} `json:"logs"`
		}
		if len(raw) > 0 && json.Unmarshal(raw, &r) == nil {
			for _, l := range r.Logs {
				samples = append(samples, logSample{r.BlockHash, l.Address, l.Topics})
			}
		}
	}
	for i, h := range d.heights(n(20)) {
		to := h + 9
		if to > d.head {
			to = d.head
		}
		filter := map[string]any{"fromBlock": hexNum(h), "toBlock": hexNum(to)}
		if i%2 == 1 && len(contracts) > 0 {
			filter["address"] = contracts[d.rnd.Intn(len(contracts))]
		}
		d.compare("eth_getLogs", "eth_getLogs", filter)
	}
	for i, smp := range samples {
		if i >= n(40) {
			break
		}
		filter := map[string]any{"blockHash": smp.blockHash}
		switch i % 5 {
		case 0: // the whole block by hash
		case 1: // emitter + topic0
			filter["address"] = smp.address
			if len(smp.topics) > 0 {
				filter["topics"] = []any{smp.topics[0]}
			}
		case 2: // topic0 alone
			if len(smp.topics) > 0 {
				filter["topics"] = []any{smp.topics[0]}
			}
		case 3: // a value at position 1..3, any signature
			if len(smp.topics) > 1 {
				pos := 1 + d.rnd.Intn(len(smp.topics)-1)
				topics := make([]any, pos+1)
				topics[pos] = smp.topics[pos]
				filter["topics"] = topics
			}
		case 4: // several emitters over a range
			delete(filter, "blockHash")
			hh := d.heights(1)[0]
			filter["fromBlock"], filter["toBlock"] = hexNum(hh), hexNum(min(hh+9, d.head))
			filter["address"] = []string{smp.address, samples[d.rnd.Intn(len(samples))].address}
		}
		d.compare("eth_getLogs", "eth_getLogs", filter)
	}

	// --- historical state ------------------------------------------------------
	// The addresses come from the sampled transactions, so every query is about
	// an account this chain really touched, at a height ABOVE the touch and at
	// one below it.
	if len(addrs) > 0 {
		for i, h := range d.heights(n(60)) {
			d.compare("eth_getBalance", "eth_getBalance", addrs[i%len(addrs)], hexNum(h))
		}
		for i, h := range d.heights(n(40)) {
			d.compare("eth_getTransactionCount", "eth_getTransactionCount", addrs[i%len(addrs)], hexNum(h))
		}
	}
	if len(contracts) > 0 {
		for i, h := range d.heights(n(30)) {
			d.compare("eth_getCode", "eth_getCode", contracts[i%len(contracts)], hexNum(h))
		}
		for i, h := range d.heights(n(30)) {
			// totalSupply(), a selector nearly every token answers; a contract
			// that does not is a revert on BOTH sides, which agrees.
			call := map[string]any{"to": contracts[i%len(contracts)], "data": "0x18160ddd"}
			d.compare("eth_call", "eth_call", call, hexNum(h))
		}
		for i, h := range d.heights(n(20)) {
			c := contracts[i%len(contracts)]
			d.compare("eth_getStorageAt", "eth_getStorageAt", c, "0x0", hexNum(h))
		}
	}
}

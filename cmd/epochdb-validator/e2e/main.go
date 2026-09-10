// Command e2e is the validator's oracle: a local tmpnet subnet with 5
// validators, 3 running our plugin and 2 running stock subnet-evm, one
// genesis, same VM id. It submits transfers, a contract deploy, calls, a
// failing tx and a nonce gap through both node kinds, checks that blocks and
// receipts agree on every node at every height and that both kinds
// proposed, then runs a load phase and samples block fill, plugin RSS and
// the Go side's heap/GC from /ext/metrics.
//
//	go run ./cmd/epochdb-validator/e2e --avalanchego ~/avalanchego/build/avalanchego \
//	  --ours <plugin dir with srEXi...=epochdb-validator> --stock <plugin dir with stock subnet-evm> \
//	  [--load 10m --rate 300 --keys 200] [--keep]
//
// tmpnet builds permissioned subnets (no ConvertSubnetToL1 helper), so the
// "L1" is a 5-validator subnet; consensus, proposervm and the VM see no
// difference for this test.
package main

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ava-labs/avalanchego/config"
	"github.com/ava-labs/avalanchego/genesis"
	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/tests"
	"github.com/ava-labs/avalanchego/tests/fixture/tmpnet"
	"github.com/ava-labs/avalanchego/utils/crypto/secp256k1"
	ethcommon "github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/crypto"
	"github.com/ava-labs/libevm/ethclient"
	"github.com/ava-labs/libevm/params"
)

const subnetEVMID = "srEXiWaHuhNyGwPUi444Tu47ZEDwxTWrbQiuD7FmgSAQ6X7Dy"

// chainGenesis: the first %s is the fee config, the second the genesis
// timestamp. The default is subnet-evm's 20 M gas, 2 s blocks at a 1970
// genesis; --stress raises the gas limit to 500 M with the target gas scaled
// 100x and seeds the ACP-226 min block delay at 1 ms. The seed
// (initialMinDelayMS) is written into the genesis header only when the
// genesis time is Granite-active, so the stress genesis is stamped "now";
// at a 1970 genesis the excess starts at ~2 s and moves 200 units per
// block (~40k blocks to reach 1 ms), which paces every block at 2 s.
const chainGenesis = `{
  "config": {
    "chainId": 99999, "homesteadBlock": 0, "eip150Block": 0, "eip155Block": 0, "eip158Block": 0,
    "byzantiumBlock": 0, "constantinopleBlock": 0, "petersburgBlock": 0, "istanbulBlock": 0, "muirGlacierBlock": 0,
    "subnetEVMTimestamp": 0,
    %s
    "allowFeeRecipients": false
  },
  "alloc": {"8db97C7cEcE249c2b98bDC0226Cc4C2A57BF52FC": {"balance": "0x52B7D2DCC80CD2E4000000"}},
  "nonce": "0x0", "timestamp": "%s", "extraData": "0x00", "gasLimit": "0x%x", "difficulty": "0x0",
  "mixHash": "0x0000000000000000000000000000000000000000000000000000000000000000",
  "coinbase": "0x0000000000000000000000000000000000000000", "number": "0x0", "gasUsed": "0x0",
  "parentHash": "0x0000000000000000000000000000000000000000000000000000000000000000"
}`

const feeConfigDefault = `"feeConfig": {"gasLimit": 20000000, "minBaseFee": 1000000000, "targetGas": 100000000, "baseFeeChangeDenominator": 48,
      "minBlockGasCost": 0, "maxBlockGasCost": 10000000, "targetBlockRate": 2, "blockGasCostStep": 500000},`

const feeConfigStress = `"feeConfig": {"gasLimit": 500000000, "minBaseFee": 1000000000, "targetGas": 10000000000, "baseFeeChangeDenominator": 48,
      "minBlockGasCost": 0, "maxBlockGasCost": 10000000, "targetBlockRate": 2, "blockGasCostStep": 500000},
    "initialMinDelayMS": 1,`

// chainConfig: the chain config bytes both plugin kinds receive. The pool
// caps are raised so one sender's burst is not dropped at 16 pending / 64
// queued and the global caps hold a load run's backlog; "min-delay-target"
// is the ACP-226 delay each validator votes for (stock and ours read it).
const chainConfig = `{"log-level":"info","state-sync-enabled":false,"pruning-enabled":false,"pprof-addr":"127.0.0.1:0",` +
	`"tx-pool-account-slots":%d,"tx-pool-global-slots":200000,"tx-pool-account-queue":2000,"tx-pool-global-queue":400000,` +
	`"min-delay-target":%d,` +
	`"eth-apis":["eth","eth-filter","net","web3","internal-eth","internal-blockchain","internal-transaction","internal-tx-pool"]}`

// storeContract: init code returning an 18-byte runtime that SSTOREs
// calldata[0:32] into slot 0, and reverts when called with no calldata.
var storeContract = ethcommon.Hex2Bytes("6012600c60003960126000f3" + "3615600c57600035600055005b60006000fd")

var (
	network  *tmpnet.Network
	keep     = flag.Bool("keep", false, "leave the network running")
	logsDir  = flag.String("logs", "", "copy every node's logs here and delete the network dir after the run (default: keep the network dir)")
	rpcNodes = flag.Int("rpc-nodes", 0, "load phase: post to the first N nodes only, the rest get every tx by gossip (0 = all nodes)")
)

// teardown stops the network and, with --logs, keeps only the logs.
func teardown() {
	if network == nil || *keep {
		return
	}
	network.Stop(context.Background())
	if *logsDir == "" {
		return
	}
	dst := filepath.Join(*logsDir, filepath.Base(network.Dir))
	for _, n := range network.Nodes {
		os.CopyFS(filepath.Join(dst, n.NodeID.String()), os.DirFS(filepath.Join(n.DataDir, "logs")))
	}
	os.RemoveAll(network.Dir)
	fmt.Println("logs kept at", dst)
}

// check exits on error, stopping the network first unless --keep.
func check(err error, what string) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "FATAL %s: %v\n", what, err)
		fail()
	}
}

func fail() {
	teardown()
	os.Exit(1)
}

type node struct {
	*tmpnet.Node
	kind string // ours | stock
	rpc  string
	ec   *ethclient.Client
}

func main() {
	avago := flag.String("avalanchego", "", "avalanchego binary")
	ours := flag.String("ours", "", "plugin dir holding our epochdb-validator as "+subnetEVMID)
	stock := flag.String("stock", "", "plugin dir holding stock subnet-evm as "+subnetEVMID)
	load := flag.Duration("load", 0, "load phase duration (0 = skip)")
	rate := flag.Int("rate", 300, "load phase tx/s")
	nkeys := flag.Int("keys", 200, "load phase sender keys")
	workers := flag.Int("workers", 8, "load phase sender goroutines")
	batch := flag.Int("batch", 200, "eth_sendRawTransaction per JSON-RPC batch")
	stress := flag.Bool("stress", false, "stress genesis: 500 M gas limit, 1 ms min block delay")
	oursN := flag.Int("ours-n", 3, "validators running our plugin")
	stockN := flag.Int("stock-n", 2, "validators running the stock plugin")
	slots := flag.Int("account-slots", 1000, "tx-pool-account-slots of the chain config (pending txs per sender; the pool's depth is keys x this)")
	nodeLog := flag.String("node-log-level", "info", "avalanchego log-level for every node (verbo logs every consensus vote and network message)")
	nodeFlags := flag.String("node-flags", "", "extra avalanchego flags for every node, comma separated key=value")
	flag.Parse()
	if *avago == "" || *ours == "" || *stock == "" {
		flag.Usage()
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute+*load)
	defer cancel()
	log := tests.NewDefaultLogger("epochdb-e2e")

	key := genesis.EWOQKey
	network = tmpnet.NewDefaultNetwork("epochdb-validator-e2e")
	network.Nodes = tmpnet.NewNodesOrPanic(*oursN + *stockN)
	nodes := make([]*node, len(network.Nodes))
	for i, n := range network.Nodes {
		kind, dir := "ours", *ours
		if i >= *oursN {
			kind, dir = "stock", *stock
		}
		n.RuntimeConfig = &tmpnet.NodeRuntimeConfig{Process: &tmpnet.ProcessRuntimeConfig{AvalancheGoPath: *avago, PluginDir: dir}}
		nodes[i] = &node{Node: n, kind: kind}
	}
	testGenesis, err := tmpnet.NewTestGenesis(2197052273, network.Nodes, []*secp256k1.PrivateKey{key})
	check(err, "genesis")
	network.Genesis = testGenesis
	network.DefaultFlags = tmpnet.FlagsMap{config.MinStakeDurationKey: "2s"}
	network.DefaultFlags.SetDefaults(tmpnet.DefaultE2EFlags())
	network.DefaultFlags[config.LogLevelKey] = *nodeLog
	network.DefaultFlags[config.LogDisplayLevelKey] = "info"
	for _, kv := range strings.Split(*nodeFlags, ",") {
		if k, v, ok := strings.Cut(kv, "="); ok {
			network.DefaultFlags[k] = v
		}
	}
	network.PreFundedKeys = []*secp256k1.PrivateKey{key}
	network.DefaultRuntimeConfig = tmpnet.NodeRuntimeConfig{Process: &tmpnet.ProcessRuntimeConfig{AvalancheGoPath: *avago}}
	vmID, err := ids.FromString(subnetEVMID)
	check(err, "vm id")
	feeConfig, minDelay, gasLimit, genesisTime := feeConfigDefault, 2000, 20e6, "0x0"
	if *stress {
		feeConfig, minDelay, gasLimit = feeConfigStress, 1, 500e6
		genesisTime = fmt.Sprintf("0x%x", time.Now().Unix())
	}
	network.Subnets = []*tmpnet.Subnet{{
		Name: "epochdb",
		Chains: []*tmpnet.Chain{{
			VMID:    vmID,
			Genesis: []byte(fmt.Sprintf(chainGenesis, feeConfig, genesisTime, uint64(gasLimit))),
			Config:  fmt.Sprintf(chainConfig, *slots, minDelay),
		}},
		ValidatorIDs: tmpnet.NodesToIDs(network.Nodes...),
	}}

	check(tmpnet.BootstrapNewNetwork(ctx, log, network, ""), "bootstrap")
	chainID := network.Subnets[0].Chains[0].ChainID
	fmt.Println("network:", network.Dir, "chain:", chainID)
	for _, n := range nodes {
		n.rpc = n.GetAccessibleURI() + "/ext/bc/" + chainID.String() + "/rpc"
		n.ec, err = ethclient.Dial(n.rpc)
		check(err, "dial "+n.rpc)
		fmt.Printf("%s %s %s\n", n.kind, n.NodeID, n.rpc)
	}
	// The chain's handlers mount after the node reports healthy: wait for them.
	for _, n := range nodes {
		for deadline := time.Now().Add(2 * time.Minute); ; time.Sleep(500 * time.Millisecond) {
			if _, err := n.ec.ChainID(ctx); err == nil {
				break
			} else if time.Now().After(deadline) {
				check(err, "chain rpc on "+n.kind)
			}
		}
	}
	defer teardown()

	ethKey := key.ToECDSA()
	d := &driver{ctx: ctx, nodes: nodes, chainID: big.NewInt(99999), gasLimit: gasLimit}
	d.signer = types.LatestSignerForChainID(d.chainID)

	// ---- functional phase: both node kinds submit, every node agrees ----
	from := crypto.PubkeyToAddress(ethKey.PublicKey)
	to := ethcommon.HexToAddress("0x1000000000000000000000000000000000000001")
	nonce, err := nodes[0].ec.NonceAt(ctx, from, nil)
	check(err, "nonce")
	last := nodes[len(nodes)-1] // a stock node when there is one
	alt := nodes[len(nodes)/2]
	for i, n := range []*node{nodes[0], last, alt, nodes[len(nodes)-2]} {
		r := d.send(n, ethKey, &nonce, &to, big.NewInt(int64(i+1)), nil)
		fmt.Printf("transfer via %s: block %d status %d\n", n.kind, r.BlockNumber, r.Status)
	}
	r := d.send(last, ethKey, &nonce, nil, nil, storeContract)
	contract := r.ContractAddress
	fmt.Printf("deploy via %s: block %d status %d contract %s\n", last.kind, r.BlockNumber, r.Status, contract)
	val := ethcommon.LeftPadBytes([]byte{0x42}, 32)
	r = d.send(nodes[0], ethKey, &nonce, &contract, nil, val)
	fmt.Printf("call via ours: block %d status %d\n", r.BlockNumber, r.Status)
	if r.Status != 1 {
		check(fmt.Errorf("status %d", r.Status), "contract call")
	}
	r = d.send(last, ethKey, &nonce, &contract, nil, nil)
	fmt.Printf("failing call via %s: block %d status %d\n", last.kind, r.BlockNumber, r.Status)
	if r.Status != 0 {
		check(fmt.Errorf("status %d", r.Status), "failing call should fail")
	}
	// nonce gap: nonce+1 first (queued everywhere), then nonce (fills it).
	gapTx := d.sign(ethKey, nonce+1, &to, big.NewInt(7), nil, 21000)
	check(alt.ec.SendTransaction(ctx, gapTx), "send gap tx")
	time.Sleep(2 * time.Second)
	if p, _ := d.pool(alt); p != 0 {
		check(fmt.Errorf("gap tx pending on %s: %d", alt.kind, p), "nonce gap")
	}
	fill := d.sign(ethKey, nonce, &to, big.NewInt(8), nil, 21000)
	check(last.ec.SendTransaction(ctx, fill), "send fill tx")
	d.receipt(nodes[0], fill.Hash())
	r = d.receipt(nodes[0], gapTx.Hash())
	nonce += 2
	fmt.Printf("nonce gap: gap tx mined in block %d\n", r.BlockNumber)

	for _, n := range nodes {
		got, err := n.ec.StorageAt(ctx, contract, ethcommon.Hash{}, nil)
		check(err, "storage")
		if !bytes.Equal(got, val) {
			check(fmt.Errorf("%s: slot0=%x", n.kind, got), "storage divergence")
		}
	}
	head := d.minHead()
	d.compareAll(1, head)
	proposers := d.proposers(network.Dir, chainID, head)
	fmt.Println("functional phase OK: head", head, "proposers", proposers)
	d.scanStockLogs(network.Dir, nodes)
	bothBuilt := func(p map[string]int) {
		if *ours != *stock && *stockN > 0 && (p["ours"] == 0 || p["stock"] == 0) { // same dir = harness dry run
			check(fmt.Errorf("both kinds must have built: %v", p), "proposers")
		}
	}
	if *load == 0 {
		bothBuilt(proposers) // 8 blocks: the proposer schedule may pick one kind only
	}

	// ---- load phase ----
	if *load > 0 {
		d.loadPhase(ethKey, &nonce, *nkeys, *rate, *load, *ours, *workers, *batch)
		head2 := d.minHead()
		d.compareAll(head+1, head2)
		proposers = d.proposers(network.Dir, chainID, head2)
		fmt.Println("load phase OK: head", head2, "proposers", proposers)
		d.scanStockLogs(network.Dir, nodes)
		bothBuilt(proposers)
	}
	if *keep {
		fmt.Println("network kept running at", network.Dir)
	}
}

type driver struct {
	ctx      context.Context
	nodes    []*node
	chainID  *big.Int
	signer   types.Signer
	gasLimit float64
}

func (d *driver) sign(key *ecdsa.PrivateKey, nonce uint64, to *ethcommon.Address, value *big.Int, data []byte, gas uint64) *types.Transaction {
	return types.MustSignNewTx(key, d.signer, &types.DynamicFeeTx{
		ChainID: d.chainID, Nonce: nonce, To: to, Value: value, Data: data, Gas: gas,
		GasFeeCap: big.NewInt(50 * params.GWei), GasTipCap: big.NewInt(params.GWei),
	})
}

// send submits one tx through n and waits for its receipt on n.
func (d *driver) send(n *node, key *ecdsa.PrivateKey, nonce *uint64, to *ethcommon.Address, value *big.Int, data []byte) *types.Receipt {
	gas := uint64(21000)
	if len(data) > 0 {
		gas = 200000
	}
	tx := d.sign(key, *nonce, to, value, data, gas)
	check(n.ec.SendTransaction(d.ctx, tx), "send via "+n.kind)
	*nonce++
	return d.receipt(n, tx.Hash())
}

func (d *driver) receipt(n *node, h ethcommon.Hash) *types.Receipt {
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		if r, err := n.ec.TransactionReceipt(d.ctx, h); err == nil {
			return r
		}
		time.Sleep(200 * time.Millisecond)
	}
	check(fmt.Errorf("tx %s", h), "receipt timeout on "+n.kind)
	return nil
}

func (d *driver) pool(n *node) (pending, queued int) {
	var res struct{ Pending, Queued string }
	json.Unmarshal(rpcRaw(n.rpc, `{"jsonrpc":"2.0","id":1,"method":"txpool_status","params":[]}`), &res)
	p, _ := strconv.ParseInt(strings.TrimPrefix(res.Pending, "0x"), 16, 64)
	q, _ := strconv.ParseInt(strings.TrimPrefix(res.Queued, "0x"), 16, 64)
	return int(p), int(q)
}

// rpcRaw posts one request and returns the raw "result" bytes.
func rpcRaw(url, body string) json.RawMessage {
	resp, err := http.Post(url, "application/json", strings.NewReader(body))
	check(err, "rpc "+url)
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var out struct {
		Result json.RawMessage `json:"result"`
		Error  json.RawMessage `json:"error"`
	}
	if err := json.Unmarshal(raw, &out); err != nil || len(out.Error) > 0 {
		check(fmt.Errorf("%s -> %s", body, raw), "rpc")
	}
	return out.Result
}

// canonical re-encodes JSON with sorted keys so two implementations'
// formatting differences do not count as divergence.
func canonical(raw json.RawMessage) string {
	var v any
	check(json.Unmarshal(raw, &v), "canonical")
	b, _ := json.Marshal(v)
	return string(b)
}

// minHead: the lowest accepted height across nodes (they may be a block apart).
func (d *driver) minHead() uint64 {
	var m uint64
	for i, n := range d.nodes {
		h, err := n.ec.BlockNumber(d.ctx)
		check(err, "head")
		if i == 0 || h < m {
			m = h
		}
	}
	return m
}

// compareAll: eth_getBlockByNumber(full) and eth_getBlockReceipts must agree
// on every node at every height in [from, to].
func (d *driver) compareAll(from, to uint64) {
	txs, gas := 0, uint64(0)
	var firstMS, lastMS uint64
	for h := from; h <= to; h++ {
		hex := fmt.Sprintf("0x%x", h)
		var ref [2]string
		for i, n := range d.nodes {
			blk := canonical(rpcRaw(n.rpc, `{"jsonrpc":"2.0","id":1,"method":"eth_getBlockByNumber","params":["`+hex+`",true]}`))
			rcp := canonical(rpcRaw(n.rpc, `{"jsonrpc":"2.0","id":1,"method":"eth_getBlockReceipts","params":["`+hex+`"]}`))
			if i == 0 {
				ref = [2]string{blk, rcp}
				var b struct {
					Transactions          []json.RawMessage
					GasUsed               string
					TimestampMilliseconds string
					Timestamp             string
				}
				json.Unmarshal([]byte(blk), &b)
				txs += len(b.Transactions)
				g, _ := strconv.ParseUint(strings.TrimPrefix(b.GasUsed, "0x"), 16, 64)
				gas += g
				ms, err := strconv.ParseUint(strings.TrimPrefix(b.TimestampMilliseconds, "0x"), 16, 64)
				if err != nil {
					sec, _ := strconv.ParseUint(strings.TrimPrefix(b.Timestamp, "0x"), 16, 64)
					ms = sec * 1000
				}
				if h == from {
					firstMS = ms
				}
				lastMS = ms
				continue
			}
			if blk != ref[0] {
				fmt.Printf("DIVERGENCE block %d: %s\n%s\nvs %s\n%s\n", h, d.nodes[0].kind, ref[0], n.kind, blk)
				fail()
			}
			if rcp != ref[1] {
				fmt.Printf("DIVERGENCE receipts %d: %s\n%s\nvs %s\n%s\n", h, d.nodes[0].kind, ref[1], n.kind, rcp)
				fail()
			}
		}
	}
	n := float64(to - from + 1)
	fmt.Printf("blocks %d..%d identical on %d nodes: %d txs, %.0f txs/block, %.1fM gas/block (%.0f%% of the %.0fM limit), %.0f ms between blocks\n",
		from, to, len(d.nodes), txs, float64(txs)/n, float64(gas)/n/1e6, float64(gas)/n/d.gasLimit*100, d.gasLimit/1e6, float64(lastMS-firstMS)/max(n-1, 1))
}

// proposers counts ACCEPTED blocks per builder kind: our plugin logs
// "proposer": "self" when it accepts a block it built; the rest were stock's.
func (d *driver) proposers(dir string, chainID ids.ID, head uint64) map[string]int {
	out := map[string]int{}
	for _, n := range d.nodes {
		if n.kind != "ours" {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(n.DataDir, "logs", chainID.String()+".log"))
		if err != nil {
			continue
		}
		out["ours"] += strings.Count(string(raw), `"proposer": "self"`)
	}
	out["stock"] = int(head) - out["ours"]
	return out
}

// scanStockLogs fails on any invalid-block rejection in a stock node's logs.
func (d *driver) scanStockLogs(dir string, nodes []*node) {
	for _, n := range nodes {
		if n.kind != "stock" {
			continue
		}
		matches, _ := filepath.Glob(filepath.Join(n.DataDir, "logs", "*.log"))
		for _, path := range matches {
			raw, err := os.ReadFile(path)
			if err != nil {
				continue
			}
			for _, line := range strings.Split(string(raw), "\n") {
				l := strings.ToLower(line)
				if strings.Contains(l, "invalid block") || strings.Contains(l, "rejecting block") || strings.Contains(l, "failed to verify block") {
					fmt.Printf("STOCK LOG %s: %s\n", filepath.Base(path), line)
					fail()
				}
			}
		}
	}
	fmt.Println("stock logs: no invalid-block rejections")
}

// loadPhase funds nkeys senders, then `workers` goroutines each sign their
// share of the keys on the fly and post JSON-RPC batches of `batch`
// eth_sendRawTransaction to the nodes round-robin, paced to `rate` tx/s in
// total, for `dur`; a sample line every 10 s.
func (d *driver) loadPhase(funder *ecdsa.PrivateKey, nonce *uint64, nkeys, rate int, dur time.Duration, oursDir string, workers, batch int) {
	keys := make([]*ecdsa.PrivateKey, nkeys)
	var fund []string
	for i := range keys {
		keys[i], _ = crypto.GenerateKey()
		addr := crypto.PubkeyToAddress(keys[i].PublicKey)
		tx := d.sign(funder, *nonce, &addr, new(big.Int).Mul(big.NewInt(params.Ether), big.NewInt(100)), nil, 21000)
		*nonce++
		fund = append(fund, rawTxReq(i, tx))
	}
	// One funder, so chunks of 100 in nonce order to one node, each chunk
	// mined before the next: remote pools cap a sender at 16 pending + 64
	// queued, so a flood of 1000 from one account would be dropped elsewhere.
	for i := 0; i < len(fund); i += 100 {
		end := min(i+100, len(fund))
		var lastHash ethcommon.Hash
		for _, r := range batchPost(d.nodes[0].rpc, fund[i:end]) {
			if r.Error != nil {
				check(fmt.Errorf("%s", r.Error.Message), "fund")
			}
			json.Unmarshal(r.Result, &lastHash)
		}
		d.receipt(d.nodes[0], lastHash)
	}
	fmt.Printf("funded %d senders\n", nkeys)

	var sent, failed atomic.Int64
	start := time.Now()
	deadline := start.Add(dur)
	to := ethcommon.HexToAddress("0x2000000000000000000000000000000000000002")
	perBatch := time.Duration(float64(batch) / (float64(rate) / float64(workers)) * float64(time.Second))
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			var mine []*ecdsa.PrivateKey
			for i := w; i < nkeys; i += workers {
				mine = append(mine, keys[i])
			}
			nonces := make([]uint64, len(mine))
			for iter, k := 0, 0; time.Now().Before(deadline); iter++ {
				t0 := time.Now()
				reqs := make([]string, 0, batch)
				owners := make([]int, 0, batch)
				for i := 0; i < batch; i++ {
					reqs = append(reqs, rawTxReq(i, d.sign(mine[k], nonces[k], &to, big.NewInt(1), nil, 21000)))
					owners = append(owners, k)
					nonces[k]++
					k = (k + 1) % len(mine)
				}
				// A worker sticks to one node (a client does): with the nodes
				// alternating per batch, every second nonce of a key had to
				// cross by gossip before the key was executable, and once
				// admission outran gossip the queue hit its cap and
				// truncateQueue broke the sequences for good (400k queued,
				// 2000 pending, blocks of 2000 then 3 txs).
				nn := len(d.nodes)
				if *rpcNodes > 0 && *rpcNodes < nn {
					nn = *rpcNodes
				}
				n := d.nodes[w%nn]
				bad := map[int]bool{}
				for i, r := range batchPost(n.rpc, reqs) {
					if r.Error != nil {
						failed.Add(1)
						bad[owners[i]] = true
						if failed.Load()%1000 == 1 {
							fmt.Println("send error:", r.Error.Message)
						}
					} else {
						sent.Add(1)
					}
				}
				for k := range bad { // resync a key whose tx was refused
					if pn, err := n.ec.PendingNonceAt(d.ctx, crypto.PubkeyToAddress(mine[k].PublicKey)); err == nil {
						nonces[k] = pn
					}
				}
				if rest := perBatch - time.Since(t0); rest > 0 {
					time.Sleep(rest)
				}
			}
		}(w)
	}

	startHead, _ := d.nodes[0].ec.BlockNumber(d.ctx)
	sample := time.NewTicker(10 * time.Second)
	defer sample.Stop()
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	for running := true; running; {
		select {
		case <-done:
			running = false
		case <-sample.C:
			head, _ := d.nodes[0].ec.BlockNumber(d.ctx)
			blk, _ := d.nodes[0].ec.BlockByNumber(d.ctx, new(big.Int).SetUint64(head))
			ntx, gas := 0, uint64(0)
			if blk != nil {
				ntx, gas = len(blk.Transactions()), blk.GasUsed()
			}
			p, q := d.pool(d.nodes[0])
			fmt.Printf("t=%3.0fs sent=%d failed=%d head=%d (+%d) lastBlock txs=%d gas=%.1fM pool=%d/%d rss=%s go=%s\n",
				time.Since(start).Seconds(), sent.Load(), failed.Load(), head, head-startHead, ntx, float64(gas)/1e6, p, q,
				pluginRSS(oursDir), d.goStats())
		}
	}
	fmt.Printf("load done: sent=%d failed=%d in %s (%.0f tx/s offered)\n", sent.Load(), failed.Load(), dur, float64(sent.Load())/dur.Seconds())
	time.Sleep(5 * time.Second)
	fmt.Println("final ours metrics:", d.goStats())
	d.consensusMetrics()
	d.acceptGaps()
}

// acceptGaps prints the distribution of the time between consecutive
// "validator: accepted" lines on every ours node (the chain log incl. its
// rotated files): bursty acceptance shows as a p50 of milliseconds next to a
// p90 of seconds.
func (d *driver) acceptGaps() {
	re := regexp.MustCompile(`^\[(\d\d-\d\d\|\d\d:\d\d:\d\d\.\d+)\].*validator: accepted`)
	for _, n := range d.nodes {
		if n.kind != "ours" {
			continue
		}
		files, _ := filepath.Glob(filepath.Join(n.DataDir, "logs", network.Subnets[0].Chains[0].ChainID.String()+"*.log"))
		var ts []time.Time
		for _, f := range files {
			raw, err := os.ReadFile(f)
			if err != nil {
				continue
			}
			for _, l := range strings.Split(string(raw), "\n") {
				if m := re.FindStringSubmatch(l); m != nil {
					if t, err := time.Parse("01-02|15:04:05.000", m[1]); err == nil {
						ts = append(ts, t)
					}
				}
			}
		}
		sort.Slice(ts, func(i, j int) bool { return ts[i].Before(ts[j]) })
		var gaps []time.Duration
		var over2 time.Duration
		for i := 1; i < len(ts); i++ {
			g := ts[i].Sub(ts[i-1])
			gaps = append(gaps, g)
			if g > 2*time.Second {
				over2 += g
			}
		}
		if len(gaps) == 0 {
			continue
		}
		sort.Slice(gaps, func(i, j int) bool { return gaps[i] < gaps[j] })
		q := func(f float64) time.Duration { return gaps[min(int(float64(len(gaps))*f), len(gaps)-1)] }
		fmt.Printf("accept gaps %s: n=%d span=%s p50=%s p90=%s p99=%s max=%s; gaps>2s sum %s\n", n.NodeID, len(gaps),
			ts[len(ts)-1].Sub(ts[0]).Round(time.Millisecond), q(.5).Round(time.Millisecond), q(.9).Round(time.Millisecond),
			q(.99).Round(time.Millisecond), gaps[len(gaps)-1].Round(time.Millisecond), over2.Round(time.Millisecond))
	}
}

// consensusMetrics prints, per node, the avalanchego counters that tell
// where a block waits between issue and accept: snowman polls and
// processing set, the handler's lock and queue, the network timeouts and the
// inbound byte throttler.
func (d *driver) consensusMetrics() {
	re := regexp.MustCompile(`_(polls_successful|polls_failed|blks_processing|blks_build_accept_latency_(sum|count)|handler_locking_time|blks_accepted_(sum|count)|blks_rejected_count|byte_throttler_inbound_acquire_latency_sum|bandwidth_throttler_inbound_acquire_latency_sum|requests_(current_timeout|average_latency|timeouts|pending_timeouts))( |\{)`)
	for _, n := range d.nodes {
		resp, err := http.Get(n.GetAccessibleURI() + "/ext/metrics")
		if err != nil {
			continue
		}
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		fmt.Printf("consensus metrics %s %s:\n", n.kind, n.NodeID)
		for _, l := range strings.Split(string(raw), "\n") {
			if re.MatchString(l) && !strings.HasPrefix(l, "#") {
				fmt.Println("  ", l)
			}
		}
	}
}

func rawTxReq(id int, tx *types.Transaction) string {
	raw, _ := tx.MarshalBinary()
	return fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"method":"eth_sendRawTransaction","params":["0x%x"]}`, id, raw)
}

type rpcResp struct {
	Result json.RawMessage           `json:"result"`
	Error  *struct{ Message string } `json:"error"`
}

// batchPost sends one JSON-RPC batch; a transport failure counts every
// element as failed.
func batchPost(url string, reqs []string) []rpcResp {
	resp, err := http.Post(url, "application/json", strings.NewReader("["+strings.Join(reqs, ",")+"]"))
	out := make([]rpcResp, len(reqs))
	if err != nil {
		for i := range out {
			out[i].Error = &struct{ Message string }{err.Error()}
		}
		return out
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if err := json.Unmarshal(raw, &out); err != nil {
		for i := range out {
			out[i].Error = &struct{ Message string }{fmt.Sprintf("bad batch response: %.120s", raw)}
		}
	}
	return out
}

// pluginRSS: VmRSS of our plugin processes (exe under dir) whose parent
// avalanchego belongs to this network (another network may share the dir).
func pluginRSS(dir string) string {
	out, err := exec.Command("pgrep", "-f", filepath.Join(dir, subnetEVMID)).Output()
	if err != nil {
		return "?"
	}
	var parts []string
	for _, pid := range strings.Fields(string(out)) {
		st, err := os.ReadFile("/proc/" + pid + "/status")
		if err != nil {
			continue
		}
		if ppid := field(string(st), "PPid:"); ppid != "" {
			if cmd, _ := os.ReadFile("/proc/" + ppid + "/cmdline"); !strings.Contains(string(cmd), network.Dir) {
				continue
			}
		}
		for _, l := range strings.Split(string(st), "\n") {
			if strings.HasPrefix(l, "VmRSS:") {
				kb, _ := strconv.Atoi(strings.Fields(l)[1])
				parts = append(parts, fmt.Sprintf("%dMB", kb>>10))
			}
		}
	}
	sort.Strings(parts)
	return strings.Join(parts, ",")
}

func field(status, key string) string {
	for _, l := range strings.Split(status, "\n") {
		if strings.HasPrefix(l, key) {
			return strings.Fields(l)[1]
		}
	}
	return ""
}

// goStats pulls our plugin's Go heap and GC share plus verify/build p50 from
// the first node's /ext/metrics.
func (d *driver) goStats() string {
	resp, err := http.Get(d.nodes[0].GetAccessibleURI() + "/ext/metrics")
	if err != nil {
		return "?"
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var heap, gcFrac, rss, verifyCount, verifySum, buildCount, buildSum, xTotal float64
	var verifyB, buildB []bucket
	for _, l := range strings.Split(string(raw), "\n") {
		f := strings.Fields(l)
		if len(f) != 2 || !strings.Contains(l, "epochdb") {
			continue
		}
		v, _ := strconv.ParseFloat(f[1], 64)
		if i := strings.IndexByte(f[0], '{'); i >= 0 {
			if le := leOf(f[0][i:]); le != "" {
				b := bucket{count: v}
				b.le, _ = strconv.ParseFloat(le, 64)
				if strings.Contains(f[0], "epochdb_verify_seconds_bucket") {
					verifyB = append(verifyB, b)
				} else if strings.Contains(f[0], "epochdb_build_seconds_bucket") {
					buildB = append(buildB, b)
				}
			}
			f[0] = f[0][:i] // avalanche_subnetevm_vm_epochdb_<name>{chain=...}
		}
		switch {
		case strings.HasSuffix(f[0], "epochdb_go_heap_alloc_bytes"):
			heap = v
		case strings.HasSuffix(f[0], "epochdb_go_gc_cpu_fraction"):
			gcFrac = v
		case strings.HasSuffix(f[0], "epochdb_process_rss_bytes"):
			rss = v
		case strings.HasSuffix(f[0], "epochdb_verify_seconds_count"):
			verifyCount = v
		case strings.HasSuffix(f[0], "epochdb_verify_seconds_sum"):
			verifySum = v
		case strings.HasSuffix(f[0], "epochdb_build_seconds_count"):
			buildCount = v
		case strings.HasSuffix(f[0], "epochdb_build_seconds_sum"):
			buildSum = v
		case strings.Contains(f[0], "epochdb_crossings_total"):
			xTotal += v
		}
	}
	avg := func(s, c float64) string {
		if c == 0 {
			return "-"
		}
		return fmt.Sprintf("%.1fms", s/c*1000)
	}
	v50, v99 := quantiles(verifyB)
	b50, b99 := quantiles(buildB)
	return fmt.Sprintf("heap=%.0fMB gc=%.3f rss=%.0fMB verify avg=%s p50=%.1fms p99=%.1fms (n=%.0f) build avg=%s p50=%.1fms p99=%.1fms (n=%.0f) crossings/block=%.1f",
		heap/1e6, gcFrac, rss/1e6, avg(verifySum, verifyCount), v50*1000, v99*1000, verifyCount,
		avg(buildSum, buildCount), b50*1000, b99*1000, buildCount, xTotal/max(verifyCount, 1))
}

type bucket struct{ le, count float64 }

func leOf(labels string) string {
	i := strings.Index(labels, `le="`)
	if i < 0 {
		return ""
	}
	rest := labels[i+4:]
	return rest[:strings.IndexByte(rest, '"')]
}

// quantiles: p50 and p99 (seconds) by linear interpolation over the
// cumulative histogram buckets (+Inf clamps to the last finite bound).
func quantiles(b []bucket) (p50, p99 float64) {
	if len(b) == 0 {
		return 0, 0
	}
	sort.Slice(b, func(i, j int) bool { return b[i].le < b[j].le })
	total := b[len(b)-1].count
	q := func(f float64) float64 {
		target := f * total
		prevLe, prevCount := 0.0, 0.0
		for _, x := range b {
			if x.count >= target {
				if isInf(x.le) {
					return prevLe
				}
				if x.count == prevCount {
					return x.le
				}
				return prevLe + (x.le-prevLe)*(target-prevCount)/(x.count-prevCount)
			}
			prevLe, prevCount = x.le, x.count
		}
		return prevLe
	}
	return q(0.5), q(0.99)
}

func isInf(f float64) bool { return f > 1e300 }

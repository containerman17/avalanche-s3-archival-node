package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/snow"
	"github.com/ava-labs/avalanchego/snow/consensus/snowman"
	"github.com/ava-labs/libevm/common"
	ethtypes "github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/crypto"
	"github.com/ava-labs/libevm/rlp"

	"github.com/containerman17/avalanche-s3-archival-node/chain"
)

// Block generator mode: a private local chain, the real plugin, no corpus.
// Prefill grows state for a while, then each measured block is built by the
// VM from txs the host signs, and Verify/Accept are timed one block at a time.
// State is what it is after prefill; nothing is faked outside the VM.

const (
	genChainID  = 0xbe4c // 48716
	genSenders  = 1024
	genGasLimit = 500_000_000
	// Token (gen_token.sol) selectors.
	selTransfer     = "a9059cbb"
	selApprove      = "095ea7b3"
	selTransferFrom = "23b872dd"
	selMint         = "40c10f19"
	selMintMany     = "9579f5d1"
	holdersPerMint  = 500 // mintMany holders per prefill tx (12.6 M gas, fits a 20 M block)
	mintsPerBlock   = 20  // mintMany txs per prefill block (state growth)
	// slotWriter runtime: calldata start, count, salt; sstore(start+i, start+i+salt)
	// for i in [0, count). Init code copies it and returns it. The Clear Street
	// shape: a precompile-like tx that is nearly all state writes.
	slotWriterInit = "6022" + "80" + "600b" + "6000" + "39" + "6000" + "f3" +
		"602035" + "600035" + "5b" + "8115" + "6020" + "57" + "8080" + "604035" + "01" + "90" + "55" + "600101" + "90600190" + "03" + "90" + "6006" + "56" + "5b00"
	slotsPerTx   = 50
	slotsPerGrow = 1000 // fresh slots per prefill grow tx (slots kind)
)

// genChain writes chain.json for the private chain when it is absent.
func genChain(dataDir string, networkID uint32) (string, error) {
	blockchainID := ids.ID(sha256.Sum256([]byte("epochdb-blockbench-chain")))
	subnetID := ids.ID(sha256.Sum256([]byte("epochdb-blockbench-subnet")))
	path := filepath.Join(dataDir, "chain.json")
	if _, err := os.Stat(path); err == nil {
		return blockchainID.String(), nil
	}
	alloc := map[string]any{}
	for i := 0; i < genSenders; i++ {
		alloc[crypto.PubkeyToAddress(genKey(i).PublicKey).Hex()] = map[string]string{"balance": "0x52B7D2DCC80CD2E4000000"}
	}
	genesis := map[string]any{
		"config": map[string]any{
			"chainId": genChainID, "initialMinDelayMS": 1,
			"feeConfig": map[string]any{
				"gasLimit": genGasLimit, "minBaseFee": 1_000_000_000, "targetGas": genGasLimit * 100,
				"baseFeeChangeDenominator": 48, "minBlockGasCost": 0, "maxBlockGasCost": 0,
				"targetBlockRate": 2, "blockGasCostStep": 0,
			},
		},
		"alloc":      alloc,
		"nonce":      "0x0",
		"timestamp":  "0x5FCB13D0",
		"extraData":  "0x00",
		"gasLimit":   fmt.Sprintf("0x%x", genGasLimit),
		"difficulty": "0x0",
		"mixHash":    "0x0000000000000000000000000000000000000000000000000000000000000000",
		"coinbase":   "0x0000000000000000000000000000000000000000",
		"number":     "0x0",
		"gasUsed":    "0x0",
		"parentHash": "0x0000000000000000000000000000000000000000000000000000000000000000",
	}
	genesisJSON, err := json.Marshal(genesis)
	if err != nil {
		return "", err
	}
	cached, err := json.Marshal(map[string]any{
		"networkID": networkID, "blockchainID": blockchainID.String(), "subnetID": subnetID.String(),
		"vmKind": "subnetevm", "genesisData": genesisJSON,
	})
	if err != nil {
		return "", err
	}
	return blockchainID.String(), os.WriteFile(path, cached, 0o644)
}

func genKey(i int) *ecdsa.PrivateKey {
	h := crypto.Keccak256([]byte(fmt.Sprintf("epochdb-blockbench-sender-%d", i)))
	k, err := crypto.ToECDSA(h)
	if err != nil {
		panic(err)
	}
	return k
}

// genState is what a finished prefill leaves in the data dir, so later runs
// skip setup and prefill and go straight to timed blocks on the grown state.
type genState struct {
	Kind     string         `json:"kind"`
	Contract common.Address `json:"contract"`
	Slots    uint64         `json:"slots"`
	Holders  uint64         `json:"holders"`
	Nonces   []uint64       `json:"nonces"`
	Rng      uint64         `json:"rng"`
}

func genStatePath(dataDir string) string { return filepath.Join(dataDir, "gen-state.json") }

func (g *gen) save(dataDir string) error {
	raw, err := json.Marshal(genState{Kind: g.kind, Contract: g.contract, Slots: g.slots, Holders: g.holders, Nonces: g.nonces, Rng: g.rng})
	if err != nil {
		return err
	}
	return os.WriteFile(genStatePath(dataDir), raw, 0o644)
}

// gen is the generator state: signers, nonces, and how much state exists.
type gen struct {
	vm       *plugin      // nil in remote mode
	rpc      http.Handler // the plugin's /rpc, or remoteRPC
	chainID  *big.Int
	signer   ethtypes.Signer
	keys     []*ecdsa.PrivateKey
	nonces   []uint64
	kind     string   // "token" or "slots"
	corpus   *os.File // optional EPCORP01 recording of every accepted block
	contract common.Address
	head     uint64 // remote mode: last height counted by awaitBlock
	holders  uint64 // token holders seeded by mintMany so far (recipients)
	slots    uint64 // slots kind: slots written so far in the slot writer
	rng      uint64
}

func (g *gen) next() uint64 {
	// splitmix64
	g.rng += 0x9e3779b97f4a7c15
	z := g.rng
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}

// holder returns the address mintMany(seed, n, amount) gave index i:
// address(uint160(keccak256(abi.encode(seed, i)))).
func holder(seed, i uint64) common.Address {
	var b [64]byte
	binary.BigEndian.PutUint64(b[24:32], seed)
	binary.BigEndian.PutUint64(b[56:64], i)
	return common.BytesToAddress(crypto.Keccak256(b[:]))
}

// holderAt maps a flat holder index to (seed, i).
func holderAt(idx uint64) common.Address { return holder(idx/holdersPerMint, idx%holdersPerMint) }

func word(v uint64) []byte {
	var b [32]byte
	binary.BigEndian.PutUint64(b[24:], v)
	return b[:]
}

func call(sel string, args ...[]byte) []byte {
	data := common.FromHex(sel)
	for _, a := range args {
		data = append(data, common.LeftPadBytes(a, 32)...)
	}
	return data
}

func (g *gen) sign(i int, to *common.Address, value *big.Int, gas uint64, data []byte) []byte {
	tx := ethtypes.NewTx(&ethtypes.DynamicFeeTx{
		ChainID: g.chainID, Nonce: g.nonces[i], GasTipCap: big.NewInt(1_000_000_000), GasFeeCap: big.NewInt(1_000_000_000_000),
		Gas: gas, To: to, Value: value, Data: data,
	})
	g.nonces[i]++
	signed, err := ethtypes.SignTx(tx, g.signer, g.keys[i])
	if err != nil {
		panic(err)
	}
	raw, err := signed.MarshalBinary()
	if err != nil {
		panic(err)
	}
	return raw
}

// submit sends raw txs to the plugin's own eth_sendRawTransaction in batches
// of 1000 (the server's batch limit).
func (g *gen) submit(ctx context.Context, raws [][]byte) error {
	for len(raws) > 1000 {
		if err := g.submit(ctx, raws[:1000]); err != nil {
			return err
		}
		raws = raws[1000:]
	}
	var sb strings.Builder
	sb.WriteByte('[')
	for i, raw := range raws {
		if i > 0 {
			sb.WriteByte(',')
		}
		fmt.Fprintf(&sb, `{"jsonrpc":"2.0","id":%d,"method":"eth_sendRawTransaction","params":["0x%x"]}`, i, raw)
	}
	sb.WriteByte(']')
	req := httptest.NewRequest(http.MethodPost, "/rpc", strings.NewReader(sb.String())).WithContext(ctx)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	g.rpc.ServeHTTP(rec, req)
	var results []struct {
		Error json.RawMessage `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &results); err != nil {
		return fmt.Errorf("sendRawTransaction batch: %w: %.200s", err, rec.Body.String())
	}
	for _, r := range results {
		if len(r.Error) != 0 {
			return fmt.Errorf("sendRawTransaction: %s", r.Error)
		}
	}
	return nil
}

// traffic signs n realistic token txs from random senders: 60% transfer to a
// random holder, 20% approve, 20% transferFrom (each sender was approved by
// its predecessor at setup). Every tx reads owner/paused/fee/treasury, two
// frozen flags, and two or three balances, and writes two or three slots.
func (g *gen) traffic(n int) [][]byte {
	if g.kind == "slots" {
		return g.slotTraffic(n)
	}
	raws := make([][]byte, 0, n)
	amount := word(1_000_000_000_000)
	for i := 0; i < n; i++ {
		s := int(g.next() % uint64(len(g.keys)))
		to := holderAt(g.next() % g.holders)
		switch r := g.next() % 10; {
		case r < 6:
			raws = append(raws, g.sign(s, &g.contract, nil, 120_000, call(selTransfer, to.Bytes(), amount)))
		case r < 8:
			spender := crypto.PubkeyToAddress(g.keys[(s+1)%len(g.keys)].PublicKey)
			raws = append(raws, g.sign(s, &g.contract, nil, 80_000, call(selApprove, spender.Bytes(), word(g.next()))))
		default:
			from := crypto.PubkeyToAddress(g.keys[(s+len(g.keys)-1)%len(g.keys)].PublicKey)
			raws = append(raws, g.sign(s, &g.contract, nil, 140_000, call(selTransferFrom, from.Bytes(), to.Bytes(), amount)))
		}
	}
	return raws
}

// grow signs mintsPerBlock mintMany txs from the owner, each seeding
// holdersPerMint fresh holders.
func (g *gen) grow() [][]byte {
	if g.kind == "slots" {
		return g.slotGrow()
	}
	raws := make([][]byte, 0, mintsPerBlock)
	for i := 0; i < mintsPerBlock; i++ {
		seed := g.holders / holdersPerMint
		raws = append(raws, g.sign(0, &g.contract, nil, 60_000+holdersPerMint*25_000, call(selMintMany, word(seed), word(holdersPerMint), word(1_000_000_000_000_000_000))))
		g.holders += holdersPerMint
	}
	return raws
}

// pending returns the tx pool's pending count via txpool_status.
func (g *gen) pending(ctx context.Context) (uint64, error) {
	req := httptest.NewRequest(http.MethodPost, "/rpc", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"txpool_status","params":[]}`)).WithContext(ctx)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	g.rpc.ServeHTTP(rec, req)
	var res struct {
		Result struct {
			Pending string `json:"pending"`
			Queued  string `json:"queued"`
		} `json:"result"`
		Error json.RawMessage `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &res); err != nil {
		return 0, fmt.Errorf("txpool_status: %w", err)
	}
	if len(res.Error) != 0 {
		return 0, fmt.Errorf("txpool_status: %s", res.Error)
	}
	p, err := strconv.ParseUint(strings.TrimPrefix(res.Result.Pending, "0x"), 16, 64)
	if err != nil {
		return 0, err
	}
	q, err := strconv.ParseUint(strings.TrimPrefix(res.Result.Queued, "0x"), 16, 64)
	return p + q, err
}

// loadNonces (remote mode): the senders' chain nonces, so a rerun against a
// used chain signs from where it left off.
func (g *gen) loadNonces(ctx context.Context) error {
	var sb strings.Builder
	sb.WriteByte('[')
	for i := range g.keys {
		if i > 0 {
			sb.WriteByte(',')
		}
		fmt.Fprintf(&sb, `{"jsonrpc":"2.0","id":%d,"method":"eth_getTransactionCount","params":["%s","pending"]}`, i, crypto.PubkeyToAddress(g.keys[i].PublicKey).Hex())
	}
	sb.WriteByte(']')
	req := httptest.NewRequest(http.MethodPost, "/rpc", strings.NewReader(sb.String())).WithContext(ctx)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	g.rpc.ServeHTTP(rec, req)
	var results []struct {
		ID     int    `json:"id"`
		Result string `json:"result"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &results); err != nil {
		return fmt.Errorf("getTransactionCount batch: %w: %.200s", err, rec.Body.String())
	}
	for _, r := range results {
		n, err := strconv.ParseUint(strings.TrimPrefix(r.Result, "0x"), 16, 64)
		if err != nil {
			return err
		}
		g.nonces[r.ID] = n
	}
	return nil
}

// drain mines until the pool has no pending txs; returns blocks, txs, gas mined.
func (g *gen) drain(ctx context.Context) (blocks, txs, gas uint64, err error) {
	for {
		n, err := g.pending(ctx)
		if err != nil || n == 0 {
			return blocks, txs, gas, err
		}
		blk, _, err := g.mine(ctx)
		if err != nil {
			return blocks, txs, gas, err
		}
		blocks++
		txs += blk.txs
		gas += blk.gas
	}
}

// slotCall encodes slotWriter(start, count, salt).
func slotCall(start, count, salt uint64) []byte {
	return call("", word(start), word(count), word(salt))
}

// slotTraffic: n txs each rewriting slotsPerTx existing random-offset slots.
func (g *gen) slotTraffic(n int) [][]byte {
	raws := make([][]byte, 0, n)
	for i := 0; i < n; i++ {
		s := int(g.next() % uint64(len(g.keys)))
		start := g.next() % (g.slots - slotsPerTx)
		raws = append(raws, g.sign(s, &g.contract, nil, 21000+slotsPerTx*25_000+10_000, slotCall(start, slotsPerTx, g.next())))
	}
	return raws
}

// slotGrow: mintsPerBlock txs each writing slotsPerGrow fresh slots.
func (g *gen) slotGrow() [][]byte {
	raws := make([][]byte, 0, mintsPerBlock)
	for i := 0; i < mintsPerBlock; i++ {
		raws = append(raws, g.sign(0, &g.contract, nil, 60_000+slotsPerGrow*25_000, slotCall(g.slots, slotsPerGrow, 1)))
		g.slots += slotsPerGrow
	}
	return raws
}

// mine builds one block from the mempool, then verifies and accepts it.
// Returns the block and the three wall durations.
// mined is what the generator needs to know about an accepted block.
type mined struct{ height, txs, gas uint64 }

func (g *gen) mine(ctx context.Context) (*mined, [3]time.Duration, error) {
	var d [3]time.Duration
	if g.vm == nil {
		return g.awaitBlock(ctx)
	}
	// ACP-226 minimum block delay: on a network where Granite activates
	// after genesis the delay is 2 s until the builder lowers it; wait it
	// out rather than count it as build time.
	var blk snowman.Block
	var err error
	for {
		t0 := time.Now()
		blk, err = g.vm.vm.BuildBlock(ctx)
		d[0] = time.Since(t0)
		if err == nil {
			break
		}
		if strings.Contains(err.Error(), "minimum block delay not met") {
			time.Sleep(20 * time.Millisecond)
			continue
		}
		return nil, d, fmt.Errorf("BuildBlock: %w", err)
	}
	t1 := time.Now()
	if err := blk.Verify(ctx); err != nil {
		return nil, d, fmt.Errorf("Verify: %w", err)
	}
	d[1] = time.Since(t1)
	t2 := time.Now()
	if err := blk.Accept(ctx); err != nil {
		return nil, d, fmt.Errorf("Accept: %w", err)
	}
	d[2] = time.Since(t2)
	if err := g.vm.vm.SetPreference(ctx, blk.ID()); err != nil {
		return nil, d, err
	}
	var eth ethtypes.Block
	if err := rlp.DecodeBytes(blk.Bytes(), &eth); err != nil {
		return nil, d, err
	}
	if g.corpus != nil {
		var frame [12]byte
		binary.BigEndian.PutUint64(frame[:8], eth.NumberU64())
		binary.BigEndian.PutUint32(frame[8:], uint32(len(blk.Bytes())))
		if _, err := g.corpus.Write(append(frame[:], blk.Bytes()...)); err != nil {
			return nil, d, err
		}
	}
	return &mined{eth.NumberU64(), uint64(len(eth.Transactions())), eth.GasUsed()}, d, nil
}

// remoteRPC is an http.Handler that forwards the request body to a live
// node's /rpc, so submit and pending work unchanged against a real network.
type remoteRPC string

func (u remoteRPC) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	resp, err := http.Post(string(u), "application/json", r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}

// rpcResult posts one JSON-RPC call and returns its result.
func (g *gen) rpcResult(ctx context.Context, method, params string) (json.RawMessage, error) {
	req := httptest.NewRequest(http.MethodPost, "/rpc", strings.NewReader(fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":"%s","params":%s}`, method, params))).WithContext(ctx)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	g.rpc.ServeHTTP(rec, req)
	var res struct {
		Result json.RawMessage `json:"result"`
		Error  json.RawMessage `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &res); err != nil {
		return nil, fmt.Errorf("%s: %w: %.200s", method, err, rec.Body.String())
	}
	if len(res.Error) != 0 {
		return nil, fmt.Errorf("%s: %s", method, res.Error)
	}
	return res.Result, nil
}

func (g *gen) rpcUint(ctx context.Context, method, params string) (uint64, error) {
	raw, err := g.rpcResult(ctx, method, params)
	if err != nil {
		return 0, err
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return 0, fmt.Errorf("%s: %w", method, err)
	}
	return strconv.ParseUint(strings.TrimPrefix(s, "0x"), 16, 64)
}

// awaitBlock (remote mode): wait until the network has accepted at least one
// block past g.head, then count every new block; d[0] is the wait.
func (g *gen) awaitBlock(ctx context.Context) (*mined, [3]time.Duration, error) {
	var d [3]time.Duration
	t0 := time.Now()
	h, err := g.rpcUint(ctx, "eth_blockNumber", "[]")
	if err != nil {
		return nil, d, err
	}
	for h <= g.head {
		time.Sleep(50 * time.Millisecond)
		if h, err = g.rpcUint(ctx, "eth_blockNumber", "[]"); err != nil {
			return nil, d, err
		}
	}
	d[0] = time.Since(t0)
	m := &mined{height: h}
	for n := g.head + 1; n <= h; n++ {
		raw, err := g.rpcResult(ctx, "eth_getBlockByNumber", fmt.Sprintf(`["0x%x",false]`, n))
		if err != nil {
			return nil, d, err
		}
		var blk struct {
			GasUsed      string   `json:"gasUsed"`
			Transactions []string `json:"transactions"`
		}
		if err := json.Unmarshal(raw, &blk); err != nil {
			return nil, d, err
		}
		gas, err := strconv.ParseUint(strings.TrimPrefix(blk.GasUsed, "0x"), 16, 64)
		if err != nil {
			return nil, d, err
		}
		m.txs += uint64(len(blk.Transactions))
		m.gas += gas
	}
	g.head = h
	return m, d, nil
}

// fund (remote mode): the funder (ewoq) sends every sender 1000 coins, then
// the chain drains.
func (g *gen) fund(ctx context.Context, funder *ecdsa.PrivateKey) error {
	from := crypto.PubkeyToAddress(funder.PublicKey)
	nonce, err := g.rpcUint(ctx, "eth_getTransactionCount", fmt.Sprintf(`["%s","pending"]`, from.Hex()))
	if err != nil {
		return err
	}
	amount := new(big.Int).Mul(big.NewInt(1000), big.NewInt(1e18))
	raws := make([][]byte, 0, genSenders)
	for i := 0; i < genSenders; i++ {
		to := crypto.PubkeyToAddress(g.keys[i].PublicKey)
		tx := ethtypes.NewTx(&ethtypes.DynamicFeeTx{ChainID: g.chainID, Nonce: nonce, GasTipCap: big.NewInt(1_000_000_000), GasFeeCap: big.NewInt(1_000_000_000_000), Gas: 21000, To: &to, Value: amount})
		nonce++
		signed, err := ethtypes.SignTx(tx, g.signer, funder)
		if err != nil {
			return err
		}
		raw, err := signed.MarshalBinary()
		if err != nil {
			return err
		}
		raws = append(raws, raw)
	}
	if err := g.submit(ctx, raws); err != nil {
		return err
	}
	b, t, _, err := g.drain(ctx)
	log.Printf("gen funded %d senders in %d blocks (%d txs)", genSenders, b, t)
	return err
}

// setupAndPrefill deploys the token, funds and approves the senders, grows
// holders for prefillFor, and saves the generator state for later runs.
func (g *gen) setupAndPrefill(ctx context.Context, dataDir string, prefillFor time.Duration, prefillBatch int) error {
	// Deploy the token (treasury = sender 1023, fee 25 bps), fund every sender,
	// and let every sender approve its successor: setup blocks, not timed.
	treasury := crypto.PubkeyToAddress(g.keys[genSenders-1].PublicKey)
	init := append(common.FromHex(tokenBin), call("", treasury.Bytes(), word(25))...)
	if g.kind == "slots" {
		init = common.FromHex(slotWriterInit)
	}
	g.contract = crypto.CreateAddress(crypto.PubkeyToAddress(g.keys[0].PublicKey), g.nonces[0])
	if err := g.submit(ctx, [][]byte{g.sign(0, nil, nil, 2_000_000, init)}); err != nil {
		return err
	}
	blk, d, err := g.mine(ctx)
	if err != nil {
		return err
	}
	if blk.txs != 1 || blk.gas == 0 {
		return fmt.Errorf("deploy block has %d txs, gas %d", blk.txs, blk.gas)
	}
	log.Printf("gen deployed %s contract at %s height=%d build=%s verify=%s accept=%s vm_pid=%d", g.kind, g.contract, blk.height, d[0], d[1], d[2], g.pid())
	if g.kind != "slots" {
		setup := make([][]byte, 0, 2*genSenders)
		for i := 0; i < genSenders; i++ {
			to := crypto.PubkeyToAddress(g.keys[i].PublicKey)
			setup = append(setup, g.sign(0, &g.contract, nil, 80_000, call(selMint, to.Bytes(), word(1<<62))))
		}
		for i := 0; i < genSenders; i++ {
			spender := crypto.PubkeyToAddress(g.keys[(i+1)%genSenders].PublicKey)
			setup = append(setup, g.sign(i, &g.contract, nil, 80_000, call(selApprove, spender.Bytes(), word(1<<62))))
		}
		if err := g.submit(ctx, setup); err != nil {
			return err
		}
		// One block locally (500 M gas); several on a live chain.
		if _, txs, _, err := g.drain(ctx); err != nil {
			return err
		} else if txs != uint64(len(setup)) {
			return fmt.Errorf("setup blocks have %d txs, wanted %d", txs, len(setup))
		}
	}
	// Prefill: each block grows holders and carries traffic.
	if err := g.submit(ctx, g.grow()); err != nil {
		return err
	}
	if blk, _, err = g.mine(ctx); err != nil {
		return err
	}
	if blk.gas == 0 {
		return errors.New("first mintMany block used no gas")
	}
	// Prefill.
	start := time.Now()
	var blocks, txs, gas uint64
	for time.Since(start) < prefillFor {
		if err := g.submit(ctx, append(g.grow(), g.traffic(prefillBatch)...)); err != nil {
			return err
		}
		b, t, ga, err := g.drain(ctx)
		if err != nil {
			return err
		}
		blocks, txs, gas = blocks+b, txs+t, gas+ga
	}
	el := time.Since(start)
	log.Printf("gen prefill done in %.1fs: blocks=%d txs=%d gas=%d mgas/s=%.1f holders=%d slots=%d senders=%d", el.Seconds(), blocks, txs, gas, float64(gas)/1e6/el.Seconds(), g.holders, g.slots, genSenders)

	return g.save(dataDir)
}

// runGen: prefill for prefillFor, then one timed block per entry of sizes.
func runGen(ctx context.Context, c *chain.Chain, vmPath, dataDir, configPath, kind string, prefillFor time.Duration, prefillBatch int, sizes, corpusOut string) error {
	if kind != "token" && kind != "slots" {
		return fmt.Errorf("--gen-kind %q: want token or slots", kind)
	}
	configBytes, err := os.ReadFile(configPath)
	if err != nil {
		return err
	}
	pl, err := openPlugin(ctx, c, nil, vmPath, dataDir, configBytes)
	if err != nil {
		return err
	}
	defer pl.close()
	if err := pl.vm.SetState(ctx, snow.Bootstrapping); err != nil {
		return err
	}
	if err := pl.vm.SetState(ctx, snow.NormalOp); err != nil {
		return err
	}
	handlers, err := pl.vm.CreateHandlers(ctx)
	if err != nil {
		return err
	}
	if handlers["/rpc"] == nil {
		return errors.New("plugin has no /rpc handler")
	}
	lastID, err := pl.vm.LastAccepted(ctx)
	if err != nil {
		return err
	}
	last, err := pl.vm.GetBlock(ctx, lastID)
	if err != nil {
		return err
	}
	return runGenOn(ctx, pl, handlers["/rpc"], big.NewInt(genChainID), last.Height(), dataDir, kind, prefillFor, prefillBatch, sizes, corpusOut)
}

// runGenRemote: the same workloads against a live node's /rpc. The network
// mines; the funder (ewoq, funded in the e2e genesis) pays the senders.
func runGenRemote(ctx context.Context, rpcURL, dataDir, kind string, prefillFor time.Duration, prefillBatch int, sizes string) error {
	g := &gen{rpc: remoteRPC(rpcURL)}
	chainID, err := g.rpcUint(ctx, "eth_chainId", "[]")
	if err != nil {
		return err
	}
	height, err := g.rpcUint(ctx, "eth_blockNumber", "[]")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return err
	}
	return runGenOn(ctx, nil, g.rpc, new(big.Int).SetUint64(chainID), height, dataDir, kind, prefillFor, prefillBatch, sizes, "")
}

func (g *gen) pid() int64 {
	if g.vm == nil {
		return 0
	}
	return g.vm.tracker.pid.Load()
}

func runGenOn(ctx context.Context, pl *plugin, rpc http.Handler, chainID *big.Int, height uint64, dataDir, kind string, prefillFor time.Duration, prefillBatch int, sizes, corpusOut string) error {
	var err error
	var corpus *os.File
	if corpusOut != "" {
		corpus, err = os.OpenFile(corpusOut, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
		if err != nil {
			return err
		}
		defer corpus.Close()
		if _, err := corpus.WriteString(corpusMagic); err != nil {
			return err
		}
	}
	g := &gen{kind: kind, corpus: corpus, vm: pl, rpc: rpc, chainID: chainID, head: height, signer: ethtypes.LatestSignerForChainID(chainID), nonces: make([]uint64, genSenders)}
	for i := 0; i < genSenders; i++ {
		g.keys = append(g.keys, genKey(i))
	}
	if raw, err := os.ReadFile(genStatePath(dataDir)); err == nil {
		var st genState
		if err := json.Unmarshal(raw, &st); err != nil {
			return err
		}
		g.kind, g.contract, g.slots, g.holders, g.nonces, g.rng = st.Kind, st.Contract, st.Slots, st.Holders, st.Nonces, st.Rng
		log.Printf("gen resumed: kind=%s height=%d holders=%d slots=%d senders=%d vm_pid=%d (prefill skipped)", g.kind, height, g.holders, g.slots, genSenders, g.pid())
	} else if height != 0 && pl != nil {
		return fmt.Errorf("data dir at height %d without %s", height, genStatePath(dataDir))
	} else {
		if pl == nil {
			funder, err := crypto.HexToECDSA("56289e99c94b6912bfc12adc093c9b51124f0dc54ac7a766b2bc5ccf558d8027") // ewoq
			if err != nil {
				return err
			}
			if err := g.fund(ctx, funder); err != nil {
				return err
			}
			if err := g.loadNonces(ctx); err != nil {
				return err
			}
		}
		if err := g.setupAndPrefill(ctx, dataDir, prefillFor, prefillBatch); err != nil {
			return err
		}
	}
	// Measured blocks: existing state, reads and writes.
	for _, f := range strings.Split(sizes, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(f))
		if err != nil {
			return fmt.Errorf("--gen-sizes: %w", err)
		}
		tSubmit := time.Now()
		if err := g.submit(ctx, g.traffic(n)); err != nil {
			return err
		}
		submit := time.Since(tSubmit)
		blk, d, err := g.mine(ctx)
		if err != nil {
			return err
		}
		total := d[0] + d[1] + d[2]
		log.Printf("gen block height=%d txs=%d gas=%d submit=%s build=%s verify=%s accept=%s total=%s verify_mgas/s=%.1f", blk.height, blk.txs, blk.gas, submit, d[0], d[1], d[2], total, float64(blk.gas)/1e6/d[1].Seconds())
		if blk.txs != uint64(n) {
			log.Printf("gen note: asked %d txs, block took %d; draining", n, blk.txs)
			if _, _, _, err := g.drain(ctx); err != nil {
				return err
			}
		}
		if err := g.save(dataDir); err != nil {
			return err
		}
	}
	return nil
}

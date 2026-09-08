package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
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
	"github.com/ava-labs/avalanchego/utils/constants"
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
	holdersPerMint  = 1000 // mintMany holders per prefill tx
	mintsPerBlock   = 20   // mintMany txs per prefill block (state growth)
)

// genChain writes chain.json for the private chain when it is absent.
func genChain(dataDir string) (string, error) {
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
				"gasLimit": genGasLimit, "minBaseFee": 1_000_000_000, "targetGas": genGasLimit * 2,
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
		"networkID": constants.LocalID, "blockchainID": blockchainID.String(), "subnetID": subnetID.String(),
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
	Contract common.Address `json:"contract"`
	Holders  uint64         `json:"holders"`
	Nonces   []uint64       `json:"nonces"`
	Rng      uint64         `json:"rng"`
}

func genStatePath(dataDir string) string { return filepath.Join(dataDir, "gen-state.json") }

func (g *gen) save(dataDir string) error {
	raw, err := json.Marshal(genState{Contract: g.contract, Holders: g.holders, Nonces: g.nonces, Rng: g.rng})
	if err != nil {
		return err
	}
	return os.WriteFile(genStatePath(dataDir), raw, 0o644)
}

// gen is the generator state: signers, nonces, and how much state exists.
type gen struct {
	vm       *plugin
	rpc      http.Handler
	signer   ethtypes.Signer
	keys     []*ecdsa.PrivateKey
	nonces   []uint64
	contract common.Address
	holders  uint64 // token holders seeded by mintMany so far (recipients)
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
		ChainID: big.NewInt(genChainID), Nonce: g.nonces[i], GasTipCap: big.NewInt(1_000_000_000), GasFeeCap: big.NewInt(5_000_000_000),
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
	raws := make([][]byte, 0, mintsPerBlock)
	for i := 0; i < mintsPerBlock; i++ {
		seed := g.holders / holdersPerMint
		raws = append(raws, g.sign(0, &g.contract, nil, 60_000+holdersPerMint*25_000, call(selMintMany, word(seed), word(holdersPerMint), word(1_000_000_000_000_000_000))))
		g.holders += holdersPerMint
	}
	return raws
}

// mine builds one block from the mempool, then verifies and accepts it.
// Returns the block and the three wall durations.
func (g *gen) mine(ctx context.Context) (*ethtypes.Block, [3]time.Duration, error) {
	var d [3]time.Duration
	t0 := time.Now()
	blk, err := g.vm.vm.BuildBlock(ctx)
	if err != nil {
		return nil, d, fmt.Errorf("BuildBlock: %w", err)
	}
	d[0] = time.Since(t0)
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
	return &eth, d, nil
}

// setupAndPrefill deploys the token, funds and approves the senders, grows
// holders for prefillFor, and saves the generator state for later runs.
func (g *gen) setupAndPrefill(ctx context.Context, dataDir string, prefillFor time.Duration, prefillBatch int) error {
	// Deploy the token (treasury = sender 1023, fee 25 bps), fund every sender,
	// and let every sender approve its successor: setup blocks, not timed.
	treasury := crypto.PubkeyToAddress(g.keys[genSenders-1].PublicKey)
	init := append(common.FromHex(tokenBin), call("", treasury.Bytes(), word(25))...)
	if err := g.submit(ctx, [][]byte{g.sign(0, nil, nil, 2_000_000, init)}); err != nil {
		return err
	}
	blk, d, err := g.mine(ctx)
	if err != nil {
		return err
	}
	if len(blk.Transactions()) != 1 || blk.GasUsed() == 0 {
		return fmt.Errorf("deploy block has %d txs, gas %d", len(blk.Transactions()), blk.GasUsed())
	}
	g.contract = crypto.CreateAddress(crypto.PubkeyToAddress(g.keys[0].PublicKey), 0)
	log.Printf("gen deployed token at %s height=%d build=%s verify=%s accept=%s vm_pid=%d", g.contract, blk.NumberU64(), d[0], d[1], d[2], g.vm.tracker.pid.Load())
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
	if blk, _, err = g.mine(ctx); err != nil {
		return err
	}
	if len(blk.Transactions()) != len(setup) {
		return fmt.Errorf("setup block has %d txs, wanted %d", len(blk.Transactions()), len(setup))
	}
	// Prefill: each block grows holders and carries traffic.
	if err := g.submit(ctx, g.grow()); err != nil {
		return err
	}
	if blk, _, err = g.mine(ctx); err != nil {
		return err
	}
	if blk.GasUsed() == 0 {
		return errors.New("first mintMany block used no gas")
	}
	// Prefill.
	start := time.Now()
	var blocks, txs, gas uint64
	for time.Since(start) < prefillFor {
		if err := g.submit(ctx, append(g.grow(), g.traffic(prefillBatch)...)); err != nil {
			return err
		}
		blk, _, err = g.mine(ctx)
		if err != nil {
			return err
		}
		blocks++
		txs += uint64(len(blk.Transactions()))
		gas += blk.GasUsed()
		if blk.Transactions().Len() < prefillBatch+mintsPerBlock {
			// The VM left txs in the pool (block gas limit); drain before the next batch.
			for blk.GasUsed() > genGasLimit*9/10 {
				if blk, _, err = g.mine(ctx); err != nil {
					return err
				}
				blocks++
				txs += uint64(len(blk.Transactions()))
				gas += blk.GasUsed()
			}
		}
	}
	el := time.Since(start)
	log.Printf("gen prefill done in %.1fs: blocks=%d txs=%d gas=%d mgas/s=%.1f holders=%d senders=%d height=%d", el.Seconds(), blocks, txs, gas, float64(gas)/1e6/el.Seconds(), g.holders, genSenders, blk.NumberU64())

	return g.save(dataDir)
}

// runGen: prefill for prefillFor, then one timed block per entry of sizes.
func runGen(ctx context.Context, c *chain.Chain, vmPath, dataDir, configPath string, prefillFor time.Duration, prefillBatch int, sizes string) error {
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
	g := &gen{vm: pl, rpc: handlers["/rpc"], signer: ethtypes.LatestSignerForChainID(big.NewInt(genChainID)), nonces: make([]uint64, genSenders)}
	for i := 0; i < genSenders; i++ {
		g.keys = append(g.keys, genKey(i))
	}
	lastID, err := pl.vm.LastAccepted(ctx)
	if err != nil {
		return err
	}
	last, err := pl.vm.GetBlock(ctx, lastID)
	if err != nil {
		return err
	}
	if raw, err := os.ReadFile(genStatePath(dataDir)); err == nil {
		var st genState
		if err := json.Unmarshal(raw, &st); err != nil {
			return err
		}
		g.contract, g.holders, g.nonces, g.rng = st.Contract, st.Holders, st.Nonces, st.Rng
		log.Printf("gen resumed: height=%d holders=%d senders=%d vm_pid=%d (prefill skipped)", last.Height(), g.holders, genSenders, pl.tracker.pid.Load())
	} else if last.Height() != 0 {
		return fmt.Errorf("data dir at height %d without %s", last.Height(), genStatePath(dataDir))
	} else {
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
		log.Printf("gen block height=%d txs=%d gas=%d submit=%s build=%s verify=%s accept=%s total=%s verify_mgas/s=%.1f", blk.NumberU64(), len(blk.Transactions()), blk.GasUsed(), submit, d[0], d[1], d[2], total, float64(blk.GasUsed())/1e6/d[1].Seconds())
		if len(blk.Transactions()) != n {
			log.Printf("gen note: asked %d txs, block took %d (gas limit %d); draining", n, len(blk.Transactions()), genGasLimit)
			for blk.GasUsed() > genGasLimit*9/10 {
				if blk, _, err = g.mine(ctx); err != nil {
					return err
				}
			}
		}
		if err := g.save(dataDir); err != nil {
			return err
		}
	}
	return nil
}

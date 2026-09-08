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
	genSenders  = 64
	genGasLimit = 500_000_000
	// slotWriter runtime: calldata start, count, salt; sstore(start+i, start+i+salt)
	// for i in [0, count). Init code copies it and returns it.
	slotWriterInit = "6022" + "80" + "600b" + "6000" + "39" + "6000" + "f3" +
		"602035" + "600035" + "5b" + "8115" + "6020" + "57" + "8080" + "604035" + "01" + "90" + "55" + "600101" + "90600190" + "03" + "90" + "6006" + "56" + "5b00"
	slotsPerTx = 50
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

// gen is the generator state: signers, nonces, and how much state exists.
type gen struct {
	vm       *plugin
	rpc      http.Handler
	signer   ethtypes.Signer
	keys     []*ecdsa.PrivateKey
	nonces   []uint64
	contract common.Address
	accounts uint64 // fresh accounts created so far (transfer targets)
	slots    uint64 // slots written so far in the contract
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

// account returns the address of fresh account n.
func genAccount(n uint64) common.Address {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], n)
	return common.BytesToAddress(crypto.Keccak256([]byte("epochdb-blockbench-account"), b[:]))
}

// txs signs n txs: half transfers, half slot writes. fresh=true creates new
// accounts and slots; fresh=false rewrites existing ones (reads then writes).
func (g *gen) txs(n int, fresh bool) [][]byte {
	raws := make([][]byte, 0, n)
	salt := big.NewInt(time.Now().UnixNano())
	for i := 0; i < n; i++ {
		s := i % len(g.keys)
		if i%2 == 0 || g.contract == (common.Address{}) {
			var idx uint64
			if fresh || g.accounts == 0 {
				idx = g.accounts
				g.accounts++
			} else {
				idx = uint64(i) % g.accounts
			}
			to := genAccount(idx)
			raws = append(raws, g.sign(s, &to, big.NewInt(1), 21000, nil))
			continue
		}
		var start uint64
		if fresh || g.slots == 0 {
			start = g.slots
			g.slots += slotsPerTx
		} else {
			start = (uint64(i) * slotsPerTx) % g.slots
		}
		data := make([]byte, 96)
		binary.BigEndian.PutUint64(data[24:32], start)
		binary.BigEndian.PutUint64(data[56:64], slotsPerTx)
		salt.FillBytes(data[64:96])
		raws = append(raws, g.sign(s, &g.contract, nil, 21000+slotsPerTx*25_000+10_000, data))
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
	if last.Height() != 0 {
		return fmt.Errorf("generator wants a fresh data dir, found height %d", last.Height())
	}
	// Deploy the slot writer.
	init := common.FromHex(slotWriterInit)
	if err := g.submit(ctx, [][]byte{g.sign(0, nil, nil, 200_000, init)}); err != nil {
		return err
	}
	blk, d, err := g.mine(ctx)
	if err != nil {
		return err
	}
	if len(blk.Transactions()) != 1 {
		return fmt.Errorf("deploy block has %d txs", len(blk.Transactions()))
	}
	g.contract = crypto.CreateAddress(crypto.PubkeyToAddress(g.keys[0].PublicKey), 0)
	log.Printf("gen deployed slot writer at %s height=%d build=%s verify=%s accept=%s vm_pid=%d", g.contract, blk.NumberU64(), d[0], d[1], d[2], pl.tracker.pid.Load())

	// Prefill.
	start := time.Now()
	var blocks, txs, gas uint64
	for time.Since(start) < prefillFor {
		if err := g.submit(ctx, g.txs(prefillBatch, true)); err != nil {
			return err
		}
		blk, _, err = g.mine(ctx)
		if err != nil {
			return err
		}
		blocks++
		txs += uint64(len(blk.Transactions()))
		gas += blk.GasUsed()
		if blk.Transactions().Len() < prefillBatch {
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
	log.Printf("gen prefill done in %.1fs: blocks=%d txs=%d gas=%d mgas/s=%.1f accounts=%d slots=%d height=%d", el.Seconds(), blocks, txs, gas, float64(gas)/1e6/el.Seconds(), g.accounts, g.slots, blk.NumberU64())

	// Measured blocks: existing state, reads and writes.
	for _, f := range strings.Split(sizes, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(f))
		if err != nil {
			return fmt.Errorf("--gen-sizes: %w", err)
		}
		tSubmit := time.Now()
		if err := g.submit(ctx, g.txs(n, false)); err != nil {
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
	}
	return nil
}

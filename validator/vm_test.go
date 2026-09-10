package validator

import (
	"context"
	"crypto/ecdsa"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/snow"
	"github.com/ava-labs/avalanchego/snow/engine/common"
	"github.com/ava-labs/avalanchego/snow/engine/enginetest"
	"github.com/ava-labs/avalanchego/snow/snowtest"
	"github.com/ava-labs/avalanchego/utils/logging"
	ethcommon "github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/common/hexutil"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/crypto"
	"github.com/ava-labs/libevm/params"
	"github.com/holiman/uint256"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// ewoq: the subnet-evm test key, funded in testGenesis.
const ewoqKey = "56289e99c94b6912bfc12adc093c9b51124f0dc54ac7a766b2bc5ccf558d8027"

const testGenesis = `{
  "config": {
    "chainId": 99999, "homesteadBlock": 0, "eip150Block": 0, "eip155Block": 0, "eip158Block": 0,
    "byzantiumBlock": 0, "constantinopleBlock": 0, "petersburgBlock": 0, "istanbulBlock": 0, "muirGlacierBlock": 0,
    "subnetEVMTimestamp": 0,
    "feeConfig": {"gasLimit": 20000000, "minBaseFee": 1000000000, "targetGas": 100000000, "baseFeeChangeDenominator": 48,
      "minBlockGasCost": 0, "maxBlockGasCost": 10000000, "targetBlockRate": 2, "blockGasCostStep": 500000},
    "allowFeeRecipients": false
  },
  "alloc": {"8db97C7cEcE249c2b98bDC0226Cc4C2A57BF52FC": {"balance": "0x52B7D2DCC80CD2E4000000"}},
  "nonce": "0x0", "timestamp": "0x0", "extraData": "0x00", "gasLimit": "0x1312d00", "difficulty": "0x0",
  "mixHash": "0x0000000000000000000000000000000000000000000000000000000000000000",
  "coinbase": "0x0000000000000000000000000000000000000000", "number": "0x0", "gasUsed": "0x0",
  "parentHash": "0x0000000000000000000000000000000000000000000000000000000000000000"
}`

type harness struct {
	t      *testing.T
	vm     *VM
	rpc    http.Handler
	key    *ecdsa.PrivateKey
	addr   ethcommon.Address
	nonce  uint64
	signer types.Signer
}

func newHarness(t *testing.T) *harness {
	return newHarnessWith(t, testGenesis, `{}`)
}

// newHarnessWith: a harness on the given genesis JSON and chain config; the
// VM's log goes to stdout (visible under -v).
func newHarnessWith(t *testing.T, genesisJSON, config string) *harness {
	t.Helper()
	genesis := []byte(genesisJSON)
	prepareEngine(genesis)
	ctx := snowtest.Context(t, ids.GenerateTestID())
	ctx.ChainDataDir = t.TempDir()
	ctx.Log = logging.NewLogger("", logging.NewWrappedCore(logging.Info, os.Stdout, logging.Plain.ConsoleEncoder()))
	sender := &enginetest.Sender{T: t}
	sender.Default(false)
	vm := &VM{}
	if err := vm.Initialize(context.Background(), ctx, nil, genesis, nil, []byte(config), nil, sender); err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	t.Cleanup(func() { vm.Shutdown(context.Background()) })
	if err := vm.SetState(context.Background(), snow.NormalOp); err != nil {
		t.Fatalf("SetState: %v", err)
	}
	handlers, err := vm.CreateHandlers(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	key, _ := crypto.HexToECDSA(ewoqKey)
	return &harness{t: t, vm: vm, rpc: handlers["/rpc"], key: key, addr: crypto.PubkeyToAddress(key.PublicKey),
		signer: types.LatestSigner(vm.config)}
}

func (h *harness) call(method string, params ...any) json.RawMessage {
	h.t.Helper()
	body, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
	rec := httptest.NewRecorder()
	h.rpc.ServeHTTP(rec, httptest.NewRequest("POST", "/rpc", strings.NewReader(string(body))))
	var resp struct {
		Result json.RawMessage           `json:"result"`
		Error  *struct{ Message string } `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		h.t.Fatalf("%s: bad response %q: %v", method, rec.Body.String(), err)
	}
	if resp.Error != nil {
		h.t.Fatalf("%s: %s", method, resp.Error.Message)
	}
	return resp.Result
}

// transfer signs and submits one 1-wei transfer to `to` at the next nonce.
func (h *harness) transfer(to ethcommon.Address, value *big.Int) ethcommon.Hash {
	h.t.Helper()
	tx := types.MustSignNewTx(h.key, h.signer, &types.DynamicFeeTx{
		ChainID: h.vm.config.ChainID, Nonce: h.nonce, To: &to, Value: value, Gas: 21000,
		GasFeeCap: big.NewInt(100 * params.GWei), GasTipCap: big.NewInt(params.GWei),
	})
	h.nonce++
	raw, _ := tx.MarshalBinary()
	res := h.call("eth_sendRawTransaction", hexutil.Bytes(raw))
	if !realEngine {
		return tx.Hash() // the stub's rpc echoes the request
	}
	var hash ethcommon.Hash
	if err := json.Unmarshal(res, &hash); err != nil {
		h.t.Fatal(err)
	}
	return hash
}

// buildAccept waits for PendingTxs, builds, verifies and accepts one block.
func (h *harness) buildAccept() (*Block, time.Duration, time.Duration) {
	h.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	msg, err := h.vm.WaitForEvent(ctx)
	if err != nil || msg != common.PendingTxs {
		h.t.Fatalf("WaitForEvent: %v %v", msg, err)
	}
	t0 := time.Now()
	blk, err := h.vm.BuildBlock(ctx)
	if err != nil {
		h.t.Fatalf("BuildBlock: %v", err)
	}
	build := time.Since(t0)
	t1 := time.Now()
	if err := blk.Verify(ctx); err != nil {
		h.t.Fatalf("Verify: %v", err)
	}
	verify := time.Since(t1)
	if err := h.vm.SetPreference(ctx, blk.ID()); err != nil {
		h.t.Fatal(err)
	}
	if err := blk.Accept(ctx); err != nil {
		h.t.Fatalf("Accept: %v", err)
	}
	return blk.(*Block), build, verify
}

func TestBuildVerifyAccept(t *testing.T) {
	h := newHarness(t)
	to := ethcommon.HexToAddress("0x1000000000000000000000000000000000000001")
	const n = 20
	for i := 0; i < n; i++ {
		h.transfer(to, big.NewInt(1))
	}
	if realEngine {
		var pending hexutil.Uint64
		if err := json.Unmarshal(h.call("eth_getTransactionCount", h.addr, "pending"), &pending); err != nil {
			t.Fatal(err)
		}
		if pending != n {
			t.Fatalf("pending nonce %d, want %d", pending, n)
		}
	}
	blk, build, verify := h.buildAccept()
	t.Logf("block %d: build %v verify %v crossings %v", blk.Height(), build, verify, h.vm.eng.snapshot())
	if blk.Height() != 1 {
		t.Fatalf("height %d", blk.Height())
	}
	if last, err := h.vm.LastAccepted(context.Background()); err != nil || last != blk.ID() {
		t.Fatalf("LastAccepted %s %v, want %s", last, err, blk.ID())
	}
	if got, err := h.vm.GetBlockIDAtHeight(context.Background(), 1); err != nil || got != blk.ID() {
		t.Fatalf("GetBlockIDAtHeight: %s %v", got, err)
	}
	if !realEngine {
		return
	}
	var bal hexutil.Big
	if err := json.Unmarshal(h.call("eth_getBalance", to, "latest"), &bal); err != nil {
		t.Fatal(err)
	}
	if bal.ToInt().Int64() != n {
		t.Fatalf("recipient balance %s, want %d", bal.ToInt(), n)
	}
	var num hexutil.Uint64
	json.Unmarshal(h.call("eth_blockNumber"), &num)
	if num != 1 {
		t.Fatalf("eth_blockNumber %d", num)
	}
	// Accept moved the pool: nothing pending.
	if p, q := h.vm.eng.poolStatus(); p+q != 0 {
		t.Fatalf("pool still holds %d pending %d queued", p, q)
	}
	got, err := h.vm.GetBlock(context.Background(), blk.ID())
	if err != nil || string(got.Bytes()) != string(blk.Bytes()) {
		t.Fatalf("GetBlock round trip: %v", err)
	}
}

// TestLatencyByBlockSize reports build + verify latency per block size.
// TestLatencyByBlockSize: one sender, blocks of 50..5000 transfers on a
// 500 M gas genesis with the pool caps raised; the per-build phase split is
// the "validator: built" log line.
func TestLatencyByBlockSize(t *testing.T) {
	if !realEngine {
		t.Skip("stub engine")
	}
	h := newHarnessWith(t, stressGenesis(),
		`{"tx-pool-account-slots": 10000, "tx-pool-global-slots": 20000, "tx-pool-account-queue": 10000, "tx-pool-global-queue": 20000}`)
	to := ethcommon.HexToAddress("0x1000000000000000000000000000000000000002")
	for _, n := range []int{50, 200, 1000, 5000} {
		for i := 0; i < n; i++ {
			h.transfer(to, big.NewInt(1))
		}
		before := h.vm.eng.snapshot()
		engBefore := histSum(h.vm.m.build)
		blk, build, verify := h.buildAccept()
		after := h.vm.eng.snapshot()
		engBuild := time.Duration((histSum(h.vm.m.build) - engBefore) * float64(time.Second))
		var x []string
		for i := range after {
			if d := after[i] - before[i]; d > 0 {
				x = append(x, fmt.Sprintf("%s=%d", crossingNames[i], d))
			}
		}
		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)
		t.Logf("txs=%d height=%d build=%v (engine %v) verify=%v crossings: %s | go heap=%dMB numGC=%d gcCPU=%.4f", n, blk.Height(), build, engBuild, verify,
			strings.Join(x, " "), ms.HeapAlloc>>20, ms.NumGC, ms.GCCPUFraction)
	}
}

// histSum reads a histogram's sample sum (seconds).
func histSum(h prometheus.Histogram) float64 {
	m := new(dto.Metric)
	h.Write(m)
	return m.GetHistogram().GetSampleSum()
}

func TestRPCForward(t *testing.T) {
	h := newHarness(t)
	if !realEngine {
		stubSet(1, []byte(`[{"jsonrpc":"2.0","id":1,"result":{"pending":"0x0","queued":"0x0"}},{"jsonrpc":"2.0","id":2,"result":"0x1869f"}]`))
	} else {
		var id hexutil.Big
		if res := h.call("eth_chainId"); json.Unmarshal(res, &id) != nil || id.ToInt().Int64() != 99999 {
			t.Fatalf("eth_chainId = %s", res)
		}
	}
	// A batch with a pool method and an engine method is answered whole by the engine.
	rec := httptest.NewRecorder()
	h.rpc.ServeHTTP(rec, httptest.NewRequest("POST", "/rpc", strings.NewReader(
		`[{"jsonrpc":"2.0","id":1,"method":"txpool_status","params":[]},{"jsonrpc":"2.0","id":2,"method":"eth_chainId","params":[]}]`)))
	var parts []json.RawMessage
	if err := json.Unmarshal(rec.Body.Bytes(), &parts); err != nil || len(parts) != 2 {
		t.Fatalf("batch: %q %v", rec.Body.String(), err)
	}
	if !strings.Contains(string(parts[0]), `"pending"`) {
		t.Fatalf("txpool_status: %s", parts[0])
	}
}

// TestAccountStateAtOldID: what the engine answers for an accepted, no
// longer head block id (the pool asks for it when its reset lags a head).
// Anything but the historical state or an error would make the pool drop a
// sender's txs as unfunded.
func TestAccountStateAtOldID(t *testing.T) {
	if !realEngine {
		t.Skip("stub engine")
	}
	h := newHarness(t)
	to := ethcommon.HexToAddress("0x1000000000000000000000000000000000000003")
	h.transfer(to, big.NewInt(1))
	b1, _, _ := h.buildAccept()
	h.transfer(to, big.NewInt(1))
	h.buildAccept()
	raw, err := h.vm.eng.accountState([]ethcommon.Address{h.addr}, b1.ID())
	if err != nil {
		t.Logf("account_state at old accepted id: error (Go falls back to the head): %v", err)
		return
	}
	acc := decodeAccount(raw)
	t.Logf("account_state at old accepted id: nonce=%d balance=%s", acc.Nonce, acc.Balance)
	if acc.Balance.IsZero() {
		t.Fatalf("funded sender reads as empty at an old accepted id")
	}
}

// TestPoolDrainsUnderChurn: many senders, nonces arriving out of order and
// through both doors (RPC and the gossip set), a block per round; after the
// last block every tx is mined and the pool is empty. Logs engine nonce vs
// pool nonce per round.
func TestPoolDrainsUnderChurn(t *testing.T) {
	if !realEngine {
		t.Skip("stub engine")
	}
	h := newHarness(t)
	const nkeys, rounds, perRound = 200, 12, 3
	keys := make([]*ecdsa.PrivateKey, nkeys)
	addrs := make([]ethcommon.Address, nkeys)
	nonces := make([]uint64, nkeys)
	for i := range keys {
		keys[i], _ = crypto.GenerateKey()
		addrs[i] = crypto.PubkeyToAddress(keys[i].PublicKey)
		h.transfer(addrs[i], new(big.Int).Mul(big.NewInt(params.Ether), big.NewInt(10)))
	}
	h.buildAccept()
	to := ethcommon.HexToAddress("0x1000000000000000000000000000000000000004")
	set, err := newGossipSet(h.vm.eng, prometheus.NewRegistry())
	if err != nil {
		t.Fatal(err)
	}
	sent := 0
	for r := 0; r < rounds; r++ {
		for k := range keys {
			txs := make([]*types.Transaction, perRound)
			for j := range txs {
				txs[j] = types.MustSignNewTx(keys[k], h.signer, &types.DynamicFeeTx{
					ChainID: h.vm.config.ChainID, Nonce: nonces[k] + uint64(j), To: &to, Value: big.NewInt(1), Gas: 21000,
					GasFeeCap: big.NewInt(50 * params.GWei), GasTipCap: big.NewInt(params.GWei)})
			}
			nonces[k] += perRound
			// highest nonce first (queued), the rest through the gossip door and RPC
			raw, _ := txs[perRound-1].MarshalBinary()
			if err := set.Add(newGossipTx(raw)); err != nil {
				t.Fatalf("round %d key %d: gossip add: %v", r, k, err)
			}
			for j := 0; j < perRound-1; j++ {
				raw, _ := txs[j].MarshalBinary()
				h.call("eth_sendRawTransaction", hexutil.Bytes(raw))
			}
			sent += perRound
		}
		blk, _, _ := h.buildAccept()
		p, q := h.vm.eng.poolStatus()
		raw, err := h.vm.eng.accountState(addrs[:3], ids.Empty)
		if err != nil {
			t.Fatal(err)
		}
		var view []string
		for i := 0; i < 3; i++ {
			acc := decodeAccount(raw[i*40 : i*40+40])
			pn, _ := h.vm.eng.poolNonce(addrs[i])
			view = append(view, fmt.Sprintf("%d:engine=%d pool=%d", i, acc.Nonce, pn))
		}
		t.Logf("round %d: block %d, pool pending=%d queued=%d, nonces %s", r, blk.Height(), p, q, strings.Join(view, " "))
	}
	// Drain whatever the last block left (the gas budget can split a round).
	for i := 0; i < 5; i++ {
		if p, q := h.vm.eng.poolStatus(); p+q == 0 {
			break
		}
		h.buildAccept()
	}
	if p, q := h.vm.eng.poolStatus(); p+q != 0 {
		t.Fatalf("pool did not drain: pending=%d queued=%d", p, q)
	}
	var nonce hexutil.Uint64
	json.Unmarshal(h.call("eth_getTransactionCount", addrs[0], "latest"), &nonce)
	if uint64(nonce) != rounds*perRound {
		t.Fatalf("sender 0 nonce %d, want %d", nonce, rounds*perRound)
	}
	t.Logf("%d txs from %d senders mined", sent, nkeys)
}

// decodeAccount: one epochdb_account_state entry (u64 LE nonce, 32-byte BE balance).
func decodeAccount(b []byte) types.StateAccount {
	var nonce uint64
	for i := 7; i >= 0; i-- {
		nonce = nonce<<8 | uint64(b[i])
	}
	return types.StateAccount{Nonce: nonce, Balance: new(uint256.Int).SetBytes32(b[8:40])}
}

// stressGenesis is testGenesis with a 500 M gas limit in BOTH the fee config
// and the header: the engine refuses a mismatch, as stock does.
func stressGenesis() string {
	g := strings.Replace(testGenesis, `"gasLimit": 20000000`, `"gasLimit": 500000000`, 1)
	return strings.Replace(g, `"gasLimit": "0x1312d00"`, `"gasLimit": "0x1dcd6500"`, 1)
}

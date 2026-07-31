// Package sevmrpc exists only to hold this test, for the same reason
// exec/sevmexec and fetch/sevm do: libevm's extras registry is process-global
// and PANICS on re-registration, package rpc's own tests register coreth, and
// `go test` gives every package its own process. So a subnet-evm RPC test
// cannot live in package rpc, and this is the isolation it needs.
//
// WHAT THIS PROVES: a synthetic subnet-evm chain, built by SUBNET-EVM'S OWN
// block builder, executed by the real exec.Executor against real Firewood, and
// then SERVED through the real rpc.Server, answers with real values on every
// executing method (eth_call against a deployed contract's storage,
// eth_getBalance, tx-by-hash, receipt, getLogs, feeHistory with reward
// percentiles, and a debug trace), and that every method in the dispatch table
// either works or returns a NAMED error. Before the rpc seam, all of those
// executing methods panicked on this chain.
//
// WHAT IT DOES NOT PROVE: nothing here comes from the network (no real FIFA
// bytes, no ProposerVM-wrapped containers, no Warp predicates in header.Extra,
// no sealed epochs, no long chain and no moving tip). The live gate is a real
// FIFA serve.
package sevmrpc

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http/httptest"
	"strings"
	"testing"

	sevmdummy "github.com/ava-labs/avalanchego/graft/subnet-evm/consensus/dummy"
	sevmcore "github.com/ava-labs/avalanchego/graft/subnet-evm/core"
	sevmparams "github.com/ava-labs/avalanchego/graft/subnet-evm/params"
	sevmextras "github.com/ava-labs/avalanchego/graft/subnet-evm/params/extras"
	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/snow"
	"github.com/ava-labs/avalanchego/upgrade"
	avaconstants "github.com/ava-labs/avalanchego/utils/constants"
	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/common/hexutil"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/crypto"
	"github.com/ava-labs/libevm/rlp"

	"github.com/containerman17/epochdb/chain"
	"github.com/containerman17/epochdb/exec"
	"github.com/containerman17/epochdb/rpc"
	"github.com/containerman17/epochdb/state"
)

// The chain under test is shaped like FIFA, exactly as exec/sevmexec's is: a
// mainnet L1 with its own ids, NativeMinter in genesis at time 0, and a
// genesis timestamp past every scheduled mainnet upgrade.
const (
	testChainID = 13322
	genesisTime = 1767225600 // 2026-01-01T00:00:00Z
	blockGap    = 2

	// The chain generated below, block by block:
	blkTransfer = 1 // a plain value transfer
	blkEmpty    = 2 // no transactions
	blkDeploy   = 3 // the storage contract's creation
	blkCall     = 4 // a call to it: reads storage, emits one log
	blkTail     = 5 // two more transfers, so head is not a special case
	headBlock   = blkTail

	transferValue = 1_000
	tailValue     = 7
	storedValue   = 0x2a // what the contract's constructor SSTOREs at slot 0
)

var (
	funded    = mustKey("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	recipient = common.HexToAddress("0x00000000000000000000000000000000000000c0")

	// logTopic is the single topic the contract's LOG1 emits.
	logTopic = common.HexToHash("0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef")
)

// storageContract is hand-written EVM, so the test carries no compiler and no
// checked-in artifact.
//
// Constructor: SSTORE(0, 0x2a), then return the 49-byte runtime at offset 0x10.
//
//	602a 6000 55        PUSH1 0x2a; PUSH1 0; SSTORE
//	6031 80 6010 6000 39  PUSH1 49; DUP1; PUSH1 16; PUSH1 0; CODECOPY
//	6000 f3             PUSH1 0; RETURN
//
// Runtime, on every call: load slot 0, log it, return it. Both, unconditionally,
// so ONE body serves eth_call (which reads the return value) and a real
// transaction (which produces the log).
//
//	6000 54 6000 52     PUSH1 0; SLOAD; PUSH1 0; MSTORE   mem[0:32] = slot0
//	7f<topic>           PUSH32 topic
//	6020 6000 a1        PUSH1 32; PUSH1 0; LOG1           log(mem[0:32], topic)
//	6020 6000 f3        PUSH1 32; PUSH1 0; RETURN         return mem[0:32]
var storageContract = common.FromHex(
	"602a60005560318060106000396000f3" +
		"600054600052" + "7f" + strings.TrimPrefix(logTopic.Hex(), "0x") +
		"60206000a160206000f3")

func mustKey(hex string) *ecdsa.PrivateKey {
	k, err := crypto.HexToECDSA(hex)
	if err != nil {
		panic(err)
	}
	return k
}

func fundedAddr() common.Address { return crypto.PubkeyToAddress(funded.PublicKey) }

func genesisJSON() []byte {
	addr := fundedAddr().Hex()
	return []byte(fmt.Sprintf(`{
	  "config": {
	    "chainId": %d,
	    "feeConfig": {
	      "gasLimit": 8000000,
	      "targetBlockRate": 2,
	      "minBaseFee": 25000000000,
	      "targetGas": 15000000,
	      "baseFeeChangeDenominator": 36,
	      "minBlockGasCost": 0,
	      "maxBlockGasCost": 1000000,
	      "blockGasCostStep": 200000
	    },
	    "subnetEVMTimestamp": 0,
	    "contractNativeMinterConfig": {
	      "blockTimestamp": 0,
	      "adminAddresses": ["%s"]
	    }
	  },
	  "alloc": {
	    "%s": {"balance": "0x52B7D2DCC80CD2E4000000"}
	  },
	  "nonce": "0x0",
	  "timestamp": "0x%x",
	  "extraData": "0x",
	  "gasLimit": "0x7A1200",
	  "difficulty": "0x0",
	  "mixHash": "0x0000000000000000000000000000000000000000000000000000000000000000",
	  "coinbase": "0x0000000000000000000000000000000000000000",
	  "number": "0x0",
	  "gasUsed": "0x0",
	  "parentHash": "0x0000000000000000000000000000000000000000000000000000000000000000"
	}`, testChainID, addr, addr[2:], genesisTime))
}

func testChain() *chain.Chain {
	return &chain.Chain{
		GenesisJSON:  genesisJSON(),
		NetworkID:    avaconstants.MainnetID,
		SubnetID:     ids.ID{0xf1, 0xfa},
		BlockchainID: ids.ID{0xf1, 0xfa, 0xb1},
		VMKind:       chain.SubnetEVM,
	}
}

// referenceGenesis parses the descriptor's bytes the way subnet-evm's own
// plugin/evm parseGenesis does, INDEPENDENTLY of exec, so the reference blocks
// are not generated from whatever exec happens to build.
func referenceGenesis(t *testing.T, c *chain.Chain) *sevmcore.Genesis {
	t.Helper()
	g := new(sevmcore.Genesis)
	if err := json.Unmarshal(c.GenesisJSON, g); err != nil {
		t.Fatalf("reference genesis: %v", err)
	}
	cx := sevmparams.GetExtra(g.Config)
	cx.AvalancheContext = sevmextras.AvalancheContext{SnowCtx: &snow.Context{
		NetworkID:       c.NetworkID,
		SubnetID:        c.SubnetID,
		ChainID:         c.BlockchainID,
		NetworkUpgrades: upgrade.GetConfig(c.NetworkID),
	}}
	cx.SetDefaults(upgrade.GetConfig(c.NetworkID))
	if err := g.Verify(); err != nil {
		t.Fatalf("reference genesis verify: %v", err)
	}
	if err := sevmparams.SetEthUpgrades(g.Config); err != nil {
		t.Fatalf("reference eth upgrades: %v", err)
	}
	return g
}

// generateChain builds the reference blocks with subnet-evm's OWN builder,
// which runs its own StateProcessor to fill every header.Root.
func generateChain(t *testing.T, g *sevmcore.Genesis) []*types.Block {
	t.Helper()
	engine := sevmdummy.NewFakerWithMode(sevmdummy.Mode{
		ModeSkipHeader: true, ModeSkipBlockFee: true, ModeSkipCoinbase: true,
	})
	chainID := g.Config.ChainID
	contract := crypto.CreateAddress(fundedAddr(), 1) // nonce 1 = the deploy tx

	_, blocks, _, err := sevmcore.GenerateChainWithGenesis(g, engine, headBlock, blockGap, func(i int, b *sevmcore.BlockGen) {
		sign := func(data types.TxData) *types.Transaction {
			tx, err := types.SignNewTx(funded, b.Signer(), data)
			if err != nil {
				t.Fatalf("sign tx: %v", err)
			}
			return tx
		}
		tx := func(to *common.Address, gas uint64, value int64, data []byte) *types.Transaction {
			return sign(&types.DynamicFeeTx{
				ChainID: chainID, Nonce: b.TxNonce(fundedAddr()),
				GasTipCap: common.Big0, GasFeeCap: b.BaseFee(),
				Gas: gas, To: to, Value: big.NewInt(value), Data: data,
			})
		}
		switch i + 1 {
		case blkTransfer:
			b.AddTx(tx(&recipient, 21_000, transferValue, nil))
		case blkEmpty:
		case blkDeploy:
			b.AddTx(tx(nil, 500_000, 0, storageContract))
		case blkCall:
			b.AddTx(tx(&contract, 200_000, 0, nil))
		case blkTail:
			b.AddTx(tx(&recipient, 21_000, tailValue, nil))
			b.AddTx(tx(&recipient, 21_000, tailValue, nil))
		}
	})
	if err != nil {
		t.Fatalf("generate reference chain: %v", err)
	}
	return blocks
}

// source is exec.BlockSource / rpc.BlockSource over pre-ProposerVM raw block
// RLP, which is the second path exec.ParseEthBlock decodes.
type source map[uint64][]byte

func (s source) GetByHeight(n uint64) ([]byte, bool, error) {
	raw, ok := s[n]
	return raw, ok, nil
}

// allHeights is the tx-hash candidate walk, newest first, offering EVERY
// height. The real source is the fp48 fingerprint index, which is fed by the
// fetch staging sidecars a network sync writes and this test has none of; it
// is also entirely VM-independent (it hashes hashes), and it is gated on the
// C-chain corpus and in package state. What this substitution keeps is the
// part rpc owns and that a subnet-evm chain could break: the container read,
// exec.ParseEthBlock on subnet-evm block RLP, and the full-hash compare inside
// rpc.findTx.
type allHeights uint64

func (h allHeights) WalkCandidates(_ common.Hash, fn func(uint64) (bool, error)) error {
	for n := uint64(h); n >= 1; n-- {
		stop, err := fn(n)
		if err != nil || stop {
			return err
		}
	}
	return nil
}

// env is the whole serve read stack over the generated chain: the real
// executor's output, cooked, then served by the real rpc.Server.
type env struct {
	srv      *rpc.Server
	blocks   []*types.Block
	genesis  *exec.Genesis
	contract common.Address
}

func newEnv(t *testing.T) *env {
	t.Helper()
	c := testChain()

	g, err := exec.ChainGenesis(c)
	if err != nil {
		t.Fatalf("ChainGenesis: %v", err)
	}
	// The silent-failure guard from exec/sevmexec, repeated because this
	// package registers the extras itself: without subnet-evm's precompile
	// registry linked in, a genesis precompile vanishes with no error.
	if pc := sevmparams.GetExtra(g.Config).GenesisPrecompiles; len(pc) != 1 {
		t.Fatalf("genesis precompiles = %v, want the one configured in genesis", pc)
	}

	ref := referenceGenesis(t, c)
	if refRoot := ref.ToBlock().Root(); refRoot != g.Root {
		t.Fatalf("genesis root: exec %x, subnet-evm's own parse %x", g.Root, refRoot)
	}
	blocks := generateChain(t, ref)

	src := source{}
	for _, blk := range blocks {
		raw, err := rlp.EncodeToBytes(blk)
		if err != nil {
			t.Fatalf("encode block %d: %v", blk.NumberU64(), err)
		}
		src[blk.NumberU64()] = raw
	}

	// --- execute, exactly as serve does (root-verified per block) ------------
	dir := t.TempDir()
	store, err := state.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	e, err := exec.New(exec.Config{DataDir: dir, Blocks: src, Store: store, Chain: c, StopAt: headBlock})
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Run(context.Background()); err != nil {
		t.Fatalf("execute: %v", err)
	}
	if e.LiveHead() != headBlock {
		t.Fatalf("executed head %d, want %d", e.LiveHead(), headBlock)
	}
	e.Close()
	store.Close()

	// --- cook, so the historical descent and the tx index answer ------------
	if err := state.CookIndex(dir); err != nil {
		t.Fatalf("cook-index: %v", err)
	}
	if err := state.CookTxIndex(dir); err != nil {
		t.Fatalf("cook-txindex: %v", err)
	}

	// --- serve --------------------------------------------------------------
	rstore, err := state.OpenReadOnly(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { rstore.Close() })
	hist, err := state.OpenHistory(dir, rstore, g.Alloc)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(hist.Close)
	if _, _, _, err := hist.EnableTail(headBlock); err != nil {
		t.Fatalf("tail overlay: %v", err)
	}
	hist.SetHead(headBlock)

	srv := rpc.NewServer(hist, rpc.HistoryChainContext(hist), g.Config)
	srv.EnableTxAPIs(allHeights(headBlock), rpc.SealedBlocks{Epochs: hist.Epochs(), Blocks: src}, exec.ParseEthBlock)
	for _, blk := range blocks {
		srv.AddBlockHash(blk.Hash(), blk.NumberU64())
	}
	return &env{srv: srv, blocks: blocks, genesis: g, contract: crypto.CreateAddress(fundedAddr(), 1)}
}

func (e *env) block(n uint64) *types.Block { return e.blocks[n-1] }

// call issues one JSON-RPC request through the real HTTP handler, so the test
// exercises dispatch and the JSON encoding, not just the Go methods.
func (e *env) call(t *testing.T, method string, params ...any) (json.RawMessage, *jsonRPCError) {
	t.Helper()
	raw := make([]json.RawMessage, len(params))
	for i, p := range params {
		b, err := json.Marshal(p)
		if err != nil {
			t.Fatal(err)
		}
		raw[i] = b
	}
	body, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": raw})
	if err != nil {
		t.Fatal(err)
	}
	rec := httptest.NewRecorder()
	e.srv.ServeHTTP(rec, httptest.NewRequest("POST", "/", bytes.NewReader(body)))
	var reply struct {
		Result json.RawMessage `json:"result"`
		Error  *jsonRPCError   `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &reply); err != nil {
		t.Fatalf("%s: decode reply %q: %v", method, rec.Body.String(), err)
	}
	return reply.Result, reply.Error
}

// mustCall fails the test on a JSON-RPC error.
func (e *env) mustCall(t *testing.T, method string, params ...any) json.RawMessage {
	t.Helper()
	res, rerr := e.call(t, method, params...)
	if rerr != nil {
		t.Fatalf("%s: %v", method, rerr)
	}
	return res
}

type jsonRPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    any    `json:"data,omitempty"`
}

func (e *jsonRPCError) String() string { return fmt.Sprintf("[%d] %s", e.Code, e.Message) }
func (e *jsonRPCError) Error() string  { return e.String() }

func decodeString(t *testing.T, raw json.RawMessage) string {
	t.Helper()
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		t.Fatalf("decode %q as string: %v", raw, err)
	}
	return s
}

func decodeUint(t *testing.T, raw json.RawMessage) uint64 {
	t.Helper()
	n, err := hexutil.DecodeUint64(decodeString(t, raw))
	if err != nil {
		t.Fatalf("decode %q as quantity: %v", raw, err)
	}
	return n
}

func decodeBig(t *testing.T, raw json.RawMessage) *big.Int {
	t.Helper()
	b, err := hexutil.DecodeBig(decodeString(t, raw))
	if err != nil {
		t.Fatalf("decode %q as big: %v", raw, err)
	}
	return b
}

// decodeWord reads a 32-byte ABI word (eth_call output, a log's data), which
// is zero-padded hex and therefore NOT a JSON-RPC quantity.
func decodeWord(t *testing.T, raw json.RawMessage) *big.Int {
	t.Helper()
	return common.HexToHash(decodeString(t, raw)).Big()
}

func numTag(n uint64) string { return hexutil.EncodeUint64(n) }

// --- the gate ----------------------------------------------------------------

// TestEthCallReadsDeployedStorage is the method that could not exist before the
// seam: eth_call on a subnet-evm chain panicked inside coreth's state
// transition. It must return the contract's actual stored word.
func TestEthCallReadsDeployedStorage(t *testing.T) {
	e := newEnv(t)

	res := e.mustCall(t, "eth_call", map[string]any{
		"from": fundedAddr(),
		"to":   e.contract,
		"data": "0x",
	}, "latest")
	got := decodeWord(t, res)
	if got.Int64() != storedValue {
		t.Fatalf("eth_call returned %s (%q), want the contract's stored %d", got, res, storedValue)
	}

	// The same call one block BEFORE the deployment must find no code and
	// return empty, which proves the height actually selects the state.
	res = e.mustCall(t, "eth_call", map[string]any{"to": e.contract, "data": "0x"}, numTag(blkDeploy-1))
	if s := decodeString(t, res); s != "0x" {
		t.Fatalf("eth_call before deployment returned %q, want 0x (no code)", s)
	}

	// eth_estimateGas runs the same message through the seam's binary search.
	gas := decodeUint(t, e.mustCall(t, "eth_estimateGas", map[string]any{
		"from": fundedAddr(), "to": e.contract, "data": "0x",
	}, "latest"))
	receiptGas := e.receiptField(t, e.block(blkCall).Transactions()[0].Hash(), "gasUsed")
	if gas != decodeUint(t, receiptGas) {
		t.Fatalf("estimateGas %d, mined receipt gasUsed %d", gas, decodeUint(t, receiptGas))
	}
}

func (e *env) receiptField(t *testing.T, h common.Hash, field string) json.RawMessage {
	t.Helper()
	res := e.mustCall(t, "eth_getTransactionReceipt", h)
	var m map[string]json.RawMessage
	if err := json.Unmarshal(res, &m); err != nil {
		t.Fatalf("decode receipt: %v", err)
	}
	v, ok := m[field]
	if !ok {
		t.Fatalf("receipt has no %q: %s", field, res)
	}
	return v
}

// TestGetBalanceAcrossHeights: the recipient's balance is the exact sum of the
// transfers that had landed at each height, and the funded account paid real
// gas out of its alloc.
func TestGetBalanceAcrossHeights(t *testing.T) {
	e := newEnv(t)

	for _, tc := range []struct {
		height uint64
		want   int64
	}{
		{blkTransfer - 1, 0},
		{blkTransfer, transferValue},
		{blkCall, transferValue},
		{blkTail, transferValue + 2*tailValue},
	} {
		got := decodeBig(t, e.mustCall(t, "eth_getBalance", recipient, numTag(tc.height)))
		if got.Int64() != tc.want {
			t.Fatalf("balance of %s at block %d = %s, want %d", recipient, tc.height, got, tc.want)
		}
	}

	// "latest" is the head, and the nonce counts every tx the funded key sent.
	if got := decodeBig(t, e.mustCall(t, "eth_getBalance", recipient, "latest")); got.Int64() != transferValue+2*tailValue {
		t.Fatalf("balance at latest = %s", got)
	}
	nonce := decodeUint(t, e.mustCall(t, "eth_getTransactionCount", fundedAddr(), "latest"))
	if want := uint64(5); nonce != want { // 1 transfer + 1 deploy + 1 call + 2 transfers
		t.Fatalf("funded nonce = %d, want %d", nonce, want)
	}

	// The deployed runtime code is on chain and readable.
	code := decodeString(t, e.mustCall(t, "eth_getCode", e.contract, "latest"))
	if want := hexutil.Encode(storageContract[16:]); code != want {
		t.Fatalf("eth_getCode = %s, want the 49-byte runtime %s", code, want)
	}
	slot := decodeString(t, e.mustCall(t, "eth_getStorageAt", e.contract, common.Hash{}, "latest"))
	if got := common.HexToHash(slot).Big().Int64(); got != storedValue {
		t.Fatalf("storage slot 0 = %s, want %d", slot, storedValue)
	}
}

// TestTxAndReceipt: the tx index resolves a subnet-evm tx hash, and the receipt
// is reconstructed from the STORED sections the executor captured (no
// re-execution), with real values.
func TestTxAndReceipt(t *testing.T) {
	e := newEnv(t)
	transfer := e.block(blkTransfer).Transactions()[0]

	var tx struct {
		BlockHash   common.Hash    `json:"blockHash"`
		BlockNumber string         `json:"blockNumber"`
		From        common.Address `json:"from"`
		To          common.Address `json:"to"`
		Value       string         `json:"value"`
		Nonce       string         `json:"nonce"`
		Hash        common.Hash    `json:"hash"`
	}
	if err := json.Unmarshal(e.mustCall(t, "eth_getTransactionByHash", transfer.Hash()), &tx); err != nil {
		t.Fatal(err)
	}
	switch {
	case tx.Hash != transfer.Hash():
		t.Fatalf("tx hash %s, want %s", tx.Hash, transfer.Hash())
	case tx.BlockHash != e.block(blkTransfer).Hash():
		t.Fatalf("tx blockHash %s, want %s", tx.BlockHash, e.block(blkTransfer).Hash())
	case tx.BlockNumber != numTag(blkTransfer):
		t.Fatalf("tx blockNumber %s, want %s", tx.BlockNumber, numTag(blkTransfer))
	case tx.From != fundedAddr():
		t.Fatalf("tx from %s, want %s", tx.From, fundedAddr())
	case tx.To != recipient:
		t.Fatalf("tx to %s, want %s", tx.To, recipient)
	case tx.Value != hexutil.EncodeUint64(transferValue):
		t.Fatalf("tx value %s, want %d", tx.Value, transferValue)
	case tx.Nonce != "0x0":
		t.Fatalf("tx nonce %s, want 0x0", tx.Nonce)
	}

	var rc struct {
		Status            string          `json:"status"`
		GasUsed           string          `json:"gasUsed"`
		CumulativeGasUsed string          `json:"cumulativeGasUsed"`
		ContractAddress   *common.Address `json:"contractAddress"`
		EffectiveGasPrice string          `json:"effectiveGasPrice"`
		Logs              []any           `json:"logs"`
	}
	if err := json.Unmarshal(e.mustCall(t, "eth_getTransactionReceipt", transfer.Hash()), &rc); err != nil {
		t.Fatal(err)
	}
	switch {
	case rc.Status != "0x1":
		t.Fatalf("transfer receipt status %s, want 0x1", rc.Status)
	case rc.GasUsed != hexutil.EncodeUint64(21_000):
		t.Fatalf("transfer gasUsed %s, want 21000", rc.GasUsed)
	case rc.CumulativeGasUsed != rc.GasUsed:
		t.Fatalf("cumulative %s != gasUsed %s for the only tx in the block", rc.CumulativeGasUsed, rc.GasUsed)
	case rc.ContractAddress != nil:
		t.Fatalf("transfer receipt has contractAddress %s", rc.ContractAddress)
	case len(rc.Logs) != 0:
		t.Fatalf("transfer emitted %d logs", len(rc.Logs))
	}
	// effectiveGasPrice is the chain's own base fee (the txs bid a zero tip),
	// which on subnet-evm comes straight off the header.
	if want := hexutil.EncodeBig(e.block(blkTransfer).BaseFee()); rc.EffectiveGasPrice != want {
		t.Fatalf("effectiveGasPrice %s, want the block's base fee %s", rc.EffectiveGasPrice, want)
	}

	// The creation receipt must name the address the code actually landed at.
	deploy := e.block(blkDeploy).Transactions()[0]
	if err := json.Unmarshal(e.mustCall(t, "eth_getTransactionReceipt", deploy.Hash()), &rc); err != nil {
		t.Fatal(err)
	}
	if rc.ContractAddress == nil || *rc.ContractAddress != e.contract {
		t.Fatalf("creation receipt contractAddress %v, want %s", rc.ContractAddress, e.contract)
	}

	// An unknown hash is a clean null, not an error and not a fabrication.
	res, rerr := e.call(t, "eth_getTransactionByHash", common.HexToHash("0xdead"))
	if rerr != nil || string(res) != "null" {
		t.Fatalf("unknown tx: result=%s err=%v", res, rerr)
	}
}

// TestGetLogs: the log the contract emitted is served out of the stored-logs
// section with its real address, topic and data.
func TestGetLogs(t *testing.T) {
	e := newEnv(t)

	var logs []struct {
		Address     common.Address `json:"address"`
		Topics      []common.Hash  `json:"topics"`
		Data        string         `json:"data"`
		BlockNumber string         `json:"blockNumber"`
		BlockHash   common.Hash    `json:"blockHash"`
		TxHash      common.Hash    `json:"transactionHash"`
		Index       string         `json:"logIndex"`
	}
	if err := json.Unmarshal(e.mustCall(t, "eth_getLogs", map[string]any{
		"fromBlock": "0x1", "toBlock": "latest", "address": e.contract,
	}), &logs); err != nil {
		t.Fatal(err)
	}
	if len(logs) != 1 {
		t.Fatalf("got %d logs, want the one the contract emitted", len(logs))
	}
	l := logs[0]
	callTx := e.block(blkCall).Transactions()[0]
	switch {
	case l.Address != e.contract:
		t.Fatalf("log address %s, want %s", l.Address, e.contract)
	case len(l.Topics) != 1 || l.Topics[0] != logTopic:
		t.Fatalf("log topics %v, want [%s]", l.Topics, logTopic)
	case common.HexToHash(l.Data).Big().Int64() != storedValue:
		t.Fatalf("log data %s, want the stored %d", l.Data, storedValue)
	case l.BlockNumber != numTag(blkCall):
		t.Fatalf("log blockNumber %s, want %s", l.BlockNumber, numTag(blkCall))
	case l.BlockHash != e.block(blkCall).Hash():
		t.Fatalf("log blockHash %s, want %s", l.BlockHash, e.block(blkCall).Hash())
	case l.TxHash != callTx.Hash():
		t.Fatalf("log txHash %s, want %s", l.TxHash, callTx.Hash())
	}

	// A topic filter that cannot match returns nothing rather than everything.
	if err := json.Unmarshal(e.mustCall(t, "eth_getLogs", map[string]any{
		"fromBlock": "0x1", "toBlock": "latest",
		"topics": []any{common.HexToHash("0xfeed")},
	}), &logs); err != nil {
		t.Fatal(err)
	}
	if len(logs) != 0 {
		t.Fatalf("a non-matching topic filter returned %d logs", len(logs))
	}
}

// TestFeeHistory: base fees come straight off the subnet-evm headers, and the
// reward percentiles run the whole block through the seam's re-execution path
// (which is what used to panic).
func TestFeeHistory(t *testing.T) {
	e := newEnv(t)

	var fh struct {
		OldestBlock   string     `json:"oldestBlock"`
		BaseFeePerGas []string   `json:"baseFeePerGas"`
		GasUsedRatio  []float64  `json:"gasUsedRatio"`
		Reward        [][]string `json:"reward"`
	}
	if err := json.Unmarshal(e.mustCall(t, "eth_feeHistory", "0x3", "latest", []float64{20, 80}), &fh); err != nil {
		t.Fatal(err)
	}
	if fh.OldestBlock != numTag(headBlock-2) {
		t.Fatalf("oldestBlock %s, want %s", fh.OldestBlock, numTag(headBlock-2))
	}
	if len(fh.BaseFeePerGas) != 4 || len(fh.GasUsedRatio) != 3 || len(fh.Reward) != 3 {
		t.Fatalf("shape: %d baseFees, %d ratios, %d rewards", len(fh.BaseFeePerGas), len(fh.GasUsedRatio), len(fh.Reward))
	}
	for i, n := range []uint64{headBlock - 2, headBlock - 1, headBlock} {
		hdr := e.block(n).Header()
		if hdr.BaseFee == nil {
			t.Fatalf("subnet-evm header %d carries no base fee", n)
		}
		if want := hexutil.EncodeBig(hdr.BaseFee); fh.BaseFeePerGas[i] != want {
			t.Fatalf("baseFeePerGas[%d] = %s, want header %d's %s", i, fh.BaseFeePerGas[i], n, want)
		}
		want := float64(hdr.GasUsed) / float64(hdr.GasLimit)
		if fh.GasUsedRatio[i] != want {
			t.Fatalf("gasUsedRatio[%d] = %v, want %v", i, fh.GasUsedRatio[i], want)
		}
	}
	// Every tx in this chain bids a zero tip, so every percentile is zero, and
	// getting there at all means the re-execution ran.
	for i, row := range fh.Reward {
		for j, v := range row {
			if v != "0x0" {
				t.Fatalf("reward[%d][%d] = %s, want 0x0 (all txs bid GasTipCap 0)", i, j, v)
			}
		}
	}

	// eth_baseFee and eth_gasPrice read the same headers and must agree with
	// them; the head block has txs, so the oracle samples rather than falling
	// back to the chain's configured minimum.
	if got, want := decodeBig(t, e.mustCall(t, "eth_baseFee")), e.block(headBlock).BaseFee(); got.Cmp(want) != 0 {
		t.Fatalf("eth_baseFee %s, want %s", got, want)
	}
	if got, want := decodeBig(t, e.mustCall(t, "eth_gasPrice")), e.block(headBlock).BaseFee(); got.Cmp(want) != 0 {
		t.Fatalf("eth_gasPrice %s, want the sampled effective price %s", got, want)
	}
}

// TestDebugTrace: one real trace of the contract call. callTracer's top frame
// gas must equal the mined receipt's, and the struct logger must agree.
func TestDebugTrace(t *testing.T) {
	e := newEnv(t)
	callTx := e.block(blkCall).Transactions()[0]
	receiptGas := decodeUint(t, e.receiptField(t, callTx.Hash(), "gasUsed"))

	var frame struct {
		Type    string         `json:"type"`
		From    common.Address `json:"from"`
		To      common.Address `json:"to"`
		GasUsed string         `json:"gasUsed"`
		Output  string         `json:"output"`
		Logs    []struct {
			Address common.Address `json:"address"`
			Topics  []common.Hash  `json:"topics"`
		} `json:"logs"`
	}
	res := e.mustCall(t, "debug_traceTransaction", callTx.Hash(),
		map[string]any{"tracer": "callTracer", "tracerConfig": map[string]any{"withLog": true}})
	if err := json.Unmarshal(res, &frame); err != nil {
		t.Fatalf("decode call frame: %v", err)
	}
	switch {
	case frame.Type != "CALL":
		t.Fatalf("frame type %q, want CALL", frame.Type)
	case frame.To != e.contract:
		t.Fatalf("frame to %s, want %s", frame.To, e.contract)
	case frame.GasUsed != hexutil.EncodeUint64(receiptGas):
		t.Fatalf("callTracer gasUsed %s, receipt %d", frame.GasUsed, receiptGas)
	case common.HexToHash(frame.Output).Big().Int64() != storedValue:
		t.Fatalf("traced output %s, want the stored %d", frame.Output, storedValue)
	case len(frame.Logs) != 1 || frame.Logs[0].Topics[0] != logTopic:
		t.Fatalf("traced logs %v, want the one LOG1", frame.Logs)
	}

	var sl struct {
		Gas    uint64 `json:"gas"`
		Failed bool   `json:"failed"`
	}
	if err := json.Unmarshal(e.mustCall(t, "debug_traceTransaction", callTx.Hash()), &sl); err != nil {
		t.Fatal(err)
	}
	if sl.Gas != receiptGas || sl.Failed {
		t.Fatalf("structlog gas=%d failed=%v, receipt %d", sl.Gas, sl.Failed, receiptGas)
	}

	// The creation traces as a CREATE, which is the other frame shape.
	if err := json.Unmarshal(e.mustCall(t, "debug_traceTransaction",
		e.block(blkDeploy).Transactions()[0].Hash(),
		map[string]any{"tracer": "callTracer"}), &frame); err != nil {
		t.Fatal(err)
	}
	if frame.Type != "CREATE" {
		t.Fatalf("creation frame type %q", frame.Type)
	}

	// debug_traceCall goes through runCall rather than the block replay.
	res = e.mustCall(t, "debug_traceCall",
		map[string]any{"from": fundedAddr(), "to": e.contract, "data": "0x"}, "latest",
		map[string]any{"tracer": "callTracer"})
	if err := json.Unmarshal(res, &frame); err != nil {
		t.Fatal(err)
	}
	if common.HexToHash(frame.Output).Big().Int64() != storedValue {
		t.Fatalf("debug_traceCall output %s, want %d", frame.Output, storedValue)
	}
}

// TestBlockAndHeaderShape: M7's omissions hold on a real subnet-evm block, and
// block-by-hash resolves through the folded fingerprint index.
func TestBlockAndHeaderShape(t *testing.T) {
	e := newEnv(t)
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(e.mustCall(t, "eth_getBlockByNumber", numTag(blkCall), true), &fields); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"extDataHash", "blockExtraData", "extDataGasUsed"} {
		if _, ok := fields[k]; ok {
			t.Fatalf("subnet-evm block carries the coreth-only field %q", k)
		}
	}
	for _, k := range []string{"number", "hash", "stateRoot", "baseFeePerGas", "transactions"} {
		if _, ok := fields[k]; !ok {
			t.Fatalf("block is missing %q", k)
		}
	}
	if got := decodeString(t, fields["hash"]); got != e.block(blkCall).Hash().Hex() {
		t.Fatalf("block hash %s, want %s", got, e.block(blkCall).Hash())
	}

	if err := json.Unmarshal(e.mustCall(t, "eth_getBlockByHash", e.block(blkDeploy).Hash(), false), &fields); err != nil {
		t.Fatal(err)
	}
	if got := decodeUint(t, fields["number"]); got != blkDeploy {
		t.Fatalf("block-by-hash resolved to %d, want %d", got, blkDeploy)
	}
}

// TestMethodTableNoPanics walks the WHOLE dispatch table on this chain: every
// method must either answer or return a named JSON-RPC error, and nothing may
// panic. That is the property `serve --chain` needs and the one the coreth
// calls used to break.
func TestMethodTableNoPanics(t *testing.T) {
	e := newEnv(t)
	callTx := e.block(blkCall).Transactions()[0].Hash()
	blkHash := e.block(blkCall).Hash()
	tag := numTag(blkCall)
	callArgs := map[string]any{"from": fundedAddr(), "to": e.contract, "data": "0x"}

	// unsupported names the methods that are DELIBERATELY refused (by design or
	// because this is a read-only archive), so a new refusal cannot sneak in.
	unsupported := map[string]bool{
		"eth_getProof": true, "debug_dumpBlock": true, "debug_accountRange": true,
		"debug_storageRangeAt": true, "eth_sendRawTransaction": true,
		"eth_sendTransaction": true, "eth_fillTransaction": true, "eth_resend": true,
		"eth_subscribe": true, "eth_unsubscribe": true,
	}

	for _, tc := range []struct {
		method string
		params []any
	}{
		{"eth_chainId", nil},
		{"eth_blockNumber", nil},
		{"eth_getBalance", []any{recipient, "latest"}},
		{"eth_getTransactionCount", []any{fundedAddr(), "latest"}},
		{"eth_getCode", []any{e.contract, "latest"}},
		{"eth_getStorageAt", []any{e.contract, common.Hash{}, "latest"}},
		{"eth_call", []any{callArgs, "latest"}},
		{"eth_getTransactionByHash", []any{callTx}},
		{"eth_getTransactionReceipt", []any{callTx}},
		{"eth_getLogs", []any{map[string]any{"fromBlock": "0x1", "toBlock": "latest"}}},
		{"eth_getBlockByNumber", []any{tag, true}},
		{"eth_getBlockByHash", []any{blkHash, false}},
		{"eth_getBlockTransactionCountByNumber", []any{tag}},
		{"eth_getBlockTransactionCountByHash", []any{blkHash}},
		{"eth_getTransactionByBlockNumberAndIndex", []any{tag, "0x0"}},
		{"eth_getTransactionByBlockHashAndIndex", []any{blkHash, "0x0"}},
		{"eth_getBlockReceipts", []any{tag}},
		{"eth_estimateGas", []any{callArgs, "latest"}},
		{"eth_gasPrice", nil},
		{"eth_maxPriorityFeePerGas", nil},
		{"eth_feeHistory", []any{"0x2", "latest", []float64{50}}},
		{"net_version", nil},
		{"web3_clientVersion", nil},
		{"eth_syncing", nil},
		{"net_listening", nil},
		{"eth_accounts", nil},
		{"eth_coinbase", nil},
		{"eth_etherbase", nil},
		{"eth_getUncleCountByBlockNumber", []any{tag}},
		{"eth_getUncleCountByBlockHash", []any{blkHash}},
		{"eth_getUncleByBlockNumberAndIndex", []any{tag, "0x0"}},
		{"eth_getUncleByBlockHashAndIndex", []any{blkHash, "0x0"}},
		{"eth_getProof", []any{e.contract, []any{}, "latest"}},
		{"debug_traceTransaction", []any{callTx}},
		{"debug_traceBlockByNumber", []any{tag}},
		{"debug_traceBlockByHash", []any{blkHash}},
		{"debug_getRawBlock", []any{tag}},
		{"debug_getRawHeader", []any{tag}},
		{"debug_getRawTransaction", []any{callTx}},
		{"debug_getRawReceipts", []any{tag}},
		{"eth_createAccessList", []any{callArgs, tag}},
		{"debug_traceCall", []any{callArgs, "latest"}},
		{"debug_getModifiedAccountsByNumber", []any{blkCall}},
		{"debug_getModifiedAccountsByHash", []any{blkHash}},
		{"debug_getBadBlocks", nil},
		{"debug_dumpBlock", []any{tag}},
		{"debug_accountRange", []any{tag}},
		{"debug_storageRangeAt", []any{tag}},
		{"eth_newFilter", []any{map[string]any{"fromBlock": "0x1", "toBlock": "latest"}}},
		{"eth_newBlockFilter", nil},
		{"eth_newPendingTransactionFilter", nil},
		{"eth_uninstallFilter", []any{"0x1"}},
		{"web3_sha3", []any{"0xdeadbeef"}},
		{"net_peerCount", nil},
		{"txpool_status", nil},
		{"txpool_content", nil},
		{"txpool_contentFrom", []any{fundedAddr()}},
		{"txpool_inspect", nil},
		{"eth_pendingTransactions", nil},
		{"eth_baseFee", nil},
		{"eth_getHeaderByNumber", []any{tag}},
		{"eth_getHeaderByHash", []any{blkHash}},
		{"eth_getRawTransactionByHash", []any{callTx}},
		{"eth_getRawTransactionByBlockNumberAndIndex", []any{tag, "0x0"}},
		{"eth_getRawTransactionByBlockHashAndIndex", []any{blkHash, "0x0"}},
		{"eth_sendRawTransaction", []any{"0x00"}},
		{"eth_sendTransaction", []any{map[string]any{}}},
		{"eth_fillTransaction", []any{map[string]any{}}},
		{"eth_resend", []any{map[string]any{}}},
		{"eth_subscribe", []any{"newHeads"}},
		{"eth_unsubscribe", []any{"0x1"}},
	} {
		res, rerr := e.call(t, tc.method, tc.params...)
		switch {
		case unsupported[tc.method]:
			if rerr == nil {
				t.Errorf("%s: expected a named unsupported error, got %s", tc.method, res)
				continue
			}
			if rerr.Message == "" {
				t.Errorf("%s: refused with an empty message", tc.method)
			}
		case rerr != nil:
			t.Errorf("%s: %v", tc.method, rerr)
		case len(res) == 0:
			t.Errorf("%s: empty result and no error", tc.method)
		}
	}

	// The filter methods need a live registration to answer, so they run as
	// their own round trip: create, read, uninstall.
	id := decodeString(t, e.mustCall(t, "eth_newFilter", map[string]any{
		"fromBlock": "0x1", "toBlock": "latest", "address": e.contract,
	}))
	var logs []any
	if err := json.Unmarshal(e.mustCall(t, "eth_getFilterLogs", id), &logs); err != nil {
		t.Fatal(err)
	}
	if len(logs) != 1 {
		t.Fatalf("eth_getFilterLogs returned %d logs, want 1", len(logs))
	}
	e.mustCall(t, "eth_getFilterChanges", id)
	e.mustCall(t, "eth_uninstallFilter", id)

	// An unknown method is still the standard -32601.
	if _, rerr := e.call(t, "eth_notAMethod"); rerr == nil || rerr.Code != -32601 {
		t.Fatalf("unknown method: %v", rerr)
	}
}

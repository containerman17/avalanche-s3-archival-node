package validator

import (
	"crypto/ecdsa"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	ethcommon "github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/common/hexutil"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/crypto"
	"github.com/ava-labs/libevm/params"
)

// TestBatchAdmission: a JSON-RPC batch admits every eth_sendRawTransaction
// in one pool call inside the engine; the answers keep the batch's order, a
// bad element (garbage bytes, a tx the sender cannot pay) refuses itself
// only, and the other elements of the same batch are still answered.
func TestBatchAdmission(t *testing.T) {
	if !realEngine {
		t.Skip("stub engine")
	}
	h := newHarness(t)
	to := ethcommon.HexToAddress("0x1000000000000000000000000000000000000008")
	poor, _ := crypto.GenerateKey()
	sign := func(key *ecdsa.PrivateKey, nonce uint64) string {
		tx := types.MustSignNewTx(key, h.signer, &types.DynamicFeeTx{ChainID: h.vm.config.ChainID, Nonce: nonce, To: &to, Value: big.NewInt(1), Gas: 21000,
			GasFeeCap: big.NewInt(100 * params.GWei), GasTipCap: big.NewInt(params.GWei)})
		raw, _ := tx.MarshalBinary()
		return fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"method":"eth_sendRawTransaction","params":["0x%x"]}`, nonce, raw)
	}
	body := "[" + sign(h.key, 0) + `,{"jsonrpc":"2.0","id":"junk","method":"eth_sendRawTransaction","params":["0x00"]},` +
		sign(poor, 0) + `,{"jsonrpc":"2.0","id":"num","method":"eth_blockNumber","params":[]},` + sign(h.key, 1) + `,` +
		`{"jsonrpc":"2.0","id":"st","method":"txpool_status","params":[]}]`
	rec := httptest.NewRecorder()
	h.rpc.ServeHTTP(rec, httptest.NewRequest("POST", "/rpc", strings.NewReader(body)))
	var resps []struct {
		ID     json.RawMessage `json:"id"`
		Result json.RawMessage `json:"result"`
		Error  *struct{ Message string }
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resps); err != nil || len(resps) != 6 {
		t.Fatalf("batch response %q: %v", rec.Body.String(), err)
	}
	wantIDs := []string{"0", `"junk"`, "0", `"num"`, "1", `"st"`}
	for i, r := range resps {
		if string(r.ID) != wantIDs[i] {
			t.Fatalf("element %d: id %s, want %s", i, r.ID, wantIDs[i])
		}
	}
	for _, i := range []int{0, 3, 4, 5} {
		if resps[i].Error != nil {
			t.Fatalf("element %d refused: %s", i, resps[i].Error.Message)
		}
	}
	if resps[1].Error == nil || resps[2].Error == nil {
		t.Fatalf("garbage and unpayable elements must be refused: %v %v", resps[1].Error, resps[2].Error)
	}
	if p, q := h.vm.eng.poolStatus(); p != 2 || q != 0 {
		t.Fatalf("pool pending=%d queued=%d, want 2/0", p, q)
	}
	var st struct{ Pending hexutil.Uint }
	json.Unmarshal(resps[5].Result, &st)
	if st.Pending != 2 {
		t.Fatalf("txpool_status inside the batch saw pending=%d, want 2 (admission is synchronous)", st.Pending)
	}
	for _, want := range []string{"insufficient funds", "does not decode"} {
		found := false
		for _, r := range resps {
			if r.Error != nil && strings.Contains(r.Error.Message, want) {
				found = true
			}
		}
		if !found {
			t.Fatalf("no refusal mentions %q", want)
		}
	}
}

// TestAdmitBatchCost (-v): the cost of one epochdb_pool_add of 1000
// presigned transfers from 1000 senders (decode, recovery and the stateless
// checks in parallel, one state read, the pool lock), against libevm's
// 46 ms (6-14 ms with its cached sender) for the same batch.
func TestAdmitBatchCost(t *testing.T) {
	if !realEngine {
		t.Skip("stub engine")
	}
	h := newHarnessWith(t, stressGenesis(),
		`{"tx-pool-account-slots": 1000, "tx-pool-global-slots": 400000, "tx-pool-account-queue": 2000, "tx-pool-global-queue": 400000}`)
	const nkeys, rounds = 1000, 5
	keys := make([]*ecdsa.PrivateKey, nkeys)
	for i := range keys {
		keys[i], _ = crypto.GenerateKey()
		h.transfer(crypto.PubkeyToAddress(keys[i].PublicKey), new(big.Int).Mul(big.NewInt(params.Ether), big.NewInt(100)))
	}
	h.buildAccept()
	to := ethcommon.HexToAddress("0x100000000000000000000000000000000000000a")
	for r := 0; r < rounds; r++ {
		raws := make([][]byte, nkeys)
		for k := range keys {
			tx := types.MustSignNewTx(keys[k], h.signer, &types.DynamicFeeTx{ChainID: h.vm.config.ChainID, Nonce: uint64(r), To: &to, Value: big.NewInt(1), Gas: 21000,
				GasFeeCap: big.NewInt(50 * params.GWei), GasTipCap: big.NewInt(params.GWei)})
			raws[k], _ = tx.MarshalBinary()
		}
		t0 := time.Now()
		res, err := h.vm.eng.poolAdd(raws, false)
		took := time.Since(t0)
		if err != nil {
			t.Fatal(err)
		}
		for _, x := range res {
			if err := x.err(); err != nil {
				t.Fatal(err)
			}
		}
		p, _ := h.vm.eng.poolStatus()
		t.Logf("round %d: epochdb_pool_add(1000) %v (%.1f us/tx), pending=%d", r, took, float64(took.Microseconds())/nkeys, p)
	}
}

// Small batches are the real-world shape (a wallet or a gateway sends one tx
// per request; the EC2 client's 64 connections per node sliced the stream to
// ~6 txs per body). Per call through the engine: 6 txs of senders the pool
// never saw (one engine state read), 6 more of the same senders (the pool
// keeps an emptied sender's nonce and balance, no state read), the same 6
// again (Known by hash: no decode, no recovery).
func TestAdmitSmallBatches(t *testing.T) {
	if !realEngine {
		t.Skip("stub engine")
	}
	h := newHarnessWith(t, stressGenesis(), `{"tx-pool-account-slots": 1000}`)
	const n = 6
	keys := make([]*ecdsa.PrivateKey, n)
	for i := range keys {
		keys[i], _ = crypto.GenerateKey()
		h.transfer(crypto.PubkeyToAddress(keys[i].PublicKey), new(big.Int).Mul(big.NewInt(params.Ether), big.NewInt(100)))
	}
	h.buildAccept()
	to := ethcommon.HexToAddress("0x100000000000000000000000000000000000000a")
	batch := func(nonce uint64) [][]byte {
		raws := make([][]byte, n)
		for k := range keys {
			tx := types.MustSignNewTx(keys[k], h.signer, &types.DynamicFeeTx{ChainID: h.vm.config.ChainID, Nonce: nonce, To: &to, Value: big.NewInt(1), Gas: 21000,
				GasFeeCap: big.NewInt(50 * params.GWei), GasTipCap: big.NewInt(params.GWei)})
			raws[k], _ = tx.MarshalBinary()
		}
		return raws
	}
	add := func(name string, raws [][]byte, want uint8) time.Duration {
		t0 := time.Now()
		res, err := h.vm.eng.poolAdd(raws, false)
		took := time.Since(t0)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range res {
			if r.code != want {
				t.Fatalf("%s: code %d, want %d", name, r.code, want)
			}
		}
		t.Logf("%s: epochdb_pool_add(%d) %v", name, n, took)
		return took
	}
	first := batch(0)
	add("new senders", first, 0)
	h.buildAccept() // mines them: the senders' accounts are now empty, and kept
	add("cached senders after a block", batch(1), 0)
	known := add("known", batch(1), 1)
	if known > 50*time.Microsecond {
		t.Errorf("6 known txs took %v, want under 50 us", known)
	}
}

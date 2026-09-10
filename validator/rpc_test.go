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
// in one pool call; the answers keep the batch's order, a bad element
// (garbage bytes, a tx the sender cannot pay) refuses itself only, and the
// non-pool elements of the same batch are still answered.
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
	if p, q := h.vm.pool.Stats(); p != 2 || q != 0 {
		t.Fatalf("pool pending=%d queued=%d, want 2/0", p, q)
	}
	var st struct{ Pending hexutil.Uint }
	json.Unmarshal(resps[5].Result, &st)
	t.Logf("txpool_status inside the batch saw pending=%d (the promotion round is asynchronous)", st.Pending)
}

// TestAdmitBatchCost (-v): the cost of one pool.Add of 1000 presigned
// transfers from 1000 senders, senders recovered and accounts warmed first,
// so what remains is legacypool under its lock: 46 ms (46 us per tx, a
// 21k tx/s ceiling) while libevm's ValidateTransactionWithState recovered
// the sender AGAIN via signer.Sender (uncached) under the lock; 6-14 ms with
// the fork's cached types.Sender there (E2E.md, Admission).
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
	h.vm.chain.settle()
	to := ethcommon.HexToAddress("0x100000000000000000000000000000000000000a")
	for r := 0; r < rounds; r++ {
		txs := make([]*types.Transaction, nkeys)
		for k := range keys {
			txs[k] = types.MustSignNewTx(keys[k], h.signer, &types.DynamicFeeTx{ChainID: h.vm.config.ChainID, Nonce: uint64(r), To: &to, Value: big.NewInt(1), Gas: 21000,
				GasFeeCap: big.NewInt(50 * params.GWei), GasTipCap: big.NewInt(params.GWei)})
		}
		t0 := time.Now()
		senders := recoverSenders(h.signer, txs)
		t1 := time.Now()
		h.vm.chain.acct.warm(senders)
		t2 := time.Now()
		errs := h.vm.pool.Add(txs, false, false)
		t3 := time.Now()
		for _, err := range errs {
			if err != nil {
				t.Fatal(err)
			}
		}
		p, _ := h.vm.pool.Stats()
		t.Logf("round %d: recover(parallel) %v, warm %v, pool.Add(1000) %v, pending=%d", r, t1.Sub(t0), t2.Sub(t1), t3.Sub(t2), p)
	}
}

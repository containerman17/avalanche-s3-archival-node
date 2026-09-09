package validator

import (
	"context"
	"crypto/ecdsa"
	"fmt"
	"math/big"
	"os"
	"testing"
	"time"

	ethcommon "github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/crypto"
	"github.com/ava-labs/libevm/params"
)

// TestAcceptCostDeepPool (EPOCHDB_DEEP=1): Accept's cost and the pool
// reset's landing time per block with 100k+ txs pending (the reset walks
// every pending account: demote, then promote the dirty ones; 570 ms at
// 104k pending, which is why it stays off the Accept path).
func TestAcceptCostDeepPool(t *testing.T) {
	if !realEngine || os.Getenv("EPOCHDB_DEEP") == "" {
		t.Skip("real engine + EPOCHDB_DEEP=1")
	}
	h := newHarnessWith(t, stressGenesis(),
		`{"tx-pool-account-slots": 10000, "tx-pool-global-slots": 400000, "tx-pool-account-queue": 10000, "tx-pool-global-queue": 400000}`)
	const nkeys, perKey = 1000, 120
	keys := make([]*ecdsa.PrivateKey, nkeys)
	for i := range keys {
		keys[i], _ = crypto.GenerateKey()
		h.transfer(crypto.PubkeyToAddress(keys[i].PublicKey), new(big.Int).Mul(big.NewInt(params.Ether), big.NewInt(100)))
	}
	h.buildAccept()
	to := ethcommon.HexToAddress("0x1000000000000000000000000000000000000007")
	t0 := time.Now()
	for n := 0; n < perKey; n++ {
		txs := make([]*types.Transaction, nkeys)
		for k := range keys {
			txs[k] = types.MustSignNewTx(keys[k], h.signer, &types.DynamicFeeTx{ChainID: h.vm.config.ChainID, Nonce: uint64(n), To: &to, Value: big.NewInt(1), Gas: 21000,
				GasFeeCap: big.NewInt(50 * params.GWei), GasTipCap: big.NewInt(params.GWei)})
		}
		for _, err := range h.vm.pool.Add(txs, false, false) {
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	p, q := h.vm.pool.Stats()
	t.Logf("admitted %d txs in %v: pending=%d queued=%d", nkeys*perKey, time.Since(t0), p, q)
	for i := 0; i < 3; i++ {
		ctx := context.Background()
		if _, err := h.vm.WaitForEvent(ctx); err != nil {
			t.Fatal(err)
		}
		blk, err := h.vm.BuildBlock(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := blk.Verify(ctx); err != nil {
			t.Fatal(err)
		}
		h.vm.SetPreference(ctx, blk.ID())
		accBefore := histSum(h.vm.m.accept)
		ta := time.Now()
		if err := blk.Accept(ctx); err != nil {
			t.Fatal(err)
		}
		accept := time.Since(ta)
		h.vm.chain.settle()
		settled := time.Since(ta)
		p, q := h.vm.pool.Stats()
		fmt.Printf("DEEP block %d: txs=%d Accept=%v (engine accept + head + refresh, metric %v) pool settled after %v; pending after=%d queued=%d\n",
			blk.Height(), len(blk.(*Block).raw)/110, accept, time.Duration((histSum(h.vm.m.accept)-accBefore)*float64(time.Second)), settled, p, q)
	}
}

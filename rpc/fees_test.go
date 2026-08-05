package rpc

import (
	"math/big"
	"strings"
	"testing"

	"github.com/ava-labs/avalanchego/vms/components/gas"
	"github.com/ava-labs/avalanchego/vms/evm/acp176"
	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/common/hexutil"
	"github.com/ava-labs/libevm/core/types"

	"github.com/containerman17/epochdb/exec"
)

// Fee-answer gates on the coreth path. Both defects here were numbers a wallet
// acts on directly and that no shape check could catch: a "tip" that was
// really the whole gas price, and a projected base fee that was really 0.

const (
	// feeBlockTime is past every mainnet C-chain upgrade in this dep set
	// (Fortuna 2025-04-08, Granite 2025-11-19), so the headers below carry
	// ACP-176 fee state and the projection runs the dynamic-fee path. Mainnet
	// has NO scheduled Helicon, which is what keeps this off the SAE branch.
	feeBlockTime = 1767225600 // 2026-01-01T00:00:00Z

	feeBase = 25_000_000_000 // the headers' base fee: 25 gwei
	feeTip  = 2_000_000_000  // what every sampled transaction bids ABOVE it
)

// feeState is the ACP-176 state the headers carry in Extra. Excess is what
// sets the price; the values are arbitrary but well inside the curve's range.
func feeState() acp176.State {
	return acp176.State{
		Gas:          gas.State{Capacity: 10_000_000, Excess: 3_000_000_000},
		TargetExcess: 13_000_000,
	}
}

// newFeeEnv is the three-block env with headers a fee answer can be computed
// from: a real base fee and real ACP-176 fee state, at a timestamp past the
// forks that introduced them.
func newFeeEnv(t *testing.T, networkID uint32, gasPrice *big.Int) *waEnv {
	t.Helper()
	txs := map[uint64]types.Transactions(nil)
	if gasPrice != nil {
		to := common.HexToAddress("0xbbbb000000000000000000000000000000000002")
		txs = map[uint64]types.Transactions{3: {types.NewTx(&types.LegacyTx{
			Nonce: 0, To: &to, Gas: 21000, GasPrice: gasPrice,
		})}}
	}
	st := feeState()
	extra := st.Bytes()
	env := newChainEnv(t, mustCChain(t, networkID), txs, func(_ uint64, h *types.Header) {
		h.Time = feeBlockTime
		h.BaseFee = big.NewInt(feeBase)
		h.Extra = extra
		h.GasUsed = 1_000_000
	})
	env.srv.EnableTxAPIs(stubCandidates{}, env.blocks, exec.ParseEthBlock)
	return env
}

// TestMaxPriorityFeePerGasIsTheTipNotThePrice: the sampled transactions pay
// exactly base fee plus a known tip, so the answer must be that tip.
// Returning the whole sampled price (what this did) makes a wallet set a
// priority fee of base+tip on top of the base fee it already pays.
func TestMaxPriorityFeePerGasIsTheTipNotThePrice(t *testing.T) {
	price := big.NewInt(feeBase + feeTip)
	env := newFeeEnv(t, 1, price)

	got := mustBig(t, env, "eth_maxPriorityFeePerGas")
	if want := big.NewInt(feeTip); got.Cmp(want) != 0 {
		t.Fatalf("eth_maxPriorityFeePerGas = %s, want the tip %s (the sampled price is %s)", got, want, price)
	}
	// eth_gasPrice is the OTHER method and still reports the whole price: the
	// two shared one case, and collapsing them again is the regression.
	if got := mustBig(t, env, "eth_gasPrice"); got.Cmp(price) != 0 {
		t.Fatalf("eth_gasPrice = %s, want the whole sampled price %s", got, price)
	}
}

// A sample BELOW the current base fee is a zero tip, never a negative one.
func TestMaxPriorityFeePerGasFloorsAtZero(t *testing.T) {
	env := newFeeEnv(t, 1, big.NewInt(feeBase/2))
	if got := mustBig(t, env, "eth_maxPriorityFeePerGas"); got.Sign() != 0 {
		t.Fatalf("eth_maxPriorityFeePerGas = %s, want 0", got)
	}
}

// TestFeeHistoryProjectsTheNextBaseFee: geth's shape wants blockCount+1 base
// fees, the last a projection for the block nobody has built. It was 0, which
// a wallet turns into maxFeePerGas 0 and a transaction that never mines.
//
// The expectation is computed straight from ACP-176: at the parent's own
// timestamp nothing advances, so the next block's base fee is the price the
// parent's own fee state already implies.
func TestFeeHistoryProjectsTheNextBaseFee(t *testing.T) {
	env := newFeeEnv(t, 1, big.NewInt(feeBase+feeTip))

	res, rerr := env.srv.dispatch(&rpcRequest{
		Method: "eth_feeHistory", Params: mustParams(t, "0x2", "latest"),
	})
	if rerr != nil {
		t.Fatal(rerr)
	}
	out, ok := res.(map[string]any)
	if !ok {
		t.Fatalf("feeHistory returned %T", res)
	}
	baseFees, ok := out["baseFeePerGas"].([]any)
	if !ok || len(baseFees) != 3 {
		t.Fatalf("baseFeePerGas = %v, want 3 entries (blockCount+1)", out["baseFeePerGas"])
	}

	st := feeState()
	want := new(big.Int).SetUint64(uint64(st.GasPrice()))
	if want.Sign() == 0 {
		t.Fatal("the test's own fee state prices at zero, so it proves nothing")
	}
	got := (*big.Int)(baseFees[2].(*hexutil.Big))
	if got.Sign() == 0 {
		t.Fatal("the trailing baseFeePerGas entry is still 0: nothing was projected")
	}
	if got.Cmp(want) != 0 {
		t.Fatalf("projected base fee %s, want ACP-176's price for the head's own fee state %s", got, want)
	}
	// The per-block entries are still the headers' own, untouched.
	for i := range 2 {
		if bf := (*big.Int)(baseFees[i].(*hexutil.Big)); bf.Cmp(big.NewInt(feeBase)) != 0 {
			t.Fatalf("baseFeePerGas[%d] = %s, want the header's %d", i, bf, feeBase)
		}
	}
}

// A range that does not end at the head needs no estimate: the block after it
// is one this archive HOLDS, so the trailing entry is that header's own base
// fee. Projecting there instead would answer 14.6 gwei for a block whose
// header says 25.
func TestFeeHistoryBelowTheHeadReadsTheNextBlock(t *testing.T) {
	env := newFeeEnv(t, 1, big.NewInt(feeBase+feeTip))

	res, rerr := env.srv.dispatch(&rpcRequest{
		Method: "eth_feeHistory", Params: mustParams(t, "0x2", "0x2"),
	})
	if rerr != nil {
		t.Fatal(rerr)
	}
	baseFees := res.(map[string]any)["baseFeePerGas"].([]any)
	if len(baseFees) != 3 {
		t.Fatalf("baseFeePerGas = %v, want 3 entries", baseFees)
	}
	if got := (*big.Int)(baseFees[2].(*hexutil.Big)); got.Cmp(big.NewInt(feeBase)) != 0 {
		t.Fatalf("trailing baseFeePerGas %s, want block 3's own %d", got, feeBase)
	}
}

// TestSuggestPriceOptionsUsesTheTipAndTheProjection: both inputs come from the
// two answers above, so both of that surface's errors used to land here as a
// tip equal to the whole gas price and a maxFeePerGas built on the CURRENT
// base fee instead of the next block's.
func TestSuggestPriceOptionsUsesTheTipAndTheProjection(t *testing.T) {
	env := newFeeEnv(t, 1, big.NewInt(feeBase+feeTip))

	res, rerr := env.srv.dispatch(&rpcRequest{Method: "eth_suggestPriceOptions"})
	if rerr != nil {
		t.Fatal(rerr)
	}
	opts, ok := res.(*priceOptions)
	if !ok {
		t.Fatalf("suggestPriceOptions returned %T", res)
	}
	st := feeState()
	projected := new(big.Int).SetUint64(uint64(st.GasPrice()))

	if tip := (*big.Int)(opts.Normal.GasTip); tip.Cmp(big.NewInt(feeTip)) != 0 {
		t.Fatalf("normal tip %s, want the tip %d, not the whole sampled price", tip, feeTip)
	}
	want := new(big.Int).Add(new(big.Int).Lsh(projected, 1), big.NewInt(feeTip))
	if fee := (*big.Int)(opts.Normal.GasFee); fee.Cmp(want) != 0 {
		t.Fatalf("normal maxFeePerGas %s, want 2*projected+tip %s", fee, want)
	}
}

// TestFeeHistoryRefusesAboveTheHeliconBoundary: past Helicon the base fee
// comes from ACP-194's gas clock, which consensus publishes only at
// settlement points and which this server never holds. coreth's estimator
// would happily read parent.Extra as ACP-176 state and answer anyway, so the
// refusal is explicit. Fuji is the network with a scheduled Helicon.
func TestFeeHistoryRefusesAboveTheHeliconBoundary(t *testing.T) {
	env := newChainEnv(t, mustCChain(t, 5), nil, func(_ uint64, h *types.Header) {
		h.Time = 1785250800 // Fuji's HeliconTime, 2026-07-28T15:00:00Z
		h.BaseFee = big.NewInt(feeBase)
		st := feeState()
		h.Extra = st.Bytes()
	})
	env.srv.EnableTxAPIs(stubCandidates{}, env.blocks, exec.ParseEthBlock)

	res, rerr := env.srv.dispatch(&rpcRequest{
		Method: "eth_feeHistory", Params: mustParams(t, "0x2", "latest"),
	})
	if rerr == nil {
		t.Fatalf("feeHistory projected a post-Helicon base fee: %v", res)
	}
	if !strings.Contains(rerr.Message, "gas clock") {
		t.Fatalf("refusal does not name the gas clock: %v", rerr)
	}
}

func mustBig(t *testing.T, env *waEnv, method string) *big.Int {
	t.Helper()
	res, rerr := env.srv.dispatch(&rpcRequest{Method: method})
	if rerr != nil {
		t.Fatalf("%s: %v", method, rerr)
	}
	got, err := hexutil.DecodeBig(res.(string))
	if err != nil {
		t.Fatalf("%s returned %v: %v", method, res, err)
	}
	return got
}

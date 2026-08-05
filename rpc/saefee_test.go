package rpc

import (
	"math/big"
	"testing"
	"time"

	ccustomtypes "github.com/ava-labs/avalanchego/graft/coreth/plugin/evm/customtypes"
	"github.com/ava-labs/avalanchego/upgrade"
	avaconstants "github.com/ava-labs/avalanchego/utils/constants"
	"github.com/ava-labs/avalanchego/vms/components/gas"
	"github.com/ava-labs/avalanchego/vms/evm/acp176"
	"github.com/ava-labs/avalanchego/vms/evm/acp226"
	"github.com/ava-labs/avalanchego/vms/saevm/cchain/dynamic"
	"github.com/ava-labs/avalanchego/vms/saevm/gastime"
	"github.com/ava-labs/avalanchego/vms/saevm/hook"
	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/common/hexutil"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/crypto"
	"github.com/ava-labs/libevm/rlp"
	"github.com/ava-labs/libevm/trie"

	"github.com/containerman17/epochdb/chain"
	"github.com/containerman17/epochdb/exec"
	"github.com/containerman17/epochdb/fetch"
	"github.com/containerman17/epochdb/state"
)

// THE POST-HELICON FEE GATE. A stored SAE header carries no base fee, so every
// fee answer above the boundary used to be ZERO and no sample caught it: post-
// SAE blocks were 36k of Fuji's 57.4M, 0.06% of the range, so an A/B "spread
// across the cooked range" essentially never drew one. Every height here is
// therefore deliberately ABOVE the boundary, and the expected values are the
// gas clock the fixture's own settlement markers commit to, computed with
// upstream gastime rather than with anything in saefee.go.

const (
	saeTxGas   = 200_000 // one transaction's gas LIMIT (what the header charges)
	saeTxUsed  = 90_000  // what it actually consumed (what the clock ticks by)
	saeOpGas   = 30_000  // end-of-block (atomic) op gas, recovered by subtraction
	saeBaseSee = 25_000_000_000
	saeTip     = 2_000_000_000

	// saeH0 is the height the fixture's SIX blocks sit above. It is past Fuji's
	// LondonBlock (805,078), which is what makes a dynamic-fee transaction
	// signable and therefore what makes effectiveGasPrice depend on the base
	// fee at all: a legacy transaction reports its own gas price regardless.
	saeH0 = 900_000
)

// saeAt maps a fixture index (1..6) to its real height.
func saeAt(i uint64) uint64 { return saeH0 + i }

// saeEnv is a chain across the Helicon boundary: block 1 is the last
// SYNCHRONOUS (coreth) block, blocks 2..6 are SAE blocks carrying real ACP-194
// settlement markers, one of them with a transaction and a stored receipt.
type saeEnv struct {
	srv    *Server
	blocks mapBlocks
	hashes map[uint64]common.Hash
	// clock[n] is the gas clock as of the post-execution state of height n,
	// computed here with upstream gastime alone.
	clock map[uint64]*gastime.Time
	// want[n] is the base fee block n executed at.
	want map[uint64]*big.Int
	tx   *types.Transaction
}

// saeGasCfg is cchain's gas configuration for a header carrying the initial
// exponents, which every block below does.
func saeGasCfg() (gas.Gas, gastime.GasPriceConfig) {
	return dynamic.InitialTargetExponent.Target(), gastime.GasPriceConfig{
		TargetToExcessScaling: 87,
		MinPrice:              dynamic.InitialPriceExponent.Price(),
	}
}

// saeConsumed is the gas the fixture's block n really consumed: the receipt
// gas of its transactions plus its end-of-block op gas. Deliberately NOT
// header.GasUsed, which under SAE is the worst case.
func (e *saeEnv) saeConsumed(n uint64) uint64 {
	if n == 3 {
		return saeTxUsed + saeOpGas
	}
	return 0
}

// newSAEEnv builds the chain, the store and the server.
func newSAEEnv(t *testing.T) *saeEnv {
	t.Helper()
	fetch.RegisterExtras(chain.Coreth)
	c := mustCChain(t, avaconstants.FujiID)
	gm, err := exec.ChainGenesis(c)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	store, err := state.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })

	helicon := uint64(upgrade.GetConfig(avaconstants.FujiID).HeliconTime.Unix())
	blkTime := func(n uint64) uint64 { return helicon + 2*(n-1) }

	// Signed for real: every receipt and transaction shape recovers the sender,
	// and a tip-bearing dynamic-fee tx is the only shape whose reported price
	// DEPENDS on the base fee.
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	to := common.HexToAddress("0xbbbb000000000000000000000000000000000002")
	tx, err := types.SignTx(
		types.NewTx(&types.DynamicFeeTx{
			ChainID: gm.Config.ChainID, Nonce: 0, To: &to, Gas: saeTxGas,
			GasFeeCap: big.NewInt(saeBaseSee * 2), GasTipCap: big.NewInt(saeTip),
		}),
		types.MakeSigner(gm.Config, new(big.Int).SetUint64(saeAt(3)), blkTime(3)), key)
	if err != nil {
		t.Fatal(err)
	}

	env := &saeEnv{
		blocks: mapBlocks{}, hashes: map[uint64]common.Hash{},
		clock: map[uint64]*gastime.Time{}, want: map[uint64]*big.Int{}, tx: tx,
	}

	// Block 1's post-execution clock is the ACP-176 -> ACP-194 handoff:
	// seeded from its own base fee, exactly as blocks.WorstCaseGasTime does.
	target, cfg := saeGasCfg()
	clock1, err := gastime.New(saeBlockTimeOf(blkTime(1)), target, saeBaseSee, cfg)
	if err != nil {
		t.Fatal(err)
	}
	env.clock[1] = clock1

	// Settlement plan. Height 3 is NEVER named by any header's markers (block 5
	// jumps 2 -> 4), which is the between-settlement-points case; block 4 is a
	// settling block whose own markers name a real height.
	settles := map[uint64]uint64{2: 1, 3: 1, 4: 2, 5: 4, 6: 4}

	var parent common.Hash
	for n := uint64(1); n <= 6; n++ {
		var blk *types.Block
		if n == 1 {
			blk = types.NewBlockWithHeader(saeSyncHeader(saeAt(n), parent, blkTime(n)))
		} else {
			s := settles[n]
			// Advance the clock from the settled height to n-1, then read the
			// base fee at n's own block time: saexec.Execute's order.
			clk := env.clock[s].Clone()
			for m := s + 1; m < n; m++ {
				clk.BeforeBlock(saeBlockTimeOf(blkTime(m)))
				if err := clk.AfterBlock(gas.Gas(env.saeConsumed(m)), target, cfg); err != nil {
					t.Fatal(err)
				}
			}
			clk.BeforeBlock(saeBlockTimeOf(blkTime(n)))
			env.want[n] = clk.BaseFee().ToBig()
			if err := clk.AfterBlock(gas.Gas(env.saeConsumed(n)), target, cfg); err != nil {
				t.Fatal(err)
			}
			env.clock[n] = clk

			var txs types.Transactions
			gasUsed := uint64(0)
			if n == 3 {
				txs = types.Transactions{tx}
				gasUsed = saeTxGas + saeOpGas // worst case: LIMITS plus op gas
			}
			h := saeHeader(saeAt(n), parent, blkTime(n), gasUsed, hook.Settled{
				Height:       saeAt(s),
				GasUnix:      env.clock[s].Unix(),
				GasNumerator: env.clock[s].Fraction().Numerator,
				Excess:       env.clock[s].Excess(),
			})
			blk = types.NewBlock(h, txs, nil, nil, trie.NewStackTrie(nil))
		}
		raw, err := rlp.EncodeToBytes(blk)
		if err != nil {
			t.Fatal(err)
		}
		hdrRLP, err := rlp.EncodeToBytes(blk.Header())
		if err != nil {
			t.Fatal(err)
		}
		if err := store.AppendHeader(saeAt(n), hdrRLP); err != nil {
			t.Fatal(err)
		}
		if n == 3 {
			rcpt := state.EncodeStoredReceipts(types.Receipts{{GasUsed: saeTxUsed, Status: 1}})
			if err := store.AppendRcpt(saeAt(n), state.EncodeTailRcpt(nil, rcpt)); err != nil {
				t.Fatal(err)
			}
		}
		env.blocks[saeAt(n)] = raw
		env.hashes[saeAt(n)] = blk.Hash()
		parent = blk.Hash()
	}
	if err := store.FlushAndSetExecHead(saeAt(6)); err != nil {
		t.Fatal(err)
	}
	hist, err := state.OpenHistory(dir, store, gm.TrieAlloc)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(hist.Close)
	hist.SetHead(saeAt(6))
	env.srv = NewServer(hist, HistoryChainContext(hist), gm.Config)
	env.srv.EnableTxAPIs(
		stubCandidates{at: map[common.Hash][]uint64{tx.Hash(): {saeAt(3)}}},
		env.blocks, exec.ParseEthBlock)
	return env
}

// saeBlockTimeOf is the fixture's block time as gastime sees it. The
// milliseconds field is a whole second here, so there is no sub-second part.
func saeBlockTimeOf(sec uint64) time.Time { return time.Unix(int64(sec), 0) }

// blockTime is the fixture's schedule: one block every two seconds from the
// Helicon boundary.
func (e *saeEnv) blockTime(n uint64) uint64 {
	return uint64(upgrade.GetConfig(avaconstants.FujiID).HeliconTime.Unix()) + 2*(n-1)
}

// saeSyncHeader is the last pre-SAE block: no settlement markers, its own base
// fee, and the ACP-176 fee state in Extra that seeds the gas clock.
func saeSyncHeader(n uint64, parent common.Hash, blkTime uint64) *types.Header {
	ms := blkTime * 1000
	delay := acp226.InitialDelayExcess
	st := acp176.State{Gas: gas.State{Capacity: 10_000_000, Excess: 0}, TargetExcess: 0}
	return ccustomtypes.WithHeaderExtra(
		&types.Header{
			ParentHash: parent, Number: new(big.Int).SetUint64(n),
			GasLimit: 10_000_000, Time: blkTime, Difficulty: big.NewInt(1),
			BaseFee: big.NewInt(saeBaseSee), Extra: st.Bytes(),
			BlobGasUsed: new(uint64), ExcessBlobGas: new(uint64), ParentBeaconRoot: new(common.Hash),
		},
		&ccustomtypes.HeaderExtra{
			ExtDataGasUsed: big.NewInt(0), BlockGasCost: big.NewInt(0),
			TimeMilliseconds: &ms, MinDelayExcess: &delay,
		})
}

// saeHeader is a post-Helicon header: the four settlement markers, the two
// exponents, worst-case GasUsed, and NO BASE FEE, which is the whole point.
func saeHeader(n uint64, parent common.Hash, blkTime, gasUsed uint64, s hook.Settled) *types.Header {
	ms := blkTime * 1000
	delay := acp226.InitialDelayExcess
	te, pe := dynamic.InitialTargetExponent, dynamic.InitialPriceExponent
	gasNum, excess := uint64(s.GasNumerator), uint64(s.Excess)
	return ccustomtypes.WithHeaderExtra(
		&types.Header{
			ParentHash: parent, Number: new(big.Int).SetUint64(n),
			GasLimit: 10_000_000, GasUsed: gasUsed, Time: blkTime, Difficulty: big.NewInt(1),
			BlobGasUsed: new(uint64), ExcessBlobGas: new(uint64), ParentBeaconRoot: new(common.Hash),
		},
		&ccustomtypes.HeaderExtra{
			ExtDataGasUsed: big.NewInt(0), BlockGasCost: big.NewInt(0),
			TimeMilliseconds: &ms, MinDelayExcess: &delay,
			TargetExponent: &te, MinPriceExponent: &pe,
			SettledHeight: &s.Height, SettledGasUnix: &s.GasUnix,
			SettledGasNumerator: &gasNum, SettledExcess: &excess,
		})
}

// TestSAEBaseFeeAboveBoundary is the core gate: every fee answer above the
// Helicon boundary reports the gas clock's value, not the zero the header
// carries. It covers a settling block, a height no header ever settles, and a
// height whose derivation must walk a block that consumed gas.
func TestSAEBaseFeeAboveBoundary(t *testing.T) {
	env := newSAEEnv(t)
	for n := uint64(2); n <= 6; n++ {
		want := env.want[n]
		if want.Sign() <= 0 {
			t.Fatalf("fixture is degenerate: block %d expects base fee %s", n, want)
		}
		got, rerr := env.srv.baseFeeAt(saeAt(n))
		if rerr != nil {
			t.Fatalf("baseFeeAt(%d): %s", saeAt(n), rerr.Message)
		}
		if got.Cmp(want) != 0 {
			t.Fatalf("base fee at %d = %s, want %s (the gas clock's)", n, got, want)
		}
	}
	// The whole point: the header itself says nothing.
	h, rerr := env.srv.headerAt(saeAt(4))
	if rerr != nil {
		t.Fatal(rerr.Message)
	}
	if h.BaseFee != nil && h.BaseFee.Sign() != 0 {
		t.Fatalf("fixture header carries base fee %s: it must carry none", h.BaseFee)
	}
	// And the answers are not one constant: the clock decays with block time.
	if env.want[2].Cmp(env.want[6]) == 0 {
		t.Fatal("fixture is degenerate: the base fee never moves across the range")
	}
}

// TestSAEBaseFeeUsesConsumedGasNotWorstCase pins the one input that is not a
// header field. Under SAE header.GasUsed is the WORST CASE (transaction gas
// LIMITS plus op gas) while the clock ticks by what execution really consumed,
// so a derivation that reads GasUsed is wrong in a way that still looks like a
// plausible fee. Block 4 sits one block above the block that carries gas.
func TestSAEBaseFeeUsesConsumedGasNotWorstCase(t *testing.T) {
	env := newSAEEnv(t)
	target, cfg := saeGasCfg()

	// What block 4 would answer if the walk ticked by header.GasUsed.
	clk := env.clock[2].Clone()
	clk.BeforeBlock(saeBlockTimeOf(env.blockTime(3)))
	if err := clk.AfterBlock(gas.Gas(saeTxGas+saeOpGas), target, cfg); err != nil {
		t.Fatal(err)
	}
	clk.BeforeBlock(saeBlockTimeOf(env.blockTime(4)))
	worstCase := clk.BaseFee().ToBig()

	if worstCase.Cmp(env.want[4]) == 0 {
		t.Skip("fixture cannot distinguish worst-case from consumed gas at this excess")
	}
	got, rerr := env.srv.baseFeeAt(saeAt(4))
	if rerr != nil {
		t.Fatal(rerr.Message)
	}
	if got.Cmp(worstCase) == 0 {
		t.Fatalf("base fee at 4 = %s: the walk ticked by the WORST-CASE gas, not the consumed gas", got)
	}
	if got.Cmp(env.want[4]) != 0 {
		t.Fatalf("base fee at 4 = %s, want %s", got, env.want[4])
	}
}

// TestSAEReceiptEffectiveGasPriceIsBasePlusTip: a receipt above the boundary
// used to report the TIP alone, because the base fee it added was the header's
// zero. A wallet reads effectiveGasPrice to learn what it paid.
func TestSAEReceiptEffectiveGasPriceIsBasePlusTip(t *testing.T) {
	env := newSAEEnv(t)
	blk, rerr := env.srv.blockAt(saeAt(3))
	if rerr != nil {
		t.Fatal(rerr.Message)
	}
	receipts, rerr := env.srv.storedBlockReceipts(blk)
	if rerr != nil {
		t.Fatal(rerr.Message)
	}
	want := new(big.Int).Add(env.want[3], big.NewInt(saeTip))
	if got := receipts[0].EffectiveGasPrice; got.Cmp(want) != 0 {
		t.Fatalf("effectiveGasPrice = %s, want %s (base %s + tip %d)",
			got, want, env.want[3], saeTip)
	}
	if receipts[0].EffectiveGasPrice.Cmp(big.NewInt(saeTip)) == 0 {
		t.Fatal("effectiveGasPrice is the tip alone: the base fee was read as zero")
	}

	// The same number through the public method, and through the transaction
	// shape, which derives its gasPrice the same way.
	out, rerr := env.srv.getTransactionReceipt(mustParams(t, env.tx.Hash()))
	if rerr != nil {
		t.Fatal(rerr.Message)
	}
	if got := out.(map[string]any)["effectiveGasPrice"]; got != (*hexutil.Big)(want) &&
		(*big.Int)(got.(*hexutil.Big)).Cmp(want) != 0 {
		t.Fatalf("eth_getTransactionReceipt effectiveGasPrice = %v, want %s", got, want)
	}
	txOut, rerr := env.srv.getTransactionByHash(mustParams(t, env.tx.Hash()))
	if rerr != nil {
		t.Fatal(rerr.Message)
	}
	if got := (*big.Int)(txOut.(*rpcTransaction).GasPrice); got.Cmp(want) != 0 {
		t.Fatalf("eth_getTransactionByHash gasPrice = %s, want %s", got, want)
	}
}

// TestSAEBlockAndFeeHistoryReportTheDerivedBaseFee covers the remaining read
// surface: eth_getBlockByNumber, eth_baseFee and eth_feeHistory (per-block
// entries AND the trailing projection, which above the boundary used to refuse
// outright and take the whole call down with it).
func TestSAEBlockAndFeeHistoryReportTheDerivedBaseFee(t *testing.T) {
	env := newSAEEnv(t)

	blkOut, rerr := env.srv.getBlockByNumber(mustParams(t, hexutil.EncodeUint64(saeAt(3))))
	if rerr != nil {
		t.Fatal(rerr.Message)
	}
	got := (*big.Int)(blkOut.(map[string]any)["baseFeePerGas"].(*hexutil.Big))
	if got.Cmp(env.want[3]) != 0 {
		t.Fatalf("eth_getBlockByNumber baseFeePerGas = %s, want %s", got, env.want[3])
	}

	bf, rerr := env.srv.baseFee()
	if rerr != nil {
		t.Fatal(rerr.Message)
	}
	if bf.(string) != hexutil.EncodeBig(env.want[6]) {
		t.Fatalf("eth_baseFee = %v, want %s", bf, hexutil.EncodeBig(env.want[6]))
	}

	res, rerr := env.srv.feeHistory(mustParams(t, "0x3", hexutil.EncodeUint64(saeAt(5))))
	if rerr != nil {
		t.Fatalf("eth_feeHistory above the boundary: %s", rerr.Message)
	}
	fees := res.(map[string]any)["baseFeePerGas"].([]any)
	if len(fees) != 4 {
		t.Fatalf("baseFeePerGas = %v, want blockCount+1 = 4 entries", fees)
	}
	for i, n := range []uint64{3, 4, 5} {
		if v := (*big.Int)(fees[i].(*hexutil.Big)); v.Cmp(env.want[n]) != 0 {
			t.Fatalf("feeHistory baseFeePerGas[%d] (block %d) = %s, want %s", i, n, v, env.want[n])
		}
	}
	// The trailing entry describes block 6, which this node HOLDS.
	if v := (*big.Int)(fees[3].(*hexutil.Big)); v.Cmp(env.want[6]) != 0 {
		t.Fatalf("feeHistory trailing baseFeePerGas = %s, want block 6's %s", v, env.want[6])
	}
}

// TestSAEProjectionAtTheHead: a range ENDING at the head has no next block to
// read, so the trailing entry is projected off the gas clock. It used to refuse
// by name above the boundary, which failed the whole eth_feeHistory call.
func TestSAEProjectionAtTheHead(t *testing.T) {
	env := newSAEEnv(t)
	next, rerr := env.srv.nextBaseFee(saeAt(6))
	if rerr != nil {
		t.Fatalf("projection above the boundary: %s", rerr.Message)
	}
	if next == nil || next.Sign() <= 0 {
		t.Fatalf("projected base fee above the head = %v, want a real number", next)
	}
	// The block after 6 is assumed to follow immediately, so it prices at
	// block 6's own gas clock advanced past block 6 (which consumed nothing
	// here) and no further: never above the fee block 6 itself paid.
	if next.Cmp(env.want[6]) > 0 {
		t.Fatalf("projection %s exceeds block 6's own base fee %s: elapsed time only lowers it", next, env.want[6])
	}
}

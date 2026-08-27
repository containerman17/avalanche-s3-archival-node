package exec

// THE REGRESSION FOR MAINNET C BLOCK 5,456,905, which halted the executor at
// 04:41Z on 2026-08-14 with "1 of 4 frames never got a CaptureExit". Its first
// transaction, 0xc5ad6d3c56b8a5245e0bebca8d8f0d588cab306799d6a901e7480aaa3648aee7,
// is a NativeAssetCall: sender 0x4464bbe4 sends 0.2 of asset 0xf1bc96d3 to
// 0xed2b42d3 and calls deposit() on it, through the coreth precompile at
// 0x0100000000000000000000000000000000000002.
//
// The transaction is REBUILT here rather than replayed, because the shape is
// the whole content: a stateful precompile that calls back into the EVM used to
// make libevm announce the SAME call twice (PrecompileEnvironment.Call fired
// CaptureEnter, then handed the call to evm.call which fired CaptureEnter
// again) and close its own announcement with CaptureEnd instead of CaptureExit.
// Both entry depths are covered: straight to the precompile as the mainnet
// transaction does, and through a contract, which is the case where the
// duplicate would otherwise be stored as a call that never happened.
//
// THIS ASSERTS THE UPSTREAM FIX, NOT A WORKAROUND. libevm db6d70f2748e removed
// the manual tracer block from PrecompileEnvironment.Call, so evm.Call emits
// the balanced pair on its own and exec/frames.go folds nothing. The capture
// used to reach the same frame counts by collapsing the duplicate; it now
// reaches them because the duplicate is not emitted. That is strictly stronger:
// the old test passed on both a fixed and an unfixed libevm, this one fails on
// an unfixed one, so it is the guard that keeps the dependency from regressing.

import (
	"math/big"
	"testing"

	"github.com/ava-labs/avalanchego/graft/coreth/core/extstate"
	"github.com/ava-labs/avalanchego/graft/coreth/nativeasset"
	corethparams "github.com/ava-labs/avalanchego/graft/coreth/params"
	"github.com/ava-labs/avalanchego/graft/coreth/params/extras"
	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/rawdb"
	"github.com/ava-labs/libevm/core/state"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/core/vm"
	ethparams "github.com/ava-labs/libevm/params"
	"github.com/holiman/uint256"

	"github.com/containerman17/avalanche-s3-archival-node/chain"
	"github.com/containerman17/avalanche-s3-archival-node/fetch"
	"github.com/containerman17/avalanche-s3-archival-node/store"
)

func TestFrameCaptureOfAPrecompileCallOut(t *testing.T) {
	fetch.RegisterExtras(chain.Coreth)

	sdb, err := state.New(types.EmptyRootHash, state.NewDatabase(rawdb.NewMemoryDatabase()), nil)
	if err != nil {
		t.Fatal(err)
	}
	st := extstate.New(sdb)

	sender := common.HexToAddress("0x4464bbe40cc1f1184bda0d43c010298fda3d8d7e")
	callee := common.HexToAddress("0xed2b42d3c9c6e97e11755bb37df29b6375ede3eb")
	proxy := common.HexToAddress("0x00000000000000000000000000000000000000aa")
	assetID := common.HexToHash("0xf1bc96d3237a93e8d18e576ca90e578eb2d04c6ae57728109525c65905e56dcf")
	amount, _ := new(big.Int).SetString("200000000000000000", 10)
	deposit := common.FromHex("0xd0e30db0")

	for _, a := range []common.Address{sender, proxy} {
		st.CreateAccount(a)
		st.AddBalanceMultiCoin(a, assetID, new(big.Int).Mul(amount, big.NewInt(10)))
	}
	// The callee only has to exist and run: what it does is not the subject.
	st.CreateAccount(callee)
	st.SetCode(callee, []byte{0x00}) // STOP
	// proxy forwards its whole calldata to the NativeAssetCall precompile, so
	// the precompile runs one level down instead of at the top of the call.
	proxyCode := []byte{
		0x36, 0x60, 0x00, 0x60, 0x00, 0x37, // calldatacopy(0, 0, calldatasize)
		0x60, 0x00, // retSize
		0x60, 0x00, // retOffset
		0x36,       // argsSize = calldatasize
		0x60, 0x00, // argsOffset
		0x60, 0x00, // value
		0x73, // push20 <precompile>
	}
	proxyCode = append(proxyCode, nativeasset.NativeAssetCallAddr.Bytes()...)
	proxyCode = append(proxyCode, 0x5a, 0xf1, 0x00) // gas, call, stop
	st.SetCode(proxy, proxyCode)

	zero := uint64(0)
	cfg := corethparams.WithExtra(
		&ethparams.ChainConfig{
			ChainID:             big.NewInt(43114),
			HomesteadBlock:      new(big.Int),
			EIP150Block:         new(big.Int),
			EIP155Block:         new(big.Int),
			EIP158Block:         new(big.Int),
			ByzantiumBlock:      new(big.Int),
			ConstantinopleBlock: new(big.Int),
			PetersburgBlock:     new(big.Int),
			IstanbulBlock:       new(big.Int),
			MuirGlacierBlock:    new(big.Int),
			BerlinBlock:         new(big.Int),
			LondonBlock:         new(big.Int),
		},
		// Apricot Phase 2 is what puts NativeAssetCall in the precompile set;
		// the halted block is Phase 4 era (2021-10-10), well inside its life.
		&extras.ChainConfig{NetworkUpgrades: extras.NetworkUpgrades{
			ApricotPhase1BlockTimestamp: &zero,
			ApricotPhase2BlockTimestamp: &zero,
			ApricotPhase3BlockTimestamp: &zero,
			ApricotPhase4BlockTimestamp: &zero,
		}},
	)
	hdr := &types.Header{Number: big.NewInt(5456905), Time: 1633867674}
	input := nativeasset.PackNativeAssetCallInput(callee, assetID, amount, deposit)

	const gasLimit = 8_000_000
	run := func(to common.Address) []store.Frame {
		t.Helper()
		c := newFrameCapture()
		l := frameLogger{c}
		evm := vm.NewEVM(vm.BlockContext{
			CanTransfer: func(vm.StateDB, common.Address, *uint256.Int) bool { return true },
			Transfer:    func(vm.StateDB, common.Address, common.Address, *uint256.Int) {},
			BlockNumber: hdr.Number, Time: hdr.Time, Difficulty: new(big.Int),
			GasLimit: gasLimit, BaseFee: big.NewInt(1), Header: hdr,
		}, vm.TxContext{Origin: sender, GasPrice: big.NewInt(1)}, sdb, cfg, vm.Config{Tracer: l})

		l.CaptureTxStart(gasLimit)
		if _, _, err := evm.Call(vm.AccountRef(sender), to, input, gasLimit, new(uint256.Int)); err != nil {
			t.Fatalf("call to %s reverted: %v", to, err)
		}
		// THE HALT: on a libevm before db6d70f2748e take() refused this
		// transaction (one enter never got an exit) and the executor turned
		// that refusal into log.Fatalf. It must now close on its own.
		rec, _, why := c.take()
		if why != "" {
			t.Fatalf("a NativeAssetCall transaction was refused as an uncaptured trace: %s", why)
		}
		frames, err := store.DecodeFrames(rec)
		if err != nil {
			t.Fatal(err)
		}
		return frames
	}

	// 1. The mainnet shape: the transaction IS the call to the precompile, so
	// the only frame is the one call the precompile makes. The capture pushes
	// one frame per CaptureEnter and drops none, so ONE frame here is the
	// direct assertion that libevm announces this call exactly once.
	frames := run(nativeasset.NativeAssetCallAddr)
	if len(frames) != 1 {
		t.Fatalf("got %d frames for a direct NativeAssetCall, want 1: %+v", len(frames), frames)
	}
	f := frames[0]
	if f.From != sender || f.To != callee || string(f.Input) != string(deposit) || f.Depth != 0 || f.Failed {
		t.Errorf("the precompile's call was not recorded faithfully: %+v", f)
	}
	// The gas is the precompile's remaining gas, NOT a zero-gas placeholder:
	// an open frame serialised with gas accounting missing is exactly the
	// silent hole the fail-stop exists to refuse.
	if f.Gas != gasLimit-corethparams.AssetCallApricot {
		t.Errorf("frame gas %d, want %d (gas limit minus the precompile's own cost)", f.Gas, gasLimit-corethparams.AssetCallApricot)
	}

	// 2. One level down: the call INTO the precompile is its own frame, and
	// the call the precompile makes hangs under it. Two frames, not three, and
	// again with no fold in the way to reach that count.
	frames = run(proxy)
	if len(frames) != 2 {
		t.Fatalf("got %d frames for a contract-driven NativeAssetCall, want 2: %+v", len(frames), frames)
	}
	if frames[0].To != nativeasset.NativeAssetCallAddr || frames[0].Depth != 0 {
		t.Errorf("the call into the precompile is not the outer frame: %+v", frames[0])
	}
	if frames[1].From != proxy || frames[1].To != callee || frames[1].Depth != 1 {
		t.Errorf("the precompile's own call is not nested under it: %+v", frames[1])
	}
	// A genuine nested call is always forwarded strictly less gas than the
	// frame that makes it: that frame pays the CALL opcode's own cost first and
	// EIP-150 then withholds a 64th. Equal gas across a parent and its child is
	// the fingerprint of the duplicate announcement, so this is what fails
	// loudly if a libevm bump ever brings the double enter back.
	if frames[1].Gas >= frames[0].Gas {
		t.Errorf("nested frame gas %d is not below its parent's %d, so these are one call announced twice, not two calls",
			frames[1].Gas, frames[0].Gas)
	}
}

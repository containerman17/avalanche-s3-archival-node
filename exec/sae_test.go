package exec

import (
	"math/big"
	"testing"

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
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/rlp"

	cparams "github.com/ava-labs/avalanchego/graft/coreth/params"

	"github.com/containerman17/epochdb/chain"
	"github.com/containerman17/epochdb/fetch"
	"github.com/containerman17/epochdb/state"
)

// saeTestBlock is a minimal post-Helicon-shaped block, used where only the
// header shape (settlement markers present or not) matters.
func saeTestBlock(t *testing.T, e *Executor, num uint64, parent common.Hash, blkTime uint64, markers bool) *types.Block {
	t.Helper()
	if markers {
		return craftSAEBlock(num, parent, blkTime, common.Hash{}, hook.Settled{})
	}
	return craftSyncBlock(num, parent, blkTime, e.genesisRoot)
}

// craftSyncBlock builds a pre-SAE (coreth) block: no settlement markers,
// its own post-execution root in header.Root, and an ACP-176 fee state in
// Extra, which is what seeds the gas clock across the transition.
func craftSyncBlock(num uint64, parent common.Hash, blkTime uint64, root common.Hash) *types.Block {
	ms := blkTime * 1000
	delay := acp226.InitialDelayExcess
	feeState := acp176.State{Gas: gas.State{Capacity: 10_000_000, Excess: 0}, TargetExcess: 0}
	h := ccustomtypes.WithHeaderExtra(
		&types.Header{
			ParentHash:       parent,
			Number:           new(big.Int).SetUint64(num),
			Root:             root,
			GasLimit:         10_000_000,
			Time:             blkTime,
			Difficulty:       big.NewInt(1),
			BaseFee:          big.NewInt(25_000_000_000),
			Extra:            feeState.Bytes(),
			BlobGasUsed:      new(uint64),
			ExcessBlobGas:    new(uint64),
			ParentBeaconRoot: new(common.Hash),
		},
		&ccustomtypes.HeaderExtra{
			ExtDataGasUsed:   big.NewInt(0),
			BlockGasCost:     big.NewInt(0),
			TimeMilliseconds: &ms,
			MinDelayExcess:   &delay,
		},
	)
	return types.NewBlockWithHeader(h)
}

// craftSAEBlock builds a post-Helicon block: settlement markers, the
// ACP-176/ACP-283 exponents, and header.Root carrying the SETTLED block's
// post-execution root rather than its own.
func craftSAEBlock(num uint64, parent common.Hash, blkTime uint64, settledRoot common.Hash, s hook.Settled) *types.Block {
	ms := blkTime * 1000
	delay := acp226.InitialDelayExcess
	te, pe := dynamic.InitialTargetExponent, dynamic.InitialPriceExponent
	gasNum, excess := uint64(s.GasNumerator), uint64(s.Excess)
	h := ccustomtypes.WithHeaderExtra(
		&types.Header{
			ParentHash:       parent,
			Number:           new(big.Int).SetUint64(num),
			Root:             settledRoot,
			GasLimit:         10_000_000,
			GasUsed:          0, // worst-case charged gas of an empty body
			Time:             blkTime,
			Difficulty:       big.NewInt(1),
			BaseFee:          big.NewInt(25_000_000_000),
			BlobGasUsed:      new(uint64),
			ExcessBlobGas:    new(uint64),
			ParentBeaconRoot: new(common.Hash),
		},
		&ccustomtypes.HeaderExtra{
			ExtDataGasUsed:      big.NewInt(0),
			BlockGasCost:        big.NewInt(0),
			TimeMilliseconds:    &ms,
			MinDelayExcess:      &delay,
			TargetExponent:      &te,
			MinPriceExponent:    &pe,
			SettledHeight:       &s.Height,
			SettledGasUnix:      &s.GasUnix,
			SettledGasNumerator: &gasNum,
			SettledExcess:       &excess,
		},
	)
	return types.NewBlockWithHeader(h)
}

func encodeBlock(t *testing.T, b *types.Block) []byte {
	t.Helper()
	raw, err := rlp.EncodeToBytes(b)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// settledMarkers builds the settlement markers a block would carry for a
// settled height whose post-execution gas clock is `c`.
func settledMarkers(height uint64, c *gastime.Time) hook.Settled {
	return hook.Settled{
		Height:       height,
		GasUnix:      c.Unix(),
		GasNumerator: c.Fraction().Numerator,
		Excess:       c.Excess(),
	}
}

// TestSAEChainAcrossBoundary drives a crafted chain across the Helicon
// boundary: a transition block executed by coreth, then SAE blocks whose
// headers attest the SETTLED block's root rather than their own. It covers
// the engine swap, the root ring, the settled-root and settled-gas-clock
// checks, the restart path that rebuilds the SAE parent from the ring, and
// the hard stop on a bad settled root.
func TestSAEChainAcrossBoundary(t *testing.T) {
	fetch.RegisterExtras(chain.Coreth)
	dir := t.TempDir()

	store, err := state.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	src := fakeSource{}
	e, err := New(Config{DataDir: dir, Blocks: src, Store: store}) // Fuji
	if err != nil {
		t.Fatal(err)
	}

	g, err := loadCorethGenesis(mustCChain(t, e.snowCtx.NetworkID), e.snowCtx)
	if err != nil {
		t.Fatal(err)
	}
	helicon := uint64(upgrade.GetConfig(avaconstants.FujiID).HeliconTime.Unix())

	// Block 1 is the transition block: its parent (genesis) predates the
	// switch, so coreth still owns it. Empty, so its root is the genesis
	// root and the coreth fast path takes it.
	blk1 := craftSyncBlock(1, g.ToBlock().Hash(), helicon, e.genesisRoot)
	src[1] = encodeBlock(t, blk1)
	if err := e.executeRaw(1, src[1]); err != nil {
		t.Fatalf("transition block: %v", err)
	}
	if e.sae != nil {
		t.Fatal("transition block must not take the SAE path")
	}

	// Block 2 is the first SAE block. It settles the transition block, so
	// its header.Root is block 1's own root and its markers carry block
	// 1's post-execution gas clock (upstream's synchronous handoff).
	hooks := &saeHooks{chainCfg: e.chainCfg, avaxAssetID: e.snowCtx.AVAXAssetID}
	clock1, err := hooks.worstCaseGasTime(blk1.Header())
	if err != nil {
		t.Fatal(err)
	}
	blk2 := craftSAEBlock(2, blk1.Hash(), helicon+2, blk1.Root(), settledMarkers(1, clock1))
	src[2] = encodeBlock(t, blk2)
	if err := e.executeRaw(2, src[2]); err != nil {
		t.Fatalf("first SAE block: %v", err)
	}
	if e.sae == nil || e.headNum != 2 {
		t.Fatalf("head=%d sae=%v after the first SAE block", e.headNum, e.sae != nil)
	}
	root2, clockBytes2, ok := e.ring.get(2)
	if !ok {
		t.Fatal("block 2 not recorded in the root ring")
	}
	if root2 != e.headRoot {
		t.Fatalf("ring root %x != head root %x", root2, e.headRoot)
	}
	clock2 := new(gastime.Time)
	if err := clock2.UnmarshalCanoto(clockBytes2); err != nil {
		t.Fatal(err)
	}

	// A block settling height 2 with the WRONG root must hard-stop.
	bad := craftSAEBlock(3, blk2.Hash(), helicon+4, common.HexToHash("0xdead"), settledMarkers(2, clock2))
	if err := e.executeRaw(3, encodeBlock(t, bad)); err == nil {
		t.Fatal("a wrong settled root must not verify")
	}

	// Restart before the good block 3: the SAE parent (block 2) has to be
	// rebuilt from the persisted ring, since no header carries its root.
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = state.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	e, err = New(Config{DataDir: dir, Blocks: src, Store: store})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer e.Close()
	if e.headNum != 2 || e.headRoot != root2 || e.headTime != helicon+2 {
		t.Fatalf("resume head=%d root=%x time=%d, want 2/%x/%d", e.headNum, e.headRoot, e.headTime, root2, helicon+2)
	}

	blk3 := craftSAEBlock(3, blk2.Hash(), helicon+4, root2, settledMarkers(2, clock2))
	src[3] = encodeBlock(t, blk3)
	if err := e.executeRaw(3, src[3]); err != nil {
		t.Fatalf("block 3 after restart: %v", err)
	}
	if e.headNum != 3 {
		t.Fatalf("head=%d, want 3", e.headNum)
	}
	if e.sae.settledHeight != 2 || e.sae.lagMax != 1 {
		t.Fatalf("settled=%d lagMax=%d, want 2/1", e.sae.settledHeight, e.sae.lagMax)
	}

	// Settlement never moves backwards.
	back := craftSAEBlock(4, blk3.Hash(), helicon+6, blk1.Root(), settledMarkers(1, clock1))
	if err := e.executeRaw(4, encodeBlock(t, back)); err == nil {
		t.Fatal("a settlement marker moving backwards must not verify")
	}
}

// TestSAEGasFloor pins the ACP-194 per-transaction charged-gas floor that
// SAE execution depends on: it rides coreth's RulesExtra (libevm
// RulesHooks.MinimumGasConsumption), which is why saexec.Execute charges it
// without any wiring of ours, and it must be inert below the boundary.
func TestSAEGasFloor(t *testing.T) {
	fetch.RegisterExtras(chain.Coreth)
	snowCtx, err := snowContextFor(mustCChain(t, avaconstants.FujiID))
	if err != nil {
		t.Fatal(err)
	}
	g, err := loadCorethGenesis(mustCChain(t, avaconstants.FujiID), snowCtx)
	if err != nil {
		t.Fatal(err)
	}
	helicon := uint64(upgrade.GetConfig(avaconstants.FujiID).HeliconTime.Unix())
	num := big.NewInt(60_000_000)

	post := g.Config.Rules(num, cparams.IsMergeTODO, helicon)
	if got := post.Hooks().MinimumGasConsumption(21_001); got != 10_501 {
		t.Fatalf("post-Helicon minimum gas consumption of 21001 = %d, want 10501 (ceil(g/2))", got)
	}
	pre := g.Config.Rules(num, cparams.IsMergeTODO, helicon-1)
	if got := pre.Hooks().MinimumGasConsumption(21_001); got != 0 {
		t.Fatalf("pre-Helicon minimum gas consumption = %d, want 0", got)
	}
}

// TestSAERingRoundTrip covers the slot file: values survive a reopen, an
// aged-out height reads as absent, and a torn slot is not trusted.
func TestSAERingRoundTrip(t *testing.T) {
	dir := t.TempDir()
	r, err := openSAERing(dir)
	if err != nil {
		t.Fatal(err)
	}
	root := common.HexToHash("0xabc")
	if err := r.put(7, root, []byte("clock")); err != nil {
		t.Fatal(err)
	}
	if err := r.close(); err != nil {
		t.Fatal(err)
	}

	r, err = openSAERing(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.close()
	got, clock, ok := r.get(7)
	if !ok || got != root || string(clock) != "clock" {
		t.Fatalf("get(7) = %x, %q, %v", got, clock, ok)
	}
	if _, _, ok := r.get(8); ok {
		t.Fatal("unwritten height must read as absent")
	}
	// Same slot, one full lap later: the height field must disqualify it.
	if _, _, ok := r.get(7 + saeRingSlots); ok {
		t.Fatal("aged-out height must read as absent")
	}
	r.slot(7)[saeRingOffRoot] ^= 0xff
	if _, _, ok := r.get(7); ok {
		t.Fatal("a torn slot must read as absent")
	}
}

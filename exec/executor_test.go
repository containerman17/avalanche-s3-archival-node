package exec

import (
	"errors"
	"math/big"
	"testing"

	"github.com/ava-labs/avalanchego/upgrade"
	avaconstants "github.com/ava-labs/avalanchego/utils/constants"
	"github.com/ava-labs/libevm/common"
	ethstate "github.com/ava-labs/libevm/core/state"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/libevm/stateconf"
	"github.com/ava-labs/libevm/rlp"

	"github.com/containerman17/epochdb/fetch"
	"github.com/containerman17/epochdb/state"
)

type fakeSource map[uint64][]byte

func (f fakeSource) GetByHeight(n uint64) ([]byte, bool, error) {
	raw, ok := f[n]
	return raw, ok, nil
}

// TestRestartAtGenesisExecutesFirstBlock is the regression for the
// production wall "no proposal found for block 1": process 1 commits
// genesis and exits at head 0; process 2 reopens (genesis already on
// disk, exechead=0) and must be able to commit the first NON-empty
// block, whose Firewood Update resolves its parent by the genesis BLOCK
// hash. A fresh-opened Firewood only knows the zero hash until
// SetHashAndHeight registers the real one.
func TestRestartAtGenesisExecutesFirstBlock(t *testing.T) {
	fetch.RegisterExtras()
	dir := t.TempDir()

	store, err := state.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	e, err := New(Config{DataDir: dir, Blocks: fakeSource{}, Store: store})
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	// "Restart": genesis initialized on disk, exechead=0, no headers.
	store, err = state.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	e, err = New(Config{DataDir: dir, Blocks: fakeSource{}, Store: store})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer e.Close()

	// Mimic executeBlock's commit path for a non-empty block 1: a state
	// change committed with the (genesis hash, block-1 hash) payload.
	sdb, err := ethstate.New(e.genesisRoot, e.wrapDB, nil)
	if err != nil {
		t.Fatal(err)
	}
	sdb.SetNonce(common.HexToAddress("0x1234"), 1)
	blk1Hash := common.HexToHash("0xb10c")
	root, err := sdb.Commit(1, true, stateconf.WithTrieDBUpdateOpts(
		stateconf.WithTrieDBUpdatePayload(e.genesisHash, blk1Hash)))
	if err != nil {
		t.Fatalf("first non-empty block after restart at genesis: %v", err)
	}
	if root == e.genesisRoot {
		t.Fatal("expected a state change")
	}
	if err := e.triedb.Commit(root, false); err != nil {
		t.Fatalf("triedb commit: %v", err)
	}
}

// TestEmptyBlockFastPathAcrossRestart replays consecutive empty blocks
// (header.Root == parent root) and then reopens the executor, the exact
// scenario behind the reference's "committable proposal not found" wall
// (deforestationdb LOG.md, block 33405). The fast path must skip Firewood
// entirely and the restart must reconcile onto the empty chain tip.
func TestEmptyBlockFastPathAcrossRestart(t *testing.T) {
	fetch.RegisterExtras()
	dir := t.TempDir()

	store, err := state.Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	src := fakeSource{}
	e, err := New(Config{DataDir: dir, Blocks: src, Store: store})
	if err != nil {
		t.Fatal(err)
	}

	g, err := loadCChainGenesis(e.snowCtx.NetworkID, e.snowCtx)
	if err != nil {
		t.Fatal(err)
	}
	genesisBlk := g.ToBlock()

	// Craft three empty blocks chained off genesis, all claiming the
	// genesis state root.
	parentHash := genesisBlk.Hash()
	for n := uint64(1); n <= 3; n++ {
		h := &types.Header{
			ParentHash: parentHash,
			Number:     new(big.Int).SetUint64(n),
			Root:       e.genesisRoot,
			GasLimit:   8_000_000,
			Time:       n,
			Difficulty: big.NewInt(1),
		}
		blk := types.NewBlockWithHeader(h)
		raw, err := rlp.EncodeToBytes(blk)
		if err != nil {
			t.Fatal(err)
		}
		src[n] = raw
		parentHash = blk.Hash()
	}

	for n := uint64(1); n <= 2; n++ {
		if err := e.executeRaw(n, src[n]); err != nil {
			t.Fatalf("block %d: %v", n, err)
		}
	}
	if e.headNum != 2 || e.headRoot != e.genesisRoot {
		t.Fatalf("head=%d root=%x after two empty blocks", e.headNum, e.headRoot)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	// Restart: reconcile must land on the empty-chain tip (block 2) and
	// execution must continue through another empty block.
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
	if e.headNum != 2 || e.headRoot != e.genesisRoot {
		t.Fatalf("resume head=%d root=%x, want 2/genesis", e.headNum, e.headRoot)
	}
	if err := e.executeRaw(3, src[3]); err != nil {
		t.Fatalf("block 3 after resume: %v", err)
	}
	if e.headNum != 3 {
		t.Fatalf("head=%d, want 3", e.headNum)
	}
}

// TestHeliconBoundaryHalts: the first block at or past Fuji's HeliconTime
// must stop replay with errHelicon instead of being executed by coreth
// (which would fail root verification with a misleading message), and
// mainnet, which has no scheduled HeliconTime, must never trip the guard.
func TestHeliconBoundaryHalts(t *testing.T) {
	fetch.RegisterExtras()
	dir := t.TempDir()

	store, err := state.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	e, err := New(Config{DataDir: dir, Blocks: fakeSource{}, Store: store}) // NetworkID 0 = Fuji
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	g, err := loadCChainGenesis(e.snowCtx.NetworkID, e.snowCtx)
	if err != nil {
		t.Fatal(err)
	}
	helicon := uint64(upgrade.GetConfig(avaconstants.FujiID).HeliconTime.Unix())
	for _, tc := range []struct {
		time uint64
		want bool
	}{
		{helicon - heliconTransitionLead - 1, false}, // last block coreth surely owns
		{helicon - heliconTransitionLead, true},      // transitionvm switch point
		{helicon, true},
	} {
		if got := postHeliconTransition(e.chainCfg, tc.time); got != tc.want {
			t.Fatalf("postHeliconTransition(fuji, %d) = %v, want %v", tc.time, got, tc.want)
		}
	}

	blk := types.NewBlockWithHeader(&types.Header{
		ParentHash: g.ToBlock().Hash(),
		Number:     big.NewInt(1),
		Root:       e.genesisRoot,
		GasLimit:   8_000_000,
		Time:       helicon,
		Difficulty: big.NewInt(1),
	})
	raw, err := rlp.EncodeToBytes(blk)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.executeRaw(1, raw); !errors.Is(err, errHelicon) {
		t.Fatalf("executeRaw at HeliconTime: got %v, want errHelicon", err)
	}
	if e.headNum != 0 {
		t.Fatalf("head advanced to %d across the boundary", e.headNum)
	}

	mainCtx, err := snowContextFor(avaconstants.MainnetID)
	if err != nil {
		t.Fatal(err)
	}
	mg, err := loadCChainGenesis(avaconstants.MainnetID, mainCtx)
	if err != nil {
		t.Fatal(err)
	}
	if postHeliconTransition(mg.Config, helicon) {
		t.Fatal("mainnet must have no scheduled Helicon activation")
	}
}

package exec

import (
	"math/big"
	"strings"
	"testing"

	"github.com/ava-labs/avalanchego/upgrade"
	avaconstants "github.com/ava-labs/avalanchego/utils/constants"
	"github.com/ava-labs/libevm/common"
	ethstate "github.com/ava-labs/libevm/core/state"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/libevm/stateconf"
	"github.com/ava-labs/libevm/rlp"

	"github.com/containerman17/epochdb/chain"
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
	fetch.RegisterExtras(chain.Coreth)
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
	fetch.RegisterExtras(chain.Coreth)
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

	g, err := loadCorethGenesis(mustCChain(t, e.snowCtx.NetworkID), e.snowCtx)
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

// TestHeliconBoundaryRule pins the exact transitionvm split: coreth
// executes up to AND INCLUDING the transition block (the first block at or
// past HeliconTime-10s), so a block is SAE i.f.f. its PARENT's timestamp is
// at or past that point. The settlement markers must agree with the
// timestamp rule, and mainnet, with no scheduled Helicon, must never take
// the SAE path.
func TestHeliconBoundaryRule(t *testing.T) {
	fetch.RegisterExtras(chain.Coreth)
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

	helicon := uint64(upgrade.GetConfig(avaconstants.FujiID).HeliconTime.Unix())
	transition := helicon - heliconTransitionLead
	if got, ok := transitionTimestamp(e.chainCfg); !ok || got != transition {
		t.Fatalf("transitionTimestamp(fuji) = %d, %v; want %d, true", got, ok, transition)
	}

	for _, tc := range []struct {
		name       string
		parentTime uint64
		markers    bool
		wantSAE    bool
		wantErr    bool
	}{
		{"parent below the switch", transition - 1, false, false, false},
		{"transition block itself is coreth's", transition - 1, true, false, true},
		{"first block after the transition block", transition, true, true, false},
		{"post-Helicon", helicon, true, true, false},
		{"SAE height without markers", helicon, false, false, true},
	} {
		blk := saeTestBlock(t, e, 2, common.Hash{}, helicon, tc.markers)
		got, err := e.saeExecuted(blk, tc.parentTime)
		if (err != nil) != tc.wantErr {
			t.Fatalf("%s: saeExecuted err = %v, wantErr %v", tc.name, err, tc.wantErr)
		}
		if err == nil && got != tc.wantSAE {
			t.Fatalf("%s: saeExecuted = %v, want %v", tc.name, got, tc.wantSAE)
		}
	}

	// The real Fuji boundary, read off the network 2026-07-29 (block
	// timestamps and settledHeight presence via the public RPC):
	//
	//	57,403,045  t=1785250767  no markers
	//	57,403,046  t=1785250790  no markers   <- transition block, t == HeliconTime-10 exactly
	//	57,403,047  t=1785250806  markers      <- first SAE block, settles 57,403,046
	//
	// so the split lands where the parent-timestamp rule puts it.
	for _, tc := range []struct {
		num        uint64
		parentTime uint64
		markers    bool
		wantSAE    bool
	}{
		{57_403_046, 1_785_250_767, false, false},
		{57_403_047, 1_785_250_790, true, true},
	} {
		blk := saeTestBlock(t, e, tc.num, common.Hash{}, tc.parentTime+16, tc.markers)
		got, err := e.saeExecuted(blk, tc.parentTime)
		if err != nil || got != tc.wantSAE {
			t.Fatalf("fuji block %d: saeExecuted = %v, %v; want %v, nil", tc.num, got, err, tc.wantSAE)
		}
	}

	mainCtx, err := snowContextFor(mustCChain(t, avaconstants.MainnetID))
	if err != nil {
		t.Fatal(err)
	}
	mg, err := loadCorethGenesis(mustCChain(t, avaconstants.MainnetID), mainCtx)
	if err != nil {
		t.Fatal(err)
	}
	// Mainnet is "unscheduled" as a year-9999 activation time rather than a
	// nil one, so the guard is inert instead of absent.
	me := &Executor{vm: corethVM{}, chainCfg: mg.Config, ring: e.ring}
	blk := saeTestBlock(t, e, 2, common.Hash{}, helicon, false)
	if sae, err := me.saeExecuted(blk, helicon); err != nil || sae {
		t.Fatalf("mainnet at Fuji's Helicon time: sae=%v err=%v, want false/nil", sae, err)
	}
}

// TestCommitEveryWalkBackGuard: raw retirement (seal's, and the pruning
// node's fold) deletes whole 100k-block buckets behind the sealed/folded end,
// while a crash walk-back re-reads containers 4096 + 64*CommitEvery blocks
// back. A CommitEvery large enough to reach past one bucket turns a kill -9
// after a retirement into an unstartable node ("container missing from
// staging"), so New refuses it up front instead of leaving the safety margin
// to a numeric coincidence.
func TestCommitEveryWalkBackGuard(t *testing.T) {
	fetch.RegisterExtras(chain.Coreth)
	newAt := func(commitEvery int) error {
		dir := t.TempDir()
		store, err := state.Open(dir)
		if err != nil {
			t.Fatal(err)
		}
		defer store.Close()
		e, err := New(Config{DataDir: dir, Blocks: fakeSource{}, Store: store, CommitEvery: commitEvery})
		if err == nil {
			e.Close()
		}
		return err
	}
	if err := newAt(1500); err == nil || !strings.Contains(err.Error(), "commit-every") {
		t.Fatalf("commit-every 1500 accepted: %v", err)
	}
	// 1498 is the documented maximum: 4096 + 64*1498 = 99,968 < 100,000.
	if budget := walkBackBudgetFor(1498); budget >= state.BucketBlocks {
		t.Fatalf("1498 budget is %d, expected under one bucket (%d)", budget, state.BucketBlocks)
	}
	if budget := walkBackBudgetFor(1499); budget < state.BucketBlocks {
		t.Fatalf("1499 budget is %d, expected at or past one bucket (%d)", budget, state.BucketBlocks)
	}
	if err := newAt(1000); err != nil {
		t.Fatalf("commit-every 1000 (the production default) refused: %v", err)
	}
}

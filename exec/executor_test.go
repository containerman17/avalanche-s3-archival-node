package exec

import (
	"math/big"
	"testing"

	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/rlp"

	"github.com/containerman17/epochdb/fetch"
	"github.com/containerman17/epochdb/state"
)

type fakeSource map[uint64][]byte

func (f fakeSource) GetByHeight(n uint64) ([]byte, bool, error) {
	raw, ok := f[n]
	return raw, ok, nil
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

	g, err := loadFujiCChainGenesis(e.snowCtx)
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

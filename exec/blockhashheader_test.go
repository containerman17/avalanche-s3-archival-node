package exec

import (
	"math/big"
	"testing"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/rlp"

	"github.com/containerman17/epochdb/dist"
	"github.com/containerman17/epochdb/state"
)

// TestGetHeaderResolvesBelowSealedEnd is the BLOCKHASH regression: on a node
// whose raw header buckets were retired by seal, chainContext.GetHeader read
// the raw headers family alone, got "absent", and returned nil, which libevm
// reads as "no such header" and the BLOCKHASH opcode turns into the ZERO HASH.
// Wrong logs, written into logsbf, indistinguishable from real ones forever.
func TestGetHeaderResolvesBelowSealedEnd(t *testing.T) {
	dir := t.TempDir()
	cas, err := dist.Local(dir)
	if err != nil {
		t.Fatal(err)
	}
	in := &state.EpochInput{Start: 1, TxHashes: map[uint64][][32]byte{}}
	want := map[uint64]common.Hash{}
	for n := uint64(1); n <= 4; n++ {
		hdr := &types.Header{
			Number:     new(big.Int).SetUint64(n),
			Difficulty: big.NewInt(1),
			GasLimit:   8_000_000,
		}
		hdrRLP, err := rlp.EncodeToBytes(hdr)
		if err != nil {
			t.Fatal(err)
		}
		container, err := rlp.EncodeToBytes(types.NewBlockWithHeader(hdr))
		if err != nil {
			t.Fatal(err)
		}
		in.Containers = append(in.Containers, container)
		in.Headers = append(in.Headers, hdrRLP)
		want[n] = hdr.Hash()
	}
	if _, err := state.BuildEpoch(cas, in); err != nil {
		t.Fatal(err)
	}

	store, err := state.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	// The situation a sealed node is in: nothing in the raw headers family.
	if _, ok, err := store.RawHeaderRLP(3); err != nil || ok {
		t.Fatalf("raw header 3 present (ok=%v err=%v): the fixture is not the retired-bucket case", ok, err)
	}

	c := chainContext{store: store}
	h := c.GetHeader(common.Hash{}, 3)
	if h == nil {
		t.Fatal("GetHeader(3) is nil below the sealed end: BLOCKHASH would return the zero hash")
	}
	if got := h.Hash(); got != want[3] {
		t.Fatalf("GetHeader(3) hash %x, want %x", got, want[3])
	}
	if h.Number.Uint64() != 3 {
		t.Fatalf("GetHeader(3) is block %d", h.Number.Uint64())
	}
	// A height nothing has is still an honest nil (a real miss).
	if h := c.GetHeader(common.Hash{}, 99); h != nil {
		t.Fatalf("GetHeader(99) resolved to block %d", h.Number.Uint64())
	}
}

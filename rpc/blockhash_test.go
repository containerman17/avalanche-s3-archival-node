package rpc

import (
	"math/big"
	"testing"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/common/hexutil"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/rlp"

	"github.com/containerman17/epochdb/dist"
	"github.com/containerman17/epochdb/exec"
	"github.com/containerman17/epochdb/state"
)

// Block-by-hash without a block-hash map (DESIGN.md ruling 2, 2026-07-31).
// The epoch's tx index carries every block hash in the same fp48 space, so
// these tests are about the three things that can go wrong: the fold not
// answering at all, an fp48 collision between a block hash and a tx hash
// being believed, and the unsealed tail falling off the end.

const (
	bhSealedEnd = 8  // blocks 1..8 live in one sealed epoch
	bhTailEnd   = 12 // blocks 9..12 exist only in the tail store
)

// bhBlock returns block n's container (plain eth block RLP, which
// exec.ParseEthBlock accepts) and its header.
func bhBlock(n uint64) ([]byte, *types.Header) {
	h := &types.Header{
		Number:     new(big.Int).SetUint64(n),
		Difficulty: big.NewInt(1),
		GasLimit:   8_000_000,
	}
	raw, err := rlp.EncodeToBytes(types.NewBlockWithHeader(h))
	if err != nil {
		panic(err)
	}
	return raw, h
}

// mapBlocks is the unsealed tail's container source.
type mapBlocks map[uint64][]byte

func (m mapBlocks) GetByHeight(n uint64) ([]byte, bool, error) {
	raw, ok := m[n]
	return raw, ok, nil
}

// countingCandidates counts how many candidate heights the walk offered, so a
// test can prove a WRONG candidate was actually visited and rejected rather
// than never produced.
type countingCandidates struct {
	inner  TxCandidateSource
	visits *int
}

func (c countingCandidates) WalkCandidates(h common.Hash, fn func(uint64) (bool, error)) error {
	return c.inner.WalkCandidates(h, func(n uint64) (bool, error) {
		*c.visits++
		return fn(n)
	})
}

type bhEnv struct {
	srv    *Server
	hist   *state.History // the ws test drives the serving head through this
	hashes map[uint64]common.Hash
	poison common.Hash // a TX hash sharing block bhSealedEnd-1's fp48
	visits int
}

// newBlockHashEnv seals blocks 1..bhSealedEnd into a real epoch, leaves
// bhSealedEnd+1..bhTailEnd in the tail store, and wires a server with NO
// block-hash map: the only sources are the epoch's folded index and the
// bounded live-tail ring.
func newBlockHashEnv(t *testing.T) *bhEnv {
	t.Helper()
	dir := t.TempDir()
	gm, err := exec.ChainGenesis(mustCChain(t, 1))
	if err != nil {
		t.Fatal(err)
	}
	cas, err := dist.Local(dir)
	if err != nil {
		t.Fatal(err)
	}

	env := &bhEnv{hashes: map[uint64]common.Hash{}}
	in := &state.EpochInput{Start: 1, TxHashes: map[uint64][][32]byte{}}
	for n := uint64(1); n <= bhSealedEnd; n++ {
		container, hdr := bhBlock(n)
		hdrRLP, err := rlp.EncodeToBytes(hdr)
		if err != nil {
			t.Fatal(err)
		}
		in.Containers = append(in.Containers, container)
		in.Headers = append(in.Headers, hdrRLP)
		env.hashes[n] = hdr.Hash()
	}
	// THE COLLISION UNDER TEST: a tx in the LAST sealed block whose hash
	// shares the top 48 bits with block bhSealedEnd-1's own hash. Candidates
	// come back newest first, so the walk hits this wrong block before the
	// right one and only header verification can tell them apart.
	env.poison = env.hashes[bhSealedEnd-1]
	env.poison[31] ^= 0xff
	in.TxHashes[bhSealedEnd] = [][32]byte{env.poison}
	in.TxCount = 1
	if _, err := state.BuildEpoch(cas, in); err != nil {
		t.Fatal(err)
	}

	store, err := state.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	tail := mapBlocks{}
	for n := uint64(bhSealedEnd + 1); n <= bhTailEnd; n++ {
		container, hdr := bhBlock(n)
		hdrRLP, err := rlp.EncodeToBytes(hdr)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.AppendHeader(n, hdrRLP); err != nil {
			t.Fatal(err)
		}
		tail[n] = container
		env.hashes[n] = hdr.Hash()
	}
	if err := store.FlushAndSetExecHead(bhTailEnd); err != nil {
		t.Fatal(err)
	}

	hist, err := state.OpenHistory(dir, store, gm.Alloc)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(hist.Close)
	hist.SetHead(bhTailEnd)
	env.hist = hist

	env.srv = NewServer(hist, HistoryChainContext(hist), gm.Config)
	env.srv.EnableTxAPIs(
		countingCandidates{inner: state.CombinedTxIndex{Epochs: hist.Epochs()}, visits: &env.visits},
		SealedBlocks{Epochs: hist.Epochs(), Blocks: tail},
		exec.ParseEthBlock,
	)
	// What serve's OnBlock does, and the ONLY block-hash state kept: the
	// heights the cook has not indexed yet.
	for n := uint64(bhSealedEnd + 1); n <= bhTailEnd; n++ {
		env.srv.AddBlockHash(env.hashes[n], n)
	}
	return env
}

func (e *bhEnv) numberByHash(t *testing.T, h common.Hash) (uint64, bool) {
	t.Helper()
	res, rerr := e.srv.dispatch(&rpcRequest{Method: "eth_getBlockByHash", Params: mustParams(t, h, false)})
	if rerr != nil {
		t.Fatalf("eth_getBlockByHash(%x): %v", h[:6], rerr)
	}
	if res == nil {
		return 0, false
	}
	fields, ok := res.(map[string]any)
	if !ok {
		t.Fatalf("unexpected result type %T", res)
	}
	return (*big.Int)(fields["number"].(*hexutil.Big)).Uint64(), true
}

// TestGetBlockByHashThroughFoldedIndex: every sealed block resolves by hash
// with no block-hash map in the process at all.
func TestGetBlockByHashThroughFoldedIndex(t *testing.T) {
	env := newBlockHashEnv(t)
	for n := uint64(1); n <= bhSealedEnd; n++ {
		got, ok := env.numberByHash(t, env.hashes[n])
		if !ok || got != n {
			t.Fatalf("block %d by hash: got %d (found=%v)", n, got, ok)
		}
	}
	// An unknown hash is still a clean null, not a fabricated block.
	unknown := common.HexToHash("0xfeedfacecafebeef0123456789abcdef0123456789abcdef0123456789abcdef")
	if got, ok := env.numberByHash(t, unknown); ok {
		t.Fatalf("unknown hash resolved to block %d", got)
	}
}

// TestGetBlockByHashRejectsWrongCandidate: an fp48 collision between a block
// hash and a tx hash must be caught by the header compare. The wrong
// candidate is offered FIRST, so a lookup that trusted the fingerprint would
// return the wrong block.
func TestGetBlockByHashRejectsWrongCandidate(t *testing.T) {
	env := newBlockHashEnv(t)
	want := uint64(bhSealedEnd - 1)

	env.visits = 0
	got, ok := env.numberByHash(t, env.hashes[want])
	if !ok || got != want {
		t.Fatalf("colliding block %d by hash: got %d (found=%v)", want, got, ok)
	}
	if env.visits < 2 {
		t.Fatalf("only %d candidates walked: the colliding wrong candidate was never offered, so the test proves nothing", env.visits)
	}

	// The other direction: the TX hash that collides is not a block hash, so
	// block-by-hash must answer null even though both its candidates verify
	// as real blocks for some other purpose.
	if got, ok := env.numberByHash(t, env.poison); ok {
		t.Fatalf("a tx hash resolved as block %d", got)
	}
}

// TestGetBlockByHashAboveSealedEnd: a block the cook has not indexed yet is
// answered by the bounded live-tail ring, which is the whole reason serve
// keeps one.
func TestGetBlockByHashAboveSealedEnd(t *testing.T) {
	env := newBlockHashEnv(t)
	for n := uint64(bhSealedEnd + 1); n <= bhTailEnd; n++ {
		got, ok := env.numberByHash(t, env.hashes[n])
		if !ok || got != n {
			t.Fatalf("tail block %d by hash: got %d (found=%v)", n, got, ok)
		}
	}
}

// TestRecentHashesBounded: the ring never grows past its cap, and the newest
// entries survive.
func TestRecentHashesBounded(t *testing.T) {
	const cap = 8
	r := newRecentHashes(cap)
	for n := uint64(1); n <= 100; n++ {
		var h common.Hash
		h[0], h[1] = byte(n), byte(n>>8)
		r.add(h, n)
	}
	if len(r.m) != cap {
		t.Fatalf("ring holds %d entries, cap is %d", len(r.m), cap)
	}
	for n := uint64(93); n <= 100; n++ {
		var h common.Hash
		h[0], h[1] = byte(n), byte(n>>8)
		if got, ok := r.get(h); !ok || got != n {
			t.Fatalf("block %d evicted while inside the cap (got %d, %v)", n, got, ok)
		}
	}
	var old common.Hash
	old[0] = 1
	if _, ok := r.get(old); ok {
		t.Fatal("the oldest entry was not evicted")
	}
}

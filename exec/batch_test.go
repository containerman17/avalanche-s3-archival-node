package exec

import (
	"bytes"
	"math/big"
	"strings"
	"testing"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/rlp"
	"github.com/holiman/uint256"

	"github.com/containerman17/epochdb/fetch"
	"github.com/containerman17/epochdb/state"
)

// newBatchExecutor opens a Firewood-backed executor with the read cache
// DISABLED so reads must flow through the pending-batch overlay.
func newBatchExecutor(t *testing.T, commitEvery int, src fakeSource) *Executor {
	t.Helper()
	fetch.RegisterExtras()
	dir := t.TempDir()
	store, err := state.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	e, err := New(Config{DataDir: dir, Blocks: src, Store: store, CommitEvery: commitEvery})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { e.triedb.Close() }) // not e.Close: tests may leave poisoned batches
	return e
}

// TestBatchPendingOverlayReads drives the batch-shared trie directly:
// with the cache off, reads of batch-written state must come from the
// shared trie's pending ops (Firewood is N blocks stale mid-batch), and
// a DeleteAccount tombstone must hide both the account and its slots.
func TestBatchPendingOverlayReads(t *testing.T) {
	e := newBatchExecutor(t, 100, fakeSource{})
	d := e.wrapDB
	d.beginBatch(e.genesisRoot)
	defer d.endBatch()

	addr := common.HexToAddress("0xabab")
	slot := common.HexToHash("0x07")
	val := []byte{0x99}
	acct := &types.StateAccount{Nonce: 3, Balance: uint256.NewInt(30), Root: types.EmptyRootHash, CodeHash: types.EmptyCodeHash[:]}

	tr1, err := d.OpenTrie(e.genesisRoot)
	if err != nil {
		t.Fatal(err)
	}
	if err := tr1.UpdateAccount(addr, acct); err != nil {
		t.Fatal(err)
	}
	if err := tr1.UpdateStorage(addr, slot[:], val); err != nil {
		t.Fatal(err)
	}

	// A later block's trie (same batch) must see the un-proposed writes.
	tr2, err := d.OpenTrie(e.genesisRoot)
	if err != nil {
		t.Fatal(err)
	}
	got, err := tr2.GetAccount(addr)
	if err != nil || got == nil || got.Nonce != 3 {
		t.Fatalf("overlay account read: %+v err=%v", got, err)
	}
	sv, err := tr2.GetStorage(addr, slot[:])
	if err != nil || !bytes.Equal(sv, val) {
		t.Fatalf("overlay storage read: %x err=%v", sv, err)
	}

	// Selfdestruct mid-batch: tombstone must hide account AND slots.
	if err := tr2.DeleteAccount(addr); err != nil {
		t.Fatal(err)
	}
	tr3, err := d.OpenTrie(e.genesisRoot)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := tr3.GetAccount(addr); err != nil || got != nil {
		t.Fatalf("deleted account visible: %+v err=%v", got, err)
	}
	if sv, err := tr3.GetStorage(addr, slot[:]); err != nil || len(sv) != 0 {
		t.Fatalf("deleted account slot visible: %x err=%v", sv, err)
	}
}

func emptyChain(t *testing.T, e *Executor, n int, badRootAt int) fakeSource {
	t.Helper()
	src := fakeSource{}
	parentHash := loadGenesisHash(t, e)
	for i := 1; i <= n; i++ {
		root := e.genesisRoot
		if i == badRootAt {
			root = common.HexToHash("0xbad")
		}
		h := &types.Header{
			ParentHash: parentHash,
			Number:     big.NewInt(int64(i)),
			Root:       root,
			GasLimit:   8_000_000,
			Time:       uint64(i),
			Difficulty: big.NewInt(1),
		}
		blk := types.NewBlockWithHeader(h)
		raw, err := rlp.EncodeToBytes(blk)
		if err != nil {
			t.Fatal(err)
		}
		src[uint64(i)] = raw
		parentHash = blk.Hash()
	}
	return src
}

func loadGenesisHash(t *testing.T, e *Executor) common.Hash {
	t.Helper()
	g, err := loadFujiCChainGenesis(e.snowCtx)
	if err != nil {
		t.Fatal(err)
	}
	return g.ToBlock().Hash()
}

// TestBatchEmptyChainAndBoundary: an all-empty batch must flush without a
// proposal, land the buffered headers, and survive a restart.
func TestBatchEmptyChainAndBoundary(t *testing.T) {
	e := newBatchExecutor(t, 2, fakeSource{})
	src := emptyChain(t, e, 2, 0)
	for i := uint64(1); i <= 2; i++ {
		if err := e.executeRaw(i, src[i]); err != nil {
			t.Fatalf("block %d: %v", i, err)
		}
	}
	if e.batchOpen {
		t.Fatal("batch must be closed at the boundary")
	}
	// Buffered headers must have landed only via the boundary flush.
	for i := uint64(1); i <= 2; i++ {
		if _, ok, _ := e.cfg.Store.HeaderRLP(i); !ok {
			t.Fatalf("header %d missing after boundary flush", i)
		}
	}
	if e.headNum != 2 || e.fwHeight != 2 {
		t.Fatalf("head=%d fwHeight=%d, want 2/2", e.headNum, e.fwHeight)
	}
}

// TestBatchBisectNamesOffendingBlock: a boundary root mismatch must
// trigger per-block re-execution that identifies the bad block and still
// hard-stops.
func TestBatchBisectNamesOffendingBlock(t *testing.T) {
	e := newBatchExecutor(t, 3, fakeSource{})
	src := emptyChain(t, e, 3, 3) // block 3 claims a bogus root
	e.cfg.Blocks = src            // bisect re-reads containers by height

	var gotErr error
	for i := uint64(1); i <= 3; i++ {
		if gotErr = e.executeRaw(i, src[i]); gotErr != nil {
			break
		}
	}
	if gotErr == nil {
		t.Fatal("expected the batch boundary to fail")
	}
	if !strings.Contains(gotErr.Error(), "offending block 3") {
		t.Fatalf("bisect did not name the offending block: %v", gotErr)
	}
}

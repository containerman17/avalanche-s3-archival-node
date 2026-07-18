package exec

import (
	"bytes"
	"fmt"
	"github.com/holiman/uint256"
	"testing"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/types"

	"github.com/containerman17/epochdb/fetch"
	"github.com/containerman17/epochdb/state"
)

// cacheHarness opens a real Firewood-backed executor with the read cache
// enabled and returns an open account trie at the genesis root.
func cacheHarness(t *testing.T, cacheBytes uint64) *wrappingTrie {
	t.Helper()
	fetch.RegisterExtras()
	dir := t.TempDir()
	store, err := state.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	e, err := New(Config{DataDir: dir, Blocks: fakeSource{}, Store: store, StateCacheBytes: cacheBytes})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { e.Close() })
	tr, err := e.wrapDB.OpenTrie(e.genesisRoot)
	if err != nil {
		t.Fatal(err)
	}
	return tr.(*wrappingTrie)
}

func TestCacheWriteThroughVisibility(t *testing.T) {
	tr := cacheHarness(t, 1<<20)
	d := tr.db
	addr := common.HexToAddress("0xaaaa")
	slot := common.HexToHash("0x01")
	val := []byte{0xde, 0xad}

	acct := &types.StateAccount{Nonce: 7, Balance: uint256.NewInt(42), Root: types.EmptyRootHash, CodeHash: types.EmptyCodeHash[:]}
	if err := tr.UpdateAccount(addr, acct); err != nil {
		t.Fatal(err)
	}
	if err := tr.UpdateStorage(addr, slot[:], val); err != nil {
		t.Fatal(err)
	}

	got, err := tr.GetAccount(addr)
	if err != nil || got == nil || got.Nonce != 7 || got.Balance.Cmp(uint256.NewInt(42)) != 0 {
		t.Fatalf("account after write-through: %+v err=%v", got, err)
	}
	sv, err := tr.GetStorage(addr, slot[:])
	if err != nil || !bytes.Equal(sv, val) {
		t.Fatalf("storage after write-through: %x err=%v", sv, err)
	}
	if hits, _ := d.CacheStats(); hits != 2 {
		t.Fatalf("expected both reads served from cache, hits=%d", hits)
	}
	// Mutating the returned account must not poison the cache.
	got.Nonce = 999
	got2, _ := tr.GetAccount(addr)
	if got2.Nonce != 7 {
		t.Fatal("cache entry aliased with returned account")
	}
}

func TestCacheDeleteAccountDropsStorage(t *testing.T) {
	tr := cacheHarness(t, 1<<20)
	d := tr.db
	addr := common.HexToAddress("0xbbbb")
	slot := common.HexToHash("0x02")

	acct := &types.StateAccount{Nonce: 1, Balance: uint256.NewInt(1), Root: types.EmptyRootHash, CodeHash: types.EmptyCodeHash[:]}
	if err := tr.UpdateAccount(addr, acct); err != nil {
		t.Fatal(err)
	}
	if err := tr.UpdateStorage(addr, slot[:], []byte{0x11}); err != nil {
		t.Fatal(err)
	}
	sizeBefore := d.cacheSize

	// Selfdestruct: entry (account + all slots) must vanish atomically.
	if err := tr.DeleteAccount(addr); err != nil {
		t.Fatal(err)
	}
	if _, ok := d.cache[addr]; ok {
		t.Fatal("cache entry must be dropped on DeleteAccount")
	}
	if d.cacheSize >= sizeBefore {
		t.Fatalf("cacheSize not reclaimed: %d >= %d", d.cacheSize, sizeBefore)
	}
	// Post-delete reads fall through to Firewood, which sees the delete.
	if sv, err := tr.GetStorage(addr, slot[:]); err != nil || len(sv) != 0 {
		t.Fatalf("storage after selfdestruct: %x err=%v", sv, err)
	}

	// Recreation repopulates via write-through.
	if err := tr.UpdateAccount(addr, &types.StateAccount{Nonce: 2, Balance: uint256.NewInt(2), Root: types.EmptyRootHash, CodeHash: types.EmptyCodeHash[:]}); err != nil {
		t.Fatal(err)
	}
	if err := tr.UpdateStorage(addr, slot[:], []byte{0x22}); err != nil {
		t.Fatal(err)
	}
	got, _ := tr.GetAccount(addr)
	if got == nil || got.Nonce != 2 {
		t.Fatalf("recreated account: %+v", got)
	}
	sv, _ := tr.GetStorage(addr, slot[:])
	if !bytes.Equal(sv, []byte{0x22}) {
		t.Fatalf("recreated slot: %x", sv)
	}
}

func TestCacheEvictionFallsThrough(t *testing.T) {
	tr := cacheHarness(t, 1024) // tiny cap forces eviction
	d := tr.db
	target := common.HexToAddress("0xcccc")
	if err := tr.UpdateAccount(target, &types.StateAccount{Nonce: 5, Balance: uint256.NewInt(5), Root: types.EmptyRootHash, CodeHash: types.EmptyCodeHash[:]}); err != nil {
		t.Fatal(err)
	}
	// Blow past the cap so the target account gets evicted eventually.
	for i := 0; i < 64; i++ {
		a := common.BytesToAddress([]byte(fmt.Sprintf("filler-%02d", i)))
		if err := tr.UpdateAccount(a, &types.StateAccount{Nonce: uint64(i), Balance: uint256.NewInt(1), Root: types.EmptyRootHash, CodeHash: types.EmptyCodeHash[:]}); err != nil {
			t.Fatal(err)
		}
	}
	if d.cacheSize > 1024+entryCost {
		// entry() may briefly overshoot by the entry being inserted.
		t.Fatalf("eviction did not bound the cache: size=%d", d.cacheSize)
	}
	// Whether or not target survived eviction, the read must return the
	// authoritative value (from Firewood's dirtyKeys on miss).
	got, err := tr.GetAccount(target)
	if err != nil || got == nil || got.Nonce != 5 {
		t.Fatalf("read after eviction: %+v err=%v", got, err)
	}
}

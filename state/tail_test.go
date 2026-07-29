package state

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/types"
)

// TestTailOverlayDeleteSemantics pins the one semantic the overlay cannot get
// from the descent: an account destructed in the uncooked tail reads as gone,
// and so does its storage, even though the cooked history still holds both.
func TestTailOverlayDeleteSemantics(t *testing.T) {
	dir := t.TempDir()
	st, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	addr := common.HexToAddress("0xaaaa000000000000000000000000000000000001")
	slot := common.HexToHash("0x01")
	slot2 := common.HexToHash("0x02")

	// Cooked history: account with one slot.
	app := func(block uint64, frame []byte) {
		if err := st.AppendWrites(block, frame); err != nil {
			t.Fatal(err)
		}
	}
	app(1, frAccount(nil, addr, accRLP(t, 1, 100)))
	app(2, frStorage(nil, addr, slot, []byte{0x2a}))
	if err := st.FlushAndSetExecHead(2); err != nil {
		t.Fatal(err)
	}
	if err := CookIndex(dir); err != nil {
		t.Fatal(err)
	}
	hist, err := OpenHistory(dir, st, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer hist.Close()
	if _, _, _, err := hist.EnableTail(2); err != nil {
		t.Fatal(err)
	}
	hist.SetHead(2)

	// Baseline: cooked values answer at latest.
	if acc, err := hist.AccountLatest(addr); err != nil || acc == nil || acc.Nonce != 1 {
		t.Fatalf("cooked account: %+v err=%v", acc, err)
	}
	if v, err := hist.StorageLatest(addr, slot[:]); err != nil || len(v) != 1 || v[0] != 0x2a {
		t.Fatalf("cooked storage: %x err=%v", v, err)
	}

	// Uncooked SELFDESTRUCT: account gone, storage gone, without a cook.
	app(3, frAccount(nil, addr, nil))
	if acc, err := hist.AccountLatest(addr); err != nil || acc != nil {
		t.Fatalf("destructed account still reads %+v err=%v", acc, err)
	}
	if v, err := hist.StorageLatest(addr, slot[:]); err != nil || v != nil {
		t.Fatalf("destructed storage still reads %x err=%v", v, err)
	}

	// Recreated in a later uncooked block: the account is back, the old slot
	// stays dead, a slot written after the destruct answers.
	app(4, frStorage(frAccount(nil, addr, accRLP(t, 0, 7)), addr, slot2, []byte{0x7f}))
	if acc, err := hist.AccountLatest(addr); err != nil || acc == nil || acc.Balance.Uint64() != 7 {
		t.Fatalf("recreated account: %+v err=%v", acc, err)
	}
	if v, err := hist.StorageLatest(addr, slot[:]); err != nil || v != nil {
		t.Fatalf("pre-destruct slot resurrected: %x err=%v", v, err)
	}
	if v, err := hist.StorageLatest(addr, slot2[:]); err != nil || len(v) != 1 || v[0] != 0x7f {
		t.Fatalf("post-destruct slot: %x err=%v", v, err)
	}

	// And the descent takes over across a cook without changing any answer.
	if err := st.FlushAndSetExecHead(4); err != nil {
		t.Fatal(err)
	}
	if err := CookIndex(dir); err != nil {
		t.Fatal(err)
	}
	if err := hist.Refresh(); err != nil {
		t.Fatal(err)
	}
	if entries, _ := hist.PruneTail(); entries != 0 {
		t.Fatalf("prune left %d entries with everything cooked", entries)
	}
	if acc, err := hist.AccountLatest(addr); err != nil || acc == nil || acc.Balance.Uint64() != 7 {
		t.Fatalf("after cook, account: %+v err=%v", acc, err)
	}
	if v, err := hist.StorageLatest(addr, slot[:]); err != nil || v != nil {
		t.Fatalf("after cook, dead slot: %x err=%v", v, err)
	}
	if v, err := hist.StorageLatest(addr, slot2[:]); err != nil || len(v) != 1 || v[0] != 0x7f {
		t.Fatalf("after cook, live slot: %x err=%v", v, err)
	}
}

// TestTailOverlayConcurrentReadCookPrune is THE race gate for the one read
// path: an executor goroutine appends writes, a cook goroutine cooks +
// refreshes + prunes, and readers hammer latest reads throughout. The
// invariant under test is the one the prune could break: a value written at
// block b must be readable at latest from the moment it is written, whether it
// currently lives in the overlay or in the freshly cooked descent, with no
// window in between. Run with -race. Teeth checked: pruning past the
// published watermark (or dropping the overlay lookup) fails it immediately.
func TestTailOverlayConcurrentReadCookPrune(t *testing.T) {
	dir := t.TempDir()
	st, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	const (
		nAddrs  = 64
		nBlocks = 1500
	)
	addrs := make([]common.Address, nAddrs)
	for i := range addrs {
		addrs[i] = common.HexToAddress(fmt.Sprintf("0x%040x", i+1))
	}
	slot := common.HexToHash("0x01")

	// Block b writes addr[b%nAddrs] with nonce b, so a reader can always name
	// the newest value it must be allowed to see.
	frameFor := func(b uint64) []byte {
		a := addrs[b%nAddrs]
		f := frAccount(nil, a, accRLP(t, b, int64(b)))
		return frStorage(f, a, slot, []byte{byte(b), byte(b >> 8)})
	}
	if err := st.AppendWrites(1, frameFor(1)); err != nil {
		t.Fatal(err)
	}
	if err := st.FlushAndSetExecHead(1); err != nil {
		t.Fatal(err)
	}
	if err := CookIndex(dir); err != nil {
		t.Fatal(err)
	}
	hist, err := OpenHistory(dir, st, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer hist.Close()
	if _, _, _, err := hist.EnableTail(1); err != nil {
		t.Fatal(err)
	}

	// written[i] = the highest block that has been fully applied for addrs[i].
	var written [nAddrs]atomic.Uint64
	written[1%nAddrs].Store(1)

	var (
		wg   sync.WaitGroup
		stop atomic.Bool
	)
	wg.Add(1)
	go func() { // the executor
		defer wg.Done()
		defer stop.Store(true)
		for b := uint64(2); b <= nBlocks; b++ {
			if err := st.AppendWrites(b, frameFor(b)); err != nil {
				t.Error(err)
				return
			}
			written[b%nAddrs].Store(b)
			if b%256 == 0 {
				if err := st.FlushAndSetExecHead(b); err != nil {
					t.Error(err)
					return
				}
			}
		}
		if err := st.FlushAndSetExecHead(nBlocks); err != nil {
			t.Error(err)
		}
	}()

	wg.Add(1)
	go func() { // the cook loop, prune strictly after Refresh
		defer wg.Done()
		for !stop.Load() {
			if err := CookIndex(dir); err != nil {
				t.Error(err)
				return
			}
			if err := hist.Refresh(); err != nil {
				t.Error(err)
				return
			}
			hist.PruneTail()
			time.Sleep(2 * time.Millisecond)
		}
	}()

	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func(seed int) { // the RPC goroutines
			defer wg.Done()
			for !stop.Load() {
				for i := range addrs {
					want := written[(i+seed)%nAddrs].Load()
					if want == 0 {
						continue
					}
					a := addrs[(i+seed)%nAddrs]
					acc, err := hist.AccountLatest(a)
					if err != nil {
						t.Errorf("account %x: %v", a, err)
						return
					}
					if acc == nil || acc.Nonce < want {
						t.Errorf("account %x: nonce %v, want >= %d (write lost between overlay and descent)", a, accNonce(acc), want)
						return
					}
					v, err := hist.StorageLatest(a, slot[:])
					if err != nil {
						t.Errorf("storage %x: %v", a, err)
						return
					}
					if len(v) != 2 || uint64(v[0])|uint64(v[1])<<8 < want {
						t.Errorf("storage %x: %x, want block >= %d", a, v, want)
						return
					}
				}
			}
		}(r)
	}
	wg.Wait()
}

func accNonce(a *types.StateAccount) any {
	if a == nil {
		return "missing"
	}
	return a.Nonce
}

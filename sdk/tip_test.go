package sdk

import (
	"encoding/binary"
	"math/big"
	"testing"
	"time"

	avaconstants "github.com/ava-labs/avalanchego/utils/constants"
	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/rlp"
	"github.com/holiman/uint256"

	"github.com/containerman17/epochdb/chain"
	"github.com/containerman17/epochdb/fetch"
	"github.com/containerman17/epochdb/state"
)

// TestTipLiveness is THE gate for the SDK's reason to exist: a reader handle
// in a SEPARATE store instance, holding the dir read-only, must see the
// writer's newest block and read the values that block wrote, WITHOUT a cook
// happening in between. Nothing here cooks after the SDK is open, so every
// answer above block 2 can only have come from the raw writelog frames the
// handle tailed into its uncooked-tail overlay.
//
// Teeth: drop the AdvanceTail call in advance() and the balance read at the
// tip returns the block-2 value; drop the RescanRO call and the head never
// moves at all.
func TestTipLiveness(t *testing.T) {
	fetch.RegisterExtras(chain.Coreth)
	dir := t.TempDir()

	addr := common.HexToAddress("0xaaaa000000000000000000000000000000000001")
	slot := common.HexToHash("0x01")

	// --- the writer: everything serve's executor appends, minus the EVM -----
	w, err := state.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	write := func(block uint64, frame []byte) {
		t.Helper()
		if err := w.AppendWrites(block, frame); err != nil {
			t.Fatal(err)
		}
		if err := w.AppendHeader(block, headerRLP(t, block)); err != nil {
			t.Fatal(err)
		}
	}

	// Cooked history: balance 100, slot 0x2a, indexed by one cook. This is the
	// only cook in the test, and it happens before the SDK opens.
	write(1, frAccount(nil, addr, accRLP(t, 1, 100)))
	write(2, frStorage(nil, addr, slot, []byte{0x2a}))
	if err := w.FlushAndSetExecHead(2); err != nil {
		t.Fatal(err)
	}
	if err := state.CookIndex(dir); err != nil {
		t.Fatal(err)
	}

	// --- the reader: separate store instance, read-only, tailing ------------
	db, err := Open(dir, avaconstants.FujiID)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if got, err := db.Head(); err != nil || got != 2 {
		t.Fatalf("head at open = %d err=%v, want 2", got, err)
	}
	if bal, err := db.Balance(addr, Latest); err != nil || bal.Uint64() != 100 {
		t.Fatalf("cooked balance = %v err=%v, want 100", bal, err)
	}

	// --- the writer moves the tip, with NO cook -----------------------------
	write(3, frStorage(frAccount(nil, addr, accRLP(t, 2, 999)), addr, slot, []byte{0x7f}))

	deadline := time.Now().Add(10 * time.Second)
	for {
		head, err := db.Head()
		if err != nil {
			t.Fatalf("follower stopped: %v", err)
		}
		if head >= 3 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("head still %d after 10s: the handle never saw the writer's block 3", head)
		}
		time.Sleep(5 * time.Millisecond)
	}

	// The newest block's writes answer at the tip, out of the overlay alone.
	if bal, err := db.Balance(addr, Latest); err != nil || bal.Uint64() != 999 {
		t.Fatalf("tip balance = %v err=%v, want 999 (block 3, uncooked)", bal, err)
	}
	if n, err := db.Nonce(addr, Latest); err != nil || n != 2 {
		t.Fatalf("tip nonce = %d err=%v, want 2", n, err)
	}
	if v, err := db.StorageAt(addr, slot, Latest); err != nil || v.Big().Uint64() != 0x7f {
		t.Fatalf("tip storage = %v err=%v, want 0x7f", v, err)
	}
	if acc, err := db.Account(addr, Latest); err != nil || acc == nil || acc.Balance.Uint64() != 999 {
		t.Fatalf("tip account = %+v err=%v", acc, err)
	}

	// And the historical descent still answers block 2 with block 2's values,
	// which is what proves the tip answer came from the overlay and not from a
	// silently moved watermark.
	if bal, err := db.Balance(addr, 2); err != nil || bal.Uint64() != 100 {
		t.Fatalf("historical balance at 2 = %v err=%v, want 100", bal, err)
	}
	if v, err := db.StorageAt(addr, slot, 2); err != nil || v.Big().Uint64() != 0x2a {
		t.Fatalf("historical storage at 2 = %v err=%v, want 0x2a", v, err)
	}
}

// --- writer-side record builders (the encodings state/cook.go parses) -------

func frAccount(f []byte, addr common.Address, valRLP []byte) []byte {
	f = append(f, 'a')
	f = append(f, addr[:]...)
	f = binary.AppendUvarint(f, uint64(len(valRLP)))
	return append(f, valRLP...)
}

func frStorage(f []byte, addr common.Address, slot common.Hash, val []byte) []byte {
	f = append(f, 's')
	f = append(f, addr[:]...)
	f = append(f, slot[:]...)
	f = binary.AppendUvarint(f, uint64(len(val)))
	return append(f, val...)
}

func accRLP(t *testing.T, nonce, balance uint64) []byte {
	t.Helper()
	raw, err := rlp.EncodeToBytes(&types.StateAccount{
		Nonce:    nonce,
		Balance:  uint256.NewInt(balance),
		Root:     types.EmptyRootHash,
		CodeHash: types.EmptyCodeHash.Bytes(),
	})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func headerRLP(t *testing.T, n uint64) []byte {
	t.Helper()
	raw, err := rlp.EncodeToBytes(&types.Header{Number: new(big.Int).SetUint64(n)})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

package state

import (
	"bytes"
	"testing"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/types"
)

// frontierCorpus is the merge's exercise sheet, cut into TWO epochs so every
// cross-file rule is actually crossed:
//
//	epoch 1 (blocks 1..4)   epoch 2 (blocks 5..8)
//	  fnA account+slot1       fnA slot1 overwritten AGAIN (newest wins across files)
//	  fnB account+slot1,2     fnB SELFDESTRUCT, then recreated with slot2 only
//	  fnC account             fnC deleted and never recreated
//	  fnD slot1               fnD slot1 zeroed (tombstone, no account row at all)
var (
	fnA = common.HexToAddress("0xaa11111111111111111111111111111111111111")
	fnB = common.HexToAddress("0xbb22222222222222222222222222222222222222")
	fnC = common.HexToAddress("0xcc33333333333333333333333333333333333333")
	fnD = common.HexToAddress("0xdd44444444444444444444444444444444444444")

	fnS1 = common.HexToHash("0x01")
	fnS2 = common.HexToHash("0x02")
)

func frontierRows(t *testing.T) map[uint64][]byte {
	t.Helper()
	return map[uint64][]byte{
		1: frStorage(frAccount(nil, fnA, accRLP(t, 1, 100)), fnA, fnS1, []byte{0x11}),
		2: frStorage(frStorage(frAccount(nil, fnB, accRLP(t, 1, 50)), fnB, fnS1, []byte{0x21}), fnB, fnS2, []byte{0x22}),
		3: frAccount(nil, fnC, accRLP(t, 7, 7)),
		4: frStorage(nil, fnD, fnS1, []byte{0x41}),
		5: frStorage(nil, fnA, fnS1, []byte{0x99}), // overwrite across the epoch boundary
		6: frAccount(nil, fnB, nil),                // SELFDESTRUCT: kills slots 1 and 2
		7: frStorage(frAccount(nil, fnB, accRLP(t, 0, 5)), fnB, fnS2, []byte{0x77}),
		8: frStorage(frAccount(nil, fnC, nil), fnD, fnS1, nil), // delete + zero write
	}
}

// buildFrontierEpochs seals the corpus into two epochs of 4 blocks each
// (3 txs per block, boundary at 12) and returns the opened set.
func buildFrontierEpochs(t *testing.T, dir string) *EpochSet {
	t.Helper()
	frames := frontierRows(t)
	for n := uint64(1); n <= 8; n++ {
		writeStagingBlock(t, dir, 0, n, 3)
	}
	st, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	for n := uint64(1); n <= 8; n++ {
		if err := st.AppendHeader(n, []byte{0x99, byte(n)}); err != nil {
			t.Fatal(err)
		}
		if err := st.AppendWrites(n, frames[n]); err != nil {
			t.Fatal(err)
		}
		if err := st.AppendRcpt(n, synthRcpt(3, n)); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.SetLogsStart(1); err != nil {
		t.Fatal(err)
	}
	if err := st.FlushAndSetExecHead(8); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	fixedEpochTxs(t, 12)
	cas := testStore(t, dir)
	if err := sealEpochs(dir, cas, [32]byte{}); err != nil {
		t.Fatal(err)
	}
	set, err := OpenEpochSet(cas)
	if err != nil {
		t.Fatal(err)
	}
	if len(set.Epochs) != 2 {
		t.Fatalf("want 2 epochs, got %d", len(set.Epochs))
	}
	return set
}

// TestMergeFrontierEqualsDescent is the merge's correctness property: the
// frontier it emits at H must be exactly what a full descent answers at H,
// key for key. The descent is the authority on tombstone semantics, so this
// is the test that pins them (SELFDESTRUCT-then-recreate included), and the
// corpus is two epochs so newest-wins is proven ACROSS files.
func TestMergeFrontierEqualsDescent(t *testing.T) {
	dir := t.TempDir()
	set := buildFrontierEpochs(t, dir)
	defer set.Close()

	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := CookIndex(dir); err != nil {
		t.Fatal(err)
	}
	hist, err := OpenHistory(dir, store, types.GenesisAlloc{})
	if err != nil {
		t.Fatal(err)
	}
	defer hist.Close()

	const h = 8
	got := map[string][]byte{}
	if err := MergeFrontier(set.Epochs, h, func(r FrontierRow) error {
		if len(r.Value) == 0 {
			return nil // not part of the frontier
		}
		got[string(r.Key)] = bytes.Clone(r.Value)
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// Every (account, slot) the corpus ever touched, checked both ways.
	for _, a := range []common.Address{fnA, fnB, fnC, fnD} {
		acc, err := hist.AccountAt(a, h)
		if err != nil {
			t.Fatal(err)
		}
		key := string(accountKey(a))
		if acc == nil {
			if v, ok := got[key]; ok {
				t.Fatalf("%x: descent says gone, merge emitted %x", a, v)
			}
		} else if _, ok := got[key]; !ok {
			t.Fatalf("%x: descent says live, merge dropped it", a)
		}
		for _, s := range []common.Hash{fnS1, fnS2} {
			want, err := hist.StorageAt(a, s.Bytes(), h)
			if err != nil {
				t.Fatal(err)
			}
			have := got[string(storageKey(a, s.Bytes()))]
			if !bytes.Equal(want, have) {
				t.Fatalf("%x slot %x: descent %x, merge %x", a, s, want, have)
			}
		}
	}

	// And the specifics, so a mutually-broken pair cannot pass:
	// A's slot1 is epoch 2's value, B's pre-destruct slot1 is gone, its
	// recreated slot2 is live, C and D are gone entirely.
	for _, c := range []struct {
		name string
		key  string
		want []byte
	}{
		{"A.slot1 newest across epochs", string(storageKey(fnA, fnS1.Bytes())), []byte{0x99}},
		{"B.slot1 killed by SELFDESTRUCT", string(storageKey(fnB, fnS1.Bytes())), nil},
		{"B.slot2 rewritten after recreate", string(storageKey(fnB, fnS2.Bytes())), []byte{0x77}},
		{"D.slot1 zeroed", string(storageKey(fnD, fnS1.Bytes())), nil},
	} {
		if !bytes.Equal(got[c.key], c.want) {
			t.Fatalf("%s: got %x, want %x", c.name, got[c.key], c.want)
		}
	}
	if _, ok := got[string(accountKey(fnC))]; ok {
		t.Fatal("C was deleted at block 8 and must not be in the frontier")
	}
	if _, ok := got[string(accountKey(fnB))]; !ok {
		t.Fatal("B was recreated at block 7 and must be in the frontier")
	}
}

// TestMergeFrontierBelowHead: the target height cuts the corpus, so a merge
// at H=5 must answer exactly like a descent at 5 (B still alive with both
// its original slots, A already overwritten).
func TestMergeFrontierBelowHead(t *testing.T) {
	dir := t.TempDir()
	set := buildFrontierEpochs(t, dir)
	defer set.Close()

	got := map[string][]byte{}
	if err := MergeFrontier(set.Epochs, 5, func(r FrontierRow) error {
		if len(r.Value) > 0 {
			got[string(r.Key)] = bytes.Clone(r.Value)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		name string
		key  string
		want []byte
	}{
		{"A.slot1 at 5", string(storageKey(fnA, fnS1.Bytes())), []byte{0x99}},
		{"B.slot1 alive at 5", string(storageKey(fnB, fnS1.Bytes())), []byte{0x21}},
		{"B.slot2 alive at 5", string(storageKey(fnB, fnS2.Bytes())), []byte{0x22}},
		{"D.slot1 alive at 5", string(storageKey(fnD, fnS1.Bytes())), []byte{0x41}},
	} {
		if !bytes.Equal(got[c.key], c.want) {
			t.Fatalf("%s: got %x, want %x", c.name, got[c.key], c.want)
		}
	}
	if _, ok := got[string(accountKey(fnC))]; !ok {
		t.Fatal("C is alive at block 5")
	}
}

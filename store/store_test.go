package store

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/containerman17/epochdb/dist"
)

func testDB(t *testing.T) (*DB, string) {
	t.Helper()
	dir := t.TempDir()
	cas, err := dist.Local(dir)
	if err != nil {
		t.Fatal(err)
	}
	db, err := Open(dir, cas, [32]byte{1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db, dir
}

func addr(i byte) []byte { b := make([]byte, 20); b[19] = i; return b }
func hash32(i byte) []byte {
	b := make([]byte, 32)
	b[31] = i
	return b
}

// block builds a synthetic block with n txs, each writing one slot.
func block(height uint64, n int) *BlockWrite {
	b := &BlockWrite{
		Height:    height,
		HeaderRLP: []byte(fmt.Sprintf("header-%d", height)),
		Pvm:       []byte(fmt.Sprintf("pvm-%d", height)),
		Code:      map[string][]byte{},
	}
	for i := 0; i < n; i++ {
		h := hash32(byte(height*10 + uint64(i)))
		b.Txs = append(b.Txs, TxWrite{
			Hash:     h,
			RLP:      []byte(fmt.Sprintf("tx-%d-%d", height, i)),
			Receipt:  []byte(fmt.Sprintf("rcpt-%d-%d", height, i)),
			Sender:   addr(1),
			To:       addr(2),
			LogAddrs: [][]byte{addr(3)},
			Topics:   [][]byte{hash32(9)},
			State: []StateRow{
				{Kind: 's', Addr: addr(2), Slot: hash32(7), Val: []byte{byte(height), byte(i)}},
				{Kind: 'a', Addr: addr(2), Val: []byte(fmt.Sprintf("acct-%d-%d", height, i))},
			},
		})
	}
	return b
}

// TestRoundTrip: every family survives a flush and reads back through the
// descent exactly as it went in, and the window serves what is not flushed yet.
func TestRoundTrip(t *testing.T) {
	db, _ := testDB(t)
	for h := uint64(0); h < 4; h++ {
		if err := db.WriteBlock(block(h, 3)); err != nil {
			t.Fatal(err)
		}
	}
	check := func(where string) {
		t.Helper()
		for h := uint64(0); h < 4; h++ {
			hdr, ok, err := db.HeaderRLP(h)
			if err != nil || !ok || string(hdr) != fmt.Sprintf("header-%d", h) {
				t.Fatalf("%s: hdr %d: %q %v %v", where, h, hdr, ok, err)
			}
			pvm, ok, err := db.Pvm(h)
			if err != nil || !ok || string(pvm) != fmt.Sprintf("pvm-%d", h) {
				t.Fatalf("%s: pvm %d: %q %v %v", where, h, pvm, ok, err)
			}
			first, n, ok, err := db.BlockTxRange(h)
			if err != nil || !ok || n != 3 || first != h*3 {
				t.Fatalf("%s: blk %d: %d %d %v %v", where, h, first, n, ok, err)
			}
			for i := uint64(0); i < 3; i++ {
				want := fmt.Sprintf("tx-%d-%d", h, i)
				got, ok, err := db.TxRLP(first + i)
				if err != nil || !ok || string(got) != want {
					t.Fatalf("%s: tx %d: %q %v %v", where, first+i, got, ok, err)
				}
				rc, ok, err := db.Receipt(first + i)
				if err != nil || !ok || string(rc) != fmt.Sprintf("rcpt-%d-%d", h, i) {
					t.Fatalf("%s: rcpt %d: %q %v %v", where, first+i, rc, ok, err)
				}
				num, ok, err := db.TxNumByHash(hash32(byte(h*10 + i)))
				if err != nil || !ok || num != first+i {
					t.Fatalf("%s: txh %d/%d: %d %v %v", where, h, i, num, ok, err)
				}
				v, ok, err := db.StorageAt(addr(2), hash32(7), first+i)
				if err != nil || !ok || !bytes.Equal(v, []byte{byte(h), byte(i)}) {
					t.Fatalf("%s: slot at %d: %x %v %v", where, first+i, v, ok, err)
				}
				a, ok, err := db.AccountAt(addr(2), first+i)
				if err != nil || !ok || string(a) != fmt.Sprintf("acct-%d-%d", h, i) {
					t.Fatalf("%s: acct at %d: %q %v %v", where, first+i, a, ok, err)
				}
				gh, ok, err := db.HeightOfTx(first + i)
				if err != nil || !ok || gh != h {
					t.Fatalf("%s: heightOfTx %d: %d %v %v", where, first+i, gh, ok, err)
				}
			}
		}
		// A hash that was never written must miss.
		if _, ok, err := db.TxNumByHash(hash32(200)); ok || err != nil {
			t.Fatalf("%s: absent tx hash reported %v %v", where, ok, err)
		}
		if _, ok, err := db.StorageAt(addr(200), hash32(7), 100); ok || err != nil {
			t.Fatalf("%s: absent slot reported %v %v", where, ok, err)
		}
	}
	check("window")
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	check("run")

	// Postings survive too.
	var got []uint64
	if err := db.Postings(LogAddrPrefix(addr(3)), 0, 1<<62, func(n uint64, _ byte) bool {
		got = append(got, n)
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if len(got) != 12 {
		t.Fatalf("logaddr postings: got %d, want 12", len(got))
	}
}

// TestDeterminism is the core promise: the same sorted rows produce byte-
// identical run files, and re-flushing the same staging range reproduces the
// same run.
func TestDeterminism(t *testing.T) {
	build := func() (string, string) {
		t.Helper()
		dir := t.TempDir()
		cas, err := dist.Local(dir)
		if err != nil {
			t.Fatal(err)
		}
		db, err := Open(dir, cas, [32]byte{1})
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		for h := uint64(0); h < 6; h++ {
			if err := db.WriteBlock(block(h, 5)); err != nil {
				t.Fatal(err)
			}
		}
		if err := db.Flush(); err != nil {
			t.Fatal(err)
		}
		m := db.Manifest()
		if len(m.Runs) != 1 {
			t.Fatalf("want 1 run, got %d", len(m.Runs))
		}
		return m.Runs[0].Name, filepath.Join(dir, "cas", m.Runs[0].Name)
	}
	nameA, pathA := build()
	nameB, pathB := build()
	if nameA != nameB {
		t.Fatalf("run names differ: %s vs %s", nameA, nameB)
	}
	a, err := os.ReadFile(pathA)
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(pathB)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(a, b) {
		t.Fatalf("run bytes differ: %d vs %d bytes", len(a), len(b))
	}
	if len(a) == 0 {
		t.Fatal("empty run")
	}
}

// TestPrevLink: the first run's prev is the chain root and each later run's
// prev is its predecessor's name.
func TestPrevLink(t *testing.T) {
	db, dir := testDB(t)
	for h := uint64(0); h < 4; h++ {
		if err := db.WriteBlock(block(h, 2)); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	for h := uint64(4); h < 8; h++ {
		if err := db.WriteBlock(block(h, 2)); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	m := db.Manifest()
	if len(m.Runs) != 2 {
		t.Fatalf("want 2 runs, got %d", len(m.Runs))
	}
	cas, err := dist.Local(dir)
	if err != nil {
		t.Fatal(err)
	}
	r0, err := OpenRun(cas, m.Runs[0].Name)
	if err != nil {
		t.Fatal(err)
	}
	defer r0.Close()
	if hexName(r0.Footer.Prev) != m.ChainRoot {
		t.Fatalf("first run prev = %s, want chain root %s", hexName(r0.Footer.Prev), m.ChainRoot)
	}
	r1, err := OpenRun(cas, m.Runs[1].Name)
	if err != nil {
		t.Fatal(err)
	}
	defer r1.Close()
	if hexName(r1.Footer.Prev) != m.Runs[0].Name {
		t.Fatalf("second run prev = %s, want %s", hexName(r1.Footer.Prev), m.Runs[0].Name)
	}
	if r1.Footer.FromTx != 8 || r1.Footer.ToTx != 16 {
		t.Fatalf("second run tx range = [%d,%d), want [8,16)", r1.Footer.FromTx, r1.Footer.ToTx)
	}
}

// TestBloomGate: the prefix bloom keeps the TxNum suffix out, so an address
// that exists answers "may have" at any position, and one that does not is
// rejected without a read.
func TestBloomGate(t *testing.T) {
	db, dir := testDB(t)
	for h := uint64(0); h < 3; h++ {
		if err := db.WriteBlock(block(h, 4)); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	cas, err := dist.Local(dir)
	if err != nil {
		t.Fatal(err)
	}
	r, err := OpenRun(cas, db.Manifest().Runs[0].Name)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if len(r.filter[SecState]) == 0 || len(r.filter[SecLookup]) == 0 {
		t.Fatal("state and lookup sections must carry a resident filter")
	}
	if r.filter[SecChain] != nil {
		t.Fatal("chain section must carry NO filter")
	}
	// Present at any TxNum, including ones past the run.
	for _, at := range []uint64{0, 5, 1 << 40} {
		if !r.MayHave(SecState, Suffixed(SlotPrefix(addr(2), hash32(7)), at)) {
			t.Fatalf("prefix bloom missed a present slot at txnum %d", at)
		}
	}
	misses := 0
	for i := 0; i < 200; i++ {
		if !r.MayHave(SecState, Suffixed(SlotPrefix(addr(byte(i)+50), hash32(byte(i))), 0)) {
			misses++
		}
	}
	if misses < 180 {
		t.Fatalf("prefix bloom rejected only %d of 200 absent slots", misses)
	}
}

// TestSplitPins pins the bloom-prefix split of every suffixed family; getting
// this wrong silently turns a prefix bloom into a whole-key one.
func TestSplitPins(t *testing.T) {
	cases := []struct {
		key  []byte
		want int
	}{
		{Suffixed(SlotPrefix(addr(1), hash32(2)), 9), 62},
		{Suffixed(AccountPrefix(addr(1)), 9), 29},
		{Suffixed(CodeRefPrefix(addr(1)), 9), 29},
		{Suffixed(AddrPrefix(addr(1)), 9), 26},
		{Suffixed(LogAddrPrefix(addr(1)), 9), 29},
		{Suffixed(TopicPrefix(hash32(1)), 9), 39},
		{TxHashKey(hash32(1)), 36},
		{CodeKey(hash32(1)), 37},
	}
	for _, c := range cases {
		if got := split(c.key); got != c.want {
			t.Errorf("split(%q) = %d, want %d", c.key[:6], got, c.want)
		}
	}
}

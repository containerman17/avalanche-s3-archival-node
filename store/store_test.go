package store

import (
	"bytes"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/crypto"
	"github.com/ava-labs/libevm/rlp"
	"github.com/cockroachdb/pebble/v2/sstable"

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
			Hash:       h,
			RLP:        []byte(fmt.Sprintf("tx-%d-%d", height, i)),
			Receipt:    []byte(fmt.Sprintf("rcpt-%d-%d", height, i)),
			Frames:     []byte(fmt.Sprintf("itx-%d-%d", height, i)),
			FrameAddrs: [][]byte{addr(4)},
			Sender:     addr(1),
			To:         addr(2),
			LogAddrs:   [][]byte{addr(3)},
			Topics:     [][]byte{hash32(9)},
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
				fr, ok, err := db.Frames(first + i)
				if err != nil || !ok || string(fr) != fmt.Sprintf("itx-%d-%d", h, i) {
					t.Fatalf("%s: itx %d: %q %v %v", where, first+i, fr, ok, err)
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

	// The frame participant earns its own role bit in the addr/ family.
	var bits byte
	if err := db.Postings(AddrPrefix(addr(4)), 0, 1<<62, func(_ uint64, v byte) bool {
		bits |= v
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if bits&RoleFrame == 0 {
		t.Fatalf("addr/ row for a frame participant has role bits %08b, want RoleFrame set", bits)
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
		// An L0 run is LOCAL FOREVER, so its file is in the local directory and
		// its name there carries the level and tx range beside the hash.
		p := cas.LocalPath(m.Runs[0].Name)
		if want := "l0-0000000000000000-0000000000000030-" + m.Runs[0].Name; filepath.Base(p) != want {
			t.Fatalf("L0 run file is %q, want %q", filepath.Base(p), want)
		}
		return m.Runs[0].Name, p
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

// TestFormatConstantsArePinned guards the format constants that a corpus is
// unreadable or non-identical without. The size comparison is the load-bearing
// half: pebble's WriterOptions.ensureDefaults SILENTLY replaces a compression
// profile the table format cannot carry with Snappy, so a profile built on the
// wrong preset would compile, run, and quietly ship snappy runs.
func TestFormatConstantsArePinned(t *testing.T) {
	if TableFormat != sstable.TableFormatPebblev2 {
		t.Errorf("TableFormat is %v, want Pebblev2", TableFormat)
	}
	if BlockSize(SecChain) != 128<<10 || BlockSize(SecState) != 32<<10 || BlockSize(SecLookup) != 8<<10 {
		t.Errorf("block sizes are %d/%d/%d, want 128K/32K/8K",
			BlockSize(SecChain), BlockSize(SecState), BlockSize(SecLookup))
	}
	if Compression.UsesMinLZ() {
		t.Fatal("the pinned profile uses MinLZ, which TableFormatPebblev2 cannot carry: pebble would swap the whole profile for Snappy")
	}
	for name, got := range map[string]uint8{
		"DataBlocks":  Compression.DataBlocks.Level,
		"ValueBlocks": Compression.ValueBlocks.Level,
		"OtherBlocks": Compression.OtherBlocks.Level,
	} {
		if got != ZstdLevel {
			t.Errorf("%s level is %d, want %d", name, got, ZstdLevel)
		}
	}
	if a := Compression.DataBlocks.Algorithm; a != sstable.ZstdCompression.DataBlocks.Algorithm {
		t.Errorf("data blocks use algorithm %v, want zstd", a)
	}

	// And it must actually reach the bytes: the same rows must come out
	// materially smaller than snappy would write them.
	size := func(prof *sstable.CompressionProfile) int {
		var buf sizeWritable
		o := writerOptions(SecChain)
		o.Compression = prof
		w := sstable.NewWriter(&buf, o)
		for i := 0; i < 20000; i++ {
			if err := w.Set(TxKey(uint64(i)), []byte(fmt.Sprintf("tx-%d-and-a-very-repetitive-tail-that-any-codec-should-shrink", i))); err != nil {
				t.Fatal(err)
			}
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		return buf.n
	}
	zstd, snappy := size(Compression), size(sstable.SnappyCompression)
	t.Logf("20,000 chain rows: pinned zstd%d %d bytes, snappy %d bytes", ZstdLevel, zstd, snappy)
	if zstd >= snappy {
		t.Errorf("the pinned profile wrote %d bytes and snappy wrote %d: the profile did not reach the blocks", zstd, snappy)
	}
}

// sizeWritable counts the bytes an sstable writer produces.
type sizeWritable struct{ n int }

func (w *sizeWritable) Write(p []byte) error { w.n += len(p); return nil }
func (w *sizeWritable) Finish() error        { return nil }
func (w *sizeWritable) Abort()               {}

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
		{BlkHashKey(hash32(1)), 37}, // whole key: a block hash carries no suffix
		{CodeKey(hash32(1)), 37},
	}
	for _, c := range cases {
		if got := split(c.key); got != c.want {
			t.Errorf("split(%q) = %d, want %d", c.key[:6], got, c.want)
		}
	}
}

// TestWindowRecovery: a restart replays the window log back into RAM, cuts a
// torn tail at the last complete block, and serves everything the window held.
// Without this the state layer would come back behind Firewood, which cannot be
// rolled back.
func TestWindowRecovery(t *testing.T) {
	dir := t.TempDir()
	cas, err := dist.Local(dir)
	if err != nil {
		t.Fatal(err)
	}
	db, err := Open(dir, cas, [32]byte{1})
	if err != nil {
		t.Fatal(err)
	}
	for h := uint64(0); h < 5; h++ {
		if err := db.WriteBlock(block(h, 3)); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// Simulate a kill mid-block: append garbage past the last end record.
	wl := filepath.Join(dir, "window", "window.log")
	f, err := os.OpenFile(wl, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("Tsome torn tail")); err != nil {
		t.Fatal(err)
	}
	f.Close()

	db2, err := Open(dir, cas, [32]byte{1})
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	if got := db2.NextHeight(); got != 5 {
		t.Fatalf("recovered next height = %d, want 5", got)
	}
	if got := db2.NextTx(); got != 15 {
		t.Fatalf("recovered next tx = %d, want 15", got)
	}
	for h := uint64(0); h < 5; h++ {
		hdr, ok, err := db2.HeaderRLP(h)
		if err != nil || !ok || string(hdr) != fmt.Sprintf("header-%d", h) {
			t.Fatalf("recovered hdr %d: %q %v %v", h, hdr, ok, err)
		}
	}
	v, ok, err := db2.StorageAt(addr(2), hash32(7), 14)
	if err != nil || !ok || !bytes.Equal(v, []byte{4, 2}) {
		t.Fatalf("recovered slot: %x %v %v", v, ok, err)
	}
	if n, ok, err := db2.TxNumByHash(hash32(42)); err != nil || !ok || n != 14 {
		t.Fatalf("recovered txh: %d %v %v", n, ok, err)
	}
	// The recovered window still flushes into a run.
	if err := db2.Flush(); err != nil {
		t.Fatal(err)
	}
	if got := db2.Manifest().Runs[0].ToTx; got != 15 {
		t.Fatalf("flushed run to_tx = %d, want 15", got)
	}
}

// TestBlockHashIndex pins the two things the block-hash family rests on: that a
// block hash is keccak256 of the STORED header RLP (so nothing has to carry it
// in), and that the row answers both from the window and from a sealed run.
func TestBlockHashIndex(t *testing.T) {
	db, _ := testDB(t)
	hdr := &types.Header{
		Number: big.NewInt(1), Time: 1767225600, GasLimit: 15_000_000,
		BaseFee: big.NewInt(25_000_000_000), Difficulty: big.NewInt(1), Extra: []byte{},
	}
	raw, err := rlp.EncodeToBytes(hdr)
	if err != nil {
		t.Fatal(err)
	}
	if got := crypto.Keccak256Hash(raw); got != hdr.Hash() {
		t.Fatalf("keccak256(header RLP) = %s, header.Hash() = %s: the derived row would be wrong", got, hdr.Hash())
	}
	if err := db.WriteBlock(&BlockWrite{Height: 1, HeaderRLP: raw, Pvm: []byte("pvm")}); err != nil {
		t.Fatal(err)
	}
	check := func(where string) {
		t.Helper()
		n, ok, err := db.HeightByHash(hdr.Hash().Bytes())
		if err != nil || !ok || n != 1 {
			t.Fatalf("%s: HeightByHash = %d,%v,%v", where, n, ok, err)
		}
		// An unknown hash is a clean miss, never an error and never a height.
		if _, ok, err := db.HeightByHash(hash32(200)); ok || err != nil {
			t.Fatalf("%s: unknown block hash: ok=%v err=%v", where, ok, err)
		}
		// A TX hash must never answer a BLOCK hash probe: the two families
		// share one window map and are told apart by their key prefix.
		if _, ok, err := db.TxNumByHash(hdr.Hash().Bytes()); ok || err != nil {
			t.Fatalf("%s: a block hash answered a tx-hash probe: ok=%v err=%v", where, ok, err)
		}
	}
	check("window")
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	check("run")
}

// TestFlatCacheNeverStale is the check the flat latest-state cache exists to
// pass: THE CACHED ANSWER AT THE HEAD IS THE HEAD'S ANSWER, at every step, and
// no read at any other height is ever served out of it. It walks the three ways
// the cache can be wrong: a value read at the head and then overwritten by the
// next block, a historical read that must not pick up the head's value, and a
// key the descent says was never written (found=false is cached too, and a
// later block must invalidate it).
func TestFlatCacheNeverStale(t *testing.T) {
	db, _ := testDB(t)
	slot := SlotPrefix(addr(2), hash32(7))

	// A key nothing has written: cached as "never written", which must not
	// survive the block that writes it.
	fresh := SlotPrefix(addr(9), hash32(9))

	for h := uint64(0); h < 12; h++ {
		if err := db.WriteBlock(block(h, 3)); err != nil {
			t.Fatal(err)
		}
		head, ok := db.Head()
		if !ok || head != h {
			t.Fatalf("head %d %v after block %d", head, ok, h)
		}
		at, any, err := db.txCeiling(head)
		if err != nil || !any {
			t.Fatalf("ceiling at %d: %d %v %v", head, at, any, err)
		}
		// Through the cache and around it must agree, every block.
		got, gotOK, err := db.stateRow(slot, head, at)
		if err != nil {
			t.Fatal(err)
		}
		want, wantOK, err := db.latest(SecState, slot, at)
		if err != nil {
			t.Fatal(err)
		}
		if gotOK != wantOK || !bytes.Equal(got, want) {
			t.Fatalf("block %d: cache says %x/%v, descent says %x/%v", h, got, gotOK, want, wantOK)
		}
		if want == nil || len(want) != 2 || want[0] != byte(h) {
			t.Fatalf("block %d: descent value %x is not this block's write", h, want)
		}
		// The absent key stays absent, through the cache and around it.
		if _, ok, err := db.stateRow(fresh, head, at); err != nil || ok {
			t.Fatalf("block %d: absent slot answered %v %v", h, ok, err)
		}
		// A historical read must never be served (or polluted) by the cache.
		if h > 0 {
			hAt, _, err := db.txCeiling(h - 1)
			if err != nil {
				t.Fatal(err)
			}
			old, _, err := db.stateRow(slot, h-1, hAt)
			if err != nil {
				t.Fatal(err)
			}
			if len(old) != 2 || old[0] != byte(h-1) {
				t.Fatalf("block %d: historical read at %d gave %x", h, h-1, old)
			}
		}
	}

	// A block that is not the next one drops the cache whole rather than
	// arguing about consistency: replaying block 5 must not leave the cache
	// claiming to be the latest state of block 11.
	db.flat.apply(block(5, 3))
	head, _ := db.Head()
	if _, _, hit := db.flat.get(slot, head); hit {
		t.Fatal("the cache still serves the head after an out-of-order block")
	}
}

// TestFlatCacheBudget: rotation drops entries and never answers, which is a
// miss and never a wrong answer.
func TestFlatCacheBudget(t *testing.T) {
	c := newFlatCache(4 * (flatEntryOverhead + 8)) // half is two entries
	c.adopt(7)
	for i := 0; i < 64; i++ {
		c.fill([]byte(fmt.Sprintf("k%07d", i)), []byte{byte(i)}, true, 7)
	}
	if n := len(c.hot) + len(c.cold); n > 6 {
		t.Fatalf("the cache holds %d entries under a two-entry budget", n)
	}
	if _, _, hit := c.get([]byte("k0000063"), 7); !hit {
		t.Fatal("the newest entry was evicted")
	}
	// A height that is not the cache's is a miss, always.
	if _, _, hit := c.get([]byte("k0000063"), 8); hit {
		t.Fatal("the cache answered for a height it is not the latest state of")
	}
	// A fill at a height the cache has moved past is refused.
	c.fill([]byte("stale"), []byte{1}, true, 6)
	if _, _, hit := c.get([]byte("stale"), 7); hit {
		t.Fatal("a fill from a stale height was accepted")
	}
}

// TestCodeCache: the code cache is keyed by the WHOLE hash and answers the same
// bytes the descent does, twice, which is the only way a content-addressed
// cache can be wrong. It runs against flushed runs, because a window hit never
// reaches the cache.
func TestCodeCache(t *testing.T) {
	db, _ := testDB(t)
	if db.code == nil {
		t.Fatal("the code cache is off by default")
	}
	blobs := map[string][]byte{}
	b := block(1, 1)
	for i := byte(1); i <= 4; i++ {
		h, blob := hash32(i), []byte(fmt.Sprintf("code-%d", i))
		b.Code[string(h)] = blob
		blobs[string(h)] = blob
	}
	if err := db.WriteBlock(b); err != nil {
		t.Fatal(err)
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	// Twice: the first pass fills, the second is served entirely by the cache.
	for pass := 0; pass < 2; pass++ {
		for i := byte(1); i <= 4; i++ {
			got, ok, err := db.Code(hash32(i))
			if err != nil || !ok {
				t.Fatalf("pass %d: code %d: ok=%v err=%v", pass, i, ok, err)
			}
			if !bytes.Equal(got, blobs[string(hash32(i))]) {
				t.Fatalf("pass %d: code %d is %q", pass, i, got)
			}
		}
		// A hash nothing carries stays absent through the cache and around it.
		if _, ok, err := db.Code(hash32(9)); err != nil || ok {
			t.Fatalf("pass %d: an unknown hash answered %v %v", pass, ok, err)
		}
	}
}

// TestConcurrentReadsWhileWriting is the check the memtable's read-lock fast
// path exists to pass: readers pread the window log while the executor keeps
// appending to it, so the flushed watermark and the append must not race and a
// reader must never see a row the writer had not finished. Run it under -race;
// that is the whole point.
func TestConcurrentReadsWhileWriting(t *testing.T) {
	db, _ := testDB(t)
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				head, ok := db.Head()
				if !ok {
					continue
				}
				// Every family, at the tip and behind it.
				for h := uint64(0); h <= head; h++ {
					hdr, ok, err := db.HeaderRLP(h)
					if err != nil {
						t.Error(err)
						return
					}
					if ok && string(hdr) != fmt.Sprintf("header-%d", h) {
						t.Errorf("torn header %d: %q", h, hdr)
						return
					}
					if _, _, _, err := db.BlockTxRange(h); err != nil {
						t.Error(err)
						return
					}
				}
			}
		}()
	}
	for h := uint64(0); h < 200; h++ {
		if err := db.WriteBlock(block(h, 3)); err != nil {
			t.Fatal(err)
		}
	}
	close(stop)
	wg.Wait()
}

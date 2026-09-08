package commit

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/rawdb"
	"github.com/ava-labs/libevm/core/state"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/crypto"
	"github.com/ava-labs/libevm/rlp"
	"github.com/ava-labs/libevm/triedb"
	"github.com/holiman/uint256"
)

// row is one contract row; rows is a sorted flat state with a Seek.
type row struct{ k, v []byte }
type rows []row

func (r rows) Len() int           { return len(r) }
func (r rows) Less(i, j int) bool { return bytes.Compare(r[i].k, r[j].k) < 0 }
func (r rows) Swap(i, j int)      { r[i], r[j] = r[j], r[i] }

func (r rows) Seek(prefix []byte) ([]byte, []byte) {
	i := sort.Search(len(r), func(i int) bool { return bytes.Compare(r[i].k, prefix) >= 0 })
	if i == len(r) {
		return nil, nil
	}
	return r[i].k, r[i].v
}

type rowIter struct {
	r rows
	i int
}

func (it *rowIter) Next() bool    { it.i++; return it.i <= len(it.r) }
func (it *rowIter) Key() []byte   { return it.r[it.i-1].k }
func (it *rowIter) Value() []byte { return it.r[it.i-1].v }
func (it *rowIter) Err() error    { return nil }

// acct is the test model of one account.
type acct struct {
	addr  common.Address
	nonce uint64
	bal   *uint256.Int
	code  []byte
	slots map[common.Hash]common.Hash
}

func acctKey(addr common.Address) []byte { return append(crypto.Keccak256(addr[:]), 0) }
func slotKey(addr common.Address, slot common.Hash) []byte {
	return append(append(crypto.Keccak256(addr[:]), 1), crypto.Keccak256(slot[:])...)
}
func acctVal(a *acct) []byte {
	ch := types.EmptyCodeHash.Bytes()
	if len(a.code) > 0 {
		ch = crypto.Keccak256(a.code)
	}
	v, _ := rlp.EncodeToBytes(&accountRow{Nonce: a.nonce, Balance: a.bal, CodeHash: ch})
	return v
}

func randWord(rng *rand.Rand) common.Hash {
	var h common.Hash
	switch rng.Intn(4) {
	case 0:
		h[31] = byte(rng.Intn(255) + 1) // short: one byte
	case 1:
		binary.BigEndian.PutUint64(h[24:], rng.Uint64()|1)
	default:
		rng.Read(h[:])
	}
	return h
}

func genState(rng *rand.Rand, n int) map[common.Address]*acct {
	m := make(map[common.Address]*acct, n)
	for i := 0; i < n; i++ {
		var a acct
		rng.Read(a.addr[:])
		a.nonce = uint64(rng.Intn(1000) + 1)
		a.bal = uint256.NewInt(rng.Uint64())
		var ns int
		switch r := rng.Float64(); {
		case r < 0.5:
			ns = 0
		case r < 0.8:
			ns = rng.Intn(4) + 1
		case r < 0.98:
			ns = rng.Intn(64) + 1
		default:
			ns = rng.Intn(3000) + 1 // the few big contracts
		}
		if ns > 0 {
			a.code = []byte{0x60, byte(i)}
			a.slots = map[common.Hash]common.Hash{}
			for j := 0; j < ns; j++ {
				var s common.Hash
				if rng.Intn(2) == 0 {
					binary.BigEndian.PutUint64(s[24:], uint64(j))
				} else {
					rng.Read(s[:])
				}
				a.slots[s] = randWord(rng)
			}
		}
		m[a.addr] = &a
	}
	return m
}

func flatten(m map[common.Address]*acct) rows {
	var r rows
	for _, a := range m {
		r = append(r, row{acctKey(a.addr), acctVal(a)})
		for s, v := range a.slots {
			r = append(r, row{slotKey(a.addr, s), common.TrimLeftZeroes(v[:])})
		}
	}
	sort.Sort(r)
	return r
}

func refDatabase() state.Database {
	db := rawdb.NewMemoryDatabase()
	return state.NewDatabaseWithNodeDB(db, triedb.NewDatabase(db, triedb.HashDefaults))
}

func refRoot(t *testing.T, sdb state.Database, m map[common.Address]*acct) common.Hash {
	st, err := state.New(types.EmptyRootHash, sdb, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, a := range m {
		st.SetNonce(a.addr, a.nonce)
		st.SetBalance(a.addr, a.bal)
		if len(a.code) > 0 {
			st.SetCode(a.addr, a.code)
		}
		for s, v := range a.slots {
			st.SetState(a.addr, s, v)
		}
	}
	root, err := st.Commit(0, true)
	if err != nil {
		t.Fatal(err)
	}
	return root
}

func rollTemp(t testing.TB, r rows) (common.Hash, *File) {
	path := filepath.Join(t.TempDir(), "nodes")
	root, _, err := Roll(&rowIter{r: r}, path, [32]byte{7})
	if err != nil {
		t.Fatal(err)
	}
	f, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close() })
	if f.Root() != root || f.UserData() != [32]byte{7} {
		t.Fatal("footer disagrees with Roll")
	}
	return root, f
}

func TestRollMatchesStateDB(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	m := genState(rng, 3000)
	want := refRoot(t, refDatabase(), m)
	got, f := rollTemp(t, flatten(m))
	if got != want {
		t.Fatalf("roll root %x, statedb root %x", got, want)
	}
	if f.KeyCount() == 0 || f.NodeCount() == 0 {
		t.Fatal("no stats")
	}
	t.Logf("keys %d nodes %d bytes %d", f.KeyCount(), f.NodeCount(), f.Size())
}

func TestRollEmpty(t *testing.T) {
	root, f := rollTemp(t, nil)
	if root != types.EmptyRootHash || f.Root() != types.EmptyRootHash {
		t.Fatal("empty state root")
	}
	d := NewDirty(f, rows(nil).Seek)
	a := &acct{nonce: 1, bal: uint256.NewInt(5)}
	rng := rand.New(rand.NewSource(2))
	rng.Read(a.addr[:])
	d.Apply(acctKey(a.addr), acctVal(a))
	got, err := d.Root()
	if err != nil {
		t.Fatal(err)
	}
	if want := refRoot(t, refDatabase(), map[common.Address]*acct{a.addr: a}); got != want {
		t.Fatalf("got %x want %x", got, want)
	}
}

// TestDirtyMatchesStateDB rolls a state, then plays rounds of random writes
// into both Dirty and a StateDB continuing from the same root.
func TestDirtyMatchesStateDB(t *testing.T) {
	runDirty(t, 3, []int{0, 1, 2, 3, 4, 5, 6}, 6, 300)
}

// TestDirtyOpKinds isolates each write kind: account fields, delete with
// slots, new account, slot writes, slot clears, delete then recreate.
func TestDirtyOpKinds(t *testing.T) {
	for kind := 0; kind <= 6; kind++ {
		t.Run(fmt.Sprint(kind), func(t *testing.T) { runDirty(t, 100+int64(kind), []int{kind}, 3, 40) })
	}
}

func runDirty(t *testing.T, seed int64, kinds []int, rounds, ops int) {
	rng := rand.New(rand.NewSource(seed))
	m := genState(rng, 3000)
	sdb := refDatabase()
	root := refRoot(t, sdb, m)
	flat := flatten(m)
	got, f := rollTemp(t, flat)
	if got != root {
		t.Fatal("roll")
	}
	d := NewDirty(f, flat.Seek)
	addrs := make([]common.Address, 0, len(m))
	for a := range m {
		addrs = append(addrs, a)
	}
	sort.Slice(addrs, func(i, j int) bool { return bytes.Compare(addrs[i][:], addrs[j][:]) < 0 })

	for round := 0; round < rounds; round++ {
		st, err := state.New(root, sdb, nil)
		if err != nil {
			t.Fatal(err)
		}
		dirty := 0
		for op := 0; op < ops; op++ {
			a := m[addrs[rng.Intn(len(addrs))]]
			if a == nil {
				continue // deleted earlier this round
			}
			switch kinds[rng.Intn(len(kinds))] {
			case 0: // account fields
				a.nonce++
				a.bal = uint256.NewInt(rng.Uint64())
				st.SetNonce(a.addr, a.nonce)
				st.SetBalance(a.addr, a.bal)
				d.Apply(acctKey(a.addr), acctVal(a))
				dirty++
			case 1: // delete, slots included
				st.SelfDestruct(a.addr)
				d.Apply(acctKey(a.addr), nil)
				delete(m, a.addr)
				dirty++
			case 2: // new account, maybe with slots
				n := &acct{nonce: 1, bal: uint256.NewInt(rng.Uint64())}
				rng.Read(n.addr[:])
				st.SetNonce(n.addr, 1)
				st.SetBalance(n.addr, n.bal)
				d.Apply(acctKey(n.addr), acctVal(n))
				dirty++
				if rng.Intn(2) == 0 {
					n.slots = map[common.Hash]common.Hash{}
					for j := 0; j < rng.Intn(5)+1; j++ {
						s, v := randWord(rng), randWord(rng)
						n.slots[s] = v
						st.SetState(n.addr, s, v)
						d.Apply(slotKey(n.addr, s), common.TrimLeftZeroes(v[:]))
						dirty++
					}
				}
				m[n.addr] = n
				addrs = append(addrs, n.addr)
			case 3, 4: // set slots, existing or new
				if a.slots == nil {
					a.slots = map[common.Hash]common.Hash{}
				}
				for j := 0; j < rng.Intn(20)+1; j++ {
					var s common.Hash
					if len(a.slots) > 0 && rng.Intn(2) == 0 {
						for s = range a.slots {
							break
						}
					} else {
						s = randWord(rng)
					}
					v := randWord(rng)
					a.slots[s] = v
					st.SetState(a.addr, s, v)
					d.Apply(slotKey(a.addr, s), common.TrimLeftZeroes(v[:]))
					dirty++
				}
			case 5: // clear slots
				for s := range a.slots {
					if rng.Intn(2) == 0 {
						st.SetState(a.addr, s, common.Hash{})
						d.Apply(slotKey(a.addr, s), nil)
						delete(a.slots, s)
						dirty++
					}
				}
			case 6: // delete then recreate in the same round: storage is wiped
				st.SelfDestruct(a.addr)
				st.Finalise(true)
				d.Apply(acctKey(a.addr), nil)
				a.nonce, a.slots, a.code = 1, nil, nil
				st.SetNonce(a.addr, 1)
				st.SetBalance(a.addr, a.bal)
				d.Apply(acctKey(a.addr), acctVal(a))
				s, v := randWord(rng), randWord(rng)
				a.slots = map[common.Hash]common.Hash{s: v}
				st.SetState(a.addr, s, v)
				d.Apply(slotKey(a.addr, s), common.TrimLeftZeroes(v[:]))
				dirty += 3
			}
		}
		// Drop deleted addresses from the pick list.
		live := addrs[:0]
		for _, ad := range addrs {
			if _, ok := m[ad]; ok {
				live = append(live, ad)
			}
		}
		addrs = live
		want, err := st.Commit(uint64(round+1), true)
		if err != nil {
			t.Fatal(err)
		}
		got, err := d.Root()
		if err != nil {
			t.Fatalf("round %d: %v", round, err)
		}
		if got != want {
			t.Fatalf("round %d: dirty root %x, statedb root %x", round, got, want)
		}
		root = want
		t.Logf("round %d: %d dirty keys, %d retained bytes", round, dirty, d.Bytes())
	}
	// A fresh Dirty over the same file must not depend on retained state.
	d2 := NewDirty(f, flat.Seek)
	if r, _ := d2.Root(); r != f.Root() {
		t.Fatal("no-op root")
	}
}

// TestFileNodes walks every internal node: a child referenced by hash must
// be found at its path with that hash, or be a leaf the overlay rebuilds
// from the flat rows with that same hash.
func TestFileNodes(t *testing.T) {
	rng := rand.New(rand.NewSource(4))
	m := genState(rng, 2000)
	flat := flatten(m)
	_, f := rollTemp(t, flat)
	d := NewDirty(f, flat.Seek)
	var internal, leaves int
	var walk func(owner common.Hash, path []byte, want common.Hash)
	walk = func(owner common.Hash, path []byte, want common.Hash) {
		blob, ok := f.Node(owner, path)
		if !ok {
			leaves++
			blob, err := d.leaf(owner, path)
			if err != nil {
				t.Fatalf("%x/%x: %v", owner, path, err)
			}
			if crypto.Keccak256Hash(blob) != want {
				t.Fatalf("%x/%x: rebuilt leaf hash mismatch", owner, path)
			}
			return
		}
		internal++
		if crypto.Keccak256Hash(blob) != want {
			t.Fatalf("%x/%x: node hash mismatch", owner, path)
		}
		content, _, _ := rlp.SplitList(blob)
		n, _ := rlp.CountValues(content)
		if n == 17 {
			for i := 0; i < 16; i++ {
				kind, item, rest, _ := rlp.Split(content)
				content = rest
				if kind == rlp.String && len(item) == 32 {
					walk(owner, append(append([]byte{}, path...), byte(i)), common.BytesToHash(item))
				}
			}
			return
		}
		key, rest, _ := rlp.SplitString(content)
		kind, item, _, _ := rlp.Split(rest)
		if kind == rlp.String && len(item) == 32 {
			walk(owner, append(append([]byte{}, path...), compactToHex(key)...), common.BytesToHash(item))
		}
	}
	walk(common.Hash{}, nil, f.Root())
	for _, a := range m {
		if len(a.slots) < 2 {
			continue
		}
		h := crypto.Keccak256Hash(a.addr[:])
		root, ok := f.StorageRoot(h)
		if !ok {
			t.Fatalf("%x: no storage root", h)
		}
		walk(h, nil, root)
	}
	if uint64(internal) != f.NodeCount() {
		t.Fatalf("walked %d internal nodes, footer says %d", internal, f.NodeCount())
	}
	t.Logf("%d internal, %d leaves checked", internal, leaves)
}

func TestOpenRefusesCorruptFooter(t *testing.T) {
	rng := rand.New(rand.NewSource(5))
	flat := flatten(genState(rng, 50))
	path := filepath.Join(t.TempDir(), "nodes")
	if _, _, err := Roll(&rowIter{r: flat}, path, [32]byte{}); err != nil {
		t.Fatal(err)
	}
	good, _ := os.ReadFile(path)
	for i, mutate := range []func([]byte) []byte{
		func(b []byte) []byte { b[len(b)-40] ^= 1; return b },  // user data
		func(b []byte) []byte { b[len(b)-100] ^= 1; return b }, // root offset
		func(b []byte) []byte { return b[:len(b)-1] },          // truncated
		func(b []byte) []byte { b[0] ^= 1; return b },          // header magic
	} {
		bad := mutate(append([]byte{}, good...))
		p := fmt.Sprintf("%s.bad%d", path, i)
		os.WriteFile(p, bad, 0o644)
		if f, err := Open(p); err == nil {
			f.Close()
			t.Fatalf("mutation %d: opened", i)
		}
	}
}

// --- benchmarks -------------------------------------------------------------

var bench struct {
	flat rows
	path string
	f    *File
}

// benchState is 4M keys shaped like mainnet C: ten contracts hold 94% of the
// slots (the top one 41%), the rest are small accounts.
func benchState(b *testing.B) {
	if bench.f != nil {
		return
	}
	rng := rand.New(rand.NewSource(9))
	const total = 4_000_000
	shares := []float64{0.41, 0.20, 0.10, 0.07, 0.05, 0.04, 0.03, 0.02, 0.01, 0.01}
	slots := int(float64(total) * 0.95)
	r := make(rows, 0, total)
	var h [32]byte
	addKey := func(k []byte, v []byte) { r = append(r, row{k, v}) }
	slotVal := func() []byte {
		w := randWord(rng)
		return common.TrimLeftZeroes(w[:])
	}
	acctv := func() []byte {
		v, _ := rlp.EncodeToBytes(&accountRow{Nonce: uint64(rng.Intn(100) + 1), Balance: uint256.NewInt(rng.Uint64()), CodeHash: types.EmptyCodeHash.Bytes()})
		return v
	}
	used := 0
	for _, s := range shares {
		rng.Read(h[:])
		addKey(append(append([]byte{}, h[:]...), 0), acctv())
		n := int(float64(slots) * s)
		for i := 0; i < n; i++ {
			var sh [32]byte
			rng.Read(sh[:])
			addKey(append(append(append([]byte{}, h[:]...), 1), sh[:]...), slotVal())
		}
		used += n + 1
	}
	for used < total {
		rng.Read(h[:])
		addKey(append(append([]byte{}, h[:]...), 0), acctv())
		used++
		for i := rng.Intn(3); i > 0 && used < total; i-- {
			var sh [32]byte
			rng.Read(sh[:])
			addKey(append(append(append([]byte{}, h[:]...), 1), sh[:]...), slotVal())
			used++
		}
	}
	sort.Sort(r)
	bench.flat = r
	bench.path = filepath.Join(os.TempDir(), "commit-bench-nodes")
	t0 := time.Now()
	root, st, err := Roll(&rowIter{r: r}, bench.path, [32]byte{})
	if err != nil {
		b.Fatal(err)
	}
	wall := time.Since(t0)
	b.Logf("roll 4M: root %x keys %d nodes %d bytes %d wall %s: %.2fM keccak/s %.2fM nodes/s %.1f MB/s",
		root, st.Keys, st.Nodes, st.Bytes, wall,
		float64(st.Keys+st.Nodes)/wall.Seconds()/1e6, float64(st.Nodes)/wall.Seconds()/1e6, float64(st.Bytes)/wall.Seconds()/1e6)
	bench.f, err = Open(bench.path)
	if err != nil {
		b.Fatal(err)
	}
}

func BenchmarkRoll4M(b *testing.B) {
	benchState(b)
	for i := 0; i < b.N; i++ {
		p := filepath.Join(b.TempDir(), "nodes")
		t0 := time.Now()
		_, st, err := Roll(&rowIter{r: bench.flat}, p, [32]byte{})
		if err != nil {
			b.Fatal(err)
		}
		wall := time.Since(t0)
		b.ReportMetric(float64(st.Keys+st.Nodes)/wall.Seconds(), "keccak/s")
		b.ReportMetric(float64(st.Nodes)/wall.Seconds(), "nodes/s")
		b.ReportMetric(float64(st.Bytes)/1e6, "MB")
		b.ReportMetric(wall.Seconds(), "wall_s")
	}
}

func benchDirty(b *testing.B, workers int) {
	benchState(b)
	rng := rand.New(rand.NewSource(11))
	for i := 0; i < b.N; i++ {
		d := NewDirty(bench.f, bench.flat.Seek)
		if workers > 0 {
			d.Workers = workers
		}
		// Slot rows dominate; picking uniformly over rows follows the
		// contract shares, so the big contracts take most of the updates.
		for j := 0; j < 20_000; j++ {
			r := bench.flat[rng.Intn(len(bench.flat))]
			if len(r.k) == 65 {
				w := randWord(rng)
				d.Apply(r.k, common.TrimLeftZeroes(w[:]))
			} else {
				v, _ := rlp.EncodeToBytes(&accountRow{Nonce: uint64(j), Balance: uint256.NewInt(1), CodeHash: types.EmptyCodeHash.Bytes()})
				d.Apply(r.k, v)
			}
		}
		t0 := time.Now()
		if _, err := d.Root(); err != nil {
			b.Fatal(err)
		}
		b.ReportMetric(float64(time.Since(t0).Milliseconds()), "ms")
		b.ReportMetric(float64(d.Bytes())/20_000, "B/key")
	}
}

func BenchmarkDirty20kSerial(b *testing.B)   { benchDirty(b, 1) }
func BenchmarkDirty20kParallel(b *testing.B) { benchDirty(b, 0) }

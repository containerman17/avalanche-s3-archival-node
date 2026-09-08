package latest

import (
	"bytes"
	"crypto/sha256"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

type entry struct{ k, v []byte }

func sortEntries(es []entry) {
	slices.SortFunc(es, func(a, b entry) int { return bytes.Compare(a.k, b.k) })
}

// genEntries makes n distinct random keys (lengths drawn from klens) with
// random values 0..255 bytes, one in ten empty, sorted.
func genEntries(rng *rand.Rand, n int, klens ...int) []entry {
	seen := map[string]bool{}
	var es []entry
	for len(es) < n {
		k := make([]byte, klens[rng.Intn(len(klens))])
		rng.Read(k)
		if seen[string(k)] {
			continue
		}
		seen[string(k)] = true
		vl := rng.Intn(256)
		if rng.Intn(10) == 0 {
			vl = 0
		}
		v := make([]byte, vl)
		rng.Read(v)
		es = append(es, entry{k, v})
	}
	sortEntries(es)
	return es
}

func writeRun(t testing.TB, path string, es []entry, user [32]byte) *Run {
	t.Helper()
	w, err := NewWriter(path)
	if err != nil {
		t.Fatal(err)
	}
	w.SetUserData(user)
	for _, e := range es {
		if err := w.Add(e.k, e.v); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	r, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func collect(t testing.TB, it Iterator) []entry {
	t.Helper()
	var out []entry
	for it.Next() {
		out = append(out, entry{bytes.Clone(it.Key()), bytes.Clone(it.Value())})
	}
	if err := it.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func sameEntries(t testing.TB, what string, got, want []entry) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: %d entries, want %d", what, len(got), len(want))
	}
	for i := range got {
		if !bytes.Equal(got[i].k, want[i].k) || !bytes.Equal(got[i].v, want[i].v) {
			t.Fatalf("%s: entry %d: %x=%x, want %x=%x", what, i, got[i].k, got[i].v, want[i].k, want[i].v)
		}
	}
}

func TestRoundTrip(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	es := genEntries(rng, 20000, 1, 8, 33, 65, 255)
	var user [32]byte
	rng.Read(user[:])
	r := writeRun(t, filepath.Join(t.TempDir(), "a.run"), es, user)
	defer r.Close()
	if r.Len() != len(es) || r.UserData() != user || r.Bytes() <= 0 {
		t.Fatalf("Len %d UserData %x Bytes %d", r.Len(), r.UserData(), r.Bytes())
	}
	for _, e := range es {
		v, ok := r.Get(e.k)
		if !ok || !bytes.Equal(v, e.v) {
			t.Fatalf("Get %x: %x %v, want %x", e.k, v, ok, e.v)
		}
	}
	for i := 0; i < 20000; i++ {
		k := make([]byte, 1+rng.Intn(70))
		rng.Read(k)
		if _, found := slices.BinarySearchFunc(es, k, func(e entry, k []byte) int { return bytes.Compare(e.k, k) }); found {
			continue
		}
		if v, ok := r.Get(k); ok {
			t.Fatalf("Get absent %x: %x", k, v)
		}
	}
	sameEntries(t, "Iter", collect(t, r.Iter(nil, nil)), es)
	if allocs := testing.AllocsPerRun(100, func() { r.Get(es[rng.Intn(len(es))].k) }); allocs != 0 {
		t.Fatalf("Get allocates %v per call", allocs)
	}
}

func TestWriterRejects(t *testing.T) {
	path := filepath.Join(t.TempDir(), "a.run")
	w, err := NewWriter(path)
	if err != nil {
		t.Fatal(err)
	}
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	reject := func(what string, err error) {
		t.Helper()
		if err == nil {
			t.Fatalf("%s: accepted", what)
		}
	}
	must(w.Add([]byte("b"), []byte("1")))
	reject("duplicate", w.Add([]byte("b"), []byte("2")))
	reject("out of order", w.Add([]byte("a"), []byte("2")))
	reject("empty key", w.Add(nil, []byte("2")))
	reject("256-byte key", w.Add(make([]byte, 256), nil))
	reject("256-byte value", w.Add([]byte("c"), make([]byte, 256)))
	must(w.Add(bytes.Repeat([]byte("c"), 255), make([]byte, 255)))
	must(w.Close())
	r, err := Open(path)
	must(err)
	defer r.Close()
	if r.Len() != 2 {
		t.Fatalf("Len %d", r.Len())
	}

	// An empty run is valid.
	w, err = NewWriter(path)
	must(err)
	must(w.Close())
	r2, err := Open(path)
	must(err)
	defer r2.Close()
	if r2.Len() != 0 || collect(t, r2.Iter(nil, nil)) != nil {
		t.Fatal("empty run not empty")
	}
	if _, ok := r2.Get([]byte("a")); ok {
		t.Fatal("empty run has a key")
	}
}

func TestCorrupt(t *testing.T) {
	rng := rand.New(rand.NewSource(2))
	es := genEntries(rng, 500, 33)
	path := filepath.Join(t.TempDir(), "a.run")
	writeRun(t, path, es, [32]byte{}).Close()
	good, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	try := func(what string, b []byte) {
		t.Helper()
		if err := os.WriteFile(path, b, 0o644); err != nil {
			t.Fatal(err)
		}
		if r, err := Open(path); err == nil {
			r.Close()
			t.Fatalf("%s: opened", what)
		}
	}
	flip := func(off int) []byte {
		b := bytes.Clone(good)
		b[off] ^= 1
		return b
	}
	try("magic", flip(len(good)-footerLen))
	try("version", flip(len(good)-footerLen+8))
	try("entry count", flip(len(good)-footerLen+16))
	try("block count", flip(len(good)-footerLen+24))
	try("index offset", flip(len(good)-footerLen+32))
	try("user data", flip(len(good)-footerLen+40))
	try("checksum", flip(len(good)-1))
	try("index byte", flip(len(good)-footerLen-1))
	try("truncated", good[:len(good)-1])
	try("short", good[:10])
	try("extra byte", append(bytes.Clone(good), 0))
}

func TestIterBounds(t *testing.T) {
	rng := rand.New(rand.NewSource(3))
	es := genEntries(rng, 5000, 4, 33)
	r := writeRun(t, filepath.Join(t.TempDir(), "a.run"), es, [32]byte{})
	defer r.Close()
	o := NewOverlay()
	for _, e := range es {
		o.Put(e.k, e.v)
	}
	live := slices.DeleteFunc(slices.Clone(es), func(e entry) bool { return len(e.v) == 0 })
	v := NewView(o, r)
	bound := func() []byte {
		switch rng.Intn(4) {
		case 0:
			return nil
		case 1:
			return es[rng.Intn(len(es))].k
		}
		k := make([]byte, 1+rng.Intn(40))
		rng.Read(k)
		return k
	}
	for i := 0; i < 300; i++ {
		lo, hi := bound(), bound()
		var want, wantLive []entry
		for _, e := range es {
			if (lo == nil || bytes.Compare(e.k, lo) >= 0) && (hi == nil || bytes.Compare(e.k, hi) < 0) {
				want = append(want, e)
			}
		}
		for _, e := range live {
			if (lo == nil || bytes.Compare(e.k, lo) >= 0) && (hi == nil || bytes.Compare(e.k, hi) < 0) {
				wantLive = append(wantLive, e)
			}
		}
		sameEntries(t, "run", collect(t, r.Iter(lo, hi)), want)
		sameEntries(t, "overlay", collect(t, o.Iter(lo, hi)), want)
		sameEntries(t, "view", collect(t, v.Iter(lo, hi)), wantLive)
	}
}

func TestMerge(t *testing.T) {
	rng := rand.New(rand.NewSource(4))
	dir := t.TempDir()
	old := genEntries(rng, 6000, 33, 65)
	ref := map[string][]byte{}
	for _, e := range old {
		ref[string(e.k)] = e.v
	}
	// The newer run overwrites a third of the old keys and adds new ones.
	var newer []entry
	for _, e := range old {
		if rng.Intn(3) == 0 {
			v := make([]byte, rng.Intn(40))
			rng.Read(v)
			newer = append(newer, entry{e.k, v})
		}
	}
	newer = append(newer, genEntries(rng, 3000, 33, 65)...)
	sortEntries(newer)
	for _, e := range newer {
		ref[string(e.k)] = e.v
	}
	r0 := writeRun(t, filepath.Join(dir, "0.run"), old, [32]byte{})
	r1 := writeRun(t, filepath.Join(dir, "1.run"), newer, [32]byte{})
	defer r0.Close()
	defer r1.Close()

	// The overlay deletes some keys from each run, overwrites some, adds
	// some, and deletes a key that never existed.
	o := NewOverlay()
	for _, es := range [][]entry{old, newer} {
		for _, e := range es {
			switch rng.Intn(6) {
			case 0:
				o.Put(e.k, nil)
				ref[string(e.k)] = nil
			case 1:
				v := []byte("overlay")
				o.Put(e.k, v)
				ref[string(e.k)] = v
			}
		}
	}
	for _, e := range genEntries(rng, 1000, 33, 65) {
		o.Put(e.k, e.v)
		ref[string(e.k)] = e.v
	}
	o.Put([]byte("never existed"), nil)
	ref["never existed"] = nil

	var want []entry
	for k, v := range ref {
		if len(v) > 0 {
			want = append(want, entry{[]byte(k), v})
		}
	}
	sortEntries(want)

	v := NewView(o, r1, r0)
	for k, rv := range ref {
		got, ok := v.Get([]byte(k))
		if ok != (len(rv) > 0) || !bytes.Equal(got, rv) {
			t.Fatalf("View.Get %x: %x %v, want %x", k, got, ok, rv)
		}
	}
	if _, ok := v.Get([]byte("absent")); ok {
		t.Fatal("View.Get absent")
	}
	sameEntries(t, "View.Iter", collect(t, v.Iter(nil, nil)), want)

	user := [32]byte{1, 2, 3}
	m, err := Merge(filepath.Join(dir, "m.run"), v, user)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	if m.Len() != len(want) || m.UserData() != user {
		t.Fatalf("merged Len %d UserData %x, want %d %x", m.Len(), m.UserData(), len(want), user)
	}
	sameEntries(t, "merged", collect(t, m.Iter(nil, nil)), want)
	for k, rv := range ref {
		got, ok := m.Get([]byte(k))
		if ok != (len(rv) > 0) || !bytes.Equal(got, rv) {
			t.Fatalf("merged Get %x: %x %v, want %x", k, got, ok, rv)
		}
	}
	// A view over runs only, no overlay.
	sameEntries(t, "no overlay", collect(t, NewView(nil, m).Iter(nil, nil)), want)
}

func TestOverlay(t *testing.T) {
	o := NewOverlay()
	o.Put([]byte("k"), []byte("v1"))
	o.Put([]byte("k"), []byte("v22"))
	o.Put([]byte("d"), nil)
	if o.Len() != 2 || o.Bytes() != (1+3+entryOverhead)+(1+0+entryOverhead) {
		t.Fatalf("Len %d Bytes %d", o.Len(), o.Bytes())
	}
	if v, ok, dead := o.Get([]byte("k")); !ok || dead || string(v) != "v22" {
		t.Fatalf("Get k: %q %v %v", v, ok, dead)
	}
	if _, ok, dead := o.Get([]byte("d")); !ok || !dead {
		t.Fatalf("Get d: %v %v", ok, dead)
	}
	if _, ok, _ := o.Get([]byte("x")); ok {
		t.Fatal("Get x")
	}
	if allocs := testing.AllocsPerRun(100, func() { o.Get([]byte("k")) }); allocs != 0 {
		t.Fatalf("Get allocates %v per call", allocs)
	}
}

// TestKeyShapes uses the state engine's keys: account = hash + 0x00,
// slot = hash + 0x01 + hash. One contract has enough slots to span many
// blocks; another set shares 60 bytes so the index prefixes tie and the
// full-key search is exercised.
func TestKeyShapes(t *testing.T) {
	rng := rand.New(rand.NewSource(5))
	h := func() []byte {
		var b [32]byte
		rng.Read(b[:])
		s := sha256.Sum256(b[:])
		return s[:]
	}
	var es []entry
	var bigAddr []byte
	for c := 0; c < 300; c++ {
		addr := h()
		es = append(es, entry{append(bytes.Clone(addr), 0), []byte("account")})
		slots := 1 + rng.Intn(20)
		if c == 0 {
			slots, bigAddr = 20000, addr
		}
		for i := 0; i < slots; i++ {
			k := append(append(bytes.Clone(addr), 1), h()...)
			es = append(es, entry{k, k[40:44]})
		}
	}
	common := h()
	for i := 0; i < 5000; i++ {
		k := append(append(append(bytes.Clone(common), 1), common[:27]...), h()[:5]...)
		es = append(es, entry{k, k[62:]})
	}
	sortEntries(es)
	r := writeRun(t, filepath.Join(t.TempDir(), "a.run"), es, [32]byte{})
	defer r.Close()
	for _, e := range es {
		if len(e.k) != 33 && len(e.k) != 65 {
			t.Fatalf("key length %d", len(e.k))
		}
		v, ok := r.Get(e.k)
		if !ok || !bytes.Equal(v, e.v) {
			t.Fatalf("Get %x: %x %v, want %x", e.k, v, ok, e.v)
		}
		// Neighbours that are absent, on both sides.
		for _, d := range []byte{0xff, 0x01} {
			k := bytes.Clone(e.k)
			k[len(k)-1] ^= d
			if _, found := slices.BinarySearchFunc(es, k, func(e entry, k []byte) int { return bytes.Compare(e.k, k) }); !found {
				if _, ok := r.Get(k); ok {
					t.Fatalf("Get absent %x", k)
				}
			}
		}
	}
	sameEntries(t, "Iter", collect(t, r.Iter(nil, nil)), es)
	// A contract's slots are exactly the range [addr+0x01, addr+0x02).
	got := collect(t, r.Iter(append(bytes.Clone(bigAddr), 1), append(bytes.Clone(bigAddr), 2)))
	var want []entry
	for _, e := range es {
		if len(e.k) == 65 && bytes.Equal(e.k[:32], bigAddr) {
			want = append(want, e)
		}
	}
	sameEntries(t, "contract range", got, want)
}

// loadRows reads [klen u8][vlen u8][key][value] rows (exp/statedump's
// output), sorted by key.
func loadRows(b *testing.B) []entry {
	path := os.Getenv("LATEST_ROWS")
	if path == "" {
		b.Skip("LATEST_ROWS not set")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		b.Skip(err)
	}
	var es []entry
	for i := 0; i+2 <= len(raw); {
		kl, vl := int(raw[i]), int(raw[i+1])
		i += 2
		es = append(es, entry{raw[i : i+kl], raw[i+kl : i+kl+vl]})
		i += kl + vl
	}
	sortEntries(es)
	return es
}

// BenchmarkRows builds a run from LATEST_ROWS and reports bytes per key,
// point reads single thread and 16-way, and Merge throughput with every
// 100th key rewritten in the overlay.
func BenchmarkRows(b *testing.B) {
	es := loadRows(b)
	dir := b.TempDir()
	r := writeRun(b, filepath.Join(dir, "a.run"), es, [32]byte{})
	defer r.Close()
	b.Logf("%d entries, %d blocks, %.2f B/key", len(es), r.nblk, float64(r.Bytes())/float64(len(es)))
	rng := rand.New(rand.NewSource(3))
	keys := make([][]byte, 1<<20)
	for i := range keys {
		keys[i] = es[rng.Intn(len(es))].k
	}
	b.Run("Get", func(b *testing.B) {
		b.ReportAllocs()
		var sink int
		for i := 0; i < b.N; i++ {
			v, _ := r.Get(keys[i&(len(keys)-1)])
			sink += len(v)
		}
		_ = sink
	})
	b.Run("GetParallel", func(b *testing.B) {
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			i, sink := rand.Intn(len(keys)), 0
			for pb.Next() {
				v, _ := r.Get(keys[i&(len(keys)-1)])
				sink += len(v)
				i++
			}
			_ = sink
		})
	})
	b.Run("Merge", func(b *testing.B) {
		o := NewOverlay()
		want := 0
		for i, e := range es {
			if i%100 == 0 {
				o.Put(e.k, []byte("rewritten"))
			}
			if i%100 == 0 || len(e.v) > 0 {
				want++
			}
		}
		v := NewView(o, r)
		for i := 0; i < b.N; i++ {
			m, err := Merge(filepath.Join(dir, "m.run"), v, [32]byte{})
			if err != nil {
				b.Fatal(err)
			}
			if m.Len() != want {
				b.Fatalf("merged %d entries, want %d", m.Len(), want)
			}
			m.Close()
		}
		b.ReportMetric(float64(len(es))*float64(b.N)/b.Elapsed().Seconds(), "entries/s")
	})
}

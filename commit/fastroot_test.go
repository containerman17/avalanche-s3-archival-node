package commit

import (
	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/state"
	"github.com/ava-labs/libevm/crypto"
	"github.com/ava-labs/libevm/rlp"
	"github.com/ava-labs/libevm/trie"
	"github.com/holiman/uint256"
)

// bigState is one contract with n slots plus a few plain accounts.
func bigState(rng *rand.Rand, n int) (map[common.Address]*acct, *acct) {
	m := map[common.Address]*acct{}
	big := &acct{nonce: 1, bal: uint256.NewInt(1), code: []byte{0x60, 0x00}, slots: map[common.Hash]common.Hash{}}
	rng.Read(big.addr[:])
	for i := 0; i < n; i++ {
		big.slots[randWord(rng)] = randWord(rng)
	}
	m[big.addr] = big
	for i := 0; i < 20; i++ {
		a := &acct{nonce: uint64(i), bal: uint256.NewInt(rng.Uint64())}
		rng.Read(a.addr[:])
		m[a.addr] = a
	}
	return m, big
}

// storageJob is the job Root builds for one account, on the current Dirty.
func storageJob(t *testing.T, d *Dirty, a *acct) *job {
	t.Helper()
	acc, err := trie.New(trie.TrieID(d.root), d)
	if err != nil {
		t.Fatal(err)
	}
	h := common.BytesToHash(crypto.Keccak256(a.addr[:]))
	p := d.acct[h]
	if p == nil {
		t.Fatal("no pending slots")
	}
	j := &job{hash: h, p: p}
	val, err := acc.Get(h[:])
	if err != nil {
		t.Fatal(err)
	}
	j.cur = new(accountLeaf)
	if err := rlp.DecodeBytes(val, j.cur); err != nil {
		t.Fatal(err)
	}
	j.root = j.cur.Root
	return j
}

// TestFastStorageMatchesTrie: same job through libevm's Trie and through the
// parallel path must give the same root and the same NodeSet, deletions too.
func TestFastStorageMatchesTrie(t *testing.T) {
	for seed := int64(1); seed <= 12; seed++ {
		t.Run(fmt.Sprint(seed), func(t *testing.T) { fastStorageMatchesTrie(t, seed) })
	}
}

func fastStorageMatchesTrie(t *testing.T, seed int64) {
	rng := rand.New(rand.NewSource(seed))
	m, big := bigState(rng, 20_000)
	flat := flatten(m)
	_, f := rollTemp(t, flat)
	d := NewDirty(f, flat.Seek)
	existing := make([]common.Hash, 0, len(big.slots))
	for s := range big.slots {
		existing = append(existing, s)
	}
	sort.Slice(existing, func(i, j int) bool { return bytes.Compare(existing[i][:], existing[j][:]) < 0 })
	for i := 0; i < 3000; i++ {
		switch rng.Intn(4) {
		case 0: // new slot
			d.Apply(slotKey(big.addr, randWord(rng)), common.TrimLeftZeroes(randWord(rng).Bytes()))
		case 1: // clear an existing slot
			d.Apply(slotKey(big.addr, existing[rng.Intn(len(existing))]), nil)
		default: // rewrite an existing slot
			d.Apply(slotKey(big.addr, existing[rng.Intn(len(existing))]), common.TrimLeftZeroes(randWord(rng).Bytes()))
		}
	}
	j := storageJob(t, d, big)

	saved := fastMinSlots
	fastMinSlots = 1 << 30
	wantRoot, wantSet, err := d.storage(j)
	fastMinSlots = saved
	if err != nil {
		t.Fatal(err)
	}
	gotRoot, gotSet, ok, err := d.fastStorage(j)
	if err != nil || !ok {
		t.Fatalf("fast path: ok=%v err=%v", ok, err)
	}
	if gotRoot != wantRoot {
		t.Fatalf("root %x, trie %x", gotRoot, wantRoot)
	}
	for path, n := range wantSet.Nodes {
		g, ok := gotSet.Nodes[path]
		if !ok {
			// libevm re-stores some nodes it resolved but did not change;
			// the fast path skips those. Same blob at the same path.
			if before, _ := d.Node(j.hash, []byte(path), common.Hash{}); !n.IsDeleted() && bytes.Equal(before, n.Blob) {
				continue
			}
			t.Errorf("path %x missing (deleted=%v)", path, n.IsDeleted())
			continue
		}
		if !bytes.Equal(g.Blob, n.Blob) {
			t.Errorf("path %x blob differs", path)
		}
	}
	for path, g := range gotSet.Nodes {
		if _, ok := wantSet.Nodes[path]; !ok {
			// Operation order differs between the two paths (map iteration),
			// so a branch may collapse and re-split here and not there; the
			// re-created node is then identical to the one already in place.
			if before, _ := d.Node(j.hash, []byte(path), common.Hash{}); !g.IsDeleted() && bytes.Equal(before, g.Blob) {
				continue
			}
			t.Errorf("path %x extra (deleted=%v)", path, g.IsDeleted())
		}
	}
	t.Logf("%d nodes in set, %d deleted", len(wantSet.Nodes), func() int {
		n := 0
		for _, x := range wantSet.Nodes {
			if x.IsDeleted() {
				n++
			}
		}
		return n
	}())
}

// TestFastStorageAcrossRounds: the fast path forced on, several rounds of
// slot writes and clears on a big contract, root checked against StateDB and
// the merged nodes reused by the next round.
func TestFastStorageAcrossRounds(t *testing.T) {
	saved := fastMinSlots
	fastMinSlots = 1
	defer func() { fastMinSlots = saved }()
	rng := rand.New(rand.NewSource(9))
	m, big := bigState(rng, 5000)
	sdb := refDatabase()
	root := refRoot(t, sdb, m)
	flat := flatten(m)
	got, f := rollTemp(t, flat)
	if got != root {
		t.Fatal("roll")
	}
	d := NewDirty(f, flat.Seek)
	for round := 0; round < 6; round++ {
		st, err := state.New(root, sdb, nil)
		if err != nil {
			t.Fatal(err)
		}
		existing := make([]common.Hash, 0, len(big.slots))
		for s := range big.slots {
			existing = append(existing, s)
		}
		for i := 0; i < 800; i++ {
			var s common.Hash
			if rng.Intn(3) == 0 {
				s = randWord(rng)
			} else {
				s = existing[rng.Intn(len(existing))]
			}
			v := randWord(rng)
			if rng.Intn(4) == 0 {
				v = common.Hash{}
			}
			st.SetState(big.addr, s, v)
			if v == (common.Hash{}) {
				d.Apply(slotKey(big.addr, s), nil)
				delete(big.slots, s)
			} else {
				d.Apply(slotKey(big.addr, s), common.TrimLeftZeroes(v[:]))
				big.slots[s] = v
			}
		}
		// Touch one plain account too, so the account trie moves.
		for a := range m {
			if a != big.addr {
				m[a].nonce++
				st.SetNonce(a, m[a].nonce)
				d.Apply(acctKey(a), acctVal(m[a]))
				break
			}
		}
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
	}
}

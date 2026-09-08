package vmexec

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"math/big"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/rawdb"
	ethstate "github.com/ava-labs/libevm/core/state"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/rlp"
	"github.com/ava-labs/libevm/triedb"
	"github.com/holiman/uint256"

	"github.com/containerman17/avalanche-s3-archival-node/chain"
	"github.com/containerman17/avalanche-s3-archival-node/dist"
	"github.com/containerman17/avalanche-s3-archival-node/fetch"
	"github.com/containerman17/avalanche-s3-archival-node/store"
)

// refState is geth's own trie over a memory db, seeded from alloc.
func refState(t *testing.T, alloc types.GenesisAlloc) (ethstate.Database, *triedb.Database, common.Hash) {
	t.Helper()
	memdb := rawdb.NewMemoryDatabase()
	tdb := triedb.NewDatabase(memdb, triedb.HashDefaults)
	refDB := ethstate.NewDatabaseWithNodeDB(memdb, tdb)
	ref, err := ethstate.New(types.EmptyRootHash, refDB, nil)
	if err != nil {
		t.Fatal(err)
	}
	for addr, a := range alloc {
		ref.SetBalance(addr, uint256.MustFromBig(a.Balance))
		ref.SetNonce(addr, a.Nonce)
		if len(a.Code) > 0 {
			ref.SetCode(addr, a.Code)
		}
		for k, v := range a.Storage {
			ref.SetState(addr, k, v)
		}
	}
	root0, err := ref.Commit(0, false)
	if err != nil {
		t.Fatal(err)
	}
	tdb.Commit(root0, false)
	return refDB, tdb, root0
}

// TestRecoverFromRows executes random blocks through the engine and the store
// (rows captured per tx exactly as the executor captures them), rolls, then
// reopens the rolled state and rebuilds the rest from the rows: after a clean
// close, and after a close with a roll in flight (the torn roll is swept).
// Every root is checked against geth's trie.
func TestRecoverFromRows(t *testing.T) {
	fetch.RegisterExtras(chain.SubnetEVM)
	rng := rand.New(rand.NewSource(2))
	addrs := make([]common.Address, 12)
	for i := range addrs {
		rng.Read(addrs[i][:])
	}
	alloc := types.GenesisAlloc{
		addrs[0]: {Balance: big.NewInt(1e18)},
		addrs[1]: {Balance: big.NewInt(5), Nonce: 3, Code: []byte{0x60, 0x00}, Storage: map[common.Hash]common.Hash{{1}: {7}, {2}: {9}}},
		addrs[2]: {Balance: big.NewInt(0)},
	}
	refDB, tdb, root0 := refState(t, alloc)
	dir := t.TempDir()
	vmdir := filepath.Join(dir, "vmstate")
	cas, err := dist.Local(dir)
	if err != nil {
		t.Fatal(err)
	}
	db, err := store.Open(dir, cas, [32]byte{1})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	eng, err := newEngine(vmdir, alloc, root0)
	if err != nil {
		t.Fatal(err)
	}
	eng.syncStore = db.Sync
	code := refDB // code lives in the reference's memory db, as the other test has it

	roots := map[uint64]common.Hash{0: root0}
	var txSeq uint64
	// block executes one random block against ref and eng, writes it to the
	// store and checks the engine's root.
	block := func(eng *engine, blk uint64) {
		t.Helper()
		parent := roots[blk-1]
		ref, err := ethstate.New(parent, refDB, nil)
		if err != nil {
			t.Fatal(err)
		}
		flat := &flatDB{code: code, eng: eng}
		wrap := wrapDatabase(flat)
		cap := &capture{code: map[string][]byte{}}
		wrap.setCapture(cap)
		flat.begin(parent)
		st, err := ethstate.New(parent, wrap, nil)
		if err != nil {
			t.Fatal(err)
		}
		bw := &store.BlockWrite{Height: blk, Pvm: make([]byte, 8), Code: map[string][]byte{}}
		both := func(f func(s *ethstate.StateDB)) { f(ref); f(st) }
		for i := 0; i < 8; i++ {
			a := addrs[rng.Intn(len(addrs))]
			switch rng.Intn(6) {
			case 0:
				v := uint256.NewInt(uint64(rng.Intn(1000)))
				both(func(s *ethstate.StateDB) { s.AddBalance(a, v) })
			case 1, 2:
				k := common.Hash{byte(rng.Intn(4))}
				v := common.Hash{byte(rng.Intn(3))}
				both(func(s *ethstate.StateDB) { s.SetState(a, k, v) })
			case 3:
				both(func(s *ethstate.StateDB) { s.SetNonce(a, s.GetNonce(a)+1) })
			case 4:
				if rng.Intn(4) == 0 {
					both(func(s *ethstate.StateDB) { s.SelfDestruct(a) })
				}
			case 5:
				c := []byte{0x60, byte(rng.Intn(256))}
				both(func(s *ethstate.StateDB) {
					if len(s.GetCode(a)) == 0 {
						s.SetCode(a, c)
					}
				})
			}
			both(func(s *ethstate.StateDB) { s.IntermediateRoot(true) })
			txSeq++
			h := sha256.Sum256([]byte(fmt.Sprint(txSeq)))
			bw.Txs = append(bw.Txs, store.TxWrite{Hash: h[:], RLP: []byte("tx"), Receipt: []byte("r"), State: cap.take()})
		}
		want, err := ref.Commit(blk, true)
		if err != nil {
			t.Fatal(err)
		}
		tdb.Commit(want, false)
		if _, err := st.Commit(blk, true); err != nil {
			t.Fatal(err)
		}
		bw.Tail = cap.take()
		if bw.HeaderRLP, err = rlp.EncodeToBytes(&types.Header{Number: new(big.Int).SetUint64(blk), Root: want, Time: blk * 2}); err != nil {
			t.Fatal(err)
		}
		ws := flat.take()
		eng.applyOverlay(ws)
		if err := eng.applyDirty(ws); err != nil {
			t.Fatal(err)
		}
		got, err := eng.root()
		if err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("block %d: root %x, want %x", blk, got, want)
		}
		if err := db.WriteBlock(bw); err != nil {
			t.Fatal(err)
		}
		roots[blk] = want
	}
	roll := func(eng *engine, blk uint64, finish bool) {
		t.Helper()
		eng.maybeRoll(1, blk, roots[blk])
		r := <-eng.rollDone
		eng.rollDone <- r
		if finish {
			if err := eng.finishRoll(); err != nil {
				t.Fatal(err)
			}
		}
	}
	reopen := func(head uint64) *engine {
		t.Helper()
		eng, m, err := openEngine(vmdir)
		if err != nil {
			t.Fatal(err)
		}
		e := &Executor{cfg: Config{Store: db, CAS: cas}, eng: eng}
		if err := e.recover(m, root0, head); err != nil {
			t.Fatal(err)
		}
		if e.headNum != head || e.headRoot != roots[head] {
			t.Fatalf("recovered to %d root %x, want %d root %x", e.headNum, e.headRoot, head, roots[head])
		}
		return eng
	}

	for blk := uint64(1); blk <= 20; blk++ {
		block(eng, blk)
	}
	roll(eng, 20, true)
	for blk := uint64(21); blk <= 40; blk++ {
		block(eng, blk)
	}
	eng.close()
	eng = reopen(40)
	if m, _ := readManifest(vmdir); m.Gen != 1 || m.Height != 20 {
		t.Fatalf("manifest %+v, want gen 1 at 20", m)
	}
	eng.syncStore = db.Sync
	for blk := uint64(41); blk <= 50; blk++ {
		block(eng, blk)
	}
	roll(eng, 50, true)
	for blk := uint64(51); blk <= 55; blk++ {
		block(eng, blk)
	}
	roll(eng, 55, false) // in flight at close: never named, swept on open
	eng.close()
	r := <-eng.rollDone
	r.run.Close()
	r.file.Close()
	eng.runs[0].Close()
	eng.file.Close()
	eng = reopen(55)
	if m, _ := readManifest(vmdir); m.Gen != 2 || m.Height != 50 {
		t.Fatalf("manifest %+v, want gen 2 at 50", m)
	}
	if _, err := os.Stat(filepath.Join(vmdir, "run.3")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the abandoned roll's run survived: %v", err)
	}
	eng.syncStore = db.Sync
	for blk := uint64(56); blk <= 60; blk++ {
		block(eng, blk)
		if blk == 58 {
			roll(eng, 58, true)
		}
	}
	if m, _ := readManifest(vmdir); m.Gen != 3 || m.Height != 58 || m.Root != roots[58] {
		t.Fatalf("manifest %+v, want gen 3 at 58", m)
	}
	eng.close()
	eng = reopen(60)
	eng.close()
}

// TestManifestAtomic: a torn manifest temp and a file no manifest names are
// swept on open; a torn manifest itself is refused, not mistaken for none.
func TestManifestAtomic(t *testing.T) {
	alloc := types.GenesisAlloc{{1}: {Balance: big.NewInt(1)}}
	_, _, root0 := refState(t, alloc)
	dir := t.TempDir()
	eng, err := newEngine(dir, alloc, root0)
	if err != nil {
		t.Fatal(err)
	}
	eng.close()
	for _, n := range []string{"MANIFEST.tmp", "trie.7", "run.1.idx"} {
		if err := os.WriteFile(filepath.Join(dir, n), []byte("torn"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	eng, m, err := openEngine(dir)
	if err != nil {
		t.Fatal(err)
	}
	eng.close()
	if m.Gen != 0 || m.Height != 0 || m.Root != root0 {
		t.Fatalf("manifest %+v", m)
	}
	entries, _ := os.ReadDir(dir)
	if len(entries) != 3 {
		t.Fatalf("dir holds %d files after the sweep, want MANIFEST, run.0, trie.0", len(entries))
	}
	if err := os.WriteFile(filepath.Join(dir, manifestName), []byte("{"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, err := openEngine(dir); err == nil || errors.Is(err, errNoManifest) {
		t.Fatalf("torn manifest: err = %v", err)
	}
	os.Remove(filepath.Join(dir, manifestName))
	if _, _, err := openEngine(dir); !errors.Is(err, errNoManifest) {
		t.Fatalf("no manifest: err = %v", err)
	}
}

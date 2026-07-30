package exec

import (
	"encoding/binary"
	"math/big"
	"strings"
	"testing"

	"github.com/ava-labs/libevm/common"
	ethstate "github.com/ava-labs/libevm/core/state"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/libevm/stateconf"
	"github.com/ava-labs/libevm/rlp"
	"github.com/holiman/uint256"

	"github.com/containerman17/epochdb/dist"
	"github.com/containerman17/epochdb/fetch"
	"github.com/containerman17/epochdb/state"
)

var (
	frA = common.HexToAddress("0xaa11111111111111111111111111111111111111")
	frB = common.HexToAddress("0xbb22222222222222222222222222222222222222")
	frC = common.HexToAddress("0xcc33333333333333333333333333333333333333")
	frD = common.HexToAddress("0xdd44444444444444444444444444444444444444")

	frS1 = common.HexToHash("0x01")
	frS2 = common.HexToHash("0x02")
)

// frontierBlocks is the replay script. It crosses every rule the merge has
// to get right, and the epoch cut at block 4 puts each pair on OPPOSITE
// sides of a file boundary:
//
//	1 A,B,D created with storage       5 A.slot1 overwritten (newest wins across epochs)
//	2 B.slot2 written                  6 B SELFDESTRUCTed (kills slot1 and slot2)
//	3 C created                        7 B recreated with slot2 only
//	4 D.slot2 written                  8 C deleted, D.slot2 zeroed
//
// Block 8 also SELFDESTRUCTs a GENESIS ALLOC account, the one case where a
// tombstone has to reach Firewood at all (everything else the merge writes IS
// the trie's content, so a dead key is simply not written).
func frontierBlocks(n uint64, alloc common.Address, sdb *ethstate.StateDB) {
	set := func(a common.Address, nonce uint64, bal int64) {
		sdb.SetNonce(a, nonce)
		sdb.SetBalance(a, uint256.NewInt(uint64(bal)))
	}
	switch n {
	case 1:
		set(frA, 1, 100)
		sdb.SetState(frA, frS1, common.HexToHash("0x11"))
		set(frB, 1, 50)
		sdb.SetState(frB, frS1, common.HexToHash("0x21"))
		set(frD, 1, 5)
		sdb.SetState(frD, frS1, common.HexToHash("0x41"))
	case 2:
		sdb.SetState(frB, frS2, common.HexToHash("0x22"))
	case 3:
		set(frC, 9, 9)
		sdb.SetCode(frC, []byte{0x60, 0x0a, 0x60, 0x0b})
	case 4:
		sdb.SetState(frD, frS2, common.HexToHash("0x42"))
	case 5:
		sdb.SetState(frA, frS1, common.HexToHash("0x99"))
	case 6:
		sdb.SelfDestruct(frB)
	case 7:
		set(frB, 3, 7)
		sdb.SetState(frB, frS2, common.HexToHash("0x77"))
	case 8:
		sdb.SelfDestruct(frC)
		sdb.SetState(frD, frS2, common.Hash{})
		sdb.SelfDestruct(alloc)
	}
}

// allocAddr is the lowest genesis-alloc address, the one the script
// SELFDESTRUCTs.
func allocAddr(e *Executor) common.Address {
	var out common.Address
	for a := range e.alloc {
		if out == (common.Address{}) || a.Cmp(out) < 0 {
			out = a
		}
	}
	return out
}

// frameRows decodes one captured write frame into epoch SST rows. It is the
// same layout state's cook reads (kind | addr 20 | [slot 32] | uvarint vlen |
// value), so the rows sealed here are the executor's post-images verbatim.
func frameRows(t *testing.T, buf []byte, block uint64) []state.StateRow {
	t.Helper()
	var out []state.StateRow
	for pos := 0; pos < len(buf); {
		kind := buf[pos]
		var r state.StateRow
		r.Key[0] = kind
		p := pos + 21 // account: kind | addr
		if kind != 'a' {
			p = pos + 53 // storage: + slot; code-use: + code hash
		}
		copy(r.Key[1:], buf[pos+1:p])
		vlen, vn := binary.Uvarint(buf[p:])
		p += vn
		r.Block, r.Seq = block, len(out)
		if vlen > 0 {
			r.Value = append([]byte(nil), buf[p:p+int(vlen)]...)
		}
		if kind != 'c' { // code-use records are reads; cook drops them too
			out = append(out, r)
		}
		pos = p + int(vlen)
	}
	return out
}

// replayCorpus executes frontierBlocks 1..8 through the real capture +
// Firewood path (statedb writes, per-block commit) and returns the true
// post-execution root of every block plus the captured rows.
func replayCorpus(t *testing.T, dir string) (roots map[uint64]common.Hash, rows map[uint64][]state.StateRow) {
	t.Helper()
	store, err := state.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	e, err := New(Config{DataDir: dir, Blocks: fakeSource{}, Store: store})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	roots, rows = map[uint64]common.Hash{}, map[uint64][]state.StateRow{}
	parentRoot, parentHash := e.genesisRoot, e.genesisHash
	for n := uint64(1); n <= 8; n++ {
		frame := &blockFrame{}
		e.wrapDB.setFrame(frame)
		sdb, err := ethstate.New(parentRoot, e.wrapDB, nil)
		if err != nil {
			t.Fatal(err)
		}
		frontierBlocks(n, allocAddr(e), sdb)
		blkHash := common.BigToHash(big.NewInt(int64(n) + 1000))
		root, err := sdb.Commit(n, true, stateconf.WithTrieDBUpdateOpts(
			stateconf.WithTrieDBUpdatePayload(parentHash, blkHash)))
		if err != nil {
			t.Fatalf("block %d commit: %v", n, err)
		}
		if err := e.triedb.Commit(root, false); err != nil {
			t.Fatalf("block %d triedb commit: %v", n, err)
		}
		e.wrapDB.setFrame(nil)
		roots[n], rows[n] = root, frameRows(t, frame.buf, n)
		parentRoot, parentHash = root, blkHash
	}
	return roots, rows
}

// sealCorpus publishes the replayed rows as two epochs (blocks 1..4 and
// 5..8) into a FRESH data dir that holds nothing else: exactly what a
// downloaded node has.
func sealCorpus(t *testing.T, dir string, roots map[uint64]common.Hash, rows map[uint64][]state.StateRow) *state.EpochSet {
	t.Helper()
	st, err := dist.Local(dir)
	if err != nil {
		t.Fatal(err)
	}
	build := func(start uint64) {
		in := &state.EpochInput{
			Start:    start,
			TxHashes: map[uint64][][32]byte{},
			// A code blob, so the SST carries real 'c' rows for the merge to
			// skip (Firewood holds accounts and storage only).
			Code: map[common.Hash][]byte{
				common.BigToHash(big.NewInt(int64(start))): {0xde, 0xad, byte(start)},
			},
		}
		for n := start; n < start+4; n++ {
			in.Containers = append(in.Containers, []byte(strings.Repeat("container", 20)))
			hdr, err := rlp.EncodeToBytes(&types.Header{
				Number:     new(big.Int).SetUint64(n),
				Difficulty: big.NewInt(1),
				Root:       roots[n],
			})
			if err != nil {
				t.Fatal(err)
			}
			in.Headers = append(in.Headers, hdr)
			in.StateRows = append(in.StateRows, rows[n]...)
		}
		if _, err := state.BuildEpoch(st, in); err != nil {
			t.Fatal(err)
		}
	}
	build(1)
	build(5)
	set, err := state.OpenEpochSet(st)
	if err != nil {
		t.Fatal(err)
	}
	if len(set.Epochs) != 2 {
		t.Fatalf("want 2 epochs, got %d", len(set.Epochs))
	}
	return set
}

// TestBuildFrontierMatchesReplayRoot is THE test: a corpus is replayed with
// the real capture + Firewood path to get the true root at H, published as
// two epochs, and then a node that has nothing but those epochs merges them
// into its own Firewood. The merged frontier must hash to the same root, and
// the node must then restart at H+1 as if it had replayed.
func TestBuildFrontierMatchesReplayRoot(t *testing.T) {
	fetch.RegisterExtras()
	roots, rows := replayCorpus(t, t.TempDir())

	dir := t.TempDir()
	set := sealCorpus(t, dir, roots, rows)
	defer set.Close()

	store, err := state.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	e, err := New(Config{DataDir: dir, Blocks: set, Store: store})
	if err != nil {
		t.Fatal(err)
	}
	if err := e.BuildFrontier(set); err != nil {
		t.Fatalf("build frontier: %v", err) // the root check lives inside
	}
	if got := common.Hash(e.fwBackend.Firewood.Root()); got != roots[8] {
		t.Fatalf("merged frontier root %x, replayed root %x", got, roots[8])
	}
	if e.Head() != 8 || e.headRoot != roots[8] {
		t.Fatalf("head after build: %d root %x", e.Head(), e.headRoot)
	}
	if n, ok := store.ExecHead(); !ok || n != 8 {
		t.Fatalf("exechead after build: %d %v", n, ok)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	// Restart: the node must reconcile onto the merged frontier out of the
	// headers the build copied down from the epochs, exactly like a replaying
	// node reconciles onto its own.
	store2, err := state.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store2.Close()
	e2, err := New(Config{DataDir: dir, Blocks: set, Store: store2})
	if err != nil {
		t.Fatalf("restart on a merged frontier: %v", err)
	}
	defer e2.Close()
	if e2.Head() != 8 || e2.headRoot != roots[8] {
		t.Fatalf("restart head %d root %x, want 8 %x", e2.Head(), e2.headRoot, roots[8])
	}
	// Idempotent: a second build over an already-frontiered dir is a no-op.
	if err := e2.BuildFrontier(set); err != nil {
		t.Fatalf("rebuild: %v", err)
	}
}

// TestBuildFrontierRefusesSAE: above the Helicon boundary header.Root is the
// SETTLED block's root, so there is nothing to check the merge against. The
// build must say so instead of reporting a root mismatch.
func TestBuildFrontierRefusesSAE(t *testing.T) {
	fetch.RegisterExtras()
	roots, rows := replayCorpus(t, t.TempDir())
	dir := t.TempDir()
	set := sealCorpus(t, dir, roots, rows)
	defer set.Close()

	prev := HasSettledMarkers
	HasSettledMarkers = func(*types.Header) bool { return true }
	defer func() { HasSettledMarkers = prev }()

	store, err := state.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	e, err := New(Config{DataDir: dir, Blocks: set, Store: store})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	err = e.BuildFrontier(set)
	if err == nil || !strings.Contains(err.Error(), "post-Helicon") {
		t.Fatalf("SAE corpus: %v, want a post-Helicon refusal", err)
	}
}

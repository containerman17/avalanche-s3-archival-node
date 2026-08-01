package exec

import (
	"encoding/binary"
	"math/big"
	"strings"
	"testing"

	ccustomtypes "github.com/ava-labs/avalanchego/graft/coreth/plugin/evm/customtypes"
	"github.com/ava-labs/libevm/common"
	ethstate "github.com/ava-labs/libevm/core/state"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/libevm/stateconf"
	"github.com/ava-labs/libevm/rlp"
	"github.com/holiman/uint256"

	"github.com/containerman17/epochdb/chain"
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

// saeMarkers turns header n into an SAE header settling height s: the four
// ACP-194 markers, with header.Root the SETTLED block's post-execution root
// instead of its own. The gas-clock markers are arbitrary but self-consistent
// (nothing here executes; the build only reconstructs the clock from them).
//
// This is what a post-Helicon header the node fetched actually looks like on
// the wire: the fields are RLP-optional tail fields on the coreth header, so
// they survive the epoch's header section byte for byte.
func saeMarkers(hdr *types.Header, s uint64, settledRoot common.Hash) *types.Header {
	unix, num, excess := uint64(1785250800+s), uint64(0), uint64(1_000_000)
	hdr.Root = settledRoot
	// The settlement markers are RLP-OPTIONAL TAIL fields: setting them forces
	// every optional field before them to be encoded too, and a nil
	// ParentBeaconRoot round-trips as an empty string that will not decode
	// into a common.Hash. Every real post-Helicon header carries one.
	hdr.ParentBeaconRoot = new(common.Hash)
	return ccustomtypes.WithHeaderExtra(hdr, &ccustomtypes.HeaderExtra{
		SettledHeight:       &s,
		SettledGasUnix:      &unix,
		SettledGasNumerator: &num,
		SettledExcess:       &excess,
	})
}

// sealCorpus publishes the replayed rows as two epochs (blocks 1..4 and
// 5..8) into a FRESH data dir that holds nothing else: exactly what a
// downloaded node has. mark, if set, rewrites a header before it is encoded,
// which is how the SAE cases get real settlement markers into the epochs.
func sealCorpus(t *testing.T, dir string, roots map[uint64]common.Hash, rows map[uint64][]state.StateRow, mark ...func(n uint64, hdr *types.Header)) *state.EpochSet {
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
			h := &types.Header{
				Number:     new(big.Int).SetUint64(n),
				Difficulty: big.NewInt(1),
				Root:       roots[n],
			}
			for _, m := range mark {
				m(n, h)
			}
			hdr, err := rlp.EncodeToBytes(h)
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
	if len(set.All()) != 2 {
		t.Fatalf("want 2 epochs, got %d", len(set.All()))
	}
	return set
}

// TestBuildFrontierMatchesReplayRoot is THE test: a corpus is replayed with
// the real capture + Firewood path to get the true root at H, published as
// two epochs, and then a node that has nothing but those epochs merges them
// into its own Firewood. The merged frontier must hash to the same root, and
// the node must then restart at H+1 as if it had replayed.
func TestBuildFrontierMatchesReplayRoot(t *testing.T) {
	fetch.RegisterExtras(chain.Coreth)
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

// buildOn runs a frontier build over a corpus sealed with mark applied to
// every header, and returns the executor (still open) for inspection.
func buildOn(t *testing.T, mark func(roots map[uint64]common.Hash, n uint64, hdr *types.Header)) (*Executor, *state.Store, *state.EpochSet, map[uint64]common.Hash, error) {
	t.Helper()
	fetch.RegisterExtras(chain.Coreth)
	roots, rows := replayCorpus(t, t.TempDir())
	dir := t.TempDir()
	set := sealCorpus(t, dir, roots, rows, func(n uint64, hdr *types.Header) { mark(roots, n, hdr) })
	t.Cleanup(set.Close)

	store, err := state.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	e, err := New(Config{DataDir: dir, Blocks: set, Store: store})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { e.Close() })
	return e, store, set, roots, e.BuildFrontier(set)
}

// TestBuildFrontierSAESettledRoot is the post-Helicon half of the root check
// (RULING 2026-08-01). header(8) is an SAE header: its Root is NOT the state
// at 8, it is the post-execution root of the height it settles. So the merge
// must land on that settled height and reproduce exactly that root, and the
// node must park there, a settlement lag below the sealed end.
//
// lag 1 (settles 7, itself an SAE height, so the frontier's gas clock has to
// be seeded into the ring out of the attesting header's markers) and lag 3
// (settles 5) are the same rule at both distances.
func TestBuildFrontierSAESettledRoot(t *testing.T) {
	for _, tc := range []struct {
		name    string
		settled uint64
	}{{"lag1", 7}, {"lag3", 5}} {
		t.Run(tc.name, func(t *testing.T) {
			e, store, _, roots, err := buildOn(t, func(roots map[uint64]common.Hash, n uint64, hdr *types.Header) {
				// Blocks 5..8 are SAE, each settling as far back as the case
				// wants and never further: monotonic, and 8 (the sealed end)
				// is the header that attests the frontier.
				if n < 5 {
					return
				}
				s := min(n-1, tc.settled)
				saeMarkers(hdr, s, roots[s])
			})
			if err != nil {
				t.Fatalf("build frontier: %v", err)
			}
			if got := common.Hash(e.fwBackend.Firewood.Root()); got != roots[tc.settled] {
				t.Fatalf("merged frontier root %x, want the root at the settled height %d (%x)", got, tc.settled, roots[tc.settled])
			}
			if e.Head() != tc.settled || e.headRoot != roots[tc.settled] {
				t.Fatalf("head after build: %d root %x, want %d", e.Head(), e.headRoot, tc.settled)
			}
			if n, ok := store.ExecHead(); !ok || n != tc.settled {
				t.Fatalf("exechead after build: %d %v, want %d", n, ok, tc.settled)
			}
			if f, ok := store.FrontierFloor(); !ok || f != tc.settled {
				t.Fatalf("frontier floor: %d %v, want %d", f, ok, tc.settled)
			}
			// The frontier height is post-Helicon here, so its root and gas
			// clock must be in the ring: without them the executor cannot
			// take a single step at settled+1, and the restart walk-back
			// cannot identify Firewood's root at all.
			root, clock, ok := e.ring.get(tc.settled)
			if !ok || root != roots[tc.settled] || len(clock) == 0 {
				t.Fatalf("ring at %d: root %x clock %d bytes ok=%v", tc.settled, root, len(clock), ok)
			}
			// Nothing above the frontier reaches the state layer: the
			// walk-back would demand containers this node never fetched.
			if n, ok := store.HeadersMax(); !ok || n != tc.settled {
				t.Fatalf("headers max %d %v, want %d", n, ok, tc.settled)
			}
		})
	}
}

// TestBuildFrontierSAEAtBoundary: the sealed end is the FIRST SAE block, so
// the height it settles is the transition block, which is synchronous. The
// frontier lands on a pre-Helicon height whose own header carries its root and
// seeds its gas clock, so nothing goes into the ring.
func TestBuildFrontierSAEAtBoundary(t *testing.T) {
	e, store, _, roots, err := buildOn(t, func(roots map[uint64]common.Hash, n uint64, hdr *types.Header) {
		if n == 8 {
			saeMarkers(hdr, 7, roots[7])
		}
	})
	if err != nil {
		t.Fatalf("build frontier: %v", err)
	}
	if got := common.Hash(e.fwBackend.Firewood.Root()); got != roots[7] || e.Head() != 7 {
		t.Fatalf("frontier at %d root %x, want 7 %x", e.Head(), got, roots[7])
	}
	if _, _, ok := e.ring.get(7); ok {
		t.Fatal("ring seeded at a synchronous height: its own header carries root and clock")
	}
	if f, ok := store.FrontierFloor(); !ok || f != 7 {
		t.Fatalf("frontier floor: %d %v, want 7", f, ok)
	}
}

// TestBuildFrontierSAERefusesWrongRoot: the whole point is that the check is
// cryptographic, so a corpus whose rows do not hash to what the attesting
// header settles must be refused, exactly like a pre-SAE mismatch.
func TestBuildFrontierSAERefusesWrongRoot(t *testing.T) {
	_, _, _, _, err := buildOn(t, func(roots map[uint64]common.Hash, n uint64, hdr *types.Header) {
		if n == 8 {
			// Settles 5, but presents the root of 6: the merge at 5 cannot
			// produce it.
			saeMarkers(hdr, 5, roots[6])
		}
	})
	if err == nil || !strings.Contains(err.Error(), "commits to") {
		t.Fatalf("corrupted settled root: %v, want a merged-root refusal", err)
	}
}

// TestBuildFrontierSAERefusesSelfSettle: a header that settles itself or the
// future is not a commitment to anything this node can merge to.
func TestBuildFrontierSAERefusesSelfSettle(t *testing.T) {
	_, _, _, _, err := buildOn(t, func(roots map[uint64]common.Hash, n uint64, hdr *types.Header) {
		if n == 8 {
			saeMarkers(hdr, 8, roots[8])
		}
	})
	if err == nil || !strings.Contains(err.Error(), "cannot settle itself") {
		t.Fatalf("self-settling header: %v, want a refusal", err)
	}
}

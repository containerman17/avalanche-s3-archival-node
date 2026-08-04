package exec

import (
	"bytes"
	"encoding/binary"
	"math/big"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	ccustomtypes "github.com/ava-labs/avalanchego/graft/coreth/plugin/evm/customtypes"
	ffi "github.com/ava-labs/firewood-go-ethhash/ffi"
	"github.com/ava-labs/libevm/common"
	ethstate "github.com/ava-labs/libevm/core/state"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/crypto"
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

	// frDCode is the contract code of frD, deployed in block 1 and never
	// removed, so it is still live at the frontier and past it.
	frDCode = []byte{0x60, 0x2a, 0x60, 0x00, 0x52}
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
		// D is a CONTRACT and it outlives the frontier, which is what the
		// block-9 execution below needs: its code is deployed inside epoch 1
		// and the only copy a joined node has is that epoch's 'c' row.
		sdb.SetCode(frD, frDCode)
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
		// The v3 placement rule: the epoch that writes an account carries that
		// account's code. frD is deployed in block 1, so epoch 1 is where its
		// blob lives, and a joined node executing past block 8 has to descend
		// to it (state/store.go Code).
		if start == 1 {
			in.Code[crypto.Keccak256Hash(frDCode)] = frDCode
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

// TestExecuteAfterFrontierResolvesCodeFromEpochs IS THE TOKYO CRASH
// (2026-08-04, first real join-from-bucket node): the frontier merged and
// root-verified, and then the very first block past it died with "can't load
// code hash ...: not found". A joined node replayed nothing, so its code.log
// is EMPTY, and the epochs' 'c' rows are the only contract code it has; the
// executor's statedb was reading code.log alone.
//
// The shape is exactly Tokyo's: a dir holding nothing but epochs, a frontier
// merged out of them, and a block that touches a contract deployed long
// before the frontier.
func TestExecuteAfterFrontierResolvesCodeFromEpochs(t *testing.T) {
	fetch.RegisterExtras(chain.Coreth)
	roots, rows := replayCorpus(t, t.TempDir())

	dir := t.TempDir()
	set := sealCorpus(t, dir, roots, rows)
	defer set.Close()
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
	if err := e.BuildFrontier(set); err != nil {
		t.Fatal(err)
	}
	// A joined node's code.log holds the genesis alloc's code and NOTHING
	// else: it never executed a deploy, so frD's blob is in the epoch or
	// nowhere.
	frDHash := crypto.Keccak256Hash(frDCode)
	raw, err := os.ReadFile(filepath.Join(dir, "code.log"))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(raw, frDHash[:]) {
		t.Fatal("fixture is not a joined node: frD's code is in code.log")
	}

	// Block 9 against the merged frontier, the block Tokyo died on.
	frame := &blockFrame{}
	e.wrapDB.setFrame(frame)
	defer e.wrapDB.setFrame(nil)
	sdb, err := ethstate.New(e.headRoot, e.wrapDB, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := sdb.GetCode(frD); !bytes.Equal(got, frDCode) {
		t.Fatalf("code of a contract deployed below the frontier: %x, want %x", got, frDCode)
	}
	if err := sdb.Error(); err != nil {
		t.Fatalf("statedb error after reading epoch-only code: %v", err)
	}
	// And the commit must go through: the crash surfaced at the drain as
	// "commit aborted due to earlier error", the swallowed read above.
	sdb.SetNonce(frD, 2)
	if _, err := sdb.Commit(9, true, stateconf.WithTrieDBUpdateOpts(
		stateconf.WithTrieDBUpdatePayload(e.lastFwHash, common.BigToHash(big.NewInt(1009))))); err != nil {
		t.Fatalf("block 9 commit: %v", err)
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

// tearFrontier reproduces what the OOM left behind on the Tokyo box: Firewood
// holds what the merge committed before the kill, the exec head is the 0 the
// first exec.New seeded, and the header window and the ring (which the build
// writes only after the root check) never happened.
func tearFrontier(t *testing.T, dir string, keepHeaders bool) {
	t.Helper()
	// "exechead" is state.Store's own file name; 0 is what a dir that opened
	// an executor and never executed a block carries.
	if err := os.WriteFile(filepath.Join(dir, "exechead"), make([]byte, 8), 0o644); err != nil {
		t.Fatal(err)
	}
	if keepHeaders {
		return
	}
	if err := os.Remove(filepath.Join(dir, saeRingFile)); err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	hdrs, err := filepath.Glob(filepath.Join(dir, "headers_*.log"))
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range hdrs {
		if err := os.Remove(p); err != nil {
			t.Fatal(err)
		}
	}
}

// buildFrontierIn seals the corpus into a fresh dir and merges it, the way a
// downloaded node does, and hands the dir back closed.
func buildFrontierIn(t *testing.T, roots map[uint64]common.Hash, rows map[uint64][]state.StateRow) string {
	t.Helper()
	dir := t.TempDir()
	set := sealCorpus(t, dir, roots, rows)
	defer set.Close()
	store, err := state.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	e, err := New(Config{DataDir: dir, Blocks: set, Store: store, FrontierBuild: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := e.BuildFrontier(set); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	return dir
}

// TestHealTornFrontier is the second half of the join-from-bucket incident: the
// build was killed mid-merge, and every restart afterwards died in exec.New
// ("head=0 but firewood root ... != genesis"), which crash-looped the container
// until docker gave up. A half-built frontier is derived state, so the node
// must wipe it and rebuild by itself; a dir that really executed something must
// never be wiped.
func TestHealTornFrontier(t *testing.T) {
	fetch.RegisterExtras(chain.Coreth)
	roots, rows := replayCorpus(t, t.TempDir())

	// (a) Killed DURING the merge: Firewood alone, and it wedges the dir.
	dir := buildFrontierIn(t, roots, rows)
	tearFrontier(t, dir, false)
	store, err := state.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := New(Config{DataDir: dir, Blocks: fakeSource{}, Store: store}); err == nil ||
		!strings.Contains(err.Error(), "head=0 but firewood root") {
		t.Fatalf("a torn dir opened with %v, want the crash-loop refusal", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	healed, err := HealTornFrontier(dir)
	if err != nil || !healed {
		t.Fatalf("heal a torn dir: %v %v", healed, err)
	}
	if _, err := os.Stat(filepath.Join(dir, "firewood")); !os.IsNotExist(err) {
		t.Fatalf("firewood survived the heal: %v", err)
	}

	// The rebuild is the whole point: same dir, same epochs, right root.
	set, err := state.OpenEpochSet(mustLocalStore(t, dir))
	if err != nil {
		t.Fatal(err)
	}
	defer set.Close()
	store2, err := state.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store2.Close()
	e2, err := New(Config{DataDir: dir, Blocks: set, Store: store2, FrontierBuild: true})
	if err != nil {
		t.Fatalf("healed dir still refuses to open: %v", err)
	}
	defer e2.Close()
	if err := e2.BuildFrontier(set); err != nil {
		t.Fatalf("rebuild after heal: %v", err)
	}
	if got := common.Hash(e2.fwBackend.Firewood.Root()); got != roots[8] || e2.Head() != 8 {
		t.Fatalf("rebuilt frontier at %d root %x, want 8 %x", e2.Head(), got, roots[8])
	}
	// An executed dir is not torn, whatever else is on disk.
	if healed, err := HealTornFrontier(dir); err != nil || healed {
		t.Fatalf("heal on a dir executed to 8: %v %v, want no-op", healed, err)
	}

	// (b) Killed AFTER the root check: the header window and the ring are on
	// disk under a head of 0. Wiping Firewood without them would leave the next
	// start walking back into blocks nothing ever executed, so they go too.
	dir2 := buildFrontierIn(t, roots, rows)
	tearFrontier(t, dir2, true)
	healed, err = HealTornFrontier(dir2)
	if err != nil || !healed {
		t.Fatalf("heal a dir torn after the root check: %v %v", healed, err)
	}
	for _, p := range []string{"firewood", saeRingFile} {
		if _, err := os.Stat(filepath.Join(dir2, p)); !os.IsNotExist(err) {
			t.Fatalf("%s survived the heal: %v", p, err)
		}
	}
	if hdrs, _ := filepath.Glob(filepath.Join(dir2, "headers_*.log")); len(hdrs) != 0 {
		t.Fatalf("header window survived the heal: %v", hdrs)
	}
}

// mustLocalStore reopens a dir's artifact store (the epochs are in its spool).
func mustLocalStore(t *testing.T, dir string) *dist.Store {
	t.Helper()
	st, err := dist.Local(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	return st
}

// rssAnonKB is the process's ANONYMOUS resident memory, which is where
// Firewood's Rust allocations land and where the OOM happened (63.9GB anon on
// a 64GB box). The Go heap is a rounding error next to it and shows up here
// too, so this is the number to bound.
func rssAnonKB(t *testing.T) int {
	t.Helper()
	b, err := os.ReadFile("/proc/self/status")
	if err != nil {
		t.Skipf("no /proc/self/status: %v", err)
	}
	for _, l := range strings.Split(string(b), "\n") {
		if !strings.HasPrefix(l, "RssAnon:") {
			continue
		}
		n, err := strconv.Atoi(strings.Fields(l)[1])
		if err != nil {
			t.Fatal(err)
		}
		return n
	}
	t.Skip("no RssAnon in /proc/self/status")
	return 0
}

// TestFrontierBuildFirewoodMemoryBounded is bug one, pinned. The frontier build
// streams the whole corpus through Firewood in batches, and with the executor's
// serving profile (128 in-memory revisions, 64 of them unpersisted) every batch
// stayed resident: growth was dead linear in BATCH COUNT, i.e. in corpus size,
// and the first real join was OOM-killed at 63.9GB. Opening Firewood for the
// build with no history to keep makes the per-batch cost FALL instead, which is
// the property this asserts: the second half of the batches must cost less than
// the first, at a total the old code passed before batch 4.
//
// Measured here at 8 batches of 100k ops: 1483MB retained with the serving
// profile, 688MB with the build's, second half 1.44x the first vs 0.42x.
func TestFrontierBuildFirewoodMemoryBounded(t *testing.T) {
	if testing.Short() {
		t.Skip("streams 800k trie ops through Firewood")
	}
	const batches, per = 8, 100_000
	dir := t.TempDir()
	store, err := state.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	e, err := New(Config{DataDir: dir, Blocks: fakeSource{}, Store: store, FrontierBuild: true})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	// The same shape BuildFrontier streams: 32-byte hashed account keys in no
	// order at all, with account-RLP-sized values.
	rng := rand.New(rand.NewSource(1))
	base, half := rssAnonKB(t), 0
	for b := 0; b < batches; b++ {
		ops := make([]ffi.BatchOp, 0, per)
		for i := 0; i < per; i++ {
			k, v := make([]byte, 32), make([]byte, 80)
			rng.Read(k)
			rng.Read(v)
			ops = append(ops, ffi.Put(k, v))
		}
		if _, err := e.fwBackend.Firewood.Update(ops); err != nil {
			t.Fatal(err)
		}
		if b == batches/2-1 {
			half = rssAnonKB(t)
		}
	}
	end := rssAnonKB(t)
	first, second := half-base, end-half
	t.Logf("firewood anon: +%dMB over the first %d batches, +%dMB over the next %d",
		first>>10, batches/2, second>>10, batches/2)
	if second >= first {
		t.Fatalf("per-batch cost is not falling (+%dMB then +%dMB): the build is retaining every batch again", first>>10, second>>10)
	}
	if grew := end - base; grew>>10 > 1200 {
		t.Fatalf("%d batches of %d ops retained %dMB, want well under the 1200MB the serving profile passes before batch 4", batches, per, grew>>10)
	}
}

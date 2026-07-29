package state

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/crypto"
	"github.com/ava-labs/libevm/rlp"
	"github.com/containerman17/epochdb/fetch"
	"github.com/holiman/uint256"
)

// The fold's synthetic corpus. Blocks carry a fixed 3 txs each so the
// boundary is arithmetic, and exechead sits a whole bucket above every
// boundary the tests cut so the retirement guard is satisfied without
// materialising 100k blocks.
const (
	foldTxsPerBlock = 3
	foldEpochTxs    = 12 // => a boundary every 4 blocks
	foldExecHead    = BucketBlocks + 100
)

type foldBlk struct {
	n     uint64
	frame []byte
}

var (
	fdA = common.HexToAddress("0x1111111111111111111111111111111111111111")
	fdB = common.HexToAddress("0x2222222222222222222222222222222222222222")
	fdC = common.HexToAddress("0x3333333333333333333333333333333333333333")
	fdD = common.HexToAddress("0x4444444444444444444444444444444444444444")
	fdE = common.HexToAddress("0x5555555555555555555555555555555555555555")
	fdG = common.HexToAddress("0x6666666666666666666666666666666666666666")

	fdS1 = common.HexToHash("0x01")
	fdS2 = common.HexToHash("0x02")
	fdS3 = common.HexToHash("0x03")

	fdCode      = []byte{0x60, 0x0a, 0x60, 0x0b}
	fdCodeHash  = crypto.Keccak256Hash(fdCode)
	fdAllocCode = []byte{0xfe, 0xed, 0xfa, 0xce}
	fdAllocAddr = common.HexToAddress("0x0100000000000000000000000000000000000000")
)

// foldAlloc is snapshot(0): hardcoded in the client, never a file, and the
// bottom of the very first fold. One plain account and one with code, whose
// blob is NOT in code.log (alloc code is never deployed by a block).
func foldAlloc() types.GenesisAlloc {
	return types.GenesisAlloc{
		fdAllocAddr: types.Account{Balance: big.NewInt(1000), Nonce: 0, Code: fdAllocCode},
		fdA:         types.Account{Balance: big.NewInt(7), Nonce: 1},
	}
}

// writeFoldCorpus materialises blocks into dir: staging containers (3 txs
// each), RLP headers, capture frames, code blobs, exechead. Appending is
// idempotent, so a second call extends the same dir.
func writeFoldCorpus(t *testing.T, dir string, blks []foldBlk) {
	t.Helper()
	for _, b := range blks {
		writeStagingBlock(t, dir, b.n/BucketBlocks, b.n, foldTxsPerBlock)
	}
	st, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, b := range blks {
		if err := st.AppendHeader(b.n, baseHdr(t, b.n, baseRoot(b.n))); err != nil {
			t.Fatal(err)
		}
		if len(b.frame) > 0 {
			if err := st.AppendWrites(b.n, b.frame); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := st.PutCode(fdCodeHash, fdCode); err != nil {
		t.Fatal(err)
	}
	if err := st.FlushAndSetExecHead(foldExecHead); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
}

func fdAccount(t *testing.T, nonce uint64, bal int64, codeHash common.Hash) []byte {
	t.Helper()
	// The capture encoding: storage root is the ZERO hash, because
	// firewood-ethhash's storage tries hash to zero.
	raw, err := rlp.EncodeToBytes(&types.StateAccount{
		Nonce: nonce, Balance: uint256.NewInt(uint64(bal)),
		Root: common.Hash{}, CodeHash: codeHash.Bytes(),
	})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// foldCorpusRound1 is blocks 1..5: enough for exactly one boundary at 4.
func foldCorpusRound1(t *testing.T) []foldBlk {
	t.Helper()
	return []foldBlk{
		{n: 1, frame: frStorage(frAccount(nil, fdA, fdAccount(t, 2, 100, types.EmptyCodeHash)), fdA, fdS1, []byte{0x11})},
		{n: 2, frame: frStorage(frAccount(nil, fdB, fdAccount(t, 1, 50, fdCodeHash)), fdB, fdS1, []byte{0x22})},
		{n: 3, frame: frStorage(nil, fdA, fdS2, []byte{0x33})},
		{n: 4, frame: frAccount(nil, fdC, fdAccount(t, 0, 9, types.EmptyCodeHash))}, // boundary: 12 txs
		{n: 5, frame: frStorage(nil, fdC, fdS1, []byte{0x44})},
	}
}

// foldCorpusRound2 is blocks 6..10: the second boundary lands at 8, so the
// period (4, 8] folds against the base_4 the first round produced.
func foldCorpusRound2(t *testing.T) []foldBlk {
	t.Helper()
	return []foldBlk{
		{n: 6, frame: frStorage(nil, fdA, fdS1, []byte{0x55})},              // shadows the base row
		{n: 7, frame: frStorage(nil, fdB, fdS1, nil)},                       // zero write: slot disappears
		{n: 8, frame: frAccount(nil, fdC, fdAccount(t, 3, 77, fdCodeHash))}, // boundary
		{n: 9, frame: frStorage(nil, fdA, fdS3, []byte{0x66})},
		{n: 10, frame: nil},
	}
}

func mustFold(t *testing.T, dir string) {
	t.Helper()
	if err := FoldSnapshots(dir, foldAlloc(), foldEpochTxs, nil); err != nil {
		t.Fatal(err)
	}
}

func openFolded(t *testing.T, dir string, wantBlock uint64) *Base {
	t.Helper()
	b, ok, err := OpenBase(dir)
	if err != nil || !ok {
		t.Fatalf("OpenBase(%s): ok=%v err=%v", dir, ok, err)
	}
	if b.Block() != wantBlock {
		b.Close()
		t.Fatalf("folded to block %d, want %d", b.Block(), wantBlock)
	}
	return b
}

func fileSHA(t *testing.T, path string) string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return fmt.Sprintf("%x", sha256.Sum256(raw))
}

// ---------------------------------------------------------------------------
// 1. THE CORRECTNESS TEST: the fold is inductive. A read of the folded
// snapshot at B must answer byte-identically to a full descent read at B over
// the inputs the fold consumed, for both bottoms (alloc at K=1, a base file
// at K=2).
// ---------------------------------------------------------------------------

func TestFoldInductiveInvariant(t *testing.T) {
	dir := t.TempDir()
	writeFoldCorpus(t, dir, foldCorpusRound1(t))
	mustFold(t, dir)

	b1 := openFolded(t, dir, 4)
	compareBaseToHistory(t, dir, b1, 4)
	if b1.CumTx() != 12 {
		t.Fatalf("cumTx at B=4 is %d, want 12", b1.CumTx())
	}
	if b1.StateRoot() != baseRoot(4) {
		t.Fatalf("footer root %x, want header(4).Root %x", b1.StateRoot(), baseRoot(4))
	}
	b1.Close()

	// K=2: the same machinery with a real base file as the bottom.
	writeFoldCorpus(t, dir, foldCorpusRound2(t))
	// Snapshot the descent's answers BEFORE the fold consumes the inputs.
	want := probeHistory(t, dir, 8)
	mustFold(t, dir)

	b2 := openFolded(t, dir, 8)
	defer b2.Close()
	if b2.CumTx() != 24 {
		t.Fatalf("cumTx at B=8 is %d, want 24 (12 + 12)", b2.CumTx())
	}
	compareProbes(t, b2, want)
	// The previous snapshot is gone: exactly one base file survives a fold.
	if _, err := os.Stat(filepath.Join(dir, "base_4")); !os.IsNotExist(err) {
		t.Fatalf("base_4 survived the fold to 8: %v", err)
	}
}

type probe struct {
	kind byte
	addr common.Address
	slot common.Hash
	// account: RLP or nil; storage: value or nil
	val []byte
}

// foldProbeKeys is every key the corpus touches plus absentees, so both the
// present and the missing answers are compared.
func foldProbeKeys() []probe {
	var out []probe
	for _, a := range []common.Address{fdA, fdB, fdC, fdD, fdE, fdG, fdAllocAddr,
		common.HexToAddress("0xdead")} {
		out = append(out, probe{kind: recKindAccount, addr: a})
		for _, s := range []common.Hash{fdS1, fdS2, fdS3, common.HexToHash("0xbeef")} {
			out = append(out, probe{kind: recKindStorage, addr: a, slot: s})
		}
	}
	return out
}

func probeHistory(t *testing.T, dir string, at uint64) []probe {
	t.Helper()
	// The descent reads cooked buckets; the fold cooks for itself, so the
	// comparison side has to do it too.
	if err := CookIndex(dir); err != nil {
		t.Fatal(err)
	}
	st, err := OpenReadOnly(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	h, err := OpenHistory(dir, st, foldAlloc())
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()

	probes := foldProbeKeys()
	for i := range probes {
		p := &probes[i]
		switch p.kind {
		case recKindAccount:
			acc, err := h.AccountAt(p.addr, at)
			if err != nil {
				t.Fatalf("AccountAt(%x, %d): %v", p.addr, at, err)
			}
			if acc != nil {
				// Re-encode with the capture's zero storage root so the bytes
				// are comparable to what the base stores.
				acc.Root = common.Hash{}
				raw, err := rlp.EncodeToBytes(acc)
				if err != nil {
					t.Fatal(err)
				}
				p.val = raw
			}
		case recKindStorage:
			v, err := h.StorageAt(p.addr, p.slot[:], at)
			if err != nil {
				t.Fatalf("StorageAt(%x/%x, %d): %v", p.addr, p.slot, at, err)
			}
			p.val = v
		}
	}
	return probes
}

func compareProbes(t *testing.T, b *Base, want []probe) {
	t.Helper()
	for _, p := range want {
		var (
			got []byte
			ok  bool
			err error
		)
		if p.kind == recKindAccount {
			got, ok, err = b.Account(p.addr)
		} else {
			got, ok, err = b.Storage(p.addr, p.slot[:])
		}
		if err != nil {
			t.Fatalf("base read %c %x/%x: %v", p.kind, p.addr, p.slot, err)
		}
		if !ok {
			got = nil
		}
		if !bytes.Equal(got, p.val) {
			t.Fatalf("folded %c %x/%x = %x, descent says %x", p.kind, p.addr, p.slot, got, p.val)
		}
	}
}

func compareBaseToHistory(t *testing.T, dir string, b *Base, at uint64) {
	t.Helper()
	compareProbes(t, b, probeHistory(t, dir, at))
}

// TestFoldCarriesCode: 'c'(K) = 'c'(K-1) union the code of every surviving
// account row, so whichever snapshot answers the account read carries its
// code. Alloc code rides along even though it is not in code.log.
func TestFoldCarriesCode(t *testing.T) {
	dir := t.TempDir()
	writeFoldCorpus(t, dir, foldCorpusRound1(t))
	mustFold(t, dir)
	b := openFolded(t, dir, 4)
	blob, ok, err := b.Code(fdCodeHash)
	if err != nil || !ok || !bytes.Equal(blob, fdCode) {
		t.Fatalf("deployed code: %x ok=%v err=%v", blob, ok, err)
	}
	allocHash := crypto.Keccak256Hash(fdAllocCode)
	blob, ok, err = b.Code(allocHash)
	if err != nil || !ok || !bytes.Equal(blob, fdAllocCode) {
		t.Fatalf("alloc code (never in code.log): %x ok=%v err=%v", blob, ok, err)
	}
	b.Close()

	// Grow-only union: fdB is never written again in round 2, so its code has
	// to survive purely by carrying 'c'(K-1) forward.
	writeFoldCorpus(t, dir, foldCorpusRound2(t))
	mustFold(t, dir)
	b2 := openFolded(t, dir, 8)
	defer b2.Close()
	for _, h := range []common.Hash{fdCodeHash, allocHash} {
		if _, ok, err := b2.Code(h); err != nil || !ok {
			t.Fatalf("code %x lost at K=2: ok=%v err=%v", h, ok, err)
		}
	}
}

// ---------------------------------------------------------------------------
// 2. SELFDESTRUCT matrix. The fold must mirror lastAccountDelete's authority
// byte-for-byte, strict inequality included, or a folded node reads different
// storage than a full one.
// ---------------------------------------------------------------------------

func TestFoldSelfDestructMatrix(t *testing.T) {
	dir := t.TempDir()
	// Round 1 establishes a base at 4 holding D, E, G with storage.
	mk := func() []byte { return fdAccount(t, 1, 5, types.EmptyCodeHash) }
	r1 := []foldBlk{
		{n: 1, frame: frStorage(frAccount(nil, fdD, mk()), fdD, fdS1, []byte{0xd1})},
		{n: 2, frame: frStorage(frAccount(nil, fdE, mk()), fdE, fdS1, []byte{0xe1})},
		{n: 3, frame: frStorage(frAccount(nil, fdG, mk()), fdG, fdS1, []byte{0x91})},
		{n: 4, frame: frAccount(nil, fdC, mk())},
	}
	writeFoldCorpus(t, dir, r1)
	mustFold(t, dir)
	openFolded(t, dir, 4).Close()

	// Period (4, 8]:
	//   D deleted at 5, never recreated.
	//   G writes s3 at 5, is deleted at 6, recreated at 7 with s2 written.
	//   E is deleted at 6 with a non-zero storage write in the SAME block:
	//     the strict-inequality case (tombstone 6 vs write block 6).
	var b6 []byte
	b6 = frAccount(b6, fdG, nil)                // G destructed
	b6 = frAccount(b6, fdE, nil)                // E destructed
	b6 = frStorage(b6, fdE, fdS2, []byte{0xe2}) // same-block write: SURVIVES, strict >

	var b7 []byte
	b7 = frAccount(b7, fdG, fdAccount(t, 0, 2, types.EmptyCodeHash))
	b7 = frStorage(b7, fdG, fdS2, []byte{0x92})

	r2 := []foldBlk{
		{n: 5, frame: frStorage(frAccount(nil, fdD, nil), fdG, fdS3, []byte{0x93})},
		{n: 6, frame: b6},
		{n: 7, frame: b7},
		{n: 8, frame: frStorage(nil, fdC, fdS1, []byte{0xc1})},
	}
	writeFoldCorpus(t, dir, r2)
	want := probeHistory(t, dir, 8)
	mustFold(t, dir)
	b := openFolded(t, dir, 8)
	defer b.Close()

	// Cross-check against the descent first: that is the contract.
	compareProbes(t, b, want)

	// Then the explicit matrix, so a change in BOTH implementations still fails.
	absentAcct := func(a common.Address, why string) {
		t.Helper()
		if _, ok, _ := b.Account(a); ok {
			t.Fatalf("%s: account %x still present", why, a)
		}
	}
	slot := func(a common.Address, s common.Hash) []byte {
		t.Helper()
		v, ok, err := b.Storage(a, s[:])
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			return nil
		}
		return v
	}
	absentAcct(fdD, "delete-only")
	if v := slot(fdD, fdS1); v != nil {
		t.Fatalf("orphaned base storage of a deleted account survived: %x", v)
	}
	absentAcct(fdE, "delete with a same-block write")
	// THE STRICT INEQUALITY. tombstone(E) == 6 and the write is at block 6,
	// so `tombstone > wblk` is false and the value survives, exactly as
	// History.StorageAt reports it. A `>=` here would silently zero a
	// recreated contract's first slots on every folded node.
	if v := slot(fdE, fdS2); !bytes.Equal(v, []byte{0xe2}) {
		t.Fatalf("storage written in the destruct block must survive (strict >), got %x", v)
	}
	// The pre-destruct slot, which lives in the BASE (write block counts as
	// F, below every tombstone), is dead.
	if v := slot(fdE, fdS1); v != nil {
		t.Fatalf("E's pre-destruct base storage survived: %x", v)
	}
	if _, ok, _ := b.Account(fdG); !ok {
		t.Fatal("later recreate: account G must exist")
	}
	if v := slot(fdG, fdS3); v != nil {
		t.Fatalf("G's pre-destruct storage (block 5, destruct 6) survived: %x", v)
	}
	if v := slot(fdG, fdS2); !bytes.Equal(v, []byte{0x92}) {
		t.Fatalf("G's post-recreate storage: %x", v)
	}
	if v := slot(fdG, fdS1); v != nil {
		t.Fatalf("G's base storage survived the destruct: %x", v)
	}
}

// ---------------------------------------------------------------------------
// 3. THE ONE THAT MATTERS: two independent nodes, same chain content, byte
// identical snapshots. Mirrors the epoch determinism proof of record.
// ---------------------------------------------------------------------------

func TestFoldByteIdentityAcrossNodes(t *testing.T) {
	dirA, dirB := t.TempDir(), t.TempDir()
	for _, d := range []string{dirA, dirB} {
		writeFoldCorpus(t, d, foldCorpusRound1(t))
		mustFold(t, d)
	}
	// K=1: the genesis alloc is the bottom.
	shaA := fileSHA(t, filepath.Join(dirA, "base_4"))
	shaB := fileSHA(t, filepath.Join(dirB, "base_4"))
	if shaA != shaB {
		t.Fatalf("K=1 snapshots differ across independent nodes:\n  %s\n  %s", shaA, shaB)
	}

	// K=2: a base file is the bottom.
	for _, d := range []string{dirA, dirB} {
		writeFoldCorpus(t, d, foldCorpusRound2(t))
		mustFold(t, d)
	}
	shaA = fileSHA(t, filepath.Join(dirA, "base_8"))
	shaB = fileSHA(t, filepath.Join(dirB, "base_8"))
	if shaA != shaB {
		t.Fatalf("K=2 snapshots differ across independent nodes:\n  %s\n  %s", shaA, shaB)
	}
}

// TestFoldBoundaryMatchesSeal is the production rule: a pruning node's
// snapshot boundaries and an archival node's epoch cuts are the same heights
// on the same chain content, or the manifest can never pair them.
func TestFoldBoundaryMatchesSeal(t *testing.T) {
	dir := t.TempDir()
	blks := append(foldCorpusRound1(t), foldCorpusRound2(t)...)
	writeFoldCorpus(t, dir, blks)

	st, err := OpenReadOnly(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	reader, err := fetch.OpenReader(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	// The fold's own scan, from the genesis floor.
	B, txs, ok, err := foldBoundary(reader, 0, 10, foldEpochTxs)
	if err != nil || !ok {
		t.Fatalf("foldBoundary: B=%d ok=%v err=%v", B, ok, err)
	}
	// The sealer's cut over the same corpus.
	in, _, err := gatherEpoch(st, reader, 1, 10, foldEpochTxs)
	if err != nil || in == nil {
		t.Fatalf("gatherEpoch: %v", err)
	}
	sealEnd := in.Start + uint64(len(in.Containers)) - 1
	if B != sealEnd || txs != in.TxCount {
		t.Fatalf("fold cuts at %d (%d txs), seal cuts at %d (%d txs)", B, txs, sealEnd, in.TxCount)
	}

	// And the second period, from the first boundary.
	B2, _, ok, err := foldBoundary(reader, B, 10, foldEpochTxs)
	if err != nil || !ok {
		t.Fatalf("second boundary: ok=%v err=%v", ok, err)
	}
	in2, _, err := gatherEpoch(st, reader, B+1, 10, foldEpochTxs)
	if err != nil || in2 == nil {
		t.Fatalf("second gatherEpoch: %v", err)
	}
	if got := in2.Start + uint64(len(in2.Containers)) - 1; B2 != got {
		t.Fatalf("second period: fold %d, seal %d", B2, got)
	}
}

// ---------------------------------------------------------------------------
// 5. Crash and idempotence, over the commit-ordering table.
// ---------------------------------------------------------------------------

func TestFoldCrashAndIdempotence(t *testing.T) {
	dir := t.TempDir()
	writeFoldCorpus(t, dir, foldCorpusRound1(t))

	// Kill mid-merge: a partial temp file. It is never matched by
	// ParseBaseFileName, the sweep removes it, and the deterministic boundary
	// means the rerun recreates the same name with identical bytes.
	if err := os.WriteFile(filepath.Join(dir, "base_4.tmp"), []byte("half a snapshot"), 0o644); err != nil {
		t.Fatal(err)
	}
	mustFold(t, dir)
	if _, err := os.Stat(filepath.Join(dir, "base_4.tmp")); !os.IsNotExist(err) {
		t.Fatalf("crash leftover survived: %v", err)
	}
	sha1 := fileSHA(t, filepath.Join(dir, "base_4"))

	// Rerunning the whole command changes nothing: no new boundary exists.
	mustFold(t, dir)
	if got := fileSHA(t, filepath.Join(dir, "base_4")); got != sha1 {
		t.Fatalf("rerun rewrote the snapshot: %s -> %s", sha1, got)
	}

	// Kill between rename and the old-base unlink: two base files. Newest
	// wins for readers, and the next fold sweeps the loser.
	stale := filepath.Join(dir, "base_2")
	raw, err := os.ReadFile(filepath.Join(dir, "base_4"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(stale, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	blk, ok, err := PeekBase(dir)
	if err != nil || !ok || blk != 4 {
		t.Fatalf("PeekBase with two bases: %d ok=%v err=%v, want 4", blk, ok, err)
	}
	mustFold(t, dir)
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Fatalf("superseded base survived the sweep: %v", err)
	}
	if got := fileSHA(t, filepath.Join(dir, "base_4")); got != sha1 {
		t.Fatalf("sweep disturbed the surviving snapshot")
	}
}

// TestFoldRetirementGuard: the fold refuses to publish (and therefore to
// retire raw buckets) until exechead is one whole bucket past the boundary,
// so a crash right after a fold always finds its walk-back containers.
func TestFoldRetirementGuard(t *testing.T) {
	dir := t.TempDir()
	writeFoldCorpus(t, dir, foldCorpusRound1(t))
	// Pull exechead back below B + BucketBlocks.
	st, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.FlushAndSetExecHead(5); err != nil {
		t.Fatal(err)
	}
	st.Close()

	mustFold(t, dir)
	if _, ok, err := OpenBase(dir); ok || err != nil {
		t.Fatalf("fold published without a bucket of retirement headroom: ok=%v err=%v", ok, err)
	}
}

// ---------------------------------------------------------------------------
// 6. Gate teeth: a failing pre-rename gate must leave the directory exactly
// as it was, temp file included.
// ---------------------------------------------------------------------------

func TestFoldGateBlocksRename(t *testing.T) {
	dir := t.TempDir()
	writeFoldCorpus(t, dir, foldCorpusRound1(t))

	gated := 0
	err := FoldSnapshots(dir, foldAlloc(), foldEpochTxs, func(tmp string) error {
		gated++
		if filepath.Base(tmp) != "base_4.tmp" {
			t.Fatalf("gate got %s, want base_4.tmp", tmp)
		}
		// The gate must be able to OPEN the temp file: that is the whole
		// point, and openBaseFile's name check has to tolerate the suffix.
		b, err := OpenBaseFile(tmp)
		if err != nil {
			t.Fatalf("gate cannot open the temp snapshot: %v", err)
		}
		if b.Block() != 4 {
			t.Fatalf("temp snapshot claims block %d", b.Block())
		}
		b.Close()
		return fmt.Errorf("synthetic gate failure")
	})
	if err == nil || !strings.Contains(err.Error(), "gate") {
		t.Fatalf("fold ignored the gate: %v", err)
	}
	if gated != 1 {
		t.Fatalf("gate called %d times", gated)
	}
	if _, err := os.Stat(filepath.Join(dir, "base_4")); !os.IsNotExist(err) {
		t.Fatal("a gate-rejected snapshot was renamed into place")
	}
	if _, err := os.Stat(filepath.Join(dir, "base_4.tmp")); !os.IsNotExist(err) {
		t.Fatal("a gate-rejected snapshot left its temp file behind")
	}
}

// TestFoldRefusesSealedDir: a node either seals epochs or folds snapshots.
func TestFoldRefusesSealedDir(t *testing.T) {
	dir := t.TempDir()
	writeFoldCorpus(t, dir, foldCorpusRound1(t))
	if err := os.WriteFile(filepath.Join(dir, EpochFileName(1, 4)), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	err := FoldSnapshots(dir, foldAlloc(), foldEpochTxs, nil)
	if err == nil || !strings.Contains(err.Error(), "sealed epochs") {
		t.Fatalf("fold accepted a sealed dir: %v", err)
	}
}

// TestFoldAllocStorageIsLoud: rendering genesis-alloc STORAGE was never
// needed (no such account on either network) and the fold must not guess.
func TestFoldAllocStorageIsLoud(t *testing.T) {
	alloc := foldAlloc()
	alloc[fdA] = types.Account{Balance: big.NewInt(1),
		Storage: map[common.Hash]common.Hash{fdS1: common.HexToHash("0x9")}}
	if _, err := allocRows(alloc); err == nil || !strings.Contains(err.Error(), "storage slots") {
		t.Fatalf("alloc storage silently dropped: %v", err)
	}
}

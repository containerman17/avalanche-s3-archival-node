//go:build subnetbench

package prunedffi

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/libevm/stateconf"
)

func testDB(t *testing.T, cfg Config) *DB {
	t.Helper()
	if cfg.Dir == "" {
		cfg.Dir = t.TempDir()
	}
	d, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := d.Close(); err != nil {
			t.Error(err)
		}
	})
	return d
}

func testHash(n uint64) common.Hash {
	var h common.Hash
	binary.BigEndian.PutUint64(h[24:], n)
	return h
}

func accountOp(n, balance uint64) []byte {
	h := testHash(n)
	op := append([]byte{opAccount}, h[:]...)
	op = append(op, make([]byte, 8)...)
	bal := testHash(balance)
	op = append(op, bal[:]...)
	return append(op, types.EmptyCodeHash[:]...)
}

func deleteOp(n uint64) []byte {
	h := testHash(n)
	return append([]byte{opDelete}, h[:]...)
}

func storageOp(owner, slot uint64, value ...byte) []byte {
	a, s := testHash(owner), testHash(slot)
	op := append(append([]byte{opSlot}, a[:]...), s[:]...)
	return append(append(op, byte(len(value))), value...)
}

// block proposes ops on the revision identified by parentBlock and links the
// result to block at height. Height 0 proposals hang off the zero block hash.
type blockID struct {
	root, hash common.Hash
	height     uint64
}

func genesis() blockID { return blockID{root: types.EmptyRootHash} }

func proposeBlock(t *testing.T, d *DB, parent blockID, block uint64, ops ...[]byte) blockID {
	t.Helper()
	root, err := d.propose(parent.root, bytes.Join(ops, nil))
	if err != nil {
		t.Fatal(err)
	}
	height := parent.height + 1
	if parent.hash == (common.Hash{}) {
		height = 0
	}
	h := testHash(block)
	if err = d.Update(root, parent.root, height, nil, nil, stateconf.WithTrieDBUpdatePayload(parent.hash, h)); err != nil {
		t.Fatal(err)
	}
	return blockID{root, h, height}
}

func acceptBlock(t *testing.T, d *DB, b blockID) {
	t.Helper()
	if err := d.Commit(b.root, false); err != nil {
		t.Fatal(err)
	}
}

func checkBalance(t *testing.T, d *DB, root common.Hash, n, balance uint64) {
	t.Helper()
	v, err := d.getAccount(root, testHash(n))
	if err != nil {
		t.Fatal(err)
	}
	if balance == 0 && v == nil {
		return
	}
	if want := testHash(balance); v == nil || !bytes.Equal(v[8:40], want[:]) {
		t.Fatalf("account %d: got %x, want balance %d", n, v, balance)
	}
}

func checkSlot(t *testing.T, d *DB, root common.Hash, owner, slot uint64, want []byte) {
	t.Helper()
	v, err := d.getStorage(root, testHash(owner), testHash(slot))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(v, want) {
		t.Fatalf("slot %d/%d: got %x, want %x", owner, slot, v, want)
	}
}

func head(t *testing.T, d *DB) blockID {
	t.Helper()
	root, block, height := d.head()
	return blockID{root, block, height}
}

func TestPendingBranchesAndPruning(t *testing.T) {
	d := testDB(t, Config{Retain: 3})
	g := proposeBlock(t, d, genesis(), 100, accountOp(1, 10))
	acceptBlock(t, d, g)
	left := proposeBlock(t, d, g, 101, accountOp(1, 20))
	right := proposeBlock(t, d, g, 102, accountOp(1, 30))
	child := proposeBlock(t, d, left, 103, storageOp(1, 1, 9))
	checkBalance(t, d, g.root, 1, 10)
	checkBalance(t, d, left.root, 1, 20)
	checkBalance(t, d, right.root, 1, 30)
	acceptBlock(t, d, left)
	checkBalance(t, d, g.root, 1, 10)
	checkBalance(t, d, child.root, 1, 20)
	checkSlot(t, d, child.root, 1, 1, []byte{9})
	if _, err := d.getAccount(right.root, testHash(1)); !errors.Is(err, ErrPruned) {
		t.Fatalf("rejected branch readable: %v", err)
	}
	acceptBlock(t, d, child)
	empty := proposeBlock(t, d, child, 104)
	if empty.root != child.root {
		t.Fatal("empty block changed the root")
	}
	acceptBlock(t, d, empty)
	if err := d.open(g.root); !errors.Is(err, ErrPruned) {
		t.Fatalf("old root retained: %v", err)
	}
	checkBalance(t, d, head(t, d).root, 1, 20)
	if got := head(t, d); got.height != 3 || got.hash != testHash(104) {
		t.Fatalf("head %+v", got)
	}
}

func TestDeletionRecreationAcrossAcceptanceAndCheckpoint(t *testing.T) {
	dir := t.TempDir()
	d := testDB(t, Config{Dir: dir})
	g := proposeBlock(t, d, genesis(), 100, accountOp(1, 10), storageOp(1, 1, 9))
	acceptBlock(t, d, g)
	recreated := proposeBlock(t, d, g, 101, deleteOp(1), accountOp(1, 25))
	checkSlot(t, d, recreated.root, 1, 1, nil)
	checkSlot(t, d, g.root, 1, 1, []byte{9})
	acceptBlock(t, d, recreated)
	checkSlot(t, d, g.root, 1, 1, []byte{9})
	checkSlot(t, d, head(t, d).root, 1, 1, nil)
	if err := d.checkpoint(); err != nil {
		t.Fatal(err)
	}
	checkSlot(t, d, g.root, 1, 1, []byte{9})
	checkSlot(t, d, head(t, d).root, 1, 1, nil)
	// A proposal based on the retained pre-checkpoint state must also hash.
	if _, err := d.propose(g.root, accountOp(1, 50)); err != nil {
		t.Fatal(err)
	}
	root := head(t, d).root
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := testDB(t, Config{Dir: dir})
	if head(t, reopened).root != root {
		t.Fatal("restart changed root")
	}
	checkBalance(t, reopened, root, 1, 25)
	checkSlot(t, reopened, root, 1, 1, nil)
}

func TestRecoveryAndCheckpointPruneJournal(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{Dir: dir, JournalLimit: 700, Retain: 4}
	d := testDB(t, cfg)
	parent := genesis()
	for n := uint64(0); n < 35; n++ {
		parent = proposeBlock(t, d, parent, 100+n, accountOp(1, n+1), storageOp(1, n+1, byte(n+1)))
		acceptBlock(t, d, parent)
	}
	journal, err := os.Stat(filepath.Join(dir, "JOURNAL"))
	if err != nil {
		t.Fatal(err)
	}
	if journal.Size() >= cfg.JournalLimit {
		t.Fatal("unbounded recovery journal")
	}
	want := head(t, d)
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := testDB(t, cfg)
	if got := head(t, reopened); got != want {
		t.Fatalf("recovered head %+v, want %+v", got, want)
	}
	for n := uint64(0); n < 35; n++ {
		checkSlot(t, reopened, want.root, 1, n+1, []byte{byte(n + 1)})
	}
	files, err := filepath.Glob(filepath.Join(dir, "state.*"))
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 {
		t.Fatalf("retained old state files: %v", files)
	}
	acceptBlock(t, reopened, proposeBlock(t, reopened, want, 200, accountOp(1, 100)))
}

func TestIncompleteJournalTailAndChecksumFailure(t *testing.T) {
	for _, partial := range []bool{true, false} {
		t.Run(map[bool]string{true: "partial", false: "checksum"}[partial], func(t *testing.T) {
			dir := t.TempDir()
			d := testDB(t, Config{Dir: dir})
			acceptBlock(t, d, proposeBlock(t, d, genesis(), 100, accountOp(1, 10)))
			root := head(t, d).root
			if err := d.Close(); err != nil {
				t.Fatal(err)
			}
			p := filepath.Join(dir, "JOURNAL")
			if partial {
				f, err := os.OpenFile(p, os.O_APPEND|os.O_WRONLY, 0600)
				if err != nil {
					t.Fatal(err)
				}
				_, err = f.Write([]byte{100, 0, 0, 0, 0, 0, 0, 0, 1, 2, 3})
				f.Close()
				if err != nil {
					t.Fatal(err)
				}
				reopened := testDB(t, Config{Dir: dir})
				if head(t, reopened).root != root {
					t.Fatal("partial record changed head")
				}
			} else {
				b, err := os.ReadFile(p)
				if err != nil {
					t.Fatal(err)
				}
				b[len(b)-1] ^= 1
				if err = os.WriteFile(p, b, 0600); err != nil {
					t.Fatal(err)
				}
				if reopened, err := New(Config{Dir: dir}); err == nil {
					reopened.Close()
					t.Fatal("accepted corrupted journal")
				}
			}
		})
	}
}

func TestExclusiveStateDirectory(t *testing.T) {
	dir := t.TempDir()
	testDB(t, Config{Dir: dir})
	if other, err := New(Config{Dir: dir}); err == nil {
		other.Close()
		t.Fatal("allowed concurrent database writers")
	}
}

func TestDuplicateRootBlockIdentities(t *testing.T) {
	d := testDB(t, Config{})
	g := proposeBlock(t, d, genesis(), 100, accountOp(1, 10))
	acceptBlock(t, d, g)
	left := proposeBlock(t, d, g, 101, accountOp(1, 20))
	right := proposeBlock(t, d, g, 102, accountOp(1, 20))
	if left.root != right.root {
		t.Fatal("identical transitions differ")
	}
	acceptBlock(t, d, left)
	// The second identity still works as a parent after the shared
	// transition is accepted under the first one.
	acceptBlock(t, d, proposeBlock(t, d, right, 103, accountOp(1, 30)))
	checkBalance(t, d, head(t, d).root, 1, 30)
}

func TestSecondAliasAfterCheckpointRestart(t *testing.T) {
	dir := t.TempDir()
	d := testDB(t, Config{Dir: dir})
	g := proposeBlock(t, d, genesis(), 100, accountOp(1, 10))
	acceptBlock(t, d, g)
	first := proposeBlock(t, d, g, 101, accountOp(1, 20))
	second := proposeBlock(t, d, g, 102, accountOp(1, 20))
	acceptBlock(t, d, second)
	if err := d.checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := testDB(t, Config{Dir: dir})
	if head(t, reopened).root != first.root {
		t.Fatal("checkpoint changed aliased state")
	}
	// subnet-evm supplies the accepted block's identity after locating its
	// persisted root. The second block hash must then work as a parent.
	reopened.SetHashAndHeight(testHash(102), 1)
	r := proposeBlock(t, reopened, second, 103, accountOp(1, 30))
	acceptBlock(t, reopened, r)
	checkBalance(t, reopened, head(t, reopened).root, 1, 30)
}

func TestClearAllOnlyForEmptyState(t *testing.T) {
	d := testDB(t, Config{})
	if err := d.ClearAll(); err != nil {
		t.Fatal(err)
	}
	acceptBlock(t, d, proposeBlock(t, d, genesis(), 100, accountOp(1, 10)))
	if err := d.ClearAll(); err == nil {
		t.Fatal("discarded persisted state")
	}
	if !d.Initialized(common.Hash{}) {
		t.Fatal("state not initialized")
	}
}

func TestKillRecovery(t *testing.T) {
	if dir := os.Getenv("PRUNEDFFI_CRASH_CHILD"); dir != "" {
		d := testDB(t, Config{Dir: dir, JournalLimit: 5000})
		parent := genesis()
		for n := uint64(0); n < 100; n++ {
			parent = proposeBlock(t, d, parent, 100+n, accountOp(1, n+1), storageOp(1, n+1, byte(n+1)))
			acceptBlock(t, d, parent)
		}
		root := head(t, d).root
		// A verified child must disappear after the crash.
		proposeBlock(t, d, parent, 999, accountOp(1, 999))
		fmt.Println(root.Hex())
		for {
			time.Sleep(time.Hour)
		}
	}
	dir := t.TempDir()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(executable, "-test.run=^TestKillRecovery$", "-test.timeout=30s")
	cmd.Env = append(os.Environ(), "PRUNEDFFI_CRASH_CHILD="+dir)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cmd.Process.Kill() })
	scanner := bufio.NewScanner(stdout)
	if !scanner.Scan() {
		cmd.Wait()
		t.Fatalf("child failed: %s", stderr.String())
	}
	root := common.HexToHash(scanner.Text())
	if root == (common.Hash{}) {
		t.Fatalf("bad child root: %s", scanner.Text())
	}
	if err = cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err = cmd.Wait(); err == nil {
		t.Fatal("child exited normally")
	}
	d := testDB(t, Config{Dir: dir, JournalLimit: 5000})
	if got := head(t, d); got.root != root || got.height != 99 {
		t.Fatalf("wrong recovered head %d:%s", got.height, got.root)
	}
	checkBalance(t, d, root, 1, 100)
	acceptBlock(t, d, proposeBlock(t, d, head(t, d), 200, accountOp(1, 101)))
}

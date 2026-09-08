package pruned

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
	"github.com/ava-labs/libevm/rlp"
	"github.com/holiman/uint256"
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
func accountOp(t *testing.T, n, balance uint64) operation {
	t.Helper()
	h := testHash(n)
	key := append(bytes.Clone(h[:]), 0)
	row := struct {
		Nonce    uint64
		Balance  *uint256.Int
		CodeHash []byte
	}{0, uint256.NewInt(balance), types.EmptyCodeHash[:]}
	v, err := rlp.EncodeToBytes(row)
	if err != nil {
		t.Fatal(err)
	}
	return operation{key, v}
}
func storageOp(owner, slot uint64, value byte) operation {
	a, s := testHash(owner), testHash(slot)
	k := append(bytes.Clone(a[:]), 1)
	k = append(k, s[:]...)
	return operation{k, []byte{value}}
}
func proposeBlock(t *testing.T, d *DB, parent *revision, block, height uint64, ops ...operation) *revision {
	t.Helper()
	root, err := d.propose(parent, ops)
	if err != nil {
		t.Fatal(err)
	}
	h := testHash(block)
	if err = d.Update(root, parent.root, height, nil, nil, stateconf.WithTrieDBUpdatePayload(parent.block, h)); err != nil {
		t.Fatal(err)
	}
	return d.blocks[h]
}
func acceptBlock(t *testing.T, d *DB, r *revision) {
	t.Helper()
	if err := d.Commit(r.root, false); err != nil {
		t.Fatal(err)
	}
}
func checkValue(t *testing.T, d *DB, r *revision, op operation) {
	t.Helper()
	v, err := d.get(r, op.key)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(v, op.value) {
		t.Fatalf("key %x: got %x, want %x", op.key, v, op.value)
	}
}

func TestPendingBranchesAndPruning(t *testing.T) {
	d := testDB(t, Config{Retain: 3})
	initial := d.current
	a := accountOp(t, 1, 10)
	g := proposeBlock(t, d, initial, 100, 0, a)
	acceptBlock(t, d, g)
	b := accountOp(t, 1, 20)
	c := accountOp(t, 1, 30)
	left := proposeBlock(t, d, g, 101, 1, b)
	right := proposeBlock(t, d, g, 102, 1, c)
	child := proposeBlock(t, d, left, 103, 2, storageOp(1, 1, 9))
	checkValue(t, d, g, a)
	checkValue(t, d, left, b)
	checkValue(t, d, right, c)
	acceptBlock(t, d, left)
	checkValue(t, d, g, a)
	checkValue(t, d, child, b)
	if _, err := d.get(right, a.key); !errors.Is(err, ErrPruned) {
		t.Fatalf("rejected branch readable: %v", err)
	}
	acceptBlock(t, d, child)
	empty := proposeBlock(t, d, child, 104, 3)
	acceptBlock(t, d, empty)
	if _, err := d.open(g.root); !errors.Is(err, ErrPruned) {
		t.Fatalf("old root retained: %v", err)
	}
	checkValue(t, d, d.current, b)
}

func TestDeletionRecreationAcrossAcceptanceAndCheckpoint(t *testing.T) {
	d := testDB(t, Config{})
	a := accountOp(t, 1, 10)
	slot := storageOp(1, 1, 9)
	g := proposeBlock(t, d, d.current, 100, 0, a, slot)
	acceptBlock(t, d, g)
	removed := operation{a.key, nil}
	newAccount := accountOp(t, 1, 25)
	recreated := proposeBlock(t, d, g, 101, 1, removed, newAccount)
	checkValue(t, d, recreated, operation{slot.key, nil})
	checkValue(t, d, g, slot)
	acceptBlock(t, d, recreated)
	checkValue(t, d, g, slot)
	checkValue(t, d, d.current, operation{slot.key, nil})
	if err := d.checkpoint(); err != nil {
		t.Fatal(err)
	}
	checkValue(t, d, g, slot)
	checkValue(t, d, d.current, operation{slot.key, nil})
	// A proposal based on the retained pre-checkpoint state must also hash.
	if _, err := d.propose(g, []operation{accountOp(t, 1, 50)}); err != nil {
		t.Fatal(err)
	}
	root := d.current.root
	cfg := d.cfg
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := testDB(t, cfg)
	if reopened.current.root != root {
		t.Fatal("restart changed root")
	}
	checkValue(t, reopened, reopened.current, newAccount)
	checkValue(t, reopened, reopened.current, operation{slot.key, nil})
}

func TestRecoveryAndCheckpointPruneJournal(t *testing.T) {
	d := testDB(t, Config{JournalLimit: 700, Retain: 4})
	for n := uint64(0); n < 35; n++ {
		r := proposeBlock(t, d, d.current, 100+n, n, accountOp(t, 1, n+1), storageOp(1, n+1, byte(n+1)))
		acceptBlock(t, d, r)
	}
	if d.gen <= 2 {
		t.Fatal("no repeated checkpoints")
	}
	if d.walBytes >= d.cfg.JournalLimit {
		t.Fatal("unbounded recovery journal")
	}
	root, block, height := d.current.root, d.current.block, d.current.height
	cfg := d.cfg
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := testDB(t, cfg)
	if reopened.current.root != root || reopened.current.block != block || reopened.current.height != height {
		t.Fatal("recovered wrong head")
	}
	for n := uint64(0); n < 35; n++ {
		checkValue(t, reopened, reopened.current, storageOp(1, n+1, byte(n+1)))
	}
	files, err := filepath.Glob(filepath.Join(cfg.Dir, "run.*"))
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 {
		t.Fatalf("retained old state files: %v", files)
	}
	r := proposeBlock(t, reopened, reopened.current, 200, height+1, accountOp(t, 1, 100))
	acceptBlock(t, reopened, r)
}

func TestIncompleteJournalTailAndChecksumFailure(t *testing.T) {
	for _, partial := range []bool{true, false} {
		t.Run(map[bool]string{true: "partial", false: "checksum"}[partial], func(t *testing.T) {
			d := testDB(t, Config{})
			r := proposeBlock(t, d, d.current, 100, 0, accountOp(t, 1, 10))
			acceptBlock(t, d, r)
			cfg, root := d.cfg, d.current.root
			if err := d.Close(); err != nil {
				t.Fatal(err)
			}
			p := filepath.Join(cfg.Dir, "JOURNAL")
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
				reopened := testDB(t, cfg)
				if reopened.current.root != root {
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
				if reopened, err := New(cfg); err == nil {
					reopened.Close()
					t.Fatal("accepted corrupted journal")
				}
			}
		})
	}
}

func TestExclusiveStateDirectory(t *testing.T) {
	d := testDB(t, Config{})
	if other, err := New(d.cfg); err == nil {
		other.Close()
		t.Fatal("allowed concurrent database writers")
	}
}

func TestDuplicateRootBlockIdentities(t *testing.T) {
	d := testDB(t, Config{})
	g := proposeBlock(t, d, d.current, 100, 0, accountOp(t, 1, 10))
	acceptBlock(t, d, g)
	left := proposeBlock(t, d, g, 101, 1, accountOp(t, 1, 20))
	right := proposeBlock(t, d, g, 102, 1, accountOp(t, 1, 20))
	if left != right {
		t.Fatal("identical transitions not shared")
	}
	acceptBlock(t, d, left)
	root, err := d.propose(right, []operation{accountOp(t, 1, 30)})
	if err != nil {
		t.Fatal(err)
	}
	if err = d.Update(root, right.root, 2, nil, nil, stateconf.WithTrieDBUpdatePayload(testHash(102), testHash(103))); err != nil {
		t.Fatal(err)
	}
	acceptBlock(t, d, d.blocks[testHash(103)])
}

func TestSecondAliasAfterCheckpointRestart(t *testing.T) {
	d := testDB(t, Config{})
	g := proposeBlock(t, d, d.current, 100, 0, accountOp(t, 1, 10))
	acceptBlock(t, d, g)
	first := proposeBlock(t, d, g, 101, 1, accountOp(t, 1, 20))
	second := proposeBlock(t, d, g, 102, 1, accountOp(t, 1, 20))
	acceptBlock(t, d, second)
	if err := d.checkpoint(); err != nil {
		t.Fatal(err)
	}
	cfg, root := d.cfg, first.root
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := testDB(t, cfg)
	if reopened.current.root != root {
		t.Fatal("checkpoint changed aliased state")
	}
	// subnet-evm supplies the accepted block's identity after locating its
	// persisted root. The second block hash must then work as a parent.
	reopened.SetHashAndHeight(testHash(102), 1)
	r := proposeBlock(t, reopened, reopened.current, 103, 2, accountOp(t, 1, 30))
	acceptBlock(t, reopened, r)
	checkValue(t, reopened, reopened.current, accountOp(t, 1, 30))
}

func TestKillRecovery(t *testing.T) {
	if dir := os.Getenv("PRUNED_CRASH_CHILD"); dir != "" {
		d := testDB(t, Config{Dir: dir, JournalLimit: 5000})
		for n := uint64(0); n < 100; n++ {
			r := proposeBlock(t, d, d.current, 100+n, n, accountOp(t, 1, n+1), storageOp(1, n+1, byte(n+1)))
			acceptBlock(t, d, r)
		}
		root := d.current.root
		// A verified child must disappear after the crash.
		proposeBlock(t, d, d.current, 999, 100, accountOp(t, 1, 999))
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
	cmd.Env = append(os.Environ(), "PRUNED_CRASH_CHILD="+dir)
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
	if d.current.root != root || d.current.height != 99 {
		t.Fatalf("wrong recovered head %d:%s", d.current.height, d.current.root)
	}
	checkValue(t, d, d.current, accountOp(t, 1, 100))
	r := proposeBlock(t, d, d.current, 200, 100, accountOp(t, 1, 101))
	acceptBlock(t, d, r)
}

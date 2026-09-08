package commit

import (
	"bytes"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/crypto"
	"github.com/holiman/uint256"
)

func layerFixture() map[common.Address]*acct {
	m := make(map[common.Address]*acct)
	for i := byte(1); i <= 12; i++ {
		a := &acct{addr: common.BytesToAddress([]byte{i}), nonce: 1, bal: uint256.NewInt(uint64(i)), slots: map[common.Hash]common.Hash{}}
		for j := byte(1); j <= 4; j++ {
			a.slots[common.Hash{j}] = common.Hash{31: i + j}
		}
		m[a.addr] = a
	}
	return m
}

func cloneLayerState(m map[common.Address]*acct) map[common.Address]*acct {
	cloned := make(map[common.Address]*acct, len(m))
	for addr, a := range m {
		copy := *a
		copy.bal = new(uint256.Int).Set(a.bal)
		copy.code = bytes.Clone(a.code)
		copy.slots = make(map[common.Hash]common.Hash, len(a.slots))
		for slot, value := range a.slots {
			copy.slots[slot] = value
		}
		cloned[addr] = &copy
	}
	return cloned
}

func applyLayerWrite(t *testing.T, d *Dirty, key, value []byte) {
	t.Helper()
	if err := d.Apply(key, value); err != nil {
		t.Fatal(err)
	}
}

func applyLayerDiff(t *testing.T, d *Dirty, before, after map[common.Address]*acct) {
	t.Helper()
	for addr := range before {
		if after[addr] == nil {
			applyLayerWrite(t, d, acctKey(addr), nil)
		}
	}
	for addr, a := range after {
		old := before[addr]
		if old == nil || !bytes.Equal(acctVal(old), acctVal(a)) {
			applyLayerWrite(t, d, acctKey(addr), acctVal(a))
		}
		if old != nil {
			for slot := range old.slots {
				if _, ok := a.slots[slot]; !ok {
					applyLayerWrite(t, d, slotKey(addr, slot), nil)
				}
			}
		}
		for slot, value := range a.slots {
			if old == nil || old.slots[slot] != value {
				applyLayerWrite(t, d, slotKey(addr, slot), common.TrimLeftZeroes(value[:]))
			}
		}
	}
}

func checkLayerRoot(t *testing.T, d *Dirty, m map[common.Address]*acct) common.Hash {
	t.Helper()
	got, err := d.Root()
	if err != nil {
		t.Fatal(err)
	}
	if want := refRoot(t, refDatabase(), m); got != want {
		t.Fatalf("layer root %x, statedb root %x", got, want)
	}
	return got
}

func TestLayerSiblingRootsAndAcceptance(t *testing.T) {
	rolled := layerFixture()
	flat := flatten(rolled)
	_, f := rollTemp(t, flat)
	accepted := NewDirty(f, flat.Seek)
	base := cloneLayerState(rolled)
	addr := common.BytesToAddress([]byte{1})
	base[addr].nonce++
	base[addr].slots[common.Hash{1}] = common.Hash{31: 70}
	applyLayerDiff(t, accepted, rolled, base)
	root := checkLayerRoot(t, accepted, base)

	leftState, rightState := cloneLayerState(base), cloneLayerState(base)
	leftState[addr].nonce = 11
	leftState[addr].slots[common.Hash{2}] = common.Hash{31: 81}
	delete(leftState, common.BytesToAddress([]byte{2}))
	rightState[addr].nonce = 22
	rightState[addr].slots[common.Hash{2}] = common.Hash{31: 82}
	delete(rightState[addr].slots, common.Hash{3})
	left, right := NewLayer(root, accepted), NewLayer(root, accepted)
	applyLayerDiff(t, left, base, leftState)
	applyLayerDiff(t, right, base, rightState)
	leftRoot := checkLayerRoot(t, left, leftState)
	checkLayerRoot(t, right, rightState)
	if unchanged := checkLayerRoot(t, accepted, base); unchanged != root {
		t.Fatal("siblings changed the accepted root")
	}

	childState := cloneLayerState(leftState)
	childState[addr].slots[common.Hash{4}] = common.Hash{31: 90}
	child := NewLayer(leftRoot, left)
	applyLayerDiff(t, child, leftState, childState)
	checkLayerRoot(t, child, childState)

	if err := accepted.ApplyNodes(leftRoot, left.NodeChanges()); err != nil {
		t.Fatal(err)
	}
	checkLayerRoot(t, accepted, leftState)
	applyLayerDiff(t, accepted, leftState, childState)
	checkLayerRoot(t, accepted, childState)
}

func TestLayerDestroyAndRecreate(t *testing.T) {
	for _, separate := range []bool{false, true} {
		t.Run(fmt.Sprintf("separate_layers=%t", separate), func(t *testing.T) {
			base := layerFixture()
			flat := flatten(base)
			root, f := rollTemp(t, flat)
			accepted := NewDirty(f, flat.Seek)
			layer := NewLayer(root, accepted)
			addr := common.BytesToAddress([]byte{1})
			applyLayerWrite(t, layer, acctKey(addr), nil)
			if separate {
				deleted := cloneLayerState(base)
				delete(deleted, addr)
				root = checkLayerRoot(t, layer, deleted)
				if err := accepted.ApplyNodes(root, layer.NodeChanges()); err != nil {
					t.Fatal(err)
				}
				layer = NewLayer(root, accepted)
			}
			recreated := cloneLayerState(base)
			a := recreated[addr]
			a.nonce = 3
			a.slots = map[common.Hash]common.Hash{common.Hash{8}: common.Hash{31: 99}}
			applyLayerWrite(t, layer, acctKey(addr), acctVal(a))
			applyLayerWrite(t, layer, slotKey(addr, common.Hash{8}), []byte{99})
			root = checkLayerRoot(t, layer, recreated)
			if err := accepted.ApplyNodes(root, layer.NodeChanges()); err != nil {
				t.Fatal(err)
			}
			checkLayerRoot(t, accepted, recreated)

			// A later write at an old slot must preserve only the new storage.
			next := cloneLayerState(recreated)
			next[addr].slots[common.Hash{1}] = common.Hash{31: 100}
			child := NewLayer(root, accepted)
			applyLayerDiff(t, child, recreated, next)
			checkLayerRoot(t, child, next)
		})
	}
}

type layerReaderFunc func(common.Hash, []byte, common.Hash) ([]byte, error)

func (f layerReaderFunc) Node(owner common.Hash, path []byte, hash common.Hash) ([]byte, error) {
	return f(owner, path, hash)
}

func TestLayerReadsRolledLeaves(t *testing.T) {
	base := layerFixture()
	flat := flatten(base)
	root, f := rollTemp(t, flat)
	accepted := NewDirty(f, flat.Seek)
	var accountLeaves, storageLeaves atomic.Int32
	parent := layerReaderFunc(func(owner common.Hash, path []byte, hash common.Hash) ([]byte, error) {
		blob, err := accepted.Node(owner, path, hash)
		if err == nil && crypto.Keccak256Hash(blob) != hash {
			t.Errorf("parent returned the wrong hash at %x/%x", owner, path)
		}
		if _, ok := f.Node(owner, path); !ok {
			if owner == (common.Hash{}) {
				accountLeaves.Add(1)
			} else {
				storageLeaves.Add(1)
			}
		}
		return blob, err
	})
	layer := NewLayer(root, parent)
	next := cloneLayerState(base)
	for _, a := range next {
		a.nonce++
		a.slots[common.Hash{1}] = common.Hash{31: 101}
		delete(a.slots, common.Hash{2})
	}
	applyLayerDiff(t, layer, base, next)
	checkLayerRoot(t, layer, next)
	if accountLeaves.Load() == 0 || storageLeaves.Load() == 0 {
		t.Fatalf("expected rolled account and storage leaves, read %d and %d", accountLeaves.Load(), storageLeaves.Load())
	}
}

func TestLayerNodeChangesOwnership(t *testing.T) {
	parent := layerReaderFunc(func(common.Hash, []byte, common.Hash) ([]byte, error) {
		t.Fatal("retained nodes must shadow the parent")
		return nil, nil
	})
	layer := NewLayer(types.EmptyRootHash, parent)
	owner := common.Hash{1}
	for _, length := range []int{0, 1, 2, 15, 16, 64} {
		path := make([]byte, length)
		for i := range path {
			path[i] = byte(i % 16)
		}
		blob := []byte{byte(length + 1)}
		if length == 2 {
			blob = nil
		}
		change := NodeChange{Owner: owner, Path: bytes.Clone(path), Blob: bytes.Clone(blob)}
		if err := layer.ApplyNodes(types.EmptyRootHash, []NodeChange{change}); err != nil {
			t.Fatal(err)
		}
		if len(change.Path) > 0 {
			change.Path[0] ^= 15
		}
		if len(change.Blob) > 0 {
			change.Blob[0] ^= 255
		}
		got, err := layer.Node(owner, path, common.Hash{})
		if err != nil || !bytes.Equal(got, blob) {
			t.Fatalf("ApplyNodes retained caller bytes at path %x: %x, %v", path, got, err)
		}
	}
	changes := layer.NodeChanges()
	if len(changes) != 6 {
		t.Fatalf("got %d node changes, want 6", len(changes))
	}
	var tombstones int
	for _, change := range changes {
		path, blob := bytes.Clone(change.Path), bytes.Clone(change.Blob)
		if change.Owner != owner {
			t.Fatalf("wrong owner %x", change.Owner)
		}
		if len(blob) == 0 {
			tombstones++
		}
		if len(change.Path) > 0 {
			change.Path[0] ^= 15
		}
		if len(change.Blob) > 0 {
			change.Blob[0] ^= 255
		}
		got, err := layer.Node(owner, path, common.Hash{})
		if err != nil || !bytes.Equal(got, blob) {
			t.Fatalf("NodeChanges exposed retained bytes at path %x: %x, %v", path, got, err)
		}
	}
	if tombstones != 1 {
		t.Fatalf("got %d tombstones, want 1", tombstones)
	}
}

func TestLayerApplyNodesRejectsQueuedWrites(t *testing.T) {
	_, f := rollTemp(t, nil)
	d := NewDirty(f, rows(nil).Seek)
	a := &acct{addr: common.Address{1}, nonce: 1, bal: uint256.NewInt(5)}
	applyLayerWrite(t, d, acctKey(a.addr), acctVal(a))
	if err := d.ApplyNodes(common.Hash{1}, []NodeChange{{Blob: []byte{1}}}); err == nil {
		t.Fatal("ApplyNodes accepted queued writes")
	}
	if d.root != types.EmptyRootHash || len(d.NodeChanges()) != 0 {
		t.Fatal("rejected ApplyNodes changed the overlay")
	}
	checkLayerRoot(t, d, map[common.Address]*acct{a.addr: a})
}

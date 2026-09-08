package main

import (
	"encoding/binary"
	"path/filepath"
	"testing"

	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/libevm/crypto"

	"github.com/containerman17/avalanche-s3-archival-node/dist"
	"github.com/containerman17/avalanche-s3-archival-node/store"
)

// Three blocks (2, 0 and 1 txs; block 1 bare, the others templated) in one
// local run: the containers come back byte for byte, the index answers each
// id with its height and nothing else, and heights outside the run are errors.
func TestArchiveServesRun(t *testing.T) {
	cas, err := dist.Local(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	tmpl := func(a, b, c string) []byte {
		p := binary.BigEndian.AppendUint32(nil, uint32(len(a)))
		p = append(p, a...)
		p = binary.BigEndian.AppendUint32(p, uint32(len(b)))
		return append(append(p, b...), c...)
	}
	type blk struct {
		hdr, pvm []byte
		txs      [][]byte
	}
	blocks := []blk{
		{hdr: []byte{0xc2, 1, 2}, txs: [][]byte{{0xc1, 1}, {0xc1, 2}}},
		{hdr: []byte("hdr2"), pvm: tmpl("A2", "B2", "C2")},
		{hdr: []byte("hdr3"), pvm: tmpl("A3", "B3", "C3"), txs: [][]byte{[]byte("tx3")}},
	}
	w, err := store.NewRunWriter(filepath.Join(cas.LocalDir(), "run.tmp"), [32]byte{}, 0)
	if err != nil {
		t.Fatal(err)
	}
	set := func(k, v []byte) {
		if err := w.Set(k, v); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Begin(store.SecChain); err != nil {
		t.Fatal(err)
	}
	var firsts []uint64
	txn := uint64(0)
	for i, b := range blocks {
		firsts = append(firsts, txn)
		v := binary.BigEndian.AppendUint64(nil, txn)
		set(store.BlkKey(uint64(i+1)), binary.BigEndian.AppendUint32(v, uint32(len(b.txs))))
		txn += uint64(len(b.txs)) + 1
	}
	for i, b := range blocks {
		set(store.HdrKey(uint64(i+1)), b.hdr)
	}
	for i, b := range blocks {
		set(store.PvmKey(uint64(i+1)), b.pvm)
	}
	for i, b := range blocks {
		for j, tx := range b.txs {
			set(store.TxKey(firsts[i]+uint64(j)), tx)
		}
	}
	for _, s := range []store.Section{store.SecChain, store.SecState, store.SecLookup} {
		if s != store.SecChain {
			if err := w.Begin(s); err != nil {
				t.Fatal(err)
			}
		}
		if err := w.End(); err != nil {
			t.Fatal(err)
		}
	}
	name, _, err := w.Finish(cas, 0, txn, 1, 3)
	if err != nil {
		t.Fatal(err)
	}
	run, err := store.OpenRunVersion(cas, name, store.StorageVersion)
	if err != nil {
		t.Fatal(err)
	}
	defer run.Close()

	a := &archive{win: map[uint64]*window{}, runs: []runRange{{store.RunRef{FromHeight: 1, ToHeight: 3, Name: name}, run}}}
	idx := filepath.Join(t.TempDir(), "index")
	if err := a.buildIndex(idx, 3); err != nil {
		t.Fatal(err)
	}
	if a.idx, err = loadIndex(idx, 3); err != nil {
		t.Fatal(err)
	}
	for i, b := range blocks {
		h := uint64(i + 1)
		want, err := store.Reassemble(b.pvm, b.hdr, b.txs)
		if err != nil {
			t.Fatal(err)
		}
		got, err := a.ContainerAt(h)
		if err != nil || string(got) != string(want) {
			t.Fatalf("ContainerAt(%d) = %x, %v; want %x", h, got, err, want)
		}
		// None of these fake containers parses as a proposervm block, so the
		// id is the eth block hash: keccak of the header row.
		id := ids.ID(crypto.Keccak256(b.hdr))
		if hh, ok, err := a.HeightByContainerID(id[:]); !ok || hh != h || err != nil {
			t.Fatalf("HeightByContainerID(block %d) = %d, %v, %v", h, hh, ok, err)
		}
		if got, err := a.idAt(h); err != nil || got != id {
			t.Fatalf("idAt(%d) = %s, %v; want %x", h, got, err, id)
		}
	}
	if _, ok, _ := a.HeightByContainerID(make([]byte, 32)); ok {
		t.Fatal("unknown id found")
	}
	for _, h := range []uint64{0, 4} {
		if _, err := a.ContainerAt(h); err == nil {
			t.Fatalf("ContainerAt(%d) succeeded", h)
		}
	}
	if a.windows.Load() != 1 || a.served.Load() != 3 {
		t.Fatalf("windows=%d served=%d", a.windows.Load(), a.served.Load())
	}
}

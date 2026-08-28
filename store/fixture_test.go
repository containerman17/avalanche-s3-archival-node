package store

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/types"

	"github.com/containerman17/avalanche-s3-archival-node/dist"
)

// fixtureBlock is block() with REAL receipts: every log is encoded the way
// the executor stores it, so a reader (or a migrator) can decode emitters and
// topics back out of the `rcpt/` row. Emitters, senders and topic values
// repeat across blocks so posting groups span runs.
func fixtureBlock(height uint64, n int) *BlockWrite {
	b := block(height, n)
	for i := range b.Txs {
		tx := &b.Txs[i]
		k := byte(height + uint64(i))
		emitter := common.BytesToAddress(addr(10 + k%3))
		holder := common.BytesToHash(addr(20 + k%4))
		other := common.BytesToHash(addr(20 + (k+1)%4))
		logs := []*types.Log{
			{Address: emitter, Topics: []common.Hash{common.BytesToHash(hash32(0xA0)), holder, other}, Data: []byte{k}},
		}
		if k%2 == 0 {
			logs = append(logs, &types.Log{Address: common.BytesToAddress(addr(30)), Topics: []common.Hash{common.BytesToHash(hash32(0xB0)), other, holder, common.BytesToHash(hash32(k))}})
		}
		if k%5 == 0 {
			logs = append(logs, &types.Log{Address: common.BytesToAddress(addr(31))}) // no topics
		}
		tx.Receipt = EncodeTxReceipt(&types.Receipt{Status: 1, GasUsed: 21000, Logs: logs}, 21000*uint64(i+1))
		tx.Logs = nil
		for _, l := range logs {
			lw := LogWrite{Emitter: l.Address.Bytes()}
			for _, tp := range l.Topics {
				lw.Topics = append(lw.Topics, tp.Bytes())
			}
			tx.Logs = append(tx.Logs, lw)
		}
		tx.Sender = addr(1 + k%2)
		if k%7 == 0 {
			tx.To, tx.Created = nil, addr(40+k)
		}
	}
	return b
}

// fixtureRuns is the shape of the committed fixture: one terminal run (the
// first MergeFanIn L0 runs merged), two L0 runs behind it, and two blocks
// left in the window. Three blocks of two txs per run, as in buildCorpus.
const fixtureRuns = MergeFanIn + 2

func writeFixture(t *testing.T, dir string) *DB {
	t.Helper()
	cas, err := dist.Local(dir)
	if err != nil {
		t.Fatal(err)
	}
	db, err := Open(dir, cas, [32]byte{1})
	if err != nil {
		t.Fatal(err)
	}
	db.scaleTriggers(FlushTxs, FlushBlocks, testTerminalTxs)
	h := uint64(0)
	for r := 0; r < fixtureRuns; r++ {
		for i := 0; i < 3; i++ {
			if err := db.WriteBlock(fixtureBlock(h, 2)); err != nil {
				t.Fatal(err)
			}
			h++
		}
		if err := db.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.WaitMerge(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := db.WriteBlock(fixtureBlock(h, 2)); err != nil {
			t.Fatal(err)
		}
		h++
	}
	return db
}

// TestWriteFixture regenerates store/testdata/v<StorageVersion> from the
// CURRENT format. It ran once under each storage version that has a
// migration source and the output is committed; it only runs again on
// request.
func TestWriteFixture(t *testing.T) {
	if os.Getenv("EPOCHDB_WRITE_FIXTURE") == "" {
		t.Skip("set EPOCHDB_WRITE_FIXTURE=1 to regenerate")
	}
	dir := filepath.Join("testdata", fmt.Sprintf("v%d", StorageVersion))
	os.RemoveAll(dir)
	db := writeFixture(t, dir)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestPostingFamilies: the four v2 families answer over runs and window, with
// the payload bits the writer promised.
func TestPostingFamilies(t *testing.T) {
	db := writeFixture(t, t.TempDir())
	defer db.Close()
	sigA, sigB := hash32(0xA0), hash32(0xB0)
	holder := common.BytesToHash(addr(21)).Bytes() // k%4==1 at pos1 of A, pos2 of B (k even: never), pos1 of B when other==21 (k%4==0)
	count := func(prefix []byte, mask byte) (n int) {
		if err := db.Postings(prefix, 0, 1<<62, func(_ uint64, p byte) bool {
			if mask == 0 || p&mask != 0 {
				n++
			}
			return true
		}); err != nil {
			t.Fatal(err)
		}
		return n
	}
	// 56 blocks x 2 txs = 112 txs, k = height+i.
	if n := count(SigGroup(sigA), 0); n != 112 {
		t.Fatalf("sig A: %d", n)
	}
	if n := count(SigGroup(sigB), 0); n != 56 {
		t.Fatalf("sig B: %d", n)
	}
	if n := count(ELogGroup(addr(31), zeroHash[:]), 0); n == 0 {
		t.Fatal("topic-less log has no elog/ entry under the zero hash")
	}
	pos1A := count(TValGroup(holder, sigA), Pos1)
	pos2A := count(TValGroup(holder, sigA), Pos2)
	if pos1A == 0 || pos2A == 0 || count(TValGroup(holder, sigA), Pos3) != 0 {
		t.Fatalf("tval positions: %d %d", pos1A, pos2A)
	}
	if n := count(TValPrefix(holder), 0); n != count(TValGroup(holder, sigA), 0)+count(TValGroup(holder, sigB), 0) {
		t.Fatalf("prefix scan %d is not the sum of its groups", n)
	}
	var groups []string
	seen := map[string]bool{}
	if err := db.Groups(TValPrefix(holder), func(g []byte) bool {
		if !seen[string(g)] {
			seen[string(g)] = true
			groups = append(groups, string(g))
		}
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if len(groups) != 2 {
		t.Fatalf("groups: %d", len(groups))
	}
	// Desc equals asc reversed, and a cursor page is exact.
	var asc, desc []uint64
	db.Postings(AddrPrefix(addr(1)), 0, 1<<62, func(n uint64, _ byte) bool { asc = append(asc, n); return true })
	db.PostingsDesc(AddrPrefix(addr(1)), 0, 1<<62, func(n uint64, _ byte) bool { desc = append(desc, n); return true })
	if len(asc) != 56 || len(desc) != 56 || asc[0] != desc[55] || asc[55] != desc[0] {
		t.Fatalf("asc %v desc %v", asc, desc)
	}
	var page []uint64
	db.Postings(AddrPrefix(addr(1)), asc[10], asc[12], func(n uint64, _ byte) bool { page = append(page, n); return true })
	if len(page) != 3 || page[0] != asc[10] {
		t.Fatalf("page %v", page)
	}
}

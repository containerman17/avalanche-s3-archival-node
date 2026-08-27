package store

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/types"

	"github.com/containerman17/epochdb/dist"
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
		// The producer dedups per tx (exec/executor.go), the store insists.
		tx.LogAddrs, tx.Topics = nil, nil
		seenA, seenT := map[common.Address]bool{}, map[common.Hash]bool{}
		for _, l := range logs {
			if !seenA[l.Address] {
				seenA[l.Address] = true
				tx.LogAddrs = append(tx.LogAddrs, l.Address.Bytes())
			}
			for _, tp := range l.Topics {
				if !seenT[tp] {
					seenT[tp] = true
					tx.Topics = append(tx.Topics, tp.Bytes())
				}
			}
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

// TestWriteV1Fixture regenerates store/testdata/v1 from the CURRENT format.
// It ran once under storage version 1 and the output is committed; it only
// runs again on request.
func TestWriteV1Fixture(t *testing.T) {
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

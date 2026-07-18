package exec

import (
	"testing"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/types"

	"github.com/containerman17/epochdb/state"
)

func TestLogsFrameRoundtripAndStore(t *testing.T) {
	a1 := common.HexToAddress("0x01")
	a2 := common.HexToAddress("0x02")
	t1 := common.HexToHash("0x11")
	t2 := common.HexToHash("0x22")
	logs := []*types.Log{
		{Address: a1, Topics: []common.Hash{t1, t2}},
		{Address: a2, Topics: []common.Hash{t1}}, // dup topic
		{Address: a1, Topics: nil},               // dup addr, no topics
	}
	rec := encodeLogsFrame(logs)
	addrs, topics, err := decodeLogsFrame(rec)
	if err != nil {
		t.Fatal(err)
	}
	if len(addrs) != 2 || addrs[0] != a1 || addrs[1] != a2 {
		t.Fatalf("addrs: %v", addrs)
	}
	if len(topics) != 2 || topics[0] != t1 || topics[1] != t2 {
		t.Fatalf("topics: %v", topics)
	}

	// No-logs path: nil record, nothing stored, absence readable.
	if encodeLogsFrame(nil) != nil {
		t.Fatal("empty block must encode to nil")
	}

	dir := t.TempDir()
	s, err := state.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.AppendLogs(7, rec); err != nil {
		t.Fatal(err)
	}
	if err := s.SetLogsStart(7); err != nil {
		t.Fatal(err)
	}
	if err := s.SetLogsStart(99); err != nil { // write-once
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s, err = state.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	got, ok, err := s.LogsRecord(7)
	if err != nil || !ok {
		t.Fatalf("logs record: ok=%v err=%v", ok, err)
	}
	ra, rt, err := decodeLogsFrame(got)
	if err != nil || len(ra) != 2 || len(rt) != 2 {
		t.Fatalf("roundtrip after reopen: %v %v %v", ra, rt, err)
	}
	if _, ok, _ := s.LogsRecord(8); ok {
		t.Fatal("block without logs must have no record")
	}
	if start, ok := s.LogsStart(); !ok || start != 7 {
		t.Fatalf("logs.start: %d ok=%v want 7 (write-once)", start, ok)
	}
}

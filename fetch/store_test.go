package fetch

import (
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ava-labs/avalanchego/ids"
)

func fakeContainer(h uint64, fill byte, n int) (parsedContainer, []byte) {
	raw := make([]byte, n)
	for i := range raw {
		raw[i] = fill
	}
	var id ids.ID
	binary.BigEndian.PutUint64(id[:8], h)
	id[8] = fill
	return parsedContainer{containerID: id, blockNumber: h, blockHash: id}, raw
}

func TestStoreSegmentsRebuildAndReaders(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	// Contiguous run spanning the bucket 0 / bucket 1 boundary, plus a
	// detached higher block in bucket 2.
	heights := []uint64{99_998, 99_999, 100_000, 100_001, 250_000}
	for i, h := range heights {
		p, raw := fakeContainer(h, byte(0xa0+i), 100+i)
		if err := s.Append(p, raw); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Destination file is a pure function of height.
	for _, name := range []string{
		"arrival_00000.log", "index_00000.log",
		"arrival_00001.log", "index_00001.log",
		"arrival_00002.log", "index_00002.log",
	} {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Fatalf("missing segment file %s: %v", name, err)
		}
	}

	s, err = OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if got := s.Count(); got != 5 {
		t.Fatalf("count=%d want 5", got)
	}
	if head, ok := s.Head(); !ok || head != 250_000 {
		t.Fatalf("head=%d,%v want 250000,true", head, ok)
	}
	if lo := s.LowestContiguous(100_001, 0); lo != 99_998 {
		t.Fatalf("lowestContiguous=%d want 99998", lo)
	}
	raw, ok, err := s.GetByHeight(100_000)
	if err != nil || !ok || len(raw) != 102 || raw[0] != 0xa2 {
		t.Fatalf("GetByHeight(100000)=%v,%v,%v", len(raw), ok, err)
	}
	if _, ok, _ := s.GetByHeight(100_002); ok {
		t.Fatal("height 100002 should be missing")
	}
	p, _ := fakeContainer(99_999, 0xa1, 0)
	if h, ok := s.HeightOf(p.containerID); !ok || h != 99_999 {
		t.Fatalf("HeightOf=%d,%v want 99999,true", h, ok)
	}
}

func TestStoreTornTailTruncationPerSegment(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	// Two blocks in bucket 0, three in bucket 1.
	for _, h := range []uint64{1, 2, 100_001, 100_002, 100_003} {
		p, raw := fakeContainer(h, byte(h), 64)
		if err := s.Append(p, raw); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Simulate a crash that tore bucket 1's last arrival write: its index
	// record now points past the arrival file's end.
	arrival1 := filepath.Join(dir, "arrival_00001.log")
	st, err := os.Stat(arrival1)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(arrival1, st.Size()-10); err != nil {
		t.Fatal(err)
	}
	// And a torn index append on top: a partial trailing record.
	idx, err := os.OpenFile(filepath.Join(dir, "index_00001.log"), os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := idx.Write(make([]byte, 30)); err != nil {
		t.Fatal(err)
	}
	idx.Close()

	s, err = OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got := s.Count(); got != 4 {
		t.Fatalf("count after torn tail=%d want 4", got)
	}
	if _, ok, _ := s.GetByHeight(100_003); ok {
		t.Fatal("torn height 100003 should have been truncated away")
	}
	// Bucket 0 untouched, bucket 1's earlier records intact.
	for _, h := range []uint64{1, 2, 100_001, 100_002} {
		if raw, ok, err := s.GetByHeight(h); err != nil || !ok || len(raw) != 64 {
			t.Fatalf("GetByHeight(%d)=%v,%v,%v", h, len(raw), ok, err)
		}
	}
	// Both files of bucket 1 truncated to the last consistent record.
	idxBytes, err := os.ReadFile(filepath.Join(dir, "index_00001.log"))
	if err != nil {
		t.Fatal(err)
	}
	if len(idxBytes) != 2*indexRecSize {
		t.Fatalf("index_00001 size=%d want %d", len(idxBytes), 2*indexRecSize)
	}
	lastRec := idxBytes[indexRecSize:]
	lastEnd := int64(binary.BigEndian.Uint64(lastRec[72:80])) + int64(binary.BigEndian.Uint32(lastRec[80:84]))
	ast, _ := os.Stat(arrival1)
	if ast.Size() != lastEnd {
		t.Fatalf("arrival_00001 size=%d want %d (end of last consistent record)", ast.Size(), lastEnd)
	}
	// Store must accept re-appends after truncation.
	p, raw := fakeContainer(100_003, 3, 64)
	if err := s.Append(p, raw); err != nil {
		t.Fatal(err)
	}
	if raw, ok, _ := s.GetByHeight(100_003); !ok || len(raw) != 64 {
		t.Fatal("re-append after truncation failed")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSubscribeAscending(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	p1, raw1 := fakeContainer(1, 1, 10)
	if err := s.Append(p1, raw1); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ch := s.Subscribe(ctx, 1)

	ev := <-ch
	if ev.BlockNumber != 1 || len(ev.Raw) != 10 || ev.ContainerID != p1.containerID {
		t.Fatalf("event 1: %+v", ev)
	}
	// Height 2 lands after the subscriber is already waiting.
	go func() {
		time.Sleep(100 * time.Millisecond)
		p2, raw2 := fakeContainer(2, 2, 11)
		s.Append(p2, raw2)
	}()
	ev = <-ch
	if ev.BlockNumber != 2 || len(ev.Raw) != 11 {
		t.Fatalf("event 2: %+v", ev)
	}
	cancel()
	for range ch { // drain until close
	}
}

// TestWalkSpanStopsAtSealedFloor: the raw below the sealed end is gone (seal
// deleted it), so a walk from the tip must stop at the bottom of the RETAINED
// run instead of dropping off it and re-fetching durable history.
func TestWalkSpanStopsAtSealedFloor(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	const sealedEnd = 500
	for h := uint64(sealedEnd + 1); h <= sealedEnd+10; h++ {
		p, raw := fakeContainer(h, 0xb0, 64)
		if err := s.Append(p, raw); err != nil {
			t.Fatal(err)
		}
	}
	tip, _ := fakeContainer(sealedEnd+10, 0xb0, 64)
	f := &Fetcher{store: s, dispatchErrCh: make(chan error, 1)}

	// Unfloored, the walk falls off the bottom of the run and goes looking for
	// the parent of the lowest retained block (here it chokes on the synthetic
	// container, which is exactly one step further than it should ever get).
	if err := f.walkSpan(context.Background(), tip.containerID, 0); err == nil {
		t.Fatal("unfloored walk stopped at the retained run; the floor test proves nothing")
	}
	f.SetFloor(sealedEnd)
	if err := f.walkSpan(context.Background(), tip.containerID, 0); err != nil {
		t.Fatalf("floored walk: %v", err)
	}
	// The floor only ever rises.
	f.SetFloor(1)
	if got := f.floor.Load(); got != sealedEnd {
		t.Fatalf("floor=%d after a lower SetFloor, want %d", got, sealedEnd)
	}
}

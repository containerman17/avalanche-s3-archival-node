package fetch

import (
	"context"
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
	id[0] = fill
	id[1] = byte(h)
	return parsedContainer{containerID: id, blockNumber: h, blockHash: id}, raw
}

func TestStoreRebuildAndReaders(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	for h := uint64(5); h <= 7; h++ {
		p, raw := fakeContainer(h, byte(0xa0+h), 100+int(h))
		if err := s.Append(p, raw); err != nil {
			t.Fatal(err)
		}
	}
	// Non-contiguous higher block.
	p9, raw9 := fakeContainer(9, 0xff, 50)
	if err := s.Append(p9, raw9); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s, err = OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if got := s.Count(); got != 4 {
		t.Fatalf("count=%d want 4", got)
	}
	if head, ok := s.Head(); !ok || head != 9 {
		t.Fatalf("head=%d,%v want 9,true", head, ok)
	}
	if lo := s.LowestContiguous(7); lo != 5 {
		t.Fatalf("lowestContiguous(7)=%d want 5", lo)
	}
	raw, ok, err := s.GetByHeight(6)
	if err != nil || !ok || len(raw) != 106 || raw[0] != 0xa6 {
		t.Fatalf("GetByHeight(6)=%v,%v,%v", len(raw), ok, err)
	}
	if _, ok, _ := s.GetByHeight(8); ok {
		t.Fatal("height 8 should be missing")
	}
	p6, _ := fakeContainer(6, 0xa6, 0)
	if h, ok := s.HeightOf(p6.containerID); !ok || h != 6 {
		t.Fatalf("HeightOf=%d,%v want 6,true", h, ok)
	}
}

func TestStoreTornTailTruncation(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	for h := uint64(1); h <= 3; h++ {
		p, raw := fakeContainer(h, byte(h), 64)
		if err := s.Append(p, raw); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Simulate a crash that tore the last arrival write: index record 3
	// now points past arrival.log's end.
	arrival := filepath.Join(dir, "arrival.log")
	st, err := os.Stat(arrival)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(arrival, st.Size()-10); err != nil {
		t.Fatal(err)
	}
	// And a torn index append on top: a partial trailing record.
	idx, err := os.OpenFile(filepath.Join(dir, "index.log"), os.O_WRONLY|os.O_APPEND, 0o644)
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
	if got := s.Count(); got != 2 {
		t.Fatalf("count after torn tail=%d want 2", got)
	}
	if _, ok, _ := s.GetByHeight(3); ok {
		t.Fatal("height 3 should have been truncated away")
	}
	if raw, ok, err := s.GetByHeight(2); err != nil || !ok || len(raw) != 64 {
		t.Fatalf("GetByHeight(2)=%v,%v,%v", len(raw), ok, err)
	}
	// Both files must be truncated to the last consistent record.
	ist, _ := os.Stat(filepath.Join(dir, "index.log"))
	if ist.Size() != 2*indexRecSize {
		t.Fatalf("index size=%d want %d", ist.Size(), 2*indexRecSize)
	}
	ast, _ := os.Stat(arrival)
	if ast.Size() != 2*(1+64) { // uvarint(64)=1 byte
		t.Fatalf("arrival size=%d want %d", ast.Size(), 2*(1+64))
	}
	// Store must accept appends again after truncation.
	p3, raw3 := fakeContainer(3, 3, 64)
	if err := s.Append(p3, raw3); err != nil {
		t.Fatal(err)
	}
	if raw, ok, _ := s.GetByHeight(3); !ok || len(raw) != 64 {
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
	if ev.BlockNumber != 1 || len(ev.Raw) != 10 {
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
	if _, open := <-ch; open {
		// One buffered event may sneak in; drain until close.
		for range ch {
		}
	}
}

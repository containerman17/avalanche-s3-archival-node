package fetch

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
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

// TestResolveCheckpointsSkipsSealedHistory is the mainnet stage-1 node of
// 2026-08-04: it sealed epoch 2, restarted with --tip-override, and died in the
// checkpoint resolve because sealing had retired the raw staging buckets the
// resolve reads. Those checkpoints are inside sealed history and are not needed
// as walk seeds at all, so the fix is to skip them; a checkpoint above the floor
// that cannot be resolved must still fail loudly.
func TestResolveCheckpointsSkipsSealedHistory(t *testing.T) {
	const (
		fujiNetworkID = 5
		sealedEnd     = 400_000
	)
	cChain, err := ids.FromString("yH8D7ThNJkxmtkuv2jgBa4P1Rn3Qpr4pPr7QYNfcdoS6k6HWp")
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	s, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	f := &Fetcher{store: s, networkID: fujiNetworkID, chainID: cChain, dispatchErrCh: make(chan error, 1)}
	cps := f.Checkpoints()
	if len(cps) == 0 {
		t.Fatal("no embedded checkpoints for the Fuji C-chain; the test proves nothing")
	}

	// Every checkpoint sits deep inside what the seal has covered, and its raw
	// is gone exactly the way seal leaves it: the files unlinked, this process's
	// handles dropped, the RAM index still holding the heights.
	for i, id := range cps {
		if err := s.Append(parsedContainer{containerID: id, blockHash: id, blockNumber: uint64(i) + 1}, []byte("raw")); err != nil {
			t.Fatal(err)
		}
	}
	for _, name := range []string{"arrival_00000.log", "index_00000.log"} {
		if err := os.Remove(filepath.Join(dir, name)); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Retire(sealedEnd); err != nil {
		t.Fatal(err)
	}

	// A cancelled context stands in for "the network is not an option here":
	// anything that tries to resolve a container has to fail.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Unfloored, this is the crash, verbatim.
	if _, err := f.ResolveCheckpoints(ctx); err == nil {
		t.Fatal("unfloored resolve succeeded over retired staging; the floor test proves nothing")
	}

	f.SetFloor(sealedEnd)
	anchors, err := f.ResolveCheckpoints(ctx)
	if err != nil {
		t.Fatalf("resolve after seal: %v", err)
	}
	if len(anchors) != 0 {
		t.Fatalf("%d anchors inside sealed history, want none", len(anchors))
	}

	// The --tip-override anchor itself is still resolved strictly: a container
	// nobody has is an error, not a shrug.
	if _, err := f.ResolveAnchor(ctx, ids.GenerateTestID()); err == nil {
		t.Fatal("a missing tip-override anchor resolved anyway")
	}
}

// TestIndexSurvivesAReadFailure: the startup scan may only truncate on a
// clean or torn END of the sidecar. An index whose arrival file is missing is
// damage, and creating an empty one (which the O_CREATE open used to do) made
// arrivalSize 0 and truncated a whole segment's index away on the spot.
func TestIndexSurvivesAReadFailure(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, h := range []uint64{1, 2, 3} {
		p, raw := fakeContainer(h, byte(h), 64)
		if err := s.Append(p, raw); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	idx := filepath.Join(dir, indexName(0))
	before, err := os.Stat(idx)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(dir, arrivalName(0))); err != nil {
		t.Fatal(err)
	}
	if s, err := OpenStore(dir); err == nil {
		s.Close()
		t.Fatal("an index sidecar with no arrival file opened cleanly")
	}
	after, err := os.Stat(idx)
	if err != nil {
		t.Fatal(err)
	}
	if after.Size() != before.Size() {
		t.Fatalf("index truncated from %d to %d bytes by a failed open", before.Size(), after.Size())
	}
}

// TestFailedIndexWriteKeepsTheArrivalOffsetHonest: the arrival file has no
// O_APPEND, so a write moves the descriptor whether or not the record is
// finished. When the index write then fails, the cached offset must catch up
// with the descriptor, or every later record in the segment is indexed short
// by the failed record's length and decodes as garbage.
func TestFailedIndexWriteKeepsTheArrivalOffsetHonest(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	p1, raw1 := fakeContainer(1, 0x11, 64)
	if err := s.Append(p1, raw1); err != nil {
		t.Fatal(err)
	}

	// A read-only descriptor for the sidecar: writes to it fail, the arrival
	// write ahead of them does not. That is a full disk or an EIO on the
	// sidecar, without needing either.
	sg := s.segs[0]
	ro, err := os.Open(filepath.Join(dir, indexName(0)))
	if err != nil {
		t.Fatal(err)
	}
	good := sg.index
	sg.index = ro
	p2, raw2 := fakeContainer(2, 0x22, 64)
	if err := s.Append(p2, raw2); err == nil {
		t.Fatal("a write to a read-only sidecar reported success")
	}
	sg.index = good
	ro.Close()

	pos, err := sg.arrival.Seek(0, io.SeekCurrent)
	if err != nil {
		t.Fatal(err)
	}
	if sg.arrivalOff != uint64(pos) {
		t.Fatalf("arrivalOff=%d but the descriptor is at %d", sg.arrivalOff, pos)
	}

	// The proof that matters: the next container is indexed where it really
	// lands and reads back byte for byte.
	p3, raw3 := fakeContainer(3, 0x33, 96)
	if err := s.Append(p3, raw3); err != nil {
		t.Fatal(err)
	}
	got, ok, err := s.GetByHeight(3)
	if err != nil || !ok {
		t.Fatalf("GetByHeight(3)=%v,%v", ok, err)
	}
	if !bytes.Equal(got, raw3) {
		t.Fatal("the container after a failed index write decoded to the wrong bytes")
	}
}

// TestSubscribeReportsReadFailure: the poll goroutine returns on any read
// error, closing the channel, which is exactly what a clean shutdown looks
// like. A corrupt container would have ended the block stream and read as
// "the chain stops here", so the failure has to arrive as an event.
func TestSubscribeReportsReadFailure(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	p1, raw1 := fakeContainer(1, 1, 32)
	if err := s.Append(p1, raw1); err != nil {
		t.Fatal(err)
	}
	if err := s.Flush(); err != nil {
		t.Fatal(err)
	}
	// Garbage where the zstd frame was: the index still points at it, so the
	// read gets that far and then cannot decompress it.
	f, err := os.OpenFile(filepath.Join(dir, arrivalName(0)), os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt(bytes.Repeat([]byte{0xff}, 16), 0); err != nil {
		t.Fatal(err)
	}
	f.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ch := s.Subscribe(ctx, 1)
	ev, ok := <-ch
	if !ok {
		t.Fatal("the channel closed with no event: a read failure is indistinguishable from a clean shutdown")
	}
	if ev.Err == nil {
		t.Fatalf("event %+v carries no error", ev)
	}
	if _, ok := <-ch; ok {
		t.Fatal("the error event is not the last one")
	}
}

// TestRetireDropsTheByHeightIndex: the RAM index of a retired bucket is dead
// weight that nothing used to reclaim, and on mainnet C it reached ~6.7-8.2GB
// while the Firewood cache that actually bounds throughput had 8GB. Retire must
// drop it. byID must SURVIVE: it is what lets ResolveCheckpoints skip a sealed
// checkpoint without a network fetch (TestResolveCheckpointsSkipsSealedHistory).
func TestRetireDropsTheByHeightIndex(t *testing.T) {
	s, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// One block in bucket 0 (retired below) and one in bucket 1 (kept).
	sealedID, liveID := ids.GenerateTestID(), ids.GenerateTestID()
	if err := s.Append(parsedContainer{containerID: sealedID, blockHash: sealedID, blockNumber: 1}, []byte("raw")); err != nil {
		t.Fatal(err)
	}
	if err := s.Append(parsedContainer{containerID: liveID, blockHash: liveID, blockNumber: SegmentBlocks + 1}, []byte("raw")); err != nil {
		t.Fatal(err)
	}
	if _, ok := s.byHeight[1]; !ok {
		t.Fatal("height 1 was never indexed; the test proves nothing")
	}

	// Seal everything in bucket 0.
	if err := s.Retire(SegmentBlocks - 1); err != nil {
		t.Fatal(err)
	}

	if _, ok := s.byHeight[1]; ok {
		t.Error("byHeight kept a height inside sealed history: the index still grows without bound")
	}
	if _, ok := s.byHeight[SegmentBlocks+1]; !ok {
		t.Error("byHeight dropped a height ABOVE the sealed end; the walk needs that one")
	}
	// The whole point of keeping byID.
	if h, ok := s.HeightOf(sealedID); !ok || h != 1 {
		t.Errorf("HeightOf(sealed) = %d, %v; want 1, true. ResolveCheckpoints needs this to skip sealed checkpoints offline", h, ok)
	}
	if h, ok := s.HeightOf(liveID); !ok || h != SegmentBlocks+1 {
		t.Errorf("HeightOf(live) = %d, %v", h, ok)
	}
}

// TestByHeightIsACacheNotTheOnlyRecord: freeing the in-RAM index must never
// turn a stored block into a missing one. The index sidecar is the durable
// copy, so a miss re-reads it; a bucket the seal retired has no sidecar and
// correctly stays missing.
func TestByHeightIsACacheNotTheOnlyRecord(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	p, raw := fakeContainer(7, 0xd0, 128)
	if err := s.Append(p, raw); err != nil {
		t.Fatal(err)
	}

	// Reclaim the whole map, the way a bounded index would.
	s.mu.Lock()
	s.byHeight = make(map[uint64]heightRec)
	s.mu.Unlock()

	got, ok, err := s.GetByHeight(7)
	if err != nil {
		t.Fatalf("read after releasing the index: %v", err)
	}
	if !ok {
		t.Fatal("a released index made a stored block look missing; the sidecar fallback did not fire")
	}
	if !bytes.Equal(got, raw) {
		t.Fatalf("re-read %d bytes, want the %d appended", len(got), len(raw))
	}

	// Retired history: the seal unlinks both files, so there is nothing to
	// fall back to and "not here" is the right answer rather than an error.
	s.mu.Lock()
	s.byHeight = make(map[uint64]heightRec)
	s.mu.Unlock()
	for _, n := range []string{arrivalName(0), indexName(0)} {
		if err := os.Remove(filepath.Join(dir, n)); err != nil {
			t.Fatal(err)
		}
	}
	if _, ok, err := s.GetByHeight(7); err != nil || ok {
		t.Fatalf("retired bucket: ok=%v err=%v; want false, nil", ok, err)
	}
}

// TestLowestContiguousIsBounded: the scan must not be O(stored history) under
// the store mutex. It is a shortcut for walkSpan, which reads the returned
// height and short-circuits again from that block's parent, so stopping early
// is correct and costs one extra block read per bucket.
func TestLowestContiguousIsBounded(t *testing.T) {
	s, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// A contiguous run two buckets long, entered directly: appending 200k real
	// containers would make this a benchmark, not a test.
	const runLen = 2 * SegmentBlocks
	s.mu.Lock()
	for h := uint64(1); h <= runLen; h++ {
		s.byHeight[h] = heightRec{off: 0, ln: 1}
	}
	s.mu.Unlock()

	got := s.LowestContiguous(runLen, 0)
	if got < runLen-maxContiguousScan {
		t.Fatalf("scanned to %d, i.e. more than %d entries below %d: the scan is unbounded", got, maxContiguousScan, runLen)
	}
	if got > runLen {
		t.Fatalf("returned %d, above the starting height %d", got, runLen)
	}
	// Bounded, but still a useful shortcut: it must cover a whole bucket.
	if runLen-got != maxContiguousScan {
		t.Fatalf("skipped %d heights, want a full bucket (%d)", runLen-got, maxContiguousScan)
	}

	// The floor still wins over the budget.
	if got := s.LowestContiguous(runLen, runLen-10); got != runLen-10 {
		t.Fatalf("floor ignored: got %d, want %d", got, runLen-10)
	}
}

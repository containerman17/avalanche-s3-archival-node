package fetch

// Throwaway spike probe: does Fuji actually serve ancient C-chain history
// over GetAncestors? Run manually:
//
//	EPOCHDB_SPIKE=1 go test ./fetch -run TestSpike -v -timeout 20m
//
// Deleted once the question is answered.

import (
	"context"
	"fmt"
	"os"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/ava-labs/avalanchego/genesis"
	"github.com/ava-labs/avalanchego/ids"
)

// raceAll fans out to parallelRequests peers and waits for ALL of them (or
// timeout), returning the first non-empty response plus answer stats.
func (f *Fetcher) raceAll(ctx context.Context, tip ids.ID) (resp ancestorsResponse, launched, answered, nonEmpty int) {
	rctx, cancel := context.WithTimeout(ctx, defaultRequestTimeout)
	defer cancel()

	results := make(chan raceOutcome, parallelRequests)
	picked := make(map[ids.NodeID]struct{}, parallelRequests)
	for launched < parallelRequests {
		peer, ok := f.tracker.SelectPeer()
		if !ok {
			break
		}
		if _, dup := picked[peer]; dup {
			continue
		}
		picked[peer] = struct{}{}
		launched++
		go f.singleRequest(rctx, peer, tip, results)
	}
	for i := 0; i < launched; i++ {
		o := <-results
		if !o.ok {
			continue
		}
		answered++
		if len(o.resp.blocks) > 0 && len(o.resp.blocks[0]) > 0 {
			nonEmpty++
			if len(resp.blocks) == 0 {
				resp = o.resp
			}
		} else {
			f.tracker.RegisterFailure(o.peer)
		}
	}
	return resp, launched, answered, nonEmpty
}

func TestSpike(t *testing.T) {
	if os.Getenv("EPOCHDB_SPIKE") == "" {
		t.Skip("set EPOCHDB_SPIKE=1 to run the network spike")
	}
	f, err := dial(Config{})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer f.net.StartClose()
	ctx := context.Background()

	cps := genesis.GetCheckpoints(f.networkID, f.chainID)
	t.Logf("checkpoints=%d", cps.Len())

	// Fetch every checkpoint container to learn its height.
	type cp struct {
		id    ids.ID
		h     uint64
		batch int
	}
	var (
		mu   sync.Mutex
		got  []cp
		fail int
		wg   sync.WaitGroup
		sem  = make(chan struct{}, 6)
	)
	for id := range cps {
		wg.Add(1)
		go func(id ids.ID) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			for attempt := 0; attempt < 3; attempt++ {
				resp, _, ok := f.raceAncestors(ctx, id)
				if !ok {
					continue
				}
				parsed, err := parseContainer(resp.blocks[0])
				if err != nil {
					t.Errorf("parse checkpoint %s: %v", id, err)
					return
				}
				if parsed.containerID != id {
					t.Errorf("checkpoint %s: wrong head %s", id, parsed.containerID)
					return
				}
				mu.Lock()
				got = append(got, cp{id, parsed.blockNumber, len(resp.blocks)})
				mu.Unlock()
				return
			}
			mu.Lock()
			fail++
			mu.Unlock()
			t.Logf("checkpoint %s: no non-empty answer after 3 races", id)
		}(id)
	}
	wg.Wait()
	sort.Slice(got, func(i, j int) bool { return got[i].h < got[j].h })
	t.Logf("checkpoint heights resolved=%d failed=%d", len(got), fail)
	if len(got) == 0 {
		t.Fatal("no checkpoint container fetched, Fuji peers serve nothing")
	}
	t.Logf("lowest=%d highest=%d", got[0].h, got[len(got)-1].h)
	for _, c := range got[:min(5, len(got))] {
		t.Logf("low checkpoint height=%d id=%s batch=%d", c.h, c.id, c.batch)
	}

	// Post-Granite (recent era) parse round-trip: highest checkpoint.
	hi := got[len(got)-1]
	t.Logf("highest checkpoint height=%d id=%s parses cleanly", hi.h, hi.id)

	// Deep history: walk down from the LOWEST checkpoint for a few batches,
	// counting how many raced peers answer non-empty.
	tip := got[0].id
	start := time.Now()
	var walked int
	for batch := 0; batch < 5; batch++ {
		resp, launched, answered, nonEmpty := f.raceAll(ctx, tip)
		t.Logf("deep race: tip=%s launched=%d answered=%d non_empty=%d containers=%d",
			tip, launched, answered, nonEmpty, len(resp.blocks))
		if len(resp.blocks) == 0 {
			t.Fatalf("no peer served ancestors of %s (deep history NOT available)", tip)
		}
		var last parsedContainer
		for i, raw := range resp.blocks {
			parsed, err := parseContainer(raw)
			if err != nil {
				t.Fatalf("parse container %d: %v", i, err)
			}
			if i > 0 && last.parentID != parsed.containerID {
				t.Fatalf("chain link broken at %d: parent=%s got=%s", i, last.parentID, parsed.containerID)
			}
			last = parsed
			walked++
		}
		t.Logf("batch %d: heights %d..%d ok", batch, last.blockNumber, last.blockNumber+uint64(len(resp.blocks))-1)
		if last.blockNumber == 0 || last.parentID == f.genesisID {
			t.Logf("hit genesis at walked=%d", walked)
			break
		}
		tip = last.parentID
	}
	elapsed := time.Since(start).Seconds()
	fmt.Printf("SPIKE: walked=%d containers in %.1fs (%.0f/s)\n", walked, elapsed, float64(walked)/elapsed)
}

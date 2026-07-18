package fetch

import (
	"context"
	"fmt"
	"log"
	"sort"
	"sync"

	"github.com/ava-labs/avalanchego/genesis"
	"github.com/ava-labs/avalanchego/ids"
	"golang.org/x/sync/errgroup"
)

// Sync populates the store with the full C-chain history using `walks`
// concurrent backward walks. Strategy: fetch every embedded checkpoint
// container (heights are not published, so each is fetched and parsed),
// sort by height, and split history into disjoint spans, one per adjacent
// checkpoint pair. Each walk claims a span and follows parent links down
// via GetAncestors until it reaches its floor (the next lower checkpoint,
// which is already stored), the locally computed genesis container ID
// (genesis is never served over GetAncestors), or block 0. A walk
// short-circuits over already-stored containers by skipping to the bottom
// of the contiguous stored run, which is also how resume works.
func (f *Fetcher) Sync(ctx context.Context, walks int) error {
	if walks < 1 {
		walks = 1
	}
	cps := genesis.GetCheckpoints(f.networkID, f.chainID)
	if cps.Len() == 0 {
		return fmt.Errorf("no embedded checkpoints for network %d chain %s", f.networkID, f.chainID)
	}
	log.Printf("fetch: checkpoints=%d walks=%d", cps.Len(), walks)

	go f.probeLoop(ctx)

	type point struct {
		id ids.ID
		h  uint64
	}
	var (
		mu  sync.Mutex
		pts = make([]point, 0, cps.Len())
	)
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(walks)
	for id := range cps {
		g.Go(func() error {
			parsed, err := f.getContainer(gctx, id)
			if err != nil {
				return fmt.Errorf("fetch checkpoint %s: %w", id, err)
			}
			mu.Lock()
			pts = append(pts, point{id, parsed.blockNumber})
			mu.Unlock()
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}
	sort.Slice(pts, func(i, j int) bool { return pts[i].h > pts[j].h })
	log.Printf("fetch: checkpoints resolved, highest=%d lowest=%d", pts[0].h, pts[len(pts)-1].h)

	g, gctx = errgroup.WithContext(ctx)
	g.SetLimit(walks)
	for i, p := range pts {
		var floor uint64
		if i+1 < len(pts) {
			floor = pts[i+1].h
		}
		g.Go(func() error {
			f.activeWalks.Add(1)
			defer f.activeWalks.Add(-1)
			return f.walkSpan(gctx, p.id, floor)
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}
	log.Printf("fetch: all %d spans complete", len(pts))
	return nil
}

// Anchor seeds one SyncTo span: a container ID and its expected block
// height (pre-verified against the fetched container).
type Anchor struct {
	ID     ids.ID
	Height uint64
}

// Checkpoints returns the embedded checkpoint container IDs for this
// network's C-chain. Heights are not published; pre-ProposerVM checkpoint
// IDs equal the eth block hash and can be resolved via any archive RPC.
func (f *Fetcher) Checkpoints() []ids.ID {
	cps := genesis.GetCheckpoints(f.networkID, f.chainID)
	out := make([]ids.ID, 0, cps.Len())
	for id := range cps {
		out = append(out, id)
	}
	return out
}

// ResolveCheckpoints fetches every embedded checkpoint container over p2p
// and returns them with parsed heights, ascending. Post-ProposerVM
// checkpoint IDs cannot be height-resolved via RPC, so this is the way to
// anchor above the activation height. Fetched containers (and the
// ancestor batches that ride along) land in the store; staging above the
// corpus ceiling is disposable.
func (f *Fetcher) ResolveCheckpoints(ctx context.Context) ([]Anchor, error) {
	var anchors []Anchor
	for _, id := range f.Checkpoints() {
		parsed, err := f.getContainer(ctx, id)
		if err != nil {
			return nil, fmt.Errorf("resolve checkpoint %s: %w", id, err)
		}
		anchors = append(anchors, Anchor{ID: id, Height: parsed.blockNumber})
	}
	sort.Slice(anchors, func(i, j int) bool { return anchors[i].Height < anchors[j].Height })
	return anchors, nil
}

// SyncTo backfills blocks [0..max(anchors)] with NO frontier following:
// the anchors (a fixed tip override plus any checkpoints at or below it)
// are sorted descending and each walk covers one span down to the next
// anchor (the lowest walks to genesis). Each anchor container is fetched
// and its parsed height verified against Anchor.Height first, so a wrong
// ID (e.g. an RPC block hash for a post-ProposerVM height) fails loudly
// instead of staging a bogus chain. Returns when the range is contiguous.
func (f *Fetcher) SyncTo(ctx context.Context, anchors []Anchor, walks int) error {
	if len(anchors) == 0 {
		return fmt.Errorf("SyncTo: no anchors")
	}
	if walks < 1 {
		walks = 1
	}
	go f.probeLoop(ctx)

	for _, a := range anchors {
		parsed, err := f.getContainer(ctx, a.ID)
		if err != nil {
			return fmt.Errorf("fetch anchor %s: %w", a.ID, err)
		}
		if parsed.blockNumber != a.Height {
			return fmt.Errorf("anchor %s parsed to height %d, want %d (pass a real container ID via --tip for post-ProposerVM heights)",
				a.ID, parsed.blockNumber, a.Height)
		}
	}
	sort.Slice(anchors, func(i, j int) bool { return anchors[i].Height > anchors[j].Height })
	log.Printf("fetch: sync-to height=%d anchors=%d walks=%d", anchors[0].Height, len(anchors), walks)

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(walks)
	for i, a := range anchors {
		var floor uint64
		if i+1 < len(anchors) {
			floor = anchors[i+1].Height
		}
		g.Go(func() error {
			f.activeWalks.Add(1)
			defer f.activeWalks.Add(-1)
			return f.walkSpan(gctx, a.ID, floor)
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}
	log.Printf("fetch: sync-to complete, %d spans", len(anchors))
	return nil
}

// walkSpan walks backward from tip until the span floor, genesis, or block 0.
func (f *Fetcher) walkSpan(ctx context.Context, id ids.ID, floor uint64) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		select {
		case err := <-f.dispatchErrCh:
			return fmt.Errorf("network stopped: %w", err)
		default:
		}
		if id == f.genesisID {
			return nil
		}
		if h, ok := f.store.HeightOf(id); ok {
			if h <= floor {
				return nil // reached the next span's territory
			}
			// Short-circuit: skip past the contiguous stored run in RAM,
			// re-parse only the bottom container to find its parent.
			lo := f.store.LowestContiguous(h, floor)
			if lo == 0 || (floor > 0 && lo <= floor+1) {
				return nil // run connects to the floor checkpoint (or block 0)
			}
			raw, ok, err := f.store.GetByHeight(lo)
			if err != nil || !ok {
				return fmt.Errorf("read stored container at height %d: %w", lo, err)
			}
			parsed, err := parseContainer(raw)
			if err != nil {
				return fmt.Errorf("parse stored container at height %d: %w", lo, err)
			}
			if parsed.blockNumber == 0 {
				return nil
			}
			id = parsed.parentID
			continue
		}
		parsed, err := f.getContainer(ctx, id)
		if err != nil {
			return err
		}
		if parsed.blockNumber == 0 {
			return nil
		}
		id = parsed.parentID
	}
}

// getContainer returns the parsed metadata for a container, reading from the
// store if present and otherwise issuing a P2P fetch that persists the
// container plus whatever ancestors the peer sent alongside it.
func (f *Fetcher) getContainer(ctx context.Context, id ids.ID) (parsedContainer, error) {
	if h, ok := f.store.HeightOf(id); ok {
		raw, ok, err := f.store.GetByHeight(h)
		if err != nil || !ok {
			return parsedContainer{}, fmt.Errorf("read stored container %s: %w", id, err)
		}
		return parseContainer(raw)
	}
	if err := f.fetchAndStore(ctx, id); err != nil {
		return parsedContainer{}, err
	}
	h, ok := f.store.HeightOf(id)
	if !ok {
		return parsedContainer{}, fmt.Errorf("container %s still missing after fetch", id)
	}
	raw, ok, err := f.store.GetByHeight(h)
	if err != nil || !ok {
		return parsedContainer{}, fmt.Errorf("read fetched container %s: %w", id, err)
	}
	return parseContainer(raw)
}

// fetchAndStore asks an archival peer for GetAncestors(id), appends every
// returned container to the store, and returns once the requested container
// itself is persisted. Peers that answer empty, time out, or return a bad
// payload are demoted out of the archival set and the request retried.
func (f *Fetcher) fetchAndStore(ctx context.Context, id ids.ID) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		resp, peer, ok := f.fetchAncestors(ctx, id)
		if !ok {
			continue
		}

		var storedTarget bool
		respOK := true
		for i, raw := range resp.blocks {
			parsed, err := parseContainer(raw)
			if err != nil {
				log.Printf("fetch: parse container %d/%d from %s: %v (raw_len=%d)",
					i, len(resp.blocks), peer, err, len(raw))
				f.pool.setArchival(peer, false)
				respOK = false
				break
			}
			if i == 0 && parsed.containerID != id {
				log.Printf("fetch: peer %s returned wrong head container: got=%s want=%s",
					peer, parsed.containerID, id)
				f.pool.setArchival(peer, false)
				respOK = false
				break
			}
			if err := f.store.Append(parsed, raw); err != nil {
				return fmt.Errorf("store append: %w", err)
			}
			if parsed.containerID == id {
				storedTarget = true
			}
		}
		if !respOK {
			continue
		}
		if err := f.store.Flush(); err != nil {
			return fmt.Errorf("store flush: %w", err)
		}
		if storedTarget {
			return nil
		}
	}
}

// Progress is a snapshot of sync counters for logging.
type Progress struct {
	Stored       uint64 // containers in the store
	Head         uint64 // highest stored block height
	SessionBytes uint64 // compressed + index bytes written since open
	SessionRaw   uint64 // container bytes before compression since open
	Requests     uint64 // GetAncestors requests sent
	Answers      uint64 // answers received (any content)
	NonEmpty     uint64 // answers carrying at least one container
	ActiveWalks  int64
	Archival     int   // current archival peer set size
	InFlight     int64 // outstanding GetAncestors requests
}

func (f *Fetcher) Progress() Progress {
	head, _ := f.store.Head()
	return Progress{
		Stored:       f.store.Count(),
		Head:         head,
		SessionBytes: f.store.SessionBytes(),
		SessionRaw:   f.store.SessionRawBytes(),
		Requests:     f.requestsSent.Load(),
		Answers:      f.answersTotal.Load(),
		NonEmpty:     f.answersNonEmpty.Load(),
		ActiveWalks:  f.activeWalks.Load(),
		Archival:     f.pool.archivalCount(),
		InFlight:     f.inFlight.Load(),
	}
}

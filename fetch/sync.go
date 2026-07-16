package fetch

import (
	"context"
	"fmt"
	"log"

	"github.com/ava-labs/avalanchego/genesis"
	"github.com/ava-labs/avalanchego/ids"
)

// Sync populates the store with the full C-chain history. Strategy: fetch
// every embedded checkpoint container (avalanchego ships 99 for Fuji C-chain,
// heights are not published so each is fetched and parsed), then walk
// backward from the highest one via GetAncestors. A walk short-circuits when
// it hits an already-stored container (skipping to the bottom of the
// contiguous stored run, which is how resume works) or the locally computed
// genesis container ID (genesis is never served over GetAncestors).
func (f *Fetcher) Sync(ctx context.Context) error {
	cps := genesis.GetCheckpoints(f.networkID, f.chainID)
	if cps.Len() == 0 {
		return fmt.Errorf("no embedded checkpoints for network %d chain %s", f.networkID, f.chainID)
	}
	log.Printf("fetch: checkpoints=%d", cps.Len())

	var (
		tip     ids.ID
		tipH    uint64
		haveTip bool
		done    int
	)
	for id := range cps {
		parsed, err := f.getContainer(ctx, id)
		if err != nil {
			return fmt.Errorf("fetch checkpoint %s: %w", id, err)
		}
		done++
		if done%20 == 0 {
			log.Printf("fetch: checkpoints resolved %d/%d", done, cps.Len())
		}
		if !haveTip || parsed.blockNumber > tipH {
			tip, tipH = parsed.containerID, parsed.blockNumber
			haveTip = true
		}
	}
	log.Printf("fetch: walking back from checkpoint height=%d id=%s", tipH, tip)

	id := tip
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
			log.Printf("fetch: reached genesis container, sync complete")
			return nil
		}
		if h, ok := f.store.HeightOf(id); ok {
			// Short-circuit: skip past the contiguous stored run in RAM,
			// re-parse only the bottom container to find its parent.
			lo := f.store.LowestContiguous(h)
			raw, ok, err := f.store.GetByHeight(lo)
			if err != nil || !ok {
				return fmt.Errorf("read stored container at height %d: %w", lo, err)
			}
			parsed, err := parseContainer(raw)
			if err != nil {
				return fmt.Errorf("parse stored container at height %d: %w", lo, err)
			}
			f.walkHeight.Store(parsed.blockNumber)
			if parsed.blockNumber == 0 {
				log.Printf("fetch: reached block 0, sync complete")
				return nil
			}
			id = parsed.parentID
			continue
		}
		parsed, err := f.getContainer(ctx, id)
		if err != nil {
			return err
		}
		f.walkHeight.Store(parsed.blockNumber)
		if parsed.blockNumber == 0 {
			log.Printf("fetch: reached block 0, sync complete")
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

// fetchAndStore races a batch of peers for GetAncestors(id), appends every
// returned container to the store, and returns once the requested container
// itself is persisted. Peers that answer empty or a wrong first container
// are penalised and rotated out.
func (f *Fetcher) fetchAndStore(ctx context.Context, id ids.ID) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		resp, peer, ok := f.raceAncestors(ctx, id)
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
				f.tracker.RegisterFailure(peer)
				respOK = false
				break
			}
			if i == 0 && parsed.containerID != id {
				log.Printf("fetch: peer %s returned wrong head container: got=%s want=%s",
					peer, parsed.containerID, id)
				f.tracker.RegisterFailure(peer)
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
	WalkHeight   uint64 // current backward-walk height
	Stored       uint64 // containers in the store
	SessionBytes uint64 // bytes written since open
	Requests     uint64 // GetAncestors requests sent
	NonEmpty     uint64 // requests answered non-empty within the race window
}

func (f *Fetcher) Progress() Progress {
	return Progress{
		WalkHeight:   f.walkHeight.Load(),
		Stored:       f.store.Count(),
		SessionBytes: f.store.SessionBytes(),
		Requests:     f.requestsSent.Load(),
		NonEmpty:     f.answersNonEmpty.Load(),
	}
}

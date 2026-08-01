package exec

// BUILDING THE BOOTSTRAP FRONTIER (DESIGN.md ruling 1 of 2026-07-31). A node
// that only downloaded epochs has no state at all: it never executed a block
// and there is no snapshot artifact to load any more. Its frontier is merged
// out of the epochs themselves (state.MergeFrontier), streamed into the
// Firewood this Executor already opened, and checked against header(H).Root.
// After that the node is indistinguishable from one that replayed to H: exec
// starts at H+1 and follows.

import (
	"bytes"
	"fmt"
	"log"
	"time"

	ffi "github.com/ava-labs/firewood-go-ethhash/ffi"
	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/crypto"
	"github.com/ava-labs/libevm/rlp"

	"github.com/containerman17/epochdb/state"
)

// frontierBatch is how many trie ops go into one Firewood Update. Rows are
// ~50-130 bytes of key+value, so a batch is 20-30MB of cgo-pinned ops; the
// merge itself adds one 64KB decoded SST block per epoch. That keeps a
// full-mainnet build (~1B rows) inside a few hundred MB while still amortising
// the FFI crossing over enough work to be invisible. Same value the deleted
// snapshot loader used, for the same reason.
const frontierBatch = 200_000

// frontierHeaderWindow is how many headers below H are copied out of the
// epochs into the state layer: BLOCKHASH at H+1 reaches back to H-255, and
// the crash walk-back needs header(H) itself to match Firewood's root. It is
// the same window the deleted base file carried.
const frontierHeaderWindow = 256

// BuildFrontier merges every epoch's SST section into this Executor's
// Firewood, parking the head at H = the last block of the contiguous sealed
// prefix. Idempotent: a store already executed to H or beyond is left alone.
//
// The merge is linear per epoch and the working set is one active SST block
// per epoch cursor, so this streams a full corpus without ever holding more
// than a batch of rows.
func (e *Executor) BuildFrontier(epochs *state.EpochSet) error {
	h := epochs.CoveredEnd()
	if h == 0 {
		return fmt.Errorf("build frontier: no contiguous sealed epochs from genesis in this data dir (run epochdb bootstrap first)")
	}
	if end, _ := epochs.SealedEnd(); end != h {
		return fmt.Errorf("build frontier: sealed coverage is contiguous only through block %d but the set reaches %d: fill the gap first", h, end)
	}
	if n, ok := e.cfg.Store.ExecHead(); ok && n >= h {
		log.Printf("exec: frontier already at %d (sealed set ends at %d), nothing to build", n, h)
		return nil
	}
	if n, ok := e.cfg.Store.ExecHead(); ok && n > 0 {
		return fmt.Errorf("build frontier: this data dir already executed to %d; the merge only ever builds a FRESH frontier (delete the dir to rebuild)", n)
	}

	top, ok := epochs.At(h)
	if !ok {
		return fmt.Errorf("build frontier: no epoch covers block %d", h)
	}
	hdrRLP, err := top.HeaderRLP(h)
	if err != nil {
		return fmt.Errorf("build frontier: header %d: %w", h, err)
	}
	var hdr types.Header
	if err := rlp.DecodeBytes(hdrRLP, &hdr); err != nil {
		return fmt.Errorf("build frontier: decode header %d: %w", h, err)
	}
	if HasSettledMarkers(&hdr) {
		// Post-Helicon (ACP-194) header.Root is the root of the block this one
		// SETTLES, so it is not the frontier's root and there is nothing here
		// to check the merge against. The settled-root plumbing belongs to the
		// verification engine; until it lands, refuse loudly.
		return fmt.Errorf("build frontier: block %d is post-Helicon (SAE): header.Root is the SETTLED block's root, not the state at %d, so the merged frontier cannot be checked against it; SAE bootstrap needs the settled-root ring (not built)", h, h)
	}

	t0 := time.Now()
	var (
		batch                  []ffi.BatchOp
		root                   ffi.Hash
		nAcct, nSlot, nDel     uint64
		accKey                 [32]byte
		slotKey                [64]byte
		lastAddr               common.Address
		lastAddrHash           common.Hash
		haveAddr               bool
		nRows, nSkip, nBatches uint64
	)
	addrHash := func(a common.Address) common.Hash {
		if !haveAddr || a != lastAddr {
			lastAddr, lastAddrHash, haveAddr = a, crypto.Keccak256Hash(a[:]), true
		}
		return lastAddrHash
	}
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		r, err := e.fwBackend.Firewood.Update(batch)
		if err != nil {
			return fmt.Errorf("build frontier: firewood update: %w", err)
		}
		root, batch, nBatches = r, batch[:0], nBatches+1
		return nil
	}

	err = state.MergeFrontier(epochs.All(), h, func(r state.FrontierRow) error {
		nRows++
		addr := common.Address(r.Key[1:21])
		switch r.Key[0] {
		case 'a':
			copy(accKey[:], addrHash(addr).Bytes())
			if len(r.Value) == 0 {
				// Only the genesis alloc can have put this account in the trie
				// (every other row here IS the trie's content), and a
				// PrefixDelete drops its storage with it.
				if _, inAlloc := e.alloc[addr]; !inAlloc {
					nSkip++
					return nil
				}
				nDel++
				batch = append(batch, ffi.PrefixDelete(bytes.Clone(accKey[:])))
			} else {
				nAcct++
				// Captured account RLP is byte-for-byte what firewood's
				// UpdateAccount writes (zero storage root included, see
				// verify/zeroroot_test.go).
				batch = append(batch, ffi.Put(bytes.Clone(accKey[:]), bytes.Clone(r.Value)))
			}
		case 's':
			copy(slotKey[:32], addrHash(addr).Bytes())
			copy(slotKey[32:], crypto.Keccak256(r.Key[21:53]))
			if len(r.Value) == 0 {
				ga, inAlloc := e.alloc[addr]
				if !inAlloc || ga.Storage == nil {
					nSkip++
					return nil
				}
				nDel++
				batch = append(batch, ffi.Delete(bytes.Clone(slotKey[:])))
			} else {
				nSlot++
				// Rows hold the raw trimmed slot value; a firewood leaf is the
				// RLP of it (graft/evm/firewood baseTrie.UpdateStorage).
				enc, err := rlp.EncodeToBytes(r.Value)
				if err != nil {
					return err
				}
				batch = append(batch, ffi.Put(bytes.Clone(slotKey[:]), enc))
			}
		default:
			return fmt.Errorf("build frontier: unknown row kind %q", r.Key[0])
		}
		if len(batch) >= frontierBatch {
			return flush()
		}
		return nil
	})
	if err != nil {
		return err
	}
	if err := flush(); err != nil {
		return err
	}
	if got := common.Hash(root); got != hdr.Root {
		return fmt.Errorf("build frontier: merged root %x != header(%d).Root %x (%d accounts, %d slots, %d deletes over %d rows)",
			got, h, hdr.Root, nAcct, nSlot, nDel, nRows)
	}

	// The headers just below H come out of the epochs into the state layer:
	// BLOCKHASH in [H+1, H+256) reads them, and so does the crash walk-back,
	// which has no other way to learn that Firewood's root belongs to H.
	from := uint64(1)
	if h > frontierHeaderWindow {
		from = h - frontierHeaderWindow
	}
	for n := from; n <= h; n++ {
		ep, ok := epochs.At(n)
		if !ok {
			continue
		}
		raw, err := ep.HeaderRLP(n)
		if err != nil {
			return fmt.Errorf("build frontier: header %d: %w", n, err)
		}
		if err := e.cfg.Store.AppendHeader(n, raw); err != nil {
			return fmt.Errorf("build frontier: store header %d: %w", n, err)
		}
	}

	e.fwBackend.SetHashAndHeight(hdr.Hash(), h)
	e.fwHeight, e.lastFwHash = h, hdr.Hash()
	e.headNum, e.headRoot, e.headTime = h, hdr.Root, hdr.Time
	e.publishLive()
	if err := e.cfg.Store.FlushAndSetExecHead(h); err != nil {
		return fmt.Errorf("build frontier: seed exechead: %w", err)
	}
	dt := time.Since(t0)
	log.Printf("exec: frontier built at %d root=%x from %d epochs: %d rows -> %d accounts, %d slots, %d deletes, %d skipped, %d batches in %s (%.0f rows/s)",
		h, hdr.Root, len(epochs.All()), nRows, nAcct, nSlot, nDel, nSkip, nBatches, dt.Round(time.Second), float64(nRows)/dt.Seconds())
	return nil
}

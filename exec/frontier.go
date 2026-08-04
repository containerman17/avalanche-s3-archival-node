package exec

// BUILDING THE BOOTSTRAP FRONTIER (DESIGN.md ruling 1 of 2026-07-31). A node
// that only downloaded epochs has no state at all: it never executed a block
// and there is no snapshot artifact to load any more. Its frontier is merged
// out of the epochs themselves (state.MergeFrontier), streamed into the
// Firewood this Executor already opened, and checked against a header that
// COMMITS to the merged root. After that the node is indistinguishable from
// one that replayed to that height: exec starts at the next block and follows.
//
// WHICH HEADER COMMITS TO WHAT (RULING 2026-08-01). Below the Helicon
// boundary a header commits to its own post-execution root, so the merge
// targets H (the last sealed block) and is checked against header(H).Root:
// unchanged, byte for byte. Above it (ACP-194) header.Root is the root of the
// block the header SETTLES, so the merge targets S = settledHeight(header(H))
// and header(H).Root is the cryptographic commitment to it. Same strength as
// the pre-SAE check, one header hop away.
//
// The pairing is FORCED, not chosen. The settled height is the only height
// above the boundary whose post-execution root AND gas clock consensus ever
// publishes (the roots of every other height live nowhere but the executor's
// own sae.ring), and the gas clock is not optional decoration: executing S+1
// needs its parent's clock, so a frontier parked anywhere but a settlement
// point could not take a single step. The lag costs the frontier 1..10 blocks
// (measured on live Fuji), which the follower re-executes on the way up.

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/ava-labs/avalanchego/graft/evm/firewood"
	"github.com/ava-labs/avalanchego/vms/saevm/hook"
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
// Firewood, parking the head at the highest height a header of the contiguous
// sealed prefix commits to: H itself below the Helicon boundary, the height H
// settles above it. Idempotent: a store already at or past that height is left
// alone.
//
// The merge is linear per epoch and its working set is one active SST block
// per epoch cursor, so the Go side never holds more than a batch of rows. The
// side that is NOT free is Firewood's, which retains a revision per batch
// unless the Executor was opened with Config.FrontierBuild (see New).
func (e *Executor) BuildFrontier(epochs *state.EpochSet) error {
	h := epochs.CoveredEnd()
	if h == 0 {
		return fmt.Errorf("build frontier: no contiguous sealed epochs from genesis in this data dir (run `epochdb dev bootstrap` on this dir)")
	}
	if end, _ := epochs.SealedEnd(); end != h {
		return fmt.Errorf("build frontier: sealed coverage is contiguous only through block %d but the set reaches %d: fill the gap first", h, end)
	}
	if n, ok := e.cfg.Store.ExecHead(); ok && n >= h {
		log.Printf("exec: frontier already at %d (sealed set ends at %d), nothing to build", n, h)
		return nil
	}
	// Idempotent above the boundary too, where the frontier legitimately parks
	// a settlement lag below the sealed end.
	if floor, ok := e.cfg.Store.FrontierFloor(); ok {
		if n, _ := e.cfg.Store.ExecHead(); n >= floor {
			log.Printf("exec: frontier already built at %d (exec head %d, sealed set ends at %d), nothing to build", floor, n, h)
			return nil
		}
	}
	if n, ok := e.cfg.Store.ExecHead(); ok && n > 0 {
		return fmt.Errorf("build frontier: this data dir already executed to %d; the merge only ever builds a FRESH frontier (delete the dir to rebuild)", n)
	}

	// attest is the header whose Root the merge must reproduce; target is the
	// height it is the root OF (see the settled-root rule at the top).
	attest, err := epochHeader(epochs, h)
	if err != nil {
		return err
	}
	target, targetHdr := h, attest
	if HasSettledMarkers(attest) {
		s := settledBy(attest).Height
		if s == 0 || s >= h {
			return fmt.Errorf("build frontier: block %d is post-Helicon but settles height %d: a block cannot settle itself, the future, or genesis", h, s)
		}
		if targetHdr, err = epochHeader(epochs, s); err != nil {
			return err
		}
		target = s
		log.Printf("exec: frontier is post-Helicon (SAE): merging to the settled height %d, attested by header(%d).Root (lag %d)", target, h, h-target)
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

	err = state.MergeFrontier(epochs.All(), target, func(r state.FrontierRow) error {
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
	if got := common.Hash(root); got != attest.Root {
		return fmt.Errorf("build frontier: merged root at %d is %x, but header(%d).Root commits to %x (%d accounts, %d slots, %d deletes over %d rows)",
			target, got, h, attest.Root, nAcct, nSlot, nDel, nRows)
	}

	// The headers just below the frontier come out of the epochs into the
	// state layer: BLOCKHASH above it reads them, and so does the crash
	// walk-back, which has no other way to learn what height Firewood's root
	// belongs to. Nothing ABOVE the frontier is copied down: the walk-back
	// starts at the highest header on disk and would then demand containers
	// for a gap this node has not fetched.
	from := uint64(1)
	if target > frontierHeaderWindow {
		from = target - frontierHeaderWindow
	}
	for n := from; n <= target; n++ {
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

	// Above the boundary the frontier height's gas clock is published by the
	// SAME markers that just attested its root, and hook.SettledGasTime is
	// upstream's own reconstruction of it, so the ring entry the executor
	// needs to execute target+1 comes straight out of the attesting header.
	// Below the boundary the block's own header carries root and clock and
	// postExecutionAt reads them from there, so there is nothing to seed.
	if target != h && e.vm.hasSettledMarkers(targetHdr) {
		hooks := &saeHooks{chainCfg: e.chainCfg, avaxAssetID: e.snowCtx.AVAXAssetID}
		clock, err := hook.SettledGasTime(hooks, targetHdr, attest)
		if err != nil {
			return fmt.Errorf("build frontier: settled gas clock at %d: %w", target, err)
		}
		if err := hooks.takeErr(); err != nil {
			return fmt.Errorf("build frontier: settled gas clock at %d: %w", target, err)
		}
		if err := e.ring.put(target, common.Hash(root), clock.MarshalCanoto()); err != nil {
			return err
		}
	}
	// Nothing below the frontier was ever executed here, so no header settling
	// such a height can be checked against a root of ours (checkSettled).
	if err := e.cfg.Store.SetFrontierFloor(target); err != nil {
		return fmt.Errorf("build frontier: stamp frontier floor: %w", err)
	}

	e.fwBackend.SetHashAndHeight(targetHdr.Hash(), target)
	e.fwHeight, e.lastFwHash = target, targetHdr.Hash()
	e.headNum, e.headRoot, e.headTime = target, common.Hash(root), targetHdr.Time
	e.publishLive()
	// Same order as maybeFlush: the ring must be on disk before exechead
	// claims a height whose root only the ring records.
	if err := e.ring.sync(); err != nil {
		return err
	}
	if err := e.cfg.Store.FlushAndSetExecHead(target); err != nil {
		return fmt.Errorf("build frontier: seed exechead: %w", err)
	}
	dt := time.Since(t0)
	log.Printf("exec: frontier built at %d root=%x from %d epochs: %d rows -> %d accounts, %d slots, %d deletes, %d skipped, %d batches in %s (%.0f rows/s)",
		target, common.Hash(root), len(epochs.All()), nRows, nAcct, nSlot, nDel, nSkip, nBatches, dt.Round(time.Second), float64(nRows)/dt.Seconds())
	return nil
}

// HealTornFrontier makes a dir whose frontier build died mid-merge buildable
// again, and it is the whole crash story of the build: the merge writes
// Firewood as it streams, so a kill leaves a Firewood holding some prefix of
// the frontier under an exec head of 0, and exec.New then refuses to open the
// dir forever ("head=0 but firewood root X != genesis Y"). That is a permanent
// crash loop out of state that is DERIVED and DISPOSABLE, which the resilience
// rule says a node organizes by itself.
//
// THE GUARD IS THE SIGNATURE, and it needs nothing on disk to remember: exec
// head 0 with a non-empty Firewood cannot be an executed history (a dir that
// executed anything has head > 0), so it is either a torn build or a Firewood
// holding nothing but the genesis alloc, and both are rebuilt from scratch in
// the same seconds. A dir with head > 0 is never touched.
//
// Everything wiped is a frontier build's own output: Firewood, the header
// window it copies down (with head 0 those headers would send the next
// exec.New walking back to re-execute blocks nothing has executed) and the SAE
// ring it seeds. The frontier floor stays: it is one misc key that the rebuild
// overwrites and that is only ever read against the exec head.
func HealTornFrontier(dataDir string) (bool, error) {
	head, _, err := state.ExecHead(dataDir)
	if err != nil {
		return false, err
	}
	if head > 0 {
		return false, nil
	}
	fw := filepath.Join(dataDir, firewood.Directory)
	ents, err := os.ReadDir(fw)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if len(ents) == 0 {
		return false, nil
	}
	log.Printf("exec: torn frontier build in %s (firewood is non-empty but nothing was ever executed): wiping it and rebuilding from the epochs", dataDir)
	victims := []string{fw, filepath.Join(dataDir, saeRingFile)}
	hdrs, err := filepath.Glob(filepath.Join(dataDir, "headers_*.log"))
	if err != nil {
		return false, err
	}
	for _, p := range append(victims, hdrs...) {
		if err := os.RemoveAll(p); err != nil {
			return false, fmt.Errorf("heal torn frontier: remove %s: %w", p, err)
		}
	}
	return true, nil
}

// epochHeader decodes the stored header of block n out of the epoch that
// covers it.
func epochHeader(epochs *state.EpochSet, n uint64) (*types.Header, error) {
	ep, ok := epochs.At(n)
	if !ok {
		return nil, fmt.Errorf("build frontier: no epoch covers block %d", n)
	}
	raw, err := ep.HeaderRLP(n)
	if err != nil {
		return nil, fmt.Errorf("build frontier: header %d: %w", n, err)
	}
	hdr := new(types.Header)
	if err := rlp.DecodeBytes(raw, hdr); err != nil {
		return nil, fmt.Errorf("build frontier: decode header %d: %w", n, err)
	}
	return hdr, nil
}

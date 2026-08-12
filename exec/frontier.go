package exec

// BUILDING THE STATE FRONTIER (DESIGN, "Execution and state history"). A node
// that only downloaded runs has no state at all: it never executed a block. Its
// frontier is merged out of the runs' STATE FAMILY (store.DB.MergeFrontier,
// latest value per key), streamed into the Firewood this Executor already
// opened, and checked against a header that COMMITS to the merged root. After
// that the node is indistinguishable from one that replayed to that height: the
// executor starts at the next block and follows.
//
// WHAT THE MERGE STREAMS ONTO (ruling carried from 2026-08-05). Not an empty
// trie: the VM's OWN COMMITTED GENESIS, which every node materialises for
// itself in exec.New, because the runs carry only what changed after it. So a
// deleted account in the merge is a real delete against real state and is
// applied UNCONDITIONALLY. It used to be applied only for addresses in the
// genesis alloc, which is false on subnet-evm: Genesis.Commit also runs
// ApplyPrecompileActivations, so a precompile enabled at genesis has an account
// and storage that no alloc mentions.
//
// WHICH HEADER COMMITS TO WHAT (ruling carried from 2026-08-01). Below the
// Helicon boundary a header commits to its own post-execution root, so the merge
// targets H (the last stored block) and is checked against header(H).Root. Above
// it (ACP-194) header.Root is the root of the block the header SETTLES, so the
// merge targets S = settledHeight(header(H)) and header(H).Root is the
// cryptographic commitment to it. Same strength, one header hop away.

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/ava-labs/avalanchego/graft/evm/firewood"
	"github.com/ava-labs/avalanchego/vms/saevm/hook"
	ffi "github.com/ava-labs/firewood-go-ethhash/ffi"
	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/crypto"
	"github.com/ava-labs/libevm/rlp"

	"github.com/containerman17/epochdb/store"
)

// frontierBatch is how many trie ops go into one Firewood Update. Rows are
// ~50-130 bytes of key+value, so a batch is 20-30MB of cgo-pinned ops; the
// merge itself adds one decoded SST block per run cursor. That keeps a
// full-mainnet build inside a few hundred MB while still amortising the FFI
// crossing over enough work to be invisible.
const frontierBatch = 200_000

// frontierBuildingKey is set while a build is running and cleared when it
// finishes. It is the ONLY thing that says a Firewood holds half a frontier.
const frontierBuildingKey = "epochdb.frontierbuilding"

// BuildFrontier merges the runs' state family into this Executor's Firewood,
// parking the head at the highest height a stored header commits to. It is
// idempotent: a dir that already executed past that height is left alone.
//
// The Executor must have been opened with Config.FrontierBuild, which both
// unpins Firewood's in-memory history and skips the reconcile walk-back that a
// joined dir (runs up to H, an empty Firewood) cannot possibly satisfy.
func (e *Executor) BuildFrontier() error {
	db := e.cfg.Store
	h, ok := db.Head()
	if !ok || h == 0 {
		return fmt.Errorf("build frontier: this data dir holds no runs, so there is nothing to build a frontier from")
	}
	if !e.cfg.FrontierBuild {
		return fmt.Errorf("build frontier: this Executor was not opened with FrontierBuild")
	}

	attest, err := e.storedHeader(h)
	if err != nil {
		return err
	}
	target, targetHdr := h, attest
	if HasSettledMarkers(attest) {
		s := settledBy(attest).Height
		if s == 0 || s >= h {
			return fmt.Errorf("build frontier: block %d is post-Helicon but settles height %d: a block cannot settle itself, the future, or genesis", h, s)
		}
		if targetHdr, err = e.storedHeader(s); err != nil {
			return err
		}
		target = s
		log.Printf("exec: frontier is post-Helicon (SAE): merging to the settled height %d, attested by header(%d).Root (lag %d)", target, h, h-target)
	}
	atTx, ok, err := db.TxNumAtEndOf(target)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("build frontier: no blk row for block %d, so its TxNum bound is unknown", target)
	}

	// THE TORN-BUILD MARK goes down before Firewood is touched and comes off
	// after the floor is stamped; HealTornFrontier reads exactly this.
	if err := e.cfg.Misc.Put([]byte(frontierBuildingKey), []byte{1}); err != nil {
		return err
	}
	if err := e.cfg.Misc.Sync(); err != nil {
		return err
	}
	t0 := time.Now()
	log.Printf("exec: frontier: merging the state family of %d runs to block %d (TxNum %d)", len(db.Manifest().Runs), target, atTx)
	var (
		batch              []ffi.BatchOp
		root               ffi.Hash
		nAcct, nSlot, nDel uint64
		accKey             [32]byte
		slotKey            [64]byte
		lastAddr           common.Address
		lastAddrHash       common.Hash
		haveAddr           bool
		nRows, nBatches    atomic.Uint64
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
		root, batch = r, batch[:0]
		nBatches.Add(1)
		return nil
	}
	stop := progressTicker("exec: frontier", func() string {
		return fmt.Sprintf("%d rows merged, %d batches committed", nRows.Load(), nBatches.Load())
	})
	defer stop()

	err = db.MergeFrontier(atTx, func(r store.FrontierRow) error {
		nRows.Add(1)
		kind, addrB, slot, err := store.SplitStateKey(r.Key)
		if err != nil {
			return fmt.Errorf("build frontier: %w", err)
		}
		addr := common.Address(addrB)
		switch kind {
		case 'c':
			// The code hash rides inside the account RLP; the 'c' rows exist so
			// the descent can answer code-by-address without decoding one.
			return nil
		case 'a':
			copy(accKey[:], addrHash(addr).Bytes())
			if len(r.Val) == 0 {
				// A DELETE IS APPLIED WHOLE: a PrefixDelete is the only thing
				// that clears the storage subtree genesis left under the
				// account. Account keys sort before this address's storage
				// keys, so the delete is always queued before any Put that
				// recreates part of it.
				nDel++
				batch = append(batch, ffi.PrefixDelete(bytes.Clone(accKey[:])))
				break
			}
			nAcct++
			// Captured account RLP is byte-for-byte what firewood's
			// UpdateAccount writes, zero storage root included.
			batch = append(batch, ffi.Put(bytes.Clone(accKey[:]), bytes.Clone(r.Val)))
		case 's':
			copy(slotKey[:32], addrHash(addr).Bytes())
			copy(slotKey[32:], crypto.Keccak256(slot))
			if len(r.Val) == 0 {
				nDel++
				batch = append(batch, ffi.Delete(bytes.Clone(slotKey[:])))
				break
			}
			nSlot++
			// ROWS HOLD THE RAW TRIMMED SLOT VALUE, not its RLP: libevm's
			// stateObject trims the 32 bytes and StateTrie.UpdateStorage does
			// the RLP itself, so the interceptor sees the trimmed bytes and a
			// firewood leaf is the RLP of them.
			enc, err := rlp.EncodeToBytes(r.Val)
			if err != nil {
				return err
			}
			batch = append(batch, ffi.Put(bytes.Clone(slotKey[:]), enc))
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
			target, got, h, attest.Root, nAcct, nSlot, nDel, nRows.Load())
	}

	// Above the boundary the frontier height's gas clock is published by the
	// SAME markers that just attested its root, so the ring entry the executor
	// needs to execute target+1 comes straight out of the attesting header.
	// Below the boundary the block's own header carries root and clock.
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
	// such a height can be checked against a root of ours.
	if err := e.cfg.Misc.SetFrontierFloor(target); err != nil {
		return fmt.Errorf("build frontier: stamp frontier floor: %w", err)
	}
	if err := e.cfg.Misc.Delete([]byte(frontierBuildingKey)); err != nil {
		return err
	}

	e.fwBackend.SetHashAndHeight(targetHdr.Hash(), target)
	e.fwHeight, e.lastFwHash = target, targetHdr.Hash()
	e.headNum, e.headRoot, e.headTime = target, common.Hash(root), targetHdr.Time
	e.publishLive()
	if err := e.ring.sync(); err != nil {
		return err
	}
	if err := e.cfg.Misc.Sync(); err != nil {
		return err
	}
	dt := time.Since(t0)
	log.Printf("exec: frontier built at %d root=%x: %d rows -> %d accounts, %d slots, %d deletes, %d batches in %s (%.0f rows/s)",
		target, common.Hash(root), nRows.Load(), nAcct, nSlot, nDel, nBatches.Load(), dt.Round(time.Second), float64(nRows.Load())/dt.Seconds())
	return nil
}

// storedHeader decodes block n's header out of the runs.
func (e *Executor) storedHeader(n uint64) (*types.Header, error) {
	raw, ok, err := e.cfg.Store.HeaderRLP(n)
	if err != nil {
		return nil, fmt.Errorf("build frontier: header %d: %w", n, err)
	}
	if !ok {
		return nil, fmt.Errorf("build frontier: no stored header for block %d", n)
	}
	hdr := new(types.Header)
	if err := rlp.DecodeBytes(raw, hdr); err != nil {
		return nil, fmt.Errorf("build frontier: decode header %d: %w", n, err)
	}
	return hdr, nil
}

// progressTicker logs a line a minute while a long build runs: a build that
// says nothing for an hour is indistinguishable from a hang.
func progressTicker(what string, line func() string) func() {
	t := time.NewTicker(time.Minute)
	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-t.C:
				log.Printf("%s: %s", what, line())
			case <-done:
				t.Stop()
				return
			}
		}
	}()
	return func() { close(done) }
}

// FrontierNeeded reports whether a dir has runs to serve but no state to serve
// them from, i.e. an empty or absent Firewood. It is the whole trigger.
func FrontierNeeded(dataDir string) (bool, error) {
	ents, err := os.ReadDir(filepath.Join(dataDir, firewood.Directory))
	if os.IsNotExist(err) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	return len(ents) == 0, nil
}

// HealTornFrontier makes a dir whose frontier build died mid-merge buildable
// again, and it is the whole crash story of the build: the merge writes
// Firewood as it streams, so a kill leaves a Firewood holding some prefix of
// the frontier under a frontier floor that was never stamped, and exec.New then
// refuses to open the dir forever ("head=0 but firewood root X != genesis Y",
// or a walk-back that finds nothing). That is a permanent crash loop out of
// state that is DERIVED and DISPOSABLE.
//
// THE GUARD IS AN EXPLICIT MARK, not an inference: the build stamps
// frontierBuildingKey before it touches Firewood and clears it after the floor
// is stamped, so the marker's presence at open means exactly "a build started
// here and did not finish". Inferring it from an empty exec head would be
// wrong now that the head comes from the runs, and a wrong inference here wipes
// a real node's state.
func HealTornFrontier(dataDir string, misc *store.MiscStore) (bool, error) {
	if _, ok := misc.Get([]byte(frontierBuildingKey)); !ok {
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
	log.Printf("exec: torn frontier build in %s (firewood is non-empty but no frontier floor was ever stamped): wiping it and rebuilding from the runs", dataDir)
	for _, p := range []string{fw, filepath.Join(dataDir, saeRingFile)} {
		if err := os.RemoveAll(p); err != nil {
			return false, fmt.Errorf("heal torn frontier: remove %s: %w", p, err)
		}
	}
	return true, nil
}

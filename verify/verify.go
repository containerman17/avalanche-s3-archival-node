// Package verify checks downloaded sealed epochs with NO EVM execution
// (DESIGN.md "Verification without re-execution"): per block, (1) the
// SST post-image diffs are applied into a throwaway Firewood and the
// resulting root must equal header.Root; (2) txRoot is recomputed from
// the verbatim container; (3) receipts are reconstructed from the stored
// logs + per-tx gasUsed/status and receiptsRoot recomputed; (4) headers
// must parent-hash-chain within and across epochs down to the genesis
// anchor. The throwaway Firewood carries across epochs sequentially.
package verify

import (
	"fmt"
	"log"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/ava-labs/avalanchego/graft/coreth/core/extstate"
	"github.com/ava-labs/avalanchego/graft/evm/firewood"
	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/rawdb"
	ethstate "github.com/ava-labs/libevm/core/state"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/libevm/stateconf"
	"github.com/ava-labs/libevm/rlp"
	"github.com/ava-labs/libevm/trie"
	"github.com/ava-labs/libevm/triedb"

	"github.com/containerman17/epochdb/exec"
	"github.com/containerman17/epochdb/fetch"
	"github.com/containerman17/epochdb/state"
)

// Verifier owns the throwaway Firewood and the verification cursor state.
// Not goroutine-safe: one verifier, epochs verified in chain order.
type Verifier struct {
	tmp     string
	tdb     *triedb.Database
	fw      *firewood.TrieDB
	db      ethstate.Database
	workers int

	parentRoot common.Hash
	parentHash common.Hash
	anchored   bool // parentHash is a real anchor (genesis, or first seen block in anchorless mode)
	fwHeight   uint64
	next       uint64 // next block to verify
	blocks     uint64 // total verified
}

// New opens a fresh throwaway Firewood under tmpDir and anchors it at the
// C-chain genesis of networkID. networkID 0 = anchorless test mode: start
// from an empty trie and adopt the first header's ParentHash as anchor.
func New(tmpDir string, networkID uint32, workers int) (*Verifier, error) {
	fetch.RegisterExtras()
	tdb, fw, db, err := newThrowawayFirewood(tmpDir)
	if err != nil {
		return nil, err
	}
	if workers <= 0 {
		workers = runtime.GOMAXPROCS(0)
	}
	v := &Verifier{tmp: tmpDir, tdb: tdb, fw: fw, db: db, workers: workers, next: 1,
		parentRoot: types.EmptyRootHash}
	if networkID != 0 {
		g, err := exec.NetworkGenesis(networkID)
		if err != nil {
			tdb.Close()
			return nil, err
		}
		gBlk := g.ToBlock()
		if !tdb.Initialized(gBlk.Root()) {
			if _, err := g.Commit(rawdb.NewMemoryDatabase(), tdb); err != nil {
				tdb.Close()
				return nil, fmt.Errorf("commit genesis: %w", err)
			}
		}
		v.parentRoot = gBlk.Root()
		v.parentHash = gBlk.Hash()
		v.anchored = true
		fw.SetHashAndHeight(v.parentHash, 0)
	}
	return v, nil
}

func newThrowawayFirewood(tmpDir string) (*triedb.Database, *firewood.TrieDB, ethstate.Database, error) {
	fwCfg := firewood.DefaultConfig(tmpDir)
	// ponytail: fixed 4GB node cache, exec's proven value on this box.
	fwCfg.CacheSizeBytes = 4 << 30
	memdb := rawdb.NewMemoryDatabase()
	tdb := triedb.NewDatabase(memdb, &triedb.Config{DBOverride: fwCfg.BackendConstructor})
	fw, ok := tdb.Backend().(*firewood.TrieDB)
	if !ok {
		tdb.Close()
		return nil, nil, nil, fmt.Errorf("triedb backend is %T, want *firewood.TrieDB", tdb.Backend())
	}
	return tdb, fw, extstate.NewDatabaseWithNodeDB(memdb, tdb), nil
}

// Close releases the throwaway Firewood (the caller removes tmpDir).
func (v *Verifier) Close() { v.tdb.Close() }

// Blocks returns the total number of verified blocks.
func (v *Verifier) Blocks() uint64 { return v.blocks }

// Next returns the next block the verifier expects (the start of the next
// contiguous epoch).
func (v *Verifier) Next() uint64 { return v.next }

// VerifyEpoch fully checks the next contiguous epoch: state roots by diff
// application (sequential), txRoot + receiptsRoot per block (parallel),
// header parent-hash chain. Any failure is fatal for the whole set.
func (v *Verifier) VerifyEpoch(e *state.Epoch) error {
	if e.Start != v.next {
		return fmt.Errorf("epoch_%d_%d out of order: next expected block %d", e.Start, e.Count, v.next)
	}
	t0 := time.Now()

	// Bodies + receipts: embarrassingly parallel chunk workers.
	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		bodyErr  error
		perChunk = (e.Count + uint64(v.workers) - 1) / uint64(v.workers)
	)
	for w := 0; w < v.workers; w++ {
		from := e.Start + uint64(w)*perChunk
		to := min(from+perChunk, e.End()+1)
		if from >= to {
			break
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := checkBodies(e, from, to); err != nil {
				mu.Lock()
				if bodyErr == nil {
					bodyErr = err
				}
				mu.Unlock()
			}
		}()
	}

	stateErr := v.verifyState(e)
	wg.Wait()
	if stateErr != nil {
		return stateErr
	}
	if bodyErr != nil {
		return bodyErr
	}

	v.next = e.End() + 1
	v.blocks += e.Count
	dt := time.Since(t0)
	log.Printf("verify: epoch_%d_%d PASS: %d blocks in %s (%.0f blk/s)",
		e.Start, e.Count, e.Count, dt.Round(time.Second), float64(e.Count)/dt.Seconds())
	return nil
}

// verifyState replays the epoch's per-block post-image diffs into the
// throwaway Firewood, committing per block; every computed root must
// equal header.Root. Also checks the header parent-hash chain (it walks
// the headers anyway).
func (v *Verifier) verifyState(e *state.Epoch) error {
	cur, err := e.SpillDiffs(v.tmp)
	if err != nil {
		return fmt.Errorf("spill diffs: %w", err)
	}
	defer cur.Close()
	dBlk, dRows, dOK, err := cur.Next()
	if err != nil {
		return err
	}
	lastLog := time.Now()
	epochT0 := lastLog

	walkErr := e.WalkHeadersRange(e.Start, e.End()+1, func(n uint64, hdrRLP []byte) error {
		var hdr types.Header
		if err := rlp.DecodeBytes(hdrRLP, &hdr); err != nil {
			return fmt.Errorf("decode header %d: %w", n, err)
		}
		if !v.anchored {
			// Anchorless mode: adopt the first block's parent as anchor.
			v.parentHash = hdr.ParentHash
			v.anchored = true
			v.fw.SetHashAndHeight(hdr.ParentHash, n-1)
			v.fwHeight = n - 1
		}
		if hdr.ParentHash != v.parentHash {
			return fmt.Errorf("header chain broken at block %d: parent hash %x, want %x", n, hdr.ParentHash, v.parentHash)
		}
		if exec.HasSettledMarkers(&hdr) {
			// Post-Helicon (ACP-194): header.Root is the post-execution
			// root of the block this one SETTLES, and receiptsRoot covers
			// the concatenated receipts of the whole settled range, so
			// neither of this engine's per-block identities holds. Fail
			// with the reason rather than reporting a root mismatch.
			return fmt.Errorf("block %d is post-Helicon (SAE): the no-execution verifier still checks per-block roots and receipts, which ACP-194 moved to settlement; it needs the settled-root ring and the merged receipt range", n)
		}
		hdrHash := hdr.Hash()

		if dOK && dBlk == n {
			if err := v.applyBlock(n, &hdr, hdrHash, dRows); err != nil {
				return err
			}
			if dBlk, dRows, dOK, err = cur.Next(); err != nil {
				return err
			}
		} else {
			// No diffs: the header must claim no state change (exec's
			// empty-block fast path never captures a frame otherwise).
			if hdr.Root != v.parentRoot {
				return fmt.Errorf("block %d: no stored diffs but root changes %x -> %x", n, v.parentRoot, hdr.Root)
			}
			v.fw.SetHashAndHeight(hdrHash, n)
			v.fwHeight = n
		}
		v.parentRoot = hdr.Root
		v.parentHash = hdrHash

		if time.Since(lastLog) >= 30*time.Second {
			done := n - e.Start + 1
			log.Printf("verify: epoch_%d_%d at block %d (%.0f blk/s)",
				e.Start, e.Count, n, float64(done)/time.Since(epochT0).Seconds())
			lastLog = time.Now()
		}
		return nil
	})
	if walkErr != nil {
		return walkErr
	}
	if dOK {
		return fmt.Errorf("SST rows for block %d beyond the header walk", dBlk)
	}
	return nil
}

// applyBlock proposes one block's diffs on top of parentRoot and commits
// it after the root matches header.Root.
func (v *Verifier) applyBlock(n uint64, hdr *types.Header, hdrHash common.Hash, rows []state.StateRow) error {
	tr, err := v.db.OpenTrie(v.parentRoot)
	if err != nil {
		return fmt.Errorf("block %d: open trie at %x: %w", n, v.parentRoot, err)
	}
	if err := applyRows(tr, rows); err != nil {
		return fmt.Errorf("block %d: %w", n, err)
	}
	root := tr.Hash()
	if root == (common.Hash{}) {
		return fmt.Errorf("block %d: firewood proposal failed", n)
	}
	if root != hdr.Root {
		return fmt.Errorf("state root mismatch at block %d: computed %x, header %x", n, root, hdr.Root)
	}
	opt := stateconf.WithTrieDBUpdatePayload(hdr.ParentHash, hdrHash)
	if err := v.tdb.Update(root, v.parentRoot, v.fwHeight+1, nil, nil, opt); err != nil {
		return fmt.Errorf("block %d: firewood update: %w", n, err)
	}
	if err := v.tdb.Commit(root, false); err != nil {
		return fmt.Errorf("block %d: firewood commit: %w", n, err)
	}
	v.fwHeight++
	return nil
}

// applyRows replays post-image rows (sorted by key: account ops land
// before storage ops, exactly the commit-time trie-op order) onto a
// Firewood account trie. Key layout is state cook.go's: kind byte
// ('a'/'s') | addr 20 | slot 32.
func applyRows(tr ethstate.Trie, rows []state.StateRow) error {
	for i := range rows {
		r := &rows[i]
		addr := common.BytesToAddress(r.Key[1:21])
		switch r.Key[0] {
		case 'a':
			if len(r.Value) == 0 {
				if err := tr.DeleteAccount(addr); err != nil {
					return err
				}
				continue
			}
			var acc types.StateAccount
			if err := rlp.DecodeBytes(r.Value, &acc); err != nil {
				return fmt.Errorf("decode account %x: %w", addr, err)
			}
			if err := tr.UpdateAccount(addr, &acc); err != nil {
				return err
			}
		case 's':
			if len(r.Value) == 0 {
				if err := tr.DeleteStorage(addr, r.Key[21:53]); err != nil {
					return err
				}
				continue
			}
			if err := tr.UpdateStorage(addr, r.Key[21:53], r.Value); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unknown row kind %q", r.Key[0])
		}
	}
	return nil
}

// checkBodies verifies blocks [from, to): the container's embedded header
// must hash to the stored header, txRoot recomputed from the verbatim
// transactions must match, and receiptsRoot recomputed from receipts
// reconstructed out of the stored logs + gasUsed/status must match.
func checkBodies(e *state.Epoch, from, to uint64) error {
	type hinfo struct{ hash, txHash, rcptHash common.Hash }
	hs := make([]hinfo, to-from)
	if err := e.WalkHeadersRange(from, to, func(n uint64, hdrRLP []byte) error {
		var hdr types.Header
		if err := rlp.DecodeBytes(hdrRLP, &hdr); err != nil {
			return fmt.Errorf("decode header %d: %w", n, err)
		}
		hs[n-from] = hinfo{hdr.Hash(), hdr.TxHash, hdr.ReceiptHash}
		return nil
	}); err != nil {
		return err
	}
	return e.WalkContainersRange(from, to, func(n uint64, raw []byte) error {
		blk, err := exec.ParseEthBlock(raw)
		if err != nil {
			return fmt.Errorf("parse container %d: %w", n, err)
		}
		if blk.NumberU64() != n {
			return fmt.Errorf("container %d has internal number %d", n, blk.NumberU64())
		}
		hi := hs[n-from]
		if blk.Hash() != hi.hash {
			return fmt.Errorf("block %d: container header hash %x != stored header %x", n, blk.Hash(), hi.hash)
		}
		txs := blk.Transactions()
		if got := types.DeriveSha(txs, trie.NewStackTrie(nil)); got != hi.txHash {
			return fmt.Errorf("tx root mismatch at block %d: computed %x, header %x", n, got, hi.txHash)
		}
		receipts, err := reconstructReceipts(e, n, txs)
		if err != nil {
			return err
		}
		if got := types.DeriveSha(receipts, trie.NewStackTrie(nil)); got != hi.rcptHash {
			return fmt.Errorf("receipts root mismatch at block %d: computed %x, header %x", n, got, hi.rcptHash)
		}
		return nil
	})
}

// reconstructReceipts rebuilds block n's consensus receipts from the v2
// stored sections: per-tx gasUsed/status (cumulative gas by prefix sum),
// logs reattached by txIndex, blooms recomputed from the logs.
func reconstructReceipts(e *state.Epoch, n uint64, txs types.Transactions) (types.Receipts, error) {
	rec, ok, err := e.StoredRcptRecord(n)
	if err != nil {
		return nil, fmt.Errorf("block %d: stored receipts: %w", n, err)
	}
	if !ok {
		if len(txs) > 0 {
			return nil, fmt.Errorf("block %d: %d txs but no stored receipt record", n, len(txs))
		}
		return nil, nil
	}
	srs, err := state.DecodeStoredReceipts(rec)
	if err != nil {
		return nil, fmt.Errorf("block %d: %w", n, err)
	}
	if len(srs) != len(txs) {
		return nil, fmt.Errorf("block %d: %d stored receipts for %d txs", n, len(srs), len(txs))
	}
	receipts := make(types.Receipts, len(txs))
	for i, tx := range txs {
		receipts[i] = &types.Receipt{
			Type:              tx.Type(),
			Status:            srs[i].Status,
			CumulativeGasUsed: srs[i].CumulativeGas,
		}
	}
	logsRec, ok, err := e.StoredLogsRecord(n)
	if err != nil {
		return nil, fmt.Errorf("block %d: stored logs: %w", n, err)
	}
	if ok {
		logs, err := state.DecodeStoredLogs(logsRec)
		if err != nil {
			return nil, fmt.Errorf("block %d: %w", n, err)
		}
		for i := range logs {
			l := &logs[i]
			if int(l.TxIndex) >= len(receipts) {
				return nil, fmt.Errorf("block %d: stored log txIndex %d out of range", n, l.TxIndex)
			}
			receipts[l.TxIndex].Logs = append(receipts[l.TxIndex].Logs, &types.Log{
				Address: l.Address, Topics: l.Topics, Data: l.Data,
			})
		}
	}
	for _, r := range receipts {
		r.Bloom = types.CreateBloom(types.Receipts{r})
	}
	return receipts, nil
}

// VerifySet verifies every epoch of an already-downloaded set in chain
// order (the standalone `epochdb verify` path). Returns total blocks and
// wall time for the runbook numbers.
func VerifySet(dataDir, tmpDir string, networkID uint32, workers int) (blocks uint64, wall time.Duration, err error) {
	set, err := state.OpenEpochSet(dataDir)
	if err != nil {
		return 0, 0, err
	}
	defer set.Close()
	if len(set.Epochs) == 0 {
		return 0, 0, fmt.Errorf("no sealed epochs in %s", dataDir)
	}
	v, err := New(tmpDir, networkID, workers)
	if err != nil {
		return 0, 0, err
	}
	defer v.Close()
	t0 := time.Now()
	for _, e := range set.Epochs {
		if err := v.VerifyEpoch(e); err != nil {
			return v.blocks, time.Since(t0), fmt.Errorf("epoch_%d_%d: %w", e.Start, e.Count, err)
		}
	}
	return v.blocks, time.Since(t0), nil
}

// Cursor is the pipelined bootstrap companion: dist notifies it as epochs
// become locally present (in any order) and a single worker goroutine
// verifies the contiguous prefix in chain order. Done() yields nil once
// all `total` epochs verified, or the first error (which names the epoch).
type Cursor struct {
	dataDir string
	ch      chan string
	done    chan error
}

// StartCursor launches the verification worker. total is the manifest
// epoch count; the worker exits after that many epochs verify.
func StartCursor(dataDir, tmpDir string, networkID uint32, workers, total int) *Cursor {
	c := &Cursor{dataDir: dataDir, ch: make(chan string, 2*total+8), done: make(chan error, 1)}
	go c.run(tmpDir, networkID, workers, total)
	return c
}

// Notify reports one locally-present epoch file name. Safe to call from
// multiple goroutines; duplicates are ignored.
func (c *Cursor) Notify(name string) { c.ch <- name }

// Done yields the final verdict once.
func (c *Cursor) Done() <-chan error { return c.done }

func (c *Cursor) run(tmpDir string, networkID uint32, workers, total int) {
	v, err := New(tmpDir, networkID, workers)
	if err != nil {
		c.done <- err
		return
	}
	defer v.Close()
	pending := map[uint64]string{} // epoch start -> file name
	seen := map[string]bool{}
	verified := 0
	for name := range c.ch {
		start, _, ok := state.ParseEpochFileName(name)
		if !ok || seen[name] {
			continue
		}
		seen[name] = true
		pending[start] = name
		for {
			nm, ok := pending[v.next]
			if !ok {
				break
			}
			e, err := state.OpenEpoch(filepath.Join(c.dataDir, nm))
			if err != nil {
				c.done <- fmt.Errorf("verify %s: %w", nm, err)
				return
			}
			delete(pending, e.Start)
			err = v.VerifyEpoch(e)
			e.Close()
			if err != nil {
				c.done <- fmt.Errorf("verify %s: %w", nm, err)
				return
			}
			if verified++; verified == total {
				c.done <- nil
				return
			}
		}
		if len(seen) == total && verified < total {
			c.done <- fmt.Errorf("all %d epochs present but the set is not contiguous: no epoch starts at block %d", total, v.next)
			return
		}
	}
}

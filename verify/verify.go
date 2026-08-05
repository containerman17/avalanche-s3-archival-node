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
	"runtime"
	"sync"
	"time"

	"github.com/ava-labs/avalanchego/graft/evm/firewood"
	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/rawdb"
	ethstate "github.com/ava-labs/libevm/core/state"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/libevm/stateconf"
	"github.com/ava-labs/libevm/rlp"
	"github.com/ava-labs/libevm/trie"
	"github.com/ava-labs/libevm/triedb"

	"github.com/containerman17/epochdb/chain"
	"github.com/containerman17/epochdb/dist"
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

// New opens a fresh throwaway Firewood under tmpDir and anchors it at c's
// genesis. THE CHAIN IS REQUIRED: an anchorless verifier adopts whatever
// ParentHash the data claims as its anchor, which checks that the corpus is
// self-consistent and NOT that it is this chain, so any internally consistent
// forgery passes. Only the in-package tests may ask for that (newAnchorless).
//
// The VM kind comes off the descriptor and picks BOTH the libevm extras (which
// decide how a header decodes and hashes) and the throwaway's state database,
// exactly as exec does. Anchorless mode has no descriptor, so it takes whatever
// kind the process already registered, and coreth when nothing has.
func New(tmpDir string, c *chain.Chain, workers int) (*Verifier, error) {
	if c == nil {
		return nil, fmt.Errorf("verify: no resolved chain: verification without a genesis anchor only proves self-consistency")
	}
	return newVerifier(tmpDir, c, workers)
}

// newAnchorless is New's test-only mode: no genesis anchor, the first header's
// ParentHash becomes the anchor.
func newAnchorless(tmpDir string, workers int) (*Verifier, error) {
	return newVerifier(tmpDir, nil, workers)
}

func newVerifier(tmpDir string, c *chain.Chain, workers int) (*Verifier, error) {
	kind := fetch.RegisteredKind()
	if c != nil {
		kind = c.VMKind
	}
	if kind == "" {
		kind = chain.Coreth
	}
	fetch.RegisterExtras(kind)
	tdb, fw, db, err := newThrowawayFirewood(tmpDir)
	if err != nil {
		return nil, err
	}
	if workers <= 0 {
		workers = runtime.GOMAXPROCS(0)
	}
	v := &Verifier{tmp: tmpDir, tdb: tdb, fw: fw, db: db, workers: workers, next: 1,
		parentRoot: types.EmptyRootHash}
	if c != nil {
		g, err := exec.ChainGenesis(c)
		if err != nil {
			tdb.Close()
			return nil, err
		}
		if !tdb.Initialized(g.Root) {
			if err := g.Commit(rawdb.NewMemoryDatabase(), tdb); err != nil {
				tdb.Close()
				return nil, fmt.Errorf("commit genesis: %w", err)
			}
		}
		v.parentRoot = g.Root
		v.parentHash = g.Hash
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
	return tdb, fw, exec.NewStateDatabase(memdb, tdb), nil
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
//
// EVERY BLOCK MUST BE EARNED. Both halves count the blocks they actually
// checked and must account for all e.Count of them: a check that could not run
// is a failure, never a pass, and the PASS line reports the counted number
// rather than the epoch's claimed block count.
func (v *Verifier) VerifyEpoch(e *state.Epoch) error {
	if e.Start != v.next {
		return fmt.Errorf("epoch_%d_%d out of order: next expected block %d", e.Start, e.Count, v.next)
	}
	t0 := time.Now()

	// Bodies + receipts: embarrassingly parallel chunk workers.
	var (
		wg         sync.WaitGroup
		mu         sync.Mutex
		bodyErr    error
		bodyBlocks uint64
		perChunk   = (e.Count + uint64(v.workers) - 1) / uint64(v.workers)
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
			done, err := checkBodies(e, from, to)
			mu.Lock()
			bodyBlocks += done
			if err != nil && bodyErr == nil {
				bodyErr = err
			}
			mu.Unlock()
		}()
	}

	stateBlocks, stateErr := v.verifyState(e)
	wg.Wait()
	if stateErr != nil {
		return stateErr
	}
	if bodyErr != nil {
		return bodyErr
	}
	if stateBlocks != e.Count {
		return fmt.Errorf("epoch_%d_%d: state verification covered %d of %d blocks", e.Start, e.Count, stateBlocks, e.Count)
	}
	if bodyBlocks != e.Count {
		return fmt.Errorf("epoch_%d_%d: body verification covered %d of %d blocks", e.Start, e.Count, bodyBlocks, e.Count)
	}

	v.next = e.End() + 1
	v.blocks += stateBlocks
	dt := time.Since(t0)
	log.Printf("verify: epoch_%d_%d PASS: %d blocks in %s (%.0f blk/s)",
		e.Start, e.Count, stateBlocks, dt.Round(time.Second), float64(stateBlocks)/dt.Seconds())
	return nil
}

// verifyState replays the epoch's per-block post-image diffs into the
// throwaway Firewood, committing per block; every computed root must
// equal header.Root. Also checks the header parent-hash chain (it walks
// the headers anyway). Returns the number of blocks it actually checked,
// which the caller requires to be the whole epoch.
func (v *Verifier) verifyState(e *state.Epoch) (uint64, error) {
	cur, err := e.SpillDiffs(v.tmp)
	if err != nil {
		return 0, fmt.Errorf("spill diffs: %w", err)
	}
	defer cur.Close()
	dBlk, dRows, dOK, err := cur.Next()
	if err != nil {
		return 0, err
	}
	var done uint64
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
		done++

		if time.Since(lastLog) >= 30*time.Second {
			done := n - e.Start + 1
			log.Printf("verify: epoch_%d_%d at block %d (%.0f blk/s)",
				e.Start, e.Count, n, float64(done)/time.Since(epochT0).Seconds())
			lastLog = time.Now()
		}
		return nil
	})
	if walkErr != nil {
		return done, walkErr
	}
	if dOK {
		return done, fmt.Errorf("SST rows for block %d beyond the header walk", dBlk)
	}
	if done != e.Count {
		return done, fmt.Errorf("epoch_%d_%d: header walk yielded %d of %d blocks: blocks [%d,%d] were never state-checked",
			e.Start, e.Count, done, e.Count, e.Start+done, e.End())
	}
	return done, nil
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
//
// It returns how many blocks it CHECKED. Unlike the state half there is no
// continuity to fall over (each container is checked against its own header),
// so a containers frame that yields fewer payloads than its block group would
// otherwise skip every check here and still return nil: the count is what
// makes that a failure.
func checkBodies(e *state.Epoch, from, to uint64) (uint64, error) {
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
		return 0, err
	}
	var done uint64
	err := e.WalkContainersRange(from, to, func(n uint64, raw []byte) error {
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
		done++
		return nil
	})
	if err != nil {
		return done, err
	}
	if done != to-from {
		return done, fmt.Errorf("epoch_%d_%d: container walk yielded %d of %d blocks: blocks [%d,%d) were never body-checked",
			e.Start, e.Count, done, to-from, from+done, to)
	}
	return done, nil
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

// VerifySet verifies every epoch of the local set in chain order (the
// standalone `epochdb dev verify` path, `serve --verify` and `dev bootstrap --verify`, which just runs
// it once the hash chain has been walked). Reads pull whatever bytes they need
// through dist, so a node with S3 credentials verifies history it does not
// hold locally. Returns total blocks and wall time for the runbook numbers.
func VerifySet(st *dist.Store, tmpDir string, c *chain.Chain, workers int) (blocks uint64, wall time.Duration, err error) {
	set, err := state.OpenEpochSet(st)
	if err != nil {
		return 0, 0, err
	}
	defer set.Close()
	eps := set.All()
	if len(eps) == 0 {
		return 0, 0, fmt.Errorf("no sealed epochs indexed in %s", st.Dir())
	}
	// The chain root is sha256(genesisData), so this comparison answers exactly
	// one question, up front and by name: is this corpus the chain the caller
	// resolved? A WRONG upgrade.json is NOT caught here and is not meant to be
	// (amended 2026-08-05): upgrades apply inside blocks, never to genesis, so
	// they cannot move the anchor, and a semantically wrong one diverges at its
	// activation height where VerifyEpoch's state-root check hard-stops on it.
	//
	// The anchor comparison is MANDATORY, not best-effort: it used to be
	// skipped whenever it could not be made (no chain, or a set not starting
	// at block 1), which silently downgraded the whole run to a
	// self-consistency check.
	if c == nil {
		return 0, 0, fmt.Errorf("verify %s: no resolved chain, so the corpus cannot be anchored: verification would only prove self-consistency", st.Dir())
	}
	if eps[0].Start != 1 {
		return 0, 0, fmt.Errorf("verify %s: epoch set starts at block %d, not 1: there is no chain root to anchor it to", st.Dir(), eps[0].Start)
	}
	if root := c.Root(); eps[0].Prev != root {
		return 0, 0, fmt.Errorf("epoch_%d_%d anchors at chain root %x, but %s resolves to %x: WRONG CHAIN (the chain root is sha256(genesisData), the P-chain CreateChainTx field verbatim)",
			eps[0].Start, eps[0].Count, eps[0].Prev, st.Dir(), root)
	}
	v, err := New(tmpDir, c, workers)
	if err != nil {
		return 0, 0, err
	}
	defer v.Close()
	t0 := time.Now()
	for _, e := range eps {
		if err := v.VerifyEpoch(e); err != nil {
			return v.blocks, time.Since(t0), fmt.Errorf("epoch_%d_%d: %w", e.Start, e.Count, err)
		}
	}
	return v.blocks, time.Since(t0), nil
}

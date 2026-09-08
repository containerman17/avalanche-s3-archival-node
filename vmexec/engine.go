package vmexec

import (
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/crypto"
	"github.com/ava-labs/libevm/rlp"
	"github.com/holiman/uint256"

	"github.com/containerman17/avalanche-s3-archival-node/commit"
	"github.com/containerman17/avalanche-s3-archival-node/latest"
)

// engine is the latest state and its trie: a fresh overlay (writes since the
// last freeze), an optional frozen overlay (being merged in the background),
// the base runs, and the commit file plus its dirty overlay for per-block
// roots. Everything here runs on the executor goroutine except the roll
// goroutine, which reads only the frozen overlay and the runs it was handed.
//
// No restart: the engine is rebuilt from genesis on every open (prototype).
type engine struct {
	dir     string
	overlay *latest.Overlay
	frozen  *latest.Overlay
	runs    []*latest.Run
	view    *latest.View // overlay, frozen, runs: rebuilt on every swap
	file    *commit.File
	dirty   *commit.Dirty
	gen     int
	// owners is the set of accounts with a slot written into the fresh
	// overlay; frozenOwners the same for the frozen one. An account delete
	// scans the overlays for slots to tombstone ONLY when one of these or the
	// runs says it has any: Overlay.Iter is a full snapshot and sort, and
	// EIP-158 empty-account deletes happen on most blocks.
	owners       map[common.Hash]struct{}
	frozenOwners map[common.Hash]struct{}

	rollRoot common.Hash // the root the frozen overlay's state must roll to
	rollH    uint64
	rollT0   time.Time
	rollDone chan rollResult
	rolls    int
}

type rollResult struct {
	run   *latest.Run
	file  *commit.File
	root  common.Hash
	stats commit.Stats
	merge time.Duration
	roll  time.Duration
	err   error
}

// accountRow is the contract's account value: RLP[nonce, balance, codeHash].
type accountRow struct {
	Nonce    uint64
	Balance  *uint256.Int
	CodeHash []byte
}

func accountKey(h common.Hash) []byte { return append(h[:], 0) }

func slotKey(ah common.Hash, slotHash common.Hash) []byte {
	k := make([]byte, 0, 65)
	k = append(k, ah[:]...)
	k = append(k, 1)
	return append(k, slotHash[:]...)
}

func heightUser(h uint64) [32]byte {
	var u [32]byte
	binary.LittleEndian.PutUint64(u[:], h)
	return u
}

func (s *engine) runPath(gen int) string  { return filepath.Join(s.dir, fmt.Sprintf("run.%d", gen)) }
func (s *engine) triePath(gen int) string { return filepath.Join(s.dir, fmt.Sprintf("trie.%d", gen)) }

// newEngine seeds the state from the genesis alloc, merges it into the first
// run, rolls the first trie file, and checks the rolled root against want:
// the first oracle.
func newEngine(dir string, alloc types.GenesisAlloc, want common.Hash) (*engine, error) {
	if err := os.RemoveAll(dir); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	s := &engine{dir: dir, overlay: latest.NewOverlay(), rollDone: make(chan rollResult, 1), owners: map[common.Hash]struct{}{}}
	for addr, a := range alloc {
		ah := crypto.Keccak256Hash(addr[:])
		row := accountRow{Nonce: a.Nonce, Balance: new(uint256.Int), CodeHash: types.EmptyCodeHash[:]}
		if a.Balance != nil {
			row.Balance = uint256.MustFromBig(a.Balance)
		}
		if len(a.Code) > 0 {
			row.CodeHash = crypto.Keccak256(a.Code)
		}
		val, err := rlp.EncodeToBytes(&row)
		if err != nil {
			return nil, err
		}
		s.overlay.Put(accountKey(ah), val)
		for slot, v := range a.Storage {
			if v == (common.Hash{}) {
				continue
			}
			s.overlay.Put(slotKey(ah, crypto.Keccak256Hash(slot[:])), common.TrimLeftZeroes(v[:]))
		}
	}
	t0 := time.Now()
	run, err := latest.Merge(s.runPath(0), latest.NewView(s.overlay), heightUser(0))
	if err != nil {
		return nil, fmt.Errorf("genesis merge: %w", err)
	}
	root, st, err := commit.Roll(run.Iter(nil, nil), s.triePath(0), heightUser(0))
	if err != nil {
		return nil, fmt.Errorf("genesis roll: %w", err)
	}
	if root != want {
		return nil, fmt.Errorf("genesis root mismatch: rolled %x, header %x", root, want)
	}
	file, err := commit.Open(s.triePath(0))
	if err != nil {
		return nil, err
	}
	log.Printf("vmexec: genesis state ok: root=%x accounts=%d keys=%d nodes=%d run=%dB trie=%dB in %s",
		root, len(alloc), st.Keys, st.Nodes, run.Bytes(), st.Bytes, time.Since(t0).Round(time.Millisecond))
	s.overlay = latest.NewOverlay()
	s.runs = []*latest.Run{run}
	s.file = file
	s.dirty = commit.NewDirty(file, s.seek)
	s.dirty.Workers = runtime.NumCPU()
	s.rebuildView()
	return s, nil
}

func (s *engine) rebuildView() {
	ovs := []*latest.Overlay{s.overlay}
	if s.frozen != nil {
		ovs = append(ovs, s.frozen)
	}
	s.view = latest.NewMultiView(ovs, s.runs...)
}

// seek is Dirty's leaf source: the first row of the ROLLED state (the runs,
// never the overlays) at or after prefix. Safe for concurrent calls.
func (s *engine) seek(prefix []byte) (key, value []byte) {
	it := latest.NewView(nil, s.runs...).Iter(prefix, nil)
	if !it.Next() {
		return nil, nil
	}
	return it.Key(), it.Value()
}

func (s *engine) get(key []byte) ([]byte, bool) { return s.view.Get(key) }

// apply folds one block's ordered write set into the overlay and the dirty
// trie. An account delete tombstones every live slot under it in the overlay
// (Dirty wipes the storage itself).
func (s *engine) apply(ws *writeSet) error {
	for _, op := range ws.ops {
		switch {
		case len(op.k) == 33 && len(op.v) == 0:
			s.tombstoneSlots(op.k[:32])
		case len(op.k) == 65 && len(op.v) > 0:
			s.owners[common.BytesToHash(op.k[:32])] = struct{}{}
		}
		s.overlay.Put(op.k, op.v)
		if err := s.dirty.Apply(op.k, op.v); err != nil {
			return err
		}
	}
	return nil
}

func (s *engine) tombstoneSlots(ah []byte) {
	lo := append(append([]byte{}, ah...), 1)
	hi := append(append([]byte{}, ah...), 2)
	h := common.BytesToHash(ah)
	_, fresh := s.owners[h]
	_, frozen := s.frozenOwners[h]
	if !fresh && !frozen && !latest.NewView(nil, s.runs...).Iter(lo, hi).Next() {
		return
	}
	var keys [][]byte
	it := s.view.Iter(lo, hi)
	for it.Next() {
		keys = append(keys, append([]byte(nil), it.Key()...))
	}
	for _, k := range keys {
		s.overlay.Put(k, nil)
	}
}

// root recomputes the state root over the dirty paths.
func (s *engine) root() (common.Hash, error) { return s.dirty.Root() }

// maybeRoll freezes the overlay once it is over budget and merges + rolls it
// in the background. h and root are the height and root the overlay's state
// is at, the oracle for the rolled file.
func (s *engine) maybeRoll(budget int, h uint64, root common.Hash) {
	if s.frozen != nil || s.overlay.Bytes() < budget {
		return
	}
	s.frozen, s.overlay = s.overlay, latest.NewOverlay()
	s.frozenOwners, s.owners = s.owners, map[common.Hash]struct{}{}
	s.rebuildView()
	s.rollRoot, s.rollH, s.rollT0 = root, h, time.Now()
	gen := s.gen + 1
	view := latest.NewView(s.frozen, s.runs...)
	log.Printf("vmexec: roll %d start: height=%d overlay=%d keys/%.0fMB dirty=%.0fMB runs=%d",
		gen, h, s.frozen.Len(), float64(s.frozen.Bytes())/1e6, float64(s.dirty.Bytes())/1e6, len(s.runs))
	go func() {
		var r rollResult
		t0 := time.Now()
		r.run, r.err = latest.Merge(s.runPath(gen), view, heightUser(h))
		r.merge = time.Since(t0)
		if r.err == nil {
			t1 := time.Now()
			r.root, r.stats, r.err = commit.Roll(r.run.Iter(nil, nil), s.triePath(gen), heightUser(h))
			r.roll = time.Since(t1)
		}
		if r.err == nil {
			r.file, r.err = commit.Open(s.triePath(gen))
		}
		s.rollDone <- r
	}()
}

// finishRoll swaps a finished roll in: runs = [new], Dirty rebased on the new
// file, and every write of the fresh overlay re-applied so Dirty's base is
// the new file. Called between blocks on the executor goroutine.
func (s *engine) finishRoll() error {
	select {
	case r := <-s.rollDone:
		if r.err != nil {
			s.frozen = nil // the roll is dead; close must not wait for it
			return fmt.Errorf("roll: %w", r.err)
		}
		if r.root != s.rollRoot {
			s.frozen = nil
			return fmt.Errorf("roll root mismatch at height %d: rolled %x, verified %x", s.rollH, r.root, s.rollRoot)
		}
		dirtyBefore := s.dirty.Bytes()
		old, oldFile, oldGen := s.runs, s.file, s.gen
		s.gen++
		s.runs = []*latest.Run{r.run}
		s.file = r.file
		s.frozen = nil
		s.frozenOwners = nil
		s.rebuildView()
		s.dirty.Reset(r.file)
		n := 0
		it := s.overlay.Iter(nil, nil)
		for it.Next() {
			if err := s.dirty.Apply(it.Key(), it.Value()); err != nil {
				return err
			}
			n++
		}
		for _, o := range old {
			o.Close()
		}
		oldFile.Close()
		for g := oldGen; g < s.gen; g++ {
			os.Remove(s.runPath(g))
			os.Remove(s.triePath(g))
		}
		s.rolls++
		log.Printf("vmexec: roll %d done: height=%d keys=%d nodes=%d run=%.0fMB trie=%.0fMB merge=%s roll=%s total=%s replayed=%d overlay=%.0fMB dirty=%.0fMB->%.0fMB",
			s.gen, s.rollH, r.stats.Keys, r.stats.Nodes, float64(r.run.Bytes())/1e6, float64(r.stats.Bytes)/1e6,
			r.merge.Round(time.Millisecond), r.roll.Round(time.Millisecond), time.Since(s.rollT0).Round(time.Millisecond),
			n, float64(s.overlay.Bytes())/1e6, float64(dirtyBefore)/1e6, float64(s.dirty.Bytes())/1e6)
	default:
	}
	return nil
}

func (s *engine) close() {
	if s.frozen != nil {
		r := <-s.rollDone // let the roll finish so its files are not torn
		if r.err == nil {
			r.run.Close()
			r.file.Close()
		}
	}
	for _, r := range s.runs {
		r.Close()
	}
	s.file.Close()
}

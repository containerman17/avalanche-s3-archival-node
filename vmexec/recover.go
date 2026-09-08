package vmexec

import (
	"fmt"
	"log"
	"slices"
	"time"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/crypto"
	"github.com/ava-labs/libevm/rlp"

	"github.com/containerman17/avalanche-s3-archival-node/dist"
	"github.com/containerman17/avalanche-s3-archival-node/store"
)

// rebuild replays the store's state rows written after the roll (every
// TxNum past the roll height's boundary slot, through head's) into the
// overlay and Dirty, as executing the blocks would have: per key the latest
// row wins, an account row of empty value is a delete that takes the slots
// with it, and an account destroyed and recreated inside the window is
// deleted first so its old slots go. Code-hash rows are skipped: the hash
// rides in the account row. The store's window must have been flushed: the
// rows are read from the runs.
func (s *engine) rebuild(db *store.DB, cas *dist.Store, rolledAt, head uint64) (rows, keys int, err error) {
	lo := uint64(0)
	if rolledAt > 0 {
		end, ok, err := db.TxNumAtEndOf(rolledAt)
		if err != nil || !ok {
			return 0, 0, fmt.Errorf("rebuild: block %d: %v (ok=%v)", rolledAt, err, ok)
		}
		lo = end + 1
	}
	hi, ok, err := db.TxNumAtEndOf(head)
	if err != nil || !ok {
		return 0, 0, fmt.Errorf("rebuild: block %d: %v (ok=%v)", head, err, ok)
	}
	type row struct {
		tx  uint64
		val []byte
	}
	latest := map[string]row{}
	dels := map[string]uint64{} // address -> newest account delete in the window
	for _, ref := range db.Manifest().Runs {
		if ref.ToTx <= lo || ref.FromTx > hi {
			continue
		}
		r, err := store.OpenRun(cas, ref.Name)
		if err != nil {
			return 0, 0, fmt.Errorf("rebuild: %w", err)
		}
		err = r.ScanRange(store.SecState, []byte(store.PrefixState), []byte("state0"), func(k, v []byte) bool {
			rows++
			n := store.TxNumOf(k)
			if n < lo || n > hi {
				return true
			}
			key := string(k[:len(k)-8])
			if cur, ok := latest[key]; !ok || n > cur.tx {
				latest[key] = row{n, append([]byte(nil), v...)}
			}
			if len(key) == len(store.PrefixState)+23 && key[len(key)-2] == 'a' && len(v) == 0 {
				if addr := key[len(store.PrefixState) : len(store.PrefixState)+20]; n > dels[addr] {
					dels[addr] = n
				}
			}
			return true
		})
		r.Close()
		if err != nil {
			return 0, 0, fmt.Errorf("rebuild: %s: %w", ref.Name, err)
		}
	}
	sorted := make([]string, 0, len(latest))
	for k := range latest {
		sorted = append(sorted, k)
	}
	slices.Sort(sorted) // an address's a row precedes its c and s rows
	ws := newWriteSet()
	var (
		cur     string
		ah      common.Hash
		deleted bool
		delTx   uint64
		hasDel  bool
	)
	for _, k := range sorted {
		kind, addr, slot, err := store.SplitStateKey([]byte(k))
		if err != nil {
			return 0, 0, fmt.Errorf("rebuild: %w", err)
		}
		if string(addr) != cur {
			cur = string(addr)
			ah = crypto.Keccak256Hash(addr)
			delTx, hasDel = dels[cur]
			deleted = false
		}
		r := latest[k]
		switch kind {
		case 'a':
			if len(r.val) == 0 {
				deleted = true
				ws.put(accountKey(ah), nil)
				continue
			}
			if hasDel { // destroyed, then recreated: the old slots go first
				ws.put(accountKey(ah), nil)
			}
			var acc types.StateAccount
			if err := rlp.DecodeBytes(r.val, &acc); err != nil {
				return 0, 0, fmt.Errorf("rebuild: account %x: %w", addr, err)
			}
			val, err := rlp.EncodeToBytes(&accountRow{Nonce: acc.Nonce, Balance: acc.Balance, CodeHash: acc.CodeHash})
			if err != nil {
				return 0, 0, err
			}
			ws.put(accountKey(ah), val)
		case 's':
			if deleted || (hasDel && r.tx < delTx) {
				continue
			}
			ws.put(slotKey(ah, crypto.Keccak256Hash(slot)), r.val)
		}
	}
	s.applyOverlay(ws)
	if err := s.applyDirty(ws); err != nil {
		return 0, 0, err
	}
	return rows, len(ws.ops), nil
}

// recover checks the rolled trie against the chain and brings the engine to
// the store's head: the header there is what the executor resumes from. A
// root mismatch after the rebuild is death, as it is for a block.
func (e *Executor) recover(m manifest, genesisRoot common.Hash, head uint64) error {
	t0 := time.Now()
	want := genesisRoot
	if m.Height > 0 {
		hdr, err := e.header(m.Height)
		if err != nil {
			return err
		}
		want = hdr.Root
	}
	if got := e.eng.file.Root(); got != want {
		return fmt.Errorf("vmstate rolled at height %d with root %x, but the chain's root there is %x", m.Height, got, want)
	}
	if head < m.Height {
		return fmt.Errorf("vmstate rolled at height %d, but the store holds blocks only through %d", m.Height, head)
	}
	var rows, keys int
	if head > 0 {
		hdr, err := e.header(head)
		if err != nil {
			return err
		}
		if head > m.Height {
			if err := e.cfg.Store.Flush(); err != nil {
				return fmt.Errorf("recover: flush window: %w", err)
			}
			if rows, keys, err = e.eng.rebuild(e.cfg.Store, e.cfg.CAS, m.Height, head); err != nil {
				return err
			}
			root, err := e.eng.root()
			if err != nil {
				return err
			}
			if root != hdr.Root {
				log.Fatalf("vmexec: recovery: state rebuilt through height %d has root %x, header %x", head, root, hdr.Root)
			}
		}
		e.headNum, e.headRoot, e.headTime = head, hdr.Root, hdr.Time
	}
	e.live.Store(e.headNum)
	e.statsMu.Store(&Stats{Height: e.headNum})
	log.Printf("vmexec: recovered: rolled at %d (gen %d), head %d, rows scanned %d, keys applied %d, root ok, in %s",
		m.Height, m.Gen, head, rows, keys, time.Since(t0).Round(time.Millisecond))
	return nil
}

func (e *Executor) header(h uint64) (*types.Header, error) {
	raw, ok, err := e.cfg.Store.HeaderRLP(h)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("the store holds no header at %d", h)
	}
	var hdr types.Header
	if err := rlp.DecodeBytes(raw, &hdr); err != nil {
		return nil, fmt.Errorf("header %d: %w", h, err)
	}
	return &hdr, nil
}

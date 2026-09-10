package validator

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/utils/logging"
	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core"
	"github.com/ava-labs/libevm/core/state"
	"github.com/ava-labs/libevm/core/txpool"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/ethdb"
	"github.com/ava-labs/libevm/event"
	"github.com/ava-labs/libevm/params"
	"github.com/ava-labs/libevm/rlp"
	"github.com/ava-labs/libevm/trie"
	"github.com/ava-labs/libevm/trie/trienode"
	"github.com/ava-labs/libevm/triedb"
	"github.com/holiman/uint256"
	"go.uber.org/zap"
)

// poolChain is the txpool's BlockChain over the engine (libevm's pool, not
// subnet-evm's: that one links firewood's Rust runtime through
// subnet-evm/core; the engine's rules are enforced at build time). The head header is
// what Accept last decoded, StateAt hands out a state.StateDB whose only
// backing "trie" answers GetAccount from the engine (nonce + balance).
//
// The pool's head moves asynchronously (the reset walks every pending
// account: 570 ms at 100k pending, too slow for Accept), but never
// invisibly: Accept calls headMoving before the engine flips its height and
// the reset's goroutine calls headMoved when the subpool is on the new head.
// While a move is in flight the builder sees no pending txs and the pool's
// RPC reads (txpool_status, eth_getTransactionCount "pending", ...) wait, so
// no caller reads a mined tx as pending after seeing the block. libevm's
// own head event is not used: txpool.TxPool's loop would run the reset with
// no way to know when it landed (E2E.md, "Validator-side lessons" 5).
type poolChain struct {
	eng    *engine
	config *params.ChainConfig
	feed   event.Feed // subscribed by txpool.TxPool, never sent on
	sub    txpool.SubPool
	log    logging.Logger

	mu     sync.RWMutex
	head   *types.Header
	headID ids.ID

	moving  int // head moves in flight (Accept started, pool reset not landed)
	settled *sync.Cond
	onMoved func()            // the builder's wake-up
	removed map[string]uint64 // the drop handler's counters at the last reset
	counts  func() map[string]uint64

	acct *accountCache
}

func newPoolChain(eng *engine, config *params.ChainConfig, head *types.Header, headID ids.ID, log logging.Logger) *poolChain {
	c := &poolChain{eng: eng, config: config, log: log}
	c.settled = sync.NewCond(&c.mu)
	c.acct = &accountCache{eng: eng, warn: func(msg string, err error) { log.Warn(msg, zap.Error(err)) }, entries: map[common.Address]types.StateAccount{}}
	c.setHead(head, headID)
	return c
}

// headMoving: a head move starts (before the engine accepts, so a client
// that sees the new height finds the pool already waiting on it).
func (c *poolChain) headMoving() {
	c.mu.Lock()
	c.moving++
	c.mu.Unlock()
}

// headMoved: one move is over (the reset landed, or Accept failed).
func (c *poolChain) headMoved() {
	c.mu.Lock()
	c.moving--
	c.mu.Unlock()
	c.settled.Broadcast()
	if c.onMoved != nil {
		c.onMoved()
	}
}

// isSettled: no head move in flight (the builder's check, non-blocking).
func (c *poolChain) isSettled() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.moving == 0
}

// settle blocks until the pool is on the accepted head (the RPC reads).
func (c *poolChain) settle() {
	c.mu.Lock()
	for c.moving > 0 {
		c.settled.Wait()
	}
	c.mu.Unlock()
}

// setHead records the accepted head and starts the pool's reset on it. One
// crossing: every cached account is re-read at the new head so the reset
// (which reads every pending sender) never crosses. The caller's headMoving
// is paired with headMoved when the reset lands.
func (c *poolChain) setHead(h *types.Header, id ids.ID) {
	c.mu.Lock()
	old := c.head
	c.head, c.headID = h, id
	c.mu.Unlock()
	c.acct.refresh()
	if c.sub == nil {
		return // Initialize: the pool does not exist yet, it starts on this head
	}
	go func() {
		start := time.Now()
		c.sub.Reset(old, h) // demote the mined txs, promote the now-executable
		fields := []zap.Field{zap.Uint64("height", h.Number.Uint64()), zap.Duration("took", time.Since(start))}
		if c.counts != nil {
			now := c.counts()
			for reason, n := range now {
				if d := n - c.removed[reason]; d > 0 {
					fields = append(fields, zap.Uint64(reason, d)) // old = mined; anything else is a drop
				}
			}
			c.removed = now
		}
		c.headMoved()
		c.log.Info("validator: pool reset", fields...)
	}()
}

func (c *poolChain) Config() *params.ChainConfig { return c.config }

func (c *poolChain) CurrentBlock() *types.Header {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.head
}

func (c *poolChain) current() (*types.Header, ids.ID) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.head, c.headID
}

// GetBlock: the pool asks only on a reorg (never on a linear chain).
func (c *poolChain) GetBlock(hash common.Hash, number uint64) *types.Block {
	raw, err := c.eng.getBlock(ids.ID(hash))
	if err != nil {
		return nil
	}
	var blk types.Block
	if err := rlp.DecodeBytes(raw, &blk); err != nil || blk.NumberU64() != number {
		return nil
	}
	return &blk
}

// StateAt answers every root with the accepted head's accounts. The pool
// asks for the root of an accepted block only (its reset walks the accepted
// chain: CurrentBlock and GetBlock), and when its loop lags several accepts
// that root is below the head. The engine keeps no readable state for a
// rolled-past block, and the head's nonces and balances are what the pool
// wants anyway (the next reset would serve them). It must never fail: on an
// error legacypool logs "Failed to reset txpool state" and keeps its old
// state, so the pool's nonce view goes stale until another head lands (seen
// with a bounded root -> id map here: 4 accepts in 25 ms, one root evicted,
// 6k txs mined then no landings for 15 s).
func (c *poolChain) StateAt(root common.Hash) (*state.StateDB, error) {
	return state.New(root, (*stateDB)(c), nil)
}

func (c *poolChain) SubscribeChainHeadEvent(ch chan<- core.ChainHeadEvent) event.Subscription {
	return c.feed.Subscribe(ch)
}

// stateDB is the state.Database whose tries are engine readers.
type stateDB poolChain

func (d *stateDB) OpenTrie(root common.Hash) (state.Trie, error) {
	return &accountTrie{cache: (*poolChain)(d).acct, root: root}, nil
}

func (d *stateDB) OpenStorageTrie(common.Hash, common.Address, common.Hash, state.Trie) (state.Trie, error) {
	return nil, errUnsupported
}
func (d *stateDB) CopyTrie(t state.Trie) state.Trie { return t }
func (d *stateDB) ContractCode(common.Address, common.Hash) ([]byte, error) {
	return nil, errUnsupported
}
func (d *stateDB) ContractCodeSize(common.Address, common.Hash) (int, error) {
	return 0, errUnsupported
}
func (d *stateDB) DiskDB() ethdb.KeyValueStore { return nil }
func (d *stateDB) TrieDB() *triedb.Database    { return nil }

var errUnsupported = fmt.Errorf("validator: the pool state reader answers accounts only")

// accountCache: nonce + balance per address at the accepted head. Misses
// cross once per address (warm: once per batch); refresh re-reads every
// entry in one crossing when the head moves.
type accountCache struct {
	eng     *engine
	warn    func(string, error)
	mu      sync.Mutex
	entries map[common.Address]types.StateAccount
	errs    atomic.Uint64 // engine read failures (each one makes an account look empty to the pool)
}

const maxCachedAccounts = 8192

func (a *accountCache) refresh() {
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.entries) > maxCachedAccounts {
		a.entries = map[common.Address]types.StateAccount{}
	}
	if len(a.entries) == 0 {
		return
	}
	addrs := make([]common.Address, 0, len(a.entries))
	for addr := range a.entries {
		addrs = append(addrs, addr)
	}
	raw, err := a.eng.accountState(addrs, ids.Empty)
	if err != nil {
		a.errs.Add(1)
		a.warn("validator: account refresh failed, cache dropped", err)
		a.entries = map[common.Address]types.StateAccount{}
		return
	}
	for i, addr := range addrs {
		acc := decodeAccount(raw[i*40 : i*40+40])
		if prev := a.entries[addr]; acc.Nonce == 0 && acc.Balance.IsZero() && (prev.Nonce != 0 || (prev.Balance != nil && !prev.Balance.IsZero())) {
			// An account the pool knew as funded now reads empty: every tx of
			// that sender would be dropped as unpayable. Loud, it is a bug somewhere.
			a.warn(fmt.Sprintf("validator: account %s read empty at the new head (was nonce %d balance %s)", addr, prev.Nonce, prev.Balance), nil)
		}
		a.entries[addr] = acc
	}
}

// warm reads the accounts the cache lacks among addrs at the accepted head
// in one crossing: a batch's senders before the pool validates them one by
// one under its lock (a miss there is one crossing per sender).
func (a *accountCache) warm(addrs []common.Address) {
	a.mu.Lock()
	defer a.mu.Unlock()
	var miss []common.Address
	seen := make(map[common.Address]struct{}, len(addrs))
	for _, addr := range addrs {
		if _, dup := seen[addr]; dup || addr == (common.Address{}) {
			continue
		}
		seen[addr] = struct{}{}
		if _, ok := a.entries[addr]; !ok {
			miss = append(miss, addr)
		}
	}
	if len(miss) == 0 {
		return
	}
	raw, err := a.eng.accountState(miss, ids.Empty)
	if err != nil {
		a.errs.Add(1)
		a.warn("validator: account warm failed", err)
		return
	}
	for i, addr := range miss {
		a.entries[addr] = decodeAccount(raw[i*40 : i*40+40])
	}
}

// get answers at the accepted head (see StateAt). It used to key the cache
// by the root the pool asked for and miss on every read of a lagging reset:
// 3841 crossings under the pool lock at 1024 senders, 27 s when the engine
// was busy, with the builder and every admitter waiting.
func (a *accountCache) get(addr common.Address) (types.StateAccount, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if acc, ok := a.entries[addr]; ok {
		return acc, nil
	}
	raw, err := a.eng.accountState([]common.Address{addr}, ids.Empty)
	if err != nil {
		a.errs.Add(1)
		a.warn("validator: account read failed, the pool sees an empty account", err)
		return types.StateAccount{}, err
	}
	acc := decodeAccount(raw)
	a.entries[addr] = acc
	return acc, nil
}

func decodeAccount(b []byte) types.StateAccount {
	var nonce uint64
	for i := 7; i >= 0; i-- {
		nonce = nonce<<8 | uint64(b[i])
	}
	return types.StateAccount{Nonce: nonce, Balance: new(uint256.Int).SetBytes32(b[8:40]), Root: types.EmptyRootHash, CodeHash: types.EmptyCodeHash[:]}
}

// accountTrie is the read-only state.Trie the pool's StateDB sits on (an
// accepted head's root; the reads answer at the accepted head, see get).
type accountTrie struct {
	cache *accountCache
	root  common.Hash
}

func (t *accountTrie) GetAccount(addr common.Address) (*types.StateAccount, error) {
	acc, err := t.cache.get(addr)
	if err != nil {
		return nil, err
	}
	if acc.Nonce == 0 && acc.Balance.IsZero() {
		return nil, nil // absent account: the StateDB treats nil as non-existent
	}
	return &acc, nil
}

func (t *accountTrie) GetKey([]byte) []byte                              { return nil }
func (t *accountTrie) GetStorage(common.Address, []byte) ([]byte, error) { return nil, errUnsupported }
func (t *accountTrie) UpdateAccount(common.Address, *types.StateAccount) error {
	return errUnsupported
}
func (t *accountTrie) UpdateStorage(common.Address, []byte, []byte) error { return errUnsupported }
func (t *accountTrie) DeleteAccount(common.Address) error                 { return errUnsupported }
func (t *accountTrie) DeleteStorage(common.Address, []byte) error         { return errUnsupported }
func (t *accountTrie) UpdateContractCode(common.Address, common.Hash, []byte) error {
	return errUnsupported
}
func (t *accountTrie) Hash() common.Hash { return t.root }
func (t *accountTrie) Commit(bool) (common.Hash, *trienode.NodeSet, error) {
	return common.Hash{}, nil, errUnsupported
}
func (t *accountTrie) NodeIterator([]byte) (trie.NodeIterator, error) { return nil, errUnsupported }
func (t *accountTrie) Prove([]byte, ethdb.KeyValueWriter) error       { return errUnsupported }

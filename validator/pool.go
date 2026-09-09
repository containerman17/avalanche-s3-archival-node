package validator

import (
	"fmt"
	"math/big"
	"sync"

	"github.com/ava-labs/avalanchego/graft/subnet-evm/commontype"
	sevmcore "github.com/ava-labs/avalanchego/graft/subnet-evm/core"
	sevmparams "github.com/ava-labs/avalanchego/graft/subnet-evm/params"
	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/state"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/ethdb"
	"github.com/ava-labs/libevm/event"
	"github.com/ava-labs/libevm/params"
	"github.com/ava-labs/libevm/rlp"
	"github.com/ava-labs/libevm/trie"
	"github.com/ava-labs/libevm/trie/trienode"
	"github.com/ava-labs/libevm/triedb"
	"github.com/holiman/uint256"
)

// poolChain is the txpool's BlockChain over the engine: the head header is
// what Accept last decoded, StateAt hands out a state.StateDB whose only
// backing "trie" answers GetAccount from the engine (nonce + balance), and
// chain-head events come from Accept.
type poolChain struct {
	eng    *engine
	config *params.ChainConfig
	cacher *sevmcore.TxSenderCacher
	feed   event.Feed

	mu     sync.RWMutex
	head   *types.Header
	headID ids.ID
	recent map[common.Hash]ids.ID // root -> block id of the last few heads

	acct *accountCache
}

func newPoolChain(eng *engine, config *params.ChainConfig, head *types.Header, headID ids.ID) *poolChain {
	c := &poolChain{eng: eng, config: config, cacher: sevmcore.NewTxSenderCacher(4), recent: map[common.Hash]ids.ID{}}
	c.acct = &accountCache{eng: eng, entries: map[common.Address]types.StateAccount{}}
	c.setHead(head, headID)
	return c
}

// setHead records the accepted head and tells the pool. One crossing: every
// cached account is re-read at the new head so the pool's reset (which reads
// every pending sender) never crosses.
func (c *poolChain) setHead(h *types.Header, id ids.ID) {
	c.mu.Lock()
	c.head, c.headID = h, id
	c.recent[h.Root] = id
	if len(c.recent) > 16 {
		for r, rid := range c.recent {
			if rid != id {
				delete(c.recent, r)
				break
			}
		}
	}
	c.mu.Unlock()
	c.acct.refresh(h.Root)
	c.feed.Send(sevmcore.ChainHeadEvent{Block: types.NewBlockWithHeader(h)})
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

func (c *poolChain) StateAt(root common.Hash) (*state.StateDB, error) {
	return state.New(root, (*stateDB)(c), nil)
}

func (c *poolChain) SenderCacher() *sevmcore.TxSenderCacher { return c.cacher }

// GetFeeConfigAt: the genesis fee config. ponytail: a FeeManager precompile
// changing it at runtime is not read (add an engine call when a chain has one).
func (c *poolChain) GetFeeConfigAt(parent *types.Header) (commontype.FeeConfig, *big.Int, error) {
	return sevmparams.GetExtra(c.config).FeeConfig, common.Big0, nil
}

func (c *poolChain) SubscribeChainHeadEvent(ch chan<- sevmcore.ChainHeadEvent) event.Subscription {
	return c.feed.Subscribe(ch)
}

// stateDB is the state.Database whose tries are engine readers.
type stateDB poolChain

func (d *stateDB) OpenTrie(root common.Hash) (state.Trie, error) {
	c := (*poolChain)(d)
	c.mu.RLock()
	id, ok := c.recent[root]
	c.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("validator: no state for root %s", root)
	}
	if id == c.headID {
		id = ids.Empty // the accepted head, whatever it is by the time the read happens
	}
	return &accountTrie{cache: c.acct, root: root, block: id}, nil
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

// accountCache: nonce + balance per address, valid for one state root.
// Misses cross once per address; refresh re-reads every entry in one
// crossing when the head moves.
type accountCache struct {
	eng     *engine
	mu      sync.Mutex
	root    common.Hash
	entries map[common.Address]types.StateAccount
}

const maxCachedAccounts = 8192

func (a *accountCache) refresh(root common.Hash) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.root = root
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
		a.entries = map[common.Address]types.StateAccount{}
		return
	}
	for i, addr := range addrs {
		a.entries[addr] = decodeAccount(raw[i*40 : i*40+40])
	}
}

func (a *accountCache) get(addr common.Address, root common.Hash, block ids.ID) (types.StateAccount, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if root == a.root {
		if acc, ok := a.entries[addr]; ok {
			return acc, nil
		}
	}
	raw, err := a.eng.accountState([]common.Address{addr}, block)
	if err != nil {
		return types.StateAccount{}, err
	}
	acc := decodeAccount(raw)
	if root == a.root {
		a.entries[addr] = acc
	}
	return acc, nil
}

func decodeAccount(b []byte) types.StateAccount {
	var nonce uint64
	for i := 7; i >= 0; i-- {
		nonce = nonce<<8 | uint64(b[i])
	}
	return types.StateAccount{Nonce: nonce, Balance: new(uint256.Int).SetBytes32(b[8:40]), Root: types.EmptyRootHash, CodeHash: types.EmptyCodeHash[:]}
}

// accountTrie is the read-only state.Trie the pool's StateDB sits on.
type accountTrie struct {
	cache *accountCache
	root  common.Hash
	block ids.ID
}

func (t *accountTrie) GetAccount(addr common.Address) (*types.StateAccount, error) {
	acc, err := t.cache.get(addr, t.root, t.block)
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

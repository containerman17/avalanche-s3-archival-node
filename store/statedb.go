package store

import (
	"errors"
	"fmt"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/state"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/crypto"
	"github.com/ava-labs/libevm/ethdb"
	"github.com/ava-labs/libevm/rlp"
	"github.com/ava-labs/libevm/trie"
	"github.com/ava-labs/libevm/trie/trienode"
	"github.com/ava-labs/libevm/triedb"
	"github.com/holiman/uint256"
)

// SentinelStorageRoot is the storage root every historical account carries.
// The real per-account root is unknowable by design (no per-block tries);
// this is non-empty so geth never short-circuits storage reads on
// EmptyRootHash, and it is deliberately not a root anything downstream can
// resolve.
var SentinelStorageRoot = crypto.Keccak256Hash([]byte("epochdb: sentinel storage root"))

var errOverlayReadOnly = errors.New("epochdb overlay: read-only historical view")

// txCeiling converts a block height into the TxNum ceiling of a read of the
// state at the END of that block: the block's BOUNDARY SLOT, which is its last
// slot and is where anything it wrote outside a transaction lives. One formula
// for every block, with or without transactions, and that is the whole point of
// the boundary slot: the old "first-1 when the block has none" excluded exactly
// the rows such a block had just written.
//
// IT IS CACHED IN ONE SLOT, and needs no invalidation: a block's tx range is
// immutable once the block has landed, so an answer for a height stays true
// forever. Every account and slot read of one call resolves the same height, so
// a single slot turns the whole tip workload into one atomic load, which is
// what keeps this off the descent (and out of the memtable's lock) entirely.
func (d *DB) txCeiling(height uint64) (at uint64, err error) {
	if c := d.ceil.Load(); c != nil && c.height == height {
		return c.at, nil
	}
	at, ok, err := d.TxNumAtEndOf(height)
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, fmt.Errorf("store: block %d is not stored, so it has no state to read", height)
	}
	d.ceil.Store(&txCeil{height: height, at: at})
	return at, nil
}

// txCeil is one memoized txCeiling answer.
type txCeil struct {
	height uint64
	at     uint64
}

// stateRow is the state-family descent with THE FLAT LATEST-STATE CACHE
// (flat.go) in front of it. AT THE HEAD ONLY: a read of any other height goes
// straight down, and a fill that raced a landing block is refused rather than
// stored. Everything the cache can get wrong is a miss.
func (d *DB) stateRow(prefix []byte, height, at uint64) ([]byte, bool, error) {
	if val, found, hit := d.flat.get(prefix, height); hit {
		return val, found, nil
	}
	val, found, err := d.latest(SecState, prefix, at)
	if err != nil {
		return nil, false, err
	}
	d.flat.fill(prefix, val, found, height)
	return val, found, nil
}

// Account returns the account state of addr as of the end of block height.
// nil, nil = account does not exist there. The returned Root is always
// SentinelStorageRoot. genesis is exec.Genesis.TrieAlloc: what the genesis
// MATERIALISED, which on subnet-evm is more than its `alloc` (a precompile
// enabled at genesis has an account and storage there and is in no alloc).
// Handing the alloc literal here answers a read of such an address with "does
// not exist".
func (d *DB) Account(genesis types.GenesisAlloc, addr common.Address, height uint64) (*types.StateAccount, error) {
	at, err := d.txCeiling(height)
	if err != nil {
		return nil, err
	}
	val, ok, err := d.stateRow(AccountPrefix(addr[:]), height, at)
	if err != nil {
		return nil, err
	}
	if ok {
		return decodeAccount(val, addr) // empty value = explicit delete, account gone
	}
	// Never written at or below here: the genesis trie-state floor.
	ga, ok := genesis[addr]
	if !ok {
		return nil, nil
	}
	acc := &types.StateAccount{
		Nonce:    ga.Nonce,
		Balance:  new(uint256.Int),
		Root:     SentinelStorageRoot,
		CodeHash: types.EmptyCodeHash.Bytes(),
	}
	if ga.Balance != nil {
		acc.Balance = uint256.MustFromBig(ga.Balance)
	}
	if len(ga.Code) > 0 {
		acc.CodeHash = crypto.Keccak256(ga.Code)
	}
	return acc, nil
}

// Storage returns the trie-encoded (RLP of trimmed bytes) value of
// (addr, slot) as of the end of block height; nil = zero. A stored empty value
// is a cleared slot, which the EVM defines as zero.
func (d *DB) Storage(genesis types.GenesisAlloc, addr common.Address, slot []byte, height uint64) ([]byte, error) {
	at, err := d.txCeiling(height)
	if err != nil {
		return nil, err
	}
	val, ok, err := d.stateRow(SlotPrefix(addr[:], slot), height, at)
	if err != nil {
		return nil, err
	}
	if ok {
		return val, nil
	}
	// Never written at or below here: the genesis trie state.
	ga, ok := genesis[addr]
	if !ok || ga.Storage == nil {
		return nil, nil
	}
	var sl common.Hash
	copy(sl[:], slot)
	v, ok := ga.Storage[sl]
	if !ok || v == (common.Hash{}) {
		return nil, nil
	}
	return rlp.EncodeToBytes(common.TrimLeftZeroes(v[:]))
}

// CodeOf returns addr's contract code as of the end of block height (nil for
// EOAs and nonexistent accounts).
func (d *DB) CodeOf(genesis types.GenesisAlloc, addr common.Address, height uint64) ([]byte, error) {
	acc, err := d.Account(genesis, addr, height)
	if err != nil || acc == nil {
		return nil, err
	}
	return d.CodeByHash(genesis, common.BytesToHash(acc.CodeHash))
}

// CodeByHash serves contract code from the descent (window, then runs
// newest-to-oldest), then the genesis trie state. Never any frontier.
// EmptyCodeHash resolves to nil, and so does a hash nothing carries.
func (d *DB) CodeByHash(genesis types.GenesisAlloc, h common.Hash) ([]byte, error) {
	if h == types.EmptyCodeHash || h == (common.Hash{}) {
		return nil, nil
	}
	blob, ok, err := d.Code(h[:])
	if err != nil {
		return nil, err
	}
	if ok {
		return blob, nil
	}
	// Genesis code was never deployed by a block, so no run carries it; the
	// genesis trie state is derived from the descriptor at open. That includes a
	// subnet-evm precompile's 0x01 body, which is in no alloc.
	for _, ga := range genesis {
		if len(ga.Code) > 0 && crypto.Keccak256Hash(ga.Code) == h {
			return ga.Code, nil
		}
	}
	return nil, nil
}

// decodeAccount turns a stored account post-image into a StateAccount.
// An empty value is an explicit delete: the account does not exist.
func decodeAccount(val []byte, addr common.Address) (*types.StateAccount, error) {
	if len(val) == 0 {
		return nil, nil
	}
	var acc types.StateAccount
	if err := rlp.DecodeBytes(val, &acc); err != nil {
		return nil, fmt.Errorf("decode account %x: %w", addr, err)
	}
	acc.Root = SentinelStorageRoot
	return &acc, nil
}

// StateAt returns a read-only libevm state.Database serving the historical
// read contract as of the end of block height.
func (d *DB) StateAt(genesis types.GenesisAlloc, height uint64) state.Database {
	return &Overlay{db: d, genesis: genesis, target: height}
}

// StateLatest returns the read surface for the current head, re-read at every
// access so a long-lived view follows the executor.
func (d *DB) StateLatest(genesis types.GenesisAlloc) state.Database {
	return &Overlay{db: d, genesis: genesis, latest: true}
}

// Overlay is the read-only historical state.Database at a fixed target
// block. Only value reads work; every trie-structure or write method fails
// loudly.
//
// latest replaces the fixed target with the store's head at read time.
type Overlay struct {
	db      *DB
	genesis types.GenesisAlloc
	target  uint64
	latest  bool
}

// height is the block this view answers for: the fixed target, or the head in
// latest mode.
func (o *Overlay) height() uint64 {
	if o.latest {
		h, _ := o.db.Head()
		return h
	}
	return o.target
}

func (o *Overlay) OpenTrie(common.Hash) (state.Trie, error) {
	// The root argument is ignored: the overlay is pinned to its target
	// block, there is no Merkle trie to open.
	return &overlayTrie{o: o}, nil
}

func (o *Overlay) OpenStorageTrie(_ common.Hash, _ common.Address, _ common.Hash, _ state.Trie) (state.Trie, error) {
	return &overlayTrie{o: o}, nil
}

func (o *Overlay) CopyTrie(t state.Trie) state.Trie {
	if w, ok := t.(*overlayTrie); ok {
		return &overlayTrie{o: w.o}
	}
	panic(fmt.Sprintf("epochdb overlay: CopyTrie of foreign trie %T", t))
}

func (o *Overlay) ContractCode(_ common.Address, codeHash common.Hash) ([]byte, error) {
	return o.db.CodeByHash(o.genesis, codeHash)
}

func (o *Overlay) ContractCodeSize(addr common.Address, codeHash common.Hash) (int, error) {
	blob, err := o.ContractCode(addr, codeHash)
	return len(blob), err
}

// DiskDB and TrieDB are not on any served read path (eth_call and the simple
// getters never touch them); a nil return fails loudly at the caller if that
// ever changes.
func (o *Overlay) DiskDB() ethdb.KeyValueStore { return nil }

func (o *Overlay) TrieDB() *triedb.Database { return nil }

// overlayTrie implements the value-read subset of state.Trie.
type overlayTrie struct {
	o *Overlay
}

func (t *overlayTrie) GetKey(k []byte) []byte { return k }

func (t *overlayTrie) GetAccount(addr common.Address) (*types.StateAccount, error) {
	return t.o.db.Account(t.o.genesis, addr, t.o.height())
}

func (t *overlayTrie) GetStorage(addr common.Address, key []byte) ([]byte, error) {
	return t.o.db.Storage(t.o.genesis, addr, key, t.o.height())
}

// Hash returns header(target).Root: the one true root for this height.
func (t *overlayTrie) Hash() common.Hash {
	n := t.o.height()
	raw, ok, err := t.o.db.HeaderRLP(n)
	if err != nil {
		panic(fmt.Sprintf("epochdb overlay: read header %d: %v", n, err))
	}
	if !ok {
		panic(fmt.Sprintf("epochdb overlay: header %d is in no run and not in the window, so this height has no root to answer with", n))
	}
	var h types.Header
	if err := rlp.DecodeBytes(raw, &h); err != nil {
		panic(fmt.Sprintf("epochdb overlay: decode header %d: %v", n, err))
	}
	return h.Root
}

// --- fail-loud: writes and trie-structure methods ---------------------------

func (t *overlayTrie) UpdateAccount(common.Address, *types.StateAccount) error {
	return errOverlayReadOnly
}
func (t *overlayTrie) UpdateStorage(common.Address, []byte, []byte) error {
	return errOverlayReadOnly
}
func (t *overlayTrie) DeleteAccount(common.Address) error         { return errOverlayReadOnly }
func (t *overlayTrie) DeleteStorage(common.Address, []byte) error { return errOverlayReadOnly }
func (t *overlayTrie) UpdateContractCode(common.Address, common.Hash, []byte) error {
	return errOverlayReadOnly
}
func (t *overlayTrie) Commit(bool) (common.Hash, *trienode.NodeSet, error) {
	return common.Hash{}, nil, errOverlayReadOnly
}
func (t *overlayTrie) NodeIterator([]byte) (trie.NodeIterator, error) {
	return nil, errors.New("epochdb overlay: NodeIterator unsupported over history")
}
func (t *overlayTrie) Prove([]byte, ethdb.KeyValueWriter) error {
	return errors.New("epochdb overlay: Prove unsupported over history (no Merkle nodes)")
}

// Compile-time proof that the overlay still satisfies the libevm read surface.
var (
	_ state.Database = (*Overlay)(nil)
	_ state.Trie     = (*overlayTrie)(nil)
)

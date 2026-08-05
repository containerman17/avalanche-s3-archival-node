// Package sdk is epochdb's read-only Go SDK: open the data directory of a
// live `epochdb serve` process from ANOTHER process and read it at that
// writer's head.
//
// HOW IT STAYS LIVE (user ruling 2026-07-30). serve is the sole writer of its
// dir and everything it writes is append-only, so a reader does not need an
// RPC hop, an embedded node, or the cook cadence: it TAILS the files. Open
// takes the same read-only openers cook and bench already used
// (state.OpenReadOnly, fetch.OpenReader), builds the same uncooked-tail
// overlay serve builds at startup (state.History.EnableTail, backfilled from
// the raw writelog), and then a single goroutine keeps three things chasing
// the writer:
//
//	Store.RescanRO      consumes the index records the writer appended since
//	                    the last poll (write frames, headers, logs, receipts,
//	                    code blobs).
//	History.AdvanceTail folds the new write frames into the overlay, exactly
//	                    what Store.AppendWrites does on the writer side.
//	History.Refresh     re-opens the sorted buckets the writer's cook
//	                    produced and republishes the cooked watermark, after
//	                    which the overlay is pruned (same ordering rule as
//	                    serve's cook loop).
//
// Reads then go through the ONE read path: the overlay first, then the
// historical descent. There is no second implementation of anything here; the
// package is wiring plus that loop.
//
// The handle is goroutine-safe and reads only: it never appends, truncates or
// fsyncs any of the writer's files. It does need FILESYSTEM write permission
// on the dir, for two reasons that both predate it: the shared bucket-log read
// path opens data files O_RDWR (the same handle cache the writer appends
// through, nothing is written through it here), and state.OpenReadOnly opens
// the artifact store, which mkdirs the spool. Nothing here ever uploads or
// releases an artifact: that is serve's syncLoop, and only with credentials.
package sdk

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ava-labs/libevm/common"
	ethstate "github.com/ava-labs/libevm/core/state"
	"github.com/ava-labs/libevm/core/types"

	"github.com/containerman17/epochdb/chain"
	"github.com/containerman17/epochdb/exec"
	"github.com/containerman17/epochdb/fetch"
	"github.com/containerman17/epochdb/rpc"
	"github.com/containerman17/epochdb/state"
)

// Latest reads at the writer's current executed head.
const Latest = ^uint64(0)

// pollInterval is how often the tailing goroutine looks for new appends. It
// is a handful of stats and preads, so it is cheap enough to keep tip latency
// at the same order as the writer's own publish.
const pollInterval = 25 * time.Millisecond

// refreshInterval is how often the cooked watermark is re-read. serve cooks
// every 60s by default and the overlay covers everything in between, so this
// only decides how quickly the descent takes over from the overlay.
const refreshInterval = 5 * time.Second

// DB is a read-only handle on an epochdb data directory.
type DB struct {
	store *state.Store
	hist  *state.History
	reads *rpc.Server
	fr    *fetch.Reader
	head  *liveHead
	txidx *txIndex

	stop context.CancelFunc
	done chan struct{}
	ferr atomic.Pointer[error]
}

// Open opens dir read-only and starts following the writer's appends.
// networkID selects the chain config and genesis alloc (0 = Fuji), exactly as
// for serve; it must match the network the dir holds.
func Open(dir string, networkID uint32) (*DB, error) {
	c, err := chain.CChain(networkID)
	if err != nil {
		return nil, err
	}
	g, err := exec.ChainGenesis(c)
	if err != nil {
		return nil, fmt.Errorf("genesis: %w", err)
	}
	store, err := state.OpenReadOnly(dir)
	if err != nil {
		return nil, err
	}
	hist, err := state.OpenHistory(dir, store, g.TrieAlloc)
	if err != nil {
		store.Close()
		return nil, err
	}
	fr, err := fetch.OpenReader(dir)
	if err != nil {
		hist.Close()
		store.Close()
		return nil, err
	}

	d := &DB{store: store, hist: hist, fr: fr, head: &liveHead{}, done: make(chan struct{})}

	// Same startup shape as serve, minus the cook (only the writer may cook):
	// the descent reaches the cooked watermark, and the residue above it, which
	// is executed but indexed nowhere, comes out of the raw writelog into the
	// overlay.
	head := hist.StateHead()
	if n, ok := store.HeadersMax(); ok && n > head {
		head = n
	}
	if _, _, _, err := hist.EnableTail(head); err != nil {
		d.closeFiles()
		return nil, err
	}
	d.head.n.Store(head)
	hist.SetHead(head)

	d.reads = rpc.NewServer(hist, rpc.HistoryChainContext(hist), g.Config)
	d.reads.EnableLive(d.head)
	d.txidx = &txIndex{}
	d.txidx.reopen(dir, hist.Epochs())
	d.reads.EnableTxAPIs(d.txidx, rpc.SealedBlocks{Epochs: hist.Epochs(), Blocks: fr}, exec.ParseEthBlock)

	// No block-hash map is built here: block hashes are in the fp48 index
	// (epoch format v6 sealed, cooked staging raw), and only the heights the
	// writer has appended since its last cook need the bounded live-tail map
	// that advance() feeds.

	ctx, cancel := context.WithCancel(context.Background())
	d.stop = cancel
	go d.follow(ctx, dir)
	return d, nil
}

// Close stops following and releases every file handle. It returns Err(), so
// a follower that died mid-run is reported at least once.
func (d *DB) Close() error {
	d.stop()
	<-d.done
	d.closeFiles()
	return d.Err()
}

func (d *DB) closeFiles() {
	if d.fr != nil {
		d.fr.Close()
	}
	d.hist.Close()
	d.store.Close()
}

// Err returns the error that stopped the tailing loop, if any. A stalled
// follower would otherwise serve silently stale data, so every read below
// returns this error too.
func (d *DB) Err() error {
	if p := d.ferr.Load(); p != nil {
		return *p
	}
	return nil
}

// Head is the writer's current executed head: the highest block this handle
// answers for. It returns Err() like every other read: a dead tailing loop
// freezes this number, and a caller polling it would otherwise see a healthy
// chain that simply stopped producing blocks.
func (d *DB) Head() (uint64, error) { return d.hist.Head(), d.Err() }

// --- the tailing loop -------------------------------------------------------

// follow is the one goroutine that chases the writer. Ordering inside a tick
// is what makes a read at the new head correct: the frames are folded into the
// overlay and the block hash is indexed BEFORE the head is published, so a
// reader that sees height N can already read N's writes.
func (d *DB) follow(ctx context.Context, dir string) {
	defer close(d.done)
	tick := time.NewTicker(pollInterval)
	defer tick.Stop()
	lastRefresh := time.Now()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		if err := d.advance(); err != nil {
			d.ferr.Store(&err)
			return
		}
		if time.Since(lastRefresh) < refreshInterval {
			continue
		}
		lastRefresh = time.Now()
		if err := d.refresh(dir); err != nil {
			d.ferr.Store(&err)
			return
		}
	}
}

// advance consumes the writer's new appends and publishes the new head.
func (d *DB) advance() error {
	if err := d.store.RescanRO(); err != nil {
		return err
	}
	head, ok := d.store.HeadersMax()
	if !ok || head <= d.hist.Head() {
		return nil
	}
	if _, err := d.hist.AdvanceTail(head); err != nil {
		return err
	}
	for n := d.hist.Head() + 1; n <= head; n++ {
		if err := d.addHash(n); err != nil {
			return err
		}
	}
	// The executed label goes up FIRST: a read at Latest resolves the serving
	// head and is then refused above the executed one, so publishing them the
	// other way round would make a read in that window say "not executed yet".
	d.head.n.Store(head)
	d.hist.SetHead(head)
	return nil
}

// refresh takes over the blocks the writer's cook has indexed: new sorted
// buckets, then the overlay prune (strictly after Refresh: see the race
// argument on state's tail.prune) and a fresh raw tx index.
func (d *DB) refresh(dir string) error {
	before := d.hist.StateHead()
	if err := d.hist.Refresh(); err != nil {
		return err
	}
	if d.hist.StateHead() == before {
		return nil
	}
	d.hist.PruneTail()
	d.txidx.reopen(dir, d.hist.Epochs())
	return nil
}

// addHash records block n in the bounded live-tail map. It covers exactly the
// window between the writer appending a block and its next cook folding that
// block's hash into the raw tx index; everything below resolves through the
// index itself.
// A header this handle cannot read is a block whose hash never enters the map,
// so BlockByHash would answer "unknown hash" for a block the writer holds:
// stop the loop instead, which makes every read return Err().
func (d *DB) addHash(n uint64) error {
	raw, ok, err := d.hist.HeaderRLP(n)
	if err != nil {
		return fmt.Errorf("read header %d: %w", n, err)
	}
	if !ok {
		return fmt.Errorf("header %d missing below the writer's header head", n)
	}
	d.reads.AddBlockHash(state.BlockHashFromHeaderRLP(raw), n)
	return nil
}

// liveHead reports the height labels to the read paths. This handle only ever
// sees EXECUTED blocks (it tails the executor's own output), so accepted and
// settled coincide with it; the SDK exposes no pending/safe/finalized surface.
type liveHead struct{ n atomic.Uint64 }

func (l *liveHead) LiveHead() uint64      { return l.n.Load() }
func (l *liveHead) AcceptedHead() uint64  { return l.n.Load() }
func (l *liveHead) SettledHeight() uint64 { return l.n.Load() }
func (l *liveHead) SyncTarget() uint64    { return l.n.Load() }

// txIndex swaps the raw tx index as the writer's cook rebuilds it. Heap-only,
// so a replaced one is just garbage.
type txIndex struct {
	mu   sync.RWMutex
	cur  state.CombinedTxIndex
	oerr error // why there is no index; nil once one is loaded
}

func (t *txIndex) WalkCandidates(hash common.Hash, fn func(blk uint64) (bool, error)) error {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.cur.Epochs == nil {
		// Zero candidates would make TransactionByHash and BlockByHash answer
		// "unknown hash" for EVERY hash on the chain, forever: only a cook
		// watermark advance calls reopen again. The handle says why instead.
		if t.oerr != nil {
			return fmt.Errorf("tx index unavailable: %w", t.oerr)
		}
		return errors.New("tx index unavailable: never loaded")
	}
	return t.cur.WalkCandidates(hash, fn)
}

func (t *txIndex) reopen(dir string, epochs *state.EpochSet) {
	raw, err := state.OpenTxIndex(dir)
	t.mu.Lock()
	defer t.mu.Unlock()
	if err != nil {
		if t.cur.Epochs == nil { // no previous index to keep serving from
			t.oerr = err
		}
		return
	}
	t.cur, t.oerr = state.CombinedTxIndex{Raw: raw, Epochs: epochs}, nil
}

// --- state reads ------------------------------------------------------------

// state opens the read surface at height, mapping Latest to the current head.
func (d *DB) state(height uint64) (*ethstate.StateDB, error) {
	if err := d.Err(); err != nil {
		return nil, err
	}
	if height == Latest {
		height = d.hist.Head()
	}
	return d.reads.StateAt(height)
}

// Account returns addr's account at height; nil means it does not exist.
// Root is state.SentinelStorageRoot: per-account storage roots are unknowable
// here by design (no per-block tries).
func (d *DB) Account(addr common.Address, height uint64) (*types.StateAccount, error) {
	st, err := d.state(height)
	if err != nil {
		return nil, err
	}
	if !st.Exist(addr) {
		return nil, st.Error()
	}
	acc := &types.StateAccount{
		Nonce:    st.GetNonce(addr),
		Balance:  st.GetBalance(addr),
		Root:     state.SentinelStorageRoot,
		CodeHash: st.GetCodeHash(addr).Bytes(),
	}
	return acc, st.Error()
}

// Balance returns addr's balance in wei at height.
func (d *DB) Balance(addr common.Address, height uint64) (*big.Int, error) {
	st, err := d.state(height)
	if err != nil {
		return nil, err
	}
	return st.GetBalance(addr).ToBig(), st.Error()
}

// Nonce returns addr's transaction count at height.
func (d *DB) Nonce(addr common.Address, height uint64) (uint64, error) {
	st, err := d.state(height)
	if err != nil {
		return 0, err
	}
	return st.GetNonce(addr), st.Error()
}

// Code returns addr's contract code at height (nil for an EOA).
func (d *DB) Code(addr common.Address, height uint64) ([]byte, error) {
	st, err := d.state(height)
	if err != nil {
		return nil, err
	}
	return st.GetCode(addr), st.Error()
}

// StorageAt returns the value of addr's storage slot at height.
func (d *DB) StorageAt(addr common.Address, slot common.Hash, height uint64) (common.Hash, error) {
	st, err := d.state(height)
	if err != nil {
		return common.Hash{}, err
	}
	return st.GetState(addr, slot), st.Error()
}

// --- block, tx, receipt and log reads ---------------------------------------

// BlockByNumber returns the block at height (Latest = the current head).
func (d *DB) BlockByNumber(height uint64) (*types.Block, error) {
	if err := d.Err(); err != nil {
		return nil, err
	}
	if height == Latest {
		height = d.hist.Head()
	}
	return d.reads.BlockAt(height)
}

// BlockByHash returns the block with this eth block hash. ok=false is an
// unknown hash.
func (d *DB) BlockByHash(hash common.Hash) (*types.Block, bool, error) {
	if err := d.Err(); err != nil {
		return nil, false, err
	}
	n, ok, err := d.reads.HeightByHash(hash)
	if err != nil || !ok {
		return nil, false, err
	}
	blk, err := d.reads.BlockAt(n)
	return blk, err == nil, err
}

// Tx is a transaction together with where it was mined.
type Tx struct {
	Tx          *types.Transaction
	BlockNumber uint64
	BlockHash   common.Hash
	Index       int
}

// TransactionByHash resolves a tx hash through the fingerprint index and the
// block bodies. ok=false is an unknown hash.
func (d *DB) TransactionByHash(hash common.Hash) (*Tx, bool, error) {
	blk, i, found, err := d.findTx(hash)
	if err != nil || !found {
		return nil, false, err
	}
	return &Tx{
		Tx:          blk.Transactions()[i],
		BlockNumber: blk.NumberU64(),
		BlockHash:   blk.Hash(),
		Index:       i,
	}, true, nil
}

// ReceiptByHash returns the receipt of a transaction, rebuilt from the stored
// receipt and log sections (sealed epoch or live tail capture, one encoding,
// no re-execution). ok=false is an unknown hash.
func (d *DB) ReceiptByHash(hash common.Hash) (*types.Receipt, bool, error) {
	blk, i, found, err := d.findTx(hash)
	if err != nil || !found {
		return nil, false, err
	}
	receipts, err := d.reads.BlockReceipts(blk)
	if err != nil {
		return nil, false, err
	}
	r := receipts[i]
	r.BlockHash, r.BlockNumber, r.TransactionIndex = blk.Hash(), blk.Number(), uint(i)
	return r, true, nil
}

// BlockReceipts returns every receipt of the block at height.
func (d *DB) BlockReceipts(height uint64) (types.Receipts, error) {
	blk, err := d.BlockByNumber(height)
	if err != nil {
		return nil, err
	}
	receipts, err := d.reads.BlockReceipts(blk)
	if err != nil {
		return nil, err
	}
	hash, num := blk.Hash(), blk.Number()
	for i, r := range receipts {
		r.BlockHash, r.BlockNumber, r.TransactionIndex = hash, num, uint(i)
	}
	return receipts, nil
}

// Logs runs a log query over the inclusive height range [from, to] with the
// standard eth_getLogs semantics (addresses OR'd, topic positions AND'd,
// values within a position OR'd, a nil position matching anything). The range
// is capped at rpc.GetLogsMaxRange blocks, as on the JSON path.
func (d *DB) Logs(from, to uint64, addrs []common.Address, topics [][]common.Hash) ([]*types.Log, error) {
	if err := d.Err(); err != nil {
		return nil, err
	}
	if to == Latest {
		to = d.hist.Head()
	}
	if to < from {
		return nil, fmt.Errorf("empty log range [%d, %d]", from, to)
	}
	if to-from >= rpc.GetLogsMaxRange {
		return nil, fmt.Errorf("log range [%d, %d] exceeds the %d block cap", from, to, rpc.GetLogsMaxRange)
	}
	return d.reads.GetLogs(from, to, addrs, topics)
}

func (d *DB) findTx(hash common.Hash) (*types.Block, int, bool, error) {
	if err := d.Err(); err != nil {
		return nil, 0, false, err
	}
	return d.reads.FindTx(hash)
}

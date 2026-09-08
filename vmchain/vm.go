// Package vmchain wraps the vmexec engine as an avalanchego block.ChainVM, so
// epochdb-vm runs as a plugin under a stock avalanchego or under
// cmd/epochdb-host. A follower VM: BuildBlock refuses, App messages are
// ignored, and every accepted block is executed and root-checked before
// Verify returns. Bytes cross the boundary once per block (ParseBlock); the
// executor's own goroutines do the work.
//
// vmexec is driven through its Run loop and BlockSource, unchanged: Verify
// hands the block's bytes to a push source and waits for the executor to
// publish that height. The executor decodes the bytes itself, so the tx
// objects the parse-time sender recovery warmed are NOT the ones it
// executes (see recover.go); a decoded-block source in vmexec fixes that.
package vmchain

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ava-labs/avalanchego/database"
	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/snow"
	"github.com/ava-labs/avalanchego/snow/consensus/snowman"
	"github.com/ava-labs/avalanchego/snow/engine/common"
	"github.com/ava-labs/avalanchego/snow/engine/snowman/block"
	"github.com/ava-labs/avalanchego/version"
	ethcommon "github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/types"

	"github.com/containerman17/avalanche-s3-archival-node/chain"
	"github.com/containerman17/avalanche-s3-archival-node/dist"
	"github.com/containerman17/avalanche-s3-archival-node/rpc"
	"github.com/containerman17/avalanche-s3-archival-node/store"
	"github.com/containerman17/avalanche-s3-archival-node/vmexec"
)

// Version is what avalanchego's version API reports for this VM.
var Version = fmt.Sprintf("epochdb-vm/0.1 [rpcchainvm=%d]", version.RPCChainVMProtocol)

var (
	_ block.ChainVM           = (*VM)(nil)
	_ block.BatchedChainVM    = (*VM)(nil)
	_ block.WithVerifyContext = (*Block)(nil)
)

// VM is the ChainVM. Zero value, then Initialize.
type VM struct {
	e    *vmexec.Executor
	db   *store.DB
	misc *store.MiscStore
	cas  *dist.Store
	g    *vmexec.Genesis
	srv  *rpc.Server
	src  *source
	rec  *recoverer

	// mu serialises Verify and guards headID/runErr; cond is broadcast by
	// the executor's OnBlock (a height was published) and by Run's exit.
	mu     sync.Mutex
	cond   *sync.Cond
	headID ids.ID // hash of the last published block; genesis before any
	runErr error

	cancel  context.CancelFunc
	runDone chan struct{}
	once    sync.Once
	normal  atomic.Bool // NormalOp: consensus is at the tip, the executor's budget follows
}

func (vm *VM) Initialize(_ context.Context, chainCtx *snow.Context, _ database.Database,
	genesisBytes, upgradeBytes, _ []byte, _ []*common.Fx, _ common.AppSender,
) error {
	if chainCtx.ChainDataDir == "" {
		return errors.New("vmchain: chain data dir is required")
	}
	c := &chain.Chain{
		GenesisJSON: genesisBytes, UpgradeJSON: upgradeBytes,
		NetworkID: chainCtx.NetworkID, SubnetID: chainCtx.SubnetID, BlockchainID: chainCtx.ChainID,
		VMKind: chain.SubnetEVM,
	}
	g, err := vmexec.ChainGenesis(c)
	if err != nil {
		return err
	}
	dir := chainCtx.ChainDataDir
	cas, err := dist.Open(dir)
	if err != nil {
		return fmt.Errorf("vmchain: open artifact store: %w", err)
	}
	if err := store.Join(cas, dir, c.Root()); err != nil {
		return fmt.Errorf("vmchain: join chain: %w", err)
	}
	db, err := store.Open(dir, cas, c.Root())
	if err != nil {
		return fmt.Errorf("vmchain: open storage v0: %w", err)
	}
	misc, err := store.OpenMisc(dir)
	if err != nil {
		db.Close()
		return fmt.Errorf("vmchain: open misc store: %w", err)
	}
	vm.g, vm.cas, vm.db, vm.misc = g, cas, db, misc
	vm.cond = sync.NewCond(&vm.mu)
	vm.headID = ids.ID(g.Hash)
	vm.src = newSource()
	e, err := vmexec.New(vmexec.Config{
		DataDir: dir, Blocks: vm.src, Store: db, CAS: cas, Misc: misc, Chain: c,
		OnBlock: func(uint64, ethcommon.Hash) { vm.mu.Lock(); vm.cond.Broadcast(); vm.mu.Unlock() },
		// Bootstrapping is catch-up (accepted head unknown); in NormalOp
		// the last published block is the accepted head, so the lag is 0.
		Budget: vmexec.Budget{Accepted: func() uint64 {
			if !vm.normal.Load() {
				return 0
			}
			return vm.e.LiveHead()
		}},
	})
	if err != nil {
		misc.Close()
		db.Close()
		return fmt.Errorf("vmchain: vmexec.New: %w", err)
	}
	vm.e = e
	vm.rec = newRecoverer(g.Config, e.LiveHead)
	vm.srv = rpc.NewServer(db, g.TrieAlloc, rpc.StoreChainContext(db), g.Config)
	vm.srv.EnableLive(vm)

	ctx, cancel := context.WithCancel(context.Background())
	vm.cancel = cancel
	vm.runDone = make(chan struct{})
	go func() {
		err := e.Run(ctx)
		if err == nil {
			err = errors.New("vmchain: executor exited")
		}
		vm.mu.Lock()
		vm.runErr = err
		vm.cond.Broadcast()
		vm.mu.Unlock()
		close(vm.runDone)
	}()
	log.Printf("vmchain: %s chainId=%s data=%s", chainCtx.ChainID, g.Config.ChainID, dir)
	return nil
}

func (vm *VM) SetState(_ context.Context, st snow.State) error {
	vm.normal.Store(st == snow.NormalOp)
	return nil
}

func (vm *VM) Shutdown(context.Context) error {
	if vm.e == nil {
		return nil
	}
	var err error
	vm.once.Do(func() {
		vm.cancel()
		vm.src.close()
		vm.rec.close()
		<-vm.runDone
		err = vm.e.Close()
		vm.misc.Close()
		if cerr := vm.db.Close(); err == nil {
			err = cerr
		}
		vm.cas.Close()
	})
	return err
}

func (vm *VM) Version(context.Context) (string, error) { return Version, nil }

func (vm *VM) HealthCheck(context.Context) (interface{}, error) {
	vm.mu.Lock()
	err := vm.runErr
	vm.mu.Unlock()
	return map[string]uint64{"height": vm.e.LiveHead()}, err
}

func (vm *VM) Connected(context.Context, ids.NodeID, *version.Application) error { return nil }
func (vm *VM) Disconnected(context.Context, ids.NodeID) error                    { return nil }
func (vm *VM) AppRequest(context.Context, ids.NodeID, uint32, time.Time, []byte) error {
	return nil
}
func (vm *VM) AppResponse(context.Context, ids.NodeID, uint32, []byte) error { return nil }
func (vm *VM) AppRequestFailed(context.Context, ids.NodeID, uint32, *common.AppError) error {
	return nil
}
func (vm *VM) AppGossip(context.Context, ids.NodeID, []byte) error { return nil }

func (vm *VM) CreateHandlers(context.Context) (map[string]http.Handler, error) {
	return map[string]http.Handler{"/rpc": vm.srv, "/ws": vm.srv}, nil
}
func (vm *VM) NewHTTPHandler(context.Context) (http.Handler, error) { return nil, nil }

// WaitForEvent: a follower never has pending txs; block until cancelled.
func (vm *VM) WaitForEvent(ctx context.Context) (common.Message, error) {
	<-ctx.Done()
	return 0, ctx.Err()
}

func (vm *VM) BuildBlock(context.Context) (snowman.Block, error) {
	return nil, errors.New("vmchain: a follower builds no blocks")
}
func (vm *VM) SetPreference(context.Context, ids.ID) error { return nil }

func (vm *VM) LastAccepted(ctx context.Context) (ids.ID, error) {
	return vm.GetBlockIDAtHeight(ctx, vm.e.LiveHead())
}

func (vm *VM) GetBlockIDAtHeight(_ context.Context, height uint64) (ids.ID, error) {
	if height == 0 {
		return ids.ID(vm.g.Hash), nil
	}
	if height > vm.e.LiveHead() {
		return ids.Empty, database.ErrNotFound
	}
	h, err := vm.srv.HeaderAt(height)
	if err != nil {
		return ids.Empty, err
	}
	return ids.ID(h.Hash()), nil
}

// GetBlock serves published blocks out of the store (blkh/ then hdr/ and tx/
// rows), and the genesis by its hash.
func (vm *VM) GetBlock(_ context.Context, id ids.ID) (snowman.Block, error) {
	if id == ids.ID(vm.g.Hash) {
		return vm.genesisBlock(), nil
	}
	n, ok, err := vm.db.HeightByHash(id[:])
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, database.ErrNotFound
	}
	blk, err := vm.srv.BlockAt(n)
	if err != nil {
		return nil, err
	}
	raw, err := vm.srv.RawBlock(n)
	if err != nil {
		return nil, err
	}
	return vm.wrap(raw, blk), nil
}

// ParseBlock takes the inner block bytes (what a plugin receives) or a whole
// container (what a host may pass): store.SplitContainer reads both. The
// decoded block is kept, and its senders are recovered ahead of Verify.
func (vm *VM) ParseBlock(_ context.Context, raw []byte) (snowman.Block, error) {
	_, blk, err := store.SplitContainer(raw)
	if err != nil {
		return nil, err
	}
	vm.rec.submit(blk)
	return vm.wrap(raw, blk), nil
}

func (vm *VM) BatchedParseBlock(_ context.Context, raws [][]byte) ([]snowman.Block, error) {
	out := make([]snowman.Block, len(raws))
	blks := make([]*types.Block, 0, len(raws))
	for i, raw := range raws {
		_, blk, err := store.SplitContainer(raw)
		if err != nil {
			return nil, fmt.Errorf("block %d of %d: %w", i, len(raws), err)
		}
		out[i] = vm.wrap(raw, blk)
		blks = append(blks, blk)
	}
	vm.rec.submit(blks...)
	return out, nil
}

func (vm *VM) GetAncestors(context.Context, ids.ID, int, int, time.Duration) ([][]byte, error) {
	return nil, block.ErrRemoteVMNotImplemented
}

// rpc.Live: no follower in a plugin, so every label is the executed head.
func (vm *VM) LiveHead() uint64      { return vm.e.LiveHead() }
func (vm *VM) AcceptedHead() uint64  { return vm.e.LiveHead() }
func (vm *VM) SettledHeight() uint64 { return vm.e.LiveHead() }
func (vm *VM) SyncTarget() uint64    { return vm.e.LiveHead() }

// Recovered is the recoverer's count of blocks whose senders it warmed.
func (vm *VM) Recovered() (blocks, dropped uint64) { return vm.rec.stats() }

func (vm *VM) wrap(raw []byte, blk *types.Block) *Block {
	return &Block{vm: vm, raw: raw, blk: blk, id: ids.ID(blk.Hash()), parent: ids.ID(blk.ParentHash()), height: blk.NumberU64(), time: blk.Time()}
}

func (vm *VM) genesisBlock() *Block {
	return &Block{vm: vm, id: ids.ID(vm.g.Hash), height: 0, time: vm.g.Timestamp}
}

// verify executes the block: hand its bytes to the executor and wait for the
// height to be published (root matched, rows written). A root mismatch is
// log.Fatalf inside vmexec, on purpose. Idempotent: a height at or below the
// executed head passes if its hash is the executed one.
func (vm *VM) verify(b *Block) error {
	if b.blk == nil {
		return nil
	}
	vm.mu.Lock()
	defer vm.mu.Unlock()
	live := vm.e.LiveHead()
	if b.height <= live {
		n, ok, err := vm.db.HeightByHash(b.id[:])
		if err != nil {
			return err
		}
		if !ok || n != b.height {
			return fmt.Errorf("vmchain: block %d %s is not the block executed at that height", b.height, b.id)
		}
		return nil
	}
	if vm.runErr != nil {
		return vm.runErr
	}
	if b.height != live+1 {
		return fmt.Errorf("vmchain: block %d verified out of order: executed head is %d", b.height, live)
	}
	if b.parent != vm.headID {
		return fmt.Errorf("vmchain: block %d parent %s is not the executed head %s", b.height, b.parent, vm.headID)
	}
	vm.src.put(b.height, b.raw)
	for vm.e.LiveHead() < b.height {
		if vm.runErr != nil {
			return vm.runErr
		}
		vm.cond.Wait()
	}
	vm.headID = b.id
	return nil
}

// Block is one parsed (or stored) block. raw is exactly what ParseBlock got.
type Block struct {
	vm     *VM
	raw    []byte
	blk    *types.Block // nil for the genesis
	id     ids.ID
	parent ids.ID
	height uint64
	time   uint64
}

func (b *Block) ID() ids.ID           { return b.id }
func (b *Block) Parent() ids.ID       { return b.parent }
func (b *Block) Height() uint64       { return b.height }
func (b *Block) Timestamp() time.Time { return time.Unix(int64(b.time), 0) }
func (b *Block) Bytes() []byte        { return b.raw }
func (b *Block) Verify(context.Context) error {
	return b.vm.verify(b)
}

// ShouldVerifyWithContext: the P-chain height is a warp input this VM does
// not check (it executes what the validators accepted), so plain Verify.
func (b *Block) ShouldVerifyWithContext(context.Context) (bool, error) { return false, nil }
func (b *Block) VerifyWithContext(context.Context, *block.Context) error {
	return b.vm.verify(b)
}

// Accept: the block was published when Verify returned; nothing to do.
func (b *Block) Accept(context.Context) error {
	if b.blk != nil && b.height > b.vm.e.LiveHead() {
		return fmt.Errorf("vmchain: block %d accepted before it was verified", b.height)
	}
	return nil
}
func (b *Block) Reject(context.Context) error { return nil }

// source is the executor's BlockSource: Verify puts a container, the
// executor's prefetcher blocks in GetByHeight until it is there.
type source struct {
	mu     sync.Mutex
	cond   *sync.Cond
	raw    map[uint64][]byte
	closed bool
}

var errSourceClosed = errors.New("vmchain: block source closed")

func newSource() *source {
	s := &source{raw: map[uint64][]byte{}}
	s.cond = sync.NewCond(&s.mu)
	return s
}

func (s *source) put(n uint64, raw []byte) {
	s.mu.Lock()
	s.raw[n] = raw
	s.cond.Broadcast()
	s.mu.Unlock()
}

func (s *source) close() {
	s.mu.Lock()
	s.closed = true
	s.cond.Broadcast()
	s.mu.Unlock()
}

func (s *source) GetByHeight(n uint64) ([]byte, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for {
		if raw, ok := s.raw[n]; ok {
			delete(s.raw, n)
			return raw, true, nil
		}
		if s.closed {
			return nil, false, errSourceClosed
		}
		s.cond.Wait()
	}
}

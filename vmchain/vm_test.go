package vmchain

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"reflect"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	sevmdummy "github.com/ava-labs/avalanchego/graft/subnet-evm/consensus/dummy"
	sevmcore "github.com/ava-labs/avalanchego/graft/subnet-evm/core"
	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/snow"
	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/crypto"
	"github.com/ava-labs/libevm/rlp"
)

// A local subnet-evm chain: one funded key, default fee config, timestamp
// past every network upgrade so the rules match a live L1's.
const testGenesis = `{"config":{"chainId":99999,"homesteadBlock":0,"eip150Block":0,"eip155Block":0,"eip158Block":0,
"byzantiumBlock":0,"constantinopleBlock":0,"petersburgBlock":0,"istanbulBlock":0,"muirGlacierBlock":0,"subnetEVMTimestamp":0,
"feeConfig":{"gasLimit":8000000,"targetBlockRate":2,"minBaseFee":25000000000,"targetGas":15000000,"baseFeeChangeDenominator":36,
"minBlockGasCost":0,"maxBlockGasCost":1000000,"blockGasCostStep":200000}},
"alloc":{"%s":{"balance":"0x3635c9adc5dea00000"}},
"nonce":"0x0","timestamp":"0x695f8f80","extraData":"0x00","gasLimit":"0x7a1200","difficulty":"0x0",
"mixHash":"0x0000000000000000000000000000000000000000000000000000000000000000",
"coinbase":"0x0000000000000000000000000000000000000000","number":"0x0","gasUsed":"0x0",
"parentHash":"0x0000000000000000000000000000000000000000000000000000000000000000"}`

// newTestVM initialises a VM on a local genesis and generates n blocks of
// transfers with geth's own trie, so every Verify is a real root check.
func newTestVM(t *testing.T, n int) (*VM, [][]byte, []*types.Block) {
	t.Helper()
	key, _ := crypto.GenerateKey()
	addr := crypto.PubkeyToAddress(key.PublicKey)
	genesisJSON := []byte(fmt.Sprintf(testGenesis, addr.Hex()[2:]))

	vm := &VM{}
	chainCtx := &snow.Context{NetworkID: 12345, SubnetID: ids.GenerateTestID(), ChainID: ids.GenerateTestID(), ChainDataDir: t.TempDir()}
	if err := vm.Initialize(context.Background(), chainCtx, nil, genesisJSON, nil, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { vm.Shutdown(context.Background()) })

	gspec := new(sevmcore.Genesis)
	if err := json.Unmarshal(genesisJSON, gspec); err != nil {
		t.Fatal(err)
	}
	gspec.Config = vm.g.Config
	signer := types.LatestSigner(gspec.Config)
	nonce := uint64(0)
	_, blocks, _, err := sevmcore.GenerateChainWithGenesis(gspec, sevmdummy.NewCoinbaseFaker(), n, 10, func(i int, b *sevmcore.BlockGen) {
		for j := 0; j < 3; j++ {
			to := common.BigToAddress(big.NewInt(int64(1000 + i*10 + j)))
			tx, err := types.SignNewTx(key, signer, &types.LegacyTx{Nonce: nonce, To: &to, Value: big.NewInt(1e15), Gas: 21000, GasPrice: big.NewInt(25_000_000_000)})
			if err != nil {
				t.Fatal(err)
			}
			nonce++
			b.AddTx(tx)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	if blocks[0].ParentHash() != vm.g.Hash {
		t.Fatalf("generated chain parent %x, vm genesis %x", blocks[0].ParentHash(), vm.g.Hash)
	}
	raws := make([][]byte, len(blocks))
	for i, b := range blocks {
		if raws[i], err = rlp.EncodeToBytes(b); err != nil {
			t.Fatal(err)
		}
	}
	return vm, raws, blocks
}

// senderCached reads libevm's per-tx signer cache (types.Sender fills it).
func senderCached(tx *types.Transaction) bool {
	f := reflect.ValueOf(tx).Elem().FieldByName("from")
	return (*atomic.Value)(unsafe.Pointer(f.UnsafeAddr())).Load() != nil
}

func TestParseVerifyAccept(t *testing.T) {
	vm, raws, blocks := newTestVM(t, 5)
	ctx := context.Background()
	if id, _ := vm.LastAccepted(ctx); id != ids.ID(vm.g.Hash) {
		t.Fatalf("LastAccepted before any block = %s, want genesis", id)
	}
	for i, raw := range raws {
		blk, err := vm.ParseBlock(ctx, raw)
		if err != nil {
			t.Fatal(err)
		}
		if blk.ID() != ids.ID(blocks[i].Hash()) || blk.Height() != uint64(i+1) {
			t.Fatalf("block %d parsed as %s height %d", i+1, blk.ID(), blk.Height())
		}
		if err := blk.Verify(ctx); err != nil {
			t.Fatalf("block %d: Verify: %v", i+1, err)
		}
		if err := blk.Verify(ctx); err != nil {
			t.Fatalf("block %d: second Verify: %v", i+1, err)
		}
		if err := blk.Accept(ctx); err != nil {
			t.Fatalf("block %d: Accept: %v", i+1, err)
		}
	}
	last, err := vm.LastAccepted(ctx)
	if err != nil || last != ids.ID(blocks[4].Hash()) {
		t.Fatalf("LastAccepted = %s, %v", last, err)
	}
	id, err := vm.GetBlockIDAtHeight(ctx, 3)
	if err != nil || id != ids.ID(blocks[2].Hash()) {
		t.Fatalf("GetBlockIDAtHeight(3) = %s, %v", id, err)
	}
	got, err := vm.GetBlock(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if string(got.Bytes()) != string(raws[2]) || got.Parent() != ids.ID(blocks[1].Hash()) {
		t.Fatalf("GetBlock(3) does not round trip")
	}
	if err := got.Verify(ctx); err != nil {
		t.Fatalf("Verify on a stored block: %v", err)
	}
	if _, err := vm.GetBlock(ctx, ids.GenerateTestID()); err == nil {
		t.Fatal("GetBlock(unknown) = nil error")
	}
	if h, err := vm.HealthCheck(ctx); err != nil || h.(map[string]uint64)["height"] != 5 {
		t.Fatalf("HealthCheck = %v, %v", h, err)
	}
}

func TestBatchedParseRecoversSenders(t *testing.T) {
	vm, raws, _ := newTestVM(t, 4)
	ctx := context.Background()
	blks, err := vm.BatchedParseBlock(ctx, raws)
	if err != nil {
		t.Fatal(err)
	}
	for deadline := time.Now().Add(10 * time.Second); !vm.rec.idle(); {
		if time.Now().After(deadline) {
			t.Fatal("recoverer did not drain")
		}
		time.Sleep(5 * time.Millisecond)
	}
	for _, b := range blks {
		for i, tx := range b.(*Block).blk.Transactions() {
			if !senderCached(tx) {
				t.Fatalf("block %d tx %d: sender not recovered before Verify", b.Height(), i)
			}
		}
	}
	if done, dropped := vm.Recovered(); done != 4 || dropped != 0 {
		t.Fatalf("recovered %d blocks, dropped %d", done, dropped)
	}
	for _, b := range blks {
		if err := b.Verify(ctx); err != nil {
			t.Fatalf("block %d: %v", b.Height(), err)
		}
		if err := b.Accept(ctx); err != nil {
			t.Fatal(err)
		}
	}
	// Out of order and wrong parent are refused, not executed.
	if err := blks[1].Verify(ctx); err != nil {
		t.Fatalf("re-Verify of an executed block: %v", err)
	}
	if err := (&Block{vm: vm, blk: blks[0].(*Block).blk, height: 6, parent: ids.GenerateTestID(), id: ids.GenerateTestID()}).Verify(ctx); err == nil {
		t.Fatal("a block with a foreign parent verified")
	}
}

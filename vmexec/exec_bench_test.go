package vmexec

// An end-to-end benchmark of the EXECUTOR THREAD's work per block
// (executeBlock: statedb open, EVM, per-tx capture, commit, overlay apply)
// on a synthetic subnet-evm chain shaped like Step: 7 contract calls per
// block, each touching a handful of slots, one nested CALL, one LOG, one
// SHA3. The checker's part (Dirty.Root, WriteBlock) is not in it.

import (
	"crypto/ecdsa"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"math/big"
	"testing"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/rawdb"
	ethstate "github.com/ava-labs/libevm/core/state"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/crypto"
	"github.com/ava-labs/libevm/rlp"
	"github.com/ava-labs/libevm/trie"
	"github.com/ava-labs/libevm/triedb"
	"github.com/holiman/uint256"

	sevmcore "github.com/ava-labs/avalanchego/graft/subnet-evm/core"
	sevmparams "github.com/ava-labs/avalanchego/graft/subnet-evm/params"
	"github.com/ava-labs/avalanchego/graft/subnet-evm/plugin/evm/customtypes"

	"github.com/containerman17/avalanche-s3-archival-node/chain"
	"github.com/containerman17/avalanche-s3-archival-node/dist"
	"github.com/containerman17/avalanche-s3-archival-node/fetch"
	"github.com/containerman17/avalanche-s3-archival-node/store"
)

type benchChain struct {
	g        *Genesis
	keys     []*ecdsa.PrivateKey
	contract common.Address
	signer   types.Signer
	code     map[common.Hash][]byte
}

var benchA, benchB = common.Address{0xaa}, common.Address{0xbb}

// poolWarmsTxHash mirrors the prefetch pool (executor.go): senders and tx
// hashes are warmed before the executor sees the block.
const poolWarmsTxHash = true

// sstoreInc is "slot k += 1": PUSH1 k SLOAD PUSH1 1 ADD PUSH1 k SSTORE.
func sstoreInc(k byte) []byte { return []byte{0x60, k, 0x54, 0x60, 1, 0x01, 0x60, k, 0x55} }

func newBenchChain(tb testing.TB, senders int) *benchChain {
	tb.Helper()
	fetch.RegisterExtras(chain.SubnetEVM)
	var codeB []byte
	for k := byte(0); k < 2; k++ {
		codeB = append(codeB, sstoreInc(k)...)
	}
	codeB = append(codeB, 0x00)
	var codeA []byte
	for k := byte(0); k < 6; k++ {
		codeA = append(codeA, sstoreInc(k)...)
	}
	// CALL(gas=0xffff, B, value 0, in 0/0, out 0/0); POP
	codeA = append(codeA, 0x60, 0, 0x60, 0, 0x60, 0, 0x60, 0, 0x60, 0, 0x73)
	codeA = append(codeA, benchB[:]...)
	codeA = append(codeA, 0x61, 0xff, 0xff, 0xf1, 0x50)
	// LOG1(0, 0, topic 0); SHA3(0, 0) POP; STOP
	codeA = append(codeA, 0x60, 0, 0x60, 0, 0x60, 0, 0xa1, 0x60, 0, 0x60, 0, 0x20, 0x50, 0x00)

	storage := func(n int) map[common.Hash]common.Hash {
		m := map[common.Hash]common.Hash{}
		for k := 0; k < n; k++ {
			m[common.BigToHash(big.NewInt(int64(k)))] = common.Hash{31: 1}
		}
		return m
	}
	alloc := types.GenesisAlloc{
		benchA: {Code: codeA, Balance: big.NewInt(0), Storage: storage(6)},
		benchB: {Code: codeB, Balance: big.NewInt(0), Storage: storage(2)},
	}
	keys := make([]*ecdsa.PrivateKey, senders)
	for i := range keys {
		k, err := crypto.GenerateKey()
		if err != nil {
			tb.Fatal(err)
		}
		keys[i] = k
		alloc[crypto.PubkeyToAddress(k.PublicKey)] = types.Account{Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(24), nil)}
	}
	gen := &sevmcore.Genesis{Config: sevmparams.TestChainConfig, Alloc: alloc, GasLimit: 8_000_000, Difficulty: big.NewInt(0)}
	js, err := json.Marshal(gen)
	if err != nil {
		tb.Fatal(err)
	}
	g, err := ChainGenesis(&chain.Chain{VMKind: chain.SubnetEVM, GenesisJSON: js, NetworkID: 12345})
	if err != nil {
		tb.Fatal(err)
	}
	return &benchChain{
		g: g, keys: keys, contract: benchA, signer: types.LatestSigner(g.Config),
		code: map[common.Hash][]byte{crypto.Keccak256Hash(codeA): codeA, crypto.Keccak256Hash(codeB): codeB},
	}
}

// block is block n with one call to the contract from every sender, senders
// already recovered (the prefetch pool's job in production).
func (bc *benchChain) block(tb testing.TB, n uint64, parent common.Hash) *types.Block {
	txs := make([]*types.Transaction, len(bc.keys))
	// The executor's signer is MakeSigner's; the sender cache only hits for
	// an equal signer, so warm it with the same one (as the pool does).
	warm := types.MakeSigner(bc.g.Config, new(big.Int).SetUint64(n), n)
	for i, k := range bc.keys {
		tx, err := types.SignTx(types.NewTx(&types.LegacyTx{
			Nonce: n - 1, GasPrice: big.NewInt(25e9), Gas: 300_000, To: &bc.contract, Data: make([]byte, 68),
		}), bc.signer, k)
		if err != nil {
			tb.Fatal(err)
		}
		types.Sender(warm, tx)
		if poolWarmsTxHash {
			tx.Hash()
		}
		txs[i] = tx
	}
	h := &types.Header{
		ParentHash: parent, Number: new(big.Int).SetUint64(n), GasLimit: 8_000_000, BaseFee: big.NewInt(25e9),
		Difficulty: big.NewInt(1), Time: n, Extra: make([]byte, 32),
	}
	customtypes.SetHeaderExtra(h, &customtypes.HeaderExtra{BlockGasCost: big.NewInt(0)})
	return types.NewBlock(h, txs, nil, nil, trie.NewStackTrie(nil))
}

func newBenchExecutor(tb testing.TB, bc *benchChain) *Executor {
	tb.Helper()
	eng, err := newEngine(tb.TempDir(), bc.g.TrieAlloc, bc.g.Root)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(eng.close)
	memdb := rawdb.NewMemoryDatabase()
	for h, c := range bc.code {
		rawdb.WriteCode(memdb, h, c)
	}
	code := ethstate.NewDatabaseWithNodeDB(memdb, triedb.NewDatabase(memdb, triedb.HashDefaults))
	flat := &flatDB{code: code, eng: eng}
	e := &Executor{
		chainCfg: bc.g.Config,
		chainCtx: chainContext{recent: map[uint64]*types.Header{}},
		wrapDB:   wrapDatabase(flat),
		flat:     flat,
		eng:      eng,
		headRoot: bc.g.Root,
	}
	e.recentHdr = e.chainCtx.recent
	return e
}

// runBlocks executes n blocks on the executor's own path and returns the
// items the checker would get.
func runBlocks(tb testing.TB, e *Executor, bc *benchChain, blocks []*types.Block) (gas uint64, items []*checkItem) {
	for _, blk := range blocks {
		it, err := e.executeBlock(blk, []byte{0})
		if err != nil {
			tb.Fatal(err)
		}
		e.headTime = blk.Time()
		delete(e.recentHdr, blk.NumberU64())
		e.unpublished = e.unpublished[:0]
		if n := len(it.receipts); n > 0 {
			gas += it.receipts[n-1].CumulativeGasUsed
		}
		items = append(items, it)
	}
	return gas, items
}

func benchBlocks(tb testing.TB, bc *benchChain, n int) []*types.Block {
	blocks := make([]*types.Block, n)
	parent := bc.g.Hash
	for i := range blocks {
		blocks[i] = bc.block(tb, uint64(i+1), parent)
		parent = blocks[i].Hash()
	}
	return blocks
}

func TestExecuteBlockSmoke(t *testing.T) {
	bc := newBenchChain(t, 7)
	e := newBenchExecutor(t, bc)
	blocks := benchBlocks(t, bc, 3)
	gas, items := runBlocks(t, e, bc, blocks)
	for _, it := range items {
		for i, r := range it.receipts {
			if r.Status != types.ReceiptStatusSuccessful {
				t.Fatalf("block %d tx %d failed: %+v", it.blk.NumberU64(), i, r)
			}
		}
		if len(it.bw.Txs) != 7 || len(it.bw.Txs[0].State) == 0 {
			t.Fatalf("block %d: %d txs, rows %d", it.blk.NumberU64(), len(it.bw.Txs), len(it.bw.Txs[0].State))
		}
	}
	t.Logf("gas/block %d", gas/3)
}

func BenchmarkExecuteBlock(b *testing.B) {
	bc := newBenchChain(b, 7)
	e := newBenchExecutor(b, bc)
	blocks := benchBlocks(b, bc, b.N)
	b.ReportAllocs()
	b.ResetTimer()
	gas, _ := runBlocks(b, e, bc, blocks)
	b.StopTimer()
	b.ReportMetric(float64(gas)/float64(b.N), "gas/block")
	b.ReportMetric(float64(gas)/b.Elapsed().Seconds()/1e6, "mgas/s")
}

// BenchmarkStoreWriteBlock is the checker's store cost per Step-like block
// (7 txs with frames JSON, receipts, 3 state rows and a log each), through
// the store's exported API.
func BenchmarkStoreWriteBlock(b *testing.B) {
	dir := b.TempDir()
	cas, err := dist.Local(dir)
	if err != nil {
		b.Fatal(err)
	}
	db, err := store.Open(dir, cas, [32]byte{1})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	fetch.RegisterExtras(chain.SubnetEVM)
	frames, _ := stepTrace(b).GetResult()
	hdr, _ := rlp.EncodeToBytes(&types.Header{Number: big.NewInt(1), GasLimit: 8e6, BaseFee: big.NewInt(25e9), Difficulty: big.NewInt(1), Extra: make([]byte, 32)})
	txRLP := make([]byte, 180)
	receipt := make([]byte, 120)
	row, _ := rlp.EncodeToBytes(&types.StateAccount{Nonce: 1, Balance: uint256.NewInt(5), Root: types.EmptyRootHash, CodeHash: types.EmptyCodeHash[:]})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bw := &store.BlockWrite{Height: uint64(i + 1), HeaderRLP: hdr, Pvm: make([]byte, 600), Code: map[string][]byte{}}
		for t := 0; t < 7; t++ {
			h := sha256.Sum256([]byte(fmt.Sprint(i, t)))
			from, to := common.Address{byte(t), 1}, common.Address{0xaa}
			bw.Txs = append(bw.Txs, store.TxWrite{
				Hash: h[:], RLP: txRLP, Receipt: receipt, Frames: frames,
				FrameAddrs: [][]byte{from[:], to[:], {0xbb}},
				State: []store.StateRow{
					{Kind: 'a', Addr: from[:], Val: row},
					{Kind: 'a', Addr: common.Address{1}.Bytes(), Val: row},
					{Kind: 's', Addr: to[:], Slot: common.Hash{byte(t)}.Bytes(), Val: []byte{byte(i), 1}},
				},
				Sender: from[:], To: to[:],
				Logs: []store.LogWrite{{Emitter: to[:], Topics: [][]byte{h[:], h[:]}}},
			})
		}
		if err := db.WriteBlock(bw); err != nil {
			b.Fatal(err)
		}
	}
}

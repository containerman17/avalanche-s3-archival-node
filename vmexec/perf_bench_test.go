package vmexec

// Microbenchmarks for the executor-thread costs the Step profile
// (scratchpad vm/step-new.prof, binary vm3f) attributes to our code and to
// the libevm paths the engine calls. Run: go test ./vmexec -run xxx -bench .
// -benchmem

import (
	"encoding/json"
	"math/big"
	"testing"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/core/vm"
	"github.com/ava-labs/libevm/crypto"
	"github.com/ava-labs/libevm/eth/tracers"
	"github.com/ava-labs/libevm/rlp"
	"github.com/holiman/uint256"

	sevmparams "github.com/ava-labs/avalanchego/graft/subnet-evm/params"

	"github.com/containerman17/avalanche-s3-archival-node/chain"
	"github.com/containerman17/avalanche-s3-archival-node/fetch"
	"github.com/containerman17/avalanche-s3-archival-node/latest"
)

// Keccak of a 20-byte address: what every GetAccount/UpdateAccount pays to
// form the flat key (flatdb.go), on top of libevm's own addrHash.
func BenchmarkKeccakAddr(b *testing.B) {
	var a common.Address
	a[3] = 7
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		a[0] = byte(i)
		_ = crypto.Keccak256Hash(a[:])
	}
}

// Keccak of a 32-byte slot: what every GetStorage/UpdateStorage pays.
func BenchmarkKeccakSlot(b *testing.B) {
	var k common.Hash
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		k[0] = byte(i)
		_ = crypto.Keccak256Hash(k[:])
	}
}

// One keccak with a reused hasher state (no NewLegacyKeccak256 alloc).
func BenchmarkKeccakAddrReusedState(b *testing.B) {
	var a common.Address
	h := crypto.NewKeccakState()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		a[0] = byte(i)
		_ = crypto.HashData(h, a[:])
	}
}

// A hit in a small per-block addr->hash map: the alternative to rehashing
// the same hot addresses (fee recipient, the game contracts) every access.
func BenchmarkAddrHashCacheHit(b *testing.B) {
	m := map[common.Address]common.Hash{}
	addrs := make([]common.Address, 64)
	for i := range addrs {
		addrs[i][0] = byte(i)
		m[addrs[i]] = crypto.Keccak256Hash(addrs[i][:])
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = m[addrs[i&63]]
	}
}

// The full flat read of a slot: key formation (keccak) + writeSet miss +
// View.Get over the 40-byte index run, as flatTrie.GetStorage does.
func BenchmarkFlatSlotRead(b *testing.B) {
	ov := latest.NewOverlay()
	ah := crypto.Keccak256Hash([]byte("contract"))
	slots := make([]common.Hash, 4096)
	for i := range slots {
		slots[i] = common.Hash{byte(i), byte(i >> 8), 1}
		ov.Put(slotKey(ah, crypto.Keccak256Hash(slots[i][:])), []byte{1, byte(i)})
	}
	run, err := latest.Merge(b.TempDir()+"/run", latest.NewView(ov), [32]byte{})
	if err != nil {
		b.Fatal(err)
	}
	defer run.Close()
	view := latest.NewView(latest.NewOverlay(), run)
	d := &flatDB{eng: &engine{view: view}, ws: newWriteSet()}
	t := &flatTrie{db: d, storage: true, ah: ah}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if v, _ := t.GetStorage(common.Address{}, slots[i&4095][:]); len(v) == 0 {
			b.Fatal("miss")
		}
	}
}

// Header.Hash() as subnet-evm's ApplyTransaction calls it: an uncached RLP
// encode + keccak of the header PER TRANSACTION (Block.Hash() caches).
func BenchmarkHeaderHash(b *testing.B) {
	fetch.RegisterExtras(chain.SubnetEVM)
	h := &types.Header{Number: big.NewInt(500_000), GasLimit: 8_000_000, BaseFee: big.NewInt(25e9), Difficulty: big.NewInt(1), Time: 1_700_000_000, Extra: make([]byte, 32)}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = h.Hash()
	}
}

// Transaction.Hash() cold: the executor pays it in SetTxContext when the
// prefetch pool has not warmed it (it only recovers senders).
func BenchmarkTxHashCold(b *testing.B) {
	txs := make([]*types.Transaction, 1024)
	for i := range txs {
		txs[i] = types.NewTx(&types.LegacyTx{Nonce: uint64(i), GasPrice: big.NewInt(25e9), Gas: 150_000, To: &common.Address{1}, Data: make([]byte, 68), V: big.NewInt(27), R: big.NewInt(1), S: big.NewInt(1)})
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if i&1023 == 0 && i > 0 {
			b.StopTimer()
			for j := range txs {
				txs[j] = types.NewTx(&types.LegacyTx{Nonce: uint64(j), GasPrice: big.NewInt(25e9), Gas: 150_000, To: &common.Address{1}, Data: make([]byte, 68), V: big.NewInt(27), R: big.NewInt(int64(i)), S: big.NewInt(1)})
			}
			b.StartTimer()
		}
		_ = txs[i&1023].Hash()
	}
}

// ChainConfig.Rules on a subnet-evm config: called three times per tx
// (TransitionDb, NewEVM, OverrideNewEVMArgs).
func BenchmarkRules(b *testing.B) {
	fetch.RegisterExtras(chain.SubnetEVM)
	cfg := sevmparams.TestChainConfig
	n := big.NewInt(500_000)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = cfg.Rules(n, sevmparams.IsMergeTODO, 1_700_000_000)
	}
}

// callTracer.GetResult (encoding/json of the frame tree) for a Step-shaped
// trace: one top frame with a 68-byte input, two nested CALLs, 32-byte
// outputs. This is what captureTx pays on the executor thread per tx.
func BenchmarkCallTracerJSON(b *testing.B) {
	t := stepTrace(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := t.GetResult(); err != nil {
			b.Fatal(err)
		}
	}
}

// The same trace through a hand-written encoder (see frames_json.go): the
// bytes must equal encoding/json's, which TestFrameJSONParity checks.
func BenchmarkCallTracerJSONHand(b *testing.B) {
	t := stepTrace(b)
	want, _ := t.GetResult()
	var f callFrameJSON
	if err := json.Unmarshal(want, &f); err != nil {
		b.Fatal(err)
	}
	buf := make([]byte, 0, 2048)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf = f.append(buf[:0])
	}
}

func stepTrace(b *testing.B) tracers.Tracer {
	t, err := tracers.DefaultDirectory.New("callTracer", &tracers.Context{}, json.RawMessage(nil))
	if err != nil {
		b.Fatal(err)
	}
	from, to, to2 := common.Address{1}, common.Address{2}, common.Address{3}
	in := make([]byte, 68)
	out := make([]byte, 32)
	t.CaptureTxStart(150_000)
	t.CaptureStart(nil, from, to, false, in, 150_000, big.NewInt(0))
	t.CaptureEnter(vm.CALL, to, to2, in[:36], 100_000, big.NewInt(0))
	t.CaptureExit(out, 21_000, nil)
	t.CaptureEnter(vm.STATICCALL, to, to2, in[:36], 70_000, nil)
	t.CaptureExit(out, 2_600, nil)
	t.CaptureEnd(out, 120_000, nil)
	t.CaptureTxEnd(30_000)
	return t
}

// Account row encode as UpdateAccount does it twice (capture's StateAccount
// RLP + the flat accountRow RLP).
func BenchmarkAccountRLP(b *testing.B) {
	acc := &types.StateAccount{Nonce: 12, Balance: uint256.NewInt(1e18), Root: types.EmptyRootHash, CodeHash: types.EmptyCodeHash[:]}
	row := &accountRow{Nonce: 12, Balance: uint256.NewInt(1e18), CodeHash: types.EmptyCodeHash[:]}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		rlp.EncodeToBytes(acc)
		rlp.EncodeToBytes(row)
	}
}

// One block's write set applied to the overlay: Step-like, 8 accounts and
// 12 slots under 3 contracts.
func BenchmarkApplyOverlay(b *testing.B) {
	eng := &engine{overlay: latest.NewOverlay(), owners: map[common.Hash]struct{}{}}
	eng.view = latest.NewView(eng.overlay)
	var accts [8]common.Hash
	for i := range accts {
		accts[i] = crypto.Keccak256Hash([]byte{byte(i)})
	}
	row, _ := rlp.EncodeToBytes(&accountRow{Nonce: 1, Balance: uint256.NewInt(5), CodeHash: types.EmptyCodeHash[:]})
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		ws := newWriteSet()
		for _, a := range accts {
			ws.put(accountKey(a), row)
		}
		for c := 0; c < 3; c++ {
			for s := 0; s < 4; s++ {
				ws.put(slotKey(accts[c], common.Hash{byte(s), byte(i)}), []byte{byte(i), 1})
			}
		}
		eng.applyOverlay(ws)
	}
}

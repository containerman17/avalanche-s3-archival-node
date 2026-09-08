package vmexec

import (
	"math/big"
	"math/rand"
	"testing"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/rawdb"
	ethstate "github.com/ava-labs/libevm/core/state"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/crypto"
	"github.com/ava-labs/libevm/rlp"
	"github.com/ava-labs/libevm/triedb"
	"github.com/holiman/uint256"
)

// BenchmarkBlockRoot is one beam-sized block (a transfer plus two slot writes
// on a contract) applied and rooted over a 100k-account state: the per-block
// fixed cost of Dirty.Root.
func BenchmarkBlockRoot(b *testing.B) { benchBlockRoot(b, 3, 1, 2) }

// BenchmarkBlockRootStep is a Step-sized block: 8 accounts (7 senders + the
// fee recipient) and 3 contracts with 4 slot writes each.
func BenchmarkBlockRootStep(b *testing.B) { benchBlockRoot(b, 8, 3, 4) }

func benchBlockRoot(b *testing.B, accounts, contracts, slots int) {
	rng := rand.New(rand.NewSource(1))
	alloc := make(types.GenesisAlloc, 100_000)
	addrs := make([]common.Address, 0, 100_000)
	for i := 0; i < 100_000; i++ {
		var a common.Address
		rng.Read(a[:])
		acct := types.Account{Balance: big.NewInt(int64(rng.Intn(1e9) + 1))}
		if i%50 == 0 {
			acct.Storage = map[common.Hash]common.Hash{}
			for j := 0; j < 40; j++ {
				acct.Storage[common.Hash{byte(j), 1}] = common.Hash{byte(j + 1)}
			}
		}
		alloc[a] = acct
		addrs = append(addrs, a)
	}
	memdb := rawdb.NewMemoryDatabase()
	tdb := triedb.NewDatabase(memdb, triedb.HashDefaults)
	ref, _ := ethstate.New(types.EmptyRootHash, ethstate.NewDatabaseWithNodeDB(memdb, tdb), nil)
	for addr, a := range alloc {
		ref.SetBalance(addr, uint256.MustFromBig(a.Balance))
		for k, v := range a.Storage {
			ref.SetState(addr, k, v)
		}
	}
	root0, _ := ref.Commit(0, false)
	eng, err := newEngine(b.TempDir(), alloc, root0)
	if err != nil {
		b.Fatal(err)
	}
	defer eng.close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ws := newWriteSet()
		for k := 0; k < accounts; k++ {
			a := addrs[rng.Intn(len(addrs))]
			row, _ := rlp.EncodeToBytes(&accountRow{Nonce: uint64(i), Balance: uint256.NewInt(uint64(i + 1)), CodeHash: types.EmptyCodeHash[:]})
			ws.put(accountKey(crypto.Keccak256Hash(a[:])), row)
		}
		for c := 0; c < contracts; c++ {
			ch := crypto.Keccak256Hash(addrs[rng.Intn(2000)*50][:])
			for j := 0; j < slots; j++ {
				ws.put(slotKey(ch, crypto.Keccak256Hash(common.Hash{byte(rng.Intn(40)), 1}.Bytes())), []byte{byte(i), byte(j + 1)})
			}
		}
		eng.applyOverlay(ws)
		if err := eng.applyDirty(ws); err != nil {
			b.Fatal(err)
		}
		if _, err := eng.root(); err != nil {
			b.Fatal(err)
		}
	}
}

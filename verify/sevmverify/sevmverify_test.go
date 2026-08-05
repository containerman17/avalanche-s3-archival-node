// Package sevmverify exists only to hold this test, for the same reason
// exec/sevmexec, rpc/sevmrpc and fetch/sevm do: libevm's extras registry is
// process-global and PANICS on re-registration, package verify's own tests
// register coreth, and `go test` gives every package its own process. So a
// subnet-evm verification test cannot live in package verify, and this is the
// isolation it needs.
package sevmverify

import (
	"crypto/ecdsa"
	"encoding/hex"
	"fmt"
	"math/big"
	"strings"
	"testing"

	"github.com/ava-labs/avalanchego/graft/evm/firewood"
	"github.com/ava-labs/avalanchego/ids"
	avaconstants "github.com/ava-labs/avalanchego/utils/constants"
	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/rawdb"
	ethstate "github.com/ava-labs/libevm/core/state"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/crypto"
	"github.com/ava-labs/libevm/libevm/stateconf"
	"github.com/ava-labs/libevm/rlp"
	"github.com/ava-labs/libevm/trie"
	"github.com/ava-labs/libevm/triedb"
	"github.com/holiman/uint256"

	"github.com/containerman17/epochdb/chain"
	"github.com/containerman17/epochdb/dist"
	"github.com/containerman17/epochdb/exec"
	"github.com/containerman17/epochdb/state"
	"github.com/containerman17/epochdb/verify"
)

// The chain under test is shaped like FIFA (mainnet L1, NativeMinter in genesis
// at time 0) PLUS the one thing FIFA does not have: an upgrade.json. Those
// bytes are inside the chain root, so they are what epoch 1's prev-hash
// commits to.
const (
	testChainID = 13322
	genesisTime = 1767225600 // 2026-01-01T00:00:00Z, past every mainnet upgrade
	perEpoch    = 4
	blocks      = 8
)

var funded = mustKey("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")

func mustKey(hex string) *ecdsa.PrivateKey {
	k, err := crypto.HexToECDSA(hex)
	if err != nil {
		panic(err)
	}
	return k
}

func fundedAddr() common.Address { return crypto.PubkeyToAddress(funded.PublicKey) }

func genesisJSON() []byte {
	addr := fundedAddr().Hex()
	return []byte(fmt.Sprintf(`{
	  "config": {
	    "chainId": %d,
	    "feeConfig": {
	      "gasLimit": 8000000,
	      "targetBlockRate": 2,
	      "minBaseFee": 25000000000,
	      "targetGas": 15000000,
	      "baseFeeChangeDenominator": 36,
	      "minBlockGasCost": 0,
	      "maxBlockGasCost": 1000000,
	      "blockGasCostStep": 200000
	    },
	    "subnetEVMTimestamp": 0,
	    "contractNativeMinterConfig": {
	      "blockTimestamp": 0,
	      "adminAddresses": ["%s"]
	    }
	  },
	  "alloc": {
	    "%s": {"balance": "0x52B7D2DCC80CD2E4000000"}
	  },
	  "nonce": "0x0",
	  "timestamp": "0x%x",
	  "extraData": "0x",
	  "gasLimit": "0x7A1200",
	  "difficulty": "0x0",
	  "mixHash": "0x0000000000000000000000000000000000000000000000000000000000000000",
	  "coinbase": "0x0000000000000000000000000000000000000000",
	  "number": "0x0",
	  "gasUsed": "0x0",
	  "parentHash": "0x0000000000000000000000000000000000000000000000000000000000000000"
	}`, testChainID, addr, addr[2:], genesisTime))
}

func upgradeJSON() []byte {
	return []byte(fmt.Sprintf(`{
	  "stateUpgrades": [
	    {
	      "blockTimestamp": %d,
	      "accounts": {
	        "0x00000000000000000000000000000000000000ff": {"balanceChange": "0x3e8"}
	      }
	    }
	  ]
	}`, genesisTime+6))
}

func testChain() *chain.Chain {
	return &chain.Chain{
		GenesisJSON:  genesisJSON(),
		UpgradeJSON:  upgradeJSON(),
		NetworkID:    avaconstants.MainnetID,
		SubnetID:     ids.ID{0xf1, 0xfa},
		BlockchainID: ids.ID{0xf1, 0xfa, 0xb1},
		VMKind:       chain.SubnetEVM,
	}
}

// ---------- corpus ----------

var (
	alice    = common.HexToAddress("0xa11ce00000000000000000000000000000000001")
	bob      = common.HexToAddress("0xb0b0000000000000000000000000000000000002")
	contract = common.HexToAddress("0xc0de000000000000000000000000000000000003")
	slot1    = common.HexToHash("0x01")
	slot2    = common.HexToHash("0x02")
)

type tblock struct {
	n        uint64
	hdr      *types.Header
	txs      types.Transactions
	receipts types.Receipts
	rows     []state.StateRow
}

func acctRow(blk uint64, addr common.Address, nonce uint64, bal int64, seq int) state.StateRow {
	acc := types.StateAccount{
		Nonce:    nonce,
		Balance:  uint256.NewInt(uint64(bal)),
		Root:     common.Hash{}, // firewood storage tries report the zero hash
		CodeHash: types.EmptyCodeHash.Bytes(),
	}
	val, err := rlp.EncodeToBytes(&acc)
	if err != nil {
		panic(err)
	}
	r := state.StateRow{Block: blk, Value: val, Seq: seq}
	r.Key[0] = 'a'
	copy(r.Key[1:21], addr[:])
	return r
}

func storageRow(blk uint64, addr common.Address, slot common.Hash, val []byte, seq int) state.StateRow {
	r := state.StateRow{Block: blk, Value: val, Seq: seq}
	r.Key[0] = 's'
	copy(r.Key[1:21], addr[:])
	copy(r.Key[21:53], slot[:])
	return r
}

// applyRows is the verifier's own row replay, duplicated here because the
// builder must write EXACTLY what the verifier will read back.
func applyRows(t *testing.T, tr ethstate.Trie, rows []state.StateRow) {
	t.Helper()
	for i := range rows {
		r := &rows[i]
		addr := common.BytesToAddress(r.Key[1:21])
		switch r.Key[0] {
		case 'a':
			var acc types.StateAccount
			if err := rlp.DecodeBytes(r.Value, &acc); err != nil {
				t.Fatalf("decode account: %v", err)
			}
			if err := tr.UpdateAccount(addr, &acc); err != nil {
				t.Fatalf("update account: %v", err)
			}
		case 's':
			if len(r.Value) == 0 {
				if err := tr.DeleteStorage(addr, r.Key[21:53]); err != nil {
					t.Fatalf("delete storage: %v", err)
				}
				continue
			}
			if err := tr.UpdateStorage(addr, r.Key[21:53], r.Value); err != nil {
				t.Fatalf("update storage: %v", err)
			}
		}
	}
}

// buildChain generates the corpus: real subnet-evm headers and transactions
// chained off the descriptor's REAL genesis (parsed and committed by the
// subnet-evm backend), with per-block post-image rows whose Firewood
// application produced each header.Root. That is precisely the shape the
// no-execution verifier checks, and none of it is coreth-decodable.
func buildChain(t *testing.T, c *chain.Chain) (*exec.Genesis, []tblock) {
	t.Helper()
	g, err := exec.ChainGenesis(c) // also performs the subnet-evm extras registration
	if err != nil {
		t.Fatalf("ChainGenesis: %v", err)
	}

	fwCfg := firewood.DefaultConfig(t.TempDir())
	fwCfg.CacheSizeBytes = 64 << 20
	memdb := rawdb.NewMemoryDatabase()
	tdb := triedb.NewDatabase(memdb, &triedb.Config{DBOverride: fwCfg.BackendConstructor})
	defer tdb.Close()
	fw, ok := tdb.Backend().(*firewood.TrieDB)
	if !ok {
		t.Fatalf("triedb backend is %T", tdb.Backend())
	}
	db := exec.NewStateDatabase(memdb, tdb)
	if !tdb.Initialized(g.Root) {
		if err := g.Commit(rawdb.NewMemoryDatabase(), tdb); err != nil {
			t.Fatalf("commit genesis: %v", err)
		}
	}
	fw.SetHashAndHeight(g.Hash, 0)

	signer := types.NewLondonSigner(big.NewInt(testChainID))
	tx := func(nonce uint64) *types.Transaction {
		signed, err := types.SignNewTx(funded, signer, &types.DynamicFeeTx{
			ChainID: big.NewInt(testChainID), Nonce: nonce, GasTipCap: common.Big0,
			GasFeeCap: big.NewInt(25_000_000_000), Gas: 21_000, To: &bob, Value: big.NewInt(7),
		})
		if err != nil {
			t.Fatal(err)
		}
		return signed
	}
	rcpt := func(status, gas, cum uint64, logs ...*types.Log) *types.Receipt {
		r := &types.Receipt{Type: types.DynamicFeeTxType, Status: status, GasUsed: gas, CumulativeGasUsed: cum, Logs: logs}
		r.Bloom = types.CreateBloom(types.Receipts{r})
		return r
	}
	lg := func(b byte) *types.Log {
		return &types.Log{
			Address: contract,
			Topics:  []common.Hash{common.HexToHash("0x7091c")},
			Data:    []byte{b, b + 1},
		}
	}

	specs := []struct {
		rows     []state.StateRow
		txs      types.Transactions
		receipts types.Receipts
	}{
		{ // 1: create alice + contract storage
			rows: []state.StateRow{
				acctRow(1, alice, 1, 100, 0),
				acctRow(1, contract, 1, 0, 1),
				storageRow(1, contract, slot1, []byte{0x01}, 2),
			},
			txs:      types.Transactions{tx(0)},
			receipts: types.Receipts{rcpt(1, 21000, 21000)},
		},
		{}, // 2: empty block (no diffs, root must not move)
		{ // 3: rewrite slot1, add slot2, create bob; one log
			rows: []state.StateRow{
				acctRow(3, alice, 2, 90, 0),
				acctRow(3, bob, 1, 10, 1),
				storageRow(3, contract, slot1, []byte{0x02}, 2),
				storageRow(3, contract, slot2, []byte{0xbe, 0xef}, 3),
			},
			txs: types.Transactions{tx(1), tx(2)},
			receipts: types.Receipts{
				rcpt(1, 21000, 21000, lg(0x30)),
				rcpt(0, 22000, 43000),
			},
		},
		{ // 4: delete slot2 (epoch boundary)
			rows:     []state.StateRow{storageRow(4, contract, slot2, nil, 0)},
			txs:      types.Transactions{tx(3)},
			receipts: types.Receipts{rcpt(1, 21000, 21000, lg(0x40))},
		},
		{ // 5: alice nonce bump, first block of epoch 2
			rows:     []state.StateRow{acctRow(5, alice, 3, 80, 0)},
			txs:      types.Transactions{tx(4)},
			receipts: types.Receipts{rcpt(1, 21000, 21000)},
		},
		{}, // 6: empty
		{ // 7: two txs, two logs on one receipt
			rows: []state.StateRow{
				acctRow(7, bob, 2, 12, 0),
				storageRow(7, contract, common.HexToHash("0x03"), []byte{0x07}, 1),
			},
			txs: types.Transactions{tx(5), tx(6)},
			receipts: types.Receipts{
				rcpt(1, 21000, 21000, lg(0x70), lg(0x71)),
				rcpt(1, 30000, 51000),
			},
		},
		{ // 8: final write
			rows:     []state.StateRow{acctRow(8, alice, 4, 70, 0)},
			txs:      types.Transactions{tx(7)},
			receipts: types.Receipts{rcpt(1, 21000, 21000)},
		},
	}
	if len(specs) != blocks {
		t.Fatalf("specs=%d, blocks=%d", len(specs), blocks)
	}

	parentRoot, parentHash, fwHeight := g.Root, g.Hash, uint64(0)
	var out []tblock
	for i, sp := range specs {
		n := uint64(i + 1)
		root := parentRoot
		if len(sp.rows) > 0 {
			tr, err := db.OpenTrie(parentRoot)
			if err != nil {
				t.Fatalf("block %d: open trie: %v", n, err)
			}
			applyRows(t, tr, sp.rows)
			if root = tr.Hash(); root == (common.Hash{}) {
				t.Fatalf("block %d: firewood proposal failed", n)
			}
		}
		var gasUsed uint64
		if len(sp.receipts) > 0 {
			gasUsed = sp.receipts[len(sp.receipts)-1].CumulativeGasUsed
		}
		hdr := &types.Header{
			ParentHash:  parentHash,
			Root:        root,
			TxHash:      types.DeriveSha(sp.txs, trie.NewStackTrie(nil)),
			ReceiptHash: types.DeriveSha(sp.receipts, trie.NewStackTrie(nil)),
			Bloom:       types.CreateBloom(sp.receipts),
			Number:      new(big.Int).SetUint64(n),
			GasLimit:    8_000_000,
			GasUsed:     gasUsed,
			Time:        genesisTime + 2*n,
			Difficulty:  big.NewInt(1),
			BaseFee:     big.NewInt(25_000_000_000),
		}
		hdrHash := hdr.Hash()
		if len(sp.rows) > 0 {
			opt := stateconf.WithTrieDBUpdatePayload(parentHash, hdrHash)
			if err := tdb.Update(root, parentRoot, fwHeight+1, nil, nil, opt); err != nil {
				t.Fatalf("block %d: firewood update: %v", n, err)
			}
			if err := tdb.Commit(root, false); err != nil {
				t.Fatalf("block %d: firewood commit: %v", n, err)
			}
			fwHeight++
		} else {
			fw.SetHashAndHeight(hdrHash, n)
			fwHeight = n
		}
		parentRoot, parentHash = root, hdrHash
		out = append(out, tblock{n: n, hdr: hdr, txs: sp.txs, receipts: sp.receipts, rows: sp.rows})
	}
	return g, out
}

// sealEpochs writes the corpus as two hash-chained epochs, anchored at the
// descriptor's chain root exactly as the real seal does.
func sealEpochs(t *testing.T, st *dist.Store, c *chain.Chain, bs []tblock, mutate func(*state.EpochInput)) {
	t.Helper()
	prev := c.Root()
	for at := 0; at < len(bs); at += perEpoch {
		chunk := bs[at:min(at+perEpoch, len(bs))]
		in := &state.EpochInput{
			Start:    chunk[0].n,
			Prev:     prev,
			TxHashes: map[uint64][][32]byte{},
			FullLogs: map[uint64][]byte{},
			RcptRecs: map[uint64][]byte{},
		}
		for _, b := range chunk {
			hdrRLP, err := rlp.EncodeToBytes(b.hdr)
			if err != nil {
				t.Fatal(err)
			}
			blk := types.NewBlockWithHeader(b.hdr).WithBody(types.Body{Transactions: b.txs})
			raw, err := rlp.EncodeToBytes(blk)
			if err != nil {
				t.Fatal(err)
			}
			in.Headers = append(in.Headers, hdrRLP)
			in.Containers = append(in.Containers, raw)
			in.StateRows = append(in.StateRows, b.rows...)
			for _, tx := range b.txs {
				in.TxHashes[b.n] = append(in.TxHashes[b.n], [32]byte(tx.Hash()))
				in.TxCount++
			}
			if rec := state.EncodeStoredReceipts(b.receipts); rec != nil {
				in.RcptRecs[b.n] = rec
			}
			if rec := state.EncodeStoredLogs(b.receipts); rec != nil {
				in.FullLogs[b.n] = rec
			}
		}
		if mutate != nil {
			mutate(in)
		}
		hash, err := state.BuildEpoch(st, in)
		if err != nil {
			t.Fatal(err)
		}
		raw, err := hex.DecodeString(hash)
		if err != nil || len(raw) != 32 {
			t.Fatalf("epoch hash %q: %v", hash, err)
		}
		prev = [32]byte(raw)
	}
}

func store(t *testing.T, dir string) *dist.Store {
	t.Helper()
	st, err := dist.Local(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	return st
}

// TestVerifySubnetEVM is the whole point: `epochdb verify --chain <blockchainID>`
// on an L1 corpus. It used to be refused by name; now it runs the same four
// checks it runs on the C-chain (diff-applied state roots from the chain's own
// genesis, txRoot, reconstructed receiptsRoot, header parent-hash chain), with
// the subnet-evm extras and the subnet-evm state database behind them.
func TestVerifySubnetEVM(t *testing.T) {
	c := testChain()
	g, bs := buildChain(t, c)
	if g.Config.ChainID == nil || g.Config.ChainID.Int64() != testChainID {
		t.Fatalf("genesis chain id = %v", g.Config.ChainID)
	}

	dir := t.TempDir()
	st := store(t, dir)
	sealEpochs(t, st, c, bs, nil)

	n, _, err := verify.VerifySet(st, t.TempDir(), c, 2)
	if err != nil {
		t.Fatalf("a clean subnet-evm corpus must verify: %v", err)
	}
	if n != blocks {
		t.Fatalf("verified %d blocks, want %d", n, blocks)
	}
}

// TestVerifySubnetEVMTeeth proves the pass above can fail, on the check that is
// specific to this engine: one flipped SST byte must break the state root.
func TestVerifySubnetEVMTeeth(t *testing.T) {
	c := testChain()
	_, bs := buildChain(t, c)

	dir := t.TempDir()
	st := store(t, dir)
	sealEpochs(t, st, c, bs, func(in *state.EpochInput) {
		for i := range in.StateRows {
			r := &in.StateRows[i]
			if r.Block == 3 && r.Key[0] == 's' && len(r.Value) > 0 {
				bad := append([]byte(nil), r.Value...)
				bad[0] ^= 0xff
				r.Value = bad
				return
			}
		}
	})

	_, _, err := verify.VerifySet(st, t.TempDir(), c, 2)
	if err == nil || !strings.Contains(err.Error(), "state root mismatch at block 3") {
		t.Fatalf("want state root mismatch at block 3, got: %v", err)
	}
}

// TestVerifyRefusesTheWrongChainAtTheAnchor is what survives of rulings 5 and
// 6 after the 2026-08-05 amendment.
//
// The chain root is sha256(genesisData) ALONE, so a different genesis is a
// DIFFERENT CHAIN and is refused at epoch 1's prev-hash, up front and by name.
// A missing upgrade.json does NOT move it, and it is not the anchor's job to
// catch one: upgrades apply inside blocks and never to genesis, a semantically
// wrong upgrade file diverges at its activation height where the executor's
// state-root check hard-stops on it, and a file that differs only in
// FORMATTING executes identically and must not manufacture a different chain.
//
// "Diverges at its activation height" is the load-bearing half of that, and it
// is pinned by exec/sevmexec's
// TestSubnetEVMWrongUpgradeJSONDivergesAtActivation. Neither test is worth much
// without the other: this one says the anchor is deliberately blind to the
// upgrade file, that one says execution is not.
func TestVerifyRefusesTheWrongChainAtTheAnchor(t *testing.T) {
	c := testChain()
	_, bs := buildChain(t, c)

	dir := t.TempDir()
	st := store(t, dir)
	sealEpochs(t, st, c, bs, nil)

	noUpgrade := *c
	noUpgrade.UpgradeJSON = nil
	if noUpgrade.Root() != c.Root() {
		t.Fatal("upgrade bytes moved the chain root: the anchor must be sha256(genesisData) alone")
	}

	other := *c
	other.GenesisJSON = append(append([]byte(nil), c.GenesisJSON...), ' ')
	if other.Root() == c.Root() {
		t.Fatal("a different genesisData did not move the chain root")
	}
	_, _, err := verify.VerifySet(st, t.TempDir(), &other, 2)
	if err == nil || !strings.Contains(err.Error(), "WRONG CHAIN") {
		t.Fatalf("want an anchor refusal naming the wrong chain, got: %v", err)
	}
}

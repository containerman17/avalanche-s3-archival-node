package rpc

import (
	"encoding/json"
	"math/big"
	"testing"

	"github.com/ava-labs/libevm/common"
	"github.com/ava-labs/libevm/core/types"
	"github.com/ava-labs/libevm/crypto"
	"github.com/ava-labs/libevm/rlp"

	"github.com/containerman17/epochdb/dist"
	"github.com/containerman17/epochdb/exec"
	"github.com/containerman17/epochdb/store"
)

// The storage v0 seam end to end: rows in through store.BlockWrite, answers
// out through dispatch. It covers the three paths that changed shape, so a
// regression in any of them fails here rather than on a corpus:
// block assembly from hdr/ + tx/ rows, the per-TRANSACTION receipt row, and
// the posting-driven eth_getLogs.

const testBlockTime = 1767225600 // past every C-chain upgrade in this dep set

func testServer(t *testing.T) (*Server, *types.Transaction, common.Address, common.Hash) {
	t.Helper()
	g, err := exec.ChainGenesis(nil) // Fuji C-chain: also registers the extras
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	cas, err := dist.Local(dir)
	if err != nil {
		t.Fatal(err)
	}
	db, err := store.Open(dir, cas, [32]byte{7})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })

	key, _ := crypto.GenerateKey()
	to := common.HexToAddress("0xbbbb000000000000000000000000000000000002")
	signer := types.MakeSigner(g.Config, big.NewInt(1), testBlockTime)
	tx, err := types.SignNewTx(key, signer, &types.LegacyTx{
		Nonce: 0, To: &to, Gas: 21000, GasPrice: big.NewInt(25_000_000_000),
	})
	if err != nil {
		t.Fatal(err)
	}
	logAddr := common.HexToAddress("0xcccc000000000000000000000000000000000003")
	topic := common.HexToHash("0x1111111111111111111111111111111111111111111111111111111111111111")
	receipt := &types.Receipt{Status: 1, GasUsed: 21000, Logs: []*types.Log{{
		Address: logAddr, Topics: []common.Hash{topic}, Data: []byte{0xaa},
	}}}
	txRLP, err := tx.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}

	header := func(n uint64) []byte {
		h := &types.Header{
			Number:     new(big.Int).SetUint64(n),
			Time:       testBlockTime,
			GasLimit:   15_000_000,
			BaseFee:    big.NewInt(25_000_000_000),
			Difficulty: big.NewInt(1),
			Extra:      make([]byte, 0),
		}
		raw, err := rlp.EncodeToBytes(h)
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	// Block 0 carries no transactions, block 1 carries the one above.
	if err := db.WriteBlock(&store.BlockWrite{Height: 0, HeaderRLP: header(0)}); err != nil {
		t.Fatal(err)
	}
	if err := db.WriteBlock(&store.BlockWrite{
		Height: 1, HeaderRLP: header(1),
		Txs: []store.TxWrite{{
			Hash:     tx.Hash().Bytes(),
			RLP:      txRLP,
			Receipt:  store.EncodeTxReceipt(receipt, 21000),
			LogAddrs: [][]byte{logAddr.Bytes()},
			Topics:   [][]byte{topic.Bytes()},
		}},
	}); err != nil {
		t.Fatal(err)
	}
	return NewServer(db, g.TrieAlloc, StoreChainContext(db), g.Config), tx, logAddr, topic
}

func call(t *testing.T, s *Server, method string, params ...any) (any, *rpcError) {
	t.Helper()
	raw := make([]json.RawMessage, len(params))
	for i, p := range params {
		b, err := json.Marshal(p)
		if err != nil {
			t.Fatal(err)
		}
		raw[i] = b
	}
	return s.dispatch(&rpcRequest{Method: method, Params: raw})
}

func TestServeStorageV0(t *testing.T) {
	srv, tx, logAddr, topic := testServer(t)

	if res, rerr := call(t, srv, "eth_blockNumber"); rerr != nil || res != "0x1" {
		t.Fatalf("eth_blockNumber: %v %v", res, rerr)
	}

	// Block assembly: header row + tx rows, no container anywhere.
	res, rerr := call(t, srv, "eth_getBlockByNumber", "0x1", true)
	if rerr != nil {
		t.Fatalf("eth_getBlockByNumber: %v", rerr)
	}
	blk := res.(map[string]any)
	if got := blk["transactions"].([]any); len(got) != 1 {
		t.Fatalf("block 1 has %d transactions", len(got))
	}

	// The tx hash lookup is one row, no candidate walk.
	res, rerr = call(t, srv, "eth_getTransactionByHash", tx.Hash())
	if rerr != nil {
		t.Fatalf("eth_getTransactionByHash: %v", rerr)
	}
	if got := res.(*rpcTransaction); got.Hash != tx.Hash() || got.BlockNumber.ToInt().Uint64() != 1 {
		t.Fatalf("tx by hash: %+v", got)
	}

	// The receipt comes from the PER-TRANSACTION row.
	res, rerr = call(t, srv, "eth_getTransactionReceipt", tx.Hash())
	if rerr != nil {
		t.Fatalf("eth_getTransactionReceipt: %v", rerr)
	}
	rcpt := res.(map[string]any)
	if len(rcpt["logs"].([]*types.Log)) != 1 {
		t.Fatalf("receipt logs: %v", rcpt["logs"])
	}

	// eth_getLogs through the postings, and a filter that must NOT match.
	logs, rerr := call(t, srv, "eth_getLogs", map[string]any{
		"fromBlock": "0x1", "toBlock": "0x1", "address": logAddr,
		"topics": []any{topic},
	})
	if rerr != nil || len(logs.([]*types.Log)) != 1 {
		t.Fatalf("eth_getLogs matching: %v %v", logs, rerr)
	}
	logs, rerr = call(t, srv, "eth_getLogs", map[string]any{
		"fromBlock": "0x1", "toBlock": "0x1",
		"address": common.HexToAddress("0xdead000000000000000000000000000000000000"),
	})
	if rerr != nil || len(logs.([]*types.Log)) != 0 {
		t.Fatalf("eth_getLogs non-matching: %v %v", logs, rerr)
	}
	// No address and no topics: the range scan, not an empty answer.
	logs, rerr = call(t, srv, "eth_getLogs", map[string]any{"fromBlock": "0x1", "toBlock": "0x1"})
	if rerr != nil || len(logs.([]*types.Log)) != 1 {
		t.Fatalf("eth_getLogs wildcard: %v %v", logs, rerr)
	}

	// Block-hash keyed methods refuse by name; they never answer null or [].
	if _, rerr := call(t, srv, "eth_getBlockByHash", common.Hash{1}, false); rerr == nil ||
		rerr.Message != "eth_getBlockByHash: not in phase 1" {
		t.Fatalf("eth_getBlockByHash: %v", rerr)
	}
}
